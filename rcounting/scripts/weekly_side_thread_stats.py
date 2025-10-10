# pylint: disable=import-outside-toplevel
import datetime as dt
import logging
import sqlite3
from pathlib import Path

import click
import pandas as pd
from fuzzywuzzy import fuzz

from rcounting import configure_logging, counters, ftf, models, parsing, side_threads, units
from rcounting import thread_directory as td
from rcounting.scripts import log_all_side_threads

printer = logging.getLogger("rcounting")

WEEK = 7 * units.DAY
temp_filename = Path("temp.sqlite")
temp_db = sqlite3.connect(temp_filename)


def find_directory_revision(subreddit, threshold):
    """Find the earliest directory revision which was made after a threshold
    timestamp. If no such revision exists, return the latest revision.

    """
    revisions = subreddit.wiki["directory"].revisions()
    old_revision = next(revisions)
    first_revision = old_revision
    for new_revision in revisions:
        if new_revision["timestamp"] < threshold:
            return old_revision
        old_revision = new_revision
    return first_revision


def get_directory_counts(reddit, directory, ftf_timestamp, db):
    """
    Find all side thread counts on threads logged in the thread directory
    starting one week before the ftf_timestamp. Use the thread directory to log
    the current threads, and use the database to log completed threads.

    """
    threshold = ftf_timestamp - WEEK
    try:
        df = pd.read_sql("select distinct thread_id from comments", temp_db)
        completed_rows = list(df["thread_id"])
    except pd.io.sql.DatabaseError:
        completed_rows = []
    for row in directory.rows[1:]:
        if row.first_submission in completed_rows:
            continue
        printer.info("Getting latest history for %s", row.name)
        tree = models.CommentTree(reddit=reddit, get_missing_replies=False)
        comments = tree.walk_up_tree(row.comment_id, cutoff=threshold) or []
        tree.add_missing_replies(row.comment_id, limit=10)
        st = side_threads.get_side_thread(row.thread_type)
        tree.prune(st, row.comment_id)
        replies = tree.walk_down_tree(row.comment_id, cutoff=ftf_timestamp) or []
        df = pd.DataFrame([models.comment_to_dict(c) for c in comments + replies])
        df["thread_id"] = row.first_submission
        df.to_sql("comments", temp_db, index=False, if_exists="append")
    query = (
        f"SELECT comments.*, thread_id FROM comments JOIN submissions "
        f"ON comments.submission_id == submissions.submission_id "
        f"WHERE comments.timestamp >= {threshold}"
    )

    completed_threads = pd.read_sql(query, db)
    partial_threads = pd.read_sql("select * from comments", temp_db)
    combined = pd.concat([completed_threads, partial_threads])
    return combined[combined["timestamp"] <= ftf_timestamp].drop_duplicates()


def get_weekly_stats(reddit, subreddit, ftf_timestamp, filename):
    db = sqlite3.connect(filename)
    revision = find_directory_revision(subreddit, ftf_timestamp)
    contents = revision["page"].content_md.replace("\r\n", "\n")
    directory = td.Directory(parsing.parse_directory_page(contents), "directory")
    name_mapping = {row.first_submission: row.name for row in directory.rows[1:]}
    return get_directory_counts(reddit, directory, ftf_timestamp, db), name_mapping


def pprint(date):
    return date.strftime("%A %B %d, %Y")


def stats_post(stats, old_counts, ftf_timestamp, name_mapping=None):
    end = dt.date.fromtimestamp(ftf_timestamp)
    start = dt.date.fromtimestamp(ftf_timestamp - WEEK)
    stats["username"] = stats["username"].apply(counters.apply_alias)
    new_counts = stats.groupby("username").size().to_frame(name="new_count").reset_index()
    combined = pd.merge(
        left=old_counts, right=new_counts, left_on="username", right_on="username", how="outer"
    ).fillna(0)

    combined["old_count"] = combined["old_count"].astype(int)
    combined["new_count"] = combined["new_count"].astype(int)
    combined["total"] = combined["old_count"] + combined["new_count"]
    combined = (
        combined.sort_values(["total"], ascending=False)
        .reset_index(drop=True)
        .reset_index(names=["host_rank"])
    )
    combined = (
        combined.sort_values(["old_count"], ascending=False)
        .reset_index(drop=True)
        .reset_index(names=["old_host_rank"])
    )
    combined["delta"] = combined["host_rank"] - combined["old_host_rank"]
    top_counters = (
        combined.loc[~combined["username"].apply(counters.is_banned_counter)]
        .sort_values("new_count", ascending=False)
        .reset_index(drop=True)
        .head(15)
        .copy()
    )

    top_counters.index += 1
    top_counters["host"] = top_counters.apply(add_delta, axis=1)
    columns = ["username", "new_count", "host"]
    tc = top_counters["username"].to_numpy()

    top_threads = stats.groupby("thread_id").size().sort_values(ascending=False).to_frame()
    if name_mapping is not None:
        top_threads.index = [name_mapping[i] for i in top_threads.index]
    top_threads = top_threads.reset_index()
    top_threads.index += 1
    s = (
        f"Weekly side thread stats from {pprint(start)} to {pprint(end)}. "
        f"Congratulations to u/{tc[0]}, u/{tc[1]}, and u/{tc[2]}!\n\n"
    )
    s += f"Total weekly side thread counts: **{len(stats)}**\n\n"
    s += "Top 15 side thread counters:\n\n"
    s += top_counters[columns].to_markdown(
        headers=["**Rank**", "**User**", "**Counts**", "**HoST Rank**"]
    )
    s += "\n\nTop 5 side threads:\n\n"
    s += top_threads.head(5).to_markdown(headers=["**Rank**", "**Thread**", "**Counts**"])
    s += "\n\n----\n\n"
    s += "*This comment was made by a script; check it out "
    # pylint: disable-next=line-too-long
    s += "[here](https://github.com/cutonbuminband/rcounting/blob/main/rcounting/scripts/weekly_side_thread_stats.py)*"
    return s


def is_duplicate(body, post):
    scores = [(comment.permalink, fuzz.ratio(body, comment.body)) for comment in post.comments]
    if not scores:
        return False, None
    link, score = max(scores, key=lambda x: x[1])
    return score > 90, link


@click.command(name="st-stats")
@click.option(
    "--dry-run", is_flag=True, help="Write results to console instead of making a comment"
)
@click.option(
    "--update-db/--no-update-db",
    default=False,
    help=("If set, add the data for all newly completed side threads to the database"),
)
@click.option("--verbose", "-v", count=True, help="Print more output")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress output")
@click.option(
    "--filename",
    "-f",
    type=click.Path(path_type=Path),
    help=(
        "Where to find (and optionally write) previous side thread stats. "
        "If none is specified, side_threads.sqlite is used"
    ),
    default="side_threads.sqlite",
)
def generate_stats_post(filename, update_db, dry_run, verbose, quiet):
    """Load all the side thread counts made in the previous FTF period and
    store them in a database. Then post a side thread stats comment in the
    current FTF as long as:

    - The current FTF is not stale
    - The comment has not been posted before.

    """
    t_start = dt.datetime.now()
    ftf_timestamp = ftf.get_ftf_timestamp().timestamp()
    threshold = ftf_timestamp - WEEK

    from rcounting.reddit_interface import reddit, subreddit

    configure_logging.setup(printer, verbose, quiet)
    if update_db:
        log_all_side_threads.log_side_threads(filename=filename, verbose=verbose, quiet=quiet)
    stats, name_mapping = get_weekly_stats(reddit, subreddit, ftf_timestamp, filename)
    db = sqlite3.connect(filename)
    query = (
        f"SELECT canonical_username as username, count() as old_count "
        f"FROM comments join counters on counters.username == comments.username "
        f"WHERE comments.position > 0 and comments.timestamp <= {threshold} "
        f"GROUP by canonical_username"
    )

    old_counts = pd.read_sql(query, db)
    db.close()
    body = stats_post(stats, old_counts, ftf_timestamp, name_mapping)
    if dry_run:
        print(body)
    else:
        ftf_post = subreddit.sticky(number=2)
        duplicate, link = is_duplicate(body, ftf_post)
        if ftf.is_within_threshold(ftf_post) and not duplicate:
            ftf_post.reply(body)
        elif not duplicate:
            printer.warning("Not posting stats comment. Pinned FTF is stale")
        else:
            s = "Not posting stats comment. Existing comment found at https://www.reddit.com%s"
            printer.warning(s, link)
    printer.warning("Running the script took %s", dt.datetime.now() - t_start)
    Path.unlink(temp_filename)


def add_delta(series):
    up_arrow = "◮"
    down_arrow = "⧩"
    delta = series["delta"]
    if delta == 0:
        return f"{series['host_rank'] + 1}"
    else:
        return f"{series['host_rank']} ({up_arrow if delta < 0 else down_arrow}{abs(delta)})"
