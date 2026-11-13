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
from rcounting.counters import is_banned_counter
from rcounting.scripts import log_all_side_threads

printer = logging.getLogger("rcounting")

WEEK = 7 * units.DAY
temp_filename = Path("temp.sqlite")
ongoing_db = sqlite3.connect(temp_filename)


def find_directory_revision(subreddit, threshold, kind: Literal["pre"] | Literal["post"] = "post"):
    """Find the directory revision closest to a threshold timestamp.

    If kind == pre, find the last revision which was made before the threshold.
    If kind == post, find the first revision after the threshold

    If the latest revision occurs before the threshold timestamp, just return that.
    """
    revisions = subreddit.wiki["directory"].revisions()
    old_revision = next(revisions)
    if old_revision["timestamp"] < threshold:
        return old_revision
    loop = False
    for new_revision in revisions:
        if new_revision["timestamp"] < threshold:
            return old_revision if kind == "post" else new_revision
        old_revision = new_revision
        loop = True
    return new_revision if loop else old_revision


def download_counts(reddit, rows, ftf_timestamp):
    """Download all side thread counts on the active threads in the list of
    rows starting one week before the ftf_timestamp.

    """
    threshold = ftf_timestamp - WEEK
    try:
        df = pd.read_sql("select * from threads", ongoing_db)
        known_rows = list(df["thread_id"])
        completed_rows = list(df.query("checkpoint == @ftf_timestamp")["thread_id"])
    except pd.io.sql.DatabaseError:
        completed_rows = []
        known_rows = []
    for row in rows:
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
        df.to_sql("comments", ongoing_db, index=False, if_exists="append")
        if row in known_rows:
            ongoing_db.execute(
                "UPDATE threads SET checkpoint = ? WHERE thread_id == ?",
                (ftf_timestamp, row.first_submission),
            )
            ongoing_db.commit()
        else:
            row = pd.DataFrame([{"thread_id": row.first_submission, "checkpoint": ftf_timestamp}])
            row.to_sql("threads", ongoing_db, if_exists="append")


def get_stats(reddit, subreddit, ftf_timestamp, filename):
    """Returns a dataframe representing all side thread counts that occurred
    from the beginning of time to the end of the ftf period, as well as a
    mapping of thread_ids -> thread names

    """
    db = sqlite3.connect(filename)
    revision = find_directory_revision(subreddit, ftf_timestamp)
    contents = revision["page"].content_md.replace("\r\n", "\n")
    directory = td.Directory(parsing.parse_directory_page(contents), "directory")

    name_mapping = {row.first_submission: row.name for row in directory.rows[1:]}
    name_mapping["5h0i88"] = "XKCD"

    # Find the state of play just before the start of the current ftf period
    old_revision = find_directory_revision(subreddit, ftf_timestamp - WEEK, kind="pre")
    old_contents = old_revision["page"].content_md.replace("\r\n", "\n")
    old_directory = td.Directory(parsing.parse_directory_page(old_contents), "directory")
    known_rows = [(r.initial_submission_id, r.initial_comment_id) for r in old_directory.rows]

    # The threads which haven't moved since can be ignored
    rows = [
        r
        for r in directory.rows[1:]
        if (r.initial_submission_id, r.initial_comment_id) not in known_rows
    ]

    download_counts(reddit, rows, ftf_timestamp)
    query = (
        f"SELECT comments.username, comments.timestamp, comment_id, thread_id "
        "FROM comments JOIN submissions "
        f"ON comments.submission_id == submissions.submission_id "
    )
    completed_threads = pd.read_sql(query, db)
    partial_threads = pd.read_sql(
        "select comment_id, username, timestamp, thread_id from comments", ongoing_db
    )
    combined = (
        pd.concat([completed_threads, partial_threads])
        .pipe(
            lambda x: x.loc[
                (~x["username"].apply(is_banned_counter)) & (x["timestamp"] <= ftf_timestamp)
            ]
        )
        .drop_duplicates(subset=["comment_id"])
        .pipe(lambda x: x.assign(username=x["username"].apply(counters.apply_alias)))
    )
    return combined, name_mapping


def pprint(date):
    return date.strftime("%A %B %d, %Y")


def get_top_counters(stats, ftf_timestamp, n=15):
    """Generate the top counters table: A table tracking the number of counts
    made and the host rank (along with any change in the past week) for the n
    people who made the most side thread counts in the week leading up to
    ftf_timestamp"""
    data = (
        pd.merge(
            stats.groupby("username").size().rename("total"),
            stats.query("timestamp < @ftf_timestamp - @WEEK")
            .groupby("username")
            .size()
            .rename("old"),
            left_index=True,
            right_index=True,
            how="left",
        )
        .fillna(0)
        .pipe(lambda x: x.assign(new=x["total"] - x["old"]))
        .pipe(
            lambda x: x.assign(
                host_rank=x["total"].rank(ascending=False),
                old_host_rank=x["old"].rank(ascending=False),
            ),
        )
        .astype(int)
        .reset_index()
    )
    top_counters = (
        data.assign(delta=data["host_rank"] - data["old_host_rank"])
        .sort_values("new", ascending=False)
        .set_index(pd.RangeIndex(1, len(data) + 1))
        .pipe(lambda x: x.assign(host=x.apply(add_delta, axis=1)))
    )

    return top_counters[["username", "new", "host"]]


def get_top_threads(data, ftf_timestamp, name_mapping):
    """Return the number of counts and the thread_ids for the n threads which
    had the most counts in the week leading up to the timestamp represented by
    ftf_timestamp

    Parameters:
      - data: A dataframe with rows of (timestamp, thread_id, ...) representing side thread counts
      - name_mapping a dictionary of thread_id -> thread_name pairs
    """
    top_threads = (
        data.query("timestamp >= @ftf_timestamp - @WEEK")
        .groupby("thread_id")
        .size()
        .sort_values(ascending=False)
        .to_frame()
        .reset_index()
        .pipe(lambda x: x.set_index(x.index + 1))
    )
    return top_threads.assign(thread_id=top_threads["thread_id"].map(name_mapping))


def tables_to_body(top_counters, top_threads, ftf_timestamp):
    end = dt.date.fromtimestamp(ftf_timestamp)
    start = dt.date.fromtimestamp(ftf_timestamp - WEEK)

    tc = top_counters["username"].to_list()
    s = (
        f"Weekly side thread stats from {pprint(start)} to {pprint(end)}. "
        f"Congratulations to u/{tc[0]}, u/{tc[1]}, and u/{tc[2]}!\n\n"
    )
    s += f"Total weekly side thread counts: **{top_counters['new'].sum()}**\n\n"
    s += "Top 15 side thread counters:\n\n"
    s += top_counters.head(15).to_markdown(
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
    stats, name_mapping = get_stats(reddit, subreddit, ftf_timestamp, filename)
    top_counters = get_top_counters(stats, ftf_timestamp)
    top_threads = get_top_threads(stats, ftf_timestamp, name_mapping)
    body = tables_to_body(top_counters, top_threads, ftf_timestamp)
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


def add_delta(series):
    up_arrow = "◮"
    down_arrow = "⧩"
    delta = series["delta"]
    if delta == 0:
        return f"{series['host_rank']}"
    else:
        return f"{series['host_rank']} ({up_arrow if delta < 0 else down_arrow}{abs(delta)})"
