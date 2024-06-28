# pylint: disable=import-outside-toplevel
import datetime as dt
import logging
import sqlite3
import time
from pathlib import Path

import click
import pandas as pd
from fuzzywuzzy import fuzz
from prawcore.exceptions import TooManyRequests

from rcounting import configure_logging, counters, ftf, models, parsing
from rcounting import thread_directory as td
from rcounting import thread_navigation as tn
from rcounting import units

printer = logging.getLogger("rcounting")

WEEK = 7 * units.DAY


def find_directory_revision(subreddit, threshold):
    """Find the first directory revision which was made after a threshold
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
    threshold = ftf_timestamp - WEEK
    multiple = 1
    try:
        threads = pd.read_sql("select * from threads", db).set_index("thread_id")
    except pd.io.sql.DatabaseError:
        threads = pd.DataFrame()

    for row in directory.rows[1:]:
        try:
            checkpoint_timestamp = threads.loc[row.first_submission, "checkpoint_timestamp"]
        except KeyError:
            checkpoint_timestamp = 0
        if checkpoint_timestamp == threshold:
            printer.info("row %s has already been logged this week!", row.name)
        else:
            printer.info("Getting history for %s", row.name)
            while True:
                try:
                    submissions, comments = get_side_thread_counts(reddit, row, threshold)
                    multiple = 1
                    break
                except TooManyRequests:
                    time.sleep(30 * multiple)
                    multiple *= 1.5
            df = pd.DataFrame([models.comment_to_dict(c) for c in comments])
            if not df.empty:
                df = df[df["timestamp"] < ftf_timestamp]
                df.to_sql("comments", db, if_exists="append", index=False)
            write_submissions(db, submissions, row.first_submission)
            update_checkpoint(db, row.first_submission, row.name, threshold)
    query = (
        "SELECT comments.*, submissions.thread_id "
        "FROM comments JOIN submissions ON comments.submission_id==submissions.submission_id "
        f"WHERE comments.timestamp >= {threshold}"
    )
    comments = pd.read_sql(query, db).drop_duplicates()
    threads = pd.read_sql("select * from threads", db)

    return pd.merge(comments, threads, left_on="thread_id", right_on="thread_id", how="left")


def update_checkpoint(db, thread_id, name, timestamp):
    row = {"thread_id": thread_id, "thread_name": name, "checkpoint_timestamp": timestamp}
    try:
        threads = pd.read_sql("select * from threads", db)
        if (threads["thread_id"] == thread_id).any():
            threads.loc[threads["thread_id"] == thread_id, "checkpoint_timestamp"] = timestamp
        else:
            threads = pd.concat([threads, pd.DataFrame([row])])
    except pd.io.sql.DatabaseError:
        threads = pd.DataFrame([row])

    threads.to_sql("threads", db, index=False, if_exists="replace")


def write_submissions(db, submissions, thread_id):
    df = pd.DataFrame([models.submission_to_dict(submission) for submission in submissions])
    df["thread_id"] = thread_id
    try:
        existing_submissions = pd.read_sql("select * from submissions", db)
    except pd.io.sql.DatabaseError:
        existing_submissions = pd.DataFrame()

    new_submissions = pd.concat([existing_submissions, df]).drop_duplicates()
    new_submissions.to_sql("submissions", db, index=False, if_exists="replace")


def get_side_thread_counts(reddit, row, threshold):
    """Return a list of all counts in the side thread represented by row that
    occurred after `threshold`, as well as the corresponding submissions the
    comments belong to.

    """
    submission_id = row.submission_id
    comment_id = row.comment_id
    submission = reddit.submission(submission_id)
    comments = []
    submissions = [submission]
    while submission.created_utc >= threshold:
        printer.debug("Fetching submission %s", submission.title)
        tree = models.CommentTree(reddit=reddit, get_missing_replies=False)
        new_comments = tree.walk_up_tree(comment_id)
        if new_comments is not None:
            comments += new_comments[:-1]
        try:
            submission_id, comment_id = tn.find_previous_submission(submission)
            if not comment_id:
                return submissions, comments
        # We've hit the first submission in a new side thread
        except StopIteration:
            return submissions, comments
        submission = reddit.submission(submission_id)
        submissions.append(submission)
    tree = models.CommentTree(reddit=reddit, get_missing_replies=False)
    new_comments = tree.walk_up_tree(comment_id, cutoff=threshold)
    if new_comments is not None:
        comments += new_comments
    return submissions, comments


def get_weekly_stats(reddit, subreddit, ftf_timestamp, filename):
    db = sqlite3.connect(filename)

    revision = find_directory_revision(subreddit, ftf_timestamp)
    contents = revision["page"].content_md.replace("\r\n", "\n")
    directory = td.Directory(parsing.parse_directory_page(contents), "directory")
    return get_directory_counts(reddit, directory, ftf_timestamp, db)


def pprint(date):
    return date.strftime("%A %B %d, %Y")


def stats_post(stats, ftf_timestamp):
    end = dt.date.fromtimestamp(ftf_timestamp)
    start = dt.date.fromtimestamp(ftf_timestamp - WEEK)
    stats["canonical_username"] = stats["username"].apply(counters.apply_alias)

    top_counters = (
        stats.groupby("canonical_username")
        .size()
        .sort_values(ascending=False)
        .to_frame()
        .reset_index()
    )

    top_counters = top_counters[
        ~top_counters["canonical_username"].apply(counters.is_banned_counter)
    ].reset_index(drop=True)

    top_counters.index += 1
    tc = top_counters["canonical_username"].to_numpy()

    top_threads = (
        stats.groupby("thread_name").size().sort_values(ascending=False).to_frame().reset_index()
    )
    top_threads.index += 1
    s = (
        f"Weekly side thread stats from {pprint(start)} to {pprint(end)}. "
        f"Congratulations to u/{tc[0]}, u/{tc[1]}, and u/{tc[2]}!\n\n"
    )
    s += f"Total weekly side thread counts: **{len(stats)}**\n\n"
    s += "Top 15 side thread counters:\n\n"
    s += top_counters.head(15).to_markdown(headers=["**Rank**", "**User**", "**Counts**"])
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
@click.option("--verbose", "-v", count=True, help="Print more output")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress output")
@click.option(
    "--filename",
    "-f",
    type=click.Path(path_type=Path),
    help=("What file to write to. If none is specified, side_threads.sqlite is used"),
)
def generate_stats_post(filename, dry_run, verbose, quiet):
    """Load all the side thread counts made in the previous FTF period and
    store them in a database. Then post a side thread stats comment in the
    current FTF as long as:

    - The current FTF is not stale
    - The comment has not been posted before.

    """
    t_start = dt.datetime.now()
    ftf_timestamp = ftf.get_ftf_timestamp().timestamp()

    from rcounting.reddit_interface import reddit, subreddit

    configure_logging.setup(printer, verbose, quiet)
    stats = get_weekly_stats(reddit, subreddit, ftf_timestamp, filename)
    body = stats_post(stats, ftf_timestamp)
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


if __name__ == "__main__":
    generate_stats_post()  # pylint: disable=no-value-for-parameter
