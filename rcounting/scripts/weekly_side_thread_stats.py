# pylint: disable=import-outside-toplevel
import datetime as dt
import logging
import time

import click
import pandas as pd
from prawcore.exceptions import TooManyRequests

from rcounting import configure_logging, counters, ftf, models, parsing
from rcounting import thread_directory as td
from rcounting import thread_navigation as tn
from rcounting import units

printer = logging.getLogger("rcounting")

WEEK = 7 * units.DAY
ftf_timestamp = ftf.get_ftf_timestamp().timestamp()
threshold_timestamp = ftf_timestamp - WEEK


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


def get_directory_counts(reddit, directory, threshold):
    counts = []
    for row in directory.rows[1:]:
        printer.info("Getting history for %s", row.name)
        counts.append((get_side_thread_counts(reddit, row, threshold), row.name))
    return counts


def get_side_thread_counts(reddit, row, threshold):
    """Return a list of all counts in the side thread represented by row that
    occurred after `threshold`.
    """
    submission_id = row.submission_id
    comment_id = row.comment_id
    submission = reddit.submission(submission_id)
    comments = []
    multiple = 1
    while submission.created_utc >= threshold:
        printer.debug("Fetching submission %s", submission.title)
        tree = models.CommentTree(reddit=reddit, get_missing_replies=False)
        try:
            new_comments = tree.walk_up_tree(comment_id)
            if new_comments is not None:
                comments += new_comments[:-1]
        # We're bumping up against reddit's API limits, so we'll manually
        # include an exponential backoff. I don't know why PRAW doesn't catch
        # this in the HTTP headers, but I can see that it doesn't.
        except TooManyRequests:
            time.sleep(30 * multiple)
            multiple *= 1.5
            continue
        try:
            submission_id, comment_id = tn.find_previous_submission(submission)
        # We've hit the first submission in a new side thread
        except StopIteration:
            return comments
        submission = reddit.submission(submission_id)
        multiple = 1
    while True:
        tree = models.CommentTree(reddit=reddit, get_missing_replies=False)
        try:
            new_comments = tree.walk_up_tree(comment_id, cutoff=threshold)
            if new_comments is not None:
                comments += new_comments
            break
        except TooManyRequests:
            time.sleep(30 * multiple)
            multiple *= 1.5
    return comments


def get_weekly_stats(reddit, subreddit):
    revision = find_directory_revision(subreddit, ftf_timestamp)
    contents = revision["page"].content_md.replace("\r\n", "\n")
    directory = td.Directory(parsing.parse_directory_page(contents), "directory")
    counts = get_directory_counts(reddit, directory, threshold_timestamp)
    comments = pd.DataFrame(
        [
            dict(models.comment_to_dict(comment), **{"thread name": thread_name})
            for comments, thread_name in counts
            for comment in comments
        ]
    )
    return comments[comments["timestamp"] < ftf_timestamp]


def pprint(date):
    return date.strftime("%A %B %d, %Y")


def stats_post(stats):
    start = dt.date.fromtimestamp(ftf_timestamp)
    end = dt.date.fromtimestamp(threshold_timestamp)
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
        stats.groupby("thread name").size().sort_values(ascending=False).to_frame().reset_index()
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
    s += "\n\n--\n\n"
    s += "*This comment was made by a script; check it out "
    # pylint: disable-next=line-too-long
    s += "[here](https://github.com/cutonbuminband/rcounting/blob/main/rcounting/scripts/weekly_side_thread_stats.py)*"
    return s


@click.command(name="st-stats")
@click.option(
    "--dry-run", is_flag=True, help="Write results to console instead of making a comment"
)
@click.option("--verbose", "-v", count=True, help="Print more output")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress output")
def generate_stats_post(dry_run, verbose, quiet):
    t_start = dt.datetime.now()
    from rcounting.reddit_interface import reddit, subreddit

    configure_logging.setup(printer, verbose, quiet)
    stats = get_weekly_stats(reddit, subreddit)
    body = stats_post(stats)
    if dry_run:
        print(body)
    else:
        ftf_post = subreddit.sticky(number=2)
        if ftf.is_within_threshold(ftf_post):
            ftf_post.reply(body)
    printer.info("Running the script took %s", dt.datetime.now() - t_start)


if __name__ == "__main__":
    generate_stats_post()  # pylint: disable=no-value-for-parameter
