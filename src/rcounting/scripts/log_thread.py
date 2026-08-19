# pylint: disable=import-outside-toplevel,too-many-arguments,too-many-locals
"""Script for logging reddit submissions to either a database or a csv file"""

import logging
import time
from datetime import datetime
from pathlib import Path

import click
import pandas as pd
from prawcore.exceptions import TooManyRequests

printer = logging.getLogger("rcounting")


@click.command()
@click.argument("leaf_comment_id", default="")
@click.option("-n", "--n-threads", default=1, help="The number of submissions to log.")
@click.option("--all", "-a", "all_counts", is_flag=True, help="Log all threads. Can take a while!")
@click.option(
    "--filename",
    "-f",
    type=click.Path(path_type=Path),
    help=(
        "What file to write output to. If none is specified, counting.sqlite is used as default."
    ),
    default="counting.sqlite",
)
@click.option(
    "--side-thread/--main",
    "-s/-m",
    default=False,
    help=(
        "Log the main thread or a side thread. Get validation is switched off for side threads."
    ),
)
@click.option("--verbose", "-v", count=True, help="Print more output")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress output")
def log(
    leaf_comment_id,
    all_counts,
    n_threads,
    filename,
    side_thread,
    verbose,
    quiet,
):
    """
    Log the reddit submission which ends in LEAF_COMMENT_ID.
    If no comment id is provided, use the latest completed thread found in the thread directory.
    By default, assumes that this is part of the main chain, and will attempt to
    find the true get if the gz or the assist are linked instead.
    """

    # This is incredibly silly and hacky but I can't think of a better way of
    # doing it right now. We need access to the undecorated log function for the
    # side thread logging, and I don't see any easy way of getting there. And I'm
    # not about to start learning about click's context rules, so a hack it is.
    # We'll manually extract the undecorated function and just call it here

    log_undecorated(
        leaf_comment_id,
        all_counts,
        n_threads,
        filename,
        side_thread,
        verbose,
        quiet,
    )


def log_undecorated(
    leaf_comment_id,
    all_counts,
    n_threads,
    filename,
    side_thread,
    verbose,
    quiet,
    first_submissions=None,
    side_thread_id=None,
    print_timing=True,
):
    from rcounting import configure_logging
    from rcounting import thread_directory as td
    from rcounting import thread_navigation as tn
    from rcounting.io import ThreadLogger, update_counters_table
    from rcounting.reddit_interface import reddit, subreddit

    # Create the output directory if it doesn't already exist.
    filename.parent.mkdir(parents=True, exist_ok=True)
    t_start = datetime.now()

    configure_logging.setup(printer, verbose, quiet)
    directory = None
    if not leaf_comment_id:
        directory = td.load_wiki_page(subreddit, "directory")
        comment = tn.find_previous_get(directory.rows[0].submission_id)
    else:
        comment = reddit.comment(leaf_comment_id)
    printer.debug(
        "Logging %s reddit submission%s starting at comment id %s and moving backwards",
        "all" if all_counts else n_threads,
        "s" if (n_threads > 1) or all_counts else "",
        comment.id,
    )

    if not first_submissions:
        if not directory:
            directory = td.load_wiki_page(subreddit, "directory")
        first_submissions = directory.first_submissions
    threadlogger = ThreadLogger(filename, not side_thread, side_thread_id)
    completed = 0

    submission = comment.submission
    submission_id = None
    comment_id = comment.id
    multiple = 1
    while (not all_counts and (completed < n_threads)) or (
        all_counts and submission.id != threadlogger.last_checkpoint
    ):
        printer.info("Logging %s", submission.title)
        if not threadlogger.is_already_logged(submission):
            try:
                if submission_id is not None:
                    comment = tn.find_get_in_submission(
                        submission_id, comment_id, validate_get=not side_thread
                    )
                df = pd.DataFrame(tn.fetch_comments(comment))
                threadlogger.log(comment, df)
            except TooManyRequests:
                printer.warning(
                    f"Getting rate limited. Sleeping for {30 * multiple} seconds and trying again"
                )
                time.sleep(30 * multiple)
                multiple *= 1.5
                continue
        else:
            printer.info("Submission %s has already been logged!", submission.title)

        if submission.id in first_submissions:
            break

        submission_id, comment_id = tn.find_previous_submission(submission)
        submission = reddit.submission(submission_id)

        multiple = 1
        completed += 1

    if completed:
        update_counters_table(threadlogger.db)
        if submission.id in first_submissions + [threadlogger.last_checkpoint]:
            threadlogger.update_checkpoint()
    else:
        printer.info("The database is already up to date!")
    if print_timing:
        printer.info("Running the script took %s", datetime.now() - t_start)
