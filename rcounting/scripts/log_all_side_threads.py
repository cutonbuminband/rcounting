import logging
from pathlib import Path

import click

from rcounting.side_threads import known_thread_ids

from .log_thread import log_undecorated

printer = logging.getLogger("rcounting")


def log_side_threads(filename, verbose, quiet, archive=False):
    from rcounting import configure_logging
    from rcounting import thread_directory as td
    from rcounting import thread_navigation as tn
    from rcounting.reddit_interface import reddit, subreddit

    configure_logging.setup(printer, verbose, quiet)
    if not archive:
        directory = td.load_wiki_page(subreddit, "directory")
        rows = directory.rows[1:]
    else:
        directory = td.load_wiki_page(subreddit, "directory/archive")
        rows = [row for row in directory.rows if row.first_submission in known_thread_ids]
    for row in rows:
        if row.first_submission == row.initial_submission_id:
            continue
        side_thread_id = row.first_submission
        try:
            side_thread_name = known_thread_ids[side_thread_id]
        except KeyError:
            side_thread_name = f"Unknown side thread: {side_thread_id}"
        submission_id = row.initial_submission_id
        previous_submission, previous_get = tn.find_previous_submission(submission_id)
        if previous_submission is None:
            continue
        if previous_get is None:
            previous_get = tn.find_deepest_comment(previous_submission, reddit)

        printer.warning("Logging side thread %s", side_thread_name)

        log_undecorated(
            previous_get,
            all_counts=True,
            filename=filename,
            side_thread=True,
            verbose=verbose,
            quiet=quiet,
            side_thread_id=side_thread_id,
            n_threads=1,
            print_timing=False,
            first_submissions=directory.first_submissions,
        )


@click.command(name="log-side-threads")
@click.option(
    "--filename",
    "-f",
    type=click.Path(path_type=Path),
    help=("What file to write output to. If none is specified, side_threads.sqlite is used."),
    default="side_threads.sqlite",
)
@click.option("--verbose", "-v", count=True, help="Print more output")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress output")
@click.option(
    "--archive",
    "-a",
    is_flag=True,
    default=False,
    help="Add the known side threads from the archive",
)
def main(
    filename,
    verbose,
    quiet,
    archive,
):
    """Log every side thread in the thread directory and write it to an sql
    database. Warning: This will take a long time

    """
    log_side_threads(filename, verbose, quiet, archive)
