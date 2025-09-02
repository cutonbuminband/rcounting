import logging
from pathlib import Path

import click

from .log_thread import log_undecorated

printer = logging.getLogger("rcounting")


@click.command(name="log-side-threads")
@click.option(
    "--filename",
    "-f",
    type=click.Path(path_type=Path),
    help=("What file to write output to. If none is specified, side_threads.sqlite is used."),
)
@click.option("--verbose", "-v", count=True, help="Print more output")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress output")
def log_side_threads(
    filename,
    verbose,
    quiet,
):
    from rcounting import configure_logging
    from rcounting import thread_directory as td
    from rcounting import thread_navigation as tn
    from rcounting.reddit_interface import reddit, subreddit

    configure_logging.setup(printer, verbose, quiet)
    directory = td.load_wiki_page(subreddit, "directory")
    for row in directory.rows[1:]:
        side_thread_id = row.first_submission
        submission_id = row.initial_submission_id
        previous_submission, previous_get = tn.find_previous_submission(submission_id)
        if previous_submission is None:
            continue
        if previous_get is None:
            previous_get = tn.find_deepest_comment(previous_submission, reddit)

        log_undecorated(
            previous_get,
            all_counts=True,
            filename=filename,
            output_directory=".",
            sql=True,
            side_thread=True,
            verbose=verbose,
            quiet=quiet,
            side_thread_id=side_thread_id,
            n_threads=1,
        )
