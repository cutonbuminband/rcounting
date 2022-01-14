"""Script for logging reddit submissions to either a database or a csv file"""
import functools
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import click
import pandas as pd

import rcounting as rct
import rcounting.thread_directory as td
import rcounting.thread_navigation as tn
from rcounting.reddit_interface import reddit

_, document = td.load_wiki_page(reddit.subreddit("counting"), "directory")
threads = rct.utils.flatten([x[1] for x in document if x[0] == "table"])
first_submissions = [x[1] for x in threads]


def find_comment(leaf_comment_id, verbosity):
    """
    Return the comment associated with the leaf comment id.
    If the leaf comment id is none, return the latest comment in the directory
    """
    if not leaf_comment_id:
        comment_id = threads[0][4]
        return tn.find_previous_get(reddit.comment(comment_id), verbosity=verbosity)
    return reddit.comment(leaf_comment_id)


class Logger:
    """Simple class for logging either to a database or to a csv file"""

    def __init__(self, sql, output_directory, filename=None, verbosity=1):
        self.sql = sql
        self.output_directory = output_directory
        self.last_checkpoint = ""
        if self.sql:
            self.setup_sql(filename)
        self.verbosity = verbosity
        self.log = self.log_sql if self.sql else self.log_csv

    def setup_sql(self, filename):
        """Connect to the database and get list of existing submissions, if any"""
        last_checkpoint = ""
        known_submissions = []
        if filename is None:
            filename = "counting.sqlite"
        path = self.output_directory / filename
        print(f"Writing submissions to sql database at {path}")
        db = sqlite3.connect(path)
        try:
            known_submissions = pd.read_sql("select * from submissions", db)[
                "submission_id"
            ].tolist()
            checkpoints = pd.read_sql("select submission_id from last_submission", db)
            last_checkpoint = checkpoints.iat[-1, 0]
        except pd.io.sql.DatabaseError:
            pass
        self.db = db
        self.last_checkpoint = last_checkpoint
        self.known_submissions = known_submissions

    def is_already_logged(self, comment):
        """Determine whether a submission has already been logged"""
        if self.sql:
            return comment.submission.id in self.known_submissions
        body = rct.parsing.strip_markdown_links(comment.body)
        basecount = rct.parsing.find_count_in_text(body) - 1000
        hoc_path = self.output_directory / Path(f"{basecount}.csv")
        return os.path.isfile(hoc_path)

    def log_sql(self, comment):
        """Save one submission to a database"""
        df = pd.DataFrame(
            tn.fetch_comments(comment, use_pushshift=False, verbosity=self.verbosity)
        )
        submission = pd.Series(rct.models.Submission(comment.submission).to_dict())
        submission = submission[["submission_id", "username", "timestamp", "title", "body"]]
        submission["integer_id"] = int(submission["submission_id"], 36)
        df.to_sql("comments", self.db, index_label="position", if_exists="append")
        submission.to_frame().T.to_sql("submissions", self.db, index=False, if_exists="append")

    def log_csv(self, comment):
        """Save one submission to a csv file"""
        df = pd.DataFrame(
            tn.fetch_comments(comment, use_pushshift=False, verbosity=self.verbosity)
        )
        extract_count = functools.partial(rct.parsing.find_count_in_text, raise_exceptions=False)
        n = int(1000 * ((df["body"].apply(extract_count) - df.index).median() // 1000))
        path = self.output_directory / Path(f"{n}.csv")

        columns = ["username", "timestamp", "comment_id", "submission_id"]
        output_df = df.set_index(df.index + n)[columns].iloc[1:]
        if self.verbosity > 0:
            print(f"Writing submission log to {path}")
        header = ["username", "timestamp", "comment_id", "submission_id"]
        with open(path, "w", encoding="utf8") as f:
            print(f"# {comment.submission.title}", file=f)
            print(output_df.to_csv(index_label="count", header=header), file=f, end="")


@click.command()
@click.argument("leaf_comment_id", default="")
@click.option("--all", "-a", "all_counts", is_flag=True)
@click.option("-n", "--n-threads", default=1, help="The number of submissions to log.")
@click.option(
    "--filename",
    "-f",
    type=click.Path(path_type=Path),
    help=(
        "What file to write output to. If none is specified, counting.sqlite is used as default in"
        " sql mode, and the base count is used in csv mode."
    ),
)
@click.option(
    "-o",
    "--output-directory",
    default=".",
    type=click.Path(path_type=Path),
    help="The directory to use for output. Default is the current working directory",
)
@click.option(
    "--sql/--csv",
    default=False,
    help="Write submissions to csv files (one per thread) or to a database.",
)
@click.option(
    "--side-thread/--main",
    "-s/-m",
    default=False,
    help=(
        "Log the main thread or a side thread. Get validation is "
        "switched off for side threads, and only sqlite output is supported"
    ),
)
@click.option("--verbose", "-v", "verbosity", count=True, help="Print more output")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress output")
def log(
    leaf_comment_id,
    all_counts,
    n_threads,
    filename,
    output_directory,
    sql,
    side_thread,
    verbosity,
    quiet,
):  # pylint: disable=too-many-arguments
    """
    Log the reddit submission which ends in LEAF_COMMENT_ID.
    If no comment id is provided, use the latest completed thread found in the thread directory.
    By default, assumes that this is part of the main chain, and will attempt to
    find the true get if the gz or the assist are linked instead.
    """
    t_start = datetime.now()
    rct.utils.ensure_directory(output_directory)

    if side_thread or (
        filename is not None
        and (n_threads != 1 or all_counts or Path(filename).suffix == ".sqlite")
    ):
        sql = True
    verbosity = (1 - quiet) * (1 + verbosity)

    comment = find_comment(leaf_comment_id, verbosity)
    print(
        f"Logging {'all' if all_counts else n_threads} "
        f"reddit submission{'s' if (n_threads > 1) or all_counts else ''} "
        f"starting at comment id {comment.id} and moving backwards"
    )

    logger = Logger(sql, output_directory, filename, verbosity)
    completed = 0

    while (not all_counts and (completed < n_threads)) or (
        all_counts and comment.submission.id != logger.last_checkpoint
    ):
        if verbosity > 0:
            print(f"Logging reddit submission {comment.submission.id}")
        completed += 1
        if not logger.is_already_logged(comment):
            logger.log(comment)
        elif verbosity > 0:
            print(f"Thread {comment.submission.id} has already been logged!")

        if comment.submission.id in first_submissions:
            break

        comment = tn.find_previous_get(comment, validate_get=not side_thread, verbosity=verbosity)

    if (
        completed
        and sql
        and (comment.submission.id in first_submissions + [logger.last_checkpoint])
    ):
        newest_submission = pd.read_sql(
            "select submission_id from submissions order by integer_id", logger.db
        ).iloc[-1]
        newest_submission.name = "submission_id"
        newest_submission.to_sql("last_submission", logger.db, index=False, if_exists="append")

    if completed == 0:
        print("The database is already up to date!")
    print(f"Running the script took {datetime.now() - t_start}")


if __name__ == "__main__":
    log()  # pylint: disable=no-value-for-parameter
