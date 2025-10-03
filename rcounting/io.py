import logging
import sqlite3
from pathlib import Path

import pandas as pd

from rcounting import counters, models, parsing

printer = logging.getLogger(__name__)


class ThreadLogger:
    """Simple class for logging either to a database or to a csv file"""

    def __init__(
        self,
        filename: Path | str | None = None,
        is_main: bool = True,
        side_thread_id: str | None = None,
    ):
        self.is_main = is_main
        self.side_thread_id = side_thread_id
        self.last_checkpoint = ""
        self.setup_sql(filename)

    def setup_sql(self, filename):
        """Connect to the database and get list of existing submissions, if any"""
        last_checkpoint = ""
        known_submissions = []
        if filename is None:
            filename = "counting.sqlite"
        path = Path(filename)
        printer.debug("Writing submissions to sql database at %s", path)
        db = sqlite3.connect(path)
        try:
            if self.side_thread_id is not None:
                known_submissions = pd.read_sql(
                    f"select submission_id from submissions "
                    f"where submissions.thread_id == '{self.side_thread_id}'",
                    db,
                )
            else:
                known_submissions = pd.read_sql("select submission_id from submissions", db)
            known_submissions = list(known_submissions["submission_id"])
        except pd.io.sql.DatabaseError:
            # The database error is for when the table doesn't exist in the
            # database. That's not a big deal, so we just write a log message
            # and continue merrily on our way.
            printer.warning("Finding known submissions failed")
            pass
        try:
            if self.side_thread_id is not None:
                checkpoint = pd.read_sql(
                    f"select submission_id from checkpoints "
                    f"where thread_id == '{self.side_thread_id}'",
                    db,
                )
            else:
                checkpoint = pd.read_sql("select submission_id from checkpoints", db).iloc[-1]
            if checkpoint.empty:
                last_checkpoint = ""
            else:
                last_checkpoint = checkpoint.squeeze()
        except pd.io.sql.DatabaseError:
            printer.warning("Finding previous checkpoints failed")
            pass
        self.db = db
        self.last_checkpoint = last_checkpoint
        self.known_submissions = known_submissions

    def is_already_logged(self, submission):
        """
        Determine whether a given submission already exists in the database.
        """
        return submission.id in self.known_submissions

    def log(self, comment, df):
        """Save one submission to a database"""
        submission = pd.Series(models.submission_to_dict(comment.submission))
        submission = submission[["submission_id", "username", "timestamp", "title", "body"]]
        if self.is_main:
            submission["base_count"] = base_count(df)
        if self.side_thread_id is not None:
            submission["thread_id"] = self.side_thread_id
        df.to_sql("comments", self.db, index_label="position", if_exists="append")
        submission.to_frame().T.to_sql("submissions", self.db, index=False, if_exists="append")

    def update_checkpoint(self):
        if self.side_thread_id is not None:
            newest_submission = pd.read_sql(
                "select submission_id from submissions "
                f"where submissions.thread_id == '{self.side_thread_id}' "
                "order by timestamp desc limit 1",
                self.db,
            ).squeeze()
            checkpoints = pd.DataFrame(
                columns=["submission_id"], index=pd.Series([], name="thread_id")
            )
            try:
                checkpoints = pd.read_sql(
                    "select * from checkpoints", self.db, index_col="thread_id"
                )
            except pd.io.sql.DatabaseError:
                pass
            checkpoints.loc[self.side_thread_id] = newest_submission
            checkpoints.to_sql("checkpoints", self.db, if_exists="replace")
        else:
            newest_submission = pd.read_sql(
                "select submission_id from submissions order by timestamp desc limit 1", self.db
            )
            newest_submission.name = "submission_id"
            newest_submission.to_sql("checkpoints", self.db, index=False, if_exists="replace")


def update_counters_table(db):
    counting_users = pd.read_sql("select distinct username from comments", db)
    counting_users["canonical_username"] = counting_users["username"].apply(counters.apply_alias)
    counting_users["is_mod"] = counting_users["username"].apply(counters.is_mod)
    counting_users["is_banned"] = counting_users["username"].apply(counters.is_banned_counter)
    counting_users.to_sql("counters", db, index=False, if_exists="replace")


def base_count(df):
    return int(round((df["body"].apply(parsing.wrapped_count_in_text) - df.index).median(), -3))
