import functools
import logging
from typing import Callable

import numpy as np
import pandas as pd

from rcounting import counters, parsing
from rcounting import thread_navigation as tn
from rcounting import utils
from rcounting.models import comment_to_dict

from .rules import default_rule
from .validate_count import base_n_count
from .validate_form import permissive

printer = logging.getLogger(__name__)


def make_title_updater(comment_to_count):
    @functools.wraps(comment_to_count)
    def wrapper(_, chain):
        title = chain[-1].title
        return comment_to_count(parsing.body_from_title(title))

    return wrapper


class SideThread:
    """A side thread class, which consists of a validation part and an update
    part In addition to checking whether a collection of counts is valid
    according to the side thread rule, the class can take a mapping
    comment->count and using this try and identify when errors were made in the
    chain. The class will also attempt to determine how many total counts have
    been made in a given side thread using one of:

    - The comment->count mapping to determine the current count, which is then
    applied to the submission title

    - The update_function parameter, which takes in the current state and
    returns the total number of counts. Sensible approaches for doing this are
    either parsing the current state from the title if it's different from the
    comment->count mapping, or traversing the chain of comments until the last
    known state is reached, and adding on all the comments encountered along
    the way. This is useful for threads which don't have a constant number of
    counts between gets, e.g. tug of war.

    - A standard thread length

    The approaches are listed in low->high priority, so if more than one
    approach is supplied the highest priority one is used.

    """

    def __init__(
        self,
        rule=default_rule,
        form: Callable[[str], bool] = permissive,
        length: int | None = None,
        comment_to_count: Callable[[str], int] | None = None,
        update_function=None,
    ):
        self.form = form
        self.rule = rule
        self.history = None
        self.comment_to_count = None
        if comment_to_count is not None:
            self.comment_to_count = comment_to_count
            self.update_count = make_title_updater(comment_to_count)
        if update_function is not None:
            self.update_count = update_function
        if length is not None or (comment_to_count is None and update_function is None):
            self.length = length if length is not None else 1000
            self.update_count = self.update_from_length

    def update_from_length(self, old_count, chain):
        if self.length is not None:
            return old_count + self.length * (len(chain))
        return None

    def is_valid_thread(self, history):
        mask = self.rule.is_valid(history)
        if mask.all():
            return (True, "")
        return (False, history.loc[~mask, "comment_id"].iloc[0])

    def is_valid_count(self, comment, history):
        history = pd.concat([history, pd.DataFrame([comment_to_dict(comment)])], ignore_index=True)
        valid_history = self.is_valid_thread(history)[0]
        valid_count = self.looks_like_count(comment)
        valid_user = not counters.is_ignored_counter(str(comment.author))
        return valid_history and valid_count and valid_user, history

    def get_history(self, comment):
        """Fetch enough previous comments to be able to determine whether replies to
        `comment` are valid according to the side thread rules.
        """
        return self.rule.get_history(comment)

    def looks_like_count(self, comment):
        return comment.body in utils.deleted_phrases or self.form(comment.body)

    def set_comment_to_count(self, f):
        self.comment_to_count = f

    def wrapped_comment_to_count(self, comment):
        comment_to_count = (
            self.comment_to_count if self.comment_to_count is not None else base_n_count(10)
        )
        try:
            return comment_to_count(comment)
        except ValueError:
            return np.nan

    def find_errors(self, history, offset=0):
        """Find points in the history of a side thread where an incorrect count was posted.

        Parameters:
          - history: Either a string representing the comment id of
            the leaf comment in the thread to be investigated, or a pandas
            dataframe with (at least) a "body" column that contains the markdown
            string of each comment in the thread.
          - offset: How much the comments in the chain are shifted with respect
            to some platonic "true chain". Used in case some broken chains mean
            that there isn't a linear thread from start to finish.

        Returns:
          - The comments in the history where an uncorrected error was introduced

        In order to do this, we need to use the `comment_to_count` member of
        the side thread to go from the string representation of a comment to
        the corresponding count. This is potentially different for each side
        thread.

        Errors are defined narrowly to avoid having too many false positives. A
        comment is considered to introduce an error if:

          - Its count is not one more than the previous count AND
          - Its count is not two more than the last but one count AND
          - Its count doesn't match where the count should be according to the
            position in the thread.

        The last criterion means that counts which correct previous errrors won't be included.

        Additionally, to avoid rehashing errors which have already been
        corrected, only comments after the last correct count in the thread
        will be considered.

        """
        if isinstance(history, str):
            self.history = pd.DataFrame(tn.fetch_comments(history))
            history = self.history

        counts = history["body"].apply(self.wrapped_comment_to_count)
        # Errors are points where the count doesn't match the index difference
        errors = counts - counts.iloc[0] - offset != counts.index
        # But only errors after the last correct value are interesting
        errors[: errors.where((~errors)).last_valid_index()] = False
        mask = errors & (counts.diff() != 1) & (counts.diff(2) != 2)
        return history[mask]
