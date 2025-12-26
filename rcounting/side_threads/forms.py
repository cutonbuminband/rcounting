import functools
import logging
from typing import Callable

import numpy as np
import pandas as pd

from rcounting import parsing, utils
from rcounting import thread_navigation as tn

from .validate_form import permissive

printer = logging.getLogger(__name__)


def make_title_updater(comment_to_count):
    @functools.wraps(comment_to_count)
    def wrapper(_, chain):
        title = chain[-1].title
        return comment_to_count(parsing.body_from_title(title))

    return wrapper


class CommentType:
    def __init__(
        self,
        form: Callable[[str], bool] = permissive,
        length: int | None = None,
        comment_to_count: Callable[[str], int] | None = None,
        update_function=None,
    ):
        self.form = form
        self.length = length
        self.comment_to_count = None
        if comment_to_count is not None:
            self.comment_to_count = comment_to_count
            self.update_count = make_title_updater(comment_to_count)
        elif update_function is not None:
            self.update_count = update_function
        else:
            self.length = length if length is not None else 1000
            self.update_count = self.update_from_length

    def update_from_length(self, old_count, chain):
        if self.length is not None:
            return old_count + self.length * (len(chain) - 1)
        return None

    def looks_like_count(self, comment_body):
        return comment_body in utils.deleted_phrases or self.form(comment_body)

    def wrapped_comment_to_count(self, comment):
        comment_to_count = (
            self.comment_to_count
            if self.comment_to_count is not None
            else parsing.find_count_in_text
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

    def count_to_comment(self, count: int) -> str:
        """Use a binary search to determine which comment string would
        correspond to a given count for this particular side thread.

        To do that, we need a comment_to_count function which can evaluate
        arbitrary comment strings and tell us what count they would correspond
        to.

        The approach used here only works if

        - The valid comment strings look like decimal numbers
        - The comment_to_count function of this side thread doesn't raise an error
          if a count is invalid, but rather returns the count corresponding to
          the previous valid count. That's true of the dfa counts, but not of much else. Oh well.

        """
        assert (
            self.comment_to_count is not None
        ), "No comment to count function found. Unable to perform the inverse operation"
        target = count
        current = count
        while self.comment_to_count(str(current)) >= target:
            current = current // 2
        low = current
        while self.comment_to_count(str(current)) < target:
            low = current
            current *= 2
        high = current
        while self.comment_to_count(str(high)) != target:
            mid = (high + low) // 2
            mid_value = self.comment_to_count(str(mid))
            if mid_value < target:
                low = mid
            else:
                high = mid
        return str(high)

    def find_correct_count(self, history):
        assert (
            self.comment_to_count is not None
        ), "No comment to count function found. Unable to find the correct count"
        target_count = self.comment_to_count(history.loc[0, "body"]) + len(history) - 1
        return self.count_to_comment(target_count)


default_type = CommentType()
