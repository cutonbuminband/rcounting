import pandas as pd

from rcounting.models import comment_to_dict
from rcounting.units import HOUR, MINUTE


class CountingRule:
    """
    A rules class. It knows how to do two things:

    - Get enough history to see whether a given comment is valid
    - Determine whether all counts in a history of counts are valid.

    Examples of things it's intended to validate are:
      - That users waited enough time since their own last comment before commenting again
      - That users waited enough time since the global last comment
      - That users let enough other counters go before them
    """

    def __init__(self, wait_n: int | None = 1, thread_time=0, user_time=0):
        self.n = wait_n
        self.thread_time = thread_time
        self.user_time = user_time

    def _valid_skip(self, history):
        n = self.n if self.n is not None else len(history)
        history = history.reset_index()
        skips = history.groupby("username")["index"].diff()
        return skips.isna() | (skips > n)

    def _valid_thread_time(self, history):
        if not self.thread_time:
            return True
        elapsed_time = history["timestamp"].diff()
        valid_time = elapsed_time.isna() | (elapsed_time >= self.thread_time)
        return valid_time

    def _valid_user_time(self, history):
        if not self.user_time:
            return True
        elapsed_user_time = history.groupby("username")["timestamp"].diff()
        valid_user_time = elapsed_user_time.isna() | (elapsed_user_time >= self.user_time)
        return valid_user_time

    def is_valid(self, history):
        return (
            self._valid_skip(history)
            & self._valid_thread_time(history)
            & self._valid_user_time(history)
        )

    def get_history(self, comment):
        limit = self.n + 1 if self.n is not None else self.n
        comments = comment.walk_up_tree(limit=limit)
        max_time = max(self.thread_time, self.user_time)
        while (
            not comments[-1].is_root
            and (comment.created_utc - comments[-1].created_utc) < max_time
        ):
            comments = comments[:-1] + comments[-1].walk_up_tree(limit=9)
        return pd.DataFrame([comment_to_dict(x) for x in comments[:0:-1]])


class FastOrSlow(CountingRule):
    """
    An special case of the rules class to account for a thread where the rule
    is not of the form 'wait at least X' before counting, but rather
    'wait at most X or at least Y'.

    """

    def __init__(self):
        super().__init__()

    def _valid_thread_time(self, history):
        elapsed_time = history["timestamp"].diff()
        valid_time = elapsed_time.isna() | (elapsed_time < 5 * MINUTE) | (elapsed_time >= HOUR)
        return valid_time


class OnlyDoubleCounting(CountingRule):
    """
    Only double counting is sufficiently strange that it gets its own class.

    A thread is valid if every user in the chain counts exactly twice in a row.
    """

    def is_valid(self, history):
        history = history.set_index("comment_id")
        history["mask"] = True
        unshifted = history.username.iloc[::2]
        up_shift = history.username.shift(-1).iloc[::2]
        up_mask = up_shift.isna() | (up_shift == unshifted)
        down_shift = history.username.shift().iloc[::2]
        down_mask = down_shift.isna() | (down_shift == unshifted)
        mask = up_mask if (up_mask.sum() > down_mask.sum()) else down_mask
        history.loc[mask.index, "mask"] = mask
        history.reset_index(inplace=True)
        return history["mask"]

    def get_history(self, comment):
        comments = comment.walk_up_tree(limit=2)[:0:-1]
        return pd.DataFrame([comment_to_dict(x) for x in comments])


default_rule = CountingRule()
