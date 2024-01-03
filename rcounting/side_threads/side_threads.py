import collections
import functools
import logging
import math

import numpy as np
import pandas as pd
import scipy.sparse

from rcounting import counters, parsing
from rcounting import thread_navigation as tn
from rcounting import utils
from rcounting.models import comment_to_dict

from .rules import default_rule
from .validate_count import base_n_count
from .validate_form import alphanumeric, base_n, permissive

printer = logging.getLogger(__name__)


def ignore_revivals(chain, was_revival):
    return chain if was_revival is None else [x for x, y in zip(chain, was_revival) if not y]


def make_title_updater(comment_to_count):
    @functools.wraps(comment_to_count)
    def wrapper(old_count, chain, was_revival=None):
        chain = ignore_revivals(chain, was_revival)
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
        form=permissive,
        length=None,
        comment_to_count=None,
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

    def update_from_length(self, old_count, chain, was_revival=None):
        chain = ignore_revivals(chain, was_revival)[1:]
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

    def find_errors(self, history):
        """Find points in the history of a side thread where an incorrect count was posted.

        Parameters:
          - history: Either a string representing the comment id of
            the leaf comment in the thread to be investigated, or a pandas
            dataframe with (at least) a "body" column that contains the markdown
            string of each comment in the thread.

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
        errors = counts - counts.iloc[0] != counts.index
        # But only errors after the last correct value are interesting
        errors[: errors.where((~errors)).last_valid_index()] = False
        mask = errors & (counts.diff() != 1) & (counts.diff(2) != 2)
        return history[mask]


class OnlyRepeatingDigits(SideThread):
    """
    A class that describes the only repeating digits side thread.

    The rule and form attributes of the side thread are the same as for base n;
    no validation that each digit actually occurs twice is currently done.

    The main aspect of this class is the update function, which is mathematically
    quite heavy. To count the number of possible only repeating digits strings,
    we build a transition matrix where the states for each digits are
      - Seen 0 times
      - Seen 1 time
      - Seen 2 or more times
    The success states are 0 and 2.

    For strings of length n, we count the success states by looking at the
    nth power of the transition matrix.
    """

    def __init__(self, n=10, rule=default_rule):
        form = base_n(n)
        self.n = n
        self.lookup = {"0": "1", "1": "2", "2": "2"}
        self.indices = self._indices(self.n)
        self.transition_matrix = self.make_dfa()
        super().__init__(rule=rule, form=form, comment_to_count=self.count)

    def connections(self, i):
        base3 = np.base_repr(i, 3).zfill(self.n)
        js = [int(base3[:ix] + self.lookup[x] + base3[ix + 1 :], 3) for ix, x in enumerate(base3)]
        result = collections.defaultdict(int)
        for j in js:
            if j == 1:
                continue
            result[j] += 1
        return (
            len(result),
            np.array(list(result.keys()), dtype=int),
            np.array(list(result.values()), dtype=int),
        )

    def make_dfa(self):
        data = np.zeros(self.n * 3**self.n, dtype=int)
        x = np.zeros(self.n * 3**self.n, dtype=int)
        y = np.zeros(self.n * 3**self.n, dtype=int)
        ix = 0
        for i in range(3**self.n):
            length, js, new_data = self.connections(i)
            x[ix : ix + length] = i
            y[ix : ix + length] = js
            data[ix : ix + length] = new_data
            ix += length
        return scipy.sparse.coo_matrix(
            (data[:ix], (x[:ix], y[:ix])), shape=(3**self.n, 3**self.n)
        )

    def _indices(self, n):
        if n == 0:
            return [0]
        partial = [3 * x for x in self._indices(n - 1)]
        return sorted(partial + [x + 2 for x in partial])

    def count_only_repeating_words(self, k):
        """The number of words of length k where no digit is present exactly once"""
        # The idea is to use the inclusion-exclusion principle, starting with
        # all n ^ k possible words. We then subtract all words where a given
        # symbol occurrs only once. For each symbol there are k * (n-1) ^ (k-1)
        # such words since there are k slots for the symbol of interest, and
        # the remaining slots must be filled with one of the remaining symbols.
        # There are thus n * k * (n-1)^ *(k-1) words where one symbol occurs
        # only once. But this double counts all the cases where two symbols
        # occur only once, so we have to add them back in. In general, there
        # are (n-i)^(n-i) * C(n,i) * P(k,i) words where i symbols occur only
        # once, giving the expression:

        # The correction factor (n-1)/n accounts for the words which would
        # start with a 0

        return (
            sum(
                (-1) ** i * (self.n - i) ** (k - i) * math.comb(self.n, i) * math.perm(k, i)
                for i in range(0, min(self.n, k) + 1)
            )
            * (self.n - 1)
            // self.n
        )

    def get_state(self, prefix):
        result = ["0"] * self.n
        for char in prefix:
            index = (self.n - 1) - int(char, self.n)
            result[index] = self.lookup[result[index]]
        return int("".join(result), 3)

    def count(self, comment):
        count = parsing.find_count_in_text(comment)
        word = str(count)
        word_length = len(word)
        if word_length < 2:
            return 0
        result = sum(self.count_only_repeating_words(i) for i in range(1, word_length))
        result += (
            (int(word[0], self.n) - 1)
            * self.count_only_repeating_words(word_length)
            // (self.n - 1)
        )
        current_matrix = scipy.sparse.identity(3**self.n, dtype="int", format="csr")
        for i in range(word_length - 1, 0, -1):
            prefix = word[:i]
            current_char = word[i].lower()
            suffixes = alphanumeric[: alphanumeric.index(current_char)]
            states = [self.get_state(prefix + suffix) for suffix in suffixes]
            result += sum(current_matrix[state, self.indices].sum() for state in states)
            current_matrix *= self.transition_matrix
        return result
