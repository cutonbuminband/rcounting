import functools
import itertools
import math
from collections import Counter, defaultdict

import numpy as np
import scipy.sparse
from scipy.special import binom

from rcounting import parsing

from .rules import default_rule
from .side_threads import SideThread
from .validate_form import alphanumeric, base_n


class DFA:
    """Generate and store transition matrices for discrete finite automate
    which track what happens when a word from an alphabet of size n_symbols is
    extended by one symbol. Calculating these can be computationally expensive,
    so the code caches them for later use.

    """

    def __init__(self, n_symbols: int, n_states: int):
        self.n_states = n_states
        self.n_symbols = n_symbols
        self.size = self.n_states**self.n_symbols
        self.lookup = {str(i): str(min(i + 1, n_states - 1)) for i in range(n_states)}
        self.transitions = None
        self.transition_matrix = None

    def __getitem__(self, i):
        if self.transitions is None:
            self.transitions = [self._generate_identity()]
            self.transition_matrix = self._generate_transition_matrix()
        while len(self.transitions) <= i:
            self.transitions.append(self.transitions[-1] @ self.transition_matrix)

        return self.transitions[i]

    def generate_identity(self):
        return scipy.sparse.eye(self.size, dtype=int, format="csr")

    def _connections(self, i):
        state = np.base_repr(i, self.n_states).zfill(self.n_symbols)
        js = [
            int(state[:ix] + self.lookup[x] + state[ix + 1 :], self.n_states)
            for ix, x in enumerate(state)
        ]
        result = defaultdict(int)
        for j in js:
            if j == 1:
                continue
            result[j] += 1
        return (
            len(result),
            np.array(list(result.keys()), dtype=int),
            np.array(list(result.values()), dtype=int),
        )

    def _generate_transition_matrix(self):
        data = np.zeros(self.n_symbols * self.size, dtype=int)
        x = np.zeros(self.n_symbols * self.size, dtype=int)
        y = np.zeros(self.n_symbols * self.size, dtype=int)
        ix = 0
        for i in range(self.size):
            length, js, new_data = self._connections(i)
            x[ix : ix + length] = i
            y[ix : ix + length] = js
            data[ix : ix + length] = new_data
            ix += length
        return scipy.sparse.coo_matrix(
            (data[:ix], (x[:ix], y[:ix])),
            shape=(self.size, self.size),
        )

    def encode(self, state):
        """Converts a word to an integer encoding of the corresponding state vector"""
        counts = Counter(state)
        return sum(
            (self.n_states**pos) * min(self.n_states - 1, counts[digit])
            for pos, digit in enumerate(alphanumeric[: self.n_symbols])
        )


class DFASideThread(SideThread):
    """Describing side threads using a deterministic finite automaton.

    A lot of side threads have rules like "valid counts are those were every
    digit repeats at least once" or "valid counts are made up of a set of
    consecutive digits". Determining how many valid counts there are smaller
    than or equal to a given number is tricky to do, but that's exactly what we
    need in order to convert a comment to a count.

    These threads have the property that the validity of a count only depends
    on which digits are present in a comment, and not on the order in which
    they appear. That means that we can describe the state vector of a given
    comment by the tuple of digit counts.

    We can then describe what happens to the state when a given digit is
    appended to the count -- the corresponding entry in the state tuple is
    increased by one, or, if the entry is already at some maximal value, the
    entry is just kept constant. For example, for only repeating digits the
    three states we are interested in are:

    - digit occurs 0 times
    - digit occurs once
    - digit occurs 2 or more times

    and after appending a digit, the possible new states for that digit are

    - digit occurs once (if it was not present before)
    - digit occurs 2 or more times (if it was present before)

    Once we have a description of the possible new states for any given state
    after appending an arbitrary digit, we are basically done: we can start
    with a given input states, apply the transition a certain number of times,
    and see how many of the states we end up with follow whatever rules we've set up.

    See
    https://cstheory.stackexchange.com/questions/8200/counting-words-accepted-by-a-regular-grammar/8202#8202
    for more of a description of how it works.

    The rule and form attributes of the side threads are the same as for base
    n; no validation that each digit actually occurs the correct number of
    times is currently done.

    """

    def __init__(self, dfa_base=3, n=10, rule=default_rule, dfa: DFA | None = None):
        self.n = n
        form = base_n(n)
        if dfa is not None:
            self.dfa = dfa
        else:
            self.dfa = DFA(n, dfa_base)
        self.indices = None
        # Some of the threads skip the single-digit counts which would
        # otherwhise be valid, so we add an offset to account for that
        self.offset = 0

        # If the digit distribution is homogeneous, the number of words
        # starting with a given digit is just 1/number of starting digits. That
        # lets us skip one of the transitions in the matrix, saving a bit of time.
        self.is_homogeneous = True
        super().__init__(rule=rule, form=form, comment_to_count=self.count)

    def _setup(self):
        self.indices = self._generate_indices()

    def word_is_valid(self, word):
        if self.indices is None:
            self._setup()
        return self.dfa.encode(word) in self.indices

    def _generate_indices(self):
        raise NotImplementedError(
            "Attempting to call `generate_indices` on the base class. "
            + "Give an implementation for it in the subclass!"
        )

    def complete_words(self, k: int) -> [int, bool]:
        return 0, False

    def count(self, comment_body: str) -> int:
        if self.indices is None:
            self._setup()
        word = parsing.extract_count_string(comment_body, self.n).lower()
        word_length = len(word)

        complete_words = [self.complete_words(i) for i in range(1, word_length)]
        shorter_words, precalculated = functools.reduce(
            lambda x, y: (x[0] + y[0], x[1] and y[1]), complete_words
        )
        if self.is_homogeneous and precalculated:
            # We can get the word with smaller first digit as a simple fraction
            # of the total number of words of length `word_length`
            enumeration = (
                (int(word[0], self.n) - 1) * self.complete_words(word_length)[0] // (self.n - 1)
            )
            lower_limit = 0
        else:
            enumeration = 0
            lower_limit = -1
        for i in range(word_length - 1, lower_limit, -1):
            current_matrix = self.dfa[word_length - 1 - i]
            prefix = word[:i]
            current_char = word[i]
            suffixes = alphanumeric[i == 0 : alphanumeric.index(current_char)]
            states = [self.dfa.encode(prefix + suffix) for suffix in suffixes]
            if not precalculated:
                states = [0] + states
            enumeration += sum(current_matrix[state, self.indices].sum() for state in states)
        return shorter_words + enumeration + self.word_is_valid(word) - self.offset


dfa_10_2 = DFA(10, 2)
dfa_10_3 = DFA(10, 3)


def count_only_repeating_words(n, k, bijective=False):
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

    total = sum(
        (-1) ** i * (n - i) ** (k - i) * math.comb(n, i) * math.perm(k, i)
        for i in range(0, min(n, k) + 1)
    )

    # The correction factor (n-1)/n accounts for the words which would
    # start with a 0
    return total if bijective else total * (n - 1) // n


class OnlyRepeatingDigits(DFASideThread):
    """A class that describes the only repeating digits side thread.

    See the base class, `DFASideThread` for a description of the approach"""

    def __init__(self, n=10, rule=default_rule):
        super().__init__(n=n, rule=rule, dfa=dfa_10_3)

    def _generate_indices(self):
        """Valid states are those which have no ones in their ternary
        represenation and at least one 2"""
        return [int("".join(x), 3) for x in itertools.product("02", repeat=self.n)][1:]

    def complete_words(self, k):
        return count_only_repeating_words(self.n, k), True


class MostlyRepeatingDigits(DFASideThread):
    """A class that describes the mostly repeating digits side thread.

    See the base class, `DFASideThread` for a description of the approach"""

    def __init__(self, n=10, rule=default_rule):
        super().__init__(n=n, rule=rule, dfa=dfa_10_3)

    def add_one(self, state):
        candidates = []
        for ix, char in enumerate(state):
            if char == "0":
                candidates.append(state[:ix] + "1" + state[ix + 1 :])

        return [int(candidate, 3) for candidate in candidates]

    def _generate_indices(self):
        """Valid states are those which have precisely one 1 in their ternary
        representation and at least one 2"""
        indices = ["".join(x) for x in itertools.product("02", repeat=self.n)][1:]
        return sorted(
            [updated_index for state in indices for updated_index in self.add_one(state)]
        )

    def complete_words(self, k):
        # For a given length k, the total number of MRD words in base n is
        # found as:
        #
        # k * n * ORD(n-1, k-1)
        #
        # That's because we have n symbols which could occur once, and then
        # (n-1) symbols which should be a valid ORD word of length (k - 1). The
        # final factor of k is due to the fact that the lone symbol could
        # appear anywhere in the final word.
        # For actual MRD counts, we're not allowed to start words with 0, which
        # means we should multiply by (n-1) / n, giving the final result
        if k < 3:
            return 0
        return (self.n - 1) * k * count_only_repeating_words(
            self.n - 1, k - 1, bijective=True
        ), True


@functools.cache
def full_base_words(n, k):
    """The number of words of length k from an alphabet of n symbols such that
    every symbol in the alphabet is used at least once. The recursion works by
    saying we take all the n**k possible words and subtract those which are
    made from all the subsets of the alphabet.

    """
    if k == 1:
        return 1
    return n**k - sum(int(binom(n, i)) * full_base_words(i, k) for i in range(n))


class OnlyConsecutiveDigits(DFASideThread):
    """A class that counts only consecutive numbers. See the base class
    `DFASideThread for a description of the approach`"""

    def __init__(self, n=10, rule=default_rule):
        super().__init__(n=n, rule=rule, dfa=dfa_10_2)
        self.offset = 9
        self.is_homogeneous = False

    def pad(self, s):
        return ["0" * i + s + "0" * (self.n - i - len(s)) for i in range(self.n - len(s) + 1)]

    def _generate_indices(self):
        ones = ["1" * i for i in range(1, self.n + 1)]
        return sorted([int(p, 2) for one in ones for p in self.pad(one)])

    def complete_words(self, k, bijective=False):
        """The total number of only consective words of length k. This is the
        sum over all consecutve alphabets of all words of length k which use
        each symbol in that alphabet at least once.

        Each consecutive alphabet of length n' is uniquely determined by the
        first letter in the alphabet, and it's not too difficult to see that
        there are (n - n' + 1) alphabets of length n'. Therefore, we can just
        loop over the length instead of looping over the alphabet.

        We need to account for words starting with zero, and we can't just do
        it by multiplying by (n-1)/n. That's because there's only one alphabet
        of a given length which contains zero, but there are multiple which
        contain the other digits.

        """
        total = sum((self.n - i + 1) * full_base_words(i, k) for i in range(1, min(k, self.n) + 1))
        if bijective:
            return total
        words_starting_with_zero = sum(
            full_base_words(i, k) // i for i in range(1, min(k, self.n) + 1)
        )
        return total - words_starting_with_zero, True


class NoConsecutiveDigits(DFASideThread):
    """A class that counts no consecutive numbers. See the base class
    `DFASideThread for a description of the approach`"""

    def __init__(self, n=10, rule=default_rule):
        super().__init__(n=n, rule=rule, dfa=dfa_10_2)

    def no_consecutive_indices(self):
        current = ["0", "01"]
        result = []
        while current:
            val = current.pop()
            if len(val) < self.n:
                current.append("0" + val)
                current.append("01" + val)
            elif len(val) == self.n:
                result.append(val)
            else:
                result.append(val[1:])
        return result

    def _generate_indices(self):
        return sorted([int(x, 2) for x in self.no_consecutive_indices() if x])
