import math
from collections import Counter, defaultdict
from typing import Sequence

import numpy as np
import scipy.sparse

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

    def __init__(self, n_symbols: int, n_states: int, sparse=True):
        self.n_states = n_states
        self.n_symbols = n_symbols
        self.size = self.n_states**self.n_symbols
        self.sparse = sparse
        self.lookup = {str(i): str(min(i + 1, n_states - 1)) for i in range(n_states)}
        self.transitions = None
        self.transition_matrix = None

    def __getitem__(self, i):
        if self.transitions is None:
            self.transitions = [self.generate_identity()]
            self.transition_matrix = self.generate_transition_matrix()
        while len(self.transitions) <= i:
            self.transitions.append(self.transitions[-1] @ self.transition_matrix)

        return self.transitions[i]

    def generate_identity(self):
        if self.sparse:
            return scipy.sparse.eye(self.size, dtype=int, format="csr")
        return np.eye(self.size, dtype=int)

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

    def generate_transition_matrix(self):
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


class CompressedDFA(DFA):
    """A more compressed DFA class, where each string is represented by the
    3-tuple of (how many digits don't occur in the string, how many digits
    occur once, how many occur two or more times). This representation
    decreases the number of states to keep track of from ~60k to ~60 for
    10-symbol words"""

    def __init__(self, n_symbols):
        super().__init__(n_symbols=n_symbols, n_states=3, sparse=False)
        self.size = math.comb(n_symbols + 2, n_symbols)
        self.total_lengths = [
            len(range(max(i - n_symbols, 0), i // 2 + 1)) for i in range(2 * n_symbols + 1)
        ]

    def _find_next_state(self, state: Sequence[int]):
        result = []
        if state[0]:
            result.append((state[0], [state[0] - 1, state[1] + 1, state[2]]))
        if state[1]:
            result.append((state[1], [state[0], state[1] - 1, state[2] + 1]))
        if state[2]:
            result.append((state[2], state))
        return result

    def generate_transition_matrix(self):
        transition_matrix = np.zeros((self.size, self.size), dtype=int)
        states = [
            (x, y, self.n_symbols - x - y)
            for x in range(self.n_symbols + 1)
            for y in range(self.n_symbols + 1 - x)
        ]
        for state in states:
            for count, new_state in self._find_next_state(state):
                transition_matrix[self.encode(state), self.encode(new_state)] = count
        return transition_matrix

    def encode(self, state: str | Sequence[int]):
        if isinstance(state, str):
            counts = Counter([min(x, 2) for x in Counter(state).values()])
        else:
            counts = state
        digit_sum = counts[1] + 2 * counts[2]
        predecessors = sum(self.total_lengths[:digit_sum])
        return predecessors + counts[1] // 2


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

    def __init__(
        self,
        dfa_base=3,
        n=10,
        rule=default_rule,
        dfa: DFA | None = None,
        indices: Sequence[int] | None = None,
        offset=0,
    ):
        self.n = n
        form = base_n(n)
        self.dfa = dfa if dfa is not None else DFA(n, dfa_base)
        self.indices = indices if indices is not None else []

        # Some of the threads skip the single-digit counts which would
        # otherwhise be valid, so we add an offset to account for that
        self.offset = offset

        super().__init__(rule=rule, form=form, comment_to_count=self.count)

    def word_is_valid(self, word):
        return self.dfa.encode(word) in self.indices

    def count(self, comment_body: str, bijective=False) -> int:
        word = parsing.extract_count_string(comment_body, self.n).lower()
        word_length = len(word)

        enumeration = 0
        for i in range(word_length - 1, -1, -1):
            matrix_power = word_length - 1 - i
            prefix = word[:i]
            current_char = word[i]
            suffixes = alphanumeric[i == 0 : alphanumeric.index(current_char)]
            states = [self.dfa.encode(prefix + suffix) for suffix in suffixes]
            if bijective:
                states += [self.dfa.encode("")]
            elif matrix_power > 0:
                enumeration += sum(
                    self.dfa[matrix_power - 1][self.dfa.encode(s), self.indices].sum()
                    for s in alphanumeric[1 : self.n]
                )

            enumeration += sum(
                self.dfa[matrix_power][state, self.indices].sum() for state in states
            )
        return enumeration + self.word_is_valid(word) - self.offset


dfa_10_2 = DFA(10, 2)
compressed_dfa = CompressedDFA(10)

only_repeating_indices = [compressed_dfa.encode((x, 0, 10 - x)) for x in range(10)]
only_repeating_digits = DFASideThread(dfa=compressed_dfa, indices=only_repeating_indices)

mostly_repeating_indices = [compressed_dfa.encode((x, 1, 10 - x - 1)) for x in range(10 - 1)]
mostly_repeating_digits = DFASideThread(dfa=compressed_dfa, indices=mostly_repeating_indices)

# The only consecutive indices are sums of adjacent powers of two
only_consecutive_indices = sorted(
    [
        sum(2**k for k in range(minval, maxval))
        for maxval in range(10 + 1)
        for minval in range(maxval)
    ]
)
only_consecutive_digits = DFASideThread(dfa=dfa_10_2, indices=only_consecutive_indices, offset=9)


def no_consecutive_states(n_symbols):
    """The valid states in no consecutive digits threads, encoded as binary
    strings where the i'th digit of the string indicates whether the
    corresponding digit is present in the original string or not.

    For example, 0000000000 is the empty string, while 1000000000 represents
    strings which only contain the digit 9

    """
    current = ["0", "01"]
    result = []
    while current:
        val = current.pop()
        if len(val) < n_symbols:
            current.append("0" + val)
            current.append("01" + val)
        elif len(val) == n_symbols:
            result.append(val)
        else:
            result.append(val[1:])
    return result


no_consecutive_indices = sorted([int(x, 2) for x in no_consecutive_states(10)])[1:]
no_consecutive_digits = DFASideThread(dfa=dfa_10_2, indices=no_consecutive_indices)
