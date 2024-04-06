import itertools
import math
from collections import Counter, defaultdict
from typing import Sequence, Tuple

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
            return scipy.sparse.eye(self.size, dtype=int, format="csc")
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
        ).tocsc()

    def encode(self, state: str) -> int:
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
        super().__init__(n_symbols=n_symbols, n_states=3)
        self.size = math.comb(n_symbols + 2, n_symbols)
        self.total_lengths = [
            len(range(max(i - n_symbols, 0), i // 2 + 1)) for i in range(2 * n_symbols + 1)
        ]

    def _find_next_states(self, state: Sequence[int]):
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
            for count, new_state in self._find_next_states(state):
                transition_matrix[self.encode(state), self.encode(new_state)] = count
        return transition_matrix

    def encode(self, state: str | Sequence[int]) -> int:
        if isinstance(state, str):
            counts = Counter([min(x, 2) for x in Counter(state).values()])
        else:
            counts = state
        digit_sum = counts[1] + 2 * counts[2]
        predecessors = sum(self.total_lengths[:digit_sum])
        return predecessors + counts[1] // 2


class LastDigitDFA(DFA):
    """A DFA that keeps track of the last digit of a string, and moves to a
    failure state if the next digit is the same as the current state. Used to
    model the no successive digits side thread

    """

    def __init__(self, n_symbols=10):
        super().__init__(n_symbols=n_symbols, n_states=3, sparse=False)
        self.size = 3

    def generate_transition_matrix(self):
        return np.array([[0, self.n_symbols, 0], [0, self.n_symbols - 1, 0], [0, 0, 0]])

    def encode(self, state: str):
        if not state:
            return 0
        previous = ""
        for char in state:
            if char == previous:
                return 2
            previous = char
        return 1


class NotAnyOfThoseDFA(DFA):
    """A class to represent the states and transitions of the not any of those
    side thread, which has the rule that the cannot match any of:

    - No Repeating Digits
    - Only Repeating Digits
    - Mostly Repeating Digits
    - No Successive Digits
    - No Consecutive Digits
    - Only Consecutive Digits

    We'll use the following representation for a state:

    [a_0, a_1, a_2, \\ldots, a_n], b, c

    where a is a boolean vector, with a_i representing whether the digit i is
    present in the state, b is a count of how many digits are already present twice
    in the state, and c is a ternary digit representing whether:

    0. The the last digit is only present once in the string
    1. The last digit of the string is present at least twice in the string
    2. The state has already successfully failed the NSD condition

    Instead of packing the states nicely in a sensible scheme, we'll just loop
    over all possible values of a, b, c in some arbitrary order, discard the
    values which correspond to invalid states, and use that order to assign an
    integer to each state.

    The invalid states are:

    b = 0, c != 0
    b = sum(a), c == 0

    and after accounting for those, it can be shown that there are 3n*2^(n-1)
    valid states

    """

    def __init__(self, n=10):
        super().__init__(n_symbols=n, n_states=3)
        self.itos_map = self.initialize_itos_map()
        self.stoi_map = {v: k for k, v in self.itos_map.items()}
        self.size = len(self.itos_map)

    def initialize_itos_map(self):
        itos_map = {}
        idx = 0
        for mask in itertools.product([False, True], repeat=self.n_symbols):
            for b in range(sum(mask) + 1):
                for c in range(3):
                    if b == 0 and c > 0:
                        continue
                    if b == sum(mask) and c == 0:
                        continue
                    itos_map[idx] = mask, b, c
                    idx += 1
        return itos_map

    def _find_next_states(self, mask: Tuple[bool, ...], b: int, c: int):
        total = sum(mask)
        result = []
        # Every digit not present in the state just results in one more bit
        # set, the same number of twos in the string, and a last digit of zero
        # if we're not already in a success state for nsd', and two if we are
        for idx, bit in enumerate(mask):
            if not bit:
                current = tuple(mask[:idx]) + (True,) + tuple(mask[idx + 1 :])
                result += [(1, (current, b, 0 if c != 2 else 2))]
        if total > 0:
            # If we aren't already in a success state for \overline{NSD}, then
            # one of the digits we add will move us there
            if c != 2:
                result += [(1, (mask, b + (c == 0), 2))]
            # The remaining digits will just keep the same mask, make b
            # either stay the same or increase by one, and make c = 1 if we are
            # not in a success state for nsd'
            result += [(b - (c == 1), (mask, b, 1 if c != 2 else 2))]
            result += [(total - b - (c == 0), (mask, b + 1, 1 if c != 2 else 2))]
        return [x for x in result if x[0] > 0]

    def int_to_mask(self, state: int) -> Tuple[bool, ...]:
        return tuple(char == "1" for char in f"{state:0>10b}"[::-1])

    def generate_transition_matrix(self):
        i = np.empty(self.n_symbols * self.size, dtype=int)
        j = np.empty(self.n_symbols * self.size, dtype=int)
        data = np.empty(self.n_symbols * self.size, dtype=int)
        idx = 0
        for state in range(self.size):
            next_states = self._find_next_states(*self.itos_map[state])
            counts, new_states = zip(*next_states)
            new_states = [self.stoi_map[ns] for ns in new_states]
            total = len(counts)
            i[idx : idx + total] = state
            j[idx : idx + total] = new_states
            data[idx : idx + total] = counts
            idx += total
        return scipy.sparse.coo_array(
            (data[:idx], (i[:idx], j[:idx])),
            shape=(self.size, self.size),
        ).tocsc()

    def encode(self, state: str) -> int:
        symbols = alphanumeric[: self.n_symbols]
        bitmask = [symbol in state for symbol in symbols]
        current = ""
        c = 0
        for char in state:
            if char == current:
                c = 2
                break
            current = char
        counts = Counter(state)
        if c != 2 and state:
            c = min(counts[state[-1]], 2) - 1
        b = Counter([min(x, 2) for x in counts.values()])[2]
        return self.stoi_map[tuple(bitmask), b, c]


class BarelyRepeatingDigitsDFA(DFA):
    """Class for tracking state transitions for barely repeating digits. We
    have three types of state:

    - One failure state
    - A state representing the empty string
    - States of the form (digits present, repeat), where digits present counts
      how many digits from the alphabet of n symbols are present in the string,
      and repeat is a bool representing whether any of these digits repeats in
      the string.

    """

    def __init__(self, n_symbols=10):
        super().__init__(n_symbols=n_symbols, n_states=3, sparse=False)
        self.size = 2 * (n_symbols + 1)

    def encode(self, state: str | Sequence[int]):
        if isinstance(state, str):
            counts = Counter([min(x, 3) for x in Counter(state).values()])
            digits_present = counts[1] + counts[2]
            repeat = counts[2] if not counts[3] else -1
        else:
            digits_present, repeat = state

        if digits_present > self.n_symbols or digits_present < 0 or repeat not in [0, 1]:
            return 0
        if digits_present == 0:
            return 1 - repeat
        return 2 * digits_present + repeat

    def _find_next_states(self, state: Tuple[int, int]):
        digits_present, repeat = state
        result = []
        if repeat == 0:
            result.append((digits_present, (digits_present, 1)))
        if digits_present != self.n_symbols:
            result.append((self.n_symbols - digits_present, (digits_present + 1, repeat)))
        return result

    def generate_transition_matrix(self):
        transition_matrix = np.zeros((self.size, self.size), dtype=int)
        states = [
            (digits_present, repeat)
            for digits_present in range(self.n_symbols + 1)
            for repeat in range(2)
        ]
        for state in states:
            for count, new_state in self._find_next_states(state):
                transition_matrix[self.encode(state), self.encode(new_state)] = count
        transition_matrix[0] = 0
        transition_matrix[:, 0] = 0
        return transition_matrix


class DFAThread(SideThread):
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
        self.encoded_symbols = [self.dfa.encode(s) for s in alphanumeric[:n]]
        self.matrices = {}

        # Some of the threads skip the single-digit counts which would
        # otherwhise be valid, so we add an offset to account for that
        self.offset = offset

        super().__init__(rule=rule, form=form, comment_to_count=self.count)

    def matrix(self, n):
        if n not in self.matrices:
            matrix = self.dfa[n][:, self.indices]
            if scipy.sparse.issparse(matrix):
                matrix = matrix.toarray()
            self.matrices[n] = matrix.sum(axis=1)
        return self.matrices[n]

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
                enumeration += self._enumerate(self.encoded_symbols[1:], matrix_power - 1)
            enumeration += self._enumerate(states, matrix_power)
        return enumeration + self.word_is_valid(word) - self.offset

    def _enumerate(self, states, n):
        """Given a list of states, how many success strings will we have after
        n rounds of appending digits?"""
        if not states:
            return 0
        states, counts = zip(*Counter(states).items())
        return np.dot(counts, self.matrix(n)[list(states)])


dfa_10_2 = DFA(10, 2)
compressed_dfa = CompressedDFA(10)


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


no_consecutive = sorted([int(x, 2) for x in no_consecutive_states(10)])[1:]
no_successive = [1]
no_repeating = [compressed_dfa.encode((x, 10 - x, 0)) for x in range(10)]
only_repeating = [compressed_dfa.encode((x, 0, 10 - x)) for x in range(10)]
mostly_repeating = [compressed_dfa.encode((x, 1, 10 - x - 1)) for x in range(10 - 1)]

# The only consecutive indices are sums of adjacent powers of two
only_consecutive = sorted(
    [
        sum(2**k for k in range(minval, maxval))
        for maxval in range(10 + 1)
        for minval in range(maxval)
    ]
)

not_any_dfa = NotAnyOfThoseDFA()
not_any_mask = [
    not_any_dfa.int_to_mask(x)
    for x in range(1024)
    if x not in only_consecutive and x not in no_consecutive
]
# Valid states for not any have a bitmask that excludes dfa
not_any = sorted(
    [not_any_dfa.stoi_map[tuple(a), b, 2] for a in not_any_mask for b in range(1, sum(a) - 1)]
)

barely_repeating_dfa = BarelyRepeatingDigitsDFA()
barely_repeating = [barely_repeating_dfa.encode((x, 1)) for x in range(2, 11)]

dfa_threads = {
    "mostly repeating digits": DFAThread(dfa=compressed_dfa, indices=mostly_repeating),
    "no consecutive digits": DFAThread(dfa=dfa_10_2, indices=no_consecutive),
    "no repeating digits": DFAThread(dfa=compressed_dfa, indices=no_repeating),
    "no successive digits": DFAThread(indices=no_successive, dfa=LastDigitDFA()),
    "only consecutive digits": DFAThread(dfa=dfa_10_2, indices=only_consecutive, offset=9),
    "only repeating digits": DFAThread(dfa=compressed_dfa, indices=only_repeating),
    "not any of those": DFAThread(dfa=not_any_dfa, indices=not_any),
    "barely repeating digits": DFAThread(dfa=barely_repeating_dfa, indices=barely_repeating),
}
