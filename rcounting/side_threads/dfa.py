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
        self.size = n_symbols + 2

    def generate_transition_matrix(self):
        transition_matrix = np.zeros((self.size, self.size), dtype=int)
        new_state = np.ones(self.size)
        new_state[0] = 0
        for row in range(self.size - 1):
            column = new_state.copy()
            column[row] = 0
            transition_matrix[row] = column
        transition_matrix[0, -1] = 0
        return transition_matrix

    def encode(self, state: str):
        if not state:
            return 0
        symbols = alphanumeric[: self.n_symbols]
        previous = ""
        for char in state:
            if char == previous:
                return self.size - 1
            previous = char
        return 1 + symbols.index(char)  # pylint: disable=undefined-loop-variable


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

    The trickiest part is finding a sensible representation for each NOAT
    state, so that we don't create matrices with large swathes of invalid
    states. For example, in a two-symbol world, the state ([0, 1], 2, 1) is
    invalid, since it claims we have two symbols present at least twice, but
    only one symbol present at all.

    We'll use the following layout:

    - The range [0, 2^n) will be used to represent the states [a, 0, 0]: for b =
    0 there is only one valid value of c.

    - The range [2^n, 3 * 2^n) will be used to represent the states [a, sum(a),
    1] and [a, sum(a), 2], since when b == sum(a), c == 0 is invalid.


    - The remaining range [3 * 2^n, 3 * n * 2^(n - 1)) will represent the
    remaining states in order according to:
        - The number of bits set in their mask
        - The (inverse) lexicographic position of their bitmask among those of the same weight
        - The value of b
        - The value of c
    """

    def __init__(self, n=10):
        super().__init__(n_symbols=n, n_states=3)
        #
        # As part of the encoding/decoding routine, we need to know the
        # cumulative value of i * C(n_symbols, i) for different values of i.
        # Each one represents the number of states in our DFA with a certain
        # number of set bits (ignoring the NSD trit since its contribution to
        # the count is trivial to calculate): Given a fixed number of symbols
        # and a number of bits to set, there are C(n_symbols, i) different bit
        # masks we can make. For each of those, there are i different possible
        # counts of how many of the set bits should be at least two.

        counts = [max((i - 1) * math.comb(n, i), 0) for i in range(n + 1)]
        # cumulative counts stores the cumulative count, so that
        # cumulative_count[k] is the total number of states with k or fewer
        # bits set in the mask.
        self.cumulative_counts = np.cumsum(counts)
        self.size = 3 * n * 2 ** (n - 1) + 1
        self.mask_decoder = {}

    def _find_next_states(self, mask: Tuple[bool, ...], b: int, c: int):
        total = sum(mask)
        result = []
        # Every digit not present in the state just results in one more bit
        # set, the same number of twos in the string, and a last digit of one
        for idx, bit in enumerate(mask):
            if not bit:
                current = tuple(mask[:idx]) + (True,) + tuple(mask[idx + 1 :])
                result += [(1, (current, b, 0 if c != 2 else 2))]
        # If we aren't already in a success state for \overline{NSD}, then one
        # of the digits we add will move us there
        if total > 0:
            if c != 2:
                result += [(1, (mask, b + (c == 0), 2))]
            result += [(b - (c == 1), (mask, b, 1 if c != 2 else 2))]
            result += [(total - b - (c == 0), (mask, b + 1, 1 if c != 2 else 2))]
        return [x for x in result if x[0] > 0]

    def mask_to_int(self, bits: Sequence[bool]):
        return sum(bit * 2**i for i, bit in enumerate(bits))

    def int_to_mask(self, state: int) -> Tuple[bool, ...]:
        return tuple(char == "1" for char in f"{state:0>10b}"[::-1])

    def state_to_int(self, bits: Tuple[bool, ...] | int, b: int, c: int) -> int:
        """A mapping of a state to an integer which represents that state.
        There are 2**n_symbols possible bit fields of length n_symbols, for
        each of those there are up to (n_symbols + 1) valid states for the
        number of ones, and then there are 3 states for the NSD trit. A naïve
        encoding would thus yield

        2**n_symbols * (n_symbols + 1) * 3 ~ 34k states for n_symbols == 10

        But we can be cleverer than that, and reduce it to less than half those
        states. The downside is that the encoding and decoding procedure
        becomes more complicated.

        """
        if isinstance(bits, int):
            bits = self.int_to_mask(bits)

        # We have 2**n states that have b = 0, one for every possible value of the
        # mask.
        if b == 0:
            return self.mask_to_int(bits)
        offset = 2**self.n_symbols

        total = sum(bits)

        # We have 2 ** n - 2 states with b = total, since the case b = 0 was
        # accounted for above.
        if b == total:
            assert c != 0
            return offset + (self.mask_to_int(bits) - 1) * 2 + (c - 1)
        offset += 2 * (2**self.n_symbols - 1)

        # Additionally, we need to account for all the states with a smaller
        # number of bits set in the mask. We need this number for the decoding
        # as well, so we've precalculated it in the init.
        offset += 3 * self.cumulative_counts[total - 1]

        # With the completed sets out of the way, we need to assign some
        # integer to the current mask, in the line of all masks with the same
        # total. We'll use an inverse lexicographic order so that 1000 is the
        # first mask of length four with weight one, 0100 is the second and so
        # on.
        cw_position = sum(
            bit * math.comb(idx, ones)
            for idx, (bit, ones) in enumerate(zip(bits, np.cumsum(bits, dtype=int)))
        )
        # That tells us how many masks we've completed before the current one
        # with the same total weight; each one has a total of 2*total states it
        # matches. Within each one, there are three valid values for the NSD trit
        # and `total` valid values for the number of twos in the string. To
        # that we need to add thrice the value of b (one for each value of the
        # NSD trit), and finally the value of the NSD trit itself.
        return offset + 3 * (total - 1) * cw_position + (b - 1) * 3 + c

    def int_to_state(self, state: int) -> Tuple[Tuple[bool, ...], int, int]:
        if state < 2**self.n_symbols:
            return self.int_to_mask(state), 0, 0

        state = state - 2**self.n_symbols

        if state < 2 * 2**self.n_symbols - 2:
            c = state % 2 + 1
            state = state // 2
            a = self.int_to_mask(state + 1)
            return a, sum(a), c
        state = state - (2 * 2**self.n_symbols - 2)
        c = state % 3
        state = state // 3
        old_value = 0
        idx = 0
        for idx, value in enumerate(self.cumulative_counts):
            if value > state:
                break
            old_value = value
        state = state - old_value
        b = (state % (idx - 1)) + 1
        state = state // (idx - 1)
        bits = self._decode_mask(state, idx)
        return bits, b, c

    def _decode_mask(self, state: int, ones: int, n: int | None = None) -> Tuple[bool, ...]:
        if n is None:
            n = self.n_symbols
        if n == 0:
            return tuple()
        if (state, ones, n) not in self.mask_decoder:
            # How many combinations are there of shorter strings with the same number
            # of ones? If more, we can safely put a zero here and move on. If fewer, we
            # need to put a one here before recursing.
            cost = math.comb(n - 1, ones)
            if state >= cost:
                self.mask_decoder[state, ones, n] = self._decode_mask(
                    state - cost, ones - 1, n - 1
                ) + (True,)
            else:
                self.mask_decoder[state, ones, n] = self._decode_mask(state, ones, n - 1) + (
                    False,
                )
        return self.mask_decoder[state, ones, n]

    def generate_transition_matrix(self):
        i = np.empty(10 * self.size, dtype=int)
        j = np.empty(10 * self.size, dtype=int)
        data = np.empty(10 * self.size, dtype=int)
        ix = 0
        for state in range(self.size):
            next_states = self._find_next_states(*self.int_to_state(state))
            counts, new_states = zip(*next_states)
            new_states = [self.state_to_int(*ns) for ns in new_states]
            if any(ns >= self.size for ns in new_states):
                print(new_states)
                print(state)
            total = len(counts)
            i[ix : ix + total] = state
            j[ix : ix + total] = new_states
            data[ix : ix + total] = counts
            ix += total
        return scipy.sparse.coo_array(
            (data[:ix], (i[:ix], j[:ix])),
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
        return self.state_to_int(tuple(bitmask), b, c)


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
no_successive = list(range(1, 11))
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
not_any = [
    not_any_dfa.state_to_int(tuple(a), b, 2) for a in not_any_mask for b in range(1, sum(a) - 1)
]

dfa_threads = {
    "mostly repeating digits": DFAThread(dfa=compressed_dfa, indices=mostly_repeating),
    "no consecutive digits": DFAThread(dfa=dfa_10_2, indices=no_consecutive),
    "no repeating digits": DFAThread(dfa=compressed_dfa, indices=no_repeating),
    "no successive digits": DFAThread(indices=no_successive, dfa=LastDigitDFA()),
    "only consecutive digits": DFAThread(dfa=dfa_10_2, indices=only_consecutive, offset=9),
    "only repeating digits": DFAThread(dfa=compressed_dfa, indices=only_repeating),
    "not any of those": DFAThread(dfa=not_any_dfa, indices=not_any),
}
