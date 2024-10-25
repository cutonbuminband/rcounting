import configparser
import functools
import logging
import math
import os
import re
import string

from fuzzywuzzy import fuzz
from scipy.special import binom

from rcounting import parsing
from rcounting import thread_navigation as tn
from rcounting import utils
from rcounting.units import DAY, HOUR, MINUTE

from .dfa import dfa_threads
from .rules import CountingRule, FastOrSlow, OnlyDoubleCounting
from .side_threads import SideThread
from .validate_count import base_n_count, by_ns_count, count_from_word_list
from .validate_form import base_n, validate_from_tokens

module_dir = os.path.dirname(__file__)
printer = logging.getLogger(__name__)

base_10 = base_n(10)
balanced_ternary = validate_from_tokens("T-0+")
brainfuck = validate_from_tokens("><+-.,[]")
roman_numeral = validate_from_tokens("IVXLCDMↁↂↇ")
mayan_form = validate_from_tokens("Ø1234|-")
twitter_form = validate_from_tokens("@")
parentheses_form = validate_from_tokens("()")


def d20_form(comment_body):
    return "|" in comment_body and base_10(comment_body)


def reddit_username_form(comment_body):
    return "u/" in comment_body


def throwaway_form(comment_body):
    return (fuzz.partial_ratio("u/throwaway", comment_body) > 80) and base_10(comment_body)


def illion_form(comment_body):
    return fuzz.partial_ratio("illion", comment_body) > 80


isenary = {"they're": 1, "taking": 2, "the": 3, "hobbits": 4, "to": 5, "isengard": 0, "gard": 0}
isenary_form = validate_from_tokens(list(isenary.keys()))
isenary_count = functools.partial(count_from_word_list, alphabet=isenary, ignored_chars="!>")

planets = ["mercury", "venus", "earth", "mars", "jupiter", "saturn", "uranus", "neptune"]
planetary_form = validate_from_tokens(planets)
planetary_count = functools.partial(count_from_word_list, alphabet=planets)

colors = ["red", "orange", "yellow", "green", "blue", "indigo", "violet"]
rainbow_form = validate_from_tokens(colors)
rainbow_count = functools.partial(count_from_word_list, alphabet=colors)

with open(os.path.join(module_dir, "us_states.txt"), encoding="utf8") as f:
    us_states = [x.strip().lower() for x in f.readlines()]
us_states_form = validate_from_tokens(us_states)
us_states_count = functools.partial(count_from_word_list, alphabet=us_states, bijective=True)

with open(os.path.join(module_dir, "elements.txt"), encoding="utf8") as f:
    elements = [x.strip() for x in f.readlines()]
element_form = validate_from_tokens(elements)


def element_tokenize(comment_body, _):
    return re.findall("[A-Z][^A-Z]*", comment_body.split("\n")[0])


element_count = functools.partial(
    count_from_word_list, alphabet=elements, tokenize=element_tokenize, bijective=True
)


def permutation_order(word, alphabet, ordered=False, no_leading_zeros=False):
    word_length = len(word)
    if word_length == 0:
        return 0
    index = alphabet.index(word[0])
    position = index - int(no_leading_zeros)
    n_digits = len(alphabet)
    prefix = [] if ordered else alphabet[:index]
    new_alphabet = prefix + alphabet[index + 1 :]
    if ordered:
        first_place_counts = sum(
            math.comb(n_digits - 1 - i, word_length - 1) for i in range(position)
        )
    else:
        first_place_counts = position * math.perm(n_digits - 1, word_length - 1)
    return first_place_counts + permutation_order(word[1:], new_alphabet, ordered=ordered)


def _permutation_count(comment_body, alphabet) -> int:
    alphabet = alphabet.lower()
    word = "".join(
        x for x in parsing.normalize_comment_body(comment_body).lower() if x in alphabet
    )
    l = len(word)
    shorter_words = sum(math.factorial(i) for i in range(1, l))
    return shorter_words + permutation_order(word, alphabet[:l]) - 1


permutation_count = functools.partial(_permutation_count, alphabet="123456789")
letter_permutation_count = functools.partial(_permutation_count, alphabet=string.ascii_lowercase)


def bcd_count(comment):
    count = parsing.extract_count_string(comment, base=2)
    digits = [str(int("".join(y for y in x), 2)) for x in utils.chunked(count, 4)]
    return int("".join(digits))


def nrd_count(comment):
    normalized_comment = parsing.extract_count_string(comment)
    result = 9 * sum(math.perm(9, i - 1) for i in range(1, len(normalized_comment)))
    return result + permutation_order(normalized_comment, string.digits, no_leading_zeros=True)


def nrl_count(comment):
    line = "".join(
        x for x in parsing.normalize_comment_body(comment).lower() if x in string.ascii_lowercase
    )
    shorter_words = sum(math.perm(26, i) for i in range(1, len(line)))
    return shorter_words + permutation_order(line, string.ascii_lowercase)


def powerball_count(comment):
    balls, powerball = parsing.normalize_comment_body(comment).split("+")
    balls = balls.split()
    alphabet = [str(x) for x in range(1, 70)]
    return permutation_order(balls, alphabet, ordered=True) * 26 + int(powerball) - 1


u_squares = [11035, 65039, 129003, 129002, 128998, 129001, 129000, 128999, 128997, 11036]
colored_squares_form = validate_from_tokens([chr(x) for x in u_squares])


@functools.cache
def collatz(n):
    if n == 1:
        return 1
    if n % 2 == 0:
        return 1 + collatz(n // 2)
    return 1 + collatz(3 * n + 1)


def collatz_count(comment):
    regex = r".*\((\d+).*(\d+)\)"
    current, steps = map(int, re.search(regex, comment).groups())
    return sum(collatz(i) for i in range(1, current)) + steps


def ordered_pairs_count(comment_body):
    # A set of brackets containing two integers, separated by at least one non-integer
    regex = r"\(([0-9]+)[^0-9]+([0-9]+)\)"
    x, y = map(int, re.search(regex, comment_body).groups())
    return x**2 + y if y <= x else y**2 + 2 * y - x


def rgb_count(comment_body: str):
    first_line = parsing.normalize_comment_body(comment_body)
    values = map(int, re.findall(r"\d+", first_line))
    return functools.reduce(lambda x, y: 256 * x + y, values)


# an int, then a bracketed int, maybe with a plus or a minus after it
wave_regex = r"(-?\d+).*\((\d+)[\+-]?\)"
double_wave_regex = r"(-?\d+).*\((\d+)\).*\((\d+)\)"


def wave_count(comment):
    comment = parsing.normalize_comment_body(comment)
    match = re.search(wave_regex, comment)
    a, b = [int(x) for x in match.groups()]
    return 2 * b**2 - a


def increasing_type_count(n):
    regex = r"(-?\d+)" + r".*\((\d+)\)" * n

    def count(comment):
        comment = parsing.normalize_comment_body(comment)
        total = 0
        values = [int(x) for x in re.search(regex, comment).groups()]
        for ix, value in enumerate(values):
            total += triangle_n_dimension(ix + 1, value)
        return total

    return count


def triangle_n_dimension(n, value):
    if value == 1:
        return 0
    return math.comb(value - 2 + n, n)


def gaussian_integer_count(comment):
    digits = parsing.extract_count_string(comment, base=4)
    corner = sum((-4) ** ix * int(digit) for ix, digit in enumerate(digits[::-2]))
    return (2 * corner + 1) ** 2


def get_base_powers(n, b):
    r = []
    while n:
        r.append(n % b)
        n = n // b
    return r[::-1]


def palindrome_count(comment_body: str, b=10) -> int:
    count = parsing.find_count_in_text(comment_body, b)
    digits = get_base_powers(count, b)
    l = math.ceil(len(digits) / 2)
    return functools.reduce(lambda x, y: b * x + y, digits[:l]) + b ** (l - len(digits) % 2)


binary_palindrome_count = functools.partial(palindrome_count, b=2)
hex_palindrome_count = functools.partial(palindrome_count, b=16)


def cw_binary_count(comment_body):
    bits = parsing.extract_count_string(comment_body, base=2)
    shorter_counts = 2 ** len(bits) - 1
    fewer_ones = sum(int(binom(len(bits), ones)) for ones in range(bits.count("1")))
    earlier_counts = 0
    ones = 1
    for i, bit in enumerate(reversed(bits)):
        if bit == "1":
            earlier_counts += int(binom(i, ones))
            ones += 1
    return shorter_counts + fewer_ones + earlier_counts


@functools.cache
def mahonian(n, k):
    if n == 1 and k == 0:
        return 1
    if n < 0 or k < 0 or k > n * (n - 1) / 2:
        return 0
    return mahonian(n, k - 1) + mahonian(n - 1, k) - mahonian(n - 1, k - n)


# See https://old.reddit.com/r/counting/comments/18z0of2/free_talk_friday_436/kgii6c0/
def cw_factoradic_count(comment_body, base=10):
    count_string = parsing.extract_count_string(comment_body, base=base)
    digits = [int(char, base) for char in count_string]

    shorter_counts = sum(math.factorial(d + 1) for d in range(len(digits)))
    smaller_weights = sum(mahonian(len(digits) + 1, w) for w in range(sum(digits)))

    earlier_counts = 0
    k = 1
    for i, digit in enumerate(reversed(digits)):
        while digit > 0:
            earlier_counts += mahonian(i + 1, k)
            k += 1
            digit -= 1

    return shorter_counts + smaller_weights + earlier_counts


def update_dates(count, chain, previous=False):
    regex = r"([,\d]+)$"  # All digits at the end of the line, plus optional separators
    for submission in chain:
        year = int(re.search(regex, submission.title).group().replace(",", ""))
        if previous:
            year = year - 3
        length = 1095 + any(map(utils.is_leap_year, range(year, year + 3)))
        count += length
    return count


update_previous_dates = functools.partial(update_dates, previous=True)


def update_from_traversal(count, chain):
    for thread in chain[1:]:
        _, get_id = tn.find_previous_submission(thread)
        comments = tn.fetch_comments(get_id)
        count += len(comments)
    return count


known_threads = {
    "-illion": SideThread(form=illion_form, length=1000),
    "2d20 experimental v theoretical": SideThread(form=d20_form, length=1000),
    "balanced ternary": SideThread(form=balanced_ternary, length=729),
    "base 16 roman": SideThread(form=roman_numeral),
    "base 2i": SideThread(form=base_n(4), comment_to_count=gaussian_integer_count),
    "beenary": SideThread(length=1024, form=validate_from_tokens(["bee", "movie"])),
    "bijective base 2": SideThread(form=base_n(3), length=1024),
    "binary encoded decimal": SideThread(form=base_n(2), comment_to_count=bcd_count),
    "binary encoded hexadecimal": SideThread(form=base_n(2), length=1024),
    "binary palindromes": SideThread(form=base_n(2), comment_to_count=binary_palindrome_count),
    "by 3s in base 7": SideThread(form=base_n(7)),
    "by 3s": SideThread(comment_to_count=by_ns_count(3)),
    "by 4s": SideThread(comment_to_count=by_ns_count(4)),
    "by 5s": SideThread(comment_to_count=by_ns_count(5)),
    "by 7s": SideThread(comment_to_count=by_ns_count(7)),
    "by 99s": SideThread(comment_to_count=by_ns_count(99)),
    "collatz conjecture": SideThread(comment_to_count=collatz_count, form=base_10),
    "colored squares": SideThread(form=colored_squares_form, length=729),
    "constant sum factoradic": SideThread(form=base_10, comment_to_count=cw_factoradic_count),
    "constant weight binary": SideThread(form=base_n(2), comment_to_count=cw_binary_count),
    "cyclical bases": SideThread(form=base_n(16)),
    "dates": SideThread(form=base_10, update_function=update_dates),
    "decimal encoded sexagesimal": SideThread(length=900, form=base_10),
    "dollars and cents": SideThread(form=base_n(4)),
    "double increasing": SideThread(form=base_10, comment_to_count=increasing_type_count(2)),
    "fast or slow": SideThread(rule=FastOrSlow()),
    "four fours": SideThread(form=validate_from_tokens("4")),
    "hexadecimal palindromes": SideThread(form=base_n(16), comment_to_count=hex_palindrome_count),
    "increasing sequences": SideThread(form=base_10, comment_to_count=increasing_type_count(1)),
    "invisible numbers": SideThread(form=base_n(10, strip_links=False)),
    "isenary": SideThread(form=isenary_form, comment_to_count=isenary_count),
    "japanese": SideThread(form=validate_from_tokens("一二三四五六七八九十百千")),
    "letter permutations": SideThread(comment_to_count=letter_permutation_count),
    "mayan numerals": SideThread(length=800, form=mayan_form),
    "no repeating letters": SideThread(comment_to_count=nrl_count),
    "o/l binary": SideThread(form=validate_from_tokens("ol"), length=1024),
    "once per thread": SideThread(form=base_10, rule=CountingRule(wait_n=None)),
    "only double counting": SideThread(form=base_10, rule=OnlyDoubleCounting()),
    "ordered pairs": SideThread(form=base_10, comment_to_count=ordered_pairs_count),
    "palindromes": SideThread(form=base_10, comment_to_count=palindrome_count),
    "parentheses": SideThread(form=parentheses_form),
    "periodic table": SideThread(form=element_form, comment_to_count=element_count),
    "permutations": SideThread(form=base_10, comment_to_count=permutation_count),
    "previous dates": SideThread(form=base_10, update_function=update_previous_dates),
    "planetary octal": SideThread(comment_to_count=planetary_count, form=planetary_form),
    "powerball": SideThread(comment_to_count=powerball_count, form=base_10),
    "rainbow": SideThread(comment_to_count=rainbow_count, form=rainbow_form),
    "reddit usernames": SideThread(length=722, form=reddit_username_form),
    "rgb values": SideThread(form=base_10, comment_to_count=rgb_count),
    "roman progressbar": SideThread(form=roman_numeral),
    "roman": SideThread(form=roman_numeral),
    "slow": SideThread(form=base_10, rule=CountingRule(thread_time=MINUTE)),
    "slower": SideThread(form=base_10, rule=CountingRule(user_time=HOUR)),
    "slowestest": SideThread(form=base_10, rule=CountingRule(thread_time=HOUR, user_time=DAY)),
    "symbols": SideThread(form=validate_from_tokens("!@#$%^&*()")),
    "throwaways": SideThread(form=throwaway_form),
    "triple increasing": SideThread(form=base_10, comment_to_count=increasing_type_count(3)),
    "twitter handles": SideThread(length=1369, form=twitter_form),
    "unary": SideThread(form=validate_from_tokens("|")),
    "unicode": SideThread(form=base_n(16), length=1024),
    "us states": SideThread(form=us_states_form, comment_to_count=us_states_count),
    "using 12345": SideThread(form=validate_from_tokens("12345")),
    "valid brainfuck programs": SideThread(form=brainfuck),
    "wait 10": SideThread(form=base_10, rule=CountingRule(wait_n=10)),
    "wait 2 - letters": SideThread(rule=CountingRule(wait_n=2)),
    "wait 2": SideThread(form=base_10, rule=CountingRule(wait_n=2)),
    "wait 3": SideThread(form=base_10, rule=CountingRule(wait_n=3)),
    "wait 4": SideThread(form=base_10, rule=CountingRule(wait_n=4)),
    "wait 5s": SideThread(form=base_10, rule=CountingRule(thread_time=5)),
    "wait 9": SideThread(form=base_10, rule=CountingRule(wait_n=9)),
    "wave": SideThread(form=base_10, comment_to_count=wave_count),
}

known_threads.update(dfa_threads)

base_n_threads = {
    f"base {n}": SideThread(form=base_n(n), comment_to_count=base_n_count(n)) for n in range(2, 37)
}
known_threads.update(base_n_threads)

known_threads.update(
    {
        thread: SideThread(form=base_10, comment_to_count=base_n_count(10))
        for thread in ["by meters", "sheep", "word association"]
    }
)
# See: https://www.reddit.com/r/counting/comments/o7ko8r/free_talk_friday_304/h3c7433/?context=3


def by_x_count(comment_body: str, x=1.0):
    regex = r"([-+]?[\.,\s0-9]*)"
    body = parsing.normalize_comment_body(comment_body)
    count_string = re.search(regex, body).groups()[0]
    count_string = "".join(count_string.split()).replace(",", "")
    return int(float(count_string) / x)


by_xs = [0.01, 0.02, 0.025, 0.05, 6, 7, 8, 10, 11, 12, 20, 23, 29, 40, 50, 64, 69, 123, 1000]
known_threads.update(
    {
        f"by {x}s": SideThread(form=base_10, comment_to_count=functools.partial(by_x_count, x=x))
        for x in by_xs
    }
)

default_threads = [
    "10 at a time",
    "3 or fewer palindromes",
    "69, 420, or 666",
    "age",
    "all even or all odd",
    "by 2s even",
    "by 2s odd",
    "california license plates",
    "chess matches",
    "decimal",
    "four squares",
    "n read as base n number",
    "negative numbers",
    "powers of 2",
    "previous dates",
    "prime factorization",
    "prime numbers",
    "rational numbers",
    "rotational symmetry",
    "scientific notation",
    "street view counting",
    "thread completion",
    "top subreddits",
    "triangular numbers",
    "unordered consecutive digits",
    "william the conqueror",
]
known_threads.update(
    {thread_name: SideThread(form=base_10, length=1000) for thread_name in default_threads}
)

default_threads = {
    "eban": 800,
    "factoradic": 720,
    "feet and inches": 600,
    "hoi4 states": 806,
    "ipv4": 1024,
    "lucas numbers": 200,
    "seconds minutes hours": 1200,
    "time": 900,
}
known_threads.update(
    {key: SideThread(form=base_10, length=length) for key, length in default_threads.items()}
)


no_validation = {
    "acronyms": 676,
    "base 40": 1600,
    "base 60": 900,
    "base 62": 992,
    "base 64": 1024,
    "base 93": 930,
    "bijective base 205": 1025,
    "cards": 676,
    "degrees": 900,
    "iterate each letter": None,
    "letters": 676,
    "musical notes": 1008,
    "octal letter stack": 1024,
    "palindromes - letters": 676,
    "permutations - letters": None,
    "previous_dates": None,
    "qwerty alphabet": 676,
    "youtube": 1024,
}

known_threads.update({k: SideThread(length=v) for k, v in no_validation.items()})

default_thread_varying_length = [
    "2d tug of war",
    "boost 5",
    "by battery percentage",
    "by coad rank",
    "by comment karma",
    "by counters met irl",
    "by day of the week",
    "by day of the year",
    "by digits in total karma",
    "by gme increase/decrease",
    "by hoc rank",
    "by how well your day is going",
    "by length of username",
    "by number of post upvotes",
    "by random number (1-1000)",
    "by random number",
    "by timestamp seconds",
    "check-in streak",
    "nim",
    "pick from five",
    "post karma",
    "total karma",
    "tug of war",
]

default_thread_unknown_length = [
    "base of previous digit",
    "by list size",
    "by number of digits squared",
    "divisors",
]


def get_side_thread(thread_name):
    """Return the properties of the side thread with name thread_name"""
    if thread_name in known_threads:
        return known_threads[thread_name]
    if thread_name in default_thread_unknown_length:
        return SideThread(form=base_10)
    if thread_name in default_thread_varying_length:
        return SideThread(update_function=update_from_traversal, form=base_10)
    if thread_name != "default":
        printer.info(
            (
                "No rule found for %s. Not validating comment contents. "
                "Assuming n=1000 and no double counting."
            ),
            thread_name,
        )
    return SideThread()


config = configparser.ConfigParser()
config.read(os.path.join(module_dir, "side_threads.ini"))
known_thread_ids = config["threads"]
