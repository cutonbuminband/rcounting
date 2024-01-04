import configparser
import functools
import logging
import math
import os
import re
import string

from fuzzywuzzy import fuzz

from rcounting import parsing
from rcounting import thread_navigation as tn
from rcounting import utils
from rcounting.units import DAY, HOUR, MINUTE

from .rules import CountingRule, FastOrSlow, OnlyDoubleCounting
from .side_threads import OnlyRepeatingDigits, SideThread, ignore_revivals
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
    word = "".join(x for x in comment_body.split("\n")[0].lower() if x in alphabet)
    l = len(word)
    shorter_words = sum(math.factorial(i) for i in range(1, l))
    return shorter_words + permutation_order(word, alphabet[:l]) - 1


permutation_count = functools.partial(_permutation_count, alphabet="123456789")
letter_permutation_count = functools.partial(_permutation_count, alphabet=string.ascii_lowercase)


def bcd_count(comment):
    count = f"{parsing.find_count_in_text(comment, base=2):b}"
    digits = [str(int("".join(y for y in x), 2)) for x in utils.chunked(count, 4)]
    return int("".join(digits))


def nrd_count(comment):
    normalized_comment = str(parsing.find_count_in_text(comment))
    result = 9 * sum(math.perm(9, i - 1) for i in range(1, len(normalized_comment)))
    return result + permutation_order(normalized_comment, string.digits, no_leading_zeros=True)


def nrl_count(comment):
    line = comment.split("\n")[0].strip().lower()
    shorter_words = sum(math.perm(26, i) for i in range(1, len(line)))
    return shorter_words + permutation_order(line, string.ascii_lowercase)


def powerball_count(comment):
    balls, powerball = parsing.normalize_comment(comment).split("+")
    balls = balls.split()
    alphabet = [str(x) for x in range(1, 70)]
    return permutation_order(balls, alphabet, ordered=True) * 26 + int(powerball) - 1


def no_successive_count(comment):
    word = str(parsing.find_count_in_text(comment))
    result = sum(9**i for i in range(1, len(word)))
    previous_i = "0"
    for ix, i in enumerate(word[:-1]):
        result += 9 ** (len(word) - ix - 1) * (int(i) - (i >= previous_i))
        previous_i = i
    return result


u_squares = [11035, 65039, 129003, 129002, 128998, 129001, 129000, 128999, 128997, 11036]
colored_squares_form = validate_from_tokens([chr(x) for x in u_squares])

collatz_dict = {}


def collatz(n):
    if n == 1:
        return 1
    if n in collatz_dict:
        return collatz_dict[n]
    if n % 2 == 0:
        return 1 + collatz(n // 2)
    return 1 + collatz(3 * n + 1)


def collatz_count(comment):
    regex = r".*\((\d+).*(\d+)\)"
    current, steps = map(int, re.search(regex, comment).groups())
    return sum(collatz(i) for i in range(1, current)) + steps


# an int, then a bracketed int, maybe with a plus or a minus after it
wave_regex = r"(-?\d+).*\((\d+)[\+-]?\)"
double_wave_regex = r"(-?\d+).*\((\d+)\).*\((\d+)\)"


def wave_count(comment):
    comment = parsing.normalize_comment(comment)
    match = re.search(wave_regex, comment)
    a, b = [int(x) for x in match.groups()]
    return 2 * b**2 - a


def increasing_type_count(n):
    regex = r"(-?\d+)" + r".*\((\d+)\)" * n

    def count(comment):
        comment = parsing.normalize_comment(comment)
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
    digits = str(parsing.find_count_in_text(comment))
    corner = sum((-4) ** ix * int(digit) for ix, digit in enumerate(digits[::-2]))
    return (2 * corner + 1) ** 2


def update_dates(count, chain, was_revival=None, previous=False):
    if previous:
        chain = ignore_revivals(chain, was_revival)[1:]
    else:
        chain = ignore_revivals(chain, was_revival)[:-1]
    regex = r"([,\d]+)$"  # All digits at the end of the line, plus optional separators
    for submission in chain:
        year = int(re.search(regex, submission.title).group().replace(",", ""))
        length = 1095 + any(map(utils.is_leap_year, range(year, year + 3)))
        count += length
    return count


update_previous_dates = functools.partial(update_dates, previous=True)


def update_from_traversal(count, chain, was_revival):
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
    "by 3s in base 7": SideThread(form=base_n(7)),
    "by 3s": SideThread(comment_to_count=by_ns_count(3)),
    "by 4s": SideThread(comment_to_count=by_ns_count(4)),
    "by 5s": SideThread(comment_to_count=by_ns_count(5)),
    "by 7s": SideThread(comment_to_count=by_ns_count(7)),
    "by 99s": SideThread(comment_to_count=by_ns_count(99)),
    "collatz conjecture": SideThread(comment_to_count=collatz_count, form=base_10),
    "colored squares": SideThread(form=colored_squares_form, length=729),
    "cyclical bases": SideThread(form=base_n(16)),
    "dates": SideThread(form=base_10, update_function=update_dates),
    "decimal encoded sexagesimal": SideThread(length=900, form=base_10),
    "dollars and cents": SideThread(form=base_n(4)),
    "double increasing": SideThread(form=base_10, comment_to_count=increasing_type_count(2)),
    "fast or slow": SideThread(rule=FastOrSlow()),
    "four fours": SideThread(form=validate_from_tokens("4")),
    "increasing sequences": SideThread(form=base_10, comment_to_count=increasing_type_count(1)),
    "invisible numbers": SideThread(form=base_n(10, strip_links=False)),
    "isenary": SideThread(form=isenary_form, comment_to_count=isenary_count),
    "japanese": SideThread(form=validate_from_tokens("一二三四五六七八九十百千")),
    "letter permutations": SideThread(comment_to_count=letter_permutation_count),
    "mayan numerals": SideThread(length=800, form=mayan_form),
    "no repeating digits": SideThread(comment_to_count=nrd_count, form=base_10),
    "no repeating letters": SideThread(comment_to_count=nrl_count),
    "no successive digits": SideThread(comment_to_count=no_successive_count, form=base_10),
    "o/l binary": SideThread(form=validate_from_tokens("ol"), length=1024),
    "once per thread": SideThread(form=base_10, rule=CountingRule(wait_n=None)),
    "only double counting": SideThread(form=base_10, rule=OnlyDoubleCounting()),
    "only repeating digits": OnlyRepeatingDigits(),
    "parentheses": SideThread(form=parentheses_form),
    "periodic table": SideThread(form=element_form, comment_to_count=element_count),
    "permutations": SideThread(form=base_10, comment_to_count=permutation_count),
    "previous dates": SideThread(form=base_10, update_function=update_previous_dates),
    "planetary octal": SideThread(comment_to_count=planetary_count, form=planetary_form),
    "powerball": SideThread(comment_to_count=powerball_count, form=base_10),
    "rainbow": SideThread(comment_to_count=rainbow_count, form=rainbow_form),
    "reddit usernames": SideThread(length=722, form=reddit_username_form),
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


base_n_threads = {
    f"base {n}": SideThread(form=base_n(n), comment_to_count=base_n_count(n)) for n in range(2, 37)
}
known_threads.update(base_n_threads)

# See: https://www.reddit.com/r/counting/comments/o7ko8r/free_talk_friday_304/h3c7433/?context=3

default_threads = [
    "10 at a time",
    "3 or fewer palindromes",
    "69, 420, or 666",
    "age",
    "all even or all odd",
    "by 0.025s",
    "by 0.02s",
    "by 0.05s",
    "by 1000s",
    "by 10s",
    "by 11s",
    "by 123s",
    "by 12s",
    "by 20s",
    "by 23s",
    "by 29s",
    "by 2s even",
    "by 2s odd",
    "by 40s",
    "by 50s",
    "by 64s",
    "by 69s",
    "by 6s",
    "by 8s",
    "by meters",
    "by one-hundredths",
    "california license plates",
    "decimal",
    "four squares",
    "n read as base n number",
    "negative numbers",
    "no consecutive digits",
    "palindromes",
    "powers of 2",
    "previous dates",
    "prime factorization",
    "prime numbers",
    "rational numbers",
    "rotational symmetry",
    "scientific notation",
    "sheep",
    "street view counting",
    "thread completion",
    "top subreddits",
    "triangular numbers",
    "unordered consecutive digits",
    "william the conqueror",
    "word association",
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
    """Return the properties of the side thread with first post thread_id"""
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
