import math
import pandas as pd
import re
from string import digits, ascii_uppercase
from models import comment_to_dict
from thread_navigation import fetch_comment_tree
import parsing
from utils import is_leap_year, deleted_phrases

minute = 60
hour = 60 * 60
day = 60 * 60 * 24


ignored_users = ["LuckyNumber-Bot", "CountingStatsBot", "CountingHelper", "WikiSummarizerBot"]
ignored_users = [x.lower() for x in ignored_users]


class CountingRule():
    def __init__(self, wait_n=1, thread_time=0, user_time=0):
        self.n = wait_n
        self.thread_time = thread_time
        self.user_time = user_time

    def _valid_skip(self, history):
        n = self.n if self.n is not None else len(history)
        history = history.reset_index()
        skips = history.groupby('username')['index'].diff()
        return skips.isna() | (skips > n)

    def _valid_thread_time(self, history):
        if not self.thread_time:
            return True
        elapsed_time = history['timestamp'].diff()
        valid_time = elapsed_time.isna() | (elapsed_time >= self.thread_time)
        return valid_time

    def _valid_user_time(self, history):
        if not self.user_time:
            return True
        elapsed_user_time = history.groupby('username')['timestamp'].diff()
        valid_user_time = elapsed_user_time.isna() | (elapsed_user_time >= self.user_time)
        return valid_user_time

    def is_valid(self, history):
        return (self._valid_skip(history)
                & self._valid_thread_time(history)
                & self._valid_user_time(history))

    def get_history(self, comment):
        comments = comment.walk_up_tree(limit=self.n + 1)
        max_time = max(self.thread_time, self.user_time)
        while (not comments[-1].is_root
               and (comment.created_utc - comments[-1].created_utc) < max_time):
            comments = comments[:-1] + comments[-1].walk_up_tree(limit=9)
        return pd.DataFrame([comment_to_dict(x) for x in comments[:0:-1]])


class OnlyDoubleCounting():
    def is_valid(self, history):
        history = history.set_index('comment_id')
        history['mask'] = True
        unshifted = history.username.iloc[::2]
        up_shift = history.username.shift(-1).iloc[::2]
        up_mask = up_shift.isna() | (up_shift == unshifted)
        down_shift = history.username.shift().iloc[::2]
        down_mask = down_shift.isna() | (down_shift == unshifted)
        mask = up_mask if(up_mask.sum() > down_mask.sum()) else down_mask
        history.loc[mask.index, 'mask'] = mask
        history.reset_index(inplace=True)
        return history['mask']

    def get_history(self, comment):
        comments = comment.walk_up_tree(limit=2)[:0:-1]
        return pd.DataFrame([comment_to_dict(x) for x in comments])


def validate_from_character_list(valid_characters, strip_links=True):
    def looks_like_count(comment_body):
        body = comment_body.upper()
        if strip_links:
            body = parsing.strip_markdown_links(body)
        return any([character in body for character in valid_characters])
    return looks_like_count


def base_n(n=10, strip_links=True):
    alphanumeric = digits + ascii_uppercase
    return validate_from_character_list(alphanumeric[:n], strip_links)


def permissive(comment):
    return True


balanced_ternary = validate_from_character_list('T-0+')
brainfuck = validate_from_character_list('><+-.,[]')
roman_numeral = validate_from_character_list('IVXLCDMↁↂↇ')
mayan_form = validate_from_character_list('Ø1234|-')
twitter_form = validate_from_character_list('@')
parentheses_form = validate_from_character_list('()')


def d20_form(comment_body):
    return "|" in comment_body and base_10(comment_body)

def reddit_username_form(comment_body):
    return 'u/' in comment_body


def throwaway_form(comment_body):
    return reddit_username_form(comment_body) and base_10(comment_body)


base_10 = base_n(10)
default_rule = CountingRule()

wave_regex = r'(-?\d+).*\((\d+)\+?\)'  # an int, then a bracketed int, maybe with a plus after it
double_wave_regex = r'(-?\d+).*\((\d+)\).*\((\d+)\)'


def update_wave(old_count, chain, was_revival=None):
    if was_revival is not None:
        chain = [x for x, y in zip(chain, was_revival) if not y]
    a, b = parsing.parse_thread_title(chain[-1].title, wave_regex)
    return 2 * b ** 2 - a


def update_increasing_type(n):
    regex = r'(-?\d+)' + r'.*\((\d+)\)' * n

    def update(old_count, chain, was_revival=None):
        if was_revival is not None:
            chain = [x for x, y in zip(chain, was_revival) if not y]
        total = 0
        values = parsing.parse_thread_title(chain[-1].title, regex)
        for idx, value in enumerate(values):
            total += triangle_n_dimension(idx + 1, value)
        return total

    return update


def triangle_n_dimension(n, value):
    if value == 1:
        return 0
    return math.comb(value - 2 + n, n)


def update_2i(count, chain, was_revival=None):
    if was_revival is not None:
        chain = [x for x, y in zip(chain, was_revival) if not y]
    title = chain[-1].title
    digits = title.split("|")[-1].strip()
    corner = sum([(-4)**idx * int(digit) for idx, digit in enumerate(digits[::-2])])
    return (2 * corner + 1)**2


def update_dates(count, chain, was_revival=None):
    if was_revival is not None:
        chain = [x for x, y in zip(chain, was_revival) if not y]
    chain = chain[:-1]
    regex = r"([,\d]+)$"  # All digits at the end of the line, plus optional separators
    for submission in chain:
        year = int(re.search(regex, submission.title).group().replace(",", ""))
        length = 1095 + any(map(is_leap_year, range(year, year + 3)))
        count += length
    return count


def update_from_traversal(old_count, chain, was_revival):
    new_thread = chain[-1]
    count = old_count
    for thread in chain[:-1][::-1]:
        try:
            urls = filter(lambda x: x[0] == thread.id,
                          parsing.find_urls_in_text(new_thread.selftext))
            submission_id, comment_id = next(urls)
        except StopIteration:
            return None
        tree = fetch_comment_tree(thread)
        count += len(tree.comment(comment_id).walk_up_tree())
        new_thread = thread
    return count


class SideThread():
    def __init__(self, rule=default_rule, form=permissive, length=1000,
                 update_function=None):
        self.form = form
        self.rule = rule
        self.length = length
        if update_function is not None:
            self.update_count = update_function
        else:
            self.update_count = self.update_from_length

    def update_from_length(self, old_count, chain, was_revival=None):
        if was_revival is not None:
            chain = [x for x, y in zip(chain[1:], was_revival[1:]) if not y]
        else:
            chain = chain[1:]
        if self.length is not None:
            return old_count + self.length * (len(chain))
        else:
            return None

    def is_valid_thread(self, history):
        mask = self.rule.is_valid(history)
        if mask.all():
            return (True, '')
        else:
            return (False, history.loc[~mask, 'comment_id'].iloc[0])

    def is_valid_count(self, comment, history):
        history = history.append(comment_to_dict(comment), ignore_index=True)
        valid_history = self.is_valid_thread(history)[0]
        valid_count = self.looks_like_count(comment)
        valid_user = str(comment.author).lower() not in ignored_users
        return valid_history and valid_count and valid_user, history

    def get_history(self, comment):
        """Fetch enough previous comments to be able to determine whether replies to
        `comment` are valid according to the side thread rules.
        """
        return self.rule.get_history(comment)

    def looks_like_count(self, comment):
        return comment.body in deleted_phrases or self.form(comment.body)


known_threads = {
    'roman': SideThread(form=roman_numeral),
    'balanced ternary': SideThread(form=balanced_ternary, length=729),
    'base 16 roman': SideThread(form=roman_numeral),
    'binary encoded hexadecimal': SideThread(form=base_n(2), length=1024),
    'binary encoded decimal': SideThread(form=base_n(2)),
    'base 2i': SideThread(form=base_n(4), update_function=update_2i),
    'bijective base 2': SideThread(form=base_n(3), length=1024),
    'cyclical bases': SideThread(form=base_n(16)),
    'wait 2': SideThread(form=base_10, rule=CountingRule(wait_n=2)),
    'wait 2 - letters': SideThread(rule=CountingRule(wait_n=2)),
    'wait 3': SideThread(form=base_10, rule=CountingRule(wait_n=3)),
    'wait 10': SideThread(form=base_10, rule=CountingRule(wait_n=10)),
    'once per thread': SideThread(form=base_10, rule=CountingRule(wait_n=None)),
    'slow': SideThread(form=base_10, rule=CountingRule(thread_time=minute)),
    'slower': SideThread(form=base_10, rule=CountingRule(user_time=hour)),
    'slowestest': SideThread(form=base_10, rule=CountingRule(thread_time=hour, user_time=day)),
    'unicode': SideThread(form=base_n(16), length=1024),
    'valid brainfuck programs': SideThread(form=brainfuck),
    'only double counting': SideThread(form=base_10, rule=OnlyDoubleCounting()),
    'mayan numerals': SideThread(length=800, form=mayan_form),
    'reddit usernames': SideThread(length=722, form=reddit_username_form),
    'twitter handles': SideThread(length=1369, form=twitter_form),
    'wave': SideThread(form=base_10, update_function=update_wave),
    'increasing sequences': SideThread(form=base_10, update_function=update_increasing_type(1)),
    'double increasing': SideThread(form=base_10, update_function=update_increasing_type(2)),
    'triple increasing': SideThread(form=base_10, update_function=update_increasing_type(3)),
    'dates': SideThread(form=base_10, update_function=update_dates),
    'invisible numbers': SideThread(form=base_n(10, strip_links=False)),
    'parentheses': SideThread(form=parentheses_form),
    'dollars and cents': SideThread(form=base_n(4)),
    'throwaways': SideThread(form=throwaway_form),
    'by 3s in base 7': SideThread(form=base_n(7)),
    'unary': SideThread(form=validate_from_character_list("|")),
    'four fours': SideThread(form=validate_from_character_list("4")),
    'using 12345': SideThread(form=validate_from_character_list("12345")),
    'japanese': SideThread(form=validate_from_character_list("一二三四五六七八九十百千")),
    'roman progressbar': SideThread(form=roman_numeral),
    'symbols': SideThread(form=validate_from_character_list("!@#$%^&*()")),
    '2d20 experimental v theoretical': SideThread(form=d20_form),
}

base_n_lengths = [None,
                  1000, 1024,  729, 1024, 1000,  # noqa E241
                  1296, 1029, 1024,  729, 1000,  # noqa E241
                  1000,  864, 1014, None, None,  # noqa E241
                  1024, 1156, None, None,  800,  # noqa E241
                   882, None, None, None, None,  # noqa E127
                  None,  729, None, None, None,  # noqa E241
                  None, None, None, None, None,
                  1296]

base_n_threads = {f'base {i}': SideThread(form=base_n(i), length=length)
                  for i, length in enumerate(base_n_lengths) if length is not None}
known_threads.update(base_n_threads)

# See: https://www.reddit.com/r/counting/comments/o7ko8r/free_talk_friday_304/h3c7433/?context=3

default_threads = ['decimal', 'age', 'palindromes', 'rational numbers',
                   'n read as base n number', 'by 8s', 'by 69s', 'powers of 2',
                   'california license plates', 'by 0.02s', 'by 2s even', 'by one-hundredths',
                   'by 2s odd', 'by 3s', 'by 4s', 'by 5s', 'by 7s', 'by 8s',
                   'by 10s', 'by 12s', 'by 20s', 'by 23s', 'by 29s', 'by 40s', 'by 50s', 'by 64s',
                   'by 99s', 'by 123s', 'by meters', 'negative numbers', 'previous dates',
                   'prime factorization', 'scientific notation', 'street view counting',
                   '3 or fewer palindromes', 'four squares', '69, 420, or 666',
                   'all even or all odd', 'no consecutive digits', 'unordered consecutive digits',
                   'prime numbers', 'triangular numbers', 'thread completion', 'sheep',
                   'top subreddits', 'william the conqueror', '10 at a time']
known_threads.update({thread_name: SideThread(form=base_10, length=1000)
                      for thread_name in default_threads})

default_threads = {
    'no repeating digits': 840,
    'time': 900,
    'permutations': 720,
    'factoradic': 720,
    'seconds minutes hours': 1200,
    'feet and inches': 600,
    'lucas numbers': 200,
    'hoi4 states': 806,
    'eban': 800,
    'ipv4': 1024,
}
known_threads.update({key: SideThread(form=base_10, length=length)
                      for key, length in default_threads.items()})


no_validation = {
    'base 40': 1600,
    'base 60': 900,
    'base 62': 992,
    'base 64': 1024,
    'base 93': 930,
    'youtube': 1024,
    'previous_dates': None,
    'qwerty alphabet': 676,
    'acronyms': 676,
    'letters': 676,
    'palindromes - letters': 676,
    'cards': 676,
    'musical notes': 1008,
    'octal letter stack': 1024,
    'planetary octal': 1024,
    'permutations - letters': None,
    'iterate each letter': None}

known_threads.update({k: SideThread(length=v) for k, v in no_validation.items()})

default_thread_varying_length = [
    'tug of war',
    'by day of the week',
    'by day of the year',
    'by gme increase/decrease',
    'by length of username',
    'by number of post upvotes',
    'by random number',
    'by digits in total karma',
    'by timestamp seconds',
    'comment karma',
    'post karma',
    'total karma',
    'nim',
    'pick from five',
    '2d tug of war',
    'boost 5',
    'by random number (1-1000)'
]

default_thread_unknown_length = [
    'only repeating digits',
    'rotational symmetry',
    'base of previous digit',
    'no successive digits',
    'rotational symmetry',
    'collatz conjecture',
    'powerball',
    'by number of digits squared',
    'by list size',
    'divisors'
]


def get_side_thread(thread_name, verbosity=1):
    """Return the properties of the side thread with first post thread_id"""
    if thread_name in known_threads:
        return known_threads[thread_name]
    elif thread_name in default_thread_unknown_length:
        return SideThread(length=None, form=base_10)
    elif thread_name in default_thread_varying_length:
        return SideThread(update_function=update_from_traversal, form=base_10)
    else:
        if verbosity > 0 and thread_name != 'default':
            print(f'No rule found for {thread_name}. '
                  'Not validating comment contents. '
                  'Assuming n=1000 and no double counting.')
        return SideThread()
