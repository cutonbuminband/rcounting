import pandas as pd
from parsing import find_count_in_text
from thread_navigation import walk_up_thread
from models import comment_to_dict

minute = 60
hour = 60 * 60
day = 60 * 60 * 24


class CountingRule():
    def __init__(self, wait_n=1, thread_time=0, user_time=0):
        self.n = wait_n
        self.thread_time = thread_time
        self.user_time = user_time

    def valid_skip(self, history):
        skip_counts = history.groupby('username')['running_index'].diff()
        if self.n is not None:
            valid_skip = skip_counts.isna() | (skip_counts > self.n)
        else:
            valid_skip = skip_counts.isna()
        return valid_skip.all()

    def valid_thread_time(self, history):
        if not self.thread_time:
            return True
        elapsed_time = history['timestamp'].diff()
        valid_time = elapsed_time.isna() | (elapsed_time >= self.thread_time)
        return valid_time.all()

    def valid_user_time(self, history):
        if not self.user_time:
            return True
        elapsed_user_time = history.groupby('username')['timestamp'].diff()
        valid_user_time = elapsed_user_time.isna() | (elapsed_user_time >= self.user_time)
        return valid_user_time.all()

    def is_valid(self, history):
        return (self.is_valid_skip(history)
                & self.is_valid_thread_time(history)
                & self.is_valid_user_time(history))

    def get_history(self, comment):
        comments = walk_up_thread(comment, max_comments=self.n)
        return pd.DataFrame([comment_to_dict(x) for x in comments])


class OnlyDoubleCounting():
    def is_valid(self, history):
        down_shift = history.username.iloc[::2] == history.username.shift().iloc[::2]
        up_shift = history.username.iloc[::2] == history.username.shift(-1).iloc[::2]
        return (up_shift.isna() | up_shift).all() or (down_shift.isna() | down_shift).all()

    def get_history(self, comment):
        comments = walk_up_thread(comment, max_comments=2)
        return pd.DataFrame([comment_to_dict(x) for x in comments])


def base_n(n=10):
    def looks_like_count(comment_body):
        try:
            find_count_in_text(comment_body, n)
            return True
        except ValueError:
            return False
    return looks_like_count


def permissive(comment):
    return True


def balanced_ternary(comment_body):
    translated_body = comment_body.translate(str.maketrans('tT-0+', '00012'))
    return base_n(3)(translated_body)


def brainfuck(comment_body):
    dictionary = str.maketrans('><+-.,[]', '01234567')
    return base_n(8)(comment_body.translate(dictionary))


def roman_numeral(comment_body):
    trans = str.maketrans('IVXLCDMↁↂↇ', '0123456789', '0123456789')
    translated_body = comment_body.upper().translate(trans)
    return base_n(10)(translated_body)


base_10 = base_n(10)
default_rule = CountingRule()


class SideThread():
    def __init__(self, rule=default_rule, form=base_10, length=1000):
        self.counting_form = form
        self.counting_rule = rule
        self.thread_length = length

    def is_valid(self, history):
        return self.counting_rule.is_valid(history)

    def get_history(self, comment):
        self.history = self.counting_rule.get_history(comment)

    def looks_like_count(self, comment):
        return self.counting_form(comment.body)

    def update_history(self, comment):
        self.history = self.history.append(comment_to_dict(comment), ignore_index=True)


known_threads = {
    'no repeating digits': SideThread(length=840),
    'time': SideThread(length=900),
    'roman': SideThread(form=roman_numeral),
    'balanced ternary': SideThread(form=balanced_ternary, length=729),
    'base 16 roman': SideThread(form=roman_numeral),
    'binary encoded hexadecimal': SideThread(form=base_n(2), length=1024),
    'binary encoded decimal': SideThread(form=base_n(2)),
    'base 2i': SideThread(form=base_n(4), length=None),
    'feet and inches': SideThread(length=600),
    'bijective base 2': SideThread(form=base_n(3), length=1024),
    'cyclical bases': SideThread(form=base_n(16)),
    'wait 2': SideThread(rule=CountingRule(wait_n=2)),
    'wait 2 - letters': SideThread(rule=CountingRule(wait_n=2),
                                   form=permissive),
    'wait 3': SideThread(rule=CountingRule(wait_n=3)),
    'wait 10': SideThread(rule=CountingRule(wait_n=10)),
    'once per thread': SideThread(rule=CountingRule(wait_n=None)),
    'slow': SideThread(rule=CountingRule(thread_time=minute)),
    'slower': SideThread(CountingRule(user_time=hour)),
    'slowestest': SideThread(CountingRule(thread_time=hour, user_time=day)),
    'unicode': SideThread(form=base_n(16), length=1024),
    'lucas_numbers': SideThread(length=200),
    'valid brainfuck': SideThread(form=brainfuck),
    'hoi4 states': SideThread(length=806),
    'only double counting': SideThread(rule=OnlyDoubleCounting()),
    'seconds minutes hours': SideThread(length=1200)
}

base_n_lengths = [None,
                  None, 1024,  729, 1024, 1000,  # noqa E241
                  1296, 1029, 1024,  729, 1000,  # noqa E241
                  1000,  864, 1014, None, None,  # noqa E241
                  1024, 1156, None, None,  800,  # noqa E241
                   882, None, None, None, None,  # noqa E127
                  None,  729, None, None, None,  # noqa E241
                  None, None, None, None, None,
                  1296]

for i in range(2, 37):
    if base_n_lengths[i] is None:
        continue
    known_threads.update({f'base {i}': SideThread(form=base_n(i), length=base_n_lengths[i])})


permissive_threads = {'base 40': 1600,
                      'base 51': 1000,
                      'base 60': 900,
                      'base 62': 992,
                      'base 64': 1024,
                      'base 93': 930,
                      'base 100': 1000,
                      'base 187': 1000,
                      'youtube': 1024,
                      'dates': None,
                      'previous_dates': None,
                      'writing numbers': 1000,
                      'mississippi': 1000,
                      'qwerty alphabet': 676,
                      'acronyms': 676,
                      'letters': 676,
                      'palindromes - letters': 676,
                      'four fours': 1000,
                      'mayan numerals': 800,
                      'top subreddits': 1000,
                      'reddit usernames': 722,
                      'twitter handles': 1369,
                      'bijective base 69': 1000,
                      'cards': 676,
                      'musical notes': 1008,
                      'octal letter stack': 1024,
                      'planetary octal': 1024,
                      'by 3s in base 7': 1000,
                      'chess matches': 1000,
                      'permutations - letters': None,
                      'rickroll base 5': 1000,
                      'iterate each letter': None}

for thread in permissive_threads:
    known_threads.update(thread=SideThread(form=permissive,
                                           length=permissive_threads[thread]))

default_thread_unknown_length = ['wave', 'palindromes', 'only repeating digits',
                                 'permutations', 'factoradic', 'tug_of_war', 'by day of the week',
                                 'by day of the year', 'by gme increase/decrease %',
                                 'by length of username', 'by number of post upvotes',
                                 'by random number', 'by digits in total karma',
                                 'by timestamp seconds', 'comment karma', 'post karma',
                                 'total karma', 'nim', 'pick from five', 'rotational symmetry',
                                 'base of previous digit', 'eban', 'no successive digits',
                                 'rotational symmetry', 'collatz conjecture',
                                 'double increasing sequences', 'increasing sequences',
                                 'triple increasing sequences', 'powerball', '2d tug of war',
                                 'boost 5', 'by number of digits squared', 'by list size']


def get_side_thread(thread_name):
    """Return the properties of the side thread with first post thread_id"""
    if thread_name in default_thread_unknown_length:
        return SideThread(length=None)
    elif thread_name not in known_threads:
        return SideThread()
    else:
        return known_threads[thread_name]
