import pandas as pd
from models import comment_to_dict
from string import digits, ascii_uppercase

minute = 60
hour = 60 * 60
day = 60 * 60 * 24


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
        if getattr(comment, 'is_root', False):
            return pd.DataFrame([])
        comments = comment.traverse(limit=self.n)
        max_time = max(self.thread_time, self.user_time)
        while not (comments[-1].is_root
                   or (comment.created_utc - comments[-1].created_utc) >= max_time):
            comments = comments[:-1] + comments[-1].traverse(limit=9)
        return pd.DataFrame([comment_to_dict(x) for x in comments[::-1]])


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
        comments = comment.traverse(limit=2)[::-1]
        return pd.DataFrame([comment_to_dict(x) for x in comments])


def validate_from_character_list(valid_characters):
    def looks_like_count(comment_body):
        body = comment_body.upper()
        return any([character in body for character in valid_characters])
    return looks_like_count


def base_n(n=10):
    alphanumeric = digits + ascii_uppercase
    return validate_from_character_list(alphanumeric[:n])


def permissive(comment):
    return True


balanced_ternary = validate_from_character_list('T-0+')
brainfuck = validate_from_character_list('><+-.,[]')
roman_numeral = validate_from_character_list('IVXLCDMↁↂↇ')
mayan_form = validate_from_character_list('Ø1234|-')
twitter_form = validate_from_character_list('@')

def reddit_username_form(comment_body):
    return 'u/' in comment_body


base_10 = base_n(10)
default_rule = CountingRule()


class SideThread():
    def __init__(self, rule=default_rule, form=base_10, length=1000):
        self.form = form
        self.rule = rule
        self.thread_length = length

    def update_count(self, count, threads):
        if self.thread_length is not None:
            return count + self.thread_length * (len(threads) - 1)
        else:
            return None

    def is_valid_thread(self, history):
        mask = self.rule.is_valid(history)
        if mask.all():
            return (True, '')
        else:
            return (False, history.loc[~mask, 'comment_id'].iloc[0])

    def is_valid_count(self, comment):
        history = self.history.append(comment_to_dict(comment), ignore_index=True)
        return self.is_valid_thread(history)[0] and self.looks_like_count(comment)

    def get_history(self, comment):
        self.history = self.rule.get_history(comment)

    def looks_like_count(self, comment):
        return self.form(comment.body)

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
    'valid brainfuck programs': SideThread(form=brainfuck),
    'hoi4 states': SideThread(length=806),
    'only double counting': SideThread(rule=OnlyDoubleCounting()),
    'seconds minutes hours': SideThread(length=1200),
    'mayan numerals': SideThread(length=800, form=mayan_form),
    'reddit usernames': SideThread(length=722, form=reddit_username_form),
    'twitter handles': SideThread(length=1369, form=twitter_form)
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

base_n_threads = {'base i': SideThread(form=base_n(i), length=length)
                  for i, length in enumerate(base_n_lengths) if length is not None}
known_threads.update(base_n_threads)


no_validation = {'base 40': 1600,
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
                 'top subreddits': 1000,
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

known_threads.update({k: SideThread(form=permissive, length=v) for k, v in no_validation.items()})

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
