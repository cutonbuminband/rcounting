import itertools
from collections.abc import Iterable, Iterator
from datetime import timedelta

from rcounting.units import HOUR, MINUTE


def flatten(mylist: Iterable[Iterable]) -> list:
    return [element for sublist in mylist for element in sublist]


def partition(mylist: list, indices: list[int]) -> list[list]:
    return [mylist[i:j] for i, j in zip([0] + indices, indices + [None])]


def is_leap_year(n: int) -> bool:
    return n % 4 == 0 and (n % 400 == 0 or n % 100 != 0)


def format_timedelta(delta_t: timedelta) -> str:
    def format_one_interval(n, unit):
        if n == 0:
            return ""
        return f"{n} {unit}{'s' if n > 1 else ''}"

    days = delta_t.days
    hours, rem = divmod(delta_t.seconds, HOUR)
    minutes, seconds = divmod(rem, MINUTE)
    amounts = [days, hours, minutes, seconds]
    units = ["day", "hour", "minute", "second"]
    formatted = [format_one_interval(n, unit) for n, unit in zip(amounts, units)]
    return ", ".join([x for x in formatted if x])


deleted_phrases = ["[deleted]", "[removed]", "[banned]"]


def chunked(iterable: Iterable, n: int) -> Iterator:
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)
