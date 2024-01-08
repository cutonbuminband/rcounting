import functools
from typing import Callable, Iterable, Mapping

from fuzzywuzzy import fuzz

from rcounting import parsing


def by_ns_count(n):
    def comment_to_count(comment):
        count = parsing.find_count_in_text(comment)
        return int(count // n)

    return comment_to_count


def base_n_count(n: int):
    def comment_to_count(comment_body: str):
        return parsing.find_count_in_text(comment_body, base=n)

    return comment_to_count


def fuzzy_tokenize(comment_body, tokens, ignored_chars=">", threshold=80):
    line = comment_body.split("\n")[0]
    line = "".join(char for char in line if char not in ignored_chars)
    words = line.lower().strip().split()
    candidates = [max((fuzz.ratio(token, word), token) for token in tokens) for word in words]
    values = []
    for candidate in candidates:
        if candidate[0] < threshold:
            break
        values.append(candidate[1])
    return values


def count_from_word_list(
    comment_body: str,
    alphabet: str | Iterable[str] | Mapping[str, int] = "0123456789",
    tokenize: Callable[[str, list[str]], list[str]] = fuzzy_tokenize,
    bijective=False,
    **kwargs
) -> int:
    if not isinstance(alphabet, dict):
        alphabet = {k: p + int(bijective) for p, k in enumerate(alphabet)}
    base = len(set(alphabet.values()))
    tokens = tokenize(comment_body, list(alphabet.keys()), **kwargs)
    values = [alphabet[token] for token in tokens]
    return functools.reduce(lambda x, y: base * x + y, values, 0)
