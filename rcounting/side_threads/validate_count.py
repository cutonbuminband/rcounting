from typing import Iterable, Mapping

from fuzzywuzzy import fuzz

from rcounting import parsing


def by_ns_count(n):
    def comment_to_count(comment):
        count = parsing.find_count_in_text(comment)
        return int(count // n)

    return comment_to_count


def base_n_count(n: int):
    def comment_to_count(comment):
        return parsing.find_count_in_text(comment, base=n)

    return comment_to_count


def count_from_word_list(
    comment_body: str,
    alphabet: str | Iterable[str] | Mapping[str, int] = "0123456789",
    base: int = 10,
    ignored_chars: str = ">",
    threshold: int = 80,
) -> int:
    if not isinstance(alphabet, dict):
        alphabet = {k: p for p, k in enumerate(alphabet)}
    line = comment_body.split("\n")[0]
    line = "".join(char for char in line if char not in ignored_chars)
    words = line.lower().strip().split()
    candidates = [max((fuzz.ratio(key, word), key) for key in alphabet) for word in words]
    s = ""
    for candidate in candidates:
        if candidate[0] < threshold:
            break
        s += str(alphabet[candidate[1]])
    return int(s, base)
