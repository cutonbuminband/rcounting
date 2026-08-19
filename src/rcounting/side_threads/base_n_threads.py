import functools
from collections.abc import Mapping
from math import ceil, floor
from typing import Callable, Iterable

from fuzzywuzzy import fuzz

from rcounting import parsing

from .forms import CommentType
from .validate_form import alphanumeric, validate_from_tokens


def fuzzy_tokenizer(comment_body, tokens, ignored_chars=">", threshold=80):
    line = comment_body.split("\n")[0]
    line = "".join(char for char in line if char not in ignored_chars)
    words = line.lower().strip().split()
    prefixes = list(set(y[0] for token in tokens if len(y := token.split()) > 1))
    complete_tokens = [token for token in tokens if len(token.split()) == 1]
    i = 0
    values = []
    while i < len(words):
        candidate = max(
            (fuzz.ratio(token, words[i]), token) for token in prefixes + complete_tokens
        )
        if candidate[1] in prefixes:
            if i + 1 == len(words):
                break
            candidate = max(
                (fuzz.ratio(token, " ".join(words[i : i + 2])), token) for token in tokens
            )
            i += 1
        if candidate[0] < threshold:
            break
        values.append(candidate[1])
        i += 1
    return values


def count_from_token_list(
    comment_body: str,
    alphabet: str | Iterable[str] | Mapping[str, int] = "0123456789",
    tokenizer: Callable[[str, list[str]], list[str]] = fuzzy_tokenizer,
    bijective=False,
    **kwargs,
) -> int:
    if not isinstance(alphabet, dict):
        alphabet = {k: p + int(bijective) for p, k in enumerate(alphabet)}
    base = len(set(alphabet.values()))
    tokens = tokenizer(comment_body, list(alphabet.keys()), **kwargs)
    values = [alphabet[token] for token in tokens]
    return functools.reduce(lambda x, y: base * x + y, values, 0)


class BaseN(CommentType):
    def __init__(
        self,
        base=None,
        tokens: str | list[str] | None = None,
        bijective=False,
        tokenizer: Callable[[str, list[str]], list[str]] = fuzzy_tokenizer,
        separator=None,
    ):
        super().__init__()
        simple = False
        if base is None:
            assert tokens is not None, "Either a base or a list/dict of tokens must be supplied"
        elif tokens is not None:
            assert (
                base == len(tokens)
            ), "If you supply both a base and a list of tokens, the length of the token list has to match the base"
        else:
            tokens = list(alphanumeric[int(bijective) : base + int(bijective)])
            simple = True

        self.tokens = tokens
        self.reverse_mapping = {
            idx + int(bijective): symbol for idx, symbol in enumerate(self.tokens)
        }
        self.bijective = bijective
        self.form = validate_from_tokens(self.tokens)
        self.base = len(tokens)
        if simple:

            def simple_tokenizer(comment_body, _):
                return list(
                    parsing.extract_count_string(comment_body, base=self.base, bijective=bijective)
                )

            self.tokenizer = simple_tokenizer
        else:
            self.tokenizer = tokenizer
        max_len = max(len(x) for x in self.tokens)
        if separator is None:
            self.separator = " " if max_len > 1 else ""
        else:
            self.separator = separator
        self.comment_to_count = self.base_n_count

    def base_n_count(self, comment_body):
        return count_from_token_list(
            comment_body,
            alphabet=self.tokens,
            bijective=self.bijective,
            tokenizer=self.tokenizer,
        )

    def count_to_comment(self, count: int) -> str:
        if self.bijective:
            f = lambda x: int(ceil(x)) - 1  # noqa
        else:
            f = lambda x: int(floor(x))  # noqa

        result = []
        while count:
            result.append(count - self.base * f(count / self.base))
            count = f(count / self.base)
        result = result[::-1]
        return self.separator.join(self.reverse_mapping[x] for x in result)
