from math import ceil, floor
from typing import Callable

from rcounting import parsing

from .forms import CommentType
from .validate_count import count_from_token_list, fuzzy_tokenize
from .validate_form import alphanumeric, validate_from_tokens


class BaseNType(CommentType):
    def __init__(
        self,
        base=None,
        tokens: str | list[str] | None = None,
        bijective=False,
        tokenizer: Callable[[str, list[str]], list[str]] = fuzzy_tokenize,
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
            tokenize=self.tokenizer,
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
