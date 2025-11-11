import functools
from math import ceil, floor

from rcounting import parsing

from .side_threads import SideThread
from .validate_count import count_from_token_list, fuzzy_tokenize
from .validate_form import alphanumeric, validate_from_tokens


class BaseNThread(SideThread):
    def __init__(
        self,
        base=None,
        tokens: str | list[str] | None = None,
        bijective=False,
        tokenizer=fuzzy_tokenize,
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

        self.mapping = tokens
        self.reverse_mapping = {
            idx + int(bijective): symbol for idx, symbol in enumerate(self.mapping)
        }
        self.bijective = bijective
        self.form = functools.partial(validate_from_tokens, valid_tokens=tokens)
        self.base = len(tokens)
        if simple:

            def simple_tokenizer(comment_body, _):
                return list(
                    parsing.extract_count_string(comment_body, base=self.base, bijective=bijective)
                )

            self.tokenizer = simple_tokenizer
        else:
            self.tokenizer = tokenizer
        max_len = max(len(x) for x in self.mapping)
        if separator is None:
            self.separator = " " if max_len > 1 else ""
        else:
            self.separator = separator

    def comment_to_count(self, comment_body):
        return count_from_token_list(
            comment_body,
            alphabet=self.mapping,
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
