import string
from typing import Iterable

from rcounting import parsing

alphanumeric = string.digits + string.ascii_lowercase


def validate_from_tokens(valid_tokens: str | Iterable[str], strip_links=True):
    def looks_like_count(comment_body: str) -> bool:
        body = comment_body.lower()
        if strip_links:
            body = parsing.strip_markdown_links(body)
        return any(character.lower() in body for character in valid_tokens)

    return looks_like_count


def base_n(n=10, strip_links=True):
    return validate_from_tokens(alphanumeric[:n], strip_links)


def permissive(_: str) -> bool:
    return True
