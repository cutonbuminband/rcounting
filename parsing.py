import re

def find_count_in_text(body):
    try:
        regex = ("^[^\d]*"    # We strip non-numeric characters from the start
                 "([\d, \.]*)")  # And then we take all digits and separators
        count = re.findall(regex, body)[0]
        # We remove any separators, and try to convert the remainder to an int.
        stripped_count = count.translate(str.maketrans('', '', ' ,.'))
        return int(stripped_count)
    except:
        raise ValueError(f"Unable to extract count from comment body: {body}")


def find_count_in_text(body):
    try:
        regex = ("^[^\d]*"    # We strip non-numeric characters from the start
                 "([\d, \.]*)")  # And then we take all digits and separators
        count = re.findall(regex, body)[0]
        # We remove any separators, and try to convert the remainder to an int.
        stripped_count = count.translate(str.maketrans('', '', ' ,.'))
        return int(stripped_count)
    except:
        raise ValueError(f"Unable to extract count from comment body: {body}")

def find_urls_in_text(body):
    # reddit lets you link to comments and posts with just /comments/stuff,
    # so everything before that is optional. We only capture the bit after
    # r/counting/comments, since that's what has the information we are
    # interested in.
    url_regex = ("(?:www\.)?"
                 "(?:old\.)?"
                 "(?:reddit\.com)?"
                 "(?:/r/counting)?"
                 "(/comments/[^ )\n?]+)")
    urls = re.findall(url_regex, body)
    return urls

