"""A collection of functions for parsing texts and extracting urls and counts"""
import re


def find_body(post):
    return post.body if hasattr(post, "body") else post.selftext


def find_count_in_text(body, base=10, raise_exceptions=True):
    """
    Extract an integer from a textual representation of a count in base n.
    Try to account for various strategies for separating thousands digits.
    """
    characters = "0123456789abcdefghijklmnopqrstuvwxyz"[:base]
    separators = "' ,.*/"  # non-whitespace separators people have used
    first_line = "".join(normalize_comment(body).split()).translate(
        str.maketrans("", "", separators)
    )

    match = re.search(f"[{characters}]+", first_line.lower())
    if match is not None:
        return int(match.group(), base)

    if raise_exceptions:
        raise ValueError(f"Unable to extract count in base {base} from comment body: {body}")
    return float("nan")


def find_urls_in_text(body):
    """Extract all substrings of a string that look like '/comments/id/stuff/id'"""
    # reddit lets you link to comments and posts with just /comments/stuff,
    # so everything before that is optional. We only capture the bit after
    # r/counting/comments, since that's what has the information we are
    # interested in.
    url_regex = r"/comments/([\w]+)(?:/[^/]*/([\w]*)|)"
    urls = re.findall(url_regex, body)
    return urls


def post_to_count(reddit_post):
    """Extract an integer from a reddit submission or comment"""
    return find_count_in_text(find_body(reddit_post))


def post_to_urls(reddit_post):
    """Find urls in a reddit submission or comment"""
    return find_urls_in_text(find_body(reddit_post))


def parse_markdown_links(body):
    """
    Find markdown links of the form [description](link) in a string.
    Correctly handling escaped backslashes in the link portion
    """
    regex = r"\[(.*?)\]\((.+?(?<!\\))\)"
    links = re.findall(regex, body)
    return links


def strip_markdown_links(body):
    """Replace all markdown links of the form [description](link) with description."""
    regex = r"\[(.*?)\]\((.+?(?<!\\))\)"
    replacement = r"\1"
    return re.sub(regex, replacement, body)


def body_from_title(title):
    return "|".join(title.split("|")[1:])


def normalize_comment(comment):
    first_line = comment.split("\n")[0]
    no_links = strip_markdown_links(first_line)
    return no_links


def parse_directory_page(directory_page):
    """Tag each paragraph of a directory page by whether it represents text or a table."""
    paragraphs = directory_page.split("\n\n")
    regex = r"^.*\|.*\|.*$"
    tagged_results = []
    text = []
    for paragraph in paragraphs:
        lines = [line for line in paragraph.split("\n") if line]
        mask = all(bool(re.match(regex, line)) for line in lines)
        if not mask:
            text.append(paragraph)
        else:
            tagged_results.append(["text", "\n\n".join(text)])
            text = []
            rows = [parse_row(row) for row in lines[2:]]
            tagged_results.append(["table", rows])
    if text:
        tagged_results.append(["text", "\n\n".join(text)])
    return tagged_results


def parse_row(markdown_row):
    """Extract the side thread attributes from a row in a markdown table"""
    first, current, count = markdown_row.split("|")
    name, first_submission = parse_markdown_links(first)[0]
    name = name.strip()
    first_submission_id = first_submission.strip()[1:]
    title, link = parse_markdown_links(current)[0]
    title = title.strip()
    submission_id, comment_id = find_urls_in_text(link)[0]
    comment_id = None if not comment_id else comment_id
    count = count.strip()
    return name, first_submission_id, title, submission_id, comment_id, count


def find_urls_in_submission(submission):
    """Extract urls from both the body of the submission and the top-level comments"""
    # Get everything that looks like a url in the body of the post
    yield from find_urls_in_text(submission.selftext)
    # And then in every top-level comment
    for comment in submission.comments:
        yield from find_urls_in_text(comment.body)


def is_revived(title):
    """Determine whether the title indicates that a submission is a revival"""
    regex = r"\(*reviv\w*\)*"
    return re.search(regex, title.lower())


def name_sort(name):
    """Intelligent sorting for strings mixed with digits"""
    title = name.translate(str.maketrans("", "", "'\"()^/*")).lower()
    return tuple(int(c) if c.isdigit() else c for c in re.split(r"(\d+)", title))


def normalise_title(title):
    """
    Normalise a string for posting to the directory

    - Literal pipes are replaced by an equivalent character, to avoid messing up tables
    - The format for indicating revivals is standardised.
    """
    title = title.translate(str.maketrans("[]", "()"))
    title = title.replace("|", "&#124;")
    revived = is_revived(title)
    if revived:
        start, end = revived.span()
        return title[:start] + "(Revival)" + title[end:]
    return title
