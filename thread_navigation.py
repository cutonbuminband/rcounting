from praw.models import MoreComments
from praw.exceptions import ClientException
from parsing import find_urls_in_text, find_count_in_text


def find_previous_get(get_id, reddit_instance):
    comment = reddit_instance.comment(get_id)
    thread = comment.submission
    urls = find_urls_in_text(thread.selftext)
    if not urls:
        urls = [find_urls_in_text(comment.body) for comment in thread.comments]
        urls = [url for comment_urls in urls for url in comment_urls]
    new_thread_id, title_url, new_get_id = urls[0]
    comment = reddit_instance.comment(new_get_id)
    new_get = find_get_from_comment(comment)
    print(new_get.submission, new_get.id)
    return new_get


def find_get_from_comment(comment):
    try:
        count = find_count_in_text(comment.body)
    except ValueError:
        # Maybe somebody linked the gz instead of the get. Let's try one more
        # time.
        comment = comment.parent()
        count = find_count_in_text(comment.body)
    comment.refresh()

    while count % 1000 != 0:
        comment = comment.replies[0]
        if isinstance(comment, MoreComments):
            comment = comment.comments()[0]
        count = find_count_in_text(comment.body)
    return comment


def extract_gets_and_assists(comment, reddit_instance, n_threads=1000):
    gets = []
    assists = []
    comment.refresh()
    for n in range(n_threads):
        rows = []
        for i in range(3):
            rows.append(comment_to_tuple(comment))
            comment = comment.parent()
        gets.append(rows[0] + (rows[0][2] - rows[1][2],))
        assists.append(rows[1] + (rows[1][2] - rows[2][2],))
        comment = find_previous_get(comment, reddit_instance)
    return gets, assists


def comment_to_tuple(comment):
    author = str(comment.author) if comment.author is not None else "[deleted]"
    return (comment.body, author,
            comment.created_utc, comment.id, str(comment.submission))


def walk_thread(leaf_comment):
    "Return a list of comments in a reddit thread from leaf to root"
    comments = []
    refresh_counter = 0
    comment = leaf_comment
    refresh_counter = 0
    while not comment.is_root:
        # By refreshing, we request the previous 9 comments in a single go. So
        # the next calls to .parent() and comment_to_tuple() don't result in a
        # network request. If the refresh fails, we just go one by one
        if refresh_counter % 9 == 0:
            try:
                comment.refresh()
                print(comment.id)
            except ClientException:
                print(f"Broken chain detected at {comment.id}")
                print("Fetching the next 9 comments one by one")
        comments.append(comment_to_tuple(comment))
        comment = comment.parent()
        refresh_counter += 1
    # We need to include the root comment as well
    comments.append(comment_to_tuple(comment))
    return comments
