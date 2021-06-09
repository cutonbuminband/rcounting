from psaw import PushshiftAPI
from praw.models import MoreComments
from praw.exceptions import ClientException
from prawcore.exceptions import ServerError
from parsing import post_to_urls, post_to_count
from utils import chunked
from models import Comment as OfflineComment, CommentTree

api = PushshiftAPI()


def find_previous_get(comment):
    reddit = comment._reddit
    thread = comment.submission
    urls = post_to_urls(thread)
    if not urls:
        urls = [post_to_urls(comment) for comment in thread.comments]
        urls = [url for comment_urls in urls for url in comment_urls]
    new_thread_id, title_url, new_get_id = urls[0]
    if not new_get_id:
        new_get_id = find_get_in_thread(new_thread_id, reddit)
    comment = reddit.comment(new_get_id)
    new_get = find_get_from_comment(comment)
    print(new_get.submission, new_get.id)
    return new_get


def find_get_in_thread(thread, reddit):
    "Find the get based on thread id"
    comment_ids = api._get_submission_comment_ids(thread.id)[::-1]
    for comment_id in comment_ids[:-1]:
        comment = reddit.comment(comment_id)
        try:
            count = post_to_count(comment)
            if count % 1000 == 0:
                return comment.id
        except ValueError:
            continue
    raise ValueError(f"Unable to locate get in thread {thread.id}")


def search_up_from_gz(comment, max_retries=5):
    "Find a count up to max_retries above the linked_comment"
    for i in range(max_retries):
        try:
            count = post_to_count(comment)
            return count, comment
        except ValueError:
            if i == max_retries:
                raise
            else:
                comment = comment.parent()


def find_get_from_comment(comment):
    count, comment = search_up_from_gz(comment)
    comment.refresh()

    while count % 1000 != 0:
        comment = comment.replies[0]
        if isinstance(comment, MoreComments):
            comment = comment.comments()[0]
        count = post_to_count(comment)
    return comment


def extract_gets_and_assists(comment, n_threads=1000):
    gets = []
    assists = []
    comment.refresh()
    for n in range(n_threads):
        rows = []
        for i in range(3):
            rows.append(OfflineComment(comment))
            comment = comment.parent()
        gets.append({**rows[0], 'timedelta': rows[0]['timestamp'] - rows[1]['timestamp']})
        assists.append({**rows[1], 'timedelta': rows[1]['timestamp'] - rows[2]['timestamp']})
        comment = find_previous_get(comment)
    return gets, assists


def walk_thread(leaf_comment):
    "Return a list of comments in a reddit thread from leaf to root"
    comments = []
    refresh_counter = 0
    comment = leaf_comment
    while not comment.is_root:
        # By refreshing, we request the previous 9 comments in a single go. So
        # the next calls to .parent() don't result in a network request. If the
        # refresh fails, we just go one by one
        if refresh_counter % 9 == 0:
            try:
                comment.refresh()
                print(comment.id)
            except (ClientException, ServerError):
                print(f"Broken chain detected at {comment.id}")
                print("Fetching the next 9 comments one by one")
        comments.append(OfflineComment(comment))
        comment = comment.parent()
        refresh_counter += 1
    # We need to include the root comment as well
    comments.append(OfflineComment(comment))
    return comments


def psaw_walk_thread(comment):
    """Use pushshift to log a reddit thread.

    If the thread is on pushshift, this is significantly faster than using the
    reddit api.

    """
    thread_id = comment.submission.id
    comment_ids = api._get_submission_comment_ids(thread_id)

    limit = 100
    psaw_comment_list = []
    chunks = chunked(comment_ids, limit)
    for chunk in chunks:
        print(chunk[0])
        comments = [x for x in api.search_comments(ids=chunk, metadata='true', limit=0)]
        psaw_comment_list += comments

    thread_tree = CommentTree(psaw_comment_list)
    print(comment.id)
    comment = find_get_from_comment(thread_tree.comment(comment.id))
    comments = walk_thread(comment)

    return comments
