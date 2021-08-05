from psaw import PushshiftAPI
from parsing import find_urls_in_submission, post_to_count
from models import CommentTree, comment_to_dict

api = PushshiftAPI()


def find_previous_get(comment):
    reddit = comment._reddit
    thread = comment.submission
    new_thread_id, new_get_id = next(find_urls_in_submission(thread))
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
            count = post_to_count(comment, api)
            if count % 1000 == 0:
                return comment.id
        except ValueError:
            continue
    raise ValueError(f"Unable to locate get in thread {thread.id}")


def search_up_from_gz(comment, max_retries=5):
    "Find a count up to max_retries above the linked_comment"
    for i in range(max_retries):
        try:
            count = post_to_count(comment, api)
            return count, comment
        except ValueError:
            if i == max_retries:
                raise
            else:
                comment = comment.parent()


def find_get_from_comment(comment):
    count, comment = search_up_from_gz(comment)
    comment.refresh()
    replies = comment.replies
    replies.replace_more(limit=None)
    while count % 1000 != 0:
        comment = comment.replies[0]
        count = post_to_count(comment, api)
    return comment


def extract_gets_and_assists(comment, n_threads=1000):
    gets = []
    assists = []
    comment.refresh()
    for n in range(n_threads):
        rows = []
        for i in range(3):
            rows.append(comment_to_dict(comment))
            comment = comment.parent()
        gets.append({**rows[0], 'timedelta': rows[0]['timestamp'] - rows[1]['timestamp']})
        assists.append({**rows[1], 'timedelta': rows[1]['timestamp'] - rows[2]['timestamp']})
        comment = find_previous_get(comment)
    return gets, assists


def fetch_comment_tree(thread, root_id=None, verbose=True, use_pushshift=True, history=1,
                       fill_gaps=False):
    r = thread._reddit
    if use_pushshift:
        comment_ids = [x for x in api._get_submission_comment_ids(thread.id)]
    else:
        comment_ids = []
    if not comment_ids:
        return CommentTree([], reddit=r, verbose=verbose)

    comment_ids.sort(key=lambda x: int(x, 36))
    if root_id is not None:
        for idx, comment_id in enumerate(comment_ids):
            if int(comment_id, 36) >= int(root_id, 36):
                break
        comment_ids = comment_ids[max(0, idx - history):]
    comments = [comment for comment in r.info(['t1_' + x for x in comment_ids])]
    thread_tree = CommentTree(comments, reddit=r, verbose=verbose)
    if fill_gaps:
        thread_tree.fill_gaps()
    return thread_tree


def fetch_thread(comment, verbose=True):
    tree = fetch_comment_tree(comment.submission, verbose=verbose)
    comments = tree.comment(comment.id).walk_up_tree()[::-1]
    return [x.to_dict() for x in comments]
