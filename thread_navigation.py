from psaw import PushshiftAPI
from praw.exceptions import ClientException
from prawcore.exceptions import ServerError
from parsing import post_to_urls, post_to_count
from models import CommentTree, comment_to_dict

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
    replies.replace_more(limit=0)
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


def walk_up_thread(comment, verbose=True, max_comments=None):
    "Return a list of reddit comments betwen root and leaf"
    comments = []
    refresh_counter = 0

    while not comment.is_root:
        # By refreshing, we request the previous 9 comments in a single go. So
        # the next calls to .parent() don't result in a network request. If the
        # refresh fails, we just go one by one
        if refresh_counter % 9 == 0:
            try:
                comment.refresh()
                if verbose:
                    print(comment.id)
            except (ClientException, ServerError):
                print(f"Broken chain detected at {comment.id}")
                print("Fetching the next 9 comments one by one")
        comments.append(comment)
        comment = comment.parent()
        refresh_counter += 1
        if max_comments is not None and refresh_counter >= max_comments:
            break
    else:  # No break. We need to include the root comment as well
        comments.append(comment)
    return comments[::-1]


def walk_down_thread(side_thread, comment, thread=None):
    if comment is None:
        comment = thread.comments[0]

    side_thread.get_history(comment)

    comment.refresh()
    replies = comment.replies
    if hasattr(replies, 'replace_more'):
        replies.replace_more(limit=None)
    print(comment.body)
    while(len(replies) > 0):
        for reply in replies:
            if side_thread.is_valid(reply)[0]:
                side_thread.update_history(reply)
                comment = reply
                break
        else:  # We looped over all replies without finding a valid one
            print("No valid replies found to {comment.id}")
            break
        replies = comment.replies
    # We've arrived at a leaf. Somewhere
    return comment


def fetch_comment_tree(thread, root_id=None):
    r = thread._reddit
    comment_ids = api._get_submission_comment_ids(thread.id)
    if root_id is not None:
        comment_ids = [x for x in comment_ids if int(x, 36) > int(root_id, 36)]
    comments = [comment for comment in r.info(['t1_' + x for x in comment_ids])]
    thread_tree = CommentTree(comments, reddit=r)
    return thread_tree


def fetch_thread(comment, verbose=True):
    tree = fetch_comment_tree(comment.submission)
    comments = walk_up_thread(tree.comment(comment.id), verbose=False)
    return [x.to_dict() for x in comments]
