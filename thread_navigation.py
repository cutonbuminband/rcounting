from psaw import PushshiftAPI
from praw.exceptions import ClientException
from prawcore.exceptions import ServerError
from parsing import post_to_urls, post_to_count
from utils import chunked
from models import Comment as OfflineComment, CommentTree
from validation import validate_thread, get_history, update_history

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
            rows.append(OfflineComment(comment))
            comment = comment.parent()
        gets.append({**rows[0], 'timedelta': rows[0]['timestamp'] - rows[1]['timestamp']})
        assists.append({**rows[1], 'timedelta': rows[1]['timestamp'] - rows[2]['timestamp']})
        comment = find_previous_get(comment)
    return gets, assists


def walk_up_thread(leaf, root=None):
    "Return a list of reddit comments betwen root and leaf"
    comments = []
    refresh_counter = 0

    def is_first_comment(comment):
        if root is None:
            return comment.is_root
        else:
            return comment.id == root.id

    comment = leaf
    while not is_first_comment(comment):
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
    return [x.to_dict() for x in comments[::-1]]


def walk_down_thread(comment, thread=None, thread_type='default'):
    if comment is None:
        comment = thread.comments[0]
    thread = get_history(comment, thread_type)

    # get all leaf comments of a type
    comment.refresh()
    replies = comment.replies
    replies.replace_more(limit=None)
    print(comment.body)
    while(len(replies) > 0):
        for reply in replies:
            new_thread = update_history(thread, reply)
            if validate_thread(thread=new_thread, rule=thread_type)[0]:
                comment = reply
                if not hasattr(comment, 'replies'):
                    comment.refresh()
                thread = new_thread
                break
        else:  # We looped over all replies without finding a valid one
            print("No valid replies found to {comment.id}")
            break
        replies = comment.replies
    # We've arrived at a leaf. Somewhere
    return comment


def psaw_get_tree(thread, root=None, limit=100):
    comment_ids = api._get_submission_comment_ids(thread.id)
    if root is not None:
        comment_ids = [x for x in comment_ids if int(x, 36) >= int(root.id, 36)]
    psaw_comment_list = []
    chunks = chunked(comment_ids, limit)
    for chunk in chunks:
        print(chunk[0])
        comments = [x for x in api.search_comments(ids=chunk, metadata='true', limit=0)]
        psaw_comment_list += comments

    thread_tree = CommentTree(psaw_comment_list)
    return thread_tree
