import requests.auth
from praw.models import MoreComments
from praw.models import Comment
from praw.exceptions import ClientException
from parsing import find_urls_in_text, find_count_in_text

ps_url = 'https://api.pushshift.io/reddit'


def get_text(reddit_post):
    if isinstance(reddit_post, Comment):
        is_comment = True
        post_type = 'comment'
    else:
        is_comment = False
        post_type = 'submission'
    body = reddit_post.body if is_comment else reddit_post.selftext
    if body == "[deleted]" or body == "[removed]":
        ps_post = requests.get(f"{ps_url}/{post_type}/search?ids={reddit_post.id}")
        try:
            post_dict = ps_post.json()['data'][0]
            body = post_dict['body'] if is_comment else post_dict['selftext']
        except (KeyError, IndexError):
            body = "[deleted]"
    return body


def find_previous_get(get_id, reddit_instance):
    comment = reddit_instance.comment(get_id)
    thread = comment.submission
    urls = find_urls_in_text(thread.selftext)
    if not urls:
        urls = [find_urls_in_text(get_text(comment)) for comment in thread.comments]
        urls = [url for comment_urls in urls for url in comment_urls]
    new_thread_id, title_url, new_get_id = urls[0]
    if not new_get_id:
        new_get_id = find_get_in_thread(new_thread_id, reddit_instance)
    comment = reddit_instance.comment(new_get_id)
    new_get = find_get_from_comment(comment)
    print(new_get.submission, new_get.id)
    return new_get


def find_get_in_thread(thread, reddit):
    "Find the get based on thread id"
    comment_ids = requests.get('/submission/'
                               f'comment_ids/{thread.id}').json()['data'][::-1]
    for comment_id in comment_ids[:-1]:
        comment = reddit.comment(comment_id)
        try:
            count = find_count_in_text(get_text(comment))
            if count % 1000 == 0:
                return comment.id
        except ValueError:
            continue
    raise ValueError(f"Unable to locate get in thread {thread.id}")


def search_up_from_gz(comment, max_retries=5):
    "Find a count up to max_retries above the linked_comment"
    for i in range(max_retries):
        try:
            count = find_count_in_text(get_text(comment))
            break
        except ValueError:
            if i == max_retries:
                raise
            else:
                comment = comment.parent()

    return count, comment


def find_get_from_comment(comment):
    count, comment = search_up_from_gz(comment)
    comment.refresh()

    while count % 1000 != 0:
        comment = comment.replies[0]
        if isinstance(comment, MoreComments):
            comment = comment.comments()[0]
        count = find_count_in_text(get_text(comment))
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
    if comment.author is None:
        try:
            ps_comment = requests.get("https://api.pushshift.io/reddit/comment/search?"
                                      f"ids={comment.id}").json()
            author = ps_comment['data'][0]['author']
        except (KeyError, IndexError):
            print(comment.id)
            author = "[deleted]"
    author = str(comment.author) if comment.author is not None else "[deleted]"
    return (get_text(comment), author,
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
