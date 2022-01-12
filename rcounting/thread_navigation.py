from psaw import PushshiftAPI
import datetime
import rcounting.parsing as parsing
import rcounting.models as models

api = PushshiftAPI()


def find_previous_get(comment, validate_get=True, verbosity=0):
    reddit = comment._reddit
    submission = comment.submission
    urls = filter(lambda x: int(x[0], 36) < int(submission.id, 36),
                  parsing.find_urls_in_submission(submission))
    url = next(urls)

    new_submission_id, new_get_id = url
    while not new_get_id:
        try:
            url = next(urls)
            new_submission_id, new_get_id = url
        except StopIteration:
            break
    if not new_get_id and validate_get:
        new_get_id = find_get_in_submission(new_submission_id, reddit)
    comment = reddit.comment(new_get_id)
    if validate_get:
        new_get = find_get_from_comment(comment)
    else:
        new_get = reddit.comment(new_get_id)
    if verbosity > 1:
        print(f"Found previous get at: "
              f"http://reddit.com/comments/{new_get.submission}/_/{new_get.id}")
    return new_get


def find_get_in_submission(submission_id, reddit):
    "Find the get based on submission id"
    comment_ids = api._get_submission_comment_ids(submission_id)[::-1]
    for comment_id in comment_ids[:-1]:
        comment = reddit.comment(comment_id)
        try:
            count = parsing.post_to_count(comment, api)
            if count % 1000 == 0:
                return comment.id
        except ValueError:
            continue
    raise ValueError(f"Unable to locate get in submission {submission_id}")


def search_up_from_gz(comment, max_retries=5):
    "Find a count up to max_retries above the linked_comment"
    for i in range(max_retries):
        try:
            count = parsing.post_to_count(comment, api)
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
        count = parsing.post_to_count(comment, api)
    return comment


def extract_gets_and_assists(comment, n_submissions=1000):
    gets = []
    assists = []
    comment.refresh()
    for n in range(n_submissions):
        rows = []
        for i in range(3):
            rows.append(models.comment_to_dict(comment))
            comment = comment.parent()
        gets.append({**rows[0], 'timedelta': rows[0]['timestamp'] - rows[1]['timestamp']})
        assists.append({**rows[1], 'timedelta': rows[1]['timestamp'] - rows[2]['timestamp']})
        comment = find_previous_get(comment)
    return gets, assists


def fetch_comment_tree(submission, root_id=None, verbosity=1, use_pushshift=True, history=1,
                       fill_gaps=False):
    r = submission._reddit
    if use_pushshift:
        comment_ids = [x for x in api._get_submission_comment_ids(submission.id)]
    else:
        comment_ids = []
    if not comment_ids:
        return models.CommentTree([], reddit=r, verbosity=verbosity)

    comment_ids.sort(key=lambda x: int(x, 36))
    if root_id is not None:
        for idx, comment_id in enumerate(comment_ids):
            if int(comment_id, 36) >= int(root_id, 36):
                break
        comment_ids = comment_ids[max(0, idx - history):]
    comments = [comment for comment in r.info(['t1_' + x for x in comment_ids])]
    submission_tree = models.CommentTree(comments, reddit=r, verbosity=verbosity)
    if fill_gaps:
        submission_tree.fill_gaps()
    return submission_tree


def fetch_comments(comment, verbosity=1, use_pushshift=True):
    tree = fetch_comment_tree(comment.submission, verbosity=verbosity, use_pushshift=use_pushshift)
    comments = tree.comment(comment.id).walk_up_tree()[::-1]
    return [x.to_dict() for x in comments]


def get_counting_history(subreddit, time_limit, verbosity=1):
    now = datetime.datetime.utcnow()
    submissions = subreddit.new(limit=1000)
    tree = {}
    submissions_dict = {}
    new_submissions = []
    for count, submission in enumerate(submissions):
        submission.comment_sort = 'old'
        if verbosity > 1 and count % 20 == 0:
            print(f"Processing reddit submission {submission.id}")
        title = submission.title.lower()
        if "tidbits" in title or "free talk friday" in title:
            continue
        submissions_dict[submission.id] = submission
        try:
            url = next(filter(lambda x: int(x[0], 36) < int(submission.id, 36),
                              parsing.find_urls_in_submission(submission)))
            tree[submission.id] = url[0]
        except StopIteration:
            new_submissions.append(submission)
        post_time = datetime.datetime.utcfromtimestamp(submission.created_utc)
        if now - post_time > time_limit:
            break
    else:  # no break
        print('Threads between {now - six_months} and {post_time} have not been collected')

    return models.SubmissionTree(submissions_dict, tree, subreddit._reddit), new_submissions
