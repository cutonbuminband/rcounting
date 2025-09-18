import datetime
import difflib
import logging

from praw.exceptions import DuplicateReplaceException

from rcounting import models, parsing
from rcounting.reddit_interface import reddit

printer = logging.getLogger(__name__)


def find_previous_submission(submission, similarity_threshold=0.5):
    """Decide which link in a submission most likely represents the intended
    link to the previous submission.

    Sometimes people write weird things in submissions selftexts or in top
    level comments, so we'll potentially be receiving a list of multiple urls,
    and have to pick the best one.

    We also have to take into account that the most common case is that there
    is a single link in the submission self text that points to the right
    place, and so we shouldn't spend too much extra time in handling that case

    We'll use the following approach:

    - Take the first link which has both submission and comment and a
      sufficiently high similarity
    - If there are none, take the first link with both submission and comment
    - If there are none, take the first link with sufficiently high similarity
    - If there are none, take the first link present
    - If there are none, return (None, None)

    """

    submission = submission if hasattr(submission, "id") else reddit.submission(submission)
    matcher = difflib.SequenceMatcher()
    matcher.set_seq2(submission.title)
    result = (None, None)
    urls = filter(
        lambda x: int(x[0], 36) < int(submission.id, 36),
        parsing.find_urls_in_submission(submission),
    )
    threshold_met = False
    for previous_submission_id, previous_get_id in urls:
        if result[0] is None:
            result = (previous_submission_id, previous_get_id)

        if result[1] is None and previous_get_id:
            result = (previous_submission_id, previous_get_id)

        matcher.set_seq1(reddit.submission(previous_submission_id).title)
        if matcher.ratio() >= similarity_threshold:
            if previous_get_id:
                result = (previous_submission_id, previous_get_id)
                break
            if not threshold_met and result[1] is None:
                result = (previous_submission_id, previous_get_id)
                threshold_met = True
    return result


def find_get_in_submission(submission_id, get_id, validate_get=True):
    if not get_id:
        get_id = find_deepest_comment(submission_id, reddit)
    comment = reddit.comment(get_id)
    if validate_get:
        get = find_get_from_comment(comment)
    else:
        get = reddit.comment(get_id)
    printer.debug(
        "Found previous get at: http://reddit.com/comments/%s/_/%s/",
        get.submission,
        get.id,
    )
    return get


def find_previous_get(submission, validate_get=True):
    """
    Find the get of the previous reddit submission in the chain of counts.

    There's a user-enforced convention that the previous get should be linked
    in either the body of the submission, or the first comment.

    Usually this convention is followed, but sometimes this isn't done.
    Frequently, even in those cases the get will be linked in a different
    top-level comment.

    Parameters:

    submission: A reddit submission instance for which we want to find the parent
    validate_get: Whether or not the prorgram should check that the linked comment ends in 000,
    and if it doesn't, try to find a nearby comment that does.
    """
    submission = submission if hasattr(submission, "id") else reddit.submission(submission)
    new_submission_id, new_get_id = find_previous_submission(submission)
    if new_submission_id is None:
        return None
    return find_get_in_submission(new_submission_id, new_get_id, validate_get)


def find_deepest_comment(submission, reddit):
    """
    Find the deepest comment on a submission
    """
    if not hasattr(submission, "id"):
        submission = reddit.submission(submission)
        submission.comment_sort = "old"
    comments = models.CommentTree(reddit=reddit)
    for comment in submission.comments:
        comments.add_missing_replies(comment)
    return comments.deepest_node.id


def search_up_from_gz(comment, max_retries=5):
    """Look for a count up to max_retries above the linked_comment"""
    for i in range(max_retries):
        try:
            count = parsing.post_to_count(comment)
            return count, comment
        except ValueError:
            if i == max_retries:
                raise
            comment = comment.parent()
    raise ValueError(f"Unable to find count in {comment.submission.permalink}")


def find_get_from_comment(comment):
    """Look for the get either above or below the linked comment"""
    count, comment = search_up_from_gz(comment)
    comment.refresh()
    replies = comment.replies
    try:
        replies.replace_more(limit=None)
    except DuplicateReplaceException:
        pass
    while count % 1000 != 0:
        for reply in comment.replies:
            try:
                count = parsing.post_to_count(reply)
                comment = reply
                break
            except ValueError:
                continue
    return comment


def fetch_comments(comment, limit=None):
    """
    Fetch a chain of comments from root to the supplied leaf comment.
    """
    tree = models.CommentTree([], reddit=reddit)
    comment_id = getattr(comment, "id", comment)
    comments = tree.comment(comment_id).walk_up_tree(limit=limit)[::-1]
    return [models.comment_to_dict(x) for x in comments]


def fetch_counting_history(subreddit, time_limit):
    """
    Fetch all submissions made to r/counting within time_limit days
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    submissions = subreddit.new(limit=1000)
    tree = {}
    submissions_dict = {}
    new_submissions = []
    for count, submission in enumerate(submissions):
        submission.comment_sort = "old"
        if count % 20 == 0:
            printer.debug("Processing reddit submission %s", submission.id)
        title = submission.title.lower()
        author = y.name.lower() if (y := submission.author) is not None else None
        if "tidbits" in title or "free talk friday" in title or author == "rcounting":
            continue
        submissions_dict[submission.id] = submission
        previous_submission = find_previous_submission(submission)[0]
        if previous_submission is not None:
            tree[submission.id] = previous_submission
        else:
            new_submissions.append(submission)
        post_time = datetime.datetime.fromtimestamp(submission.created_utc, datetime.timezone.utc)
        if now - post_time > time_limit:
            break
    else:  # no break
        printer.warning(
            "Threads between %s and %s have not been collected", now - time_limit, post_time
        )

    return (
        models.SubmissionTree(submissions_dict, tree, reddit),
        new_submissions,
    )
