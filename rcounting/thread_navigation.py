import datetime
import logging

from praw.exceptions import DuplicateReplaceException

from rcounting import models, parsing, reddit_interface

printer = logging.getLogger(__name__)


def find_previous_submission(submission):
    """
    Find the previous reddit submission in the chain of counts. The code
    will look first in the body of the submission text and then in the top
    level comments for everything that looks like a link to a reddit comment.
    It will take the first comment link it finds and use that as the previous
    submission in the chain. Failing that, it will take the first link to a
    reddit submission it finds and use as the previous link in the chain.
    Failing that, it will just fail.

    """

    urls = filter(
        lambda x: int(x[0], 36) < int(submission.id, 36),
        parsing.find_urls_in_submission(submission),
    )
    next_submission_id = None
    new_submission_id, new_get_id = next(urls)
    # This gets a bit silly but if the first link we found only had a
    # submission id and not a comment id we scan through all the rest, and then
    # take the first one that also has a comment id. If none of them do, we
    # fall back to the first link we found
    while not new_get_id:
        try:
            next_submission_id, new_get_id = next(urls)
        except StopIteration:
            break
    if next_submission_id is not None and new_get_id:
        return next_submission_id, new_get_id
    return new_submission_id, new_get_id


def find_get_in_submission(submission_id, get_id, validate_get=True):
    reddit = reddit_interface.reddit
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
    reddit = reddit_interface.reddit
    submission = submission if hasattr(submission, "id") else reddit.submission(submission)
    new_submission_id, new_get_id = find_previous_submission(submission)
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
        comment = comment.replies[0]
        count = parsing.post_to_count(comment)
    return comment


def fetch_comments(comment, limit=None):
    """
    Fetch a chain of comments from root to the supplied leaf comment.
    """
    reddit = reddit_interface.reddit
    tree = models.CommentTree([], reddit=reddit)
    comment_id = getattr(comment, "id", comment)
    comments = tree.comment(comment_id).walk_up_tree(limit=limit)[::-1]
    return [models.comment_to_dict(x) for x in comments]


def fetch_counting_history(subreddit, time_limit):
    """
    Fetch all submissions made to r/counting within time_limit days
    """
    now = datetime.datetime.utcnow()
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
        urls = parsing.find_urls_in_submission(submission)
        try:
            url = next(filter(lambda x, s=submission: int(x[0], 36) < int(s.id, 36), urls))
            tree[submission.id] = url[0]
        except StopIteration:
            new_submissions.append(submission)
        post_time = datetime.datetime.utcfromtimestamp(submission.created_utc)
        if now - post_time > time_limit:
            break
    else:  # no break
        printer.warning(
            "Threads between %s and %s have not been collected", now - time_limit, post_time
        )

    return (
        models.SubmissionTree(submissions_dict, tree, reddit_interface.reddit),
        new_submissions,
    )
