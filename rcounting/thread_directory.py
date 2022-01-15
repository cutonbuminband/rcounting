import logging

from rcounting import models, parsing
from rcounting import side_threads as st

printer = logging.getLogger(__name__)


def load_wiki_page(subreddit, location):
    """
    Load the wiki page at reddit.com/r/subreddit/wiki/location

    Normalise the newlines, and parse it into a list of paragraphs.
    """
    wiki_page = subreddit.wiki[location]
    document = wiki_page.content_md.replace("\r\n", "\n")
    return parsing.parse_directory_page(document)


def title_from_first_comment(submission):
    """
    Return the body of the first comment of the submission, appropriately normalised:

    - Markdown links are stripped.
    - Only the first line is considered.
    """
    comment = sorted(list(submission.comments), key=lambda x: x.created_utc)[0]
    body = comment.body.split("\n")[0]
    return parsing.normalise_title(parsing.strip_markdown_links(body))


class Row:
    """
    A class corresponding to a row in a markdown table in the directory.

    A row has the following information associated with it:
      - The human-readable name of the thread. E.g. 'No repeating digits'
      - The submission id of the first submission, used to identify the side thread
      - The title of the current submission in the thread
      - The id of the current submission in the thread
      - The id of the latest comment in the current submission
      - The total number of counts made in the thread

    The core of this class is the `update` method, which updates
    the last four pieces of information listed.
    """

    def __init__(self, name, first_submission, title, submission_id, comment_id, count):
        """
        Initialise a new row with all the necessary information. Associate a side
        thread object with the row.
        """
        self.archived = False
        self.name = name
        self.first_submission = first_submission
        self.title = parsing.normalise_title(title)
        self.initial_submission_id = submission_id
        self.initial_comment_id = comment_id
        self.count_string = count
        self.count = parsing.find_count_in_text(self.count_string.replace("-", "0"))
        self.is_approximate = self.count_string[0] == "~"
        self.starred_count = self.count_string[-1] == "*"
        self.thread_type = st.known_thread_ids.get(self.first_submission, fallback="default")
        self.submission = None
        self.comment = None

    def __str__(self):
        return (
            f"[{self.name}](/{self.first_submission}) | "
            f"[{self.title}]({self.link}) | {self.count_string}"
        )

    def order_tuple(self):
        """
        The order in which total counts should be compared.

        Tuple comparison works in the dictionary order, so a < b means that one
        of the following is true
          - a[0] < b[0]
          - a[0] == b[0] and a[1:] < b[1:]
        """
        return (self.count, self.starred_count, self.is_approximate)

    def __lt__(self, other):
        return self.order_tuple() < other.order_tuple()

    @property
    def submission_id(self):
        return self.submission.id if self.submission is not None else self.initial_submission_id

    @property
    def comment_id(self):
        return self.comment.id if self.comment is not None else self.initial_comment_id

    @property
    def link(self):
        """Set the full link if we have it, otherwise just link the submission."""
        if self.comment_id is not None:
            return f"/comments/{self.submission_id}/_/{self.comment_id}?context=3"
        return f"/comments/{self.submission_id}"

    def update_title(self):
        """
        Set the title of the current submission.

        If it's the first submission, set the title from the first comment.
        Otherwise, treat the submission title as a | delimited list and use all
        but the first section.

        Then normalise the title before setting it
        """
        if self.first_submission == self.submission.id:
            self.title = title_from_first_comment(self.submission)
            return
        sections = self.submission.title.split("|")
        if len(sections) > 1:
            title = "|".join(sections[1:]).strip()
        else:
            title = title_from_first_comment(self.submission)
        self.title = parsing.normalise_title(title)

    def update_count(self, chain, was_revival, side_thread):
        """
        Use the side thread get an updated tally of how many counts have been
        made in the thread, taking the revival status of each submission into account."""
        try:
            count = side_thread.update_count(self.count, chain, was_revival)
        except (ValueError, IndexError):
            count = None
        self.count_string = self.format_count(count)
        if count is not None:
            self.count = count
        else:
            self.starred_count = True

    def format_count(self, count):
        """Add asterisks and tildes to erroneous and approximate counts"""
        if count is None:
            return self.count_string + "*"
        if count == 0:
            return "-"
        if self.is_approximate:
            return f"~{count:,d}"
        return f"{count:,d}"

    def update(self, submission_tree, from_archive=False, deepest_comment=False):
        """Find the latest comment in the latest submission of the side thread
        represented by this row.

        Parameters:

        submission_tree: A models.Tree object representing which
        submissions are linked to which. If no mistakes have been made, this
        should just be a series of straight line chains

        from_archive: Whether the current side thread originally comes from the
        archive. If it does, a bit of care is needed when updating the total
        number of counts

        deepest_comment: A flag used to say that the function should find the
        deepest comment overall, rather than the deepest comment in the
        earliest valid chain. Earliest is defined according to the order

        a < b if a is an ancestor of b
        a < b if a and b have the same parent and a was posted before b
        a < b if π(a) < π(b), where π(a) is the oldest ancestor of a which is
        not an ancestor of b, and similarly for b.

        This is used for new threads, where non-count comments are frequently
        posted either as early top-level comments, or as replies to a top level
        comment.

        """
        side_thread = st.get_side_thread(self.thread_type)
        printer.debug("Updating side thread: %s", self.thread_type)
        if self.thread_type == "default":
            printer.warning(
                "No rule found for %s. Not validating comment contents. "
                "Assuming n=1000 and no double counting.",
                self.name,
            )

        chain = submission_tree.walk_down_tree(submission_tree.node(self.submission_id))
        self.submission = chain[-1]
        if submission_tree.is_archived(self.submission):
            self.archived = True
            return

        if len(chain) > 1:
            self.initial_comment_id = None

        comments = models.CommentTree(reddit=submission_tree.reddit)
        if deepest_comment:
            for comment in self.submission.comments:
                comments.add_missing_replies(comment)
        elif self.comment_id is None:
            comment = next(filter(side_thread.looks_like_count, self.submission.comments))
            comments.add_missing_replies(comment)
        else:
            comment = submission_tree.reddit.comment(self.comment_id)
            comments.add_missing_replies(comment)
        comments.get_missing_replies = False
        comments.prune(side_thread)
        if deepest_comment:
            comment = comments.deepest_node.walk_up_tree(limit=3)[-1]
        else:
            comment_chain = comments.walk_down_tree(comment)
            comment = comment_chain[-3 if len(comment_chain) >= 3 else 0]

        self.comment = comment
        was_revival = [parsing.is_revived(x.title) for x in chain]
        if from_archive:
            was_revival[1] = True
        if not all(was_revival[1:]):
            # If there's really a new thread, the title & count need updating
            self.update_count(chain, was_revival, side_thread)
            self.update_title()


def rows2string(rows=None, show_archived=False, kind="directory"):
    """Convert a list of rows to a markdown table."""
    if rows is None:
        rows = []
    labels = {"directory": "Current", "archive": "Last"}
    header = [
        "⠀" * 10 + "Name &amp; Initial Thread" + "⠀" * 10,
        "⠀" * 10 + f"{labels[kind]} Thread" + "⠀" * 10,
        "⠀" * 3 + "# of Counts" + "⠀" * 3,
    ]
    header = [" | ".join(header), ":--:|:--:|--:"]
    if not show_archived:
        rows = [x for x in rows if not x.archived]
    return "\n".join(header + [str(x) for x in rows])
