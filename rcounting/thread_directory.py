import rcounting.parsing as parsing
import rcounting.side_threads as st
import rcounting.models as models


def load_wiki_page(subreddit, location):
    wiki_page = subreddit.wiki[location]
    document = wiki_page.content_md.replace("\r\n", "\n")
    return wiki_page, parsing.parse_directory_page(document)


def title_from_first_comment(submission):
    comment = sorted(list(submission.comments), key=lambda x: x.created_utc)[0]
    body = comment.body.split('\n')[0]
    return parsing.normalise_title(parsing.strip_markdown_links(body))


class Row():
    def __init__(self, name, first_submission, title, submission_id, comment_id, count):
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
        self.thread_type = st.known_thread_ids.get(self.first_submission, fallback='default')

    def __str__(self):
        return (f"[{self.name}](/{self.first_submission}) | "
                f"[{self.title}]({self.link}) | {self.count_string}")

    def __lt__(self, other):
        return ((self.count, self.starred_count, self.is_approximate)
                < (other.count, other.starred_count, other.is_approximate))

    @property
    def submission_id(self):
        return self.submission.id if hasattr(self, 'submission') else self.initial_submission_id

    @property
    def comment_id(self):
        return self.comment.id if hasattr(self, 'comment') else self.initial_comment_id

    @property
    def link(self):
        if self.comment_id is not None:
            return f"/comments/{self.submission_id}/_/{self.comment_id}?context=3"
        else:
            return f"/comments/{self.submission_id}"

    def set_submission(self, submission):
        submission.comment_sort = 'old'
        self.submission = submission

    def update_title(self):
        if self.first_submission == self.submission.id:
            self.title = title_from_first_comment(self.submission)
            return
        else:
            sections = self.submission.title.split("|")
            if len(sections) > 1:
                title = '|'.join(sections[1:]).strip()
            else:
                title = title_from_first_comment(self.submission)
        self.title = parsing.normalise_title(title)

    def format_count(self, count):
        if count is None:
            return self.count_string + "*"
        if count == 0:
            return "-"
        if self.is_approximate:
            return f"~{count:,d}"
        return f"{count:,d}"

    def update(self, submission_tree, from_archive=False, verbosity=1, deepest_comment=False):
        side_thread = st.get_side_thread(self.thread_type, verbosity)
        if verbosity > 1:
            print(f"Updating side thread: {self.thread_type}")
        if verbosity > 0 and self.thread_type == "default":
            print(f'No rule found for {self.name}. '
                  'Not validating comment contents. '
                  'Assuming n=1000 and no double counting.')

        chain = submission_tree.walk_down_tree(submission_tree.node(self.submission_id))
        self.set_submission(chain[-1])
        if submission_tree.is_archived(self.submission):
            self.archived = True
            return

        if len(chain) > 1:
            self.initial_comment_id = None

        comments = models.CommentTree(reddit=submission_tree.reddit, verbosity=verbosity)
        if deepest_comment:
            for comment in self.submission.comments:
                comments.add_missing_replies(comment)
        else:
            if self.comment_id is None:
                comment = next(filter(side_thread.looks_like_count, self.submission.comments))
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
            try:
                count = side_thread.update_count(self.count, chain, was_revival)
            except Exception:
                count = None
            self.count_string = self.format_count(count)
            if count is not None:
                self.count = count
            else:
                self.starred_count = True
            self.update_title()


def rows2string(rows=[], show_archived=False, kind='directory'):
    labels = {'directory': 'Current', 'archive': 'Last'}
    header = ['⠀' * 10 + 'Name &amp; Initial Thread' + '⠀' * 10,
              '⠀' * 10 + f'{labels[kind]} Thread' + '⠀' * 10,
              '⠀' * 3 + '# of Counts' + '⠀' * 3]
    header = [' | '.join(header), ':--:|:--:|--:']
    if not show_archived:
        rows = [x for x in rows if not x.archived]
    return "\n".join(header + [str(x) for x in rows])
