import datetime
import configparser
import re
from models import Tree
from side_threads import get_side_thread
import parsing
from thread_navigation import fetch_comment_tree
from utils import flatten

config = configparser.ConfigParser()
config.read('side_threads.ini')
known_threads = config['threads']


def normalise_title(title):
    title = title.translate(str.maketrans('[]', '()'))
    regex = r'\(*reviv\w*\)*'
    match = re.search(regex, title.lower())
    if match:
        return title[:match.span()[0]] + '(Revival)' + title[match.span()[1]:]
    return title


class Table():
    def __init__(self, rows, show_archived=False):
        self.rows = rows
        self.show_archived = show_archived

    def __str__(self):
        table_header = [' ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀Name &amp; Initial Thread⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀ |'
                        ' ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀Current Thread⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀ |'
                        ' ⠀⠀⠀# of Counts⠀⠀⠀', ':--:|:--:|--:']
        if self.show_archived:
            return "\n".join(table_header + [str(x) for x in self.rows])
        else:
            return "\n".join(table_header + [str(x) for x in self.rows if not x.archived])

    def update(self, tree):
        for row in self.rows:
            if verbosity > 0:
                print(f"Updating side thread: {row.thread_type}")
            row.update(tree)

    def archived_rows(self):
        rows = [row for row in self.rows if row.archived]
        return Table(rows, show_archived=True)

    def __getitem__(self, key):
        return self.rows[key]

    def __len__(self):
        return len(self.rows)

    @property
    def submissions(self):
        return [row.submission for row in self.rows]

    def sort(self, **kwargs):
        self.rows = sorted(self.rows, **kwargs)


class Row():
    def __init__(self, first_thread, current_thread, current_count, keep_title=True):
        thread_name, first_thread_id = parsing.parse_markdown_links(first_thread)[0]
        self.thread_name = thread_name.strip()
        self.first_thread = first_thread_id.strip()[1:]
        title, link = parsing.parse_markdown_links(current_thread)[0]
        self.title = normalise_title(title)
        submission_id, comment_id = parsing.find_urls_in_text(link)[0]
        comment_id = None if not comment_id else comment_id
        self.submission_id = submission_id
        self.comment_id = comment_id
        self.count_string = current_count.strip()
        self.count = parsing.find_count_in_text(self.count_string.replace("-", "0"))
        self.is_approximate = self.count_string[0] == "~"
        self.thread_type = known_threads.get(self.first_thread, fallback='decimal')
        self.side_thread = get_side_thread(self.thread_type)
        self.keep_title = keep_title

    def __str__(self):
        return (f"[{self.thread_name}](/{self.first_thread}) | "
                f"[{self.title}]({self.link}) | {self.count_string}")

    def __lt__(self, other):
        return (self.count < other.count
                or ((self.count == other.count)
                   and (other.is_approximate and not self.is_approximate)))

    @property
    def link(self):
        if self.comment_id is not None:
            return f"/comments/{self.submission_id}/_/{self.comment_id}?context=3"
        else:
            return f"/comments/{self.submission_id}"

    def update_title(self):
        if self.first_thread == self.submission.id:
            self.submission.comment_sort = 'old'
            body = self.submission.comments[0].body.split('\n')[0]
            markdown_link = parsing.parse_markdown_links(body)
            title = markdown_link[0][0] if markdown_link else body
        else:
            sections = self.submission.title.split("|")
            if len(sections) > 1:
                sections = sections[1:]
            title = (' '.join(sections)).strip()
            title = title if title else str(self.count)
        self.title = normalise_title(title)

    def format_count(self, count):
        if count is None:
            return self.count_string + "*"
        if count == 0:
            return "-"
        if self.is_approximate:
            return f"~{count:,d}"
        return f"{count:,d}"

    def update(self, submission_tree):
        submission = tree.node(self.submission_id)
        comment, chain, archived = submission_tree.find_latest_comment(self.side_thread,
                                                                       submission,
                                                                       self.comment_id)
        comment = comment.walk_up_tree(limit=3)[-1]
        self.comment_id = comment.id
        self.submission = chain[-1]
        self.submission_id = self.submission.id
        self.archived = archived
        if not self.keep_title:
            self.update_title()
        if len(chain) > 1:
            count = self.side_thread.update_count(self.count, chain)
            self.count_string = self.format_count(count)
            if count is not None:
                self.count = count
            self.update_title()


class SubmissionTree(Tree):
    def __init__(self, submissions, submission_tree, reddit=None, verbosity=1,
                 use_pushshift=True):
        self.verbosity = verbosity
        self.reddit = reddit
        self.use_pushshift = use_pushshift
        super().__init__(submissions, submission_tree)

    def find_latest_comment(self, side_thread, old_submission, comment_id=None):
        chain = self.walk_up_tree(old_submission)
        archived = False
        if chain is None:
            archived = True
            chain = [old_submission]
        if len(chain) > 1 or comment_id is None:
            comment_id = chain[-1].comments[0].id
        comments = fetch_comment_tree(chain[-1], root_id=comment_id, verbose=False,
                                      use_pushshift=self.use_pushshift)
        comments.get_missing_replies = False
        comments.verbose = (self.verbosity > 1)
        comment = comments.comment(comment_id)
        comments.add_missing_replies(comment)
        comments.prune(side_thread)
        new_comment = comments.walk_down_tree(comment)[-1]
        return new_comment, chain, archived

    def node(self, node_id):
        try:
            return super().node(node_id)
        except KeyError:
            if self.reddit is not None:
                return self.reddit.submission(node_id)
        raise


def get_counting_history(subreddit, time_limit, verbosity=1):
    now = datetime.datetime.utcnow()
    submissions = subreddit.new(limit=1000)
    tree = {}
    submissions_dict = {}
    new_threads = []
    for count, submission in enumerate(submissions):
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
            new_threads.append(submission)
        post_time = datetime.datetime.utcfromtimestamp(submission.created_utc)
        if now - post_time > time_limit:
            break
    else:  # no break
        print('Threads between {now - six_months} and {post_time} have not been collected')

    tree = {v: k for k, v in tree.items()}
    return submissions_dict, tree, new_threads


if __name__ == "__main__":
    import argparse
    from reddit_interface import reddit
    parser = argparse.ArgumentParser(description='Update the thread directory located at'
                                     ' reddit.com/r/counting/wiki/directory')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--verbose', '-v', action='store_true',
                       help='Print more output during directory updates')

    group.add_argument('--quiet', '-q', action='store_true',
                       help='Print less output during directory updates')

    parser.add_argument('--pushshift', '-p', action='store_true',
                        help=('Use  an online archive fetch older comments '))

    args = parser.parse_args()
    verbosity = 1 - args.quiet + args.verbose
    start = datetime.datetime.now()
    subreddit = reddit.subreddit('counting')

    wiki_page = subreddit.wiki['directory']
    document = wiki_page.content_md.replace("\r\n", "\n")
    document = parsing.parse_directory_page(document)

    time_limit = datetime.timedelta(weeks=26.5)
    if verbosity > 0:
        print("Getting history")
    submissions, submission_tree, new_threads = get_counting_history(subreddit,
                                                                     time_limit,
                                                                     verbosity)
    tree = SubmissionTree(submissions, submission_tree, reddit, verbosity,
                          use_pushshift=args.pushshift)

    if verbosity > 0:
        print("Updating tables")
    updated_document = []
    table_counter = 0
    for paragraph in document:
        if paragraph[0] == "text":
            updated_document.append(paragraph[1])
        elif paragraph[0] == "table":
            table_counter += 1
            table = Table([Row(*x) for x in paragraph[1]], show_archived=True)
            table.update(tree)
            if table_counter == 2:
                table.sort(reverse=True)
            updated_document.append(table)

    new_page = '\n\n'.join(map(str, updated_document))
    wiki_page.edit(new_page, reason="Ran the update script")

    table = Table(flatten([x.rows for x in updated_document if hasattr(x, 'rows')]))
    archived_threads = table.archived_rows()
    if archived_threads:
        n = len(archived_threads)
        if verbosity > 0:
            print(f'writing {n} archived thread{"s" if n > 1 else ""}'
                  ' to archived_threads.md')
        with open("archived_threads.md", "a") as f:
            print(archived_threads)

    new_threads = set(tree.walk_up_tree(thread)[-1].id for thread in new_threads)
    known_submissions = set([x.id for x in table.submissions])
    new_threads = new_threads - known_submissions
    if new_threads:
        with open("new_threads.txt", "w") as f:
            if verbosity > 0:
                n = len(new_threads)
                print(f"{n} new thread{'' if n == 1 else 's'} found. Writing to file")
            print(*[f"New thread '{reddit.submission(x).title}' "
                    f"at reddit.com/comments/{x}" for x in new_threads],
                  sep="\n", file=f)

    end = datetime.datetime.now()
    print(end - start)
