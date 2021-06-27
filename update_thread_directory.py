import re
import numpy as np
import datetime
import configparser
from side_threads import get_side_thread
from parsing import find_urls_in_text, parse_markdown_links
from thread_navigation import fetch_comment_tree, walk_down_thread
from models import Tree
from utils import flatten

config = configparser.ConfigParser()
config.read('side_threads.ini')
known_threads = config['threads']


def parse_directory_page(directory_page):
    paragraphs = directory_page.split("\n\n")
    regex = r"^.*\|.*\|.*$"
    parsed_results = []
    for paragraph in paragraphs:
        lines = [line for line in paragraph.split("\n") if line]
        mask = np.all([bool(re.match(regex, line)) for line in lines])
        if not mask:
            parsed_results.append(paragraph)
        else:
            rows = []
            for row in lines[2:]:
                fields = row.split('|')
                row = Row(*fields)
                rows.append(row)
            parsed_results.append(Table(rows))
    return parsed_results


class Table():
    def __init__(self, rows):
        self.rows = rows
        for row in self.rows:
            row.is_archived = False

    def __str__(self):
        table_header = ['Name &amp; Initial Thread|Current Thread|# of Counts',
                        ':--:|:--:|--:']
        return "\n".join(table_header + [str(x) for x in self.rows if not x.is_archived])

    def update(self, verbosity=1, accuracy=0):
        for row in self.rows:
            if verbosity > 0:
                print(f"Updating side thread: {row.thread_type}")
            row.update(verbosity=verbosity, accuracy=accuracy)

    def add_tree(self, tree):
        for row in self.rows:
            row.add_tree(tree)

    def archived_threads(self):
        rows = [row.copy() for row in self.rows if row.is_archived]
        return Table(rows)

    @property
    def submissions(self):
        return [row.submission for row in self.rows]


class Row():
    def __init__(self, first_thread, current_thread, current_count):
        thread_name, first_thread_id = parse_markdown_links(first_thread)[0]
        self.thread_name = thread_name.strip()
        self.first_thread = first_thread_id.strip()[1:]
        submission_id, comment_id = find_urls_in_text(current_thread)[0]
        self.submission_id = submission_id
        self.comment_id = comment_id
        self.count = current_count.strip()
        self.thread_type = known_threads.get(self.first_thread, fallback='decimal')
        self.side_thread = get_side_thread(self.thread_type)

    def __str__(self):
        return (f"[{self.thread_name}](/{self.first_thread}) | "
                f"[{self.title}]({self.link}) | {self.count}")

    @property
    def link(self):
        return f"/comments/{self.submission.id}/_/{self.comment_id}"

    @property
    def title(self):
        return self.submission.title.split("|")[-1].strip()

    def add_tree(self, tree):
        self.tree = tree
        self.submission = tree.node(self.submission_id)

    def update(self, verbosity=1, accuracy=0):
        chain = self.tree.traverse(self.submission)
        if chain is None:
            self.is_archived = True
            chain = [self.submission]
        if chain[-1].id != self.submission.id or not self.comment_id:
            self.comment_id = chain[-1].comments[0].id
        self.submission = chain[-1]
        comments = fetch_comment_tree(self.submission, root_id=self.comment_id,
                                      verbose=False)
        comments.set_accuracy(accuracy)
        comments.verbose = (verbosity > 1)
        self.comment = walk_down_thread(self.side_thread,
                                        comments.comment(self.comment_id)).id
        if len(chain) > 1:
            self.update_count(chain)

    def update_count(self, chain):
        try:
            count = int(self.count.translate(str.maketrans('-', '0', ', ')))
            new_count = self.side_thread.update_count(count, chain)
            self.count = f"{new_count:,}"
            if self.count == "0":
                self.count = "-"
        except (ValueError, TypeError):
            self.count = f"{self.count}*"


def get_counting_history(subreddit, time_limit, verbosity=1):
    now = datetime.datetime.utcnow()
    submissions = subreddit.new(limit=1000)
    tree = {}
    submissions_dict = {}
    new_threads = []
    for count, submission in enumerate(submissions):
        if verbosity > 1 and count % 10 == 0:
            print(f"Processing reddit submission {submission.id}")
        submissions_dict[submission.id] = submission
        title = submission.title.lower()
        if "tidbits" in title or "free talk friday" in title:
            continue
        body = submission.selftext
        try:
            try:
                urls = filter(lambda x: int(x[0], 36) < int(submission.id, 36),
                              find_urls_in_text(body))
                url = next(urls)
                tree[submission.id] = url[0]
            except StopIteration:
                urls = flatten([find_urls_in_text(comment.body) for comment in submission.comments])
                urls = filter(lambda x: int(x[0], 36) < int(submission.id, 36), urls)
                url = next(urls)
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

    parser.add_argument('--accurate', '-a', action='store_true',
                        help=('Use reddit api to fetch newest comments instead of using '
                              'an online archve. Warning: very slow!'))

    args = parser.parse_args()

    verbosity = 1 - args.quiet + args.verbose
    start = datetime.datetime.now()
    subreddit = reddit.subreddit('counting')

    directory_page = subreddit.wiki['directory'].content_md
    directory_page = directory_page.replace("\r\n", "\n")
    document = parse_directory_page(directory_page)

    time_limit = datetime.timedelta(weeks=26.5)
    if verbosity > 0:
        print("Getting history")
    submissions_dict, tree, new_threads = get_counting_history(subreddit, time_limit, verbosity)
    tree = Tree(submissions_dict, tree)

    if verbosity > 0:
        print("Updating tables")
    for paragraph in document:
        if hasattr(paragraph, 'update'):
            paragraph.add_tree(tree)
            paragraph.update(verbosity, accuracy=int(args.accurate))

    with open("directory_file.md", "w") as f:
        print(*document, file=f, sep='\n\n')

    table = Table(flatten([x.rows for x in document if hasattr(x, 'rows')]))
    archived_threads = table.archived_threads()
    with open("archived_threads.md", "w") as f:
        print(archived_threads, file=f)

    new_threads = set([tree.traverse(x)[-1].id for x in new_threads])
    leaf_submissions = set([x.id for x in table.submissions])
    print(*[f"New thread '{r.submission(x).title}' "
            f"at reddit.com/comments/{x}" for x in new_threads - leaf_submissions], sep="\n")

    end = datetime.datetime.now()
    print(end - start)
