import datetime
import configparser
import re
import bisect
import itertools
import models
import side_threads
import parsing
import thread_navigation as navigation
import utils

config = configparser.ConfigParser()
config.read('side_threads.ini')
known_threads = config['threads']


def normalise_title(title):
    title = title.translate(str.maketrans('[]', '()'))
    title = title.replace('|', '&#124;')
    if revived := parsing.is_revived(title):
        start, end = revived.span()
        return title[:start] + '(Revival)' + title[end:]
    return title


def title_from_first_comment(submission):
    submission.comment_sort = 'old'
    body = submission.comments[0].body.split('\n')[0]
    return normalise_title(parsing.strip_markdown_links(body))


class Table():
    def __init__(self, rows=[], show_archived=False, kind='directory'):
        labels = {'directory': 'Current', 'archive': 'Last'}
        header = ['⠀' * 10 + 'Name &amp; Initial Thread' + '⠀' * 10,
                  '⠀' * 10 + f'{labels[kind]} Thread' + '⠀' * 10,
                  '⠀' * 3 + '# of Counts' + '⠀' * 3]
        self.header = [' | '.join(header), ':--:|:--:|--:']
        self.rows = rows
        self.show_archived = show_archived

    def __str__(self):
        if self.show_archived:
            return "\n".join(self.header + [str(x) for x in self.rows])
        else:
            return "\n".join(self.header + [str(x) for x in self.rows if not x.archived])

    def append(self, row):
        self.rows.append(row)

    def __add__(self, other):
        return Table(self.rows + other.rows)

    def update(self, tree, verbosity=1):
        for row in self.rows:
            if verbosity > 1:
                print(f"Updating side thread: {row.thread_type}")
            row.update(tree, verbosity=verbosity)

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
    def __init__(self, name, first_thread, title, submission_id, comment_id, count):
        self.archived = False
        self.name = name
        self.first_thread = first_thread
        self.title = normalise_title(title)
        self.submission_id = submission_id
        self.comment_id = comment_id
        self.count_string = count
        self.count = parsing.find_count_in_text(self.count_string.replace("-", "0"))
        self.is_approximate = self.count_string[0] == "~"
        self.starred_count = self.count_string[-1] == "*"
        self.thread_type = known_threads.get(self.first_thread, fallback='default')

    def __str__(self):
        return (f"[{self.name}](/{self.first_thread}) | "
                f"[{self.title}]({self.link}) | {self.count_string}")

    def __lt__(self, other):
        return ((self.count, self.starred_count, self.is_approximate)
                < (other.count, other.starred_count, other.is_approximate))

    @property
    def link(self):
        if self.comment_id is not None:
            return f"/comments/{self.submission_id}/_/{self.comment_id}?context=3"
        else:
            return f"/comments/{self.submission_id}"

    def update_title(self):
        if self.first_thread == self.submission.id:
            self.title = title_from_first_comment(self.submission)
            return
        else:
            sections = self.submission.title.split("|")
            if len(sections) > 1:
                title = '|'.join(sections[1:]).strip()
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

    def update(self, submission_tree, from_archive=False, verbosity=1, deepest_comment=False):
        side_thread = side_threads.get_side_thread(self.thread_type, verbosity)
        if verbosity > 0 and self.thread_type == "default":
            print(f'No rule found for {self.name}. '
                  'Not validating comment contents. '
                  'Assuming n=1000 and no double counting.')

        submission = tree.node(self.submission_id)
        comment, chain, archived = submission_tree.find_latest_comment(submission,
                                                                       side_thread,
                                                                       self.comment_id,
                                                                       verbosity,
                                                                       deepest_comment)
        try:
            comment = comment.walk_up_tree(limit=3)[-1]
        except TypeError:
            pass
        self.comment = comment
        self.comment_id = comment.id
        self.submission = chain[-1]
        self.submission_id = self.submission.id
        self.archived = archived
        was_revival = [parsing.is_revived(x.title) for x in chain]
        if from_archive:
            was_revival[1] = True
        if not all(was_revival[1:]):
            count = side_thread.update_count(self.count, chain, was_revival)
            self.count_string = self.format_count(count)
            if count is not None:
                self.count = count
            else:
                self.starred_count = True
            self.update_title()


class SubmissionTree(models.Tree):
    def __init__(self, submissions, submission_tree, reddit=None, use_pushshift=True):
        self.reddit = reddit
        self.use_pushshift = use_pushshift
        super().__init__(submissions, submission_tree)

    def find_latest_comment(self, old_submission, side_thread, comment_id=None, verbosity=1,
                            deepest_comment=False):
        chain = self.walk_down_tree(old_submission)
        archived = False
        if chain is None:
            archived = True
            chain = [old_submission]
        if len(chain) > 1 or comment_id is None:
            chain[-1].comment_sort = 'old'
            comment_id = chain[-1].comments[0].id
        if deepest_comment:
            comments = chain[-1].comments
            comments.replace_more(limit=None)
            comments = models.CommentTree(comments.list(), reddit=chain[-1]._reddit)
        else:
            comments = navigation.fetch_comment_tree(chain[-1], root_id=comment_id, verbose=False,
                                                     use_pushshift=self.use_pushshift)
            comments.get_missing_replies = False
            comments.add_missing_replies(self.reddit.comment(comment_id))
        comments.verbose = (verbosity > 1)
        comment = comments.comment(comment_id)
        comments.prune(side_thread)
        if deepest_comment:
            new_comment = comments.deepest_node
        else:
            try:
                new_comment = comments.walk_down_tree(comment)[-1]
            except TypeError:
                print(f"failed to update {chain[-1].title}")
                new_comment = comment
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

    return submissions_dict, tree, new_threads


def name_sort(row):
    title = row.name.translate(str.maketrans('', '', '\'"()^/*')).lower()
    return tuple(int(c) if c.isdigit() else c for c in re.split(r'(\d+)', title))


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
                        help=('Use an online archive fetch older comments.'))

    parser.add_argument('--dry-run', action='store_true',
                        help=('Write results to files instead of updating the wiki pages'))

    args = parser.parse_args()
    verbosity = 1 - args.quiet + args.verbose
    start = datetime.datetime.now()
    subreddit = reddit.subreddit('counting')

    wiki_page = subreddit.wiki['directory']
    document = wiki_page.content_md.replace("\r\n", "\n")
    document = parsing.parse_directory_page(document)

    time_limit = datetime.timedelta(days=187)
    if verbosity > 0:
        print("Getting history")
    submissions, submission_tree, new_threads = get_counting_history(subreddit,
                                                                     time_limit,
                                                                     verbosity)
    tree = SubmissionTree(submissions, submission_tree, reddit, use_pushshift=args.pushshift)

    if verbosity > 0:
        print("Updating tables")
    table_counter = 0
    for idx, paragraph in enumerate(document):
        if paragraph[0] == "text":
            document[idx] = paragraph[1]
        elif paragraph[0] == "table":
            table_counter += 1
            table = Table([Row(*x) for x in paragraph[1]])
            table.update(tree, verbosity=verbosity)
            if table_counter == 2:
                table.sort(reverse=True)
            document[idx] = table

    second_last_header = document[-3].lower()
    if "new" in second_last_header and "revived" in second_last_header:
        new_table = document[-2]
    else:
        new_table = Table()
        document = document[:-1] + ['\n## New and Revived Threads', new_table] + document[-1:]

    new_submission_ids = set(tree.walk_down_tree(thread)[-1].id for thread in new_threads)
    full_table = Table(utils.flatten([x.rows for x in document if hasattr(x, 'rows')]))
    known_submissions = set([x.id for x in full_table.submissions])
    new_submission_ids = new_submission_ids - known_submissions
    if new_submission_ids:
        print('Finding new threads')
        for submission_id in new_submission_ids:
            first_submission = tree.walk_up_tree(submission_id)[-1]
            name = f'**{first_submission.title.split("|")[0].strip()}**'
            title = title_from_first_comment(first_submission)
            row = Row(name, first_submission.id, title, first_submission.id, None, '-')
            row.update(tree, deepest_comment=True)
            n_authors = len(set(x.author for x in row.comment.walk_up_tree()))
            if ((row.comment.depth >= 50 and n_authors >= 5)
                    or row.submission_id != first_submission.id):
                new_table.append(row)

    archive_wiki = subreddit.wiki['directory/archive']
    archive = archive_wiki.content_md.replace('\r\n', '\n')
    archive = parsing.parse_directory_page(archive)
    archive_header = archive[0][1]
    archived_rows = [entry[1][:] for entry in archive if entry[0] == 'table']
    archived_rows = [Row(*x) for x in utils.flatten(archived_rows)]
    archived_dict = {x.submission_id: x for x in archived_rows}

    revived_threads = set([x.id for x in tree.leaves]) - new_submission_ids - known_submissions
    print('Finding revived threads')
    updated_archive = False
    for thread in revived_threads:
        chain = tree.walk_up_tree(thread)
        for submission in chain:
            submission.comment_sort = 'old'
            if submission.id in archived_dict:
                row = archived_dict[submission.id]
                row.update(tree, from_archive=True, deepest_comment=True)
                if row.comment.depth >= 20 or (row.submission_id != submission.id):
                    updated_archive = True
                    new_table.append(row)
                    del archived_dict[submission.id]
                break

    new_table.sort(key=name_sort)
    new_page = '\n\n'.join(map(str, document))
    if not args.dry_run:
        wiki_page.edit(new_page, reason="Ran the update script")
    else:
        with open('directory.md', 'w') as f:
            print(new_page, file=f)

    archived_rows = list(archived_dict.values())
    archived_threads = full_table.archived_rows()
    if archived_threads or updated_archive:
        n = len(archived_threads)
        if verbosity > 0:
            print(f'Moving {n} archived thread{"s" if n != 1 else ""}'
                  ' to /r/counting/wiki/directory/archive')
        archived_rows += archived_threads.rows
        archived_rows.sort(key=name_sort)
        splits = ['A', 'D', 'I', 'P', 'T', '[']
        titles = [f'\n### {splits[idx]}-{chr(ord(x) - 1)}' for idx, x in enumerate(splits[1:])]
        titles[0] = archive_header
        keys = [name_sort(x) for x in archived_rows]
        indices = [bisect.bisect_left(keys, (split.lower(),)) for split in splits[1:-1]]
        parts = [Table(list(x), kind='archive') for x in utils.partition(archived_rows, indices)]
        archive = list(itertools.chain.from_iterable(zip(titles, parts)))

        new_archive = '\n\n'.join([str(x) for x in archive])
        if not args.dry_run:
            archive_wiki.edit(new_archive, reason="Ran the update script")
        else:
            with open('archive.md', 'w') as f:
                print(new_archive, file=f)

    end = datetime.datetime.now()
    print(end - start)
