import datetime
import configparser
import bisect
import itertools
import models
import side_threads
import parsing
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


def rows2string(rows=[], show_archived=False, kind='directory'):
    labels = {'directory': 'Current', 'archive': 'Last'}
    header = ['⠀' * 10 + 'Name &amp; Initial Thread' + '⠀' * 10,
              '⠀' * 10 + f'{labels[kind]} Thread' + '⠀' * 10,
              '⠀' * 3 + '# of Counts' + '⠀' * 3]
    header = [' | '.join(header), ':--:|:--:|--:']
    if not show_archived:
        rows = [x for x in rows if not x.archived]
    return "\n".join(header + [str(x) for x in rows])


class Row():
    def __init__(self, name, first_thread, title, submission_id, comment_id, count):
        self.archived = False
        self.name = name
        self.first_thread = first_thread
        self.title = normalise_title(title)
        self.initial_submission_id = submission_id
        self.initial_comment_id = comment_id
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

    def update_title(self):
        if self.first_thread == self.submission.id:
            self.title = title_from_first_comment(self.submission)
            return
        else:
            sections = self.submission.title.split("|")
            if len(sections) > 1:
                title = '|'.join(sections[1:]).strip()
            else:
                title = title_from_first_comment(self.submission)
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
        if verbosity > 1:
            print(f"Updating side thread: {row.thread_type}")
        if verbosity > 0 and self.thread_type == "default":
            print(f'No rule found for {self.name}. '
                  'Not validating comment contents. '
                  'Assuming n=1000 and no double counting.')

        chain = tree.walk_down_tree(tree.node(self.submission_id))
        self.submission = chain[-1]
        if tree.is_archived(self.submission):
            self.archived = True
            return

        if len(chain) > 1:
            self.initial_comment_id = None

        comments = models.CommentTree(reddit=tree.reddit)
        if deepest_comment:
            for comment in self.submission.comments:
                comments.add_missing_replies(comment)
        else:
            if self.comment_id is None:
                comment = next(filter(side_thread.looks_like_count, self.submission.comments))
            else:
                comment = tree.reddit.comment(self.comment_id)
            comments.add_missing_replies(comment)
        comments.get_missing_replies = False
        comments.verbose = (verbosity > 1)
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


def load_wiki_page(subreddit, location):
    wiki_page = subreddit.wiki[location]
    document = wiki_page.content_md.replace("\r\n", "\n")
    return wiki_page, parsing.parse_directory_page(document)


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
    wiki_page, document = load_wiki_page(subreddit, 'directory')

    time_limit = datetime.timedelta(days=187)
    if verbosity > 0:
        print("Getting history")
    submissions, submission_tree, new_threads = get_counting_history(subreddit,
                                                                     time_limit,
                                                                     verbosity)
    tree = models.SubmissionTree(submissions, submission_tree, reddit)

    if verbosity > 0:
        print("Updating tables")
    table_counter = 0
    for idx, paragraph in enumerate(document):
        if paragraph[0] == "text":
            document[idx] = paragraph[1]
        elif paragraph[0] == "table":
            table_counter += 1
            rows = [Row(*x) for x in paragraph[1]]
            for row in rows:
                row.update(tree, verbosity=verbosity)
            if table_counter == 2:
                rows.sort(reverse=True)
            document[idx] = rows

    second_last_header = document[-3].lower()
    if "new" in second_last_header and "revived" in second_last_header:
        new_table = document[-2]
    else:
        new_table = []
        document = document[:-1] + ['\n## New and Revived Threads', new_table] + document[-1:]

    new_submission_ids = set(tree.walk_down_tree(thread)[-1].id for thread in new_threads)
    full_table = utils.flatten([x for x in document if not isinstance(x, str)])
    known_submissions = set([x.submission_id for x in full_table])
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

    archive_wiki, archive = load_wiki_page(subreddit, 'directory/archive')
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
                if row.comment.depth >= 20 or len(chain) > 2:
                    updated_archive = True
                    new_table.append(row)
                    del archived_dict[submission.id]
                break

    new_table.sort(key=lambda x: parsing.name_sort(x.name))
    new_page = '\n\n'.join([x if isinstance(x, str) else rows2string(x) for x in document])
    if not args.dry_run:
        wiki_page.edit(new_page, reason="Ran the update script")
    else:
        with open('directory.md', 'w') as f:
            print(new_page, file=f)

    archived_rows = list(archived_dict.values())
    new_archived_threads = [x for x in full_table if x.archived]
    if new_archived_threads or updated_archive:
        n = len(new_archived_threads)
        if verbosity > 0:
            print(f'Moving {n} archived thread{"s" if n != 1 else ""}'
                  ' to /r/counting/wiki/directory/archive')
        archived_rows += new_archived_threads
        archived_rows.sort(key=lambda x: parsing.name_sort(x.name))
        splits = ['A', 'D', 'I', 'P', 'T', '[']
        titles = [f'\n### {splits[idx]}-{chr(ord(x) - 1)}' for idx, x in enumerate(splits[1:])]
        titles[0] = archive_header
        keys = [parsing.name_sort(x.name) for x in archived_rows]
        indices = [bisect.bisect_left(keys, (split.lower(),)) for split in splits[1:-1]]
        parts = utils.partition(archived_rows, indices)
        parts = [rows2string(x, show_archived=True, kind='archive') for x in parts]
        archive = list(itertools.chain.from_iterable(zip(titles, parts)))
        new_archive = '\n\n'.join(archive)
        if not args.dry_run:
            archive_wiki.edit(new_archive, reason="Ran the update script")
        else:
            with open('archive.md', 'w') as f:
                print(new_archive, file=f)

    end = datetime.datetime.now()
    print(end - start)
