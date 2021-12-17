#! /usr/bin/python3
import copy
import datetime
import bisect
import itertools

import rcounting.parsing as parsing
import rcounting.utils as utils
import rcounting.thread_directory as td
import rcounting.thread_navigation as tn
from rcounting.reddit_interface import reddit


def main(args):
    verbosity = 1 - args.quiet + args.verbose
    start = datetime.datetime.now()
    subreddit = reddit.subreddit('counting')
    wiki_page, document = td.load_wiki_page(subreddit, 'directory')

    time_limit = datetime.timedelta(days=187)
    if verbosity > 0:
        print("Getting history")
    tree, new_submissions = tn.get_counting_history(subreddit, time_limit, verbosity)

    if verbosity > 0:
        print("Updating tables")
    table_counter = 0
    for idx, paragraph in enumerate(document):
        if paragraph[0] == "text":
            document[idx] = paragraph[1]
        elif paragraph[0] == "table":
            table_counter += 1
            rows = [td.Row(*x) for x in paragraph[1]]
            for row_idx, row in enumerate(rows):
                try:
                    backup_row = copy.copy(row)
                    row.update(tree, verbosity=verbosity)
                except Exception as e:
                    print(f"Unable to update new thread {row.title}")
                    print(e)
                    rows[row_idx] = backup_row
            if table_counter == 2:
                rows.sort(reverse=True)
            document[idx] = rows

    second_last_header = document[-3].lower()
    if "new" in second_last_header and "revived" in second_last_header:
        new_table = document[-2]
    else:
        new_table = []
        document = document[:-1] + ['\n## New and Revived Threads', new_table] + document[-1:]

    new_submission_ids = set(tree.walk_down_tree(submission)[-1].id
                             for submission in new_submissions)
    full_table = utils.flatten([x for x in document if not isinstance(x, str)])
    known_submissions = set([x.submission_id for x in full_table])
    new_submission_ids = new_submission_ids - known_submissions
    if new_submission_ids:
        print('Finding new threads')
        for submission_id in new_submission_ids:
            first_submission = tree.walk_up_tree(submission_id)[-1]
            name = f'**{first_submission.title.split("|")[0].strip()}**'
            try:
                title = td.title_from_first_comment(first_submission)
            except IndexError:
                continue
            row = td.Row(name, first_submission.id, title, first_submission.id, None, '-')
            try:
                row.update(tree, deepest_comment=True)
            except Exception as e:
                print(f"Unable to update new thread {row.title}")
                print(e)
                continue
            n_authors = len(set(x.author for x in row.comment.walk_up_tree()))
            if ((row.comment.depth >= 50 and n_authors >= 5)
                    or row.submission_id != first_submission.id):
                new_table.append(row)

    archive_wiki, archive = td.load_wiki_page(subreddit, 'directory/archive')
    archive_header = archive[0][1]
    archived_rows = [entry[1][:] for entry in archive if entry[0] == 'table']
    archived_rows = [td.Row(*x) for x in utils.flatten(archived_rows)]
    archived_dict = {x.submission_id: x for x in archived_rows}

    revived_threads = set([x.id for x in tree.leaves]) - new_submission_ids - known_submissions
    print('Finding revived threads')
    updated_archive = False
    for thread in revived_threads:
        chain = tree.walk_up_tree(thread)
        for submission in chain:
            submission.comment_sort = 'old'
            if submission.id in archived_dict:
                row = copy.copy(archived_dict[submission.id])
                try:
                    row.update(tree, from_archive=True, deepest_comment=True)
                except Exception as e:
                    print(f"Unable to update new thread {row.title}")
                    print(e)
                    continue
                if row.comment.depth >= 20 or len(chain) > 2:
                    updated_archive = True
                    new_table.append(row)
                    del archived_dict[submission.id]
                break

    new_table.sort(key=lambda x: parsing.name_sort(x.name))
    new_page = '\n\n'.join([x if isinstance(x, str) else td.rows2string(x) for x in document])
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
        parts = [td.rows2string(x, show_archived=True, kind='archive') for x in parts]
        archive = list(itertools.chain.from_iterable(zip(titles, parts)))
        new_archive = '\n\n'.join(archive)
        if not args.dry_run:
            archive_wiki.edit(new_archive, reason="Ran the update script")
        else:
            with open('archive.md', 'w') as f:
                print(new_archive, file=f)

    end = datetime.datetime.now()
    print(end - start)
