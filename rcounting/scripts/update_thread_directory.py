"""A script to update the thread directory"""
import bisect
import copy
import datetime
import itertools
import logging

import click

from rcounting import configure_logging, parsing
from rcounting import thread_directory as td
from rcounting import thread_navigation as tn
from rcounting import utils
from rcounting.reddit_interface import subreddit

printer = logging.getLogger("rcounting")


def document_to_string(document):
    """
    Convert a list of paragraphs to a single string.

    The paragraphs can be either strings or a list of Row objects.
    """
    return "\n\n".join([x if isinstance(x, str) else td.rows2string(x) for x in document])


def document_to_dict(document):
    """
    Generate a dictionary of all the rows present in the document, indexed by their original id
    """

    rows = [entry[1][:] for entry in document if entry[0] == "table"]
    rows = [td.Row(*x) for x in utils.flatten(rows)]
    return {x.submission_id: x for x in rows}


def update_main_document(document, tree):
    """Update every row in the main directory page and sort the second table"""
    table_counter = 0
    for idx, paragraph in enumerate(document):
        if paragraph[0] == "text":
            document[idx] = paragraph[1]
        elif paragraph[0] == "table":
            table_counter += 1
            rows = [td.Row(*x) for x in paragraph[1]]
            for row_idx, row in enumerate(rows):
                try:
                    row.update(tree)
                except Exception:  # pylint: disable=broad-except
                    printer.warning("Unable to update thread %s", row.title)
                    raise
            if table_counter == 2:
                rows.sort(reverse=True)
            document[idx] = rows
    return document


def find_new_submissions(new_submission_ids, tree, threads):
    """
    Make a list of update rows corresponding to new submissions

    If a row cannot be updated or, don't include it.
    The same goes for new submissions with only a few comments on them.
    """
    known_submissions = {x.submission_id for x in threads}
    result = []
    for submission_id in new_submission_ids - known_submissions:
        first_submission = tree.walk_up_tree(submission_id)[-1]
        name = f'**{first_submission.title.split("|")[0].strip()}**'
        try:
            title = td.title_from_first_comment(first_submission)
        except IndexError:
            continue
        row = td.Row(name, first_submission.id, title, first_submission.id, None, "-")
        try:
            row.update(tree, deepest_comment=True)
        except Exception:  # pylint: disable=broad-except
            printer.warning("Unable to update new thread %s", row.title)
            raise
        n_authors = len(set(x.author for x in row.comment.walk_up_tree()))
        is_long_chain = row.comment.depth >= 50 and n_authors >= 5
        if is_long_chain or row.submission_id != first_submission.id:
            result.append(row)
    return result


def find_revived_submissions(new_submission_ids, tree, threads, archive_dict):
    """
    Make a list of updated rows corresponding to revived threads.
    If a row is included here, delete it from the archive.

    If a row cannot be updated or, don't include it.
    The same goes for new submissions with only a few comments on them.
    """
    revivals = []
    known_submissions = {x.submission_id for x in threads}
    revived_threads = {x.id for x in tree.leaves} - new_submission_ids - known_submissions
    for thread in revived_threads:
        chain = tree.walk_up_tree(thread)
        for submission in chain:
            if submission.id in archive_dict:
                row = copy.copy(archive_dict[submission.id])
                try:
                    row.update(tree, from_archive=True, deepest_comment=True)
                except Exception:  # pylint: disable=broad-except
                    printer.warning("Unable to update revived thread %s", row.title)
                    raise
                if row.comment.depth >= 20 or len(chain) > 2:
                    revivals.append(row)
                    del archive_dict[submission.id]
                break
    return revivals, archive_dict


def update_archive(threads, archive_dict, dry_run):
    """Update the archive located at http://reddit.com/r/counting/wiki/directory/archive"""
    archive_wiki = subreddit.wiki["directory/archive"]
    archive = td.load_wiki_page(subreddit, "directory/archive")
    newly_archived_threads = [x for x in threads if x.archived]
    archived_rows = list(archive_dict.values()) + newly_archived_threads
    n = len(newly_archived_threads)
    printer.info(
        "Moving %s archived thread%s to /r/counting/wiki/directory/archive",
        n,
        "s" if n != 1 else "",
    )
    archived_rows.sort(key=lambda x: parsing.name_sort(x.name))
    splits = ["A", "D", "I", "P", "T", "["]
    titles = [f"\n### {splits[idx]}-{chr(ord(x) - 1)}" for idx, x in enumerate(splits[1:])]
    titles[0] = archive[0][1]
    keys = [parsing.name_sort(x.name) for x in archived_rows]
    indices = [bisect.bisect_left(keys, (split.lower(),)) for split in splits[1:-1]]
    parts = utils.partition(archived_rows, indices)
    parts = [td.rows2string(x, show_archived=True, kind="archive") for x in parts]
    archive = list(itertools.chain.from_iterable(zip(titles, parts)))
    new_archive = "\n\n".join(archive)
    if not dry_run:
        archive_wiki.edit(new_archive, reason="Ran the update script")
    else:
        with open("archive.md", "w", encoding="utf8") as f:
            print(new_archive, file=f)


@click.command()
@click.option(
    "--dry-run", is_flag=True, help="Write results to files instead of updating the wiki pages"
)
@click.option("-v", "--verbose", count=True, help="Print more output")
@click.option("-q", "--quiet", is_flag=True)
def update_directory(quiet, verbose, dry_run):
    """
    Update the thread directory located at reddit.com/r/counting/wiki/directory.
    """
    configure_logging.setup(printer, verbose, quiet)
    start = datetime.datetime.now()
    document = td.load_wiki_page(subreddit, "directory")
    printer.info("Getting history")

    tree, new_submissions = tn.fetch_counting_history(subreddit, datetime.timedelta(days=187))

    new_submissions = {tree.walk_down_tree(submission)[-1].id for submission in new_submissions}
    printer.info("Updating tables")
    document = update_main_document(document, tree)

    if "new" in document[-3].lower() and "revived" in document[-3].lower():
        new_table = document[-2]
    else:
        new_table = []
        document = document[:-1] + ["\n## New and Revived Threads", new_table] + document[-1:]

    threads = utils.flatten([x for x in document if not isinstance(x, str)])

    new_table += find_new_submissions(new_submissions, tree, threads)

    archive_dict = document_to_dict(td.load_wiki_page(subreddit, "directory/archive"))

    printer.info("Finding revived threads")
    revived_submissions, archived_dict = find_revived_submissions(
        new_submissions, tree, threads, archive_dict
    )
    new_table += revived_submissions

    new_table.sort(key=lambda x: parsing.name_sort(x.name))
    document[-2] = new_table
    if not dry_run:
        subreddit.wiki["directory"].edit(
            document_to_string(document), reason="Ran the update script"
        )
    else:
        with open("directory.md", "w", encoding="utf8") as f:
            print(document_to_string(document), file=f)

    if [x for x in threads if x.archived] or bool(revived_submissions):
        update_archive(threads, archived_dict, dry_run)
    end = datetime.datetime.now()
    printer.info("Running the script took %s", end - start)


if __name__ == "__main__":
    update_directory()  # pylint: disable=no-value-for-parameter
