# pylint: disable=import-outside-toplevel
"""A script to update the thread directory"""
import datetime
import logging

import click

from rcounting import configure_logging

printer = logging.getLogger("rcounting")

# Sometimes people include spurious links in posts. Here's a list of
# (link_from, link_to) which should be ignored.
spurious_edges = [
    ("r2h98h", "nyg22w"),
    ("11va8n8", "11siye8"),
    ("12p80i5", "11nouml"),
    ("1d1pyvo", "1crh3wj"),
    ("1e37w8n", "1diqgjm"),
]

# And some posts just shouldn't be tracked - here's a hardcoded list of those
spurious_nodes = ("12x4vpl", "13s4tu6")


@click.command()
@click.option(
    "--dry-run", is_flag=True, help="Write results to files instead of updating the wiki pages"
)
@click.option("-v", "--verbose", count=True, help="Print more output")
@click.option("-q", "--quiet", is_flag=True)
@click.option("--sleep", default=0)
def update_directory(quiet, verbose, dry_run, sleep):
    """
    Update the thread directory located at reddit.com/r/counting/wiki/directory.
    """

    from rcounting import thread_directory as td
    from rcounting import thread_navigation as tn
    from rcounting.reddit_interface import subreddit

    configure_logging.setup(printer, verbose, quiet)
    start = datetime.datetime.now()
    printer.info("Getting history")
    tree, new_submissions = tn.fetch_counting_history(subreddit, datetime.timedelta(days=187))
    for edge in spurious_edges:
        tree.delete_edge(*edge)

    for node in spurious_nodes:
        if node in tree:
            tree.delete_node(node)
    new_submissions = [x for x in new_submissions if x.id not in spurious_nodes]
    new_submission_ids = {tree.walk_down_tree(submission)[-1].id for submission in new_submissions}

    directory = td.load_wiki_page(subreddit, "directory")
    archive = td.load_wiki_page(subreddit, "directory/archive", kind="archive")
    directory.set_archive(archive)
    directory.update(tree, new_submission_ids, sleep)

    if not dry_run:
        subreddit.wiki["directory"].edit(content=str(directory), reason="Ran the update script")
    else:
        with open("directory.md", "w", encoding="utf8") as f:
            print(directory, file=f)

    if directory.updated_archive:
        archive = "\n\n".join([archive.header, directory.archive2string()])
        if not dry_run:
            subreddit.wiki["directory/archive"].edit(
                content=archive, reason="Ran the update script"
            )
        else:
            with open("archive.md", "w", encoding="utf8") as f:
                print(archive, file=f)
    end = datetime.datetime.now()
    printer.info("Running the script took %s", end - start)


if __name__ == "__main__":
    update_directory()  # pylint: disable=no-value-for-parameter
