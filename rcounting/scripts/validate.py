# pylint: disable=import-outside-toplevel
"""Validate the thread ending at COMMENT_ID according to the specified rule."""

import click
from fuzzywuzzy import fuzz

from rcounting.side_threads import known_thread_ids


def add_whitespace(rule):
    translation = str.maketrans("_-", "  ")
    return str.translate(rule, translation)


def find_side_thread(rule):
    original_rule = rule
    thread_names = list(known_thread_ids.values())
    rule = add_whitespace(rule).lower()
    if rule in thread_names:
        return rule
    else:
        ordered = sorted(thread_names, key=lambda x: fuzz.ratio(x, rule), reverse=True)
        result = None
        while result not in ["a", "b", "c"]:
            print(
                f"No exact match found for {original_rule}. The three closest matches are:\n"
                f"a) {ordered[0]}\n"
                f"b) {ordered[1]}\n"
                f"c) {ordered[2]}\n"
            )
            result = input("select a, b or c\n").strip()
        mapping = {"a": 0, "b": 1, "c": 2}
        return ordered[mapping[result]]


@click.command(no_args_is_help=True)
@click.option(
    "--rule",
    help="Which rule to apply. Default is no double counting",
    default="default",
)
@click.argument("comment_id")
def validate(comment_id, rule):
    """Validate the thread ending at COMMENT_ID according to the specified rule."""
    import pandas as pd

    from rcounting import side_threads as st
    from rcounting import thread_navigation as tn
    from rcounting.reddit_interface import reddit

    comment = reddit.comment(comment_id)
    side_thread_name = find_side_thread(rule)
    print(f"Validating thread: '{comment.submission.title}' according to rule {side_thread_name}")
    comments = pd.DataFrame(tn.fetch_comments(comment))
    side_thread = st.get_side_thread(side_thread_name)
    result = side_thread.is_valid_thread(comments)
    if result[0]:
        print("All counts were valid")
        errors = side_thread.find_errors(comments)
        if errors.empty:
            print("The last count in the the thread has the correct value")
        else:
            print(
                f"The last count in the thread has an incorrect value. "
                f"Earlier errors can be found at {errors}"
            )
            try:
                target_count = side_thread.comment_type.comment_to_count(
                    side_thread.history.loc[0, "body"]
                ) + len(side_thread.history)
                target_string = side_thread.comment_type.count_to_comment(target_count)
                print(f"The last count should have been {target_string}")
            except:
                filename = "thread.csv"
                print(
                    f"Saving thread data to disk at {filename} to help finding "
                    f"the correct value of the last count"
                )

    else:
        print(f"Invalid count found at http://reddit.com{reddit.comment(result[1]).permalink}!")
