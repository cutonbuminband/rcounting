# pylint: disable=import-outside-toplevel
"""Validate the thread ending at COMMENT_ID according to the specified rule."""

import click
from fuzzywuzzy import fuzz

from rcounting.side_threads import known_thread_names


def add_whitespace(rule):
    translation = str.maketrans("_-", "  ")
    return str.translate(rule, translation)


def find_side_thread(rule):
    original_rule = rule
    rule = add_whitespace(rule).lower()
    if rule in known_thread_names:
        return rule
    else:
        ordered = sorted(known_thread_names, key=lambda x: fuzz.ratio(x, rule), reverse=True)
        result = None
        print(
            f"No exact match found for {original_rule}. The three closest matches are:\n"
            f"a) {ordered[0]}\n"
            f"b) {ordered[1]}\n"
            f"c) {ordered[2]}\n"
        )
        result = input("select a, b or c, or try writing the rule again\n").strip()
        mapping = {"a": 0, "b": 1, "c": 2}
        if result in mapping:
            return ordered[mapping[result]]
        else:
            return find_side_thread(result)


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
                last_comment = comments.iloc[-1].loc["body"]
                target_string = side_thread.comment_type.find_correct_count(comments)
                print(
                    f"The last comment should have been {target_string} instead of {last_comment}"
                )
            except:
                filename = "thread.csv"
                print(
                    f"Saving thread data to disk at {filename} to help finding "
                    f"the correct value of the last count"
                )

    else:
        print(f"Invalid count found at http://reddit.com{reddit.comment(result[1]).permalink}!")
