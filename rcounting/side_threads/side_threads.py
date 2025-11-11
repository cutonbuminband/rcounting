import logging

import pandas as pd

from rcounting import counters
from rcounting.models import comment_to_dict

from .forms import default_type
from .rules import default_rule

printer = logging.getLogger(__name__)


class SideThread:
    """A side thread class, which consists of a validation part and an update
    part In addition to checking whether a collection of counts is valid
    according to the side thread rule, the class can take a mapping
    comment->count and using this try and identify when errors were made in the
    chain. The class will also attempt to determine how many total counts have
    been made in a given side thread using one of:

    - The comment->count mapping to determine the current count, which is then
    applied to the submission title

    - The update_function parameter, which takes in the current state and
    returns the total number of counts. Sensible approaches for doing this are
    either parsing the current state from the title if it's different from the
    comment->count mapping, or traversing the chain of comments until the last
    known state is reached, and adding on all the comments encountered along
    the way. This is useful for threads which don't have a constant number of
    counts between gets, e.g. tug of war.

    - A standard thread length

    The approaches are listed in low->high priority, so if more than one
    approach is supplied the highest priority one is used.

    """

    def __init__(
        self,
        comment_type=default_type,
        rule=default_rule,
    ):
        self.comment_type = comment_type
        self.rule = rule
        self.history = None

    def is_valid_thread(self, history):
        mask = self.rule.is_valid(history)
        if mask.all():
            return (True, "")
        return (False, history.loc[~mask, "comment_id"].iloc[0])

    def is_valid_count(self, comment, history):
        history = pd.concat([history, pd.DataFrame([comment_to_dict(comment)])], ignore_index=True)
        valid_history = self.is_valid_thread(history)[0]
        valid_count = self.looks_like_count(comment.body)
        valid_user = not counters.is_ignored_counter(str(comment.author))
        return valid_history and valid_count and valid_user, history

    def get_history(self, comment):
        """Fetch enough previous comments to be able to determine whether replies to
        `comment` are valid according to the side thread rules.
        """
        return self.rule.get_history(comment)

    def looks_like_count(self, comment_body):
        return self.comment_type.looks_like_count(comment_body)

    def find_errors(self, history, offset=0):
        errors = self.comment_type.find_errors(history, offset)
        return errors

    def update_count(self, count, chain):
        return self.comment_type.update_count(count, chain)
