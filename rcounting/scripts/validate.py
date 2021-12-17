#! /usr/bin/python3
import pandas as pd

import rcounting.side_threads as st
import rcounting.thread_navigation as tn
from rcounting.reddit_interface import reddit

rule_dict = {'default': 'default',
             'wait2': 'wait 2',
             'wait3': 'wait 3',
             'wait9': 'wait 9',
             'wait10': 'wait 10',
             'once_per_thread': 'once per thread',
             'slow': 'slow',
             'slower': 'slower',
             'slowestest': 'slowestest',
             'only_double_counting': 'only double counting'}


def main(args):
    comment = reddit.comment(args.comment_id)
    print(f"Validating thread: '{comment.submission.title}' according to rule {args.rule}")
    comments = pd.DataFrame(tn.fetch_comments(comment, use_pushshift=False))
    side_thread = st.get_side_thread(rule_dict[args.rule])
    result = side_thread.is_valid_thread(comments)
    if result[0]:
        print('All counts were valid')
    else:
        print(f'Invalid count found at reddit.com{reddit.comment(result[1]).permalink}!')
