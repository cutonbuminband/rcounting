if __name__ == "__main__":
    import pandas as pd
    from side_threads import get_side_thread
    from reddit_interface import reddit
    from thread_navigation import fetch_thread
    import argparse

    rule_dict = {'default': 'default',
                 'wait2': 'wait 2',
                 'wait3': 'wait 3',
                 'wait9': 'wait 9',
                 'wait10': 'wait 10',
                 'once_per_thread': 'once per thread',
                 'slow': 'slow',
                 'slower': 'slower',
                 'slowestest': 'slowestest',
                 'only_double_counting': 'only_double_counting'}

    parser = argparse.ArgumentParser(description='Validate the reddit thread which'
                                     ' contains the comment with id `comment_id` according to rule')
    parser.add_argument('comment_id',
                        help='The id of the comment to start logging from')
    parser.add_argument('--rule', choices=rule_dict.keys(),
                        default='default',
                        help='Which rule to apply. Default is no double counting')
    args = parser.parse_args()

    comments = fetch_thread(reddit.comment(args.comment_id))
    thread = pd.DataFrame(comments)
    side_thread = get_side_thread(rule_dict[args.rule])
    result = side_thread.is_valid_thread(thread)
    if result[0]:
        print('All counts were valid')
    else:
        print(f'Invalid count found at reddit.com{reddit.comment(result[1]).permalink}!')
