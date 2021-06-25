if __name__ == "__main__":
    import pandas as pd
    from side_threads import get_side_thread

    rule_dict = {'default': 'default',
                 'wait2': 'wait 2',
                 'wait3': 'wait 3',
                 'wait10': 'wait 10',
                 'once_per_thread': 'once per thread',
                 'slow': 'slow',
                 'slower': 'slower',
                 'slowestest': 'slowestest'}

    import praw
    from thread_navigation import fetch_thread
    import argparse
    parser = argparse.ArgumentParser(description='Validate the reddit thread which'
                                     ' contains the comment with id `comment_id` according to rule')
    parser.add_argument('comment_id',
                        help='The id of the comment to start logging from')
    parser.add_argument('--rule', choices=rule_dict.keys(),
                        default='default',
                        help='Which rule to apply. Default is no double counting')
    args = parser.parse_args()

    r = praw.Reddit('stats_bot')
    comments = fetch_thread(r.comment(args.comment_id))
    thread = pd.DataFrame(comments)
    side_thread = get_side_thread(rule_dict[args.rule])
    side_thread.history = thread
    result = side_thread.is_valid()
    if result[0]:
        print('All counts were valid')
    else:
        print(f'Invalid count found at {result[1]}!')
