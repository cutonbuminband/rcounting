from models import comment_to_dict
import pandas as pd


def validate_thread(thread, rule='default'):
    """ Walk the thread ending in `comment` and determine if it fulfills `rule`"""
    if rule not in rule_dict:
        raise ValueError(f"Unknown validation rule {rule}")
    rule = rule_dict[rule]
    thread['running_index'] = pd.RangeIndex(len(thread))
    mask = rule(thread)
    if mask.all():
        return (True, "")
    else:
        return (False, thread[~mask].iloc[0]['comment_id'])


def get_history(comment, thread_type):
    "Return enough parents of comment that validation of thread_type is possible"
    weird_threads = {}
    if comment is None:
        return pd.DataFrame()
    if thread_type not in weird_threads:
        return pd.DataFrame([comment_to_dict(comment)])


def update_history(history, comment):
    return history.append(comment_to_dict(comment), ignore_index=True)


def wait_2(df):
    # You can only count after two others have counted
    skip_counts = df.groupby('username')['running_index'].diff()
    return skip_counts.isna() | (skip_counts > 2)


def wait_3(df):
    # You can only count after three others have counted
    skip_counts = df.groupby('username')['running_index'].diff()
    return skip_counts.isna() | (skip_counts > 3)


def once_per_thread(df):
    skip_counts = df.groupby('username')['running_index'].diff()
    return skip_counts.isna()


def slow(df):
    # You cannot reply to yourself and must wait 1 minute since last count
    skip_counts = df.groupby('username')['running_index'].diff()
    valid_skip = skip_counts.isna() | (skip_counts > 1)
    elapsed_time = df['timestamp'].diff()
    valid_time = elapsed_time.isna() | (elapsed_time >= 60)
    return valid_skip & valid_time


def slower(df):
    # You cannot reply to yourself and can only count once per hour
    skip_counts = df.groupby('username')['running_index'].diff()
    valid_skip = skip_counts.isna() | (skip_counts > 1)
    elapsed_time = df.groupby('username')['timestamp'].diff()
    valid_time = elapsed_time.isna() | (elapsed_time >= 60 * 60)
    return valid_skip & valid_time


def slowestest(df):
    # You cannot reply to yourself, you can only count once every 24 hours, and
    # must wait 1 hour since the last count
    skip_counts = df.groupby('username')['running_index'].diff()
    valid_skip = skip_counts.isna() | (skip_counts > 1)
    elapsed_time = df['timestamp'].diff()
    valid_time = elapsed_time.isna() | (elapsed_time >= 60 * 60)
    elapsed_user_time = df.groupby('username')['timestamp'].diff()
    valid_user_time = elapsed_user_time.isna() | (elapsed_user_time >= 60 * 60 * 24)
    return valid_skip & valid_time & valid_user_time


def default(df):
    # You cannot reply to yourself
    skip_counts = df.groupby('username')['running_index'].diff()
    valid_skip = skip_counts.isna() | (skip_counts > 1)
    return valid_skip


rule_dict = {'default': default,
             'wait2': wait_2,
             'wait3': wait_3,
             'once_per_thread': once_per_thread,
             'slow': slow,
             'slower': slower,
             'slowestest': slowestest}


if __name__ == "__main__":
    import praw
    from thread_navigation import fetch_thread
    import argparse
    parser = argparse.ArgumentParser(description='Validate the reddit thread which'
                                     ' contains the comment with id `get_id` according to rule')
    parser.add_argument('comment_id',
                        help='The id of the comment to start logging from')
    parser.add_argument('--rule', choices=rule_dict.keys(),
                        default='default',
                        help='Which rule to apply. Default is no double counting')
    args = parser.parse_args()

    r = praw.Reddit('stats_bot')
    comments = fetch_thread(r.comment(args.comment_id))
    thread = pd.DataFrame(comments)
    result = validate_thread(thread, args.rule)
    if result[0]:
        print('All counts were valid')
    else:
        print(f'Invalid count found at {result[1]}!')
