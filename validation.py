from thread_navigation import walk_up_thread
import pandas as pd


def validate_thread(leaf_comment=None, thread=None, rule='default'):
    """ Walk the thread ending in `comment` and determine if it fulfills `rule`"""
    if thread is None:
        thread = walk_up_thread(leaf_comment)
    rule_dict = {'default': default,
                 'wait 2': wait_2,
                 'wait 3': wait_3,
                 'once per thread': once_per_thread,
                 'slow': slow,
                 'slower': slower,
                 'slowestest': slowestest}
    if rule not in rule_dict:
        raise ValueError(f"Unknown validation rule {rule}")
    rule = rule_dict[rule]
    if not isinstance(thread, pd.DataFrame):
        thread = pd.DataFrame(thread)
    thread['running_index'] = pd.RangeIndex(len(thread))
    mask = rule(thread)
    if mask.all():
        return (True, "")
    else:
        return (False, thread[~mask].iloc[0]['comment_id'])


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

x
def default(df):
    # You cannot reply to yourself
    skip_counts = df.groupby('username')['running_index'].diff()
    valid_skip = skip_counts.isna() | (skip_counts > 1)
    return valid_skip
