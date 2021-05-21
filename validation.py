from thread_navigation import walk_thread
import pandas as pd


def validate_thread(comment, rule):
    """ Walk the thread ending in `comment` and determine if it fulfills `rule`"""
    counts = walk_thread(comment)[::-1]
    df = pd.DataFrame(counts, columns=["body", "username", "timestamp",
                                       "comment_id", "thread_id"])
    df['running_index'] = df.index
    mask = rule(df)
    if mask.all():
        return (True, "")
    else:
        return (False, df[~mask].iloc[0]['comment_id'])


def wait_2_rule(df):
    interval = df.groupby('username').diff()['running_index']
    return interval.isna() | (interval > 2)


def wait_3_rule(df):
    interval = df.groupby('username').diff()['running_index']
    return interval.isna() | (interval > 3)


def slow_rule(df):
    interval = df.groupby('username').diff()['running_index']
    valid_interval = interval.isna() | (interval > 1)
    elapsed_time = df['timestamp'].diff()
    valid_time = elapsed_time.isna() | (elapsed_time >= 60)
    return valid_interval & valid_time


def slower_rule(df):
    interval = df.groupby('username').diff()['running_index']
    valid_interval = interval.isna() | (interval > 1)
    elapsed_time = df.groupby('username').diff()['timestamp']
    valid_time = elapsed_time.isna() | (elapsed_time >= 60 * 60)
    return valid_interval & valid_time
