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


def wait_2(df):
    # You can only count after two others have counted
    skip_counts = df.groupby('username').diff()['running_index']
    return skip_counts.isna() | (skip_counts > 2)


def wait_3(df):
    # You can only count after three others have counted
    skip_counts = df.groupby('username').diff()['running_index']
    return skip_counts.isna() | (skip_counts > 3)


def once_per_thread(df):
    skip_counts = df.groupby('username').diff()['running_index']
    return skip_counts.isna()


def slow(df):
    # You cannot reply to yourself and must wait 1 minute since last count
    skip_counts = df.groupby('username').diff()['running_index']
    valid_skip = skip_counts.isna() | (skip_counts > 1)
    elapsed_time = df['timestamp'].diff()
    valid_time = elapsed_time.isna() | (elapsed_time >= 60)
    return valid_skip & valid_time


def slower(df):
    # You cannot reply to yourself and can only count once per hour
    skip_counts = df.groupby('username').diff()['running_index']
    valid_skip = skip_counts.isna() | (skip_counts > 1)
    elapsed_time = df.groupby('username').diff()['timestamp']
    valid_time = elapsed_time.isna() | (elapsed_time >= 60 * 60)
    return valid_skip & valid_time
