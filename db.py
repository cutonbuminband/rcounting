from pathlib import Path
import pandas as pd
import sqlite3
import praw
from models import Submission as OfflineSubmission
from cleanup import find_missing_ids


def get_thread_list(df, db):
    exceptions = {'uuikz': 0,
                  'wwe09': 16691}

    df['integer_thread_id'] = df['thread_id'].apply(lambda x: int(x, 36))
    delta = df['integer_thread_id'].diff()
    mask = (delta.isna()) | (delta != 0)
    thread_ids = df.loc[mask, 'thread_id']
    submissions = []
    counter = 0
    for thread_id in thread_ids:
        submission = r.submission(thread_id)
        submissions.append(submission)
        if counter % 10 == 0:
            print(submission.title)
        counter += 1
    thread_df = pd.DataFrame([OfflineSubmission(x).to_dict() for x in submissions])
    thread_df['basecount'] = (thread_df.index + 15) * 1000
    for exception in exceptions:
        thread_df.loc[thread_df['thread_id'] == exception, 'basecount'] = exceptions[exception]

    return thread_df


def thread_to_table_name(x):
    return f"table_{x}"


def table_to_thread_name(x):
    return x[6:]


if __name__ == "__main__":
    from thread_navigation import psaw_get_comments, walk_up_thread
    r = praw.Reddit("stats_bot")

    df = pd.read_csv(Path('~/Downloads/ALL_clean.csv'), usecols=[1, 2, 3, 4, 5])
    if df['comment_id'].isna().any():
        df = find_missing_ids(df)
    db = sqlite3.connect(Path('results/counting.sqlite'))
    tables = pd.read_sql_query("SELECT name from sqlite_master WHERE type='table'", db)
    print(f"{len(tables) - 1} threads already logged")
    if not (tables['name'] == 'threads').any():
        threads = get_thread_list(df)
        threads.to_sql('threads', db, index=False)
    threads = pd.read_sql_query("select thread_id from threads", db)

    archived_threads = df.groupby('thread_id')
    for thread, thread_df in archived_threads:
        if (tables['name'] == thread_to_table_name(thread)).any():
            continue
        print(f"Writing thread {thread} to db")
        comment = r.comment(thread_df.iloc[-1]['comment_id'])
        try:
            tree = psaw_get_comments(comment.submission)
            comments = walk_up_thread(tree.comment(comment.id))
        except KeyError:
            comments = walk_up_thread(comment)

        new_df = pd.DataFrame(comments)
        merged_df = pd.merge(thread_df.drop(['timestamp', 'thread_id'], axis=1),
                             new_df,
                             on='comment_id', how='right', suffixes=('_x', None))
        missing = merged_df.username == 'None'
        merged_df.loc[missing, 'username'] = merged_df.loc[missing, 'username_x']
        merged_df.loc[merged_df['username'].isna(), 'username'] = '[deleted]'
        merged_df.drop(['username_x', 'count'], axis=1, inplace=True)
        merged_df.index.name = 'thread_position'
        merged_df.to_sql(thread_to_table_name(thread), db)
