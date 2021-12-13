from pathlib import Path
import pandas as pd
import sqlite3
from models import Submission as OfflineSubmission
from cleanup import find_missing_ids


def get_submission_list(df, db):
    exceptions = {'uuikz': 0,
                  'wwe09': 16691}

    df['integer_submission_id'] = df['submission_id'].apply(lambda x: int(x, 36))
    delta = df['integer_submission_id'].diff()
    mask = (delta.isna()) | (delta != 0)
    submission_ids = df.loc[mask, 'submission_id']
    submissions = []
    counter = 0
    for submission_id in submission_ids:
        submission = reddit.submission(submission_id)
        submissions.append(submission)
        if counter % 10 == 0:
            print(submission.title)
        counter += 1
    submission_df = pd.DataFrame([OfflineSubmission(x).to_dict() for x in submissions])
    submission_df['basecount'] = (submission_df.index + 15) * 1000
    for exception in exceptions:
        basecount = exceptions[exception]
        submission_df.loc[submission_df['submission_id'] == exception, 'basecount'] = basecount

    return submission_df


def submission_to_table_name(x):
    return f"table_{x}"


def table_to_submission_id(x):
    return x[6:]


if __name__ == "__main__":
    from thread_navigation import fetch_comments
    from reddit_interface import reddit

    df = pd.read_csv(Path('~/Downloads/ALL_clean.csv'), usecols=[1, 2, 3, 4, 5])
    if df['comment_id'].isna().any():
        df = find_missing_ids(df)
    db = sqlite3.connect(Path('results/counting.sqlite'))
    tables = pd.read_sql_query("SELECT name from sqlite_master WHERE type='table'", db)
    print(f"{len(tables) - 1} submissions already logged")
    if not (tables['name'] == 'submissions').any():
        submissions = get_submission_list(df)
        submissions.to_sql('submissions', db, index=False)
    submissions = pd.read_sql_query("select submission_id from submissions", db)

    archived_submissions = df.groupby('submission_id')
    for submission, submission_df in archived_submissions:
        if (tables['name'] == submission_to_table_name(submission)).any():
            continue
        print(f"Writing submission {submission} to db")
        comment = reddit.comment(submission_df.iloc[-1]['comment_id'])
        comments = fetch_comments(comment)

        new_df = pd.DataFrame(comments)
        merged_df = pd.merge(submission_df.drop(['timestamp', 'submission_id'], axis=1),
                             new_df,
                             on='comment_id', how='right', suffixes=('_x', None))
        missing = merged_df.username == 'None'
        merged_df.loc[missing, 'username'] = merged_df.loc[missing, 'username_x']
        merged_df.loc[merged_df['username'].isna(), 'username'] = '[deleted]'
        merged_df.drop(['username_x', 'count'], axis=1, inplace=True)
        merged_df.index.name = 'position'
        merged_df.to_sql(submission_to_table_name(submission), db)
