# encoding=utf8
import os
from pathlib import Path
from parsing import post_to_count
from thread_navigation import fetch_thread, find_previous_get
import pandas as pd
from aliases import apply_alias
from utils import format_timedelta


def log_one_thread(leaf_comment):
    basecount = post_to_count(leaf_comment) - 1000
    hog_path = Path(f'results/LOG_{basecount}to{basecount+1000}.csv')
    hoc_path = Path(f'results/TABLE_{basecount}to{basecount+1000}.csv')
    if os.path.isfile(hoc_path):
        return

    comments = fetch_thread(leaf_comment)

    title = leaf_comment.submission.title
    df = pd.DataFrame(comments)
    hog_columns = ['username', 'timestamp', 'comment_id', 'thread_id']
    df.set_index(df.index + basecount)[hog_columns].iloc[1:].to_csv(hog_path, header=None)
    with open(hoc_path, 'w') as f:
        print(hoc_string(df, title), file=f)
    return df


def hoc_string(df, thread_title):
    getter = apply_alias(df.iloc[-1]['username'])

    def hoc_format(username):
        username = apply_alias(username)
        return f'**/u/{username}**' if username == getter else f'/u/{username}'

    df['hoc_username'] = df['username'].apply(hoc_format)
    dt = pd.to_timedelta(df.iloc[-1].timestamp - df.iloc[0].timestamp, unit='s')
    table = df.iloc[1:]['hoc_username'].value_counts().to_frame().reset_index()
    data = table.set_index(table.index + 1).to_csv(None, sep='|', header=0)

    header = (f'Thread Participation Chart for {thread_title}\n\nRank|Username|Counts\n---|---|---')
    footer = (f'It took {len(table)} counters {format_timedelta(dt)} to complete this thread. '
              f'Bold is the user with the get\n'
              f'total counts in this chain logged: {len(df) - 1}')
    return '\n'.join([header, data, footer])


if __name__ == '__main__':
    from datetime import datetime
    from reddit_interface import reddit
    import argparse
    parser = argparse.ArgumentParser(description='Log the reddit thread which'
                                     ' contains the comment with id `get_id`')
    parser.add_argument('get_id',
                        help='The id of the leaf comment (get) to start logging from')
    parser.add_argument('-n', type=int,
                        default=1,
                        help='The number of threads to log. Default 1')
    args = parser.parse_args()

    print(f'Logging {args.n} reddit thread{"s" if args.n > 1 else ""} '
          f'starting at {args.get_id} and moving backwards')

    t_start = datetime.now()
    leaf_comment = reddit.comment(args.get_id)
    for i in range(args.n):
        print(f'Logging thread {i + 1} out of {args.n}')
        log_one_thread(leaf_comment)
        leaf_comment = find_previous_get(leaf_comment)
    elapsed_time = datetime.now() - t_start
    print(f'Running the script took {elapsed_time}')
