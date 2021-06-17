# encoding=utf8
import os
from pathlib import Path
from parsing import post_to_count
from thread_navigation import psaw_get_tree, walk_up_thread, find_previous_get
import pandas as pd
from aliases import apply_alias


def log_one_thread(leaf_comment, use_psaw=False):
    basecount = post_to_count(leaf_comment) - 1000
    hog_path = Path(f'results/LOG_{basecount}to{basecount+1000}.csv')
    hoc_path = Path(f'results/TABLE_{basecount}to{basecount+1000}.csv')
    if os.path.isfile(hoc_path):
        return

    if use_psaw:
        try:
            tree = psaw_get_tree(leaf_comment.submission)
            comments = walk_up_thread(tree.comment(leaf_comment.id))
        except IndexError:
            comments = walk_up_thread(leaf_comment)
    else:
        comments = walk_up_thread(leaf_comment)

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
    t = pd.to_timedelta(df.iloc[-1].timestamp - df.iloc[0].timestamp, unit='s').components
    table = df.iloc[1:]['hoc_username'].value_counts().to_frame().reset_index()
    data = table.set_index(table.index + 1).to_csv(None, sep='|', header=0)

    header = (f'Thread Participation Chart for {thread_title}\n\nRank|Username|Counts\n---|---|---')
    footer = (f'It took {len(table)} counters {t.days} days {t.hours} hours '
              f'{t.minutes} mins {t.seconds} secs to complete this thread. '
              f'Bold is the user with the get\n'
              f'total counts in this chain logged: {len(df) - 1}')
    return '\n'.join([header, data, footer])


if __name__ == '__main__':
    from datetime import datetime
    import praw
    import argparse
    parser = argparse.ArgumentParser(description='Log the reddit thread which'
                                     ' contains the comment with id `get_id`')
    parser.add_argument('get_id',
                        help='The id of the leaf comment (get) to start logging from')
    parser.add_argument('-n', type=int,
                        default=1,
                        help='The number of threads to log. Default 1')
    parser.add_argument('--use_psaw', action='store_true',
                        help='Use pushshift to get reddit comments in bulk')
    args = parser.parse_args()

    get_id = args.get_id
    n = args.n
    engine = args.engine
    print(f'Logging {n} reddit thread{"s" if n > 1 else ""} starting at {get_id}'
          ' and moving backwards')

    t_start = datetime.now()
    r = praw.Reddit('stats_bot')
    get_comment = r.comment(get_id)
    for i in range(n):
        print(f'Logging thread {i + 1} out of {n}')
        log_one_thread(get_comment, engine)
        get_comment = find_previous_get(get_comment)
    elapsed_time = datetime.now() - t_start
    print(f'Running the script took {elapsed_time}')
