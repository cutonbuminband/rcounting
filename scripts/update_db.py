#! /usr/bin/python3
import pandas as pd
import sqlite3
import rcounting.parsing as parsing
from pathlib import Path
from rcounting.reddit_interface import reddit
import rcounting.thread_navigation as tn
import rcounting.models as models

db = sqlite3.connect(Path('results/counting.sqlite'))

subreddit = reddit.subreddit('counting')
wiki_page = subreddit.wiki['directory']
document = wiki_page.content_md.replace("\r\n", "\n")
result = parsing.parse_directory_page(document)

comment_id = result[1][1][0][4]
comment = reddit.comment(comment_id)
comment = tn.find_previous_get(comment)

# The database stores the id of the last complete update, so we can keep going
# until we hit that
last_submission_id = pd.read_sql("select submission_id from last_submission", db).iat[-1, 0]
known_submission_ids = pd.read_sql("select * from submissions", db)['submission_id'].tolist()
is_updated = False
while comment.submission.id != last_submission_id:
    is_updated = True
    if comment.submission.id not in known_submission_ids:
        comments = pd.DataFrame(tn.fetch_comments(comment, use_pushshift=False))
        comments = comments[['comment_id', 'username', 'timestamp', 'submission_id', 'body']]
        submission = pd.DataFrame([models.Submission(comment.submission).to_dict()])
        submission = submission[['submission_id', 'username', 'timestamp', 'title', 'body']]
        n = (comments['body'].apply(lambda x: parsing.find_count_in_text(x, raise_exceptions=False))
             - comments.index).median()
        submission['basecount'] = int(n - (n % 1000))
        comments.to_sql('comments', db, index_label='position', if_exists='append')
        submission.to_sql('submissions', db, index=False, if_exists='append')
    comment = tn.find_previous_get(comment)

if is_updated:
    new_submission_id = pd.read_sql("select submission_id "
                                    "from submissions order by basecount", db).iat[-1, 0]
    cursor = db.cursor()
    cursor.execute('insert into last_submission values(?)', (new_submission_id,))
    db.commit()
    cursor.close()
else:
    print('Db already up to date!')
