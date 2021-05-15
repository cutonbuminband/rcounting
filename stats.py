# encoding=utf8
import datetime
from collections import defaultdict
from pathlib import PurePath
from urllib.parse import urlparse
import re
import praw
from praw.models import MoreComments
from praw.exceptions import ClientException

id_get = 'e1slbz0'
reddit_instance = praw.Reddit("stats_bot")

def get_count_from_text(body):
    try:
        regex = '^\[?[\d, ]*\]?'
        count = re.findall(regex, body)[0]
        stripped_count = count.translate(str.maketrans('', '', '[] ,'))
        return int(stripped_count)
    except:
        raise ValueError(f"Unable to extract count from comment body: {body}")

def find_urls_in_body(body):
    url_regex = "(?:www\.)?(?:old\.)?reddit.com(/[^ )]+)(?:[ )]|$)"
    markdown_link_regex = '\[[^][]+]\(([^()]+)\)'
    urls = re.findall(url_regex, body)
    if not urls:
        urls = re.findall(markdown_link_regex, body)
    return urls

def find_previous_get(comment):
    thread = comment.submission
    urls = find_urls_in_body(thread.selftext)
    if not urls:
        urls = [find_url_in_body(comment.body) for comment in thread.comments]
        urls = [url for comment_urls in urls for url in comment_urls]
    url = urls[0]
    path_components = PurePath(urlparse(url).path).parts
    new_id_get = path_components[-1]
    comment = reddit_instance.comment(new_id_get)
    new_get = find_get_from_comment(comment)
    print(new_get.submission, new_get.id)
    return new_get

def find_get_from_comment(comment):
    count = get_count_from_text(comment.body)
    if count % 1000 == 0:
        return comment
    replies = comment.replies.list()
    for reply in replies:
        if not isinstance(reply, MoreComments):
            count = get_count_from_text(reply.body)
            if count % 1000 == 0:
                return reply
    raise(ValueError, "Unable to locate get near comment id {comment.id}")

def parse_data(comment):
    return (comment.body, str(comment.author), comment.created_utc,
            comment.id, str(comment.submission))

def extract_gets_and_assists(id_get, n_threads=2):
    gets = []
    assists = []
    comment = reddit_instance.comment(id_get)
    comment.refresh()
    for n in range(n_threads):
        rows = []
        for i in range(3):
            rows.append(parse_data(comment))
            comment = comment.parent()
        gets.append(rows[0] + (rows[0][2] - rows[1][2],))
        assists.append(rows[1] + (rows[1][2] - rows[2][2],))
        comment = find_previous_get(comment)
    return gets, assists

def walk_thread(leaf_comment):
    comments = []
    refresh_counter = 0
    comment = leaf_comment
    refresh_counter = 0
    while not comment.is_root:
        # By refreshing, we request the previous 9 comments in a single go. So
        # the next calls to comment.parent() and parse_data() don't result in a
        # network request. If the refresh fails, we just go one by one
        if refresh_counter % 9 == 0:
            try:
                comment.refresh()
            except ClientException:
                print(f"Broken chain detected at {comment.id}")
                print(f"Fetching the next 9 comments one by one")
        comments.append(parse_data(comment))
        print(comment.id)
        comment = comment.parent()
        refresh_counter += 1
    # comments.append(parse_data(comment))
    return comments[::-1]

if __name__ == "__main__":
    t_start = datetime.datetime.now()
    get_comment = reddit_instance.comment(id_get)
    thread = get_comment.submission
    title = thread.title

    counts = walk_thread(get_comment)
    base_count = get_count_from_text(counts[0][0])
    thread_duration = datetime.timedelta(seconds=counts[-1][2] - counts[0][2])
    days = thread_duration.days
    hours, mins, secs = str(thread_duration).split(':')[-3:]

    counters = defaultdict(int)
    with open("thread_log.csv", "w") as hog_file:
        for idx, x in enumerate(counts[1:]):
            counters['/u/' + x[1]] += 1
            try:
                print(base_count + idx + 1, *x[1:], sep=",", file=hog_file)
            except:
                continue

    with open("thread_participation.csv", "w") as hoc_file:
        hoc_file.write("Thread Participation Chart for " + title + "\n\n")
        hoc_file.write("Rank|Username|Counts\n")
        hoc_file.write("---|---|---\n")
        counters = sorted(counters.items(), key=lambda kv: (kv[1], kv[0]))[::-1]
        total_counts = sum([x[1] for x in counters])

        for rank, counter in enumerate(counters):
            if counter[0][3:] == str(get_comment.author):
                counter = (f"**{counter[0]}**", counter[1])
            print(rank + 1, *counter, sep="|", file=hoc_file)

        hoc_file.write(f"\nIt took {rank+1} counters "
                       f"{days} days {hours} hours {mins} mins {secs} secs "
                       "to complete this thread. Bold is the user with the get"
                       f"\ntotal counts in this chain logged: {total_counts}")

    elapsed_time = datetime.datetime.now() - t_start
    print (elapsed_time)
