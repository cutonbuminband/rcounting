import praw
import re
import numpy as np
import datetime
import configparser
from side_threads import get_side_thread
from parsing import find_urls_in_text, parse_markdown_links
from thread_navigation import fetch_comment_tree, walk_down_thread
from utils import flatten


start = datetime.datetime.now()

r = praw.Reddit('stats_bot')
subreddit = r.subreddit('counting')

directory_page = subreddit.wiki['directory'].content_md
directory_page = directory_page.replace("\r\n", "\n")


def parse_directory_page(directory_page):
    paragraphs = directory_page.split("\n\n")
    regex = r"^.*\|.*\|.*$"
    tagged_results = []
    for paragraph in paragraphs:
        lines = paragraph.split("\n")
        mask = np.all([bool(re.match(regex, line)) for line in lines])
        if not mask:
            tagged_results.append(['text', paragraph])
        else:
            tagged_results.append(['table', [x.split('|') for x in lines[2:]]])
    return tagged_results


tagged_results = parse_directory_page(directory_page)

six_months = datetime.timedelta(weeks=26.5)
now = datetime.datetime.utcnow()
posts = r.subreddit('counting').new(limit=1000)
tidbits_regex = "tidbits"
ftf_regex = "free talk friday"
tree = {}
posts_dict = {}
new_threads = []
weird_threads = ['nehhvf']
for post in posts:
    if post.id in weird_threads:
        continue
    posts_dict[post.id] = post
    title = post.title.lower()
    if re.match(tidbits_regex, title) or re.match(ftf_regex, title):
        continue
    body = post.selftext
    try:
        try:
            urls = filter(lambda x: int(x[0], 36) < int(post.id, 36), find_urls_in_text(body))
            url = next(urls)
            tree[post.id] = url[0]
        except StopIteration:
            post_urls = flatten([find_urls_in_text(comment.body) for comment in post.comments])
            urls = filter(lambda x: int(x[0], 36) < int(post.id, 36), post_urls)
            url = next(urls)
            tree[post.id] = url[0]
    except StopIteration:
        new_threads.append(post.id)
    post_time = datetime.datetime.utcfromtimestamp(post.created_utc)
    if now - post_time > six_months:
        break
else:  # no break
    print('Threads between {now - six_months} and {post_time} have not been collected')

child_tree = {v: k for k, v in tree.items()}


def walk_down_thread_chain(submission_id):
    result = [submission_id]
    if submission_id not in posts_dict and submission_id not in child_tree:
        # Post has been archived with no children
        return None
    while submission_id in child_tree:
        submission_id = child_tree[submission_id]
        result.append(submission_id)
    return result


new_threads = [walk_down_thread_chain(x)[-1] for x in new_threads]

config = configparser.ConfigParser()
config.read('side_threads.ini')
known_side_threads = config['threads']


def update_row(row, verbose=True):
    first, current, count = row
    thread_name, first_thread = parse_markdown_links(first)[0]
    first_thread = first_thread[1:]
    previous_thread, previous_comment = find_urls_in_text(current)[0]
    thread_chain = walk_down_thread_chain(previous_thread)
    is_archived = False
    if thread_chain is None:
        is_archived = True
        thread_chain = [previous_thread]
    latest_thread = thread_chain[-1]
    praw_thread = r.submission(latest_thread)
    if latest_thread != previous_thread or not previous_comment:
        previous_comment = praw_thread.comments[0].id
    comment_tree = fetch_comment_tree(praw_thread, root_id=previous_comment)
    thread_name = known_side_threads.get(first_thread, fallback='decimal')
    side_thread = get_side_thread(thread_name)
    new_comment = walk_down_thread(side_thread, comment_tree.comment(previous_comment))
    new_title = praw_thread.title.split("|")[-1]
    new_link = f'[{new_title}](reddit.com/r/comments/{latest_thread}/_/{new_comment.id})'
    try:
        old_count = int(count.translate(str.maketrans('-', '0', ', ')))
        new_count = side_thread.update_count(old_count, thread_chain)
        new_count = f"{new_count:,}"
        if new_count == "0":
            new_count = "-"
    except (ValueError, TypeError):
        new_count = f"{count}*"
    new_row = [first, new_link, new_count]
    if verbose:
        print(thread_name)
    return is_archived, new_row, latest_thread


def update_directory(tagged_results, new_threads):
    result = []
    archived_threads = []
    for idx, entry in enumerate(tagged_results):
        if entry[0] == "text":
            result.append(entry)
        elif entry[0] == "table":
            table = entry[1]
            new_table = []
            for row in table:
                is_archived, new_row, latest_thread = update_row(row)
                if latest_thread in new_threads:
                    new_threads.remove(latest_thread)
                if is_archived:
                    archived_threads.append(new_row)
                    continue
                new_table.append(new_row)
            result.append(['table', new_table])
    return result, archived_threads, new_threads


new_directory, archived_threads, new_threads = update_directory(tagged_results, new_threads)


def stringify(paragraph):
    table_header = [['Name &amp; Initial Thread', 'Current Thread', '# of Counts'],
                    [':--:', ':--:', '--:']]
    if paragraph[0] == "text":
        return paragraph[1]
    elif paragraph[0] == "table":
        return '\n'.join('|'.join(x) for x in table_header + paragraph[1])


def tagged_list_to_md(tagged_list):
    return '\n\n'.join([stringify(entry) for entry in tagged_list])


with open("new_directory_file.md", "w") as f:
    print(tagged_list_to_md(new_directory), file=f)

with open("archived_threads.md", "w") as f:
    print(stringify(["table", archived_threads]), file=f)

print(*[f"New thread '{r.submission(x).title}' "
        f"at reddit.com/comments/{x}" for x in new_threads], sep="\n")

end = datetime.datetime.now()
print(end - start)
