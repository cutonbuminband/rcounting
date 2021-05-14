# encoding=utf8
import datetime
import configparser
import requests.auth
from collections import OrderedDict, defaultdict
from pathlib import PurePath
from urllib.parse import urlparse
import re

id_get = 'e1slbz0'
id_thread = '8w151j'

netloc = "oauth.reddit.com"
path = f"r/counting/comments/{id_thread}/c/" + "{}"
batch_url = f"http://{netloc}/{path}?context=10"
single_url = f"https://{netloc}/api/info.json?id=t1_" + "{}"

def get_oauth_headers():
    config = configparser.ConfigParser()
    config.read('secrets.txt')
    auth = config['AUTH']
    client_auth = requests.auth.HTTPBasicAuth(auth['client_id'],
                                              auth['secret_key'])
    post_data = {"grant_type": "password",
                 "username": auth['username'],
                 "password": auth['password']}
    headers = {"User-Agent": "cobibh/counting_stats/v1"}
    response = requests.post("https://www.reddit.com/api/v1/access_token",
                             auth=client_auth,
                             data=post_data,
                             headers=headers).json()

    headers['Authorization'] = "bearer " + response['access_token']
    return headers

def get_count_from_text(body):
    try:
        regex = '^\[?[\d, ]*\]?'
        count = re.findall(regex, body)[0]
        stripped_count = count.translate(str.maketrans('', '', '[] ,'))
        return int(stripped_count)
    except:
        raise ValueError(f"Unable to extract count from comment body: {body}")

def find_previous_get(id_thread, headers):
    thread = requests.get(f"https://{netloc}/by_id/t3_{id_thread}",
                          headers=headers).json()
    body = get_data(thread)['selftext']
    url_regex = "(?:www\.)?(?:old\.)?reddit.com(/[^ )]+)(?:[ )]|$)"
    markdown_link_regex = '\[[^][]+]\(([^()]+)\)'
    urls = re.findall(url_regex, body)
    if not urls:
        urls = re.findall(markdown_link_regex, body)
    url = urls[0]
    path_components = PurePath(urlparse(url).path).parts
    new_id_thread = path_components[-3]
    new_id_get = path_components[-1]
    new_id_get = find_get_from_comment(new_id_thread,
                                       new_id_get,
                                       headers)
    print(new_id_thread, new_id_get)
    return new_id_thread, new_id_get


def find_get_from_comment(id_thread, id_comment, headers):
    path = f"r/counting/comments/{id_thread}/_/{id_comment}"
    batch_url = f"http://{netloc}/{path}?depth=10&sort=old"
    current_thread = requests.get(batch_url,
                                  headers=headers).json()[1]
    current_count = get_count_from_text(get_data(current_thread)['body'])
    current_id = id_comment
    while current_count % 1000 != 0:
        try:
            current_thread = get_data(current_thread)['replies']
        except KeyError:
            new_id = get_data(current_thread)['id']
            return find_get_from_comment(id_thread, new_id, headers)
        data = get_data(current_thread)
        current_count = get_count_from_text(data['body'])
        current_id = data['id']
    return current_id

def get_data(json_response):
    return json_response['data']['children'][0]['data']

def parse_data(comment):
    comment_data = get_data(comment)
    return OrderedDict(message=comment_data['body'],
                       author=comment_data['author'],
                       timestamp=comment_data['created_utc'],
                       comment_id=comment_data['id'],
                       thread_id=comment_data['link_id'].split("_", 1)[1])

def extract_gets_and_assists(id_get, id_thread, n_threads=1000):
    headers = get_headers()
    gets = []
    assists = []
    for n in range(n_threads):
        rows = []
        url = f"http://{netloc}/r/counting/comments/{id_thread}/_/{id_get}?context=2"
        comments = requests.get(url, headers).json([1])
        for i in range(3):
            rows.append(tuple(parse_data(comments.values())))
            comments = get_data(comments)['replies']
        gets.append(rows[2] + (int(rows[2][2]) - int(rows[1][2]),))
        assists.append(rows[1] + (rows[1][2] - rows[0][2],))
        id_thread, id_get = find_previous_get(id_thread, headers=headers)
    return gets, assists

def walk_thread(id_thread, id_main, headers):
    """Walk a reddit thread and return a list of comments

    Walk the reddit thread represented by id_thread from the leaf comment
    id_main up to the root, and return a flat list of the comments. This is
    ordered so that the root node is the first entry, and the leaf node is the
    last.

    Parameters:
    id_thread (str): the base36 encoded id of the reddit thread in question
    id_main (str): the base36 encoded id of the comment to start from
    headers: a dict to be consumed by requests.get containing the necessary access tokens.

    Returns:
        A list of all the comments between id_main and the first comment in that
    tree, both inclusive. Each comment is represented by the tuple of (body,
    author, timestamp, comment id, thread id).

    """
    comments = []
    while id_main != id_thread:
        # Request 10 comments in the thread, based on the latest count we have
        # received. The returned data is a nested dict, starting at the 10th parent
        # of the base comment and moving down from there.
        current_comments = requests.get(batch_url.format(id_main),
                                        headers=headers).json()[1]
        temp_data = []
        data = parse_data(current_comments)
        next_target = get_data(current_comments)['parent_id'].split("_", 1)[1]
        thread_start = data['timestamp']
        id_first_comment = data['comment_id']
        while id_main != next_target:
            if get_data(current_comments)['id'] != '_':
                # The happy path. We process all the comments we have received,
                # and when we are done we ask for a new batch.
                current_data = parse_data(current_comments)
                if current_data['comment_id'] == id_main:
                    id_main = next_target
                temp_data.append(tuple(current_data.values()))
                current_comments = get_data(current_comments)['replies']
            else:
                print("\n\nBroken chain detected\n" + f"Last id_main: {id_main}\n")
                for x in range(25):
                    broken_comment = requests.get(single_url.format(id_main),
                                                  headers=headers).json()
                    broken_data = parse_data(broken_comment)

                    if id_main != '_':
                        comments.append(tuple(broken_data.values()))
                    parent_id = get_data(broken_comment)['parent_id'].split("_", 1)[1]
                    id_main = parent_id
                print(f"\nNew id_main: {id_main}")
                print("\nEnd of broken chain\n\n")
                temp_data = []
                break
        comments += temp_data[::-1]

        print (id_main)
    return comments[::-1]

if __name__ == "__main__":
    t_start = datetime.datetime.now()
    headers = get_oauth_headers()
    get = get_data(requests.get(single_url.format(id_get),
                                headers=headers).json())
    id_thread = get['link_id'].split('_')[1]
    thread = requests.get(f"https://{netloc}/by_id/t3_{id_thread}.json",
                          headers=headers).json()
    title = get_data(thread)['title']

    counts = walk_thread(id_thread, id_get, headers)
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
            if counter[0][3:] == get['author']:
                counter = (f"**{counter[0]}**", counter[1])
            print(rank + 1, *counter, sep="|", file=hoc_file)

        hoc_file.write(f"\nIt took {rank+1} counters "
                       f"{days} days {hours} hours {mins} mins {secs} secs "
                       "to complete this thread. Bold is the user with the get"
                       f"\ntotal counts in this chain logged: {total_counts}")

    elapsed_time = datetime.datetime.now() - t_start
    print (elapsed_time)
