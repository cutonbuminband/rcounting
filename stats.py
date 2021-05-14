# encoding=utf8
import datetime
import configparser
import requests.auth
from collections import OrderedDict, defaultdict

id_get = 'e1slbz0'
id_thread = '8w151j'
baseCount = 2171000
netloc = "oauth.reddit.com"
path = f"r/counting/comments/{id_thread}/c/" + "{}"
batch_url = f"http://{netloc}/{path}?context=10"
single_url = f"https://{netloc}/api/info.json?id=t1_" + "{}"

def get_headers():
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

def get_data(json_response):
    return json_response['data']['children'][0]['data']

def parse_data(comment):
    comment_data = get_data(comment)
    return OrderedDict(message=comment_data['body'],
                       author=comment_data['author'],
                       timestamp=comment_data['created_utc'],
                       comment_id=comment_data['id'],
                       thread_id=comment_data['link_id'].split("_", 1)[1])

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
    headers = get_headers()
    thread = requests.get(f"https://{netloc}/by_id/t3_{id_thread}.json",
                          headers=headers).json()
    title = get_data(thread)['title']
    body = get_data(thread)['selftext']

    counts = walk_thread(id_thread, id_get, headers)
    thread_duration = datetime.timedelta(seconds=counts[-1][2] - counts[0][2])
    days = thread_duration.days
    hours, mins, secs = str(thread_duration).split(':')[-3:]

    counters = defaultdict(int)
    with open("thread_log.csv", "w") as hog_file:
        for idx, x in enumerate(counts[1:]):
            counters['/u/' + x[1]] += 1
            try:
                print(baseCount + idx + 1, *x[1:], sep=",", file=hog_file)
            except:
                continue

    get_author = counts[0][1]
    with open("thread_participation.csv", "w") as hoc_file:
        hoc_file.write("Thread Participation Chart for " + title + "\n\n")
        hoc_file.write("Rank|Username|Counts\n")
        hoc_file.write("---|---|---\n")
        counters = sorted(counters.items(), key=lambda kv: (kv[1], kv[0]))[::-1]
        total_counts = sum([x[1] for x in counters])

        for rank, counter in enumerate(counters):
            if counter[0][3:] == get_author:
                counter = (f"**{counter[0]}**", counter[1])
            print(rank + 1, *counter, sep="|", file=hoc_file)

        hoc_file.write(f"\nIt took {rank+1} counters "
                       f"{days} days {hours} hours {mins} mins {secs} secs "
                       "to complete this thread. Bold is the user with the get"
                       f"\ntotal counts in this chain logged: {total_counts}")

    elapsed_time = datetime.datetime.now() - t_start
    print (elapsed_time)
