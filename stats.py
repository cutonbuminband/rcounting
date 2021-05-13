# encoding=utf8
import datetime
import time
import configparser
import requests.auth
from urllib.parse import urlunparse
from pathlib import PurePath
from collections import OrderedDict, defaultdict

t_start = datetime.datetime.now()

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

access_token = response['access_token']
headers = {"Authorization": "bearer " + access_token,
           "User-Agent": "cobibh/counting_stats/v1"}

all_the_data = []
temp = True
id_get = 'e1slbz0'
id_main = id_get
id_thread = '8w151j'
timestamp_noted = False

def get_data(json_response):
    return json_response['data']['children'][0]['data']

def parse_data(comment_data):
    return OrderedDict(message=comment_data['body'],
                       author=comment_data['author'],
                       timestamp=comment_data['created_utc'],
                       comment_id=comment_data['id'],
                       thread_id=comment_data['link_id'].split("_", 1)[1],
                       parent_id=comment_data['parent_id'].split("_", 1)[1])

response_thread = requests.get("http://oauth.reddit.com/by_id/t3_" + id_thread + ".json",
                               headers=headers).json()
title = get_data(response_thread)['title']
body = get_data(response_thread)['selftext']
netloc = 'oauth.reddit.com'
scheme = 'https'
path = f'/r/counting/comments/{id_thread}/c/' + '{}'
url = urlunparse([scheme, netloc, path, '', 'context=10', ''])
failure_url = urlunparse([scheme, netloc, 'api/info.json', '', 'id=t1_{}', ''])
while id_main != id_thread:
    # Request 10 comments in the thread, based on the latest count we have
    # received. The returned data is a nested dict, starting at the 10th parent
    # of the base comment and moving down from there.
    current_comments = requests.get(url.format(id_main),
                                    headers=headers).json()[1]
    temp_data = []
    data = parse_data(get_data(current_comments))
    next_target = data['parent_id']
    timestamp_first = data['timestamp']
    id_first_comment = data['comment_id']
    while id_main != next_target:
        if get_data(current_comments)['id'] != '_':
            # The happy path. We parse all the comments we have received, and
            # when we are done we ask for a new batch.
            current_data = parse_data(get_data(current_comments))
            timestamp = current_data['timestamp']
            if current_data['comment_id'] == id_main:
                id_main = next_target
            temp_data.append(tuple(current_data.values())[:-1])
            current_comments = get_data(current_comments)['replies']
        else:
            print("\n\nBroken chain detected\n" + f"Last id_main: {id_main}\n")
            for x in range(25):
                broken_comment = requests.get(failure_url.format(id_main),
                                              headers=headers).json()
                broken_data = parse_data(get_data(broken_comment))

                if id_main != '_':
                    all_the_data.append(tuple(broken_data.values())[:-1])
                if not timestamp_noted:
                    get_author = broken_data['author']
                    timestamp_last = broken_data['timestamp']
                    timestamp_noted = True
                id_main = broken_data['parent_id']
            print(f"\nNew id_main: {id_main}")
            print("\nEnd of broken chain\n\n")
            temp_data = []
            break
    if not timestamp_noted:
        get_author = current_data['author']
        timestamp_last = current_data['timestamp']
        timestamp_noted = True
    for l in reversed(temp_data):
        all_the_data.append(l)

    print (id_main)

thread_time = datetime.timedelta(seconds=(timestamp_last - timestamp_first))
days = thread_time.days
hours, mins, secs = str(thread_time).split(':')[-3:]
baseCount = 2171000

dict_count = defaultdict(int)
with open("thread_log.csv", "w") as hog_file:
    for idx, x in enumerate(list(reversed(all_the_data))):
        if x[3] != id_first_comment:
            dict_count['/u/' + x[1]] += 1
            try:
                print(baseCount + idx, *x[1:], sep=",", file=hog_file)
            except:
                continue

with open("thread_participation.csv", "w") as hoc_file:
    hoc_file.write("Thread Participation Chart for " + title + "\n\n")
    hoc_file.write("Rank|Username|Counts\n")
    hoc_file.write("---|---|---\n")
    unique_counters = 0
    sorted_list = []
    countSum = 0
    for key, value in sorted(dict_count.items(), key=lambda kv: (kv[1], kv[0])):
        sorted_list.append((key, value))
        countSum = countSum + value

    for counter in reversed(sorted_list):
        unique_counters += 1
        if counter[0][3:] == get_author:
            counter = (f"**{counter[0]}**", counter[1])
        print(unique_counters, *counter, sep="|", file=hoc_file)

    hoc_file.write(f"\nIt took {unique_counters} counters "
                   f"{days} days {hours} hours {mins} mins {secs} secs "
                   "to complete this thread. Bold is the user with the get"
                   f"\ntotal counts in this chain logged: {countSum}")

elapsed_time = datetime.datetime.now() - t_start
print (elapsed_time)
