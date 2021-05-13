# encoding=utf8
import datetime
import time
import configparser
import requests.auth
from urllib.parse import urlunparse
import pathlib
from collections import OrderedDict, defaultdict

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
id_first_comment = 'e1rvbvp'
id_main = 'e1slcal'
id_thread = '8w151j'
thread_partUrl = '2171k_counting_thread'
timestamp_last = 0
timestamp_first = 0
timestamp_noted = False
last_timestamp = 0
second_last_timestamp = 0
get_author = ''
last_author = ''
second_last_author = ''
t_start = datetime.datetime.now()

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
path = pathlib.PurePath('/r/counting/comments') / id_thread / thread_partUrl
while True:
    # Request 10 comments in the thread, based on the latest count we have
    # received. The returned data is a nested dict, starting at the 10th parent
    # of the base comment and moving down from there.
    url = urlunparse([scheme, netloc, str(path / id_main),
                      '', 'context=10', ''])
    json_response_comments = requests.get(url, headers=headers).json()
    json_position_comments = json_response_comments[1]
    temp_data = []
    id_2_check_temp = get_data(json_position_comments)['id']
    while True:
        if get_data(json_position_comments)['id'] != '_':
            # The happy path. We parse all the comments we have received, and
            # when we are done we ask for a new batch.
            position_comment_data = get_data(json_position_comments)
            parsed_data = parse_data(position_comment_data)
            second_last_timestamp = last_timestamp
            last_timestamp = parsed_data['timestamp']
            second_last_author = last_author
            last_author = parsed_data['author']
            if parsed_data['comment_id'] == id_main:
                id_main = id_2_check_temp
                break
            temp_data.append(tuple(parsed_data.values())[:-1])
            json_position_comments = get_data(json_position_comments)['replies']
        else:
            print("\n\nBroken chain detected\n" + "Last id_main: " + str(id_main) + "\n");
            response_comment_broken = requests.get(
                "https://oauth.reddit.com/api/info.json?id=t1_" + id_main, headers=headers)
            json_response_comment_broken = response_comment_broken.json()
            parent_id = get_data(json_response_comment_broken)['parent_id'].split("_", 1)[1]
            for x in range(25):
                id_main = parent_id
                response_comment_broken = requests.get(
                    "https://oauth.reddit.com/api/info.json?id=t1_" + id_main, headers=headers)
                json_response_comment_broken = response_comment_broken.json()
                comment_broken_data = get_data(json_response_comment_broken)
                parsed_data = parse_data(comment_broken_data)
                parent_id = parsed_data['parent_id']

                if id_main != '_':
                    all_the_data.append(tuple(parsed_data.values())[:-1])
                if not timestamp_noted:
                    get_author = parsed_data['author']
                    timestamp_last = parsed_data['timestamp']
                    timestamp_noted = True
            print("\nNew id_main: " + str(id_main))
            print("\nEnd of broken chain\n\n")
            temp_data = []
            break
    if not timestamp_noted:
        get_author = second_last_author
        timestamp_last = second_last_timestamp
        timestamp_noted = True
    for l in reversed(temp_data):
        all_the_data.append(l)

    print (id_main)
    try:
        parent = get_data(json_position_comments)['parent_id'].split("_", 1)[1]
    except:
        print (json_position_comments.values())
    if parent == id_thread:
        timestamp_first = last_timestamp
        break

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
