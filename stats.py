# encoding=utf8
import datetime
import time
import configparser
import requests.auth
from urllib.parse import urlunparse
import pathlib

config = configparser.ConfigParser()
config.read('secrets.txt')
auth = config['AUTH']

client_auth = requests.auth.HTTPBasicAuth(auth['client_id'],
                                          auth['secret_key'])
post_data = {"grant_type": "password",
             "username": auth['username'],
             "password": auth['password']}
headers = {"User-Agent": "sthaa/counting"}
response = requests.post("https://www.reddit.com/api/v1/access_token",
                         auth=client_auth,
                         data=post_data,
                         headers=headers)
k = response.json()
print(k)
access_token = k['access_token']
headers = {"Authorization": "bearer " + access_token, "User-Agent": "Something"}

all_the_data = []
temp = True
id_first_comment = 'e1rvbvp'
id_main = 'e1slcal'
id_thread = '8w151j'
thread_partUrl = '2171k_counting_thread'
baseCount = 2171000
title = ''
i = 1
dict_count = {}
timestamp_last = 0
timestamp_first = 0
timestamp_noted = False
last_timestamp = 0
second_last_timestamp = 0
get_author = ''
last_author = ''
second_last_author = ''
t_start = datetime.datetime.now()

response_thread = requests.get("http://oauth.reddit.com/by_id/t3_" + id_thread + ".json",
                               headers=headers).json()
title = response_thread['data']['children'][0]['data']['title']
body = response_thread['data']['children'][0]['data']['selftext']
netloc = 'oauth.reddit.com'
scheme = 'https'
path_root = pathlib.PurePath('/r/counting/comments')
while True:
    try:
        path = str(path_root / id_thread / thread_partUrl / id_main)
        url = urlunparse([scheme, netloc, path, '', 'context=10', ''])
        json_response_comments = requests.get(url, headers=headers).json()
        json_position_comments = json_response_comments[1]
        i += 1
        temp_data = []
        id_2_check_temp = json_position_comments['data']['children'][0]['data']['id']
        while True:
            if json_position_comments['data']['children'][0]['data']['id'] == '_':
                print("\n\nBroken chain detected\n" + "Last id_main: " + str(id_main) + "\n");
                response_comment_broken = requests.get(
                    urlunparse([scheme, netloc, "api/info.json?id=t1_" + id_main , '', '', '']), headers=headers)
                json_response_comment_broken = response_comment_broken.json()
                parent_id = json_response_comment_broken['data']['children'][0]['data']['parent_id'].split("_", 1)[1]
                for x in range(25):
                    id_main = parent_id
                    response_comment_broken = requests.get(
                        "https://oauth.reddit.com/api/info.json?id=t1_" + id_main, headers=headers)
                    json_response_comment_broken = response_comment_broken.json()
                    parent_id = json_response_comment_broken['data']['children'][0]['data']['parent_id'].split("_", 1)[1]
                    comment_id = json_response_comment_broken['data']['children'][0]['data']['id']
                    author = json_response_comment_broken['data']['children'][0]['data']['author']
                    timestamp = json_response_comment_broken['data']['children'][0]['data']['created_utc']
                    thread_id = json_response_comment_broken['data']['children'][0]['data']['link_id'].split("_", 1)[1]
                    message = json_response_comment_broken['data']['children'][0]['data']['body']

                    tuple_comment = (message, author, timestamp, comment_id, thread_id)
                    if id_main != '_':
                        all_the_data.append(tuple_comment)
                    if not timestamp_noted:
                        get_author = author
                        timestamp_last = timestamp
                        timestamp_noted = True
                print("\nNew id_main: " + str(id_main))
                print("\nEnd of broken chain\n\n")
                temp_data = []
                break
            else:
                comment_id = json_position_comments['data']['children'][0]['data']['id']
                author = json_position_comments['data']['children'][0]['data']['author']
                timestamp = json_position_comments['data']['children'][0]['data']['created_utc']
                thread_id = json_position_comments['data']['children'][0]['data']['link_id'].split("_", 1)[1]
                message = json_position_comments['data']['children'][0]['data']['body']
                second_last_timestamp = last_timestamp
                last_timestamp = timestamp
                second_last_author = last_author
                last_author = author
                if comment_id == id_main:
                    id_main = id_2_check_temp
                    break
                tuple_comment = (message, author, timestamp, comment_id, thread_id)
                temp_data.append(tuple_comment)
                json_position_comments = json_position_comments['data']['children'][0]['data']['replies']
                id = comment_id
        if not timestamp_noted:
            get_author = second_last_author
            timestamp_last = second_last_timestamp
            timestamp_noted = True
        for l in reversed(temp_data):
            all_the_data.append(l)
    except Exception as e:
        time.sleep(30)

    print (id_main)
    try:
        parent = json_position_comments['data']['children'][0]['data']['parent_id'].split("_", 1)[1]
    except:
        print (json_position_comments.values())
    if parent == id_thread:
        timestamp_first = last_timestamp
        break

time_taken = int(timestamp_last - timestamp_first)
rem_sec = time_taken % 60
min1 = time_taken // 60
hours = min1 // 60
rem_min = min1 % 60
days = hours // 24
rem_hours = hours % 24

fileforhog = open("thread_log.csv", "w")

for idx, x in enumerate(list(reversed(all_the_data))):
    if x[3] != id_first_comment:
        if x[1] not in dict_count:
            dict_count[x[1]] = 1
        else:
            dict_count[x[1]] += 1
        try:
            date_of_com = datetime.datetime.fromtimestamp(float(x[2])).strftime('%Y-%m-%d %H:%M:%S')
            fileforhog.write(str(baseCount + idx) + "," +
                             str(x[1]) + "," + str(x[2]) + "," + str(x[3]) + "," + str(x[4]) + "\n")
        except:
            continue
fileforhoc = open("thread_participation.csv", "w")
fileforhoc.write("Thread Participation Chart for " + title + "\n\n")
fileforhoc.write("Rank|Username|Counts\n")
fileforhoc.write("---|---|---\n")
unique_counters = 0




sorted_list = []
countSum = 0
for key, value in sorted(dict_count.items(), key=lambda kv: (kv[1], kv[0])):
    sorted_list.append((key, value))
    countSum = countSum + value

for tuple_uc in reversed(sorted_list):
    unique_counters += 1
    if tuple_uc[0] == get_author:
        fileforhoc.write(str(unique_counters) + "|**/u/" + str(tuple_uc[0]) + "**|" + str(tuple_uc[1]) + "\n")
    else:
        fileforhoc.write(str(unique_counters) + "|/u/" + str(tuple_uc[0]) + "|" + str(tuple_uc[1]) + "\n")

fileforhoc.write("\nIt took " + str(unique_counters) + " counters " + str(days) + " days " + str(rem_hours) + " hours " + str(
        rem_min) + " mins " + str(rem_sec) + " secs to complete this thread. Bold is the user with the get" + "\ntotal counts in this chain logged: "
                 + str(countSum))
t_end = datetime.datetime.now()

Time_taken = t_end - t_start

print (Time_taken)
