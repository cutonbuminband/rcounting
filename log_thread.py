# encoding=utf8
import argparse
import datetime
from collections import defaultdict
import praw
from parsing import find_count_in_text
from thread_navigation import walk_thread
from pprint import pprint

parser = argparse.ArgumentParser(description="Log the reddit thread which contains the comment with id `id_thread`")
parser.add_argument("id_get",
                    help="The id of the leaf comment (get) to start logging from")
args = parser.parse_args()

id_get = args.id_get
print(f"Logging reddit thread starting at {id_get} and moving towards the root")


t_start = datetime.datetime.now()
reddit_instance = praw.Reddit("stats_bot")
get_comment = reddit_instance.comment(id_get)
title = get_comment.submission.title

counts = walk_thread(get_comment)[::-1]
base_count = find_count_in_text(counts[0][0])
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
print (f"Running the script took {elapsed_time}")
