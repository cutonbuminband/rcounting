# encoding=utf8
import os
import argparse
import datetime
from collections import defaultdict
import praw
from parsing import find_count_in_text
from thread_navigation import walk_thread, find_previous_get
from pathlib import Path

parser = argparse.ArgumentParser(description="Log the reddit thread which"
                                 " contains the comment with id `get_id`")
parser.add_argument("get_id",
                    help="The id of the leaf comment (get) to start logging from")
parser.add_argument("-n",
                    default=1,
                    help="The number of threads to log. Default 1")
args = parser.parse_args()

get_id = args.get_id
n = int(args.n)
print(f"Logging {n} reddit thread{'s' if n > 1 else ''} starting at {get_id}"
      " and moving backwards")


def log_one_thread(get_id, reddit_instance):
    get_comment = reddit_instance.comment(get_id)
    base_count = find_count_in_text(get_comment.body) - 1000
    hoc_path = Path(f"results/TABLE_{base_count}to{base_count+1000}.csv")
    hog_path = Path(f"results/LOG_{base_count}to{base_count+1000}.csv")
    if os.path.isfile(hoc_path):
        return
    title = get_comment.submission.title
    counts = walk_thread(get_comment)[::-1]
    thread_duration = datetime.timedelta(seconds=counts[-1][2] - counts[0][2])
    days = thread_duration.days
    hours, mins, secs = str(thread_duration).split(':')[-3:]

    counters = defaultdict(int)
    with open(hog_path, "w") as hog_file:
        for idx, x in enumerate(counts[1:]):
            counters['/u/' + x[1]] += 1
            print(base_count + idx + 1, *x[1:], sep=",", file=hog_file)

    with open(hoc_path, 'w') as hoc_file:
        hoc_file.write(f"Thread Participation Chart for {title}\n\n")
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


t_start = datetime.datetime.now()
reddit_instance = praw.Reddit("stats_bot")
for i in range(n):
    print(f"Logging thread {i + 1} out of {n}")
    get_comment = reddit_instance.comment(get_id)
    log_one_thread(get_comment, reddit_instance)
    get_id = find_previous_get(get_id, reddit_instance).id
elapsed_time = datetime.datetime.now() - t_start
print(f"Running the script took {elapsed_time}")

