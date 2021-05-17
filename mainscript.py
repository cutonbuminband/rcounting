# encoding=utf8
import datetime
from collections import defaultdict
import re
import praw
import os
from pathlib import Path

#the ids are now in a list (array). much easier to do 
#and you can have it idle in the background

client_id="14 char"
client_secret="30 char"
 
def find_count_in_text(body):
    try:
        regex = ("^[^\d]*"    # We strip non-numeric characters from the start
                 "([\d, \.]*)")  # And then we take all digits and separators
        count = re.findall(regex, body)[0]
        # We remove any separators, and try to convert the remainder to an int.
        stripped_count = count.translate(str.maketrans('', '', ' ,.'))
        return int(stripped_count)
    except:
        raise ValueError(f"Unable to extract count from comment body: {body}")
 
def comment_to_tuple(comment):
    author = str(comment.author) if comment.author is not None else "[deleted]"
    return (comment.body, author, comment.created_utc, comment.id, str(comment.submission))
 
def walk_thread(leaf_comment):
    "Return a list of comments in a reddit thread from leaf to root"
    comments = []
    refresh_counter = 0
    comment = leaf_comment
    refresh_counter = 0
    while not comment.is_root:
        # By refreshing, we request the previous 9 comments in a single go. So
        # the next calls to .parent() and comment_to_tuple() don't result in a
        # network request. If the refresh fails, we just go one by one
        if refresh_counter % 9 == 0:
            try:
                comment.refresh()
                print(comment.id)
            except praw.exceptions.ClientException:
                print(f"Broken chain detected at {comment.id}")
                print(f"Fetching the next 9 comments one by one")
        comments.append(comment_to_tuple(comment))
        comment = comment.parent()
        refresh_counter += 1
    # We need to include the root comment as well
    comments.append(comment_to_tuple(comment))
    return comments

def find_alias(user):
    aliaseslist = []
    f = open(Path(os.getcwd()) / "input/prefs/Aliases.txt","r")
    lines = f.readlines()
    for line in lines:
        aliaseslist.append(line.replace("\n","",1).split(","))

    for x in aliaseslist:
        if (user in x) and (user != x[0]):
            return x[0]
    return user

if __name__ == "__main__":
    

    ids = []
    threads = ""
    while threads is not "idk lmao just make it throw an error and it will work":
        try:
            context = (input().split("/"))[8]
            print(context)
            ids.append(context)
        except:
            break
    
    for current_id in ids:
        t_start = datetime.datetime.now()
        reddit_instance = praw.Reddit(client_id=client_id,
                                      client_secret=client_secret,
                                      user_agent="cobibh/counting_stats/v1",
                                      ratelimit_seconds=1)
        get_comment = reddit_instance.comment(current_id)
        title = get_comment.submission.title
     
        counts = walk_thread(get_comment)[::-1]
        base_count = find_count_in_text(counts[0][0])
        thread_duration = datetime.timedelta(seconds=counts[-1][2] - counts[0][2])
        days = thread_duration.days
        hours, mins, secs = str(thread_duration).split(':')[-3:]
     
        counters = defaultdict(int)

        
        
        with open(Path(os.getcwd()) / ("results/LOG_"+str(base_count)+"to"+str(base_count+1000)+".csv"), "w") as hog_file:
            for idx, x in enumerate(counts[1:]):
                counters['/u/' + find_alias(x[1])] += 1
                try:
                    print(base_count + idx + 1, *x[1:], sep=",", file=hog_file)
                except:
                    continue
     
        with open(Path(os.getcwd()) / ("results/TABLE_"+str(base_count)+"to"+str(base_count+1000)+".csv"), "w") as hoc_file:
            hoc_file.write("Thread Participation Chart for " + title + "\n\n")
            hoc_file.write("Rank|Username|Counts\n")
            hoc_file.write("---|---|---\n")
            counters = sorted(counters.items(), key=lambda kv: (kv[1], kv[0]))[::-1]
            total_counts = sum([x[1] for x in counters])
     
            for rank, counter in enumerate(counters):
                if counter[0][3:] == find_alias(str(get_comment.author)):
                    counter = (f"**{counter[0]}**", counter[1])
                print(rank + 1, *counter, sep="|", file=hoc_file)
     
            hoc_file.write(f"\nIt took {rank+1} counters "
                           f"{days} days {hours} hours {mins} mins {secs} secs "
                           "to complete this thread. Bold is the user with the get"
                           f"\ntotal counts in this chain logged: {total_counts}")
     
        elapsed_time = datetime.datetime.now() - t_start
        print(elapsed_time)
        print(Path(os.getcwd()) / ("results/LOG_"+str(base_count)+"to"+str(base_count+1000)+".csv"))
        print(Path(os.getcwd()) / ("results/TABLE_"+str(base_count)+"to"+str(base_count+1000)+".csv"))
        print(base_count)
     
