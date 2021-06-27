A collection of tools for getting data on the counting threads in /r/reddit.com/r/counting.

## Description
There's a [community on reddit](www.reddit.com/r/counting) that likes to count collaboratively. Any kind of count - if you can think of a sequence of numbers (or letters, or words) what could be fun to count in order, we've probably tried counting it.

As well as counting, we also make stats on who's made how many counts, how fast they were, and who managed to count numbers ending in lots of nines or zeros.

This repository has tools for interacting with the reddit api through the [Python Reddit API Wrapper](https://praw.readthedocs.io/en/latest/), to help gathering these statistics.

Currently, the main non-trivial functions get statistics for a single reddit thread, or for the "999" and "000" counts for many threads.

## Installation and usage
You can clone the repository by: typing `git clone https://github.com/cutonbuminband/counting_stats.git` in a terminal.

To install the dependencies, type `python3 -m pip -r requirements.txt`

Finally, you need to get [oauth credentials for the reddit api](https://github.com/reddit-archive/reddit/wiki/OAuth2), and store them somewhere where the program knows about it. The easiest way is to create a file in called `praw.ini` in the directory with the following contents

```
[stats_bot]
client_id = 14CHARACTER_ID
client_secret = 30CHARACTER_SECRET
user_agent= PICK_SOMETHING_SENSIBLE
username = USERNAME
password = PASSWORD
```

Alternatively, take a look at the instructions in `reddit_interface.py`.

If you only intend to scrape public posts, and don't want to comment or access private posts, the `username` and `password` lines are unnecessary.

The main command line interfaces are `log_thread.py`, `validate.py` and `update_thread_directory.py` which will be described below. All scripts accept a `-h` or  `--help` parameter to explain the usage.


### Thread Logging
To log a thread, you need to find the reddit id of the last comment you want to log (typically the get, ending in 000). The `comment_id` is the last part of the url of of a comment, which is generally of the form `http://reddit.com/r/counting/comments/thread_id/thread_title/get_id`.

For example, you can type `python3 log_thread.py e1slbz0` to log [this thread](https://www.reddit.com/r/counting/comments/8w151j/2171k_counting_thread/e1slbz0/), which ends with the count `2 172 000`. You should see the following text being printed
```
Logging reddit thread starting at e1slbz0 and moving towards the root
e1slbz0
e1slaku
e1sl8sh
...
```

Reddit limits us to 60 api requests per minute, and we can get at most nine comments for each api request, so it takes a bit of time. We speed that up by: using the [Pushshift API Wrapper](https://psaw.readthedocs.io/en/latest/#) to query an archive of reddit comments instead of reddit itself. This is faster, but doesn't work for the very oldest threads, or the very newest ones, and some comments are just missing. For full usage information, try typing `python3 log_thread.py -h`.

The output of the logging script is two csv files called `results/table_{something}.csv` and `results/log_{something}.csv`. Taking a peek at the past one, it looks as follows:

```
Thread Participation Chart for 2171k counting thread

Rank|Username|Counts
---|---|---
1|**/u/qualw**|492
2|/u/TheNitromeFan|459
3|/u/itsthecount|25
4|/u/[deleted]|7
5|/u/dm4uz3|4
6|/u/parker_cube|3
7|/u/Executebravo|3
8|/u/smarvin6689|1
9|/u/janzus|1
10|/u/foxthechicken|1
11|/u/davidjl123|1
12|/u/Tornado9797|1
13|/u/TehVulpez|1
14|/u/AWiseTurtle|1

It took 14 counters 0 days 7 hours 38 mins 45 secs to complete this thread. Bold is the user with the get
total counts in this chain logged: 1000
```

In markdown, the table looks like

Rank|Username|Counts
---|---|---
1|**/u/qualw**|492
2|/u/TheNitromeFan|459
3|/u/itsthecount|25
4|/u/[deleted]|7
5|/u/dm4uz3|4
6|/u/parker_cube|3
7|/u/Executebravo|3
8|/u/smarvin6689|1
9|/u/janzus|1
10|/u/foxthechicken|1
11|/u/davidjl123|1
12|/u/Tornado9797|1
13|/u/TehVulpez|1
14|/u/AWiseTurtle|1

### Validation
The `validate.py` script works in the same way, except that it takes an additional `--rule` parameter specifying which rule should be checked. The following options are available:

- default: No counter can reply to themselves
- wait2: Counters can only count once two others have counted
- wait3: Counters can only count once three others have counted
- once\_per\_thread: Counters can only count once on a given reddit submission
- slow: One minute must elapse between counts
- slower: Counters must wait one hour between each of their counts
- slowestest: One hour must elapse between each count, and counters must wait 24h between each of their counts
- only\_double\_counting: Counters must reply to themselves exactly once before it's somebody else's turn.

If no rule is supplied, the script will only check that nobody double counted.

After you run it, it'll print out whether all the counts in the chain were valid, and if there was an invalid count, which one it was.

### Updating the thread directory

The script `update_thread_directory.py` will try and update the directory of side threads found at www.reddit.com/r/counting/wiki/directory. It roughly follows the following steps

1. It gets all submissions to r/counting made within the last six months
2. It tries to find a link to the parent submission of each submission
   - First it looks for a reddit url in the body of each submission (trying to find the "continued from here" line
   - If that fails, it goes through all the top level comments of the submission looking for a reddit url
3. It Constructs a chain for each thread from parent submission to child submission
4. For each row in each table in the directory, it extracts
  - Which side thread it is, based on the link to the first thread
  - What the previous submission, comment and total count were.
5. It then uses the chain of submissions to find the latest submission of each thread type
6. And walks down the comments on each submission to the latest one. At each level of comments it goes to the first valid reply based on
  - A per-thread rule describing when a count is valid
  - A per-thread rule describing what looks like a count (to skip over mid-thread conversations)
7. If the latest submission is not the same as the previous one, it tries to update the total count field

Once it's done all that, it outputs three files:
- `directory.md`: The updated directory in markdown format
- `archived_threads.md`: A table of all threads which were in the existing directory, but where no non-archived submissions were found in their chain
- `new_threads.txt`: A list of all submissions made to r/counting which did not match any threads already found in the directory.

The rows corresponding to archived threads are not included in `directory.md`.

If you run the script with no parameters it takes around 15 minutes to run. It relies on pushshift to get the comments for each reddit thread, and the pushshift archive is sometimes quite a ways behind in getting data from reddit. Currently, there's a delay of around 3 days. If you want a more up to date listing, you can call the script as `python3 update_thread_directory.py --accurate` to get the latest comments from reddit. This causes the script to run 2-3 times more slowly.
## Contributing and Future Ideas
This is a loosely organised list of things which could be done in the future. If you have any suggestions, don't hesitate to write, or to send a pull request.

* Adding a command line interface for the get and assist finding functionality
* ~~Recovering gracefully if a linked comment is inaccessible because it's been deleted or removed~~ This has been done more or less, in that the code tries to see if the reddit object is accessible on pushshift. If it isn't, it still crashes, but there's no simple way of recovering gently if e.g. the body of a submission is missing so that the previous get can't be found.
* Making the comment and url extraction less brittle
* Adding more analyis tools: currently most of the code is focussed on data gathering, rather than analysis
* Adding code to local storage to save and extract data, and to keep track of what has already been extracted. Long-term, it might make sense to try and get a complete listing of all threads and completely bypass the reddit interface. That would also resolve the problem of deleted counts and usernames.
## License

This project is licensed under the terms of the GNU General Public License v3.0, or, at your discretion, any later version. You can read the license terms in [LICENSE.md](https://github.com/cutonbuminband/counting_stats/blob/master/LICENSE.md)

## Dependencies

The program has only been tested on python 3.8

The program makes use of the [Python Reddit API Wrapper](https://praw.readthedocs.io/en/latest/) to interact with reddit.

The program also uses the [Pushshift API Wrapper](https://psaw.readthedocs.io/en/latest/#)

The program uses the `pandas` package to work with tabular data

## Get in touch

If you have any questions, suggestions or comments about this repository, you can contact the maintainer at cutonbuminband@gmail.com, or visit the [counting subreddit](www.reddit.com/r/counting) and post in the weekly Free Talk Friday thread
