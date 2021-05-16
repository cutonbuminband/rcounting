A collection of tools for getting data on the counting threads in /r/reddit.com/r/counting.

## Description
There's a [community on reddit](www.reddit.com/r/counting) that likes to count collaboratively. Any kind of count - if you can think of a sequence of numbers (or letters, or words) what could be fun to count in order, we've probably tried counting it.

As well as counting, we also make stats on who's made how many counts, how fast they were, and who managed to count numbers ending in lots of nines or zeros.

This repository has tools for interacting with the reddit api through the [Python Reddit API Wrapper](https://praw.readthedocs.io/en/latest/), to help gathering these statistics.

Currently, the main non-trivial functions get statistics for a single reddit thread, or for the "999" and "000" counts for many threads.

## Installation and usage
You can clone the repository by typing `git clone https://github.com/cutonbuminband/counting_stats.git` in a terminal.

To install the dependencies, type `python3 -m pip -r requirements.txt`

Finally, you need to get [oauth credentials for the reddit api](https://github.com/reddit-archive/reddit/wiki/OAuth2), and store them somewhere where praw can find them.

Currently, this means creating a file called `praw.ini` in the repository with the following contents

```
[stats_bot]
client_id = 14CHARACTER_ID
client_secret = 30CHARACTER_SECRET
user_agent= cobibh/counting_stats/v1
ratelimit_seconds = 1
username = USERNAME
password = PASSWORD
```

If you only intend to scrape public posts, and don't want to comment or access private posts, the `username` and `password` lines are unnecessary.

To check that everything is working properly, you can type `python3 log_thread.py e1slbz0`. `e1slbz0` is the reddit id of count `2 172 000`, the last count in [this thread](https://www.reddit.com/r/counting/comments/8w151j/2171k_counting_thread/e1slbz0/). To log a different thread, you should supply a different `get_id` on the command line. The `get_id` is the last part of the url, which is usually of the form `http://reddit.com/r/counting/comments/thread_id/thread_title/get_id`

You should see the following text being printed
```
Logging reddit thread starting at e1slbz0 and moving towards the root
e1slbz0
e1slaku
e1sl8sh
...
```

Reddit limits us to 60 api requests per minute, and we can get at most nine comments for each api request, so it takes a bit of time.

Finally, the program prints how long it took to run, and writes two files: `thread_log.csv` and `thread_participation.csv`. Taking a peek at the past one, it looks as follows:

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

## Contributing and Future Ideas
This is a loosely organised list of things which could be done in the future. If you have any suggestions, don't hesitate to write, or to send a pull request.

* Adding a command line interface for the get and assist finding functionality: Currently only the thread logging script has any form of command line interface
* Recovering gracefully if a linked comment is inaccessible because it's been deleted or removed
* Making the comment and url extraction less brittle
* Adding more analyis tools: currently most of the code is focussed on data gathering, rather than analysis
* Adding code to local storage to save and extract data, and to keep track of what has already been extracted. Long-term, it might make sense to try and get a complete listing of all threads and completely bypass the reddit interface. That would also resolve the problem of deleted counts and usernames.
## License

This project is licensed under the terms of the GNU General Public License v3.0, or, at your discretion, any later version. You can read the license terms in [LICENSE.md](https://github.com/cutonbuminband/counting_stats/blob/master/LICENSE.md)

## Dependencies

The program has only been tested on python 3.8

The program makes use of the [Python Reddit API Wrapper](https://praw.readthedocs.io/en/latest/) to interact with reddit.

## Get in touch

If you have any questions, suggestions or comments about this repository, you can contact the maintainer at cutonbuminband@gmail.com, or visit the [counting subreddit](www.reddit.com/r/counting) and post in the weekly Free Talk Friday thread
