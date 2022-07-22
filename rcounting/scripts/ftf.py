import datetime as dt

from rcounting.reddit_interface import subreddit

threshold_timestamp = dt.datetime.combine(dt.date.today(), dt.time(hour=7)).timestamp()


def is_within_threshold(post):
    return post.created_utc > threshold_timestamp


def find_manual_ftf(previous_ftf_poster):
    submissions = []
    for submission in subreddit.new(limit=1000):
        if submission.created_utc >= threshold_timestamp:
            submissions.append(submission)
        else:
            break
    candidate_ftfs = [
        submission for submission in submissions if "Free Talk Friday" in submission.title
    ]
    if not candidate_ftfs:
        return False
    if len(candidate_ftfs) > 1:
        for candidate_ftf in candidate_ftfs[::-1]:
            if candidate_ftf.author != previous_ftf_poster:
                return candidate_ftf
    return candidate_ftfs[-1]


def generate_new_title(previous_title):
    n = int(previous_title.split("#")[1])
    return f"Free Talk Friday #{n+1}"


def generate_new_body(previous_ftf_id):

    ftf_body = (
        "Continued from last week's FTF [here](/comments/{}/)\n\n"
        "It's that time of the week again. Speak anything on your mind! "
        "This thread is for talking about anything off-topic, "
        "be it your lives, your strava, your plans, your hobbies, studies, stats, "
        "pets, bears, hikes, dragons, trousers, travels, transit, cycling, family, "
        "or anything you like or dislike, except politics\n\n"
        "Feel free to check out our [tidbits](https://redd.it/n6onl8) thread "
        "and introduce yourself if you haven't already."
    )

    return ftf_body.format(previous_ftf_id)


previous_ftf_post = subreddit.sticky(number=2)

if not is_within_threshold(previous_ftf_post):
    ftf_post = find_manual_ftf(previous_ftf_post.author)
    if not ftf_post:
        title = generate_new_title(previous_ftf_post.title)
        body = generate_new_body(previous_ftf_post.id)
        ftf_post = subreddit.submit(title=title, selftext=body)

    ftf_post.mod.sticky()
    ftf_post.mod.suggested_sort(sort="new")
