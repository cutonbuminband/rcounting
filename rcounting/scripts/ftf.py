# pylint: disable=import-outside-toplevel
import datetime as dt

import click

from rcounting.counters import apply_alias
from rcounting.parsing import parse_markdown_links


def is_within_threshold(post, timestamp):
    """
    Check if a post was made after the most recent Friday at 0700 UTC
    """
    return post.created_utc >= timestamp.timestamp()


def find_manual_ftf(previous_ftf_poster, subreddit, timestamp):
    submissions = []
    for submission in subreddit.new(limit=1000):
        if is_within_threshold(submission, timestamp):
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


def pprint(date):
    return date.strftime("%A %B %d, %Y")


def generate_new_title(previous_title):
    n = int(previous_title.split("#")[1])
    return f"Free Talk Friday #{n+1}"


def generate_new_body(previous_ftf_id, threshold_date):

    ftf_body = (
        "Continued from last week's FTF [here](/comments/{}/)\n\n"
        "It's that time of the week again. Speak anything on your mind! "
        "This thread is for talking about anything off-topic, "
        "be it your lives, your strava, your plans, your hobbies, studies, stats, "
        "pets, bears, hikes, dragons, trousers, travels, transit, cycling, family, "
        "or anything you like or dislike, except politics\n\n"
        "Feel free to check out our [tidbits](https://redd.it/10roh5l) thread "
        "and introduce yourself if you haven't already.\n\n"
        "---\n\n"
        "*This post was made by a bot, because no-one else made a Free Talk Friday post "
        " before {} UTC 13:00. Anyone can post the FTF, so if you want to have your "
        "post pinned here for a week, just make one {} between UTC 07:00 and 13:00. "
        "The rules for these posts can be found in the "
        "[faq](/r/counting/wiki/faq/#wiki_3.6_free_talk_friday_posts). You can also check out "
        "our [directory](/r/counting/wiki/ftf_directory) of older posts for inspiration.*\n\n"
        "*If you have any questions or comments about the bot, feel free to write them below,"
        " or message the mods.*"
    )

    return ftf_body.format(
        previous_ftf_id,
        pprint(threshold_date),
        pprint(threshold_date + dt.timedelta(weeks=1)),
    )


def make_directory_row(post):
    date = dt.date.fromtimestamp(post.created_utc)
    link = f"[FTF #{post.title.split('#')[1]}](/{post.id})"
    formatted_date = date.strftime("%b %d, %Y")
    author = apply_alias(str(post.author))
    return f"|{link}|{formatted_date}|{author}"


def update_directory(post, subreddit):
    row = make_directory_row(post)
    wiki = subreddit.wiki["ftf_directory"]
    contents_list = wiki.content_md.split("\n")
    links = [parse_markdown_links(x) for x in contents_list]
    known_posts = [x[0][1][1:] for x in links if x]
    if post.id not in known_posts:
        new_contents = "\n".join(contents_list + [row])
        wiki.edit(content=new_contents, reason="Added latest FTF")


def get_ftf_timestamp():
    current_time = dt.datetime.now()
    threshold_date = (
        current_time.date()
        - dt.timedelta(days=current_time.weekday())
        + dt.timedelta(days=4, weeks=-1)
    )
    threshold_timestamp = dt.datetime.combine(threshold_date, dt.time(7))
    if current_time - threshold_timestamp >= dt.timedelta(weeks=1):
        threshold_timestamp += dt.timedelta(weeks=1)
    return threshold_timestamp


@click.command(name="ftf")
@click.argument("subreddit", default="counting")
def pin_or_create_ftf(subreddit):
    """
    Pin the earliest valid Free Talk Friday thread for this week in [subreddit].
    The subreddit is r/counting by default, but passing a different value lets you
    test things on a subreddit you control.

    If no FTF has been posted, create one, and then pin it.

    Also update the FTF directory with the newest FTF.
    """
    from rcounting.reddit_interface import reddit

    subreddit = reddit.subreddit(subreddit)

    previous_ftf_post = subreddit.sticky(number=2)
    threshold_timestamp = get_ftf_timestamp()

    if is_within_threshold(previous_ftf_post, threshold_timestamp):
        ftf_post = previous_ftf_post
    else:
        ftf_post = find_manual_ftf(previous_ftf_post.author, subreddit, threshold_timestamp)
        if not ftf_post:
            title = generate_new_title(previous_ftf_post.title)
            body = generate_new_body(previous_ftf_post.id, threshold_timestamp)
            ftf_post = subreddit.submit(title=title, selftext=body)

        ftf_post.mod.approve()
        ftf_post.mod.sticky()
    ftf_post.mod.suggested_sort(sort="new")
    update_directory(ftf_post, subreddit)


if __name__ == "__main__":
    pin_or_create_ftf()  # pylint: disable=no-value-for-parameter
