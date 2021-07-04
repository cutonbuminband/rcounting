import praw

# You need [OAuth](https://github.com/reddit-archive/reddit/wiki/OAuth2)
# credentials to interact with reddit, and you need to tell praw where to find
# them. This can be done in one of three ways: either create a file called
# praw.ini in the current directory with the following contents:

# [your_bot_name]
# client_id = 14CHARACTER_ID
# client_secret = 30CHARACTER_SECRET
# user_agent= PICK_SOMETHING_SENSIBLE
# username = USERNAME
# password = PASSWORD

# and then uncomment this call
# reddit = praw.Reddit('your_bot_name')
#

# Alternatively, you can write the credentials directly into this file, and
# make the call
# reddit = praw.Reddit(client_id="14CHARACTER_ID",
#                      client_secret="30CHARACTER_SECRET",
#                      user_agent="PICK_SOMETHING_SENSIBLE",
#                      username="USERNAME",
#                      password="PASSWORD")

# Finally, praw looks for environment variables called "praw_client_id" etc,
# and if you've set those, you can just write

reddit = praw.Reddit(user_agent="cobibh_counting_bot/v0.2")
