import praw

# You need [OAuth](https://github.com/reddit-archive/reddit/wiki/OAuth2)
# credentials to interact with reddit, and you need to tell praw where to find
# them. This can be done in one of two ways: either create a file called
# praw.ini in the current directory with the following contents:

# [your_bot_name]
# client_id = 14CHARACTER_ID
# client_secret = 30CHARACTER_SECRET
# user_agent= PICK_SOMETHING_SENSIBLE
# username = USERNAME
# password = PASSWORD

# and then update this call so that it says
# reddit = praw.Reddit('your_bot_name')
reddit = praw.Reddit('counting_bot')


# Alternatively, you can write the credentials directly into this file, and
# make the call
# reddit = praw.Reddit(client_id="14CHARACTER_ID",
#                      client_secret="30CHARACTER_SECRET",
#                      user_agent="PICK_SOMETHING_SENSIBLE",
#                      username="USERNAME",
#                      password="PASSWORD")
