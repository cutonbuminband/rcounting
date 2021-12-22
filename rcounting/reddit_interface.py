import os
import socket
import random
import configparser
import praw

# The script works with OAuth access and refresh tokens, so you need to grant
# access. Once you've done that, they token will be stored in a file for future
# use.
_client_id = "S2MLNKwl9tpgaS5jUtlIIQ"
_user_agent = "rcounting_tools/v0.3 by u/CutOnBumInBandHere"


def get_refresh_token():
    # To do logging, reading reddit posts is necessary
    # To update the wiki, both wikiread and wikiedit are needed
    scopes = ['read', 'wikiread', 'wikiedit']

    reddit = praw.Reddit(
        client_id=_client_id,
        user_agent=_user_agent,
        redirect_uri="http://localhost:8080",
        client_secret=None,
    )
    print("This is the first time you're using the rcounting tools.",
          "You need to allow the program to interact with reddit on your behalf",
          "For logging, it needs to be able to read wiki pages and posts, "
          "and to update the thread directory it needs permission to edit wiki pages.",
          sep="\n")
    state = str(random.randint(0, 65000))
    url = reddit.auth.url(scopes, state, "permanent")
    print(f"Please open this url in a web browser, and follow the instructions there: {url}")

    client = receive_connection()
    data = client.recv(1024).decode("utf-8")
    param_tokens = data.split(" ", 2)[1].split("?", 1)[1].split("&")
    params = {
        key: value for (key, value) in [token.split("=") for token in param_tokens]
    }

    if state != params["state"]:
        send_message(
            client,
            f"State mismatch. Expected: {state} Received: {params['state']}",
        )
        return 1
    elif "error" in params:
        send_message(client, params["error"])
        return 1

    refresh_token = reddit.auth.authorize(params["code"])
    send_message(client,
                 ("Successfully retrieved refresh token! "
                  "You can close this window now"))
    return refresh_token


def receive_connection():
    """Wait for and then return a connected socket..

    Opens a TCP connection on port 8080, and waits for a single client.

    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("localhost", 8080))
    server.listen(1)
    client = server.accept()[0]
    server.close()
    return client


def send_message(client, message):
    """Send message to client and close the connection."""
    print(message)
    client.send(f"HTTP/1.1 200 OK\r\n\r\n{message}".encode("utf-8"))
    client.close()


refresh_token = os.getenv('praw_refresh_token')
if refresh_token is None:
    module_dir = os.path.dirname(__file__)
    credentials_file = os.path.join(module_dir, "credentials.ini")
    if not os.path.isfile(credentials_file):
        refresh_token = get_refresh_token()
        with open(credentials_file, 'w') as f:
            print(f"[tokens]\nrefresh_token = {refresh_token}",
                  file=f)
    config = configparser.ConfigParser()
    config.read(credentials_file)
    refresh_token = config["tokens"]["refresh_token"]

reddit = praw.Reddit(client_id=_client_id,
                     user_agent=_user_agent,
                     client_secret=None,
                     refresh_token=refresh_token)
