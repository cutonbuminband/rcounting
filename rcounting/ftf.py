import datetime as dt


def get_ftf_timestamp():
    current_time = dt.datetime.now(dt.timezone.utc)
    threshold_date = (
        current_time.date()
        - dt.timedelta(days=current_time.weekday())
        + dt.timedelta(days=4, weeks=-1)
    )
    threshold_timestamp = dt.datetime.combine(threshold_date, dt.time(7, tzinfo=dt.timezone.utc))
    if current_time - threshold_timestamp >= dt.timedelta(weeks=1):
        threshold_timestamp += dt.timedelta(weeks=1)
    return threshold_timestamp


def is_within_threshold(post):
    """
    Check if a post was made after the most recent Friday at 0700 UTC
    """
    return post.created_utc >= get_ftf_timestamp().timestamp()
