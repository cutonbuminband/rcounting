def flatten(mylist):
    return [element for sublist in mylist for element in sublist]


def is_leap_year(n):
    return n % 4 == 0 and (n % 400 == 0 or n % 100 != 0)


def format_timedelta(timedelta):
    def format_one_interval(n, unit):
        if n == 0:
            return ""
        else:
            return f"{n} {unit}{'s' if n > 1 else ''}"
    days = timedelta.days
    hours, rem = divmod(timedelta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    amounts = [days, hours, minutes, seconds]
    units = ["day", "hour", "minute", "second"]
    formatted = [format_one_interval(n, unit) for n, unit in zip(amounts, units)]
    return ", ".join([x for x in formatted if x])
