from datetime import datetime, timezone, timedelta

def time_millis(time):
    # `timestamp()` returns seconds as a float, so we multiply by 1000 and trunc to int to get
    # millis.
    return int(time.timestamp() * 1000)

def timenow_millis(datetime_override):
    # `timestamp()` returns seconds as a float, so we multiply by 1000 and trunc to int to get
    # millis.
    return int(datetime_override.now(timezone.utc).timestamp() * 1000)

def timeago_millis(time_ago: timedelta):
    # `total_seconds()` returns seconds as a float, so we multiply by 1000 and trunc to int to get
    # millis.
    return int(time_ago.total_seconds()) * 1000

