import copy
from datetime import datetime, timezone

def init_timestamps(struct):
    # init_timestamps should produce timestamps that are timezone aware
    # and in UTC.
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.now
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.utcnow
    # https://docs.python.org/3/library/datetime.html#datetime.timezone
    if not isinstance(struct, dict):
        return struct
    ret = copy.deepcopy(struct)
    ret['time_of_save'] = datetime.now().astimezone()
    ret['time_of_removal'] = None
    return ret

def remove_timestamps(struct):
    if not isinstance(struct, dict):
        return struct
    ret = copy.deepcopy(struct)
    for key in ['time_of_save', 'time_of_removal']:
        if key in ret:
            del ret[key]
    return ret

