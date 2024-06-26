import tomli
from datetime import datetime, timedelta
# from datetime import UTC as dtUTC
import pytz
from pytz import UTC


def current_dt_utc():
    return datetime.now(UTC)

def dt_to_tz(dt, tz, require_existing_timezone=False):
    if require_existing_timezone and dt.tzinfo is None:
        raise AttributeError(f"{dt} is missing a timezone!")
    if isinstance(tz, str):
        tz = pytz.timezone(tz)
    return dt.astimezone(tz)

def dt_to_utc(dt, require_existing_timezone=False):
    return dt_to_tz(dt, UTC, require_existing_timezone)

def read_config(config_path):
    with open(config_path, "rb") as f:
        cfg = tomli.load(f)
    return cfg

def multi_replace(string:str, old_strs, subst_str):
    for s in old_strs:
        string = string.replace(s, subst_str)
    return string

STRFTIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def string_to_time(dt:datetime, fname=False):
    """ Use standard module format to convert string to time """
    timestr = dt.strptime(STRFTIME_FORMAT)
    if fname:
        timestr = multi_replace(timestr,("-",":"," "),"_")
    return timestr

def stt(dt:datetime, fname=False):
    """alias for string_to_time"""
    return string_to_time(dt=dt, fname=fname)

def time_to_string(timestr:str, from_fname=False):
    """ Use standard module format to convert time to string"""
    fmt = STRFTIME_FORMAT
    if from_fname:
        fmt = multi_replace(fmt,("-",":"," "),"_")
    return datetime.strftime(timestr, fmt)

def tts(timestr:str, from_fname=False):
    """alias for time_to_string"""
    return time_to_string(timestr=timestr, from_fname=from_fname)

