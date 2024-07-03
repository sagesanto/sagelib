import os
import tomli
from datetime import datetime, timedelta
import pytz
from pytz import UTC
from typing import List, Any

class Config:
    def __init__(self,filepath:str,default_env_key:str="CONFIG_DEFAULTS"):
        self._cfg = _read_config(filepath)
        self.selected_profile = None
        self._defaults = None
        self._filepath = filepath 
        self.selected_profile_name = None
        self._default_path = os.getenv(default_env_key)
        if self._default_path:
            try:
                self._defaults = _read_config(self._default_path)
            except Exception as e:
                print(f"ERROR: config tried to load defaults file {self._default_path} but encountered the following: {e}")
                print("Preceding without defaults")

    def choose_profile(self, profile_name:str):
        self.selected_profile = self._cfg[profile_name]
        self.selected_profile_name = profile_name
        return self
    
    def clear_profile(self):
        self.selected_profile = None
        self.selected_profile_name = None
    
    def load_defaults(self, filepath:str):
        self._defaults = _read_config(filepath)
        self._default_path = filepath

    @property
    def has_defaults(self):
        return self._defaults is not None
    
    def _get_default(self, key:str):
        if not self.has_defaults:
            raise AttributeError("No default configuration set!")
        return self._defaults[key]

    def get_default(self, key:str, default:Any|None=None):
        try: 
            self._get_default(key)
        except KeyError:
            return default
    
    def get(self,key:str,default:Any=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def set(self,key:str,value:Any,profile:bool=False):
        if profile:
            if not self.selected_profile:
                raise AttributeError("No config profile selected.")
            self.selected_profile[key] = value
        else:
            self._cfg[key] = value

    def __call__(self, index:str) -> Any:
        return self.__getitem__(index)

    def __getitem__(self,index:str) -> Any:
        if self.selected_profile:
            try:
                return self.selected_profile[index]
            except Exception:
                pass
        try:
            return self._cfg[index]    
        except Exception:
            if self.has_defaults:
                return self._get_default(index)
    
    def __str__(self):
        self_str = ""
        if self.selected_profile:
            self_str = f"(Profile '{self.selected_profile_name}') "
        
        self_str += str(self._cfg)
        if self.has_defaults:
            self_str += f"\nDefaults: {self._defaults}"
        return self_str

    def __repr__(self) -> str:
        return f"Config from {self._filepath} with {f'profile {self.selected_profile_name}' if self.selected_profile_name else 'no profile'} selected and {f'defaults loaded from {self._default_path}' if self.has_defaults else ' no defaults loaded'}"


def current_dt_utc():
    return datetime.now(UTC)

def dt_to_tz(dt:datetime, tz:pytz.BaseTzInfo|str, require_existing_timezone:bool=False) -> datetime:
    """Take an input datetime and transform it to the input timezone

    :type dt: datetime
    :param tz: the desired ending timezone
    :type tz: pytz.BaseTzInfo | str
    :param require_existing_timezone: whether to raise an error if the `dt` has no timezone set. If this is False and `dt` is missing a timezone, it will simply have its timezone set to be `tz`. , defaults to False
    :type require_existing_timezone: bool, optional
    :raises AttributeError: if require_existing_timezone is true and dt is missing a timezone
    :return: the datetime object with its timezone set
    :rtype: datetime
    """
    if require_existing_timezone and dt.tzinfo is None:
        raise AttributeError(f"{dt} is missing a timezone!")
    if isinstance(tz, str):
        tz = pytz.timezone(tz)
    return dt.astimezone(tz)

def dt_to_utc(dt:datetime, require_existing_timezone:bool=False) -> datetime:
    """A convenience wrapper around :func:`dt_to_tz` for when `tz` is UTC. See :func:`dt_to_tz` for details

    :type dt: datetime
    :param require_existing_timezone: defaults to False
    :type require_existing_timezone: bool, optional
    :return: the input datetime, in UTC
    :rtype: datetime
    """
    return dt_to_tz(dt, UTC, require_existing_timezone)

def _read_config(config_path:str):
    with open(config_path, "rb") as f:
        cfg = tomli.load(f)
    return cfg

def multi_replace(string:str, old_strs:list[str], subst_str:str) -> str:
    # WARNING: this is clumsy and can get behave unexpectedly if subst_str and one of old_strs are too similar 
    for s in old_strs:
        string = string.replace(s, subst_str)
    return string

STRFTIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def time_to_string(dt:datetime, fname:bool=False):
    """Use standard sagelib module format to convert string to time

    :type dt: datetime
    :param fname: whether the output string should be formatted for use in a file, defaults to False
    :type fname: bool, optional
    :return: the string representation of a time, in module format
    :rtype: str
    """
    timestr = dt.strftime(STRFTIME_FORMAT)
    if fname:
        timestr = multi_replace(timestr,("-",":"," "),"_")
    return timestr

def tts(dt:datetime, fname:bool=False):
    """alias for :func:`time_to_string`"""
    return time_to_string(dt=dt, fname=fname)

def stt(timestr:str, from_fname:bool=False):
    """alias for :func:`string_to_time`"""
    return string_to_time(timestr=timestr, from_fname=from_fname)

def string_to_time(timestr:str, from_fname:bool=False) -> datetime:
    """Use standard sagelib module format to convert time to string

    :param timestr: the string, matching one of the two formats generated by :func:`time_to_string`
    :type timestr: str
    :param from_fname: whether the time should be read as if `timestr` is formatted for use in a filename, defaults to False
    :type from_fname: bool, optional
    :rtype: datetime
    """
    fmt = STRFTIME_FORMAT
    if from_fname:
        fmt = multi_replace(fmt,("-",":"," "),"_")
    return datetime.strptime(timestr, fmt)

def now_stamp() -> str:
    """equivalent to :func:`tts()` of :func:`current_dt_utc()` 

    :return: string representation of the current time, in module form
    :rtype: str
    """
    return tts(current_dt_utc())
