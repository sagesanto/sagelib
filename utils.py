import os
import tomli
from datetime import datetime, timedelta
# from datetime import UTC as dtUTC
import pytz
from pytz import UTC

class Config:
    def __init__(self,filepath,default_env_key="CONFIG_DEFAULTS"):
        self._cfg = read_config(filepath)
        self.selected_profile = None
        self._defaults = None
        self._filepath = filepath 
        self.selected_profile_name = None
        self._default_path = os.getenv(default_env_key)
        if self._default_path:
            try:
                self._defaults = read_config(self._default_path)
            except Exception as e:
                print(f"ERROR: config tried to load defaults file {self._default_path} but encountered the following: {e}")
                print(f"Preceding without defaults")

    def choose_profile(self, profile_name):
        self.selected_profile = self._cfg[profile_name]
        self.selected_profile_name = profile_name
        return self
    
    def load_defaults(self, filepath):
        self._defaults = read_config(filepath)
        self._default_path = filepath

    @property
    def has_defaults(self):
        return self._defaults is not None
    
    def get_default(self, key):
        if not self.has_defaults:
            raise AttributeError("No default configuration set!")
        return self._defaults[key]
    
    def set(self,key,value):
        self._cfg[key] = value

    def __call__(self, index):
        return self.__getitem__(index)

    def __getitem__(self,index):
        if self.selected_profile:
            try:
                return self.selected_profile[index]
            except Exception as e:
                pass
        try:
            return self._cfg[index]    
        except Exception as e:
            pass
            if self.has_defaults:
                return self.get_default(index)
    
    def __str__(self):
        self_str = ""
        if self.selected_profile:
            self_str = f"(Profile '{self.selected_profile_name}') "
        
        self_str += str(self._cfg)
        if self.has_defaults:
            self_str += f"\nDefaults: {self._defaults}"
        return self_str

    def __repr__(self) -> str:
        return f"Config from {self._filepath} with {'profile ' + self.selected_profile_name if self.selected_profile_name else 'no profile'} selected and {f'defaults loaded from {self._default_path}' if self.has_defaults else ' no defaults loaded'}" 


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

