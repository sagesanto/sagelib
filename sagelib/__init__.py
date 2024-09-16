
import os
from .frame import Frame, FrameSet

py_in_dir = [os.path.splitext(f)[0] for f in os.listdir(os.path.dirname(__file__)) if f.endswith('.py') and not f.startswith('_')]

__all__ = py_in_dir + ["Frame", "FrameSet"]

import importlib.resources as pkg_r

def get_pkg_config_path(cfg_name):
    with pkg_r.path('sagelib.config', cfg_name) as config_path:
        return config_path

def logging_config():
    return get_pkg_config_path("logging.json")

from astropy.config.paths import _find_home

HOME = _find_home()
USER_CONFIG_FOLDER = os.path.join(_find_home(),".sagelib")
os.makedirs(USER_CONFIG_FOLDER,exist_ok=True)

def get_user_config_path(cfg_name):
    return os.path.join(USER_CONFIG_FOLDER,cfg_name)