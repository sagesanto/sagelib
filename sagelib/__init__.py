
import os
from os.path import join, dirname, splitext, exists
from .frame import Frame, FrameSet
import sqlite3

py_in_dir = [splitext(f)[0] for f in os.listdir(dirname(__file__)) if f.endswith('.py') and not f.startswith('_')]

__all__ = py_in_dir + ["Frame", "FrameSet"]

import importlib.resources as pkg_r
from importlib.metadata import version

def get_pkg_config_path(cfg_name):
    with pkg_r.path('sagelib.config', cfg_name) as config_path:
        return config_path

def logging_config():
    return get_pkg_config_path("logging.json")

from astropy.config.paths import _find_home

HOME = _find_home()
USER_CONFIG_FOLDER = join(_find_home(),".sagelib")
os.makedirs(USER_CONFIG_FOLDER,exist_ok=True)

VERSION = version("sagelib")

def get_user_config_path(cfg_name):
    return join(USER_CONFIG_FOLDER,cfg_name)

FLAGS_DB_PATH = join(USER_CONFIG_FOLDER,".flags.db")
__flags_db = sqlite3.connect(FLAGS_DB_PATH)
__cur = __flags_db.cursor()

create_statement = 'CREATE TABLE IF NOT EXISTS "flags" (\n"key"\tSTRING NOT NULL UNIQUE,\n"value"\tSTRING NOT NULL,\n"ID"\tINTEGER,\nPRIMARY KEY("ID" AUTOINCREMENT)\n)'
__cur.execute(create_statement)

def _get_flag(key):
    __cur.execute("SELECT * FROM flags WHERE key==?",(key,))
    r = __cur.fetchone()
    if not r: return r
    return r[1] # key, value, id

def _set_flag(key,value):
    __cur.execute("INSERT OR IGNORE INTO flags(key, value) Values (?,?)",(key,value))
    __cur.execute("UPDATE flags SET value = ? WHERE key = ?",(value, key))
    __flags_db.commit()
