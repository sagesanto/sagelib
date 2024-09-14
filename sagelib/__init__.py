
import os
from .frame import Frame, FrameSet

py_in_dir = [os.path.splitext(f)[0] for f in os.listdir(os.path.dirname(__file__)) if f.endswith('.py') and not f.startswith('_')]

__all__ = py_in_dir + ["Frame", "FrameSet"]

import importlib.resources as pkg_r

def logging_config():
    with pkg_r.path('sagelib.config', 'logging.json') as config_path:
        return config_path