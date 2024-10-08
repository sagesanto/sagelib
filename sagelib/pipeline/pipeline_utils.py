import sys, os
MODULE_PATH = os.path.abspath(os.path.dirname(__file__))
def mod(path): return os.path.join(MODULE_PATH,path)
import json
import numpy as np
import logging
from pathlib import Path
import logging.config
from sagelib import logging_config

# try:
#     from .multiprocess_logging import install_mp_handler
# except:
#     from multiprocess_logging import install_mp_handler

BAD_SEX_FLAGS = np.array([8,16,32,64,128])

def ldac_to_table(fits_file,frame=1):
    import astromatic_wrapper as aw
    return aw.utils.ldac.get_table_from_ldac(fits_file, frame=frame)

def configure_logger(name, outfile_path):
    # first, check if the logger has already been configured
    if logging.getLogger(name).hasHandlers():
        return logging.getLogger(name)
    try:
        with open(logging_config(), 'r') as log_cfg:
            logging.config.dictConfig(json.load(log_cfg))
            logger = logging.getLogger(name)
            # set outfile of existing filehandler. need to do this instead of making a new handler in order to not wipe the formatter off
            # NOTE RELIES ON FILE HANDLER BEING THE SECOND HANDLER
            root_logger = logging.getLogger()
            file_handler = root_logger.handlers[1]
            file_handler.setStream(Path(outfile_path).open('a'))
            try:
                os.remove("should_be_set_by_code.log")  # pardon this
            except:
                pass

    except Exception as e:
        print(f"Can't load logging config ({e}). Using default config.")
        logger = logging.getLogger(name)
        file_handler = logging.FileHandler(outfile_path, mode="a+")
        logger.addHandler(file_handler)

    # install_mp_handler()
    return logger

def check_sextractor_flags(flag, bad_flags = BAD_SEX_FLAGS):
    row = np.zeros_like(bad_flags)
    row.fill(flag)
    return not np.any(np.bitwise_and(row,bad_flags))