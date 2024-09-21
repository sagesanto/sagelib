from sagelib.utils import Config

def format_flat_name(calib_cfg, filter_name:str):
    return calib_cfg["flats"]["pattern"].replace("{filter}",str(filter_name)) 

def format_dark_name(calib_cfg:Config, exptime):
    return calib_cfg["darks"]["pattern"].replace("{exptime}",str(exptime))