import sys,os
import tomlkit

from sagelib import get_user_config_path
from .utils import format_dark_name, format_flat_name


CALIB_CONFIG = get_user_config_path("calib.toml")

if not os.path.exists(CALIB_CONFIG):
    from tomlkit import comment, document, nl

    doc = document()
    doc.add(comment("configuration for sagelib calibration routines"))
    doc.add(nl())
    cfg = { "calib_path": "replaceme", 
            "flat_subdir":"flats",
            "dark_subdir":"darks",
            "bias_subdir":"bias",
            "bias_pattern": "SuperBias.fits",   
            "dark_pattern": "SuperDark_{exptime}s.fits",   
            "flat_pattern": "SuperNormFlat_{filter}.fits"}
    for k,v in cfg.items():
        doc.add(k,v)

    doc["calib_path"].comment("points to directory in which calibration files can be found")
    doc["dark_pattern"].comment("if provided, {exptime} will be replaced with frame exposure time when darks are queried")
    doc["flat_pattern"].comment("if provided, {filter} will be replaced with frame filter name when flats are queried")
    with open(CALIB_CONFIG,"w") as f:
        f.write(tomlkit.dumps(doc))