import sys,os, shutil
import tomlkit

from sagelib import get_user_config_path, _set_flag, _get_flag, VERSION
from .utils import format_dark_name, format_flat_name


CALIB_CONFIG = get_user_config_path("calib.toml")

cfg_ver = _get_flag("USER_CFG_VERSION")
remake_config = not os.path.exists(CALIB_CONFIG) or not cfg_ver or cfg_ver != VERSION

if os.path.exists(CALIB_CONFIG) and remake_config:
    shutil.copy(CALIB_CONFIG,CALIB_CONFIG+".old")

if remake_config:
    print("Remaking user config...")
    from tomlkit import comment, document, nl, item

    doc = document()
    doc.add(comment("configuration for sagelib calibration routines"))
    doc.add(nl())
    doc.add(comment("path specifications"))
    doc.add(comment("\tpattern: the pattern to match when looking for file(s)"))
    doc.add(comment("\tregex: if true, can use python-flavor regex in the 'pattern' field. else, will match using unix path wildcards. NOTE: will match files that start with '.' by default."))
    doc.add(comment("\trecursive: whether to recursively search in sub-directories for files matching 'pattern'"))
    doc.add(nl())

    data_table = item(
        {"pattern": r"^[^\.].*\.fits$",
         "recursive_search": False,
         "regex": True
        }
    )
    flats_table = item(
        {"pattern":"SuperNormFlat_{filter}.fits",
         "recursive_search": False,
         "regex": False
        }
    )
    darks_table = item(
        {"pattern":"SuperDark_{exptime}s.fits",
         "recursive_search": False,
         "regex": False
        }
    )
    bias_table = item(
        {"pattern":"SuperBias.fits",
         "recursive_search": False,
         "regex": False
        }
    )
    cfg = { "calib_path": "replaceme",
            "data": data_table,
            "flats": flats_table,   
            "darks": darks_table,   
            "biases": bias_table,
            "date_format_in": "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "date_format_out": "%Y-%m-%dT%H:%M:%S.%f+00:00"}
    for k,v in cfg.items():
        doc.add(k,v)
    # print(doc.items.())
    print(doc["data"].__dict__)
    # print(doc.keys())
    doc["calib_path"].comment("points to directory in which calibration files can be found")
    doc["darks"].comment("in pattern field: if provided, {exptime} will be replaced with frame exposure time when darks are queried")
    doc["flats"].comment("in pattern field: if provided, {filter} will be replaced with frame filter name when flats are queried")
    doc["date_format_in"].comment("datetime format of datetime as appears in input fits file headers")
    doc["date_format_out"].comment("datetime format that should be used when writing output fits file headers")
    with open(CALIB_CONFIG,"w") as f:
        f.write(tomlkit.dumps(doc))
    _set_flag("USER_CFG_VERSION",VERSION)