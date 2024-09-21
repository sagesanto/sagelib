from sagelib import USER_CONFIG_FOLDER, _set_flag, _get_flag, VERSION
from sagelib.calib import CALIB_CONFIG # prompt the setup to occur
from os.path import join
import os


TEST_DIR = join(USER_CONFIG_FOLDER,".test")
TEST_DATA_PATH = join(TEST_DIR,"data")
TEST_CALIB_PATH = join(TEST_DIR,"calib")
TEST_CONFIG_PATH = join(TEST_DIR,"test_config.toml")

# need to switch over to using a test config file when doing reduce.py tests so that it will look at TEST_CALIB_PATH etc
# reduce.py should take config file as an optional argument

os.makedirs(TEST_DATA_PATH,exist_ok=True)
os.makedirs(TEST_CALIB_PATH,exist_ok=True)

_set_flag("hi","hi")
print(_get_flag("hi"))

cfg_ver = _get_flag("TEST_CFG_VERSION")
remake_config = not os.path.exists(TEST_CONFIG_PATH) or not cfg_ver or cfg_ver != VERSION

if remake_config:
    print("Remaking testing config...")
    import tomlkit

    # load the user's config (we assume that it will always be up to date), then make changes and save as test config 
    with open(CALIB_CONFIG, "rb") as f:
        cfg = tomlkit.load(f)

    cfg["data"] = {"pattern": r"*.fits",
         "recursive_search": False,
         "regex": False
        }
    cfg["flats"] = {"pattern":"TestFlat_{filter}.fits",
         "recursive_search": False,
         "regex": False
        }
    cfg["darks"] = {"pattern":"TestDark_{exptime}s.fits",
         "recursive_search": False,
         "regex": False
        }
    cfg["biases"] = {"pattern":"TestBias.fits",
         "recursive_search": False,
         "regex": False
        }
    cfg["calib_path"] = TEST_CALIB_PATH
    cfg["date_format_in"] = "%Y-%m-%dT%H:%M:%S.%f+00:00"
    cfg["date_format_out"] = "%Y-%m-%dT%H:%M:%S.%f+00:00"

    with open(TEST_CONFIG_PATH,"w") as f:
        f.write(tomlkit.dumps(cfg))
    _set_flag("TEST_CFG_VERSION",VERSION)