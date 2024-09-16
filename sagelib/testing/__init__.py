from sagelib import USER_CONFIG_FOLDER
from os.path import join
import os


TEST_DIR = join(USER_CONFIG_FOLDER,".test")
TEST_DATA_PATH = join(TEST_DIR,"data")
TEST_CALIB_PATH = join(TEST_DIR,"calib")

# need to switch over to using a test config file when doing reduce.py tests so that it will look at TEST_CALIB_PATH etc
# reduce.py should take config file as an optional argument


os.makedirs(TEST_DATA_PATH,exist_ok=True)
os.makedirs(TEST_CALIB_PATH,exist_ok=True)