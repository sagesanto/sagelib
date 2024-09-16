import sys, os, shutil
from os.path import join

from sagelib.utils import Config
from sagelib.synthesizer.calibs import flat
from sagelib.synthesizer.data import starfield
from sagelib.calib import CALIB_CONFIG, format_dark_name, format_flat_name
from sagelib.testing import TEST_DATA_PATH, TEST_CALIB_PATH, TEST_DIR
from sagelib.image_utils import show_img
from sagelib.calib.bin import reduce

from astropy.io import fits
from subprocess import Popen
from matplotlib.colors import LogNorm
import matplotlib.pyplot as plt

cfg = Config(CALIB_CONFIG)
print(CALIB_CONFIG)

print(TEST_DIR)

# make a new config with a test calibration path instead of the user's
cfg["calib_path"] = TEST_CALIB_PATH
config_path = join(TEST_DIR,"test_calib.toml")
cfg.write(config_path)

mean_bkg = 5
flat_val = 1.5 * mean_bkg
imgsize = 200
FILTNAME = "SYNTH"
EXPTIME = 1 # seconds

# make fake data
orig = starfield(imgsize=imgsize, mean_bkg=mean_bkg)
data_hdul = fits.HDUList(fits.PrimaryHDU(data=orig))
data_hdul[0].header["FILTER"] = FILTNAME
data_hdul[0].header["EXPTIME"] = EXPTIME
data_hdul.writeto(join(TEST_DATA_PATH,"test.fits.orig"),overwrite=True)

# make fake flats
f = flat(imgsize,flat_val)
flat_hdul = fits.HDUList(fits.PrimaryHDU(data=f))
flat_hdul[0].header["FILTER"] = FILTNAME
flat_hdul[0].header["EXPTIME"] = EXPTIME
flatname = format_flat_name(cfg,FILTNAME)
flat_hdul.writeto(join(TEST_CALIB_PATH,flatname),overwrite=True)

test_img = orig+f
img_hdul = fits.HDUList(fits.PrimaryHDU(data=test_img))
img_hdul[0].header["FILTER"] = FILTNAME
img_hdul[0].header["EXPTIME"] = EXPTIME
img_hdul.writeto(join(TEST_DATA_PATH,"test.fits"),overwrite=True)

show_img(test_img,"Test Data")

outdir = join(TEST_DIR,"reduce_out")
if os.path.exists(outdir):
    shutil.rmtree(outdir)
p = Popen([sys.executable, reduce.__file__, "TEST", TEST_DATA_PATH, outdir, "-f", "-c", config_path])
p.wait()