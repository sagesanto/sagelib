import sys
from astropy import wcs, utils
import os
import glob
from pathlib import Path
from astropy.io import fits
import numpy as np
import pandas as pd
from astropy.nddata import CCDData
import sys
import six
sys.modules['astropy.extern.six'] = six

from datetime import datetime, timedelta

# for display 
import matplotlib.pyplot as plt
from astropy.visualization import ZScaleInterval
from astropy.visualization.mpl_normalize import ImageNormalize
import matplotlib.pyplot as plt


#@pchoi @Pei Qin
def read_ccddata_ls(ls_toOp, data_dir, return_ls = False):
    if data_dir[-1] != '/':
        data_dir = data_dir + '/'
    if isinstance(ls_toOp, str):
        input_ls = pd.read_csv(ls_toOp, header = None)
        ls = input_ls[0]
    else:
        ls = ls_toOp
    toOp = []
    for i in ls:
        toOp.append(CCDData.read(data_dir + i, unit = 'adu'))
    toReturn = toOp
    if return_ls:
        toReturn = (toOp, ls)
    return toReturn