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


#@pchoi @Pei Qin
def findAllIn(data_dir, file_matching, contain_dir=False, save_ls=True, save_name=None):
    if data_dir[-1] != '/':
        data_dir = data_dir + '/'
    if save_name == None:
        save_name = 'all_' + file_matching + '.txt'
    list_files = glob.glob(data_dir + file_matching)
    if not contain_dir:
        list_files[:] = (os.path.basename(i) for i in list_files)
    if save_ls:
        with open(data_dir + save_name, "w") as output:
            for i in list_files:
                output.write(str(i) + '\n')
    return list_files


# adapted from @Pei Qin
def increment_date(strdate, tincrement):
    parsed = datetime.strptime(strdate,"%Y-%m-%dT%H:%M:%S+00:00")
    later = parsed + timedelta(seconds=tincrement)
    incremented_dateobj = later.strftime('%Y-%m-%dT%X.0000')
    return incremented_dateobj

# @pchoi @Pei Qin
def show_img(img, title=None,titlesize=14):
    norm = ImageNormalize(img, interval=ZScaleInterval(nsamples=600, contrast=0.25))
    # would be nice to be able to choose between arcseconds and pixels for axes - plate scale in config file
    fig, ax = plt.subplots()
    fig.set_size_inches(6,6)
    ax.imshow(img, cmap='Greys_r', origin='lower', norm=norm)
    if title != None:
        ax.set_title(title, fontsize=titlesize)   
    plt.show()