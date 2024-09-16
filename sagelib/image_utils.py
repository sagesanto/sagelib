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
from matplotlib.colors import LogNorm


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

# @pchoi @Pei Qin
def show_img(img, title=None,titlesize=14,scale="log"):
    if scale == "zscale" or scale == "z":
        norm = ImageNormalize(img, interval=ZScaleInterval(nsamples=600, contrast=0.25))
    elif scale == "log":
        from astropy.stats import sigma_clipped_stats
        mean, med, std = sigma_clipped_stats(img,sigma=3)
        norm=LogNorm(vmin=mean)
    else:
        raise ValueError(f"'{scale}' is not a valid scaling option. Valid options are: 'log', 'zscale', 'z'")
    # would be nice to be able to choose between arcseconds and pixels for axes - plate scale in config file
    fig, ax = plt.subplots()
    fig.set_size_inches(6,6)

    ax.imshow(img, cmap='Greys_r', origin='lower', norm=norm)
    if title != None:
        ax.set_title(title, fontsize=titlesize)   
    plt.show()