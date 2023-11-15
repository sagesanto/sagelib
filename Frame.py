import sys
import warnings
from astropy import wcs, utils
warnings.filterwarnings("ignore", category=wcs.FITSFixedWarning)

warnings.filterwarnings("ignore", category=utils.exceptions.AstropyDeprecationWarning)

from dateutil.parser import parse
from datetime import *; from dateutil.relativedelta import *

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
import ccdproc
import matplotlib.pyplot as plt
from astropy.visualization import ZScaleInterval
from astropy.visualization.mpl_normalize import ImageNormalize

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


# @Pei Qin
def increment_date(strdate, tincrement):
    parsed = parse(strdate)
    later = parsed + relativedelta(seconds=tincrement)
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

class Frame:
    def __init__(self, img, name, header=None, savepath=None) -> None:
        self.img = img.astype(np.float32)
        self.header = header
        self.name = name
    
    @classmethod
    def from_fits(cls,path):
        f = fits.open(Path(path))
        img = f[0].data
        header = f[0].header
        f.close()
        name = str(path).split(os.sep)[-1].replace(".fits",'').replace(".fit",'')
        return cls(img,name=name,header=header,savepath=path)

    def calc_stats(self):
        self.median = np.median(self.img)
        self.mean = np.mean(self.img)
        self.stdev = np.std(self.img)

    def show(self):
        show_img(self.img,title=self.name,titlesize=14)
    
    def __add__(self,other):
        data = self.img+other.img
        f = Frame(img=data,name=self.name)
        f.header = self.header
        return f
    
    def __sub__(self,other):
        data = self.img-other.img
        f = Frame(img=data,name=self.name)
        f.header = self.header
        return f    
    
    # largely lifted from @Pei Qin
    def slice(self, name_extension=None, tincrement=None):
        """
        Slice this cube into its constituent frames, returning the sliced frames in a list. This cube is unchanged.
        """
        if self.img.ndim != 3:
            raise ValueError("Incorrect number of dimensions to slice - must have exactly 3")
        frames = []
        try:
            start_time = self.header['DATE-OBS']
        except:
            start_time = None
        counter = 0
        if name_extension is None:
            name_extension = '_00'
        for i in self.img:
            newName = self.name + name_extension + str(counter+1)
            if self.header:
                newheader = self.header
                if tincrement != None and start_time:
                    newheader['DATE-OBS'] = increment_date(start_time, tincrement * counter)
            else:
                newheader = fits.PrimaryHDU(do_not_scale_image_data=True, ignore_blank=True)
            
            frames.append(Frame(img=i,name=newName,header=newheader))
            counter += 1
        print(f'Successfully sliced {self.name}.')
        return frames

    def write_fits(self,filename):
        fits.writeto(filename, self.img, header=self.header)
    
    def __mul__(self,other):
        if isinstance(other,Frame):
            raise NotImplementedError("Multiplication of two Frames is not yet supported")
        else:
            return Frame(img=self.img*other, name=self.name, header=self.header)
    
    def __rmul__(other,self):
        return self*other
    
    def __truediv__(self,other):
        if isinstance(other,Frame):
            return Frame(img = self.img/other.img, name = self.name, header = self.header)
        return Frame(img = self.img/other, name = self.name, header = self.header)
    
    def __div__(self,other):
        if isinstance(other,Frame):
            return Frame(img = self.img/other.img, name = self.name, header = self.header)
        return Frame(img = self.img/other, name = self.name, header = self.header)
        


    def __str__(self):
        self.calc_stats()
        attrs = [
            f"Image {self.name}",
            f"Shape: {self.img.shape}",
            f"Header: {bool(self.header)}",
            f"Median Pixel: {self.median}",
            f"Mean Pixel: {self.mean}",
            f"Std Dev: {self.stdev}",
            ""
        ]
        return "\n".join(attrs)

    def __repr__(self) -> str:
        return f"Frame {self.name}"
