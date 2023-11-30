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

from image_utils import read_ccdata_ls, findAllIn, increment_date, show_img


def open_frames_in_chunks(filename_list,max_size_mb):
    """
    Open frames in groups of size max_size_mb
    @param max_size_mb: maximum size of chunk in MB
    @return: generator of chunks of frames
    """
    max_size_bytes = max_size_mb*1024*1024
    current_size = 0
    current_chunk = []
    for filename in filename_list:
        filesize = os.path.getsize(filename)
        if current_size+filesize > max_size_bytes:
            yield current_chunk
            current_chunk = []
            current_size = 0
        current_chunk.append(filename)
        current_size += filesize
    yield current_chunk


class FrameSet:
    def __init__(self,filename_list,max_chunk_size_mb):
        self.filename_list = filename_list
        self.max_chunk_size_mb = max_chunk_size_mb
        self.frames = []
    def __enter__(self):
        self.chunks = open_frames_in_chunks(self.filename_list, max_size_mb=self.max_chunk_size_mb)
        self.chunk_generator = (Frame.from_fits(filename) for chunk in self.chunks for filename in chunk)
        return self.chunk_generator
    
    def __exit__(self, exc_type, exc_value, traceback):
        # for frame in self.frames:
        #     frame.img = None
        #     frame.header = None
        # self.frames = []
        # return False
        pass

class Frame:
    def __init__(self, img, name, header=None, savepath=None) -> None:
        self.img = img.astype(np.float32)
        self.header = header
        self.name = name
        self._median = self._mean = self._stdev = None
    
    @classmethod
    def from_fits(cls,path,kwargs={}):
        f = fits.open(Path(path))
        img = f[0].data
        header = f[0].header
        f.close()
        name = str(path).split(os.sep)[-1].replace(".fits",'').replace(".fit",'')
        return cls(img,name=name,header=header,savepath=path,**kwargs)

    def calc_stats(self):
        self.median = np.median(self.img)
        self.mean = np.mean(self.img)
        self.stdev = np.std(self.img)
    
    @property
    def median(self):
        if self._median is None:
            self.calc_stats()
        return self._median
    
    @property
    def mean(self):
        if self._mean is None:
            self.calc_stats()
        return self._mean
    
    @property
    def stdev(self):
        if self._stdev is None:
            self.calc_stats()
        return self._stdev

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
        if name_extension is None:
            name_extension = '_00'
        for i, im in enumerate(self.img):
            newName = self.name + name_extension + str(i+1)
            if self.header:
                newheader = self.header.copy()
                if tincrement is not None and start_time is not None:
                    newheader['DATE-OBS'] = increment_date(start_time, tincrement * i)
            else:
                newheader = fits.PrimaryHDU(do_not_scale_image_data=True, ignore_blank=True)
            
            frames.append(Frame(img=im,name=newName,header=newheader))
        print(f'Successfully sliced {self.name} into {len(frames)} frames.')
        return frames

    def write_fits(self,filename,overwrite=False):
        fits.writeto(filename, self.img, header=self.header, overwrite=overwrite)
    
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
