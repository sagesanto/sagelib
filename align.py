from Frame import Frame, findAllIn, read_ccddata_ls
import sys
import warnings
from astropy import wcs, utils
import os
import glob
from pathlib import Path
from astropy.io import fits
import numpy as np
import configparser
import argparse
import pandas as pd
from astropy.nddata import CCDData
import sys
import six
sys.modules['astropy.extern.six'] = six
import ccdproc
import alipy
# displaying imports
import matplotlib.pyplot as plt
from astropy.visualization import ZScaleInterval
from astropy.visualization.mpl_normalize import ImageNormalize

from photutils.utils import calc_total_error
from photutils.aperture import CircularAperture, CircularAnnulus, aperture_photometry
#from photutils.aperture import CircularAperture, CircularAnnulus, aperture_photometry, ApertureStats
from photutils.detection import DAOStarFinder
import argparse

warnings.filterwarnings("ignore", category=wcs.FITSFixedWarning)
warnings.filterwarnings("ignore", category=utils.exceptions.AstropyDeprecationWarning)


def align(img_dir, pattern,ref_image_path,aligned_out,target_name, also_make_combined_aligned=False):
    # print(os.listdir(img_dir))
    images_to_align = sorted(glob.glob(os.path.join(img_dir,pattern)))
    if not len(images_to_align):
        print(f"No images found in {img_dir} matching pattern {pattern}")
        exit()
    print()
    print("Aligning the following files:",images_to_align)
    if ref_image_path is None:
        ref_image_path = images_to_align[0]
        print(f"No reference image provided. Using {ref_image_path} as reference image.")
    # for img in images_to_align:
    #     print(img, Frame.from_fits(img).img.ndim)
    identifications = alipy.ident.run(ref_image_path, images_to_align, visu=False,verbose=False,hdu=1)
    print("ID'd")
    outputshape = alipy.align.shape(ref_image_path)


    if not os.path.exists(aligned_out):
        os.mkdir(aligned_out)
    unable_to_align = []
    for id in identifications:
        if id.ok == True:
            alipy.align.affineremap(id.ukn.filepath, id.trans, shape=outputshape, makepng=True,outdir=aligned_out,verbose=False)
        else:
            unable_to_align.append(id.ukn.filepath)

    print(f"Was not able to align the following {len(unable_to_align)} files: {unable_to_align}")

    if also_make_combined_aligned:
        aligned_ls = findAllIn(data_dir = aligned_out, file_matching='fdb_*.fits')

        # running ccdproc.combine() + saving resulting image
        aligned_slices = read_ccddata_ls(aligned_ls, aligned_out)

        result_img = ccdproc.combine(aligned_slices,
                                method='average',
                                sigma_clip=True, sigma_clip_low_thresh=3, sigma_clip_high_thresh=3,
                                sigma_clip_func=np.ma.average)
        result_img.meta['combined'] = True
        result_img.write(aligned_out/Path(f'combined_{target_name}.fits'),overwrite=True)
    print("Done aligning.")

#"fdb_*.fits"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Image alignment')
    parser.add_argument('input_dir', type=str, help='directory containing images to align')
    parser.add_argument('pattern', type=str, help='Pattern')
    parser.add_argument('out_dir', type=str, help='Output directory')
    parser.add_argument('target_name', type=str, help='Target name')
    parser.add_argument('--ref_img', type=str, default=None, help='Optional path to reference image to align to. If not provided, will use first image in input_dir')

    args = parser.parse_args()

    input_dir = args.input_dir
    ref_img = args.ref_img
    pattern = args.pattern
    out_dir = args.out_dir
    target_name = args.target_name

    align(input_dir,pattern,ref_img,out_dir,target_name)