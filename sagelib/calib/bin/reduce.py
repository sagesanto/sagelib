#! python

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
import shutil
sys.modules['astropy.extern.six'] = six
import ccdproc
from inspect import getsourcefile
from os.path import abspath
# displaying imports
import matplotlib.pyplot as plt
from astropy.visualization import ZScaleInterval
from astropy.visualization.mpl_normalize import ImageNormalize

from photutils.utils import calc_total_error
from photutils.aperture import CircularAperture, CircularAnnulus, aperture_photometry
#from photutils.aperture import CircularAperture, CircularAnnulus, aperture_photometry, ApertureStats
from photutils.detection import DAOStarFinder

warnings.filterwarnings("ignore", category=wcs.FITSFixedWarning)
warnings.filterwarnings("ignore", category=utils.exceptions.AstropyDeprecationWarning)

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

def show_img(img, title=None):
    norm = ImageNormalize(img, interval=ZScaleInterval(nsamples=600, contrast=0.25))

    fig, ax = plt.subplots()
    fig.set_size_inches(6,6)
    ax.imshow(img, cmap='Greys_r', origin='lower', norm=norm)
    if title != None:
        ax.set_title(title, fontsize=20)
    # ax.tick_params(labelsize='large', width=1)

    plt.show()

def main():
    parser = argparse.ArgumentParser(description="Perform slicing, calibration, and alignment on input fits files. If no operations are specified, all will be performed.")
    parser.add_argument("target_name", action="store", type=str,help="the name of the target. no spaces")
    parser.add_argument("sci_data_dir", action="store", type = str, help = "the directory containing raw science data frames and/or cubes")
    parser.add_argument("output_dir", action="store", type = str, help = "the directory to place reduced output in. will be created if does not exist")
    # the following arguments will tell us which operations to perform on the input data:
    # -s : slice any cubes present. otherwise, will ignore cubes
    # -f : perform flat fielding
    # -d : perform dark subtraction
    # -b : perform bias subtraction
    # -a : perform alignment
    # -w : perform wcs solving (not implemented yet)
    # -i : save intermediate files

    # the default will be to do all of the above, but if any are specified, we don't do the ones that aren't specified

    parser.add_argument("-s", "--slice", action="store_true", help="slice any cubes present and process their frames too. otherwise, will ignore cubes")
    parser.add_argument("-f", "--flat", action="store_true", help="perform flat fielding")
    parser.add_argument("-d", "--dark", action="store_true", help="perform dark subtraction")
    parser.add_argument("-b", "--bias", action="store_true", help="perform bias subtraction")
    parser.add_argument("-a", "--align", action="store_true", help="perform alignment. will result in temporarily saving intermediate files. Will clear 'temp_align_dir' if it exists.")
    parser.add_argument("-w", "--wcs", action="store_true", help="(NOT IMPLEMENTED) perform wcs solving")

    parser.add_argument("-i", "--intermediate", action="store_true", help="save intermediate files throughout process. will overwrite existing files that share the same name in the intermediate directory.")

    parser.add_argument("--ref_image_path", action="store", type=str,help="the path to the reference image to use for alignment. if not specified, will use the first image in the input directory")

    parser.add_argument("-v", "--visualize", action="store_true", default=True, help="show superstack when finished")

    args = parser.parse_args()

    do_slice = args.slice
    do_flat = args.flat
    do_dark = args.dark
    do_bias = args.bias
    do_align = args.align
    do_wcs = args.wcs
    save_intermediate = args.intermediate
    ref_image_path = args.ref_image_path
    
    visualize = args.visualize

    # if no operation arguments are specified, do all of them
    if not (do_slice or do_flat or do_dark or do_bias or do_align or do_wcs):
        print("No operation arguments specified. Doing all operations.")
        do_slice = True
        do_flat = True
        do_dark = True
        do_bias = True
        do_align = True
        do_wcs = True

    for op_bool, op_name in zip([do_slice,do_flat,do_dark,do_bias,do_align,do_wcs],["slice","flat","dark","bias","align","wcs"]):
        if op_bool:
            print(f"Will perform {op_name} operation")
        else:
            print(f"Will not perform {op_name} operation")

    target_name = args.target_name.replace(" ","_")
    raw_data_dir = args.sci_data_dir
    output_dir = args.output_dir

    raw_data_dir = os.path.abspath(raw_data_dir)
    output_dir = os.path.abspath(output_dir)

    # path to this folder
    CALIB_ROOT = abspath(os.path.join(abspath(getsourcefile(lambda:0)),os.pardir))
    os.chdir(CALIB_ROOT)
    from sagelib.Frame import Frame

    calib_config = configparser.ConfigParser()
    calib_config.read('calib_config.txt')
    calib_config = calib_config["DEFAULT"]


    _CALIB_PATH = calib_config["calib_path"]

    filenames = [f for f in os.listdir(raw_data_dir) if not f.startswith(".") and (f.endswith("fits") or f.endswith("fit"))]
    print(f"Found the following files as input: {', '.join(filenames)}")

    frames = []
    cubenames = []
    # slice any cubes - into what directory should these go? should we then move non-cube data to that folder too before proceeding?
    for f in filenames:
        frame = Frame.from_fits(raw_data_dir/Path(f))
        d = frame.img.ndim
        if d > 2:
            cubenames.append(f)
            if do_slice:
                if d == 3:
                    print(f"Slicing cube {f}")
                    sliced = frame.slice(tincrement=float(frame.header["EXPTIME"]))
                    for sliced_frame in sliced:
                        frames.append(sliced_frame)
                        # p = raw_data_dir/Path(sliced_frame.name+".fits")
                        # print(f"Saving {p}")
                        # sliced_frame.write_fits(p)
                        # del(sliced_frame)
                else:
                    raise ValueError("Can't reduce data that isn't 2 or 3 dimensional")


    filenames = [f for f in os.listdir(raw_data_dir) if not f.startswith(".") and (f.endswith("fits") or f.endswith("fit"))]
    filenames = [f for f in filenames if f not in cubenames] # we don't delete the cubes after we slice them so we need to be careful not to re-open them
    filters = {}
    for f in filenames:
        print(f"Opening {f}")
        frame = Frame.from_fits(os.path.join(raw_data_dir,f))
        frames.append(frame)

    if not frames:
        print("No frames to process!")
        exit()

    reduced = []
    if do_bias:
        print("Subtracting superbias")
        super_bias = Frame.from_fits(_CALIB_PATH/Path("SuperBias.fits"))
        for i, frame in enumerate(frames):
            reduced.append(frames[i]-super_bias)
            reduced[i].name = "b_"+frames[i].name
        print("Bias subtracted")

    if not reduced:
        reduced = frames

    # all images must have the same exposure time!!!
    exptime = int(reduced[0].header["EXPTIME"])

    if do_dark:
        dark_path = f"SuperDark_{exptime}s.fits"
        super_dark = Frame.from_fits(_CALIB_PATH/Path(dark_path))

        print("Subtracting superdark")
        for i, frame in enumerate(reduced):
            reduced[i] = reduced[i] - super_dark
            reduced[i].name = "d"+reduced[i].name
        print("Subtracted")

    if not reduced:
        reduced = frames

    filters = []
    for frame in reduced:
        filters.append(frame.header["FILTER"])
    filters = list(set(filters))

    if do_flat:
        print("Loading superflats")
        superflats = {}
        for filt in filters:
            superflats[filt] = Frame.from_fits(_CALIB_PATH/Path(f'SuperNormFlat_{filt}.fits'))

        print("Subtracting superflats")
        for i, frame in enumerate(reduced):
            reduced[i] = (frame-superflats[frame.header["FILTER"]])
            reduced[i].name = "f"+frame.name
        print("Subtracted")

    if not reduced:
        reduced = frames
    reduced_dir = os.path.join(raw_data_dir,"intermediate") if "intermediate" not in output_dir else os.path.join(raw_data_dir,"intermediate_temp")
    intermediate_align_dir =  os.path.join(raw_data_dir,"temp_align_dir") if not save_intermediate else reduced_dir
    # if we have steps left to do (alignment or wcs) and the user has asked us to save intermediate files, we do that here
    if (save_intermediate and (do_wcs or do_align)):
        if not os.path.exists(reduced_dir):
            os.mkdir(reduced_dir)

        for filt in filters:
            if not os.path.exists(os.path.join(reduced_dir,filt)):
                os.mkdir(os.path.join(reduced_dir,filt))
        print("Saving reduced frames.")
        for frame in reduced:
            frame.write_fits(os.path.join(reduced_dir,frame.header["FILTER"],frame.name+".fits"),overwrite=True)
        print("Saved")
    elif do_align and not save_intermediate:
            print("Saving frames in preparation for alignment")
            if os.path.exists(intermediate_align_dir):
                shutil.rmtree(intermediate_align_dir)
            os.mkdir(intermediate_align_dir)
            for filt in filters:
                if not os.path.exists(os.path.join(intermediate_align_dir,filt)): # getting wierd error where this already exists, don't know why
                    os.mkdir(os.path.join(intermediate_align_dir,filt))
            for frame in reduced:
                frame.write_fits(os.path.join(intermediate_align_dir,frame.header["FILTER"],frame.name+".fits"))
            print("Saved")

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    if do_wcs:
        print("WCS solving is not yet implemented - skipping")

    if not do_align:
        print(f"Writing final output ({len(frames)} frames)")
        for frame in reduced:
            frame.write_fits(os.path.join(output_dir,frame.name+".fits"))
        print("Saved")
        exit()

    # if we haven't exited by this point, do alignment
    import alipy
    import glob

    print("Aligning frames")
    if not ref_image_path:
        ref_image_path = os.path.join(intermediate_align_dir,filters[0],[f for f in os.listdir(os.path.join(intermediate_align_dir,filters[0])) if f.endswith("fits")][0])
    stacks = []
    for filt in filters:
        images_to_align = sorted(glob.glob(os.path.join(intermediate_align_dir,filt,"*.fit*")))
        print()
        print("Aligning the following files:",images_to_align)
        identifications = alipy.ident.run(ref_image_path, images_to_align, visu=False,verbose=False)

        outputshape = alipy.align.shape(ref_image_path)

        aligned_out = os.path.join(output_dir,filt+"_aligned")
        if not os.path.exists(aligned_out):
            os.mkdir(aligned_out)

        for id in identifications:
            if id.ok == True:
                alipy.align.affineremap(id.ukn.filepath, id.trans, shape=outputshape, makepng=True,outdir=aligned_out,verbose=False)

        aligned_ls = findAllIn(data_dir = aligned_out, file_matching='fdb_*.fits')

        # running ccdproc.combine() + saving resulting image
        aligned_slices = read_ccddata_ls(aligned_ls, aligned_out)

        result_img = ccdproc.combine(aligned_slices,
                                method='average',
                                sigma_clip=True, sigma_clip_low_thresh=3, sigma_clip_high_thresh=3,
                                sigma_clip_func=np.ma.average)
        result_img.meta['combined'] = True
        result_img.write(output_dir/Path(f'combined_{target_name}_{filt}.fits'),overwrite=True)
        stacks.append(f'combined_{target_name}_{filt}.fits')

    # make one superstack
    stacked_images = read_ccddata_ls(stacks, output_dir)

    super_stack = ccdproc.combine(stacked_images,
                            method='average',
                            sigma_clip=True, sigma_clip_low_thresh=3, sigma_clip_high_thresh=3,
                            sigma_clip_func=np.ma.average)
    super_stack.meta['combined'] = True
    super_stack.meta['FILTER'] = "all"

    super_stack.write(output_dir/Path(f'{target_name}_superstack.fits'),overwrite=True)

    super_stack = Frame.from_fits(output_dir/Path(f'{target_name}_superstack.fits'))

    # clean up after ourselves: if the user asked for alignment but not intermediate file saving, delete the intermediate files
    if not save_intermediate:
        print("Cleaning up intermediate files")
        try:
            for filt in filters:
                os.remove(os.path.join(intermediate_align_dir,filt))
            os.remove(os.path.join(intermediate_align_dir,"intermediate"))
            print("Cleaned up") 
        except:
            print(f"Warning: unable to remove intermediate alignment files at {intermediate_align_dir}. They may have already been deleted or access may be restricted.")
    
    if visualize:
        show_img(super_stack.img,"Superstack")
    
    #  measure psf, judge frame quality and do rejection?
    # if wcs, solve the images (solve the aligned images if we aligned, otherwise solve the non-aligned ones, or just solve the stacked images)
    #   write to same directory probably
    #   add 's' to name

if __name__ == "__main__":
    main()