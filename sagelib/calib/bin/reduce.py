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

from sagelib import Frame, get_user_config_path
from sagelib.utils import Config, findAllIn
from sagelib.image_utils import read_ccddata_ls, show_img
from sagelib.calib import CALIB_CONFIG
from sagelib.calib.utils import format_flat_name, format_dark_name
import sagelib.calib

from os.path import join, abspath

from photutils.utils import calc_total_error
from photutils.aperture import CircularAperture, CircularAnnulus, aperture_photometry
from photutils.detection import DAOStarFinder

import glob
import re

warnings.filterwarnings("ignore", category=wcs.FITSFixedWarning)
warnings.filterwarnings("ignore", category=utils.exceptions.AstropyDeprecationWarning)

def _find_files(rootdir, pattern, recursive=False, regex=False):
    paths = []
    if regex:
        match_all = "**/*" if recursive else "*"
        paths = [str(p) for p in Path.glob(Path(rootdir),match_all) if not p.is_dir()]
        paths = [p for p in paths if re.match(pattern,p)]
    else:
        pattern = "**/"+pattern if recursive else pattern
        paths = [str(p) for p in Path.glob(Path(rootdir),pattern) if not p.is_dir()]
    return paths

def find_files(rootdir, pattern, recursive=False, regex=False):
    try:
        return _find_files(rootdir,pattern,recursive,regex)
    except Exception as e:
        print(f"ERROR: {'recursive' if recursive else ''} path search with {'regex' if regex else ''} pattern '{pattern}' failed")
        print(repr(e))
        sys.exit(1)

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

    parser.add_argument("-o", "--overwrite", action="store_true", default=False, help="if output files with the same names in the same locations already exist, overwrite them. if this is not enabled, conflicting new/existing products will cause an error.")
    parser.add_argument("-i", "--intermediate", action="store_true", help="save intermediate files throughout process. will overwrite existing files that share the same name in the intermediate directory.")

    parser.add_argument("--ref_image_path", action="store", type=str,help="the path to the reference image to use for alignment. if not specified, will use the first image in the input directory")

    parser.add_argument("-v", "--visualize", action="store_true", default=True, help="show superstack when finished")
    
    parser.add_argument("-c", "--config", action="store", default=CALIB_CONFIG, help="optional configuration path. not necessary for most use-cases")
    
    parser.add_argument("-p", "--profile", action="store", default=None, help="profile in configuration file to use")


    args = parser.parse_args()

    do_slice = args.slice
    do_flat = args.flat
    do_dark = args.dark
    do_bias = args.bias
    do_align = args.align
    do_wcs = args.wcs

    overwrite = args.overwrite
    save_intermediate = args.intermediate
    ref_image_path = args.ref_image_path
    config_path = args.config  
    profile = args.profile
    
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

    calib_config = Config(config_path)  # config_path *can* be passed in by cmdline and defaults to CALIB_CONFIG if not provided
    if profile is not None:
        try:
            calib_config.choose_profile(profile)
        except Exception as e:
            print(f"ERROR: Couldn't select profile {profile} from the config file at {calib_config}: {e}")
            exit(1)

    CALIB_PATH = calib_config["calib_path"]

    FITS_DATE_FMT_IN = calib_config["date_format_in"]
    FITS_DATE_FMT_OUT = calib_config["date_format_out"]


    # find the data to reduce
    data_cfg = calib_config["data"]

    filenames = find_files(raw_data_dir,data_cfg["pattern"],data_cfg["recursive_search"],data_cfg["regex"])
    print(f"Found the following {len(filenames)} files to reduce: {', '.join(filenames)}")

    frames = []
    cubenames = []
    # slice any cubes - into what directory should these go? should we then move non-cube data to that folder too before proceeding?
    for f in filenames:
        frame = Frame.from_fits(f, date_format_in=FITS_DATE_FMT_IN, date_format_out=FITS_DATE_FMT_OUT)
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


    filenames = find_files(raw_data_dir,data_cfg["pattern"],data_cfg["recursive_search"],data_cfg["regex"])
    filenames = [f for f in filenames if f not in cubenames] # we don't delete the cubes after we slice them so we need to be careful not to re-open them
    filters = []
    for f in filenames:
        print(f"Opening {f}")
        frame = Frame.from_fits(f, date_format_in=FITS_DATE_FMT_IN, date_format_out=FITS_DATE_FMT_OUT)
        frames.append(frame)
        filters.append(frame.header["FILTER"])
    filters = list(set(filters))

    if not frames:
        print("No frames to process!")
        sys.exit(1)

    reduced = []
    if do_bias:
        bias_cfg = calib_config["biases"]
        biases = find_files(CALIB_PATH, bias_cfg["pattern"], bias_cfg["recursive_search"], bias_cfg["regex"])
        if not biases:
            print(f"ERROR: no biases found matching pattern '{bias_cfg['pattern']}'")
            sys.exit(1)
        if len(biases) > 1:
            print(f"ERROR: more than one bias found matching pattern '{bias_cfg['pattern']}'")
            sys.exit(1)
        bias_path = biases[0]
        print("Subtracting superbias")
        super_bias = Frame.from_fits(bias_path, date_format_in=FITS_DATE_FMT_IN, date_format_out=FITS_DATE_FMT_OUT)
        for i, frame in enumerate(frames):
            reduced.append(frames[i]-super_bias)
            reduced[i].name = "b_"+frames[i].name
        print("Bias subtracted")

    if not reduced:
        reduced = frames

    # TODO: calibrate each unique exposure time separately?
    # for now, all images must have the same exposure time!!!
    exptime = int(reduced[0].header["EXPTIME"])

    if do_dark:
        dark_cfg = calib_config["darks"]
        dark_pattern = format_dark_name(calib_config,exptime)
        darks = find_files(CALIB_PATH, dark_pattern, dark_cfg["recursive_search"], dark_cfg["regex"])
        if not darks:
            print(f"ERROR: no darks found matching pattern '{dark_pattern}'")
            sys.exit(1)
        if len(darks) > 1:
            print(f"ERROR: more than one dark found matching pattern '{dark_pattern}'")
            sys.exit(1)
        dark_path = darks[0]
  
        super_dark = Frame.from_fits(dark_path, date_format_in=FITS_DATE_FMT_IN, date_format_out=FITS_DATE_FMT_OUT)

        print("Subtracting superdark")
        for i, frame in enumerate(reduced):
            reduced[i] = reduced[i] - super_dark
            reduced[i].name = "d"+reduced[i].name
        print("Subtracted")

    if not reduced:
        reduced = frames

    if do_flat:
        print("Loading superflats")
        superflats = {}
        flat_cfg = calib_config["flats"]
        for filt in filters:
            flat_pattern = format_flat_name(calib_config,filt)
            flats = find_files(CALIB_PATH, flat_pattern, flat_cfg["recursive_search"], flat_cfg["regex"])
            if not flats:
                print(f"ERROR: no flats found matching pattern '{flat_pattern}'")
                sys.exit(1)
            if len(flats) > 1:
                print(f"ERROR: more than one flat found matching pattern '{flat_pattern}'")
                sys.exit(1)
            flat_path = flats[0]
            superflats[filt] = Frame.from_fits(flat_path, date_format_in=FITS_DATE_FMT_IN, date_format_out=FITS_DATE_FMT_OUT)

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
            frame.write_fits(os.path.join(reduced_dir,frame.header["FILTER"],frame.name+".fits"),overwrite=overwrite)
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
        sys.exit(0)

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
        result_img.write(output_dir/Path(f'combined_{target_name}_{filt}.fits'),overwrite=overwrite)
        stacks.append(f'combined_{target_name}_{filt}.fits')

    # make one superstack
    stacked_images = read_ccddata_ls(stacks, output_dir)

    super_stack = ccdproc.combine(stacked_images,
                            method='average',
                            sigma_clip=True, sigma_clip_low_thresh=3, sigma_clip_high_thresh=3,
                            sigma_clip_func=np.ma.average)
    super_stack.meta['combined'] = True
    super_stack.meta['FILTER'] = "all"

    super_stack.write(output_dir/Path(f'{target_name}_superstack.fits'),overwrite=overwrite)

    super_stack = Frame.from_fits(output_dir/Path(f'{target_name}_superstack.fits'), date_format_in=FITS_DATE_FMT_IN, date_format_out=FITS_DATE_FMT_OUT)

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