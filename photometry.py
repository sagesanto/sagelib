from Frame import Frame
from imanalysis import fwhm
from photutils.utils import calc_total_error
from photutils.aperture import CircularAperture, CircularAnnulus, aperture_photometry, ApertureStats
from photutils.detection import DAOStarFinder
from astropy.io import fits
import numpy as np
import sys, os, glob, time
import pandas as pd
from datetime import datetime
from pathlib import Path
from astropy.visualization import ZScaleInterval
from astropy.visualization.mpl_normalize import ImageNormalize
from astropy.io import fits
from astropy import stats
from astropy.table import QTable, vstack
from astropy.nddata import CCDData
from multiprocessing import Pool


def add_err_img(filename,bkg_std_dev,effective_gain):
    with fits.open(filename, 'update') as hdu:
        if len(hdu) == 1:
            # print("Datatype:",hdu[0].data.dtype)
            err_img = calc_total_error(hdu[0].data.astype(np.float64), bkg_std_dev, effective_gain)
            err_hdu = fits.ImageHDU(err_img.astype(np.float32))
            err_hdu.header['EXTNAME'] = ('UNCERT', 'extension name')
            hdu.append(err_hdu)
            print("Appended error image to",filename)
        else:
            print("Error image already exists in",filename," - not adding a new one")

def _photometry(img_path, bkg_std_dev, effective_gain, all_apertures, all_annulus, phot_zp):
    try:
        add_err_img(img_path, bkg_std_dev, effective_gain)
        with fits.open(img_path) as im:
            batch_phot_table = aperture_photometry(im[0].data.astype(np.float64), all_apertures, im[1].data.astype(np.float64))
            
            # This loops through all of the columns in the aperture_phot_table to reformat the output :
            for col in batch_phot_table.colnames:
                batch_phot_table[col].info.format = '%.8g'  # for consistent table output

            # add timestamp + filter info
            batch_phot_table['timestamp'] = im[0].header['DATE-OBS']
            batch_phot_table['filter'] = im[0].header['FILTER']
            annulus_stats = [ApertureStats(im[0].data, ann_ap) for ann_ap in all_annulus]
            bkg_median = [stat.median for stat in annulus_stats]

            #aperture_areas = []
            for ap_num in range(len(all_apertures)):
                # Name the new columns
                aperture_sum_title = 'aperture_sum_' + str(ap_num)  # This is not written out, it is read it.  Others below are written out.
                aperture_sum_err_title = 'aperture_sum_err_' + str(ap_num) 
                skyflux_title = 'skyflux_' + str(ap_num)
                objflux_title = 'objflux_' + str(ap_num)
                mag_title = 'mag_' + str(ap_num)
                magerr_title = 'mag_err_' + str(ap_num)

                # annulus background calculations
                # the following line used to have "img" (the ref img) instead of "im" (the current img), changed on 1/27/24
                # changed from im[0] back to img
                aperture_area = [ap.area_overlap(im[0].data.astype(np.float64)) for ap in all_apertures[ap_num]]

                # compute background total_bkg & phot_bkgsub
                total_bkg = [bkg_median[0][i] * aperture_area[i] for i in range(len(all_apertures[ap_num]))]        
                phot_bkgsub = batch_phot_table[aperture_sum_title] - total_bkg
                batch_phot_table[skyflux_title] = total_bkg
                batch_phot_table[objflux_title] = phot_bkgsub
                
                # compute instrumental (uncalibrated) magnitude from aperture sum
                mag = -2.5 * np.log10(phot_bkgsub) + phot_zp
                batch_phot_table[mag_title] = mag
                batch_phot_table[magerr_title] = 1.0875*(batch_phot_table[aperture_sum_err_title]/batch_phot_table[objflux_title])  
        print(f"Completed photometry on {img_path}.")
        return {img_path: batch_phot_table.to_pandas()}
    except Exception as e:
        print(f"Failed to do photometry on {img_path}. Error: {e}")
        return {img_path: None}



def photometry(ref_img_path, img_paths, ap_radius, ann_radius_inner, ann_radius_outer, radii, output_csv_dir, output_csv_name, bkg_std_dev, stellar_fwhm=None, phot_zp=25, keep_brightest=10, effective_gain=0.8,detection_sigma=3):
    """
    Perform aperture photometry on a series of *aligned* images, using a reference image to find the positions of stars.
    :param ref_img_path: path to reference image
    :param img_paths: list of paths to images to be photometered
    :param ap_radius: radius of aperture, in units of hwhm
    :param ann_radius_inner: radius of inner annulus, in units of hwhm
    :param ann_radius_outer: radius of outer annulus, in units of hwhm
    :param radii: list of radii to use for aperture photometry, in units of hwhm
    :param output_csv_dir: directory to save output csv
    :param output_csv_name: name of output csv
    :param bkg_std_dev: standard deviation of background noise
    :param stellar_fwhm: fwhm of stars in image, in units of pixels
    :param phot_zp: photometric zeropoint
    :param keep_brightest: number of brightest stars to do photometry on, will ignore the rest
    :param effective_gain: effective gain of camera
    :param detection_sigma: number of sigma above background to use as detection threshold for source extraction
    """
    assert os.path.exists(ref_img_path), f"Reference image {ref_img_path} does not exist."
    if not os.path.exists(output_csv_dir):
        os.mkdir(output_csv_dir)

    ### SETUP ###
    # do the following setup once at the beginning: 
        # load reference frame
        # add error image to reference frame, if it does not already have one
        # do stats on reference frame
        # find stars in reference frame
        # get positions of sources in reference frame, make list
        # make annuli and apertures
    start = time.perf_counter()
    # add error img to reference img if it does not already have one:
    add_err_img(ref_img_path, bkg_std_dev, effective_gain)
    ref_frame = Frame.from_fits(ref_img_path)

    if stellar_fwhm is None:
        print("No stellar fwhm provided. Calculating stellar fwhm...")
        stellar_fwhm = fwhm(ref_frame)["avg_fwhm"].value
        print(f"Average stellar fwhm calculated as {stellar_fwhm} pixels.")

    # convert radii to pixels
    hwhm = stellar_fwhm / 2
    ap_radius *= hwhm
    ann_radius_inner *= hwhm
    ann_radius_outer *= hwhm
    radii = [r * hwhm for r in radii]

    # load reference image
    hdu = fits.open(Path(ref_img_path))
    ref_detect = hdu[0].data.astype(np.float64)
    ref_err = hdu[1].data.astype(np.float64)
    ref_detect_header = hdu[0].header
    hdu.close()

    # get stats for setting detection threshold
    stars_mean, stars_med, stars_sd = stats.sigma_clipped_stats(ref_detect, sigma=3.0, maxiters=3, std_ddof=1)
    print("mean, median, standard deviation: %5.3f / %5.3f / %5.3f" % (stars_mean, stars_med, stars_sd))

    # extract sources based on stats
    daofind = DAOStarFinder(fwhm=stellar_fwhm, threshold=detection_sigma*stars_sd) # you can change the fwhm based on an estimate from imexam()
    sp_all = daofind(ref_detect - stars_med)
    sp_all.sort('flux', reverse=True)

    # downselect to just the keep_brightest brightest sources
    sources = sp_all[:keep_brightest]
    x_colname = 'xcentroid'
    y_colname = 'ycentroid'

    # aperture and annulus sizes are defined according to the input values
    print("annulus (inner/outer edge) radius (pixels): %5.3f / %5.3f" % (ann_radius_inner, ann_radius_outer))

    area_ap=3.14*ap_radius**2
    area_ann=3.14*ann_radius_outer**2 - 3.14*ann_radius_inner**2
    print("Aperture vs. annulus area (pixels): %5.3f / %5.3f " % (area_ap, area_ann))

    # get positions of sources in ref img (in descending order of flux)
    positions = []
    for i in range(len(sources)):
        positions.append((sources[x_colname][i], sources[y_colname][i]))

    # make annuli and apertures
    all_apertures = [CircularAperture(positions, r=r) for r in radii]
    all_annulus = [CircularAnnulus(positions, r_in=ann_radius_inner, r_out=ann_radius_outer)]

    ### PHOTOMETRY ###
    # multiprocess the following for each image:
        # do aperture photometry
        # modify data
        # find sums of fluxes in apertures
        # compute sky background
        # compute instrumental mag
        # compute mag error
    
    # do aperture photometry on each image
    print("Doing aperture photometry...")
    with Pool() as pool:
        df_dict = pool.starmap(_photometry, [(img_path, bkg_std_dev, effective_gain, all_apertures, all_annulus, phot_zp) for img_path in img_paths])
    # combine results into a csv
    failed = [k for d in df_dict for k, v in d.items() if v is None]
    print(f"Failed to do photometry on {len(failed)} images: {failed}")
    df_all = pd.concat([v for d in df_dict for _, v in d.items() if v is not None])
    try:
        df_all["timestamp"] = pd.to_datetime(df_all["timestamp"],infer_datetime_format=True)
    except:
        pass
    df_all.sort_values(by=['timestamp'],inplace=True)
    df_all.to_csv(os.path.join(output_csv_dir,output_csv_name), index=False)

    print(f"Done with photometry on {len(img_paths)} in {time.perf_counter()-start} seconds.")
    return df_all


if __name__ == "__main__":
    # testing, using set values for now
    img_dir = "/Volumes/TMO_Data_18/Sage/sagelib/test/photometry_test"
    paths = sorted(glob.glob(os.path.join(img_dir,"*.fits")))
    ref_img_path = paths[0]
    ap_radius = 2
    ann_radius_inner = 4
    ann_radius_outer = 6
    radii = [2,3]
    output_csv_dir = "/Volumes/TMO_Data_18/Sage/sagelib/test"
    output_csv_name = "photometry_test.csv"
    bkg_std_dev = 10
    phot_zp = 25
    keep_brightest = 10
    effective_gain = 0.8
    detection_sigma = 3
    photometry(ref_img_path, paths, ap_radius, ann_radius_inner, ann_radius_outer, radii, output_csv_dir, output_csv_name, bkg_std_dev, None, phot_zp, keep_brightest, effective_gain,detection_sigma)
