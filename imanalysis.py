from Frame import Frame
import numpy as np
from datetime import datetime
from photutils.segmentation import deblend_sources
from astropy.convolution import Gaussian2DKernel, convolve
from astropy.stats import gaussian_fwhm_to_sigma
from photutils.segmentation import detect_sources
import pandas as pd
from photutils.segmentation import SourceCatalog
from astropy.stats import sigma_clipped_stats
from datetime import datetime
import os, argparse
from tqdm import tqdm


def Image_Segmetation(data, threshold, npixels):
	sigma = 3.0 * gaussian_fwhm_to_sigma  # FWHM = 3.
	kernel = Gaussian2DKernel(sigma, x_size=3, y_size=3)
	convolved_data = convolve(data, kernel, normalize_kernel=True)
	
	segm = detect_sources(convolved_data, threshold, npixels=npixels)

	return convolved_data, segm


def Deblending(convolved_data, segm, npixels, nlevels, contrast):
	segm_deblend = deblend_sources(convolved_data, segm, npixels, nlevels=nlevels, contrast=contrast)
	return segm_deblend

# given a list of sources and a frame, return the FWHM of each of the sources
def fwhm(Frame):
    fwhms = []
    timestamp = datetime.strptime(Frame.header["DATE-OBS"], '%Y-%m-%dT%H:%M:%S.0000').timestamp()

    data = Frame.img
    mean, median, std = sigma_clipped_stats(data, sigma=3.0)
    threshold = 5 * std
    data -= median

    npixels = 16   # number of connected pixels needed, each above threshold, for an area to qualify as a source
    convolved_data, segm = Image_Segmetation(data, threshold, npixels)
    if convolved_data is None or segm is None:
        return None
    segm_deblend = Deblending(convolved_data, segm, npixels, nlevels=8, contrast=1)

    
    cat = SourceCatalog(data, segm_deblend, convolved_data=convolved_data)
    columns = ['label','xcentroid','ycentroid','fwhm','gini','eccentricity','orientation','kron_flux']
    table = cat.to_table(columns=columns)

    for i in range(len(table)):
        if table['kron_flux'][i] > 1:
            continue
        else:
            table['kron_flux'][i] = 0

    table.sort(['kron_flux'], reverse = True)
    table = table.groups.aggregate(np.mean)
    return {"julian":timestamp,"avg_fwhm":table["fwhm"][0]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate the FWHM of a series of frames")
    parser.add_argument("data_dir", type=str, help="Path to the data directory")
    parser.add_argument("output_path", type=str, help="Path to the output csv file")
    args = parser.parse_args()

    data_dir = args.data_dir
    output_path = args.output_path
    dicts = []
    files = os.listdir(data_dir)
    for file in tqdm(files):
        if file.endswith(".fits"):
            try:
                data = Frame.from_fits(os.path.join(data_dir,file))
            except:
                print(f"Failed to load frame {file}")
                continue
            row = fwhm(data)
            if row is not None:
                dicts.append(row)
            else:
                print(f"No dict back from frame {file}")

    df = pd.DataFrame(dicts)
    print(df)
    df.to_csv(os.path.join(output_path,"fwhms.csv"))