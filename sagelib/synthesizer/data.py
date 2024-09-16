from os.path import join, abspath
import numpy as np
from astropy.io import fits
# from scipy.ndimage import convolve
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from skimage.morphology import star, disk, diamond, octagon
import skimage.morphology as morph
from astropy.convolution import convolve, Gaussian2DKernel
try:
    from .utils import sigma, sample_gaussian
except:
    from utils import sigma, sample_gaussian

rng = np.random.default_rng()

transform = [
 [0,0,0,0,0,0,0,1,0,0,0,0,0,0,0],
 [0,0,0,0,0,0,1,1,1,0,0,0,0,0,0],
 [0,0,0,0,0,0,1,1,1,0,0,0,0,0,0],
 [0,0,0,0,0,1,1,1,1,1,0,0,0,0,0],
 [0,0,0,0,1,1,1,1,1,1,1,0,0,0,0],
 [0,0,0,1,1,1,1,1,1,1,1,1,0,0,0],
 [0,1,1,1,1,1,1,1,1,1,1,1,1,1,0],
 [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
 [0,1,1,1,1,1,1,1,1,1,1,1,1,1,0],
 [0,0,0,1,1,1,1,1,1,1,1,1,0,0,0],
 [0,0,0,0,1,1,1,1,1,1,1,0,0,0,0],
 [0,0,0,0,0,1,1,1,1,1,0,0,0,0,0],
 [0,0,0,0,0,0,1,1,1,0,0,0,0,0,0],
 [0,0,0,0,0,0,1,1,1,0,0,0,0,0,0],
 [0,0,0,0,0,0,0,1,0,0,0,0,0,0,0]]


def starfield(imgsize=200, nsources=25, desired_fwhm=3, saturation_thresh=2000, mean_bkg=5, noise_1sigma=1.6):
    # generate fake image
    min_amp = mean_bkg
    noise = rng.normal(mean_bkg,noise_1sigma,imgsize**2)
    noise.resize((imgsize,imgsize))

    source_locs = rng.random(nsources*2)*(imgsize-1)
    source_locs.resize(nsources,2)
    imgdata = noise
    amps = rng.exponential(4,nsources) + min_amp
    for amp, (x,y) in zip(amps,source_locs):
        sourcex = sample_gaussian(amp,desired_fwhm,x)(np.arange(imgsize))
        sourcey = sample_gaussian(amp,desired_fwhm,y)(np.arange(imgsize))
        img = np.outer(sourcex,sourcey)
        img[img>saturation_thresh] = saturation_thresh
        img += convolve(convolve(img, transform) * 0.0001 * amp, Gaussian2DKernel(sigma(desired_fwhm)))
        # img += convolve(convolve(img, transform) * 0.0001 * amp, gkern(5,sigma(desired_fwhm)))
        imgdata += img

    saturated_img = np.zeros_like(imgdata)
    saturated_img[np.where(imgdata>saturation_thresh)] = saturation_thresh

    imgdata[imgdata<=0] = 0.000001
    imgdata[imgdata > saturation_thresh] = saturation_thresh

    return imgdata

def main():
    imgdata = starfield(mean_bkg=5)
    plt.imshow(imgdata,cmap="gray",norm=LogNorm(vmin=5),origin="lower")
    plt.show()

if __name__ == "__main__":
    main()