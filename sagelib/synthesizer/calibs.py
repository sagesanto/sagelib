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

def flat(side_len,max_val):
    sigma = (side_len-1)/8 * 3
    kern_arr = Gaussian2DKernel(sigma,x_size=side_len,y_size=side_len).array
    flat = np.ones_like(kern_arr) * np.max(kern_arr)
    flat -= 2.355*kern_arr
    flat[flat < 0] = 0
    flat *= max_val/flat.max()
    return flat


if __name__ == "__main__":
    f = flat(200,1)
    plt.imshow(f)
    plt.show()