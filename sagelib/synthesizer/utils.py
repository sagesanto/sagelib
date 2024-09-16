import numpy as np

def sample_gaussian(amp, fwhm, mean):
    return lambda x: amp * np.exp(-4. * np.log(2) * (x-mean)**2 / fwhm**2)

def sigma(fwhm):
    return fwhm/2.355

def FWHM(sig):
    return sig * 2.355