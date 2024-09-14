# sagelib
### Astro Utils
* `Frame`: Loads and represents fits files (cubes or individual frames). Supports image arithmetic, statistics, and display. `Frames` representing cubes of data can be sliced to yield their constituent images as `Frame` objects.
* `FrameSet`: Provides a context that allows easy iteration over large directories of data that respects memory usage limits.
* `ds9`: Provides utilities for creating ds9 region files
* `pipeline`: optional extra that provides database-governed persistent pipeline infrastructure
* `calib`: optional extra that provides image-manipulation scripts
    * `align`: script to align directories of data
    * `imanalysis`: tools for image analysis. currently configured to measure source fwhm.
    * `reduce`: script that takes an input directory of raw data and a calibration directory then can perform slicing, flat-dark-bias subtraction, and alignment
        *  usage: `reduce.py [-h][-s][-f][-d][-b][-a][-w][-i] [--ref_image_path REF_IMAGE_PATH] target_name sci_data_dir output_dir`
