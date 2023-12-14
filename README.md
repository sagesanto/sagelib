# sagelib
### Astronomy image utilities
* `Frame`: Loads and represents fits files (cubes or individual frames). Supports image arithmetic, statistics, and display. `Frames` representing cubes of data can be sliced to yield their constituent images as `Frame` objects.
* `FrameSet`: Provides a context that allows easy iteration over large directories of data that respects memory usage limits.
* `align.py`: script to align directories of data
* `image_utils.py`: utilities for data loading, slicing, and display
* `imanalysis.py`: tools for image analysis. currently configured to measure source fwhm.
*  `observing_utils.py`: tools for calculating observability
*  'reduce.py': script that takes an input directory of raw data and a calibration directory then can perform slicing, flat-dark-bias subtraction, and alignment
    *  usage: `reduce.py [-h][-s][-f][-d][-b][-a][-w][-i] [--ref_image_path REF_IMAGE_PATH] target_name sci_data_dir output_dir`
