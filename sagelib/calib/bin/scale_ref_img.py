#! python

from sagelib import Frame, FrameSet
import argparse
import numpy as np
from sagelib.utils import findAllIn
import os

def main():
    parser = argparse.ArgumentParser(description="Scale a reference image so its average value matches the average average value of a series of frames. Useful for alignment with reference images not taken in the same run as the series.")

    parser.add_argument("ref_image_input_path", type=str, help="Path to the reference image fits file")
    parser.add_argument("data_dir", type=str, help="Path to the data directory")
    parser.add_argument("ref_image_output_path", type=str, help="Path to the reference image output file")
    parser.add_argument("--file_pattern", type=str, default="*fits", help="File pattern to match in the data directory. default is '*fits'")
    parser.add_argument("--max_mem_usage_mb", type=int, default=40000, help="Maximum memory usage in MB (default: 40000)")

    parser.add_argument("--show", action="store_true", default=False, help="Show the resulting reference image. Defaults to False")
    args = parser.parse_args()

    ref_image_input_path = args.ref_image_input_path
    data_dir = args.data_dir
    max_mem_usage_mb = args.max_mem_usage_mb
    ref_image_output_path = args.ref_image_output_path

    filenames = [os.path.join(data_dir,filename) for filename in findAllIn(data_dir,file_matching=args.file_pattern)]

    avgs = np.array([])
    with FrameSet(filenames, max_mem_usage_mb) as frames:
        for frame in frames:
            avgs = np.append(avgs, frame.mean)
    series_mean = avgs.mean()

    ref_img = Frame.from_fits(ref_image_input_path,kwargs={"name":"Scaled Reference Image"})
    ref_img = (ref_img*series_mean/ref_img.mean)
    ref_img.write_fits(ref_image_output_path, overwrite=True)
    if args.show:
        ref_img.show()

if __name__ == "__main__":
    main()