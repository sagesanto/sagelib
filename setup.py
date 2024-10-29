
import os
import site
from setuptools import setup, find_packages

''' Run this file using

sudo python setup.py develop

To install the python telescope control package. Using the 'develop'
option (rather than 'install') allows you to make changes to the code
without having to rebuild the package
'''

# do setup
setup(
    name="sagelib",
    version="0.0.2",
    description='Astro Utilities',
    author='Sage Santomenna',
    author_email='sage.santomenna@gmail.com',
    packages=find_packages(include=['sagelib', 'sagelib.*']),
    package_data={
        'sagelib': ['config/logging.json']
    },
    install_requires=['astropy','numpy','sqlalchemy','matplotlib','networkx','pandas','pytz','scipy','colorlog','tomlkit', 'astral'],
    entry_points={
        'console_scripts': [
            'csv_to_latex = sagelib.bin.csv_to_latex:main',
            'align = sagelib.calib.bin.align:main',
            'reduce = sagelib.calib.bin.reduce:main',
            'scale_ref_img = sagelib.calib.bin.scale_ref_img:main',
            'run_info = sagelib.pipeline.bin.run_info:main',
            'product_info = sagelib.pipeline.bin.product_info:main',
            'create_db = sagelib.pipeline.bin.create_db:main'
        ]
    },
    extras_require = {
        "calib": ['alipy','ccdproc'],
        "pipeline": ['sqlalchemy','networkx','tqdm']
    }
)
