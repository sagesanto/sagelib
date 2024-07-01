import astropy
import astromatic_wrapper as aw
from argparse import ArgumentParser
import numpy as np
try:
    from pipeline_utils import check_sextractor_flags, ldac_to_table
except:
    from .pipeline_utils import check_sextractor_flags, ldac_to_table

# REGION RADIUS SHOULD BE A STRING WITH " in it (ARCSEC) IF PHYSICAL IS FALSE
def df_to_ds9(df,outname,xname,yname,physical=False,region_radius=5,color="green"):
    with open(outname,"w") as f:
        f.write(f'global color={color} dashlist=8 3 width=1 font="helvetica 10 normal roman" select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n')
        f.write('physical\n' if physical else 'WCS\n')
        for i, row in df.iterrows():
            f.write(f"circle({row[xname]},{row[yname]},{region_radius}) # width=2\n")
    print(f"Wrote {len(df.index)} regions to {outname}.")

def table_to_ds9(table, outname, xname, yname, physical=False, region_radius=5,color="green"):
    df = table.to_pandas()
    df_to_ds9(df, outname, xname, yname, physical, region_radius,color=color)

def ldac_to_ds9(fits_file, outname, physical=False,bad_sextractor_flags=None,region_radius=5):
    table = ldac_to_table(fits_file)
    df = table.to_pandas()
    if bad_sextractor_flags is not None:
        df = df[df["FLAGS"].apply(lambda f: check_sextractor_flags(f,bad_flags=bad_sextractor_flags))]
    df_to_ds9(df,outname,xname="ALPHA_J2000" if physical else "X_IMAGE", yname="DELTA_J2000" if physical else "Y_IMAGE",physical=physical,region_radius=region_radius)

def filtered_ldac_to_ds9(fits_file, outname, physical=False,region_radius=5):
    table = ldac_to_table(fits_file)
    df = table.to_pandas()
    df = df[df["FLUX_RADIUS"] > 0]
    df = df[df["FLAGS"].apply(lambda f: check_sextractor_flags(f))]
    df_to_ds9(df,outname,xname="ALPHA_J2000" if physical else "X_IMAGE", yname="DELTA_J2000" if physical else "Y_IMAGE",physical=physical,region_radius=region_radius)


if __name__ == "__main__":
    parser = ArgumentParser(description="Create a ds9 region file from a FITS-LDAC (SExtractor) file")

    parser.add_argument("fits_file", help="FITS-LDAC path")
    parser.add_argument("outname", help="Path to output region file to")

    args = parser.parse_args()

    fits_file, outname = args.fits_file, args.outname
    ldac_to_ds9(fits_file, outname)

    
    