# Author:
# github repository:
import glob
# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
import pandas as pd
import shutil

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)


# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def combine_provinces(in_dir, year, out_dir):

    print("Combine file of ", year)

    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)
    iacs_files = [file for file in iacs_files if "PARCEL" in file]
    print(f"There are {len(iacs_files)} files for {year}")
    if len(iacs_files) < 1:
        return
    elif len(iacs_files) < 2:
        src = iacs_files[0]
        ext = os.path.basename(src).split(".")[1]
        dst = f"{out_dir}\IACS_IE_{year}.{ext}"
        helper_functions.create_folder(out_dir)
        shutil.copyfile(src, dst)
        return

    print("Reading data")
    file_list = [gpd.read_file(pth) for i, pth in enumerate(iacs_files)]

    print("Combining.")
    out_file = pd.concat(file_list)
    helper_functions.create_folder(out_dir)
    out_pth = f"{out_dir}\IACS_IE_{year}.gpkg"
    print("Writing out to", out_pth)

    out_file.to_file(out_pth, driver="GPKG")

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    for year in range(2017, 2024):

        in_dir = rf"data\vector\IACS\IE_temp\{year}"
        out_dir = fr"data\vector\IACS\IE"

        combine_provinces(
            in_dir=in_dir,
            year=year,
            out_dir=out_dir)



    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
