# Author:
# github repository:
import glob
# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
import pandas as pd
import zipfile

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)


# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def combine_provinces(in_dir, region, year, out_dir):

    print("Combine provinces of ", region)

    # ## list all zip files
    # files_grabbed = glob.glob(rf"{in_dir}/*.zip")
    #
    # if len(files_grabbed) < 1:
    #     return
    #
    # print("Unzipping file")
    # for path_to_zip_file in files_grabbed:
    #     with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
    #         zip_ref.extractall(in_dir)

    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)
    print(f"There are {len(iacs_files)} files for {region}")
    if len(iacs_files) < 1:
        return

    print("Reading data")
    file_list = [gpd.read_file(pth) for i, pth in enumerate(iacs_files)]

    print("Combining.")
    out_file = pd.concat(file_list)
    helper_functions.create_folder(out_dir)
    out_pth = f"{out_dir}\IACS_{region}_{year}.gpkg"
    print("Writing out to", out_pth)
    if "fid" in out_file.columns:
        out_file["fid_old"] = out_file["fid"]
    out_file["fid"] = range(1, len(out_file)+1)
    out_file["fid"] = out_file["fid"].astype(int)

    out_file.to_file(out_pth, driver="GPKG")


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    run_dict = {
        # "IT_temp/Toscana": {"file_encoding": "utf-8", "years": range(2016, 2017), "region_abbreviation": "TOS"},
        "IT_temp/Emilia-Romagna": {"file_encoding": "utf-8", "years": range(2021, 2025), "region_abbreviation": "EMR"},
    }

    for country_code in run_dict:

        years = run_dict[country_code]["years"]
        region_abbreviation = run_dict[country_code]["region_abbreviation"]

        for year in years:

            in_dir = rf"data\vector\IACS\{country_code}\{year}"
            out_dir = fr"data\vector\IACS\IT\{region_abbreviation}"

            combine_provinces(
                in_dir=in_dir,
                region=region_abbreviation,
                year=year,
                out_dir=out_dir)



    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
