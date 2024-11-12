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
def combine_subdistricts(in_dir, crop_names_pth, district, year, out_dir):

    print("Combine subdistricts of ", district)
    # layer cod_producto contains information on the meaning of the crops
    # linea_declaration contains the code for each parcel
    # thus cod_producto has to be assigned to the linea_declaration
    # are there information on farms? exp_num @Phillip Metadata

    unzip_list = glob.glob(fr"data\vector\IACS\ES_temp\{year}\{district}\*.zip")

    for i, path in enumerate(unzip_list):
        print(f"{i}/{len(unzip_list)} - UZ {path}")
        ## Get folder
        folder = rf"data\vector\IACS\ES_temp\{year}\{district}"

        ## Unzip
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(folder)

    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

    if len(iacs_files) < 1:
        return
    crops = pd.read_csv(crop_names_pth)

    file_list = []
    print(f"There are {len(iacs_files)} files for {district}")
    for i, pth in enumerate(iacs_files):
        print(f"Processing {i+1}/{len(iacs_files)}")
        file_year = os.path.basename(pth).split('_')[1]

        if int(file_year) != int(year):
            continue

        fields = gpd.read_file(pth, layer="linea_declaracion")
        file = pd.merge(fields, crops, how="left", left_on="parc_producto", right_on="codigo")
        file_list.append(file)

    print("Combining.")
    out_file = pd.concat(file_list)
    out_folder = f"{out_dir}\{district[:3]}"
    helper_functions.create_folder(out_folder)
    out_pth = f"{out_folder}\IACS_{district[:3]}_{year}.gpkg"
    print("Writing out to", out_pth)
    out_file.to_file(out_pth, driver="GPKG")


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    for year in [2023]: # [2022]
        # districts = [x[0] for x in os.walk(fr"data\vector\IACS\ES_temp\{year}")]
        districts = glob.glob(f'data/vector/IACS/ES_temp/{year}/*')
        # districts = [districts[16]]
        districts = [x for x in districts if "CIU" in x]

        for district_dir in districts:
            district = os.path.basename(district_dir)
            combine_subdistricts(
                in_dir=district_dir,
                crop_names_pth=r"data\vector\IACS\ES\crop_names.csv",
                district=district,
                year=year,
                out_dir=r"data\vector\IACS\ES")

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
