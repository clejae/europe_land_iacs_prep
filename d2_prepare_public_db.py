# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# This script is only needed internally to separate the data the Europe-LAND project can share from the data we are
# not allowed to share.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import sys
# os.environ['GDAL_DATA'] = os.path.join(f'{os.sep}'.join(sys.executable.split(os.sep)[:-1]), 'Library', 'share', 'gdal')
import time
import pandas as pd
import geopandas as gpd
import warnings
import glob
import shutil

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    only_crop_cols = ["field_id", "crop_code", "crop_name", "EC_trans_n", "EC_hcat_n", "EC_hcat_c", "field_size"]
    crop_org_cols = ["field_id", "crop_code", "crop_name", "EC_trans_n", "EC_hcat_n", "EC_hcat_c", "organic", "field_size"]
    crop_farm_cols = ["field_id", "farm_id", "crop_code", "crop_name", "EC_trans_n", "EC_hcat_n", "EC_hcat_c", "field_size"]

    run_dict = {
        # "AT": {str(year): only_crop_cols for year in range(2015, 2022)} |
        #       {str(year): crop_org_cols for year in range(2022, 2025)},
        "BG": {"everything": True},
        ## "BE/FLA": {"everything": True}, # no correction needed 10.12.24
        # "CZ": {"2023": crop_farm_cols},
        # "DE/BRB": {str(year): only_crop_cols for year in range(2010, 2025)},
        # "DE/LSA": {str(year): only_crop_cols for year in range(2023, 2025)},
        ## "DE/NRW": {str(year): only_crop_cols for year in range(2019, 2025)}, # no correction needed 10.12.24
        # "DK": {"everything": True},
        # "EE": {"everything": True},
        # "FI": {"everything": True},
        # "FR/FR": {"everything": True},
        # "IE": {"everything": True},
        ## "HR": {"everything": True}, # no correction needed 10.12.24
        # "LT": {"everything": True},
        # "LV": {str(year): only_crop_cols for year in range(2023, 2025)},
        # "NL": {str(year): only_crop_cols for year in range(2009, 2025)},
        # "PT/PT": {"everything": True},
        # "PT/ALE": {"everything": True},
        # "PT/ALG": {"everything": True},
        # "PT/AML": {"everything": True},
        # "PT/CET": {"everything": True},
        # "PT/CEN": {"everything": True},
        # "PT/CES": {"everything": True},
        # "PT/NOR": {"everything": True},
        # "PT/NON": {"everything": True},
        # "PT/NOS": {"everything": True},
        # "SE": {str(year): only_crop_cols for year in range(2015, 2024)}, # no correction needed 10.12.24
        # "SI": {str(year): only_crop_cols for year in range(2018, 2024)},
        # "SK": {str(year): only_crop_cols for year in range(2018, 2025)}
    }

    ## For france create a dictionary in a loop, because of the many subregions
    # FR_districts = pd.read_csv(r"data\vector\IACS\FR\region_code.txt")
    # FR_districts = list(FR_districts["code"])
    # for district in FR_districts:
    #     run_dict[f"FR/{district}"] = {"everything": True}

    ## For spain create a dictionary in a loop, because of the many subregions
    ## This code snippet needs to be corrected. I did the copying manually!
    ## ES_districts = pd.read_csv(r"data\vector\IACS\ES\region_code.txt")
    ## ES_districts = list(ES_districts["code"])
    ## for district in ES_districts:
    ##     run_dict[f"ES/{district}"] = {"everything": True},

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        ## Derive input variables for processing
        region_id = country_code.replace(r"/", "_")
        in_dir = os.path.join("data", "vector", "IACS_EU_Land", country_code)
        print(region_id)

        ## Check if everything can be shared. If so, then copy the files as they are.
        if "everything" in run_dict[country_code]:
            print("Everything can be shared.")
            file_list = glob.glob(os.path.join(in_dir, "*"))
            for from_pth in file_list:
                print("Copying", from_pth)
                file_name = os.path.basename(from_pth)
                out_folder = os.path.join("data", "vector", "IACS_public_database", country_code)
                helper_functions.create_folder(out_folder)
                to_pth = os.path.join("data", "vector", "IACS_public_database", country_code, file_name)
                shutil.copy(from_pth, to_pth)
            continue

        ## If not everything can be shared, there are only years as keys left in the dictionary
        ## Loop over them
        for year in run_dict[country_code]:
            print(f"Subsetting {year}")
            ## Retrieve columns that can be shared.
            cols = run_dict[country_code][year] + ["geometry"]

            ## Open file and copy with relevant columns
            in_pth = os.path.join("data", "vector", "IACS_EU_Land", country_code, f"GSA-{region_id}-{year}.geoparquet")
            gdf = gpd.read_parquet(in_pth)
            gdf_out = gdf[cols].copy()

            ## Copy to public database folder
            file_name = os.path.basename(in_pth)
            out_folder = os.path.join("data", "vector", "IACS_public_database", country_code)
            out_pth = os.path.join(out_folder, file_name)
            helper_functions.create_folder(out_folder)
            print(f"Writing to {out_pth}")
            gdf_out.to_parquet(out_pth)

            ## Check if there is also a supplementary table and copy that as well
            csv_pth = os.path.join("data", "vector", "IACS_EU_Land", country_code, f"GSA-{region_id}-{year}.csv")
            if os.path.exists(csv_pth):
                print("Supplementary table found. Copying.")
                ## So far there are not countries with supplementary tables for which we cannot share everything
                ## Therefore, we can simply copy them.
                file_name = os.path.basename(csv_pth)
                to_pth = os.path.join("data", "vector", "IACS_public_database", country_code, file_name)
                shutil.copy(csv_pth, to_pth)

                ## Once there are countries with supplementary tables, we need to make sure, that these tables also
                ## do not include the "unsharable" information. This could be a solution:
                # cols = run_dict[country_code][year]
                # df = pd.read_csv(file_name)
                # df = df[cols].copy()
                # df.to_csv(to_pth, index=False)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()
