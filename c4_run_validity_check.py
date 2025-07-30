# Author:
# github repository:

# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

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

    run_dict = {
        # "AT": {}, #[2015, 2019, 2020, 2021, 2022]
        # "BE/FLA": {},
        # "BE/WAL": {},
        # "CY": {},
        # "CZ": {},
        # "DE/BRB": {},
        # "DE/LSA": {},
        # "DE/NRW": {},
        # "DE/SAA": {},
        # "DE/SAT": {},
        # "DK": {},
        # "EE": {},
        # "EL": {},
        # "FI": {},
        # "FR/FR": {},
        # "IE": {},
        # "HR": {},
        # "HU": {},
        # "IE": {},
        # "IT/EMR": {},
        # "IT/MAR": {},
        # "IT/TOS": {},
        # "LT": {},
        # "LV": {},
        # "NL": {},
        # "PT/PT": {},
        # "RO": {},
        # "SE": {},
        # "SI": {},
        # "SK": {}
    }

    ## For france create a dictionary in a loop, because of the many subregions
    # FR_districts = pd.read_csv(r"data\vector\IACS\FR\region_code.txt")
    # FR_districts = list(FR_districts["code"])
    # for district in FR_districts:
    #     run_dict[f"FR/{district}"] = {"everything": True}
    #
    ## For spain create a dictionary in a loop, because of the many subregions
    ES_districts = pd.read_csv(r"data\vector\IACS\ES\region_code.txt")
    ES_districts = list(ES_districts["code"])
    for district in ES_districts:
        run_dict[f"ES/{district}"] = {"everything": True},

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        ## Derive input variables for processing
        region_id = country_code.replace(r"/", "_")
        in_dir = fr"data\vector\IACS_EU_Land\{country_code}"
        print(region_id)

        ## Get years that should be skipped
        if "skip_years" in run_dict[country_code]:
            skip_years = run_dict[country_code]["skip_years"]
        else:
            skip_years = []

        file_list = glob.glob(rf"{in_dir}\*")

        df_lst = []

        for file_pth in file_list:
            year = helper_functions.get_year_from_path(file_pth)
            print(year)
            if int(year) in skip_years:
                print(f"Skipping year {year}")
                continue

            if "animals" in file_pth:
                continue

            if "misses.csv" in file_pth:
                continue

            root, ext = os.path.splitext(file_pth)

            if ext == ".csv":
                df = pd.read_csv(file_pth)
            elif ext == ".geoparquet":
                df = gpd.read_parquet(file_pth)

            df_na = df.loc[df["EC_hcat_n"].isna()].copy()
            print(f"Number of rows with NA in EC_hcat_n: {len(df_na)}")
            if len(df_na) > 0:
                cols = ["crop_code", "crop_name", "EC_trans_n", "EC_hcat_n", "EC_hcat_c"]
                df_na.drop_duplicates(subset=cols, inplace=True)

                df_lst.append(df_na)
            else:
                print("No missed crops")

            ## Check for duplicate crop code - crop name combinations in original data
            ## we had a mistake in script c3 that caused that in some cases.
            df_dupl = df.drop_duplicates(subset=["crop_code", "crop_name"]).copy()

            ## Group by crop_code and count unique crop_name entries
            duplicates = df_dupl.groupby('crop_code')['crop_name'].nunique().reset_index()

            ## Filter for crop_codes with more than one unique crop_name
            duplicate_codes = duplicates[duplicates['crop_name'] > 1]['crop_code'].copy()

            ## Filter original DataFrame to include only those duplicate crop_codes
            duplicate_entries_df = df_dupl[df_dupl['crop_code'].isin(duplicate_codes)].copy()
            duplicate_entries_df.sort_values(by="crop_code", inplace=True)
            out_pth = fr"data\vector\IACS_EU_Land\{country_code}\duplicate_code-name_combs_{region_id}_{year}.csv"
            if not duplicate_entries_df.empty:
                print("Duplicate original crops detected.")
                duplicate_entries_df.to_csv(out_pth, index=False)

        if len(df_lst) > 0:
            df_out = pd.concat(df_lst)
            df_out.drop_duplicates(subset=["crop_code", "crop_name"], inplace=True)
            out_pth = fr"data\vector\IACS_EU_Land\missed_crops_{region_id}.csv"
            df_out[["crop_code",  "crop_name"]].to_csv(out_pth, index=False)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
