# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# This script is only needed internally to separate the data the Europe-LAND project can share from the data we are
# not allowed to share.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
from os.path import dirname, abspath
import time
import glob
import pandas as pd
import os

from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # run_dict = {
    #     "AT": {"switch": "off"},
    #     "BG": {"switch": "on"},
    #     "BE/FLA": {"switch": "off"},
    #     "BE/WAL": {"switch": "off"},
    #     "CZ": {"switch": "off"},
    #     "DE/BRB": {"switch": "off"},
    #     "DE/BWB": {"switch": "off"},
    #     "DE/LSA": {"switch": "off"},
    #     "DE/NRW": {"switch": "off"},
    #     "DK": {"switch": "off"},
    #     "EE": {"switch": "off"},
    #     "FI": {"switch": "off"},
    #     "FR/FR": {"switch": "off"},
    #     "IE": {"switch": "off"},
    #     "IT/TOS": {"switch": "off"},
    #     "HR": {"switch": "off"},
    #     "LT": {"switch": "off"},
    #     "LV": {"switch": "off"},
    #     "NL": {"switch": "off"},
    #     "PT/PT": {"switch": "off"},
    #     "PT/ALE": {"switch": "off"},
    #     "PT/ALG": {"switch": "off"},
    #     "PT/AML": {"switch": "off"},
    #     "PT/CET": {"switch": "off"},
    #     "PT/CEN": {"switch": "off"},
    #     "PT/CES": {"switch": "off"},
    #     "PT/NOR": {"switch": "off"},
    #     "PT/NON": {"switch": "off"},
    #     "PT/NOS": {"switch": "off"},
    #     "SE": {"switch": "off"},
    #     "SI": {"switch": "off"},
    #     "SK": {"switch": "off"}
    # }
    #
    # ## For france create a dictionary in a loop, because of the many subregions
    # FR_districts = pd.read_csv(os.path.join(r"data", "vector", "IACS", "FR", "region_code.txt"))
    # FR_districts = list(FR_districts["code"])
    # for district in FR_districts:
    #     run_dict[f"FR/{district}"] = {"switch": "off"}
    #
    # ## For spain create a dictionary in a loop, because of the many subregions
    # ## This code snippet needs to be corrected. I did the copying manually!
    # ES_districts = pd.read_csv(os.path.join(r"data", "vector", "IACS", "ES", "region_code.txt"))
    # ES_districts = list(ES_districts["code"])
    # for district in ES_districts:
    #     run_dict[f"ES/{district}"] = {"switch": "off"}
    #
    # ## Loop over country codes in dict for processing
    # for country_code in run_dict:
    #     switch = run_dict[country_code].get("switch", "off").lower()
    #     if switch != "on":
    #         continue
    #     ## Derive input variables for processing
    #     region_id = country_code.replace(r"/", "_")

    csv_lst = glob.glob(os.path.join("data", "tables", "crop_classifications", "*.csv"))

    out_lst = []
    for pth in csv_lst:
        print(pth)
        df = pd.read_csv(pth)

        df["EC_hcat_c"] = df["EC_hcat_c"].astype(str)
        df["HCAT_l2"] = df["EC_hcat_c"].str[:6]

        df = df.drop_duplicates(subset=["crop_code", "HCAT_l2"]).copy()
        df.sort_values(by="crop_code", inplace=True)

        # Show codes with their occurrence counts
        crop_code_counts = df['crop_code'].value_counts()
        duplicate_crop_codes = crop_code_counts[crop_code_counts > 1]

        if not duplicate_crop_codes.empty:
            sub = df.loc[df['crop_code'].isin(duplicate_crop_codes.index)]
            sub = sub[['crop_code', 'crop_name', 'EC_trans_n', 'EC_hcat_n', 'EC_hcat_c', 'HCAT_l2', 'country_code']].copy()
            print("Crop codes that occur multiple times:")
            print(duplicate_crop_codes)
            out_lst.append(sub)
        else:
            print("No crop codes occur multiple times")

    out_df = pd.concat(out_lst)
    out_df.to_csv(os.path.join("data", "tables", "statistics", "changing_original_crop_codes",
                               "overview_changing_crop_codes.csv"), index=False)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()

