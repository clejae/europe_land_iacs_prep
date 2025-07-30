# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import pandas as pd
import geopandas as gpd
import warnings
import numpy as np
import glob

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

COL_NAMES_FOLDER = os.path.join("data", "tables", "column_names")
CROP_CLASSIFICATION_FOLDER = os.path.join("data", "tables", "crop_classifications")
# check the existence of the outpu dir and create if needed 
os.makedirs(os.path.join("data","tables","hcat_levels_v2","reclassification"), exist_ok=True)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## Go through all classifications and get original codes and crops assigned to unmaintained and not_known_and_other

    pth_lst = glob.glob(os.path.join(CROP_CLASSIFICATION_FOLDER, "*_final.xlsx"))
    df_lst = [pd.read_excel(pth) for pth in pth_lst]
    abbr_lst = [os.path.basename(pth).split("_crop_")[0] for pth in pth_lst]

    cols = ['crop_code', 'crop_name', 'crop_name_de', 'crop_name_en',
            'EC_trans_n', 'EC_hcat_n', 'EC_hcat_c', 'country_code']
    for i, df in enumerate(df_lst):
        print(abbr_lst[i])
        df["country_code"] = abbr_lst[i]

    df_lst = [df[cols] for df in df_lst]
    df_out = pd.concat(df_lst)
    df_out = df_out.loc[df_out["EC_hcat_n"].isin(["unmaintained", "afforestation_reforestation", "not_known_and_other", "other_arable_crops"])]
    df_out.drop_duplicates(inplace=True)
    df_out.to_csv(os.path.join("data","tables","hcat_levels_v2","reclassification","candidate_classes_for_reclassification.csv"), index=False)
   
    df1 = df_lst[0]

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()
