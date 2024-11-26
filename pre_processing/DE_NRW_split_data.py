# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import fiona
import numpy as np
import pandas as pd
import math
import geopandas as gpd
from osgeo import ogr

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def separate_file_by_years(in_pth, year_col, out_folder):

    print(f"Read {in_pth}")
    gdf = gpd.read_file(in_pth)

    uni_years = gdf[year_col].unique()

    for year in uni_years:
        print(year)
        if year == 2019:
            continue
        gdf_sub = gdf.loc[gdf[year_col] == year].copy()
        out_pth = fr"{out_folder}\{os.path.basename(in_pth).split('.')[0]}_{year}.{os.path.basename(in_pth).split('.')[1]}"
        gdf_sub.to_file(out_pth)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    separate_file_by_years(
        in_pth=r"data\vector\IACS\DE\NRW\V_OD_LWK_TSCHLAG_HIST.shp",
        year_col="WJ",
        out_folder=r"data\vector\IACS\DE\NRW")

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')