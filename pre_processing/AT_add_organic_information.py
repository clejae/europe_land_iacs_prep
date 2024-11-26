# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import warnings
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

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    for year in range(2022, 2023):

        print("Reading input", year)
        pth1 = fr"data\vector\IACS\AT\invekos_a{year}_sl_anonym.gpkg"
        pth2 = fr"data\vector\IACS\AT\Measures_{year}.csv"

        iacs = gpd.read_file(pth1)
        print("IACS columns:", iacs.columns)
        orga = pd.read_csv(pth2, sep=";", encoding="ISO-8859-1")
        print("Measures columns:", orga.columns)

        if iacs["fsnr"].dtype == float:
            iacs["fsnr"] = iacs["fsnr"].astype(int)
        if iacs["slnr"].dtype == float:
            iacs["slnr"] = iacs["slnr"].astype(int)

        iacs["schlag_id"] = (iacs["hbnr"].astype(str) + "_" + iacs["fsnr"].astype(str) + "_" + iacs["slnr"].astype(str))
        orga["organic"] = 0
        orga.loc[orga["o5bio"].notna() | orga["o5bioteil"].notna() | orga["o5biolse"].notna(), "organic"] = 1

        iacs = pd.merge(iacs, orga[["schlag_id", "organic"]], "left", "schlag_id")

        out_pth = fr"data\vector\IACS\AT\invekos_a{year}_prep.geoparquet"
        iacs.to_parquet(out_pth)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')