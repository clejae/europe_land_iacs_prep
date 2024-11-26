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

    for year in range(2020, 2025):

        print("Reading input", year)
        pth1 = fr"data\vector\IACS\DK\Marker_{year}\Marker_{year}.shp"
        pth2 = fr"data\vector\IACS\DK\Marker_{year}\Marker{year}.shp"

        if os.path.exists(pth1):
            iacs = gpd.read_file(pth1)
        elif os.path.exists(pth2):
            iacs = gpd.read_file(pth2)
        else:
            print("file path does not exist")
            break

        ## Create field_id
        if year < 2012:
            farm_id_col = "Ansoeger"
        elif year < 2014:
            farm_id_col = "KUNDE_LB"
        else:
            farm_id_col = "Journalnr"

        iacs["Marknr"] = iacs["Marknr"].astype(str)
        iacs[farm_id_col] = iacs[farm_id_col].astype(str)

        iacs.loc[iacs[farm_id_col].isna(), farm_id_col] = "NA"
        iacs.loc[iacs["Marknr"].isna(), "Marknr"] = "NA"

        iacs["field_id"] = iacs[farm_id_col] + "_" + iacs["Marknr"]

        ## Unfortunately, the combination of farm id and marknr is not unique, because of errors in both columns, thus
        ## for the duplicates we create a generic unique id
        if len(iacs) != len(iacs["field_id"].unique()):
            iacs["temp_id"] = range(1, len(iacs) + 1)
            print("No unique field ID", len(iacs), len(iacs["field_id"].unique()))

            dups = iacs[iacs.duplicated(subset=["field_id"])].copy()
            dups.sort_values(by="field_id", inplace=True)

            dups['field_id_distinguished'] = iacs.groupby('field_id').cumcount()
            dups['field_id_distinguished'] = dups['field_id_distinguished'].astype(int)
            dups['unique_field_id'] = dups['field_id'].astype(str) + '_' + dups['field_id_distinguished'].astype(str)

            field_id_dict = {row.temp_id: row.field_id for row in iacs.itertuples()}
            for row in dups.itertuples():
                field_id_dict[row.temp_id] = row.unique_field_id

            iacs["field_id"] = iacs["temp_id"].map(field_id_dict)

            iacs.drop(columns=["temp_id"], inplace=True)

        ## For these years, there are not duplicated farm_id field id combinations, so we can use them to merge the OML
        ## to the df
        if year in [2018, 2019, 2020, 2021, 2022, 2023]:
            pth = fr"data\vector\IACS\DK\HiDrive-Organic\Oekologiske_arealer_{year}.shp"
            org_col = "OML"

            print("Reading organic", year)
            gdf_organic = gpd.read_file(pth)

            ## This would be the merging by the spatial join, however this produces some duplicated
            # centr_org = gdf_organic.copy()
            # centr_org.geometry = centr_org.geometry.centroid
            # t = centr_org[["OML", "geometry"]].sjoin(iacs)
            # dups = t[t.duplicated(subset=["field_id"])].copy()
            # dups.sort_values(by="field_id", inplace=True)
            # iacs = pd.merge(iacs, t[["field_id", "OML"]], "left", "field_id")
            if year >= 2020:
                farm_id_col = "FSjournal"

            gdf_organic["field_id"] = gdf_organic[farm_id_col] + "_" + gdf_organic["Marknr"]
            iacs = pd.merge(iacs, gdf_organic[["field_id", "OML"]], "left", "field_id")

            iacs.loc[iacs["OML"].isna(), "OML"] = "0"
            iacs["OML"] = iacs["OML"].map({"0": 0, "1": 1, "2": 2})
            iacs["OML"] = iacs["OML"].astype(int)

        out_pth = fr"data\vector\IACS\DK\Marker_{year}\Marker_{year}_prep.geoparquet"
        iacs.to_parquet(out_pth)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')