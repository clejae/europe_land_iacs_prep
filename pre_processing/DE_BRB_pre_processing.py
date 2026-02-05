# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
from osgeo import gdal
import sys
script_dir = dirname(abspath(__file__))
project_root = dirname(script_dir)
sys.path.append(project_root)

gdal.SetConfigOption("OGR_GEOMETRY_ACCEPT_UNCLOSED_RING", "NO")

from my_utils import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
# WD =
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    for year in range(2025, 2026):
        if year < 2023:
            in_pth = os.path.join("data", "vector", "IACS", "DE", "BRB", "Original", f"IACS_BB_{year}.shp")
        else:
            in_pth = os.path.join("data", "vector", "IACS", "DE", "BRB", "Original", f"antrag_land_{year}_with_organic.shp")
        out_pth = os.path.join("data", "vector", "IACS", "DE", "BRB", f"IACS_BB_{year}.geoparquet")
        id_col = "FLIK_SC"

        gdf = gpd.read_file(in_pth)

        if year in [2017, 2018]:
            gdf.loc[gdf["PARZ_NR"].isna(), "PARZ_NR"] = 0
            gdf[id_col] = gdf["FB_FLIK"] + "_" + gdf["PARZ_NR"].astype(str)
        elif year in [2019, 2020, 2021, 2022]:
            gdf.loc[gdf["PARZ_NR"].isna(), "PARZ_NR"] = 0
            gdf[id_col] = gdf["REF_IDENT"] + "_" + gdf["PARZ_NR"].astype(int).astype(str)
        elif year in [2023, 2024]:
            gdf[id_col] = gdf["REF_IDENT"] + "_0"
        elif year in [2025]:
            gdf[id_col] = gdf["ref_ident"] + "_0"

        in_len = len(gdf)

        gdf["geom_id"] = gdf.geometry.to_wkb()
        dups = gdf[gdf.duplicated("geom_id", "first")].copy()
        if len(dups) > 0:
            gdf.drop_duplicates(subset="geom_id", inplace=True)
            dups.to_file(os.path.join("data", "vector", "IACS", "DE", "BRB", "Original", f"DUPS_antrag_land_{year}_with_organic.shp"))
        print(f"{len(dups)} geometry duplicates were found for {in_pth}.")

        gdf["unique_id"] = helper_functions.make_id_unique_by_adding_cumcount(gdf[id_col])
        gdf = gdf.loc[gdf["geometry"].notna()].copy()
        out_len = len(gdf)
        gdf.drop(columns="geom_id", inplace=True)
        print(in_len, out_len)
        gdf.to_parquet(out_pth)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')