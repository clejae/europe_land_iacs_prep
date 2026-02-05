# Author:
# github repository:

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
import glob

from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def add_organic_information():
    years = range(2015, 2024)

    for year in years:

        pth = os.path.join("data", "vector", "IACS", "NL", f"Percelen_{year}.gpkg") #fr"data\vector\IACS\NL\Percelen_{year}.gpkg"
        org_col = "BIOLOGISCHEPRODUCTIEWIJZE"

        print("Reading input", year)
        gdf = gpd.read_file(pth)

        gdf["organic"] = 0
        gdf.loc[gdf[org_col] == "01", "organic"] = 1

        print("Writing out.")
        gdf.to_file(os.path.splitext(pth)[0] + '_with_organic_information.gpkg', driver="GPKG")


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # add_organic_information()

    ## Exploration
    # in_dir = os.path.join("data", "vector", "IACS", "NL")
    # iacs_files = glob.glob(os.path.join(in_dir, "*.gpkg"))
    #
    # for i, in_pth in enumerate(iacs_files[:1]):
    #
    #     year = helper_functions.get_year_from_path(in_pth)
    #     print(year)
    #
    #     print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")
    #     # count_duplicate_geometries(in_pth)
    #
    #     out_pth = os.path.join("data", "vector", "IACS", "NL", f"DUPS-layer_nl_{year}.gpkg")
    #     helper_functions.extract_geometry_duplicates(in_pth, out_pth)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
