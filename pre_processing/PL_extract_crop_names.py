# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd

from my_utils import helper_functions
import glob


# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## Exploration
    # in_dir = os.path.join("data", "vector", "IACS", "PL", "Original")
    # iacs_files = glob.glob(os.path.join(in_dir, "*.shp"))
    #
    # for i, in_pth in enumerate(iacs_files[:1]):
    #
    #     year = helper_functions.get_year_from_path(in_pth)
    #     print(year)
    #
    #     print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")
    #     # count_duplicate_geometries(in_pth)
    #
    #     out_pth = os.path.join("data", "vector", "IACS", "PL", "Original", f"DUPS-layer_pl_{year}.gpkg")
    #     helper_functions.extract_geometry_duplicates(in_pth, out_pth)

    pth = os.path.join("data", "vector", "IACS", "PL", "Original", "GSAA_Poland_2024.shp")
    iacs = gpd.read_file(pth)

    ## Get unique names
    uni_crops = iacs[["crop"]].drop_duplicates()
    uni_crops.to_excel(os.path.join("data", "vector", "IACS", "PL", "Original", "PL_unique_crops_2024.xlsx"))

    iacs["area_ha"] = [float(s.replace(" ha",  "")) for s in iacs["pow"]]

    farms = iacs[["eps", "pow_gosp"]].drop_duplicates()
    farms.to_csv(os.path.join("data", "vector", "IACS", "PL", "Original", r"farm_ids_and_sizes.csv"))

    print(iacs[:10])

    iacs = helper_functions.drop_non_geometries(iacs)

    if len(iacs) != len(iacs["id_dz_rol"].unique()):
        iacs["uni_id"] = helper_functions.make_id_unique_by_adding_cumcount(iacs["id_dz_rol"])
    iacs = helper_functions.remove_geometry_duplicates(iacs)

    iacs.drop(columns=["pow", "pow_gosp"], inplace=True)
    iacs.to_parquet(os.path.join("data", "vector", "IACS", "PL", "pre_processed_data", "Poland_GSAA_2024.geoparquet"))

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')