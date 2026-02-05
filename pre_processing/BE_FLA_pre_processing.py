# Author: Clemens Jänicke
# github repository:


# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd

from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    in_dir = os.path.join("data", "vector", "IACS", "BE", "FLA", "Original")
    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

    for i, iacs_pth in enumerate(iacs_files):
        print(f"{i + 1}/{len(iacs_files)} - Processing - {iacs_pth}")
        year = helper_functions.get_year_from_path(iacs_pth)
        out_pth = os.path.join("data", "vector", "IACS", "BE", "FLA", f"GSA-BE_FLA-{year}.geoparquet")

        print("Reading input", iacs_pth)
        iacs = gpd.read_file(iacs_pth)

        ## Run this to get a feeling for the duplicate IDs
        # id_counts = iacs["REF_ID"].value_counts()
        # duplicated_ids = id_counts[id_counts > 1].index
        # iacs_sub = iacs[iacs["REF_ID"].isin(duplicated_ids)].copy()
        # iacs_sub.sort_values(by=["REF_ID", "GEWAS"])

        iacs = iacs.loc[iacs["geometry"].notna()].copy()
        iacs["new_id"] = helper_functions.make_id_unique_by_adding_cumcount(iacs["REF_ID"])

        print(f"Write out: {out_pth}")
        iacs.to_parquet(out_pth)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')