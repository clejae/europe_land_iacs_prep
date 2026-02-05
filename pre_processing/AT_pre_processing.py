# Author: Clemens Jänicke
# github repository:


# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import pandas as pd
import geopandas as gpd

from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

## correct the crop name errors in the files with farm id with the information from the files without farm id
def correct_crop_names(iacs_corr, iacs_inc, corr_code_col, corr_name_col, incorr_code_col, incorr_name_col):
    print("CORRECT CROP NAMES")

    ## make sure the crop code columns have the same dtype
    iacs_corr[corr_code_col] = iacs_corr[corr_code_col].astype(int)
    iacs_inc[incorr_code_col] = iacs_inc[incorr_code_col].astype(int)

    ## get correction table
    crop_table = iacs_corr[[corr_code_col, corr_name_col]].drop_duplicates()
    crop_codes_counts = crop_table[corr_code_col].value_counts()

    ## check if we crop codes are unique to crop names
    if sum(crop_codes_counts) == len(crop_table):
        crop_dict = dict(zip(crop_table[corr_code_col], crop_table[corr_name_col]))
    else:
        print("Crop codes not unique to crop names!")
        return

    ## correct the crop names
    iacs_inc[incorr_name_col] = iacs_inc[incorr_code_col].map(crop_dict)

    return iacs_inc

def add_organic_information(iacs, measures_pth):

    print("ADD ORGANIC INFORMATION")
    print("IACS columns:", iacs.columns)
    orga = pd.read_csv(measures_pth, sep=";", encoding="ISO-8859-1")
    print("Measures columns:", orga.columns)

    if iacs["fsnr"].dtype == float:
        iacs["fsnr"] = iacs["fsnr"].astype(int)
    if iacs["slnr"].dtype == float:
        iacs["slnr"] = iacs["slnr"].astype(int)

    iacs["schlag_id"] = (iacs["hbnr"].astype(str) + "_" + iacs["fsnr"].astype(str) + "_" + iacs["slnr"].astype(str))
    orga["organic"] = 0
    orga.loc[orga["o5bio"].notna() | orga["o5bioteil"].notna() | orga["o5biolse"].notna(), "organic"] = 1

    org_dict = dict(zip(orga["schlag_id"], orga["organic"]))
    iacs["organic"] = iacs["schlag_id"].map(org_dict)

    return iacs




def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    #################################
    for year in [2016, 2017, 2018]: #
        corr_pth = os.path.join("data", "vector", "IACS", "AT", "Original", f"INSPIRE_SCHLAEGE_{year}_POLYGON.gpkg")
        incorr_pth = os.path.join("data", "vector", "IACS", "AT", "Original", f"invekos_a{year}_sl_anonym.gpkg")
        out_pth = os.path.join("data", "vector", "IACS", "AT", f"invekos_a{year}_sl_anonym_prepared.geoparquet")

        print("Reading input", corr_pth, incorr_pth)
        iacs_corr = gpd.read_file(corr_pth)
        iacs_inc = gpd.read_file(incorr_pth)

        iacs = correct_crop_names(
            iacs_corr=iacs_corr,
            iacs_inc=iacs_inc,
            corr_code_col="SNAR_CODE",
            corr_name_col="SNAR_BEZEICHNUNG",
            incorr_code_col="snart_code",
            incorr_name_col="snart"
        )

        iacs = add_organic_information(
            iacs=iacs,
            measures_pth=os.path.join("data", "vector", "IACS", "AT", f"Measures_{year}.csv")
        )

        iacs = helper_functions.drop_non_geometries_and_add_unique_fid(
            iacs=iacs
        )

        print(f"Write out: {out_pth}")
        iacs.to_parquet(out_pth)

    #################################
    for year in [2015, 2019, 2020, 2021, 2022]:
        iacs_pth = os.path.join("data", "vector", "IACS", "AT", "Original", f"invekos_a{year}_sl_anonym.gpkg")
        out_pth = os.path.join("data", "vector", "IACS", "AT", f"invekos_a{year}_sl_anonym_prepared.geoparquet")

        print("Reading input", iacs_pth)
        iacs = gpd.read_file(iacs_pth)

        iacs = add_organic_information(
            iacs=iacs,
            measures_pth=os.path.join("data", "vector", "IACS", "AT", f"Measures_{year}.csv")
        )

        iacs = helper_functions.drop_non_geometries_and_add_unique_fid(
            iacs=iacs
        )

        print(f"Write out: {out_pth}")
        iacs.to_parquet(out_pth)

    #################################
    for year in [2023, 2024]:
        iacs_pth = os.path.join("data", "vector", "IACS", "AT", "Original",
                                f"INSPIRE_SCHLAEGE_{year}-2_POLYGON.gpkg")
        out_pth = os.path.join("data", "vector", "IACS", "AT",
                               f"INSPIRE_SCHLAEGE{year}-2_POLYGON_prepared.geoparquet")

        print("Reading input", iacs_pth)
        iacs = gpd.read_file(iacs_pth)
        iacs = helper_functions.drop_non_geometries_and_add_unique_fid(
            iacs=iacs
        )

        print(f"Write out: {out_pth}")
        iacs.to_parquet(out_pth)

    #################################
    year = 2025
    iacs_pth = os.path.join("data", "vector", "IACS", "AT", "Original", f"INSPIRE_SCHLAEGE_{year}-1_POLYGON.gpkg")
    out_pth = os.path.join("data", "vector", "IACS", "AT", f"INSPIRE_SCHLAEGE{year}-1_POLYGON_prepared.geoparquet")
    print("Reading input", iacs_pth)
    iacs = gpd.read_file(iacs_pth)
    iacs = helper_functions.drop_non_geometries_and_add_unique_fid(
        iacs=iacs
    )

    print(f"Write out: {out_pth}")
    iacs.to_parquet(out_pth)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')