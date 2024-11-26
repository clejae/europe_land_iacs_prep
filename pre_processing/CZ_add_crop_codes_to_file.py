# Author:
# github repository:

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
import pandas as pd
import glob

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

in_dir = fr"data\vector\IACS\CZ"
# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def add_crop_codes(in_dir, out_dir):

    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)
    for file in iacs_files:
        year = helper_functions.get_year_from_path(file)
        df_pth = glob.glob(os.path.dirname(file) + f"\dotace_dpb_plodina_*({year})\*.xlsx")

        print("Processing", year)
        print("DF with crop culture information", df_pth)

        if len(df_pth) == 0:
            continue

        df_pth = df_pth[0]

        if int(year) < 2023:
            df_cols = ["NKOD_DPB", "PLODINA_ID", "PLODINA_NAZEV", "GREENING_SKUPINA"]
            gdf_cols = ["NKOD_DPB", "ID_UZ", "VYMERA", "EKO", "geometry"]
            merge_col = "NKOD_DPB"
        else:
            df_cols = ["FB_ID", "Plodina ID", "Název plodiny"]
            gdf_cols = ["ID_DPB", "NKOD_DPB", "ID_UZ", "VYMERA", "EKO", "geometry"]
            merge_col = "ID_DPB"

        print("Reading input")
        df = pd.read_excel(df_pth)
        gdf = gpd.read_file(file, include_fields=gdf_cols)

        print("Merging datasets")
        if int(year) == 2023:
            df.rename(columns={"FB_ID": "ID_DPB", "Plodina ID": "PLODINA_ID", "Název plodiny": "PLODINA_NAZEV"},
                      inplace=True)
            df_cols = ["ID_DPB", "PLODINA_ID", "PLODINA_NAZEV"]
        gdf = pd.merge(gdf[gdf_cols], df[df_cols], "left", merge_col)

        print("Number rows in df:", len(df))
        print("Number rows in vector df:", len(gdf))
        print("Number rows not matched:", len(gdf.loc[gdf["PLODINA_ID"].isna()]))

        print("Writing out.")
        gdf.to_file(out_dir + f'\IACS-CZ-{year}_with_crops.gpkg', driver="GPKG")


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    add_crop_codes(
        in_dir=r"data\vector\IACS\CZ\IACS_Czechia",
        out_dir=r"data\vector\IACS\CZ")

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
