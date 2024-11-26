# Author:
# github repository:

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
import pandas as pd

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def restructure_data():
    years = list(range(2022, 2025))
    # years.remove(2022)
    years = [2021]

    for year in years:

        pth1 = fr"Q:\Europe-LAND\data\vector\IACS\HU_temp\{year}\ige_blokk_metszet_{year}_1.shp"
        pth2 = fr"Q:\Europe-LAND\data\vector\IACS\HU_temp\{year}\ige_blokk_metszet_{year}_2.shp"

        print("Reading input", year)
        gdf1 = gpd.read_file(pth1)
        gdf2 = gpd.read_file(pth2)

        block_col = "blosz"

        cols1 = list(gdf1.columns)
        cols2 = list(gdf2.columns)

        cols1.remove(block_col)
        cols2.remove(block_col)
        cols1.remove("geometry")
        cols2.remove("geometry")

        df1 = pd.melt(gdf1, id_vars=block_col, value_vars=cols1)
        df1 = df1.loc[df1["value"] > 0].copy()
        df2 = pd.melt(gdf2, id_vars=block_col, value_vars=cols2)
        df2 = df2.loc[df2["value"] > 0].copy()

        df = pd.concat([df1, df2])
        df.rename(columns={"variable": "crop_code", "value": "field_size"}, inplace=True)
        df["field_size"] = df["field_size"] / 10000

        df.sort_values(by="field_size", ascending=False, inplace=True)
        df["temp_id"] = range(len(df))

        df_vec = df.copy()
        df_vec.drop_duplicates(subset=block_col, inplace=True)
        df_csv = df.loc[~df["temp_id"].isin(df_vec["temp_id"])].copy()

        geom_dict1 = pd.Series(gdf1.geometry.values, index=gdf1.blosz).to_dict()
        geom_dict2 = pd.Series(gdf2.geometry.values, index=gdf2.blosz).to_dict()
        geom_dict = geom_dict1 | geom_dict2

        df_vec["geometry"] = df_vec[block_col].map(geom_dict)
        df_vec.reset_index(inplace=True)
        df_vec.drop(columns=["temp_id", "index"], inplace=True)
        df_vec = gpd.GeoDataFrame(df_vec)
        # df_vec.crs = df_vec.to_crs(23700)

        df_csv.drop(columns=["temp_id"], inplace=True)

        print("Writing out.")
        # df_vec.to_file(rf"data\vector\IACS\HU\{year}\ige_blokk_metszet_{year}.gpkg", driver="GPKG")#
        helper_functions.create_folder(rf"data\vector\IACS\HU\{year}")
        df_vec.to_parquet(rf"data\vector\IACS\HU\{year}\ige_blokk_metszet_{year}.geoparquet")
        df_csv.to_csv(rf"data\vector\IACS\HU\{year}\ige_blokk_metszet_{year}.csv", index=False)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    restructure_data()

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
