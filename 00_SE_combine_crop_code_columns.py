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
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)


# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def combine_crop_codes_and_add_organic_information():
    df_crops = pd.read_excel(r"data\vector\IACS\SE\crop_codes_prepared.xlsx")

    pth_lst = [r"data\vector\IACS\SE_temp\2020\beslutade_skiften_2020.shp",
               r"data\vector\IACS\SE_temp\2021\Beslutade_skiften_2021.shp"]
    # pth_lst = [r"data\vector\IACS\SE_temp\2020\sub_2020.gpkg"]

    for pth in pth_lst:

        year = helper_functions.get_year_from_path(pth)

        print("Reading input", year)
        xl_org = pd.ExcelFile(rf"data\vector\IACS\SE\_organic_SBI-2683 LPIS_parcelid_{year}.xlsx")

        # Define an empty list to store individual DataFrames
        list_of_dfs = []
        for sheet in xl_org.sheet_names:
            # Parse data from each worksheet as a Pandas DataFrame
            df = xl_org.parse(sheet)

            # And append it to the list
            list_of_dfs.append(df)

        # Combine all DataFrames into one
        df_org = pd.concat(list_of_dfs, ignore_index=True)
        df_org["organic_applied"] = df["Organic applied?"].map({"Y": 1, "N": 0})

        gdf = gpd.read_file(pth)

        print("Adding organic information")
        gdf = pd.merge(gdf, df_org[["LPIS (11)+parcel_id", "organic_applied"]], "left", left_on="individid", right_on="LPIS (11)+parcel_id")

        # t = gdf.loc[gdf["organic_applied"].isna()].copy()
        # t2 = gdf.loc[gdf["organic_applied"].notna()].copy()

        print("Combining crop codes and assigning crop names")
        gdf["code"] = gdf["GRDKOD_MAR"] + '_' + gdf["GRDKOD_UND"]
        gdf.loc[gdf["GRDKOD_UND"].isna(), "code"] = gdf.loc[gdf["GRDKOD_UND"].isna(), "GRDKOD_MAR"]
        gdf["code"] = gdf["code"].astype(str)

        df_crops["Grödkod"] = df_crops["Grödkod"].astype(str)
        gdf = pd.merge(gdf, df_crops, "left", left_on="code", right_on="Grödkod")

        if "Grödkod" in gdf.columns:
            gdf.drop(columns=["Grödkod"], inplace=True)
        if "LPIS (11)+parcel_id" in gdf.columns:
            gdf.drop(columns=["LPIS (11)+parcel_id"], inplace=True)

        print("Writing out.")
        gdf.to_file(os.path.splitext(pth)[0] + '_with_crops.gpkg', driver="GPKG")


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    combine_crop_codes_and_add_organic_information()

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
