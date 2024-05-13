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

    year_dict = {
        2020: {
            "path": r"data\vector\IACS\SE_temp\MULTI_KUNDRED_SKIFTE2020_GV.shp",
            "blockid": "SAMIBLOCK_",
            "skiftesbok": "SKIFTESBET",
            "main_crop_id": "GRDKOD_MAR",
            "sub_crop_id": "GRDKOD_UND"
        },
        2021: {
            "path": r"data\vector\IACS\SE_temp\KUNDRED_SKIFTE_2021.gpkg",
            "blockid": "SAMIBLOCK_",
            "skiftesbok": "SKIFTESBET",
            "main_crop_id": "GRDKOD_MAR",
            "sub_crop_id": "GRDKOD_UND"
        },
        2022: {
            "path": r"data\vector\IACS\SE_temp\KUNDRED_SKIFTE_2022.gpkg",
            "blockid": "sami_blockid",
            "skiftesbok": "skiftesbeteckning",
            "main_crop_id": "grodkod_markanvandning",
            "sub_crop_id": "grodkod_under"
        }
    }


    for year in year_dict:

        pth = year_dict[year]["path"]
        blockid = year_dict[year]["blockid"]
        skiftesbok = year_dict[year]["skiftesbok"]
        main_crop_id = year_dict[year]["main_crop_id"]
        sub_crop_id = year_dict[year]["sub_crop_id"]

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
        df_org["organic_applied"] = df_org["Organic applied?"].map({"Y": 1, "N": 0})

        gdf = gpd.read_file(pth)
        gdf["individid"] = gdf[blockid] + gdf[skiftesbok]

        print("Adding organic information")
        gdf = pd.merge(gdf, df_org[["LPIS (11)+parcel_id", "organic_applied"]], "left", left_on="individid", right_on="LPIS (11)+parcel_id")

        # t = gdf.loc[gdf["organic_applied"].isna()].copy()
        # t2 = gdf.loc[gdf["organic_applied"].notna()].copy()

        print("Combining crop codes and assigning crop names")
        gdf["code"] = gdf[main_crop_id] + '_' + gdf[sub_crop_id]
        gdf.loc[gdf[sub_crop_id].isna(), "code"] = gdf.loc[gdf[sub_crop_id].isna(), main_crop_id]
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
