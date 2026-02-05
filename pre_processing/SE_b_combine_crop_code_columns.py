# Author:
# github repository:

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
import pandas as pd
import glob

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)


# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def combine_crop_codes_and_add_organic_information():
    df_crops = pd.read_excel(os.path.join("data", "vector", "IACS", "SE", "crop_codes_prepared.xlsx"))

    year_dict = {
        2020: {
            "switch": "off",
            "path": os.path.join("data", "vector", "IACS", "SE_temp", "MULTI_KUNDRED_SKIFTE2020_GV.shp"),
            "blockid": "SAMIBLOCK_",
            "skiftesbok": "SKIFTESBET",
            "main_crop_id": "GRDKOD_MAR",
            "sub_crop_id": "GRDKOD_UND"
        },
        2021: {
            "switch": "off",
            "path": os.path.join("data", "vector", "IACS", "SE_temp", "KUNDRED_SKIFTE_2021.shp"),
            "blockid": "SAMIBLOCK_",
            "skiftesbok": "SKIFTESBET",
            "main_crop_id": "GRDKOD_MAR",
            "sub_crop_id": "GRDKOD_UND"
        },
        2022: {
            "switch": "off",
            "path": os.path.join("data", "vector", "IACS", "SE_temp", "KUNDRED_SKIFTE_2022.shp"),
            "blockid": "sami_blockid",
            "skiftesbok": "skiftesbeteckning",
            "main_crop_id": "grodkod_markanvandning",
            "sub_crop_id": "grodkod_under"
        },
        2023: {
            "switch": "off",
            "path": os.path.join("data", "vector", "IACS", "SE_temp", "SAMI_2023_Kundred_Skifte_GV.shp"),
            "blockid": "sami_blockid",
            "skiftesbok": "skiftesbeteckning",
            "main_crop_id": "grodkod_markanvandning",
            "sub_crop_id": "grodkod_under"
        }
    }

    for year in year_dict:
        switch = year_dict[year].get("switch", "off").lower()
        if switch != "on":
            continue

        pth = year_dict[year]["path"]
        blockid = year_dict[year]["blockid"]
        skiftesbok = year_dict[year]["skiftesbok"]
        main_crop_id = year_dict[year]["main_crop_id"]
        sub_crop_id = year_dict[year]["sub_crop_id"]

        print("Reading input", year)
        xl_org = pd.ExcelFile(os.path.join("data", "vector", "IACS", "SE", f"_organic_SBI-2683 LPIS_parcelid_{year}.xlsx")),

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
        # gdf.to_file(os.path.splitext(pth)[0] + '_with_crops.gpkg', driver="GPKG")
        gdf.to_parquet(os.path.splitext(pth)[0] + ".geoparquet")

def create_applicant_id():
    year_dict = {
        2020: {
            "switch": "off",
            "path": os.path.join("data", "vector", "IACS", "SE", "w_appl", "MULTI_KUNDRED_SKIFTE2020_GV_with_crops.gpkg"),
            "kund_lan": "KUND_LAN",
            "kund_lopnr": "KUND_LOPNR"
        },
        2021: {
            "switch": "off",
            "path": os.path.join("data", "vector", "IACS", "SE", "w_appl", "KUNDRED_SKIFTE_2021_with_crops.gpkg"),
            "kund_lan": "KUND_LAN",
            "kund_lopnr": "KUND_LOPNR"
        }
    }


    for year in year_dict:
        switch = year_dict[year].get("switch", "off").lower()
        if switch != "on":
            continue
        pth = year_dict[year]["path"]
        kund_lan = year_dict[year]["kund_lan"]
        kund_lopnr = year_dict[year]["kund_lopnr"]

        gdf = gpd.read_file(pth)
        gdf["kundnummer"] = gdf[kund_lan].astype(str) + gdf[kund_lopnr].astype(str)

        gdf.to_parquet(os.path.splitext(pth)[0] + ".geoparquet")


def combine_crop_codes_in_public_data(min_year, max_year):

    year_dict = {
        year: {
            "path": os.path.join("data", "vector", "IACS","SE_temp", "public", f"arslager_skiftePolygon_{year}.shp"),
            "blockid": "blockid",
            "skiftesbok": "skiftesbet",
            "main_crop_id": "grdkod_mar",
            "sub_crop_id": "grdkod_und"
        } for year in range(min_year, max_year+1)
    }

    for year in year_dict:

        if year <= 2020:
            df_crops = pd.read_excel(os.path.join("data", "vector", "IACS", "SE", "crop_codes_prepared.xlsx"), sheet_name="2020")
        elif year <= 2023:
            df_crops = pd.read_excel(os.path.join("data", "vector", "IACS", "SE", "crop_codes_prepared.xlsx"), sheet_name=str(year))
        else:
            df_crops = pd.read_excel(os.path.join("data", "vector", "IACS", "SE", "crop_codes_prepared.xlsx"),
                                     sheet_name="2023")

        pth = year_dict[year]["path"]
        blockid = year_dict[year]["blockid"]
        skiftesbok = year_dict[year]["skiftesbok"]
        main_crop_id = year_dict[year]["main_crop_id"]
        sub_crop_id = year_dict[year]["sub_crop_id"]

        gdf = gpd.read_file(pth)
        gdf["individid"] = gdf[blockid] + gdf[skiftesbok]

        print("Combining crop codes and assigning crop names")
        gdf["code"] = gdf[main_crop_id].astype(str) + '_' + gdf[sub_crop_id].astype(str)
        gdf.loc[gdf[sub_crop_id] == 0, "code"] = gdf.loc[gdf[sub_crop_id] == 0, main_crop_id]
        gdf.loc[gdf[sub_crop_id].isna(), "code"] = gdf.loc[gdf[sub_crop_id].isna(), main_crop_id]
        gdf["code"] = gdf["code"].astype(str)

        df_crops["Grödkod"] = df_crops["Grödkod"].astype(str)
        gdf = pd.merge(gdf, df_crops, "left", left_on="code", right_on="Grödkod")

        if "Grödkod" in gdf.columns:
            gdf.drop(columns=["Grödkod"], inplace=True)
        if "LPIS (11)+parcel_id" in gdf.columns:
            gdf.drop(columns=["LPIS (11)+parcel_id"], inplace=True)

        print("Writing out.")
        # gdf.to_file(os.path.splitext(pth)[0] + '_with_crops.gpkg', driver="GPKG")
        gdf.to_parquet(os.path.splitext(pth)[0] + ".geoparquet")


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # combine_crop_codes_and_add_organic_information()
    # combine_crop_codes_in_public_data(min_year=2024, max_year=2024)
    # create_applicant_id()

    # for year in range(2024, 2025):
    #     pth = glob.glob(os.path.join("data", "vector", "IACS", "SE", "Original", f"*{year}*"))[0]
    #
    #     root, ext = os.path.splitext(pth)
    #
    #     if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
    #         gdf = gpd.read_file(pth)
    #     elif ext in ['.geoparquet']:
    #         gdf = gpd.read_parquet(pth)
    #
    #     t1 = gdf.loc[gdf["Gröda"].isna()].copy()
    #     t1["code"].unique()
    #
    #     df = gdf.drop_duplicates(subset=["code", "Gröda"]).copy()
    #     df.dropna(subset="Gröda", inplace=True)
    #     cl_dict = dict(zip(df["code"], df["Gröda"]))
    #
    #     gdf.loc[gdf["Gröda"].isna(), "Gröda"] = gdf.loc[gdf["Gröda"].isna(), "code"].map(cl_dict)
    #     t2 = gdf.loc[gdf["Gröda"].isna()].copy()
    #
    #     t2["code"].unique()
    #
    #     if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
    #        gdf.to_file(pth)
    #     elif ext in ['.geoparquet']:
    #         gdf.to_parquet(pth)



    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
