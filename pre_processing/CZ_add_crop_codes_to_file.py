# Author:
# github repository:

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import sys
script_dir = dirname(abspath(__file__))
project_root = dirname(script_dir)
sys.path.append(project_root)

import time
import geopandas as gpd
import pandas as pd
import glob

from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def add_crops_clean_data_and_separate_parcels_within_field_blocks(in_dir, out_dir):

    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)
    for file in iacs_files:
        year = helper_functions.get_year_from_path(file)
        if int(year) != 2024:
            continue
        df_pth = glob.glob(os.path.dirname(file) + rf"\Plodiny_tabulkova_data_2015-2025\Plodiny_{year}.xlsx") # old

        print("Processing", year)
        print("DF with crop culture information", df_pth)

        if len(df_pth) == 0:
            continue

        df_pth = df_pth[0]

        if int(year) < 2023:
            gdf_cols = ["NKOD_DPB", "ID_UZ", "VYMERA", "EKO", "geometry"]
            merge_col = "NKOD_DPB"
        else:
            gdf_cols = ["ID_DPB", "NKOD_DPB", "ID_UZ", "VYMERA", "EKO", "geometry"]
            merge_col = "ID_DPB"

        print("Reading input")
        df = pd.read_excel(df_pth)
        gdf = gpd.read_file(file, columns=gdf_cols)

        in_len = len(gdf)
        if int(year) < 2023:
            decl_area_col = "VYM_OP_PP_DP"
        # if int(year) in (2019, 2020, 2021, 2022): #for different version of plodiny_yyyy_xlsx
        #     decl_area_col = "DEKL_VYMER"
        if int(year) in (2023, 2024, 2025):
            decl_area_col = "DEKL_VYM"

        print("Merging datasets")
        if int(year) < 2023:
            df_cols = ["NKOD_DPB", "PLODINA_ID", "PLODINA_NAZEV", "GREENING_SKUPINA", decl_area_col]
        # if int(year) == 2023:
        #     df.rename(columns={"DPB_ID": "ID_DPB", "PLOD_NAZE": "PLODINA_NAZEV"},
        #               inplace=True)
        #     df_cols = ["ID_DPB", "PLODINA_ID", "PLODINA_NAZEV", decl_area_col]
        if int(year) >= 2023:
            df.rename(columns={"DPB_ID": "ID_DPB", "Plodina ID": "PLODINA_ID", "PLOD_NAZE": "PLODINA_NAZEV"},
                      inplace=True)
            df_cols = ["ID_DPB", "PLODINA_ID", "PLODINA_NAZEV", decl_area_col]
        gdf = pd.merge(gdf[gdf_cols], df[df_cols], "left", merge_col)
        gdf.sort_values(by=[merge_col, decl_area_col], inplace=True, ascending=False)
        out_len = len(gdf)

        ## Separate the main crops from the remaining crops in each field block
        ## Remaining crops will be saved as csv file
        gdf["temp_id"] = range(len(gdf))
        gdf_out = gdf.drop_duplicates(subset=gdf_cols, keep="first").copy()
        df_out = gdf.loc[~gdf["temp_id"].isin(gdf_out["temp_id"])].copy()

        print("Check if vector and accompanying file add up:", len(df_out) + len(gdf_out), out_len)

        ## Drop non usefull columns
        gdf_out.drop(columns="temp_id", inplace=True)
        df_out.drop(columns=["temp_id", "geometry"], inplace=True)

        ## Check for entries with duplicate IDs
        id_counts = gdf_out[merge_col].value_counts()
        duplicated_ids = id_counts[id_counts > 1].index
        iacs_sub = gdf_out[gdf_out[merge_col].isin(duplicated_ids)].copy()
        print(f"Number of entries with duplicate IDs ({merge_col}): {len(iacs_sub)}")

        ## Check for entries with no geometry
        in_len2 = len(gdf_out)
        gdf_out = gdf_out.loc[gdf_out["geometry"].notna()].copy()
        out_len2 = len(gdf_out)
        print(f"{in_len2 - out_len2} entries with no geometries")

        ## Make sure that declared area column is filled for field blocks with only one crop
        gdf_out.loc[gdf_out[decl_area_col].isna(), decl_area_col] = gdf_out.loc[gdf_out[decl_area_col].isna(), "VYMERA"]

        print("Number rows in input df:", len(df))
        print("Number rows in input vector df:", in_len)
        print("Number rows in final vector df:", in_len2)
        print("Number rows not matched:", len(gdf_out.loc[gdf_out["PLODINA_ID"].isna()]))

        print("Writing out.")
        gdf_out.to_parquet(out_dir + fr'\IACS-CZ-{year}_with_crops.geoparquet')
        df_out.to_csv(out_dir + fr'\IACS-CZ-{year}_with_crops.csv', index=False)

def add_land_use_from_lpis():

    for year in range(2024, 2026):
        print(year)

        in_pth = os.path.join("data", "vector", "IACS", "CZ", f"IACS-CZ-{year}_with_crops.geoparquet")
        lps_pth = glob.glob(os.path.join("data", "vector", "IACS", "CZ", "Original", "LPIS", f"DPB_VEREJNY_GUI_{year}*.shp"))[0]

        gsa_crop_col = "PLODINA_NAZEV"
        gsa_crop_id_col = "PLODINA_ID"

        lpis_lu_col = "KULTURANAZ"
        lpis_lu_id_col =  "KULTURA"

        gdf1 = gpd.read_parquet(in_pth)
        lpis = gpd.read_file(lps_pth)
        gdf1[gsa_crop_id_col] = pd.to_numeric(gdf1[gsa_crop_id_col], errors='coerce')
        print("crop_code max:", gdf1["PLODINA_ID"].max())
        print("lu_code_max:", lpis["KULTURA"].max())
        gdf1[gsa_crop_id_col] = gdf1[gsa_crop_id_col]
        lpis[lpis_lu_id_col] += 60000

        gdf2 = gdf1.loc[gdf1[gsa_crop_col].isna()].copy()
        gdf2.geometry = gdf2.geometry.representative_point()
        gdf2.to_crs(lpis.crs, inplace=True)
        gdf3 = gpd.overlay(gdf2, lpis[[lpis_lu_id_col, lpis_lu_col, "geometry"]], "intersection")

        gdf1 = pd.merge(gdf1, gdf3[["NKOD_DPB", lpis_lu_id_col, lpis_lu_col]], how="left", on="NKOD_DPB")

        mask = gdf1[gsa_crop_col].isna()
        gdf1.loc[mask, gsa_crop_col] = gdf1.loc[mask, lpis_lu_col]
        gdf1.loc[mask, gsa_crop_id_col] = gdf1.loc[mask, lpis_lu_id_col]

        gdf1.drop(columns=[lpis_lu_id_col, lpis_lu_col], inplace=True)

        gdf1.to_parquet(in_pth)


def list_lpis_lu_names_and_codes():

    lst = []
    for year in range(2015, 2026):
        print(year)

        lps_pth = glob.glob(os.path.join("data", "vector", "IACS", "CZ", "Original", "LPIS", f"DPB_VEREJNY_GUI_{year}*.shp"))[0]

        lpis_lu_col = "KULTURANAZ"
        lpis_lu_id_col =  "KULTURA"

        lpis = gpd.read_file(lps_pth)

        lpis.drop_duplicates(subset=[lpis_lu_col, lpis_lu_id_col], inplace=True)

        df = lpis[[lpis_lu_col, lpis_lu_id_col]]
        lst.append(df)

    out_df = pd.concat(lst)
    out_df.drop_duplicates(inplace=True)

    out_df.to_csv(os.path.join("data", "vector", "IACS", "CZ", "unique_LPIS_LU_codes_and_names.csv"), index=False)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # helper_functions.extract_fields_with_double_field_id(
    #     iacs_pth=os.path.join("data", "vector", "IACS", "CZ", "Original", "DPB_VEREJNY_GUI_2025-08-01_epsg4258.shp"),
    #     id_col="NKOD_DPB",
    #     out_pth=os.path.join("data", "vector", "IACS", "CZ", "Original", "SUB_DPB_VEREJNY_GUI_2025-08-01_epsg4258.shp"))

    # add_crops_clean_data_and_separate_parcels_within_field_blocks(
    #     in_dir=os.path.join("data", "vector", "IACS", "CZ", "Original"),
    #     out_dir=os.path.join("data", "vector", "IACS", "CZ"))

    # add_land_use_from_lpis()

    list_lpis_lu_names_and_codes()

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
