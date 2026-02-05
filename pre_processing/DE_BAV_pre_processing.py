# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import pandas as pd
import geopandas as gpd
from osgeo import gdal
import sys
script_dir = dirname(abspath(__file__))
project_root = dirname(script_dir)
sys.path.append(project_root)

gdal.SetConfigOption("OGR_GEOMETRY_ACCEPT_UNCLOSED_RING", "NO")

from my_utils import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
# WD =
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    in_farmid_pth = os.path.join("data", "vector", "IACS", "DE", "BAV", "Original", "fid_pseudobetnr.csv")
    df_farmid = pd.read_csv(in_farmid_pth)

    for year in range(2021, 2026):

        in_gdf_pth = os.path.join("data", "vector", "IACS", "DE", "BAV", "Original", "1_Feldstücke", f"Feld_{year}.parquet")
        in_cr_pth = os.path.join("data", "vector", "IACS", "DE", "BAV", "Original", "2_Nutzung", f"Nutzung_{year}.csv")

        out_dir = os.path.join("data", "vector", "IACS", "DE", "BAV")
        id_col = "fid"

        iacs = gpd.read_parquet(in_gdf_pth)
        df_cr = pd.read_csv(in_cr_pth)

        in_len = len(iacs)
        iacs.sort_values(by="fid", inplace=True)
        df_cr.sort_values(by="fid", inplace=True)

        ## Merge fields with farm IDs
        df_farmid_year = df_farmid.loc[df_farmid["jahr"] == year].copy()
        df_farmid_year.drop(columns="jahr", inplace=True)
        df_farmid_year.drop_duplicates(subset="fid", inplace=True)
        iacs = pd.merge(iacs, df_farmid_year, "left", "fid")

        nas = len(iacs.loc[iacs["betnr_pseudo"].isna()])
        len_fids = len(iacs["fid"].unique())
        print("NAs in betnr_pseudo:", nas, "Number if unique FIDs:", len_fids)

        ## First drop non-geometries
        iacs = helper_functions.drop_non_geometries(iacs)

        iacs_cols = ["fid", "betnr_pseudo", "flaeche", "flaechen_anteil", "SHAPE_Area", "geometry"]
        df_cr_cols = ["fid", "schlag_id", "nutz_code", "nutz_besch", "flaeche_nutz"]
        merge_col = "fid"

        iacs2 = pd.merge(iacs[iacs_cols], df_cr[df_cr_cols], "left", merge_col)
        out_len = len(iacs2)
        iacs2.sort_values(by=[merge_col, "flaeche_nutz"], inplace=True, ascending=False)

        ## Separate the main crops from the remaining crops in each field block
        ## Remaining crops will be saved as csv file
        iacs2["temp_id"] = range(len(iacs2))
        gdf_out = iacs2.drop_duplicates(subset=iacs_cols, keep="first").copy()
        df_out = iacs2.loc[~iacs2["temp_id"].isin(gdf_out["temp_id"])].copy()

        print("Check if vector and accompanying file add up:", len(df_out) + len(gdf_out), out_len)

        ## Drop entries that have duplicate geometries
        gdf_out["geom_id"] = gdf_out.geometry.to_wkb()
        dups = gdf_out[gdf_out.duplicated("geom_id", "first")].copy()
        if len(dups) > 0:
            gdf_out.drop_duplicates(subset="geom_id", inplace=True)
            dups.to_file(os.path.join("data", "vector", "IACS", "DE", "BAV", "Original", "1_Feldstücke", f"DUPS_Feld_{year}.shp"))
        print(f"{len(dups)} geometry duplicates were found for {in_gdf_pth}.")

        ## Make FID unique
        # gdf_out["unique_id"] = helper_functions.make_id_unique_by_adding_cumcount(iacs[id_col])
        ## Run this to get a feeling for the duplicate IDs
        id_counts = gdf_out[id_col].value_counts()
        duplicated_ids = id_counts[id_counts > 1].index
        gdf_out_sub = gdf_out[gdf_out[id_col].isin(duplicated_ids)].copy()
        print("Number of fields with non-unique IDs:", len(gdf_out_sub))
        # gdf_out_sub.to_parquet(os.path.join("data", "vector", "IACS", "DE", "BAV", "Original", "1_Feldstücke", f"DUPLICATE_IDS_Feld_{year}.parquet"))
        mask = gdf_out[id_col].isin(duplicated_ids)

        gdf_out.loc[mask, id_col] = (
            gdf_out.loc[mask]
            .groupby(id_col)
            .cumcount()
            .add(1)
            .astype(str)
            .radd(gdf_out.loc[mask, id_col] + "_")
        )

        ## Drop non useful columns
        gdf_out.drop(columns=["temp_id", "geom_id"], inplace=True)
        df_out.drop(columns=["temp_id", "geometry"], inplace=True)

        ## Check for non-matched FLIKS in crop csv
        df_cr_nm = df_cr.loc[~df_cr["fid"].isin(gdf_out["fid"])].copy()
        print("Number of entries in Nutzung-df that could not be matchd with Feld vector:", len(df_cr_nm))

        gdf_out["SHAPE_Area"] = round(gdf_out["SHAPE_Area"]/10000, 4)
        gdf_out.loc[gdf_out["flaeche"] == 0, "flaeche"] = gdf_out.loc[gdf_out["flaeche"] == 0, "SHAPE_Area"]

        gdf_out.to_parquet(out_dir + fr"\GSA_BAV_{year}.geoparquet")
        df_out.to_csv(out_dir + fr'\GSA_BAV_{year}.csv', index=False)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')