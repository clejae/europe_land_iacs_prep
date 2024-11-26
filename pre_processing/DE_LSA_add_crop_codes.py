# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import fiona
import numpy as np
import pandas as pd
import math
import geopandas as gpd
from osgeo import ogr

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def add_crop_codes(in_pth, in_df_pth, gdf_flik_col, gdf_schlag_col, df_crop_code_col, df_crop_name_col, out_pth):

    print(f"Read {in_pth}")
    root, ext = os.path.splitext(in_pth)
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson', '.geoparquet']:
        gdf = gpd.read_file(in_pth)
    if ext in ['.geoparquet']:
        gdf = gpd.read_parquet(in_pth)

    df = pd.read_excel(in_df_pth)

    gdf["NC_FESTG"] = gdf["NC_FESTG"].astype(str)
    df[df_crop_code_col] = df[df_crop_code_col].astype(str)

    gdf = pd.merge(gdf, df[[df_crop_code_col, df_crop_name_col]], "left", left_on="NC_FESTG",
                   right_on=df_crop_code_col)
    gdf.drop(columns=df_crop_code_col, inplace=True)

    gdf["field_id"] = gdf[gdf_flik_col] + gdf[gdf_schlag_col].astype(str)
    gdf['field_id_distinguished'] = gdf.groupby('field_id').cumcount() + 1

    # Combine the original field_id with the distinguished number
    gdf['unique_field_id'] = gdf['field_id'].astype(str) + '_' + gdf['field_id_distinguished'].astype(str)
    gdf.drop(columns=["field_id", "field_id_distinguished"], inplace=True)

    gdf.to_parquet(out_pth)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    add_crop_codes(
        in_pth=r"Q:\Europe-LAND\data\vector\IACS\DE\LSA\UD_2023_S.shp",
        in_df_pth=r"Q:\Europe-LAND\data\vector\IACS\DE\LSA\Nutzcodes_2023.xlsx",
        gdf_flik_col="FLIK",
        gdf_schlag_col="SCHLAGNR",
        df_crop_code_col="NC",
        df_crop_name_col="Kulturart",
        out_pth=r"Q:\Europe-LAND\data\vector\IACS\DE\LSA\UD_2023_S_prep.geoparquet")

    add_crop_codes(
        in_pth=r"Q:\Europe-LAND\data\vector\IACS\DE\LSA\UD_2024_S.shp",
        in_df_pth=r"Q:\Europe-LAND\data\vector\IACS\DE\LSA\Nutzcode-Liste_2024.xlsx",
        gdf_flik_col="FLIK",
        gdf_schlag_col="SCHLAGNR",
        df_crop_code_col="NC",
        df_crop_name_col="Nds Kurzbezeichnung",
        out_pth=r"Q:\Europe-LAND\data\vector\IACS\DE\LSA\UD_2024_S_prep.geoparquet")

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')