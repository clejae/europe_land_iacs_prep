# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
from osgeo import gdal
import pandas as pd

gdal.SetConfigOption("OGR_GEOMETRY_ACCEPT_UNCLOSED_RING", "NO")

from my_utils import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def count_duplicate_geometries(in_pth):

    gdf = gpd.read_file(in_pth)
    gdf["geom_id"] = gdf.geometry.to_wkb()

    dups = gdf[gdf.duplicated("geom_id", "first")].copy()

    print(f"{len(dups)} geometry duplicates were found for {in_pth}.")


def count_duplicate_geometries_v2(gdf):
    gdf["geom_id"] = gdf.geometry.to_wkb()

    dups = gdf[gdf.duplicated("geom_id", "first")].copy()

    gdf.drop(columns="geom_id", inplace=True)

    print(f"{len(dups)} geometry duplicates were found.")


def extract_geometry_duplicates(in_pth, out_pth):

    gdf = gpd.read_file(in_pth)
    gdf["geom_id"] = gdf.geometry.to_wkb()

    dups = gdf[gdf.duplicated("geom_id", "first")].copy()

    print(f"{len(dups)} geometry duplicates were found for {in_pth}.")

    dups_out = gdf.loc[gdf["geom_id"].isin(list(dups["geom_id"].unique()))].copy()
    dups_out.drop(columns="geom_id", inplace=True)
    dups.to_file(out_pth)


def remove_geometry_duplicates(gdf):

    in_len = len(gdf)
    gdf["geom_id"] = gdf.geometry.to_wkb()

    gdf.drop_duplicates(subset="geom_id", inplace=True)
    out_len = len(gdf)
    gdf.drop(columns="geom_id", inplace=True)
    return gdf


def create_unique_id(gdf, flik_flek="flik_flek", fl_kenng="fl_kenng"):

    gdf["uni_id"] = gdf[flik_flek] + '_' + gdf[fl_kenng].astype(str)

    print("Number Unique IDs:", len(gdf["uni_id"].unique()))
    print("Number Parcels:", len(gdf))

    if len(gdf["uni_id"].unique()) != len(gdf):
        print("Number of Unique IDs not yet unique. Adding cumulative count.")
        gdf['uni_id_distinguished'] = gdf.groupby('uni_id').cumcount() + 1
        gdf.loc[gdf['uni_id_distinguished'].isna(), 'uni_id_distinguished'] = 1
        gdf['uni_id'] = gdf['uni_id'].astype(str) + '_' + gdf['uni_id_distinguished'].astype(str)
        gdf.drop(columns=["uni_id_distinguished"], inplace=True)
        print("Number Unique IDs:", len(gdf["uni_id"].unique()))
        print("Number Parcels:", len(gdf))

    return gdf

def add_landscape_elements_to_crop_vectors(gdf_s, gdf_le, gdf_s_nr_col, gdf_s_flik_col,
                                           le_s_nr_col, le_s_flik_col, gdf_c_col, le_code_col,
                                           gdf_n_col, le_n_col, gdf_area_col, le_area_col):

    print("Adding landscape elements to fields.")

    gdf_le = gdf_le.to_crs(gdf_s.crs)

    gdf_s.loc[gdf_s[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    gdf_s[gdf_s_nr_col] = gdf_s[gdf_s_nr_col].astype(int).astype(str)
    gdf_s.loc[gdf_s[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    gdf_s["field_id"] = gdf_s[gdf_s_flik_col] + '_' + gdf_s[gdf_s_nr_col]

    gdf_le.loc[gdf_le[le_s_nr_col].isna(), le_s_nr_col] = 9999
    gdf_le[le_s_nr_col] = gdf_le[le_s_nr_col].astype(int).astype(str)
    gdf_le.loc[gdf_le[le_s_nr_col] == "9999", le_s_nr_col] = ""
    gdf_le["field_id"] = gdf_le[le_s_flik_col] + '_' + gdf_le[le_s_nr_col]

    num_s = len(gdf_s)
    num_le = len(gdf_le)

    gdf_le.rename(columns={
        le_s_flik_col: gdf_s_flik_col,
        le_s_nr_col: gdf_s_nr_col,
        le_code_col: gdf_c_col,
        le_n_col: gdf_n_col,
        le_area_col: gdf_area_col
    }, inplace=True)

    # Concatenate landscape elements to schlaege
    mand_cols = ["field_id", gdf_s_flik_col, gdf_s_nr_col, gdf_c_col, gdf_n_col, gdf_area_col, "geometry"]

    gdf_out = pd.concat([gdf_s, gdf_le[mand_cols]])

    gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    gdf_out.drop(columns=["field_id_distinguished"], inplace=True)

    print(len(gdf_out), num_s + num_le)

    print("Number Unique IDs:", len(gdf_out["field_id"].unique()))
    print("Number Parcels:", len(gdf_out))

    return gdf_out


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## 1. Check if data contain a unique field id
    ## if not, create one. In the best case it hase some meaning.

    ## 2. Check if data contain duplicate geometries
    ## If yes, then check if the entries are complete duplicates of each other (e.g. by looking at actual polygon area
    ## and by comparing it with the reported land use/crop area).
    ## If they are complete duplicates, then remove them.
    ## If no, then check if the data are field blocks and each entry is actually a field entry

    ## 3. Check if data contain non-geometries.

    ## 4. Check if data also have fields outside their administrative borders

    ## Exploration
    # in_dir = os.path.join("data", "vector", "IACS", "DE", "BWB", "Original")
    # iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)
    #
    # for i, in_pth in enumerate(iacs_files):
    #
    #     year = helper_functions.get_year_from_path(in_pth)
    #     print(year)
    #     print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")
    #
    #     out_pth = os.path.join("data", "vector", "IACS", "DE", "BWB", "Original", f"DUPS-layer_bw_{year}.gpkg")
    #     # extract_geometry_duplicates(in_pth, out_pth)
    #
    #     gdf = gpd.read_file(in_pth)
    #     gdf = helper_functions.drop_non_geometries(gdf)
    #     gdf = remove_geometry_duplicates(gdf)
    #
    #     ## Extract geo-id duplicates
    #     dups = gdf[gdf.duplicated("geo_id", "first")].copy()
    #     print(f"{len(dups)} geo id duplicates were found for {in_pth}.")
    #     dups_out = gdf.loc[gdf["geo_id"].isin(list(dups["geo_id"].unique()))].copy()
    #     dups_out.drop(columns="geo_id", inplace=True)
    #     dups.to_file(os.path.join("data", "vector", "IACS", "DE", "BWB", "Original", f"DUPS_geo_id-layer_bw_{year}.gpkg"))
    #
    #     print("Number of fields:", len(gdf), "number of geo ids:", len(gdf["geo_id"].unique()))
    #     print("NAs in geo_id:", len(gdf.loc[gdf["geo_id"].isna()]))

    ## Actual pre-processing
    in_dir = os.path.join("data", "vector", "IACS", "DE", "BWB", "Original")
    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

    for i, in_pth in enumerate(iacs_files):

        year = helper_functions.get_year_from_path(in_pth)
        print(year)
        print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")

        out_pth = os.path.join("data", "vector", "IACS", "DE", "BWB", "Original", f"DUPS-layer_bw_{year}.gpkg")
        # extract_geometry_duplicates(in_pth, out_pth)

        gdf = gpd.read_file(in_pth)
        gdf = helper_functions.drop_non_geometries(gdf)
        gdf = remove_geometry_duplicates(gdf)
        gdf.drop_duplicates(subset="geo_id", inplace=True)

        if year in ["2023", "2024"]:
            fakt_col = "fakt_massnahme"
        if year in ["2022"]:
            fakt_col = "FAKT-Massnahme"

        gdf.loc[gdf[fakt_col].isna(), fakt_col] = ""
        gdf["oeko"] = 0
        gdf.loc[gdf[fakt_col].str.contains("D2"), "oeko"] = 1

        out_pth = os.path.join("data", "vector", "IACS", "DE", "BWB", f"layer_bw_{year}.geoparquet")
        gdf.to_parquet(out_pth)

        print("Number of fields:", len(gdf), "number of geo ids:", len(gdf["geo_id"].unique()))
        print("NAs in geo_id:", len(gdf.loc[gdf["geo_id"].isna()]))

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')