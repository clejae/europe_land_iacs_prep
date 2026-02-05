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
import glob

gdal.SetConfigOption("OGR_GEOMETRY_ACCEPT_UNCLOSED_RING", "NO")

from my_utils import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
WD = r"Q:\Europe-LAND"
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


def remove_fields_outside_official_borders(gdf, adm):
    in_len = len(gdf)
    gdf["temp_id"] = range(len(gdf))
    gdf_c = gdf.copy()
    gdf_c["geometry"] = gdf_c["geometry"].centroid
    adm = adm.to_crs(gdf.crs)
    intersection = gpd.sjoin(gdf_c, adm)
    gdf_out = gdf.loc[gdf["temp_id"].isin(intersection["temp_id"])].copy()
    out_len = len(gdf_out)
    gdf_out.drop(columns="temp_id", inplace=True)

    print("Number of all fields:", in_len, "Number of fields inside borders:", out_len)

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
    # in_dir = os.path.join("data", "vector", "IACS", "DE", "THU", "Original", "Shapes")
    # iacs_files = glob.glob(os.path.join(in_dir, "*BDF_HN*.shp")) + glob.glob(os.path.join(in_dir, "layer_th*.gpkg"))
    # # iacs_files2 = glob.glob(os.path.join(in_dir, "layer_th*.gpkg"))
    #
    # for i, in_pth in enumerate(iacs_files):
    #
    #     year = helper_functions.get_year_from_path(in_pth)
    #     print(year)
    #     if int(year) < 2013:
    #         continue
    #
    #     print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")
    #     # count_duplicate_geometries(in_pth)
    #     out_pth = os.path.join("data", "vector", "IACS", "DE", "THU", "Original", "Shapes", f"DUPS-layer_th_{year}.gpkg")
    #     extract_geometry_duplicates(in_pth, out_pth)
    #
    #     gdf = gpd.read_file(in_pth)
    #     gdf = helper_functions.drop_non_geometries(gdf)
    #     if int(year) < 2020:
    #         flik_col = "BDF_FBI"
    #         fl_kenng = "BDF_TFNR"
    #         gdf["uni_id"] = gdf[flik_col] + gdf[fl_kenng].astype(int).astype(str)
    #     else:
    #         flik_col = "flik_flek"
    #         fl_kenng = "fl_kenng"
    #         gdf["uni_id"] = gdf[flik_col] + gdf[fl_kenng]
    #
    #     print(len(gdf), len(gdf["uni_id"].unique()))
    #
    #     print("NAs in FLIK:", len(gdf.loc[gdf[flik_col].isna()]))

    ## Actual pre-processing
    in_dir = os.path.join("data", "vector", "IACS", "DE", "THU", "Original", "Shapes")
    iacs_files = glob.glob(os.path.join(in_dir, "*BDF_HN*.shp")) + glob.glob(os.path.join(in_dir, "layer_th*.gpkg"))

    adm = gpd.read_file(os.path.join("data", "vector", "administrative", "THU.gpkg"))

    for i, in_pth in enumerate(iacs_files):

        year = helper_functions.get_year_from_path(in_pth)
        print(year)
        if year not in ["2013", "2014"]:
            continue

        print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")

        if year in ["2013", "2014", "2015", "2016", "2017", "2018", "2019"]:
            flik_col = "BDF_FBI"
            fl_kenng = "BDF_TFNR"
            farm_id_col = "BDF_PI"
        elif year in ["2021", "2022", "2023"]:
            flik_col = "flik_flek"
            fl_kenng = "fl_kenng"
            farm_id_col = "betr_id"

        in_org_pth = os.path.join("data", "vector", "IACS", "DE", "THU", "Original", "Tables", f"Öko_{year}.txt")
        out_folder = os.path.join("data", "vector", "IACS", "DE", "THU")

        ## Open files
        gdf = gpd.read_file(in_pth)
        gdf[farm_id_col] = gdf[farm_id_col].astype(str)

        if year in ["2015", "2016", "2017", "2018", "2019"]:
            df_org = pd.read_csv(in_org_pth, sep=";")
            # df_org = df_org.loc[df_org["Öko_ges_b"] == "J"].copy()
            df_org["PI"] = df_org["PI"].astype(str)
            gdf = pd.merge(gdf, df_org, "left", left_on=farm_id_col, right_on="PI")
            gdf["oeko"] = 0
            gdf.loc[gdf["Öko_ges_b"] == "J", "oeko"] = 1

        ## Remove NAs from FLIK and Schlagnr to be able to create unique IDs
        gdf.loc[gdf[flik_col].isna(), flik_col] = "DETHLI0"
        gdf.loc[gdf[fl_kenng].isna(), fl_kenng] = "0"

        gdf = helper_functions.drop_non_geometries(gdf)
        gdf = remove_geometry_duplicates(gdf)

        ## Calculate area
        gdf["area_ha"] = round(gdf.geometry.area / 10000, 3)

        gdf = create_unique_id(gdf, flik_flek=flik_col, fl_kenng=fl_kenng)

        if year in ["2013", "2014"]:
            gdf = remove_fields_outside_official_borders(gdf, adm)

        gdf.to_parquet(os.path.join(out_folder, f"layer_th_{year}.geoparquet"))


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')