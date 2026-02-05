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


def create_unique_id(gdf):

    gdf["uni_id"] = gdf["flik_flek"] + '_' + gdf["fl_kenng"].astype(str)

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


def separate_subparcels_and_add_unique_id(gdf):
    ## Create a unique ID based on geometries
    gdf["geom_id"] = gdf.geometry.to_wkb()

    ## Create a simple unique ID that later can be used to link geometries with additional crops
    ## Use the running count for all unique geometries. Later on, each unique geometry should get a unique ID
    uni_ids = {gid: i for i, gid in enumerate(gdf["geom_id"].unique())}
    gdf["uni_id"] = gdf["geom_id"].map(uni_ids)

    ## Remove geometries that do not have a reported area
    gdf = gdf.loc[gdf["fl_ha_meldg"] > 0].copy()

    ## Now we need to separate the subfields for which a larger subfield exists in the field blocks
    ## Extract the ID for which the reported area is largest
    idx_max = (
        gdf
        .groupby("geom_id")["fl_ha_meldg"]
        .idxmax()
    )

    ## Get the unique fields based on the max ID
    gdf_unique = gdf.loc[idx_max].drop(columns="geom_id")
    gdf_unique.sort_values(by="uni_id", inplace=True)

    ## Create a new unique ID by comgning FLIK + Schlagnr + cumulative count
    gdf_unique["uni_id_new"] = gdf_unique["flik_flek"] + '_' + gdf_unique["fl_kenng"].astype(str)
    gdf_unique['uni_id_distinguished'] = gdf_unique.groupby('uni_id_new').cumcount() + 1
    gdf_unique['uni_id_new'] = gdf_unique['uni_id_new'].astype(str) + '_' + gdf_unique['uni_id_distinguished'].astype(str)
    gdf_unique.drop(columns=["uni_id_distinguished"], inplace=True)

    ## Create a dictionary that links the simple unique with the new ID
    id_dict = dict(zip(gdf_unique["uni_id"], gdf_unique["uni_id_new"]))

    # ## This is probably not need, but just to make sure that the vector and the table have the same field IDs
    # gdf_unique["uni_id_new"] = gdf_unique["uni_id"].map(id_dict)

    ## Extract all entries that have not the largest area in a field block
    mask_kept = gdf.index.isin(idx_max)
    gdf_others = gdf.loc[~mask_kept].drop(columns=["geom_id", "geometry"]).copy()
    gdf_others.sort_values(by="uni_id", inplace=True)
    gdf_others["uni_id_new"] = gdf_others["uni_id"].map(id_dict)

    gdf_others.drop(columns=["uni_id"], inplace=True)
    gdf_unique.drop(columns=["uni_id"], inplace=True)

    print("Number Unique IDs:", len(gdf_unique["uni_id_new"].unique()))
    print("Number Parcels:", len(gdf_unique))
    print("Number Additional Crops:", len(gdf_others))
    if len(gdf_unique["uni_id_new"].unique()) != len(gdf_unique):
        print("Warning: ID is not unique.")

    return gdf_unique, gdf_others


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
    # for year in range(2010, 2024):
    #     print(year)
    #     in_pth = os.path.join("data", "vector", "IACS", "DE", "RLP", "Original", f"layer_rp_{year}.gpkg")
    #     count_duplicate_geometries(in_pth)

    # for year in list(range(2012, 2016)) + list(range(2020, 2025)):
    #     print(year)
    #     in_pth = os.path.join("data", "vector", "IACS", "DE", "SAA", "Original", f"layer_sl_{year}.gpkg")
    #     out_pth = os.path.join("data", "vector", "IACS", "DE", "SAA", "Original", f"DUPS-layer_sl_{year}.gpkg")
    #     extract_geometry_duplicates(in_pth, out_pth)

    # For the years 2017-2019 we use the Referenzschlaege from the ministry
    # They use the Antragsschlaege from the previous year as reference for the current year. Thus the Referenzschlaege
    # 2018 include the crop info from 2017
    # for year in list(range(2017, 2020)):
    #     print(year)
    #     in_pth = os.path.join("data", "vector", "IACS", "DE", "SAA", "Original", f"REFS_{year+1}.shp")
    #     out_pth = os.path.join("data", "vector", "IACS", "DE", "SAA", "Original", f"DUPS-REFS_{year+1}.gpkg")
    #     extract_geometry_duplicates(in_pth, out_pth)

    ## Actual pre-processing
    # for year in range(2010, 2019):
    #     print(year)
    #     in_pth = os.path.join("data", "vector", "IACS", "DE", "RLP", "Original", f"layer_rp_{year}.gpkg")
    #     out_folder = os.path.join("data", "vector", "IACS", "DE", "RLP")
    #
    #     ## Open files
    #     gdf = gpd.read_file(in_pth)
    #
    #     ## Remove NAs from FLIK and Schlagnr to be able to create unique IDs
    #     gdf.loc[gdf["flik_flek"].isna(), "flik_flek"] = "DERPLI0"
    #     gdf.loc[gdf["fl_kenng"].isna(), "fl_kenng"] = "0"
    #
    #     ## Separate Subparcels of field blocks
    #     gdf_unique, gdf_others = separate_subparcels_and_add_unique_id(gdf)
    #
    #     gdf_unique = helper_functions.drop_non_geometries(gdf_unique)
    #
    #     gdf_unique.to_parquet(os.path.join(out_folder, f"layer_rp_{year}.geoparquet"))
    #     gdf_others.to_csv(os.path.join(out_folder, f"layer_rp_{year}.csv"), index=False)

    for year in list(range(2012, 2025)) :
        print(year)
        if year in [2017, 2018, 2019]:
            # For the years 2017-2019 we use the Referenzschlaege from the ministry
            # They use the Antragsschlaege from the previous year as reference for the current year. Thus the Referenzschlaege
            # 2018 include the crop info from 2017
            in_pth = os.path.join("data", "vector", "IACS", "DE", "SAA", "Original", f"REFS_{year+1}.shp")
        else:
            in_pth = os.path.join("data", "vector", "IACS", "DE", "SAA", "Original", f"layer_sl_{year}.gpkg")
        out_pth = os.path.join("data", "vector", "IACS", "DE", "SAA", f"layer_sl_{year}.geoparquet")
        gdf = gpd.read_file(in_pth)

        if year in [2014]:
            gdf.loc[gdf["fl_kenng"].isna(), "fl_kenng"] = "0"
            gdf["fl_kenng"] = gdf["fl_kenng"].astype(float).astype(int).astype(str)

        if year in [2017, 2018, 2019]:
            gdf.rename(columns={"FLIK": "flik_flek"}, inplace=True)
            gdf["fl_kenng"] = "0"

        gdf.loc[gdf["flik_flek"].isna(), "flik_flek"] = "DESLLI0"

        gdf = remove_geometry_duplicates(gdf)
        gdf = helper_functions.drop_non_geometries(gdf)

        count_duplicate_geometries_v2(gdf=gdf)
        gdf = create_unique_id(gdf)

        gdf.to_parquet(out_pth)


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')