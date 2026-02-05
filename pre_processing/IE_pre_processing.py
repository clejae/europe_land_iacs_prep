# Author:
# github repository:
# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
import pandas as pd
import shutil
import glob

from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)


# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def combine_provinces(in_dir, year, out_dir):

    print("Combine file of ", year)

    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)
    iacs_files = [file for file in iacs_files if "PARCEL" in file]
    print(f"There are {len(iacs_files)} files for {year}")
    if len(iacs_files) < 1:
        return
    elif len(iacs_files) < 2:
        src = iacs_files[0]
        ext = os.path.basename(src).split(".")[1]
        dst = os.path.join(out_dir, f"IACS_IE_{year}.{ext}") #f"{out_dir}\IACS_IE_{year}.{ext}"
        helper_functions.create_folder(out_dir)
        shutil.copyfile(src, dst)
        return

    print("Reading data")
    file_list = [gpd.read_file(pth) for i, pth in enumerate(iacs_files)]

    print("Combining.")
    out_file = pd.concat(file_list)
    helper_functions.create_folder(out_dir)
    out_pth = os.path.join(out_dir, f"IACS_IE_{year}.gpkg") #f"{out_dir}\IACS_IE_{year}.gpkg"
    print("Writing out to", out_pth)

    out_file.to_file(out_pth, driver="GPKG")

def separate_fields(in_pth, reported_area_col, gdf_out_pth, csv_out_pth):
    gdf1 = gpd.read_file(in_pth)

    gdf1 = helper_functions.drop_non_geometries(gdf1)

    ## Create a unique ID based on geometries
    gdf1["geom_id"] = gdf1.geometry.to_wkb()

    ## Create a unique ID that later can be used to link geometries with additional crops
    uni_ids = {gid: i for i, gid in enumerate(gdf1["geom_id"].unique())}
    gdf1["uni_id"] = gdf1["geom_id"].map(uni_ids)
    gdf1.dropna(subset=reported_area_col, inplace=True)

    ## Now we need to separate the subfields for which a larger subfield exists in the field blocks
    idx_max = (
        gdf1
        .groupby("geom_id")[reported_area_col]
        .idxmax()
    )

    gdf_unique = gdf1.loc[idx_max].drop(columns="geom_id")
    gdf_unique.sort_values(by="uni_id", inplace=True)

    gdf_unique["uni_id_new"] = helper_functions.create_unique_field_ids(gdf_unique.geometry)

    id_dict = dict(zip(gdf_unique["uni_id"], gdf_unique["uni_id_new"]))
    gdf_unique["uni_id"] = gdf_unique["uni_id"].map(id_dict)
    gdf_unique.drop(columns=["uni_id_new"], inplace=True)

    mask_kept = gdf1.index.isin(idx_max)
    df_others = gdf1.loc[~mask_kept].drop(columns=["geom_id", "geometry"]).copy()
    df_others.sort_values(by="uni_id", inplace=True)
    df_others["uni_id"] = df_others["uni_id"].map(id_dict)

    gdf_unique['geometry'] = gdf_unique['geometry'].buffer(0)
    gdf_unique['geometry'] = gdf_unique.normalize()

    # gdf_others.drop(columns="uni_id", inplace=True)
    # gdf_unique.drop(columns="uni_id", inplace=True)

    print("Number Unique IDs:", len(gdf_unique["uni_id"].unique()))
    print("Number Parcels:", len(gdf_unique))
    if len(gdf_unique["uni_id"].unique()) == len(gdf_unique):
        gdf_unique.to_parquet(gdf_out_pth)
        df_others.to_csv(csv_out_pth, index=False)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # for year in range(2017, 2024):
    #
    #     in_dir = rf"data\vector\IACS\IE_temp\{year}"
    #     out_dir = fr"data\vector\IACS\IE"
    #
    #     combine_provinces(
    #         in_dir=in_dir,
    #         year=year,
    #         out_dir=out_dir)

    ## Exploration
    # in_dir = os.path.join("data", "vector", "IACS", "IE")
    # iacs_files = glob.glob(os.path.join(in_dir, "*.gpkg"))
    #
    # for i, in_pth in enumerate(iacs_files[:1]):
    #
    #     year = helper_functions.get_year_from_path(in_pth)
    #     print(year)
    #
    #     print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")
    #     # count_duplicate_geometries(in_pth)
    #
    #     out_pth = os.path.join("data", "vector", "IACS", "IE", f"DUPS-layer_ie_{year}.gpkg")
    #     helper_functions.extract_geometry_duplicates(in_pth, out_pth)


    ## Actual pre-processing
    in_dir = os.path.join("data", "vector", "IACS", "IE", "Original")
    iacs_files = glob.glob(os.path.join(in_dir, "IACS*.gpkg"))

    for i, in_pth in enumerate(iacs_files):
        year = helper_functions.get_year_from_path(in_pth)
        print(year)
        if year in ["2017"]:
            continue

        gdf_out_pth = os.path.join("data", "vector", "IACS", "IE", "pre_processed_data", f"GSA-{year}.geoparquet")
        csv_out_pth = os.path.join("data", "vector", "IACS", "IE", "pre_processed_data", f"GSA-{year}.csv")

        separate_fields(in_pth, reported_area_col="CLAIM_AREA", gdf_out_pth=gdf_out_pth, csv_out_pth=csv_out_pth)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
