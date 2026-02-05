# Author:
# github repository:
# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
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
def combine_provinces(in_dir, region, year, out_dir):

    print("Combine provinces of ", region)

    # ## list all zip files
    # files_grabbed = glob.glob(os.path.join(in_dir, "*.zip") # rf"{}/*.zip")
    #
    # if len(files_grabbed) < 1:
    #     return
    #
    # print("Unzipping file")
    # for path_to_zip_file in files_grabbed:
    #     with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
    #         zip_ref.extractall(in_dir)

    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)
    print(f"There are {len(iacs_files)} files for {region}")
    if len(iacs_files) < 1:
        return

    print("Reading data")
    file_list = [gpd.read_file(pth) for i, pth in enumerate(iacs_files)]

    print("Combining.")
    out_file = pd.concat(file_list)
    helper_functions.create_folder(out_dir)
    out_pth = os.path.join(out_dir, f"IACS_{region}_{year}.gpkg") #f"{}\
    print("Writing out to", out_pth)
    if "fid" in out_file.columns:
        out_file["fid_old"] = out_file["fid"]
    out_file["fid"] = range(1, len(out_file)+1)
    out_file["fid"] = out_file["fid"].astype(int)

    out_file.to_file(out_pth, driver="GPKG")

def separate_fields(gdf1, gdf_out_pth, csv_out_pth, reported_area_col=None, field_id_col=None):
    gdf1 = helper_functions.drop_non_geometries(gdf1)

    # Create a unique ID based on geometries
    gdf1["geom_id"] = gdf1.geometry.to_wkb()

    # Create a unique ID to link geometries with additional crops
    uni_ids = {gid: i for i, gid in enumerate(gdf1["geom_id"].unique())}
    gdf1["uni_id"] = gdf1["geom_id"].map(uni_ids)

    # Decide how to pick the "kept" row per geometry
    use_area = (
            reported_area_col is not None
            and reported_area_col in gdf1.columns
            and gdf1[reported_area_col].notna().any()
    )

    if use_area:
        # Keep the largest reported sub-field per geometry
        idx_keep = (
            gdf1
            .dropna(subset=[reported_area_col])
            .groupby("geom_id")[reported_area_col]
            .idxmax()
        )
    else:
        # Fallback: keep the first record per geometry
        idx_keep = gdf1.groupby("geom_id").head(1).index

    gdf_unique = gdf1.loc[idx_keep].drop(columns="geom_id")
    gdf_unique.sort_values(by="uni_id", inplace=True)

    if not field_id_col:
        gdf_unique["uni_id_new"] = helper_functions.create_unique_field_ids(gdf_unique.geometry)
        id_dict = dict(zip(gdf_unique["uni_id"], gdf_unique["uni_id_new"]))
        gdf_unique["uni_id"] = gdf_unique["uni_id"].map(id_dict)
        gdf_unique.drop(columns=["uni_id_new"], inplace=True)
        field_id_col = "uni_id"
    else:
        id_dict = dict(zip(gdf_unique["uni_id"], gdf_unique[field_id_col]))

    mask_kept = gdf1.index.isin(idx_keep)
    df_others = gdf1.loc[~mask_kept].drop(columns=["geom_id", "geometry"]).copy()
    df_others[field_id_col] = df_others["uni_id"].map(id_dict)

    gdf_unique["geometry"] = gdf_unique["geometry"].buffer(0)
    gdf_unique["geometry"] = gdf_unique.normalize()

    print("Number Unique IDs:", gdf_unique["uni_id"].nunique())
    print("Number Parcels:", len(gdf_unique))

    if gdf_unique["uni_id"].nunique() == len(gdf_unique):
        gdf_unique.to_parquet(gdf_out_pth)
        df_others.to_csv(csv_out_pth, index=False)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # !!! This does not work anymore. The files were moved to IT/XXX/Original. The file paths need to be adapted.
    # run_dict = {
    #     "IT_temp/Toscana": {"file_encoding": "utf-8", "years": range(2016, 2017), "region_abbreviation": "TOS"},
        # "IT_temp/Emilia-Romagna": {"file_encoding": "utf-8", "years": range(2023, 2025), "region_abbreviation": "EMR"},
    # }

    # for country_code in run_dict:
    #
    #     years = run_dict[country_code]["years"]
    #     region_abbreviation = run_dict[country_code]["region_abbreviation"]
    #
    #     for year in years:
    #
    #         in_dir = os.path.join("data", "vector", "IACS", country_code, year) #rf"data\vector\IACS\{country_code}\{year}"
    #         out_dir = os.path.join("data", "vector", "IACS", "IT", region_abbreviation) #fr"data\vector\IACS\IT\{region_abbreviation}"
    #
    #         combine_provinces(
    #             in_dir=in_dir,
    #             region=region_abbreviation,
    #             year=year,
    #             out_dir=out_dir)

    ##  Toscana: Change dtype of crop_code to str with leading zeros of to match 3 digits
    # lst = glob.glob(os.path.join("data", "vector", "IACS", "IT", "TOS", "IACS_TOS_*.gpkg"))
    # crop_code_col = "IDSpecie"
    #
    # for pth in lst:
    #     gdf = gpd.read_file(pth)
    #     gdf[crop_code_col + "_"] = (
    #         gdf[crop_code_col]
    #         .astype("Int64")
    #         .astype("string")
    #         .str.zfill(3)
    #     )
    #
    #     gdf.to_file(pth)

    ## Exploration
    # in_dir = os.path.join("data", "vector", "IACS", "IT", "EMR", "Original")
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
    #     out_pth = os.path.join("data", "vector", "IACS", "IT", "EMR", "Original" ,f"DUPS-layer_it_emr_{year}.gpkg")
    #     helper_functions.extract_geometry_duplicates(in_pth, out_pth)

    # in_dir = os.path.join("data", "vector", "IACS", "IT", "TOS", "Original")
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
    #     out_pth = os.path.join("data", "vector", "IACS", "IT", "TOS", "Original", f"DUPS-layer_it_tos_{year}.gpkg")
    #     helper_functions.extract_geometry_duplicates(in_pth, out_pth)

    ## Actual pre-processing
    # in_dir = os.path.join("data", "vector", "IACS", "IT", "EMR", "Original")
    # iacs_files = glob.glob(os.path.join(in_dir, "IACS*.gpkg"))
    #
    # for i, in_pth in enumerate(iacs_files):
    #     year = helper_functions.get_year_from_path(in_pth)
    #     print(year)
    #
    #     out_dir = os.path.join("data", "vector", "IACS", "IT", "EMR", "pre_processed_data")
    #     helper_functions.create_folder(out_dir)
    #
    #     gdf_out_pth = os.path.join(out_dir, f"GSA-{year}.geoparquet")
    #     csv_out_pth = os.path.join(out_dir, f"GSA-{year}.csv")
    #
    #     gdf1 = gpd.read_file(in_pth)
    #     gdf1["new_id"] = helper_functions.make_id_unique_by_adding_cumcount(gdf1["ID_APPEZ"])
    #
    #     separate_fields(gdf1=gdf1, reported_area_col=None, gdf_out_pth=gdf_out_pth, csv_out_pth=csv_out_pth,
    #                     field_id_col="new_id")

    ## Toscana
    in_dir = os.path.join("data", "vector", "IACS", "IT", "TOS", "Original")
    iacs_files = glob.glob(os.path.join(in_dir, "IACS*.gpkg"))

    for i, in_pth in enumerate(iacs_files):
        year = helper_functions.get_year_from_path(in_pth)
        print(year)

        out_dir = os.path.join("data", "vector", "IACS", "IT", "TOS", "pre_processed_data")
        helper_functions.create_folder(out_dir)

        gdf_out_pth = os.path.join(out_dir, f"GSA-{year}.geoparquet")
        csv_out_pth = os.path.join(out_dir, f"GSA-{year}.csv")

        gdf1 = gpd.read_file(in_pth)

        separate_fields(gdf1=gdf1, reported_area_col=None, gdf_out_pth=gdf_out_pth, csv_out_pth=csv_out_pth,
                        field_id_col="objectid")

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
