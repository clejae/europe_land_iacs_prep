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

    dups.drop(columns="geom_id", inplace=True)
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
        gdf['uni_id'] = gdf['uni_id'].astype(str) + '_' + gdf['uni_id_distinguished'].astype(str)
        gdf.drop(columns=["uni_id_distinguished"], inplace=True)
        print("Number Unique IDs:", len(gdf["uni_id"].unique()))
        print("Number Parcels:", len(gdf))

    return gdf


def separate_subparcels_and_add_unique_id(gdf):
    ## Create a unique ID based on geometries
    gdf["geom_id"] = gdf.geometry.to_wkb()

    ## Create a unique ID that later can be used to link geometries with additional crops
    uni_ids = {gid: i for i, gid in enumerate(gdf["geom_id"].unique())}
    gdf["uni_id"] = gdf["geom_id"].map(uni_ids)

    ## Now we need to separate the subfields for which a larger subfield exists in the field blocks
    idx_max = (
        gdf
        .groupby("geom_id")["fl_ha_meldg"]
        .idxmax()
    )

    gdf_unique = gdf.loc[idx_max].drop(columns="geom_id")
    gdf_unique.sort_values(by="uni_id", inplace=True)

    gdf_unique["uni_id_new"] = gdf_unique["flik_flek"] + '_' + gdf_unique["fl_kenng"].astype(str)
    gdf_unique['uni_id_distinguished'] = gdf_unique.groupby('uni_id_new').cumcount() + 1
    gdf_unique['uni_id_new'] = gdf_unique['uni_id_new'].astype(str) + '_' + gdf_unique['uni_id_distinguished'].astype(str)
    gdf_unique.drop(columns=["uni_id_distinguished"], inplace=True)

    id_dict = dict(zip(gdf_unique["uni_id"], gdf_unique["uni_id_new"]))
    gdf_unique["uni_id_new"] = gdf_unique["uni_id"].map(id_dict)
    # gdf_unique.drop(columns=["uni_id_new"], inplace=True)

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

def separate_file_by_years(in_pth, year_col, out_folder):

    print(f"Read {in_pth}")
    gdf = gpd.read_file(in_pth)

    uni_years = gdf[year_col].unique()

    for year in uni_years:
        print(year)
        # if year == 2019:
        #     continue
        gdf_sub = gdf.loc[gdf[year_col] == year].copy()
        out_pth = os.path.join(out_folder, f"{os.path.basename(in_pth).split('.')[0]}_{year}.{os.path.basename(in_pth).split('.')[1]}")
        gdf_sub.to_file(out_pth)


def add_landscape_elements_to_crop_vectors(gdf_s, gdf_le, gdf_s_nr_col, gdf_s_flik_col,
                                           le_s_nr_col, le_s_flik_col, gdf_c_col, le_code_col,
                                           gdf_n_col, le_n_col, gdf_area_col, le_area_col):
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
    # for year in range(2010, 2024):
    #     print(year)
    #     in_pth = os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", f"layer_nw_{year}.gpkg")
    #     count_duplicate_geometries(in_pth)

    # for year in [2016, 2017, 2023]:
    #     print(year)
    #     in_pth = os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", f"layer_nw_{year}.gpkg")
    #     out_pth = os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", f"DUPS-layer_nw_{year}.gpkg")
    #     extract_geometry_duplicates(in_pth, out_pth)

    adm = gpd.read_file(os.path.join("data", "vector", "administrative", "NRW.gpkg"))

    ## Actual pre-processing
    # for year in range(2010, 2016):
    #     print(year)
    #     in_pth = os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", f"layer_nw_{year}.gpkg")
    #     out_folder = os.path.join("data", "vector", "IACS", "DE", "NRW")
    #
    #     ## Open files
    #     gdf = gpd.read_file(in_pth)
    #
    #     ## Removing all entries that fall outside of NRW is not necessary, as only fields inside NRW are in the data!)
    #
    #     ## Separate Subparcels of field blocks
    #     gdf_unique, gdf_others = separate_subparcels_and_add_unique_id(gdf)
    #
    #     gdf_unique = helper_functions.drop_non_geometries(gdf_unique)
    #
    #     gdf_unique.to_parquet(os.path.join(out_folder, f"layer_nw_{year}.geoparquet"))
    #     gdf_others.to_csv(os.path.join(out_folder, f"layer_nw_{year}.csv"), index=False)

    for year in range(2020, 2022):
    # for year in range(2017, 2024):
        print(year)
        in_pth = os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", f"layer_nw_{year}.gpkg")
        out_pth = os.path.join("data", "vector", "IACS", "DE", "NRW", f"layer_nw_{year}.geoparquet")
        gdf = gpd.read_file(in_pth)
        # gdf = helper_functions.drop_non_geometries(gdf)
        # count_duplicate_geometries_v2(gdf=gdf)

        ## Remove all entries that fall outside of NRW
        gdf = remove_geometry_duplicates(gdf)
        gdf = helper_functions.drop_non_geometries(gdf)

        gdf = remove_fields_outside_official_borders(gdf, adm)
        count_duplicate_geometries_v2(gdf=gdf)
        gdf = create_unique_id(gdf)

        gdf.to_parquet(out_pth)

    ## Do this for the publicly available data:
    # separate_file_by_years(
    #     in_pth=os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", "public", "Teilschlaege", "V_OD_LWK_TSCHLAG_HIST.shp"),
    #     year_col="WJ",
    #     out_folder=os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", "public", "Teilschlaege"))

    # separate_file_by_years(
    #     in_pth=os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", "public", "Landschaftselemente", "V_OD_LWK_LAND_ELEM_HIST.shp"),
    #     year_col="VALIDFROM",
    #     out_folder=os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", "public", "Landschaftselemente"))

    ## Add landschaftselemente to crop parcel vector files
    # for year in range(2024, 2026):
    #     print(year)
    #     parcel_pth = os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", "public", "Teilschlaege", f"V_OD_LWK_TSCHLAG_{year}.shp")
    #     land_ele_pth = os.path.join("data", "vector", "IACS", "DE", "NRW", "Original", "public", "Landschaftselemente", f"V_OD_LWK_LAND_ELEM_HIST_{year}.shp")
    #     out_pth = os.path.join("data", "vector", "IACS", "DE", "NRW", f"layer_nw_{year}.geoparquet")
    #
    #     ## Open files
    #     gdf_s = gpd.read_file(parcel_pth)
    #     gdf_le = gpd.read_file(land_ele_pth)
    #
    #     ## Add column "SCHLAGNR" and set it to 0, to indicate that there are no Schlagnumbers
    #     gdf_s["SCHLAGNR"] = 0
    #     gdf_le["SCHLAGNR"] = 0
    #
    #     gdf_s = remove_geometry_duplicates(gdf_s)
    #     gdf_s = helper_functions.drop_non_geometries(gdf_s)
    #
    #     gdf_le = remove_geometry_duplicates(gdf_le)
    #     gdf_le = helper_functions.drop_non_geometries(gdf_le)
    #
    #     gdf_out = add_landscape_elements_to_crop_vectors(
    #         gdf_s=gdf_s,
    #         gdf_le=gdf_le,
    #         gdf_s_nr_col="SCHLAGNR",
    #         gdf_s_flik_col="FLIK",
    #         le_s_nr_col="SCHLAGNR",
    #         le_s_flik_col="FLEK",
    #         gdf_c_col="CODE",
    #         le_code_col="CODE",
    #         gdf_n_col="CODE_TXT",
    #         le_n_col="CODE_TXT",
    #         gdf_area_col="AREA_HA",
    #         le_area_col="AREA_HA"
    #     )

        ## Removing all entries that fall outside of NRW is not necessary, as only fields inside NRW are in the data!)
        # gdf_out.to_parquet(out_pth)


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')