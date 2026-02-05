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

gdal.SetConfigOption("OGR_GEOMETRY_ACCEPT_UNCLOSED_RING", "NO")

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


def create_unique_id(in_pth):
    root, ext = os.path.splitext(in_pth)
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
        gdf = gpd.read_file(in_pth)
    elif ext in ['.geoparquet']:
        gdf = gpd.read_parquet(in_pth)

    gdf["uni_id"] = gdf["flik_flek"] + '_' + gdf["fl_kenng"].astype(str)

    print("Number Unique IDs:", len(gdf["uni_id"].unique()))
    print("Number Parcels:",  len(gdf))

    if len(gdf["uni_id"].unique()) == len(gdf):
        gdf.to_parquet(os.path.splitext(in_pth)[0] + ".geoparquet")
    else:
        gdf['uni_id_distinguished'] = gdf.groupby('uni_id').cumcount() + 1
        gdf['uni_id'] = gdf['uni_id'].astype(str) + '_' + gdf['uni_id_distinguished'].astype(str)
        gdf.drop(columns=["uni_id_distinguished"], inplace=True)
        print("Number Unique IDs:", len(gdf["uni_id"].unique()))
        print("Number Parcels:", len(gdf))
        if len(gdf["uni_id"].unique()) == len(gdf):
            gdf.to_parquet(os.path.splitext(in_pth)[0] + ".geoparquet")


def separate_subparcels_in_data():
    #### 2015
    print(2015)
    gdf1 = gpd.read_file(r"data\vector\IACS\DE\MWP\TI_Original\layer_mv_2015.gpkg")

    ## Create a unique ID based on geometries
    gdf1["geom_id"] = gdf1.geometry.to_wkb()

    ## Drop all duplicates that have the same geometry, the same land use and the same reported area.
    gdf1.drop_duplicates(subset=["geom_id", "nutz_le_code_meldg", "fl_ha_meldg"], inplace=True)

    ## Create a unique ID that later can be used to link geometries with additional crops
    uni_ids = {gid: i for i, gid in enumerate(gdf1["geom_id"].unique())}
    gdf1["uni_id"] = gdf1["geom_id"].map(uni_ids)

    ## Now we need to separate the subfields for which a larger subfield exists in the field blocks
    idx_max = (
        gdf1
        .groupby("geom_id")["fl_ha_meldg"]
        .idxmax()
    )

    gdf_unique = gdf1.loc[idx_max].drop(columns="geom_id")
    gdf_unique.sort_values(by="uni_id", inplace=True)

    gdf_unique["uni_id_new"] = gdf_unique["flik_flek"] + '_' + gdf_unique["fl_kenng"].astype(str)
    gdf_unique['uni_id_distinguished'] = gdf_unique.groupby('uni_id_new').cumcount() + 1
    gdf_unique['uni_id_new'] = gdf_unique['uni_id_new'].astype(str) + '_' + gdf_unique['uni_id_distinguished'].astype(str)
    gdf_unique.drop(columns=["uni_id_distinguished"], inplace=True)

    id_dict = dict(zip(gdf_unique["uni_id"], gdf_unique["uni_id_new"]))
    gdf_unique["uni_id_new"] = gdf_unique["uni_id"].map(id_dict)
    # gdf_unique.drop(columns=["uni_id_new"], inplace=True)

    mask_kept = gdf1.index.isin(idx_max)
    gdf_others = gdf1.loc[~mask_kept].drop(columns=["geom_id", "geometry"]).copy()
    gdf_others.sort_values(by="uni_id", inplace=True)
    gdf_others["uni_id_new"] = gdf_others["uni_id"].map(id_dict)

    gdf_others.drop(columns="uni_id", inplace=True)
    gdf_unique.drop(columns="uni_id", inplace=True)

    print("Number Unique IDs:", len(gdf_unique["uni_id_new"].unique()))
    print("Number Parcels:", len(gdf_unique))
    if len(gdf_unique["uni_id_new"].unique()) == len(gdf_unique):
        gdf_unique.to_parquet(r"data\vector\IACS\DE\MWP\layer_mv_2015.geoparquet")
        gdf_others.to_csv(r"data\vector\IACS\DE\MWP\layer_mv_2015.csv", index=False)

    #### 2014
    print(2014)
    gdf1 = gpd.read_file(r"data\vector\IACS\DE\MWP\TI_Original\layer_mv_2014.gpkg")

    ## Create a unique ID based on geometries
    gdf1["geom_id"] = gdf1.geometry.to_wkb()

    ## Drop all duplicates that have the same geometry, the same land use and the same reported area.
    gdf1.drop_duplicates(subset=["geom_id", "nutz_le_code_meldg", "fl_ha_meldg"], inplace=True)

    ## Create a unique ID that later can be used to link geometries with additional crops
    uni_ids = {gid: i for i, gid in enumerate(gdf1["geom_id"].unique())}
    gdf1["uni_id"] = gdf1["geom_id"].map(uni_ids)

    ## Now we need to separate the subfields for which a larger subfield exists in the field blocks
    idx_max = (
        gdf1
        .groupby("geom_id")["fl_ha_meldg"]
        .idxmax()
    )

    gdf_unique = gdf1.loc[idx_max].drop(columns="geom_id")
    gdf_unique.sort_values(by="uni_id", inplace=True)

    gdf_unique["uni_id_new"] = gdf_unique["flik_flek"] + '_' + gdf_unique["fl_kenng"].astype(str)
    gdf_unique['uni_id_distinguished'] = gdf_unique.groupby('uni_id_new').cumcount() + 1
    gdf_unique['uni_id_new'] = gdf_unique['uni_id_new'].astype(str) + '_' + gdf_unique['uni_id_distinguished'].astype(
        str)
    gdf_unique.drop(columns=["uni_id_distinguished"], inplace=True)

    id_dict = dict(zip(gdf_unique["uni_id"], gdf_unique["uni_id_new"]))
    gdf_unique["uni_id_new"] = gdf_unique["uni_id"].map(id_dict)
    # gdf_unique.drop(columns=["uni_id_new"], inplace=True)

    mask_kept = gdf1.index.isin(idx_max)
    gdf_others = gdf1.loc[~mask_kept].drop(columns=["geom_id", "geometry"]).copy()
    gdf_others.sort_values(by="uni_id", inplace=True)
    gdf_others["uni_id_new"] = gdf_others["uni_id"].map(id_dict)

    gdf_others.drop(columns="uni_id", inplace=True)
    gdf_unique.drop(columns="uni_id", inplace=True)

    print("Number Unique IDs:", len(gdf_unique["uni_id_new"].unique()))
    print("Number Parcels:", len(gdf_unique))
    if len(gdf_unique["uni_id_new"].unique()) == len(gdf_unique):
        gdf_unique.to_parquet(r"data\vector\IACS\DE\MWP\layer_mv_2014.geoparquet")
        gdf_others.to_csv(r"data\vector\IACS\DE\MWP\layer_mv_2014.csv", index=False)



def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # for year in range(2014, 2024):
    #     print(year)
    #     in_pth = rf"data\vector\IACS\DE\MWP\TI_Original\layer_mv_{year}.gpkg"
    #     count_duplicate_geometries(in_pth)
    ## --> For 2014 and 2015 the vector data contain field blocks, for the other years the data contain fields.

    #### 2016
    # gdf1 = gpd.read_file(r"data\vector\IACS\DE\MWP\TI_Original\layer_mv_2016.gpkg")
    # gdf1["field_id"] = gdf1["flik_flek"] + gdf1["fl_kenng"].astype(str) + gdf1["fl_ha_geom"].astype(str)
    #
    # gdf2 = gdf1[gdf1.duplicated(subset="field_id", keep=False)].copy()
    # gdf2.sort_values(by="field_id", inplace=True)
    #
    # counts = gdf2.groupby("field_id")["nutz_le_code_meldg"].nunique()
    # conflicts = gdf2[gdf2["field_id"].isin(counts[counts > 1].index)].sort_values("field_id")
    #
    # gdf_out = gdf1.drop_duplicates(subset=["field_id", "nutz_le_code_meldg"]).copy()
    # # len(gdf_out), len(gdf_out["id"].unique())
    # gdf_out.drop(columns="field_id", inplace=True)
    #
    # gdf_out.to_parquet(r"data\vector\IACS\DE\MWP\layer_mv_2016.geoparquet")

    # 2014-2015
    separate_subparcels_in_data()

    # for year in range(2016, 2024):
    #     print(year)
    #     in_pth = rf"data\vector\IACS\DE\MWP\layer_mv_{year}.geoparquet"
    #     create_unique_id(in_pth)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')