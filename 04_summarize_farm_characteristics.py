# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import numpy as np
import pandas as pd
import geopandas as gpd

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def summarize_farm_characteristics(iacs_pth, iacs_file_encoding, animals_pth, crop_class_col, out_pth,
                                   farm_id_col="farm_id", organic_col="organic"):
    print("Read data.")
    ## Read input
    iacs = gpd.read_file(iacs_pth, encoding=iacs_file_encoding)
    animals = pd.read_csv(animals_pth, dtype={farm_id_col: str})

    ## TEMPORARY:
    recl_df = pd.read_excel(r"data\tables\hcat_levels_v2\EU-LAND derived\hcat_levels_all.xlsx", "qad_reclassification")
    iacs = pd.merge(iacs, recl_df[["level_6", "qad_reclassification"]], how="left", left_on="EC_hcat_c",
                    right_on="level_6")
    def q95(x):
        return np.quantile(x, .95)

    ## Get areas of crop classes per farm
    farms = iacs.groupby([farm_id_col, crop_class_col])[["field_size"]].sum().reset_index()
    farms.rename(columns={'field_size': 'crop_area'}, inplace=True)
    total_area_per_farm = iacs.groupby('farm_id')['field_size'].sum().reset_index()
    total_area_per_farm.rename(columns={'field_size': 'total_area'}, inplace=True)
    farms = pd.merge(farms, total_area_per_farm, on='farm_id')
    farms['crop_share'] = farms['crop_area'] / farms['total_area']
    farms = farms.pivot(index=farm_id_col, columns=crop_class_col, values="crop_share").reset_index()
    farms.fillna(0, inplace=True)

    field_sizes = iacs.groupby([farm_id_col]).agg(
        median_field_size=pd.NamedAgg("field_size", "median"),
        q95_field_size=pd.NamedAgg("field_size", q95)
    ).reset_index()

    ## Derive field centroids
    orig_crs = iacs.crs
    iacs["geometry"] = iacs["geometry"].to_crs(3035)
    iacs["field_centroids"] = iacs.centroid
    iacs['centroid_x'] = iacs.field_centroids.x
    iacs['centroid_y'] = iacs.field_centroids.y
    centres = iacs.groupby([farm_id_col]).agg({
        'centroid_x': 'mean',
        'centroid_y': 'mean'
    }).reset_index()

    ## Get organic share of per farm
    organic = iacs.groupby([farm_id_col, organic_col])[["field_size"]].sum().reset_index()
    organic[organic_col] = organic[organic_col].map({1: "organic", 0: "conventional"})
    organic = organic.pivot(index=farm_id_col, columns=organic_col, values="field_size").reset_index()
    organic.fillna(0, inplace=True)
    organic["farm_size"] = organic[["conventional", "organic"]].sum(axis=1)
    organic["organic_share"] = round(organic["organic"] / organic["farm_size"], 3)

    out_df = pd.merge(farms, organic, how="left", on=farm_id_col)
    out_df = pd.merge(out_df, field_sizes, how="left", on=farm_id_col)
    out_df = pd.merge(out_df, animals, how="left", on=farm_id_col)
    out_df = pd.merge(out_df, centres, how="left", on=farm_id_col)
    out_df.fillna(0, inplace=True)

    out_df.to_csv(out_pth, index=False)




def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    run_dict = {
        "DE/BB": {
            "region_id": "DE_BB",
            "years": [2018],
            "iacs_file_encoding": "ISO-8859-1"
        }
    }

    for country_code in run_dict:
        region_id = run_dict[country_code]["region_id"]
        file_encoding = run_dict[country_code]["iacs_file_encoding"]
        years = run_dict[country_code]["years"]

        ## Loop over files to unify columns and classify crops
        for i, year in enumerate(years):
            iacs_pth = fr"data\vector\IACS_EU_Land\{country_code}\IACS-{region_id}-{year}.gpkg"
            animals_pth = fr"data\vector\IACS_EU_Land\{country_code}\IACS_animals-{region_id}-{year}.csv"
            out_pth = fr"data\tables\IACS_EU_Land_farms\IACS_animals-{region_id}-{year}.csv"

            ## Create output folder
            folder = os.path.dirname(out_pth)
            helper_functions.create_folder(folder)

            summarize_farm_characteristics(
                iacs_pth=iacs_pth,
                iacs_file_encoding=file_encoding,
                animals_pth=animals_pth,
                crop_class_col="qad_reclassification",
                out_pth=out_pth,
                farm_id_col="farm_id",
                organic_col="organic")

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')