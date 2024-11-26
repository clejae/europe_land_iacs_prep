# Author:
# github repository:

# 1. Loop over available files and get unique crop names
# 2. Get EuroCrops classification from shapefile that they provide
# 3. Translate crop names to English and German. Match crop names with their classification.
# Afterwards: Manually classify missing crop names to EuroCrops classification.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import pandas as pd
import geopandas as gpd
import glob
import numpy as np

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## For france create a dictionary in a loop, because of the many subregions
    run_dict = {}
    FR_districts = pd.read_csv(r"data\vector\IACS\FR\region_code.txt")
    FR_districts = list(FR_districts["code"])
    FR_districts.remove("GRE")
    for district in FR_districts:
        run_dict[f"FR/{district}"] = {
            "region_id": f"FR_{district}",
            "file_encoding": "utf-8",
            "col_translate_pth": f"data/tables/column_name_translations/FR_SUBREGIONS_column_name_translation_vector.xlsx",
            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
            "col_transl_descr_overwrite": "FR",
            "skip_years": [2007, 2008, 2009]
        }

    for country_code in run_dict:
        print(country_code)
        if "skip_years" in run_dict[country_code]:
            skip_years = run_dict[country_code]["skip_years"]
        else:
            skip_years = []

        ## Get files that should be skipped
        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None

        in_dir = fr"data\vector\IACS\{country_code}"

        ## Get list of IACS files
        iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

        if ignore_files_descr:
            iacs_files = [file for file in iacs_files if ignore_files_descr not in file]

        ## Loop over files to derive crop names from all files
        for path in iacs_files:
            year = helper_functions.get_year_from_path(path)
            print(f"Processing: {year} - {path}")

            if int(year) in skip_years:
                print(f"Skipping year {year}")
                continue

            csv_pth = os.path.dirname(path) + rf"\ILOTS-ANONYMES-GROUPES-CULTURE.csv"

            gdf = gpd.read_file(path)
            df = pd.read_csv(csv_pth, sep=";", dtype={"CODE_GROUPE_CULTURE": str})

            field_size_col = "SURF_GRAPH"

            ## Replace NR with the total area of the field
            gdf.loc[gdf[field_size_col] == "NR", field_size_col] = gdf.loc[gdf[field_size_col] == "NR", "geometry"].area
            gdf[field_size_col] = gdf[field_size_col].astype(float)

            gdf.loc[gdf["SURF_CULTU"] == "NR", "SURF_CULTU"] = gdf.loc[gdf["SURF_CULTU"] == "NR", field_size_col]
            gdf["SURF_CULTU"] = gdf["SURF_CULTU"].astype(float)

            ## Remove crops that are already in the gdf from the accompanying df
            result = pd.merge(df, gdf, left_on=['NUM_ILOT', 'CODE_GROUPE_CULTURE'],
                              right_on=['NUM_ILOT', 'CODE_CULTU'],
                              how='left', indicator=True)
            result = result[result['_merge'] == 'left_only'].drop(columns=['_merge'])

            t = gdf.loc[gdf["NUM_ILOT"] == "008-341015"]
            t2 = df.loc[df["NUM_ILOT"] == "008-341015"]
            ## save geometries + first crop columns to gpkg
            root, ext = os.path.splitext(path)
            gdf.to_file(root + "_sep.gpkg")
            ## save as csv
            result.to_csv(root + "_sep.csv", index=False)

            ## move original file to "temp" folder
            folder = fr"data\vector\IACS\{country_code}_temp\{year}"
            helper_functions.create_folder(folder)
            if ext == ".shp":
                files = glob.glob(f"{root}.*")
                for f in files:
                    new_path = fr"{folder}\{os.path.basename(f)}"
                    os.rename(f, new_path)
            else:
                new_path = fr"{folder}\{os.path.basename(path)}"
                os.rename(path, new_path)

            new_csv_path = fr"{folder}\{os.path.basename(csv_pth)}"
            os.rename(csv_pth, new_csv_path)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
