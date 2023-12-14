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
from osgeo import ogr
import glob

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def pt_combine_crop_codes_with_crop_names(crop_codes_pth, crop_names_pth):

    ## Load input
    codes_df = pd.read_csv(crop_codes_pth)
    names_df = pd.read_excel(crop_names_pth)

    ## Save backup of original codes df as it takes quite long to create

    ## Merge in both ways to identify possible misses
    merge_df1 = pd.merge(names_df, codes_df, "left", left_on="CTnum", right_on="crop_code")
    print(f"There are {len(merge_df1.loc[merge_df1['crop_code'].isna()])} entries in the {crop_names_pth} that don't occur in the IACS data.")
    miss_df = merge_df1.loc[merge_df1['crop_code'].isna()].copy()
    merge_df2 = pd.merge(codes_df[["crop_code"]], names_df, "left", left_on="crop_code", right_on="CTnum")
    print(f"There are {len(merge_df2.loc[merge_df2['CTnum'].isna()])} entries in the IACS data that don't occur in the {crop_names_pth}.")

    ## Clean the second merge-df and concatenate with misses in the other merge-df
    merge_df2 = merge_df2[["crop_code", "CT_português", "CT"]].copy()
    miss_df = miss_df[["CTnum", "CT_português", "CT"]].copy()
    miss_df.rename(columns={"CTnum": "crop_code", "CT_português": "crop_name",  "CT": "translated_name"}, inplace=True)
    merge_df2.rename(columns={"CT_português": "crop_name", "CT": "translated_name"}, inplace=True)
    out_df = pd.concat([merge_df2, miss_df])
    out_df.dropna(subset="crop_code", inplace=True)

    ## Write out
    out_df.to_csv(crop_codes_pth, index=False)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    run_dict = {
        ## The portugese files sometimes come with fieldblocks and sometimes with fields - not all years need to be separated
        "PT/PT": {
            "region_id": "PT_PT",
            "file_encoding": "utf-8",
            "pt_special_function": pt_combine_crop_codes_with_crop_names
        }
        # "PT/ALE": {
        #     "region_id": "PT_ALE",
        #     "file_encoding": "utf-8",
        #     "skip_years": [2018, 2019, 2020, 2021, 2022],
        #     "ignore_files_descr":  "_sep_.gpkg",
        # "PT/ALG": {
        #     "region_id": "PT_ALG",
        #     "file_encoding": "utf-8"},
        # "PT/AML": {
        #     "region_id": "PT_AML",
        #     "file_encoding": "utf-8",
        #     "ignore_files_descr": "_sep_.gpkg"},
        # "PT/CE": {
        #     "region_id": "PT_CE",
        #     "file_encoding": "utf-8"},
        # "PT/CEN": {
        #     "region_id": "PT_CEN",
        #     "file_encoding": "utf-8"},
        # "PT/CES": {
        #     "region_id": "PT_CES",
        #     "file_encoding": "utf-8"},
        # "PT/NO": {
        #     "region_id": "PT_NO",
        #     "file_encoding": "utf-8"},
        # "PT/NON": {
        #     "region_id": "PT_NON",
        #     "file_encoding": "utf-8"},
        # "PT/NOS": {
        #     "region_id": "PT_NOS",
        #     "file_encoding": "utf-8"},

    }

    for country_code in run_dict:
        print(country_code)
        region_id = run_dict[country_code]["region_id"] # country_code.replace(r"/", "_")
        encoding = run_dict[country_code]["file_encoding"]
        if "skip_years" in run_dict[country_code]:
            skip_years = run_dict[country_code]["skip_years"]
        else:
            skip_years = []

        ## Get files that should be skipped
        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None

        col_translate_pth = rf"data\tables\{region_id}_column_name_translation.xlsx"

        in_dir = fr"data\vector\IACS\{country_code}"

        ## Get list of IACS files
        iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

        if ignore_files_descr:
            iacs_files = [file for file in iacs_files if ignore_files_descr not in file]

        ## Open column translations
        tr_df = pd.read_excel(col_translate_pth)

        ## Loop over files to derive crop names from all files
        for path in iacs_files:
            year = helper_functions.get_year_from_path(path)
            print(f"Processing: {year} - {path}")

            if int(year) in skip_years:
                print(f"Skipping year {year}")
                continue

            col_year = f"{region_id}_{year}"
            col_dict = dict(zip(tr_df["column_name"], tr_df[col_year]))

            ## create a list of crop names and crop codes, so if multiple crop columns are provided (and separated by "|")
            ## then they can be looped over
            if type(col_dict["crop_name"]) != float:
                col_dict["crop_name"] = col_dict["crop_name"].split("|")

            if type(col_dict["crop_code"]) != float:
                col_dict["crop_code"] = col_dict["crop_code"].split("|")

            target_column = None
            if type(col_dict["crop_name"]) != float and len(col_dict["crop_name"]) > 1:
                target_column = "crop_name"
            elif type(col_dict["crop_code"]) != float and len(col_dict["crop_code"]) > 1:
                target_column = "crop_code"
            else:
                print("Only one target column ('crop_name' or 'crop_code') provided. Nothing to be done.")
                continue

            ## If multiple crop name columns are provided, then separate the additional crop columns into a csv
            if target_column:
                print("Multiple crop columns provided. Separating them into csv.")
                ## Create separated file
                ## Open file and layer
                gdf = gpd.read_file(path)
                cols = list(gdf.columns)
                cols_csv = []

                ## Create field id if not existent
                ## if not update also the column translation table
                if type(col_dict["field_id"]) != float:
                    cols_csv.append(col_dict["field_id"])
                else:
                    gdf["field_id"] = range(1, len(gdf)+1)
                    cols_csv.append("field_id")
                    tr_df.loc[tr_df['column_name'] == "field_id", col_year] = "field_id"
                    tr_df.to_excel(col_translate_pth)

                ## loop over the crop columns
                for col in col_dict[target_column][1:]:
                    cols.remove(col)
                    cols_csv.append(col)

                ## separate the file based on the columns
                df = gdf[cols_csv]
                gdf = gdf[cols]

                ## save geometries + first crop columns to gpkg
                root, ext = os.path.splitext(path)
                gdf.to_file(root + "_sep.gpkg")

                ## melt additional crop columns from wide to long df and save as csv
                df = pd.melt(df, id_vars=cols_csv[0], value_vars=cols_csv[1:], var_name="crop_number", value_name=target_column)
                df.dropna(inplace=True)
                df.to_csv(root + "_sep.csv", index=False)

                del gdf, df

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
            else:
                print("Only one target column ('crop_name' or 'crop_code') provided. Nothing to be done.")


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
