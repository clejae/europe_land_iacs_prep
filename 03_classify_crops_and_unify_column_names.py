# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import pandas as pd
import geopandas as gpd
import warnings

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

COL_NAMES_FOLDER = r"data\tables\column_names"
CROP_CLASSIFICATION_FOLDER = r"data\tables\crop_classifications"

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def unify_column_names(iacs_pth, file_encoding, col_translate_pth, crop_class_pth, region_id, year, iacs_new_pth, csv_sep=","):
    print("Unifying column names, classifying crops, reprojecting and saving as gpkg (or csv if input is csv).")

    ## Open files
    print("Reading input.")
    root, ext = os.path.splitext(iacs_pth)
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
        iacs = gpd.read_file(iacs_pth, encoding=file_encoding)
    if ext in ['.csv']:
        iacs = pd.read_csv(iacs_pth, sep=csv_sep)

    tr_df = pd.read_excel(col_translate_pth)
    cl_df = pd.read_excel(crop_class_pth)

    ## Optional: Subset the columns that should be in the final file
    print("Unifying column names.")
    tr_df = tr_df.loc[tr_df["prelim"] == 1].copy()

    ## Create a dictionary that translates old column names to unified column names
    col_year = f"{region_id}_{year}"
    col_dict = dict(zip(tr_df.loc[tr_df[col_year].notna(), col_year], tr_df.loc[tr_df[col_year].notna(), "column_name"]))

    ## Rename columns
    iacs.rename(columns=col_dict, inplace=True)

    ## Check if column with field size in ha is already in file. if not create
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
        if not "field_size" in iacs.columns:
            iacs["field_size"] = iacs.geometry.area / 10000

        ## Check if field_id is in file. if not create
        if not "field_id" in iacs.columns:
            iacs["field_id"] = range(len(iacs))

    ## Merge on crop name if it is availalbe in IACS data
    ## Then it is also likely it is available in classification table but we check anyways
    print("Classifying crops.")
    if ("crop_name" in iacs.columns) & ("crop_name" in cl_df.columns):
        print("Classifying (i.e. merging) on crop name.")
        if ("crop_code" in iacs.columns) & ("crop_code" in cl_df.columns):
            ## Drop crop_code because otherwise it will occur twice with appendic _x and _y
            iacs.drop(columns="crop_code", inplace=True)
        ## As we are classifying on crop name, we drop duplicates that might have arised, because of different codes
        cl_df.drop_duplicates(subset=["crop_name"], inplace=True)
        if iacs["crop_name"].dtype != 'object':
            iacs["crop_name"] = iacs["crop_name"].astype(int)
        iacs = pd.merge(iacs, cl_df, how="left", on="crop_name")
    elif ("crop_code" in iacs.columns) & ("crop_code" in cl_df.columns):
        print("Classifying (i.e. merging) on crop code")
        if ("crop_name" in iacs.columns) & ("crop_name" in cl_df.columns):
            iacs.drop(columns="crop_name", inplace=True)
        if iacs["crop_code"].dtype != 'object':
            iacs["crop_code"] = iacs["crop_code"].astype(int)
        ## As we are classifying on crop codes, we drop duplicates that might have arised, because of different names
        cl_df.drop_duplicates(subset=["crop_code"], inplace=True)
        iacs = pd.merge(iacs, cl_df, how="left", on="crop_code")
    else:
        warnings.warn("Could not classify the crop names or crop codes. Either one of them has to be in the IACS file and the classification table.")
        return

    # iacs["country_id"] = region_id.split("_")[0]

    ### Get all column names that should appear in final file
    cols = tr_df["column_name"].tolist() #[col_dict[k] for k in col_dict]
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
        cols.append("geometry")

    ### Check if all columns are in the file
    ## If not add the column and then subset file to the selected columns
    for col in cols:
        if col not in iacs.columns:
            iacs[col] = ""
    iacs = iacs[cols].copy()

    ## Reproject file
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
        print("Reprojecting.")
        iacs = iacs.to_crs(4326) # WGS 84

    ## Classify entries with no crop as unkown
    iacs.loc[iacs["crop_name"].isna(), "EC_hcat_n"] = "not_known_and_other"
    iacs.loc[iacs["crop_name"].isna(), "EC_hcat_c"] = 3399000000

    ## Create output folder
    folder = os.path.dirname(iacs_new_pth)
    helper_functions.create_folder(folder)

    ## Check if all crops were classified
    check = iacs.loc[iacs["EC_hcat_c"].isna()].copy()
    unique_crops = check["crop_name"].unique()
    root_new, ext_new = os.path.splitext(iacs_new_pth)
    if len(unique_crops) > 0:
        warnings.warn(f"{len(unique_crops)} crops were not classified into the EuroCrops classification.")
        if ext_new in ['.gpkg', '.gdb', '.shp', '.geojson']:
            check.to_file(iacs_new_pth[:-5] + "_misses.gpkg", encoding=file_encoding)
        if ext_new in ['.csv']:
            check.to_csv(iacs_new_pth[:-5] + "_misses.csv", index=False)

    ## Write out
    print("Writing out.")
    if ext_new in ['.gpkg', '.gdb', '.shp', '.geojson']:
        iacs.to_file(iacs_new_pth, encoding=file_encoding)
        # iacs.to_parquet(iacs_new_pth[:-4] + ".geoparquet")
    if ext_new in ['.csv']:
        iacs.to_csv(iacs_new_pth, index=False)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## Input for geodata harmonization (in some cases, e.g. France, some csv file have also to be harmonized. see below)

    run_dict = {
        # "BE/FLA": {"region_id": "BE_FLA", "file_encoding": "utf-8"},
        # "AT": {"region_id": "AT", "file_encoding": "utf-8"},
        # "DK": {"region_id": "DK", "file_encoding": "ISO-8859-1"},
        # "SI": {"region_id": "SI", "file_encoding": "utf-8"},
        # "NL": {"region_id": "NL", "file_encoding": "utf-8"},
        # "FI": {"region_id": "FI", "file_encoding": "ISO-8859-1"},

        ## Here are 9 crops missing in the crop classification -- need to be added
        # "LV": {"region_id": "LV", "file_encoding": "utf-8"},
        ## Here is some problem with the crop name column of the years 2021 and after. We need to find the right column
        "SK": {"region_id": "SK", "file_encoding": "utf-8", "skip_years": [2018, 2019, 2020, 2021, 2022]},
        "LV": {"region_id": "LV", "file_encoding": "utf-8"},

        # "FR/FR": {"region_id": "FR_FR", "file_encoding": "utf-8",  "ignore_files_descr": "ILOTS_ANONYMES"},

        ## For the years 2017-2014, the files are separated into subregions.
        ## There are also field blocks instead of fields. The share of the different crops per block
        ## is provided in a separate csv file. The main crop is provided in the vector file.
        # "FR/ARA": {"region_id": "FR_ARA", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/BRC": {"region_id": "FR_BRC", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/BRE": {"region_id": "FR_BRE", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/COR": {"region_id": "FR_COR", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/CVL": {"region_id": "FR_CVL", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/GRE": {"region_id": "FR_GRE", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/HDF": {"region_id": "FR_HDF", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/IDF": {"region_id": "FR_IDF", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/NOR": {"region_id": "FR_NOR", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/NOU": {"region_id": "FR_NOU", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/OCC": {"region_id": "FR_OCC", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/PDL": {"region_id": "FR_PDL", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
        # "FR/PRO": {"region_id": "FR_PRO", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_vector.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR"},
    }

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"] # country_code.replace(r"/", "_")
        col_translate_pth = f"data/tables/{region_id}_column_name_translation.xlsx"
        crop_class_pth = f"{CROP_CLASSIFICATION_FOLDER}/{region_id}_crop_classification_final.xlsx"
        file_encoding = run_dict[country_code]["file_encoding"]

        ## If the file naming of the columns translation and the crop classificaiton table deviate, then correct them
        if "col_translate_pth" in run_dict[country_code]:
            col_translate_pth = run_dict[country_code]["col_translate_pth"]
        if "crop_class_pth" in run_dict[country_code]:
            crop_class_pth = run_dict[country_code]["crop_class_pth"]

        ## Get years that should be skipped
        if "skip_years" in run_dict[country_code]:
            skip_years = run_dict[country_code]["skip_years"]
        else:
            skip_years = []

        ## Get files that should be skipped
        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None

        ## Get list of all available files
        in_dir = fr"data\vector\IACS\{country_code}"
        iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

        ## Exclude files that should be skipped
        if ignore_files_descr:
            iacs_files = [file for file in iacs_files if ignore_files_descr not in file]

        ## Temporary, if you want to subset the list.
        # iacs_files = iacs_files[12:13]

        ## Loop over files to unify columns and classify crops
        for i, iacs_pth in enumerate(iacs_files):
            print(f"{i + 1}/{len(iacs_files)} - Processing - {iacs_pth}")
            year = helper_functions.get_year_from_path(iacs_pth)
            if int(year) in skip_years:
                print(f"Skipping year {year}")
                continue
            ## First create out path with original region ID
            ## We have to fetch the region ID for safety reason again, as it might have been overwritten in
            region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")
            iacs_new_pth = rf"data\vector\IACS_EU_Land\{country_code}\IACS-{region_id}-{year}.gpkg"

            ## If an overwrite for the column translation is provided, it means that the columns in the
            ## column name translation table do not use the original region ID but another one
            if "col_transl_descr_overwrite" in run_dict[country_code]:
                region_id = run_dict[country_code]["col_transl_descr_overwrite"]

            unify_column_names(
                iacs_pth=iacs_pth,
                file_encoding=file_encoding,
                col_translate_pth=col_translate_pth,
                crop_class_pth=crop_class_pth,
                region_id=region_id,
                year=year,
                iacs_new_pth=iacs_new_pth,
            )

    ####################################################################################################################

    ## Input for csv harmonization, e.g. in France there are accompanying csv files that provide information on the
    ## crop share per field block for 2007-2014

    run_dict = {
        ## For the years 2017-2014, the files are separated into subregions.
        ## There are also field blocks instead of fields. The share of the different crops per block
        ## is provided in a separate csv file. The main crop is provided in the vector file.
        # "FR/ARA": {"region_id": "FR_ARA", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/BRC": {"region_id": "FR_BRC", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/BRE": {"region_id": "FR_BRE", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/COR": {"region_id": "FR_COR", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/CVL": {"region_id": "FR_CVL", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/GRE": {"region_id": "FR_GRE", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/HDF": {"region_id": "FR_HDF", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/IDF": {"region_id": "FR_IDF", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/NOR": {"region_id": "FR_NOR", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/NOU": {"region_id": "FR_NOU", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/OCC": {"region_id": "FR_OCC", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/PDL": {"region_id": "FR_PDL", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
        # "FR/PRO": {"region_id": "FR_PRO", "file_encoding": "utf-8",
        #            "col_translate_pth": f"data/tables/FR_SUBREGIONS_column_name_translation_csv.xlsx",
        #            "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
        #            "col_transl_descr_overwrite": "FR", "csv_sep": ";"},
    }

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")
        col_translate_pth = f"data/tables/{region_id}_column_name_translation.xlsx"
        crop_class_pth = f"{CROP_CLASSIFICATION_FOLDER}/{region_id}_crop_classification_final.xlsx"
        file_encoding = run_dict[country_code]["file_encoding"]

        ## If there is an alternative csv separator, fetch it
        if "csv_sep" in run_dict[country_code]:
            csv_sep = run_dict[country_code]["csv_sep"]
        else:
            csv_sep = ","

        ## If the file naming of the columns translation and the crop classificaiton table deviate, then correct them
        if "col_translate_pth" in run_dict[country_code]:
            col_translate_pth = run_dict[country_code]["col_translate_pth"]
        if "crop_class_pth" in run_dict[country_code]:
            crop_class_pth = run_dict[country_code]["crop_class_pth"]

        ## Get years that should be skipped
        if "skip_years" in run_dict[country_code]:
            skip_years = run_dict[country_code]["skip_years"]
        else:
            skip_years = []

        ## Get files that should be skipped
        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None

        ## Get list of all available files
        in_dir = fr"data\vector\IACS\{country_code}"
        csv_files = helper_functions.list_csv_files_in_dir(in_dir)

        ## Exclude files that should be skipped
        if ignore_files_descr:
            csv_files = [file for file in csv_files if ignore_files_descr not in file]

        ## Temporary, if you want to subset the list.
        # iacs_files = iacs_files[12:13]

        ## Loop over files to unify columns and classify crops
        for i, csv_pth in enumerate(csv_files):
            print(f"{i + 1}/{len(csv_files)} - Processing - {csv_pth}")
            year = helper_functions.get_year_from_path(csv_pth)
            if int(year) in skip_years:
                print(f"Skipping year {year}")
                continue
            ## First create out path with original region ID
            ## We have to fetch the region ID for safety reason again, as it might have been overwritten later on
            region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")
            csv_new_pth = rf"data\vector\IACS_EU_Land\{country_code}\IACS-{region_id}-{year}.csv"

            ## If an overwrite for the column translation is provided, it means that the columns in the
            ## column name translation table do not use the original region ID but another one
            if "col_transl_descr_overwrite" in run_dict[country_code]:
                region_id = run_dict[country_code]["col_transl_descr_overwrite"]

            unify_column_names(
                iacs_pth=csv_pth,
                file_encoding=file_encoding,
                col_translate_pth=col_translate_pth,
                crop_class_pth=crop_class_pth,
                region_id=region_id,
                year=year,
                iacs_new_pth=csv_new_pth,
                csv_sep=csv_sep
            )




    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()
