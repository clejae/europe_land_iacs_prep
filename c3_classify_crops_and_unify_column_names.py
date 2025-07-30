# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# This script will produce a harmonized version of the GSA data. For this, all the crop entries in the original GSA data
# will be classified and all the column names will be harmonized from the original language to a column name in English.

# The original GSA data should be stored in:
# data\vector\IACS\XX\.
# There should be a correct column name translation table in:
# data\tables\column_name_translations\XX_column_name_translations.xlsx
# There should be a correct crop classification table in:
# data\tables\crop_classifications\XX_crop_classification_final.xlsx

# The harmonized GSA data will be saved to as geoparquets:
# data\vector\IACS_EU_Land\XX\.

# If you want to run this script for a specific country, put an entry in the run_dict at the top of the main function.
# The run_dict key should be the country or country and subdivision abbreviations (for example, "DK" or "DE/THU). The
# item is another dictionary. In this dictionary, you should include the following keys:

# "region_id" - basically the main key (XX), but for XX/XXX changed into XX_XXX
# "file_encoding" - Encoding of the original GSA file
# "file_year_encoding" - [optional] use if specific years deviate from that encoding
# "organic_dict" - [optional] use if there is an organic column in the original GSA data and the information
# is not already in the right form (0, 1 and 2 should indicate conventional, organic, and in transition)
# "organic_dict_year" - [optional] use to specify for specific years how the information from the original organic column
# should be mapped, e.g. "Y" should be 1, "N" should be 0
# "skip_years" - [optional] can be used to provide a list of years that should not be harmonized
# "ignore_file_descr" - [optional] use if there are other geospatial datasets in your folder that are not GSA data
# "pre_transformation_crs" - [optional] provide an epsg code for input files that are not correctly defined in the files, e.g. in Croatia

# To turn off/on the matching of a specific country, just comment/uncomment the specific line of the run_dict

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import sys
# os.environ['GDAL_DATA'] = os.path.join(f'{os.sep}'.join(sys.executable.split(os.sep)[:-1]), 'Library', 'share', 'gdal')
import time
import pandas as pd
import geopandas as gpd
import warnings
import numpy as np

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

COL_NAMES_FOLDER = os.path.join("data", "tables", "column_names")
CROP_CLASSIFICATION_FOLDER = os.path.join("data", "tables", "crop_classifications")

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def unify_column_names_in_vector_data(iacs_pth, file_encoding, col_translate_pth, crop_class_pth, region_id, year,
                                      iacs_new_pth, csv_sep=",", pre_transformation_crs=None, organic_dict=None,
                                      classify_on="automatic"):
    """
       Unify column names in vector data.

       Parameters:
       ----------
       iacs_pth : str
           Path to the IACS file.
       file_encoding : str
           Encoding of the IACS file.
       col_translate_pth : str
           Path to the column translation file.
       crop_class_pth : str
           Path to the crop classification file.
       region_id : int or str
           Identifier for the region.
       year : int
           Year of the data.
       iacs_new_pth : str
           Output path for the harmonized IACS file.
       csv_sep : str, optional
           Separator for CSV files (default is ",").
       pre_transformation_crs : str or None, optional
           Coordinate reference system (CRS) that the input data have (default is None).
       organic_dict : dict or None, optional
           Dictionary mapping organic classifications (default is None).
       classify_on : str, optional
           Specifies the column to classify on. Should be used with care and only if all crop codes have a unique
            classification. Otherwise, missclassifications might occur.
            Must be one of:
           - "crop_code"
           - "crop_name"
           - "automatic"
           Default is "automatic".

       Raises:
       ------
       ValueError:
           If `classify_on` is not one of "crop_code", "crop_name", or "automatic".

       Returns:
       -------
       None
       """

    valid_options = ["crop_code", "crop_name", "automatic"]
    if classify_on not in valid_options:
        raise ValueError(f"Invalid value for classify_on: '{classify_on}'. Must be one of {valid_options}.")

    root, ext = os.path.splitext(iacs_pth)
    print(f"Unifying column names, classifying crops, reprojecting and saving as Geoparquet (or csv if input is csv).")

    ## Open files
    print("Reading input.")

    if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
        iacs = gpd.read_file(iacs_pth, encoding=file_encoding)
    elif ext in ['.geoparquet']:
        iacs = gpd.read_parquet(iacs_pth)
    elif ext in ['.csv']:
        iacs = pd.read_csv(iacs_pth, sep=csv_sep)

    tr_df = pd.read_excel(col_translate_pth)
    cl_df = pd.read_excel(crop_class_pth) #, dtype={"EC_hcat_c": int}

    ## Optional: Subset the columns that should be in the final file
    print("Unifying column names.")
    tr_df = tr_df.loc[tr_df["prelim"] == 1].copy()

    ## Create a dictionary that translates old column names to unified column names
    col_year = f"{region_id}_{year}"
    col_dict = dict(zip(tr_df.loc[tr_df[col_year].notna(), col_year], tr_df.loc[tr_df[col_year].notna(), "column_name"]))

    ## In some cases, multiple columns are provided (e.g. for crops in field blocks), therefore the dictionary has to
    ## be corrected.
    keys = list(col_dict.keys())
    keys_dict = {i: i.split('|')[0] for i in keys}
    col_dict = {keys_dict[key]: col_dict[key] for key in keys_dict}

    ## Rename columns
    iacs.rename(columns=col_dict, inplace=True)

    ## Check if column with field size in ha is already in file. if not create
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson', '.geoparquet']:

        ## in some cases (e.g. HR), there were some issues with the CRS. Setting it anew, helped to solve it.
        if pre_transformation_crs:
            iacs.crs = None
            iacs.set_crs(epsg=pre_transformation_crs, inplace=True)

        if not "field_size" in iacs.columns:
            ## Reproject only here, if the crs is geographic (if so, the area calculations will likely be wrong)
            if not iacs.crs.is_projected:
                iacs = iacs.to_crs(3857)
            iacs["field_size"] = iacs.geometry.area / 10000
        iacs["field_size"] = iacs["field_size"].astype(float)

        ## Check if field_id is in file. if not create
        if not "field_id" in iacs.columns:
            iacs["field_id"] = range(len(iacs))

    ## Merge on crop name if it is availalbe in IACS data
    ## Then it is also likely it is available in classification table but we check anyways
    print("Classifying crops.")
    if (("crop_name" in iacs.columns) & ("crop_name" in cl_df.columns)) and (classify_on in ["crop_name", "automatic"]):
        print("Classifying (i.e. merging) on crop name.")

        crop_codes_bool = False
        if ("crop_code" in iacs.columns) & ("crop_code" in cl_df.columns):
            crop_codes = iacs[["field_id", "crop_code"]].copy()
            ## Drop crop_code because otherwise it will occur twice with appendic _x and _y
            iacs.drop(columns="crop_code", inplace=True)
            crop_codes_bool = True

        ## As we are classifying on crop name, we drop duplicates that might have arisen because of different codes
        cl_df.drop_duplicates(subset=["crop_name"], inplace=True)
        if iacs["crop_name"].dtype != 'object':
            iacs["crop_name"] = iacs["crop_name"].astype(str)

        ## remove any line breaks that could not be captured in the crop classification tables
        cl_df['crop_name'] = cl_df['crop_name'].str.replace('\n', '')
        cl_df['crop_name'] = cl_df['crop_name'].str.replace('\r', '')
        iacs['crop_name'] = iacs['crop_name'].str.replace('\n', '')
        iacs['crop_name'] = iacs['crop_name'].str.replace('\r', '')
        iacs = pd.merge(iacs, cl_df, how="left", on="crop_name")

        ## As we are merging on crop names, it is possible that codes from other years are assigned to the
        ## original crop code column (e.g. BRB 2005, crop nan -->710). To be correct, we assign the code back.
        if crop_codes_bool:
            # iacs["crop_code"] = crop_codes
            iacs.drop(columns="crop_code", inplace=True)
            iacs = pd.merge(iacs, crop_codes, how="left", on="field_id")
        ##
    elif (("crop_code" in iacs.columns) & ("crop_code" in cl_df.columns)) and (classify_on in ["crop_code", "automatic"]):
        print("Classifying (i.e. merging) on crop code")

        if ("crop_name" in iacs.columns) & ("crop_name" in cl_df.columns):
            iacs.drop(columns="crop_name", inplace=True)
        if iacs["crop_code"].dtype != 'object':
            iacs["crop_code"] = iacs["crop_code"].astype(int)

        ## As we are classifying on crop codes, we drop duplicates that might have arised, because of different names
        cl_df.drop_duplicates(subset=["crop_code"], inplace=True)
        cl_df.dropna(subset="crop_code", inplace=True)
        iacs["crop_code"] = iacs["crop_code"].astype(cl_df["crop_code"].dtype)
        iacs = pd.merge(iacs, cl_df, how="left", on="crop_code")
    else:
        warnings.warn("Could not classify the crop names or crop codes. Either one of them has to be in the IACS file and the classification table.")
        return

    # iacs["country_id"] = region_id.split("_")[0]
    if organic_dict:
        if "organic" in iacs.columns:
            iacs["organic"] = iacs["organic"].map(organic_dict)
            iacs.loc[iacs["organic"].isna(), "organic"] = 0

    ### Get all column names that should appear in final file
    cols = tr_df["column_name"].tolist() #[col_dict[k] for k in col_dict]
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson', '.geoparquet']:
        cols.append("geometry")

    ### Check if all columns are in the file
    ## If not add the column and then subset file to the selected columns
    for col in cols:
        if col not in iacs.columns:
            iacs[col] = ""
    iacs = iacs[cols].copy()

    ## Classify entries with no crop as unkown
    check = iacs.loc[iacs["EC_hcat_n"].isna()].copy()
    iacs.loc[iacs["crop_name"].isna(), "EC_hcat_n"] = "not_known_and_other"
    iacs.loc[iacs["crop_name"].isna(), "EC_hcat_c"] = 3399000000
    iacs.loc[iacs["EC_hcat_n"].isna(), "EC_hcat_n"] = "missing"
    iacs.loc[iacs["EC_hcat_c"].isna(), "EC_hcat_c"] = 1000000000

    iacs["EC_hcat_c"] = iacs["EC_hcat_c"].astype(np.int64)

    ## Reproject
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson', '.geoparquet']:
        print("Reprojecting.")
        iacs = iacs.to_crs(3035)  # in meters

    ## Create output folder
    folder = os.path.dirname(iacs_new_pth)
    helper_functions.create_folder(folder)

    ## Check if all crops were classified
    unique_crops = check["crop_name"].unique()
    check.drop_duplicates(subset=["crop_code", "crop_name"], inplace=True)
    check = check[["crop_code", "crop_name"]]
    root_new, ext_new = os.path.splitext(iacs_new_pth)

    if len(unique_crops) > 0:
        print(f"{len(unique_crops)} crops were not classified into the EuroCrops classification.")
        check.to_csv(os.path.splitext(iacs_new_pth)[0] + "_misses.csv", index=False)
        # if ext_new in ['.gpkg', '.gdb', '.shp', '.geojson']:
        #     check.to_file(os.path.splitext(iacs_new_pth)[0] + "_misses.gpkg", encoding=file_encoding)

    ## Write out
    print("Writing out.")
    if ext_new in ['.gpkg', '.gdb', '.shp', '.geojson']:
        iacs.to_file(iacs_new_pth, encoding=file_encoding)
    if ext_new in ['.geoparquet']:
        iacs.to_parquet(iacs_new_pth)
    if ext_new in ['.csv']:
        iacs.to_csv(iacs_new_pth, index=False)

def unify_column_names_in_animal_data(iacs_animal_pth, col_translate_pth, region_id, year, iacs_animal_new_pth, csv_sep=",", farm_id_dtype="str"):

    tr_df = pd.read_excel(col_translate_pth)

    print("Unifying column names.")
    tr_df = tr_df.loc[tr_df["prelim"] == 1].copy()

    ## Create a dictionary that translates old column names to unified column names
    col_year = f"{region_id}_{year}"
    col_dict = dict(
        zip(tr_df.loc[tr_df[col_year].notna(), col_year], tr_df.loc[tr_df[col_year].notna(), "column_name"]))

    col_dict_inv = inv_map = {v: k for k, v in col_dict.items()}

    types = {col_dict_inv["farm_id"]: farm_id_dtype}

    ## Open files
    print("Reading input.")
    root, ext = os.path.splitext(iacs_animal_pth)
    if ext in ['.xlsx', '.xls']:
        animal_df = pd.read_excel(iacs_animal_pth, dtype=types)
    if ext in ['.csv']:
        animal_df = pd.read_csv(iacs_animal_pth, dtype=types, sep=csv_sep)

    ## Rename columns
    animal_df.rename(columns=col_dict, inplace=True)

    ### Get all column names that should appear in final file
    cols = tr_df["column_name"].tolist()  # [col_dict[k] for k in col_dict]
    if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
        cols.append("geometry")

    ### Check if all columns are in the file
    ## If not add the column and then subset file to the selected columns
    for col in cols:
        if col not in animal_df.columns:
            animal_df[col] = ""
    animal_df = animal_df[cols].copy()

    ## Write out
    print("Writing out.")
    animal_df.to_csv(iacs_animal_new_pth, index=False)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## Input for geodata harmonization (in some cases, e.g. France or Portugal, some csv file have also to
    ## be harmonized. See below)

    ## To turn off/on the harmonization of a specific country, just comment/uncomment the specific line

    run_dict = {
        # "AT": {"region_id": "AT", "file_encoding": "utf-8", "organic_dict_year": {"2023": {"Y": 1, "N": 0}}, "ignore_files_descr": "temp"},
        # "BE/FLA": {"region_id": "BE_FLA", "file_encoding": "utf-8", "file_year_encoding": {"2020": "ISO-8859-1"},
        #            "organic_dict": {"J": 1, "N": 0}}, #"skip_years": list(range(2008, 2022))+[2024],
        # "BE/WAL": {"region_id": "BE_WAL", "file_encoding": "utf-8", "file_year_encoding":  {"2015": "windows-1252", "2016":
        #     "windows-1252", "2017": "windows-1252"}}, #, 2018: "utf-8", 2019: "utf-8", 2020: "utf-8", 2021: "utf-8", 2022: "utf-8"
        # "CY": {"region_id": "CY", "file_encoding": "utf-8", "ignore_files_descr": "LPIS"},
        # "CZ": {"region_id": "CZ", "file_encoding": "utf-8", "ignore_files_descr": "IACS_Czechia"},
        # "DE/BRB": {"region_id": "DE_BRB", "file_encoding": "ISO-8859-1"}, #,
        # "DE/LSA": {"region_id": "DE_LSA", "file_encoding": "utf-8", "ignore_files_descr": "other_files"},
        # "DE/NRW": {"region_id": "DE_NRW", "file_encoding": "ISO-8859-1", "ignore_files_descr": "HIST"},
        # "DE/SAA": {"region_id": "DE_SAA", "file_encoding": "utf-8", "file_year_encoding": {"2023": "windows-1252"},
        #     "ignore_files_descr": "Antrag"},
        # "DE/SAT": {"region_id": "DE_SAT", "file_encoding": "utf-8", "ignore_files_descr": "Referenz"}, #, "skip_years": list(range(2005, 2021))
        # "DK": {"region_id": "DK", "file_encoding": "ISO-8859-1", "ignore_files_descr": "original"}, #,range(2009, 2024)
        # "EE": {"region_id": "EE", "file_encoding": "utf-8"},
        # "EL": {"region_id": "EL", "file_encoding": "utf-8", "multiple_crop_entries_sep": ",", "ignore_files_descr": "stables"},
        # "FI": {"region_id": "FI", "file_encoding": "utf-8"}, #, "skip_years": range(2009, 2023)
        # "FR/FR": {"region_id": "FR_FR", "file_encoding": "utf-8", "ignore_files_descr": "ILOTS_ANONYMES"},
        # "IE": {"region_id": "IE", "file_encoding": "utf-8", "organic_dict": {"Y": 1, "N": 0}},
        # "HR": {"region_id": "HR", "file_encoding": "utf-8", "pre_transformation_crs": 3765},
        # "HU": {"region_id": "HU", "file_encoding": "utf-8"},
        # "IT/EMR": {"region_id": "IT_EMR", "file_encoding": "utf-8", #"skip_years": range(2016, 2021),
        #            "organic_dict_year": {"2018": {0: 0, 1: 1}, "2019": {0: 0, 1: 1}, "2020": {0: 0, 1: 1},
        #                                   "2021": {"1": 0, "2": 2, "3": 1, "4": 0}, "2022": {"1": 0, "2": 2, "3": 1, "4": 0},
        #                                   "2023": {"1": 0, "2": 2, "3": 1, "4": 0}, "2024": {"1": 0, "2": 2, "3": 1, "4": 0}}},
        # "IT/MAR": {"region_id": "IT_MAR", "file_encoding": "utf-8"},
        # "IT/TOS": {"region_id": "IT_TOS", "file_encoding": "utf-8"},
        # "LT": {"region_id": "LT", "file_encoding": "ISO-8859-1"},
        # "LV": {"region_id": "LV", "file_encoding": "utf-8", "ignore_files_descr": "DATA"}, #, "skip_years": range(2019, 2024)
        # "NL": {"region_id": "NL", "file_encoding": "utf-8", "organic_dict": {"01": 1, "02": 2, "03": 2, "04": 2}}, #, "skip_years": range(2022, 2023)
        # "PT/PT": {"region_id": "PT_PT", "file_encoding": "utf-8"},
        # "PT/ALE": {"region_id": "PT_ALE", "file_encoding": "utf-8"},
        # "PT/ALG": {"region_id": "PT_ALG", "file_encoding": "utf-8"},
        # "PT/AML": {"region_id": "PT_AML", "file_encoding": "utf-8"},
        # "PT/CET": {"region_id": "PT_CET", "file_encoding": "utf-8"},
        # "PT/CEN": {"region_id": "PT_CEN", "file_encoding": "utf-8"},
        # "PT/CES": {"region_id": "PT_CES", "file_encoding": "utf-8"},
        # "PT/NOR": {"region_id": "PT_NOR", "file_encoding": "utf-8"},
        # "PT/NON": {"region_id": "PT_NON", "file_encoding": "utf-8"},
        # "PT/NOS": {"region_id": "PT_NOS", "file_encoding": "utf-8"},
        # "RO": {"region_id": "RO", "file_encoding": "utf-8"},
        # "SE": {"region_id": "SE", "file_encoding": "ISO-8859-1", "ignore_files_descr": "NOAPPL"}, ## With applicant ID
        # "SE/NOAPPL": {"region_id": "SE_NOAPPL", "file_encoding": "ISO-8859-1"}, ## Without applicant ID
        # "SI": {"region_id": "SI", "file_encoding": "utf-8", "organic_dict": {"E": 1, "P": 2}}, #range(2005, 2023)
        # "SK": {"region_id": "SK", "file_encoding": "utf-8"}, #"skip_years": [2018, 2019, 2020, 2021, 2022],
    }

    ## For france create a dictionary in a loop, because of the many subregions
    # FR_districts = pd.read_csv(r"data\vector\IACS\FR\region_code.txt")
    # FR_districts = list(FR_districts["code"])
    # for district in FR_districts:
    #     run_dict[f"FR/{district}"] = {
    #         "region_id": f"FR_{district}",
    #         "file_encoding": "utf-8",
    #         "col_translate_pth": f"data/tables/column_name_translations/FR_SUBREGIONS_column_name_translation_vector.xlsx",
    #         "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
    #         "col_transl_descr_overwrite": "FR"
    #         }

    ## For spain create a dictionary in a loop, because of the many subregions
    ES_districts = pd.read_csv(r"data\vector\IACS\ES\region_code.txt")
    ES_districts = list(ES_districts["code"])
    # ES_districts = ["CDB"]
    for district in ES_districts:
        run_dict[f"ES/{district}"] = {
            "region_id": f"ES_{district}",
            "file_encoding": "utf-8",
            "col_translate_pth": f"data/tables/column_name_translations/ES_column_name_translation.xlsx",
            "crop_class_pth": "data/tables/crop_classifications/ES_crop_classification_final.xlsx",
            "col_transl_descr_overwrite": "ES"
            }

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"] # country_code.replace(r"/", "_")
        col_translate_pth = os.path.join("data", "tables", "column_name_translations", f"{region_id}_column_name_translation.xlsx")
        crop_class_pth = os.path.join(CROP_CLASSIFICATION_FOLDER, f"{region_id}_crop_classification_final.xlsx")

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

        if "file_year_encoding" in run_dict[country_code]:
            file_year_encoding = run_dict[country_code]["file_year_encoding"]
        else:
            file_year_encoding = None

        ## Get list of all available files
        in_dir = os.path.join("data", "vector", "IACS", country_code)
        iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

        ## Exclude files that should be skipped
        if ignore_files_descr:
            iacs_files = [file for file in iacs_files if ignore_files_descr not in file]

        ## Get epsg code for input files that are not correctly defined in the files, e.g. in Croatia
        if "pre_transformation_crs" in run_dict[country_code]:
            pre_transformation_crs = run_dict[country_code]["pre_transformation_crs"]
        else:
            pre_transformation_crs = None

        ## Get organic dictionary if provided
        if "organic_dict" in run_dict[country_code]:
            organic_dict = run_dict[country_code]["organic_dict"]
        else:
            organic_dict = None

        if "organic_dict_year" in run_dict[country_code]:
            organic_dict_year = run_dict[country_code]["organic_dict_year"]
        else:
            organic_dict_year = None

        if "classify_on_year_dict" in run_dict[country_code]:
            classify_on_year_dict = run_dict[country_code]["classify_on_year_dict"]
        else:
            classify_on_year_dict = None

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
            # iacs_new_pth = rf"data\vector\IACS_EU_Land\{country_code}\GSA-{region_id}-{year}.gpkg"
            iacs_new_pth = os.path.join("data", "vector", "IACS_EU_Land", country_code, f"GSA-{region_id}-{year}.geoparquet")

            ## If an overwrite for the column translation is provided, it means that the columns in the
            ## column name translation table do not use the original region ID but another one
            if "col_transl_descr_overwrite" in run_dict[country_code]:
                region_id = run_dict[country_code]["col_transl_descr_overwrite"]

            ## If a file encoding dictionary for specific years is provided, fetch the current version here
            if file_year_encoding:
                if year in file_year_encoding:
                    file_encoding = file_year_encoding[year]
                else:
                    file_encoding = run_dict[country_code]["file_encoding"]
            else:
                file_encoding = run_dict[country_code]["file_encoding"]

            ## If a organic dictionary for specific years is provided, fetch the current version here
            if organic_dict_year:
                if year in organic_dict_year:
                    organic_dict = organic_dict_year[year]
                else:
                    organic_dict = None

            ## If the users wants to force the column that should be used for the classificatin, fetch it here
            if classify_on_year_dict:
                if year in classify_on_year_dict:
                    classify_on = classify_on_year_dict[year]
                else:
                    classify_on = "automatic"
            else:
                classify_on = "automatic"

            unify_column_names_in_vector_data(
                iacs_pth=iacs_pth,
                file_encoding=file_encoding,
                col_translate_pth=col_translate_pth,
                crop_class_pth=crop_class_pth,
                region_id=region_id,
                year=year,
                iacs_new_pth=iacs_new_pth,
                pre_transformation_crs=pre_transformation_crs,
                organic_dict=organic_dict,
                classify_on=classify_on
            )

    ####################################################################################################################

    ## Input for csv harmonization, e.g. in France there are accompanying csv files that provide information on the
    ## crop share per field block for 2007-2014

    ## Use  "col_translate_pth" and "crop_class_pth" to provide paths that deviate from the common naming pattern
    ## Use  "col_translate_pth" and "crop_class_pth" to provide paths that deviate from the common naming pattern
    run_dict = {
        # "EL": {
        #     "region_id": "EL",
        #     "file_encoding": "utf-8",
        #     "ignore_files_descr": "additional_information"
        # },
        # "HU": {
        #     "region_id": "HU",
        #     "file_encoding": "utf-8"
        # },
        # "PT/PT": {
        #     "region_id": "PT_PT",
        #     "file_encoding": "utf-8"},
        # "PT/ALE": {
        #     "region_id": "PT_ALE",
        #     "file_encoding": "utf-8"},
        # "PT/ALG": {
        #     "region_id": "PT_ALG",
        #     "file_encoding": "utf-8"},
        # "PT/AML": {
        #     "region_id": "PT_AML",
        #     "file_encoding": "utf-8"},
        # "PT/CET": {
        #     "region_id": "PT_CET",
        #     "file_encoding": "utf-8"},
        # "PT/CEN": {
        #     "region_id": "PT_CEN",
        #     "file_encoding": "utf-8"},
        # "PT/CES": {
        #     "region_id": "PT_CES",
        #     "file_encoding": "utf-8"},
        # "PT/NOR": {
        #     "region_id": "PT_NOR",
        #     "file_encoding": "utf-8"},
        # "PT/NON": {
        #     "region_id": "PT_NON",
        #     "file_encoding": "utf-8"},
        # "PT/NOS": {
        #     "region_id": "PT_NOS",
        #     "file_encoding": "utf-8"}
    }

    ## For france create a dictionary in a loop, because of the many subregions
    # FR_districts = pd.read_csv(r"data\vector\IACS\FR\region_code.txt")
    # FR_districts = list(FR_districts["code"])
    # for district in FR_districts:
    #     run_dict[f"FR/{district}"] = {
    #         "region_id": f"FR_{district}",
    #         "file_encoding": "utf-8",
    #         "col_translate_pth": f"data/tables/column_name_translations/FR_SUBREGIONS_column_name_translation_csv.xlsx",
    #         "crop_class_pth": "data/tables/crop_classifications/FR_SUBREGIONS_crop_classification_final.xlsx",
    #         "col_transl_descr_overwrite": "FR",
    #         "skip_years": [2007, 2008, 2009]
    #         }

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")
        col_translate_pth = os.path.join("data", "tables", "column_name_translations", f"{region_id}_column_name_translation.xlsx")
        crop_class_pth = os.path.join(CROP_CLASSIFICATION_FOLDER, f"{region_id}_crop_classification_final.xlsx")
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
        in_dir = os.path.join("data", "vector", "IACS", country_code)
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
            csv_new_pth = os.path.join("data", "vector", "IACS_EU_Land", country_code, f"GSA-{region_id}-{year}.csv")

            ## If an overwrite for the column translation is provided, it means that the columns in the
            ## column-name translation table do not use the original region ID but another one
            ## E.g. for France and Spain we use only one column-name translation table, although there are multiple sub-
            ## regions. Normally, the columns names in the table contain the region code, e.g. ES_ALA_YYYY,
            ## but because it is only one table for all, it contains only the ES_YYYY
            if "col_transl_descr_overwrite" in run_dict[country_code]:
                region_id = run_dict[country_code]["col_transl_descr_overwrite"]

            unify_column_names_in_vector_data(
                iacs_pth=csv_pth,
                file_encoding=file_encoding,
                col_translate_pth=col_translate_pth,
                crop_class_pth=crop_class_pth,
                region_id=region_id,
                year=year,
                iacs_new_pth=csv_new_pth,
                csv_sep=csv_sep
            )

    ####################################################################################################################
    ## Input for animal table harmonization

    run_dict = {
        # "DE/BRB": {"region_id": "DE_BRB"}
    }

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")
        col_translate_pth = os.path.join("data", "tables", f"{region_id}_column_name_translation_animals.xlsx")

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
        in_dir = os.path.join("data", "vector", "IACS", country_code)
        table_files = helper_functions.list_tables_files_in_dir(in_dir)

        ## Exclude files that should be skipped
        if ignore_files_descr:
            table_files = [file for file in table_files if ignore_files_descr not in file]

        ## Temporary, if you want to subset the list.
        # table_files = table_files[12:13]

        ## Loop over files to unify columns and classify crops
        for i, table_pth in enumerate(table_files):
            print(f"{i + 1}/{len(table_files)} - Processing - {table_pth}")
            year = helper_functions.get_year_from_path(table_pth)
            if int(year) in skip_years:
                print(f"Skipping year {year}")
                continue

            ## First create out path with original region ID
            ## We have to fetch the region ID for safety reason again, as it might have been overwritten later on
            region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")
            csv_new_pth = os.path.join("data", "vector", "IACS_EU_Land", country_code, f"IACS_animals-{region_id}-{year}.csv")

            unify_column_names_in_animal_data(
                iacs_animal_pth=table_pth,
                col_translate_pth=col_translate_pth,
                region_id=region_id,
                year=year,
                iacs_animal_new_pth=csv_new_pth
            )

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()
