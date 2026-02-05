# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
# os.environ['GDAL_DATA'] = os.path.join(f'{os.sep}'.join(sys.executable.split(os.sep)[:-1]), 'Library', 'share', 'gdal')
os.environ['PYDEVD_USE_CYTHON'] = 'NO'
import time
import pandas as pd
import geopandas as gpd
import numpy as np

import sys
script_dir = abspath(__file__)
project_root = dirname(script_dir)
sys.path.append(project_root)
from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

COL_NAMES_FOLDER = os.path.join("data", "tables", "column_names")
CROP_CLASSIFICATION_FOLDER = os.path.join("data", "tables", "crop_classifications")
_TRANSLATION_CACHE = {}

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def truncate_coord(value, p):
    """Truncate coordinate to p decimal places (no rounding)."""
    factor = 10 ** p
    return np.floor(value * factor) / factor

def get_translation_df(col_translate_pth):
    if col_translate_pth not in _TRANSLATION_CACHE:
        _TRANSLATION_CACHE[col_translate_pth] = pd.read_excel(col_translate_pth, engine="openpyxl")
    return _TRANSLATION_CACHE[col_translate_pth]

def check_uniqueness_of_field_ids_duplicates_and_non_geometries(iacs_pth, file_encoding, col_translate_pth, region_id, year, csv_sep=","):
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


    root, ext = os.path.splitext(iacs_pth)
    print(f"Check if field ID is truly unique and check for non-geometries and duplicate geometries.")

    ## Open files
    print("Reading GSA data:")

    if ext in ['.gpkg', '.gdb', '.shp', '.geojson']:
        iacs = gpd.read_file(iacs_pth, encoding=file_encoding)
    elif ext in ['.geoparquet']:
        iacs = gpd.read_parquet(iacs_pth)
    elif ext in ['.csv']:
        iacs = pd.read_csv(iacs_pth, sep=csv_sep)
    print("Reading Translation table.")
    tr_df_orig = pd.read_csv(col_translate_pth) #get_translation_df(col_translate_pth)

    nrows_in = len(iacs)
    for col in iacs.columns:
        n_unique_col = len(iacs[col].unique())
        print("Num. input features:", nrows_in, f"Num. unique values {col}:", n_unique_col)

    ## Optional: Subset the columns that should be in the final file
    print("Unifying column names.")
    tr_df = tr_df_orig.loc[tr_df_orig["prelim"] == 1].copy()
    del tr_df_orig

    ## Create a dictionary that renames the original assumed unique ID to "field_id"
    col_year = f"{region_id}_{year}"
    col_dict = dict(zip(tr_df.loc[tr_df[col_year].notna(), col_year], tr_df.loc[tr_df[col_year].notna(), "column_name"]))
    col_dict = {k: v for k, v in col_dict.items() if v == "field_id"}

    ## Create a reversed dictionary to be able to retrieve original field_id name
    col_dict_rev = {v: k for k, v in col_dict.items()}
    iacs.rename(columns=col_dict, inplace=True)

    ## Remove duplicate geometries and non geometries
    if "field_id" in iacs.columns:
        unique_fids_in = len(iacs["field_id"].unique())
    iacs = helper_functions.drop_non_geometries(iacs)
    nrows_non_geom_clean= len(iacs)
    iacs = helper_functions.remove_geometry_duplicates(iacs)
    nrows_dup_geom_clean = len(iacs)

    out_txt_pth = os.path.join("data", "vector", "IACS", "countries_with_non-unique_field_ids.txt")

    if "field_id" not in col_dict.values():
        print("No 'field_id' provided.")

        ## Get modification date of input data
        mod_date = os.path.getmtime(iacs_pth)
        pretty_date = time.strftime("%d %b %Y %H:%M:%S", time.localtime(mod_date))
        txt = (f"{region_id}\n{pretty_date} - {iacs_pth}\n\t"
               f"No field ID in original file.\n\t"
               f"Number of input features: {nrows_in}\n\t"
               f"Number of features after dropping non-geometries: {nrows_non_geom_clean}\n\t"
               f"Number of features after dropping duplicate geometries: {nrows_dup_geom_clean}\n\t")
        with open(out_txt_pth, "a") as file:
            file.write(txt)

    else:

        unique_fids_clean = len(iacs["field_id"].unique())
        with open(out_txt_pth, "a") as file:
            ## Get modification date of input data
            mod_date = os.path.getmtime(iacs_pth)
            pretty_date = time.strftime("%d %b %Y %H:%M:%S", time.localtime(mod_date))
            txt = (f"{region_id}\n{pretty_date} - {iacs_pth}\n\t"
                   f"Number of input features: {nrows_in}\n\t"
                   f"Number of input unique IDs: {unique_fids_in}\n\t"
                   f"Number of features after dropping non-geometries: {nrows_non_geom_clean}\n\t"
                   f"Number of features after dropping duplicate geometries: {nrows_dup_geom_clean}\n\t"
                   f"Number of unique IDs after cleaning: {unique_fids_clean}\n\t"
                   f"Field ID column: {col_dict_rev['field_id']}\n\n")
            file.write(txt)

    del iacs
    del tr_df


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## Input for uniquenss check

    ## To turn off/on the check for a specific country, just comment/uncomment the specific line

    run_dict = {
        "AT": {"switch": "off", "region_id": "AT", "file_encoding": "utf-8", "ignore_files_descr": "temp"},
        "BE/FLA": {"switch": "off", "region_id": "BE_FLA", "file_encoding": "utf-8", "file_year_encoding": {"2020": "ISO-8859-1"}},
        "BE/WAL": {"switch": "off", "region_id": "BE_WAL", "file_encoding": "utf-8"},
        "BG": {"switch": "off", "region_id": "BG", "file_encoding": "utf-8", "ignore_files_descr": "LPIS"},
        "CY": {"switch": "off", "region_id": "CY", "file_encoding": "utf-8", "ignore_files_descr": "LPIS"},
        "CZ": {"switch": "off", "region_id": "CZ", "file_encoding": "utf-8", "ignore_files_descr": "IACS_Czechia"},
        "DE/BRB": {"switch": "off", "region_id": "DE_BRB", "file_encoding": "ISO-8859-1"}, #,
        "DE/LSA": {"switch": "off", "region_id": "DE_LSA", "file_encoding": "utf-8", "ignore_files_descr": "other_files"},
        "DE/MWP": {"switch": "off", "region_id": "DE_MWP", "file_encoding": "utf-8", "ignore_files_descr": "TI_Original"},
        "DE/NRW": {"switch": "off", "region_id": "DE_NRW", "file_encoding": "ISO-8859-1", "ignore_files_descr": "HIST"},
        "DE/SAA": {"switch": "off", "region_id": "DE_SAA", "file_encoding": "utf-8", "file_year_encoding": {"2023": "windows-1252"},
            "ignore_files_descr": "Antrag"},
        "DE/SAT": {"switch": "off", "region_id": "DE_SAT", "file_encoding": "utf-8", "ignore_files_descr": "Referenz"}, #, "skip_years": list(range(2005, 2021))
        "DK": {"switch": "off", "region_id": "DK", "file_encoding": "ISO-8859-1", "ignore_files_descr": "original"}, #,range(2009, 2024)
        "EE": {"switch": "off", "region_id": "EE", "file_encoding": "utf-8"},
        "EL": {"switch": "on", "region_id": "EL", "file_encoding": "utf-8", "multiple_crop_entries_sep": ",", "ignore_files_descr": "stables"},
        "FI": {"switch": "off", "region_id": "FI", "file_encoding": "utf-8"}, #, "skip_years": range(2009, 2024)
        "FR/FR": {"switch": "off", "region_id": "FR_FR", "file_encoding": "utf-8", "ignore_files_descr": "prepared_data",
                  "skip_years":[2019, 2020, 2021,2022, 2023]},
        # "HR": {"switch": "off", "region_id": "HR", "file_encoding": "utf-8", "pre_transformation_crs": 3765},
        "HU": {"switch": "off", "region_id": "HU", "file_encoding": "utf-8"},
        "IE": {"switch": "off", "region_id": "IE", "file_encoding": "utf-8", "organic_dict": {"Y": 1, "N": 0}},
        "IT/EMR": {"switch": "off", "region_id": "IT_EMR", "file_encoding": "utf-8", #"skip_years": range(2016, 2021),
                   "organic_dict_year": {"2018": {0: 0, 1: 1}, "2019": {0: 0, 1: 1}, "2020": {0: 0, 1: 1},
                                          "2021": {"1": 0, "2": 2, "3": 1, "4": 0}, "2022": {"1": 0, "2": 2, "3": 1, "4": 0},
                                          "2023": {"1": 0, "2": 2, "3": 1, "4": 0}, "2024": {"1": 0, "2": 2, "3": 1, "4": 0}}},
        "IT/MAR": {"switch": "off", "region_id": "IT_MAR", "file_encoding": "utf-8"},
        "IT/TOS": {"switch": "off", "region_id": "IT_TOS", "file_encoding": "utf-8"},
        # "LT": {"switch": "off", "region_id": "LT", "file_encoding": "ISO-8859-1", "skip_years":[2024]},
        "LV": {"switch": "off", "region_id": "LV", "file_encoding": "utf-8", "ignore_files_descr": "DATA"}, #, "skip_years": range(2019, 2024)
        "NL": {"switch": "off", "region_id": "NL", "file_encoding": "utf-8",
               "organic_dict": {"01": 1, "02": 2, "03": 2, "04": 2}, "skip_years": range(2024, 2026)}, #,
        "PL": {"switch": "off", "region_id": "PL", "file_encoding": "utf-8", "ignore_files_descr": "GSAA_Poland"},
        "PT/PT": {"switch": "off", "region_id": "PT_PT", "file_encoding": "utf-8"},
        "PT/ALE": {"switch": "off", "region_id": "PT_ALE", "file_encoding": "utf-8", "skip_years": list(range(2016))+list(range(2017, 2020))},
        "PT/ALG": {"switch": "off", "region_id": "PT_ALG", "file_encoding": "utf-8"},
        "PT/AML": {"switch": "off", "region_id": "PT_AML", "file_encoding": "utf-8"},
        "PT/CET": {"switch": "off", "region_id": "PT_CET", "file_encoding": "utf-8"},
        "PT/CEN": {"switch": "off", "region_id": "PT_CEN", "file_encoding": "utf-8"},
        "PT/CES": {"switch": "off", "region_id": "PT_CES", "file_encoding": "utf-8"},
        "PT/NOR": {"switch": "off", "region_id": "PT_NOR", "file_encoding": "utf-8"},
        "PT/NON": {"switch": "off", "region_id": "PT_NON", "file_encoding": "utf-8"},
        "PT/NOS": {"switch": "off", "region_id": "PT_NOS", "file_encoding": "utf-8"},
        "RO": {"switch": "off", "region_id": "RO", "file_encoding": "utf-8"},
        "SE": {"switch": "off", "region_id": "SE", "file_encoding": "ISO-8859-1", "ignore_files_descr": "NOAPPL"}, ## With applicant ID
        "SE/NOAPPL": {"switch": "off", "region_id": "SE_NOAPPL", "file_encoding": "ISO-8859-1"}, ## Without applicant ID
        "SI": {"switch": "off", "region_id": "SI", "file_encoding": "utf-8", "organic_dict": {"E": 1, "P": 2}}, #range(2005, 2023)
        "SK": {"switch": "off", "region_id": "SK", "file_encoding": "utf-8", "ignore_files_descr":"no_crop"}, #"skip_years": [2018, 2019, 2020, 2021, 2022],
    }

    ## For france create a dictionary in a loop, because of the many subregions
    FR_districts = pd.read_csv(os.path.join("data", "vector", "IACS", "FR", "region_code.txt"))
    FR_districts = list(FR_districts["code"])
    for district in FR_districts:
        run_dict[f"FR/{district}"] = {
            "switch": "off",
            "region_id": f"FR_{district}",
            "file_encoding": "utf-8",
            "col_translate_pth": os.path.join("data", "tables", "column_name_translations",
                                              "FR_SUBREGIONS_column_name_translation_vector.csv"),
            "crop_class_pth": os.path.join("data", "tables", "crop_classifications", "FR_SUBREGIONS_crop_classification_final.xlsx"),
            "col_transl_descr_overwrite": "FR"
            }

    ## For spain create a dictionary in a loop, because of the many subregions
    ES_districts = pd.read_csv(os.path.join("data", "vector", "IACS", "ES", "region_code.txt"))
    ES_districts = list(ES_districts["code"])
    for district in ES_districts:
        run_dict[f"ES/{district}"] = {
            "switch": "off",
            "region_id": f"ES_{district}",
            "file_encoding": "utf-8",
            "col_translate_pth": os.path.join("data", "tables", "column_name_translations",
                                              "ES_column_name_translation.csv"),
            "col_transl_descr_overwrite": "ES"
            }

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        switch = run_dict[country_code].get("switch", "off").lower()
        if switch != "on":
            continue

        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"] # country_code.replace(r"/", "_")
        col_translate_pth = os.path.join("data", "tables", "column_name_translations",
                                         f"{region_id}_column_name_translation.csv")

        ## If the file naming of the columns translation and the crop classificaiton table deviate, then correct them
        if "col_translate_pth" in run_dict[country_code]:
            col_translate_pth = run_dict[country_code]["col_translate_pth"]

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

        ## Temporary, if you want to subset the list.
        # iacs_files = iacs_files[12:13]

        ## Loop over files to check if unique IDs are indeed unique
        for i, iacs_pth in enumerate(iacs_files):
            print(f"{i + 1}/{len(iacs_files)} - Processing - {iacs_pth}")
            year = helper_functions.get_year_from_path(iacs_pth)
            if int(year) in skip_years:
                print(f"Skipping year {year}")
                continue

            ## First create out path with original region ID
            ## We have to fetch the region ID for safety reason again, as it might have been overwritten in
            region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")

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

            check_uniqueness_of_field_ids_duplicates_and_non_geometries(
                iacs_pth=iacs_pth,
                file_encoding=file_encoding,
                col_translate_pth=col_translate_pth,
                region_id=region_id,
                year=year
            )


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()
