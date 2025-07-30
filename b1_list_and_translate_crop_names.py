# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# This script uses the column_translation_table of each country to identify the correct columns and to extract the unique
# crop codes - crop names combinations for all years.
# If there is already a EuroCrops (EC) classification table, it will be used to match the unique crop name - crop code
# from the original file and classify them according to the EC classification. The EC classification table should
# be stored in the folder "data\tables\crop_names" with the following name XX_EuroCrops_classification.csv,
# where XX stands for the country abbreviation (for sub-datasets it should be XX_XXX_).
# If no EC classification table is provided, then the original crop names will be translated to English and German.
# The output will either be a table of the unique crop codes - crop names combinations classified into EC classific, or
# a table with translations for the unique crop codes - crop names combinations. They will be automatically saved to:
# data\tables\crop_names\XX_crop_names_w_EuroCrops_class.xlsx or
# data\tables\crop_names\XX_XXX_crop_names_w_translation.xlsx
# Both files should be used to manually classify missing crop names to EuroCrops classification. The final table should be
# stored in data\tables\crop_classifications\XX_crop_classification_final.xlsx

# If you want to run this script for a specific country, put an entry in the run_dict at the top of the main function.
# The run_dict key should be the country or country and subdivision abbreviations (for example, "DK" or "DE/THU). The
# item is another dictionary. In this dictionary, you should include the following keys:

# "region_id" - basically the main key (XX), but for XX/XXX changed into XX_XXX
# "from_lang" - input for GoogleTranslator function to indicate which language needs to be translated
# "file_encoding" - Encoding of the original GSA file
# "skip_list_crop_names" - [optional] use if the original GSA data do not contain crop names. Sometimes they come in separate tables
# "crop_names_pth" - [optional] the path to the separate table with crop names (see line above).
# "file_year_encoding" - [optional] use if specific years deviate from that encoding
# "ignore_file_descr" - [optional] use if there are other geospatial datasets in your folder that are not GSA data

# For example:
# "CZ" : {
#     "region_id": "DE_SAA",
#     "from_lang": "de",
#     "file_encoding": "utf-8",
#     "file_year_encoding": {"2023": "windows-1252"},
#     "ignore_files_descr": "Antrag",
#     "eurocrops_pth": False,
#     "skip_list_crop_names": True,
#     "file_encoding": "utf-8",
#     "crop_names_pth": os.path.join("data", "tables", "crop_names", "CY_unique_crop_names.csv")}

# To turn off/on the processing of a specific country, just comment/uncomment the specific line of the run_dict

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import pandas as pd
import geopandas as gpd
from osgeo import ogr
import glob
from deep_translator import GoogleTranslator
## LingueeTranslator --> language specification is different. Words instead of abbreviations
## PonsTranslator
# from translate import Translator

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def list_crop_names(in_dir, region_id, col_translate_pth, out_pth):
    ## Get list of IACS files
    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

    ## Open column translations
    tr_df = pd.read_excel(col_translate_pth)

    ## Loop over files to derive crop names from all files
    res_lst = []
    for pth in iacs_files:
        year = helper_functions.get_year_from_path(pth)
        print(f"Processing: {year} - {pth}")
        gdf = gpd.read_file(pth)
        col_year = f"{region_id}_{year}"
        col_dict = dict(zip(tr_df["column_name"], tr_df[col_year]))
        cols = [col_dict["crop_code"], col_dict["crop_name"]]

        crop_names = gdf[cols].drop_duplicates()
        res_lst.append(crop_names)

    ## Get unique crop code-crop name combinations
    ## Turn into df and save to file
    out_df = pd.concat(res_lst)
    out_df.drop_duplicates(inplace=True)
    out_df.sort_values(by=col_dict["crop_name"], inplace=True)

    out_df.to_csv(out_pth, index=False)


def list_crop_names_ogr(in_dir, region_id, col_translate_pth, out_pth, encoding, ignore_files_descr=None,
                        file_year_encoding=None, multiple_crop_entries_sep=False):
    print("Derive list of unique crop names from IACS files.")

    ## Get list of IACS files
    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

    # Drop files that should be ignored
    if ignore_files_descr:
        iacs_files = [file for file in iacs_files if ignore_files_descr not in file]

    ## Open column translations
    tr_df = pd.read_excel(col_translate_pth)

    ## Loop over files to derive crop names from all files
    res_lst = []
    for path in iacs_files:
        year = helper_functions.get_year_from_path(path)
        if file_year_encoding:
            if year in file_year_encoding:
                year_encoding = file_year_encoding[year]
                os.environ['SHAPE_ENCODING'] = year_encoding
            else:
                os.environ['SHAPE_ENCODING'] = encoding

        print(f"Processing: {year} - {path}")

        ## Open file and layer
        file_name, file_extension = os.path.splitext(path)
        driver_dict = {
            ".gdb": "OpenFileGDB",
            ".geojson": "GeoJSON",
            ".gpkg": "GPKG",
            ".shp": "ESRI Shapefile",
            ".geoparquet": "Parquet"
        }
        driver = ogr.GetDriverByName(driver_dict[file_extension])
        ds = driver.Open(path, 0)
        lyr = ds.GetLayer(0)
        col_year = f"{region_id}_{year}"
        col_dict = dict(zip(tr_df["column_name"], tr_df[col_year]))

        ## create a list of crop names and crop codes, so if multiple crop columns are provided (and separated by "|")
        ## then they can be looped over
        if type(col_dict["crop_name"]) != float:
            col_dict["crop_name"] = col_dict["crop_name"].split("|")
        if type(col_dict["crop_code"]) != float:
            col_dict["crop_code"] = col_dict["crop_code"].split("|")

        if (type(col_dict["crop_name"]) == float) & (type(col_dict["crop_code"]) == float):
            print(F"No crop name or crop type column in {path}. Skipping.")
            continue

        ## Loop over features to derive crop names of current file
        for feat in lyr:
            names = []
            codes = []
            if type(col_dict["crop_code"]) != float:
                for c_code in col_dict["crop_code"]:
                    if multiple_crop_entries_sep:
                        crop_codes = str(feat.GetField(c_code)).split(multiple_crop_entries_sep)
                        codes += crop_codes
                    else:
                        crop_code = feat.GetField(c_code)
                        codes.append(crop_code)
            else:
                crop_code = ""
                codes.append(crop_code)
            if type(col_dict["crop_name"]) != float:
                for c_name in col_dict["crop_name"]:
                    if multiple_crop_entries_sep:
                        crop_names = str(feat.GetField(c_name)).split(multiple_crop_entries_sep)
                        names += crop_names
                    else:
                        crop_name = feat.GetField(c_name)
                        names.append(crop_name)
            else:
                crop_name = ""
                names.append(crop_name)

            if len(names) == len(codes):
                for i, name in enumerate(names):
                    # res_lst.append((codes[i], name, year))
                    res_lst.append((codes[i], name))
            elif len(names) > len(codes):
                for i, name in enumerate(names):
                    # res_lst.append(("", name, year))
                    res_lst.append(("", name))
            elif len(codes) > len(names):
                for i, code in enumerate(codes):
                    # res_lst.append((code, "", year))
                    res_lst.append((code, ""))

        lyr.ResetReading()
        ds = None

    ## Get unique crop code-crop name combinations
    res_lst = list(set(res_lst))

    ## Turn into df and save to file
    # out_df = pd.DataFrame(res_lst, columns=["crop_code", "crop_name", "year"])
    out_df = pd.DataFrame(res_lst, columns=["crop_code", "crop_name"])
    out_df.sort_values(by="crop_name", inplace=True)
    out_df.to_csv(out_pth, index=False)


def get_eurocrops_classification(eurocrops_pth, region_id, col_translate_pth, out_pth):
    print("Get EuroCrops classification from their shapefile.")
    ## Open column translation df
    tr_df = pd.read_excel(col_translate_pth)

    ## Open EuroCrops Shapefile
    file_name, file_extension = os.path.splitext(eurocrops_pth)
    driver_dict = {
        ".gdb": "OpenFileGDB",
        ".geojson": "GeoJSON",
        ".gpkg": "GPKG",
        ".shp": "ESRI Shapefile",
        ".geoparquet": "Parquet"
    }
    driver = ogr.GetDriverByName(driver_dict[file_extension])
    ds = driver.Open(eurocrops_pth, 0)
    lyr = ds.GetLayer(0)
    col_eur = f"{region_id}_EUROCROPS"
    col_dict = dict(zip(tr_df["column_name"], tr_df[col_eur]))

    ## Loop over features to get EuroCrops classification
    res_lst = []
    for feat in lyr:
        crop_name = feat.GetField(col_dict["crop_name"])
        ec_trans_n = feat.GetField(col_dict["EC_trans_n"])
        ec_hcat_n = feat.GetField(col_dict["EC_hcat_n"])
        ec_hcat_c = feat.GetField(col_dict["EC_hcat_c"])
        res_lst.append((crop_name, ec_trans_n, ec_hcat_n, ec_hcat_c))
    lyr.ResetReading()
    ds = None

    ## Get unique crop code-crop name combinations
    res_lst = list(set(res_lst))

    ## Turn into df and save to file
    out_df = pd.DataFrame(res_lst, columns=["crop_name", "EC_trans_n", "EC_hcat_n", "EC_hcat_c"])
    out_df.sort_values(by="crop_name", inplace=True)
    out_df.to_csv(out_pth, index=False)


def match_crop_names_with_eurocrops_classification(crop_names_pth, eurocrops_cl_pth, from_lang, out_pth):
    print("Match crop names with EuroCrops classification and translate crop names.")

    ## Read input
    df_cnames = pd.read_csv(crop_names_pth)
    df_eucr = pd.read_csv(eurocrops_cl_pth)

    cnames = df_cnames["crop_name"].unique()
    num_cnames = len(cnames)
    ccodes  = df_cnames["crop_code"].unique()
    num_ccodes = len(ccodes)

    ## Translate crop names
    df_cnames.loc[df_cnames["crop_name"].isna(), "crop_name"] = ""
    # translator_en = Translator(provider="mymemory", to_lang="en", from_lang=from_lang) #['mymemory', 'microsoft', 'deepl', 'libre']
    # translator_de = Translator(provider="mymemory", to_lang="de", from_lang=from_lang)
    # df_cnames["crop_name_en"] = df_cnames["crop_name"].apply(translator_en.translate)
    # df_cnames["crop_name_de"] = df_cnames["crop_name"].apply(translator_de.translate)
    df_cnames["crop_name_de"] = df_cnames["crop_name"].apply(GoogleTranslator(source=from_lang, target='de').translate)
    df_cnames["crop_name_en"] = df_cnames["crop_name"].apply(GoogleTranslator(source=from_lang, target='en').translate)


    ## Rename columns in EuroCrops classification
    col_dict = {
        "original_name": "crop_name",
        "translated_name": "EC_trans_n",
        "HCAT3_name": "EC_hcat_n",
        "HCAT3_code": "EC_hcat_c"
    }

    df_eucr.rename(columns=col_dict, inplace=True)
    df_eucr.drop_duplicates(subset="crop_name", inplace=True)

    ## Merge tables
    ec_cols = ["crop_name", "EC_trans_n", "EC_hcat_n", "EC_hcat_c"]
    if (num_cnames > 1):
        df_match = pd.merge(df_cnames, df_eucr[ec_cols], how="outer", on="crop_name")
        df_match.sort_values(by="crop_name", inplace=True)
    elif num_ccodes > num_cnames:
        # cn_cols = ["crop_code"]
        cn_cols = ["crop_code"] #, "year"
        datatype = df_cnames["crop_code"].dtype
        df_eucr["original_code"] = df_eucr["original_code"].astype(datatype)
        df_match = pd.merge(df_cnames[cn_cols], df_eucr[ec_cols + ["original_code"]], how="outer", left_on="crop_code", right_on="original_code")
        df_match.sort_values(by="crop_code", inplace=True)
        df_match.loc[df_match["crop_name"].isna(), "crop_name"] = ""
        df_match["crop_name_de"] = df_match["crop_name"].apply(GoogleTranslator(source=from_lang, target='de').translate)
        df_match["crop_name_en"] = df_match["crop_name"].apply(GoogleTranslator(source=from_lang, target='en').translate)
        df_match = df_match[["crop_code", "crop_name", "crop_name_de", "crop_name_en", "EC_trans_n", "EC_hcat_n", "EC_hcat_c"]]
    else:
        df_match = pd.merge(df_cnames, df_eucr[ec_cols], how="outer", on="crop_name")
        df_match.sort_values(by="crop_name", inplace=True)

    ## Write out
    df_match.to_excel(out_pth, index=False)


def translate_crop_names(crop_names_pth, from_lang, country_code, out_pth):
    print("Translate crop names.")

    ## Read input
    df_cnames = pd.read_csv(crop_names_pth)

    ## Translate crop names
    df_cnames.dropna(inplace=True, subset=["crop_name"])
    df_cnames.drop_duplicates(subset=["crop_code", "crop_name"], inplace=True)
    # translator_en = Translator(provider="mymemory", to_lang="en", from_lang=from_lang) #['mymemory', 'microsoft', 'deepl', 'libre']
    # translator_de = Translator(provider="mymemory", to_lang="de", from_lang=from_lang)
    # df_cnames["crop_name_en"] = df_cnames["crop_name"].apply(translator_en.translate)
    # df_cnames["crop_name_de"] = df_cnames["crop_name"].apply(translator_de.translate)
    df_cnames["crop_name_de"] = df_cnames["crop_name"].apply(GoogleTranslator(source=from_lang, target='de').translate)
    df_cnames["crop_name_en"] = df_cnames["crop_name"].apply(GoogleTranslator(source=from_lang, target='en').translate)
    df_cnames["crop_name_dk"] = df_cnames["crop_name"].apply(GoogleTranslator(source=from_lang, target='da').translate)

    ## Merge tables
    df_cnames.sort_values(by="crop_name", inplace=True)
    df_cnames["EC_trans_n"] = ""
    df_cnames["EC_hcat_n"] = ""
    df_cnames["EC_hcat_c"] = ""
    df_cnames["country_code"] = country_code

    ## Write out
    df_cnames.to_excel(out_pth, index=False)

def get_french_values_from_csv(in_dir, out_pth):
    res_lst = []

    ## Open column translations
    tr_df = pd.read_excel(os.path.join("data", "tables", "FR_SUBREGIONS_column_name_translation_csv.xlsx"))
    ## Loop over subregions
    for sr in ["ARA", "BRC", "BRE", "COR", "CVL" "GRE", "HDF", "IDF", "NOR", "NOU", "OCC", "PDL", "PRO"]:

        # csv_files = glob.glob(rf"{in_dir}\{sr}\**\*GROUPES-CULTURE*.csv")
        csv_files = glob.glob(os.path.join(in_dir, sr, "**", "*GROUPES-CULTURE*.csv"), recursive=True)

        ## Loop over files to derive crop names from all files
        for path in csv_files:
            year = helper_functions.get_year_from_path(path)
            print(f"Processing: {year} - {path}")

            df = pd.read_csv(path, sep=";")

            col_year = f"FR_{year}"
            col_dict = dict(zip(tr_df["column_name"], tr_df[col_year]))
            if (type(col_dict["crop_name"]) == float) & (type(col_dict["crop_code"]) == float):
                print(F"No crop name or crop type column in {path}. Skipping.")
                continue
            col_dict = {value: key for (key, value) in col_dict.items() if value != float}

            df.rename(columns=col_dict, inplace=True)

            cols = ["crop_code", "crop_name"]
            for col in cols:
                if col not in df.columns:
                    df[col] = ""

            res_lst.append(df[cols])

    ## Turn into one df and save to file and get unique crop_code-name combinations
    out_df = pd.concat(res_lst)
    out_df.drop_duplicates(inplace=True)
    out_df.sort_values(by="crop_name", inplace=True)
    out_df.to_csv(out_pth, index=False)


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

    # check whether the `crop_names` exists already in the file structure and if not create it
    crop_names_path = os.path.join("data", "tables", "crop_names")
    os.makedirs(crop_names_path, exist_ok=True)

    ## To turn off/on the processing of a specific country, just comment/uncomment the specific line

    run_dict = {
        "AT": {
            "region_id": "AT",
            "from_lang": "de",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "BE/FLA": {
           "region_id": "BE_FLA",
           "from_lang": "nl",
           "eurocrops_pth":os.path.join("data", "vector", "EuroCrops", "BE_FLA_2021", "BE_VLG_2021_EC21.shp")
        },
        "BE/WAL": {
           "region_id": "BE_WAL",
           "from_lang": "fr",
           "eurocrops_pth": False,
           "file_encoding": "ISO-8859-1"
        },
        "BG": {
            "region_id": "BG",
            "from_lang": "bg",
            "eurocrops_pth": False,
            "file_encoding": "windows-1251"},
        "CY/APPL": {
            "region_id": "CY_APPL",
            "from_lang": "el",
            "eurocrops_pth": False,
            "skip_list_crop_names": True,
            "file_encoding": "utf-8",
            "crop_names_pth": os.path.join("data", "tables", "crop_names", "CY_unique_crop_names.csv"),
        },
        "CZ": {
            "region_id": "CZ",
            "from_lang": "cs",
            "file_encoding": "ISO-8859-1",
            "eurocrops_pth": False,
            "ignore_files_descr": "IACS_Czechia"},
        "DE/BRB": {
            "region_id": "DE_BRB",
            "from_lang": "de",
            "file_encoding": "ISO-8859-1",
            "eurocrops_pth": True},
        "DE/LSA": {
            "region_id": "DE_LSA",
            "from_lang": "de",
            "file_encoding": "utf-8",
            "eurocrops_pth": True,
            "ignore_files_descr": "other_files"},
        "DE/NRW": {
            "region_id": "DE_NRW",
            "from_lang": "de",
            "file_encoding": "ISO-8859-1",
            "ignore_files_descr": "HIST",
            "eurocrops_pth": True},
        "DE/SAT": {
            "region_id": "DE_SAT",
            "from_lang": "de",
            "file_encoding": "utf-8",
            "ignore_files_descr": "Referenz",
            "eurocrops_pth": False},
        "DE/SAA": {
            "region_id": "DE_SAA",
            "from_lang": "de",
            "file_encoding": "utf-8",
            "file_year_encoding": {"2023": "windows-1252"},
            "ignore_files_descr": "Antrag",
            "eurocrops_pth": False},
        "DE/THU": {
            "region_id": "DE_THU",
            "from_lang": "de",
            "file_encoding": "utf-8",
            "ignore_files_descr": "ZN",
            "eurocrops_pth": False},
        "DK": {
            "region_id": "DK",
            "from_lang": "da",
            "eurocrops_pth": True,
            "file_encoding": "ISO-8859-1"
        },
        "EE": {
            "region_id": "EE",
            "from_lang": "et",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "EL": {
            "region_id": "EL",
            "from_lang": "el",
            "eurocrops_pth": False,
            "skip_list_crop_names": True,
            "file_encoding": "utf-8",
            "crop_names_pth": os.path.join("data", "tables", "crop_names", "EL_unique_crop_names.csv")
        },
        "ES": {
           "region_id": "ES",
           "from_lang": "es",
           "eurocrops_pth": False,
            "file_encoding": "utf-8"
            },
        "FI": {
            "region_id": "FI",
            "from_lang": "fi",
            "eurocrops_pth": False,
            "file_encoding": "ISO-8859-1"
        },
        "FR/SUBREGIONS": {
            "fr_special_function": get_french_values_from_csv,
            "region_id": "FR_SUBREGIONS",
            "from_lang": "fr",
            "eurocrops_pth": False,
            "file_encoding": "utf-8",
        },
        "FR/FR": {
            "region_id": "FR_FR",
            "from_lang": "fr",
            "eurocrops_pth": True,
            "file_encoding": "utf-8",
            "ignore_files_descr": "ILOTS_ANONYMES"
        },
        "HR": {
            "region_id": "HR",
            "from_lang": "hr",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "HU": {
            "region_id": "HU",
            "from_lang": "hu",
            "eurocrops_pth": False,
            "skip_list_crop_names": True,
            "file_encoding": "utf-8",
            "crop_names_pth": os.path.join("data", "tables", "crop_names", "HU_unique_crop_names.csv")
        },
        "IE": {
            "region_id": "IE",
            "from_lang": "en",
            "eurocrops_pth": False,
            "file_encoding": "utf-8"
        },
        "IT/EMR": {
            "region_id": "IT_EMR",
            "from_lang": "it",
            "eurocrops_pth": False,
            "file_encoding": "utf-8"
        },
        "IT/MAR": {
            "region_id": "IT_MAR",
            "from_lang": "it",
            "eurocrops_pth": False,
            "file_encoding": "utf-8"
        },
        "IT/TOS": {
            "region_id": "IT_TOS",
            "from_lang": "it",
            "eurocrops_pth": False,
            "file_encoding": "utf-8"
        },
        "LV": {
            "region_id": "LV",
            "from_lang": "lv",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "NL": {
            "region_id": "NL",
            "from_lang": "nl",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/ALE": {
            "region_id": "PT_ALE",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/ALG": {
            "region_id": "PT_ALG",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/AML": {
            "region_id": "PT_AML",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/CE": {
            "region_id": "PT_CE",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/CEN": {
            "region_id": "PT_CEN",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/CES": {
            "region_id": "PT_CES",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/NO": {
            "region_id": "PT_NO",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/NON": {
            "region_id": "PT_NON",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/NOS": {
            "region_id": "PT_NOS",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "PT/PT": {
            "region_id": "PT_PT",
            "from_lang": "pt",
            "eurocrops_pth": True,
            "file_encoding": "utf-8",
            "pt_special_function": pt_combine_crop_codes_with_crop_names
        },
        "RO": {
            "region_id": "RO",
            "from_lang": "ro",
            "eurocrops_pth": False,
            "file_encoding": "utf-8"
        },
        "SE": {
           "region_id": "SE",
           "from_lang": "sv",
           "eurocrops_pth": True,
            "file_encoding": "ISO-8859-1"
        },
        "SI": {
            "region_id": "SI",
            "from_lang": "sl",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        },
        "SK": {
            "region_id": "SK",
            "from_lang": "sk",
            "eurocrops_pth": True,
            "file_encoding": "utf-8"
        }
    }

    ## For spain create a dictionary in a loop, because of the many subregions
    ES_districts = pd.read_csv(os.path.join("data", "vector", "IACS", "ES", "region_code.txt"))
    ES_districts = list(ES_districts["code"])
    for district in ES_districts:
        run_dict[f"ES/{district}"] = {
            "region_id": f"ES_{district}",
            "from_lang": "es",
            "eurocrops_pth": False,
            "file_encoding": "utf-8",
            "col_translate_pth": os.path.join("data", "tables", "IACS", "column_name_translations", "ES_column_name_translation.xlsx")
        }

    ## Loop through tasks in run_dict
    for country_code in run_dict:
        print(country_code)
        region_id = run_dict[country_code]["region_id"] # country_code.replace(r"/", "_")
        eurocrops_pth = run_dict[country_code]["eurocrops_pth"]
        encoding = run_dict[country_code]["file_encoding"]

        if "skip_list_crop_names" in run_dict[country_code]:
            skip_list_crop_names = run_dict[country_code]["skip_list_crop_names"]
        else:
            skip_list_crop_names = False

        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None

        if "file_year_encoding" in run_dict[country_code]:
            file_year_encoding = run_dict[country_code]["file_year_encoding"]
        else:
            file_year_encoding = None

        ## list_crop_names(), Debug mode, 2 files:
        ## start: Tue, 01 Aug 2023 13:56:06
        ## end: Tue, 01 Aug 2023 14:17:21

        if not skip_list_crop_names:
            if "fr_special_function" in run_dict[country_code]:
                # call the function
                run_dict[country_code]["fr_special_function"](
                    in_dir = os.path.join("data", "vector", "IACS", "FR"),
                    out_pth = os.path.join("data", "tables", "crop_names", "FR_SUBREGIONS_unique_crop_names.csv"))
            else:
                list_crop_names_ogr(
                    in_dir = os.path.join("data", "vector", "IACS", country_code),
                    region_id=region_id,
                    col_translate_pth = os.path.join("data", "tables", "column_name_translations", f"{region_id}_column_name_translation.xlsx"),
                    out_pth = os.path.join("data", "tables", "crop_names", f"{region_id}_unique_crop_names.csv"),
                    encoding=encoding,
                    file_year_encoding=file_year_encoding,
                    ignore_files_descr=ignore_files_descr
                )

            if "pt_special_function" in run_dict[country_code]:
                # call the function
                run_dict[country_code]["pt_special_function"](
                    crop_codes_pth = os.path.join("data", "tables", "crop_names", "PT_PT_unique_crop_names.csv"),
                    crop_names_pth = os.path.join("data", "vector", "IACS", "PT", "Crops.xlsx")
                   )

        ## Debug mode, 2 files
        ## start: Tue, 01 Aug 2023 14:19:10
        ## end: Tue, 01 Aug 2023 14:21:11

        ## Do this only if NO EuroCrops classification was provided on their github repo but a shapefile
        ## This derives the classification from the shapefile
        # # get_eurocrops_classification(
        # #     eurocrops_pth=path/to/eurocrop_iacs_file_w_classification.shp,
        # #     region_id=region_id,
        # #     col_translate_pth=rf"data\tables\column_name_translations\{country_code}_column_name_translation.xlsx",
        # #     out_pth=rf"data\tables\crop_names\{region_id}_EuroCrops_classification.csv")

        if "crop_names_pth" in run_dict[country_code]:
            crop_names_pth = run_dict[country_code]["crop_names_pth"]
        else:
            crop_names_pth = os.path.join("data", "tables", "crop_names", f"{region_id}_unique_crop_names.csv")

        if eurocrops_pth:
            match_crop_names_with_eurocrops_classification(
                crop_names_pth=crop_names_pth,
                eurocrops_cl_pth = os.path.join("data", "tables", "crop_names", f"{region_id}_EuroCrops_classification.csv"),
                from_lang = run_dict[country_code]["from_lang"],
                out_pth = os.path.join("data", "tables", "crop_names", f"{region_id}_crop_names_w_EuroCrops_class.xlsx"))
        else:
            translate_crop_names(
                crop_names_pth=crop_names_pth,
                from_lang=run_dict[country_code]["from_lang"],
                country_code=region_id,
                out_pth = os.path.join("data", "tables", "crop_names", f"{region_id}_crop_names_w_translation.xlsx"))

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()