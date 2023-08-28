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
from deep_translator import GoogleTranslator
## LingueeTranslator --> language specification is different. Words instead of abbreviations
## PonsTranslator
# from translate import Translator

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

COL_TRANSL_PTH = r"data\tables\AT_column_name_translation.xlsx"

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


def list_crop_names_ogr(in_dir, region_id, col_translate_pth, out_pth, encoding, ignore_files_descr=None):
    print("Derive list of unique crop names from IACS files.")

    os.environ['SHAPE_ENCODING'] = encoding

    ## Get list of IACS files
    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

    # Drop files that should be ignores
    if ignore_files_descr:
        iacs_files = [file for file in iacs_files if ignore_files_descr not in file]

    ## Open column translations
    tr_df = pd.read_excel(col_translate_pth)

    ## Loop over files to derive crop names from all files
    res_lst = []
    for path in iacs_files:
        year = helper_functions.get_year_from_path(path)
        print(f"Processing: {year} - {path}")

        ## Open file and layer
        file_name, file_extension = os.path.splitext(path)
        driver_dict = {
            ".gdb": "OpenFileGDB",
            ".geojson": "GeoJSON",
            ".gpkg": "GPKG",
            ".shp": "ESRI Shapefile"
        }
        driver = ogr.GetDriverByName(driver_dict[file_extension])
        ds = driver.Open(path, 0)
        lyr = ds.GetLayer(0)
        col_year = f"{region_id}_{year}"
        col_dict = dict(zip(tr_df["column_name"], tr_df[col_year]))

        if (type(col_dict["crop_name"]) == float) & (type(col_dict["crop_code"]) == float):
            print(F"No crop name or crop type column in {path}. Skipping.")
            continue

        ## Loop over features to derive crop names of current file
        for feat in lyr:
            if type(col_dict["crop_code"]) != float:
                crop_code = feat.GetField(col_dict["crop_code"])
            else:
                crop_code = ""
            if type(col_dict["crop_name"]) != float:
                crop_name = feat.GetField(col_dict["crop_name"])
            else:
                crop_name = ""
            res_lst.append((crop_code, crop_name))
        lyr.ResetReading()
        ds = None

    ## Get unique crop code-crop name combinations
    res_lst = list(set(res_lst))

    ## Turn into df and save to file
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
        ".shp": "ESRI Shapefile"
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
    num_ccodes = len(ccodes )

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
        cn_cols = ["crop_code"]
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
    df_cnames.dropna(inplace=True)
    # translator_en = Translator(provider="mymemory", to_lang="en", from_lang=from_lang) #['mymemory', 'microsoft', 'deepl', 'libre']
    # translator_de = Translator(provider="mymemory", to_lang="de", from_lang=from_lang)
    # df_cnames["crop_name_en"] = df_cnames["crop_name"].apply(translator_en.translate)
    # df_cnames["crop_name_de"] = df_cnames["crop_name"].apply(translator_de.translate)
    df_cnames["crop_name_de"] = df_cnames["crop_name"].apply(GoogleTranslator(source=from_lang, target='de').translate)
    df_cnames["crop_name_en"] = df_cnames["crop_name"].apply(GoogleTranslator(source=from_lang, target='en').translate)

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
    tr_df = pd.read_excel(r"data\tables\FR_SUBREGIONS_column_name_translation_csv.xlsx")
    ## Loop over subregions
    for sr in ["ARA", "BRC", "BRE", "COR", "CVL" "GRE", "HDF", "IDF", "NOR", "NOU", "OCC", "PDL", "PRO"]:

        csv_files = glob.glob(rf"{in_dir}\{sr}\**\*GROUPES-CULTURE*.csv")

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


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    run_dict = {
        # "BE/FLA": {
        #    "region_id": "BE_FLA",
        #    "from_lang": "nl",
        #    "eurocrops_pth": r"data\vector\EuroCrops\BE_FLA_2021\BE_VLG_2021_EC21.shp"
        # },
        # "AT": {
        #     "region_id": "AT",
        #     "from_lang": "de",
        #     "eurocrops_pth": r"data\vector\EuroCrops\AT_2021\AT_2021_EC21.shp"
        # },
        # "NL": {
        #     "region_id": "NL",
        #     "from_lang": "nl",
        #     "eurocrops_pth": r"data\vector\EuroCrops\NL_2020\NL_2020_EC21.shp"
        # },
        # "FI": {
        #     "region_id": "FI",
        #     "from_lang": "fi",
        #     "eurocrops_pth": False,
        #     "file_encoding": "ISO-8859-1"
        # },
        # "DK": {
        #     "region_id": "DK",
        #     "from_lang": "da",
        #     "eurocrops_pth": True,
        #     "file_encoding": "ISO-8859-1"
        # },
        # "HR": {
        #     "region_id": "HR",
        #     "from_lang": "hr",
        #     "eurocrops_pth": True,
        #     "file_encoding": "utf-8"
        # },
        # "LV": {
        #     "region_id": "LV",
        #     "from_lang": "lv",
        #     "eurocrops_pth": True,
        #     "file_encoding": "utf-8"
        # },
        # "SI": {
        #     "region_id": "SI",
        #     "from_lang": "sl",
        #     "eurocrops_pth": True,
        #     "file_encoding": "utf-8"
        # },
        # "SK": {
        #     "region_id": "SK",
        #     "from_lang": "sk",
        #     "eurocrops_pth": True,
        #     "file_encoding": "utf-8"
        # }
        # "FR/SUBREGIONS": {
        #     "special_function": get_french_values_from_csv,
        #     "region_id": "FR_SUBREGIONS",
        #     "from_lang": "fr",
        #     "eurocrops_pth": False,
        #     "file_encoding": "utf-8",
        # },
        "FR/FR": {
            "region_id": "FR_FR",
            "from_lang": "fr",
            "eurocrops_pth": True,
            "file_encoding": "utf-8",
            "ignore_files_descr": "ILOTS_ANONYMES"
        }
    }

    for country_code in run_dict:
        print(country_code)
        region_id = run_dict[country_code]["region_id"] # country_code.replace(r"/", "_")
        eurocrops_pth = run_dict[country_code]["eurocrops_pth"]
        encoding = run_dict[country_code]["file_encoding"]
        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None

        ## list_crop_names(), Debug mode, 2 files:
        ## start: Tue, 01 Aug 2023 13:56:06
        ## end: Tue, 01 Aug 2023 14:17:21

        if "special_function" in run_dict[country_code]:
            # call the function
            run_dict[country_code]["special_function"](
                in_dir=fr"data\vector\IACS\FR",
                out_pth=rf"data\tables\crop_names\FR_SUBREGIONS_unique_crop_names.csv")
        else:
            list_crop_names_ogr(
                in_dir=fr"data\vector\IACS\{country_code}",
                region_id=region_id,
                col_translate_pth=rf"data\tables\{region_id}_column_name_translation.xlsx",
                out_pth=rf"data\tables\crop_names\{region_id}_unique_crop_names.csv",
                encoding=encoding,
                ignore_files_descr=ignore_files_descr
            )
        ## Debug mode, 2 files
        ## start: Tue, 01 Aug 2023 14:19:10
        ## end: Tue, 01 Aug 2023 14:21:11

        ## Do this only if NO EuroCrops classification was provided on their github repo
        ## This derives it from the shapefiles
        # get_eurocrops_classification(
        #     eurocrops_pth=path/to/eurocrop_iacs_file_w_classification.shp,
        #     region_id=region_id,
        #     col_translate_pth=rf"data\tables\{country_code}_column_name_translation.xlsx",
        #     out_pth=rf"data\tables\crop_names\{region_id}_EuroCrops_classification.csv")

        if eurocrops_pth:
            match_crop_names_with_eurocrops_classification(
                crop_names_pth=rf"data\tables\crop_names\{region_id}_unique_crop_names.csv",
                eurocrops_cl_pth=rf"data\tables\crop_names\{region_id}_EuroCrops_classification.csv",
                from_lang=run_dict[country_code]["from_lang"],
                out_pth=rf"data\tables\crop_names\{region_id}_crop_names_w_EuroCrops_class.xlsx")
        else:
            translate_crop_names(
                crop_names_pth=rf"data\tables\crop_names\{region_id}_unique_crop_names.csv",
                from_lang=run_dict[country_code]["from_lang"],
                country_code=region_id,
                out_pth=rf"data\tables\crop_names\{region_id}_crop_names_w_translation.xlsx")

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
