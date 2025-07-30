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
import numpy as np

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)



COL_NAMES_FOLDER = os.path.join("data", "tables","column_names")
CROP_CLASSIFICATION_FOLDER = os.path.join("data", "tables", "crop_classifications")

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## Input for geodata harmonization (in some cases, e.g. France or Portugal,
    ## some csv file have also to be harmonized. See below)

    ## To turn off/on the processing of a specific country, just comment/uncomment the specific line

    run_dict = {
        "BE/FLA": {"region_id": "BE_FLA"},
        "AT": {"region_id": "AT", "crop_class_pth": "AT_crop_classification_final_INSPIRE.xlsx"},
        "DK": {"region_id": "DK"},
        "SI": {"region_id": "SI"},
        "NL": {"region_id": "NL"},
        "FI": {"region_id": "FI"},
        "LV": {"region_id": "LV"},
        "SK": {"region_id": "SK"},
        "FR/FR": {"region_id": "FR_FR"},
        "FR/SUBREGIONS": {"region_id": "FR_SUBREGIONS"},
        "PT/PT": {"region_id": "PT_PT"},
        "PT/ALE": {"region_id": "PT_ALE"},
        "PT/ALG": {"region_id": "PT_ALG"},
        "PT/AML": {"region_id": "PT_AML"},
        "PT/CE": {"region_id": "PT_CE"},
        "PT/CEN": {"region_id": "PT_CEN"},
        "PT/CES": {"region_id": "PT_CES"},
        "PT/NO": {"region_id": "PT_NO"},
        "PT/NON": {"region_id": "PT_NON"},
        "PT/NOS": {"region_id": "PT_NOS"},
        "HR": {"region_id": "HR"},
        "SE": {"region_id": "SE"},
        "BE/WAL": {"region_id": "BE_WAL"},
        "DE/BB": {"region_id": "DE_BB"},
        "CZ": {"region_id": "CZ"},
        "RO": {"region_id": "RO"},
        "DE/ST": {"region_id": "DE_ST"},
        "DE/SL": {"region_id": "DE_SL"},
        "CY/APPL": {"region_id": "CY_APPL"},
        "ES": {"region_id": "ES"},
    }

    ## For spain create a dictionary in a loop, because of the many subregions
    ES_districts = pd.read_csv(r"data\vector\IACS\ES\region_code.txt")
    ES_districts = list(ES_districts["code"])
    run_dict = {f"ES/{district}": {
        "region_id": f"ES_{district}",
        "file_encoding": "utf-8",
        "col_translate_pth": f"data/tables/column_name_translations/ES_column_name_translation.xlsx",
        "crop_class_pth": "data/tables/crop_classifications/ES_crop_classification_final.xlsx",
        "col_transl_descr_overwrite": "ES"
    } for district in ES_districts}

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        hcat_correct_pth = os.path.join("data", "tables", "hcat_levels_v2", "HCAT3_HCAT_mapping.csv")
        hcat_errors_pth = os.path.join("data", "tables", "hcat_levels_v2", "hcat_errors_by_kristoffer.xlsx")

        # check whether the output dir exists and create it if not
        print(os.path.dirname(hcat_correct_pth))
        os.makedirs(os.path.dirname(hcat_correct_pth), exist_ok=True)

        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"] # country_code.replace(r"/", "_")

        if "crop_class_pth" in run_dict[country_code]:
            crop_class_pth = f"{CROP_CLASSIFICATION_FOLDER}/{run_dict[country_code]['crop_class_pth']}"
        else:
            crop_class_pth = f"{CROP_CLASSIFICATION_FOLDER}/{region_id}_crop_classification_final.xlsx"

        ## Read input
        hcat_correct = pd.read_csv(hcat_correct_pth, dtype={"EC_hcat_c": str})
        crop_class = pd.read_excel(crop_class_pth)
        hcat_kris = pd.read_excel(hcat_errors_pth)

        ## Merge data frames
        df_m = pd.merge(crop_class, hcat_correct[["HCAT3_name", "HCAT3_code"]],
                        "left", left_on="EC_hcat_c", right_on="HCAT3_code")
        df_wrong = df_m.loc[df_m["EC_hcat_n"] != df_m["HCAT3_name"]].copy()
        df_wrong["comment_on_mistake"] = "mismatch in names (check if there is another fitting class or if the class name has simply changed, there is also that genetically_modified_organism and energy_crops are swapped in our version"

        df_m2 = pd.merge(crop_class, hcat_correct[["HCAT3_name", "HCAT3_code"]],
                         "left", left_on="EC_hcat_n", right_on="HCAT3_name")
        df_wrong2 = df_m2.loc[df_m2["EC_hcat_c"] != df_m2["HCAT3_code"]].copy()
        df_wrong2["comment_on_mistake"] = "mismatch in codes, this likely means that the assigned code is not correct anymore"

        hcat_kris2 = hcat_kris.loc[hcat_kris["EC_hcat_c"].isna() | hcat_kris["EC_hcat_n"].isna()].copy()
        hcat_kris2["crop_code"] = np.nan
        hcat_kris2["crop_name"] = np.nan
        hcat_kris2["crop_name_de"] = np.nan
        hcat_kris2["crop_name_en"] = np.nan
        hcat_kris2["EC_trans_n"] = np.nan
        hcat_kris2 = hcat_kris2[['crop_code', 'crop_name', 'crop_name_de', 'crop_name_en', 'EC_trans_n',
       'EC_hcat_n', 'EC_hcat_c', 'HCAT3_name', 'HCAT3_code', 'comment_on_mistake']]
        # hcat_kris3 = hcat_kris.loc[(hcat_kris["HCAT3_name"].notna()) & (hcat_kris["EC_hcat_c"].notna()) & (hcat_kris["EC_hcat_n"].notna())].copy()

        df_out = pd.concat([hcat_kris2, df_wrong, df_wrong2])
        df_out.drop_duplicates(subset=["crop_code", "crop_name", "EC_hcat_n", "EC_hcat_c", "HCAT3_name"], inplace=True)

        df_out.loc[df_out["HCAT3_code"].isna(), "comment_on_mistake"] = "class does not exist anymore in HCAT3 (maybe summer was reclassified to spring?)"
        df_out.sort_values(by="crop_name", inplace=True)
        out_pth = rf"{CROP_CLASSIFICATION_FOLDER}/{region_id}_crop_classification_wrong_entries.xlsx"
        df_out.to_excel(out_pth, index=False)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()
