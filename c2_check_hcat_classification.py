# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# This script is optional and can be used to validate an old version of the HCAT (prior to HCAT v3) with the HCAT3
# version.

# If you want to run this script for a specific country, include an entry into the run_dict which can be found
# the top of the main function.
# The run_dict key should be the country or country and subdivision abbreviations (for example, "DK" or "DE/THU). The
# item should be another dictionary. In this dictionary, you should include the following keys:

# "region_id" - basically the main key (XX), but for XX/XXX changed into XX_XXX
# "crop_class_pth" - [optional] can be used to point to a different version of the classification table that should be used as input

# To turn off/on the processing of a specific country, set the key "switch" in the dictionary to "off" or "on"

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import pandas as pd
import numpy as np

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
        "BE/FLA": {"switch": "off", "region_id": "BE_FLA"},
        "AT": {"switch": "off", "region_id": "AT", "crop_class_pth": "AT_crop_classification_final_INSPIRE.xlsx"},
        "DK": {"switch": "off", "region_id": "DK"},
        "SI": {"switch": "off", "region_id": "SI"},
        "NL": {"switch": "off", "region_id": "NL"},
        "FI": {"switch": "off", "region_id": "FI"},
        "LV": {"switch": "off", "region_id": "LV"},
        "SK": {"switch": "off", "region_id": "SK"},
        "FR/FR": {"switch": "off", "region_id": "FR_FR"},
        "FR/SUBREGIONS": {"switch": "off", "region_id": "FR_SUBREGIONS"},
        "PT/PT": {"switch": "off", "region_id": "PT_PT"},
        "PT/ALE": {"switch": "off", "region_id": "PT_ALE"},
        "PT/ALG": {"switch": "off", "region_id": "PT_ALG"},
        "PT/AML": {"switch": "off", "region_id": "PT_AML"},
        "PT/CE": {"switch": "off", "region_id": "PT_CE"},
        "PT/CEN": {"switch": "off", "region_id": "PT_CEN"},
        "PT/CES": {"switch": "off", "region_id": "PT_CES"},
        "PT/NO": {"switch": "off", "region_id": "PT_NO"},
        "PT/NON": {"switch": "off", "region_id": "PT_NON"},
        "PT/NOS": {"switch": "off", "region_id": "PT_NOS"},
        "HR": {"switch": "off", "region_id": "HR"},
        "SE": {"switch": "off", "region_id": "SE"},
        "BE/WAL": {"switch": "off", "region_id": "BE_WAL"},
        "DE/BB": {"switch": "off", "region_id": "DE_BRB"},
        "CZ": {"switch": "off", "region_id": "CZ"},
        "RO": {"switch": "off", "region_id": "RO"},
        "DE/SAT": {"switch": "off", "region_id": "DE_SAT"},
        "DE/SAA": {"switch": "off", "region_id": "DE_SAL"},
        "CY/APPL": {"switch": "off", "region_id": "CY_APPL"},
        "ES": {"switch": "off", "region_id": "ES"},
    }

    ## For spain create a dictionary in a loop, because of the many subregions
    ES_districts = pd.read_csv(os.path.join("data", "vector", "IACS", "ES", "region_code.txt"))
    ES_districts = list(ES_districts["code"])
    run_dict = {f"ES/{district}": {
        "switch": "off",
        "region_id": f"ES_{district}",
        "file_encoding": "utf-8",
        "col_translate_pth": os.path.join("data", "tables", "column_name_translations",
                                          "ES_column_name_translation.xlsx"),
        "crop_class_pth": os.path.join("data", "tables", "crop_classifications", "ES_crop_classification_final.xlsx"),
        "col_transl_descr_overwrite": "ES"
    } for district in ES_districts}

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        switch = run_dict[country_code].get("switch", "off").lower()
        if switch != "on":
            continue
        hcat_correct_pth = os.path.join("data", "tables", "hcat_levels_v2", "HCAT3_HCAT_mapping.csv")
        hcat_errors_pth = os.path.join("data", "tables", "hcat_levels_v2", "hcat_errors_by_kristoffer.xlsx")

        # check whether the output dir exists and create it if not
        print(os.path.dirname(hcat_correct_pth))
        os.makedirs(os.path.dirname(hcat_correct_pth), exist_ok=True)

        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"] # country_code.replace(r"/", "_")

        if "crop_class_pth" in run_dict[country_code]:
            crop_class_pth = os.path.join(CROP_CLASSIFICATION_FOLDER, {run_dict[country_code]['crop_class_pth']})
        else:
            crop_class_pth = os.path.join(CROP_CLASSIFICATION_FOLDER, f"{region_id}_crop_classification_final.xlsx")

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
        out_pth = os.path.join(CROP_CLASSIFICATION_FOLDER, f"{region_id}_crop_classification_wrong_entries.xlsx" )
        df_out.to_excel(out_pth, index=False)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()
