# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# This script can be used to match translated crop names derived with script b1  with already
# existing crop classifications va a string matching algorithm.
# This means based on the similarity of the translated crop name with an already
# classified crop, a HCAT class will be assigned to the crop. All existing classifications will be used as matching candiates.
# The input table should be stored here: data\tables\crop_names\XX_XXX_crop_names_w_translation.xlsx

# Alternatively to using all existing classifications, you can use a specific already existing classification table as
# matching candidates. This is the second part of the main function.

# The output will be stored in :
# data\tables\crop_names\XX_crop_names_w_translation_and_match.xlsx"
# The output should be checked manually, i.e. go over each entry an check based on the English translation name, if the
# assigned HCAT class is correct. The final table should then be stored in:
# data\tables\crop_classifications\XX_crop_classification_final.xlsx

# If you want to run this script for a specific country, include an entry into the run_dict which can be found
# the top of the main function.
# The run_dict key should be the country or country and subdivision abbreviations (for example, "DK" or "DE/THU). The
# item should be another dictionary. In this dictionary, you should include the following keys:

# "region_id" - basically the main key (XX), but for XX/XXX changed into XX_XXX
# "file_encoding" - Encoding of the original GSA file
# "file_year_encoding" - [optional] use if specific years deviate from that encoding
# ""match_df_pth": [optional for second function] use to specify the classification that should be used solely.

# To turn off/on the processing of a specific country, set the key "switch" in the dictionary to "off" or "on"

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import pandas as pd
import glob
import jaro
from typing import Literal, get_args

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

_MATCHES = Literal["crop_code", "crop_name"]

COL_NAMES_FOLDER = os.path.join("data", "tables", "column_names")
CROP_NAMES_FOLDER = os.path.join("data", "tables", "crop_names")
CROP_CLASSIFICATION_FOLDER = os.path.join("data", "tables", "crop_classifications")

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def find_string_with_highest_jaro_winkler(str1, str_lst):

    jw_values = [jaro.jaro_winkler_metric(str1, str2) for str2 in str_lst]
    # jw_values = []
    # for str2 in str_lst:
    #     try:
    #         jaro.jaro_winkler_metric(str1, str2)
    #     except:
    #         print(str2)

    index_max = max(range(len(jw_values)), key=jw_values.__getitem__)
    best_match = str_lst[index_max]

    return best_match


def find_best_matching_ec_crop_code_with_jaro(df_pth, crop_class_folder, out_pth):

    ## Read tabel with unclassified crops
    df = pd.read_excel(df_pth)

    ## Make sure there are no duplicates in the table
    if "year" in df.columns:
        df.drop(columns=["year"], inplace=True)
    df.drop_duplicates(inplace=True)

    ## crop translations to lower, because this might affect the jaro-winkler metric matching
    df["crop_name_en"] = df["crop_name_en"].str.lower()
    # df["crop_name_de"] = df["crop_name_de"].str.lower()

    ## Read existing crop classification, but not use EL because it has too many unuseful names in it
    df_lst = glob.glob(os.path.join(crop_class_folder, "*.xlsx"))
    df_lst = [pth for pth in df_lst if "EL_crop" not in pth]
    df_lst = [pd.read_excel(pth) for pth in df_lst]

    ## Concatenate them to one df
    sub_cols = ["crop_code", "crop_name", "crop_name_de", "crop_name_en", "EC_trans_n", "EC_hcat_n", "EC_hcat_c"]
    df_lst = [df[sub_cols] for df in df_lst]
    df_class = pd.concat(df_lst)

    ## Drop NAs and duplicates
    df_class = df_class.loc[df_class["crop_name_en"].notna()].copy()
    # df_class = df_class.loc[df_class["crop_name_de"].notna()].copy()
    df_class.drop_duplicates(inplace=True)

    ## crop translations to lower, because this might affect the jaro-winkler metric matching
    df_class["crop_name_en"] = df_class["crop_name_en"].str.lower()
    # df_class["crop_name_de"] = df_class["crop_name_de"].str.lower()

    ## Extract crop translations of existing classifications to list. Will be used for matching. Do it for GER and EN
    crop_names_en = df_class["crop_name_en"].tolist()
    # crop_names_de = df_class["crop_name_en"].tolist()

    ## Find the best matches
    df["best_match_en"] = df["crop_name_en"].apply(find_string_with_highest_jaro_winkler, str_lst=crop_names_en)
    # df["best_match_de"] = df["crop_name_de"].apply(find_string_with_highest_jaro_winkler, str_lst=crop_names_de)

    ## Merge the matches with the EC classification from existing classifications EN
    ## Make sure the names are uniform for GER df and EN df
    df_class_en = df_class[["crop_name_en", "EC_trans_n", "EC_hcat_n", "EC_hcat_c"]].copy()
    df_class_en.drop_duplicates(subset=["crop_name_en"], inplace=True)
    df_class_en.rename(columns={"crop_name_en": "best_match_en"}, inplace=True)

    sub_cols2_en = ["crop_code", "crop_name", "crop_name_de", "crop_name_en", "best_match_en"]
    df_out_en = pd.merge(df[sub_cols2_en], df_class_en, on="best_match_en")
    df_out_en.drop_duplicates(inplace=True)
    df_out_en["match_on"] = "en"
    df_out_en.rename(columns={"best_match_en": "best_match"}, inplace=True)

    ## Merge the matches with the EC classification from existing classifications GER
    ## Make sure the names are uniform for GER df and EN df
    # df_class_de = df_class[["crop_name_de", "EC_trans_n", "EC_hcat_n", "EC_hcat_c"]].copy()
    # df_class_de.drop_duplicates(subset=["crop_name_de"], inplace=True)
    # df_class_de.rename(columns={"crop_name_de": "best_match_de"}, inplace=True)
    #
    # sub_cols2_de = ["crop_code", "crop_name", "crop_name_de", "crop_name_en", "best_match_de"]
    # df_out_de = pd.merge(df[sub_cols2_de], df_class_de, on="best_match_de")
    # df_out_de.drop_duplicates(inplace=True)
    # df_out_de["match_on"] = "de"
    # df_out_de.rename(columns={"best_match_de": "best_match"}, inplace=True)

    ## Write out
    # df_out = pd.concat([df_out_en, df_out_de])
    df_out = df_out_en
    # df_out.sort_values(by="crop_code", inplace=True)
    df_out.to_excel(out_pth, index=False)


def find_best_matching_ec_crop_code_with_jaro_two_dfs(df_pth1, df_pth2, match_col, out_pth):

    ## Read tabel with unclassified crops
    df = pd.read_excel(df_pth1)
    df_class = pd.read_excel(df_pth2)

    ## Make sure there are no duplicates in the table
    df.drop(columns=["year"], inplace=True)
    df.drop_duplicates(inplace=True)
    df = df.loc[df[match_col].notna()].copy()

    df_class.drop(columns=["year"], inplace=True)
    df_class.drop_duplicates(inplace=True)
    df_class = df_class.loc[df_class[match_col].notna()].copy()

    ## crop translations to lower, because this might affect the matching
    df[match_col] = df[match_col].str.lower()
    df_class[match_col] = df_class[match_col].str.lower()

    ## Extract crop translations of existing classifications to list. Will be used for matching.
    crop_names = df_class[match_col].tolist()

    ## Find the best matches
    df["best_match"] = df[match_col].apply(find_string_with_highest_jaro_winkler, str_lst=crop_names)

    ## Merge the matches with the EC classification from existing classifications EN
    ## Make sure the names are uniform for GER df and EN df
    df_class_en = df_class[[match_col, "EC_trans_n", "EC_hcat_n", "EC_hcat_c"]].copy()
    df_class_en.drop_duplicates(subset=[match_col], inplace=True)
    df_class_en.rename(columns={match_col: "best_match"}, inplace=True)

    sub_cols2_en = ["crop_code", "crop_name", "crop_name_de", "crop_name_en", "best_match"]
    df_out_en = pd.merge(df[sub_cols2_en], df_class_en, on="best_match")
    df_out_en.drop_duplicates(inplace=True)

    df_out = df_out_en
    df_out.to_excel(out_pth, index=False)




def match_crop_names_of_two_tables(df_pth1, df_pth2, out_pth, match_on: _MATCHES = "crop_code"):

    options = get_args(_MATCHES)
    assert match_on in options, f"'{match_on}' is not in {options}"

    ## Read table with unclassified crops
    df1 = pd.read_excel(df_pth1)
    df2 = pd.read_excel(df_pth2)

    ## Make sure there are no duplicates in the table
    if "year" in df1.columns:
        df1.drop(columns=["year"], inplace=True)
    df1.drop_duplicates(inplace=True)

    if "year" in df2.columns:
        df2.drop(columns=["year"], inplace=True)
    df2.drop_duplicates(inplace=True)

    ## crop translations to lower, because this might affect the jaro-winkler metric matching
    # df1["crop_name"] = df1["crop_name"].str.lower()
    # df2["crop_name"] = df2["crop_name"].str.lower()

    ## Merge
    if match_on == "crop_name":
        crop_names = df1["crop_name"]
        df1["crop_name"] = df1["crop_name"].str.lower()
        df2["crop_name"] = df2["crop_name"].str.lower()
        sub_cols1 = ["crop_code", "crop_name", "crop_name_de", "crop_name_en"]
        sub_cols2 = ["crop_name", "EC_trans_n", "EC_hcat_n", "EC_hcat_c"]
        df_out = pd.merge(df1[sub_cols1], df2[sub_cols2], how="left", on=["crop_name"])
        df_out["crop_name"] = crop_names
    elif match_on == "crop_code":
        sub_cols1 = ["crop_code", "crop_name", "crop_name_de", "crop_name_en"]
        sub_cols2 = ["crop_code", "EC_trans_n", "EC_hcat_n", "EC_hcat_c"]
        df_out = pd.merge(df1[sub_cols1], df2[sub_cols2], how="left",  on="crop_name")

    df_out.to_excel(out_pth, index=False)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## Input for finding the best EC crop codes in an automatic way. Existing crop classifications and a string matching
    ## algorithm are used to find the best match of an english translation of an already classified crop to the enlish
    ## translation of a crop that is not yet classified.

    ## To turn off/on the matching of a specific country, just comment/uncomment the specific line

    ## 1. For matching with all already existing classifications
    run_dict = {
        "IT/EMR": {
            "switch": "off",
            "region_id": "IT_EMR",
            "file_encoding": "utf-8"
        },
        "IT/MAR": {
            "switch": "off",
            "region_id": "IT_MAR",
            "file_encoding": "utf-8"
        },
        "IT/TOS": {
            "region_id": "IT_TOS",
            "file_encoding": "utf-8"
        },
        "IE": {
            "switch": "off",
            "region_id": "IE",
            "file_encoding": "utf-8"
        },
        "DE/THU": {
            "switch": "off",
            "region_id": "DE_THU",
            "file_encoding": "utf-8"
        }
    }

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        switch = run_dict[country_code].get("switch", "off").lower()
        if switch != "on":
            continue
        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"] # country_code.replace(r"/", "_")

        in_pth = os.path.join(CROP_NAMES_FOLDER, f"{region_id}_crop_names_w_translation.xlsx")
        out_pth = os.path.join(CROP_NAMES_FOLDER, f"{region_id}_crop_names_w_translation_and_match.xlsx")

        # check whether input file exists
        if not os.path.isfile(in_pth):
            raise FileNotFoundError(f"The file '{in_pth}' does not exist.")
        
        find_best_matching_ec_crop_code_with_jaro(
            df_pth=in_pth,
            crop_class_folder=CROP_CLASSIFICATION_FOLDER,
            out_pth=out_pth)

    ## For matching with a specific df
    run_dict = {
        "IT/MAR": {
            "switch": "off",
            "region_id": "IT_MAR",
            "file_encoding": "utf-8",
            "match_df_pth": r"data\tables\crop_classifications\IT_EMR_crop_classification_final.xlsx"
        },
        "IT/TOS": {
            "switch": "off",
            "region_id": "IT_TOS",
            "file_encoding": "utf-8",
            "match_df_pth": r"data\tables\crop_classifications\IT_EMR_crop_classification_final.xlsx"
        },
    }

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        switch = run_dict[country_code].get("switch", "off").lower()
        if switch != "on":
            continue
        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"]  #country_code.replace(r"/", "_")

        in_pth = os.path.join(CROP_NAMES_FOLDER, f"{region_id}_crop_names_w_translation.xlsx")
        match_df_pth = run_dict[country_code]["match_df_pth"]
        out_pth = os.path.join(CROP_NAMES_FOLDER, f"{region_id}_crop_names_w_translation_and_match_specific.xlsx")

        # match_crop_names_of_two_tables(df_pth1, df_pth2, out_pth, match_on: _MATCHES = "crop_code")
        match_crop_names_of_two_tables(
            df_pth1=in_pth,
            df_pth2=match_df_pth,
            out_pth=out_pth,
            match_on="crop_name")


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()
