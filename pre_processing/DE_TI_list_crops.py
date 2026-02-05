# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import glob
import pandas as pd
import geopandas as gpd
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def list_crop_names(folder, country_code):

    print(country_code)

    lst = glob.glob(os.path.join(folder, "*.gpkg"))
    dfs = [gpd.read_file(pth) for pth in lst]

    print(len(dfs))

    crops = []
    for df in dfs:
        uni = df[["bdl", "nutz_le_code_meldg", "nutz_le_text_meldg", "ti_nutz_le_code", "ti_nutz_le_text"]].drop_duplicates()
        crops.append(uni)

    crops_df = pd.concat(crops)
    crops_df.drop_duplicates(inplace=True)

    out_pth = os.path.join("data", "tables", "crop_names", f"{country_code}_unique_crop_names.xlsx")
    crops_df.to_excel(out_pth)

def list_crop_names_thu(folder, country_code):

    print(country_code)

    lst = glob.glob(os.path.join(folder, "*.shp"))
    dfs = [gpd.read_file(pth) for pth in lst]

    print(len(dfs))

    crops = []
    for df in dfs:
        uni = df[["bdl", "nutz_le_code_meldg", "nutz_le_text_meldg", "ti_nutz_le_code", "ti_nutz_le_text"]].drop_duplicates()
        crops.append(uni)

    crops_df = pd.concat(crops)
    crops_df.drop_duplicates(inplace=True)

    out_pth = os.path.join("data", "tables", "crop_names", f"{country_code}_unique_crop_names.xlsx")
    crops_df.to_excel(out_pth)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    folders = [
        # r"data\vector\IACS\DE\MWP",
        r"data\vector\IACS\DE\NRW",
        r"data\vector\IACS\DE\RLP",
        r"data\vector\IACS\DE\SAA",
        r"data\vector\IACS\DE\THU\Shapes"
    ]

    country_codes = [ "DE_NRW", "DE_RLP", "DE_SAA", "DE_THU"] #"DE_MWP",

    # for i, folder in enumerate(folders):
    #     list_crop_names(folder=folder, country_code=country_codes[i])

    df_lst = [pd.read_excel(fr"data\tables\crop_names\DE_{abbr}_unique_crop_names.xlsx") for abbr in ["NRW", "RLP", "SAA", "THU"]]
    df_lst = [df[["bdl", "nutz_le_code_meldg", "nutz_le_text_meldg", "ti_nutz_le_code", "ti_nutz_le_text"]] for df in df_lst]

    df_comb = pd.concat(df_lst)
    # df_comb.drop_duplicates(inplace=True)
    df_comb.to_excel(r"data\tables\crop_names\DE_TI_unique_crop_names.xlsx")


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')