# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import numpy as np
import pandas as pd
import geopandas as gpd
from osgeo import gdal

gdal.SetConfigOption("OGR_GEOMETRY_ACCEPT_UNCLOSED_RING", "NO")

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def prep_data(schlaege_csv_pth, schlaege_vec_pth, code_pth, land_ele_pth, out_pth,
              year,
              gdf_c_col,
              le_code_col,
              df_s_nr_col="SCHLAGNR", gdf_s_nr_col="SCHLAGNR", le_s_nr_col="SCHLAGNR",
              df_s_flik_col="FLIK", gdf_s_flik_col="FLIK", le_s_flik_col="FLIK",
              df_s_org_col="BEANT_FL_BV1",
              farm_id_col=None):

    # Open Data
    df_s = pd.read_csv(schlaege_csv_pth, sep=";", encoding="ISO-8859-1")
    gdf_s = gpd.read_file(schlaege_vec_pth)
    df_c = pd.read_excel(code_pth, sheet_name=str(year))
    gdf_le = gpd.read_file(land_ele_pth)

    cols = list(df_s.columns)
    cols = [col.strip() for col in cols]
    df_s.columns = cols

    # Create unique field ID
    df_s.loc[df_s[df_s_nr_col].isna(), df_s_nr_col] = 9999
    df_s[df_s_nr_col] = df_s[df_s_nr_col].astype(int).astype(str)
    df_s.loc[df_s[df_s_nr_col] == "9999", df_s_nr_col] = ""
    df_s["field_id"] = df_s[df_s_flik_col] + '_' + df_s[df_s_nr_col]

    gdf_s.loc[gdf_s[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    gdf_s[gdf_s_nr_col] = gdf_s[gdf_s_nr_col].astype(int).astype(str)
    gdf_s.loc[gdf_s[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    gdf_s["field_id"] = gdf_s[gdf_s_flik_col] + '_' + gdf_s[gdf_s_nr_col]

    gdf_le.loc[gdf_le[le_s_nr_col].isna(), le_s_nr_col] = 9999
    gdf_le[le_s_nr_col] = gdf_le[le_s_nr_col].astype(int).astype(str)
    gdf_le.loc[gdf_le[le_s_nr_col] == "9999", le_s_nr_col] = ""
    gdf_le["field_id"] = gdf_le[le_s_flik_col] + '_' + gdf_le[le_s_nr_col]

    num_s = len(gdf_s)
    num_le = len(gdf_le)

    # Create dictionary with ID and organic information
    df_s["organic"] = 0
    df_s[df_s_org_col] = df_s[df_s_org_col].str.replace(",", ".")
    df_s[df_s_org_col] = df_s[df_s_org_col].str.replace(" ", "")
    df_s.loc[df_s[df_s_org_col] == "", df_s_org_col] = 0
    df_s[df_s_org_col] = df_s[df_s_org_col].astype(float)
    df_s.loc[(df_s[df_s_org_col] > 0.0), "organic"] = 1
    # if year == 2021:
    #     df_s.loc[(df_s[df_s_org_col].notna()) & (df_s[df_s_org_col] != '            ' ) & (df_s[df_s_org_col].astype(float) > 0), "organic"] = 1
    # elif year == 2023:
    #     df_s.loc[(df_s[df_s_org_col] == 0.0), "organic"] = 0
    org_dict = dict(zip(df_s["field_id"], df_s["organic"]))

    # Add organic info to vector data
    gdf_s["organic"] = gdf_s["field_id"].map(org_dict)
    gdf_s.loc[gdf_s["organic"].isna(), "organic"] = 0

    # remove trailing zeros from codes
    df_c["Code"] = df_c["Code"].astype(str).str.lstrip('0')
    df_c.drop_duplicates(subset="Code", inplace=True)
    gdf_s[gdf_c_col] = gdf_s[gdf_c_col].astype(str).str.lstrip('0')

    # Add Nutzungsbeschreibung to vector data
    gdf_s = pd.merge(gdf_s, df_c, "left", left_on=gdf_c_col, right_on="Code")

    nas = list(gdf_s.loc[gdf_s["Kulturart"].isna(), gdf_c_col].unique())

    if len(nas) > 0:
        with open(fr"data\vector\IACS\DE\LSA\Original\Nutzungscodes\{year}_no_classification.csv", "w") as file:
            for code in nas:
                file.write(str(code)+"\n")

    le_dict1 = {
        1: "A",
        2: "B",
        3: "C",
        4: "D",
        6: "J",
        7: "E",
        8: "H",
        9 : "I"
    }

    # For all years, where the codes are number, turn them into letters
    if year < 2020:
        gdf_le["TYP_LE"] = gdf_le[le_code_col].map(le_dict1)

    le_dict2 = {
        "A": "Hecken / Knicks",
        "B": "Baumreihen",
        "C": "Feldgehölze",
        "D": "Feuchtgebiete",
        "E": "Einzelbäume",
        "H": "Feldraine",
        "I": "Trocken - und Natursteinmauern / Lesesteinwälle",
        "J": "Fels - und Steinriegel"
    }

    # Then add description of letters to gdf
    gdf_le["Kulturart"] = gdf_le["TYP_LE"].map(le_dict2)

    # Add Betriebsnummer to landscape elements
    if farm_id_col:
        if farm_id_col not in list(gdf_le.columns):
            ## create field id farm id dict
            t = gdf_s.drop_duplicates(subset=["field_id", farm_id_col])
            ff_dict = dict(zip(t["field_id"], t[farm_id_col]))
            gdf_le["REG_NR"] = gdf_le["field_id"].map(ff_dict)
    else:
        gdf_le["REG_NR"] = np.nan

    gdf_le["organic"] = 0
    gdf_le.rename(columns={le_code_col:"Code"}, inplace=True)

    gdf_s.rename(columns={
        farm_id_col: "REG_NR",
        gdf_s_flik_col: "FLIK",
        gdf_s_nr_col: "SCHLAGNR"
    }, inplace=True)

    gdf_le.rename(columns={
        farm_id_col: "REG_NR",
        le_s_flik_col: "FLIK",
        le_s_nr_col: "SCHLAGNR"
    }, inplace=True)

    # Concatenate landscape elements to schlaege (make sure the
    mand_cols = ["field_id", "FLIK", "REG_NR", "SCHLAGNR", "Code", "Kulturart", "organic", "geometry"]

    gdf_out = pd.concat([gdf_s[mand_cols], gdf_le[mand_cols]])

    gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    gdf_out.drop(columns=["field_id_distinguished"], inplace=True)

    print(len(gdf_out), num_s + num_le)
    if len(gdf_out) == num_s + num_le:
        gdf_out.to_parquet(out_pth)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## 2024 - 2025
    # for year in range(2025, 2026):
    #     schlaege_vec_pth = rf"data\vector\IACS\DE\LSA\Original\Schlaege\UD_{year}_S.shp"
    #     code_pth = r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx"
    #     land_ele_pth = rf"data\vector\IACS\DE\LSA\Original\Landschaftselemente\UD_{year-2000}_TLE.shp"
    #     gdf_s_nr_col = "SCHLAGNR"
    #     gdf_s_flik_col = "FLIK"
    #     le_s_nr_col = "SCHLAGNR"
    #     le_s_flik_col = "FLIK"
    #     gdf_c_col = "NC_FESTG"
    #     le_code_col = "TYP_LE"
    #     out_pth = fr"data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_{year}.geoparquet"
    #
    #     gdf_s = gpd.read_file(schlaege_vec_pth)
    #     df_c = pd.read_excel(code_pth, sheet_name=str(year))
    #     gdf_le = gpd.read_file(land_ele_pth)
    #
    #     gdf_s.loc[gdf_s[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    #     gdf_s[gdf_s_nr_col] = gdf_s[gdf_s_nr_col].astype(int).astype(str)
    #     gdf_s.loc[gdf_s[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    #     gdf_s["field_id"] = gdf_s[gdf_s_flik_col] + '_' + gdf_s[gdf_s_nr_col]
    #
    #     gdf_le.loc[gdf_le[le_s_nr_col].isna(), le_s_nr_col] = 9999
    #     gdf_le[le_s_nr_col] = gdf_le[le_s_nr_col].astype(int).astype(str)
    #     gdf_le.loc[gdf_le[le_s_nr_col] == "9999", le_s_nr_col] = ""
    #     gdf_le["field_id"] = gdf_le[le_s_flik_col] + '_' + gdf_le[le_s_nr_col]
    #
    #     num_s = len(gdf_s)
    #     num_le = len(gdf_le)
    #
    #     # remove trailing zeros from codes
    #     df_c["Code"] = df_c["Code"].astype(str).str.lstrip('0')
    #     df_c.drop_duplicates(subset="Code", inplace=True)
    #     gdf_s[gdf_c_col] = gdf_s[gdf_c_col].astype(str).str.lstrip('0')
    #
    #     # Add Nutzungsbeschreibung to vector data
    #     gdf_s = pd.merge(gdf_s, df_c, "left", left_on=gdf_c_col, right_on="Code")
    #
    #     nas = list(gdf_s.loc[gdf_s["Kulturart"].isna(), gdf_c_col].unique())
    #
    #     if len(nas) > 0:
    #         with open(fr"data\vector\IACS\DE\LSA\Original\Nutzungscodes\{year}_no_classification.csv", "w") as file:
    #             for code in nas:
    #                 file.write(str(code) + "\n")
    #
    #     le_dict2 = {
    #         "A": "Hecken / Knicks",
    #         "B": "Baumreihen",
    #         "C": "Feldgehölze",
    #         "D": "Feuchtgebiete",
    #         "E": "Einzelbäume",
    #         "H": "Feldraine",
    #         "I": "Trocken - und Natursteinmauern / Lesesteinwälle",
    #         "J": "Fels - und Steinriegel"
    #     }
    #
    #     # Then add description of letters to gdf
    #     gdf_le["Kulturart"] = gdf_le["TYP_LE"].map(le_dict2)
    #
    #     gdf_le.rename(columns={le_code_col: "Code"}, inplace=True)
    #
    #     gdf_s.rename(columns={
    #         gdf_s_flik_col: "FLIK",
    #         gdf_s_nr_col: "SCHLAGNR"
    #     }, inplace=True)
    #
    #     gdf_le.rename(columns={
    #         le_s_flik_col: "FLIK",
    #         le_s_nr_col: "SCHLAGNR"
    #     }, inplace=True)
    #
    #     # Concatenate landscape elements to schlaege (make sure the
    #     mand_cols = ["field_id", "FLIK", "SCHLAGNR", "Code", "Kulturart", "geometry"]
    #
    #     gdf_out = pd.concat([gdf_s[mand_cols], gdf_le[mand_cols]])
    #
    #     gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    #     gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    #     gdf_out.drop(columns=["field_id_distinguished"], inplace=True)
    #
    #     print(len(gdf_out), num_s + num_le)
    #     if len(gdf_out) == num_s + num_le:
    #         gdf_out.to_parquet(out_pth)

    run_dict = {
        # 2023: {
        #     "schlaege_csv_pth": r"data\vector\IACS\DE\LSA\Original\Schlaege\Flaechen_2023-12-18_ohneHH_neu.csv",
        #     "schlaege_vec_pth": r"data\vector\IACS\DE\LSA\Original\Schlaege\LSN_2023_S.shp",
        #     "code_pth": r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx",
        #     "land_ele_pth": r"data\vector\IACS\DE\LSA\Original\Landschaftselemente\LSN_2023_TLE.shp",
        #     "gdf_c_col": "NC_FESTG",
        #     "le_code_col": "TYP_LE",
        #     "farm_id_col": "REG_NR",
        #     "out_pth": r"data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_2023.geoparquet"
        # },
        # 2022: {
        #     "schlaege_csv_pth": r"data\vector\IACS\DE\LSA\Original\Schlaege\Flaechen_2022_neu.csv",
        #     "schlaege_vec_pth": r"data\vector\IACS\DE\LSA\Original\Schlaege\LSN_2022_S.shp",
        #     "code_pth": r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx",
        #     "land_ele_pth": r"data\vector\IACS\DE\LSA\Original\Landschaftselemente\LSN_2022_TSLE.shp",
        #     "gdf_c_col": "KC_FESTG",
        #     "le_code_col": "TYP_LE",
        #     "farm_id_col": "REG_NR",
        #     "out_pth": r"data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_2022.geoparquet"
        # },
        # 2021: {
        #     "schlaege_csv_pth": r"data\vector\IACS\DE\LSA\Original\Schlaege\Flaechen_2021-12-13_neu.csv",
        #     "schlaege_vec_pth": r"data\vector\IACS\DE\LSA\Original\Schlaege\LSN_2021_S.shp",
        #     "code_pth": r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx",
        #     "land_ele_pth": r"data\vector\IACS\DE\LSA\Original\Landschaftselemente\LSN_2021_TSLE.shp",
        #     "gdf_c_col": "KC_FESTG",
        #     "le_code_col": "TYP_LE",
        #     "farm_id_col": "REG_NR",
        #     "out_pth": r"data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_2021.geoparquet"
        # }
    }

    for year in run_dict:
        prep_data(
            schlaege_csv_pth=run_dict[year]["schlaege_csv_pth"],
            schlaege_vec_pth=run_dict[year]["schlaege_vec_pth"],
            code_pth=run_dict[year]["code_pth"],
            land_ele_pth=run_dict[year]["land_ele_pth"],
            year=year,
            gdf_c_col=run_dict[year]["gdf_c_col"],
            farm_id_col=run_dict[year]["farm_id_col"],
            le_code_col=run_dict[year]["le_code_col"],
            out_pth=run_dict[year]["out_pth"]
        )

    # #### 2020
    # schlaege_csv_pth=r"data\vector\IACS\DE\LSA\Original\Schlaege\Flaechen_2020-06-15_neu.csv"
    # schlaege_vec_pth=r"data\vector\IACS\DE\LSA\Original\Teilschlaege\Teilschlaege_2020_0615_mitFlaeche.shp"
    # code_pth=r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx"
    # out_pth=r"data\vector\IACS\DE\LSA\prepared_data\Teilschlaege+LSE_2020.geoparquet"
    # year=2020
    # df_s_flik_col = "FLIK"
    # df_s_nr_col="SCHLAGNR"
    # df_s_tn_col="TS_BEZ"
    # df_s_cc_col="KULTURARTFACHCODE"
    # df_s_org_col = "BEANT_FL_BV1"
    # gdf_s_flik_col="FLIK"
    # gdf_s_nr_col="SCHLAGNR"
    # gdf_s_tn_col="TEILSCHLAG"
    #
    # # Open Data
    # df_s = pd.read_csv(schlaege_csv_pth, sep=";", encoding="ISO-8859-1")
    # gdf_s = gpd.read_file(schlaege_vec_pth)
    # df_c = pd.read_excel(code_pth, sheet_name=str(year))
    #
    # cols = list(df_s.columns)
    # cols = [col.strip() for col in cols]
    # df_s.columns = cols
    #
    # # Create unique field ID
    # df_s.loc[df_s[df_s_nr_col].isna(), df_s_nr_col] = 9999
    # df_s.loc[pd.to_numeric(df_s[df_s_nr_col], errors="coerce").isna(), df_s_nr_col] = 1
    # df_s[df_s_nr_col] = df_s[df_s_nr_col].astype(int).astype(str)
    # df_s.loc[df_s[df_s_nr_col] == "9999", df_s_nr_col] = ""
    # df_s[df_s_tn_col] = df_s[df_s_tn_col].str.replace(" ", "")
    # df_s["field_id"] = df_s[df_s_flik_col] + '_' + df_s[df_s_nr_col] + df_s[df_s_tn_col]
    #
    # gdf_s.loc[gdf_s[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_s[gdf_s_nr_col] = gdf_s[gdf_s_nr_col].astype(int).astype(str)
    # gdf_s.loc[gdf_s[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_s[gdf_s_tn_col] = gdf_s[gdf_s_tn_col].str.replace(" ", "")
    # gdf_s["field_id"] = gdf_s[gdf_s_flik_col] + '_' + gdf_s[gdf_s_nr_col] + gdf_s[gdf_s_tn_col]
    #
    # num_s = len(gdf_s)
    #
    # # Create dictionary with ID and organic information
    # df_s["organic"] = 0
    # df_s[df_s_org_col] = df_s[df_s_org_col].str.replace(" ", "")
    # df_s.loc[df_s[df_s_org_col] != "", "organic"] = 1
    # org_dict = dict(zip(df_s["field_id"], df_s["organic"]))
    #
    # cc_dict = dict(zip(df_s["field_id"], df_s[df_s_cc_col]))
    #
    # # Add organic info to vector data
    # gdf_s["organic"] = gdf_s["field_id"].map(org_dict)
    # gdf_s.loc[gdf_s["organic"].isna(), "organic"] = 0
    # x = gdf_s.loc[gdf_s["organic"] == 1].copy()
    #
    # # Add use code info to vector data
    # gdf_s["Code"] = gdf_s["field_id"].map(cc_dict)
    # gdf_s.loc[gdf_s["Code"].isna(), "Code"] = 9999
    # gdf_s["Code"] = gdf_s["Code"].astype(int).astype(str)
    #
    # # remove trailing zeros from codes
    # df_c["Code"] = df_c["Code"].astype(str).str.lstrip('0')
    # df_c.drop_duplicates(subset="Code", inplace=True)
    #
    # # Add Nutzungsbeschreibung to vector data
    # gdf_s = pd.merge(gdf_s, df_c, "left", on="Code")
    #
    # nas = list(gdf_s.loc[gdf_s["Kulturart"].isna(), "Code"].unique())
    #
    # if len(nas) > 0:
    #     with open(fr"data\vector\IACS\DE\LSA\Original\Nutzungscodes\{year}_no_classification.csv", "w") as file:
    #         for code in nas:
    #             file.write(str(code) + "\n")
    #
    # gdf_s.rename(columns={
    #     "REGISTRIER": "REG_NR",
    #     gdf_s_flik_col: "FLIK",
    #     gdf_s_nr_col: "SCHLAGNR"
    # }, inplace=True)
    #
    # gdf_s.loc[gdf_s["FOERDERART"].str.contains("BV1"), "organic"] = 1
    #
    # # Concatenate landscape elements to schlaege (make sure the
    # mand_cols = ["field_id", "FLIK", "REG_NR", "SCHLAGNR", "Code", "Kulturart", "organic", "geometry"]
    #
    # gdf_out = gdf_s[mand_cols].copy()
    #
    # gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    # gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    # gdf_out.drop(columns=["field_id_distinguished"], inplace=True)
    #
    # print(len(gdf_out), num_s)
    # if len(gdf_out) == num_s:
    #     gdf_out.to_parquet(out_pth)

    # #### 2019
    # schlaege_vec_pth = r"data\vector\IACS\DE\LSA\Original\Schlaege\GFN_Schlaege_2019.shp"
    # code_pth = r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx"
    # teilschlaege_vec_pth = r"data\vector\IACS\DE\LSA\Original\Teilschlaege\AUM_Teilschlaege_2019.shp"
    # out_pth = r"data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_2019.geoparquet"
    # year = 2019
    # gdf_c_col = "KULTURARTF"
    # gdf_s_nr_col = "SCHLAGNR"
    # gdf_s_flik_col = "FLIK"
    #
    # # Open Data
    # gdf_s = gpd.read_file(schlaege_vec_pth)
    # gdf_ts = gpd.read_file(teilschlaege_vec_pth)
    # df_c = pd.read_excel(code_pth, sheet_name=str(year))
    #
    # # Create unique field ID
    # gdf_s.loc[gdf_s[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_s[gdf_s_nr_col] = gdf_s[gdf_s_nr_col].astype(int).astype(str)
    # gdf_s.loc[gdf_s[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_s["field_id"] = gdf_s[gdf_s_flik_col] + '_' + gdf_s[gdf_s_nr_col]
    #
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_ts[gdf_s_nr_col] = gdf_ts[gdf_s_nr_col].astype(int).astype(str)
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_ts["field_id"] = gdf_ts[gdf_s_flik_col] + '_' + gdf_ts[gdf_s_nr_col]
    # gdf_ts["organic"] = 0
    # gdf_ts.loc[gdf_ts["FOERDERART"].isna(), "FOERDERART"] = "BBBB"
    # gdf_ts.loc[gdf_ts["FOERDERART"].str.contains("BV1"), "organic"] = 1
    # # l = list(gdf_ts["FOERDERART"].unique())
    # # l = [i for i in l if type(i) is str]
    # # l = [i.split(",") for i in l]
    # # l2 = []
    # # for i in l:
    # #     l2 += i
    # # l2 = [i.replace(" ", "") for i in l2]
    # # l2 = list(set(l2))
    # org_dict = dict(zip(gdf_ts["field_id"], gdf_ts["organic"]))
    #
    # ## Add organic cultivation to schlaege
    # gdf_s["organic"] = gdf_s["field_id"].map(org_dict)
    #
    # num_s = len(gdf_s)
    #
    # # remove trailing zeros from codes
    # df_c["Code"] = df_c["Code"].astype(str).str.lstrip('0')
    # df_c.drop_duplicates(subset="Code", inplace=True)
    # gdf_s[gdf_c_col] = gdf_s[gdf_c_col].astype(str).str.lstrip('0')
    #
    # # Add Nutzungsbeschreibung to vector data
    # gdf_s = pd.merge(gdf_s, df_c, "left", left_on=gdf_c_col, right_on="Code")
    #
    # nas = list(gdf_s.loc[gdf_s["Kulturart"].isna(), gdf_c_col].unique())
    #
    # if len(nas) > 0:
    #     with open(fr"data\vector\IACS\DE\LSA\Original\Nutzungscodes\{year}_no_classification.csv", "w") as file:
    #         for code in nas:
    #             file.write(str(code) + "\n")
    #
    # gdf_s.rename(columns={"SCHLAG_NR": "SCHLAGNR", "REGISTRIER": "REG_NR"}, inplace=True)
    # mand_cols = ["field_id", "FLIK", "REG_NR", "SCHLAGNR", "Code", "Kulturart", "organic", "geometry"]
    #
    # gdf_out = gdf_s[mand_cols].copy()
    #
    # gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    # gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    # gdf_out.drop(columns=["field_id_distinguished"], inplace=True)
    #
    # gdf_out.loc[gdf_out["organic"].isna(), "organic"] = 0
    #
    # print(len(gdf_out), num_s)
    # if len(gdf_out) == num_s:
    #     gdf_out.to_parquet(out_pth)

    # #### 2018
    # schlaege_csv_pth=r"data\vector\IACS\DE\LSA\Original\Schlaege\Flaechen_2018-11-15.csv"
    # schlaege_vec_pth=r"data\vector\IACS\DE\LSA\Original\Schlaege\GFN_Schlaege_2018.shp"
    # teilschlaege_vec_pth = r"data\vector\IACS\DE\LSA\Original\Teilschlaege\AUM_Teilschlaege_2018.shp"
    # code_pth=r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx"
    # out_pth=r"data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_2018.geoparquet"
    # year=2018
    # gdf_c_col="KULTURARTF"
    # df_s_nr_col="SCHLAGNR"
    # gdf_s_nr_col="SCHLAGNR"
    # gdf_s_flik_col="FLIK"
    # df_s_flik_col="FLIK"
    # df_s_org_col="OEKOKONTROLLNUMMER"
    #
    # # Open Data
    # df_s = pd.read_csv(schlaege_csv_pth, sep=";", encoding="ISO-8859-1", header=None)
    # gdf_s = gpd.read_file(schlaege_vec_pth)
    # gdf_ts = gpd.read_file(teilschlaege_vec_pth)
    # df_c = pd.read_excel(code_pth, sheet_name=str(year))
    #
    # cols = ["REG_NR","LFDNR","FLIK","SCHLAGNR","SCHLAGBEZEICHNUNG","KULTURARTFACHID","TE","GEMELDETEFLAECHE","NRLE",
    #         "FLEK","TYPLE","OEKOVORRANGFLAECHE","GEMELDETEFLAECHE","ANTRAGSJAHR","ANBAUART","OEKOVORRANGFLANGRSCH",
    #         "OEKOVORRANGFLAECHEFA","BEZEICHNUNG","KEINEAKTIVIERUNGZA","KEINEZUWEISUNGZA","FESTGEST_FL_ZID_SCHLAG",
    #         "FESTGEST_FL_ZID_LE","F2","F3","OEKOKONTROLLNUMMER","FFID","AA","FESTGESTELLTEFLAECHEZID"]
    # df_s.columns = cols
    #
    # # Create unique field ID
    # df_s.loc[df_s[df_s_nr_col].isna(), df_s_nr_col] = 9999
    # df_s[df_s_nr_col] = df_s[df_s_nr_col].astype(int).astype(str)
    # df_s.loc[df_s[df_s_nr_col] == "9999", df_s_nr_col] = ""
    # df_s["field_id"] = df_s[df_s_flik_col] + '_' + df_s[df_s_nr_col]
    #
    # gdf_s.loc[gdf_s[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_s[gdf_s_nr_col] = gdf_s[gdf_s_nr_col].astype(int).astype(str)
    # gdf_s.loc[gdf_s[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_s["field_id"] = gdf_s[gdf_s_flik_col] + '_' + gdf_s[gdf_s_nr_col]
    #
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_ts[gdf_s_nr_col] = gdf_ts[gdf_s_nr_col].astype(int).astype(str)
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_ts["field_id"] = gdf_ts[gdf_s_flik_col] + '_' + gdf_ts[gdf_s_nr_col]
    # gdf_ts["organic"] = 0
    # gdf_ts.loc[gdf_ts["FOERDERART"].isna(), "FOERDERART"] = "BBBB"
    # gdf_ts.loc[gdf_ts["FOERDERART"].str.contains("BV1"), "organic"] = 1
    # # l = list(gdf_ts["FOERDERART"].unique())
    # # l = [i for i in l if type(i) is str]
    # # l = [i.split(",") for i in l]
    # # l2 = []
    # # for i in l:
    # #     l2 += i
    # # l2 = [i.replace(" ", "") for i in l2]
    # # l2 = list(set(l2))
    # org_dict = dict(zip(gdf_ts["field_id"], gdf_ts["organic"]))
    #
    # ## Add organic cultivation to schlaege
    # gdf_s["organic"] = gdf_s["field_id"].map(org_dict)
    #
    # num_s = len(gdf_s)
    #
    # # # Create dictionary with ID and organic information
    # # df_s["organic"] = 0
    # # df_s[df_s_org_col] = df_s[df_s_org_col].str.replace(" ", "")
    # # df_s.loc[df_s[df_s_org_col] != "", "organic"] = 1
    # # org_dict = dict(zip(df_s["field_id"], df_s["organic"]))
    # #
    # # # Add organic info to vector data
    # # gdf_s["organic"] = gdf_s["field_id"].map(org_dict)
    # # gdf_s.loc[gdf_s["organic"].isna(), "organic"] = 0
    #
    # # remove trailing zeros from codes
    # df_c["Code"] = df_c["Code"].astype(str).str.lstrip('0')
    # df_c.drop_duplicates(subset="Code", inplace=True)
    # gdf_s[gdf_c_col] = gdf_s[gdf_c_col].astype(str).str.lstrip('0')
    #
    # # Add Nutzungsbeschreibung to vector data
    # gdf_s = pd.merge(gdf_s, df_c, "left", left_on=gdf_c_col, right_on="Code")
    #
    # nas = list(gdf_s.loc[gdf_s["Kulturart"].isna(), gdf_c_col].unique())
    #
    # if len(nas) > 0:
    #     with open(fr"data\vector\IACS\DE\LSA\Original\Nutzungscodes\{year}_no_classification.csv", "w") as file:
    #         for code in nas:
    #             file.write(str(code) + "\n")
    #
    # gdf_s.rename(columns={
    #     "REGISTRIER": "REG_NR",
    #     gdf_s_flik_col: "FLIK",
    #     gdf_s_nr_col: "SCHLAGNR"
    # }, inplace=True)
    #
    # # Concatenate landscape elements to schlaege (make sure the
    # mand_cols = ["field_id", "FLIK", "REG_NR", "SCHLAGNR", "Code", "Kulturart", "organic", "geometry"]
    #
    # gdf_out = gdf_s[mand_cols].copy()
    #
    # gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    # gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    # gdf_out.drop(columns=["field_id_distinguished"], inplace=True)
    #
    # gdf_out.loc[gdf_out["organic"].isna(), "organic"] = 0
    #
    # print(len(gdf_out), num_s )
    # if len(gdf_out) == num_s:
    #     gdf_out.to_parquet(out_pth)
    #
    #### 2017
    # schlaege_csv_pth = r"data\vector\IACS\DE\LSA\Original\Schlaege\Flaechen_2017-12-07.csv"
    # schlaege_vec_pth = r"data\vector\IACS\DE\LSA\Original\Schlaege\NS_InvHarm_2017.shp"
    # teilschlaege_vec_pth = r"data\vector\IACS\DE\LSA\Original\Teilschlaege\AUM_Teilschlaege_2017.shp"
    # code_pth = r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx"
    # out_pth = r"data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_2017.geoparquet"
    # year = 2017
    # gdf_c_col = "K_ART" #"KULTURARTF"
    # df_s_nr_col = "SCHLAGNR"
    # gdf_s_nr_col = "SCHLAGNR"
    # gdf_s_flik_col = "FLIK"
    # df_s_flik_col = "FLIK"
    # df_s_org_col = "OEKOKONTROLLNUMMER"
    #
    # # Open Data
    # df_s = pd.read_csv(schlaege_csv_pth, sep=";", encoding="ISO-8859-1")
    # gdf_s = gpd.read_file(schlaege_vec_pth)
    # gdf_ts = gpd.read_file(teilschlaege_vec_pth)
    # df_c = pd.read_excel(code_pth, sheet_name=str(year))
    #
    # cols = ["REG_NR", "LFDNR", "FLIK", "SCHLAGNR", "SCHLAGBEZEICHNUNG", "KULTURARTFACHID", "TE", "GEMELDETEFLAECHE",
    #         "NRLE",
    #         "FLEK", "TYPLE", "OEKOVORRANGFLAECHE", "GEMELDETEFLAECHE", "ANTRAGSJAHR", "ANBAUART",
    #         "OEKOVORRANGFLANGRSCH",
    #         "OEKOVORRANGFLAECHEFA", "BEZEICHNUNG", "KEINEAKTIVIERUNGZA", "KEINEZUWEISUNGZA",
    #         "FESTGEST_FL_ZID_SCHLAG",
    #         "FESTGEST_FL_ZID_LE", "F2", "F3", "OEKOKONTROLLNUMMER", "FFID", "AA", "FESTGESTELLTEFLAECHEZID"]
    # df_s.columns = cols
    #
    # df_s = df_s[pd.to_numeric(df_s["SCHLAGNR"], errors="coerce").notna()].copy()
    #
    # # Create unique field ID
    # df_s.loc[df_s[df_s_nr_col].isna(), df_s_nr_col] = 9999
    # df_s[df_s_nr_col] = df_s[df_s_nr_col].astype(int).astype(str)
    # df_s.loc[df_s[df_s_nr_col] == "9999", df_s_nr_col] = ""
    # df_s["field_id"] = df_s[df_s_flik_col] + '_' + df_s[df_s_nr_col]
    #
    # gdf_s.loc[gdf_s[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_s[gdf_s_nr_col] = gdf_s[gdf_s_nr_col].astype(int).astype(str)
    # gdf_s.loc[gdf_s[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_s["field_id"] = gdf_s[gdf_s_flik_col] + '_' + gdf_s[gdf_s_nr_col]
    #
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_ts[gdf_s_nr_col] = gdf_ts[gdf_s_nr_col].astype(int).astype(str)
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_ts["field_id"] = gdf_ts[gdf_s_flik_col] + '_' + gdf_ts[gdf_s_nr_col]
    # gdf_ts["organic"] = 0
    # gdf_ts.loc[gdf_ts["FOERDERART"].isna(), "FOERDERART"] = "BBBB"
    # gdf_ts.loc[gdf_ts["FOERDERART"].str.contains("BV1"), "organic"] = 1
    # # l = list(gdf_ts["FOERDERART"].unique())
    # # l = [i for i in l if type(i) is str]
    # # l = [i.split(",") for i in l]
    # # l2 = []
    # # for i in l:
    # #     l2 += i
    # # l2 = [i.replace(" ", "") for i in l2]
    # # l2 = list(set(l2))
    # org_dict = dict(zip(gdf_ts["field_id"], gdf_ts["organic"]))
    #
    # ## Add organic cultivation to schlaege
    # gdf_s["organic"] = gdf_s["field_id"].map(org_dict)
    #
    # num_s = len(gdf_s)
    #
    # ### This is another way to add the organic cultivation, but in this case the information is not area-specific but
    # # farm specific, i.e. areas in an organic farm that were not cultivated as organic would be shown as organic
    # # # Create dictionary with ID and organic information
    # # df_s["organic"] = 0
    # # df_s[df_s_org_col] = df_s[df_s_org_col].str.replace(" ", "")
    # # df_s.loc[df_s[df_s_org_col] != "", "organic"] = 1
    # # org_dict = dict(zip(df_s["field_id"], df_s["organic"]))
    # #
    # # # Add organic info to vector data
    # # gdf_s["organic"] = gdf_s["field_id"].map(org_dict)
    # # gdf_s.loc[gdf_s["organic"].isna(), "organic"] = 0
    #
    # # remove trailing zeros from codes
    # df_c["Code"] = df_c["Code"].astype(str).str.lstrip('0')
    # df_c.drop_duplicates(subset="Code", inplace=True)
    # gdf_s[gdf_c_col] = gdf_s[gdf_c_col].astype(str).str.lstrip('0')
    #
    # # Add Nutzungsbeschreibung to vector data
    # gdf_s = pd.merge(gdf_s, df_c, "left", left_on=gdf_c_col, right_on="Code")
    #
    # nas = list(gdf_s.loc[gdf_s["Kulturart"].isna(), gdf_c_col].unique())
    #
    # if len(nas) > 0:
    #     with open(fr"data\vector\IACS\DE\LSA\Original\Nutzungscodes\{year}_no_classification.csv", "w") as file:
    #         for code in nas:
    #             file.write(str(code) + "\n")
    #
    # gdf_s.rename(columns={
    #     "REGISTRIER": "REG_NR",
    #     "REGNR": "REG_NR",
    #     gdf_s_flik_col: "FLIK",
    #     gdf_s_nr_col: "SCHLAGNR"
    # }, inplace=True)
    #
    # # Concatenate landscape elements to schlaege (make sure the
    # mand_cols = ["field_id", "FLIK", "REG_NR", "SCHLAGNR", "Code", "Kulturart", "organic", "geometry"]
    #
    # gdf_out = gdf_s[mand_cols].copy()
    #
    # gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    # gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    # gdf_out.drop(columns=["field_id_distinguished"], inplace=True)
    #
    # gdf_out.loc[gdf_out["organic"].isna(), "organic"] = 0
    #
    # print(len(gdf_out), num_s)
    # if len(gdf_out) == num_s:
    #     gdf_out.to_parquet(out_pth)

    # # #### 2016
    # schlaege_vec_pth = r"data\vector\IACS\DE\LSA\Original\Schlaege\GFN_Schlaege_2016.shp"
    # teilschlaege_vec_pth = r"data\vector\IACS\DE\LSA\Original\Teilschlaege\AUM_Teilschlaege_2016.shp"
    # code_pth = r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx"
    # out_pth = r"data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_2016.geoparquet"
    # year = 2016
    # gdf_c_col = "KULTURCODE"
    # gdf_s_nr_col = "SCHLAGNR"
    # gdf_s_flik_col = "FLIK"
    #
    # # Open Data
    # gdf_s = gpd.read_file(schlaege_vec_pth)
    # df_c = pd.read_excel(code_pth, sheet_name=str(year))
    # gdf_ts = gpd.read_file(teilschlaege_vec_pth)
    #
    # # Create unique field ID
    # gdf_s.loc[gdf_s[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_s[gdf_s_nr_col] = gdf_s[gdf_s_nr_col].astype(int).astype(str)
    # gdf_s.loc[gdf_s[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_s["field_id"] = gdf_s[gdf_s_flik_col] + '_' + gdf_s[gdf_s_nr_col]
    #
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_ts[gdf_s_nr_col] = gdf_ts[gdf_s_nr_col].astype(int).astype(str)
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_ts["field_id"] = gdf_ts[gdf_s_flik_col] + '_' + gdf_ts[gdf_s_nr_col]
    # gdf_ts["organic"] = 0
    # gdf_ts.loc[gdf_ts["FOERDERART"].isna(), "FOERDERART"] = "BBBB"
    # gdf_ts.loc[gdf_ts["FOERDERART"].str.contains("BV1"), "organic"] = 1
    # # l = list(gdf_ts["FOERDERART"].unique())
    # # l = [i for i in l if type(i) is str]
    # # l = [i.split(",") for i in l]
    # # l2 = []
    # # for i in l:
    # #     l2 += i
    # # l2 = [i.replace(" ", "") for i in l2]
    # # l2 = list(set(l2))
    # org_dict = dict(zip(gdf_ts["field_id"], gdf_ts["organic"]))
    #
    # ## Add organic cultivation to schlaege
    # gdf_s["organic"] = gdf_s["field_id"].map(org_dict)
    # gdf_s.loc[gdf_s["organic"].isna(), "organic"] = 0
    #
    # num_s = len(gdf_s)
    #
    # # remove trailing zeros from codes
    # df_c["Code"] = df_c["Code"].astype(str).str.lstrip('0')
    # df_c.drop_duplicates(subset="Code", inplace=True)
    # gdf_s[gdf_c_col] = gdf_s[gdf_c_col].astype(str).str.lstrip('0')
    #
    # # Add Nutzungsbeschreibung to vector data
    # gdf_s = pd.merge(gdf_s, df_c, "left", left_on=gdf_c_col, right_on="Code")
    #
    # nas = list(gdf_s.loc[gdf_s["Kulturart"].isna(), gdf_c_col].unique())
    #
    # if len(nas) > 0:
    #     with open(fr"data\vector\IACS\DE\LSA\Original\Nutzungscodes\{year}_no_classification.csv", "w") as file:
    #         for code in nas:
    #             file.write(str(code) + "\n")
    #
    # # Concatenate landscape elements to schlaege (make sure the
    # mand_cols = ["field_id", "FLIK", "REG_NR", "SCHLAGNR", "Code", "Kulturart", "organic", "geometry"]
    #
    # gdf_out = gdf_s[mand_cols].copy()
    #
    # gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    # gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    # gdf_out.drop(columns=["field_id_distinguished"], inplace=True)
    #
    # gdf_out.loc[gdf_out["organic"].isna(), "organic"] = 0
    #
    # print(len(gdf_out), num_s)
    # if len(gdf_out) == num_s:
    #     gdf_out.to_parquet(out_pth)
    #
    # #### 2015
    # schlaege_vec_pth = r"data\vector\IACS\DE\LSA\Original\Schlaege\GFN_Schlaege_2015.shp"
    # teilschlaege_vec_pth = r"data\vector\IACS\DE\LSA\Original\Teilschlaege\AUM_Teilschlaege_2015.shp"
    # code_pth = r"data\vector\IACS\DE\LSA\Original\Nutzungscodes\Nutzungscodes_2014-2025.xlsx"
    # out_pth = r"data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_2015.geoparquet"
    # year = 2015
    # gdf_c_col = "KULTURCODE"
    # gdf_s_nr_col = "SCHLAG_NR"
    # gdf_s_flik_col = "FLIK"
    #
    # # Open Data
    # gdf_s = gpd.read_file(schlaege_vec_pth)
    # df_c = pd.read_excel(code_pth, sheet_name=str(year))
    # gdf_ts = gpd.read_file(teilschlaege_vec_pth)
    #
    # # Create unique field ID
    # gdf_s.loc[gdf_s[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_s[gdf_s_nr_col] = gdf_s[gdf_s_nr_col].astype(int).astype(str)
    # gdf_s.loc[gdf_s[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_s["field_id"] = gdf_s[gdf_s_flik_col] + '_' + gdf_s[gdf_s_nr_col]
    #
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col].isna(), gdf_s_nr_col] = 9999
    # gdf_ts[gdf_s_nr_col] = gdf_ts[gdf_s_nr_col].astype(int).astype(str)
    # gdf_ts.loc[gdf_ts[gdf_s_nr_col] == "9999", gdf_s_nr_col] = ""
    # gdf_ts["field_id"] = gdf_ts[gdf_s_flik_col] + '_' + gdf_ts[gdf_s_nr_col]
    # gdf_ts["organic"] = 0
    # gdf_ts.loc[gdf_ts["FOERDERART"].isna(), "FOERDERART"] = "BBBB"
    # gdf_ts.loc[gdf_ts["FOERDERART"].str.contains("BV1"), "organic"] = 1
    # # l = list(gdf_ts["FOERDERART"].unique())
    # # l = [i for i in l if type(i) is str]
    # # l = [i.split(",") for i in l]
    # # l2 = []
    # # for i in l:
    # #     l2 += i
    # # l2 = [i.replace(" ", "") for i in l2]
    # # l2 = list(set(l2))
    # org_dict = dict(zip(gdf_ts["field_id"], gdf_ts["organic"]))
    #
    # # Add organic info to vector data
    # gdf_s["organic"] = gdf_s["field_id"].map(org_dict)
    # gdf_s.loc[gdf_s["organic"].isna(), "organic"] = 0
    #
    # num_s = len(gdf_s)
    #
    # # remove trailing zeros from codes
    # df_c["Code"] = df_c["Code"].astype(str).str.lstrip('0')
    # df_c.drop_duplicates(subset="Code", inplace=True)
    # gdf_s[gdf_c_col] = gdf_s[gdf_c_col].astype(str).str.lstrip('0')
    #
    # # Add Nutzungsbeschreibung to vector data
    # gdf_s = pd.merge(gdf_s, df_c, "left", left_on=gdf_c_col, right_on="Code")
    #
    # nas = list(gdf_s.loc[gdf_s["Kulturart"].isna(), gdf_c_col].unique())
    #
    # if len(nas) > 0:
    #     with open(fr"data\vector\IACS\DE\LSA\Original\Nutzungscodes\{year}_no_classification.csv", "w") as file:
    #         for code in nas:
    #             file.write(str(code) + "\n")
    #
    # # Concatenate landscape elements to schlaege (make sure the
    # gdf_s.rename(columns={"SCHLAG_NR": "SCHLAGNR"}, inplace=True)
    # mand_cols = ["field_id", "FLIK", "REG_NR", "SCHLAGNR", "Code", "Kulturart", "organic", "geometry"]
    #
    # gdf_out = gdf_s[mand_cols].copy()
    #
    # gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    # gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    # gdf_out.drop(columns=["field_id_distinguished"], inplace=True)
    #
    # gdf_out.loc[gdf_out["organic"].isna(), "organic"] = 0
    #
    # print(len(gdf_out), num_s)
    # if len(gdf_out) == num_s:
    #     gdf_out.to_parquet(out_pth)

    adm = gpd.read_file(r"data\vector\administrative\LSA_Bremen_Hamburg.gpkg")

    for year in range(2015, 2018): # the other don't need this cleaning
        pth = rf"Q:\Europe-LAND\data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_{year}.geoparquet"
        gdf = gpd.read_parquet(pth)
        gdf_c = gdf.copy()
        gdf_c["geometry"] = gdf_c["geometry"].centroid
        adm = adm.to_crs(gdf_c.crs)
        intersection = gpd.sjoin(gdf_c, adm)
        gdf_out = gdf.loc[gdf["field_id"].isin(intersection["field_id"])].copy()
        pth2 = rf"Q:\Europe-LAND\data\vector\IACS\DE\LSA\prepared_data\Schlaege+LSE_{year}_b.geoparquet"
        print(len(gdf), len(gdf_out))
        gdf_out.to_parquet(pth2)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')