# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import geopandas as gpd
from osgeo import gdal
import pandas as pd

gdal.SetConfigOption("OGR_GEOMETRY_ACCEPT_UNCLOSED_RING", "NO")

from my_utils import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
WD = r"Q:\Europe-LAND"
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def count_duplicate_geometries(in_pth):

    gdf = gpd.read_file(in_pth)
    gdf["geom_id"] = gdf.geometry.to_wkb()

    dups = gdf[gdf.duplicated("geom_id", "first")].copy()

    print(f"{len(dups)} geometry duplicates were found for {in_pth}.")


def count_duplicate_geometries_v2(gdf):
    gdf["geom_id"] = gdf.geometry.to_wkb()

    dups = gdf[gdf.duplicated("geom_id", "first")].copy()

    gdf.drop(columns="geom_id", inplace=True)

    print(f"{len(dups)} geometry duplicates were found.")


def extract_geometry_duplicates(in_pth, out_pth):

    gdf = gpd.read_file(in_pth)
    gdf["geom_id"] = gdf.geometry.to_wkb()

    dups = gdf[gdf.duplicated("geom_id", "first")].copy()

    print(f"{len(dups)} geometry duplicates were found for {in_pth}.")

    dups_out = gdf.loc[gdf["geom_id"].isin(list(dups["geom_id"].unique()))].copy()
    dups_out.drop(columns="geom_id", inplace=True)
    dups.to_file(out_pth)


def remove_geometry_duplicates(gdf):

    in_len = len(gdf)
    gdf["geom_id"] = gdf.geometry.to_wkb()

    gdf.drop_duplicates(subset="geom_id", inplace=True)
    out_len = len(gdf)
    gdf.drop(columns="geom_id", inplace=True)
    return gdf


def create_unique_id(gdf, flik_flek="flik_flek", fl_kenng="fl_kenng"):

    gdf["uni_id"] = gdf[flik_flek] + '_' + gdf[fl_kenng].astype(str)

    print("Number Unique IDs:", len(gdf["uni_id"].unique()))
    print("Number Parcels:", len(gdf))

    if len(gdf["uni_id"].unique()) != len(gdf):
        print("Number of Unique IDs not yet unique. Adding cumulative count.")
        gdf['uni_id_distinguished'] = gdf.groupby('uni_id').cumcount() + 1
        gdf.loc[gdf['uni_id_distinguished'].isna(), 'uni_id_distinguished'] = 1
        gdf['uni_id'] = gdf['uni_id'].astype(str) + '_' + gdf['uni_id_distinguished'].astype(str)
        gdf.drop(columns=["uni_id_distinguished"], inplace=True)
        print("Number Unique IDs:", len(gdf["uni_id"].unique()))
        print("Number Parcels:", len(gdf))

    return gdf

def add_landscape_elements_to_crop_vectors(gdf_s, gdf_le, gdf_s_nr_col, gdf_s_flik_col,
                                           le_s_nr_col, le_s_flik_col, gdf_c_col, le_code_col,
                                           gdf_n_col, le_n_col, gdf_area_col, le_area_col):

    print("Adding landscape elements to fields.")

    gdf_le = gdf_le.to_crs(gdf_s.crs)

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

    gdf_le.rename(columns={
        le_s_flik_col: gdf_s_flik_col,
        le_s_nr_col: gdf_s_nr_col,
        le_code_col: gdf_c_col,
        le_n_col: gdf_n_col,
        le_area_col: gdf_area_col
    }, inplace=True)

    # Concatenate landscape elements to schlaege
    mand_cols = ["field_id", gdf_s_flik_col, gdf_s_nr_col, gdf_c_col, gdf_n_col, gdf_area_col, "geometry"]

    gdf_out = pd.concat([gdf_s, gdf_le[mand_cols]])

    gdf_out['field_id_distinguished'] = gdf_out.groupby('field_id').cumcount() + 1
    gdf_out['field_id'] = gdf_out['field_id'].astype(str) + '_' + gdf_out['field_id_distinguished'].astype(str)
    gdf_out.drop(columns=["field_id_distinguished"], inplace=True)

    print(len(gdf_out), num_s + num_le)

    print("Number Unique IDs:", len(gdf_out["field_id"].unique()))
    print("Number Parcels:", len(gdf_out))

    return gdf_out


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## 1. Check if data contain a unique field id
    ## if not, create one. In the best case it hase some meaning.

    ## 2. Check if data contain duplicate geometries
    ## If yes, then check if the entries are complete duplicates of each other (e.g. by looking at actual polygon area
    ## and by comparing it with the reported land use/crop area).
    ## If they are complete duplicates, then remove them.
    ## If no, then check if the data are field blocks and each entry is actually a field entry

    ## 3. Check if data contain non-geometries.

    ## 4. Check if data also have fields outside their administrative borders

    ## Exploration
    # in_dir = os.path.join("data", "vector", "IACS", "DE", "SAT", "Original", "Flächennutzung_Antrag")
    # iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)
    #
    # for i, in_pth in enumerate(iacs_files):
    #
    #     year = helper_functions.get_year_from_path(in_pth)
    #     print(year)
    #     print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")
    #     # count_duplicate_geometries(in_pth)
    #     out_pth = os.path.join("data", "vector", "IACS", "DE", "SAT", "Original", f"DUPS-layer_st_{year}.gpkg")
    #     if year == "2023":
    #         extract_geometry_duplicates(in_pth, out_pth)

    ## Actual pre-processing
    ## Only for 2013:
    # in_pth = r"data\vector\IACS\DE\SAT\Flächennutzung_Antrag\Antraege2013.shp"
    # prep_pth = r"Qdata\vector\IACS\DE\SAT\Flächennutzung_Antrag\Antraege2013_prep.gpkg"
    # copy_to_pth = r"data\vector\IACS\DE\SAT\Referenz"
    #
    # gdf = gpd.read_file(in_pth)
    # print(len(gdf))
    # code_to_name = gdf.dropna(subset=["NU_CODE"]).drop_duplicates(subset=["NU_CODE"]).set_index("NU_CODE")["NU_BEZ"]
    # gdf["NU_BEZ"] = gdf["NU_CODE"].map(code_to_name).fillna(gdf["NU_BEZ"])
    #
    # print(len(gdf))
    # gdf.to_file(prep_pth)
    #
    # lst = glob.glob(os.path.splitext(in_pth)[0] + ".*")
    # for file in lst:
    #     shutil.copy(
    #         src=file,
    #         dst=copy_to_pth
    #     )
    #     os.remove(file)

    in_dir = os.path.join("data", "vector", "IACS", "DE", "SAT", "Original", "Flächennutzung_Antrag")
    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

    col_dict = {
        "2008": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": False},
        "2009": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": False},
        "2010": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": False},
        "2011": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": True,
                 "flek_col": "FLEK",
                 "le_bez_col": "L_BEZEICH",
                 "le_code_col": "CODE"},
        "2012": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": True,
                 "flek_col": "FLEK",
                 "le_bez_col": "L_BEZEICH",
                 "le_code_col": "CODE"},
        "2013": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": True,
                 "flek_col": "FLEK",
                 "le_bez_col": "L_BEZEICH",
                 "le_code_col": "CODE"},
        "2014": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": True,
                 "flek_col": "FLEK",
                 "le_bez_col": "L_BEZEICH",
                 "le_code_col": "CODE"},
        "2015": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": True,
                 "flek_col": "FLEK",
                 "le_bez_col": "L_BEZEICH",
                 "le_code_col": "CODE"},
        "2016": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": False},
        "2017": {"flik_col": "FB_FLIK",
                 "fl_kenng": "PARZ_NR",
                 "crop_code_col": "NU_CODE",
                 "crop_name_col": "NU_BEZ",
                 "le_exist": True,
                 "flek_col": "FLEK",
                 "le_bez_col": "L_BEZEICH",
                 "le_code_col": "CODE"},
        "2018": {"flik_col": "ref_ident",
                 "fl_kenng": "parz_nr",
                 "crop_code_col": "code",
                 "crop_name_col": "code_bez",
                 "le_exist": True,
                 "flek_col": "flek",
                 "le_bez_col": "l_bezeich",
                 "le_code_col": "code"},
        "2019": {"flik_col": "REF_IDENT",
                 "fl_kenng": None,
                 "crop_code_col": "CODE",
                 "crop_name_col": "CODE_BEZ",
                 "le_exist": True,
                 "flek_col": "FLEK",
                 "le_bez_col": "L_BEZEICH",
                 "le_code_col": "CODE"},
        "2020": {"flik_col": "ref_ident",
                 "fl_kenng": None,
                 "crop_code_col": "code",
                 "crop_name_col": "code_bez",
                 "le_exist": True,
                 "flek_col": "flek",
                 "le_bez_col": "l_bezeich",
                 "le_code_col": "code"},
        "2021": {"flik_col": "ref_ident",
                 "fl_kenng": None,
                 "crop_code_col": "code",
                 "crop_name_col": "code_bez",
                 "le_exist": True,
                 "flek_col": "FLEK",
                 "le_bez_col": "L_BEZEICH",
                 "le_code_col": "CODE"},
        "2022": {"flik_col": "ref_ident",
                 "fl_kenng": None,
                 "crop_code_col": "code",
                 "crop_name_col": "code_bez",
                 "le_exist": True,
                 "flek_col": "flek",
                 "le_bez_col": "l_bezeich",
                 "le_code_col": "code"
                 },
        "2023": {"flik_col": "ref_ident",
                 "fl_kenng": None,
                 "crop_code_col": "code",
                 "crop_name_col": "code_bez",
                 "le_exist": False},
    }

    for i, in_pth in enumerate(iacs_files):

        year = helper_functions.get_year_from_path(in_pth)
        # if year in ["2013", "2020", "2022", "2023", "2008", "2009", "2010", "2012", "2014", "2015", "2016", "2017",
        #             "2018"]:
        #     continue

        print(year)
        print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")

        in_le_pth = os.path.join("data", "vector", "IACS", "DE", "SAT", "Original", "Landschaftselemente", f"LE_{year}.shp")
        out_folder = os.path.join("data", "vector", "IACS", "DE", "SAT")

        ## Open files
        gdf = gpd.read_file(in_pth)

        flik_col = col_dict[year]["flik_col"]
        fl_kenng = col_dict[year]["fl_kenng"]

        if not fl_kenng:
            fl_kenng = "fl_kenng"
            gdf[fl_kenng] = "0"

        ## Remove NAs from FLIK and Schlagnr to be able to create unique IDs
        gdf.loc[gdf[flik_col].isna(), flik_col] = "DESTLI0"
        gdf.loc[gdf[fl_kenng].isna(), fl_kenng] = "0"

        gdf = helper_functions.drop_non_geometries(gdf)
        gdf = remove_geometry_duplicates(gdf)

        ## Calculate area
        gdf["area_ha"] = round(gdf.geometry.area / 10000, 3)

        ## Add landscape elements to vector files
        if col_dict[year]["le_exist"]:
            gdf_le = gpd.read_file(in_le_pth)

            flek_col = col_dict[year]["flek_col"]
            le_bez_col = col_dict[year]["le_bez_col"]
            le_code_col = col_dict[year]["le_code_col"]
            crop_code_col = col_dict[year]["crop_code_col"]
            crop_name_col = col_dict[year]["crop_name_col"]

            ## There are in general no parcel numbers in the data, thus we set them to 0
            gdf_le[fl_kenng] = "0"

            ## Calculate area
            gdf_le["area_ha"] = round(gdf_le.geometry.area / 10000, 3)

            ## Give meaningful names
            le_dict = {
                "HK": "Heck/Knick",
                "BR": "Baumreihen",
                "FH": "Feldgehölz",
                "FG": "Feuchtgebiete",
                "EB": "Einzelbaum",
                "TÜ": "Tümpel",
                "NT": "Naturstein und Trockenmauer, Lesesteinwall",
                "FS": "Fels- und Steinriegel sowie naturversteinte Fläche",
                "FR": "Feldraine",
                "BD": "Binnendüne",
                "TR": "Terrassen",
                "T": "Feldgehölz"
            }
            gdf_le[le_bez_col] = gdf_le[le_bez_col].map(le_dict)
            print(list(gdf_le[le_bez_col].unique()))
            gdf_le[le_code_col] = gdf_le[le_code_col].astype(str)
            gdf[crop_code_col] = gdf[crop_code_col].astype(str)

            gdf = add_landscape_elements_to_crop_vectors(gdf_s=gdf, gdf_le=gdf_le, gdf_s_nr_col=fl_kenng, gdf_s_flik_col=flik_col,
                                                   le_s_nr_col=fl_kenng, le_s_flik_col=flek_col, gdf_c_col=crop_code_col, le_code_col=le_code_col,
                                                   gdf_n_col=crop_name_col, le_n_col=le_bez_col, gdf_area_col="area_ha", le_area_col="area_ha")
        else:

            gdf = create_unique_id(gdf, flik_flek=flik_col, fl_kenng=fl_kenng)

        gdf.to_parquet(os.path.join(out_folder, f"layer_st_{year}.geoparquet"))



    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')