# Author:
# github repository:

# 1. Loop over available files and get column names with example attribute values
# 2. Save in data frame so that column names can be assigned to "universal" column names.
# Afterwards: Manually assign column names to "universal" column names.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import pandas as pd
import chardet

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)


# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#


def print_ogr_drivers():

    print("Print all available OGR drivers.")

    from osgeo import ogr

    cnt = ogr.GetDriverCount()
    formatsList = []  # Empty List
    for i in range(cnt):
        driver = ogr.GetDriver(i)
        driverName = driver.GetName()
        if not driverName in formatsList:
            formatsList.append(driverName)
    formatsList.sort()  # Sorting the messy list of ogr drivers
    for i in formatsList:
        print(i)

def get_geodata_column_names(path, encoding="utf-8"):
    import os
    from osgeo import ogr

    os.environ['SHAPE_ENCODING'] = encoding

    print(f"Get column names of geodata file. {path}")

    # print_ogr_drivers()

    file_name, file_extension = os.path.splitext(path)

    # if file_extension == ".geoparquet":
    #
    # else:
    driver_dict = {
        ".gdb": "OpenFileGDB",
        ".geojson": "GeoJSON",
        ".gpkg": "GPKG",
        ".shp": "ESRI Shapefile",
        ".gml": "GML",
        ".geoparquet": "Parquet"
    }

    driver = ogr.GetDriverByName(driver_dict[str.lower(file_extension)])
    ds = driver.Open(path, 0)
    lyr = ds.GetLayer(0)
    lyr_def = lyr.GetLayerDefn()

    column_names = []
    for i in range(lyr_def.GetFieldCount()):
        column_names.append(lyr_def.GetFieldDefn(i).GetName())

    # print(path, "\n", column_names)

    feat = lyr.GetNextFeature()
    attr_lst = []
    for fname in column_names:
        attr = feat.GetField(fname)
        # if type(feat.GetField(fname)) == str:
        #     if decoding:
        #         attr = feat.GetField(fname).decode(decoding)
        #     else:
        #         attr = feat.GetField(fname)
        # else:
        #     attr = feat.GetField(fname)
        attr_lst.append(attr)

    ds = None

    return column_names, attr_lst


def get_table_column_names(path, encoding, sep=","):
    import os
    from osgeo import ogr

    print(f"Get column names of table: {path}")

    file_name, file_extension = os.path.splitext(path)

    if "csv" in file_extension:
        df = pd.read_csv(path, nrows=3, encoding=encoding, sep=sep)
    if "xls" in file_extension:
        df = pd.read_excel(path, nrows=3)

    column_names = df.columns
    attr_lst = df.loc[0, :].values.tolist()

    return column_names, attr_lst


def list_column_names_of_iacs_data_in_dir(in_dir, out_pth, encoding=None, file_year_encoding=None, ignore_files_descr=None):

    print(f"List column names of iacs data in {in_dir}")

    # Get list of all IACS geospatial data in folder
    iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

    # Drop files that should be ignored
    if ignore_files_descr:
        iacs_files = [file for file in iacs_files if ignore_files_descr not in file]


    # Get column names of IACS data and save in dict along with example values
    res_dict = {}
    for p in iacs_files:
        year = helper_functions.get_year_from_path(p)
        if file_year_encoding:
            if year in file_year_encoding:
                year_encoding = file_year_encoding[year]
            else:
                year_encoding = encoding
        else:
            year_encoding = encoding
        column_names, attr_examples = get_geodata_column_names(p, year_encoding)
        res_dict[f"{year}_col"] = column_names
        res_dict[f"{year}_ex"] = attr_examples

    ## Sort by years
    keys = list(res_dict.keys())
    keys.sort()
    res_dict = {i: res_dict[i] for i in keys}

    ## Append lists that do not have the maximum length
    max_len = max(len(res_dict[x]) for x in res_dict)
    enc_lst = []
    for key in res_dict:
        ## get file_encoding of attributes
        sub_enc_lst = []
        for i in res_dict[key]:
            if type(i) == str:
                try:
                    e = chardet.detect(i.encode())['file_encoding']
                    sub_enc_lst.append(e)
                except:
                    continue
        enc_lst += sub_enc_lst

        ## append lists
        if len(res_dict[key]) < max_len:
            add = max_len - len(res_dict[key])
            for i in range(add):
                res_dict[key].append("")
    # enc = helper_functions.most_common(enc_lst)

    ## Save to df and disc
    out_df = pd.DataFrame.from_dict(res_dict)
    out_folder = os.path.dirname(out_pth)
    helper_functions.create_folder(out_folder)
    out_df.to_excel(out_pth, index=False)


def list_column_names_of_animal_data_in_dir(in_dir, out_pth, encoding, ignore_files_descr=None, sep=","):

    print(f"List column names of iacs data in {in_dir}")

    # Get list of all IACS geospatial data in folder
    iacs_files = helper_functions.list_tables_files_in_dir(in_dir)

    # Drop files that should be ignored
    if ignore_files_descr:
        iacs_files = [file for file in iacs_files if ignore_files_descr not in file]

    # Get column names of IACS data and save in dict along with example values
    res_dict = {}
    for p in iacs_files:
        year = helper_functions.get_year_from_path(p)
        column_names, attr_examples = get_table_column_names(p, encoding=encoding, sep=sep)
        res_dict[f"{year}_col"] = column_names
        res_dict[f"{year}_ex"] = attr_examples

    ## Sort by years
    keys = list(res_dict.keys())
    keys.sort()
    res_dict = {i: res_dict[i] for i in keys}

    ## Append lists that do not have the maximum length
    max_len = max(len(res_dict[x]) for x in res_dict)
    enc_lst = []
    for key in res_dict:
        ## append lists
        if len(res_dict[key]) < max_len:
            add = max_len - len(res_dict[key])
            for i in range(add):
                res_dict[key].append("")

    ## Save to df and disc
    out_df = pd.DataFrame.from_dict(res_dict)
    out_folder = os.path.dirname(out_pth)
    helper_functions.create_folder(out_folder)
    out_df.to_excel(out_pth, index=False)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## To turn off/on the processing of a specific country, just comment/uncomment the specific line

    ## List column names of the IACS vector data listed in the dictionary
    run_dict = {
        # "AT": {"file_encoding": "utf-8"},
        # "BG": {"file_encoding": "windows-1251"},
        # "BE/FLA": {"file_encoding": "utf-8"},
        # "BE/WAL": {"file_encoding": "ISO-8859-1"},
        # "CY/LPIS": {"file_encoding": "utf-8"},
        # "CY/APPL": {"file_encoding": "utf-8"},
        # "CZ": {"file_encoding": "ISO-8859-1", "ignore_files_descr": "IACS_Czechia"},
        # "DE/BRB": {"file_encoding": "ISO-8859-1"},
        "DE/LSA": {"file_encoding": "utf-8", "file_year_encoding": {"2015": "ISO-8859-1", "2016": "ISO-8859-1",
                                                                    "2017": "ISO-8859-1", "2018": "ISO-8859-1",
                                                                    "2019": "ISO-8859-1"}, "ignore_files_descr": "ignore"},
        # "DE/NRW": {"file_encoding": "ISO-8859-1", "ignore_files_descr": "HIST"},
        # "DE/SAT": {"file_encoding": "utf-8", "ignore_files_descr": "Referenz"},
        # "DE/SAA": {"file_encoding": "utf-8", "file_year_encoding": {"2023": "windows-1252"}, "ignore_files_descr": "Antrag"},
        # "DE/THU": {"file_encoding": "utf-8", "ignore_files_descr": "ZN"},
        # "DK": {"file_encoding": "ISO-8859-1"},
        # "EE": {"file_encoding": "utf-8"},
        # "EL": {"file_encoding": "utf-8", "ignore_files_descr": "stables"},
        # "ES/ALA": {"file_encoding": "utf-8"},
        # "FI": {"file_encoding": "utf-8"},
        # "FR/FR": {"file_encoding": "utf-8", "ignore_files_descr": "ILOTS_ANONYMES"},
        # "FR/ARA": {"file_encoding": "utf-8"},
        # "FR/BRC": {"file_encoding": "utf-8"},
        # "FR/BRE": {"file_encoding": "utf-8"},
        # "FR/COR": {"file_encoding": "utf-8"},
        # "FR/CVL": {"file_encoding": "utf-8"},
        # "FR/GRE": {"file_encoding": "utf-8"},
        # "FR/HDF": {"file_encoding": "utf-8"},
        # "FR/IDF": {"file_encoding": "utf-8"},
        # "FR/NOR": {"file_encoding": "utf-8"},
        # "FR/NOU": {"file_encoding": "utf-8"},
        # "FR/OCC": {"file_encoding": "utf-8"},
        # "FR/PDL": {"file_encoding": "utf-8"},
        # "FR/PRO": {"file_encoding": "utf-8"},
        # "HR": {"file_encoding": "utf-8"},
        # "HU": {"file_encoding": "utf-8"},
        # "IE": {"file_encoding": "utf-8", "ignore_files_descr": "Exclusions"},
        # "IT/EMR": {"file_encoding": "utf-8"},
        # "IT/MAR": {"file_encoding": "utf-8"},
        # "IT/TOS": {"file_encoding": "utf-8"},
        # "LV": {"file_encoding": "utf-8"},
        # "LT": {"file_encoding": "utf-8"},
        # "NL": {"file_encoding": "utf-8"},
        # "PT/ALE": {"file_encoding": "utf-8"},
        # "PT/ALG": {"file_encoding": "utf-8"},
        # "PT/AML": {"file_encoding": "utf-8"},
        # "PT/CE": {"file_encoding": "utf-8"},
        # "PT/CEN": {"file_encoding": "utf-8"},
        # "PT/CES": {"file_encoding": "utf-8"},
        # "PT/NO": {"file_encoding": "utf-8"},
        # "PT/NON": {"file_encoding": "utf-8"},
        # "PT/NOS": {"file_encoding": "utf-8"},
        # "PT/PT": {"file_encoding": "utf-8"},
        # "RO": {"file_encoding": "utf-8"},
        # "SE": {"file_encoding": "utf-8", "ignore_files_descr": "skiften"},
        # "SI": {"file_encoding": "utf-8"},
        # "SK": {"file_encoding": "utf-8"}
    }

    for country_code in run_dict:
        encoding = run_dict[country_code]["file_encoding"]
        if "file_year_encoding" in run_dict[country_code]:
            file_year_encoding = run_dict[country_code]["file_year_encoding"]
        else:
            file_year_encoding = None
        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None
        list_column_names_of_iacs_data_in_dir(
            in_dir=fr"data\vector\IACS\{country_code}",
            out_pth=rf"data\tables\column_names\{country_code.replace('/','_')}_column_names.xlsx",
            ignore_files_descr=ignore_files_descr,
            file_year_encoding=file_year_encoding,
            encoding=encoding)

    ## List column names of the IACS animal data listed in the dictionary
    run_dict = {
        # "DE/BB": {"file_encoding": "ISO-8859-1"},
        # "DE/ST": {"file_encoding": "utf-8", "ignore_files_descr": "Referenz"},
        # "AT": {"file_encoding": "ISO-8859-1", "sep": ";", "ignore_files_descr": "Measure"},
        ## "EL": {"file_encoding": "utf-8", "ignore_files_descr": "parcels"} # do not run. first the code needs to be adapted to also be able to run on geodata
    }

    for country_code in run_dict:
        encoding = run_dict[country_code]["file_encoding"]
        if "file_year_encoding" in run_dict[country_code]:
            file_year_encoding = run_dict[country_code]["file_year_encoding"]
        else:
            file_year_encoding = None
        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None
        if "sep" in run_dict[country_code]:
            sep = run_dict[country_code]["sep"]
        else:
            sep = ","
        list_column_names_of_animal_data_in_dir(
            in_dir=fr"data\vector\IACS\{country_code}",
            out_pth=rf"data\tables\column_names\{country_code.replace('/','_')}_animal_table_column_names.xlsx",
            ignore_files_descr=ignore_files_descr,
            encoding=encoding,
            sep=sep)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
