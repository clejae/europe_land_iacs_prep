# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# This script takes the original IACS/GSA data and provides a table with column names and an example of the
# content of the columns. The user has to put the original files in a folder named with the Member state code
# of the Interinstitutional Style Guide of the EU (see link next line) in following place: data\vector\IACS\XX
# https://style-guide.europa.eu/en/content/-/isg/topic?identifier=annex-a6-country-and-territory-codes
# For example, Danish data would be stored here: data/vector/IACS/DK.
# If you have sub-datasets for the country, e.g. for federal states in Germany, create a subfolder. Choose an abbreviation
# of your liking. For example, for Thuringian data from Germany, the data would be stored here: data/vector/IACS/DE/THU
# Each file should contain a number indicating the year of the data, best case 4 digits, but the last 2 also work.

# After the script has run: Manually assign column names to "universal" column names in a column_name_translation table.
# This table should be stored in
# data\tables\column_name_translations\XX_column_name_translations.xlsx,
# where XX stands for the country or country_subregion abbreviation (XX_XXX_). We used Excel-tables, because that was
# easiest for handling the encoding mess of all the member states.

# If you want to run this script for a specific country, include an entry into the run_dict which can be found
# the top of the main function.
# The run_dict key should be the country or country and subdivision abbreviations (for example, "DK" or "DE/THU). The
# item should be another dictionary. In this dictionary, you should include the following keys:
#
# "file_encoding" - Encoding of the original GSA file
# "file_year_encoding" - [optional] use if specific years deviate from that encoding
# "ignore_file_descr" - [optional] use if there are other geospatial datasets in your folder that are not GSA data
#
# For example: CZ": {"file_encoding": "ISO-8859-1", file_year_encoding": {"2015": "utf-8"}, "ignore_files_descr": "other data"}

# To turn off/on the processing of a specific country, set the key "switch" in the dictionary to "off" or "on"

# There is a second run_dict for animal data at the bottom of the main function. This is work in progress.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import sys
os.environ["GDAL_DRIVER_PATH"] = os.path.join(f'{os.sep}'.join(sys.executable.split(os.sep)[:-1]), 'Library', 'lib', 'gdalplugins')
from os.path import dirname, abspath
import time
import pandas as pd
import chardet

from my_utils import helper_functions

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
    out_df.to_csv(out_pth, index=False)


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
        "AT": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "Original"},
        "BG": {"switch": "off", "file_encoding": "windows-1251"},
        "BE/FLA": {"switch": "off", "file_encoding": "utf-8"},
        "BE/WAL": {"switch": "off", "file_encoding": "utf-8"},
        "CY/LPIS": {"switch": "off", "file_encoding": "utf-8"},
        "CY/APPL": {"switch": "off", "file_encoding": "utf-8"},
        "CZ": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "Original"},
        "DE/BRB": {"switch": "off", "file_encoding": "ISO-8859-1"},
        "DE/LSA": {"switch": "off", "file_encoding": "utf-8", "file_year_encoding": {"2015": "ISO-8859-1", "2016": "ISO-8859-1",
                                                                    "2017": "ISO-8859-1", "2018": "ISO-8859-1",
                                                                    "2019": "ISO-8859-1"}, "ignore_files_descr": "ignore"},
        "DE/MWP": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "public"},
        "DE/NRW": {"switch": "off", "file_encoding": "ISO-8859-1", "ignore_files_descr": "HIST"}, # Public
        "DE/NRW": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "public"},
        "DE/RLP": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "public"},
        "DE/SAT": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "Referenz"},
        "DE/SAA": {"switch": "off", "file_encoding": "utf-8", "file_year_encoding": {"2023": "windows-1252"}, "ignore_files_descr": "Antrag"}, #old
        "DE/SAA": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "old"},
        "DE/THU": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "ZN"},
        "DK": {"switch": "off", "file_encoding": "ISO-8859-1"},
        "EE": {"switch": "off", "file_encoding": "utf-8"},
        "EL": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "stables"},
        "ES/ALA": {"switch": "off", "file_encoding": "utf-8"},
        "FI": {"switch": "off", "file_encoding": "utf-8"},
        "FR/FR": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "ILOTS_ANONYMES"},
        "FR/ARA": {"switch": "off", "file_encoding": "utf-8"},
        "FR/BRC": {"switch": "off", "file_encoding": "utf-8"},
        "FR/BRE": {"switch": "off", "file_encoding": "utf-8"},
        "FR/COR": {"switch": "off", "file_encoding": "utf-8"},
        "FR/CVL": {"switch": "off", "file_encoding": "utf-8"},
        "FR/GRE": {"switch": "off", "file_encoding": "utf-8"},
        "FR/HDF": {"switch": "off", "file_encoding": "utf-8"},
        "FR/IDF": {"switch": "off", "file_encoding": "utf-8"},
        "FR/NOR": {"switch": "off", "file_encoding": "utf-8"},
        "FR/NOU": {"switch": "off", "file_encoding": "utf-8"},
        "FR/OCC": {"switch": "off", "file_encoding": "utf-8"},
        "FR/PDL": {"switch": "off", "file_encoding": "utf-8"},
        "FR/PRO": {"switch": "off", "file_encoding": "utf-8"},
        "HR": {"switch": "off", "file_encoding": "utf-8"},
        "HU": {"switch": "off", "file_encoding": "utf-8"},
        "IE": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "Exclusions"},
        "IT/EMR": {"switch": "off", "file_encoding": "utf-8"},
        "IT/MAR": {"switch": "off", "file_encoding": "utf-8"},
        "IT/TOS": {"switch": "off", "file_encoding": "utf-8"},
        "LV": {"switch": "off", "file_encoding": "utf-8"},
        "LT": {"switch": "off", "file_encoding": "utf-8"},
        "NL": {"switch": "off", "file_encoding": "utf-8"},
        "PL": {"switch": "off", "file_encoding": "windows-1250"},
        "PT/ALE": {"switch": "off", "file_encoding": "utf-8"},
        "PT/ALG": {"switch": "off", "file_encoding": "utf-8"},
        "PT/AML": {"switch": "off", "file_encoding": "utf-8"},
        "PT/CE": {"switch": "off", "file_encoding": "utf-8"},
        "PT/CEN": {"switch": "off", "file_encoding": "utf-8"},
        "PT/CES": {"switch": "off", "file_encoding": "utf-8"},
        "PT/NO": {"switch": "off", "file_encoding": "utf-8"},
        "PT/NON": {"switch": "off", "file_encoding": "utf-8"},
        "PT/NOS": {"switch": "off", "file_encoding": "utf-8"},
        "PT/PT": {"switch": "off", "file_encoding": "utf-8"},
        "RO": {"switch": "off", "file_encoding": "utf-8"},
        "SE": {"switch": "off", "file_encoding": "utf-8", "ignore_files_descr": "skiften"},
        "SI": {"switch": "off", "file_encoding": "utf-8"},
        "SK": {"switch": "off", "file_encoding": "utf-8"}
    }

    for country_code in run_dict:
        switch = run_dict[country_code].get("switch", "off").lower()
        if switch != "on":
            continue
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
            in_dir = os.path.join("data", "vector", "IACS", country_code),
            out_pth=os.path.join("data", "tables", "column_names",f"{country_code.replace('/','_')}_column_names.xlsx"),
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
            in_dir=os.path.join("data", "vector", "IACS", country_code),
            out_pth=os.path.join("data", "tables", "column_names",f"{country_code.replace('/','_')}_animal_table_column_names.xlsx"),
            ignore_files_descr=ignore_files_descr,
            encoding=encoding,
            sep=sep)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
