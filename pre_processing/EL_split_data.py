# Author:
# github repository:


# 1. Loop over files and classify the crops and unify the column names.
# 2. Save a new version of the IACS data.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time
import fiona
import numpy as np
import pandas as pd
import math
import geopandas as gpd
from osgeo import ogr

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def records(filename, list):
    ## see https://gis.stackexchange.com/questions/220023/only-read-specific-rows-of-a-shapefile-with-geopandas-fiona
    list = sorted(list) # if the elements of the list are not sorted
    with fiona.open(filename) as source:
        for i, feature in enumerate(source[:max(list)+1]):
            if i in list:
                yield feature

def chunk_large_vector(in_pth, out_folder, chunk_size=500000):


    data_source = ogr.Open(in_pth)
    if data_source is None:
        print("Failed to open the file.")
        return
    else:
        layer = data_source.GetLayer()
        feature_count = layer.GetFeatureCount()
        print(f"Number of features: {feature_count}")
        data_source = None  # Close the data source

    num_chunks = math.ceil(feature_count / chunk_size)
    chunk_inds = [[i*chunk_size, (i+1) * chunk_size] for i in range(num_chunks)]

    gpd.GeoDataFrame.from_features(records("test.shp", [4, 0, 7]))


def separate_unique_crop_code_from_file(in_dir, region_id, col_translate_pth, out_folder, encoding, multiple_crop_entries_sep,
                                        ignore_files_descr=None, file_year_encoding=None):
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
    # ToDo: It seems that I created an endless loop here. It works, but it should stop after the first round of corrections of files
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

        # Create the destination file
        out_driver = ogr.GetDriverByName("GPKG")
        destination_file = f"{out_folder}/{os.path.basename(path).split('.')[0]}_sep.gpkg"
        if os.path.exists(destination_file):
            os.remove(destination_file)
        destination_ds = out_driver.CreateDataSource(destination_file)

        if destination_ds is None:
            print(f"Failed to create the destination file: {destination_file}")
            return

        # Create the new layer in the destination with the same spatial reference as the source
        destination_layer = destination_ds.CreateLayer(lyr.GetName(), lyr.GetSpatialRef(),
                                                       lyr.GetGeomType())

        # Copy the fields (attribute table structure)
        layer_definition = lyr.GetLayerDefn()
        for i in range(layer_definition.GetFieldCount()):
            field_definition = layer_definition.GetFieldDefn(i)
            destination_layer.CreateField(field_definition)

        if (type(col_dict["field_id"]) != float):
            field_id_col = col_dict["field_id"]
        else:
            field_id_col = False

        ## Loop over features to derive crop names of current file
        field_ids_codes = []
        field_ids_names = []
        codes = []
        names = []
        for f, feat in enumerate(lyr):
            # if f >= 100:
            #     break  # Stop after copying 100 features
            print(f)

            if field_id_col:
                field_id = feat.GetField(field_id_col)
            else:
                field_id = feat.GetFID()

            ## copy feature to out_file
            new_feature = ogr.Feature(layer_definition)
            new_feature.SetGeometry(feat.GetGeometryRef().Clone())
            new_feature.SetFID(feat.GetFID())
            for i in range(layer_definition.GetFieldCount()):
                new_feature.SetField(i, feat.GetField(i))

            ## split multiple crops from column
            if type(col_dict["crop_code"]) != float:
                ## get all the codes stored in the column
                crop_codes = str(feat.GetField(col_dict["crop_code"])).split(multiple_crop_entries_sep)
                if len(crop_codes) > 1:
                    ## if multiple codes are found, save them in the result list
                    codes += crop_codes[1:]
                    field_ids_codes += [field_id for i in range(len(crop_codes[1:]))]
                    ## and overwrite the field "crop_code"
                    new_feature.SetField(col_dict["crop_code"], crop_codes[0])

            ## ToDo: So far, we only need this for the greek data. They only have crop codes. I skip the crop names, as
            ## I don't want to deal with the field ids. THe problem is, I dont know how to handle the situation when
            ## multiple codes are provided but less names. Then I create ID lists with different length.
            # if type(col_dict["crop_name"]) != float:
            #     ## get all the codes stored in the column
            #     crop_names = str(feat.GetField(col_dict["crop_name"])).split(multiple_crop_entries_sep)
            #     if len(crop_names) > 1:
            #         ## if multiple codes are found, save them in the result list
            #         names.append(crop_names[1:])
            #         field_ids_names.append([field_id for i in range(len(crop_names[1:]))])
            #         ## and overwrite the field "crop_code"
            #         new_feature.SetField(col_dict["crop_name"], crop_names[0])

            # Add the feature to the destination layer
            destination_layer.CreateFeature(new_feature)
            new_feature = None
        lyr.ResetReading()
        ds = None
        destination_ds = None

        out_df_pth = f"{out_folder}/{os.path.basename(path).split('.')[0]}_sep.csv"
        out_df = pd.DataFrame({'field_id': field_ids_codes, col_dict["crop_code"]: codes})
        out_df.to_csv(out_df_pth, index=False, encoding=encoding)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    run_dict = {
        "EL": {
            "region_id": "EL",
            "from_lang": "el",
            "file_encoding": "utf-8",
            "multiple_crop_entries_sep": ",",
            "ignore_files_descr": "stables"
        }
    }

    ## Loop through tasks in run_dict
    for country_code in run_dict:
        print(country_code)
        region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")
        encoding = run_dict[country_code]["file_encoding"]
        multiple_crop_entries_sep = run_dict[country_code]["multiple_crop_entries_sep"]

        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None

        if "file_year_encoding" in run_dict[country_code]:
            file_year_encoding = run_dict[country_code]["file_year_encoding"]
        else:
            file_year_encoding = None

        separate_unique_crop_code_from_file(
            in_dir=fr"data\vector\IACS\{country_code}",
            region_id=region_id,
            col_translate_pth=rf"data\tables\{region_id}_column_name_translation.xlsx",
            out_folder=fr"data\vector\IACS\{country_code}",
            multiple_crop_entries_sep=multiple_crop_entries_sep,
            encoding=encoding,
            file_year_encoding=file_year_encoding,
            ignore_files_descr=ignore_files_descr)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')