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
import glob
from shapely.validation import make_valid

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def combine_information_from_wfs(year):

    print(year)

    if year < 2023:

        sub_lst = glob.glob(fr"data\vector\IACS\PT\PT_temp\ocupacoes_solo\{year}\*.gpkg")
        sub_lst = [pth.split("_sub")[1].split("_")[0] for pth in sub_lst]
        print(len(sub_lst))

        parcels_lst = []
        add_crops_lst = []
        for sub in sub_lst:
            print(year, sub)

            ## Open files
            parc_pth = fr"data\vector\IACS\PT\PT_temp\parcelas\{year}\parcelas_sub{sub}_{year}.gpkg"
            ocup_pth = fr"data\vector\IACS\PT\PT_temp\ocupacoes_solo\{year}\ocupacoes_solo_sub{sub}_{year}.gpkg"
            cult_pth = fr"data\vector\IACS\PT\PT_temp\culturas\{year}\culturas_sub{sub}_{year}.gpkg"
            ocup = gpd.read_file(ocup_pth)

            if not os.path.exists(parc_pth):
                continue
            elif not os.path.exists(cult_pth):
                continue

            cult = gpd.read_file(cult_pth)
            parc = gpd.read_file(parc_pth)

            ## derive crop df
            cult = cult.drop_duplicates(subset=["PUN_CUL_COD", "PUN_CUL_DESC"])

            ## Combine ocupacoes solo with parcelas
            ocup = pd.merge(ocup[["OSA_ID", "PAR_ID", "PUN_CUL", "geometry"]], parc[["PAR_ID", "ENT_ID"]],
                            "left", "PAR_ID")

            ## Stretch ocupacoes solo with multiple crops
            ocup["PUN_CUL"] = ocup["PUN_CUL"].str.split(";")
            first_entries = ocup.copy()
            first_entries["PUN_CUL"] = ocup["PUN_CUL"].str[0]

            remaining_entries = ocup.dropna(subset=["PUN_CUL"]).copy()
            remaining_entries["PUN_CUL"] = remaining_entries["PUN_CUL"].apply(lambda x: x[1:] if len(x) > 1 else None)
            remaining_entries = remaining_entries.explode("PUN_CUL", ignore_index=True)
            remaining_entries = remaining_entries.dropna(subset=["PUN_CUL"])
            remaining_entries.drop(columns=["geometry"], inplace=True)

            ## Add crop descriptions to parcels
            first_entries = pd.merge(first_entries, cult[["PUN_CUL_COD", "PUN_CUL_DESC"]], "left",
                                     left_on="PUN_CUL", right_on="PUN_CUL_COD")
            remaining_entries = pd.merge(remaining_entries, cult[["PUN_CUL_COD", "PUN_CUL_DESC"]], "left",
                                         left_on="PUN_CUL", right_on="PUN_CUL_COD")

            first_entries.drop(columns=["PUN_CUL_COD"], inplace=True)
            remaining_entries.drop(columns=["PUN_CUL_COD"], inplace=True)

            remaining_entries['crop_number'] = remaining_entries.groupby('OSA_ID').cumcount() + 2
            remaining_entries['crop_number'] = 'C' + remaining_entries['crop_number'].astype(str)

            first_entries = first_entries[['OSA_ID', 'PAR_ID', 'ENT_ID', 'PUN_CUL', 'PUN_CUL_DESC', 'geometry']]
            # first_entries['geometry'] = first_entries['geometry'].apply(make_valid)
            first_entries['geometry'] = first_entries['geometry'].buffer(0)
            first_entries['geometry'] = first_entries.normalize()
            remaining_entries = remaining_entries[['OSA_ID', 'PAR_ID', 'ENT_ID', 'PUN_CUL',  'PUN_CUL_DESC',
                                                   'crop_number']]

            parcels_lst.append(first_entries)
            add_crops_lst.append(remaining_entries)

        parcels = pd.concat(parcels_lst)
        parcels.drop_duplicates(inplace=True)
        parcels.index = range(1, len(parcels)+1)

        # parcels = parcels.loc[parcels["PUN_CUL"].notna()].copy()
        add_crops = pd.concat(add_crops_lst)

        helper_functions.create_folder(rf"data\vector\IACS\PT\PT\{year}")

        parcels.to_parquet(fr"data\vector\IACS\PT\PT\{year}\ocupacoes_solo_{year}.geoparquet")
        add_crops.to_csv(fr"data\vector\IACS\PT\PT\{year}\ocupacoes_solo_{year}.csv")

    else:
        sub_lst = glob.glob(fr"data\vector\IACS\PT\PT_temp\culturas\{year}\*.gpkg")
        sub_lst = [pth.split("_sub")[1].split("_")[0] for pth in sub_lst]
        print(len(sub_lst))

        fields_lst = []
        for sub in sub_lst:
            print(year, sub)

            ## Open files
            parc_pth = fr"data\vector\IACS\PT\PT_temp\parcelas\{year}\parcelas_sub{sub}_{year}.gpkg"
            ocup_pth = fr"data\vector\IACS\PT\PT_temp\ocupacoes_solo\{year}\ocupacoes_solo_sub{sub}_{year}.gpkg"
            cult_pth = fr"data\vector\IACS\PT\PT_temp\culturas\{year}\culturas_sub{sub}_{year}.gpkg"
            cult = gpd.read_file(cult_pth)

            if not os.path.exists(parc_pth):
                continue
            elif not os.path.exists(ocup_pth):
                continue

            ocup = gpd.read_file(ocup_pth)
            parc = gpd.read_file(parc_pth)

            ## Combine ocupacoes solo with parcelas
            ocup = pd.merge(ocup[["OSA_ID", "PAR_ID", "geometry"]], parc[["PAR_ID", "ENT_ID"]], "left",
                            "PAR_ID")

            fields = pd.merge(cult[["OSA_ID", "PUN_CUL_COD", "PUN_CUL_DESC", "geometry"]],
                              ocup[["OSA_ID", "PAR_ID", "ENT_ID"]], "left", "OSA_ID")

            # fields['geometry'] = fields['geometry'].apply(make_valid)
            fields['geometry'] = fields['geometry'].buffer(0)
            fields['geometry'] = fields.normalize()

            fields_lst.append(fields)

        fields_out = pd.concat(fields_lst)
        fields_out.drop_duplicates(inplace=True)
        fields_out.index = range(1, len(fields_out) + 1)


        helper_functions.create_folder(rf"data\vector\IACS\PT\PT\{year}")

        fields_out.to_parquet(fr"data\vector\IACS\PT\PT\{year}\culturas_{year}.geoparquet")

def pt_combine_crop_codes_with_crop_names(crop_codes_pth, crop_names_pth):

    ## Load input
    codes_df = pd.read_csv(crop_codes_pth)
    names_df = pd.read_excel(crop_names_pth)

    ## Save backup of original codes df as it takes quite long to create

    ## Merge in both ways to identify possible misses
    merge_df1 = pd.merge(names_df, codes_df, "left", left_on="CTnum", right_on="crop_code")
    print(f"There are {len(merge_df1.loc[merge_df1['crop_code'].isna()])} entries in the {crop_names_pth} that don't occur in the IACS data.")
    miss_df = merge_df1.loc[merge_df1['crop_code'].isna()].copy()
    merge_df2 = pd.merge(codes_df[["crop_code"]], names_df, "left", left_on="crop_code", right_on="CTnum")
    print(f"There are {len(merge_df2.loc[merge_df2['CTnum'].isna()])} entries in the IACS data that don't occur in the {crop_names_pth}.")

    ## Clean the second merge-df and concatenate with misses in the other merge-df
    merge_df2 = merge_df2[["crop_code", "CT_português", "CT"]].copy()
    miss_df = miss_df[["CTnum", "CT_português", "CT"]].copy()
    miss_df.rename(columns={"CTnum": "crop_code", "CT_português": "crop_name",  "CT": "translated_name"}, inplace=True)
    merge_df2.rename(columns={"CT_português": "crop_name", "CT": "translated_name"}, inplace=True)
    out_df = pd.concat([merge_df2, miss_df])
    out_df.dropna(subset="crop_code", inplace=True)

    ## Write out
    out_df.to_csv(crop_codes_pth, index=False)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## For subregions 2011-2019
    run_dict = {
        ## The portugese files sometimes come with fieldblocks and sometimes with fields - not all years need to be separated
        # "PT/ALE": {
        #     "region_id": "PT_ALE",
        #     "file_encoding": "utf-8",
        #     "skip_years": [2018, 2019, 2020, 2021, 2022],
        #     "ignore_files_descr":  "_sep_.gpkg"},
        # "PT/ALG": {
        #     "region_id": "PT_ALG",
        #     "file_encoding": "utf-8"},
        # "PT/AML": {
        #     "region_id": "PT_AML",
        #     "file_encoding": "utf-8",
        #     "ignore_files_descr": "_sep_.gpkg"},
        # "PT/CET": {
        #     "region_id": "PT_CET",
        #     "file_encoding": "utf-8"},
        # "PT/CEN": {
        #     "region_id": "PT_CEN",
        #     "file_encoding": "utf-8"},
        # "PT/CES": {
        #     "region_id": "PT_CES",
        #     "file_encoding": "utf-8"},
        # "PT/NOR": {
        #     "region_id": "PT_NOR",
        #     "file_encoding": "utf-8"},
        # "PT/NON": {
        #     "region_id": "PT_NON",
        #     "file_encoding": "utf-8"},
        # "PT/NOS": {
        #     "region_id": "PT_NOS",
        #     "file_encoding": "utf-8"},
    }

    for country_code in run_dict:
        print(country_code)
        region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")
        encoding = run_dict[country_code]["file_encoding"]
        if "skip_years" in run_dict[country_code]:
            skip_years = run_dict[country_code]["skip_years"]
        else:
            skip_years = []

        ## Get files that should be skipped
        if "ignore_files_descr" in run_dict[country_code]:
            ignore_files_descr = run_dict[country_code]["ignore_files_descr"]
        else:
            ignore_files_descr = None

        col_translate_pth = rf"data\tables\column_name_translations\{region_id}_column_name_translation.xlsx"

        in_dir = fr"data\vector\IACS\{country_code}"

        ## Get list of IACS files
        iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

        if ignore_files_descr:
            iacs_files = [file for file in iacs_files if ignore_files_descr not in file]

        ## Open column translations
        tr_df = pd.read_excel(col_translate_pth)

        ## Loop over files to derive crop names from all files
        for path in iacs_files:
            year = helper_functions.get_year_from_path(path)
            print(f"Processing: {year} - {path}")

            if int(year) in skip_years:
                print(f"Skipping year {year}")
                continue

            col_year = f"{region_id}_{year}"
            col_dict = dict(zip(tr_df["column_name"], tr_df[col_year]))

            ## create a list of crop names and crop codes, so if multiple crop columns are provided (and separated by "|")
            ## then they can be looped over
            if type(col_dict["crop_name"]) != float:
                col_dict["crop_name"] = col_dict["crop_name"].split("|")

            if type(col_dict["crop_code"]) != float:
                col_dict["crop_code"] = col_dict["crop_code"].split("|")

            target_column = None
            if type(col_dict["crop_name"]) != float and len(col_dict["crop_name"]) > 1:
                target_column = "crop_name"
            elif type(col_dict["crop_code"]) != float and len(col_dict["crop_code"]) > 1:
                target_column = "crop_code"
            else:
                print("Only one target column ('crop_name' or 'crop_code') provided. Nothing to be done.")
                continue

            ## If multiple crop name columns are provided, then separate the additional crop columns into a csv
            if target_column:
                print("Multiple crop columns provided. Separating them into csv.")
                ## Create separated file
                ## Open file and layer
                gdf = gpd.read_file(path)
                cols = list(gdf.columns)
                cols_csv = []

                ## Create field id if not existent
                ## if not update also the column translation table
                if type(col_dict["field_id"]) != float:
                    cols_csv.append(col_dict["field_id"])
                else:
                    gdf["field_id"] = range(1, len(gdf) + 1)
                    cols_csv.append("field_id")
                    tr_df.loc[tr_df['column_name'] == "field_id", col_year] = "field_id"
                    tr_df.to_excel(col_translate_pth)

                ## loop over the crop columns
                for col in col_dict[target_column][1:]:
                    cols.remove(col)
                    cols_csv.append(col)

                ## separate the file based on the columns
                df = gdf[cols_csv]
                gdf = gdf[cols]

                ## save geometries + first crop columns to gpkg
                root, ext = os.path.splitext(path)
                gdf.to_file(root + "_sep.gpkg")

                ## melt additional crop columns from wide to long df and save as csv
                df = pd.melt(df, id_vars=cols_csv[0], value_vars=cols_csv[1:], var_name="crop_number",
                             value_name=target_column)
                df.dropna(inplace=True)
                df.to_csv(root + "_sep.csv", index=False)

                del gdf, df

                ## move original file to "temp" folder
                folder = fr"data\vector\IACS\{country_code}_temp\{year}"
                helper_functions.create_folder(folder)
                if ext == ".shp":
                    files = glob.glob(f"{root}.*")
                    for f in files:
                        new_path = fr"{folder}\{os.path.basename(f)}"
                        os.rename(f, new_path)
                else:
                    new_path = fr"{folder}\{os.path.basename(path)}"
                    os.rename(path, new_path)
            else:
                print("Only one target column ('crop_name' or 'crop_code') provided. Nothing to be done.")

    ## For entire PO 2020-2024
    for year in range(2020, 2025):
        combine_information_from_wfs(year)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
