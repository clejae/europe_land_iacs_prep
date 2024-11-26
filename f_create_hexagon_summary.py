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
import glob
import warnings
import math

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def summarize_iacs_per_hexagon_characteristics(iacs_pth, iacs_file_encoding, hexa_dict, region_id, year, out_pth,
                                               crop_class_col="EC_hcat_c", field_id_col="field_id",  hexa_id_col="id",
                                               farm_id_col="farm_id", recl_df_dict=None):
    print("Start summarize_iacs_per_hexagon_characteristics")
    print("Read IACS.")

    ## Read input
    iacs = gpd.read_file(iacs_pth, encoding=iacs_file_encoding)
    iacs[crop_class_col] = iacs[crop_class_col].astype(str)
    iacs[crop_class_col] = [_.split(".")[0] for _ in iacs[crop_class_col]]
    iacs["field_size"] = iacs["field_size"].astype(float)
    ## For safety: create field id
    # iacs["field_id"] = range(len(iacs))

    ## Check if there are additional information in csvs
    csv_pth = iacs_pth.split(".")[0] + ".csv"
    csv_check = False
    if os.path.exists(csv_pth):
        print(".csv file found.")
        iacs_csv = pd.read_csv(csv_pth)
        iacs_csv[crop_class_col] = iacs_csv[crop_class_col].astype(str)
        csv_check = True

    ## Do reclassification if wanted
    if recl_df_dict:
        print("Reclassifying")
        recl_df_pth = recl_df_dict["recl_df_pth"]
        sheet_name = recl_df_dict["sheet_name"]
        new_class_col = recl_df_dict["new_class_col"]
        merge_col = recl_df_dict["merge_col"]
        recl_df = pd.read_excel(
            io=recl_df_pth,
            sheet_name=sheet_name,
            dtype={merge_col: str}
        )
        iacs = pd.merge(iacs, recl_df[[merge_col, new_class_col]], how="left", left_on=crop_class_col,
                        right_on=merge_col)

        if csv_check == True:
            iacs_csv = pd.merge(iacs_csv, recl_df[[merge_col, new_class_col]], how="left", left_on=crop_class_col,
                        right_on=merge_col)
            crop_class_col = new_class_col

    ## Create centroids
    iacs["centroid"] = iacs.geometry.centroid
    centroids = iacs[[field_id_col, "centroid"]].copy()
    centroids.rename(columns={"centroid": "geometry"}, inplace=True)
    centroids = gpd.GeoDataFrame(centroids, crs=iacs.crs)

    ## Get number of farms from dataset if there is a farm id
    num_farms = 0
    if farm_id_col in iacs.columns:
        unique_farm_ids = iacs[farm_id_col].unique()
        num_farms = len(unique_farm_ids)

    ## Join csv to vector data AFTER deriving the centroids (becaus it doesn't work for csv data)
    ## and BEFORE calculating the farm sizes
    if csv_check == True:
        iacs = pd.concat([iacs, iacs_csv])

    ## Calculate farm sizes and add to IACS data if there are farms indicate in the ds
    if num_farms > 1:
        if csv_check == True:
            farm_id_dict = pd.Series(iacs.farm_id.values, index=iacs.field_id).to_dict()
            iacs_csv["farm_id"] = iacs_csv["field_id"].map(farm_id_dict)
        farm_sizes = iacs.groupby(farm_id_col).agg(
            farm_size=pd.NamedAgg(column="field_size", aggfunc="sum")
        ).reset_index()
        iacs = pd.merge(iacs, farm_sizes, on=farm_id_col, how="left")

    iacs_columns = iacs.columns
    centroids_columns = centroids.columns

    ## Loop over hexagon grids
    for grid_size in hexa_dict:
        print(f"Grid size: {grid_size}")

        ## Reset the content of the data
        iacs = iacs[iacs_columns]
        centroids = centroids[centroids_columns]

        ## Reset variables
        field_id_col = "field_id"

        ## Fetch hexa gdf, define output path
        hexa = hexa_dict[grid_size]
        out_pth_ = out_pth.split(".")[0] + f"_{grid_size}." + out_pth.split(".")[1]

        if hexa.crs != iacs.crs:
            warnings.warn("The input data do not have the same CRS.")
            return

        ## Add the hex id to the centroids and then to the IACS data
        print("Join field centroids with hexagons. Add hexa IDs to IACS data.")
        ## Delete some columns causing issues when merging
        # if hexa_id_col in centroids.columns:
        #     iacs = centroids.drop(hexa_id_col, axis=1)
        for name in ['index_left', 'index_right']:
            if name in hexa.columns:
                hexa = hexa.drop(name, axis=1)
            if name in centroids.columns:
                centroids = centroids.drop(name, axis=1)

        centroids = gpd.sjoin(centroids, hexa, how="left")
        iacs = pd.merge(iacs, centroids[[field_id_col, hexa_id_col]], how="left", on=field_id_col)

        ## Calculate statistics
        print("Calculate statistics.")
        if num_farms > 1:
            iacs_red = iacs.drop_duplicates(subset=[farm_id_col, hexa_id_col])

            farm_stats = iacs_red.groupby(hexa_id_col).agg(
                avgfarm_s=pd.NamedAgg(column="farm_size", aggfunc="mean"),
                num_farms=pd.NamedAgg(column=farm_id_col, aggfunc="nunique")
            ).reset_index()

        ## Reset the field id in case there was a csv file, because each entry in the csv represents a field
        ## but the id is not unique anymore
        if csv_check == True:
            iacs["field_id_new"] = range(len(iacs))
            field_id_col = "field_id_new"

        field_stats = iacs.groupby(hexa_id_col).agg(
            avgfield_s=pd.NamedAgg(column="field_size", aggfunc="mean"),
            num_fields=pd.NamedAgg(column=field_id_col, aggfunc="nunique")
        ).reset_index()
        if crop_stats:
            crop_stats = iacs.groupby([hexa_id_col, crop_class_col])[["field_size"]].sum().reset_index()
            unique_crops = list(crop_stats[crop_class_col].unique())
            crop_stats.rename(columns={'field_size': 'crop_area'}, inplace=True)
            crop_stats = crop_stats.pivot(index=hexa_id_col, columns=crop_class_col, values='crop_area').reset_index()

            out_df = pd.merge(field_stats, crop_stats, how="left", on=hexa_id_col)
            column_order = [hexa_id_col, "region_id", "year", "avgfarm_s", "num_farms", "avgfield_s",
                            "num_fields"] + unique_crops
        else:
            out_df = field_stats
            column_order = [hexa_id_col, "region_id", "year", "avgfarm_s", "num_farms", "avgfield_s",
                            "num_fields"]

        if num_farms > 1:
            out_df = pd.merge(out_df, farm_stats, how="left", on=hexa_id_col)
        else:
            out_df["avgfarm_s"] = np.nan
            out_df["num_farms"] = np.nan
        out_df["region_id"] = region_id
        out_df["year"] = year
        out_df = out_df[column_order]
        out_df.fillna(np.nan, inplace=True)

        print("Write out.")
        out_df.to_csv(out_pth_, index=False)
        out_df.to_excel(out_pth_[:-3] + "xlsx", index=False)

def combine_regions(year,
                    folder,
                    hexa_pth,
                    hexa_diameter,
                    table_out_pth,
                    vector_out_pth
                    ):

    ## Read input
    pth_lst = glob.glob(f"{folder}\IACS_hexa_summary*{year}*{hexa_diameter}km.csv")
    df_lst = [pd.read_csv(pth) for pth in pth_lst]
    df = pd.concat(df_lst)
    hexa = gpd.read_file(hexa_pth)

    ## Define some variabels
    # hexa.rename(columns={"id": "fid"}, inplace=True)
    hexa_id_col = "id"
    columns = df.columns

    ## Aggregate values
    def concatenate_unique_items(x):
        return "|".join(set(list(x)))

    df["weigted_farm_areas"] = df["avgfarm_s"] * df["num_farms"]
    df["weigted_field_areas"] = df["avgfield_s"] * df["num_fields"]

    ## Aggregate hexagons that occur multiple times
    df_agg = df.groupby(hexa_id_col).agg(
        region_id=pd.NamedAgg(column="region_id", aggfunc=concatenate_unique_items),
        year=pd.NamedAgg(column="year", aggfunc="first"),
        weigted_farm_areas=pd.NamedAgg(column="weigted_farm_areas", aggfunc=sum),
        num_farms=pd.NamedAgg(column="num_farms", aggfunc=sum),
        weigted_field_areas=pd.NamedAgg(column="weigted_field_areas", aggfunc=sum),
        num_fields=pd.NamedAgg(column="num_fields", aggfunc=sum),

        ## ToDo: Adapt the crop classes to the class names of your classification
        broad_arable_class=pd.NamedAgg(column="Broad arable class", aggfunc=sum),
        broad_permanent_class=pd.NamedAgg(column="Broad permanent crop class", aggfunc=sum),
        cereals=pd.NamedAgg(column="Cereals", aggfunc=sum),
        fruits_and_nuts=pd.NamedAgg(column="Fruit and nuts", aggfunc=sum),
        greenhouses=pd.NamedAgg(column="Greenhouses", aggfunc=sum),
        leguminous_crops=pd.NamedAgg(column="Leguminous crops", aggfunc=sum),
        oilseed_crops=pd.NamedAgg(column="Oilseed crops and oleaginous fruits", aggfunc=sum),
        other_crops=pd.NamedAgg(column="Other crops", aggfunc=sum),
        grassland=pd.NamedAgg(column="Pastures", aggfunc=sum),
        root_tuber_vegetables=pd.NamedAgg(column="Root/tuber crops with high starch or inulin content", aggfunc=sum),
        spice_aromatic_crops=pd.NamedAgg(column="Stimulant, spice and aromatic crops", aggfunc=sum),
        sugar_crops=pd.NamedAgg(column="Sugar crops", aggfunc=sum),
        temporary_grasses_other_fodder_crops=pd.NamedAgg(column="Temporary grasses and other fodder crops",
                                                         aggfunc=sum),
        trees=pd.NamedAgg(column="Trees", aggfunc=sum),
        fallow_unmaintained=pd.NamedAgg(column="Unmaintained and fallow", aggfunc=sum),
        vegetables_melons=pd.NamedAgg(column="Vegetables and melons", aggfunc=sum)
    ).reset_index()

    df_agg.loc[df_agg["num_farms"] > 0, "avgfarm_s"] = df_agg.loc[df_agg["num_farms"] > 0, "weigted_farm_areas"] / df_agg.loc[df_agg["num_farms"] > 0, "num_farms"]
    df_agg["avgfield_s"] = df_agg["weigted_field_areas"] / df_agg["num_fields"]

    ## Get rid of unnecessary columns
    df_agg.drop(columns=["weigted_farm_areas", "weigted_field_areas"])

    df_agg["avgfarm_s"] = df_agg["avgfarm_s"].astype(float)
    df_agg["avgfield_s"] = df_agg["avgfield_s"].astype(float)

    df_agg.replace(0, np.nan, inplace=True)

    ## Combine hexa grid with data
    hexa = pd.merge(hexa, df_agg, "left", on=hexa_id_col)
    hexa.rename(columns={"fid": "id"}, inplace=True)

    ## Write out
    df_agg.to_csv(fr"{table_out_pth}", index=False)
    hexa.to_file(rf"{vector_out_pth}", driver="GPKG")
    if len(df_agg) > 1048576:
        num_dfs = math.ceil(len(df_agg) / 1048576)
        df_lst = [df_agg[x*1048576:(x+1)*1048576] for x in range(num_dfs)]
        for i, _ in enumerate(df_lst):
            _.to_excel(fr"{table_out_pth[:-4]+ f'_{i+1}.xlsx'}", index=False)

    df_agg.to_excel(fr"{table_out_pth[:-3]+'xlsx'}", index=False)
    # df_agg.to_excel(rf"{table_out_folder}\IACS_hexa_summary-ALL-2022.xlsx")



def split_grid_by_region(grid_pth, region_pth, region_col, out_folder, out_descr):
    print("Read input")
    grid = gpd.read_file(grid_pth)
    regions = gpd.read_file(region_pth)

    if regions.crs != grid.crs:
        regions = regions.to_crs(grid.crs)

    for reg in regions[region_col].unique():
        print(f"{reg}")
        curr_reg = regions.loc[regions[region_col] == reg].copy()

        selected_grid = gpd.sjoin(grid, curr_reg, how="left")
        selected_grid = selected_grid.loc[selected_grid[region_col] == reg].copy()

        helper_functions.create_folder(out_folder)
        out_pth = fr"{out_folder}\{reg}_{out_descr}.gpkg"
        selected_grid = selected_grid[["id", "geometry"]].copy()
        selected_grid.to_file(out_pth, driver="GPKG")

    print("Done!")

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    ## Create country specific hexagon grids

    # split_grid_by_region(
    #     grid_pth=r"data\vector\grids\Europe-Land-15km-hexa-grid_selection.shp",
    #     region_pth=r"data\vector\administrative\Europe-LAND-countries_repaired.gpkg",
    #     region_col=r"CNTR_ID",
    #     out_folder=r"data\vector\grids\15km_country_specific",
    #     out_descr=r"15km_hexa_grid")

    # split_grid_by_region(
    #     grid_pth=r"data\vector\grids\Europe-Land-15km-hexa-grid_selection.shp",
    #     region_pth=r"data\vector\administrative\GER_bundeslaender_3035.gpkg",
    #     region_col=r"RGN_ID",
    #     out_folder=r"data\vector\grids\15km_country_specific",
    #     out_descr=r"15km_hexa_grid")

    # split_grid_by_region(
    #     grid_pth=r"data\vector\grids\Europe-Land-15km-hexa-grid_selection.shp",
    #     region_pth=r"data\vector\administrative\BELGIUM_-_Provinces.shp",
    #     region_col=r"RGN_ID",
    #     out_folder=r"data\vector\grids\15km_country_specific",
    #     out_descr=r"15km_hexa_grid")

    # split_grid_by_region(
    #     grid_pth=r"data\vector\grids\1km_country_specific\BE_1km_hexa_grid.gpkg",
    #     region_pth=r"data\vector\administrative\BELGIUM_-_Provinces.shp",
    #     region_col=r"RGN_ID",
    #     out_folder=r"data\vector\grids\1km_country_specific",
    #     out_descr=r"1km_hexa_grid")

    # split_grid_by_region(
    #     grid_pth=r"data\vector\grids\15km_country_specific\ES_ES_15km_hexa_grid.gpkg",
    #     region_pth=r"data\vector\administrative\georef-spain-provincia-millesime.shp",
    #     region_col=r"RGN_ID",
    #     out_folder=r"data\vector\grids\15km_country_specific",
    #     out_descr=r"15km_hexa_grid")

    # split_grid_by_region(
    #     grid_pth=r"data\vector\grids\1km_country_specific\ES_ES_1km_hexa_grid.gpkg",
    #     region_pth=r"data\vector\administrative\georef-spain-provincia-millesime.shp",
    #     region_col=r"RGN_ID",
    #     out_folder=r"data\vector\grids\1km_country_specific",
    #     out_descr=r"1km_hexa_grid")

    # split_grid_by_region(
    #     grid_pth=r"data\vector\grids\Europe-Land-15km-hexa-grid_selection.shp",
    #     region_pth=r"data\vector\administrative\FR_regions_3035.gpkg",
    #     region_col=r"RGN_ID",
    #     out_folder=r"data\vector\grids\15km_country_specific",
    #     out_descr=r"15km_hexa_grid")

    # split_grid_by_region(
    #     grid_pth=r"data\vector\grids\1km_country_specific\FR_FR_1km_hexa_grid.gpkg",
    #     region_pth=r"data\vector\administrative\FR_regions_3035.gpkg",
    #     region_col=r"RGN_ID",
    #     out_folder=r"data\vector\grids\1km_country_specific",
    #     out_descr=r"1km_hexa_grid")

    ##
    # grid_lst = glob.glob(r"data\vector\grids\1km_country_specific\**.gpkg")
    # for pth in grid_lst:
    #     grid = gpd.read_file(pth)
    #     grid = grid[["id", "geometry"]].copy()
    #     grid.to_file(pth)
    #
    # grid_lst = glob.glob(r"data\vector\grids\15km_country_specific\**.gpkg")
    # for pth in grid_lst:
    #     grid = gpd.read_file(pth)
    #     grid = grid[["id", "geometry"]].copy()
    #     grid.to_file(pth)

    run_dict = {
        # "AT": {
        #     "region_id": "AT",
        #     "years": [2022],
        #     "iacs_file_encoding": "utf-8"
        # },
        # "BE/FLA": {
        #     "region_id": "BE_FLA",
        #     "years": [2022],
        #     "iacs_file_encoding": "utf-8"
        # },
        # "BE/WAL": {
        #     "region_id": "BE_WAL",
        #     "years": [2022],
        #     "iacs_file_encoding": "ISO-8859-1"
        # },
        # "CY/APPL": {
        #     "region_id": "CY_APPL",
        #     "years": [2022],
        #     "iacs_file_encoding": "utf-8"
        # },
        # "CZ": {
        #     "region_id": "CZ",
        #     "years": [2022],
        #     "iacs_file_encoding": "ISO-8859-1"
        # },
        # "DE/BRB": {
        #     "region_id": "DE_BRB",
        #     "years": [2022],
        #     "iacs_file_encoding": "ISO-8859-1"
        # },
        # "DE/SAA": {
        #     "region_id": "DE_SAA",
        #     "years": [2022],
        #     "iacs_file_encoding": "utf-8"
        # },
        # "DE/SAT": {
        #     "region_id": "DE_SAT",
        #     "years": [2022],
        #     "iacs_file_encoding": "ISO-8859-1"
        # },
        # "DK": {
        #     "region_id": "DK",
        #     "years": [2022],
        #     "iacs_file_encoding": "ISO-8859-1"
        # },
        # "FI": {
        #     "region_id": "FI",
        #     "years": [2022],
        #     "iacs_file_encoding": "ISO-8859-1"
        # },
        # "FR/FR": {
        #     "region_id": "FR_FR",
        #     "years": [2021],
        #     "iacs_file_encoding": "utf-8"
        # },
        # # "HR": {
        # #     "region_id": "HR",
        # #     "years": [2022],
        # #     "iacs_file_encoding": "utf-8"},
        # "LV": {
        #     "region_id": "LV",
        #     "years": [2022],
        #     "iacs_file_encoding": "utf-8"},
        # "NL": {
        #     "region_id": "NL",
        #     "years": [2022],
        #     "iacs_file_encoding": "utf-8"},
        # "PT/PT": {
        #     "region_id": "PT_PT",
        #     "years": [2022],
        #     "iacs_file_encoding": "utf-8"},
        # "RO": {
        #     "region_id": "RO",
        #     "years": [2023],
        #     "iacs_file_encoding": "utf-8"},
        # "SE": {
        #     "region_id": "SE",
        #     "years": [2022],
        #     "iacs_file_encoding": "ISO-8859-1"},
        # "SI": {
        #     "region_id": "SI",
        #     "years": [2022],
        #     "iacs_file_encoding": "utf-8"},
        # "SK": {
        #     "region_id": "SK",
        #     "years": [2022],
        #     "iacs_file_encoding": "utf-8"},
        # "FR/COR": {
        #         "region_id": "FR_COR",
        #         "years": [2014],
        #         "iacs_file_encoding": "utf-8"
        #     },
    }

    # ES_districts = pd.read_csv(r"data\vector\IACS_EU_Land\ES\region_code_run_dict.txt")
    # ES_districts = list(ES_districts["code"])
    # for district in ES_districts:
    #     run_dict[f"ES/{district}"] = {
    #         "region_id": f"ES_{district}",
    #         "years": [2022],
    #         "iacs_file_encoding": "utf-8"
    #     }

    for country_code in run_dict:
        region_id = run_dict[country_code]["region_id"]
        file_encoding = run_dict[country_code]["iacs_file_encoding"]
        years = run_dict[country_code]["years"]
        print(region_id)

        ## Loop over files to unify columns and classify crops
        for i, year in enumerate(years):
            print(year)
            iacs_pth = fr"data\vector\IACS_EU_Land\{country_code}\IACS-{region_id}-{year}.gpkg"
            hexa1_pth = fr"data\vector\grids\1km_country_specific\{region_id}_1km_hexa_grid.gpkg"
            hexa15_pth = fr"data\vector\grids\15km_country_specific\{region_id}_15km_hexa_grid.gpkg"
            out_pth = fr"data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-{region_id}-{year}.csv"

            ## Put grids into a dictionary (and pass it to the processing function)
            ## to avoid opening the IACS data multiple times
            print("Read grids.")
            hexa_dict = {"15km": gpd.read_file(hexa15_pth),
                         "1km": gpd.read_file(hexa1_pth)}

            ## Create output folder
            folder = os.path.dirname(out_pth)
            helper_functions.create_folder(folder)

            recl_df_dict = {
                "recl_df_pth": r"data\tables\hcat_levels_v2\reclassification\HCAT3_reclass_by_ka-cj.xlsx",
                "sheet_name": "Sheet1",
                "merge_col": "HCAT3_code",
                "new_class_col": "ICC_l1_name"
            }

            summarize_iacs_per_hexagon_characteristics(
                iacs_pth=iacs_pth,
                iacs_file_encoding=file_encoding,
                hexa_dict=hexa_dict,
                out_pth=out_pth,
                region_id=region_id,
                year=year,
                crop_class_col="EC_hcat_c",
                field_id_col="field_id",
                farm_id_col="farm_id",
                recl_df_dict=recl_df_dict
            )

    ## Rename files for FR and RO to match year 2022
    if os.path.exists(r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-FR_FR-2021_1km.csv"):
        os.rename(r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-FR_FR-2021_1km.csv",
                  r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-FR_FR-2022_1km.csv")
    if os.path.exists(r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-FR_FR-2021_15km.csv"):
        os.rename(r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-FR_FR-2021_15km.csv",
                  r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-FR_FR-2022_15km.csv")
    if os.path.exists(r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-RO-2023_1km.csv"):
        os.rename(r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-RO-2023_1km.csv",
                  r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-RO-2022_1km.csv")
    if os.path.exists(r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-RO-2023_15km.csv"):
        os.rename(r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-RO-2023_15km.csv",
                  r"\data\tables\IACS_EU_Land_hexa\IACS_hexa_summary-RO-2022_15km.csv")

    # combine_regions(year=2022,
    #                 folder=r"data\tables\IACS_EU_Land_hexa",
    #                 hexa_pth=r"data\vector\grids\Europe-Land-15km-hexa-grid_selection.shp",
    #                 hexa_diameter=15,
    #                 table_out_pth=r"data\tables\IACS_EU_Land_hexa\hexa_15km_combined\Europe-Land-15km-hexa-grid_selection_w_values.csv",
    #                 vector_out_pth=r"data\vector\grids\Europe-Land-15km-hexa-grid_selection_w_values.gpkg")

    combine_regions(year=2022,
                    folder=r"data\tables\IACS_EU_Land_hexa",
                    hexa_pth=r"data\vector\grids\Europe-Land-1km-hexa-grid_selection.shp",
                    hexa_diameter=1,
                    table_out_pth=r"data\tables\IACS_EU_Land_hexa\hexa_1km_combined\Europe-Land-1km-hexa-grid_selection_w_values.csv",
                    vector_out_pth=r"data\vector\grids\Europe-Land-1km-hexa-grid_selection_w_values.shp")


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')