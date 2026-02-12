# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# This script is optional and can be used to produce some summary plots and tables of the harmonized data.
# It compares the number of input crop classes to the harmonized crop classes in a plot.
# It summarizes the number of fields/parcels, the number of farms, the total area, the area of grassland and
# not_known_and_other in a table

# If you want to run this script for specific countries, you should change the list of region_ids below.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import warnings
from os.path import dirname, abspath
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import geopandas as gpd

from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def plot_number_original_crops_and_harmonized_crops(crop_class_folder, out_pth):

    in_pths = glob.glob(os.path.join(crop_class_folder, "*final.csv"))
    in_pths = [pth for pth in in_pths if "EuroCrops2" not in pth]
    in_pths = [pth for pth in in_pths if "backup" not in pth]

    out_dict = {}
    for in_pth in in_pths:
        region_id = os.path.basename(in_pth).split("_crop_")[0]
        df = pd.read_csv(in_pth)

        num_original_entries = len(df.drop_duplicates(subset=["crop_code", "crop_name"]))
        num_ec_entries = len(df.drop_duplicates(subset="EC_hcat_c"))
        out_dict[region_id] = {}
        out_dict[region_id]["num_original_entries"] = num_original_entries
        out_dict[region_id]["num_ec_entries"] = num_ec_entries

    out_df = pd.DataFrame.from_dict(out_dict, orient="index").reset_index()
    out_df.rename(columns={"index": "country"}, inplace=True)

    out_df["country_name"] = out_df["country"].map(
        {
            "AT": "Austria",
            "BE_FLA": "Belgium (Flanders)",
            "BE_WAL": "Belgium (Wallonia)",
            "BG": "Bulgaria",
            "CY": "Cyprus",
            "CZ": "Czech Republic",
            "DE_BWB": "Germany (Baden-Württemberg)",
            "DE_BAV": "Germany (Bavaria)",
            "DE_BRB": "Germany (Brandenburg)",
            "DE_LSA": "Germany (Lower Saxony)",
            "DE_MWP": "Germany (Mecklenburg-Western Pomerania)",
            "DE_NRW": "Germany (North Rhine-Westphalia)",
            "DE_RLP": "Germany (Rhineland-Palatinate)",
            "DE_SAA": "Germany (Saarland)",
            "DE_SAT": "Germany (Saxony-Anhalt)",
            "DE_THU": "Germany (Thuringia)",
            "DK": "Denmark",
            "EE": "Estonia",
            "EL": "Greece",
            "ES": "Spain",
            "ES_CAT": "Spain (Catalonia)",
            "FI": "Finland",
            "FR_FR": "France",
            "FR_SUBREGIONS": "France (Subregions)",
            "IE": "Ireland",
            "IT_EMR": "Italy (Emilia-Romagna)",
            "IT_MAR": "Italy (Marche)",
            "IT_TOS": "Italy (Tuscany)",
            "HR": "Croatia",
            "HU": "Hungary",
            "LT": "Lithuania",
            "LU": "Luxembourg",
            "LV": "Latvia",
            "MT": "Malta",
            "NL": "Netherlands",
            "PL": "Poland",
            "PT_PT": "Portugal",
            "PT_SUBREGIONS": "Portugal (Subregions)",
            "RO": "Romania",
            "SE": "Sweden",
            "SI": "Slovenia",
            "SK": "Slovakia"
        }
    )
    
     # chech if the output dir structure exist and create it if des not
    os.makedirs(os.path.join("data", "tables", "statistics"), exist_ok=True)
    os.makedirs(os.path.join("data", "tables", "statistics", "crop_classifications"), exist_ok=True)

    out_df.to_csv(os.path.join("data", "tables", "statistics", "crop_classifications", "number_of_crop_entries.csv"))
    
    print("Plotting")
    plt.figure(figsize=(10, 8))
    sns.barplot(x='num_original_entries', y='country_name', data=out_df.sort_values(by="num_original_entries"),
                color="blue")
    plt.grid(which='both', axis="both")
    plt.title("Number of original crop entries")
    plt.xlabel("Number entries")
    plt.ylabel("Country")
    plt.tight_layout()
    # chech if the output dir structure exist and create it if des not
    os.makedirs(os.path.join("figures", "statistics_on_crop_classifications"), exist_ok=True)
    plt.savefig(os.path.join("figures", "statistics_on_crop_classifications", "num_original_crop_entries.png"))  # Save plot to disk
    plt.close()

    # Plot for `num2`
    plt.figure(figsize=(10, 8))
    sns.barplot(x='num_ec_entries', y='country_name', data=out_df.sort_values(by="num_ec_entries"))
    plt.title("Number of original crop entries")
    plt.xlabel("Number entries")
    plt.ylabel("Country")
    plt.savefig(os.path.join("figures", "statistics_on_crop_classifications", "num_ec_crop_entries.png"))  # Save plot to disk
    plt.close()  # Close the figure

def count_number_features_per_year(region_id, out_pth):

    ## Get all geodata from current region/country
    in_pths = glob.glob(os.path.join("data", "vector", "IACS_EU_Land", region_id, "*.geoparquet"))

    ## Loop over files
    out_dict = {}
    for in_pth in in_pths:
        print(in_pth)
        root, ext = os.path.splitext(in_pth)
        csv_pth = root + ".csv"

        gdf = gpd.read_parquet(in_pth)

        ## Count features and if companion csv file exists, also count these fields
        num_feat_csv = 0
        total_area_csv = 0
        if os.path.exists(csv_pth):
            df = pd.read_csv(csv_pth)
            num_feat_csv += len(df)
            if "field_size" in df.columns:
                df["field_size"] = df["field_size"].astype(float)
                total_area_csv += df["field_size"].sum()
            elif "crop_area" in df.columns:
                df["crop_area"] = df["crop_area"].astype(float)
                total_area_csv += df["crop_area"].sum()
            else:
                warnings.WarningMessage("No field_size or crop_area column in accompanying table.")

        year = helper_functions.get_year_from_path(in_pth)

        num_feat = len(gdf) + num_feat_csv
        if "farm_id" in gdf.columns:
            num_farm = len(gdf["farm_id"].unique())
        else:
            num_farm = 0
        gdf["field_size"] = gdf["field_size"].astype(float)
        total_area = gdf["field_size"].sum() + total_area_csv

        ## Only becaue of ES
        unknown_area = gdf.loc[gdf["EC_hcat_n"] == "not_known_and_other", "field_size"].sum()
        pastures = gdf.loc[gdf["EC_hcat_n"] == "pasture_meadow_grassland_grass", "field_size"].sum()

        out_dict[year] = {}
        out_dict[year]["Number features"] = num_feat
        out_dict[year]["Number farms"] = num_farm
        out_dict[year]["Total area [ha]"] = total_area
        out_dict[year]["not_known_and_other [ha]"] = unknown_area
        out_dict[year]["pasture_meadow_grassland_grass [ha]"] = pastures

    out_df = pd.DataFrame.from_dict(out_dict, orient="index").reset_index()
    out_df.rename(columns={"index": "year"}, inplace=True)
    helper_functions.create_folder(os.path.dirname(out_pth))
    out_df.to_excel(out_pth, index=False)

def count_geoparquet_files(directory):
    geoparquet_count = 0
    # Walk through the directory and its subdirectories
    for root, dirs, files in os.walk(directory):
        # Count files with the .geoparquet extension
        geoparquet_count += sum(1 for file in files if file.endswith('.geoparquet'))
    return geoparquet_count


def sum_dataframes(dataframes):
    # Start with a DataFrame of zeros with the same shape and columns
    result = dataframes[0].copy()
    result.iloc[:, :] = 0  # Initialize all values to 0

    # Add each DataFrame to the result
    for df in dataframes:
        # print(df)
        result += df

    return result

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # check if the output dir structure exist and create it if des not
    os.makedirs(os.path.join("data", "tables", "statistics"), exist_ok=True)
    os.makedirs(os.path.join("data", "tables", "statistics", "crop_classifications"), exist_ok=True)

    plot_number_original_crops_and_harmonized_crops(
        crop_class_folder = os.path.join("data", "tables", "crop_classifications"),
        out_pth="")

    ## List all regions for which the fields should be counted
    regions_ids = ["AT", "BG", "BE/FLA", "BE/WAL", "CY", "CZ", "DE/BWB", "DE/BAV", "DE/BRB", "DE/LSA", "DE/MWP",
                   "DE/NRW", "DE/RLP",  "DE/SAA", "DE/SAT", "DE_THU",
                   "DK", "EE", "EL", "FI", "FR/FR", "HR", "HU", "IE", "IT/EMR", "IT/MAR", "IT/TOS",
                   "LV", "LT", "NL", "PL", "PT/PT", "PT/ALE", "PT/ALG", "PT/AML", "PT/CEN", "PT/CES", "PT/CET",
                   "PT/NON", "PT/NOR", "PT/NOS", "RO", "SE", "SI", "SK"]
    regions_ids_fr = pd.read_csv(os.path.join("data", "vector", "IACS", "FR", "region_code.txt"))
    regions_ids_fr = list(regions_ids_fr["code"])
    regions_ids_fr = [f"FR/{n}" for n in regions_ids_fr]
    regions_ids_es = pd.read_csv(os.path.join("data", "vector", "IACS", "ES", "region_code.txt"))
    regions_ids_es = list(regions_ids_es["code"])
    regions_ids_es = [f"ES/{n}" for n in regions_ids_es]

    regions_ids += regions_ids_fr
    regions_ids += regions_ids_es

    # for region_id in regions_ids:
    #     print(region_id)
    #     out_pth = os.path.join("data", "tables", "statistics", "num_parcels",
    #                            f"{region_id.replace('/', '_')}_count_num_parcels_per_year.xlsx")
    #     count_number_features_per_year(region_id, out_pth)

    ## Aggregate regions of France
    df_lst = [pd.read_excel(os.path.join("data", "tables", "statistics", "num_parcels",
                                          f"{region_id.replace('/','_')}_count_num_parcels_per_year.xlsx"))
               for region_id in regions_ids_fr]
    summed_df = sum_dataframes(df_lst)
    summed_df.to_excel(os.path.join("data", "tables", "statistics", "num_parcels",
                                    "FR_FR_count_num_parcels_per_year_05-14.xlsx"))

    ## Aggregate provinces of Spain
    if "CAT" in regions_ids_es:
        regions_ids_es.remove("CAT")

    df_lst = [pd.read_excel(os.path.join("data", "tables", "statistics", "num_parcels",
                                         f"{region_id.replace('/','_')}_count_num_parcels_per_year.xlsx")) for
              region_id in regions_ids_es]
    df_new_lst = []
    missing_years = [2022, 2023, 2024]
    for i, df in enumerate(df_lst):
        # Check which years are missing
        missing = [year for year in missing_years if year not in list(df["year"])]

        if missing:
            for year in missing:
                print(year, regions_ids_es[i])
                df2 = pd.DataFrame.from_dict({year: {
                    "Number features": 0,
                    "Number farms": 0,
                    "Total area [ha]": 0,
                    "not_known_and_other [ha]": 0,
                    "pasture_meadow_grassland_grass [ha]": 0}}, orient="index")
                df2.reset_index(inplace=True, names="year")
                df_new = pd.concat([df2, df], ignore_index=True)
                df_new.sort_values(by="year", inplace=True)
                df_new.index = range(len(df_new))
                df_new_lst.append(df_new)
        else:
            df_new_lst.append(df)

    summed_df = sum_dataframes(df_new_lst)
    summed_df.to_excel(os.path.join("data", "tables", "statistics", "num_parcels",
                                    "ES_ES_count_num_parcels_per_year_22-24.xlsx"))

    directory_path = os.path.join("data", "vector", "IACS_EU_Land")
    count = count_geoparquet_files(directory_path)
    print(f"Number of .geoparquet files: {count}")

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
