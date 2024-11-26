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
import matplotlib.pyplot as plt
import seaborn as sns
import glob
from osgeo import ogr
import geopandas as gpd

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def plot_number_original_crops_and_harmonized_crops(crop_class_folder, out_pth):

    in_pths = glob.glob(fr"{crop_class_folder}\*final.xlsx")

    out_dict = {}
    for in_pth in in_pths:
        region_id = os.path.basename(in_pth).split(r"\\")[-1].split("_crop_")[0]
        df = pd.read_excel(in_pth)

        num_original_entries = len(df.drop_duplicates(subset=["crop_code", "crop_name"]))
        num_ec_entries = len(df.drop_duplicates(subset="EC_hcat_c"))
        out_dict[region_id] = {}
        out_dict[region_id]["num_original_entries"] = num_original_entries
        out_dict[region_id]["num_ec_entries"] = num_ec_entries

    out_df = pd.DataFrame.from_dict(out_dict, orient="index").reset_index()
    out_df.rename(columns={"index": "country"}, inplace=True)
    out_df.to_excel(r"data\tables\statistics\crop_classifications\number_of_crop_entries.xlsx")


    print("Plotting")
    plt.figure(figsize=(8, 6))
    sns.barplot(x='country', y='num_original_entries', data=out_df.sort_values(by="num_original_entries"), color="blue")
    plt.grid(which='both', axis="both")
    plt.title("Number of original crop entries")
    plt.xticks(rotation=90)
    plt.xlabel("Country")
    plt.ylabel("Number entries")
    plt.tight_layout()
    plt.savefig(r"figures\statistics_on_crop_classifications\num_original_crop_entries.png")  # Save plot to disk
    plt.close()

    # Plot for `num2`
    plt.figure(figsize=(8, 6))
    sns.barplot(x='country', y='num_ec_entries', data=out_df.sort_values(by="num_ec_entries"))
    plt.title("Number of original crop entries")
    plt.xlabel("Country")
    plt.ylabel("Number entries")
    plt.savefig(r"figures\statistics_on_crop_classifications\num_ec_crop_entries.png")  # Save plot to disk
    plt.close()  # Close the figure

def count_number_features_per_year(region_id, out_pth):

    in_pths = glob.glob(rf"data\vector\IACS_EU_Land\{region_id}\*.geoparquet")

    out_dict = {}
    for in_pth in in_pths:
        print(in_pth)
        root, ext = os.path.splitext(in_pth)
        csv_pth = root + ".csv"

        gdf = gpd.read_parquet(in_pth)
        num_feat_csv = 0
        total_area_csv = 0
        if os.path.exists(csv_pth):
            df = pd.read_csv(csv_pth)
            num_feat_csv += len(df)
            df["field_size"] = df["field_size"].astype(float)
            total_area_csv += df["field_size"].sum()

        year = helper_functions.get_year_from_path(in_pth)

        num_feat = len(gdf) + num_feat_csv
        if "farm_id" in gdf.columns:
            num_farm = len(gdf["farm_id"].unique())
        else:
            num_farm = 0
        gdf["field_size"] = gdf["field_size"].astype(float)
        total_area = gdf["field_size"].sum() + total_area_csv

        out_dict[year] = {}
        out_dict[year]["Number features"] = num_feat
        out_dict[year]["Number farms"] = num_farm
        out_dict[year]["Total area [ha]"] = total_area

    out_df = pd.DataFrame.from_dict(out_dict, orient="index").reset_index()
    out_df.rename(columns={"index": "year"}, inplace=True)
    helper_functions.create_folder(os.path.dirname(out_pth))
    out_df.to_excel(out_pth, index=False)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    regions_ids = ["AT", "BE_FLA", "BE_WAL", "BG", "CY", "CZ", "DE_BRB", "DE_LSA", "DE_NRW", "DE_SAA", "DE_SAT", "DE_THU",
                   "DK", "EE", "EL", "ES", "FI", "FR", "FR_SUBREGIONS", "IE", "IT_EMR", "IT_MAR", "IT_TOS", "HR", "HU",
                   "LT", "LU", "LV", "MT", "NL", "PL", "PT", "PT_ALE", "RO", "SE", "SI", "SK"]

    # plot_number_original_crops_and_harmonized_crops(
    #     crop_class_folder=r"data\tables\crop_classifications",
    #     out_pth="")

    # regions_ids = ["AT", "BE/FLA", "BE/WAL", "CY", "CZ", "DE/BRB", "DE/LSA", "DE/NRW", "DE/SAA", "DE/SAT",
    #                "DK", "EE", "EL", "FI", "FR/FR", "HR", "HU", "IE", "IT/EMR", "IT/MAR", "IT/TOS",
    #                "LV", "LT", "NL", "PL", "PT/PT", "RO", "SE", "SI", "SK"]
    # regions_ids = pd.read_csv(r"data\vector\IACS\FR\region_code.txt")
    # regions_ids = list(regions_ids["code"])
    # regions_ids = [f"FR/{n}" for n in regions_ids]
    # regions_ids = ["SI"]
    # regions_ids += ["PT/PT", "PT/ALE", "PT/ALG", "PT/AML", "PT/CEN", "PT/CES", "PT/CET", "PT/NON", "PT/NOR", "PT/NOS"]
    # # regions_ids += ["HU"]
    # # regions_ids = ["EL"]
    # regions_ids = ["SE"]
    regions_ids = pd.read_csv(r"data\vector\IACS\ES\region_code.txt")
    regions_ids = list(regions_ids["code"])
    regions_ids = [f"ES/{n}" for n in regions_ids]

    for region_id in regions_ids:
        print(region_id)
        out_pth = fr"data\tables\statistics\num_parcels\{region_id.replace(r'/', '_')}_count_num_parcels_per_year.xlsx"
        count_number_features_per_year(region_id, out_pth)

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
