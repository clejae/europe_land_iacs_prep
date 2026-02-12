# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# This script is only needed internally to separate the data the Europe-LAND project can share from the data we are
# not allowed to share.

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
# os.environ['GDAL_DATA'] = os.path.join(f'{os.sep}'.join(sys.executable.split(os.sep)[:-1]), 'Library', 'share', 'gdal')
import time
import geopandas as gpd
import glob
import shutil
import pandas as pd
import os
import zipfile
import re
from collections import defaultdict
from pathlib import Path

from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def get_year_from_filename(filename):
    """
    Robustly extract a 4-digit year (1900-2099) from a filename using Regex.
    Matches: 'file_2020.txt', '2020-data.csv', 'report 2020 final.pdf'
    """
    # Look for 19xx or 20xx surrounded by word boundaries (spaces, underscores, dots, etc.)
    match = re.search(r'\b(19|20)\d{2}\b', filename)
    if match:
        return int(match.group(0))
    return None


def create_zip_files(root_folder, output_folder, max_size=5 * 1024 * 1024 * 1024):
    """
    Create zip files containing consecutive years from each folder.
    Saves archives to a separate 'output_folder' to avoid polluting source.
    """
    folder_path = Path(root_folder)
    output_path = Path(output_folder)

    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Processing source folder: {folder_path.name}")

    # Group files by year
    files_by_year = defaultdict(list)

    for file_path in folder_path.iterdir():
        if file_path.is_file():
            year = get_year_from_filename(file_path.name)
            if year:
                files_by_year[year].append(file_path)

    if not files_by_year:
        print(f"  No files with years found in {folder_path.name}. Skipping.")
        return

    # Sort years
    sorted_years = sorted(files_by_year.keys())

    # Group consecutive years (e.g., 2018, 2019, 2020 -> one group)
    consecutive_groups = []
    current_group = []
    last_year = None

    for year in sorted_years:
        if last_year is None or year == last_year + 1:
            current_group.append(year)
        else:
            if current_group:
                consecutive_groups.append(current_group)
            current_group = [year]
        last_year = year

    if current_group:
        consecutive_groups.append(current_group)

    # Create zip files for each group
    for year_group in consecutive_groups:
        # Construct a base name for the zip file
        base_zip_name = f"{folder_path.name}_years_{year_group[0]}-{year_group[-1]}" if len(
            year_group) > 1 else f"{folder_path.name}_year_{year_group[0]}"

        # 1. Collect all files for this group
        all_group_files = []
        for year in year_group:
            all_group_files.extend(files_by_year[year])

        # 2. Logic to split into parts if > max_size
        current_zip_files = []
        current_zip_size = 0
        part_counter = 1

        for file_path in all_group_files:
            file_size = file_path.stat().st_size

            # Check if adding this file exceeds the limit
            if current_zip_files and (current_zip_size + file_size > max_size):
                # Save the current batch
                zip_name = f"{base_zip_name}_part{part_counter}.zip"
                # SAVE TO OUTPUT FOLDER
                save_path = output_path / zip_name

                print(f"  Creating {zip_name}...")
                with zipfile.ZipFile(save_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for f in current_zip_files:
                        # arcane arcname=f.name stores just the filename, not the full path
                        zf.write(f, arcname=f.name)

                # Reset for next batch
                current_zip_files = []
                current_zip_size = 0
                part_counter += 1

            current_zip_files.append(file_path)
            current_zip_size += file_size

        # 3. Save any remaining files
        if current_zip_files:
            # If we split previous parts, add a part number to this one too
            suffix = f"_part{part_counter}" if part_counter > 1 else ""
            zip_name = f"{base_zip_name}{suffix}.zip"
            # SAVE TO OUTPUT FOLDER
            save_path = output_path / zip_name

            print(f"  Creating {zip_name}...")
            with zipfile.ZipFile(save_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for f in current_zip_files:
                    zf.write(f, arcname=f.name)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    run_dict = {
        "AT": {"switch": "off"},
        "BG": {"switch": "on"},
        "BE/FLA": {"switch": "on"},
        "BE/WAL": {"switch": "on"},
        "CZ": {"switch": "on"},
        "DE/BRB": {"switch": "on"},
        "DE/BWB": {"switch": "on"},
        "DE/LSA": {"switch": "on"},
        "DE/NRW": {"switch": "on"},
        "DK": {"switch": "on"},
        "EE": {"switch": "on"},
        "FI": {"switch": "on"},
        "FR/FR": {"switch": "on"},
        "IE": {"switch": "on"},
        "IT/TOS": {"switch": "on"},
        "HR": {"switch": "on"},
        "LT": {"switch": "on"},
        "LV": {"switch": "on"},
        "NL": {"switch": "on"},
        "PT/PT": {"switch": "on"},
        "PT/ALE": {"switch": "on"},
        "PT/ALG": {"switch": "on"},
        "PT/AML": {"switch": "on"},
        "PT/CET": {"switch": "on"},
        "PT/CEN": {"switch": "on"},
        "PT/CES": {"switch": "on"},
        "PT/NOR": {"switch": "on"},
        "PT/NON": {"switch": "on"},
        "PT/NOS": {"switch": "on"},
        "SE": {"switch": "on"},
        "SI": {"switch": "on"},
        "SK": {"switch": "on"}
    }

    ## For france create a dictionary in a loop, because of the many subregions
    FR_districts = pd.read_csv(os.path.join(r"data", "vector", "IACS", "FR", "region_code.txt"))
    FR_districts = list(FR_districts["code"])
    for district in FR_districts:
        run_dict[f"FR/{district}"] = {"switch": "on"}

    ## For spain create a dictionary in a loop, because of the many subregions
    ## This code snippet needs to be corrected. I did the copying manually!
    ES_districts = pd.read_csv(os.path.join(r"data", "vector", "IACS", "ES", "region_code.txt"))
    ES_districts = list(ES_districts["code"])
    for district in ES_districts:
        run_dict[f"ES/{district}"] = {"switch": "off"}

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        switch = run_dict[country_code].get("switch", "off").lower()
        if switch != "on":
            continue
        ## Derive input variables for processing
        region_id = country_code.replace(r"/", "_")


        root_in = os.path.join("data", "vector", "IACS_public_database", country_code)
        root_out = os.path.join("data", "vector", "IACS_public_database")

        if os.path.exists(root_in):
            create_zip_files(root_in, root_out)
            print("\nDone! Check your destination folder.")
        else:
            print("Source folder does not exist!")

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

    # POSTGRESQL Database


if __name__ == '__main__':
    main()

