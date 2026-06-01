# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import sys
# os.environ['GDAL_DATA'] = os.path.join(f'{os.sep}'.join(sys.executable.split(os.sep)[:-1]), 'Library', 'share', 'gdal')
os.environ["GDAL_DRIVER_PATH"] = os.path.join(f'{os.sep}'.join(sys.executable.split(os.sep)[:-1]), 'Library', 'lib', 'gdalplugins')
import time
import pandas as pd
import geopandas as gpd
import glob

import plotly.graph_objects as go
from plotly.subplots import make_subplots
# import warnings
# import numpy as np
# import seaborn as sns
# import matplotlib.pyplot as plt
import plotly.express as px
# from dash import Dash, dcc, html, Input, Output

from my_utils import helper_functions

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

COL_NAMES_FOLDER = os.path.join("data", "tables", "column_names")
CROP_CLASSIFICATION_FOLDER = os.path.join("data", "tables", "crop_classifications")
_TRANSLATION_CACHE = {}
_CROP_CLASSIFICATION_CACHE = {}
# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def preprocess_eurostat_data():


    lst = glob.glob(os.path.join("data", "tables", "statistics", "official statistics", "crop_shares", "*.tsv"))
    df_lst = [pd.read_csv(pth, sep="\t", header=0, encoding='utf-8') for pth in lst]

    df = pd.concat(df_lst)
    df[['freq', 'crops', 'strucpro', 'geo']] = df.iloc[:, 0].str.split(',', expand=True)
    df = df.drop(df.columns[0], axis=1)

    numeric_cols = [str(i) + ' ' for i in range(2013, 2027)]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    strucpro_dict = {"MA": "main_area", "AR": "area", "HU_EU": "eu_standard_humidity",
                     "PR_HU_EU": "harvested_production_in_eu_standard_humidity"}

    crops_dict = {
        'UAA': 'utilised_agricultural_area',
        'ARA': 'arable_land',
        'PECR': 'permanent_crops',

        'J0000': 'permanent_grassland',
        'K0000': 'kitchen_gardens',
        'C0000': 'cereals_grain_production',
        'P0000': 'dry_pulses_and_protein_crops',
        'V0000_S0000': 'fresh_vegetables_strawberries_melons',
        'H0000': 'permanent_crops_human_consumption',

        'C1100': 'wheat_spelt',
        'C1200': 'rye_winter_cereal_mixtures',
        'C1300': 'barley',
        'C1400': 'oats_spring_cereal_mixtures',
        'C1500': 'grain_maize_ccm',
        'I1110': 'oilseeds',
        'I1120': 'sunflower',
        'I1130': 'soy',
        'G3000': 'green_maize',
        'G1000': 'temporary_grasses_grazing',
        'G2000': 'leguminous_plants_harvested_green',
        'G9100': 'other_cereals_harvested_green',
        'R1000': 'potatoes',
        'R2000': 'sugar_beet',
        'V3100': 'tomatoes',
        'V3200': 'cucumbers',
        'V4100': 'carrots',
        'V4210': 'onions',
        'F1110': 'apples',
        'F1120': 'pears',
        'F1240': 'cherries',
        'T1000': 'oranges',
        'W1000': 'grapes',
        'W1100': 'grapes_for_wine',
        'W1200': 'grapes_for_wine_pdo',
        'W1300': 'grapes_for_raisins',
        'O1100': 'olives_table_use',
        'O1910': 'olives_oil'
    }

    df['strucpro'] = df['strucpro'].map(strucpro_dict)
    df['crops'] = df['crops'].map(crops_dict)

    df.to_csv(os.path.join("data", "tables", "statistics", "official statistics", "crop_shares",
                           "crop_shares_2013-2026.csv"), sep=",", index=False)

    print("done")


def preprocess_eurostat_data_nuts2():

    pth = os.path.join("data", "tables", "statistics", "official statistics", "crop_shares", "ef_lus_allcrops__custom_21570126_tabular.tsv")
    df = pd.read_csv(pth, sep="\t", header=0, encoding='utf-8')

    df[['freq', 'statinfo', 'so_euro', 'uaarea', 'crops', 'unit', 'geo']] = df.iloc[:, 0].str.split(',', expand=True)
    df = df.drop(df.columns[0], axis=1)

    numeric_cols = [str(i) + ' ' for i in [2005, 2007, 2010, 2013, 2016, 2020, 2023]]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    strucpro_dict = {"HA": "area"}

    crops_dict = {
        'UAA': 'utilised_agricultural_area',
        'ARA': 'arable_land',
        'PECR': 'permanent_crops',
        'ARA09S': 'other_arable_land_crops_nec_under_glass_or_high_accessible_cover',
        'ARA99': 'other_arable_land_crops_nec',
        'J0000': 'permanent_grassland',
        'K0000': 'kitchen_gardens',
        'C0000': 'cereals_grain_production',
        'P0000': 'dry_pulses_and_protein_crops',
        'V0000_S0000': 'fresh_vegetables_strawberries_melons',
        'H0000': 'permanent_crops_human_consumption',
        'E0000': 'seeds_and_seedlings',
        'F0000': 'fruits_berries_nuts_excl_citrus_grapes_strawberries',
        'G0000': 'plants_harvested_green_from_arable_land',
        'G0000X1000': 'plants_harvested_green_from_arable_land_excl_temporary_grasses_grazings',
        'I0000': 'industrial_crops',
        'L0000': 'nurseries',
        'N0000': 'flowers_and_ornamental_plants_excl_nurseries',
        'N0000S': 'flowers_and_ornamental_plants_excl_nurseries_under_glass_or_high_accessible_cover',
        'N0000T': 'flowers_and_ornamental_plants_excl_nurseries_outdoor',
        'Q0000': 'fallow_land',
        'R0000': 'root_crops',
        'T0000': 'citrus_fruits',
        'UAA09S': 'utilised_agricultural_area_under_glass_or_high_accessible_cover',
        'UAAS': 'utilised_agricultural_area_under_glass_or_high_accessible_cover',
        'UAAT': 'utilised_agricultural_area_outdoor',
        'C1100': 'wheat_spelt',
        'C1200': 'rye_winter_cereal_mixtures',
        'C1300': 'barley',
        'C1400': 'oats_spring_cereal_mixtures',
        'C1500': 'grain_maize_ccm',
        'C1600_1700_1900': 'other_cereals_incl_triticale_sorghum',
        'C2000': 'rice',
        'F1000_2000': 'fruits_temperate_subtropical_tropical_climate_zones',
        'F1100': 'pome_fruits',
        'F1200': 'stone_fruits',
        'F2000': 'fruits_subtropical_tropical_climate_zones',
        'F3000': 'berries_excl_strawberries',
        'F4000': 'nuts',
        'I2000': 'fibre_crops',
        'I3000': 'tobacco',
        'I4000': 'hops',
        'I5000': 'aromatic_medicinal_culinary_plants',
        'I6000_9000': 'other_industrial_crops_incl_energy_crops_nec',
        'I9000': 'other_industrial_crops_nec',
        'J1000': 'permanent_pastures_and_meadows',
        'J2000': 'permanent_rough_grazings',
        'J3000TE': 'permanent_agricultural_grassland_not_in_use_and_not_eligible_for_subsidies_outdoor',
        'O1000': 'olives',
        'P1000': 'field_peas_beans_sweet_lupins',
        'P9000': 'other_dry_pulses_and_protein_crops_nec',
        'PECR9_H9000': 'other_permanent_crops_incl_other_permanent_crops_human_consumption',
        'PECRS': 'permanent_crops_under_glass_or_high_accessible_cover',
        'PECRT': 'permanent_crops_outdoor',
        'R1000': 'potatoes',
        'R2000': 'sugar_beet',
        'R9000': 'other_root_crops_nec',
        'SRCAA': 'short_rotation_coppice_areas',
        'U1000': 'mushrooms',
        'I1110': 'rape_turnip_rape_seeds',
        'I1120': 'sunflower_seed',
        'I1130': 'soy',
        'I1140': 'linseed_oilflax',
        'I1150_2300': 'cotton_seed_and_fibre',
        'I1190': 'other_oilseed_crops_nec',
        'I2100': 'fibre_flax',
        'I2200': 'hemp',
        'I2900': 'other_fibre_crops_nec',
        'G1000': 'temporary_grasses_grazing',
        'G2000': 'leguminous_plants_harvested_green',
        'G2000_9000': 'leguminous_and_other_plants_harvested_green',
        'G3000': 'green_maize',
        'G9000': 'other_plants_harvested_green_from_arable_land',
        'G9100': 'other_cereals_harvested_green',
        'G9900': 'other_plants_harvested_green_from_arable_land_nec',
        'V3100': 'tomatoes',
        'V3200': 'cucumbers',
        'V4100': 'carrots',
        'V4210': 'onions',
        'F1110': 'apples',
        'F1120': 'pears',
        'F1240': 'cherries',
        'T1000': 'oranges',
        'W1000': 'grapes',
        'W1100': 'grapes_for_wine',
        'W1110': 'grapes_for_wines_pdo',
        'W1110_1120': 'grapes_for_quality_wines_pdo_pgi',
        'W1120': 'grapes_for_wines_pgi',
        'W1190': 'grapes_for_other_wines_nec_without_pdo_pgi',
        'W1200': 'grapes_for_table_use',
        'W1300': 'grapes_for_raisins',
        'O1100': 'olives_table_use',
        'O1910': 'olives_oil',
        'V0000_S0000S': 'fresh_vegetables_strawberries_melons_under_glass_or_high_accessible_cover',
        'V0000_S0000T': 'fresh_vegetables_strawberries_melons_outdoor',
        'V0000_S0000TK': 'fresh_vegetables_strawberries_melons_outdoor_market_gardening',
        'V0000_S0000TO': 'fresh_vegetables_strawberries_melons_outdoor_open_field',
        'X0000': 'christmas_trees',
    }

    df.rename(columns={'unit': 'strucpro'}, inplace=True)
    df['strucpro'] = df['strucpro'].map(strucpro_dict)
    df['crops'] = df['crops'].map(crops_dict)

    df.to_csv(os.path.join("data", "tables", "statistics", "official statistics", "crop_shares",
                           "crop_shares_nuts2_2013-2026.csv"), sep=",", index=False)

    target_nuts2 = ["ITI1", "ITH5", "ITI3", "ITE1", "ITD5", "ITE3"]
    nuts_dict = {"ITI1": "IT_TOS",
                 "ITE1": "IT_TOS",
                 "ITH5": "IT_EMR",
                 "ITD5": "IT_EMR",
                 "ITI3": "IT_MAR",
                 "ITE3": "IT_MAR"}

    df_it = df.loc[df["geo"].isin(target_nuts2)].copy()
    df_it["geo"] = df_it["geo"].map(nuts_dict)

    df_it.to_csv(os.path.join("data", "tables", "statistics", "official statistics", "crop_shares",
                           "crop_shares_it_nuts2_2013-2026.csv"), sep=",", index=False)

    print("done")

def prepare_europeland_data():
    ## To turn off/on a specific country, use the "switch" key
    run_dict = {

        "AT": {"switch": "off", "region_id": "AT", "file_encoding": "utf-8"},
        "BE/FLA": {"switch": "off", "region_id": "BE_FLA", "file_encoding": "utf-8",
                   "file_year_encoding": {"2020": "ISO-8859-1"}},
        "BE/WAL": {"switch": "off", "region_id": "BE_WAL", "file_encoding": "utf-8"},
        "BG": {"switch": "off", "region_id": "BG", "file_encoding": "utf-8", "language":"en"},
        "CY": {"switch": "off", "region_id": "CY", "file_encoding": "utf-8", "language":"en"},
        "CZ": {"switch": "on", "region_id": "CZ", "file_encoding": "utf-8"},
        "DE/BAV": {"switch": "off", "region_id": "DE_BAV", "file_encoding": "utf-8"},
        "DE/BRB": {"switch": "off", "region_id": "DE_BRB", "file_encoding": "utf-8"},
        "DE/BWB": {"switch": "off", "region_id": "DE_BWB", "file_encoding": "utf-8"},
        "DE/LSA": {"switch": "off", "region_id": "DE_LSA", "file_encoding": "utf-8"},
        "DE/MWP": {"switch": "off", "region_id": "DE_MWP", "file_encoding": "utf-8"},
        "DE/NRW": {"switch": "off", "region_id": "DE_NRW", "file_encoding": "utf-8"},
        "DE/RLP": {"switch": "off", "region_id": "DE_RLP", "file_encoding": "utf-8"},
        "DE/SAA": {"switch": "off", "region_id": "DE_SAA", "file_encoding": "utf-8"},
        "DE/SAT": {"switch": "off", "region_id": "DE_SAT", "file_encoding": "utf-8"},
        "DE/THU": {"switch": "off", "region_id": "DE_THU", "file_encoding": "utf-8"},
        "DK": {"switch": "off", "region_id": "DK", "file_encoding": "ISO-8859-1"},
        "EE": {"switch": "off", "region_id": "EE", "file_encoding": "utf-8"},
        "EL": {"switch": "off", "region_id": "EL", "file_encoding": "utf-8"},
        "FI": {"switch": "off", "region_id": "FI", "file_encoding": "utf-8"},
        "FR/FR": {"switch": "off", "region_id": "FR_FR", "file_encoding": "utf-8"},
        "IE": {"switch": "off", "region_id": "IE", "file_encoding": "utf-8"},
        "HR": {"switch": "off", "region_id": "HR", "file_encoding": "utf-8"},
        "HU": {"switch": "off", "region_id": "HU", "file_encoding": "utf-8"},
        "IT/EMR": {"switch": "off", "region_id": "IT_EMR", "file_encoding": "utf-8"},
        "IT/MAR": {"switch": "off", "region_id": "IT_MAR", "file_encoding": "utf-8"},
        "IT/TOS": {"switch": "off", "region_id": "IT_TOS", "file_encoding": "utf-8"},
        "LV": {"switch": "off", "region_id": "LV", "file_encoding": "utf-8"},
        "NL": {"switch": "off", "region_id": "NL", "file_encoding": "utf-8"},
        "PL": {"switch": "off", "region_id": "PL", "file_encoding": "utf-8"},
        "PT/PT": {"switch": "off", "region_id": "PT_PT", "file_encoding": "utf-8"},
        "PT/ALE": {"switch": "off", "region_id": "PT_ALE", "file_encoding": "utf-8"},
        "PT/ALG": {"switch": "off", "region_id": "PT_ALG", "file_encoding": "utf-8"},
        "PT/AML": {"switch": "off", "region_id": "PT_AML", "file_encoding": "utf-8"},
        "PT/CET": {"switch": "off", "region_id": "PT_CET", "file_encoding": "utf-8"},
        "PT/CEN": {"switch": "off", "region_id": "PT_CEN", "file_encoding": "utf-8"},
        "PT/CES": {"switch": "off", "region_id": "PT_CES", "file_encoding": "utf-8"},
        "PT/NOR": {"switch": "off", "region_id": "PT_NOR", "file_encoding": "utf-8"},
        "PT/NON": {"switch": "off", "region_id": "PT_NON", "file_encoding": "utf-8"},
        "PT/NOS": {"switch": "off", "region_id": "PT_NOS", "file_encoding": "utf-8"},
        "RO": {"switch": "off", "region_id": "RO", "file_encoding": "utf-8"},
        "SE": {"switch": "off", "region_id": "SE", "file_encoding": "utf-8"},
        "SI": {"switch": "off", "region_id": "SI", "file_encoding": "utf-8",
               "file_year_encoding": {"2025": "cp1250"}},
        "SK": {"switch": "off", "region_id": "SK", "file_encoding": "utf-8"},
    }

    ## For france create a dictionary in a loop, because of the many subregions
    FR_districts = pd.read_csv(os.path.join("data", "vector", "IACS", "FR", "region_code.txt"))
    FR_districts = list(FR_districts["code"])
    for district in FR_districts:
        run_dict[f"FR/{district}"] = {
            "switch": "off",
            "region_id": f"FR_{district}",
            "file_encoding": "utf-8"
        }

    ## For spain create a dictionary in a loop, because of the many subregions
    ES_districts = pd.read_csv(os.path.join("data", "vector", "IACS", "ES", "region_code.txt"))
    ES_districts = list(ES_districts["code"])
    for district in ES_districts:
        run_dict[f"ES/{district}"] = {
            "switch": "off",
            "region_id": f"ES_{district}",
            "file_encoding": "utf-8"
        }

    ## Loop over country codes in dict for processing
    for country_code in run_dict:
        switch = run_dict[country_code].get("switch", "off").lower()
        if switch != "on":
            continue

        ## Derive input variables for function
        region_id = run_dict[country_code]["region_id"]  # country_code.replace(r"/", "_")

        ## Get years that should be skipped
        if "skip_years" in run_dict[country_code]:
            skip_years = run_dict[country_code]["skip_years"]
        else:
            skip_years = []

        ## Get files that should be skipped
        if "language" in run_dict[country_code]:
            language = run_dict[country_code]["language"]
        else:
            language = "original"

        ## Get list of all available files
        in_dir = os.path.join("data", "vector", "IACS_EU_Land", country_code)
        iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)


        ## Loop over files
        for i, iacs_pth in enumerate(iacs_files):
            print(f"{i + 1}/{len(iacs_files)} - Processing - {iacs_pth}")
            year = helper_functions.get_year_from_path(iacs_pth)
            if int(year) in skip_years:
                print(f"Skipping year {year}")
                continue

            if language == "original":
                original_crop_col = "crop_name"
                ext = ""
            elif language == "en":
                original_crop_col = "EC_trans_n"
                ext = "_en"

            out_pth = os.path.join("data", "tables", "statistics", "crop_shares_harmonized_v1_3",
                                   f"{region_id}_{year}_crop_shares{ext}.csv")

            root, ext = os.path.splitext(iacs_pth)
            csv_pth = root + ".csv"

            area_col = "field_size"

            if os.path.exists(csv_pth):
                df = pd.read_csv(csv_pth)
                if "crop_area" in df.columns:
                    area_col = "crop_area"
                df = df[[original_crop_col, "EC_hcat_n", area_col]].copy()

            gdf = gpd.read_parquet(iacs_pth)
            gdf[area_col] = round(gdf.geometry.area / 10000, 2)
            gdf = gdf[[original_crop_col, "EC_hcat_n", area_col]].copy()

            if area_col == "crop_area":
                gdf = pd.concat([gdf, df])

            aggr = gdf.groupby([original_crop_col, "EC_hcat_n"]).sum(area_col).reset_index()
            aggr.sort_values(by=[area_col], ascending=False, inplace=True)
            aggr.sort_values(by=["EC_hcat_n"], ascending=True, inplace=True)

            aggr.rename(columns={area_col: "field_size"}, inplace=True)
            aggr.to_csv(out_pth, index=False)


def compare_shares(tr_pth, crop_shares_official_pth, value_vars, country_code, col, crop, language='original', fs_scale_factor=1, csv_sep=','):

    ## Open dataframes
    df_map = pd.read_excel(tr_pth)
    df_ocs = pd.read_csv(crop_shares_official_pth, sep=csv_sep)

    ## Turn official statistics into long dataframe
    df_ocs = df_ocs.melt(id_vars=["crops", "geo", "strucpro"],
                         value_vars=value_vars,
                         var_name="year",
                         value_name="field_size")
    df_ocs["year"] = pd.to_numeric(df_ocs["year"], errors='coerce')
    df_ocs["field_size"] *= fs_scale_factor

    if language == "original":
        original_crop_col = "crop_name"
        ext = ""
    elif language == "en":
        original_crop_col = "EC_trans_n"
        ext = "_en"

    ## Get all areas calculated from GSA data for current regions
    folder = os.path.join("data", "tables", "statistics", "crop_shares_harmonized_v1_3")
    pth_lst = glob.glob(os.path.join(folder, f"{country_code}*{ext}.csv"))

    lst = []
    for pth in pth_lst:
        df = pd.read_csv(pth)
        year = helper_functions.get_year_from_path(pth)
        df["year"] = year
        df["geo"] = country_code
        lst.append(df)

    df_el = pd.concat(lst)

    ## Map Agripod crop names to Europe-LAND areas
    df_el = pd.merge(df_el, df_map[['EC_hcat_n', 'crop_group', 'agripod_l3', 'agripod_l2', 'agripod_l1', 'agripod_l0']],
                     how="left", on="EC_hcat_n")
    df_el["year"] = pd.to_numeric(df_el["year"], errors='coerce')

    ## Only go on if the target crop is in the dataframe
    if crop not in df_el[col].tolist():
        return

    ## Aggregate areas by year and by crop+year
    df_el_agg = df_el.groupby([col, "year"]).sum(crop).reset_index()
    df_el_agg2 = df_el.groupby([col, original_crop_col, "year"]).sum(crop).reset_index()

    ## Subset aggregates to target crop for plotting
    df_plt = df_el_agg.loc[df_el_agg[col] == crop].copy()
    df_plt2 = df_el_agg2.loc[df_el_agg2[col] == crop].copy()

    ## Subset official statistics to current region, target crop and target variable
    mask = (df_ocs["geo"] == country_code) & (df_ocs["crops"] == crop) & (df_ocs["strucpro"].isin(["area", "main_area"]))
    df_plt3 = df_ocs.loc[mask].copy()

    ## Figure
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    ## Make sure all years are displayed
    min_year = max(2013, df_plt2['year'].min(), df_plt3['year'].min())
    max_year = min(2026, df_plt2['year'].max(), df_plt3['year'].max())
    all_years = list(range(min_year, max_year + 1))
    use_stacked_bar = len(all_years) < 2

    ## Add stacked area chart for crop contributions
    for sub_crop in df_plt2[original_crop_col].unique():
        crop_data = df_plt2[df_plt2[original_crop_col] == sub_crop]

        # Truncate long legend names
        legend_name = sub_crop[:30] + '...' if len(sub_crop) > 30 else sub_crop

        # Reindex to include all years, filling missing values with 0
        crop_data = crop_data.set_index('year')['field_size'].reindex(all_years, fill_value=0).reset_index()
        crop_data.columns = ['year', 'field_size']

        if use_stacked_bar:
            crop_data = (
                crop_data
                .groupby('year', as_index=False)['field_size']
                .sum()
                .set_index('year')
                .reindex(all_years, fill_value=0)
                .reset_index()
            )

            fig.add_trace(
                go.Bar(
                    x=crop_data['year'],
                    y=crop_data['field_size'],
                    name=legend_name
                )
            )

            fig.update_layout(barmode='stack')
        else:
            fig.add_trace(go.Scatter(x=crop_data['year'], y=crop_data['field_size'],
                                     mode='lines', stackgroup='one', name=legend_name))


    # Add line plot for total field size
    df_plt3.dropna(inplace=True)
    total_data = df_plt3.set_index('year')['field_size'].reindex(all_years, fill_value=0).reset_index()
    total_data.columns = ['year', 'field_size']
    # Add dotted lines for ±10% around total area
    upper_band = total_data['field_size'] * 1.1
    lower_band = total_data['field_size'] * 0.9

    if use_stacked_bar:
        dash='solid'
        mode='lines+markers'
    else:
        dash='dot'
        mode = 'lines'

    fig.add_trace(go.Scatter(x=total_data['year'], y=upper_band,
                             mode=mode, name='Upper Bound Eurostat (+10%)',
                             line=dict(color='gray', width=1, dash=dash)))

    fig.add_trace(go.Scatter(x=total_data['year'], y=lower_band,
                             mode=mode, name='Lower Bound Eurostat(-10%)',
                             line=dict(color='gray', width=1, dash=dash)))

    fig.add_trace(go.Scatter(x=total_data['year'], y=total_data['field_size'],
                             mode='lines+markers', name='Eurostat Area',
                             line=dict(color='black')))

    fig.update_layout(title=f"{country_code} - {col} - {crop}",
                      xaxis_title="Year", yaxis_title="Area")

    ## Save figure to disc
    out_folder = os.path.join("figures", "crop_share_validation", country_code)
    helper_functions.create_folder(out_folder)
    out_pth = os.path.join(out_folder,
            f"{country_code}_{col}_{crop.replace('\\', '').replace('/', '').replace('.', '').replace(' ', '_').replace('-', '_')}{ext}.html")
    fig.write_html(out_pth)

    print(country_code, crop, "done.")


def match_original_crop_codes_to_agripod(tr_pth, country_code):

    df_map = pd.read_excel(tr_pth)

    if country_code in ["IT_MAR", "IT_TOS", "IT_EMR"]:
        lst = glob.glob(os.path.join("data", "tables", "crop_classifications",
                                     f"IT_crop_classification_final.csv"))
    else:
        lst = glob.glob(os.path.join("data", "tables", "crop_classifications",
                                    f"{country_code}*_crop_classification_final.csv"))
    lst = [pd.read_csv(pth) for pth in lst]
    df_hcat = pd.concat(lst)

    df_out = pd.merge(df_hcat, df_map[['EC_hcat_n', 'crop_group', 'agripod_l3', 'agripod_l2', 'agripod_l1', 'agripod_l0']],
                     how="left", on="EC_hcat_n")

    out_folder = os.path.join("figures", "crop_share_validation", country_code)
    helper_functions.create_folder(out_folder)

    out_pth = os.path.join(out_folder, f"{country_code}_crop_classification_mapped.csv")
    df_out.to_csv(out_pth, index=False)


def calculate_difference_tables(tr_pth, crop_shares_official_pth, value_vars, country_code, fs_scale_factor=1, csv_sep=";"):
    ## Open dataframes
    df_map = pd.read_excel(tr_pth)
    df_ocs = pd.read_csv(crop_shares_official_pth, sep=csv_sep)

    ## Turn official statistics into long dataframe
    df_ocs = df_ocs.melt(id_vars=["crops", "geo", "strucpro"],
                         value_vars=value_vars,
                         var_name="year",
                         value_name="field_size")
    df_ocs["year"] = pd.to_numeric(df_ocs["year"], errors='coerce')
    df_ocs["field_size"] *= fs_scale_factor


    ## Get all areas calculated from GSA data for current regions
    folder = os.path.join("data", "tables", "statistics", "crop_shares_harmonized_v1_3")
    pth_lst = glob.glob(os.path.join(folder, f"{country_code}*.csv"))

    lst = []
    for pth in pth_lst:
        df = pd.read_csv(pth)
        year = helper_functions.get_year_from_path(pth)
        df["year"] = year
        df["geo"] = country_code
        lst.append(df)

    df_el = pd.concat(lst)

    ## Map Agripod crop names to Europe-LAND areas
    df_el = pd.merge(df_el, df_map[['EC_hcat_n', 'crop_group', 'agripod_l3', 'agripod_l2', 'agripod_l1', 'agripod_l0']],
                     how="left", on="EC_hcat_n")
    df_el["year"] = pd.to_numeric(df_el["year"], errors='coerce')

    cols = ["agripod_l0", "agripod_l1", "agripod_l2", "agripod_l3"]

    col = "agripod_l2"
    lst = []
    for col in cols:
        df_aggr = df_el.groupby([col, "year"]).sum("field_size").reset_index()
        df_aggr.rename(columns={col: "crops"}, inplace=True)
        mask = (df_ocs["geo"] == country_code) & (df_ocs["strucpro"].isin(["area", "main_area"]))
        df_temp = df_ocs.loc[mask].copy()
        df_aggr = pd.merge(df_aggr, df_temp, "left", on=["crops", "year"])
        df_aggr["level"] = col
        lst.append(df_aggr)

    df_out = pd.concat(lst)
    df_out.dropna(inplace=True)
    df_out["difference"] = df_out["field_size_x"] - df_out["field_size_y"]
    df_out["difference_percent"] = round(df_out["difference"] / df_out["field_size_y"] * 100, 2)

    df_out1 = pd.pivot(data=df_out, columns="year", index=["crops", "level"], values=["difference"]).reset_index()
    df_out2 = pd.pivot(data=df_out, columns="year", index=["crops", "level"], values=["difference_percent"]).reset_index()

    df_out1.sort_values(by=["level", "crops"], inplace=True)
    df_out2.sort_values(by=["level", "crops"], inplace=True)

    with pd.ExcelWriter(os.path.join('figures', 'crop_share_validation', country_code,
                                     f'{country_code}_crop_share_differences.xlsx')) as writer:
        df_out1.to_excel(writer, sheet_name='Absolute_Differences')
        df_out2.to_excel(writer, sheet_name='Percentage_Differences')

    ## Aggregate areas by year and by crop+year
    # df_el_agg = df_el.groupby([col, "year"]).sum(crop).reset_index()

    print("Done.")



def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # preprocess_eurostat_data()
    # preprocess_eurostat_data_nuts2()
    prepare_europeland_data()

    for country_code in ["CZ"]:#["CY", "CZ", "DK", "EE", "EL", "ES", "FI", "FR", "HU", "IE",
                         #"IT_TOS", "IT_EMR", "IT_MAR", "LV", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]:

        # "AT", "BE", "BG",

        language="en"
        language = "original"

        tr_pth = os.path.join("data", "tables", "hcat_levels_v2", "HCAT3_levels_adapted_crop_groups_info.xlsx")

        if country_code in ["IT_TOS", "IT_EMR", "IT_MAR"]:
            crop_shares_official_pth = os.path.join("data", "tables", "statistics", "official statistics",
                                                    "crop_shares", "crop_shares_it_nuts2_2013-2026.csv")
            value_vars = [str(i) + ' ' for i in ["2005", "2007", "2010", "2013", "2016", "2020", "2023"]]
            fs_scale_factor = 1
        else:
            crop_shares_official_pth = os.path.join("data", "tables", "statistics", "official statistics",
                                                    "crop_shares", "crop_shares_2013-2026.csv")
            value_vars=[str(i) + ' ' for i in range(2013, 2027)]
            fs_scale_factor = 1000

        match_original_crop_codes_to_agripod(tr_pth, country_code)
        calculate_difference_tables(tr_pth, crop_shares_official_pth, value_vars, country_code,
                                     fs_scale_factor=fs_scale_factor, csv_sep=",")

        ######## LEVEL 0 ########
        col = "agripod_l0"
        agripod_l0_list = [
            'utilised_agricultural_area'
        ]

        for crop in agripod_l0_list:
            compare_shares(tr_pth, crop_shares_official_pth, value_vars, country_code, col, crop,
                           fs_scale_factor=fs_scale_factor, language=language)

        ######## LEVEL 1 ########
        col = "agripod_l1"
        agripod_l1_list = [
            'arable_land',
            'permanent_grassland',
            'permanent_crops',
            'other'
        ]

        for crop in agripod_l1_list:
            compare_shares(tr_pth, crop_shares_official_pth, value_vars, country_code, col, crop,
                           fs_scale_factor=fs_scale_factor, language=language)

        ######## LEVEL 2 ########
        col = "agripod_l2"
        agripod_l2_list = [
            'kitchen_gardens',
            'cereals_grain_production',
            'dry_pulses_and_protein_crops',
            'fresh_vegetables_strawberries_melons',
            'permanent_crops_human_consumption',
            'other'
        ]

        ######## LEVEL 3 ########
        for crop in agripod_l2_list:
            compare_shares(tr_pth, crop_shares_official_pth, value_vars, country_code, col, crop,
                           fs_scale_factor=fs_scale_factor, language=language) #, language="en"

        col = "agripod_l3"
        agripod_l3_list = [
            'wheat_spelt',
            'rye_winter_cereal_mixtures',
            'barley',
            'oats_spring_cereal_mixtures',
            'grain_maize_ccm',
            'oilseeds',
            'sunflower',
            'soy',
            'green_maize',
            'temporary_grasses_grazing',
            'leguminous_plants_harvested_green',
            'other_cereals_harvested_green',
            'potatoes',
            'sugar_beet',
            'tomatoes',
            'cucumbers',
            'carrots',
            'onions',
            'apples',
            'pears',
            'cherries',
            'oranges',
            'grapes',
            'grapes_for_wine',
            'grapes_for_wine_pdo',
            'grapes_for_raisins',
            'olives_table_use',
            'olives_oil',
            'other'
        ]

        for crop in agripod_l3_list:
            compare_shares(tr_pth, crop_shares_official_pth, value_vars, country_code, col, crop,
                           fs_scale_factor=fs_scale_factor, language=language) #, language="en"

    for country_code in ['DE_BAV', "DE_BRB", "DE_BWB", "DE_MWP", "DE_NRW", "DE_RLP", "DE_SAA", "DE_SAT", "DE_THU"]: #
        tr_pth = os.path.join("data", "tables", "hcat_levels_v2", "HCAT3_levels_adapted_crop_groups_info_de.xlsx")
        crop_shares_official_pth = os.path.join("data", "tables", "statistics", "official statistics",
                                                "crop_shares", "crop_shares_DE_2013-2023.csv")
        value_vars = ['2010', '2013', '2016', '2020', '2023']
        fs_scale_factor = 1

        match_original_crop_codes_to_agripod(tr_pth, country_code)
        calculate_difference_tables(tr_pth, crop_shares_official_pth, value_vars, country_code, fs_scale_factor=1,
                                    csv_sep=";")

        ######## LEVEL 0 ########
        col = "agripod_l0"
        agripod_l0_list = [
            'Landwirtschaftlich genutzte Fläche'
        ]

        for crop in agripod_l0_list:
            compare_shares(tr_pth, crop_shares_official_pth, value_vars, country_code, col, crop,
                           fs_scale_factor=fs_scale_factor, csv_sep=";")

        ######## LEVEL 1 ########
        col = "agripod_l1"
        agripod_l1_list = [
            'Ackerland',
            'Haus- und Nutzgärten (Gartenland)',
            'Dauergrünland',
            'Dauerkulturen',
            'Andere Speisepilze',
            'Waldflächen und Kurzumtriebsplantagen',
            'Gebäude- und Hofflächen sowie andere Flächen',
            'Dauerh.a.d.landw.Prod.genommene Fl. oh.Präm.anspr.'
        ]

        for crop in agripod_l1_list:
            compare_shares(tr_pth, crop_shares_official_pth, value_vars, country_code, col, crop,
                           fs_scale_factor=fs_scale_factor, csv_sep=";")

        ######## LEVEL 2 ########
        col = "agripod_l2"
        agripod_l2_list = [
            'Andere Flächen (z.B. Landschaftselemente)',
            'Anderes',
            'Aus d.Erz.gen.Dauergrünl.m.Beihilfe-/Prämienanspr.',
            'Dauerkulturen im Freiland',
            'Gemüse, Erdbeeren u.a. Gartengewächse',
            'Getreide zur Körnergewinnung',
            'Hackfrüchte',
            'Handelsgewächse',
            'Hülsenfrüchte zur Körnergewinnung',
            'Kurzumtriebsplantagen',
            'Pflanzen zur Grünernte',
            'Saat-/Pflanzguterzeugung für Gräser, Hackfrüchte',
            'Sonstige Kulturen auf dem Ackerland',
            'Waldflächen'
        ]

        ######## LEVEL 3 ########
        for crop in agripod_l2_list:
            compare_shares(tr_pth, crop_shares_official_pth, value_vars, country_code, col, crop,
                           fs_scale_factor=fs_scale_factor, csv_sep=";")

        col = "agripod_l3"
        agripod_l3_list = [
            'Ackerbohnen',
            'And. Hülsenfrüchte u.Mischkulturen zur Körnergew.',
            'Andere Dauerkulturen',
            'Andere Hackfrüchte ohne Saatguterzeugung',
            'Anderes',
            'Anderes Getreide zur Körnergewinnung',
            'Baum- und Beerenobst (einschließlich Nüsse)',
            'Blumen und Zierpflanzen',
            'Erbsen (ohne Frischerbsen)',
            'Feldgras/Grasanbau auf dem Ackerland',
            'Gartenbausämereien, Jungpflanzenerz. zum Verkauf',
            'Gemüse und Erdbeeren',
            'Gerste',
            'Getreide zur Ganzpflanzenernte',
            'Hafer',
            'Kartoffeln',
            'Körnermais/Mais zum Ausreifen (einschließlich CCM)',
            'Leguminosen zur Ganzpflanzenernte',
            'Ölfrüchte zur Körnergewinnung',
            'Rebflächen',
            'Roggen und Wintermenggetreide',
            'Silomais/Grünmais einschl. Lieschkolbenschrot',
            'Sojabohnen',
            'Sommermenggetreide',
            'Stillgelegte Flächen mit Beihilfe-/Prämienanspruch',
            'Süßlupinen',
            'Triticale',
            'Weihnachtsbaumkulturen',
            'Weitere Handelsgewächse',
            'Weizen',
            'Zuckerrüben ohne Saatguterzeugung'
        ]

        for crop in agripod_l3_list:
            compare_shares(tr_pth, crop_shares_official_pth, value_vars, country_code, col, crop,
                           fs_scale_factor=fs_scale_factor, csv_sep=";")


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()
