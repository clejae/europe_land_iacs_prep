# Europe-LAND - Harmonized IACS Inventory
The [Harmonized IACS inventory of Europe-LAND](https://zenodo.org/records/14230621) is a harmonized collection of data from the Geospatial Aid (GSA) system of the Integrated Control and Administration System (IACS), which manages and controls agricultural subsidies in the European Union (EU). The GSA data are a unique data source with field-levels of land use information that are annually generated. The data carry information on crops grown per field, a unique identifier of the subsidy applicants that allows to aggregate fields to farms, information on organic cultivation and animal numbers per farm. The [Europe-LAND project](https://europe-land.eu/) is funded by the EU within Horizon Europa (Grant Agreement No. 101081307).  __Disclaimer:__ If you use the harmonized data, please also cite the original sources of the data.

This repository comes along with the inventory and contains all scripts that were written
1) to obtain and preprocess the data and
2) a workflow to harmonize the data across the European Union

The crop information were harmonized using the Hierarchical Crop and Agriculture Taxonomy (HCAT) v3 of the [EuroCrops project](https://github.com/maja601/EuroCrops).

## Workflow
All pre-processing scripts can be found in the "pre_processing" folder. These scripts are country specific and prepare the data for the workflow. During pre-processing we made sure that all fields have a unique field-id. If possible, we used information from other columns, such as the "FLIK" in Germany - the official area indicators, and added running cumulative counts to make them unique. If not, we used the first seven digits of the x and y- coordinates of the representative point of the fields to construct a unique ID xxxxxxx_yyyyyyy_cc (where cc is also a cumulative count). Uunfortunately, for half of the countries, we used the centroid points, but cannot reconstruct, where we changed the procedure. We also made sure, that there are not entries with no geometries in the input files, and no duplicate entries. Lastly, in case the crops were recorded in field-blocks (i.e. multiple crops per field block without specific locations), we kept only the largest recorded block in vector file and saved the other ones in an accompanying .csv table.

The harmonization workflow is indicated with the letters a - d. We provide a diagramm of our the project structure below and tables we created for the harmonization to exemplify the workflow. Each script contains a detailed description of what it does, what inputs are needed and where the output is stored at the top. We used a "run_dict" that is specified at the top of the main function to turn off or on the processing of a specific country. You can use the "switch" key in the country-specific item of the run_dict to turn on or off the script for the respective countries. With the latest version all tables are saved as csv.
1) __Script a__ lists all columns found in the vector data and provides an example of an attribute for each column. The input vector data should be stored in `\data\vector\IACS\XX\`, where XX is the member state code
of the [Interinstitutional Style Guide of the EU](https://style-guide.europa.eu/en/content/-/isg/topic?identifier=annex-a6-country-and-territory-codes). If you have sub-datasets for the country, e.g. for federal states in Germany, create a subfolder `\data\vector\IACS\XX\XXX\`. Choose an abbreviation of your liking. For example, for Thuringian data from Germany, the data would be stored here: `\data\vector\IACS\DE\THU\`. Each file should contain a number indicating the year of the data, best case 4 digits, but the last 2 digits of the year also work.

2) *Manual work:* After executing  __Script a__ the user has to create a column name translation table (xlsx) that assigns all the original columns to the harmonized column names per year. Before moving on, this file has to be present in `\data\tables\column_name_translations\`. The file naming convention and its internal structure has to follow the predefined standards. 
The file name should be `XX_column_name_translation.xlsx`. It should have one column called `column_name` which contains the column name entries from the table below. Additionally, for each year for which GSA data is available it should contain an additional column with the naming convention XX_YYYY where XX is the country abbreviation and YYYY is the year. It should be filled with the original column names of the GSA data that match the column name description.
Please use one of the files distributed with the codes as a template. The meaning of the column names is the following:

| Column Name   | Column Description                                                                                                                                             | Mandatory for Harmonisation |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| field_id      | Unique identifier for each parcel per member state, state, or region                                                                                            | Mandatory                   |
| farm_id       | Unique identifier for each farm per member state, state, or region                                                                                              | Optional                    |
| crop_code     | Original member state-specific crop code                                                                                                                        | Mandatory                   |
| crop_name     | Original member state-specific crop name                                                                                                                        | Mandatory                   |
| EC_trans_n    | Original crop name translated into English                                                                                                                      | Mandatory                   |
| EC_hcat_n     | Machine-readable name of the crop from HCAT                                                                                                                     | Mandatory                   |
| EC_hcat_c     | The ten-digit HCAT code of the hierarchy of the crop                                                                                                            | Mandatory                   |
| organic       | Whether a parcel was cultivated conventional (0), organic (1), or is in the conversion process to organic cultivation (2)                                      | Optional                    |
| field_size    | Size of parcel/reference parcel in hectares                                                                                                                     | Mandatory                   |

3) The tables manually generated in step 2 are needed for __script b1__ that lists all available crop code - crop name combinations found in the vector data. It then translates all crop names to English and German. If the EuroCrops project already provided a mapping table to their classes, the script matches the new table to their classification. The outputs of this code are placed in `\data\tables\crop_names\` either as `XX_crop_names_w_EuroCrops_class.xlsx` or as `XX_crop_names_w_translation.xlsx`.
4) __Script b2__ is optional and only necessary for French data. It is included mostly for documentation purposes.
5) __Script b3__ is optional. It matches a table of unclassified crops with all already created HCAT crop classifications. For that, we use a string matching algorithm with the Jaro-Winkler metric. The matching is performed on the translated crop names to English. However, __the user still has to verify the matches manually__, but the workload is drastically reduced, if there is no existing EuroCrops classification. The outputs of this code are placed in `\data\tables\crop_names\` as `XX_crop_names_w_translation_and_match.xlsx`.
6) __Script b4__ is optional validates if the unique field identifiers are truly unique. Before the uniqueness-check features without geometry and duplicate geometries are removed.
7) __Script b5__ is optional cleans the data by removing geometry errors and ensuring every record has a unique field identifier. It also standardizes geometries (buffering and normalizing) and repairs ID columns by either generating new unique IDs or appending counters to existing duplicate IDs.
8) *Manual work:* Then, the user has to manually fill the gaps that are still existent in the crop classification tables. If you used only b1, your input will be in `\data\tables\crop_names\` either `XX_crop_names_w_EuroCrops_class.xlsx` or as `XX_crop_names_w_translation.xlsx` or if you used also b3 your input will be in `\data\tables\crop_names\` as `XX_crop_names_w_translation_and_match.xlsx`. You need to create a classification table that lists all unique crop codes and all crop names (either one of them has to be filled) and additionally the EuroCrops HCAT columns. The table should be stored in 'data\tables\crop_classifications\XX_crop_classification_final.xlsx' and should contain at least the following columns:

| Column Name   | Column Description                                                                                                                                             |
|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| crop_code     | Original member state-specific crop code                                                                                                                        | 
| crop_name     | Original member state-specific crop name                                                                                                                        | 
| EC_trans_n    | Original crop name translated into English                                                                                                                      | 
| EC_hcat_n     | Machine-readable name of the crop from HCAT                                                                                                                     | 
| EC_hcat_c     | The ten-digit HCAT code of the hierarchy of the crop                                                                                                            | 

9) __Scripts c1 and c2__ are optional. They can be used to explore the unmaintained and not_known_and_other HCAT classes and to verify our version of the HCAT with a new version of the HCAT.
10) __Script c3__ uses the manually generated classification table in `\data\tables\crop_classifications\` and the column name translation tables in `\data\tables\column_names\` to harmonize the crop codes and the column names from the original GSA data. Removes also geometry duplicates and creates a unique field id and calculates the field area, if needed. The results will be saved as geoparquets to `\data\vector\IACS_EU_Land\XX\`.
11) __Script d1__ prepares information on the harmonized data and the classifications.
12) __Script d3__ prepares the data for publication (e.g. removes information that cannot be shared).
13) __Script e1__ creates the prompt for a LLM to learn the current version of the HCAT v3 classification.  

__We provide all column name translation tables and crop classification tables that we created in the [tables folder](tables) .__ If you find errors, please do not hesitate to contact us.

## Project setup for replication
```
├── data 
│    └── tables
│    │     └── column_name_translations
│    │     └── column_names
│    │     └── crop_classifications
│    │     └── crop_names
│    │     └── statistics
│    └── vector
│          └── IACS
|              └── XX
|                   └──XX_YYYY.gpkg (or .shp, .geoparquet, .geojson)
|                   └──... 
|                   └──XXX (only if data are provided for subregions of a country)
|                       └──XX_YYYY.gpkg (or .shp, .geoparquet, .geojson)
|                       └──...
│          └── IACS_EU_Land
├── figures
├── scripts
```
Where `XX` is the country code (or `XX_XXX` is country code with code for a subregion) and `YYYY` is the year.

## Correction procedure or v1.3
We conducted a comprehensive verification of the national crop and land-use lists against the **HCAT v3 (Harmonised Crop and Agriculture Taxonomy)** scheme. Our approach combined Large Language Model (LLM) processing with manual expert review to ensure high-fidelity data harmonization.

### LLM-Assisted Classification Process
To cross-reference our internal data, we utilized ChatGPT as an independent classification validator:
* **Context Initialization**: The model was first initialized with a specialized context prompt __e2_classification_scheme_prompt.txt__ to ensure it adhered strictly to the HCAT v3 taxonomy rules.
* **Batch Processing**: Crop data was copied from the crop_classification tables in batches of 50–80 rows into th ChatGPT chat, containing the `crop_code` and `crop_name`. Using more rows led to errors.
* **Comparative Analysis**: We compared the existing `EC_hcat_n` classifications against the LLM-generated results. This allowed us to identify discrepancies and refine both the `EC_hcat_n` and `EC_hcat_c` (coding) columns where the LLM provided a more accurate or granular fit.

#### Refined Classification Logic
During the verification, we applied the following hierarchical rules to maintain consistency:
* **Cereals**: We did not use the `unspecified_season_` class-).
* **Ornamentals**: We favored broader functional categories over "other" designations; for instance, "Cut flowers" were assigned to `flower_ornamental_plants` and not to other_flowers_ornamental_plants`.
* **Seedlings**: We prioritized the specific crop type over general seed categories. "Planting material of vegetables" was classified under `fresh_vegetables` rather than the general `arable_land_seed_seedlings` class unless no specific match existed.

#### Validation Resources
To resolve ambiguous cases, we cross-referenced multiple sources:
* **Visual & Botanical Verification**: Original crop names were researched in their native languages combined with native terms for "plant" to confirm the classification.
* **Linguistic Cross-Referencing**: Wikipedia was used to translate local crop names into English to check the translation and resolve misunderstandings.
* **Project Alignment**: We referenced existing [EuroCrops Country Mappings](https://github.com/maja601/EuroCrops/tree/main/csvs/country_mappings) to ensure our logic remained consistent with established international datasets.

### Updates to the Classification Scheme
Based on the recent verification process, we implemented several specific refinements and new classes to the taxonomy:

* **Granular Crop Specifications**: We differentiated between `early_season_potatoes` and `late_season_potatoes` to capture seasonal variations.
* **Added Land-Use Classes**: We integrated `agroforestry`, `paludiculture`, `wetlands`, `agrivoltaics`,  and `short_rotation_coppices` into the scheme.
* **Added Crop Classes**: We integrated `black_salsify`, `sugar_cane`,  `turnip_rape`, `oil_fodder_radish`, `bitter_lupins`, `lathyrus_peavine`, and `birdsfoot_trefoil` into the scheme. Some also got refined subclasses
* **Refined existing Classes**: We added refinements to `greenhouses` and `foil_films` to distinguish between different crops.
* **Mixed Class Refinements**:  
    * `cereal_for_fodder` was reclassified as `green_cut_[cereals]` (e.g., green cut rye, barley, or oat) and assigned to `plants_harvested_green`
    * We introduced `mixed_cropping` as a subcategory for general crop mixes that included main crops, such as cereals or rapeseed, and undersowings. They were mapped as `[cereal]_with_undersowing`.
    * `mixed_cropping` also includes `mixed_row_cropping` as a sublcass
    * Mixes that were likely used as fodder or silage, such as clover-grass-mixes or cereal-legume mixes or mustard with undersowing got a new class and were classified under `plants_harvested_green`
    * `mixed_permanent_plantations`, `pastures_meadows_with_trees` were introduced
* **Non-Productive Elements**: We created a `non_productive_landscape_elements` class to encompass buffer vegetation, including subclasses for `field_margins`, `buffer_strips`, `flowering_areas`, `tree_lines`, `hedges`, `wetlands`, `ditches`, and `stone_walls`.
* **Environmental Protection**: A dedicated `environmental_protection` category was established for nature-protection-linked features. However, specific elements like biobelts and nectar strips were assigned to the `field_margins_buffer_strips_flowering_areas` sub-classification.
* **Handling of Forage Crops**: We removed the general `other_forage_crops` class in favor of more specific mapping:
    * Forage items were redistributed into `temporary_grass`, `plants_harvested_green`, `fodder_roots`, or `legumes_harvested_green`.
    * Specific crops (e.g., sorrel or malva) described as forage were mapped to `other_plants_harvested_green`.
 
### Handling of Mixed Classes
We implemented a specific protocol to indicate mixed land uses. However these mixes are only indicated in the `crop_classification_tables` and not in the vector files:
* **Identification**: We scanned the dataset for keywords such as "mix," "and," or "under" within the English translations (`EC_trans_n`).
* **Indication**: A `mixed_crops` column was created in the `crop_classification_tables` and the `mix_id` from the table below was used to describe the observed mixes. 

| mix_id | Description | Methodology / Handling |
|--------|-------------|-------------------------|
| 1 | **Mixed Seeds**<br><br>Refers to crop mixtures cultivated simultaneously. Examples include clover-grass, cereals with undersown clover, cereal-pea/bean mixtures, cereal-buckwheat, etc. | Where applicable, existing HCAT classes were utilized (e.g., spring mixed cereals → `spring_meslin`) or new classes were created. In cases where class descriptions were too vague, data was assigned to an **other** category or a higher-level classification. |
| 2 | **Intercropping (Mixed Rows)**<br><br>Crop types cultivated in alternating rows, such as maize and scarlet runner beans, or soybeans and spring vetches. | New HCAT classes were created: `Maize_bean_row_mix` and `soybeans_vetches_row_mix`. |
| 3 | **Lists of Multiple Crop Types**<br><br>Land-use descriptions that encompass several crop types where the specific crop cultivated by the applicant is not clearly identified (e.g., “Millet, sorghum, canary seed, or durum wheat”). | If crops share a common higher-level HCAT category, that category was selected. If they fall under different higher levels, they were classified as `mixed_cropping` for simplification.<br><br>**Special case:** For “Emmer-Einkorn,” Emmer was consistently selected. |
| 4 | **Combined Specific/Non-Specific Descriptions**<br><br>Entries where a specific usage is provided alongside references to broader usages (e.g., “Mowing meadow/pasture with three or usages”). | Classification was based on the specific usage provided. |
| 5 | **Main Crop with Field Margin Vegetation**<br><br>Classes encompassing a primary crop alongside boundary strips (e.g., maize with hunting/flowering strips). | The primary crop was used for classification. |
| 6 | **Successive Crops**<br><br>Land-use descriptions involving two crops grown sequentially (common in Austria, e.g., maize followed by field vegetables). | Only the initial crop type was classified. |
| 7 | **In-Class Mixtures**<br><br>Mixtures consisting of different varieties of the same or similar plants (e.g., clover mixtures, grass-herb mixtures, or wheat-triticale mixtures). | These were mapped to existing HCAT classes where possible (e.g., clover mixtures → `clover`). Field bean–pea mixtures were classified as `legumes_dried_pulses_protein_crops`. |
| 8 | **Pasture with Trees or Orchards with Pastures**<br><br>Permanent grassland interspersed with trees. | A new class, `pastures_meadows_with_trees`, was created for the former; `orchards` was selected for the latter. |
| 9 | **Mixed Plantations (Fruit, Berries, Olives, Nuts, and Viticulture)** | New classes were established for frequent combinations (e.g., citrus with persimmon/khaki). All others were categorized under `permanent_crop_`. |
| 10 | **Mixture of Agricultural Use and Open Ground/Containers** | The specific agricultural usage was used for classification. |
| 11 | **Listed Crop Types (Adjacent Plots)**<br><br>Specific to Portugal: Crops listed together but likely grown on separate, adjacent plots. | The category `mixed_cropping` was applied. |
| - | **Broadly defined land-use descriptions** (e.g., “Cereals”). | These were classified at a higher level of the HCAT hierarchy. |
| - | **Descriptions containing “other” categories** (e.g., “other legumes”). | These were classified at a higher level of the HCAT hierarchy. |
| - | **Multiple crop types per field block** (permitted in certain countries), reported across multiple columns in original data. | Every crop type was classified. The crop with the largest area is stored in the geodata. If areas are equal or unspecified, the first listed crop is used; additional crops are provided in a supplementary CSV file. |



### Linux
The Python environment suitable to execute the workflow can be created using the `./pythonEnv.sh` code. Alternatively, a Docker image suitable to execute the codes can be build from   `./Dockerfile/Dockerfile` of be pulled with the following command `docker pull kelewinska/europe_land_iacs_prep:latest`
