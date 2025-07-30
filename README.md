# Europe-LAND - Harmonized IACS Inventory
The [Harmonized IACS inventory of Europe-LAND](https://zenodo.org/records/14230621) is a harmonized collection of data from the Geospatial Aid (GSA) system of the Integrated Control and Administration System (IACS), which manages and controls agricultural subsidies in the European Union (EU). The GSA data are a unique data source with field-levels of land use information that are annually generated. The data carry information on crops grown per field, a unique identifier of the subsidy applicants that allows to aggregate fields to farms, information on organic cultivation and animal numbers per farm. The [Europe-LAND project](https://europe-land.eu/) is funded by the EU within Horizon Europa (Grant Agreement No. 101081307).  __Disclaimer:__ If you use the harmonized data, please also cite the original sources of the data.

This repository comes along with the inventory and contains all scripts that were written
1) to obtain and preprocess the data and
2) a workflow to harmonize the data across the European Union

The crop information were harmonized using the Hierarchical Crop and Agriculture Taxonomy (HCAT) of the [EuroCrops project](https://github.com/maja601/EuroCrops).

## Workflow
All pre-processing scripts can be found in the "pre_processing" folder. These scripts are country specific and prepare the data for the workflow.
The harmonization workflow is indicated with the letters a - d. We provide a diagramm of our the project structure below and tables we created for the harmonization to exemplify the workflow. Each script contains a detailed description of what it does, what inputs are needed and where the output is stored at the top. We used a "run_dict" that is specified at the top of the main function to turn off or on the processing of a specific country. You can comment/uncomment the specific lines of the run_dict to run the script for the respective countries. Please note that we used the .xlsx data format in many scripts as input and output as it was the easiest way to handle the encoding mess between the different countries - we know it is not optimal.
1) __Script a__ is a simple script to list all columns found in the vector data and provide an example of an attribute. The original data should be stored in `\data\vector\IACS\XX\`, where XX is the member state code
of the [Interinstitutional Style Guide of the EU](https://style-guide.europa.eu/en/content/-/isg/topic?identifier=annex-a6-country-and-territory-codes). Each file should contain a number indicating the year of the data, best case 4 digits, but the last 2 digits of the year also work.

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



3) These tables manually generated in step 2 are needed for __script b1__ that lists all available crop code - crop name combinations found in the vector data. It then translates all crop names to English and German. If the EuroCrops project already provided a mapping table to their classes, the script matches the new table to their classification. The outputs of this code are placed in `\data\tables\crop_names\` either as `XX_crop_names_w_EuroCrops_class.xlsx` or as `XX_crop_names_w_translation.xlsx`.
4) __Script b2__ is optional and only necessary for French data. It is included mostly for documentation purposes.
5) __Script b3__ is optional. It matches a table of unclassified crops with all already created HCAT crop classifications. For that, we use a string matching algorithm with the Jaro-Winkler metric. The matching is performed on the translated crop names to English. However, __the user still has to verify the matches manually__, but the workload is drastically reduced, if there is no existing EuroCrops classification. The outputs of this code are placed in `\data\tables\crop_names\` as `XX_crop_names_w_translation_and_match.xlsx`.
6) *Manual work:* Then, the user has to manually fill the gaps that are still existent in the crop classification tables. If you used only b1, your input will be in `\data\tables\crop_names\` either `XX_crop_names_w_EuroCrops_class.xlsx` or as `XX_crop_names_w_translation.xlsx` or if you used also b3 your input will be in `\data\tables\crop_names\` as `XX_crop_names_w_translation_and_match.xlsx`. You need to create a classification table that lists all unique crop codes and all crop names (either one of them has to be filled) and additionally the EuroCrops HCAT columns. The table should be stored in 'data\tables\crop_classifications\XX_crop_classification_final.xlsx' and should contain at least the following columns:

| Column Name   | Column Description                                                                                                                                             |
|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| crop_code     | Original member state-specific crop code                                                                                                                        | 
| crop_name     | Original member state-specific crop name                                                                                                                        | 
| EC_trans_n    | Original crop name translated into English                                                                                                                      | 
| EC_hcat_n     | Machine-readable name of the crop from HCAT                                                                                                                     | 
| EC_hcat_c     | The ten-digit HCAT code of the hierarchy of the crop                                                                                                            | 

7) __Scripts c1 and c2__ are optional. They can be used to explore the unmaintained and not_known_and_other HCAT classes and to verify our version of the HCAT with a new version of the HCAT.
8) __Script c3__ uses the manually generated classification table in `\data\tables\crop_classifications\` and the column name translation tables in `\data\tables\column_names\` to harmonize the crop codes and the column names from the original GSA data. The results will be saved as geoparquets to `\data\vector\IACS_EU_Land\XX\`.
9) __Script d1__ prepares information on the harmonized data and the classifications.
10) __Script d3__ prepares the data for publication (e.g. removes information that cannot be shared).

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

### Linux
The Python environment suitable to execute the workflow can be created using the `./pythonEnv.sh` code. Alternatively, a Docker image suitable to execute the codes can be build from   `./Dockerfile/Dockerfile` of be pulled with the following command `docker pull kelewinska/europe_land_iacs_prep:latest`