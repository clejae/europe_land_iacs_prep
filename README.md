# Europe-LAND - Harmonized IACS Inventory
The [Harmonized IACS inventory of Europe-LAND](https://doi.org/10.5281/zenodo.14230621) is a harmonized collection of data from the Geospatial Aid (GSA) system of the Integrated Control and Administration System (IACS), which manages and controls agricultural subsidies in the European Union (EU). The GSA data are a unique data source with field-levels of land use information that are annually generated. The data carry information on crops grown per field, a unique identifier of the subsidy applicants that allows to aggregate fields to farms, information on organic cultivation and animal numbers per farm. The [Europe-LAND project](https://europe-land.eu/) is funded by the EU within Horizon Europa (Grant Agreement No. 101081307).

This repository comes along with the inventory and contains all scripts that were written
1) to obtain and preprocess the data and
2) a workflow to harmonize the data across the European Union

The crop information were harmonized using the Hierarchical Crop and Agriculture Taxonomy (HCAT) of the [EuroCrops project](https://github.com/maja601/EuroCrops).

## Workflow
All pre-processing scripts can be found in the "pre_processing" folder. These scripts are country specific and prepare the data for the workflow.
The harmonization workflow is indicated with the letters a - d. We uploaded the project folder structure and example tables to exemplify the workflow. 
1) __Script a__ is a simple script to list all columns found in the vector data and provide an example of an attribute.
2) *Manual work:* After that the user has to create a column name translation table (xlsx) that assigns all the original columns to the harmonized column names per year.
3) These tables are needed for __script b1__ that lists all available crop code - crop name combinations found in the vector data. It then translates all crop names to English and German. If the EuroCrops project already provided a mapping table to their classes, the script matches the new table to their classification.
4) __Script b2__ is optional. It matches a table of unclassified crops with all already created HCAT crop classifications. For that, we use a string matching algorithm with the Jaro-Winkler metric. The matching is perfomed on the translated crop names to English. However, __the user still has to verify the matches manually__, but the workload is drastically reduced, if there is no existing EuroCrops classification.
5) *Manual work:* Then, the user has to manually fill the gaps that are still existent in the crop classification tables.
6) __Scripts c1 and c2__ are optional. They can be used to explore the unmaintained and not_known_and_other HCAT classes and to verify our version of the HCAT with a new version of the HCAT.
7) __Script c3__ uses the manually generated classification table and the column name translation tables to harmonize the crop codes and the column names.
8) __Script d1__ prepares information on the harmonized data and the classifications.
9) __Script d3__ prepares the data for publication (e.g. removes information that cannot be shared).

__We provide all column name translation tables and crop classification tables that we created in the respective folders.__ If you find errors, please do not hesitate to contact us.

## Project setup for replication
├── data
|    └── tables
|          └── column_name_translations
|          └── column_names
|          └── crop classifications
|          └── crop_names
|          └── statistics
|    └── vector
|          └── IACS
|          └── IACS_EU_Land
├──figures
├──scripts
