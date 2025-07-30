install.packages("writexl")
library(polyglotr)
library(readxl)
library(writexl)
library(dplyr)

# This script is only needed for the French data

FR_df <- read_xlsx("Q:/Europe-LAND/data/tables/crop_names/FR_FR_crop_names_w_EuroCrops_class.xlsx")
cultures <- read.csv2("Q:/Europe-LAND/data/vector/IACS/FR/REF_CULTURES_2021.csv")

#add crop culture name to eurocrops_class file

Code_cult <- data.frame(crop_name = cultures$CODE, crop = cultures$LIBELLE_CULTURE )

FR_cult_joined <- full_join(FR_df, Code_cult, by = join_by(crop_name))

#split data 

FR_cult_code <- FR_cult_joined[!(is.na(FR_cult_joined$crop)), ]
FR_without_cult_code <- FR_cult_joined[is.na(FR_cult_joined$crop), ]

#create new dataframe and fill columns based on crop translation

Fixed_rows <- data.frame(crop_code = FR_cult_code$crop_code,
                         crop_name = FR_cult_code$crop,
                         crop_name_de = NA,
                         crop_name_en = NA,
                         EC_trans_n = NA,
                         EC_hcat_n = NA,
                         EC_hcat_c = NA,
                         FR_crop_code = FR_cult_code$crop_name)

Fixed_rows$crop_name_de <- google_translate(Fixed_rows$crop_name, target_language = "de", source_language = "fr")
Fixed_rows$crop_name_en <- google_translate(Fixed_rows$crop_name, target_language = "en", source_language = "fr")
Fixed_rows$EC_trans_n <- Fixed_rows$crop_name_en

Fixed_rows$crop_name_de <- as.character(Fixed_rows$crop_name_de)
Fixed_rows$crop_name_en <- as.character(Fixed_rows$crop_name_en)
Fixed_rows$EC_trans_n <- as.character(Fixed_rows$EC_trans_n)

#union with data subset which didn't contain appreciations 

FR_without_cult_code <-  FR_without_cult_code %>% select(-crop)
FR_without_cult_code$FR_crop_code <- NA

FR_FR_crop_names_w_translation_without_abbreviations <- rbind(FR_without_cult_code, Fixed_rows)

FR_FR_crop_names_w_translation_without_abbreviations[order(FR_FR_crop_names_w_translation_without_abbreviations$crop_name), ]

# save file

write_xlsx(FR_FR_crop_names_w_translation_without_abbreviations, "Q:/Europe-LAND/Phillip/FR_FR_crop_names_w_translation_without_abbreviations.xlsx")
