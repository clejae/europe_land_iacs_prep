#' ---
#' Script to classify organic fileds via 'bindungs_codes'
#' documentation: ""
#' ---
#' 
##############################################################################################################
# 0. LOAD PACKAGES
##############################################################################################################

library(sf)
library(dplyr)

##############################################################################################################
# classify oeko in invekos 2005-2018
##############################################################################################################

# load list with invekos files from 2005-2018
invekos_files <- list.files("Q:/Europe-LAND/data/vector/IACS/DE/BRB/antrag_land_2025", pattern=".shp$", full.names=T, recursive=T)

# set output folder
output_folder <- "Q:/Europe-LAND/data/vector/IACS/DE/BRB"

# bindungsc_codes list with FP "Oekologischer Landbau"
code_list <- list("323|223", # 2005
                  "323", # 2006
                  "323|423", # 2007
                  "323|423|523", # 2008
                  "323|423|523|623", # 2009
                  "423|623", # 2010
                  "423|623", # 2011
                  "423|623", # 2012
                  "423|623", # 2013
                  "423|623", # 2014
                  "623|881|882|883|884|885", # 2015
                  "881|882|883|884|885", # 2016
                  "881|882|883|884|885", # 2017
                  "881|882|883|884|885") # 2018

# create counter for year list position
n <- 1

# loop over code_list 
for(i in invekos_files){
  
  # read in data and remove existing oeko col 
  invekos <- as.data.frame(st_read(i, quiet = F)) %>% 
    dplyr::select(!Oeko)
  
  # get file basename and year of Invekos data
  file_name <- tools::file_path_sans_ext(basename(i))
  jahr <- strsplit(file_name, "_")[[1]][3]
  
  # filter invekos data that have oeko bindungscodes in the respective year
  filtered_inv <- dplyr::filter(invekos, grepl(code_list[[n]],invekos$bind_code))
  # classify all oeko fields with 1
  invekos$organic[invekos$ID %in% filtered_inv$ID] <- 1
  # classify all the other fields as 0
  invekos$organic[is.na(invekos$organic)] = 0
  
  # write out file as shp
  st_write(invekos, paste0(output_folder, file_name, ".shp" ))
  
  # write out file and append to gpkg
  # st_write(invekos, dsn = paste0(output_folder, "invekos_2005_2018.gpkg"), layer = paste0(file_name, ".gpkg"))
  
  # increase counter 
  n <- n+1
  print(paste("Done with:", year))
}


##############################################################################################################
# classify oeko in invekos for 2019 to 2024
##############################################################################################################

path_invekos_2019 <- "/Users/Tillman/Documents/Studium/Master/Masterarbeit/data/vector/invekos/Invekos_2019/antrag_2019_wfs_gem.shp"
invekos_2019 <- as.data.frame(st_read(path_invekos_2019))

# get file basename and year of Invekos data
file_name <- tools::file_path_sans_ext(basename(path_invekos_2019))
jahr <- strsplit(file_name, "_")[[1]][2]

# create new ID class
invekos_2019$ID <- seq_len(nrow(invekos_2019))

# filter invekos data that have oeko bindungscodes in the respective year
filtered_inv <- dplyr::filter(invekos_2019, grepl("881|882|883|884|885",invekos_2019$BIND_CODE))
# classify all oeko fields with 1
invekos_2019$Oeko[invekos_2019$ID %in% filtered_inv$ID] <- 1
# classify all the other fields as 0
invekos_2019$Oeko[is.na(invekos_2019$Oeko)] = 0

# write out file as shp
st_write(invekos_2019, paste0(output_folder, file_name, ".shp" ))


##############################################################################################################
# classify oeko in invekos 2025
##############################################################################################################
path_invekos_2025 <- "Q:/Europe-LAND/data/vector/IACS/DE/BRB/antrag_land_2025.shp"

invekos_2025 <- as.data.frame(st_read(path_invekos_2025))

# get file basename and year of Invekos data
file_name <- tools::file_path_sans_ext(basename(path_invekos_2025))
jahr <- "2025"

# create new ID class
invekos_2025$ID <- seq_len(nrow(invekos_2025))

# filter invekos data that have oeko bindungscodes in the respective year
filtered_inv <- dplyr::filter(invekos_2025, grepl("3181|3182|3183|3184|3185|3186",invekos_2025$bind_code))
# classify all oeko fields with 1
invekos_2025$organic[invekos_2025$ID %in% filtered_inv$ID] <- 1
# classify all the other fields as 0
invekos_2025$organic[is.na(invekos_2025$organic)] = 0

# write out file as shp
st_write(invekos_2025, paste0(output_folder, file_name, "_with_organic.shp" ))
