

####################################
#' Tall LPI QC
#'
#'after the lpi tall table is prepared for terradactylutils2::geofiles() using terradactylutils2::clean_tall_lpi(), this function produces the tall_lpi file QC checks
#'
#' @param cleaned_tall_lpi as a data.frame, the tall lpi data that has been through terradactylutils2::clean_tall_lpi
#' @param speciescode the column name in the USDA plant list file that contains the four letter codes
#' @param USDA_plants a data.frame of the USDA plants with the 4 letter code, GrowthHabit and Duration
#' @param tblLPIDetail as a data.frame, the tblLPIDetail from the DIMA tables
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV containing information about the QC of the tall lpi data saved to the designated QC file
#'
#' @examples tall_lpi_qc(cleaned_tall_lpi = cleaned_tall_lpi, speciescode = "UpdatedSpeciesCode", tblLPIDetail = tblLPIDetail, USDA_plants = read.csv("D:/modifying_data_prep_script_10032025/2004-2023_ceap_species_list.csv") , path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC")))
#' @export
tall_lpi_qc <- function(cleaned_tall_lpi, speciescode, USDA_plants, tblLPIDetail, path_qc){
  # list two letter codes and compare to terradat
  tall_lpi <- cleaned_tall_lpi
  #get two letter codes
  two_letter <- tall_lpi[nchar(tall_lpi$code) <= 2, ]

  # only keep the unique codes
  #two_letter <- two_letter[!duplicated(two_letter$code),]

  # this is the list currently used in terradactyl for two letter codes
  terra_two_letter <- c("L","HL", "AM", "DN", "ER", "HT", "NL","AL","DS","D","LC","M","WL", "CY","EL",
                        "W","WA","RF","R","GR","ST","CB","BY","VL","AG","CM","LM","FG","PC",
                        "BR","S", "OM")
  # determine whether the tall lpi code is associated with the terradactyl two letter codes, if not provide feedback
  two_letter$tl_error <- ifelse(two_letter$code %in% terra_two_letter, 0, 1)
  two_letter$Notes <- ifelse(two_letter$tl_error == 1,
                             "Two letter codes present that are not associated with terradactyl codes", NA)
  two_letter$Action = ifelse(two_letter$tl_error == 1, "Check with project manager to determine what code represents", NA)
  two_letter <- two_letter |> dplyr::select(PrimaryKey, DBKey, LineKey, RecKey, layer, code, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  two_letter <- two_letter[!is.na(two_letter$Notes),]



  # check same number of unique codes as og data
  # get the unique codes from the original data table
  og_codes <- c(tblLPIDetail$TopCanopy, tblLPIDetail$Lower1, tblLPIDetail$Lower2, tblLPIDetail$Lower3,
                tblLPIDetail$Lower4,tblLPIDetail$Lower5, tblLPIDetail$Lower6, tblLPIDetail$Lower7,
                tblLPIDetail$SoilSurface)
  og_codes <- unique(og_codes)

  #determine whether the tall lpi code is a code from the original data
  tall_lpi_codes <- tall_lpi

  tall_lpi_codes$add_codes <- ifelse(tall_lpi_codes$code %in% og_codes, 0, 1)

  # provide feedback where tall lpi codes are not in the original data
  tall_lpi_codes$Notes <- ifelse(tall_lpi_codes$add_codes == 1,
                                 "Codes present that are not in the original data", NA)

  tall_lpi_codes$Action <- ifelse(tall_lpi_codes$add_codes == 1,
                                  "Determine whether code addition was intentional", NA)
  tall_lpi_codes <- tall_lpi_codes |> dplyr::select(PrimaryKey, DBKey, LineKey, RecKey, layer, code, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  tall_lpi_codes <- tall_lpi_codes[!is.na(tall_lpi_codes$Notes),]




  # looking for soil surface codes that are not terradactyl accepted soil surface codes
  # get the unique two letter soil surface codes from the tall lpi
  ss <- tall_lpi |> filter(layer == "SoilSurface")
  ss <- ss[nchar(ss$code) <= 2, ]
  #ss <- ss[!duplicated(ss$code),]
  # these are the two letter surface codes used in terradactyl
  terra_two_letter_surf <- c("DS","D","LC","M", "CY", "EL",
                             "W","WA","RF","R","GR","ST","CB","BY","VL","AG","CM","LM","FG","PC",
                             "BR","S")

  # determine whether the tall lpi surface code is one of the codes from terradactyl

  ss$add_codes <- ifelse(ss$code %in% terra_two_letter_surf, 0, 1)

  # provide feedback where the tall lpi surface code is not associated with the terradactyl codes
  ss$Notes <- ifelse( ss$add_codes == 1,
                      "Soil surface codes present that are not associated with terradactyl", NA)

  ss$Action <- ifelse(ss$add_codes == 1,
                      "Check with the project manager to determine what the code represents", NA)
  ss <- ss |> dplyr::select(PrimaryKey, DBKey, LineKey, RecKey, layer, code, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  ss <- ss[!is.na(ss$Notes),]



  ## identifying where the tall lpi codes are not a USDA plant code
  #get the accepted USDA plant codes
  USDA_plant_codes <- USDA_plants[,paste0(speciescode)]


  # checking that the tall_lpi codes are in the USDA database
  tall_lpi_plant_codes <- tall_lpi[nchar(tall_lpi$code) > 2, ]


  tall_lpi_plant_codes$usda_code <- ifelse(tall_lpi_plant_codes$code %in% USDA_plant_codes, 0, 1)

  # providing feedback for the tall lpi codes that are not in the USDA plant code list
  tall_lpi_plant_codes$Notes <- ifelse( tall_lpi_plant_codes$usda_code == 1,
                                        "Codes present that are not an accepted USDA plant code", NA)
  tall_lpi_plant_codes$Action <- ifelse(tall_lpi_plant_codes$usda_code ==1,
                                        "If not unknown code, confirm with project manager the correct USDA plant code or species attributes", NA)
  tall_lpi_plant_codes <- tall_lpi_plant_codes |> dplyr::select(PrimaryKey, DBKey, LineKey, RecKey, layer, code, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  tall_lpi_plant_codes <- tall_lpi_plant_codes[!is.na(tall_lpi_plant_codes$Notes),]


  # joining the errors for the tall lpi data

  tall_lpi_code_check <-  rbind(two_letter, tall_lpi_codes) %>%
    rbind(., ss) %>% rbind(., tall_lpi_plant_codes)

  # exporting to the QC folder
  write.csv(tall_lpi_code_check, file.path(path_qc, "tall_lpi_code_check.csv"), row.names = FALSE)

  select_me <- c("PrimaryKey", "LineKey", "RecKey","TopCanopy", "SoilSurface")
  og_layers <- tblLPIDetail |> dplyr:: select( all_of(select_me), contains("Lower") & !contains("Chk")& !contains("Height")& !contains("Species"))
  og_layers <- gather(og_layers, layer, code, -PrimaryKey, -LineKey, -RecKey)
  og_layers <- og_layers |> dplyr::filter(code != "None", !is.na(code))

  tall_lpi_layer_codes <- tall_lpi |> dplyr::select(PrimaryKey, LineKey, RecKey,layer, code)
  tall_lpi_layer_codes$LineKey <- as.numeric(tall_lpi_layer_codes$LineKey)
  missing_in_tall_lpi <- dplyr::setdiff(og_layers, tall_lpi_layer_codes)
  missing_in_tall_lpi <- as.data.frame(missing_in_tall_lpi)
  if(nrow(missing_in_tall_lpi) > 0){
    missing_in_tall_lpi$Notes <- "The specific hit (layer and code) in tall lpi does not match the original data"
    missing_in_tall_lpi$Action <- "Determine why gather or cleaning is changing the original data"

  }

  missing_in_og <- dplyr::setdiff(tall_lpi_layer_codes, og_layers)
  missing_in_og <- as.data.frame(missing_in_og)
  if(nrow(missing_in_og) > 0){
    missing_in_og$Notes <- "The specific hit (layer and code) in original data does not match or is missing from the tall lpi data"
    missing_in_og$Action <- "Determine why gather or cleaning is changing the tall data"

  }

  if(length(missing_in_og) ==  length(missing_in_tall_lpi)){
    missing_layer_codes <- rbind(missing_in_tall_lpi, missing_in_og)
  }
  if(length(missing_in_og) >  length(missing_in_tall_lpi)){
    missing_layer_codes <- missing_in_og
  }

  if(length(missing_in_og) <  length(missing_in_tall_lpi)){
    missing_layer_codes <- missing_in_tall_lpi
  }

  missing_layer_codes <- as.data.frame(missing_layer_codes)

  if(nrow(missing_layer_codes) > 0){
    missing_layer_codes <- missing_layer_codes |> filter_all(any_vars(duplicated(.)))
  }

  write.csv(missing_layer_codes, file.path(path_qc, "differing_layer_codes_check.csv"), row.names = F)

}
############################################

#####################################
#' Tall Gap QC
#'
#' produces QC information using the tall_gap file in the format for running terradactylutils2::geofiles() created using terradactylutils2::clean_tall_gap()
#'
#' @param cleaned_tall_gap the tall_gap file that has been through terradactylutils2::clean_tall_gap()
#' @param tblGapDetail the tblGapDetail file from DIMA tables
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with QC information about that tall_gap file saved to the QC folder specified
#'
#' @examples gap_qc(cleaned_tall_gap = cleaned_tall_gap, tblGapDetail = tblGapDetail, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
#' @export
tall_gap_qc <- function(cleaned_tall_gap, tblGapDetail, path_qc){
  tall_gap <- cleaned_tall_gap
  # function(tblGapDetail, tall_gap)
  ### gap QC
  # checking that the tall and og GapStart data match
  tall_gap_start <- tall_gap |> dplyr::select(PrimaryKey, LineKey, RecKey,GapStart)
  og_gap_start <- tblGapDetail |> dplyr::select(PrimaryKey, LineKey, RecKey,GapStart)

  tall_gap_start_differ <- dplyr::setdiff(og_gap_start, tall_gap_start)
  if(nrow(tall_gap_start_differ) > 0){
    tall_gap_start_differ$Notes <- "There is a GapStart in the tall data that differs from the original data"
    tall_gap_start_differ$Action <- "Determine why gather or clean functions are altering the original GapStart"

  }

  og_gap_start_differ <- dplyr::setdiff(tall_gap_start, og_gap_start)
  if(nrow(og_gap_start_differ) > 0){
    og_gap_start_differ$Notes <- "There is a GapStart in the original data that differs from the tall tables"
    og_gap_start_differ$Action <- "Determine why gather or clean functions are altering the tall GapStart"

  }


  gap_start_errors <- rbind(tall_gap_start_differ, og_gap_start_differ)

  if(nrow(gap_start_errors) > 0){
    gap_start_errors <- gap_start_errors |> filter_all(any_vars(duplicated(.)))
  }


  # checking the GapStart is not NA
  no_start <- tall_gap_start[is.na(tall_gap_start$GapStart),] #
  if(nrow(no_start) > 0){
    no_start$Notes <- "The GapStart for the line is NA"
    no_start$Action <- "Work with project manager to determine whether line needs removed"
  }
  gap_start_errors <- rbind(gap_start_errors, no_start)

  write.csv(gap_start_errors, file.path(path_qc, "GapStart_check.csv"), row.names = F)

  # checking max and min
  tall_gap_gaps <- tall_gap |> dplyr::select(PrimaryKey, LineKey, RecKey,Gap)
  og_gap_gaps <- tblGapDetail |> dplyr::select(PrimaryKey, LineKey, RecKey,Gap)

  max_tall_gap <- slice_max(tall_gap_gaps, Gap, by = c('PrimaryKey', 'LineKey','RecKey'))
  max_og_gap <- slice_max(og_gap_gaps, Gap, by = c('PrimaryKey', 'LineKey','RecKey'))


  max_gap_error_tall <- dplyr::setdiff(max_og_gap, max_tall_gap)
  if(nrow(max_gap_error_tall) > 0){
    max_gap_error_tall$Notes <- "There is a Gap in the tall data that differs from the original data"
    max_gap_error_tall$Action <- "Determine why gather or clean functions are altering the original Gap"

  }

  max_gap_error_og <- dplyr::setdiff(max_tall_gap, max_og_gap)
  if(nrow(max_gap_error_og) > 0){
    max_gap_error_og$Notes <- "There is a Gap in the original data that differs from the tall tables"
    max_gap_error_og$Action <- "Determine why gather or clean functions are altering the tall Gap"

  }


  max_gap_errors <- rbind(max_gap_error_tall, max_gap_error_og)

  if(nrow(max_gap_errors) > 0){
    max_gap_errors <- max_gap_errors |> filter_all(any_vars(duplicated(.)))
  }




  min_tall_gap <- slice_min(tall_gap_gaps, Gap, by = c('PrimaryKey', 'LineKey','RecKey'))
  min_og_gap <- slice_min(og_gap_gaps, Gap, by = c('PrimaryKey', 'LineKey','RecKey'))


  min_gap_error_tall <- dplyr::setdiff(min_og_gap, min_tall_gap)
  if(nrow(min_gap_error_tall) > 0){
    min_gap_error_tall$Notes <- "There is a Gap in the tall data that differs from the original data"
    min_gap_error_tall$Action <- "Determine why gather or clean functions are altering the original Gap"

  }

  min_gap_error_og <- dplyr::setdiff(min_tall_gap, min_og_gap)
  if(nrow(min_gap_error_og) > 0){
    min_gap_error_og$Notes <- "There is a Gap in the original data that differs from the tall tables"
    min_gap_error_og$Action <- "Determine why gather or clean functions are altering the tall Gap"

  }


  min_gap_errors <- rbind(min_gap_error_tall, min_gap_error_og)

  if(nrow(min_gap_errors) > 0){
    min_gap_errors <- min_gap_errors |> filter_all(any_vars(duplicated(.)))
  }


  gap_errors <- rbind(max_gap_errors, min_gap_errors)

  ## checking for negatives or NAs
  neg_gap <- tall_gap_gaps |> filter(Gap < 0)
  if(nrow(neg_gap) > 0){
    neg_gap$Notes <- "There are negative gaps present"
    neg_gap$Action <- "Determine if the gap should be positive or work with project manager to determine whether line needs removed"
  }
  gap_errors <- rbind(gap_errors, neg_gap)

  write.csv(gap_errors, file.path(path_qc, "Gap_check.csv"), row.names = F)

  # GapEnd errors
  tall_gap_end <- tall_gap |> dplyr::select(PrimaryKey, LineKey, RecKey,GapEnd)

  no_end <- tall_gap_end[is.na(tall_gap_end$GapEnd),]

  if(nrow(no_end) > 0){
    no_end$Notes <- "The GapEnd is NA"
    no_end$Action <- "Work with project manager to determine whether line needs removed"
  }

  write.csv(no_end, file.path(path_qc, "GapEnd_check.csv"), row.names = F)

}
#####################################



##################################
#' Tall Soil Stability QC
#'
#'produces QC information using the tall_soil_stability file prepared for terradactylutils2::geofiles() using terradactylutils2::clean_tall_soil_stability()
#'
#' @param tblSoilStabDetail tblSoilStabDetail from the DIMA tables
#' @param cleaned_tall_soil_stability tall_soil_stability created using terradactylutils2::clean_tall_soil_stability()
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with information about the tall soil stability QC
#'
#' @examples soil_stability_qc(tblSoilStabDetail = tblSoilStabDetail, cleaned_tall_soil_stability = cleaned_tall_soil_stability, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
#' @export
tall_soil_stability_qc <- function(tblSoilStabDetail, cleaned_tall_soil_stability, path_qc){
  tall_soil_stability <- cleaned_tall_soil_stability
  # SS rating errors
  ss_og_rating <- tblSoilStabDetail |> dplyr::select(contains("Rating"),  RecKey) |>
    gather("Position", "Rating"  , -RecKey)
  ss_og_rating$Position <- gsub("^.{0,6}", "", ss_og_rating$Position)


  ss_og_rating_2 <- tblSoilStabDetail |> dplyr::select(contains("Pos"), RecKey) |>
    gather("Position", "Pos"  , -RecKey)
  ss_og_rating_2$Position <- gsub("^.{0,3}", "", ss_og_rating_2$Position)

  ss_og_rating <- merge(ss_og_rating, ss_og_rating_2)

  ss_og_rating <- ss_og_rating[!is.na(ss_og_rating$Rating),]
  ss_og_rating <- ss_og_rating[!is.na(ss_og_rating$Pos),]

  ss_tall_rating <- tall_soil_stability |> dplyr::select(RecKey, Pos, Rating, Position)

  # checking max and min



  max_tall_rating <- slice_max(ss_tall_rating, Rating, by = c('Position', 'Pos','RecKey'))
  max_og_rating <- slice_max(ss_og_rating, Rating, by = c('Position', 'Pos','RecKey'))

  max_tall_rating$Position <- as.character(max_tall_rating$Position)
  max_og_rating$Position <- as.character(max_og_rating$Position)
  max_og_rating$Pos <- as.character(max_og_rating$Pos)
  max_tall_rating$Pos <- as.character(max_tall_rating$Pos)

  max_rating_error_tall <- dplyr::setdiff(max_og_rating, max_tall_rating)
  if(nrow(max_rating_error_tall) > 0){
    max_rating_error_tall$Notes <- "There is a rating in the tall data that differs from the original data"
    max_rating_error_tall$Action <- "Determine why gather or clean functions are altering the original rating"

  }

  max_rating_error_og <- dplyr::setdiff(max_tall_rating, max_og_rating)
  if(nrow(max_rating_error_og) > 0){
    max_rating_error_og$Notes <- "There is a rating in the original data that differs from the tall tables"
    max_rating_error_og$Action <- "Determine why gather or clean functions are altering the tall rating"

  }


  max_rating_errors <- rbind(max_rating_error_tall, max_rating_error_og)

  if(nrow(max_rating_errors) > 0){
    max_rating_errors <- max_rating_errors |> filter_all(any_vars(duplicated(.)))
  }




  min_tall_rating <- slice_min(ss_tall_rating, Rating, by = c('Position', 'Pos','RecKey'))
  min_og_rating <- slice_min(ss_og_rating, Rating, by = c('Position', 'Pos','RecKey'))

  min_tall_rating$Position <- as.character(min_tall_rating$Position)
  min_og_rating$Position <- as.character(min_og_rating$Position)
  min_og_rating$Pos <- as.character(min_og_rating$Pos)

  min_tall_rating$Pos <- as.character(min_tall_rating$Pos)

  min_rating_error_tall <- dplyr::setdiff(min_og_rating, min_tall_rating)
  if(nrow(min_rating_error_tall) > 0){
    min_rating_error_tall$Notes <- "There is a rating in the tall data that differs from the original data"
    min_rating_error_tall$Action <- "Determine why gather or clean functions are altering the original rating"

  }

  min_rating_error_og <- dplyr::setdiff(min_tall_rating, min_og_rating)
  if(nrow(min_rating_error_og) > 0){
    min_rating_error_og$Notes <- "There is a rating in the original data that differs from the tall tables"
    min_rating_error_og$Action <- "Determine why gather or clean functions are altering the tall rating"

  }


  min_rating_errors <- rbind(min_rating_error_tall, min_rating_error_og)

  if(nrow(min_rating_errors) > 0){
    min_rating_errors <- min_rating_errors |> filter_all(any_vars(duplicated(.)))
  }


  rating_errors <- rbind(max_rating_errors, min_rating_errors)




  # SS shouldn't be more than 6 in raw and calcd

  ss_raw_six <- ss_og_rating |> filter(Rating >6)
  if(nrow(ss_raw_six) > 0){
    ss_raw_six$Notes <- "There is a rating in the original soil stability data that is greater than 6"
    ss_raw_six$Action <- "Work with the project manager to determine if the rating should be removed"

  }


  ss_calcd_six <- ss_tall_rating |> filter(Rating > 6)
  if(nrow(ss_calcd_six) > 0){
    ss_calcd_six$Notes <- "There is a rating in the tall soil stability data that is greater than 6"
    ss_calcd_six$Action <- "Work with the project manager to determine if the rating should be removed"

  }

  ss_six <- rbind(ss_raw_six, ss_calcd_six)

  if(nrow(ss_six) > 0){
    ss_six <- ss_six |> filter_all(any_vars(duplicated(.)))
  }



  ss_rating_errors <- rbind(rating_errors, ss_six)

  # write CSV
  write.csv(ss_rating_errors,   file.path(path_qc, "soil_stability_rating_check.csv"), row.names = F)


  # veg cover classes
  ss_og_veg <- tblSoilStabDetail |> dplyr::select(contains("Veg"),  RecKey) |>
    gather("Position", "Veg"  , -RecKey)
  ss_og_veg$Position <- gsub("^.{0,3}", "", ss_og_veg$Position)


  ss_og_veg_2 <- tblSoilStabDetail |> dplyr::select(contains("Pos"), RecKey) |>
    gather("Position", "Pos"  , -RecKey)
  ss_og_veg_2$Position <- gsub("^.{0,3}", "", ss_og_veg_2$Position)

  ss_og_veg <- merge(ss_og_veg, ss_og_veg_2)

  ss_og_veg <- ss_og_veg[!is.na(ss_og_veg$Veg),]
  ss_og_veg <- ss_og_veg[!is.na(ss_og_veg$Pos),]

  ss_tall_veg <- tall_soil_stability |> dplyr::select(RecKey, Pos, Veg, Position)


  ss_og_veg$Position <- as.character(ss_og_veg$Position)
  ss_tall_veg$Position <- as.character(ss_tall_veg$Position)
  ss_og_veg$Pos <- as.character(ss_og_veg$Pos)
  ss_tall_veg$Pos <- as.character(ss_tall_veg$Pos)

  veg_error_tall <- dplyr::setdiff(ss_og_veg, ss_tall_veg)
  if(nrow(veg_error_tall) > 0){
    veg_error_tall$Notes <- "There is a Veg record in the tall data that differs from the original data"
    veg_error_tall$Action <- "Determine why gather or clean functions are altering the original Veg"

  }

  veg_error_og <- dplyr::setdiff(ss_tall_veg, ss_og_veg)
  if(nrow(veg_error_og) > 0){
    veg_error_og$Notes <- "There is a Veg record in the original data that differs from the tall tables"
    veg_error_og$Action <- "Determine why gather or clean functions are altering the tall Veg"

  }


  veg_errors <- rbind(veg_error_tall, veg_error_og)

  if(nrow(veg_errors) > 0){
    veg_errors <-veg_errors |> filter_all(any_vars(duplicated(.)))
  }

  write.csv(veg_errors, file.path(path_qc, "soil_stability_Veg_check.csv"), row.names = F)


}
########################################


##################################
#' Tall Height QC
#'
#' produces QC information using the tall_height file produced from terradactylutils2::clean_tall_height()
#'
#' @param tblLPIDetail as a data.frame, tblLPIDetail from the DIMATables
#' @param cleaned_tall_height as a data.frame, the tall_height file produced from terradactylutils2::clean_tall_height()
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV file with QC information about the height data saved to the specified path_qc
#'
#' @examples height_qc(tblLPIDetail = tblLPIDetail, cleaned_tall_height = cleaned_tall_height, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
#' @export
tall_height_qc <- function(tblLPIDetail, cleaned_tall_height, path_qc){
  ### HGT QC
  # checking heights are the same in the original and tall data
  tall_height <- cleaned_tall_height
  heights_og <- tblLPIDetail |> dplyr::select(PrimaryKey, LineKey, PointNbr, HeightWoody, HeightHerbaceous)

  heights_og <- gather(heights_og, "type", "Height", -PrimaryKey, -LineKey,-PointNbr)
  heights_og$type <- gsub("^.{0,6}", "", heights_og$type)

  heights_og$type <- tolower(heights_og$type)

  heights_og <- heights_og[!is.na(heights_og$Height),]
  tall_height_max <- tall_height |> dplyr::select(PrimaryKey, LineKey, PointNbr, type, Height)

  max_tall_Height <- slice_max(tall_height_max, Height, by = c('PrimaryKey', 'LineKey','PointNbr', "type"))
  max_og_Height <- slice_max(heights_og, Height, by = c('PrimaryKey', 'LineKey','PointNbr', "type"))


  max_Height_error_tall <- dplyr::setdiff(max_og_Height, max_tall_Height)
  if(nrow(max_Height_error_tall) > 0){
    max_Height_error_tall$Notes <- "There is a max Height in the tall data that differs from the original data"
    max_Height_error_tall$Action <- "Determine why gather or clean functions are altering the original Height"

  }

  max_Height_error_og <- dplyr::setdiff(max_tall_Height, max_og_Height)
  if(nrow(max_Height_error_og) > 0){
    max_Height_error_og$Notes <- "There is a max Height in the original data that differs from the tall tables"
    max_Height_error_og$Action <- "Determine why gather or clean functions are altering the tall Height"

  }


  max_Height_errors <- rbind(max_Height_error_tall, max_Height_error_og)

  if(nrow(max_Height_errors) > 0){
    max_Height_errors <- max_Height_errors |> filter_all(any_vars(duplicated(.)))
  }




  min_tall_Height <- slice_min(tall_height_max, Height, by = c('PrimaryKey', 'LineKey','PointNbr', "type"))
  min_og_Height <- slice_min(heights_og, Height, by = c('PrimaryKey', 'LineKey','PointNbr', "type"))

  min_Height_error_tall <- dplyr::setdiff(min_og_Height, min_tall_Height)
  if(nrow(min_Height_error_tall) > 0){
    min_Height_error_tall$Notes <- "There is a min Height in the tall data that differs from the original data"
    min_Height_error_tall$Action <- "Determine why gather or clean functions are altering the original Height"

  }

  min_Height_error_og <- dplyr::setdiff(min_tall_Height, min_og_Height)
  if(nrow(min_Height_error_og) > 0){
    min_Height_error_og$Notes <- "There is a min Height in the original data that differs from the tall tables"
    min_Height_error_og$Action <- "Determine why gather or clean functions are altering the tall Height"

  }


  min_Height_errors <- rbind(min_Height_error_tall, min_Height_error_og)

  if(nrow(min_Height_errors) > 0){
    min_Height_errors <- min_Height_errors |> filter_all(any_vars(duplicated(.)))
  }


  Height_errors <- rbind(max_Height_errors, min_Height_errors)

  write.csv(Height_errors, file.path(path_qc, "Height_check.csv"), row.names = F)

}
#########################################




####################################
#' NRI Tall LPI QC
#'
#'after the lpi tall table is prepared for terradactylutils2::geofiles() using terradactylutils2::clean_tall_lpi(), this function produces the tall_lpi file QC checks
#'
#' @param tall_lpi as a data.frame, the tall lpi data that has been gathered
#' @param speciescode the column name in the USDA plant list file that contains the four letter codes
#' @param USDA_plants a data.frame of the USDA plants with the 4 letter code, GrowthHabit and Duration
#' @param PINTERCEPT as a data.frame, the PINTERCEPT from the DIMA tables
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV containing information about the QC of the tall lpi data saved to the designated QC file
#'
#' @examples tall_lpi_qc(cleaned_tall_lpi = cleaned_tall_lpi, speciescode = "UpdatedSpeciesCode", PINTERCEPT = PINTERCEPT, USDA_plants = read.csv("D:/modifying_data_prep_script_10032025/2004-2023_ceap_species_list.csv") , path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC")))
#' @export
tall_lpi_qc_nri <- function(tall_lpi, speciescode, USDA_plants, PINTERCEPT, path_qc){
  # list two letter codes and compare to terradat
  tall_lpi_2l <- tall_lpi[!is.na(tall_lpi$code),]
  #get two letter codes
  two_letter <- tall_lpi_2l[nchar(tall_lpi_2l$code) <= 2, ]

  # only keep the unique codes
  #two_letter <- two_letter[!duplicated(two_letter$code),]

  # this is the list currently used in terradactyl for two letter codes
  terra_two_letter <- c("L","HL", "AM", "DN", "ER", "HT", "NL","AL","DS","D","LC","M","WL", "CY","EL",
                        "W","WA","RF","R","GR","ST","CB","BY","VL","AG","CM","LM","FG","PC",
                        "BR","S", "OM")
  # determine whether the tall lpi code is associated with the terradactyl two letter codes, if not provide feedback
  two_letter$tl_error <- ifelse(two_letter$code %in% terra_two_letter, 0, 1)
  two_letter$Notes <- ifelse(two_letter$tl_error == 1,
                             "Two letter codes present that are not associated with terradactyl codes", NA)
  two_letter$Action = ifelse(two_letter$tl_error == 1, "Check with project manager to determine what code represents", NA)
  two_letter <- two_letter |> dplyr::select(PrimaryKey, LineKey, layer, code, PointNbr, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  two_letter <- two_letter[!is.na(two_letter$Notes),]



  # check same number of unique codes as og data
  # get the unique codes from the original data table
  PINTERCEPT$BASAL <- ifelse(!is.na(PINTERCEPT$NONSOIL)  ,
                             PINTERCEPT$NONSOIL,
                             ifelse(PINTERCEPT$BASAL == "None", "S", PINTERCEPT$BASAL))

  og_codes <- c(PINTERCEPT$HIT1, PINTERCEPT$HIT2, PINTERCEPT$HIT3, PINTERCEPT$HIT4,
                PINTERCEPT$HIT5,PINTERCEPT$HIT6, PINTERCEPT$BASAL)
  og_codes <- unique(og_codes)

  #determine whether the tall lpi code is a code from the original data
  tall_lpi_codes <- tall_lpi

  tall_lpi_codes$add_codes <- ifelse(tall_lpi_codes$code %in% og_codes, 0, 1)

  # provide feedback where tall lpi codes are not in the original data
  tall_lpi_codes$Notes <- ifelse(tall_lpi_codes$add_codes == 1,
                                 "Codes present that are not in the original data", NA)

  tall_lpi_codes$Action <- ifelse(tall_lpi_codes$add_codes == 1,
                                  "Determine whether code addition was intentional", NA)
  tall_lpi_codes <- tall_lpi_codes |> dplyr::select(PrimaryKey, LineKey, layer, code, PointNbr, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  tall_lpi_codes <- tall_lpi_codes[!is.na(tall_lpi_codes$Notes),]




  # looking for soil surface codes that are not terradactyl accepted soil surface codes
  # get the unique two letter soil surface codes from the tall lpi
  ss <- tall_lpi_2l |> filter(layer == "SoilSurface")
  ss <- ss[nchar(ss$code) <= 2, ]
  #ss <- ss[!duplicated(ss$code),]
  # these are the two letter surface codes used in terradactyl
  terra_two_letter_surf <- c("DS","D","LC","M", "CY", "EL",
                             "W","WA","RF","R","GR","ST","CB","BY","VL","AG","CM","LM","FG","PC",
                             "BR","S")

  # determine whether the tall lpi surface code is one of the codes from terradactyl

  ss$add_codes <- ifelse(ss$code %in% terra_two_letter_surf, 0, 1)

  # provide feedback where the tall lpi surface code is not associated with the terradactyl codes
  ss$Notes <- ifelse( ss$add_codes == 1,
                      "Soil surface codes present that are not associated with terradactyl", NA)

  ss$Action <- ifelse(ss$add_codes == 1,
                      "Check with the project manager to determine what the code represents", NA)
  ss <- ss |> dplyr::select(PrimaryKey, LineKey, layer, code, PointNbr, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  ss <- ss[!is.na(ss$Notes),]



  ## identifying where the tall lpi codes are not a USDA plant code
  #get the accepted USDA plant codes
  USDA_plant_codes <- USDA_plants[,paste0(speciescode)]


  # checking that the tall_lpi codes are in the USDA database
  tall_lpi_plant_codes <- tall_lpi[nchar(tall_lpi$code) > 2, ]


  tall_lpi_plant_codes$usda_code <- ifelse(tall_lpi_plant_codes$code %in% USDA_plant_codes, 0, 1)

  # providing feedback for the tall lpi codes that are not in the USDA plant code list
  tall_lpi_plant_codes$Notes <- ifelse( tall_lpi_plant_codes$usda_code == 1,
                                        "Codes present that are not an accepted USDA plant code", NA)
  tall_lpi_plant_codes$Action <- ifelse(tall_lpi_plant_codes$usda_code ==1,
                                        "If not unknown code, confirm with project manager the correct USDA plant code or species attributes", NA)
  tall_lpi_plant_codes <- tall_lpi_plant_codes |> dplyr::select(PrimaryKey, LineKey, layer, code, PointNbr, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  tall_lpi_plant_codes <- tall_lpi_plant_codes[!is.na(tall_lpi_plant_codes$Notes),]


  # joining the errors for the tall lpi data

  tall_lpi_code_check <-  rbind(two_letter, tall_lpi_codes) %>%
    rbind(., ss) %>% rbind(., tall_lpi_plant_codes)

  # exporting to the QC folder
  write.csv(tall_lpi_code_check, file.path(path_qc, "tall_lpi_code_check.csv"), row.names = FALSE)

  select_me <- c("PrimaryKey", "BASAL", "MARK", "TRANSECT")
  og_layers <- PINTERCEPT |> dplyr:: select( all_of(select_me), contains("HIT") & !contains("Chk")& !contains("Height")& !contains("Species"))
  colnames(og_layers)[colnames(og_layers) == "HIT1"] <- "TopCanopy"
  colnames(og_layers)[colnames(og_layers) == "HIT2"] <- "Lower1"
  colnames(og_layers)[colnames(og_layers) == "HIT3"] <- "Lower2"
  colnames(og_layers)[colnames(og_layers) == "HIT4"] <- "Lower3"
  colnames(og_layers)[colnames(og_layers) == "HIT5"] <- "Lower4"
  colnames(og_layers)[colnames(og_layers) == "HIT6"] <- "Lower5"
  colnames(og_layers)[colnames(og_layers) == "BASAL"] <- "SoilSurface"

  #og_layers <- og_layers[og_layers$MARK != 75, ]

  colnames(og_layers)[colnames(og_layers) == "MARK"] <- "PointNbr"
  colnames(og_layers)[colnames(og_layers) == "TRANSECT"] <- "LineKey"

  og_layers <- gather(og_layers, layer, code, -PrimaryKey, -PointNbr, -LineKey)
  og_layers <- og_layers |> dplyr::filter(code != "None", !is.na(code))

  tall_lpi_layer_codes <- tall_lpi |> dplyr::select(PrimaryKey, layer, code, PointNbr, LineKey)
  og_layers$layer <- as.character(og_layers$layer)
  tall_lpi_layer_codes$layer <- as.character(tall_lpi_layer_codes$layer)
  og_layers <- og_layers |>
    mutate(across(where(is.character), trimws))

  tall_lpi_layer_codes <- tall_lpi_layer_codes |>
    mutate(across(where(is.character), trimws))

  missing_in_tall_lpi <- dplyr::setdiff(og_layers, tall_lpi_layer_codes)
  missing_in_tall_lpi <- as.data.frame(missing_in_tall_lpi)
  if(nrow(missing_in_tall_lpi) > 0){
    missing_in_tall_lpi$Notes <- "The specific hit (layer and code) in tall lpi does not match the original data"
    missing_in_tall_lpi$Action <- "Determine why gather or cleaning is changing the original data"

  }

  missing_in_og <- dplyr::setdiff(tall_lpi_layer_codes, og_layers)
  missing_in_og <- as.data.frame(missing_in_og)
  missing_in_og <- missing_in_og[!is.na(missing_in_og$code),]
  if(nrow(missing_in_og) > 0){
    missing_in_og$Notes <- "The specific hit (layer and code) in original data does not match or is missing from the tall lpi data"
    missing_in_og$Action <- "Determine why gather or cleaning is changing the tall data"

  }

  if(length(missing_in_og) ==  length(missing_in_tall_lpi)){
    missing_layer_codes <- rbind(missing_in_tall_lpi, missing_in_og)
  }
  if(length(missing_in_og) >  length(missing_in_tall_lpi)){
    missing_layer_codes <- missing_in_og
  }

  if(length(missing_in_og) <  length(missing_in_tall_lpi)){
    missing_layer_codes <- missing_in_tall_lpi
  }

  missing_layer_codes <- as.data.frame(missing_layer_codes)

  if(nrow(missing_layer_codes) > 0){
    missing_layer_codes <- missing_layer_codes |> filter_all(any_vars(duplicated(.)))
  }
  write.csv(missing_layer_codes, file.path(path_qc, "differing_layer_codes_check.csv"), row.names = F)

}
############################################



#####################################
#' Tall NRI Gap QC
#'
#' produces QC information using the tall_gap file that has been gathered
#'
#' @param tall_gap the tall_gap file that has been through terradactylutils2::clean_tall_gap()
#' @param GINTERCEPT the gap file from NRI tables
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with QC information about that tall_gap file saved to the QC folder specified
#'
#' @examples gap_qc(cleaned_tall_gap = cleaned_tall_gap, GINTERCEPT = GINTERCEPT, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
#' @export
tall_gap_qc_nri <- function(tall_gap, GINTERCEPT, path_qc){
  # function(GINTERCEPT, tall_gap)
  ### gap QC
  # checking that the tall and og GapStart data match
  GINTERCEPT$GapStart <- GINTERCEPT$START_GAP * 2.54 * 12
  GINTERCEPT$GapEnd <- GINTERCEPT$END_GAP * 2.54 * 12
  GINTERCEPT$Gap <- abs(GINTERCEPT$GapStart - GINTERCEPT$GapEnd)

GINTERCEPT <- GINTERCEPT[GINTERCEPT$SEQNUM != 75,]
GINTERCEPT$SeqNo <- GINTERCEPT$SEQNUM
GINTERCEPT$RecType <- ifelse(GINTERCEPT$GAP_TYPE == "basal", "B",
                             ifelse(GINTERCEPT$GAP_TYPE == "canopy", "C", "P"))

GINTERCEPT$LineKey <- GINTERCEPT$TRANSECT

tall_gap_start <- tall_gap |> dplyr::select(PrimaryKey,  GapStart, LineKey, RecType, SeqNo)
og_gap_start <- GINTERCEPT |> dplyr::select(PrimaryKey,  GapStart, LineKey, RecType, SeqNo)

og_gap_clean <- og_gap_start |> dplyr::mutate(across(where(is.character), trimws))
tall_gap_clean <- tall_gap_start |> dplyr::mutate(across(where(is.character), trimws))

og_gap_start$GapStart <- round(og_gap_start$GapStart, 2)
tall_gap_start$GapStart <- round(tall_gap_start$GapStart, 2)

  tall_gap_start_differ <- dplyr::setdiff(og_gap_start, tall_gap_start)
  if(nrow(tall_gap_start_differ) > 0){
    tall_gap_start_differ$Notes <- "There is a GapStart in the tall data that differs from the original data"
    tall_gap_start_differ$Action <- "Determine why gather or clean functions are altering the original GapStart"

  }

  og_gap_start_differ <- dplyr::setdiff(tall_gap_start, og_gap_start)
  if(nrow(og_gap_start_differ) > 0){
    og_gap_start_differ$Notes <- "There is a GapStart in the original data that differs from the tall tables"
    og_gap_start_differ$Action <- "Determine why gather or clean functions are altering the tall GapStart"

  }


  gap_start_errors <- rbind(tall_gap_start_differ, og_gap_start_differ)

  if(nrow(gap_start_errors) > 0){
    gap_start_errors <- gap_start_errors |> filter_all(any_vars(duplicated(.)))
  }


  # checking the GapStart is not NA
  no_start <- tall_gap_start[is.na(tall_gap_start$GapStart),] #
  if(nrow(no_start) > 0){
    no_start$Notes <- "The GapStart for the line is NA"
    no_start$Action <- "Work with project manager to determine whether line needs removed"
  }
  gap_start_errors <- rbind(gap_start_errors, no_start)

  write.csv(gap_start_errors, file.path(path_qc, "GapStart_check.csv"), row.names = F)

  # checking max and min
  tall_gap_gaps <- tall_gap |> dplyr::select(PrimaryKey,  Gap, RecType, SeqNo)
  og_gap_gaps <- GINTERCEPT |> dplyr::select(PrimaryKey,  Gap, RecType, SeqNo)

  tall_gap_gaps$Gap <- round(tall_gap_gaps$Gap, 2)
  og_gap_gaps$Gap <- round(og_gap_gaps$Gap, 2)

  max_tall_gap <- slice_max(tall_gap_gaps, Gap, by = c('PrimaryKey', 'RecType'), n = 1)
  max_og_gap <- slice_max(og_gap_gaps, Gap, by = c('PrimaryKey', 'RecType'), n = 1)


  max_gap_error_tall <- dplyr::setdiff(max_og_gap, max_tall_gap)
  if(nrow(max_gap_error_tall) > 0){
    max_gap_error_tall$Notes <- "There is a Gap in the tall data that differs from the original data"
    max_gap_error_tall$Action <- "Determine why gather or clean functions are altering the original Gap"

  }

  max_gap_error_og <- dplyr::setdiff(max_tall_gap, max_og_gap)
  if(nrow(max_gap_error_og) > 0){
    max_gap_error_og$Notes <- "There is a Gap in the original data that differs from the tall tables"
    max_gap_error_og$Action <- "Determine why gather or clean functions are altering the tall Gap"

  }


  max_gap_errors <- rbind(max_gap_error_tall, max_gap_error_og)

  if(nrow(max_gap_errors) > 0){
    max_gap_errors <- max_gap_errors |> filter_all(any_vars(duplicated(.)))
  }



  # lines are autofilled with 0 when not collected, we don't want these in the comparison
  tall_gap_gaps <- tall_gap_gaps[tall_gap_gaps$Gap != 0,]



  min_tall_gap <- slice_min(tall_gap_gaps, Gap, by = c('PrimaryKey', 'RecType'))
  min_og_gap <- slice_min(og_gap_gaps, Gap, by = c('PrimaryKey', 'RecType'))


  min_gap_error_tall <- dplyr::setdiff(min_og_gap, min_tall_gap)
  if(nrow(min_gap_error_tall) > 0){
    min_gap_error_tall$Notes <- "There is a Gap in the tall data that differs from the original data"
    min_gap_error_tall$Action <- "Determine why gather or clean functions are altering the original Gap"

  }

  min_gap_error_og <- dplyr::setdiff(min_tall_gap, min_og_gap)
  if(nrow(min_gap_error_og) > 0){
    min_gap_error_og$Notes <- "There is a Gap in the original data that differs from the tall tables"
    min_gap_error_og$Action <- "Determine why gather or clean functions are altering the tall Gap"

  }


  min_gap_errors <- rbind(min_gap_error_tall, min_gap_error_og)

  if(nrow(min_gap_errors) > 0){
    min_gap_errors <- min_gap_errors |> filter_all(any_vars(duplicated(.)))
  }

  #
  gap_errors <- rbind(max_gap_errors, min_gap_errors)

  ## checking for negatives or NAs
  neg_gap <- tall_gap_gaps |> filter(Gap < 0)
  if(nrow(neg_gap) > 0){
    neg_gap$Notes <- "There are negative gaps present"
    neg_gap$Action <- "Determine if the gap should be positive or work with project manager to determine whether line needs removed"
  }
  gap_errors <- rbind(gap_errors, neg_gap)

  write.csv(gap_errors, file.path(path_qc, "Gap_check.csv"), row.names = F)

  # GapEnd errors
  tall_gap_end <- tall_gap |> dplyr::select(PrimaryKey, GapEnd)

  no_end <- tall_gap_end[is.na(tall_gap_end$GapEnd),]

  if(nrow(no_end) > 0){
    no_end$Notes <- "The GapEnd is NA"
    no_end$Action <- "Work with project manager to determine whether line needs removed"
  }

  write.csv(no_end, file.path(path_qc, "GapEnd_check.csv"), row.names = F)

}
#####################################



##################################
#' Tall Height QC NRI
#'
#' produces QC information using the tall_height file produced from terradactylutils2::clean_tall_height()
#'
#' @param PASTUREHEIGHTS as a data.frame, PASTUREHEIGHTS table
#' @param tall_height as a data.frame, the tall_height file produced gathering
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV file with QC information about the height data saved to the specified path_qc
#'
#' @export
tall_height_qc_nri <- function(PASTUREHEIGHTS, tall_height, path_qc){
  ### HGT QC
  # checking heights are the same in the original and tall data
  heights_og <- PASTUREHEIGHTS |> dplyr::select(PrimaryKey, HEIGHT,  TRANSECT, DISTANCE)
  colnames(heights_og)[colnames(heights_og) == "HEIGHT"] <- "Height"
  colnames(heights_og)[colnames(heights_og) == "TRANSECT"] <- "LineKey"
  colnames(heights_og)[colnames(heights_og) == "DISTANCE"] <- "PointNbr"

  heights_og <- heights_og[heights_og$PointNbr != 75,]

  heights_og$Type <- gsub(".*(.{2})$", "\\1", heights_og$Height)



  heights_og <- heights_og |>
    mutate(Height = Height |>
             stringr::str_extract("^[0-9.]+") |>
             as.numeric() * 2.54)

  heights_og$Height <- ifelse(heights_og$Type == "ft", heights_og$Height * 12, heights_og$Height)


  heights_og <- heights_og[!is.na(heights_og$Height),]
  heights_og$Type <- NULL


  heights_og_W <- PASTUREHEIGHTS |> dplyr::select(PrimaryKey, WHEIGHT,  TRANSECT, DISTANCE)
  colnames(heights_og_W)[colnames(heights_og_W) == "WHEIGHT"] <- "Height"
  colnames(heights_og_W)[colnames(heights_og_W) == "TRANSECT"] <- "LineKey"
  colnames(heights_og_W)[colnames(heights_og_W) == "DISTANCE"] <- "PointNbr"

  heights_og_W <- heights_og_W[heights_og_W$PointNbr != 75,]

  heights_og_W$Type <- gsub(".*(.{2})$", "\\1", heights_og_W$Height)



  heights_og_W <- heights_og_W |>
    mutate(Height = Height |>
             # 1. Extract the first sequence of digits and decimals
             str_extract("^[0-9.]+") |>
             # 2. Convert to numeric
             as.numeric() * 2.54)

  heights_og_W$Height <- ifelse(heights_og_W$Type == "ft", heights_og_W$Height * 12, heights_og_W$Height)


  heights_og_W <- heights_og_W[!is.na(heights_og_W$Height),]
  heights_og_W$Type <- NULL

  heights_og <- rbind(heights_og, heights_og_W)



  tall_height_max <- tall_height |> dplyr::select(PrimaryKey, LineKey, PointNbr, Height)
  tall_height_max <- tall_height_max[tall_height_max$PointNbr != 75,]

  max_tall_Height <- slice_max(tall_height_max, Height, by = c('PrimaryKey', 'LineKey'))
  max_og_Height <- slice_max(heights_og, Height, by = c('PrimaryKey', 'LineKey'))

  max_og_Height$Height <- round(max_og_Height$Height, 2)
  max_tall_Height$Height <- round(max_tall_Height$Height, 2)

  max_og_Height$PrimaryKey <- trimws(max_og_Height$PrimaryKey)
  max_tall_Height$PrimaryKey <- trimws(max_tall_Height$PrimaryKey)

  max_og_Height$LineKey <- trimws(max_og_Height$LineKey)
  max_tall_Height$LineKey <- trimws(max_tall_Height$LineKey)


  max_Height_error_tall <- dplyr::setdiff(max_og_Height, max_tall_Height)
  if(nrow(max_Height_error_tall) > 0){
    max_Height_error_tall$Notes <- "There is a max Height in the tall data that differs from the original data"
    max_Height_error_tall$Action <- "Determine why gather or clean functions are altering the original Height"

  }

  max_Height_error_og <- dplyr::setdiff(max_tall_Height, max_og_Height)
  if(nrow(max_Height_error_og) > 0){
    max_Height_error_og$Notes <- "There is a max Height in the original data that differs from the tall tables"
    max_Height_error_og$Action <- "Determine why gather or clean functions are altering the tall Height"

  }


  max_Height_errors <- rbind(max_Height_error_tall, max_Height_error_og)

  if(nrow(max_Height_errors) > 0){
    max_Height_errors <- max_Height_errors |> filter_all(any_vars(duplicated(.)))
  }



  min_tall_Height <- slice_min(tall_height_max, Height, by = c('PrimaryKey', 'LineKey'))
  min_og_Height <- slice_min(heights_og, Height, by = c('PrimaryKey', 'LineKey'))


  min_Height_error_tall <- dplyr::setdiff(min_og_Height, min_tall_Height)

  if(nrow(min_Height_error_tall) > 0){
    #removing where the issue is from height tall adding in 0
    comparison_heights <- min_Height_error_tall |>
      dplyr::select(PrimaryKey, LineKey, PointNbr) |>
      dplyr::distinct() |>
      #join og and tall with clarifying names
      dplyr::left_join(min_og_Height,
                       by = c("PrimaryKey", "LineKey", "PointNbr")) |>

      dplyr::left_join(min_tall_Height,
                       by = c("PrimaryKey", "LineKey", "PointNbr"),
                       suffix = c("_original", ""))

    comparison_heights <- comparison_heights |> dplyr::filter(Height != 0)

    min_Height_error_tall <- comparison_heights

    min_Height_error_tall$diff <- min_Height_error_tall$Height_original - min_Height_error_tall$Height

    min_Height_error_tall <- min_Height_error_tall |> dplyr::filter(diff > 1)

  }



  if(nrow(min_Height_error_tall) > 0){

    min_height_error_tall$diff <- NULL
    min_Height_error_tall$Height_original <- NULL

    min_Height_error_tall$Notes <- "There is a min Height in the tall data that differs from the original data"
    min_Height_error_tall$Action <- "Determine why gather or clean functions are altering the original Height"

  }

  min_Height_error_og <- dplyr::setdiff(min_tall_Height, min_og_Height)

  if(nrow(min_Height_error_og) > 0){
    # comparing and if diff > 1 displaying
    comparison_heights <- min_Height_error_og |>
      dplyr::select(PrimaryKey, LineKey, PointNbr) |>
      dplyr::distinct() |>
      dplyr::left_join(min_og_Height,
                       by = c("PrimaryKey", "LineKey", "PointNbr")) |>

      dplyr::left_join(min_tall_Height,
                       by = c("PrimaryKey", "LineKey", "PointNbr"),
                       suffix = c("", "_tall"))

    comparison_heights <- comparison_heights[comparison_heights$Height_tall != 0,]

    min_Height_error_og <- comparison_heights


    min_Height_error_og$diff <- min_Height_error_og$Height - min_Height_error_og$Height_tall

    min_Height_error_og <- min_Height_error_og |> dplyr::filter(diff > 1)


  }


  if(nrow(min_Height_error_og) > 0){

    min_Height_error_og$Height_tall <- NULL
    min_height_error_og$diff <- NULL

    min_Height_error_og$Notes <- "There is a min Height in the original data that differs from the tall tables"
    min_Height_error_og$Action <- "Determine why gather or clean functions are altering the tall Height"

  }


  min_Height_errors <- rbind(min_Height_error_tall, min_Height_error_og)

  if(nrow(min_Height_errors) > 0){
    min_Height_errors <- min_Height_errors |> filter_all(any_vars(duplicated(.)))
  }


  Height_errors <- rbind(max_Height_errors, min_Height_errors)

  write.csv(Height_errors, file.path(path_qc, "Height_check.csv"), row.names = F)

}
#########################################




##################################
#' Tall Soil Stability QC NRI
#'
#' produces QC information using the tall_height file produced from terradactylutils2::clean_tall_height()
#'
#' @param SOILDISAG as a data.frame, SOILDISAG table
#' @param cleaned_tall_soil_stability gathered soil stability file
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV file with QC information about the soil stability data saved to the specified path_qc
#'
#' @export
######################
tall_soil_stability_qc_nri <- function(SOILDISAG, cleaned_tall_soil_stability, path_qc){

  tall_soil_stability <- cleaned_tall_soil_stability
  # SS rating errors
  ss_og_rating <- SOILDISAG |> dplyr::select(contains("STABILITY"),  PrimaryKey) |>
    gather("Position", "Rating"  , -PrimaryKey)
  ss_og_rating$Position <- gsub("^.{0,9}", "", ss_og_rating$Position)


  ss_og_rating <- ss_og_rating[!is.na(ss_og_rating$Rating),]

  ss_tall_rating <- tall_soil_stability |> dplyr::select(PrimaryKey, Rating, Position)

  # checking max and min



  max_tall_rating <- slice_max(ss_tall_rating, Rating, by = c('Position', 'PrimaryKey'))
  max_og_rating <- slice_max(ss_og_rating, Rating, by = c('Position','PrimaryKey'))

  max_tall_rating$Position <- as.character(max_tall_rating$Position)
  max_og_rating$Position <- as.character(max_og_rating$Position)

  max_rating_error_tall <- dplyr::setdiff(max_og_rating, max_tall_rating)
  if(nrow(max_rating_error_tall) > 0){
    max_rating_error_tall$Notes <- "There is a rating in the tall data that differs from the original data"
    max_rating_error_tall$Action <- "Determine why gather or clean functions are altering the original rating"

  }

  max_rating_error_og <- dplyr::setdiff(max_tall_rating, max_og_rating)
  if(nrow(max_rating_error_og) > 0){
    max_rating_error_og$Notes <- "There is a rating in the original data that differs from the tall tables"
    max_rating_error_og$Action <- "Determine why gather or clean functions are altering the tall rating"

  }


  max_rating_errors <- rbind(max_rating_error_tall, max_rating_error_og)

  if(nrow(max_rating_errors) > 0){
    max_rating_errors <- max_rating_errors |> filter_all(any_vars(duplicated(.)))
  }




  min_tall_rating <- slice_min(ss_tall_rating, Rating, by = c('Position', 'PrimaryKey'))
  min_og_rating <- slice_min(ss_og_rating, Rating, by = c('Position', 'PrimaryKey'))

  min_tall_rating$Position <- as.character(min_tall_rating$Position)
  min_og_rating$Position <- as.character(min_og_rating$Position)

  min_rating_error_tall <- dplyr::setdiff(min_og_rating, min_tall_rating)
  if(nrow(min_rating_error_tall) > 0){
    min_rating_error_tall$Notes <- "There is a rating in the tall data that differs from the original data"
    min_rating_error_tall$Action <- "Determine why gather or clean functions are altering the original rating"

  }

  min_rating_error_og <- dplyr::setdiff(min_tall_rating, min_og_rating)
  if(nrow(min_rating_error_og) > 0){
    min_rating_error_og$Notes <- "There is a rating in the original data that differs from the tall tables"
    min_rating_error_og$Action <- "Determine why gather or clean functions are altering the tall rating"

  }


  min_rating_errors <- rbind(min_rating_error_tall, min_rating_error_og)

  if(nrow(min_rating_errors) > 0){
    min_rating_errors <- min_rating_errors |> filter_all(any_vars(duplicated(.)))
  }


  rating_errors <- rbind(max_rating_errors, min_rating_errors)




  # SS shouldn't be more than 6 in raw and calcd

  ss_raw_six <- ss_og_rating |> filter(Rating >6)
  if(nrow(ss_raw_six) > 0){
    ss_raw_six$Notes <- "There is a rating in the original soil stability data that is greater than 6"
    ss_raw_six$Action <- "Work with the project manager to determine if the rating should be removed"

  }


  ss_calcd_six <- ss_tall_rating |> filter(Rating > 6)
  if(nrow(ss_calcd_six) > 0){
    ss_calcd_six$Notes <- "There is a rating in the tall soil stability data that is greater than 6"
    ss_calcd_six$Action <- "Work with the project manager to determine if the rating should be removed"

  }

  ss_six <- rbind(ss_raw_six, ss_calcd_six)

  if(nrow(ss_six) > 0){
    ss_six <- ss_six |> filter_all(any_vars(duplicated(.)))
  }



  ss_rating_errors <- rbind(rating_errors, ss_six)

  # write CSV
  write.csv(ss_rating_errors,   file.path(path_qc, "soil_stability_rating_check.csv"), row.names = F)


  # veg cover classes
  ss_og_veg <- SOILDISAG |> dplyr::select(contains("VEG"),  PrimaryKey) |>
    gather("Position", "Veg"  , -PrimaryKey)
  ss_og_veg$Position <- gsub("^.{0,3}", "", ss_og_veg$Position)


  ss_og_veg <- ss_og_veg[!is.na(ss_og_veg$Veg),]

  ss_tall_veg <- tall_soil_stability |> dplyr::select(PrimaryKey, Veg, Position)


  ss_og_veg$Position <- as.character(ss_og_veg$Position)
  ss_tall_veg$Position <- as.character(ss_tall_veg$Position)

  veg_error_tall <- dplyr::setdiff(ss_og_veg, ss_tall_veg)
  if(nrow(veg_error_tall) > 0){
    veg_error_tall$Notes <- "There is a Veg record in the tall data that differs from the original data"
    veg_error_tall$Action <- "Determine why gather or clean functions are altering the original Veg"

  }

  veg_error_og <- dplyr::setdiff(ss_tall_veg, ss_og_veg)
  if(nrow(veg_error_og) > 0){
    veg_error_og$Notes <- "There is a Veg record in the original data that differs from the tall tables"
    veg_error_og$Action <- "Determine why gather or clean functions are altering the tall Veg"

  }


  veg_errors <- rbind(veg_error_tall, veg_error_og)

  if(nrow(veg_errors) > 0){
    veg_errors <-veg_errors |> filter_all(any_vars(duplicated(.)))
  }

  write.csv(veg_errors, file.path(path_qc, "soil_stability_Veg_check.csv"), row.names = F)


  valid_veg <- c("NC", "C", "G", "F", "Sh", "T", "M")
  `%notin%` <- Negate(`%in%`)

ss_veg_issues <- tall_soil_stability[tall_soil_stability$Veg %notin% valid_veg,]
  ss_veg_issues$Notes <- "Not an expected Veg class"

  write.csv(ss_veg_issues, paste0(path_qc, "/soil_stability_veg_type_check_tall_data.csv"))
}






#' QC tall files
#'
#' @param path_tall path to the directory containing tall CSV files
#' @param source source type
#' @param speciescode speciescode
#' @param USDA_plants species list of USDA codes
#' @param data_list list of raw files with keys assigned
#' @param path_qc file path where QC documents will be stored
#' @param DIMATables folder path to raw DIMA tables - AIM and DIMA only - keys not yet assigned
#' @param subset_nbr one or multiple numbers; an optional variable for running in subsets
#' @param projectkey ProjectKey
#'
#' @export
qc_tall_all <- function(source, path_tall, speciescode, USDA_plants,
                        data_list = NULL, path_qc, DIMATables = NULL,
                        subset_nbr = NULL, projectkey) {

  tall_file_names <- c("lpi_tall", "height_tall", "gap_tall",
                       "species_inventory_tall", "soil_stability_tall")

  # 1. Dynamically read files based on whether we are dealing with a subset or a whole project
  for (file_name in tall_file_names) {

    if (!is.null(subset_nbr)) {
      # If processing a subset, look for the subset prefix pattern
      file_path <- file.path(path_tall, paste0("subset", subset_nbr, "_", file_name, ".csv"))
    } else {
      # If processing the whole project at once, look for the standard file name
      file_path <- file.path(path_tall, projectkey, paste0(file_name, ".csv"))
    }

    if (file.exists(file_path)) {
      dat <- vroom::vroom(file_path, show_col_types = FALSE)
      assign(file_name, dat)
    } else {
      # Optional warning to help you track down missing expected files
      message("Note: ", file_name, " not found at: ", file_path)
    }
  }

  # 2. Dynamically resolve the QC output directory path
  if (!is.null(subset_nbr)) {
    # If it's a subset, isolate it in its own subset folder
    subset_qc_path <- file.path(path_qc, paste0("subset", subset_nbr))
  } else {
    # Otherwise, write the QC files directly to the project's root QC directory
    subset_qc_path <- path_qc
  }

  if (!dir.exists(subset_qc_path)) {
    dir.create(subset_qc_path, recursive = TRUE)
  }

  ## LPI
  if(exists("nri") && !is.null(nri$PINTERCEPT) && nrow(nri$PINTERCEPT) > 0){
    terradactylutils3::tall_lpi_qc_nri(tall_lpi = lpi_tall, speciescode = speciescode, USDA_plants = USDA_plants, PINTERCEPT = nri$PINTERCEPT, path_qc = subset_qc_path)

  } else if(exists("data_list") && !is.null(data_list[["tblLPIHeader"]]) && nrow(data_list[["tblLPIHeader"]]) > 0){
    lpi_tall$LineKey <- as.numeric(lpi_tall$LineKey)
    lpi_tall$RecKey <- as.character(lpi_tall$RecKey)
    data_list[["tblLPIHeader"]]$LineKey <- as.numeric(data_list[["tblLPIHeader"]]$LineKey)
    data_list[["tblLPIHeader"]]$RecKey <- as.character(data_list[["tblLPIHeader"]]$RecKey)
    data_list[["tblLPIDetail"]]$LineKey <- as.numeric(data_list[["tblLPIDetail"]]$LineKey)
    data_list[["tblLPIDetail"]]$RecKey <- as.character(data_list[["tblLPIDetail"]]$RecKey)
    terradactylutils3::tall_lpi_qc(cleaned_tall_lpi = lpi_tall, speciescode = speciescode, tblLPIDetail = data_list$tblLPIDetail, USDA_plants = USDA_plants , path_qc = subset_qc_path)

  } else if (source == "BLM_AIM"){
    terradactylutils3::tall_lpi_qc_AIM(tall_lpi = lpi_tall, path_tall = path_tall, path_qc = subset_path_qc)

  } else {
    message("No LPI data found")
  }

  ## Gap
  if(exists("nri") && !is.null(nri$GINTERCEPT) && nrow(nri$GINTERCEPT) > 0 ) {
    terradactylutils3::tall_gap_qc_nri(tall_gap = gap_tall, GINTERCEPT = nri$GINTERCEPT, path_qc = subset_qc_path)

  } else if(exists("data_list") && !is.null(data_list[["tblGapHeader"]]) && nrow(data_list[["tblGapHeader"]]) > 0){
    message("Found DIMA gap data; processing")
    gap_tall$LineKey <- as.numeric(gap_tall$LineKey)
    gap_tall$RecKey <- as.numeric(gap_tall$RecKey)
    data_list[["tblGapHeader"]]$LineKey <- as.numeric(data_list[["tblGapHeader"]]$LineKey)
    data_list[["tblGapHeader"]]$RecKey <- as.numeric(data_list[["tblGapHeader"]]$RecKey)
    data_list[["tblGapDetail"]]$LineKey <- as.numeric(data_list[["tblGapDetail"]]$LineKey)
    data_list[["tblGapDetail"]]$RecKey <- as.numeric(data_list[["tblGapDetail"]]$RecKey)


    terradactylutils3::tall_gap_qc(cleaned_tall_gap = gap_tall, tblGapDetail = data_list$tblGapDetail, path_qc = subset_qc_path)

  } else if(source == "BLM_AIM"){
    # Fixed path and extension
    tblGapDetail <- read.csv(file.path(DIMATables, "tblGapDetail.csv"))
    terradactylutils3::tall_gap_qc_AIM(cleaned_tall_gap = gap_tall, tblGapDetail = tblGapDetail, path_qc = subset_qc_path)

  } else {
    message("No Gap data found")
  }

  ## Soil stability
  if(exists("nri") && !is.null(nri$SOILDISAG) && nrow(nri$SOILDISAG) > 0) {
    terradactylutils3::tall_soil_stability_qc_nri(
      cleaned_tall_soil_stability = soil_stability_tall,
      SOILDISAG = nri$SOILDISAG,
      path_qc = subset_qc_path
    )
  } else if(exists("data_list") && !is.null(data_list[["tblSoilStabHeader"]]) && nrow(data_list[["tblSoilStabHeader"]]) > 0){
    terradactylutils3::tall_soil_stability_qc(tblSoilStabDetail = data_list$tblSoilStabDetail, cleaned_tall_soil_stability = soil_stability_tall, path_qc = subset_qc_path)

  } else if (source == "BLM_AIM"){
    message("Currently no QC for BLM AIM soil stability")

  } else {
    message("No soil stability data found")
  }

  ## Species richness
  message("No species richness QC")

  ## Height
  if(exists("nri") && !is.null(nri$PASTUREHEIGHTS) && nrow(nri$PASTUREHEIGHTS) > 0) {
    terradactylutils3::tall_height_qc_nri(PASTUREHEIGHTS = nri$PASTUREHEIGHTS, tall_height = height_tall, path_qc = subset_qc_path)

  } else if(exists("data_list") && !is.null(data_list[["tblLPIHeader"]]) && sum(data_list[["tblLPIDetail"]][["HeightHerbaceous"]], na.rm = T) > 0){
    terradactylutils3::tall_height_qc(tblLPIDetail = data_list$tblLPIDetail, cleaned_tall_height = height_tall, path_qc = subset_qc_path)

  } else if (source == "BLM_AIM"){
    message("Currently no BLM AIM height QC")

  } else {
    message("No height data found")
  }
}


#' QC by ProjectKey
#'
#' Identifies which project directories physically exist within the tall output directory,
#' and executes qc_tall_all checks. It dynamically determines whether a project
#' has been broken down into multi-part subset chunks, processing chunks sequentially or
#' processing the project as a single entity.
#'
#' @param projectkey Character vector. A list of target project identification strings/keys to validate.
#' @param path_tall Character string. The base target folder path containing the project subdirectories.
#' @param path_qc Character string. The base directory path where QC output project subfolders will be target routed.
#' @param source Character string. The data source type; options include "BLM_AIM", "DIMA", or "NRI".
#' @param speciescode Character vector or dataframe. Reference species code lookup table for checking data taxonomies.
#' @param USDA_plants Dataframe or object. Reference matching layout for validation against the USDA PLANTS database.
#' @param dima_data_list List. A named list of native DIMA source dataframes. Only required if \code{source = "DIMA"}. Defaults to NULL.
#' @param nri List or object. Cleaned/parsed NRI master data list asset object. Only required if \code{source = "NRI"}. Defaults to NULL.
#' @param DIMATables Character string. Path to the folder containing raw DIMA tables. Only evaluated if \code{source = "BLM_AIM"}. Defaults to NULL.
#'
#' @return Silently returns NULL. Executes processing validation and writes out metrics/reports directly to disk.
#'
#' @export
qc_by_projkey <- function(projectkey,
                                    path_tall,
                                    path_qc,
                                    source,
                                    speciescode,
                                    USDA_plants,
                                    dima_data_list = NULL,
                                    nri = NULL,
                                    DIMATables = NULL) {

  # =========================================================================
  # HELPER INNER FUNCTION: EXECUTE QC ENGINE ROUTINES
  # =========================================================================
  execute_qc_call <- function(source, path_t, path_q, subset_nbr = NULL) {
    if (source == "BLM_AIM") {
      qc_tall_all(source = source, path_tall = path_t, speciescode = speciescode,
                  USDA_plants = USDA_plants, data_list = NULL, path_qc = path_q,
                  DIMATables = DIMATables, subset_nbr = subset_nbr, projectkey = projectkey)
    } else if (source == "DIMA") {
      qc_tall_all(source = source, path_tall = path_t, speciescode = speciescode,
                  USDA_plants = USDA_plants, data_list = dima_data_list,
                  path_qc = path_q, subset_nbr = subset_nbr, projectkey = projectkey)
    } else if (source == "NRI") {
      qc_tall_all(source = source, path_tall = path_t, speciescode = speciescode,
                  USDA_plants = USDA_plants, data_list = nri,
                  path_qc = path_q, subset_nbr = subset_nbr, projectkey = projectkey)
    }
  }

  # =========================================================================
  # STEP 1: RESOLVE VALID SYSTEM DIRECTORIES
  # =========================================================================
  # List all immediate directories inside path_tall
  all_dirs <- list.dirs(path_tall, full.names = TRUE, recursive = FALSE)
  dir_names <- basename(all_dirs)

  # Define project_folders as only the project keys that physically exist as folders
  project_folders <- dir_names[dir_names %in% projectkey]

  if (length(project_folders) == 0) {
    warning("No directories matching the provided 'projectkey' vector were found in: ", path_tall)
    return(invisible(NULL))
  }

  # =========================================================================
  # STEP 2: LOOP & EVALUATE METADATA SUBSETS
  # =========================================================================
  lapply(project_folders, function(proj) {

    message("--- Starting QC for Project: ", proj, " ---")

    # Define clean paths pointing directly to the project roots
    current_path_tall <- file.path(path_tall, proj)
    current_path_qc   <- file.path(path_qc, proj)

    # Ensure target QC output folder structure exists before processing
    if (!dir.exists(current_path_qc)) {
      dir.create(current_path_qc, recursive = TRUE)
    }

    # Check if this specific project folder contains subset files
    sub_files <- list.files(current_path_tall, pattern = "^subset\\d+_", full.names = FALSE)

    if (length(sub_files) > 0) {
      # Extract unique subset numbers found in this folder (e.g., "1", "2")
      unique_subsets <- unique(gsub("^subset(\\d+)_.*$", "\\1", sub_files))
      message("Found ", length(unique_subsets), " subset chunks inside ", proj, ". Processing sequentially...")

      # Loop through each subset chunk present
      for (s_nbr in unique_subsets) {
        execute_qc_call(source, current_path_tall, current_path_qc, subset_nbr = s_nbr)
      }

    } else {
      # No subsets found! Process the entire project asset as a single entity
      message("No subsets found. Processing whole project at once...")
      execute_qc_call(source, current_path_tall, current_path_qc, subset_nbr = NULL)
    }

    message("--- Finished QC for Project: ", proj, " ---")
  })

  return(invisible(NULL))
}
