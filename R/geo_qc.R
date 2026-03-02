#######################################
#' geoIndicators QC
#'
#' produces a CSV with QC information about the geoIndicators table
#'
#' @param path_foringest where geoIndicators data was saved
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with QC information about the geoIndicators table
#'
#' @examples geoind_qc(path_foringest = path_foringest, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
#' @export
geoind_qc <- function(path_foringest, path_qc){

  geoind <- read.csv(paste0(path_foringest, "/geoIndicators.csv"))

  dat_selected <- geoind[sapply(geoind, is.numeric)]
  geoind_NA <- dat_selected |>
    summarise(across(everything(), ~ sum(is.na(.x)))) |> mutate(number_obs_in_geoind = nrow(dat_selected))

  geoind_NA <- gather(geoind_NA, col_name, number_NAs, -number_obs_in_geoind)
  geoind_NA <- geoind_NA[,c(2,3,1)]


  geoind_zero <- dat_selected |> summarise(across(everything(), ~ sum(.x, na.rm = T) ))
  geoind_zero <- gather(geoind_zero, col_name, sum_for_entire_column)

  geoind_check <-  left_join(geoind_NA, geoind_zero, by = "col_name")

  geoind_check$Notes <- ifelse(geoind_check$sum_for_entire_column == 0, "Every observation for this column is zero",
                               "")

  geoind_check$Action <- ifelse(geoind_check$sum_for_entire_column == 0,
                                "If this is unexpected, review the template has the desired column followed by the geofiles calculations",
                                "")


  write.csv(geoind_NA, file.path(path_qc, "geoind_number_NAs_zeros_per_numeric_column.csv"), row.names = FALSE)

  # make sure core are not adding up to more than 100
  #bare_tf <- geoind |> mutate(bare_tf = BareSoil + TotalFoliarCover)
  gaps <- geoind

  gaps$Notes <- ifelse(gaps$GapCover_25_plus > 100.01, "Sum of GapCover is greater than 100%", "")

  # putting in an action for all rows for now, at the end, only the problem observation remain and
  #will be seen by the user

  gaps$Action <- "Determine if rounding error and work with project manager to decide if plot needs removed"

  gaps <- gaps |> dplyr::select(ProjectKey, PrimaryKey, Notes, Action)



  total_cover <- geoind |> mutate(FH_BareSoil = BareSoil) |> mutate(FH_TotalFoliarCover = TotalFoliarCover)
  total_cover <- total_cover |>
    dplyr::select(matches("FH_"), ProjectKey, PrimaryKey)
  total_cover<- subset(total_cover, select=-c(FH_TotalLitterCover)) #? not to be included
  #total_cover$total_cover <- rowSums(total_cover)
  total_cover <- total_cover |> dplyr::mutate(total_cover=rowSums(dplyr::select(total_cover,-ProjectKey, -PrimaryKey), na.rm = T))
  total_cover$Notes <- ifelse(total_cover$total_cover > 100.01, "Sum of FH cover is greater than 100%", "")
  # putting in an action for all rows for now, at the end, only the problem observation remain and
  #will be seen by the user

  total_cover$Action <- "Determine if rounding error and work with project manager to decide if plot needs removed"

  total_cover <- total_cover |> dplyr::select(ProjectKey, PrimaryKey, Notes, Action )

  cover_errors <- rbind(gaps, total_cover)

  cover_errors <- cover_errors |> filter(Notes != "")

  write.csv(cover_errors, file.path(path_qc, "geoind_total_and_gap_cover_check.csv"), row.names = FALSE)

}
#################################



#################################
#' geoSpecies QC
#'
#'produces a CSV with information about the QC of the geoSpecies file
#'
#' @param path_foringest path to where geoSpecies file is stored
#' @param USDA_plants as a data.frame, a file containing the accepted USDA codes with a code, GrowthHabit and Duration column
#' @param speciescode the name of the column with the 4 letter USDA codes in the USDA_plants file
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with information about the QC of the geoSpecies file
#'
#' @examples geospecies_qc(path_foringest = path_foringest, USDA_plants = read.csv("D:/modifying_data_prep_script_10032025/2004-2023_ceap_species_list.csv"), speciescode = "UpdatedSpeciesCode", path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
#' @export
geospecies_qc <- function(path_foringest, USDA_plants, speciescode, path_qc){

  a2 <- read.csv(paste0(path_foringest, "/geoSpecies.csv"))

  # list species < 4 char
  issue_codes_char <- a2[nchar(a2$Species) <= 3, ]

  if(nrow(issue_codes_char >0)){
    issue_codes_char$Notes <- "The species code is less than the expected 4 characters"
    issue_codes_char <- issue_codes_char |> dplyr::select(ProjectKey, PrimaryKey, Species, Notes)
  }

  #list any with NA GH or duration
  issue_codes_GH <- a2[is.na(a2$GrowthHabit),]
  if(nrow(issue_codes_GH >0)){
    issue_codes_GH$Notes <- "The GrowthHabit is NA"
    issue_codes_GH <- issue_codes_GH |> dplyr::select(ProjectKey, PrimaryKey, Species, Notes)
  }

  issue_codes_D <- a2[is.na(a2$Duration),]

  if(nrow(issue_codes_D >0)){
    issue_codes_D$Notes <- "The Duration is NA"
    issue_codes_D <- issue_codes_D |> dplyr::select(ProjectKey, PrimaryKey, Species, Notes)
  }

  # checking that the geospecies codes are in the USDA database
  USDA_plant_codes <- USDA_plants[,paste0(speciescode)]
  `%notin%` <- Negate(`%in%`)
  incorrect_code_sp <- a2[a2$code %notin% USDA_plant_codes,]

  if(nrow(incorrect_code_sp >0)){
    incorrect_code_sp$Notes <- "Species code not associated with a USDA plant code"
    incorrect_code_sp <- incorrect_code_sp |> dplyr::select(ProjectKey, PrimaryKey, Species, Notes)
  }


  incorrect_codes <-  rbind(issue_codes_char, issue_codes_GH) %>%
    rbind(., issue_codes_D) %>% rbind(., incorrect_code_sp)

  if(nrow(incorrect_codes >0)){
    incorrect_codes$Action <- "Work with the project manager to determine the correct species code and attributes"
  }

  write.csv(incorrect_codes, file.path(path_qc, "geoSpecies_code_check.csv"), row.names = FALSE)

}
##############################################

