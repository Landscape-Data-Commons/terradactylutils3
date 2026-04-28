


###################################
#' Clean Tall LPI
#'
#'after gathering lpi, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param lpi as a data.frame, the tall_lpi file
#' @param dataHeader as a data.frame, the dataHeader file produced from terradactylutils2::create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return updated tall file written to path_tall and a tall_lpi data frame in the console (unless saved to an object)
#'
#' @examples clean_tall_lpi(lpi = terradactyl::gather_lpi(source = source, tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_lpi <- function(lpi, dataHeader, path_tall){
  if (any(class(lpi) %in% c("POSIXct", "POSIXt"))) {
    change_vars <- names(lpi)[do.call(rbind, vapply(lpi,
                                                    class))[, 1] %in% c("POSIXct", "POSIXt")]
    lpi <- dplyr::mutate_at(lpi, dplyr::vars(change_vars),
                            dplyr::funs(as.character))
  }

  # reorder so that primary key is leftmost column
  lpi$DBKey <- dataHeader$DBKey[match(lpi$PrimaryKey, dataHeader$PrimaryKey)]
  lpi <- lpi |>
    dplyr::select(PrimaryKey, DBKey, LineKey, tidyselect::everything())

  # Drop rows with no data
  lpi <- lpi |>
    dplyr::filter(!(is.na(LineKey) &
                      is.na(layer) &
                      is.na(code) &
                      is.na(ShrubShape) &
                      is.na(PointNbr)))




  ### remove duplicates and empty rows


  lpi <- lpi |> tdact_remove_duplicates() |> tdact_remove_empty(datatype = "lpi")


  tall_lpi <- lpi

  pkeys <- dataHeader$PrimaryKey
  dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # making sure all codes are capital
  tall_lpi$code <- toupper(tall_lpi$code)
  tall_lpi$ProjectKey <- dataHeader$ProjectKey[match(tall_lpi$PrimaryKey, dataHeader$PrimaryKey)]


  saveRDS(tall_lpi, file.path(path_tall, "lpi_tall.rdata"))
  write.csv(tall_lpi, file.path(path_tall, "lpi_tall.csv"), row.names = F)

  return(tall_lpi)
}
####################################

############################################
#' Clean Tall Gap
#'
#'removes and adds columns to the tall_gap file produced using terradactyl::gather_gap that are (not) necessary to produce geofiles
#'
#' @param tall_gap as a data.frame, tall gap file produced from terradactyl::gather_gap()
#' @param dataHeader as a data.frame, the dataHeader file produced from terradactylutils2::create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return an updated tall_gap file saved to path_tall and tall_gap in the console (unless saved to an object)
#'
#' @examples clean_tall_gap(tall_gap = terradactyl::gather_gap(source = "DIMA", tblGapHeader = tblGapHeader, tblGapDetail = tblGapDetail2), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_gap <- function(tall_gap, dataHeader, path_tall){

  dropcols_gap <- tall_gap  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_gap <- tall_gap[which(!duplicated(dropcols_gap)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_gap$DBKey <- dataHeader$DBKey[match(tall_gap$PrimaryKey, dataHeader$PrimaryKey)]

  tall_gap$DateVisited <- tblGapHeader$DateVisited[match(tall_gap$PrimaryKey, tblGapHeader$PrimaryKey)]
  #tall_gap$DateVisited <- as.character(tall_gap$DateVisited)

  tall_gap$Direction <- tblGapHeader$Direction[match(tall_gap$PrimaryKey, tblGapHeader$PrimaryKey)]
  #match
  tall_gap$ProjectKey <- dataHeader$ProjectKey[match(tall_gap$PrimaryKey, dataHeader$PrimaryKey)]

  saveRDS(tall_gap, file.path(path_tall, "gap_tall.rdata"))
  write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = F)
  return(tall_gap)
}
#####################################



#####################################
#' Clean Tall Soil Stability
#'
#'adds and removes columns to the data produced from terradactyl::gather_soil_stability that are (not) necessary to run terradactylutils2::geofiles()
#'
#' @param tall_soil_stability file produced from terradactyl::gather_soil_stability
#' @param dataHeader dataHeader produced from terradactylutils2::create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return a CSV saved to the specified path_tall and a tall_soil_stability data frame in the console (unless saved to an object)
#'
#' @examples clean_tall_soil_stability(tall_soil_stability = terradactyl::gather_soil_stability(source = source, tblSoilStabDetail = tblSoilStabDetail, tblSoilStabHeader = tblSoilStabHeader), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_soil_stability <- function(tall_soil_stability, dataHeader, path_tall){

  dropcols_soil_stability <- tall_soil_stability  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))

  pkeys <- dataHeader$PrimaryKey
  tall_soil_stability <- tall_soil_stability[which(!duplicated(dropcols_soil_stability)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_soil_stability$DBKey <- dataHeader$DBKey[match(tall_soil_stability$PrimaryKey, dataHeader$PrimaryKey)]
  tall_soil_stability$Hydro <- rep(FALSE)
  #tall_soil_stability$DateVisited <- as.character(tall_soil_stability$DateVisited)
  #rename
  tall_soil_stability <- tall_soil_stability |>
    rename(
      ProjectKey = project
    )
  saveRDS(tall_soil_stability, file.path(path_tall, "soil_stability_tall.rdata"))
  write.csv(tall_soil_stability, file.path(path_tall, "soil_stability_tall.csv"), row.names = F)
  return(tall_soil_stability)
}
##################################


########################################
#' Clean Tall Species Richness
#'
#'adds or removes columns from the  tall_species_richness file produced with terradactyl::gather_species_richness() that are (not) necessary to run terradactylutils2::geofiles()
#'
#' @param tall_species tall_species file produced from terradactyl::gather_species_richness()
#' @param dataHeader dataHeader file produced from create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return a CSV saved to the speficied path_tall and the updated tall_species_richness saved to the R enviornment
#'
#' @examples clean_tall_species(tall_species = gather_species_inventory(source = "DIMA", tblSpecRichDetail = tblSpecRichDetail, tblSpecRichHeader = tblSpecRichHeader), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_species <- function(tall_species, dataHeader, path_tall){

  dropcols_species <- tall_species  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_species <- tall_species[which(!duplicated(dropcols_species)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_species$DBKey <- dataHeader$DBKey[match(tall_species$PrimaryKey, dataHeader$PrimaryKey)]
  tall_species$Direction <- tblSpecRichHeader$Direction[match(tall_species$PrimaryKey, tblSpecRichHeader$PrimaryKey)]
  #tall_species$DateVisited <- as.character(tall_species$DateVisited)
  tall_species <- tall_species |>
    rename(
      ProjectKey = project
    )

  saveRDS(tall_species, file.path(path_tall, "species_inventory_tall.rdata"))
  write.csv(tall_species, file.path(path_tall, "species_inventory_tall.csv"), row.names = F)

  return(tall_species)
}
########################################



########################################
#' Clean Tall Height
#'
#' adds and removes columns (not) necessary to run terradactylutils2::geofiles() for the the file produced using terradactyl::gather_height()
#'
#' @param tall_height as a data.frame, the tall_file produced from terradactyl::gather_height()
#' @param dataHeader as data.frame, dataHeader file produced from terradactylutils2::create_header()
#' @param tblLPIHeader as data.frame, tblLPIHeader from the DIMA tables
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'@param source data type
#'@param todaysDate today's date
#'
#' @return a CSV saved to the specified path_tall and an updated tall_height file saved to the console(unless saved to an object)
#'
#' @examples clean_tall_height(tall_height = gather_height(source = "DIMA", tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader), dataHeader = dataHeader, tblLPIHeader = tblLPIHeader,  source = DIMA, todaysDate = todaysDate, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_height <- function(tall_height, dataHeader, tblLPIHeader,  source,todaysDate, path_tall){

  dropcols_height <- tall_height  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "heightList")))
  pkeys <- dataHeader$PrimaryKey
  tall_height <- tall_height[which(!duplicated(dropcols_height)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_height$DBKey <- dataHeader$DBKey[match(tall_height$PrimaryKey, dataHeader$PrimaryKey)]
  tall_height$ProjectKey <- dataHeader$ProjectKey[match(tall_height$PrimaryKey, dataHeader$PrimaryKey)]
  tall_height$FormType <- tblLPIHeader$FormType[match(tall_height$PrimaryKey, tblLPIHeader$PrimaryKey)]
  tall_height$source <- rep(source)
  tall_height$DateVisited <-tblLPIHeader$DateVisited[match(tall_height$PrimaryKey, tblLPIHeader$PrimaryKey)]
  #tall_height$DateVisited <- as.Date(tall_height$DateVisited, format = format)
  tall_height$DateLoadedInDb <- rep(todaysDate) #
  tall_height$FormDate <- tblLPIHeader$FormDate[match(tall_height$PrimaryKey, tblLPIHeader$PrimaryKey)]


  saveRDS(tall_height, file.path(path_tall, "height_tall.rdata"))
  write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = F)

  return(tall_height)
}
##################################







###################################
#' gather, QC and prepare tall data
#'
#' based on the source, create tall tables and get the QC information when available
#'
#' @param source source, either "NRI", "AIM" or "DIMA"
#' @param path_foringest where final files for LDC ingest are saved
#'
#' @return gathered, cleaned and QC files to their respective folders
#'
#' @export
gather_clean_all <- function(source){
  ## 8.1 LPI
  # gather lpi, prepare tall lpi and qc
  if( exists("nri") && !is.null(nri$PINTERCEPT) && nrow(nri$PINTERCEPT) > 0){
    message("Found NRI LPI data; processing")
    output <<- paste0(path_original_files, "/")
    lpi_tall <<- terradactyl::gather_lpi(dsn = paste0(output, "PINTERCEPT.csv"),
                                        file_type = "csv", source = "NRI")
    cleaned_lpi_tall <<- terradactylutils3::clean_tall_lpi_nri(lpi = lpi_tall, dataHeader = dataHeader, path_tall = path_tall)

    terradactylutils3::tall_lpi_qc_nri(tall_lpi = cleaned_lpi_tall, speciescode = speciescode, USDA_plants = USDA_plants, PINTERCEPT = nri$PINTERCEPT, path_qc = path_qc)

  } else if(exists("dima_data_list") && !is.null(dima_data_list[["tblLPIHeader"]]) && nrow(dima_data_list[["tblLPIHeader"]]) > 0){
    message("Found DIMA LPI data; processing")

    lpi <<- terradactyl::gather_lpi(source = source, tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader)

    #get the tall file into a format that can be used to produce the prepared data
    cleaned_tall_lpi <<- terradactylutils3::clean_tall_lpi(lpi = lpi, dataHeader = dataHeader, path_tall = path_tall)

    #produce QC files available in QC directory
    terradactylutils3::tall_lpi_qc(cleaned_tall_lpi = cleaned_tall_lpi, speciescode = speciescode, tblLPIDetail = tblLPIDetail, USDA_plants = USDA_plants , path_qc = path_qc)
  }else if (source == "BLM_AIM"){
    message("Found BLM gap data; processing")

    tall_lpi <<- gather_lpi_terradat(dsn = dsn)
    cleaned_tall_lpi <<- terradactylutils3::clean_tall_lpi_aim(tall_lpi = tall_lpi, dataHeader,  source = "AIM", todaysDate = todaysDate, path_tall)
    terradactylutils3::tall_lpi_qc_AIM(tall_lpi = tall_lpi, path_tall = path_tall)

  }else{message("No LPI data found")}


  #### STOP HERE to check QC file outputs ( QC/differing_layer_codes_check & tall_lpi_codes_check) ####################

  ############################


  ############################
  ## 8.2 Gap
  #gather gap, prepare tall gap and qc
  if(exists("nri") && !is.null(nri$GINTERCEPT) && nrow(nri$GINTERCEPT) > 0 ) {

    message("Found NRI gap data; processing")
    gap_tall <<- terradactyl::gather_gap(source = "NRI", GINTERCEPT = read.csv(paste0(output, "GINTERCEPT.csv")), POINT = read.csv(paste0(output, "POINT.csv")))
    cleaned_tall_gap <<- terradactylutils3::clean_tall_gap_nri(tall_gap = gap_tall, dataHeader = dataHeader, path_tall = path_tall)

    terradactylutils3::tall_gap_qc_nri(tall_gap = gap_tall, GINTERCEPT = nri$GINTERCEPT, path_qc = path_qc)

  } else if(exists("dima_data_list") && !is.null(dima_data_list[["tblGapHeader"]]) && nrow(dima_data_list[["tblGapHeader"]]) > 0){
    message("Found DIMA gap data; processing")
    #having LineKey and FormDate in both tblGapDetail and tblGapHeader prevents gather_gap from running - removing
    tblGapDetail2 <<- tblGapDetail %>% mutate(LineKey = NULL)
    tblGapDetail2 <<- tblGapDetail2 %>% mutate(FormDate = NULL)

    #gather
    tall_gap <<- terradactyl::gather_gap(source = "DIMA", tblGapHeader = tblGapHeader, tblGapDetail = tblGapDetail2) %>% dplyr::filter(PrimaryKey %in% pkeys)

    #get the tall file into a format that can be used to produce the prepared data
    cleaned_tall_gap <<- terradactylutils3::clean_tall_gap(tall_gap = tall_gap, dataHeader = dataHeader, path_tall = path_tall)

    #produce QC files available in QC directory
    terradactylutils3::tall_gap_qc(cleaned_tall_gap = cleaned_tall_gap, tblGapDetail = tblGapDetail, path_qc = path_qc)
  }else if(source == "BLM_AIM"){
    tall_gap <<- gather_gap_terradat(dsn = dsn)
    terradactylutils3::clean_tall_gap_aim(path_tall = path_tall, tall_gap = tall_gap, dataHeader = dataHeader, source = "AIM", todaysDate = todaysDate)
  }else{message("No Gap data found")}

  #### STOP AND CHECK QC/ GapStart_check, Gap_check, GapEnd_check #########

  ############################



  ############################

  ## 8.3 Soil stability

  # gather soilstability, prepare tall soil stability and qc
  if(exists("nri") && !is.null(nri$SOILDISAG) && nrow(nri$SOILDISAG) > 0) {

    message("Found NRI soil stability data; processing")
    soilstab_tall <<- terradactyl::gather_soil_stability(source = "NRI", SOILDISAG = read.csv(paste0(output, "/SOILDISAG.csv")))
    cleaned_tall_soil_stability <<- terradactylutils3::clean_tall_soil_stability_nri(tall_soil_stability = soilstab_tall, dataHeader = dataHeader, path_tall = path_tall)
    terradactylutils3::tall_soil_stability_qc_nri(tall_soil_stability = cleaned_tall_soil_stability, SOILDISAG = nri$SOILDISAG, path_qc = path_qc)

  } else if(exists("dima_data_list") && !is.null(dima_data_list[["tblSoilStabHeader"]]) && nrow(dima_data_list[["tblSoilStabHeader"]]) > 0){
    message("Found DIMA soil stability data; processing")
    #gather
    tall_soil_stability <<- terradactyl::gather_soil_stability(source = source, tblSoilStabDetail = tblSoilStabDetail, tblSoilStabHeader = tblSoilStabHeader)

    #get the tall file into a format that can be used to produce the prepared data
    cleaned_tall_soil_stability <<- terradactylutils3::clean_tall_soil_stability(tall_soil_stability = tall_soil_stability, dataHeader = dataHeader, path_tall = path_tall)

    #produce QC files available in QC directory
    terradactylutils3::tall_soil_stability_qc(tblSoilStabDetail = tblSoilStabDetail, cleaned_tall_soil_stability = cleaned_tall_soil_stability, path_qc = path_qc)
  }else if (source == "BLM_AIM"){
    tall_soilstability <<- gather_soil_stability_terradat(dsn = dsn)
    terradactylutils3::clean_tall_soil_stability_aim(tall_soil_stability = tall_soilstability, path_tall = path_tall, dataHeader = dataHeader, source = "AIM", todaysDate = todaysDate)


  }else{message("No soil stability data found")}

  #### STOP AND CHECK QC/ soil_stability_rating_check, soil_stability_Veg_check #########

  ############################



  ############################

  ## 8.4 Species richness

  # gather species richness, prepare tall species richness and qc
  if(exists("nri") && !is.null(nri$PLANTCENSUS) && nrow(nri$PLANTCENSUS) > 0) {
    message("Found NRI species richness data; processing")
    species_inventory_tall <<- terradactyl::gather_species_inventory(source = "NRI", PLANTCENSUS = read.csv(paste0(output, "/PLANTCENSUS.csv")))

    cleaned_tall_species <<- terradactylutils3::clean_tall_species_nri(tall_species = species_inventory_tall, dataHeader = dataHeader, path_tall = path_tall)


  }else if(exists("dima_data_list") && !is.null(dima_data_list[["tblSpecRichHeader"]]) && nrow(dima_data_list[["tblSpecRichHeader"]]) > 0){
    message("Found DIMA species richness data; processing")
    #gather
    tblSpecRichHeader$RecKey <<- as.character(tblSpecRichHeader$RecKey)

    tall_species <- terradactyl::gather_species_inventory(source = source, tblSpecRichDetail = tblSpecRichDetail, tblSpecRichHeader = tblSpecRichHeader)

    #get the tall file into a format that can be used to produce the prepared data
    cleaned_tall_species <<- terradactylutils3::clean_tall_species(tall_species = tall_species, dataHeader = dataHeader, path_tall = path_tall)
  }else if (source == "BLM_AIM"){
    tall_sr <<- gather_species_inventory_terradat(dsn = dsn)
    terradactylutils3::clean_tall_species_richness_aim(tall_species_richness = tall_sr, path_tall = path_tall, dataHeader = dataHeader, source = "AIM", todaysDate = todaysDate)
  }else{message("No species richness data found")}

  ############################



  ############################
  ## 8.5 Height
  # height has different requirements for the class of RecKey and SpeciesLowerHerb than LPI for gather to run,
  # which means height with the changes below needs to be run after gather LPI
  if(exists("nri") && !is.null(nri$PASTUREHEIGHTS) && nrow(nri$PASTUREHEIGHTS) > 0) {
    message("Found NRI height data; processing")
    height_tall <<- terradactyl::gather_height(source = "NRI", PASTUREHEIGHTS = read.csv(paste0(output, "PASTUREHEIGHTS.csv")))
    cleaned_tall_height <<- terradactylutils3::clean_tall_height_nri(tall_height = height_tall, dataHeader = dataHeader, tblLPIHeader = tblLPIHeader, source = source, todaysDate = todaysDate, path_tall = path_tall)
    terradactylutils3::tall_height_qc_nri(PASTUREHEIGHTS = nri$PASTUREHEIGHTS, tall_height = height_tall, path_qc = path_qc)
  } else if(exists("dima_data_list") && !is.null(dima_data_list[["tblLPIHeader"]]) && sum(dima_data_list[["tblLPIDetail"]][["HeightHerbaceous"]], na.rm = T) > 0){
    tblLPIHeader$RecKey <<- as.character(tblLPIHeader$RecKey)
    tblLPIDetail$RecKey <<- as.character(tblLPIDetail$RecKey)
    tblLPIDetail$SpeciesLowerHerb <<- as.character(tblLPIDetail$SpeciesLowerHerb)

    # gather height, prepare tall height data and qc


    #gather
    tall_height <<- terradactyl::gather_height(source = source, tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader)

    #get the tall file into a format that can be used to produce the prepared data
    cleaned_tall_height <<- terradactylutils3::clean_tall_height(tall_height = tall_height, dataHeader = dataHeader, tblLPIHeader = tblLPIHeader, source = source, todaysDate = todaysDate, path_tall = path_tall)

    #produce QC files available in QC directory
    terradactylutils3::tall_height_qc(tblLPIDetail = tblLPIDetail, cleaned_tall_height = cleaned_tall_height, path_qc = path_qc)
  }else if (source == "BLM_AIM"){
    tall_height <<- gather_height_terradat(dsn = dsn)
    cleaned_tall_height <<- terradactylutils3::clean_tall_height_aim(tall_height = tall_height, dataHeader = dataHeader, source = "AIM", todaysDate = todaysDate, path_tall = path_tall)



  }else{message("No height data found")}

  #### STOP AND CHECK QC/Height_check #########


  # 8.6 RANGEHEALTH
  if(exists("nri") && !is.null(nri$RANGEHEALTH) && nrow(nri$RANGEHEALTH) > 0){
    header <<- read.csv(paste0(path_tall, "/header.csv"))
    rangehealth_tall <<- gather_rangeland_health(source = "NRI", RANGEHEALTH = read.csv(paste0(output, "/RANGEHEALTH.csv")))
    saveRDS(rangehealth_tall,paste0(path_tall,"/rangelandhealth_tall.Rdata"))
    write.csv(rangehealth_tall,paste0(path_tall,"/rangelandhealth_tall.csv"))

  }else{message("No RH NRI data found")}
  ############################

  ###########################
  # soil horizons
  if(exists("nri") && !is.null(nri$SOILHORIZON) && nrow(nri$SOILHORIZON) > 0){
    terradactylutils3::create_soil_horizons_nri(nri = nri, path_tall = path_tall)
  }else{message("No NRI soil horizon data found")}




  ##########################
  # Horizontal flux and DDT
  if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_BoxCollection"]]) && nrow(dima_data_list[["tblBSNE_BoxCollection"]]) > 0){
    message("DIMA MWAC data found; processing")

    terradactylutils3::create_mwac(tblBSNE_BoxCollection = dima_data_list[["tblBSNE_BoxCollection"]], path_foringest = path_foringest)
  }else{message("No DIMA MWAC data found")}


  ##########################


  ##########################
  # DDT
  if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_TrapCollection"]]) && nrow(dima_data_list[["tblBSNE_TrapCollection"]]) > 0){
    message("DIMA DDT data found; processing")

    create_ddt(dima_data_list[["tblBSNE_TrapCollection"]], path_foringest = path_foringest)
  }else{message("No DIMA DDT data found")}



}






###################################
#' gather all data
#'
#' based on the source, create tall tables and get the QC information when available
#'
#' @param source source, either "NRI", "AIM" or "DIMA"
#' @param path_original_files default NULL, path where CSV of raw NRI data saved
#' @param path_tall path where cleaned tall files are/will be stored
#' @param path_schema path to LDC schema plan
#' @param gathered_data file path where gathered data, not yet cleaned, will be saved
#'
#' @return saves CSV and RDS file of terradactyl gathered files to path gathered_data
#'
#' @export
gather_all <- function(source, path_original_files = NULL, gathered_data, path_tall, path_schema) {
  # Initialize a list to store the data frames
  tall_files_list <- list()

  ## 8.1 LPI
  if (exists("nri") && !is.null(nri$PINTERCEPT) && nrow(nri$PINTERCEPT) > 0) {
    message("Found NRI LPI data; processing")
    output <- paste0(path_original_files, "/")
    lpi_tall <- terradactyl::gather_lpi(
      dsn = paste0(output, "PINTERCEPT.csv"),
      file_type = "csv", source = "NRI"
    )
    write.csv(lpi_tall, paste0(gathered_data, "/lpi_tall.csv"))
    tall_files_list$lpi_tall <- lpi_tall
  } else if (exists("dima_data_list") && !is.null(dima_data_list[["tblLPIHeader"]]) && nrow(dima_data_list[["tblLPIHeader"]]) > 0) {
    message("Found DIMA LPI data; processing")

    lpi <- terradactyl::gather_lpi(source = source, tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader)
    write.csv(lpi, paste0(gathered_data, "/lpi_tall.csv"))
    tall_files_list$lpi_tall <- lpi
  } else if (source == "BLM_AIM") {
    message("Found BLM gap data; processing")

    tall_lpi <- gather_lpi_terradat(dsn = dsn)
    write.csv(tall_lpi, paste0(gathered_data, "/lpi_tall.csv"))
    tall_files_list$lpi_tall <- tall_lpi
  } else {
    message("No LPI data found")
  }

  ## 8.2 Gap
  if (exists("nri") && !is.null(nri$GINTERCEPT) && nrow(nri$GINTERCEPT) > 0) {
    message("Found NRI gap data; processing")
    gap_tall <- terradactyl::gather_gap(source = "NRI", GINTERCEPT = read.csv(paste0(output, "GINTERCEPT.csv")), POINT = read.csv(paste0(output, "POINT.csv")))
    write.csv(gap_tall, paste0(gathered_data, "/gap_tall.csv"))
    tall_files_list$gap_tall <- gap_tall
  } else if (exists("dima_data_list") && !is.null(dima_data_list[["tblGapHeader"]]) && nrow(dima_data_list[["tblGapHeader"]]) > 0) {
    message("Found DIMA gap data; processing")
    tblGapDetail2 <- tblGapDetail %>% mutate(LineKey = NULL)
    tblGapDetail2 <- tblGapDetail2 %>% mutate(FormDate = NULL)

    tall_gap <- terradactyl::gather_gap(source = "DIMA", tblGapHeader = tblGapHeader, tblGapDetail = tblGapDetail2) %>% dplyr::filter(PrimaryKey %in% pkeys)
    write.csv(tall_gap, paste0(gathered_data, "/gap_tall.csv"))
    tall_files_list$gap_tall <- tall_gap
  } else if (source == "BLM_AIM") {
    tall_gap <- gather_gap_terradat(dsn = dsn)
    write.csv(tall_gap, paste0(gathered_data, "/gap_tall.csv"))
    tall_files_list$gap_tall <- tall_gap
  } else {
    message("No Gap data found")
  }

  ## 8.3 Soil stability
  if (exists("nri") && !is.null(nri$SOILDISAG) && nrow(nri$SOILDISAG) > 0) {
    message("Found NRI soil stability data; processing")
    soilstab_tall <- terradactyl::gather_soil_stability(source = "NRI", SOILDISAG = read.csv(paste0(output, "/SOILDISAG.csv")))
    write.csv(soilstab_tall, paste0(gathered_data, "/soil_stability_tall.csv"))
    tall_files_list$soil_stability_tall <- soilstab_tall
  } else if (exists("dima_data_list") && !is.null(dima_data_list[["tblSoilStabHeader"]]) && nrow(dima_data_list[["tblSoilStabHeader"]]) > 0) {
    message("Found DIMA soil stability data; processing")
    tall_soil_stability <- terradactyl::gather_soil_stability(source = source, tblSoilStabDetail = tblSoilStabDetail, tblSoilStabHeader = tblSoilStabHeader)
    write.csv(tall_soil_stability, paste0(gathered_data, "/soil_stability_tall.csv"))
    tall_files_list$soil_stability_tall <- tall_soil_stability
  } else if (source == "BLM_AIM") {
    tall_soilstability <- gather_soil_stability_terradat(dsn = dsn)
    write.csv(tall_soilstability, paste0(gathered_data, "/soil_stability_tall.csv"))
    tall_files_list$soil_stability_tall <- tall_soilstability
  } else {
    message("No soil stability data found")
  }

  ## 8.4 Species richness
  if (exists("nri") && !is.null(nri$PLANTCENSUS) && nrow(nri$PLANTCENSUS) > 0) {
    message("Found NRI species richness data; processing")
    species_inventory_tall <- terradactyl::gather_species_inventory(source = "NRI", PLANTCENSUS = read.csv(paste0(output, "/PLANTCENSUS.csv")))
    write.csv(species_inventory_tall, paste0(gathered_data, "/species_inventory_tall.csv"))
    tall_files_list$species_inventory_tall <- species_inventory_tall
  } else if (exists("dima_data_list") && !is.null(dima_data_list[["tblSpecRichHeader"]]) && nrow(dima_data_list[["tblSpecRichHeader"]]) > 0) {
    message("Found DIMA species richness data; processing")
    tblSpecRichHeader$RecKey <- as.character(tblSpecRichHeader$RecKey)
    tall_species <- terradactyl::gather_species_inventory(source = source, tblSpecRichDetail = tblSpecRichDetail, tblSpecRichHeader = tblSpecRichHeader)
    write.csv(tall_species, paste0(gathered_data, "/species_inventory_tall.csv"))
    tall_files_list$species_inventory_tall <- tall_species
  } else if (source == "BLM_AIM") {
    tall_sr <- gather_species_inventory_terradat(dsn = dsn)
    write.csv(tall_sr, paste0(gathered_data, "/species_inventory_tall.csv"))
    tall_files_list$species_inventory_tall <- tall_sr
  } else {
    message("No species richness data found")
  }

  ## 8.5 Height
  if (exists("nri") && !is.null(nri$PASTUREHEIGHTS) && nrow(nri$PASTUREHEIGHTS) > 0) {
    message("Found NRI height data; processing")
    height_tall <- terradactyl::gather_height(source = "NRI", PASTUREHEIGHTS = read.csv(paste0(output, "PASTUREHEIGHTS.csv")))
    write.csv(height_tall, paste0(gathered_data, "/height_tall.csv"))
    tall_files_list$height_tall <- height_tall
  } else if (exists("dima_data_list") && !is.null(dima_data_list[["tblLPIHeader"]]) && sum(dima_data_list[["tblLPIDetail"]][["HeightHerbaceous"]], na.rm = T) > 0) {
    tblLPIHeader$RecKey <- as.character(tblLPIHeader$RecKey)
    tblLPIDetail$RecKey <- as.character(tblLPIDetail$RecKey)
    tblLPIDetail$SpeciesLowerHerb <- as.character(tblLPIDetail$SpeciesLowerHerb)

    tall_height <- terradactyl::gather_height(source = source, tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader)
    write.csv(tall_height, paste0(gathered_data, "/height_tall.csv"))
    tall_files_list$height_tall <- tall_height
  } else if (source == "BLM_AIM") {
    tall_height <- gather_height_terradat(dsn = dsn)
    write.csv(tall_height, paste0(gathered_data, "/height_tall.csv"))
    tall_files_list$height_tall <- tall_height
  } else {
    message("No height data found")
  }

  # 8.6 RANGEHEALTH
  if (exists("nri") && !is.null(nri$RANGEHEALTH) && nrow(nri$RANGEHEALTH) > 0) {
    message("Found NRI rangeland health data; Processing")
    header <- read.csv(paste0(path_tall, "/header.csv"))
    rangehealth_tall <- gather_rangeland_health(source = "NRI", RANGEHEALTH = read.csv(paste0(output, "/RANGEHEALTH.csv")))
    write.csv(rangehealth_tall, paste0(gathered_data, "/rangelandhealth_tall.csv"))
    tall_files_list$rangelandhealth_tall <- rangehealth_tall
  } else {
    message("No RH NRI data found")
  }

  # Soil horizons
  if (exists("nri") && !is.null(nri$SOILHORIZON) && nrow(nri$SOILHORIZON) > 0) {
    message("Found NRI soil horizons data; Processing")
    # Assuming this function handles its own output or returns something useful
    header <- read.csv(paste0(path_tall, "/header.csv"))
    tall_files_list$soil_horizons <- terradactylutils3::create_soil_horizons_nri(nri = nri, gathered_data = gathered_data, path_schema = path_schema, dataHeader = header)
  } else {
    message("No NRI soil horizon data found")
  }

  # Horizontal flux and DDT
  if (exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_BoxCollection"]]) && nrow(dima_data_list[["tblBSNE_BoxCollection"]]) > 0) {
    message("DIMA MWAC data found; processing")
    tall_files_list$mwac <- terradactylutils3::create_mwac(tblBSNE_BoxCollection = dima_data_list[["tblBSNE_BoxCollection"]], gathered_data = gathered_data)
  } else {
    message("No DIMA MWAC data found")
  }

  # DDT
  if (exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_TrapCollection"]]) && nrow(dima_data_list[["tblBSNE_TrapCollection"]]) > 0) {
    message("DIMA DDT data found; processing")
    tall_files_list$ddt <- create_ddt(dima_data_list[["tblBSNE_TrapCollection"]], gathered_data = gathered_data)
  } else {
    message("No DIMA DDT data found")
  }

  # Return the collected data frames
  return(tall_files_list)
}




###################################
#' clean all data
#'
#' clean any of the tall files available in the gathered_data folder
#'
#' @param source source, either "NRI", "AIM" or "DIMA"
#' @param dataHeader as data frame dataHeader
#' @param path_tall path where cleaned tall files are/will be stored
#' @param gathered_data file path where gathered data, not yet cleaned, will be saved
#'
#' @return saves CSV and RDS file of terradactyl gathered files to path gathered_data
#'
#' @export
clean_all <- function(source, gathered_data, dataHeader, path_tall, subset_to_filter = NULL) {

  # Initialize the return list
  tall_files_list <- list()

  # Define the output directory: path_tall/subset/subset_X
  # If no subset is provided, it defaults to the main path_tall
  if (!is.null(subset_to_filter)) {
    output_dir <- file.path(path_tall, "subset", paste0("subset_", subset_to_filter))
  } else {
    output_dir <- path_tall
  }

  # Ensure the directory exists (recursive = TRUE handles creating /subset/ then /subset_X/)
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  # load all files in gathered_data
  tall_file_names <- c("lpi_tall", "height_tall", "gap_tall", "species_inventory_tall",
                       "soil_stability_tall", "rangelandhealth_tall", "header", "soil_horizons_tall",
                       "horizontalflux_tall", "dustdeposition_tall")

  for (file_name in tall_file_names) {
    file_path <- file.path(gathered_data, paste0(file_name, ".csv"))
    if (file.exists(file_path)) {
      dat <- vroom::vroom(file_path, show_col_types = FALSE)

      # Filter by subset_nbr if a subset is specified and the column exists
      if (!is.null(subset_to_filter) && "subset_nbr" %in% names(dat)) {
        dat <- dat %>% dplyr::filter(subset_nbr == subset_to_filter)
      }

      # Assign to Global for your other scripts
      assign(file_name, dat, envir = .GlobalEnv)
      # Assign to Local so THIS function can see it
      assign(file_name, dat)
    }
  }

  # source suffix for function names
  # NRI -> "_nri", BLM_AIM -> "_aim", DIMA -> ""
  s_suffix <- case_when(
    source == "NRI" ~ "_nri",
    source == "BLM_AIM" ~ "_aim",
    TRUE ~ ""
  )

  # Improved dynamic function runner
  run_process <- function(protocol, ...) {
    # Construct just the function name WITHOUT the ::
    # Handle the 'richness' naming discrepancy if it exists in your package
    p_name <- if(protocol == "species") "species_richness" else protocol
    func_name <- paste0("clean_tall_", p_name, s_suffix)

    # Check if the function exists in the terradactylutils3 namespace
    if (exists(func_name, where = asNamespace("terradactylutils3"), mode = "function")) {
      message("Executing: terradactylutils3::", func_name)

      # Get the actual function object from the package
      actual_func <- getExportedValue("terradactylutils3", func_name)

      return(do.call(actual_func, list(...)))
    } else {
      message("Warning: Function ", func_name, " not found in terradactylutils3")
      return(NULL)
    }
  }

  ## process each method by source

  # LPI
  if (exists("lpi_tall")) {
    cleaned_lpi_tall <- run_process("lpi", lpi = lpi_tall, dataHeader = dataHeader, path_tall = path_tall)
    tall_files_list$lpi <- cleaned_lpi_tall
  }

  # Gap
  if (exists("gap_tall")) {
    cleaned_tall_gap <- run_process("gap", tall_gap = gap_tall, dataHeader = dataHeader, path_tall = path_tall)
    tall_files_list$gap <- cleaned_tall_gap
  }

  # Soil Stability
  if (exists("soil_stability_tall")) {
    cleaned_tall_soil_stability <- run_process("soil_stability", tall_soil_stability = soil_stability_tall, dataHeader = dataHeader, path_tall = path_tall)
    tall_files_list$soil_stability <- cleaned_tall_soil_stability
  }

  # Species Richness
  if (exists("species_inventory_tall")) {
    cleaned_tall_species <- run_process("species", tall_species = species_inventory_tall, dataHeader = dataHeader, path_tall = path_tall)
    tall_files_list$species_inventory <- cleaned_tall_species
  }

  # Height
  if (exists("height_tall")) {
    cleaned_tall_height <- run_process("height", tall_height = height_tall, dataHeader = dataHeader, path_tall = path_tall)
    tall_files_list$height <- cleaned_tall_height
  }

  #  Range Health
  if (exists("nri") && !is.null(nri$RANGEHEALTH) && nrow(nri$RANGEHEALTH) > 0) {
    if(exists("rangelandhealth_tall")){
      tall_files_list$rangehealth <- rangelandhealth_tall
      write.csv(rangelandhealth_tall, file.path(output_dir, "rangelandhealth_tall.csv"), row.names = FALSE)
    }
  }

  # soil horizons
  if(exists("soil_horizons_tall")){
    tall_files_list$soil_horizons <- soil_horizons_tall
    write.csv(soil_horizons_tall, file.path(output_dir, "soil_horizons_tall.csv"), row.names = FALSE)
  }

  # Horizontal flux and DDT
  if(exists("horizontalflux_tall")){
    tall_files_list$horizontal_flux <- horizontalflux_tall
    write.csv(horizontalflux_tall, file.path(output_dir, "horizontalflux_tall.csv"), row.names = FALSE)
  }

  # DDT
  if(exists("dustdeposition_tall")){
    tall_files_list$dust_deposition <- dustdeposition_tall
    write.csv(dustdeposition_tall, file.path(output_dir, "dustdeposition_tall.csv"), row.names = FALSE)
  }

  return(tall_files_list)
}
