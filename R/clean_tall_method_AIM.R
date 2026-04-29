
###################################
#' Clean Tall LPI AIM
#'
#'after gathering lpi, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_lpi as a data.frame, the tall_lpi file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader dataHeader as dataframe
#'
#' @return cleaned, in LDC format, tall lpi to the path_tall
#'
#' @export
clean_tall_lpi_aim <- function(tall_lpi, path_tall, dataHeader){

header <- dataHeader
pkeys <- dataHeader$PrimaryKey
dropcols_lpi <- tall_lpi %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] %>%
  dplyr::filter(PrimaryKey %in% pkeys) %>% unique()

# Set classes #can we just add this to terra?
## date fields
lpi <- tall_lpi
if (any(class(lpi) %in% c("POSIXct", "POSIXt"))) {
  change_vars <- names(lpi)[do.call(rbind, vapply(lpi,
                                                  class))[, 1] %in% c("POSIXct", "POSIXt")]
  lpi <- dplyr::mutate_at(lpi, dplyr::vars(change_vars),
                          dplyr::funs(as.character))
}
## text field
# reorder so that primary key is leftmost column
# adding DBKey
lpi$DBKey <- header$DBKey[match(lpi$PrimaryKey,header$PrimaryKey)] # adding outside of terra


lpi <- lpi %>%
  dplyr::select(PrimaryKey, DBKey, LineKey, tidyselect::everything())

# Drop rows with no data
lpi <- lpi %>%
  dplyr::filter(!(is.na(LineKey) &
                    is.na(layer) &
                    is.na(code) &
                    is.na(ShrubShape) &
                    is.na(PointNbr)))


lpi <- lpi %>% tdact_remove_duplicates() %>% tdact_remove_empty(datatype = "lpi")


tall_lpi <- lpi

#dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
# we want to keep the DBKey and DateLoadedInDb
dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c( "rid", "DateModified", "SpeciesList")))
pkeys <- dataHeader$PrimaryKey
tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] %>%
  dplyr::filter(PrimaryKey %in% pkeys) %>% unique()

tall_lpi$source <- "BLM_AIM"
tall_lpi$ProjectKey <- "BLM_AIM"
tall_lpi$DateLoadedInDb <- Sys.Date()
tall_lpi$SpeciesState <- rep("BLM_AIM") # should this be the species state from header??
tall_lpi$DBKey <- header$DBKey[match(tall_lpi$PrimaryKey,header$PrimaryKey)] # adding outside of terra
tall_lpi$ShowCheckbox <- NA
tall_lpi$code<- trimws(tall_lpi$code)
tall_lpi$SpeciesState <- NULL
saveRDS(tall_lpi, file.path(path_tall, "lpi_tall.rdata"))
write.csv(tall_lpi, file.path(path_tall, "lpi_tall.csv"), row.names = F)

}



###################################
#' Clean Tall gap AIM
#'
#'after gathering, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_gap as a data.frame, the tall_gap file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader dataframe dataHeader
#' @param tblGapHeader for BLM_AIM, this argument remains NULL
#'
#' @return cleaned, in LDC format, tall gap file to path_tall
#'
#' @export
clean_tall_gap_aim <- function(tall_gap, path_tall, dataHeader,  tblGapHeader = NULL){
  header <- dataHeader
  dropcols_gap <- tall_gap  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  #dropcols_gap <- tall_gap  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_gap <- tall_gap[which(!duplicated(dropcols_gap)),] %>%
    dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
  #tall_gap$DBKey.y <- NULL
  #colnames(tall_gap)[colnames(tall_gap) == 'DBKey.x'] <- 'DBKey'
  tall_gap$ProjectKey  <-"BLM_AIM"
  tall_gap$chckbox <- rep(NA)
  tall_gap$DateVisited   <- as.Date(tall_gap$FormDate, format = "%Y-%m-%d")
  tall_gap$FormType        <- rep("Gap")

  tall_gap$DateLoadedInDb    <-Sys.Date()
  tall_gap$source <- rep("BLM_AIM") #
  tall_gap <- tall_gap %>% dplyr::group_by(PrimaryKey, LineKey, RecType) %>%
    dplyr::mutate(sumCanCat1 = sum(Gap, na.rm = T)) %>% dplyr::ungroup() #
  tall_gap$Notes <- rep(NA) #
  tall_gap$DBKey <- header$DBKey[match(tall_gap$PrimaryKey,header$PrimaryKey)]
  saveRDS(tall_gap, file.path(path_tall, "gap_tall.rdata"))
  write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = F)
  return(tall_gap)
}


###################################
#' Clean Tall soil stability AIM
#'
#'after gathering, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_soil_stability as a data.frame, the tall_gap file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader dataframe dataHeader
#'
#' @return updated tall file written to path_tall
#'
#' @export
clean_tall_soil_stability_aim <- function(tall_soil_stability, path_tall, dataHeader){
header <- dataHeader
  #dropcols_soilstability <- tall_soilstability  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))

  dropcols_soilstability <- tall_soilstability  %>% dplyr::select_if(!(names(.) %in% c( "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_soilstability <- tall_soilstability[which(!duplicated(dropcols_soilstability)),] %>%
    dplyr::filter(PrimaryKey %in% pkeys) %>% unique()

  tall_soilstability$DateVisited  <- as.Date(tall_soilstability$FormDate, format = "%Y-%m-%d") # formdate, don't keep hr and min
  tall_soilstability$FormType   <- rep("SoilStability")
  tall_soilstability$source <-"BLM_AIM"
  tall_soilstability$Notes <- rep(NA)
  tall_soilstability$DBKey <- header$DBKey[match(tall_soilstability$PrimaryKey,header$PrimaryKey)]
  # add DBKey and DateLoadedInDb?
  # missing cols from schema
  schema_ss <- read.csv(path_schema)

  schema_ss <- schema_ss %>% dplyr::filter(Table == "dataSoilStability")
  missing_cols <- setdiff(schema_ss$Field, names(tall_soilstability))

  tall_soilstability$ProjectKey <- "BLM_AIM"
  tall_soilstability$LineKey <- NA
  tall_soilstability$SoilStabSubSurface <- NA
  tall_soilstability$Line <- NA
  tall_soilstability$Pos <- NA

  # missing columns with NA
  for (col in missing_cols) {
    tall_soilstability[[col]] <- NA
  }


  saveRDS(tall_soilstability, file.path(path_tall, "soil_stability_tall.rdata"))
  write.csv(tall_soilstability, file.path(path_tall, "soil_stability_tall.csv"), row.names = F)

return(tall_soilstability)

}




###################################
#' Clean Tall species richness AIM
#'
#'after gathering, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_species_richness as a data.frame, the tall_gap file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader as data frame dataHeader
#'
#' @return updated tall file written to path_tall
#'
#' @export
clean_tall_species_richness_aim <- function(tall_species_richness, path_tall, dataHeader){
header <- dataHeader
  #stop rempving DateLoadedInDb and DBKey if add on terra ?
  dropcols_speciesinventory <- tall_sr  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  tall_speciesinventory <- tall_sr[which(!duplicated(dropcols_speciesinventory)),] %>%
    dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
  tall_speciesinventory$ProjectKey <- "BLM_AIM"


  tall_speciesinventory$DateVisited <- as.Date(tall_speciesinventory$FormDate, format = "%Y-%m-%d")

  tall_speciesinventory$DENSITY <- rep(NA)
  tall_speciesinventory$ FormType <- rep("SpeciesInventory")

  tall_speciesinventory$DateLoadedInDb   <- Sys.Date()
  tall_speciesinventory$Notes <- rep(NA)
  tall_speciesinventory$source <- "BLM_AIM"
  tall_speciesinventory$DBKey <- header$DBKey[match(tall_speciesinventory$PrimaryKey,header$PrimaryKey)]
  schema_spr <- read.csv(path_schema)

  schema_spr <- schema_spr %>% dplyr::filter(Table == "dataSpeciesInventory")
  missing_cols <- setdiff(schema_spr$Field, names(tall_speciesinventory))

  # missing columns with NA
  for (col in missing_cols) {
    tall_speciesinventory[[col]] <- NA
  }



  saveRDS(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.rdata"))
  write.csv(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.csv"), row.names = F)

return(tall_speciesinventory)
}




###################################
#' Clean Tall height AIM
#'
#'after gathering, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_height as a data.frame, the tall_height file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader as data frame dataHeader
#' @param tblLPIHeader for aim this remains NULL
#'
#' @return cleaned, in LDC format, file to path_tall
#'
#' @export
clean_tall_height_aim <- function(tall_height, dataHeader, tblLPIHeader,  path_tall){
header <- dataHeader
  #dropcols_height <- tall_height  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  # keep DBKey and DateLoadedInDb if change on terra ?
  dropcols_height <- tall_height  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_height <- tall_height[which(!duplicated(dropcols_height)),] %>%
    dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
  tall_height$ProjectKey <- "BLM_AIM"
  tall_height$FormType <- rep("LPI") # add formtype to all in terra?
  tall_height$source <- "BLM_AIM"
  tall_height$DateVisited <- as.Date(tall_height$FormDate, format = "%Y-%m-%d")
  tall_height$DateLoadedInDb <- Sys.Date()
  tall_height$DBKey <- header$DBKey[match(tall_height$PrimaryKey,header$PrimaryKey)] # adding outside of terra
  tall_height$ShowCheckbox <- NA

  saveRDS(tall_height, file.path(path_tall, "height_tall.rdata"))
  write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = F)
return(tall_height)

}
