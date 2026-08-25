###################################
#' Clean Tall gap lmf
#'
#'after gathering, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_gap as a data.frame, the tall_gap file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader dataframe dataHeader
#' @param tblGapHeader for lmf, this argument remains NULL
#'
#' @return cleaned, in LDC format, tall gap file to path_tall
#'
#' @export
#' @export
clean_tall_gap_lmf <- function(tall_gap, path_tall, dataHeader, tblGapHeader = NULL) {
dropcols_gap <- tall_gap  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
tall_gap <- tall_gap[which(!duplicated(dropcols_gap)),] %>%
  dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
tall_gap$ProjectKey  <- rep(projectkey)
tall_gap$RecKey <- paste0(tall_gap$PrimaryKey,"_",1)
tall_gap$chckbox <- rep(NA)
tall_gap$FormDate <- header$DateVisited[match(tall_gap$PrimaryKey, header$PrimaryKey)]
tall_gap$FormDate <- as.Date(tall_gap$FormDate, format = "%Y-%m-%d")
tall_gap$DateVisited   <- as.Date(tall_gap$FormDate, format = "%Y-%m-%d")

tall_gap$OtherBasal       <- rep(NA)
tall_gap$FormType        <- rep("Gap")
tall_gap$GapData        <- rep(2)
tall_gap$PerennialsCanopy <- ifelse(tall_gap$RecType == "P", 1,0)
tall_gap$AnnualGrassesCanopy<- ifelse(tall_gap$RecType == "P", 0,1)
tall_gap$AnnualForbsCanopy  <- ifelse(tall_gap$RecType == "P", 0,1)
tall_gap$OtherCanopy       <- rep(NA)
tall_gap$Notes       <- rep(NA)
tall_gap$PerennialsBasal   <- rep(0)
tall_gap$AnnualGrassesBasal<-rep(0)
tall_gap$AnnualForbsBasal  <-rep(0)
tall_gap$DBKey      <- rep(dbname)
tall_gap$DateLoadedInDb    <-rep(todaysDate)
tall_gap$source <- rep("LMF")
tall_gap$Direction <- tall_gap$LineKey
tall_gap <- tall_gap %>% group_by(PrimaryKey, LineKey, RecType) %>% mutate(sumCanCat1 = sum(Gap, na.rm = T)) %>% ungroup()
tall_gap$NoCanopyGaps <- ifelse(tall_gap$RecType == "C" & tall_gap$sumCanCat1 > 0, 1, 0)
tall_gap$NoBasalGaps <- rep(0)
tall_gap$sumCanCat1 <- NULL
return(tall_gap)
saveRDS(tall_gap, file.path(path_tall, "gap_tall.rdata"))
write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = F)}





###################################
#' Clean Tall LPI lmf
#'
#'after gathering lpi, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param lpi as a data.frame, the tall_lpi file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader dataHeader as dataframe
#' @param nonvasc_codes list of nonvascular codes in the data
#'
#' @return cleaned, in LDC format, tall lpi to the path_tall
#'
#' @export
clean_tall_lpi_lmf <- function(lpi, path_tall, dataHeader, nonvasc_codes) {
tall_lpi <- lpi
  dropcols_lpi <- tall_lpi %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] %>%
  dplyr::filter(PrimaryKey %in% pkeys) %>% unique()

# Set classes
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
lpi <- lpi %>%
  dplyr::select(PrimaryKey,  LineKey, tidyselect::everything())

# Drop rows with no data
lpi <- lpi %>%
  dplyr::filter(!(is.na(LineKey) &
                    is.na(layer) &
                    is.na(code) &
                    is.na(ShrubShape) &
                    is.na(PointNbr)))




### remove duplicates and empty rows

tdact_remove_duplicates <- function(indata) {

  cols_to_exclude_from_duplicate_check <- c("DBKey", "DateLoadedInDb")
  data_check <- indata[,!(colnames(indata) %in% cols_to_exclude_from_duplicate_check)]

  # For runspeed, drop columns that are all identical
  vec_varied_cols <- vapply(data_check, function(x) length(unique(x)) > 1, logical(1L))
  vec_varied_cols["PrimaryKey"] <- TRUE # Needed if only one primary key is in the input data
  data_varied_cols_only <- data_check[,vec_varied_cols]

  # get just duplicated rows
  data_duplicated_columns <-
    data_varied_cols_only[duplicated(data_varied_cols_only) | duplicated(data_varied_cols_only, fromLast = T),]

  # give a warning if duplicated rows are found
  if(nrow(data_duplicated_columns) > 0){
    message("Duplicate rows found in input data (columns not printed have no variation in all input data)")

    # Print the data, including DBKey and DateLoaded, but not columsn with only one value in the whole table
    print(indata %>% dplyr::filter(PrimaryKey %in% data_duplicated_columns$PrimaryKey) %>%
            dplyr::select(dplyr::any_of(c(colnames(data_duplicated_columns), cols_to_exclude_from_duplicate_check))) %>%
            dplyr::arrange(PrimaryKey))

    # drop duplicates from output data
    n_duplicates <- sum(duplicated(data_varied_cols_only))
    warning(paste(n_duplicates, "duplicates removed"))
    outdata <- indata[!duplicated(data_varied_cols_only),]
  } else {
    outdata <- indata
  }

  return(outdata)
}


tdact_remove_empty <- function(indata, datatype){

  # Create vector to select which fields are essential
  datacols <- switch(datatype,
                     "gap" = c("GapStart", "GapEnd", "Gap"),
                     "height" = c("Height"), # Species field is very important but not used by all early projects
                     "hzflux" = c("sedimentWeight", "sedimentGperDayByInlet", "sedimentGperDay"),
                     "lpi" = c("layer", "code"),
                     "soilhz" = c("HorizonDepthUpper", "HorizonDepthLower"),
                     "soilstab" = c("Veg", "Rating"),
                     "specinv" = c("Species"),
                     "geosp" = c("Species"),
                     "rh" = c("RH_WaterFlowPatterns", "RH_PedestalsTerracettes", "RH_BareGround",
                              "RH_Gullies", "RH_WindScouredAreas", "RH_LitterMovement",
                              "RH_SoilSurfResisErosion", "RH_SoilSurfLossDeg",
                              "RH_PlantCommunityComp", "RH_Compaction", "RH_FuncSructGroup",
                              "RH_DeadDyingPlantParts", "RH_LitterAmount", "RH_AnnualProd",
                              "RH_InvasivePlants", "RH_ReprodCapabilityPeren",
                              "RH_SoilSiteStability", "RH_BioticIntegrity", "RH_HydrologicFunction"),
                     "unknown"
                     ## Not necessary for geoIndicators or header
  )

  if(length(datacols) == 1){ # if datacols is a vector of length >1 (it usually is) this line is needed
    if(datacols == "unknown"){
      stop("datacols value not recognized")
    }
  }

  message(paste("Checking for rows with no data in all of these columns:", paste(datacols, collapse = ", ")))

  # Select only data columns and count how many are NA
  data_datacols_only <- data.frame(indata[,datacols]) %>% dplyr::mutate(nNA = rowSums(is.na(.)))

  # Rows where all essential values are NA must be eliminated
  vec_hasdata <- data_datacols_only$nNA != length(datacols)

  if(sum(vec_hasdata) < nrow(indata)){
    n_missing <- sum(!vec_hasdata)
    warning(paste(n_missing, "row(s) with no essential data removed"))
  }

  outdata <- indata[vec_hasdata,]

  return(outdata)
}



lpi <- lpi %>% tdact_remove_duplicates() %>% tdact_remove_empty(datatype = "lpi")
tall_lpi <- lpi



#######
dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] %>%
  dplyr::filter(PrimaryKey %in% pkeys) %>% unique()

tall_lpi$source <- rep(source)
tall_lpi$chckbox <- rep(NA)
tall_lpi$FormType <- rep("LPI")
tall_lpi$FormDate <- header$DateVisited[match(tall_lpi$PrimaryKey, header$PrimaryKey)]
tall_lpi$FormDate <- as.Date(tall_lpi$FormDate, format = "%Y-%m-%d")

tall_lpi$Measure <- rep(1)
tall_lpi$LineLengthAmount <- rep(45.72)
tall_lpi$SpacingIntervalAmount <- rep(0.9144)
tall_lpi$SpacingType <- rep("m")

tall_lpi$ShowCheckbox <- rep(0)
tall_lpi$CheckboxLabel <- rep("")
tall_lpi$chckbox <- rep(NA)
tall_lpi$ProjectKey <- rep(projectkey)
tall_lpi$DateLoadedInDb <- todaysDate
tall_lpi$Direction <- rep(NA)
#tall_lpi$SpeciesState <- rep(projectkey)
tall_lpi$RecKey <- paste0(tall_lpi$PrimaryKey,"_",1)
tall_lpi$PointLoc <- ifelse(tall_lpi$LineKey == "nesw", 1, 2)
saveRDS(tall_lpi, file.path(path_tall, "lpi_tall.rdata"))
write.csv(tall_lpi, file.path(path_tall, "lpi_tall.csv"), row.names = F)

return(tall_lpi)
}




###################################
#' Clean Tall height lmf
#'
#'after gathering, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_height as a data.frame, the tall_height file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader as data frame dataHeader
#' @param tblLPIHeader for lmf this remains NULL
#'
#' @return cleaned, in LDC format, file to path_tall
#'
#' @export
clean_tall_height_lmf <- function(tall_height, path_tall, dataHeader, tblLPIHeader = NULL) {

  # immediately filter to relevant Primary Keys
dropcols_height <- tall_height  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
tall_height <- tall_height[which(!duplicated(dropcols_height)),] %>%
  dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
#adding in missing cols
tall_height$type <- ifelse(tall_height$GrowthHabit_measured == "Woody", "woody", "nonwoody")
tall_height$Measure <- rep(1)
tall_height$LineLengthAmount <- rep(45.72)
tall_height$ProjectKey <- rep(projectkey)
tall_height$FormType <- rep("LPI")
tall_height$HeightOption <- rep("ad hoc")
tall_height$ShowCheckbox <- rep(0)
tall_height$source <- rep(source)
tall_height$SpacingIntervalAmount <- rep(3.048)
tall_height$SpacingType <- rep("m")
tall_height$DateVisited <- header$DateVisited[match(tall_height$PrimaryKey, header$PrimaryKey)]
tall_height$DateVisited <- as.Date(tall_height$DateVisited, format = "%Y-%m-%d")
tall_height$DateLoadedInDb <- rep(todaysDate)
tall_height$RecKey <- paste0(tall_height$PrimaryKey,"_",1)
tall_height$FormDate <-header$DateVisited[match(tall_height$PrimaryKey, header$PrimaryKey)]
tall_height$FormDate <- as.Date(tall_height$FormDate, format = "%Y-%m-%d")
tall_height$PointLoc <- ifelse(tall_height$LineKey == "nesw",1,2)
tall_height$Direction <- tall_height$LineKey
tall_height$Chkbox <- rep(NA)
tall_height$CheckboxLabel <- rep("")
saveRDS(tall_height, file.path(path_tall, "height_tall.rdata"))
write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = F)
return(tall_height)
}


###################################
#' Clean Tall species richness lmf
#'
#'after gathering, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_species as a data.frame, the tall_gap file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader as data frame dataHeader
#'
#' @return updated tall file written to path_tall
#'
#' @export
#' @export
clean_tall_species_inventory_lmf <- function(tall_species, path_tall, dataHeader, tblGapHeader = NULL) {
  tall_sr <- tall_species
dropcols_speciesinventory <- tall_sr  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
tall_speciesinventory <- tall_sr[which(!duplicated(dropcols_speciesinventory)),] %>%
  dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
tall_speciesinventory$ProjectKey <- rep(projectkey)
tall_speciesinventory$LineKey <- rep(NA)
tall_speciesinventory$RecKey  <- rep(NA)
tall_speciesinventory$FormDate <- header$DateVisited[match(tall_speciesinventory$PrimaryKey, header$PrimaryKey)]
tall_speciesinventory$DateVisited <- as.Date(tall_speciesinventory$FormDate, format = "%Y-%m-%d")
tall_speciesinventory$FormDate <- tall_speciesinventory$DateVisited
#tall_speciesinventory$DateVisited <- as.character(tall_speciesinventory$DateVisited)

#tall_speciesinventory$FormDate <-   tblSpecRichHeader$DateFormat[match(tall_speciesinventory$PrimaryKey, tblSpecRichHeader$PrimaryKey)]
# tall_speciesinventory$DENSITY  <- tall_speciesinventory$abundance
tall_speciesinventory$ FormType <- rep("SpeciesInventory")

tall_speciesinventory$ SpecRichMethod  <- rep(NA)
tall_speciesinventory$ SpecRichMeasure   <- rep(NA)
tall_speciesinventory$ SpecRichNbrSubPlots <- rep(NA)
tall_speciesinventory$ SpecRich1Container <- rep(NA)
tall_speciesinventory$ SpecRich1Shape    <- rep(NA)
tall_speciesinventory$ SpecRich1Dim1    <- rep(NA)
tall_speciesinventory$ SpecRich1Dim2   <- rep(NA)
tall_speciesinventory$SpecRich1Area   <- rep(NA)
tall_speciesinventory$SpecRich2Container  <- rep(NA)
tall_speciesinventory$SpecRich2Shape     <- rep(NA)
tall_speciesinventory$SpecRich2Dim1    <- rep(NA)
tall_speciesinventory$ SpecRich2Dim2    <- rep(NA)
tall_speciesinventory$ SpecRich2Area   <- rep(NA)
tall_speciesinventory$ SpecRich3Container  <- rep(NA)
tall_speciesinventory$ SpecRich3Shape     <- rep(NA)
tall_speciesinventory$ SpecRich3Dim1    <- rep(NA)
tall_speciesinventory$ SpecRich3Dim2     <- rep(NA)
tall_speciesinventory$ SpecRich3Area    <- rep(NA)
tall_speciesinventory$SpecRich4Container  <- rep(NA)
tall_speciesinventory$SpecRich4Shape     <- rep(NA)
tall_speciesinventory$SpecRich4Dim1     <- rep(NA)
tall_speciesinventory$SpecRich4Dim2     <- rep(NA)
tall_speciesinventory$SpecRich4Area    <- rep(NA)
tall_speciesinventory$SpecRich5Container <- rep(NA)
tall_speciesinventory$SpecRich5Shape    <- rep(NA)
tall_speciesinventory$SpecRich5Dim1     <- rep(NA)
tall_speciesinventory$SpecRich5Dim2     <- rep(NA)
tall_speciesinventory$SpecRich5Area     <- rep(NA)
tall_speciesinventory$SpecRich6Container  <- rep(NA)
tall_speciesinventory$SpecRich6Shape      <- rep(NA)
tall_speciesinventory$SpecRich6Dim1      <- rep(NA)
tall_speciesinventory$SpecRich6Dim2     <- rep(NA)
tall_speciesinventory$ SpecRich6Area     <- rep(NA)
tall_speciesinventory$Notes     <- rep(NA)
tall_speciesinventory$DateLoadedInDb   <- rep(todaysDate)
tall_speciesinventory$source <- rep(source)

saveRDS(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.rdata"))
write.csv(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.csv"), row.names = F)
return(tall_speciesinventory)
}



###################################
#' Clean Tall soil stability lmf
#'
#'after gathering, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_soil_stability as a data.frame, the tall_gap file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader dataframe dataHeader
#' @importFrom dplyr select_if filter %>%
#' @importFrom rlang .data
#' @return updated tall file written to path_tall
#'
#' @export
#' @export
clean_tall_soil_stability_lmf <- function(tall_soil_stability, path_tall, dataHeader, tblGapHeader = NULL) {
  tall_soilstability <- tall_soil_stability
  dropcols_soilstability <- tall_soilstability  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  tall_soilstability <- tall_soilstability[which(!duplicated(dropcols_soilstability)),] %>%
    dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
  tall_soilstability$ProjectKey  <- rep(projectkey)
  tall_soilstability$FormDate <- header$DateVisited[match(tall_soilstability$PrimaryKey, header$PrimaryKey)]
  tall_soilstability$DateVisited  <- as.Date(tall_soilstability$FormDate, format = "%Y-%m-%d") # formdate, don't keep hr and min
  #tall_soilstability$DateVisited <- as.character(tall_soilstability$DateVisited)

  tall_soilstability$FormType   <- rep("SoilStability")
  tall_soilstability$SoilStabSubSurface <- rep(1)
  tall_soilstability$Line  <- rep(NA)
  tall_soilstability$Pos <- rep(NA) #the position on the line was not recorded
  tall_soilstability$source <-rep(source)
  tall_soilstability$LineKey <- rep(NA)
  tall_soilstability$RecKey <- rep(NA)
  tall_soilstability$Hydro <- rep(NA)
  tall_soilstability$Notes <- rep(NA)
  saveRDS(tall_soilstability, file.path(path_tall, "soil_stability_tall.rdata"))
  write.csv(tall_soilstability, file.path(path_tall, "soil_stability_tall.csv"), row.names = F)


}



###################################
#' Clean Tall RH lmf
#'
#'after gathering, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_rangeland_health as a data.frame, the tall_height file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return cleaned, in LDC format, file to path_tall
#'
#' @export
clean_tall_rangeland_health_lmf <- function(tall_rangeland_health, path_tall) {
  tall_rangeland_health <- tall_rangeland_health
  dropcols_rangelandhealth <- tall_rangelandhealth  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  tall_rangelandhealth <- tall_rangelandhealth[which(!duplicated(dropcols_rangelandhealth)),] %>%
    dplyr::filter(PrimaryKey %in% pkeys) %>% unique()

  saveRDS(tall_rangelandhealth, file.path(path_tall, "rangeland_health_tall.rdata"))
  write.csv(tall_rangelandhealth, file.path(path_tall, "rangeland_health_tall.csv"), row.names = F)
  return(tall_rangelandhealth)
}


