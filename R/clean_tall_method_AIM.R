
###################################
#' Clean Tall LPI AIM
#'
#'after gathering lpi, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param lpi as a data.frame, the tall_lpi file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param dataHeader dataHeader as dataframe
#'
#' @return cleaned, in LDC format, tall lpi to the path_tall
#'
#' @export
clean_tall_lpi_aim <- function(lpi, path_tall, dataHeader) {

  message("Starting AIM LPI cleaning...")

  # join to get missing vals
  header_subset <- dataHeader %>%
    dplyr::select(PrimaryKey, DBKey, ProjectKey) %>%
    dplyr::distinct()
# some cleaning to make sure code matches species list
  lpi <- lpi %>%
    dplyr::inner_join(header_subset, by = "PrimaryKey") %>%
    dplyr::filter(!is.na(code)) %>%
    dplyr::mutate(code = toupper(trimws(code)))

  # posit to char to avoid future date issues
  lpi <- lpi %>%
    dplyr::mutate(across(where(~any(class(.x) %in% c("POSIXct", "POSIXt"))), as.character))

  # drop rows without meanigful data
  lpi <- lpi %>%
    dplyr::filter(!(is.na(LineKey) & is.na(layer) & is.na(code) & is.na(PointNbr)))

  # remove duplicates
  message("Removing duplicates...")
  lpi <- lpi %>%
    dplyr::select(-any_of(c("rid", "DateModified", "SpeciesList"))) %>%
    dplyr::distinct() %>%
    terradactylutils3::tdact_remove_duplicates() %>%
    terradactylutils3::tdact_remove_empty(datatype = "lpi")

  # add missing cols
  lpi$source <- "BLM_AIM"
  lpi$DateLoadedInDb <- Sys.Date()
  lpi$ShowCheckbox <- NA

  # save
  # output_file <- file.path(path_tall, "lpi_tall.rds")
  # saveRDS(lpi, output_file)

  message(paste("Successfully saved:", output_file))
  return(lpi)
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
  tall_gap <- tall_gap |>
    # Perform the grouped sum (Equivalent to group_by + mutate)
    transform(
      sumCanCat1 = ave(Gap, PrimaryKey, LineKey, RecType, FUN = \(x) sum(x, na.rm = TRUE))
    ) |>
    # Assign all other metadata
    transform(
      ProjectKey     = "BLM_AIM",
      chckbox        = NA,
      DateVisited    = as.Date(FormDate, format = "%Y-%m-%d"),
      FormType       = "Gap",
      DateLoadedInDb = Sys.Date(),
      source         = "BLM_AIM",
      Notes          = NA,
      DBKey          = header$DBKey[match(PrimaryKey, header$PrimaryKey)]
    )
  # saveRDS(tall_gap, file.path(path_tall, "gap_tall.rdata"))
 # write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = F)
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

  dropcols_soilstability <- tall_soil_stability  %>% dplyr::select_if(!(names(.) %in% c( "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_soil_stability <- tall_soil_stability[which(!duplicated(dropcols_soilstability)),] %>%
    dplyr::filter(PrimaryKey %in% pkeys) %>% unique()

  tall_soil_stability <- tall_soil_stability |>
    transform(
      ProjectKey         = "BLM_AIM",
      DateVisited        = as.Date(FormDate, format = "%Y-%m-%d"),
      FormType           = "SoilStability",
      source             = "BLM_AIM",
      Notes              = NA,
      DBKey              = header$DBKey[match(PrimaryKey, header$PrimaryKey)],
      # New columns added below
      LineKey            = NA,
      SoilStabSubSurface = NA,
      Line               = NA,
      Pos                = NA
    )  # add DBKey and DateLoadedInDb?
  # missing cols from schema
  schema_ss <- read.csv(path_schema)

  schema_ss <- schema_ss %>% dplyr::filter(Table == "dataSoilStability")
  missing_cols <- setdiff(schema_ss$Field, names(tall_soil_stability))



  # missing columns with NA
  for (col in missing_cols) {
    tall_soil_stability[[col]] <- NA
  }


  saveRDS(tall_soil_stability, file.path(path_tall, "soil_stability_tall.rdata"))
  #write.csv(tall_soil_stability, file.path(path_tall, "soil_stability_tall.csv"), row.names = F)

return(tall_soil_stability)

}




###################################
#' Clean Tall species richness AIM
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
clean_tall_species_inventory_aim <- function(tall_species, path_tall, dataHeader){
header <- dataHeader
  #stop rempving DateLoadedInDb and DBKey if add on terra ?
  dropcols_speciesinventory <- tall_sr  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  tall_speciesinventory <- tall_sr[which(!duplicated(dropcols_speciesinventory)),] %>%
    dplyr::filter(PrimaryKey %in% pkeys) %>% unique()
  tall_speciesinventory <- tall_speciesinventory |>
    transform(
      ProjectKey     = "BLM_AIM",
      DateVisited    = as.Date(FormDate, format = "%Y-%m-%d"),
      DENSITY        = NA,
      FormType       = "SpeciesInventory",
      DateLoadedInDb = Sys.Date(),
      Notes          = NA,
      source         = "BLM_AIM",
      DBKey          = header$DBKey[match(PrimaryKey, header$PrimaryKey)]
    )
  schema_spr <- read.csv(path_schema)

  schema_spr <- schema_spr %>% dplyr::filter(Table == "dataSpeciesInventory")
  missing_cols <- setdiff(schema_spr$Field, names(tall_speciesinventory))

  # missing columns with NA
  for (col in missing_cols) {
    tall_speciesinventory[[col]] <- NA
  }



  # saveRDS(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.rdata"))
  #write.csv(tall_speciesinventory, file.path(path_tall, "species_inventory_tall.csv"), row.names = F)

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
  tall_height <- tall_height |>
    transform(
      ProjectKey     = "BLM_AIM",
      FormType       = "LPI",
      source         = "BLM_AIM",
      DateVisited    = as.Date(FormDate, format = "%Y-%m-%d"),
      DateLoadedInDb = Sys.Date(),
      DBKey          = header$DBKey[match(PrimaryKey, header$PrimaryKey)],
      ShowCheckbox   = NA
    )
  # saveRDS(tall_height, file.path(path_tall, "height_tall.rdata"))
  #write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = F)
return(tall_height)

}
