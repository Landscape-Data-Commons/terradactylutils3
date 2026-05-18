
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
  output_file <- file.path(path_tall, "lpi_tall.rds")
  saveRDS(lpi, output_file)

  #message(paste("Successfully saved:", output_file))
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
#' @export
clean_tall_gap_aim <- function(tall_gap, path_tall, dataHeader, tblGapHeader = NULL) {

  pkeys <- dataHeader$PrimaryKey

  # remove duplicates and certain cols immediately
  ignore_cols <- c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")
  keep_indices <- !duplicated(tall_gap[, !names(tall_gap) %in% ignore_cols, drop = FALSE]) &
    (tall_gap$PrimaryKey %in% pkeys)

  tall_gap <- tall_gap[keep_indices, , drop = FALSE]

  # calc ave for gap groups
  # Create a combined interaction factor key
  group_factor <- paste(tall_gap$PrimaryKey, tall_gap$LineKey, tall_gap$RecType, sep = "_")

  # Calculate sums using rowsum which is apparently faster than ave
  gap_sums <- rowsum(tall_gap$Gap, group_factor, na.rm = TRUE)

  # Map back to the original rows
  tall_gap$sumCanCat1 <- gap_sums[group_factor, 1]

  # map dbkey
  header_lookup <- dataHeader$DBKey
  names(header_lookup) <- dataHeader$PrimaryKey

  # Metadata Assignment
  tall_gap$ProjectKey     <- "BLM_AIM"
  tall_gap$chckbox        <- NA
  tall_gap$DateVisited    <- as.Date(tall_gap$FormDate, format = "%Y-%m-%d")
  tall_gap$FormType       <- "Gap"
  tall_gap$DateLoadedInDb <- Sys.Date()
  tall_gap$source         <- "BLM_AIM"
  tall_gap$Notes          <- NA
  tall_gap$DBKey          <- header_lookup[tall_gap$PrimaryKey]

  # write Files
  if (!dir.exists(path_tall)) dir.create(path_tall, recursive = TRUE)
  saveRDS(tall_gap, file.path(path_tall, "gap_tall.rds"))
  write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = FALSE)

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
#' @importFrom dplyr select_if filter %>%
#' @importFrom rlang .data
#' @return updated tall file written to path_tall
#'
#' @export
#' @export
clean_tall_soil_stability_aim <- function(tall_soil_stability, path_tall, dataHeader, tblGapHeader = NULL) {

  # immediately by filtering to relevant Primary Keys
  pkeys <- dataHeader$PrimaryKey

  # immediately remove duplicates and unnecessary cols
  ignore_cols <- c("rid", "DateModified", "SpeciesList")
  dup_check_data <- tall_soil_stability[, !names(tall_soil_stability) %in% ignore_cols, drop = FALSE]

  # filter rows before doing mutations
  tall_soil_stability <- tall_soil_stability[!duplicated(dup_check_data) & tall_soil_stability$PrimaryKey %in% pkeys, ]

  # assign dbkey
  header_lookup <- dataHeader$DBKey
  names(header_lookup) <- dataHeader$PrimaryKey

  # Metadata assignment
  tall_soil_stability$ProjectKey         <- "BLM_AIM"
  tall_soil_stability$DateVisited        <- as.Date(tall_soil_stability$FormDate, format = "%Y-%m-%d")
  tall_soil_stability$FormType           <- "SoilStability"
  tall_soil_stability$source             <- "BLM_AIM"
  tall_soil_stability$Notes              <- NA
  tall_soil_stability$DBKey              <- header_lookup[tall_soil_stability$PrimaryKey]
  tall_soil_stability$LineKey            <- NA
  tall_soil_stability$SoilStabSubSurface <- NA
  tall_soil_stability$Line               <- NA
  tall_soil_stability$Pos                <- NA

  # hardcoded schema column matching rather than reading a schema and adding a new var
  schema_cols <- c(
    "ProjectKey", "PrimaryKey", "LineKey", "RecKey",
    "DateVisited", "FormDate", "FormType", "SoilStabSubSurface",
    "Line", "Position", "Pos", "Veg",
    "Rating", "Hydro", "Notes", "DBKey",
    "DateLoadedInDb", "source"
  )

  missing_cols <- setdiff(schema_cols, names(tall_soil_stability))

  # vectorized column creation - allocates all missing columns at once
  if (length(missing_cols) > 0) {
    tall_soil_stability[, missing_cols] <- NA
  }

  # save
  if (!dir.exists(path_tall)) dir.create(path_tall, recursive = TRUE)
  saveRDS(tall_soil_stability, file.path(path_tall, "soil_stability_tall.rds"))
  #write.csv(tall_soil_stability, file.path(path_tall, "soil_stability_tall.csv"), row.names = FALSE)

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
#' @export
clean_tall_species_inventory_aim <- function(tall_species, path_tall, dataHeader, tblGapHeader = NULL) {

  # immediately by filter to relevant Primary Keys
  pkeys <- dataHeader$PrimaryKey

  # remove dups and unnecessary cols
  ignore_cols <- c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")
  dup_check_data <- tall_species[, !names(tall_species) %in% ignore_cols, drop = FALSE]

  # filter rows before doing mutations
  tall_species_inventory <- tall_species[!duplicated(dup_check_data) & tall_species$PrimaryKey %in% pkeys, ]

  # 3assign dbkey
  header_lookup <- dataHeader$DBKey
  names(header_lookup) <- dataHeader$PrimaryKey

  # Metadata assignment
  tall_species_inventory$ProjectKey     <- "BLM_AIM"
  tall_species_inventory$DateVisited    <- as.Date(tall_species_inventory$FormDate, format = "%Y-%m-%d")
  tall_species_inventory$DENSITY        <- NA
  tall_species_inventory$FormType       <- "SpeciesInventory"
  tall_species_inventory$DateLoadedInDb <- Sys.Date()
  tall_species_inventory$Notes          <- NA
  tall_species_inventory$source         <- "BLM_AIM"
  tall_species_inventory$DBKey          <- header_lookup[tall_species_inventory$PrimaryKey]

  # hardcoded schema column filtering
  schema_cols <- c(
    "ProjectKey",          "PrimaryKey",          "LineKey",             "RecKey",
    "DateVisited",         "FormDate",            "Species",             "DENSITY",
    "FormType",            "SpecRichMethod",      "SpecRichMeasure",     "SpecRichNbrSubPlots",
    "SpecRich1Container",  "SpecRich1Shape",      "SpecRich1Dim1",       "SpecRich1Dim2",
    "SpecRich1Area",       "SpecRich2Container",  "SpecRich2Shape",      "SpecRich2Dim1",
    "SpecRich2Dim2",       "SpecRich2Area",       "SpecRich3Container",  "SpecRich3Shape",
    "SpecRich3Dim1",       "SpecRich3Dim2",       "SpecRich3Area",       "SpecRich4Container",
    "SpecRich4Shape",      "SpecRich4Dim1",       "SpecRich4Dim2",       "SpecRich4Area",
    "SpecRich5Container",  "SpecRich5Shape",      "SpecRich5Dim1",       "SpecRich5Dim2",
    "SpecRich5Area",       "SpecRich6Container",  "SpecRich6Shape",      "SpecRich6Dim1",
    "SpecRich6Dim2",       "SpecRich6Area",       "Notes",               "DBKey",
    "DateLoadedInDb",      "source"
  )

  missing_cols <- setdiff(schema_cols, names(tall_species_inventory))

  # Vectorized column creation - allocates all missing columns simultaneously
  if (length(missing_cols) > 0) {
    tall_species_inventory[, missing_cols] <- NA
  }

  #save
  if (!dir.exists(path_tall)) dir.create(path_tall, recursive = TRUE)
  saveRDS(tall_species_inventory, file.path(path_tall, "species_inventory_tall.rds"))
  #write.csv(tall_species_inventory, file.path(path_tall, "species_inventory_tall.csv"), row.names = FALSE)

  return(tall_species_inventory)
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
clean_tall_height_aim <- function(tall_height, path_tall, dataHeader, tblLPIHeader = NULL) {

  # immediately filter to relevant Primary Keys
  pkeys <- dataHeader$PrimaryKey

  # removes dups and unnecessary cols
  ignore_cols <- c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")
  dup_check_data <- tall_height[, !names(tall_height) %in% ignore_cols, drop = FALSE]

  # Filter rows before doing mutations
  tall_height <- tall_height[!duplicated(dup_check_data) & tall_height$PrimaryKey %in% pkeys, ]

  # assign dbkey
  header_lookup <- dataHeader$DBKey
  names(header_lookup) <- dataHeader$PrimaryKey

  # Metadata
  tall_height$ProjectKey     <- "BLM_AIM"
  tall_height$FormType       <- "LPI"
  tall_height$source         <- "BLM_AIM"
  tall_height$DateVisited    <- as.Date(tall_height$FormDate, format = "%Y-%m-%d")
  tall_height$DateLoadedInDb <- Sys.Date()
  tall_height$DBKey          <- header_lookup[tall_height$PrimaryKey]
  tall_height$ShowCheckbox   <- NA

  # save
  if (!dir.exists(path_tall)) dir.create(path_tall, recursive = TRUE)
  saveRDS(tall_height, file.path(path_tall, "height_tall.rds"))
  #write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = FALSE)

  return(tall_height)
}
