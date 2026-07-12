


###################################
#' Clean Tall LPI
#'
#'after gathering lpi, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param lpi as a data.frame, the tall_lpi file
#' @param dataHeader as a data.frame, the dataHeader file produced from terradactylutils2::create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#' @param nonvasc_codes list of nonvascular codes in the LPI data
#' @return updated tall file written to path_tall and a tall_lpi data frame in the console (unless saved to an object)
#'
#' @examples clean_tall_lpi(lpi = terradactyl::gather_lpi(source = source, tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_lpi <- function(lpi, dataHeader, path_tall, nonvasc_codes){
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


  #lpi <- lpi |> tdact_remove_duplicates() |> tdact_remove_empty(datatype = "lpi")
  lpi <- lpi |>
    dplyr::as_tibble() |>
    tdact_remove_duplicates() |>
    tdact_remove_empty(datatype = "lpi")

  tall_lpi <- lpi

  pkeys <- dataHeader$PrimaryKey
  dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # making sure all codes are capital
  tall_lpi$code <- toupper(tall_lpi$code)
  tall_lpi$ProjectKey <- dataHeader$ProjectKey[match(tall_lpi$PrimaryKey, dataHeader$PrimaryKey)]


  lpi <- lpi %>%
    group_by(PrimaryKey, LineKey, PointNbr) %>%

    # Flag if ANY of the bad codes are in the TopCanopy for this group
    mutate(has_bad_top = any(layer == "TopCanopy" & code %in% nonvasc_codes)) %>%

    # Mutate the layers for flagged groups
    mutate(
      layer = case_when(
        !has_bad_top ~ layer,                   # Do nothing if no bad code on top
        layer == "SoilSurface" ~ layer,          # Do nothing to SoilSurface
        layer == "TopCanopy" ~ "Lower1",         # Bad code (TopCanopy) becomes Lower1

        # Shift LowerX layers down by 1
        str_detect(layer, "Lower") ~ {
          old_num <- as.numeric(str_extract(layer, "\\d+"))
          paste0("Lower", old_num + 1)
        },

        TRUE ~ layer
      )
    ) %>%
    # Filter out remaining TopCanopy rows for flagged groups
    filter(!(has_bad_top & layer == "TopCanopy")) %>%
    select(-has_bad_top) %>%
    ungroup()

  saveRDS(tall_lpi, file.path(path_tall, "lpi_tall.rds"))
  #write.csv(tall_lpi, file.path(path_tall, "lpi_tall.csv"), row.names = F)

  return(tall_lpi)
}
####################################

############################################
#' Clean Tall Gap
#'
#'removes and adds columns to the tall_gap file produced using terradactyl::gather_gap that are necessary to produce geofiles
#'
#' @param tall_gap as a data.frame, tall gap file produced from terradactyl::gather_gap
#' @param dataHeader as a data.frame, the dataHeader file produced from terradactylutils2::create_header
#' @param tblGapHeader as a data.frame, the tblGapHeader file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return an updated tall_gap file saved to path_tall and tall_gap in the console, unless saved to an object
#'
#' @examples clean_tall_gap(tall_gap = terradactyl::gather_gap(source = "DIMA", tblGapHeader = tblGapHeader, tblGapDetail = tblGapDetail2), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_gap <- function(tall_gap, dataHeader, path_tall, tblGapHeader){

  dropcols_gap <- tall_gap  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_gap <- tall_gap[which(!duplicated(dropcols_gap)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_gap <- tall_gap |>
    transform(
      # Pulling from dataHeader
      DBKey       = dataHeader$DBKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      ProjectKey  = dataHeader$ProjectKey[match(PrimaryKey, dataHeader$PrimaryKey)],

      # Pulling from tblGapHeader
      DateVisited = tblGapHeader$FormDate[match(PrimaryKey, tblGapHeader$PrimaryKey)],
      Direction   = tblGapHeader$Direction[match(PrimaryKey, tblGapHeader$PrimaryKey)]
    )
  saveRDS(tall_gap, file.path(path_tall, "gap_tall.rds"))
  #write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = F)
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
    transform(
      # Pulling from dataHeader

      ProjectKey  = dataHeader$ProjectKey[match(PrimaryKey, dataHeader$PrimaryKey)]


    )
  saveRDS(tall_soil_stability, file.path(path_tall, "soil_stability_tall.rds"))
  #write.csv(tall_soil_stability, file.path(path_tall, "soil_stability_tall.csv"), row.names = F)
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
clean_tall_species_inventory <- function(tall_species, dataHeader, path_tall){

  dropcols_species <- tall_species  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_species <- tall_species[which(!duplicated(dropcols_species)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_species$DBKey <- dataHeader$DBKey[match(tall_species$PrimaryKey, dataHeader$PrimaryKey)]
  tall_species$Direction <- tblSpecRichHeader$Direction[match(tall_species$PrimaryKey, tblSpecRichHeader$PrimaryKey)]
  #tall_species$DateVisited <- as.character(tall_species$DateVisited)
  tall_species <- tall_species |>
    transform(
      # Pulling from dataHeader

      ProjectKey  = dataHeader$ProjectKey[match(PrimaryKey, dataHeader$PrimaryKey)]


    )

  saveRDS(tall_species, file.path(path_tall, "species_inventory_tall.rds"))  #write.csv(tall_species, file.path(path_tall, "species_inventory_tall.csv"), row.names = F)

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
#'
#' @return a CSV saved to the specified path_tall and an updated tall_height file saved to the console(unless saved to an object)
#'
#' @export
clean_tall_height <- function(tall_height, dataHeader, tblLPIHeader,   path_tall){

  dropcols_height <- tall_height  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "heightList")))
  pkeys <- dataHeader$PrimaryKey
  tall_height <- tall_height[which(!duplicated(dropcols_height)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_height <- tall_height |>
    transform(
      # Mapping from dataHeader
      DBKey          = dataHeader$DBKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      ProjectKey     = dataHeader$ProjectKey[match(PrimaryKey, dataHeader$PrimaryKey)],

      # Mapping from tblLPIHeader
      FormType       = tblLPIHeader$FormType[match(PrimaryKey, tblLPIHeader$PrimaryKey)],
      DateVisited    = tblLPIHeader$FormDate[match(PrimaryKey, tblLPIHeader$PrimaryKey)],
      FormDate       = tblLPIHeader$FormDate[match(PrimaryKey, tblLPIHeader$PrimaryKey)],

      # Constants
      source         = "DIMA",
      DateLoadedInDb = Sys.Date()
    )
  saveRDS(tall_height, file.path(path_tall, "height_tall.rds"))

  #write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = F)

  return(tall_height)
}
##################################











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
#' @param dsn if BLM_AIM dsn to gdb
#' @param data_list if BLM_AIM dsn to gdb
#'
#' @return saves CSV and RDS file of terradactyl gathered files to path gathered_data
#'
#' @export
gather_all <- function(source, path_original_files = NULL, gathered_data, path_tall, path_schema, dsn = NULL, data_list = NULL) {
  # Initialize a list to store the data frames
  tall_files_list <- list()
  if(source == "DIMA"){data_list <- lapply(data_list, function(df) {
    if ("RecKey" %in% names(df)) {
      df$RecKey <- as.character(df$RecKey)
    }
    return(df)
  })}
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
  } else if (exists("data_list") && !is.null(data_list[["tblLPIHeader"]]) && nrow(data_list[["tblLPIHeader"]]) > 0) {
    message("Found DIMA LPI data; processing")

    lpi <- terradactyl::gather_lpi(source = source, tblLPIDetail = data_list[["tblLPIDetail"]], tblLPIHeader = data_list[["tblLPIHeader"]])
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
  } else if (exists("data_list") && !is.null(data_list[["tblGapHeader"]]) && nrow(data_list[["tblGapHeader"]]) > 0) {
    message("Found DIMA gap data; processing")
    tblGapDetail <- data_list[["tblGapDetail"]]
    tblGapDetail2 <- tblGapDetail %>% mutate(LineKey = NULL)
    tblGapDetail2 <- tblGapDetail2 %>% mutate(FormDate = NULL)

    tall_gap <- terradactyl::gather_gap(source = "DIMA", tblGapHeader = data_list[["tblGapHeader"]], tblGapDetail = tblGapDetail2) %>% dplyr::filter(PrimaryKey %in% pkeys)
    if ("DateVisited.x" %in% names(tall_gap)) {
      tall_gap <- tall_gap %>%
        rename(DateVisited = DateVisited.x) %>%
        select(-any_of("DateVisited.y"))
    }
    write.csv(tall_gap, paste0(gathered_data, "/gap_tall.csv"))
    tall_files_list$gap_tall <- tall_gap
  } else if (source == "BLM_AIM") {
    tall_gap <- gather_gap_terradat(dsn = dsn)
    if ("DateVisited.x" %in% names(tall_gap)) {
      tall_gap <- tall_gap %>%
        rename(DateVisited = DateVisited.x) %>%
        select(-any_of("DateVisited.y"))
    }
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
  } else if (exists("data_list") && !is.null(data_list[["tblSoilStabHeader"]]) && nrow(data_list[["tblSoilStabHeader"]]) > 0) {
    message("Found DIMA soil stability data; processing")
    tall_soil_stability <- terradactyl::gather_soil_stability(source = "DIMA", tblSoilStabDetail = data_list[["tblSoilStabDetail"]], tblSoilStabHeader = data_list[["tblSoilStabHeader"]])
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
  } else if (exists("data_list") && !is.null(data_list[["tblSpecRichHeader"]]) && nrow(data_list[["tblSpecRichHeader"]]) > 0) {
    message("Found DIMA species richness data; processing")
    tblSpecRichHeader <- data_list[["tblSpecRichHeader"]]
    tblSpecRichHeader$RecKey <- as.character(tblSpecRichHeader$RecKey)
    tall_species <- terradactyl::gather_species_inventory(source = "DIMA", tblSpecRichDetail = data_list[["tblSpecRichDetail"]], tblSpecRichHeader = tblSpecRichHeader)
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
  } else if (exists("data_list") && !is.null(data_list[["tblLPIHeader"]]) && sum(data_list[["tblLPIDetail"]][["HeightHerbaceous"]], na.rm = T) > 0) {
    tblLPIHeader <- data_list[["tblLPIHeader"]]
    tblLPIHeader$RecKey <- as.character(tblLPIHeader$RecKey)
    tblLPIDetail <- data_list[["tblLPIDetail"]]
    tblLPIDetail$RecKey <- as.character(tblLPIDetail$RecKey)
    tblLPIDetail$SpeciesLowerHerb <- as.character(tblLPIDetail$SpeciesLowerHerb)
    # Convert any column with "Chkbox" or "Checkbox" in its name to character
    tblLPIDetail <- tblLPIDetail |>
      dplyr::mutate(dplyr::across(
        .cols = tidyselect::contains("chkbox", ignore.case = TRUE),
        .fns = as.character
      ))
    tall_height <- terradactyl::gather_height(source = "DIMA", tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader)
    if ("DateVisited.x" %in% names(tall_height)) {
      tall_height <- tall_height %>%
        rename(DateVisited = DateVisited.x) %>%
        select(-any_of("DateVisited.y"))
    }
    write.csv(tall_height, paste0(gathered_data, "/height_tall.csv"))
    tall_files_list$height_tall <- tall_height
  } else if (source == "BLM_AIM") {
    tall_height <- gather_height_terradat(dsn = dsn)
    tall_height <- tall_height %>%
      rename(DateVisited = DateVisited.x) %>%
      select(-any_of("DateVisited.y"))
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
  if (exists("data_list") && !is.null(data_list[["tblBSNE_BoxCollection"]]) && nrow(data_list[["tblBSNE_BoxCollection"]]) > 0) {
    message("DIMA MWAC data found; processing")
    tall_files_list$HorizontalFlux <- terradactylutils3::create_mwac(tblBSNE_BoxCollection = data_list[["tblBSNE_BoxCollection"]], gathered_data = gathered_data)
  } else {
    message("No DIMA MWAC data found")
  }

  # DDT
  if (exists("data_list") && !is.null(data_list[["tblBSNE_TrapCollection"]]) && nrow(data_list[["tblBSNE_TrapCollection"]]) > 0) {
    message("DIMA DDT data found; processing")
    tall_files_list$DustDeposition <- create_ddt(data_list[["tblBSNE_TrapCollection"]], gathered_data = gathered_data)
  } else {
    message("No DIMA DDT data found")
  }

  # Return the collected data frames
  return(tall_files_list)
}




#' clean all data from list or file paths
#'
#' @param data_source source, either "NRI", "BLM_AIM" or "DIMA"
#' @param dataHeader as data frame dataHeader
#' @param path_tall path where cleaned tall files are/will be stored (used for downstream sub-functions)
#' @param subset_to_filter number or numbers to process (used to adjust internal paths)
#' @param gathered_data file path where gathered data CSVs are stored (used if data_list is NULL)
#' @param data_list list of original files in memory (optional)
#' @param gathered_data_list list of gathered files if not putting to gathered_data file path
#' @param nonvasc_codes list of nonvascular codes in the LPI data
#' @export
clean_tall_all <- function(data_source, gathered_data = NULL, dataHeader, path_tall, subset_to_filter = NULL, data_list = NULL, verbose = TRUE, nonvasc_codes = NULL, gathered_data_list = NULL) {

  start_total <- Sys.time()
  tall_files_list <- list()

  # output path tracking for downstream tools
  output_dir <- if (!is.null(subset_to_filter)) {
    file.path(path_tall, "subset", paste0("subset_", subset_to_filter))
  } else {
    path_tall
  }

  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  # Expected element/file names
  tall_file_names <- c("lpi_tall", "height_tall", "gap_tall", "species_inventory_tall",
                       "soil_stability_tall", "rangelandhealth_tall", "header", "soil_horizons_tall",
                       "horizontalflux_tall", "dustdeposition_tall")

  loaded_data <- list()

  # --- HYBRID DATA LOADING BLOCK ---
  for (file_name in tall_file_names) {
    dat <- NULL

    # Method A: Check if it exists in the provided in-memory list
    if (!is.null(gathered_data_list) && !is.null(gathered_data_list[[file_name]])) {
      message("Processing in-memory element: ", file_name, "...")
      load_start <- Sys.time()
      dat <- data.table::as.data.table(gathered_data_list[[file_name]])
      load_end <- Sys.time()
      message("   Finished processing ", file_name, " from list in ", round(difftime(load_end, load_start, units = "secs"), 2), "s")

      # Method B: Fallback to reading the physical CSV file path
    } else if (!is.null(gathered_data)) {
      file_path <- file.path(gathered_data, paste0(file_name, ".csv"))

      if (file.exists(file_path)) {
        message("Reading file from disk: ", file_name, ".csv ...")
        load_start <- Sys.time()
        dat <- data.table::fread(file_path)

        # If running old file workflows, handle subset filtering if the column exists
        if (!is.null(subset_to_filter) && "subset_nbr" %in% names(dat)) {
          dat <- dat[as.numeric(get("subset_nbr")) == as.numeric(subset_to_filter), ]
        }

        load_end <- Sys.time()
        message("   Finished loading ", file_name, " from disk in ", round(difftime(load_end, load_start, units = "secs"), 2), "s")
      }
    }

    # Store data if found by either method
    if (!is.null(dat)) {
      loaded_data[[file_name]] <- dat
    }
  }
  # ----------------------------------

  # get correct suffix based on source
  s_suffix <- data.table::fcase(
    data_source == "NRI", "_nri",
    data_source == "BLM_AIM", "_aim",
    default = ""
  )

  # dynamically process based on source
  run_process <- function(protocol, ...) {
    func_name <- paste0("clean_tall_", protocol, s_suffix)

    if (exists(func_name, where = asNamespace("terradactylutils3"), mode = "function")) {
      actual_func <- getExportedValue("terradactylutils3", func_name)
      message("Running cleanup: ", func_name)
      return(do.call(actual_func, list(...)))
    } else {
      message("Skipping: ", func_name, " (Function not found)")
      return(NULL)
    }
  }

  # standard args
  standard_args <- list(dataHeader = dataHeader, path_tall = output_dir)

  # --- Method-Specific Calls (Extracting Headers dynamically from list or standard args) ---

  # LPI
  # Inside clean_tall_all, right before processing LPI:
  if (!is.null(loaded_data$lpi_tall)) {

    # Safeguard: if it's NULL, convert it to character(0) so %in% doesn't complain downstream
    passed_codes <- if (is.null(nonvasc_codes)) character(0) else nonvasc_codes

    tall_files_list$lpi <- do.call(
      run_process,
      c(list(protocol = "lpi", lpi = loaded_data$lpi_tall, nonvasc_codes = passed_codes), standard_args)
    )
  }

  # Gap
  gap_header <- if(s_suffix %in% c("_nri", "_aim")) NULL else data_list$tblGapHeader
  if (!is.null(loaded_data$gap_tall)) {
    tall_files_list$gap <- do.call(run_process, c(list(protocol = "gap", tall_gap = loaded_data$gap_tall, tblGapHeader = gap_header), standard_args))
  }

  # Height
  lpi_header <- if(s_suffix %in% c("_nri", "_aim")) NULL else data_list$tblLPIHeader
  if (!is.null(loaded_data$height_tall)) {
    tall_files_list$height <- do.call(run_process, c(list(protocol = "height", tall_height = loaded_data$height_tall, tblLPIHeader = lpi_header), standard_args))
  }

  # Soil Stability
  if (!is.null(loaded_data$soil_stability_tall)) {
    tall_files_list$soil_stability <- do.call(run_process, c(list(protocol = "soil_stability", tall_soil_stability = loaded_data$soil_stability_tall), standard_args))
  }

  # Species
  if (!is.null(loaded_data$species_inventory_tall)) {
    tall_files_list$species_inventory <- do.call(run_process, c(list(protocol = "species_inventory", tall_species = loaded_data$species_inventory_tall), standard_args))
  }

  end_total <- Sys.time()
  message("Total process completed in: ", round(difftime(end_total, start_total, units = "mins"), 2), " mins")

  return(tall_files_list)
}
