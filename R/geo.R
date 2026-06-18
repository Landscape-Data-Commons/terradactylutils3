##################################
#' Geofiles
#'
#' creates the geoIndicators and geoSpecies files and writes them to the path_foringest using the path_tall data that was produced using translate_coremethods2. Updated from translate_coremethods from terradactyl_Utils to not require projkey.
#'
#' @param path_foringest path where data for ingest will be exported
#' @param path_tall path to the tall files produced from terradactylutils2::clean_tall_"method"()
#' @param header as a data.frame, the tall header file in path_tall
#' @param path_specieslist path to species lists including the ProjectKey
#' @param template path to an indicator list using graminoid identifiers, currently used while certain agencies use GRASS
#' @param doGSP TRUE unless user does not want a geoSpecies file produced
#' @param calculate_dead Logical. If \code{TRUE} and \code{doGSP} is \code{TRUE} then the accumulated species calculations will differentiate between "live" and "dead" records. Defaults to \code{FALSE}.
#' @param date Optional character string. The date value for the DateLoadedInDb variable. Must be in the format mm/dd/YYYY, e.g. "6/19/2026". Defaults to the date returned by \code{Sys.date()}.
#' @param path_schema file path to LDC schema plan
#' @param digits Number of digits user wants observations rounded to
#' @return geoSpecies and geoIndicators file written to the path_foringest
#'
#'
#' @examples geofiles(path_foringest = path_foringest,path_tall = file.path(path_parent, "Tall"),header = tall_header, path_specieslist =  paste0(path_species,  projkey, ".csv"),path_template = template, digits = 2)
#' @export
geofiles <- function(path_foringest,
                     path_tall,
                     header,
                     path_specieslist,
                     path_schema,
                     template,
                     doGSP = TRUE,
                     calculate_dead = FALSE,
                     ingestion_date = NULL,
                     verbose = FALSE,
                     digits = 6){

  # continuous issue with rdata reading in - making a
  # smart reader
  # This tries multiple extensions and both R binary formats
  smart_read <- function(base_path, base_filename) {
    # Extensions to check in order
    exts <- c(".rdata", ".Rdata", ".RDATA", ".csv", ".CSV")

    for (ext in exts) {
      full_path <- file.path(base_path, paste0(base_filename, ext))

      if (file.exists(full_path)) {
        if (grepl("\\.csv$", ext, ignore.case = TRUE)) {
          return(read.csv(full_path, stringsAsFactors = FALSE))
        } else {
          # Try readRDS first (modern single-object format)
          res <- tryCatch({
            readRDS(full_path)
          }, error = function(e) {
            # If readRDS fails, try load() (workspace image format)
            tmp_env <- new.env()
            load(full_path, envir = tmp_env)
            tmp_env[[ls(tmp_env)[1]]] # Return the first object found
          })
          return(res)
        }
      }
    }
    return(NULL) # If no version of the file exists
  }

  if (is.null(ingestion_date)) {
    ingestion_date <- format(x = Sys.time(), "%m/%d/%Y")
  }

  if (verbose) message("Reading in headers.")

  # Read in the headers because these will be used to filter the incoming data
  # by PrimaryKey before indicators are calculated.
  header_data <- smart_read(path_tall, "header")
  if (is.null(header_data)) stop("Could not find header file in any supported format.")

  tall_filenames <- c("lpi_tall", "gap_tall", "height_tall",
                      "species_inventory_tall", "soil_stability_tall",
                      "rangelandhealth_tall")

  if (verbose) message("Reading in tall data.")
  # Try to read in the data if the file exists.
  # If the file doesn't exist or if the file contains no data corresponding to
  # PrimaryKey values in header this'll return NULL.
  data <- lapply(X = tall_filenames, function(X) {
    current_data <- smart_read(path_tall, X)

    if (!is.null(current_data)) {
      # Remove invalid records which may happen depending on
      # how the Rdata was exported.
      current_data <- current_data |>
        dplyr::filter(PrimaryKey %in% header_data$PrimaryKey)
      # Solving the issue of empty data frames not being handled
      # by lpi_calc()
      if (nrow(current_data) > 0) return(current_data)
    }
    return(NULL)
  }) |>
    setNames(nm = tall_filenames)

  # Keep only data, removing the NULLs.
  data <- data[!sapply(X = data,
                       FUN = is.null)]

  if (verbose) {
    message(paste0("The following data were successfully read in: ",
                   paste(names(data),
                         collapse = ", ")))
  }

  # An empty list to store indicators in as they're calculated.
  # This way, there's a list to make it super easy to combine the indicators
  # using purrr::reduce(dplyr::full_join()) later.
  indicators <- list()

  # For each data type, calculate indicators if there's relevant data available.
  if ("lpi_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating cover indicators")
    }
    indicators[["lpi"]] <- terradactyl::lpi_calc(lpi_tall = data[["lpi_tall"]],
                                                 header = header,
                                                 species_file = path_specieslist,
                                                 verbose = verbose,
                                                 digits = digits) |>
      dplyr::rename(.data = _,
                    # Because lpi_calc() calls it BareSoilCover and the LDC
                    # (rightfully) does not include "Cover"
                    tidyselect::any_of(x = c("BareSoil" = "BareSoilCover")))
  }

  if ("gap_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating gap indicators")
    }
    indicators[["gap"]] <- terradactyl::gap_calc(gap_tall = data[["gap_tall"]],
                                                 header = header,
                                                 verbose = verbose,
                                                 digits = digits)
  }

  if ("height_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating height indicators")
    }
    indicators[["height"]] <- terradactyl::height_calc(height_tall = data[["height_tall"]],
                                                       header = header,
                                                       source = "DIMA",
                                                       species_file = path_specieslist,
                                                       verbose = verbose,
                                                       digits = digits)
  }

  if ("species_inventory_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating species inventory indicators")
    }
    indicators[["species_inventory"]] <- terradactyl::spp_inventory_calc(header = header,
                                                                         spp_inventory_tall = data[["species_inventory_tall"]],
                                                                         species_file = path_specieslist,
                                                                         source = "DIMA",
                                                                         verbose = verbose)
  }

  if ("soil_stability_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating soil stability indicators")
    }
    indicators[["soil_stability"]] <- terradactyl::soil_stability_calc(soil_stability_tall = data[["soil_stability_tall"]],
                                                                       verbose = verbose,
                                                                       digits = digits)
  }

  if ("rangelandhealth_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating rangeland health indicators")
    }
    # No calculations to do with Rangeland Health!
    indicators[["rangeland_health"]] <- data[["rangelandhealth_tall"]]
  }

  # Combine all the calculated indicators then join them to the header.
  all_indicators <- purrr::reduce(.x = indicators,
                                  .f = dplyr::full_join,
                                  by = "PrimaryKey") |>
    dplyr::left_join(x = header,
                     y = _,
                     by = "PrimaryKey")


  # These are used for data management and we're going to drop them.
  internal_use_vars <- c("GlobalID",
                         "created_user",
                         "created_date",
                         "last_edited_user",
                         "last_edited_date",
                         "DateLoadedInDb",
                         "DateLoadedinDB",
                         "rid",
                         "DataErrorChecking",
                         "DataEntry",
                         "DateModified",
                         "FormType",
                         "SpeciesList")

  # Chuck the internal use variables and make sure that only unique records are
  # kept.
  all_indicators <- dplyr::select(.data = all_indicators,
                                  -tidyselect::any_of(internal_use_vars)) |>
    dplyr::distinct(.data = _)

  # We want to replace NA with 0 only for methods that were actually collected
  # and indicators were calculated for.
  prefixes_to_zero <- c()
  if ("lpi_tall" %in% names(data)) {
    prefixes_to_zero <- c(prefixes_to_zero,
                          "AH",
                          "FH")
  }
  if ("species_inventory_tall" %in% names(data)) {
    prefixes_to_zero <- c(prefixes_to_zero,
                          "NumSpp")
  }

  all_indicators <- terradactylutils3::add_indicator_columns(template = template,
                                                             source = "DIMA",
                                                             all_indicators = all_indicators,
                                                             prefixes_to_zero = prefixes_to_zero)



  schema <- read.csv(path_schema) |>
    # I don't know why this would be necessary, but it was used elsewhere so I'm
    # keeping it here just in case it was load-bearing.
    dplyr::distinct()

  geoInd <- translate_schema2(data = all_indicators,
                              schema = schema,
                              datatype = "geoIndicators",
                              dropcols = TRUE,
                              verbose = verbose)

  # add missing gap col

  cols_to_sum <- c("GapCover_25_50", "GapCover_51_100", "GapCover_101_200", "GapCover_200_plus")

  geoInd$GapCover_25_plus <- NA

  # getting incorrect value with sum function, having to do a for loop
  for (i in 1:nrow(geoInd)) {

    row_data <- geoInd[i, cols_to_sum]

    # need to keep the col NA if all gap vals are NA
    if (all(is.na(row_data))) {
      geoInd$GapCover_25_plus[i] <- NA
    } else {
      # sum the cols, removing NA
      geoInd$GapCover_25_plus[i] <- sum(row_data, na.rm = TRUE)
    }
  }


  write.csv(x = geoInd,
            file = file.path(path_foringest,
                             "geoIndicators.csv"),
            row.names = FALSE)

  write.csv(x = all_indicators,
            file = file.path(path_foringest,
                             "geoIndicators_all_indicators.csv"),
            row.names = FALSE)


  #### Accumulated species stuff -----------------------------------------------
  if (doGSP) {
    schema <- read.csv(path_schema) |>
      # I don't know why this would be necessary, but it was used elsewhere so I'm
      # keeping it here just in case it was load-bearing.
      dplyr::distinct()
    species_list <- read.csv(path_specieslist)

    accumulated_species_data <- accumulated_species(lpi_tall = data[["lpi_tall"]],
                                                    height_tall = data[["height_tall"]],
                                                    spp_inventory_tall = data[["species_inventory_tall"]],
                                                    header = header,
                                                    species_file = species_list,
                                                    dead = calculate_dead,
                                                    source = "DIMA",
                                                    digits = digits,
                                                    verbose = verbose) |>
      dplyr::left_join(x = _,
                       y = dplyr::select(.data = header,
                                         tidyselect::any_of(x = c("PrimaryKey",
                                                                  "DateVisited",
                                                                  "DBKey",
                                                                  "ProjectKey"))) |>
                         dplyr::distinct(),
                       by = "PrimaryKey",
                       relationship = "many-to-one") |>
      dplyr::filter(.data = _,
                    !(is.na(AH_SpeciesCover) &
                        is.na(AH_SpeciesCover_n) &
                        is.na(Hgt_Species_Avg) &
                        is.na(Hgt_Species_Avg_n))) |>
      dplyr::mutate(.data = _,
                    DateLoadedInDb = ingestion_date)
    accumulated_species_data <- translate_schema2(data = accumulated_species_data,
                                                  schema = schema,
                                                  datatype = "geoSpecies",
                                                  dropcols = TRUE,
                                                  verbose = verbose)
    accumulated_species_data <- accumulated_species_data %>%
      dplyr::filter(!is.na(Species),
                    (AH_SpeciesCover != 0 | Hgt_Species_Avg != 0))
    write.csv(x = accumulated_species_data,
              file.path(path_foringest,
                        "geoSpecies.csv"),
              row.names = FALSE)
  }
}









##################################
#' Geofiles NRI
#'
#' creates the geoIndicators and geoSpecies files and writes them to the path_foringest using the path_tall data that was produced using translate_coremethods2. Updated from translate_coremethods from terradactyl_Utils to not require projkey.
#'
#' @param path_foringest path where data for ingest will be exported
#' @param path_tall path to the tall files produced from terradactylutils2::clean_tall_"method"()
#' @param header as a data.frame, the tall header file in path_tall
#' @param path_specieslist path to species lists including the ProjectKey
#' @param template path to an indicator list using graminoid identifiers, currently used while certain agencies use GRASS
#' @param doGSP TRUE unless user does not want a geoSpecies file produced
#' @param calculate_dead Logical. If \code{TRUE} and \code{doGSP} is \code{TRUE} then the accumulated species calculations will differentiate between "live" and "dead" records. Defaults to \code{FALSE}.
#' @param date Optional character string. The date value for the DateLoadedInDb variable. Must be in the format mm/dd/YYYY, e.g. "6/19/2026". Defaults to the date returned by \code{Sys.date()}.
#' @param date Optional character string. The date value for the DateLoadedInDb variable. Must be in the format mm/dd/YYYY, e.g. "6/19/2026". Defaults to the date returned by \code{Sys.date()}.
#' @param path_schema file path to LDC schema plan
#' @return geoSpecies and geoIndicators file written to the path_foringest
#'
#'
#' @examples geofiles(path_foringest = path_foringest,path_tall = file.path(path_parent, "Tall"),header = tall_header, path_specieslist =  paste0(path_species,  projkey, ".csv"),path_template = template, digits = 2)
#' @export
geofiles_nri <- function(path_foringest,
                     path_tall,
                     header,
                     path_specieslist,
                     path_schema,
                     template,
                     doGSP = TRUE,
                     calculate_dead = FALSE,
                     ingestion_date = NULL,
                     verbose = FALSE,
                     digits = 6){

  # continuous issue with rdata reading in - making a
  # smart reader
  # This tries multiple extensions and both R binary formats
  smart_read <- function(base_path, base_filename) {
    # Extensions to check in order
    exts <- c(".rdata", ".Rdata", ".RDATA", ".csv", ".CSV")

    for (ext in exts) {
      full_path <- file.path(base_path, paste0(base_filename, ext))

      if (file.exists(full_path)) {
        if (grepl("\\.csv$", ext, ignore.case = TRUE)) {
          return(read.csv(full_path, stringsAsFactors = FALSE))
        } else {
          # Try readRDS first (modern single-object format)
          res <- tryCatch({
            readRDS(full_path)
          }, error = function(e) {
            # If readRDS fails, try load() (workspace image format)
            tmp_env <- new.env()
            load(full_path, envir = tmp_env)
            tmp_env[[ls(tmp_env)[1]]] # Return the first object found
          })
          return(res)
        }
      }
    }
    return(NULL) # If no version of the file exists
  }

  if (is.null(ingestion_date)) {
    ingestion_date <- format(x = Sys.time(), "%m/%d/%Y")
  }

  if (verbose) message("Reading in headers.")

  # Read in the headers because these will be used to filter the incoming data
  # by PrimaryKey before indicators are calculated.
  header_data <- smart_read(path_tall, "header")
  if (is.null(header_data)) stop("Could not find header file in any supported format.")

  tall_filenames <- c("lpi_tall", "gap_tall", "height_tall",
                      "species_inventory_tall", "soil_stability_tall",
                      "rangelandhealth_tall")

  if (verbose) message("Reading in tall data.")
  # Try to read in the data if the file exists.
  # If the file doesn't exist or if the file contains no data corresponding to
  # PrimaryKey values in header this'll return NULL.
  data <- lapply(X = tall_filenames, function(X) {
    current_data <- smart_read(path_tall, X)

    if (!is.null(current_data)) {
      # Remove invalid records which may happen depending on
      # how the Rdata was exported.
      current_data <- current_data |>
        dplyr::filter(PrimaryKey %in% header_data$PrimaryKey)
      # Solving the issue of empty data frames not being handled
      # by lpi_calc()
      if (nrow(current_data) > 0) return(current_data)
    }
    return(NULL)
  }) |>
    setNames(nm = tall_filenames)

  # Keep only data, removing the NULLs.
  data <- data[!sapply(X = data,
                       FUN = is.null)]

  if (verbose) {
    message(paste0("The following data were successfully read in: ",
                   paste(names(data),
                         collapse = ", ")))
  }

  # An empty list to store indicators in as they're calculated.
  # This way, there's a list to make it super easy to combine the indicators
  # using purrr::reduce(dplyr::full_join()) later.
  indicators <- list()

  # For each data type, calculate indicators if there's relevant data available.
  if ("lpi_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating cover indicators")
    }
    indicators[["lpi"]] <- terradactyl::lpi_calc(lpi_tall = data[["lpi_tall"]],
                                                 header = header,
                                                 species_file = path_specieslist,
                                                 verbose = verbose,
                                                 digits = digits) |>
      dplyr::rename(.data = _,
                    # Because lpi_calc() calls it BareSoilCover and the LDC
                    # (rightfully) does not include "Cover"
                    tidyselect::any_of(x = c("BareSoil" = "BareSoilCover")))
  }

  if ("gap_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating gap indicators")
    }
    indicators[["gap"]] <- terradactyl::gap_calc(gap_tall = data[["gap_tall"]],
                                                 header = header,
                                                 verbose = verbose,
                                                 digits = digits)
  }

  if ("height_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating height indicators")
    }
    indicators[["height"]] <- terradactyl::height_calc(height_tall = data[["height_tall"]],
                                                       header = header,
                                                       source = "DIMA",
                                                       species_file = path_specieslist,
                                                       verbose = verbose,
                                                       digits = digits)
  }

  if ("species_inventory_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating species inventory indicators")
    }
    indicators[["species_inventory"]] <- terradactyl::spp_inventory_calc(header = header,
                                                                         spp_inventory_tall = data[["species_inventory_tall"]],
                                                                         species_file = path_specieslist,
                                                                         source = "DIMA",
                                                                         verbose = verbose)
  }

  if ("soil_stability_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating soil stability indicators")
    }
    indicators[["soil_stability"]] <- terradactyl::soil_stability_calc(soil_stability_tall = data[["soil_stability_tall"]],
                                                                       verbose = verbose,
                                                                       digits = digits)
  }

  if ("rangelandhealth_tall" %in% names(data)) {
    if (verbose) {
      message("Calculating rangeland health indicators")
    }
    # No calculations to do with Rangeland Health!
    indicators[["rangeland_health"]] <- data[["rangelandhealth_tall"]]
  }

  # Combine all the calculated indicators then join them to the header.
  all_indicators <- purrr::reduce(.x = indicators,
                                  .f = dplyr::full_join,
                                  by = "PrimaryKey") |>
    dplyr::left_join(x = header,
                     y = _,
                     by = "PrimaryKey")


  # These are used for data management and we're going to drop them.
  internal_use_vars <- c("GlobalID",
                         "created_user",
                         "created_date",
                         "last_edited_user",
                         "last_edited_date",
                         "DateLoadedInDb",
                         "DateLoadedinDB",
                         "rid",
                         "DataErrorChecking",
                         "DataEntry",
                         "DateModified",
                         "FormType",
                         "SpeciesList")

  # Chuck the internal use variables and make sure that only unique records are
  # kept.
  all_indicators <- dplyr::select(.data = all_indicators,
                                  -tidyselect::any_of(internal_use_vars)) |>
    dplyr::distinct(.data = _)

  # We want to replace NA with 0 only for methods that were actually collected
  # and indicators were calculated for.
  prefixes_to_zero <- c()
  if ("lpi_tall" %in% names(data)) {
    prefixes_to_zero <- c(prefixes_to_zero,
                          "AH",
                          "FH")
  }
  if ("species_inventory_tall" %in% names(data)) {
    prefixes_to_zero <- c(prefixes_to_zero,
                          "NumSpp")
  }

  geoInd <- terradactylutils3::add_indicator_columns(template = template,
                                                             source = "DIMA",
                                                             all_indicators = all_indicators,
                                                             prefixes_to_zero = prefixes_to_zero)





  # add missing gap col

  cols_to_sum <- c("GapCover_25_50", "GapCover_51_100", "GapCover_101_200", "GapCover_200_plus")

  geoInd$GapCover_25_plus <- NA

  # getting incorrect value with sum function, having to do a for loop
  for (i in 1:nrow(geoInd)) {

    row_data <- geoInd[i, cols_to_sum]

    # need to keep the col NA if all gap vals are NA
    if (all(is.na(row_data))) {
      geoInd$GapCover_25_plus[i] <- NA
    } else {
      # sum the cols, removing NA
      geoInd$GapCover_25_plus[i] <- sum(row_data, na.rm = TRUE)
    }
  }


  write.csv(x = geoInd,
            file = file.path(path_foringest,
                             "geoIndicators.csv"),
            row.names = FALSE)
  write.csv(x = all_indicators,
            file = file.path(path_foringest,
                             "geoIndicators_all_indicators.csv"),
            row.names = FALSE)

  #### Accumulated species stuff -----------------------------------------------
  if (doGSP) {
    species_list <- read.csv(path_specieslist)
    schema <- read.csv(path_schema)
    accumulated_species_data <- accumulated_species(lpi_tall = data[["lpi_tall"]],
                                                    height_tall = data[["height_tall"]],
                                                    spp_inventory_tall = data[["species_inventory_tall"]],
                                                    header = header,
                                                    species_file = species_list,
                                                    dead = calculate_dead,
                                                    source = "DIMA",
                                                    digits = digits,
                                                    verbose = verbose) |>
      dplyr::left_join(x = _,
                       y = dplyr::select(.data = header,
                                         tidyselect::any_of(x = c("PrimaryKey",
                                                                  "DateVisited",
                                                                  "DBKey",
                                                                  "ProjectKey"))) |>
                         dplyr::distinct(),
                       by = "PrimaryKey",
                       relationship = "many-to-one") |>
      dplyr::filter(.data = _,
                    !(is.na(AH_SpeciesCover) &
                        is.na(AH_SpeciesCover_n) &
                        is.na(Hgt_Species_Avg) &
                        is.na(Hgt_Species_Avg_n))) |>
      dplyr::mutate(.data = _,
                    DateLoadedInDb = ingestion_date)
    accumulated_species_data <- translate_schema2(data = accumulated_species_data,
                                                  schema = schema,
                                                  datatype = "geoSpecies",
                                                  dropcols = TRUE,
                                                  verbose = verbose)
    accumulated_species_data <- accumulated_species_data %>%
      dplyr::filter(!is.na(Species),
                    (AH_SpeciesCover != 0 | Hgt_Species_Avg != 0))

    write.csv(x = accumulated_species_data,
              file.path(path_foringest,
                        "geoSpecies.csv"),
              row.names = FALSE)
  }
}



#' Generate and Merge NRI GeoFiles from Data Subsets
#'
#' This wrapper function iterates through data subsets, runs the core \code{geofiles_nri}
#' function for each, and merges the resulting indicators into a master file.
#'
#' @param path_foringest Character. Path where the final merged GeoFiles will be saved.
#' @param path_tall Character. Path to the directory containing subset folders (e.g., subset/subset_1).
#' @param path_species Character. Base path/prefix for species list CSVs.
#' @param template Character. Path to the Excel template file.
#' @param path_schema Character. Path to the schema file.
#' @param tall_header Data frame. The header data containing \code{ProjectKey} and \code{subset_nbr}.
#' @param doGSP Logical. If \code{TRUE}, processes and merges GeoSpecies files. Default is \code{FALSE}.
#' @importFrom dplyr filter bind_rows
#' @importFrom magrittr %>%
#'
#' @return Generates CSV files in the \code{path_foringest} directory.
#' @export
geofiles_from_subsets_nri <- function(path_foringest,
                                      path_tall,
                                      path_species,
                                      template,
                                      path_schema,
                                      tall_header,
                                      doGSP = FALSE) {
  # Prevent R CMD check notes for unquoted variables
  ProjectKey <- subset_nbr <- NULL
  # 1. Validation and Repair of subset_nbr
  if (!"subset_nbr" %in% colnames(tall_header)) {
    message("Warning: 'subset_nbr' not found in tall_header. Creating column and setting to 100.")
    tall_header$subset_nbr <- 100
  }

  # check for NAs in subset_nbr to prevent file path errors
  if (any(is.na(tall_header$subset_nbr))) {
    message("Warning: NA values found in 'subset_nbr'. Setting NAs to 100.")
    tall_header$subset_nbr[is.na(tall_header$subset_nbr)] <- 100
  }

  # master header setup
  projectkey <- unique(tall_header$ProjectKey)
  subset_indices <- unique(tall_header$subset_nbr)

  # create the subset directory structure within foringest
  path_ingest_subset_root <- file.path(path_foringest, "subset")
  if (!dir.exists(path_ingest_subset_root)) dir.create(path_ingest_subset_root, recursive = TRUE)

    # use projectkey and subset_nbr to run geoind
  lapply(subset_indices, function(s_nbr) {

    message("--- Generating GeoFiles for Subset: ", s_nbr, " ---")

    # subset paths
    current_path_tall   <- file.path(path_tall, "subset", paste0("subset_", s_nbr))
    current_path_ingest <- file.path(path_ingest_subset_root, paste0("subset_", s_nbr))

    if (!dir.exists(current_path_ingest)) dir.create(current_path_ingest, recursive = TRUE)

    # subset header
    subset_header <- tall_header %>%
      dplyr::filter(as.character(subset_nbr) == as.character(s_nbr))

    # geofiles uses readRDS(), so we MUST use saveRDS()
    temp_header_path <- file.path(current_path_tall, "header.Rdata")

    # Ensure the folder exists before saving
    if (!dir.exists(current_path_tall)) dir.create(current_path_tall, recursive = TRUE)
    saveRDS(subset_header, file = temp_header_path)

    for (projkey in projectkey) {
      # Only run if the project actually exists in this subset
      if (!(projkey %in% subset_header$ProjectKey)) next

      path_specieslist <- paste0(path_species, projkey, ".csv")

      # assign doGSP for each subset dynamically
      run_gsp_this_subset <- doGSP
      if (doGSP) {
        lpi_path <- file.path(current_path_tall, "lpi_tall.csv")
        spec_path <- file.path(current_path_tall, "species_inventory_tall.csv")

        # If neither file exists, or if they exist but are empty, we can't run doGSP
        has_lpi <- file.exists(lpi_path) && file.info(lpi_path)$size > 10
        has_spec <- file.exists(spec_path) && file.info(spec_path)$size > 10

        if (!has_lpi && !has_spec) {
          message("  Notice: No LPI or Species Inventory data found for Subset ", s_nbr, ". Disabling doGSP for this loop.")
          run_gsp_this_subset <- FALSE
        }
      }

      # Run the core geofiles function
      geofiles(
        path_foringest   = current_path_ingest,
        path_tall        = current_path_tall,
        header           = subset_header,
        path_specieslist = path_specieslist,
        template         = template,
        path_schema      = path_schema,
        doGSP            = run_gsp_this_subset,
        verbose          = TRUE,
        calculate_dead   = FALSE,
        digits           = 6
      )

      # renaming - explicit environment masking with .data$ to prevent evaluation failures
      geo_file <- file.path(current_path_ingest, "geoIndicators.csv")
      if (file.exists(geo_file)) {
        geoind <- read.csv(geo_file) %>%
          dplyr::filter(.data$ProjectKey == !!projkey)

        new_name <- file.path(current_path_ingest, paste0("geoIndicators_", projkey, "_sub", s_nbr, ".csv"))
        write.csv(geoind, new_name, row.names = FALSE)
        file.remove(geo_file)
      }

      # geosp
      if (run_gsp_this_subset) {
        spec_file <- file.path(current_path_ingest, "geoSpecies.csv")
        if (file.exists(spec_file)) {
          geosp <- read.csv(spec_file) %>%
            dplyr::filter(.data$ProjectKey == !!projkey)

          new_spec_name <- file.path(current_path_ingest, paste0("geoSpecies_", projkey, "_sub", s_nbr, ".csv"))
          write.csv(geosp, new_spec_name, row.names = FALSE)
          file.remove(spec_file)
        }
      }
    }

    # clean up the temp file per subset loop
    if (file.exists(temp_header_path)) file.remove(temp_header_path)
  })

  # merge files to end up with one geo file
  message("\nMerging all subset outputs into master files in path_foringest")

  # Merge GeoIndicators
  all_geo_files <- list.files(path_ingest_subset_root,
                              pattern = "geoIndicators_.*\\.csv",
                              recursive = TRUE, full.names = TRUE)

  if (length(all_geo_files) > 0) {
    master_geo <- lapply(all_geo_files, read.csv) %>% dplyr::bind_rows()
    write.csv(master_geo, file.path(path_foringest, "geoIndicators.csv"), row.names = FALSE)
    file.remove(all_geo_files)
  }

  # Merge GeoSpecies
  if (doGSP) {
    all_spec_files <- list.files(path_ingest_subset_root,
                                 pattern = "geoSpecies_.*\\.csv",
                                 recursive = TRUE, full.names = TRUE)
    if (length(all_spec_files) > 0) {
      master_spec <- lapply(all_spec_files, read.csv) %>% dplyr::bind_rows()
      write.csv(master_spec, file.path(path_foringest, "geoSpecies.csv"), row.names = FALSE)
      file.remove(all_spec_files)
    } else {
      message("Notice: No subset geoSpecies files were generated to merge.")
    }
  }
}



#' Generate and Merge GeoFiles from Data Subsets
#'
#' This wrapper function iterates through data subsets, runs the core \code{geofiles}
#' function for each, and merges the resulting indicators into a master file.
#'
#' @param path_foringest Character. Path where the final merged GeoFiles will be saved.
#' @param path_tall Character. Path to the directory containing subset folders (e.g., subset/subset_1).
#' @param path_species Character. Base path/prefix for species list CSVs.
#' @param template Character. Path to the Excel template file.
#' @param path_schema Character. Path to the schema file.
#' @param tall_header Data frame. The header data containing \code{ProjectKey} and \code{subset_nbr}.
#' @param doGSP Logical. If \code{TRUE}, processes and merges GeoSpecies files. Default is \code{FALSE}.
#'
#' @return Generates CSV files in the \code{path_foringest} directory.
#' @export
geofiles_from_subsets <- function(path_foringest,
                                  path_tall,
                                  path_species,
                                  template,
                                  path_schema,
                                  tall_header,
                                  doGSP = FALSE) {

  # 1. Validation and Repair of subset_nbr
  if (!"subset_nbr" %in% colnames(tall_header)) {
    message("Warning: 'subset_nbr' not found in tall_header. Creating column and setting to 100.")
    tall_header$subset_nbr <- 100
  }

  # check for NAs in subset_nbr to prevent file path errors
  if (any(is.na(tall_header$subset_nbr))) {
    message("Warning: NA values found in 'subset_nbr'. Setting NAs to 100.")
    tall_header$subset_nbr[is.na(tall_header$subset_nbr)] <- 100
  }

  # master header setup
  projectkey <- unique(tall_header$ProjectKey)
  subset_indices <- unique(tall_header$subset_nbr)

  # create the subset directory structure within foringest
  path_ingest_subset_root <- file.path(path_foringest, "subset")
  if (!dir.exists(path_ingest_subset_root)) dir.create(path_ingest_subset_root, recursive = TRUE)

  # use projectkey and subset_nbr to run geoind
  lapply(subset_indices, function(s_nbr) {

    message("--- Generating GeoFiles for Subset: ", s_nbr, " ---")

    # subset paths
    current_path_tall   <- file.path(path_tall, "subset", paste0("subset_", s_nbr))
    current_path_ingest <- file.path(path_ingest_subset_root, paste0("subset_", s_nbr))

    if (!dir.exists(current_path_ingest)) dir.create(current_path_ingest, recursive = TRUE)

    # subset header
    subset_header <- tall_header %>%
      dplyr::filter(as.character(subset_nbr) == as.character(s_nbr))

    # geofiles_nri uses readRDS(), so we MUST use saveRDS()
    temp_header_path <- file.path(current_path_tall, "header.Rdata")

    # Ensure the folder exists before saving (if path_tall is empty)
    if (!dir.exists(current_path_tall)) dir.create(current_path_tall, recursive = TRUE)
    saveRDS(subset_header, file = temp_header_path)

    # delete temp file
    on.exit(if (file.exists(temp_header_path)) file.remove(temp_header_path))

    for (projkey in projectkey) {
      # Only run if the project actually exists in this subset
      if (!(projkey %in% subset_header$ProjectKey)) next

      path_specieslist <- paste0(path_species, projkey, ".csv")

      # Run the core NRI geofiles function
      geofiles(
        path_foringest   = current_path_ingest,
        path_tall        = current_path_tall,
        header           = subset_header,
        path_specieslist = path_specieslist,
        template         = template,
        path_schema      = path_schema,
        doGSP            = doGSP,
        verbose          = TRUE,
        calculate_dead   = FALSE,
        digits           = 6
      )

      # renaming
      geo_file <- file.path(current_path_ingest, "geoIndicators.csv")
      if (file.exists(geo_file)) {
        geoind <- read.csv(geo_file) %>% dplyr::filter(ProjectKey == projkey)
        new_name <- file.path(current_path_ingest, paste0("geoIndicators_", projkey, "_sub", s_nbr, ".csv"))
        write.csv(geoind, new_name, row.names = FALSE)
        file.remove(geo_file)
      }

      # geosp
      if (doGSP) {
        spec_file <- file.path(current_path_ingest, "geoSpecies.csv")
        if (file.exists(spec_file)) {
          geosp <- read.csv(spec_file) %>% dplyr::filter(ProjectKey == projkey)
          new_spec_name <- file.path(current_path_ingest, paste0("geoSpecies_", projkey, "_sub", s_nbr, ".csv"))
          write.csv(geosp, new_spec_name, row.names = FALSE)
          file.remove(spec_file)
        }
      }
    }
  })

  # merge files to end up with one geo file
  message("\nMerging all subset outputs into master files in path_foringest")

  # Merge GeoIndicators
  all_geo_files <- list.files(path_ingest_subset_root,
                              pattern = "geoIndicators_.*\\.csv",
                              recursive = TRUE, full.names = TRUE)

  if (length(all_geo_files) > 0) {
    master_geo <- lapply(all_geo_files, read.csv) %>% dplyr::bind_rows()
    write.csv(master_geo, file.path(path_foringest, "geoIndicators.csv"), row.names = FALSE)
    file.remove(all_geo_files)
  }

  # Merge GeoSpecies
  if (doGSP) {
    all_spec_files <- list.files(path_ingest_subset_root,
                                 pattern = "geoSpecies_.*\\.csv",
                                 recursive = TRUE, full.names = TRUE)
    if (length(all_spec_files) > 0) {
      master_spec <- lapply(all_spec_files, read.csv) %>% dplyr::bind_rows()
      write.csv(master_spec, file.path(path_foringest, "geoSpecies.csv"), row.names = FALSE)
      file.remove(all_spec_files)
    }
  }

}




#' Generate and Merge GeoIndicators from Data Subsets or Lists
#'
#' @param path_foringest Character. Path where the final merged geoIndicators.csv will be saved.
#' @param path_tall Character. Path to the directory containing subset folders (used if tall_files_list is NULL).
#' @param path_species Character. Base path/prefix for species list CSVs.
#' @param template Character. Path to the Excel template file.
#' @param path_schema Character. Path to the schema file.
#' @param tall_header Data frame. The header data containing \code{ProjectKey} and \code{subset_nbr}.
#' @param tall_files_list List. Optional in-memory list of tall datasets.
#'
#' @export
generate_geoIndicators <- function(path_foringest,
                                   path_tall,
                                   path_species,
                                   template,
                                   path_schema,
                                   tall_header,
                                   tall_files_list = NULL) {

  message("=== Starting Geo-Indicators Processing ===")

  projectkey <- unique(tall_header$ProjectKey)

  # --- CHECK DISK FOR SUBSETS ---
  has_subsets <- dir.exists(file.path(path_tall, "subset"))

  if (has_subsets) {
    if ("subset_nbr" %in% colnames(tall_header))
      subset_indices <- unique(tall_header$subset_nbr)
    path_ingest_subset_root <- file.path(path_foringest, "subset_indicators")
    if (!dir.exists(path_ingest_subset_root)) dir.create(path_ingest_subset_root, recursive = TRUE)

    message("Found subset directories. Processing chunks...")
  } else {
    # No subsets exist. Run the entire dataset as a single pass.
    subset_indices <- "root"
    message("No subset directory found. Processing directly from root path_tall...")
    path_ingest_subset_root <- path_foringest
  }

  lapply(subset_indices, function(s_nbr) {

    if (s_nbr == "root") {
      current_path_tall   <- path_tall
      current_path_ingest <- file.path(path_ingest_subset_root, "root_run")
      subset_header       <- tall_header
      message("--- Generating Indicators for Global Dataset ---")
    } else {
      current_path_tall   <- file.path(path_tall, "subset", paste0("subset_", s_nbr))
      current_path_ingest <- file.path(path_ingest_subset_root, paste0("subset_", s_nbr))
      subset_header       <- tall_header %>% dplyr::filter(as.character(subset_nbr) == as.character(s_nbr))
      message("--- Generating Indicators for Subset: ", s_nbr, " ---")
    }

    if (!dir.exists(current_path_ingest)) dir.create(current_path_ingest, recursive = TRUE)

    if (s_nbr != "root") {
      temp_header_path <- file.path(current_path_tall, "header.Rdata")
      if (!dir.exists(current_path_tall)) dir.create(current_path_tall, recursive = TRUE)
      saveRDS(subset_header, file = temp_header_path)
      eval(substitute(on.exit(if (file.exists(P)) file.remove(P), add = TRUE), list(P = temp_header_path)))
    }

    for (projkey in projectkey) {
      if (!(projkey %in% subset_header$ProjectKey)) next
      path_specieslist <- paste0(path_species, projkey, ".csv")

      geofiles(
        path_foringest   = current_path_ingest,
        path_tall        = current_path_tall,
        header           = subset_header,
        path_specieslist = path_specieslist,
        template         = template,
        path_schema      = path_schema,
        doGSP            = FALSE,
        verbose          = TRUE,
        calculate_dead   = FALSE,
        digits           = 6
      )

      geo_file <- file.path(current_path_ingest, "geoIndicators.csv")
      if (file.exists(geo_file)) {
        geoind <- read.csv(geo_file) %>% dplyr::filter(ProjectKey == projkey)

        suffix <- if(s_nbr == "root") "" else paste0("_sub", s_nbr)
        new_name <- file.path(current_path_ingest, paste0("geoIndicators_", projkey, suffix, ".csv"))

        write.csv(geoind, new_name, row.names = FALSE)
        file.remove(geo_file)
      }
    }
  })

  message("\nMerging outputs into master file...")
  all_geo_files <- list.files(path_ingest_subset_root, pattern = "geoIndicators_.*\\.csv", recursive = TRUE, full.names = TRUE)

  if (length(all_geo_files) > 0) {
    master_geo <- lapply(all_geo_files, read.csv) %>% dplyr::bind_rows()
    write.csv(master_geo, file.path(path_foringest, "geoIndicators.csv"), row.names = FALSE)

    # --- SAFE CLEANUP FIX ---
    if (has_subsets) {
      # Safe to delete the temporary wrapper folder
      unlink(path_ingest_subset_root, recursive = TRUE)
    } else {
      # ONLY delete the nested "root_run" folder, leaving your path_foringest pristine!
      unlink(file.path(path_foringest, "root_run"), recursive = TRUE)
    }
  }
}


#' Generate and Merge GeoSpecies from Data Subsets or Lists
#'
#' @param path_foringest Character. Path where the final merged geoSpecies.csv will be saved.
#' @param path_tall Character. Path to the directory containing subset folders
#' @param path_species Character. Base path/prefix for species list CSVs.
#' @param template Character. Path to the Excel template file.
#' @param path_schema Character. Path to the schema file.
#' @param tall_header Data frame. The header data containing \code{ProjectKey} and \code{subset_nbr}.
#' @export
generate_geoSpecies <- function(path_foringest,
                                path_tall,
                                path_species,
                                template,
                                path_schema,
                                tall_header,
                                verbose = TRUE,
                                calculate_dead = FALSE,
                                digits = 6,
                                ingestion_date = NULL) {

  message("=== Starting Geo-Species (GSP) Processing ===")

  projectkey <- unique(tall_header$ProjectKey)

  # --- CHECK DISK FOR SUBSETS ---
  has_subsets <- dir.exists(file.path(path_tall, "subset"))

  if (has_subsets) {
    if ("subset_nbr" %in% colnames(tall_header))
      subset_indices <- unique(tall_header$subset_nbr)
    path_ingest_subset_root <- file.path(path_foringest, "subset_indicators")
    if (!dir.exists(path_ingest_subset_root)) dir.create(path_ingest_subset_root, recursive = TRUE)

    message("Found subset directories. Processing chunks...")
  } else {
    # No subsets exist. Run the entire dataset as a single pass.
    subset_indices <- "root"
    message("No subset directory found. Processing directly from root path_tall...")
    path_ingest_subset_root <- path_foringest
  }

  lapply(subset_indices, function(s_nbr) {

    if (s_nbr == "root") {
      current_path_tall   <- path_tall
      current_path_ingest <- file.path(path_ingest_subset_root, "root_run")
      subset_header       <- tall_header
      message("--- Generating GeoSpecies for Global Dataset ---")
    } else {
      current_path_tall   <- file.path(path_tall, "subset", paste0("subset_", s_nbr))
      current_path_ingest <- file.path(path_ingest_subset_root, paste0("subset_", s_nbr))
      subset_header       <- tall_header %>% dplyr::filter(as.character(subset_nbr) == as.character(s_nbr))
      message("--- Generating GeoSpecies for Subset: ", s_nbr, " ---")
    }

    if (!dir.exists(current_path_ingest)) dir.create(current_path_ingest, recursive = TRUE)

    # Only manage/save an environment header file if we are running real subset chunks
    if (s_nbr != "root") {
      temp_header_path <- file.path(current_path_tall, "header.Rdata")
      if (!dir.exists(current_path_tall)) dir.create(current_path_tall, recursive = TRUE)
      saveRDS(subset_header, file = temp_header_path)

      eval(substitute(on.exit(if (file.exists(P)) file.remove(P), add = TRUE), list(P = temp_header_path)))
    }

    for (projkey in projectkey) {
      if (!(projkey %in% subset_header$ProjectKey)) next
      path_specieslist <- paste0(path_species, projkey, ".csv")

      # Smart Reader function definition
      smart_read <- function(base_path, base_filename) {
        exts <- c(".rdata", ".Rdata", ".RDATA", ".csv", ".CSV")
        for (ext in exts) {
          full_path <- file.path(base_path, paste0(base_filename, ext))
          if (file.exists(full_path)) {
            if (grepl("\\.csv$", ext, ignore.case = TRUE)) {
              return(read.csv(full_path, stringsAsFactors = FALSE))
            } else {
              res <- tryCatch({
                readRDS(full_path)
              }, error = function(e) {
                tmp_env <- new.env()
                load(full_path, envir = tmp_env)
                tmp_env[[ls(tmp_env)[1]]]
              })
              return(res)
            }
          }
        }
        return(NULL)
      }

      if (is.null(ingestion_date)) {
        ingestion_date <- format(x = Sys.time(), "%m/%d/%Y")
      }

      if (verbose) message("Reading in headers.")

      # FIX: Read from current_path_tall so subset chunks are actually respected
      header_data <- smart_read(current_path_tall, "header")
      if (is.null(header_data)) {
        # Fall back to incoming subset_header object directly if file isn't on disk yet
        header_data <- subset_header
      }

      tall_filenames <- c("lpi_tall", "gap_tall", "height_tall",
                          "species_inventory_tall", "soil_stability_tall",
                          "rangelandhealth_tall")

      if (verbose) message("Reading in tall data.")

      # FIX: Read from current_path_tall instead of path_tall
      data <- lapply(X = tall_filenames, function(X) {
        current_data <- smart_read(current_path_tall, X)

        if (!is.null(current_data)) {
          current_data <- current_data |>
            dplyr::filter(PrimaryKey %in% header_data$PrimaryKey)
          if (nrow(current_data) > 0) return(current_data)
        }
        return(NULL)
      }) |>
        setNames(nm = tall_filenames)

      data <- data[!sapply(X = data, FUN = is.null)]

      if (verbose) {
        message(paste0("The following data were successfully read in: ",
                       paste(names(data), collapse = ", ")))
      }

      schema <- read.csv(path_schema) |> dplyr::distinct()
      species_list <- read.csv(path_specieslist)

      # Core calculation execution
      accumulated_species_data <- accumulated_species(lpi_tall = data[["lpi_tall"]],
                                                      height_tall = data[["height_tall"]],
                                                      spp_inventory_tall = data[["species_inventory_tall"]],
                                                      header = subset_header, # Use local slice loop header
                                                      species_file = species_list,
                                                      dead = calculate_dead,
                                                      source = "DIMA",
                                                      digits = digits,
                                                      verbose = verbose) |>
        dplyr::left_join(x = _,
                         y = dplyr::select(.data = subset_header,
                                           tidyselect::any_of(x = c("PrimaryKey",
                                                                    "DateVisited",
                                                                    "DBKey",
                                                                    "ProjectKey"))) |>
                           dplyr::distinct(),
                         by = "PrimaryKey",
                         relationship = "many-to-one") |>
        dplyr::filter(.data = _,
                      !(is.na(AH_SpeciesCover) &
                          is.na(AH_SpeciesCover_n) &
                          is.na(Hgt_Species_Avg) &
                          is.na(Hgt_Species_Avg_n))) |>
        dplyr::mutate(.data = _, DateLoadedInDb = ingestion_date)

      accumulated_species_data <- translate_schema2(data = accumulated_species_data,
                                                    schema = schema,
                                                    datatype = "geoSpecies",
                                                    dropcols = TRUE,
                                                    verbose = verbose)

      accumulated_species_data <- accumulated_species_data %>%
        dplyr::filter(!is.na(Species),
                      (AH_SpeciesCover != 0 | Hgt_Species_Avg != 0))

      # FIX: Write directly to current_path_ingest so the renaming step below can see it!
      write.csv(x = accumulated_species_data,
                file = file.path(current_path_ingest, "geoSpecies.csv"),
                row.names = FALSE)

      # Rename subset output
      spec_file <- file.path(current_path_ingest, "geoSpecies.csv")
      if (file.exists(spec_file)) {
        geosp <- read.csv(spec_file) %>% dplyr::filter(ProjectKey == projkey)

        suffix <- if(s_nbr == "root") "" else paste0("_sub", s_nbr)
        new_spec_name = file.path(current_path_ingest, paste0("geoSpecies_", projkey, suffix, ".csv"))

        write.csv(geosp, new_spec_name, row.names = FALSE)
        file.remove(spec_file)
      }


    }
  })

  # Master Merge
  message("\nMerging all subset species into master file...")
  all_spec_files <- list.files(path_ingest_subset_root, pattern = "geoSpecies_.*\\.csv", recursive = TRUE, full.names = TRUE)

  if (length(all_spec_files) > 0) {
    master_spec <- lapply(all_spec_files, read.csv) %>% dplyr::bind_rows()
    write.csv(master_spec, file.path(path_foringest, "geoSpecies.csv"), row.names = FALSE)

    # Safe cleanup separation
    if (has_subsets) {
      unlink(path_ingest_subset_root, recursive = TRUE)
    } else {
      unlink(file.path(path_foringest, "root_run"), recursive = TRUE)
    }
  }
}


