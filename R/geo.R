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
                                                 digits = digits,
                                                 indicators_vars = list(
                                                   first = list(
                                                     c("Duration", "GrowthHabitSub"),
                                                     c("Duration", "ForbGraminoid"),
                                                     c("GrowthHabitSub"),
                                                     c("SG_Group"),
                                                     c("Noxious", "Duration", "GrowthHabitSub"),
                                                     c("between_plant"),
                                                     c("Litter"),
                                                     c("Lichen"),
                                                     c("TotalLitter"),
                                                     c("Moss")
                                                   ),
                                                   any = list(
                                                     c("Plant"),
                                                     c("GrowthHabit"),
                                                     c("GrowthHabitSub"),
                                                     c("Duration", "GrowthHabit"),
                                                     c("Duration", "GrowthHabitSub"),
                                                     c("Duration", "ForbGraminoid"),
                                                     c("ShrubSucculent"),
                                                     c("Noxious"),
                                                     c("Litter"),
                                                     c("TotalLitter"),
                                                     c("SG_Group"),
                                                     c("SG_Group", "Live"),
                                                     c("Grass"),
                                                     c("Duration", "Grass"),
                                                     c("C3", "Duration", "Grass"),
                                                     c("C4", "Duration", "Grass"),
                                                     c("Native"),
                                                     c("Invasive"),
                                                     c("Invasive", "Duration", "GrowthHabitSub"),
                                                     c("Invasive", "Duration", "ShrubSucculent"),
                                                     c("Invasive", "Duration", "Grass"),
                                                     c("Invasive", "Duration", "ForbGrass"),
                                                     c("Conifer"),
                                                     c("PJ"),
                                                     c("Moss"),
                                                     c("Rock"),
                                                     c("Biocrust"),
                                                     c("Lichen")
                                                   ),
                                                   basal = list(
                                                     c("Duration", "Grass"),
                                                     c("Plant")
                                                   )
                                                 ),apply_species_adjustment = TRUE
                                                ) |>
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
    height_tall <- data[["height_tall"]]
    # Extract project key as fallback
    projectkey <- unique(header$ProjectKey)

    # Read species file first so we can check its column names
    species_file <- read_csv(path_specieslist)

    # Check if "SpeciesState" exists in BOTH header AND species_file
    if ("SpeciesState" %in% names(header) && "SpeciesState" %in% names(species_file)) {
      # Keep existing SpeciesState values in both
      # (No extra mutation needed for species_file unless you need to pull unique header states)

    } else {
      # Fallback: Assign ProjectKey as SpeciesState to both data frames
      header <- header %>%
        mutate(SpeciesState = projectkey)

      species_file <- species_file %>%
        mutate(SpeciesState = projectkey)
    }


    indicators_vars = list(
      first = list(
        c("Duration", "GrowthHabitSub"),
        c("Duration", "ForbGraminoid"),
        c("GrowthHabitSub"),
        c("SG_Group"),
        c("Noxious", "Duration", "GrowthHabitSub"),
        c("between_plant"),
        c("Litter"),
        c("Lichen"),
        c("TotalLitter"),
        c("Moss")
      ),
      any = list(
        c("Plant"),
        c("GrowthHabit"),
        c("GrowthHabitSub"),
        c("Duration", "GrowthHabit"),
        c("Duration", "GrowthHabitSub"),
        c("Duration", "ForbGraminoid"),
        c("ShrubSucculent"),
        c("Noxious"),
        c("Litter"),
        c("TotalLitter"),
        c("SG_Group"),
        c("SG_Group", "Live"),
        c("Grass"),
        c("Duration", "Grass"),
        c("C3", "Duration", "Grass"),
        c("C4", "Duration", "Grass"),
        c("Native"),
        c("Invasive"),
        c("Invasive", "Duration", "GrowthHabitSub"),
        c("Invasive", "Duration", "ShrubSucculent"),
        c("Invasive", "Duration", "Grass"),
        c("Invasive", "Duration", "ForbGrass"),
        c("Conifer"),
        c("PJ"),
        c("Moss"),
        c("Rock"),
        c("Biocrust"),
        c("Lichen")
      ),
      basal = list(
        c("Duration", "Grass"),
        c("Plant")
      )
    )

    species_code_var = "SpeciesCode"
    generic_species_file = NULL


    nonstandard_indicator_lookup <- c("FH_BareSoilCover" = "BareSoilCover",
                                      "AH_SagebrushLiveCover" = "AH_SagebrushCover_Live",
                                      "AH_BasalPlantCover" = "AH_BasalCover")
    #### Grouping variables lists ------------------------------------------------


    extraneous_names <- setdiff(x = names(indicators_vars),
                                y = c("any", "first", "basal"))


    indicators_vars <- indicators_vars[intersect(x = names(indicators_vars),
                                                 y = c("any", "first", "basal"))]



    indicators_vars_lists <- sapply(X = indicators_vars, FUN = is.list)


    indicators_vars_character <- unlist(indicators_vars) |>
      sapply(FUN = is.character) # Switched to is.character to accurately validate strings


    variable_groups <- indicators_vars


    #### Handling header and raw data ############################################


    lpi_tall_header <- dplyr::left_join(x = dplyr::select(.data = header,
                                                          tidyselect::any_of(c("PrimaryKey",
                                                                               "State",
                                                                               "County"))),
                                        y = height_tall,
                                        relationship = "one-to-many",
                                        by = "PrimaryKey")

    if (verbose) {
      message("Checking species_file and reading in as necessary.")
    }

    if (is.character(species_file)) {
      current_species_file_extension <- tools::file_ext(species_file)

      if (nchar(current_species_file_extension) == 0) {
        stop("When species_file is a character string, it must be a filepath to either a CSV or a GDB (geodatabase).")
      } else if (current_species_file_extension %in% c("CSV", "csv")) {
        if (!file.exists(species_file)) {
          stop(paste0("The provided species_file value, ", species_file, ", points to a file that does not exist."))
        }
        species_list <- read.csv(file = species_file, stringsAsFactors = FALSE)
      } else if (current_species_file_extension %in% c("GDB", "gdb")) {
        species_list <- species_read_aim(dsn = species_file, verbose = verbose)
      }
    } else if (is.data.frame(species_file)) {
      species_list <- species_file
    } else {
      stop("species_file must either be a filepath to a CSV or a GDB file or a data frame.")
    }

    if (verbose) {
      message("Attempting to join the species list to the LPI data.")
    }

    lpi_species <- species_join(data = sf::st_drop_geometry(lpi_tall_header),
                                data_code = "Species",
                                species_file = species_list,
                                species_code = species_code_var,
                                species_growth_habit_code = "GrowthHabitSub",
                                species_duration = "Duration",
                                species_property_vars = c("GrowthHabit",
                                                          "GrowthHabitSub",
                                                          "Duration",
                                                          "Family",
                                                          "SG_Group",
                                                          "HigherTaxon",
                                                          "Nonnative",
                                                          "Invasive",
                                                          "Noxious",
                                                          "SpecialStatus",
                                                          "Photosynthesis",
                                                          "PJ",
                                                          "CurrentPLANTSCode"),
                                growth_habit_file = "",
                                growth_habit_code = "Code",
                                overwrite_generic_species = FALSE,
                                generic_species_file = generic_species_file,
                                update_species_codes = FALSE,
                                by_species_key = FALSE,
                                check_species = FALSE,
                                verbose = verbose)

    ##### Sanitization/harmonization #############################################
    data = lpi_species
    fail_on_missing = FALSE

    # This is a list of all the various bits of definitions for modifying the
    # species attributes in accordance with AIM definitions
    definitions_list <- terradactyl::lpi_indicator_definitions()

    if (verbose) {
      message("Harmonizing species characteristics with AIM indicator needs.")
    }

    # Let's check for the required variables for all these.
    # If any are missing, we can warn the user that those variables will be
    # created but populated with NA and so no indicators that involve them will
    # be calculated.
    expected_variables <- c("GrowthHabit",
                            "GrowthHabitSub",
                            "Duration",
                            "Family",
                            "HigherTaxon",
                            "Nonnative",
                            "Invasive",
                            "Noxious",
                            "SpecialStatus",
                            "Photosynthesis",
                            "PJ",
                            "chckbox")

    missing_expected_variables <- setdiff(x = expected_variables,
                                          names(data))

    if (length(missing_expected_variables) > 0) {
      if (fail_on_missing) {
        stop(paste0("The provided species information does not contain all expected variables required for the standard set of indicators. Set fail_on_missing = FALSE to skip indicators which cannot be calculated. The variables in question are: ",
                    paste(missing_expected_variables,
                          collapse = ", ")))
      }
      warning(paste0("The provided species information does not contain all expected variables required for the standard set of indicators. Indicators which depend on those variables will not be calculated. The variables in question are: ",
                     paste(missing_expected_variables,
                           collapse = ", ")))
      # This makes a new data frame without any data in it consisting of only the
      # missing variables and a number of rows equal to the number of lpi_species
      # records then binds them together.
      data <- matrix(nrow = nrow(data),
                     ncol = length(missing_expected_variables)) |>
        as.data.frame() |>
        setNames(object = _,
                 nm = missing_expected_variables) |>
        dplyr::bind_cols(data,
                         .x = _)
    }


    #### Duration ----------
    if (all(c("Duration") %in% names(data))) {
      data <- dplyr::mutate(.data = data,
                            Duration = dplyr::case_when(grepl(x = Duration,
                                                              pattern = "perennial",
                                                              ignore.case = TRUE) ~ "Peren",
                                                        grepl(x = Duration,
                                                              pattern = "(annual)|(biennial)",
                                                              ignore.case = TRUE) ~ "Ann",
                                                        is.na(Duration) ~ "duration_irrelevant",
                                                        .default = Duration)
      )
    }

    #### GrowthHabit ------------
    if (all(c("GrowthHabit") %in% names(data))) {
      data <- dplyr::mutate(.data = data,
                            GrowthHabit = dplyr::case_when(grepl(x = GrowthHabit,
                                                                 pattern = "^non-?woody$",
                                                                 ignore.case = TRUE) ~ "NonWoody",
                                                           grepl(x = GrowthHabitSub,
                                                                 pattern = "^non-?vascular$",
                                                                 ignore.case = TRUE) ~ "Nonvascular",
                                                           # This removes sedges from consideration???
                                                           # Maybe an artifact of trying to avoid spitting
                                                           # out unused indicators
                                                           # GrowthHabitSub == "Sedge" ~ "growthhabit_irrelevant",
                                                           # For first-hit calculations
                                                           # is.na(GrowthHabit) ~ "growthhabit_irrelevant",
                                                           .default = GrowthHabit)
      )
    }

    #### GrowthHabitSub -----------
    if (all(c("GrowthHabitSub") %in% names(data))) {
      data <- dplyr::mutate(.data = data,
                            GrowthHabitSub = dplyr::case_when(grepl(x = GrowthHabitSub,
                                                                    pattern = "forb",
                                                                    ignore.case = TRUE) ~ "Forb",
                                                              grepl(x = GrowthHabitSub,
                                                                    pattern = "^sub-?shrub$",
                                                                    ignore.case = TRUE) ~ "SubShrub",
                                                              # Not sure why we're removing non-vasculars??
                                                              # Maybe an artifact of trying to avoid spitting
                                                              # out unused indicators. Blame Alaska.
                                                              grepl(x = GrowthHabitSub,
                                                                    pattern = "^non-?vascular$",
                                                                    ignore.case = TRUE) ~ "growthhabitsub_irrelevant",
                                                              # Anyway, doing the exact same to moss
                                                              grepl(x = GrowthHabitSub,
                                                                    pattern = "^moss$",
                                                                    ignore.case = TRUE) ~ "growthhabitsub_irrelevant",
                                                              # And to lichen
                                                              grepl(x = GrowthHabitSub,
                                                                    pattern = "^lichen$",
                                                                    ignore.case = TRUE) ~ "growthhabitsub_irrelevant",
                                                              # For first-hit calculations
                                                              # is.na(GrowthHabit) ~ "growthhabitsub_irrelevant",
                                                              .default = GrowthHabitSub)
      )
    }

    #### Plant --------------
    if (all(c("GrowthHabit", "GrowthHabitSub", "Species") %in% names(data))) {
      data <- dplyr::mutate(.data = data,
                            Plant = dplyr::case_when(
                              (!(GrowthHabitSub %in% c("growthhabitsub_irrelevant")) &
                                 GrowthHabit != "Nonvascular" &
                                 stringi::stri_length(Species) >= 3) ~ "Plant",

                              .default = NA
                            )
      )
    }



    lpi_species <- data


    # check for nonumerics
    non_numeric_count <- sum(is.na(suppressWarnings(as.numeric(lpi_species$Height))) & !is.na(lpi_species$Height))

    if (non_numeric_count > 0) {
      warning(paste(
        "Warning:", non_numeric_count,
        "non-numeric value(s) found in the 'Height' column. Converting them to NA."
      ))

      # Convert the column to numeric (coerces characters/strings to NA)
      lpi_species <- lpi_species %>%
        mutate(Height = suppressWarnings(as.numeric(Height)))
    }
    moss_lichen_codes <- c("LC", "2LICHN", "2LICHN1", "VL", "M", "2MOSS", "2MOSS1")

    lpi_species_filtered <- lpi_species %>%
      filter(
        (
          is.na(Species) |
            Species %in% c("None", "N", "") |
            Plant %in% "Plant" |
            # Keep if GrowthHabitSub is NA but HigherTaxon is NOT liverwort/moss
            (is.na(GrowthHabitSub) & !HigherTaxon %in% c("liverwort", "moss"))
        ) &
          # Filter out specified moss and lichen species codes
          !Species %in% moss_lichen_codes
      )

    height_tall <- lpi_species_filtered %>%
      # Keep only the columns that are present in the height_tall dataframe
      select(any_of(names(height_tall)))
species_file$SpeciesState <- NULL
height_tall$SpeciesState <- NULL
    indicators[["height"]] <- terradactyl::height_calc(height_tall = height_tall,
                                                       header = header,
                                                       source = "DIMA",
                                                       species_file = species_file,
                                                       verbose = verbose,
                                                       digits = digits)
  }
  header$SpeciesState <-NA

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
header$State <- NA
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
                                                 digits = digits,
                                                indicators_vars =  list(
                                                   first = list(
                                                     c("Duration", "GrowthHabitSub"),
                                                     c("Duration", "ForbGraminoid"),
                                                     c("GrowthHabitSub"),
                                                     c("SG_Group"),
                                                     c("Noxious", "Duration", "GrowthHabitSub"),
                                                     c("between_plant"),
                                                     c("Litter"),
                                                     c("Lichen"),
                                                     c("TotalLitter"),
                                                     c("Moss")
                                                   ),
                                                   any = list(
                                                     c("Plant"),
                                                     c("GrowthHabit"),
                                                     c("GrowthHabitSub"),
                                                     c("Duration", "GrowthHabit"),
                                                     c("Duration", "GrowthHabitSub"),
                                                     c("Duration", "ForbGraminoid"),
                                                     c("ShrubSucculent"),
                                                     c("Noxious"),
                                                     c("Litter"),
                                                     c("TotalLitter"),
                                                     c("SG_Group"),
                                                     c("SG_Group", "Live"),
                                                     c("Grass"),
                                                     c("Duration", "Grass"),
                                                     c("C3", "Duration", "Grass"),
                                                     c("C4", "Duration", "Grass"),
                                                     c("Native"),
                                                     c("Invasive"),
                                                     c("Invasive", "Duration", "GrowthHabitSub"),
                                                     c("Invasive", "Duration", "ShrubSucculent"),
                                                     c("Invasive", "Duration", "Grass"),
                                                     c("Invasive", "Duration", "ForbGrass"),
                                                     c("Conifer"),
                                                     c("PJ"),
                                                     c("Moss"),
                                                     c("Rock"),
                                                     c("Biocrust"),
                                                     c("Lichen")
                                                   ),
                                                   basal = list(
                                                     c("Duration", "Grass"),
                                                     c("Plant")
                                                   )
                                                 ),
                                                apply_species_adjustment = TRUE) |>
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

      # --- Handle geoIndicators ---
      geo_file <- file.path(current_path_ingest, "geoIndicators.csv")
      if (file.exists(geo_file)) {
        geoind <- read.csv(geo_file) %>% dplyr::filter(ProjectKey == projkey)

        suffix <- if(s_nbr == "root") "" else paste0("_sub", s_nbr)
        new_name <- file.path(current_path_ingest, paste0("geoIndicators_", projkey, suffix, ".csv"))

        write.csv(geoind, new_name, row.names = FALSE)
        file.remove(geo_file)
      }

      # --- Handle geoIndicators_all_indicators ---
      all_ind_file <- file.path(current_path_ingest, "geoIndicators_all_indicators.csv")
      if (file.exists(all_ind_file)) {
        all_ind <- read.csv(all_ind_file) %>% dplyr::filter(ProjectKey == projkey)

        suffix <- if(s_nbr == "root") "" else paste0("_sub", s_nbr)
        new_all_name <- file.path(current_path_ingest, paste0("geoIndicators_all_indicators_", projkey, suffix, ".csv"))

        write.csv(all_ind, new_all_name, row.names = FALSE)
        file.remove(all_ind_file)
      }
    }
  })

  message("\nMerging outputs into master file...")
  all_geo_files <- list.files(path_ingest_subset_root, pattern = "^geoIndicators_[^all].*\\.csv", recursive = TRUE, full.names = TRUE)

  if (length(all_geo_files) > 0) {
    master_geo <- lapply(all_geo_files, read.csv) %>% dplyr::bind_rows()
    write.csv(master_geo, file.path(path_foringest, "geoIndicators.csv"), row.names = FALSE)

    # combined master file for ALL indicators across projects/subsets:
    all_ind_files <- list.files(path_ingest_subset_root, pattern = "geoIndicators_all_indicators_.*\\.csv", recursive = TRUE, full.names = TRUE)
    if (length(all_ind_files) > 0) {
      master_all_ind <- lapply(all_ind_files, read.csv) %>% dplyr::bind_rows()
      write.csv(master_all_ind, file.path(path_foringest, "geoIndicators_all_indicators.csv"), row.names = FALSE)
    }

    # --- cleanup files ---
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


  # --- HELPER: SMART READER ---
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
      path_specieslist <- paste0(path_species, paste0(projkey, ".csv"))

      if (verbose) message("Reading in headers.")

      header_data <- smart_read(current_path_tall, "header")
      if (is.null(header_data)) {
        header_data <- subset_header
      }

      tall_filenames <- c("lpi_tall", "gap_tall", "height_tall",
                          "species_inventory_tall", "soil_stability_tall",
                          "rangelandhealth_tall")

      if (verbose) message("Reading in tall data.")

      data <- lapply(X = tall_filenames, function(X) {
        current_data <- smart_read(current_path_tall, X)

        if (!is.null(current_data)) {
          current_data <- current_data %>%
            dplyr::filter(PrimaryKey %in% header_data$PrimaryKey)
          if (nrow(current_data) > 0) return(current_data)
        }
        return(NULL)
      }) %>%
        setNames(nm = tall_filenames)

      data <- data[!sapply(X = data, FUN = is.null)]

      if (verbose) {
        message(paste0("The following data were successfully read in: ",
                       paste(names(data), collapse = ", ")))
      }

      schema <- read.csv(path_schema, stringsAsFactors = FALSE) %>% dplyr::distinct()
      species_list <- read.csv(path_specieslist, stringsAsFactors = FALSE)

      header_data$State <- NA
      header_data$SpeciesState <- NA

      species_file <- species_list

      # Harmonize SpeciesState
      if ("SpeciesState" %in% names(header_data) && "SpeciesState" %in% names(species_file)) {
        # Keep existing SpeciesState values
      } else {
        header_data <- header_data %>% dplyr::mutate(SpeciesState = projkey)
        species_file <- species_file %>% dplyr::mutate(SpeciesState = projkey)
      }

      species_code_var <- "SpeciesCode"
      generic_species_file <- NULL

      # Dynamic lookup for tall inputs
      # (hgt_tall_raw safely stays NULL/empty if missing)
      hgt_tall_raw <- if ("height_tall" %in% names(data)) data[["height_tall"]] else NULL

      # Height processing ONLY runs if height_tall actually exists and has data
      height_tall_clean <- NULL

      if (!is.null(hgt_tall_raw) && nrow(hgt_tall_raw) > 0) {

        hgt_tall_header <- dplyr::left_join(
          x = dplyr::select(.data = header_data, tidyselect::any_of(c("PrimaryKey", "State", "County"))),
          y = hgt_tall_raw,
          relationship = "one-to-many",
          by = "PrimaryKey"
        )

        if (verbose) message("Attempting to join the species list to the height data.")

        hgt_species <- species_join(
          data = sf::st_drop_geometry(hgt_tall_header),
          data_code = "Species",
          species_file = species_file,
          species_code = species_code_var,
          species_growth_habit_code = "GrowthHabitSub",
          species_duration = "Duration",
          species_property_vars = c("GrowthHabit", "GrowthHabitSub", "Duration", "Family",
                                    "SG_Group", "HigherTaxon", "Nonnative", "Invasive",
                                    "Noxious", "SpecialStatus", "Photosynthesis", "PJ",
                                    "CurrentPLANTSCode"),
          growth_habit_file = "",
          growth_habit_code = "Species",
          overwrite_generic_species = FALSE,
          generic_species_file = generic_species_file,
          update_species_codes = FALSE,
          by_species_key = FALSE,
          check_species = FALSE,
          verbose = verbose
        )

        # Sanitization / Harmonization
        expected_variables <- c("GrowthHabit", "GrowthHabitSub", "Duration", "Family",
                                "HigherTaxon", "Nonnative", "Invasive", "Noxious",
                                "SpecialStatus", "Photosynthesis", "PJ", "chckbox")

        missing_expected_variables <- setdiff(x = expected_variables, names(hgt_species))

        if (length(missing_expected_variables) > 0) {
          if (verbose) {
            warning(paste0("Missing expected variables for standard indicators: ",
                           paste(missing_expected_variables, collapse = ", ")))
          }

          missing_df <- matrix(nrow = nrow(hgt_species), ncol = length(missing_expected_variables)) %>%
            as.data.frame() %>%
            setNames(nm = missing_expected_variables)

          hgt_species <- dplyr::bind_cols(hgt_species, missing_df)
        }

        # Standardize Attributes
        if ("Duration" %in% names(hgt_species)) {
          hgt_species <- hgt_species %>%
            dplyr::mutate(Duration = dplyr::case_when(
              grepl("perennial", Duration, ignore.case = TRUE) ~ "Peren",
              grepl("(annual)|(biennial)", Duration, ignore.case = TRUE) ~ "Ann",
              is.na(Duration) ~ "duration_irrelevant",
              TRUE ~ Duration
            ))
        }

        if ("GrowthHabit" %in% names(hgt_species)) {
          hgt_species <- hgt_species %>%
            dplyr::mutate(GrowthHabit = dplyr::case_when(
              grepl("^non-?woody$", GrowthHabit, ignore.case = TRUE) ~ "NonWoody",
              grepl("^non-?vascular$", GrowthHabitSub, ignore.case = TRUE) ~ "Nonvascular",
              TRUE ~ GrowthHabit
            ))
        }

        if ("GrowthHabitSub" %in% names(hgt_species)) {
          hgt_species <- hgt_species %>%
            dplyr::mutate(GrowthHabitSub = dplyr::case_when(
              grepl("forb", GrowthHabitSub, ignore.case = TRUE) ~ "Forb",
              grepl("^sub-?shrub$", GrowthHabitSub, ignore.case = TRUE) ~ "SubShrub",
              grepl("^non-?vascular$", GrowthHabitSub, ignore.case = TRUE) ~ "growthhabitsub_irrelevant",
              grepl("^moss$", GrowthHabitSub, ignore.case = TRUE) ~ "growthhabitsub_irrelevant",
              grepl("^lichen$", GrowthHabitSub, ignore.case = TRUE) ~ "growthhabitsub_irrelevant",
              TRUE ~ GrowthHabitSub
            ))
        }

        if (all(c("GrowthHabit", "GrowthHabitSub", "Species") %in% names(hgt_species))) {
          hgt_species <- hgt_species %>%
            dplyr::mutate(Plant = dplyr::case_when(
              (!GrowthHabitSub %in% c("growthhabitsub_irrelevant")) &
                GrowthHabit != "Nonvascular" &
                stringi::stri_length(Species) >= 3 ~ "Plant",
              TRUE ~ NA_character_
            ))
        }

        # Validate and Clean Height values
        if ("Height" %in% names(hgt_species)) {
          non_numeric_count <- sum(is.na(suppressWarnings(as.numeric(hgt_species$Height))) & !is.na(hgt_species$Height))
          if (non_numeric_count > 0 && verbose) {
            warning(paste("Warning:", non_numeric_count, "non-numeric Height value(s) converted to NA."))
          }
          hgt_species <- hgt_species %>%
            dplyr::mutate(Height = suppressWarnings(as.numeric(Height)))
        }

        moss_lichen_codes <- c("LC", "2LICHN", "2LICHN1", "VL", "M", "2MOSS", "2MOSS1")

        hgt_species_filtered <- hgt_species %>%
          dplyr::filter(
            (is.na(Species) |
               Species %in% c("None", "N", "") |
               Plant %in% "Plant" |
               (is.na(GrowthHabitSub) & !HigherTaxon %in% c("liverwort", "moss"))) &
              !Species %in% moss_lichen_codes
          )

        height_tall_clean <- hgt_species_filtered %>%
          dplyr::filter(!Species %in% c("", " ", "N", "None"))
        # Define the vector of joined columns to drop
        cols_to_drop <- c(
          "ScientificName", "ScientificNameFormatted", "CommonName",
          "GrowthHabit", "Duration", "GrowthHabitSub", "CurrentPLANTSCode",
          "Notes", "Family", "Noxious", "Invasive", "SG_Group",
          "HigherTaxon", "Nonnative", "SpecialStatus", "Photosynthesis", "PJ"
        )

        # Strip existing species traits from height_tall (Element 2)
        height_tall_clean <- height_tall_clean |>
          dplyr::select(-dplyr::any_of(cols_to_drop))
        if ("SpeciesState" %in% names(height_tall_clean)) {
          height_tall_clean$SpeciesState <- NULL
        }
      }

      subset_header$State <- NA
      subset_header$SpeciesState <- NA
      species_list$SpeciesState <- NULL

      # -------------------------------------------------------------
      # DYNAMIC ACCUMULATION ARGUMENTS
      # Build args list conditionally so missing elements are never passed
      # -------------------------------------------------------------
      accum_args <- list(
        header       = subset_header,
        species_file = species_list,
        dead         = calculate_dead,
        source       = "DIMA",
        digits       = digits,
        verbose      = verbose
      )

      # Conditionally append tall datasets if present and populated
      if (!is.null(data[["lpi_tall"]]) && nrow(data[["lpi_tall"]]) > 0) {
        accum_args$lpi_tall <- data[["lpi_tall"]]
      }else{accum_args$lpi_tall <- NULL}

      if (!is.null(height_tall_clean) && nrow(height_tall_clean) > 0) {
        accum_args$height_tall <- height_tall_clean
      }else{accum_args$height_tall <- NULL}

      if (!is.null(data[["species_inventory_tall"]]) && nrow(data[["species_inventory_tall"]]) > 0) {
        accum_args$spp_inventory_tall <- data[["species_inventory_tall"]]
      }else{accum_args$spp_inventory_tall <- NULL}

      # Pass dynamic list using do.call
      accumulated_species_data <- do.call(accumulated_species, accum_args)

      # Execute core calculations
      accumulated_species_data <- accumulated_species_data %>%
        dplyr::left_join(y = dplyr::select(subset_header,
                                           tidyselect::any_of(c("PrimaryKey", "DateVisited", "DBKey", "ProjectKey"))),
                         by = "PrimaryKey",
                         relationship = "many-to-one") %>%
        dplyr::distinct() %>%
        dplyr::filter(!(is.na(AH_SpeciesCover) &
                          is.na(AH_SpeciesCover_n) &
                          is.na(Hgt_Species_Avg) &
                          is.na(Hgt_Species_Avg_n))) %>%
        dplyr::mutate(DateLoadedInDb = ingestion_date)

      accumulated_species_data <- translate_schema2(data = accumulated_species_data,
                                                    schema = schema,
                                                    datatype = "geoSpecies",
                                                    dropcols = TRUE,
                                                    verbose = verbose)

      accumulated_species_data <- accumulated_species_data %>%
        dplyr::filter(!is.na(Species),
                      (AH_SpeciesCover != 0 | Hgt_Species_Avg != 0))

      # Save subset output
      write.csv(x = accumulated_species_data,
                file = file.path(current_path_ingest, "geoSpecies.csv"),
                row.names = FALSE)

      spec_file <- file.path(current_path_ingest, "geoSpecies.csv")
      if (file.exists(spec_file)) {
        geosp <- read.csv(spec_file, stringsAsFactors = FALSE) %>%
          dplyr::filter(ProjectKey == projkey)

        suffix <- if (s_nbr == "root") "" else paste0("_sub", s_nbr)
        new_spec_name <- file.path(current_path_ingest, paste0("geoSpecies_", projkey, suffix, ".csv"))

        write.csv(geosp, new_spec_name, row.names = FALSE)
        file.remove(spec_file)
      }
    }
  })

  # Master Merge
  message("\nMerging all subset species into master file...")
  all_spec_files <- list.files(path_ingest_subset_root, pattern = "geoSpecies_.*\\.csv", recursive = TRUE, full.names = TRUE)

  if (length(all_spec_files) > 0) {
    master_spec <- lapply(all_spec_files, read.csv, stringsAsFactors = FALSE) %>% dplyr::bind_rows()
    write.csv(master_spec, file.path(path_foringest, "geoSpecies.csv"), row.names = FALSE)

    if (has_subsets) {
      unlink(path_ingest_subset_root, recursive = TRUE)
    } else {
      unlink(file.path(path_foringest, "root_run"), recursive = TRUE)
    }
  }
}

#' Split Main Header and Generate Geographic Indicators
#'
#' Reads a master header file, subdivides it into project-specific folders
#' based on unique Project Keys, and executes geographic indicator/species generators
#' for ingestion.
#'
#' @param path_tall Character string. The base target folder path containing the master "header.csv" and where subfolders will be created.
#' @param path_foringest Character string. Target directory path where generated geofiles will be written for ingestion.
#' @param path_species Character string. The file path leading to the reference species lookup database or folder.
#' @param template Character string or object. The structural template layout required by the generator routines.
#' @param path_schema Character string. The file path or schema object definition defining formatting validation boundaries.
#' @param BSNE_only Logical. If TRUE, skips geo-indicator and geo-species generation entirely. Defaults to FALSE.
#' @param doGSP Logical. Controls execution of the `generate_geoSpecies()` processing sequence if \code{BSNE_only = FALSE}. Defaults to TRUE.
#'
#' @return Silently returns NULL. Generates project-level `header.csv` files and processes geo-indicators/species assets to disk.
#'
#' @export
generate_project_geofiles <- function(path_tall,
                                      path_foringest,
                                      path_species,
                                      template,
                                      path_schema,
                                      BSNE_only = FALSE,
                                      doGSP = TRUE) {

  # Clean up trailing slashes
  base_path_tall <- gsub("/$", "", path_tall)
  master_header_path <- file.path(base_path_tall, "header.csv")

  if (!file.exists(master_header_path)) {
    stop("The master header.csv file was not found in: ", base_path_tall)
  }

  # Read master header file using readr
  header <- readr::read_csv(master_header_path, show_col_types = FALSE)
  projects_to_run <- unique(header$ProjectKey)

  # =========================================================================
  # STEP 1: SUBDIVIDE MASTER HEADER INTO PROJECT SUBFOLDERS
  # =========================================================================
  message("Subdividing master header into project-specific folders...")
  for (project in projects_to_run) {
    if (is.na(project) || project == "") next

    # Filter the header data to only keep rows matching the current project
    project_data <- header[header$ProjectKey == project, ]

    # Construct the target subfolder path and ensure it exists
    project_folder <- file.path(base_path_tall, project)
    if (!dir.exists(project_folder)) {
      dir.create(project_folder, recursive = TRUE)
    }

    # Save the subdivided dataframe as header.csv
    write.csv(project_data, file = file.path(project_folder, "header.csv"), row.names = FALSE)
  }

  # =========================================================================
  # STEP 2: GENERATE GEOFILES (INDICATORS & SPECIES)
  # =========================================================================
  if (BSNE_only == TRUE) {
    message("BSNE_only is set to TRUE. Skipping indicator and species geofile generation.")
    return(invisible(NULL))
  }

  # Combined into a single loop to avoid reading header files off the disk multiple times
  for (project in projects_to_run) {
    if (is.na(project) || project == "") next

    message("Processing indicators and species for project: ", project)

    current_project_path <- file.path(base_path_tall, project)
    header_file_path     <- file.path(current_project_path, "header.csv")

    if (!file.exists(header_file_path)) {
      warning("Skipping ", project, " because header.csv was not found.")
      next
    }

    # Read the specific scoped dataHeader for this project
    dataHeader <- read.csv(header_file_path, stringsAsFactors = FALSE)

    # Automatically inventory the tall data files inside this project folder
    project_files <- list.files(path = current_project_path,
                                pattern = "\\.(csv|rdata|RData)$",
                                full.names = TRUE)

    # 2A. Run Geographic Indicator Engine
    generate_geoIndicators(
      path_foringest  = path_foringest,
      path_tall       = current_project_path,
      path_species    = path_species,
      template        = template,
      path_schema     = path_schema,
      tall_header     = dataHeader,
      tall_files_list = project_files
    )

    # 2B. Run Geographic Species Engine (Conditional)
    if (doGSP) {
      generate_geoSpecies(
        path_foringest  = path_foringest,
        path_tall       = current_project_path,
        path_species    = path_species,
        template        = template,
        path_schema     = path_schema,
        tall_header     = dataHeader,
        verbose         = TRUE,
        calculate_dead  = FALSE,
        digits          = 6,
        ingestion_date  = NULL
      )
    }
  }

  return(invisible(NULL))
}
