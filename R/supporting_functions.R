###############################
#' Blank or NA
#'
#' ID blank or NA data in tblLPIDetail; used in the bareground QC
#'
#' @param x tblLPIDetail with keys assigned
#'
#' @return blank and NA observations
#' @export
#'
#' @noRd
#'

#blank or na helper function
is_blank_or_na <- function(x) {
  is.na(x) | trimws(as.character(x)) == ""
}

###############################
#' Remove duplicates
#'
#' removes duplicated data that is not kept in the LDC or used in the terradactylutils2::clean_tall_"method" functions. This is for data produced using the terradactyl gather functions.
#'
#' @param indata any tall table produced from terradactyl::gather()
#'
#' @return a data.frame of the tall file in your console (unless saved to an object)
#' @export
#'
#' @examples tdact_remove_duplicates(indata = tall_lpi)
#'
#' @noRd
#'
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
    print(indata |> dplyr::filter(PrimaryKey %in% data_duplicated_columns$PrimaryKey) |>
            dplyr::select(dplyr::any_of(c(colnames(data_duplicated_columns), cols_to_exclude_from_duplicate_check))) |>
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
#########################################



#####################################
#' Remove empty data
#'
#' Hidden helper function used to remove rows with no data that is used after running terradactyl::gather functions and in the terradactylutils2::clean_tall_"method" functions
#'
#' @param indata any tall table produced from terradactyl::gather()
#' @param datatype related to the method used to create the tall table
#'
#'@noRd
#'
#'@examples tdact_remove_empty(indata = tall_lpi, datatype = "lpi")
#'@export
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
###################################

###################################
#' add in all indicators columns from the "TerrADat" layer in a template GDB or a provided list
#' @description Add indicator columns to terradat from a template.
#' @param template String or data frame. A path to a geodatabase containing your template table, or the template table itself.
#' @param source String. Name of the layer to be read from the geodatabase, if provided. If "AIM" or "TerrADat", layer TerrADat will be read.
#' @param all_indicators Data frame. Indicator data that is missing columns.
#' @param prefixes_to_zero Vector of column prefixes to return 0 for rather than NA. Defaults to "AH", "FH", "NumSpp", and "Spp".
#' @return A data frame containing the data from all_indicators, with added NA or 0 columns.

## add indicator names
#' @rdname add_indicator_columns
#' @export add_indicator_columns
add_indicator_columns <- function(template,
                                  source,
                                  all_indicators,
                                  prefixes_to_zero = NULL){

  # template can either be a list of column names to add if not present, or a path to a geodatabase
  # So, we'll check to make sure that template is a character string or vector.
  if (!is.character(template)) {
    stop("template must either be a character vector of variable names or the filepath to a geodatabase containing a feature class with the name provided as the argument source and which has the variables to potentially add as variables in the feature class.")
  } else {
    if (length(template) == 1) {
      # If there's only one character string, try to figure out if it's a valid
      # filepath to a geodatabase to read in from.
      current_file_extension <- tools::file_ext(template)
      if (nchar(current_file_extension) < 1) {
        # If it's a lone character string with no file extension, that's just
        # the one variable name, apparently!
        feature_class_field_names <- template
      } else if (toupper(current_file_extension) == "GDB") {
        # Try to grab the feature class if it exists and yank variable names
        # from that.
        if (file.exists(template)) {
          feature_class_field_names <- sf::st_read(template,
                                                   layer = dplyr::if_else(condition = source %in% c("AIM", "TerrADat", "DIMA"),
                                                                          true = "TerrADat",
                                                                          false = source)) |>
            names() |>
            setdiff(x = _,
                    y = c("created_user",
                          "created_date",
                          "last_edited_user",
                          "last_edited_date"))
        }
      } else {
        stop(paste("template has the file extension",
                   current_file_extension,
                   "but the only valid file extension recognized is GDB."))
      }
    } else {
      # If it's a character vector with more than one string, those are just the
      # variable names.
      feature_class_field_names <- template
    }
  }

  # Which of the template variables are missing?
  missing_variables <- setdiff(x = feature_class_field_names,
                               y = names(all_indicators))

  # Make a data frame for all those variables that we can bind to all_indicators
  # starting from an empty matrix which gets converted into a data frame with
  # the variable names before any that need to be converted to 0s are swapped.
  missing_data <- matrix(nrow = nrow(all_indicators),
                         ncol = length(missing_variables)) |>
    as.data.frame(x = _) |>
    setNames(object = _,
             nm = missing_variables)

  # Only attempt this if there are actually any prefixes provided!
  if (length(prefixes_to_zero) > 1) {
    missing_data <- dplyr::mutate(.data = missing_data,
                                  # So this works "across" any variable starting with the prefixes
                                  # and puts 0s there.
                                  dplyr::across(.cols = tidyselect::starts_with(match = prefixes_to_zero),
                                                # Silly, but this is an "anonymous" function that
                                                # takes no arguments and always returns 0.
                                                .fns = ~ 0))
  }


  # Note that this won't put the indicators in the order that's expected
  # (except by total chance) so reordering elsewhere will be necessary.
  final_feature_class <- dplyr::bind_cols(all_indicators,
                                          missing_data)

  return(final_feature_class)
}
###################################


#########################################
#' LPI Calculations for Graminoid
#'
#' This is a helper function that is used within the geofiles function that produces a data frame with variable calculations for all graminoid species
#'
#' @param header as a data.frame, dataHeader file produced from terradactylutils2::create_header()
#' @param lpi_tall as a data.frame, the tall_LPI file produced from terradactylutils2::clean_tall_lpi()
#' @param species_file path to species lists including the ProjectKey
#' @param source source type
#' @param dsn dsn if applicable, not necessary for DIMA data
#' @param verbose T or F describing whether to return commentary
#'
#' @return a data.frame that includes graminoid related data calculations
#' @export
#'
#' @examples lpi_calc_graminoid(lpi_tall = file.path(path_tall, "lpi_tall.rdata"),header = header, source = "DIMA", dsn = path_template, species_file = paste0(path_species,  projkey, ".csv"))
#' @noRd
lpi_calc_graminoid <- function(header, lpi_tall,species_file,source,dsn,verbose = TRUE) {
  print("Beginning LPI indicator calculation")
  # Join the lpi data to the header PrimaryKeys and add the StateSpecies Key
  lpi_tall_header <- readRDS(lpi_tall) |>
    dplyr::left_join(x = dplyr::select(.data = header,
                                       tidyselect::all_of(c("PrimaryKey",
                                                            "SpeciesState"))) |>
                       dplyr::distinct(),
                     y = _,
                     by = c("PrimaryKey"))

  # check for generic species in Species list
  if (source %in% c("LMF", "AIM", "TerrADat")) {
    species_list <- sf::st_read(
      dsn = dsn,
      layer = "tblStateSpecies",
      stringsAsFactors = FALSE
    ) |>
      # Get unknown codes and clean them up. Unknown codes beginning with a 2 (LMF/NRI)
      # or a 2 letter prefix followed by a number.
      # Older projects also used "AAFF" etc. to identify unknown and dead
      # beyond recognition codes. So we'll need to detect those too
      dplyr::filter(stringr::str_detect(
        string = SpeciesCode,
        pattern = "^2[[:alpha:]]|^[A-z]{2}[[:digit:]]"
      ) &
        is.na(Notes))

    try(if (nrow(species_list) > 0) {
      stop(
        "Invalid generic species codes present in species list.
       Please resolve before calculating indicators."
      )
    })
  }


  # Join to the state species list via the SpeciesState value
  lpi_species <- species_join(
    data = lpi_tall_header,
    species_file = species_file,
    overwrite_generic_species = dplyr::if_else(
      source == "TerrADat",
      TRUE,
      FALSE),
    by_species_key = FALSE) |>
    dplyr::distinct()

  fh_variable_groupings <- list(c("Duration", "Graminoid"),
                                c("Duration", "ForbGraminoid"),
                                c("Noxious", "Duration", "Graminoid"))
  ah_variable_groupings <- list(c("Duration", "Graminoid"),
                                c("Duration", "ForbGraminoid"),
                                c("Noxious", "Duration", "Graminoid"),
                                c("Graminoid"))
  basal_variable_groupings <- list(c("Duration", "Graminoid"))

  # To make a graminoid variable
  graminoid_identifiers <- list(family = c("Poaceae",
                                           "Cyperaceae",
                                           "Juncaceae"),
                                GrowthHabitSub = c("Grass",
                                                   "Graminoid",
                                                   "Sedge"))

  # The indicators that have nonstandard names. This'll let us rename them with
  # the help of stringr::str_replace_all().
  nonstandard_indicator_lookup <- c("^FH_BareSoilCover$" = "BareSoil",
                                    "^AH_SagebrushLiveCover$" = "AH_SagebrushCover_Live")


  lpi_species <- dplyr::mutate(.data = lpi_species,
                               # Update the Duration values so that we don't
                               # need to do special renaming of indicators.
                               # This also lumps biennials in with annuals.
                               Duration = dplyr::case_when(grepl(x = Duration,
                                                                 pattern = "perennial",
                                                                 ignore.case = TRUE) ~ "Peren",
                                                           grepl(x = Duration,
                                                                 pattern = "(annual)|(biennial)",
                                                                 ignore.case = TRUE) ~ "Ann",
                                                           .default = Duration),
                               # Updates to the GrowthHabit variable to harmonize
                               # values with expectations, including adding a
                               # new value for nonvasculars which shifts those
                               # qualifying species out of the general nonwoody
                               # calculations
                               GrowthHabit = dplyr::case_when(grepl(x = GrowthHabit,
                                                                    pattern = "^non-?woody$",
                                                                    ignore.case = TRUE) ~ "NonWoody",
                                                              grepl(x = GrowthHabitSub,
                                                                    pattern = "^non-?vascular$",
                                                                    ignore.case = TRUE) ~ "Nonvascular",
                                                              # This removes sedges from consideration???
                                                              # Maybe an artifact of trying to avoid spitting
                                                              # out unused indicators
                                                              #GrowthHabitSub == "Sedge" ~ "Graminoid",
                                                              .default = GrowthHabit),
                               # Updates to GrowthHabitSub, mostly harmonizing
                               # variations on naming conventions
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
                                                                       ignore.case = TRUE) ~ NA,
                                                                 # Anyway, doing the exact same to moss
                                                                 grepl(x = GrowthHabitSub,
                                                                       pattern = "^moss$",
                                                                       ignore.case = TRUE) ~ NA,
                                                                 # And to lichen
                                                                 grepl(x = GrowthHabitSub,
                                                                       pattern = "^lichen$",
                                                                       ignore.case = TRUE) ~ NA,
                                                                 .default = GrowthHabitSub),
                               # The chckbox variable is a numeric representation
                               # of a logical value, but 0 is for a "dead" record
                               # and 1 is for a "live" record, so let's actually
                               # make that easy on ourselves
                               Live = dplyr::case_when(chckbox %in% c("0") ~ "Live",
                                                       # chckbox %in% c("1") ~ "Dead",
                                                       .default = NA),
                               # # Add a variable for shrubs and succulents so we
                               # # can easily calculate indicators for just them
                               # ShrubSucculent = dplyr::case_when(grepl(x = GrowthHabitSub,
                               #                                         pattern = "shrub|succulent",
                               #                                         ignore.case = TRUE) ~ "ShrubSucculent",
                               #                                   .default = NA),
                               # # For the any hit litter cover
                               # Litter = dplyr::case_when(code %in% litter_codes[["HerbLitter"]] ~ "HerbLitter",
                               #                           code %in% litter_codes[["WoodyLitter"]] ~ "WoodyLitter",
                               #                           .default = NA),
                               # TotalLitter = dplyr::case_when(code %in% unlist(litter_codes) ~ "TotalLitter",
                               #                                .default = NA),
                               # # Make separate photosynthesis columns because at
                               # # least one species is classified as both
                               # C3 = dplyr::case_when(grepl(x = photosynthesis,
                               #                             pattern = "C3") ~ "C3",
                               #                       .default = NA),
                               # C4 = dplyr::case_when(grepl(x = photosynthesis,
                               #                             pattern = "C4") ~ "C4",
                               #                       .default = NA),
                               Graminoid = dplyr::case_when(Family %in% graminoid_identifiers[["family"]] |
                                                              GrowthHabitSub %in% graminoid_identifiers[["GrowthHabitSub"]] ~ "Graminoid",
                                                            .default = NA),
                               # For all the grass-specific indicators
                               Grass = dplyr::case_when(Family %in% c("Poaceae")  |
                                                          GrowthHabitSub %in% c("Grass") ~ "Grass",
                                                        .default = NA),
                               # # This is to turn the SG_Group codes into values
                               # # that match the expected indicator names
                               # SG_Group = dplyr::case_when(grepl(x = SG_Group,
                               #                                   pattern = "Short") ~ "ShortPerenGrass",
                               #                             grepl(x = SG_Group,
                               #                                   pattern = "Tall") ~ "TallPerenGrass",
                               #                             .default = SG_Group),
                               # For combined forb and graminoid cover
                               ForbGraminoid = dplyr::case_when(grepl(x = GrowthHabitSub,
                                                                      pattern = "forb",
                                                                      ignore.case = TRUE) |
                                                                  Family %in% graminoid_identifiers |
                                                                  GrowthHabitSub %in% graminoid_identifiers[["GrowthHabitSub"]] ~ "ForbGraminoid",
                                                                .default = NA),
                               # For combined forb and grass cover
                               ForbGrass = dplyr::case_when(grepl(x = GrowthHabitSub,
                                                                  pattern = "forb|grass",
                                                                  ignore.case = TRUE) |
                                                              Family %in% "Poaceae" ~ "ForbGrass",
                                                            .default = NA),
                               # # For biocrust cover
                               # Biocrust = dplyr::case_when(code %in% biocrust_identifiers ~ "Biocrust",
                               #                             .default = NA),
                               # # For pinyon-juniper cover
                               # PJ = dplyr::case_when(code %in% pj_identifiers ~ "PJ",
                               #                       .default = NA),
                               # # For conifer cover
                               # Conifer = dplyr::case_when(Family %in% conifer_identifiers ~ "Conifer",
                               #                            .default = NA),
                               # # This is for basal cover by plants
                               # Plant = dplyr::case_when(!is.na(GrowthHabit) ~ "Plant",
                               #                          .default = NA),
                               # # This is just to make the Invasive values match
                               # # the desired indicator names
                               # Invasive = stringr::str_to_title(string = invasive),
                               # # This is for the native and non-native cover
                               # # It assumes that everything flagged as EXOTIC or
                               # # ABSENT should be considered NonNative and that
                               # # everything else is Native
                               # Native = dplyr::case_when(!(exotic %in% c("EXOTIC", "ABSENT")) ~ "Native",
                               #                           .default = "NonNative"),
                               # For noxious cover. This assumes that anything
                               # flagged as YES is noxious and nothing else is
                               Noxious = dplyr::case_when(Noxious %in% c("YES") ~ "Noxious",
                                                          .default = NA)
                               # # For rock cover
                               # Rock = dplyr::case_when(code %in% rock_codes ~ "Rock",
                               #                         .default = NA),
                               # # This is for values in the code variable that we
                               # # want to calculate cover for. This is a distinct
                               # # variable so we can do that without calculating
                               # # cover for *EVERY* value in the code variable.
                               # SpecialConsiderationCode = dplyr::case_when(code %in% special_consideration_codes["Duff"] ~ "Duff",
                               #                                             code %in% special_consideration_codes["Lichen"] ~ "Lichen",
                               #                                             code %in% special_consideration_codes["Moss"] ~ "Moss",
                               #                                             code %in% special_consideration_codes["EmbLitter"] ~ "EmbLitter",
                               #                                             code %in% special_consideration_codes["Water"] ~ "Water",
                               #                                             code %in% special_consideration_codes["Cyanobacteria"] ~ "Cyanobacteria",
                               #                                             code %in% special_consideration_codes["VagrLichen"] ~ "VagrLichen",
                               #                                             .default = NA),
                               # between_plant = dplyr::case_when(code %in% between_plant_codes[["Woodylitter"]] ~ "WoodyLitter",
                               #                                  code %in% between_plant_codes[["HerbLitter"]] ~ "HerbLitter",
                               #                                  code %in% between_plant_codes[["NonVegLitter"]] ~ "NonVegLitter",
                               #                                  code %in% between_plant_codes[["EmbLitter"]] ~ "EmbLitter",
                               #                                  code %in% between_plant_codes[["DepSoil"]] ~ "DepSoil",
                               #                                  code %in% between_plant_codes[["Duff"]] ~ "Duff",
                               #                                  code %in% between_plant_codes[["Lichen"]] ~ "Lichen",
                               #                                  code %in% between_plant_codes[["Moss"]] ~ "Moss",
                               #                                  code %in% between_plant_codes[["Cyanobacteria"]] ~ "Cyanobacteria",
                               #                                  code %in% between_plant_codes[["Water"]] ~ "Water",
                               #                                  code %in% between_plant_codes[["Rock"]] ~ "Rock",
                               #                                  code %in% between_plant_codes[["VagrLichen"]] ~ "VagrLichen",
                               #                                  code %in% between_plant_codes[["BareSoil"]] ~ "BareSoil",
                               #                                  .default = NA),
                               # # Special indicators for remote sensing use
                               # AdditionalRemoteSensing = dplyr::case_when(code %in% c("DS") ~ "DS",
                               #                                            .default = NA)
  )

  variable_groups <- list("first" = fh_variable_groupings,
                          "any" = ah_variable_groupings,
                          "basal" = basal_variable_groupings)

  # This is going to look gnarly, but automates stuff so we don't have to do the
  # capitalization corrections by hand
  unique_grouping_vars <- unique(c(unlist(fh_variable_groupings),
                                   unlist(ah_variable_groupings),
                                   unlist(basal_variable_groupings)))
  capitalization_lookup_list <- lapply(X = unique_grouping_vars,
                                       data = lpi_species,
                                       FUN = function(X, data){
                                         # message(paste(X,
                                         #               collapse = ", "))
                                         current_values <- unique(data[[X]])
                                         current_values <- current_values[!is.na(current_values)]
                                         if (length(current_values) > 0) {
                                           setNames(object = current_values,
                                                    nm = paste0("^",
                                                                toupper(current_values),
                                                                "$"))
                                         } else {
                                           NULL
                                         }
                                       })
  names(capitalization_lookup_list) <- unique_grouping_vars

  # This calculates the indicators.
  # The first level is iterating over the list variable_groups, working through
  # the hit types and the second level is working through all the groupings
  # within the hit type.
  cover_indicators_list <- lapply(X = names(variable_groups),
                                  variable_groups = variable_groups,
                                  data = lpi_species,
                                  capitalization_lookup_list = capitalization_lookup_list,
                                  verbose = verbose,
                                  FUN = function(X, variable_groups, data, capitalization_lookup_list, verbose){
                                    current_hit <- X
                                    message(paste("Calculating", current_hit, "hit indicators."))

                                    current_variable_groupings <- variable_groups[[current_hit]]
                                    # For the current hit type ("first", "any",
                                    # "basal"), calculate indicators for each
                                    # required variable grouping
                                    current_results_list <- lapply(X = seq(length(current_variable_groupings)),
                                                                   data = data,
                                                                   hit = current_hit,
                                                                   current_variable_groupings = current_variable_groupings,
                                                                   capitalization_lookup_list = capitalization_lookup_list,
                                                                   verbose = verbose,
                                                                   FUN = function(X, data, hit, current_variable_groupings, capitalization_lookup_list, verbose){
                                                                     current_grouping_vars <- current_variable_groupings[[X]]
                                                                     if (verbose) {
                                                                       message(paste("Calculating", hit, "hit indicators grouped by the variable(s):",
                                                                                     paste(current_grouping_vars,
                                                                                           collapse = ", "),
                                                                                     paste0("(Grouping ", X, " of ", length(current_variable_groupings), ")")))
                                                                     }
                                                                     # This is a little messy because pct_cover()
                                                                     # wants bare variable names.
                                                                     # There may be a better way to do this, but
                                                                     # for now this builds the function call as a
                                                                     # string and then executes that
                                                                     base_function_call_string <- paste0("pct_cover(lpi_tall = data,",
                                                                                                         "tall = TRUE,",
                                                                                                         "by_line = FALSE,",
                                                                                                         "hit = '", hit, "'")
                                                                     if (!is.null(current_grouping_vars)) {
                                                                       base_function_call_string <- paste0(base_function_call_string,
                                                                                                           ",")
                                                                     }
                                                                     function_call_string <- paste0(base_function_call_string,
                                                                                                    paste(current_grouping_vars,
                                                                                                          collapse = ","),
                                                                                                    ")")
                                                                     current_results_raw <- eval(expr = parse(text = function_call_string))

                                                                     # Sometimes there are no data that had non-NA
                                                                     # values in the variables of interest, so
                                                                     # we have to be prepared for that.
                                                                     if (nrow(current_results_raw) < 1) {
                                                                       if (verbose) {
                                                                         message("No qualifying data for the requested indicator(s). Returning NULL.")
                                                                       }
                                                                       return(NULL)
                                                                     }

                                                                     if (verbose) {
                                                                       message("Adjusting indicator names.")
                                                                     }

                                                                     # Now we rename the indicators.
                                                                     # We'll split them into their component parts
                                                                     # and then use the appropriate lookup vector
                                                                     # for each part to correct the capitalization.
                                                                     # There are more efficient ways to do this,
                                                                     # but this is extensible, standardized, and
                                                                     # basically hands-off for us when we update
                                                                     # indicators.
                                                                     current_results <- tidyr::separate_wider_delim(data = current_results_raw,
                                                                                                                    cols = indicator,
                                                                                                                    # Of course this doesn't use
                                                                                                                    # actual regex despite that
                                                                                                                    # being the tidyverse standard
                                                                                                                    delim = ".",
                                                                                                                    names = current_grouping_vars)


                                                                     # A for loop might actually be fastest (and
                                                                     # is certainly easiest), so that's the
                                                                     # solution for now.
                                                                     # I attempted to use mutate() with {{}} and
                                                                     # := but it wasn't evaluating the
                                                                     # str_replace_all() correctly because I couldn't
                                                                     # convince it to retrieve the relevant vector
                                                                     # with {{}} or dplyr::vars() for use as the
                                                                     # string argument.
                                                                     for (current_variable in current_grouping_vars) {
                                                                       current_results[[current_variable]] <- stringr::str_replace_all(string = current_results[[current_variable]],
                                                                                                                                       pattern = capitalization_lookup_list[[current_variable]])
                                                                     }

                                                                     # Having now made the variables with the
                                                                     # corrected components, we can recombine them
                                                                     current_results <- tidyr::unite(data = current_results,
                                                                                                     col = indicator,
                                                                                                     dplyr::all_of(current_grouping_vars),
                                                                                                     sep = "")

                                                                     # And add the hit prefix and "Cover" to the
                                                                     # indicator names
                                                                     current_prefix <- switch(EXPR = hit,
                                                                                              "first" = "FH_",
                                                                                              "any" = "AH_",
                                                                                              "basal" = "AH_Basal")
                                                                     current_results <- dplyr::mutate(.data = current_results,
                                                                                                      indicator = paste0(current_prefix,
                                                                                                                         indicator,
                                                                                                                         "Cover")) |>
                                                                       # And correct for the special case indicators
                                                                       dplyr::mutate(.data = _,
                                                                                     indicator = stringr::str_replace_all(string = indicator,
                                                                                                                          pattern = nonstandard_indicator_lookup))
                                                                     # We'll keep only the bare minimum here.
                                                                     # dplyr::select(.data = current_results,
                                                                     #               PrimaryKey,
                                                                     #               indicator,
                                                                     #               percent)# |>
                                                                     # # Get only the indicators we want to actually keep. Doing this saves us
                                                                     # from wasting memory storing unnecessary indicators even temporarily
                                                                     # and spares us the horror of storing them even less efficiently in
                                                                     # a wide format after this loop.
                                                                     # dplyr::filter(.data = _,
                                                                     #               indicator %in% expected_indicator_names)
                                                                   })

                                    # Bind all those results together
                                    dplyr::bind_rows(current_results_list)
                                  })

  # It's possible to accidentally calculate the same indicator more than once,
  # e.g. in Alaska where you might find "Moss" in the variable GrowthHabitSub
  # and so get a FH_MossCover when calculating both from GrowthHabitSub *AND*
  # SpecialConsiderationCode
  dplyr::bind_rows(cover_indicators_list) |>
    dplyr::distinct()
}
#########################################






#########################################
#' add o to save dates in nri data
#'
#'
#' @param data_list nri data as list
#' @param path_original_files path where nri original_files as csv are stored
#'
#' @return data frames saved with date corrections
#' @export
#'

date_corrections_nri <- function(PINTERCEPT, PH, path_original_files){
  PINTERCEPT <- data_list$PINTERCEPT
  PH <- data_list$PASTUREHEIGHTS

  # cols to fix
hit_cols <- c(paste0("HIT", 1:6), "BASAL", "NONSOIL")

# make dates codes and add o to dates
PINTERCEPT <- PINTERCEPT %>%
  mutate(across(all_of(hit_cols), ~ {
    val <- as.character(.x)

    # regex to catch: 2-Dec, 2-JUN, 2-Mar, 2-Nov, 2-Feb
    # Pattern: Digit(s) followed by a hyphen and a 3-letter month
    is_date_corrupted <- str_detect(val, "^[0-9]{1,2}-(Dec|Jun|Mar|Nov|Feb|Jan|Jul|Aug|Sep|Oct)$")

    # Turn "2-Dec" into "DECE2", "2-Mar" into "MARC2", etc.
    # take the first 4 letters of the month (uppercase) and add the number
    if (any(is_date_corrupted, na.rm = TRUE)) {
      val <- if_else(is_date_corrupted,
                     paste0(toupper(str_sub(str_extract(val, "[A-Za-z]+$"), 1, 4)),
                            str_extract(val, "^[0-9]+")),
                     val)
    }

    # dec70 differs
    val <- if_else(val == "Dec-70", "DECE70", val)

    return(val)
  }))

# "o"
PINTERCEPT <- PINTERCEPT %>%
  mutate(across(all_of(hit_cols), ~ {
    # Check if it matches our reconstructed patterns
    if_else(str_detect(.x, "^(FEBR|DECE|MARC|NOVE|JUNE)"),
            paste0("o", .x),
            as.character(.x))
  }))

# save
write_csv(PINTERCEPT, paste0(path_original_files,"/PINTERCEPT.csv"), quote = "all")

data_list$PINTERCEPT <- PINTERCEPT


## do same for PASTUREHEIGHTS


# cols to be fixed
hit_cols <- c("HPLANT")

#
PH <- PH %>%
  mutate(across(all_of(hit_cols), ~ {
    val <- as.character(.x)

    # Identify strings that match Digit-Month (e.g., 2-Dec, 2-JUN)
    # or Month-Digit (e.g., Dec-70)
    # Pattern: Digit-Abbrev OR Abbrev-Digit
    months_regex <- "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    is_date_error <- str_detect(val, paste0("^[0-9]+-", months_regex, "$|^", months_regex, "-[0-9]+$"))


    if_else(is_date_error,
            # Extract the month, make it 4 letters, uppercase it, and attach the number
            # We use str_remove_all to clean out the hyphens
            paste0(toupper(str_sub(str_extract(val, "[A-Za-z]+"), 1, 4)),
                   str_extract(val, "[0-9]+")),
            val)
  }))

# "o"
PH <- PH %>%
  mutate(across(all_of(hit_cols), ~ {
    # This regex catches any of our reconstructed prefixes
    prefix_pattern <- "^(JANN|FEBR|MARC|APRI|MAYY|JUNE|JULY|AUGU|SEPT|OCTO|NOVE|DECE)"

    if_else(str_detect(.x, prefix_pattern),
            paste0("o", .x),
            as.character(.x))
  }))


#save
write_csv(PH, paste0(path_original_files,"/PASTUREHEIGHTS.csv"), quote = "all")

data_list$PASTUREHEIGHTS <- PH

return(data_list)

}






#########################################
#' divide data into four groups based on pkey
#'
#'
#' @param gathered_data path where nri terradactyl gathered tall files are stored
#' @param path_tall path where nri cleaned tall files are stored
#'
#' @return 4 files with a subset of the tall files saved within a subset folder
#' @export
#'
subset_tall_files <- function(gathered_data, path_tall) {
  # Define the new folder structure
  output_root <- file.path(gathered_data, "subset")

  # Create the nested directory structure if it doesn't exist
  if (!dir.exists(output_root)) dir.create(output_root, recursive = TRUE)

  # Gather files
  csv_files <- list.files(path = gathered_data, pattern = "\\.csv$", full.names = TRUE, recursive = FALSE)
  header <- read_csv(file.path(path_tall, "header.csv"), show_col_types = FALSE)

  # subset if more than 10000 rows
  if (nrow(header) > 10000) {
    num_groups <- 4
    set.seed(123)
    keys_assigned <- header %>%
      distinct(PrimaryKey) %>%
      mutate(group = ntile(row_number(), num_groups))
  } else {
    num_groups <- 1 # Only one group (group 0)
    keys_assigned <- header %>%
      distinct(PrimaryKey) %>%
      mutate(group = 0)
  }

  # process groups
  # if row count was low, this loop runs once for group 0
  unique_groups <- unique(keys_assigned$group)

  for (i in unique_groups) {
    current_output_dir <- file.path(output_root, paste0("subset_", i))
    if (!dir.exists(current_output_dir)) dir.create(current_output_dir, recursive = TRUE)

    selected_keys <- keys_assigned %>%
      filter(group == i) %>%
      pull(PrimaryKey)

    message(paste("--- Processing Group", i, "---"))

    walk(csv_files, function(file_path) {
      file_name <- basename(file_path)
      current_df <- read_csv(file_path, show_col_types = FALSE)

      if ("PrimaryKey" %in% names(current_df)) {
        filtered_df <- current_df %>%
          filter(PrimaryKey %in% selected_keys) %>%
          # Add the column based on the folder number
          mutate(subset_nbr = i)

        write_csv(filtered_df, file.path(current_output_dir, file_name))
      }
    })
  }
}



#' Assign Subset Numbers to gathered data
#' @param gathered_data Path to the folder containing your CSV files.
#' @param path_tall Path to the folder containing the 'header.csv' file.
#' @import data.table
#' @export
assign_subset_nbr <- function(gathered_data, path_tall) {
  .N <- subset_nbr <- i.subset_nbr <- PrimaryKey <- NULL
  # output is within gathered data
  output_root <- file.path(gathered_data, "subset")
  if (!dir.exists(output_root)) dir.create(output_root, recursive = TRUE)

  # Use 'select' to keep memory usage to the absolute minimum
  header_path <- file.path(path_tall, "header.csv")
  if (!file.exists(header_path)) stop("header.csv not found in path_tall")

  # Use fread and immediately setDT to fix the 'cedta()' / awareness error
  h_dt <- data.table::fread(header_path, select = "PrimaryKey")
  data.table::setDT(h_dt)

  # Create unique mapping
  keys_assigned <- unique(h_dt, by = "PrimaryKey")

  # Assign subset numbers - right now only doing 4 at a time
  # Replaced dplyr::ntile with base/DT logic to avoid namespace overhead
  keys_assigned[, subset_nbr := as.integer(cut(seq_len(.N), breaks = 4, labels = FALSE))]

  # Set key for lightning-fast join
  data.table::setkey(keys_assigned, PrimaryKey)

  # Clean up the raw header read
  rm(h_dt)

  # process all other files
  csv_files <- list.files(path = gathered_data, pattern = "\\.csv$", full.names = TRUE)

  message(paste("Processing", length(csv_files), "files..."))

  for (f in csv_files) {
    fname <- basename(f)


    # skip directories or empty files
    if (file.info(f)$isdir || file.info(f)$size == 0) next

    # Start timing for the file
    start_time <- Sys.time()

    # Read current file and ensure DT awareness
    current_dt <- data.table::fread(f)
    data.table::setDT(current_dt)

    if ("PrimaryKey" %in% names(current_dt)) {
      # In-place join: adds subset_nbr column without copying the table
      #current_dt[keys_assigned, subset_nbr := i.subset_nbr, on = .(PrimaryKey)]
      current_dt[keys_assigned, subset_nbr := i.subset_nbr, on = c(PrimaryKey = "PrimaryKey")]
      # Write updated file
      data.table::fwrite(current_dt, file.path(output_root, fname))

      # Timing message
      end_time <- Sys.time()
      message("  Finished ", fname, " in ", round(difftime(end_time, start_time, units = "secs"), 2), "s")
    }

    # Force memory release for this file before moving to the next
    rm(current_dt)
    gc(full = FALSE) # Keeping RAM stable so Windows doesn't freak out
  }

}


#' Merge CSVs from subfolders with matching names (Base R Version)
#'
#' @param parent_path String. The path to a parent directory with subfolders within for merging.
#' @param verbose Logical. If TRUE, prints progress messages.
merge_subfolder_csvs <- function(parent_path, verbose = TRUE) {

  # check path
  if (!dir.exists(parent_path)) {
    stop("The provided parent directory does not exist.")
  }

  # subfolders
  # full.names = TRUE gives the path, recursive = FALSE stays in top level
  sub_folders <- list.dirs(parent_path, full.names = TRUE, recursive = FALSE)

  if (length(sub_folders) == 0) {
    warning("No subfolders found in the parent directory.")
    return(invisible(NULL))
  }

  # all csvs
  all_files <- list.files(
    path = sub_folders,
    pattern = "\\.csv$",
    full.names = TRUE,
    recursive = FALSE
  )

  if (length(all_files) == 0) {
    warning("No CSV files found within the subfolders.")
    return(invisible(NULL))
  }

  # group by name
  file_groups <- split(all_files, basename(all_files))

  lapply(names(file_groups), function(filename) {
    paths <- file_groups[[filename]]

    if (verbose) message("Processing: ", filename)

    # Read and combine using do.call(rbind, ...)
    # lapply replaces map(); read.csv is the base equivalent to read_csv
    list_of_dfs <- lapply(paths, read.csv, stringsAsFactors = FALSE)
    combined_df <- do.call(rbind, list_of_dfs)

    # save to parent folder
    out_path <- file.path(parent_path, filename)
    write.csv(combined_df, out_path, row.names = FALSE)
  })

}


#' Merge CSVs from subfolders with matching names (Base R Version)
#'
#' @param parent_path String. The path to a parent directory with subfolders within for merging.
#' @param verbose Logical. If TRUE, prints progress messages.
#' @export
merge_subfolder <- function(parent_path, verbose = TRUE) {

  # find all files
  if (!dir.exists(parent_path)) {
    stop("The provided parent directory does not exist.")
  }


  sub_folders <- list.dirs(parent_path, full.names = TRUE, recursive = FALSE)

  if (length(sub_folders) == 0) {
    warning("No subfolders found in the parent directory.")
    return(invisible(NULL))
  }


  all_files <- list.files(
    path = sub_folders,
    pattern = "\\.rdata$",
    full.names = TRUE,
    recursive = FALSE,
    ignore.case = TRUE
  )

  if (length(all_files) == 0) {
    warning("No .rdata files found within the subfolders.")
    return(invisible(NULL))
  }

  # group by name
  file_groups <- split(all_files, basename(all_files))

  lapply(names(file_groups), function(filename) {
    paths <- file_groups[[filename]]

    if (verbose) message("Processing: ", filename)

    # read and combine
    list_of_dfs <- lapply(paths, function(path) {

      # Try reading as a standard .Rdata file first
      result <- tryCatch({
        tmp_env <- new.env()
        load(path, envir = tmp_env)
        obj_name <- ls(tmp_env)[1]
        tmp_env[[obj_name]]
      }, error = function(e) {
        # If load fails, try reading as an RDS file
        tryCatch({
          readRDS(path)
        }, error = function(e2) {
          message("Failed to read: ", path, " - File may be corrupted or wrong format.")
          return(NULL)
        })
      })

      return(result)
    })

    # Remove any NULLs from failed reads before combining
    list_of_dfs <- list_of_dfs[!sapply(list_of_dfs, is.null)]

    if (length(list_of_dfs) > 0) {
      combined_df <- do.call(rbind, list_of_dfs)}

    # write
    base_name <- gsub("\\.[Rr]data$", "", filename)
    csv_out <- file.path(parent_path, paste0(base_name, ".csv"))
    rdata_out <- file.path(parent_path, paste0(base_name, ".rdata"))


    write.csv(combined_df, csv_out, row.names = FALSE)


    assign(base_name, combined_df)
    save(list = base_name, file = rdata_out)

    if (verbose) message("Saved CSV and RData for: ", base_name)
  })
}



#' Merge CSVs from subfolders with matching names (Base R Version)
#'
#' @param parent_path String. The path to a parent directory with subfolders within for merging.
#' @param verbose Logical. If TRUE, prints progress messages.
#' @export
merge_subfolder <- function(parent_path, verbose = TRUE) {

 # read in all subfolder rdata
  if (!dir.exists(parent_path)) {
    stop("The provided parent directory does not exist.")
  }


  sub_folders <- list.dirs(parent_path, full.names = TRUE, recursive = FALSE)

  if (length(sub_folders) == 0) {
    warning("No subfolders found in the parent directory.")
    return(invisible(NULL))
  }


  all_files <- list.files(
    path = sub_folders,
    pattern = "\\.rdata$",
    full.names = TRUE,
    recursive = FALSE,
    ignore.case = TRUE
  )

  if (length(all_files) == 0) {
    warning("No .rdata files found within the subfolders.")
    return(invisible(NULL))
  }

  # group by name
  file_groups <- split(all_files, basename(all_files))

  lapply(names(file_groups), function(filename) {
    paths <- file_groups[[filename]]

    if (verbose) message("Processing: ", filename)

    # combine
    list_of_dfs <- lapply(paths, function(path) {
      # Create a temporary environment to load the .rdata into
      # This prevents overwriting variables in your global workspace
      tmp_env <- new.env()
      load(path, envir = tmp_env)

      # Extract the first (and usually only) object from that environment
      obj_name <- ls(tmp_env)[1]
      return(tmp_env[[obj_name]])
    })

    # Combine using dplyr::bind_rows
    combined_df <- do.call(rbind, list_of_dfs)

    # strip the .rdata extension to create clean filenames
    base_name <- gsub("\\.[Rr]data$", "", filename)
    csv_out <- file.path(parent_path, paste0(base_name, ".csv"))
    rdata_out <- file.path(parent_path, paste0(base_name, ".rdata"))

    # 7save as CSV and rdata
    write.csv(combined_df, csv_out, row.names = FALSE)

    assign(base_name, combined_df)
    save(list = base_name, file = rdata_out)

    if (verbose) message("Saved CSV and RData for: ", base_name)
  })
}





#' Filter data to only keep schema columns
#'
#' @param data_list list of gathered data
#' @param path_schema Path to LDC schema plan
#' @export
filter_data_by_schema <- function(data_list, path_schema) {

    # 2. Read the schema
    schema <- read.csv(path_schema)

    # 3. Filter out the header if it exists in the list
    # (Equivalent to your previous target_files step)
    list_names <- names(data_list)
    target_keys <- list_names[list_names != "header" & list_names != "header_tall"]

    if (length(target_keys) == 0) {
      message("No target data frames found in the list to process.")
      return(data_list)
    }

    # 4. Process each data frame in the list
    # we use map() here so we can return the updated list
    updated_list <- data_list

    for (key in target_keys) {
      df <- data_list[[key]]

      # Ensure it's actually a data frame/tibble before processing
      if (!is.data.frame(df)) next

      # --- DYNAMIC LIST KEY TO SCHEMA TABLE TRANSFORMATION ---
      # Drop "_tall" from the list element name if it's there
      clean_name <- str_remove(key, "_tall")

      # Capitalize first letter and any letter after an underscore, remove underscores
      table_name <- clean_name %>%
        str_replace_all("(^|_)([a-z])", function(x) toupper(str_remove(x, "_"))) %>%
        str_replace("Lpi", "LPI") %>%
        paste0("data", .)

      # Get fields from schema for this specific table
      valid_fields <- schema %>%
        filter(Table == table_name) %>%
        { c(.$Field, .$terradactylAlias) } %>%
        unique() %>%
        na.omit()

      if (length(valid_fields) == 0) {
        warning(paste("No schema fields found for table:", table_name, "(from list key:", key, ")"))
        next
      }

      # Filter columns
      cols_to_keep <- names(df)[names(df) %in% c(valid_fields, "PrimaryKey")]

      # Update the data frame inside our new list
      updated_list[[key]] <- df %>% select(all_of(cols_to_keep))

      message(paste("Successfully filtered list element [", key, "] using schema Table:", table_name))
    }

    message("All list elements successfully processed against the schema.")
    return(updated_list)
  }




#' Process and save "clean" tall files
#'
#' @param gathered_data_list list of gathered data files
#' @param dataHeader dataHeader as data frame
#' @param source data type
#' @param path_tall path_tall folder path
#' @param nonvasc_codes list of nonvascular codes
#' @param data_list list of original files for DIMA
#' @export
process_and_save_tall <- function(gathered_data_list, dataHeader, source, path_tall, nonvasc_codes = NULL, data_list = NULL) {

  # 1. Verification Check
  if (is.null(dataHeader) || !is.data.frame(dataHeader)) {
    stop("dataHeader must be a valid data frame in the environment.")
  }
  if (!"PrimaryKey" %in% names(dataHeader)) {
    stop("dataHeader must contain a 'PrimaryKey' column to perform subsetting.")
  }

  # 2. Determine Subsetting Logic based on observation count (> 10,000)
  total_rows <- nrow(dataHeader)

  if (total_rows > 10000) {
    message("Observation count (", total_rows, ") exceeds 10,000. Splitting into 4 subsets...")

    # Generate an assignment vector (1 to 4) repeated to match dataHeader rows
    # Using a repeating sequence ensures an even distribution across the 4 subsets
    set.seed(123) # Optional: for reproducible splits if row order changes
    subset_vector <- rep(1:4, length.out = total_rows)

    # Map PrimaryKeys to their respective group (1 through 4)
    pk_groups <- split(dataHeader$PrimaryKey, subset_vector)

    # Process each subset group
    results_by_subset <- lapply(1:4, function(s_nbr) {
      message("--- Processing Subset Group: ", s_nbr, "/4 ---")

      # Grab the specific PrimaryKeys belonging to this subset chunk
      current_pks <- pk_groups[[s_nbr]]

      # Create a subsetted version of dataHeader
      sub_dataHeader <- dataHeader %>% filter(PrimaryKey %in% current_pks)

      # Subset every data frame inside gathered_data_list that contains PrimaryKey
      sub_gathered_data_list <- lapply(gathered_data_list, function(df) {
        if (is.data.frame(df) && "PrimaryKey" %in% names(df)) {
          return(df %>% filter(PrimaryKey %in% current_pks))
        }
        return(df) # Return untouched if it doesn't have a PrimaryKey
      })

      # Subset every data frame inside gathered_data_list that contains PrimaryKey
      sub_data_list <- lapply(data_list, function(df) {
        if (is.data.frame(df) && "PrimaryKey" %in% names(df)) {
          return(df %>% filter(PrimaryKey %in% current_pks))
        }
        return(df) # Return untouched if it doesn't have a PrimaryKey
      })

      # Call clean_tall_all directly using the subsetted structures in memory
      # Note: data_list is now fed our subsetted in-memory list
      result <- clean_tall_all(
        data_source      = source,
        gathered_data    = NULL, # Set to NULL or ignore if your clean_tall_all accepts data_list
        dataHeader       = sub_dataHeader,
        path_tall        = path_tall,
        subset_to_filter = s_nbr,
        gathered_data_list        = sub_gathered_data_list,
        data_list = sub_data_list,
        nonvasc_codes    = nonvasc_codes
      )

      return(result)
    })

    # Recombine (transpose and bind) the split lists back into a single unified list
    message("Recombining all processed subsets...")
    tall_files_list_final <- results_by_subset %>%
      purrr::list_transpose() %>%
      lapply(function(table_list) {
        dplyr::bind_rows(table_list[!sapply(table_list, is.null)])
      })

  } else {
    # If rows <= 10,000, skip subsetting completely and run directly on the data
    message("Observation count (", total_rows, ") is <= 10,000. Processing whole dataset at once...")

    tall_files_list_final <- clean_tall_all(
      data_source      = source,
      gathered_data    = NULL,
      dataHeader       = dataHeader,
      path_tall        = path_tall,
      subset_to_filter = NULL,
      data_list        = data_list,
      gathered_data_list = gathered_data_list
    )

    # Handle case where clean_tall_all returns a single item vs a named list structure
    if (!is.list(tall_files_list_final) || is.data.frame(tall_files_list_final)) {
      stop("clean_tall_all must return a named list of data frames.")
    }
  }

  # 3. Output and Save block (RDS and CSV)
  output_dir <- path_tall
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  lapply(names(tall_files_list_final), function(name) {
    df <- tall_files_list_final[[name]]
    if (is.null(df)) return(NULL)

    # Standardize names ending in _tall
    clean_name <- if(grepl("_tall$", name)) name else paste0(name, "_tall")

    # Save as RDS
    saveRDS(df, file = file.path(output_dir, paste0(clean_name, ".rds")))

    # Save as CSV
    write.csv(df, file = file.path(output_dir, paste0(clean_name, ".csv")), row.names = FALSE)

    message(paste("Successfully saved final data asset:", clean_name))
  })

  message("--- Processing Complete! ---")
  return(invisible(tall_files_list_final))
}


#' Filter Projects for Nonvascular Growth Habit
#'
#' Loops through a vector of project keys, reads their corresponding species CSV files,
#' and filters the data for rows where `GrowthHabitSub` matches "nonvascular"
#' (including variations like "Non-Vascular", "non_vascular", etc.).
#'
#' @param project_keys A character vector of project identifiers.
#' @param path_species A character string specifying the base directory path where
#'   the CSV files are located. Should end with a slash (e.g., "path/to/data/").
#' @param file_extension A character string specifying the file extension. Defaults to ".csv".
#'
#' @return A named list of data frames. Each element in the list corresponds to a
#'   project key and contains the filtered data frame. If a file is missing,
#'   the list element will be `NULL`.
#' @export
#'
#' @examples
#' nonvascular_data <- filter_nonvascular_projects(my_keys, species_path)
filter_nonvascular <- function(project_keys, path_species, file_extension = ".csv") {

  # Ensure the output list is initialized
  filtered_projects_list <- list()

  # Define the regex pattern for nonvascular variations
  regex_pattern <- "^non[-_]?vascular$"

  for (proj in project_keys) {

    # Construct the full file path
    file_path <- paste0(path_species, proj, file_extension)

    # Safely check if the file exists before reading
    if (file.exists(file_path)) {

      # Read the data
      df <- read.csv(file_path, stringsAsFactors = FALSE)

      # Check if the target column actually exists in this file
      if ("GrowthHabitSub" %in% names(df)) {

        # FIX: Changed ignore_case to ignore.case
        match_mask <- grepl(regex_pattern, df$GrowthHabitSub, ignore.case = TRUE)
        filtered_df <- df[match_mask, ]

        # Store the result (Will be an empty 0-row df if no matches are found)
        filtered_projects_list[[proj]] <- filtered_df

      } else {
        warning(paste0("Column 'GrowthHabitSub' not found in file: ", file_path))
        filtered_projects_list[[proj]] <- data.frame()
      }

    } else {
      warning(paste("File not found:", file_path))
      filtered_projects_list[[proj]] <- NULL
    }
  }

  return(filtered_projects_list)
}
