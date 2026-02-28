library(devtools)
use_package("tidyr")
use_package("dplyr")
use_package("stringr")
use_package("lubridate")
use_package("tidyselect")
use_package("magrittr")


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



########################################
#' Translate Tall Data into LDC Schema
#'
#'helper function for translate_coremethods that uses a schema to translate the tall data sets into the LDC format (column class and name). This is an updated version of translate_schema from terradactyl_Utils that does not require user to input projkey.
#'
#' @param data any cleaned tall data
#' @param datatype the data type in the tall table such as lpi
#' @param schema the LDC schema describing the characteristics of the columns in the tall tables on the LDC
#' @param dropcols T or F describing whether to drop columns that are not used
#' @param verbose T or F describing whether to return commentary
#'
#' @return rewrites the tall tables in path_tall in the LDC format
#' @export
#'
#' @examples translate_schema2(schema = schema,datatype = "dataHeight",dropcols = TRUE, verbose = TRUE)
#' @noRd
translate_schema2 <- function(data,
                              datatype,
                              schema,
                              dropcols = TRUE,
                              verbose = TRUE){
  
  #### Sanitization ------------------------------------------------------------
  ##### Schema -----------------------------------------------------------------
  ### standardize names
  # colnames(matrix)[colnames(matrix) == fromcol] <- "terradactylAlias"
  # colnames(matrix)[colnames(matrix) == tocol] <- "ToColumn"
  
  ### process the incoming matrix by assigning actions to take at each row
  matrix_processed <- dplyr::filter(.data = schema,
                                    Table == datatype) |>
    dplyr::mutate(.data = _,
                  Field <- stringr::str_trim(string = Field,
                                             side = "both")) |>
    dplyr::filter(.data = _,
                  Field != "" | terradactylAlias != "") |>
    dplyr::select(.data = _,
                  tidyselect::all_of(x = c("terradactylAlias",
                                           "Field"))) |>
    dplyr::mutate(.data = _,
                  DropColumn = terradactylAlias != "" & Field == "",
                  AddColumn = Field != "" & terradactylAlias == "",
                  ChangeColumn = Field != "" & terradactylAlias != "" & Field != terradactylAlias,
                  NoAction = Field == terradactylAlias & !AddColumn & !DropColumn,
                  Error = (AddColumn + DropColumn + ChangeColumn + NoAction) != 1)
  
  # Check for errors!
  if(sum(matrix_processed$Error) > 0) {
    warning("Errors found in translation matrix. Returning diagnostic information.")
    return(errors)
  }
  
  ChangeColumn <-
    matrix_processed |>
    dplyr::filter(ChangeColumn)
  
  AddColumn <-
    matrix_processed |>
    dplyr::filter(AddColumn)
  
  DropColumn <-
    matrix_processed |>
    dplyr::filter(DropColumn)
  
  ## run translation and add data
  outdata <- dplyr::rename_at(.tbl = data,
                              .vars = ChangeColumn$terradactylAlias,
                              .funs = ~ ChangeColumn$Field) |>
    `is.na<-`(AddColumn$Field |> unique())

  
  # select only the tables in the out schema
  goodnames <- dplyr::filter(.data = matrix_processed,
                             Field != "") |>
    dplyr::pull(.data = _,
                Field)
  
  if (verbose) {
    message(paste("Returning the following columns/variables:",
                  paste(goodnames,
                        collapse = ", ")))
  }
  
  # This was an all_of() in the past, but that was brittle.
  # Now we use an any_of() and then inform the user about the missing variables
  outdata <- dplyr::select(.data = outdata,
                           tidyselect::any_of(x = goodnames))
  
  missing_names <- setdiff(x = goodnames,
                           y = names(outdata))
  if (length(missing_names) > 0) {
    if (verbose) {
      message(paste("The following variables are still missing and will be added, populated with the value NA:",
                    paste(missing_names,
                          sep = ", ")))
    }
    for (current_missing_name in missing_names) {
      outdata[[current_missing_name]] <- NA
    }
  }
  
  return(outdata)
}
#############################################


#############################################
#' Translate Core Methods
#'
#' produces the tall tables in a format for the LDC using the tall data produced from terradactylutils2::clean_tall_"method"() and a schema
#'
#' @param path_tall path to the tall files produced from terradactylutils2::clean_tall_"method"()
#' @param path_out where to write the files for ingest
#' @param path_schema file path to a schema used to adjust the tall files
#' @param verbose T or F describing whether to return commentary
#'
#' @return updated CSV of the tall files that are written to path_out (typically, a For Ingest directory)
#' @export
#'
#' @examples translate_coremethods2(path_tall = file.path(path_parent, "Tall"),path_out = path_foringest,path_schema = path_schema,verbose = T)
translate_coremethods2 <- function(path_tall, path_out, path_schema,  verbose = F){

  schema <- read.csv(path_schema)

  if(file.exists(file.path(path_tall, "header.Rdata"))){
    print("Translating header data")
    header   <- readRDS(file.path(path_tall, "header.Rdata"))
    dataHeader <- header |>
      translate_schema2(schema = schema,
                        #projectkey = projectkey,
                        datatype = "dataHeader",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataHeader, file.path(path_out, "dataHeader.csv"), row.names = F)
  } else {
    stop("Header data not found. Unable to translate data")
  }

  if(file.exists(file.path(path_tall, "lpi_tall.Rdata"))){
    print("Translating LPI data")
    tall_lpi <- readRDS(file.path(path_tall, "lpi_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataLPI <- tall_lpi |>
      translate_schema2(schema = schema,
                        #projectkey = projectkey,
                        datatype = "dataLPI",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataLPI, file.path(path_out, "dataLPI.csv"), row.names = F)
  } else {
    print("LPI data not found")
  }

  if(file.exists(file.path(path_tall, "height_tall.Rdata"))){
    print("Translating height data")
    tall_ht  <- readRDS(file.path(path_tall, "height_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataHeight <- tall_ht |>
      translate_schema2(schema = schema,
                        # projectkey = projectkey,
                        datatype = "dataHeight",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataHeight, file.path(path_out, "dataHeight.csv"), row.names = F)
  } else {
    print("Height data not found")
  }

  if(file.exists(file.path(path_tall, "species_inventory_tall.Rdata"))){
    print("Translating species inventory data")
    tall_sr  <- readRDS(file.path(path_tall, "species_inventory_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataSpeciesInventory <- tall_sr |>
      translate_schema2(schema = schema,
                        # projectkey = projectkey,
                        datatype = "dataSpeciesInventory",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataSpeciesInventory, file.path(path_out, "dataSpeciesInventory.csv"), row.names = F)
  } else {
    print("Species inventory data not found")
  }


  if(file.exists(file.path(path_tall, "soil_stability_tall.Rdata"))){
    print("Translating soil stability data")
    tall_ss  <- readRDS(file.path(path_tall, "soil_stability_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataSoilStability <- tall_ss |>
      translate_schema2(schema = schema,
                        #  projectkey = projectkey,
                        datatype = "dataSoilStability",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataSoilStability, file.path(path_out, "dataSoilStability.csv"), row.names = F)
  } else {
    print("Soil stability data not found")
  }

  if(file.exists(file.path(path_tall, "gap_tall.Rdata"))){
    print("Translating canopy gap data")
    tall_gap <- readRDS(file.path(path_tall, "gap_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataGap <- tall_gap |>
      translate_schema2(schema = schema,
                        #    projectkey = projectkey,
                        datatype = "dataGap",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataGap, file.path(path_out, "dataGap.csv"), row.names = F)
  } else {
    print("Gap data not found")
  }

}
##################################



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
#' @param digits Number of digits user wants observations rounded to
#' @return geoSpecies and geoIndicators file written to the path_foringest
#' 
#'@export
#'
#' @examples geofiles(path_foringest = path_foringest,path_tall = file.path(path_parent, "Tall"),header = tall_header, path_specieslist =  paste0(path_species,  projkey, ".csv"),path_template = template, digits = 2)
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
  
  if (is.null(ingestion_date)){
    ingestion_date <- format(x = Sys.time(),
                             "%m/%d/%Y")
  }
  
  if (verbose) {
    message("Reading in headers.")
  }
  # Read in the headers because these will be used to filter the incoming data
  # by PrimaryKey before indicators are calculated.
  header <- readRDS(file = file.path(path_tall, "header.Rdata"))
  
  # These are the assumed base filenames (with the extension .Rdata) that
  # correspond to the data types.
  tall_filenames <- c("lpi_tall",
                      "gap_tall",
                      "height_tall",
                      "species_inventory_tall",
                      "soil_stability_tall",
                      "rangelandhealth_tall")
  
  if (verbose) {
    message("Reading in tall data.")
  }
  # Try to read in the data if the file exists.
  # If the file doesn't exist or if the file contains no data corresponding to
  # PrimaryKey values in header this'll return NULL.
  data <- lapply(X = tall_filenames,
                 path_tall = path_tall,
                 header = header,
                 FUN = function(X, path_tall, header){
                   # Create the assumed filepath.
                   current_filepath <- file.path(path_tall,
                                                 paste0(X, ".Rdata"))
                   
                   if (file.exists(current_filepath)) {
                     # Read in and filter data
                     current_data <- readRDS(file = current_filepath) |>
                       # Remove invalid records which may happen depending on
                       # how the Rdata was exported.
                       dplyr::filter(.data = _,
                                     PrimaryKey %in% header$PrimaryKey)
                     # Solving the issue of empty data frames not being handled
                     # by lpi_calc()
                     if (nrow(current_data) > 0) {
                       current_data
                     } else {
                       NULL
                     }
                   } else {
                     NULL
                   }
                 }) |>
    # Setting the names of the data in the list for ease of reference later.
    setNames(object = _,
             nm = tall_filenames)
  
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
  
  #### Accumulated species stuff -----------------------------------------------
  if (doGSP) {
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
    
    write.csv(x = accumulated_species_data,
              file.path(path_foringest,
                        "geoSpecies.csv"),
              row.names = FALSE)
  }
}



#######################################
#' geoIndicators QC
#'
#' produces a CSV with QC information about the geoIndicators table
#'
#' @param path_foringest where geoIndicators data was saved
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with QC information about the geoIndicators table
#' @export
#'
#' @examples geoind_qc(path_foringest = path_foringest, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
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
#' @export
#'
#' @examples geospecies_qc(path_foringest = path_foringest, USDA_plants = read.csv("D:/modifying_data_prep_script_10032025/2004-2023_ceap_species_list.csv"), speciescode = "UpdatedSpeciesCode", path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
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



##############################################
#' Format for ingest files
#'
#' updates the files in the path_foringest path to have the correct DBKey and DateLoadedInDb
#'
#' @param path_foringest path where data for ingest are saved
#' @param DateLoadedInDb in standard date format, the date you are running the code
#'
#' @return CSVs of the for ingest files saved to the specified path_foringest
#' @export
#'
#' @examples db_info(path_foringest = path_foringest,  DateLoadedInDb = format(Sys.Date(), "%m/%d/%Y"))
db_info <- function(path_foringest, DateLoadedInDb){

  # read in data
  header <- read.csv(paste0(path_foringest, "/dataHeader.csv"))
  ind <- read.csv(paste0(path_foringest, "/geoIndicators.csv"))
  if(file.exists(file.path(path_foringest, "/dataLPI.csv"))) {
    LPI <- read.csv(paste0(path_foringest, "/dataLPI.csv"))}
  if(file.exists(file.path(path_foringest, "/dataGap.csv"))){
    gap <- read.csv(paste0(path_foringest, "/dataGap.csv"))}
  if(file.exists(file.path(path_foringest, "/dataSoilStability.csv"))){
    ss <- read.csv(paste0(path_foringest, "/dataSoilStability.csv"))}
  if(file.exists(file.path(path_foringest, "/dataHeight.csv"))) {
    hgt <- read.csv(paste0(path_foringest, "/dataHeight.csv"))}
  if(file.exists(file.path(path_foringest, "/geoSpecies.csv"))){
    sp <- read.csv(paste0(path_foringest, "/geoSpecies.csv"))}
  if(file.exists(file.path(path_foringest, "/dataSpeciesInventory.csv"))){
    spin <- read.csv(paste0(path_foringest, "/dataSpeciesInventory.csv"))}


  header$DateLoadedInDb <- rep(todaysDate)
  ind$DateLoadedInDb <- header$DateLoadedInDb[match(ind$PrimaryKey, header$PrimaryKey)]

  if(file.exists(file.path(path_foringest, "/dataLPI.csv"))) {
    LPI$DateLoadedInDb <- header$DateLoadedInDb[match(LPI$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/dataGap.csv"))) {
    gap$DateLoadedInDb <- header$DateLoadedInDb[match(gap$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/dataHeight.csv"))) {
    hgt$DateLoadedInDb <- header$DateLoadedInDb[match(hgt$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/dataSoilStability.csv"))) {
    ss$DateLoadedInDb <- header$DateLoadedInDb[match(ss$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/geoSpecies.csv"))) {
    sp$DateLoadedInDb <- header$DateLoadedInDb[match(sp$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/dataSpeciesInventory.csv"))) {
    spin$DateLoadedInDb <- header$DateLoadedInDb[match(spin$PrimaryKey, header$PrimaryKey)]}



  write.csv(header,paste0(path_foringest,"/dataHeader.csv"), row.names=FALSE)
  write.csv(ind,paste0(path_foringest,"/geoIndicators.csv"), row.names=FALSE)

  if(file.exists(file.path(path_foringest, "/dataLPI.csv"))) {
    write.csv(LPI,paste0(path_foringest,"/dataLPI.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataGap.csv"))) {
    write.csv(gap,paste0(path_foringest,"/dataGap.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataHeight.csv"))) {
    write.csv(hgt,paste0(path_foringest,"/dataHeight.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataSoilStability.csv"))) {
    write.csv(ss,paste0(path_foringest,"/dataSoilStability.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/geoSpecies.csv"))) {
    write.csv(sp,paste0(path_foringest,"/geoSpecies.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataSpeciesInventory.csv"))) {
    write.csv(spin,paste0(path_foringest,"/dataSpeciesInventory.csv"), row.names=FALSE)}



}
##############################################






















































