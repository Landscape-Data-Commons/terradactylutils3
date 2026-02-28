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






