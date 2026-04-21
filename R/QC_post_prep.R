#' Validate cover between GeoSpecies and GeoIndicators
#'
#' determine if cover aligns between geospecies and geoIndicators
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#' @return data.frame of any plots whose geospecies and geoIndicator cover values do not align (if any)
#'
#' @export

validate_species_vs_indicators <- function(path_foringest, recursive) {

  # all geoIndicators files to identify target folders
  geo_files <- list.files(
    path = path_foringest,
    pattern = "^geoIndicators\\.csv$",
    recursive = recursive,
    full.names = TRUE
  )


  issues_all <- data.frame()

  for(file in geo_files){
    folder <- dirname(file)
    gsp <- file.path(folder, "geoSpecies.csv")

    # geoSpecies in the same folder
    if (!file.exists(gsp)) return(NULL)

    tryCatch({
      # force numeric conversion for the cover columns to ensure math works
      df_ind <- read.csv(file) %>%
        select(PrimaryKey, TotalFoliarCover) %>%
        mutate(TotalFoliarCover = as.numeric(TotalFoliarCover))

      df_spec <- read.csv(gsp) %>%
        # ensure we only pull the species cover column if it exists
        select(PrimaryKey, any_of("AH_SpeciesCover")) %>%
        mutate(AH_SpeciesCover = as.numeric(AH_SpeciesCover))

      # join by PrimaryKey
      combined <- df_spec %>%
        inner_join(df_ind, by = "PrimaryKey") %>%
        # filter for the discrepancy: Species Cover > Total Foliar Cover
        filter(
          !is.na(AH_SpeciesCover),
          AH_SpeciesCover > 0,
          AH_SpeciesCover > TotalFoliarCover
        ) %>%
        mutate(file_path = file)

      if(nrow(combined) > 0){issues_all <- rbind(issues_all, combined)}

    }, error = function(e) {
      return(NULL)
    })
  }
  if(nrow(issues_all) > 0){
    return(issues_all)}else{message("GeoSpecies and GeoIndicator cover values align")}
}

#' Validate GH and Duration NAs in geoIndicators
#'
#' determine if any of the NA hgt calculations are incorrectly NA
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#' @return data.frame of any plots whose GH or duration are present in the geoSpecies file and not the geoIndicators hgt column (if any)
#'
#' @export

validate_height_data <- function(path_foringest, recursive) {
  # identify all folders containing a 'geoIndicators.csv'
  geo_files <- list.files(path_foringest, pattern = "^geoIndicators\\.csv$",
                                    recursive = recursive, full.names = TRUE)


  issues_all <- data.frame()

  for(file in geo_files){

    folder <- dirname(file)
    all_folder_files <- list.files(folder)

    # presence of height file in the same folder
    has_height <- "dataHeight.csv" %in% all_folder_files
    if (!has_height) return(NULL)

    # read geoind columns as characters to safely check for NA/content
    df <- read.csv(file)

    # ydentify corresponding geoSpecies file
    species_path <- file.path(folder, "geoSpecies.csv")
    if (!file.exists(species_path)) return(NULL)

    # dat
    geoind <- read.csv(file)
    gsp <- read.csv(species_path)

    # identify Hgt columns (excluding Herbaceous and Woody)
    hgt_cols <- names(geoind) %>%
      stringr::str_subset("Hgt") %>%
      stringr::str_subset("Herbaceous|Woody", negate = TRUE)

    # process each Hgt column
    for(col_name in hgt_cols)
      # Duration and GrowthHabits from column name
      duration_target <- if(stringr::str_detect(col_name, "Peren")) "Peren" else NA

    # GrowthHabits (Forb, Graminoid)
    habits_found <- c()
    if(stringr::str_detect(col_name, "Forb")) habits_found <- c(habits_found, "Forb")
    if(stringr::str_detect(col_name, "Graminoid")) habits_found <- c(habits_found, "Graminoid")

    # check rows in geoIndicator where this column is NA
    na_primary_keys <- geoind %>%
      dplyr::filter(is.na(!!sym(col_name))) %>%
      dplyr::pull(PrimaryKey)

    if (length(na_primary_keys) == 0) return(NULL)

    # check if any species match the criteria defined by the column name
    gsp_na <- gsp %>% dplyr::filter(PrimaryKey %in% na_primary_keys)
    GH_d_exists <- gsp_na %>%
      filter(
        (is.na(duration_target) | Duration == duration_target),
        GrowthHabit %in% habits_found
      )

    issues_all <- rbind(issues_all, GH_d_exists)
  }
  if (nrow(issues_all) > 0) {warning("Hgt is NA but GrowthHabit/Duration exists in geoSpecies")
    return(issues_all)
  }else{message("GH and Duration align between geoSpecies and geoIndicators")}

}


#' Identify Gap sum > 100 percent
#'
#' identifies plots where Gap indicators sum to > 100 percent
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#' @return data.frame of any plots whose Gap cols sum to greater than 100 percent (if any)
#'
#' @export

gap_100 <- function(path_foringest, recursive){
  # all folders containing a 'geoIndicators.csv'
  geo_files <- list.files(path_foringest, pattern = "^geoIndicators\\.csv$",
                          recursive = recursive, full.names = TRUE)

  issues_all <- data.frame()

  for(file in geo_files){
    # read geoind columns as characters to safely check for NA/content
    df <- read.csv(file)
    df$file_path <- file
    target_cols <- colnames(df)[stringr::str_detect(colnames(df), "Gap")]


    # calc row sums and filter
    gap_issues <- df %>%
      dplyr::filter(if_any(all_of(target_cols), ~ .x > 100))


    if (nrow(gap_issues) > 0) {
      issues_all <- rbind(issues_all, gap_issues)
    } }
  if(nrow(issues_all > 0)){warning("Gap sum to greater than 100 percent")
    return(issues_all)}else{message("No Gap sum issue detected")}
}



#' Identify FH sum > 100 percent
#'
#' identifies plots where FH indicators sum to > 100 percent
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#' @return data.frame of any plots whose FH cols sum to greater than 100 percent (if any)
#'
#' @export
FH_100 <- function(path_foringest, recursive){
  # all folders containing a 'geoIndicators.csv'
  geo_files <- list.files(path_foringest, pattern = "^geoIndicators\\.csv$",
                          recursive = recursive, full.names = TRUE)

  issues_all <- data.frame()

  for(file in geo_files){
    # read geoind columns as characters to safely check for NA/content
    df <- read.csv(file)
    df$file_path <- file
    df$FH_TotalLitterCover <- NULL
    target_cols <- colnames(df)[stringr::str_detect(colnames(df), "FH")]


    # calc row sums and filter
    fh_sum_issues <- df %>%
      mutate(
        Calculated_FH_Sum = rowSums(across(all_of(fh_cols)), na.rm = TRUE)
      ) %>%
      dplyr::filter(Calculated_FH_Sum > 100.1) %>%
      dplyr::select(PrimaryKey, Calculated_FH_Sum, all_of(fh_cols))


    if (nrow(fh_sum_issues) > 0) {
      issues_all <- rbind(issues_all, fh_sum_issues)
    } }
  if(nrow(issues_all > 0)){warning("FH sum to greater than 100 percent")
    return(issues_all)}else{message("No FH sum issue detected")}
}


#' Identify AH or FH > 100 percent
#'
#' identifies plots with AH and FH indicators that are > 100 percent
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#' @return data.frame of any FH or AH cols that are greater than 100 percent (if any)
#'
#' @export

greater_than_100_geoind <- function(path_foringest, recursive) {
  # all folders containing a 'geoIndicators.csv'
  geo_files <- list.files(path_foringest, pattern = "^geoIndicators\\.csv$",
                          recursive = recursive, full.names = TRUE)

  issues_all <- data.frame()

  for(file in geo_files){
    # read geoind columns as characters to safely check for NA/content
    df <- read.csv(file)
    df$file_path <- file
    # identify columns that contain "AH" or "FH"
    target_cols <- colnames(df)[stringr::str_detect(colnames(df), "AH|FH")]

    # reshape data and filter for values > 100
    issues <- df %>%
      dplyr::filter(if_any(all_of(target_cols), ~ .x > 100)) #check all_of the cols in target cols - .x calls each column


    if(nrow(issues) > 0){
      issues_all <- rbind(issues, issues_all)
      return(issues_all)
    }

  }
  if(nrow(issues_all) > 0){warning("AH or FH cols present that are greater than 100 percent cover")
    issues_all <- rbind(issues, issues_all)
    return(issues_all)
  }
}

#' Checking for correct NA values in geoIndicators
#'
#' identifies plots with coordinates that have less than four decimal places
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#' @return warning messages if unexpected values in height, gap or lpi columns of geoInidicators
#'
#' @export
geoind_NA_check <- function(path_foringest, recursive) {
  # all folders containing a 'geoIndicators.csv'
  geo_files <- list.files(path_foringest, pattern = "^geoIndicators\\.csv$",
                          recursive = recursive, full.names = TRUE)


  for(file in geo_files){
    folder <- dirname(file)
    all_folder_files <- list.files(folder)

    # presence of method files in the same folder
    has_lpi    <- "dataLPI.csv" %in% all_folder_files
    has_gap    <- "dataGap.csv" %in% all_folder_files
    has_height <- "dataHeight.csv" %in% all_folder_files


    # read geoind columns as characters to safely check for NA/content
    df <- read.csv(file)

    if (has_lpi) {
      if (all(is.na(df[,12:34]))){warning("LPI data should be present but geoIndicators is returning NA")}
    } else {
      if (any(!is.na(df[, 12:34]))){warning("LPI data should all be NA but geoIndicators is returning non-NA value")}
    }

    # gap cols 35 to 39

    if (has_gap) {
      if (all(is.na(df[,35:39]))){warning("Gap data should be present but geoIndicators is returning NA")}
    } else {
      if (any(!is.na(df[, 35:39]))){warning("Gap data should all be NA but geoIndicators is returning non-NA value")}
    }
    # hgt cols 40 to 46
    height_cols <- 40:46
    if (has_height) {
      if (all(is.na(df[,40:46]))){warning("Hgt data should be present but geoIndicators is returning NA")}
    } else {
      if (any(!is.na(df[, 40:46]))){warning("Hgt data should all be NA but geoIndicators is returning non-NA value")}
    }


  }
}



#' Coordinate precision check
#'
#' identifies plots with coordinates that have less than four decimal places
#'
#' @param data data.frame of either dataHeader or geoIndicators
#'
#' @return whether the data have coorinates with precision less than four decimal places; if so, the PrimaryKeys with the issue are returned
#'
#' @export

coordinate_precision <- function(data) {

  # regex to find digits after the decimal
  precision_issues <- data %>%
    mutate(
      lat_digits = nchar(stringr::str_extract(Latitude_NAD83, "(?<=\\.).*")),
      lon_digits = nchar(stringr::str_extract(Longitude_NAD83, "(?<=\\.).*")),
      # replace nas with 0 if no decimal point is found
      lat_digits = coalesce(lat_digits, 0),
      lon_digits = coalesce(lon_digits, 0)
    ) %>%
    # filter for less than 4 decimal places
    filter(lat_digits < 4 | lon_digits < 4)

  if (nrow(precision_issues) > 0) {
    return(precision_issues %>%
             select(PrimaryKey, Latitude_NAD83, Longitude_NAD83))
  } else {
    return(NULL)
  }

}

#' Audit PrimaryKey integrity
#'
#' determines whether the expected PrimaryKey format is maintained across For Ingest tables
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#'
#' @return whether PriamryKey is in the expected format; if not, return a data.frame of the PriamryKeys that are not
#'
#' @export

audit_primary_key_integrity <- function(path_foringest, recursive) {

  # all CSV files
  target_files <- list.files(
    path = path_foringest,
    pattern = "\\.csv$",
    recursive = recursive,
    full.names = TRUE
  )

  # initialize data frame
  bad_rows_all <- data.frame()
  # iterate through tables

  for(file_path in target_files){
    file_name <- basename(file_path)

    # trycatch in case of recursive structure
    tryCatch({
      # only the PrimaryKey column and as character
      data <- read_csv(
        file_path,
        col_select = any_of("PrimaryKey"),
        col_types = cols(PrimaryKey = col_character()),
        show_col_types = FALSE
      )

      #skipping if doesn't have PrimaryKey in the name - shouldn't be the case but if other files are saved in the folder
      # this prevents fail
      if (!"PrimaryKey" %in% names(data)) return(NULL)

      # define the "Illegal Characters" regex
      # [^A-Za-z0-9-] means: any character that is NOT a letter, number, or hyphen
      illegal_char_pattern <- "[^A-Za-z0-9-]"

      # issues
      bad_rows <- data %>%
        filter(
          is.na(PrimaryKey) |                         # check for NAs
            trimws(PrimaryKey) == "" |                  # check for Blanks
            str_detect(PrimaryKey, "999999999") |       # check for test plots
            str_detect(PrimaryKey, illegal_char_pattern) # check for special chars
        )

      if (nrow(bad_rows) > 0) {
        bad_rows_all <- rbind(bad_rows, bad_rows_all)
        return(bad_rows_all)
      } else {
        return(NULL)
      }

    }, error = function(e) {
      return(tibble(Table = file_name, PrimaryKey = "ERROR", Issue = e$message))
    })
  }
  if(nrow(bad_rows_all) > 0){
    return(bad_rows_all)}else{message("PrimaryKeys are all in the expected format")}
}



#' LPI uniqueness
#'
#' determines whether unique LPI hits are maintained in dataLPI
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#'
#' @return whether dataLPI has maintained unique hits; if not, returns a dataframe of the duplicated records that includes the file_path where the duplicate is located
#'
#' @export

lpi_uniqueness <- function(path_foringest, recursive) {

  # recursive search for files with lpi in the name
  target_files <- list.files(
    path = path_foringest,
    pattern = "LPI.*\\.csv$",
    recursive = recursive,
    full.names = TRUE
  )

  if (length(target_files) == 0) return(message("no lpi files found"))

  #initialize list
  lpi_issue_list <- list()

  for (file_path in target_files) {
    file_name <- basename(file_path)

    # read file as character to ensure all key parts match correctly; using tryCatch
    # in case the file doesn't exist in recursive structure
    data <- tryCatch({
      read.csv(file_path, colClasses = "character", check.names = FALSE)
    }, error = function(e) return(NULL))

    data$file_name <- file_name
    # ensure all required columns for the unique hit check exist
    # if not skip to the next file (ie should only be finding dataLPI)
    required_cols <- c("PrimaryKey", "LineKey", "PointNbr", "PointLoc", "layer")
    if (is.null(data) || !all(required_cols %in% names(data))) next

    # create the unique points key
    data$unique_points <- paste0(
      data$PrimaryKey,
      data$LineKey,
      data$PointNbr,
      data$PointLoc,
      data$layer
    )

    # identify duplicates
    dups <- data[duplicated(data$unique_points), ]

    if (nrow(dups) > 0) {
      lpi_issue_list[[file_path]] <- dups
    }
  }

  # save to environment and return result
  if (length(lpi_issue_list) > 0) {
    lpi_duplicate_errors <- do.call(rbind, lpi_issue_list)
    message("Duplicate LPI records found")
    return(lpi_duplicate_errors)
  } else {
    message("unique lpi hits maintained")
    return(NULL)
  }
}



#' Audit GrowthHabit_measured in dataHeight
#'
#' provides the unique values of DBKey, DateLoadedInDB, ProjectKey and source across the For Ingest tables
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#'
#' @return whether GrwothHabit_measured contains expected values; if not, a dataframe of the unexpected values
#'
#' @export

audit_height_growth_habits <- function(path_foringest, recursive) {

  # recursive search for files with height in the name
  target_files <- list.files(
    path = path_foringest,
    pattern = "Height.*\\.csv$",
    recursive = recursive,
    full.names = TRUE
  )

  if (length(target_files) == 0) return(message("no height files found"))

  allowed_habits <- c("Woody", "NonWoody")
  #initialize list
  height_issue_list <- list()

  for (file_path in target_files) {
    file_name <- basename(file_path)

    # read file; using tryCatch again in case of fail for one of the folders (if reursive)
    # also forcing to be character because of issues with R guessing the wrong class
    # also skipping to the next file if it doesnt have GrowthHabit_measured
    data <- tryCatch({
      read.csv(file_path, colClasses = "character", check.names = FALSE)
    }, error = function(e) return(NULL))

    if (is.null(data) || !"GrowthHabit_measured" %in% names(data)) next

    # identify unique values and find unexpected ones
    found_habits <- unique(data$GrowthHabit_measured)
    unexpected <- setdiff(found_habits, allowed_habits)

    # if unexpected values exist, add to report
    if (length(unexpected) > 0) {
      height_issue_list[[file_path]] <- data.frame(
        table = file_name,
        unexpected_values = paste(unexpected, collapse = ", "),
        full_path = file_path,
        stringsAsFactors = FALSE
      )
    }
  }

  # save to environment and message status
  if (length(height_issue_list) > 0) {
    height_habit_errors <<- do.call(rbind, height_issue_list)
    message("unexpected values found in dataHeight")
    return(height_habit_errors)
  } else {
    message("GrowthHabit_measured contains only woody and nonwoody values")
    return(NULL)
  }
}


#' Metadata summary
#'
#' provides the unique values of DBKey, DateLoadedInDB, ProjectKey and source across the For Ingest tables
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#'
#' @return data.frame of the unique DBKey, DateLoadedInDB, ProjectKey and sourcevalues
#'
#' @export

summarize_table_metadata <- function(path_foringest, recursive) {

  # all CSV files
  target_files <- list.files(
    path = path_foringest,
    pattern = "\\.csv$",
    recursive = recursive,
    full.names = TRUE
  )

  #initialize unique_vals data frame to hold the metadata info
  unique_vals_all <- data.frame()
  # iterate and extract unique values
  for(file_path in target_files){

    file_name <- basename(file_path)

    tryCatch({
      # columns as character to avoid type conflicts during 'distinct'
      # use any_of to only grab the columns if they exist
      data <- read_csv(
        file_path,
        col_select = any_of(c("DBKey", "ProjectKey", "DateLoadedInDb", "source")),
        col_types = cols(.default = col_character()),
        show_col_types = FALSE
      ) %>%
        distinct() %>%
        mutate(TableName = file_name)

      if (ncol(data) == 0) return(NULL) # Skip if none of the columns exist

      unique_vals_all <- rbind(unique_vals_all, data)



    }, error = function(e) {
      return(tibble(TableName = file_name, DBKey = "READ ERROR"))
    })
  }


  final_report <- unique_vals_all %>%
    relocate(TableName) %>%
    arrange(TableName)

  return(final_report)
}


#' Cache check
#'
#' updates the files in the path_foringest path to have the correct DBKey and DateLoadedInDb
#'
#' @param path_foringest path where data for ingest are saved
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#' @param header_ldc data.frame; the return of fetch_ldc data type header
#'
#' @return whether data are on the LDC; if they are, a dataframe of those found on the LDC is returned
#'
#' @export

cache_check <- function(path_foringest, recursive, header_ldc){

  #### getting the locs and dates


  # list files
  header_paths <- list.files(
    path = path_foringest,
    recursive = recursive,
    pattern = "^dataHeader\\.csv$",
    full.names = TRUE
  )



  # initialize df
  master_header <- data.frame()


  for (header in header_paths){
    header <- read.csv(header)
    master_header <- rbind(master_header, header)

  }

  # round coords to avoid misidentifying matches

  new_data <- master_header %>%
    mutate(
      Lat_Rounded = round(as.numeric(Latitude_NAD83), 4),
      Lon_Rounded = round(as.numeric(Longitude_NAD83), 4),
      Date = as.Date(DateVisited),
      uniquekey = paste0(Lat_Rounded, Lon_Rounded, Date)
    )

  hist_data <- header_ldc %>%
    mutate(
      Lat_Rounded = round(as.numeric(Latitude_NAD83), 4),
      Lon_Rounded = round(as.numeric(Longitude_NAD83), 4),
      Date = as.Date(DateVisited),
      uniquekey = paste0(Lat_Rounded, Lon_Rounded, Date)
    ) %>%
    distinct()

  #compare
  on_ldc <- hist_data[hist_data$uniquekey %in% new_data$uniquekey,]

  if(nrow(on_ldc) > 0){warning("location and date found on LDC")
    write.csv(on_ldc, paste0(path_cache, "/dataonldc.csv"))
    print(on_ldc)
  }else{message("data not found on the LDC")}
}


###############################
#' BG Check
#'
#' compare the BG in tblLPIDetail to that produced by geoInd
#'
#' @param DIMATables path to DIMATables folder
#' @param path_foringest path to For Ingest folder
#' @param recursive TRUE or FALSE; TRUE if there is more than one For Ingest folder
#'
#' @return whether there is a mismatch in the BG; if there is, returns a data.frame of the PrimaryKey with mismatches
#' @export
#'
bare_soil_comparison <- function(DIMATables, path_foringest, recursive) {


  dima_folders <- data.frame(path_name = list.dirs(path = DIMATables, recursive = TRUE, full.names = TRUE))

  dima_folders <- dima_folders %>% dplyr::mutate(dima_folders = paste0(path_name))


  dima_folders <- dima_folders$dima_folders

  #initialize data frame
  bare_soil_summary <- data.frame()


  for (folder in dima_folders) {
    skip_to_next <- FALSE

    tryCatch({
      detail_path  <- file.path(folder, "tblLPIDetail.csv")
      header_path  <- file.path(folder, "tblLPIHeader.csv")
      lines_path   <- file.path(folder, "tblLines.csv")


      # get data from the needed tbls
      detail <- read.csv(detail_path)
      header <- read.csv(header_path)
      lines  <- read.csv(lines_path)

      # SoilSurface values according to terradactyl
      target_soil_values <- c("AG", "CM", "LM", "FG", "PC", "S")

      # join based on reckey or linekey to get the info for primarykey
      detail_processed <- detail %>%
        left_join(
          header %>% select(RecKey, LineKey, FormDate) %>% distinct(),
          by = "RecKey"
        ) %>%
        left_join(
          lines %>% select(LineKey, PlotKey) %>% distinct(),
          by = "LineKey",
          relationship = "many-to-many" # reps of linekey and plotkey
        ) %>%
        mutate(
          Date = as.Date(lubridate::parse_date_time(FormDate, orders = c("ymd HMS", "ymd", "mdy HMS", "mdy"))),
          # Use format to ensure PrimaryKey strings match perfectly (e.g. PlotID2024-01-01)
          PrimaryKey = paste0(PlotKey, format(Date, "%Y-%m-%d")))


      detail_processed <- detail_processed %>% mutate(BareSoil = ifelse(
        TopCanopy == "None" &
          SoilSurface %in% target_soil_values &
          if_all(matches("^Lower\\d+"), is_blank_or_na),
        1, 0
      )) %>%
        dplyr::select(PrimaryKey, BareSoil)


      bare_soil_summ <- detail_processed %>%
        group_by(PrimaryKey) %>%
        summarize(
          Total_Points = n(),
          BareSoil_Count = sum(BareSoil, na.rm = TRUE),
          BareSoilR = round((sum(BareSoil, na.rm = TRUE) / n()) * 100, 2)) %>% ungroup()

      bare_soil_summary <- rbind(bare_soil_summary, bare_soil_summ)



    }, error = function(e) { skip_to_next <<- TRUE
    }) # or should we do  error = function(e) return(NULL))
  }


  # ignore.case = true in case saved as geoindicators or geoIndicators
  geoind_paths <- list.files(
    path = path_foringest,
    # using a slightly more flexible pattern in case of slight name variations
    pattern = "geoIndicators\\.csv$",
    recursive = recursive,
    full.names = TRUE,
    ignore.case = TRUE
  )

  if (length(geoind_paths) == 0) return(message("no geoindicators files found"))

  geo_list <- list()

  for (file_path in geoind_paths) {
    # reading as characters to keep coordinate precision
    data <- tryCatch({
      read.csv(file_path, colClasses = "character")
    }, error = function(e) return(NULL))

    if (!is.null(data)) {
      # source_path lets you trace data back to the exact subfolder
      data$source_path <- file_path
      geo_list[[file_path]] <- data
    }
  }

  if (length(geo_list) > 0) {
    # bind_rows handles different column counts across files by filling with na
    all_geoindicators <- bind_rows(geo_list)

  } else {
    return(NULL)
  }




  # compare bare soil terradactyl and reconstructed

  # join tables on primarykey
  # we keep baresoil from geo and BareSoil from bare_soil_reconstructed
  comparison <- all_geoindicators %>%
    dplyr::select(PrimaryKey, BareSoil) %>%
    left_join(
      bare_soil_summary %>% dplyr::select(PrimaryKey, BareSoilR),
      by = "PrimaryKey"
    ) %>%
    # convert to numeric for math (handles character loading)
    dplyr::mutate(
      BareSoil = as.numeric(BareSoil),
      BareSoilR = as.numeric(BareSoilR),
      soil_diff = BareSoil - BareSoilR
    )

  # identify mismatches (using a small tolerance for rounding)
  bare_soil_mismatches <- comparison %>%
    dplyr::filter(abs(soil_diff) > 1)

  # save to environment and return header
  if (nrow(bare_soil_mismatches) > 0) {
    message("bare soil mismatches found, review bare_soil_mismatches df")
    return(bare_soil_mismatches)
  } else {

    message("all values match perfectly")
    return(NULL)
  }
}







###############################
#' Post preparation QC with checks
#'
#' provide dialogue and data frames that are helpful for quality control of data from all method types
#'
#' @param source data type ("NRI", "AIM" or "DIMA")
#' @param path_foringest path to For Ingest folder
#' @param path_parent path to folder containing all of the data folders for data preparation
#'
#' @return verbose about quality control of data and data frames where issues need to be investigated
#' @export
#'

QC_all_post_prep <- function(source, path_foringest, path_parent){
# BG CHECK
if(source == "DIMA" && exists("dima_data_list") && !is.null(dima_data_list$tblLPIDetail) | source == "BLM_AIM"){
  terradactylutils3::bare_soil_comparison(DIMATables = DIMATables, path_foringest = path_foringest, recursive = FALSE)
}else if(source == "NRI" && exists("nri") && !is.null(nri$POINTCOORDINATES)){
  terradactylutils3::bare_soil_comparison_nri(path_original_files = path_original_files, path_foringest = path_foringest, recursive = FALSE)
}else{message("No LPI detected for QC")}

# checking expected values only for metadata cols
metadata_report <- terradactylutils3::summarize_table_metadata(path_foringest, recursive = F)

print(metadata_report, n = Inf)

# check only accepted GrowthHabit_measured
if (source == "DIMA" && exists("dima_data_list") && !is.null(dima_data_list$tblLPIDetail) | source == "BLM_AIM") {
  audit_height_growth_habits(path_foringest, recursive = FALSE)
}

#unique pkey, linekey and point number in lpi
if(source == "DIMA" && exists("dima_data_list") && !is.null(dima_data_list$tblLPIDetail) | source == "BLM_AIM" | source == "NRI"){
  lpi_uniqueness(path_foringest, recursive = FALSE)
}

# PrimaryKey format
pk_integrity_report <- audit_primary_key_integrity(path_foringest, recursive = FALSE)


if (!is.null(pk_integrity_report) && nrow(pk_integrity_report) > 0) {
  print(pk_integrity_report)
} else {
  message("All PrimaryKeys are valid, alphanumeric, and non-dummy.")
}

# lat lon precision
if(source != "NRI"){
header <- read.csv(paste0(path_foringest, "/dataHeader.csv"))
geoind <- read.csv(paste0(path_foringest, "/geoIndicators.csv"))

precision_report1 <- coordinate_precision(header)
precision_report2 <- coordinate_precision(geoind)


if (!is.null(precision_report1) && nrow(precision_report1) > 0) {
  print(precision_report1)
} else {
  message("All coordinates in header files have at least 4 decimal places.")
}
if (!is.null(precision_report2) && nrow(precision_report2) > 0) {
  print(precision_report1)
} else {
  message("All coordinates in geoIndicators files have at least 4 decimal places.")
}
}
# NA or 0 for LPI, Gap, Hgt in geoIndicators
# manually checked; the code isn't catching when there shouldn't be Gap data but is handling LPI correctly

validation_results <- geoind_NA_check(path_foringest, recursive = FALSE)

if(!is.null(validation_results) > 0) {
  print(validation_results)
} else {
  message("All gap, LPI and height values in geoIndicators are correctly assigned NA or value.")
}


#ah fh > 100

ah_fh_issues <- greater_than_100_geoind(path_foringest = path_foringest, recursive = FALSE)


if (!is.null(ah_fh_issues) && nrow(ah_fh_issues) > 0) {
  message(paste("Found", nrow(ah_fh_issues), "observations exceeding 100."))
  print(ah_fh_issues)
} else {
  message("All AH/FH columns are within the valid range (<= 100).")
}

# gap cols < 100
gap_100(path_foringest = path_foringest, recursive = FALSE)



# geosp cover < total foliar
if(source == "DIMA" && exists("dima_data_list") && !is.null(dima_data_list$tblLPIDetail)){
  species_cover_audit <- validate_species_vs_indicators(path_foringest, recursive = F)
}


# NA hgt check

height_validation_report <- validate_height_data(path_foringest, recursive = FALSE)

if(NROW(height_validation_report) == 0){"No NA in Hgt detected or Height method not completed."}


### BSNE QC

if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_BoxCollection"]]) && nrow(dima_data_list[["tblBSNE_BoxCollection"]]) > 0){
  # compare og and LDC data
  missing <- compare_flux_to_og_data(path_parent, recursive = FALSE)
  print(missing)
}else{message("No BSNE data detected for QC")}

#not null
if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_BoxCollection"]]) && nrow(dima_data_list[["tblBSNE_BoxCollection"]]) > 0){
  df <- read_csv(paste0(path_foringest, "/dataHorizontalFlux.csv"))
  terradactylutils3::empty_cols(df)
}else{message("No BSNE data detected for QC")}

#checking the sediment weight is correctly recorded
if(exists("dima_data_list") &&  !is.null(dima_data_list[["tblBSNE_BoxCollection"]]) && nrow(dima_data_list[["tblBSNE_BoxCollection"]]) > 0){
  df <- read_csv(paste0(path_foringest, "/dataHorizontalFlux.csv"))
  sediment_weight_check(df)
}else{message("No BSNE data detected for QC")}
# warn if negative sediment records
if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_BoxCollection"]]) && nrow(dima_data_list[["tblBSNE_BoxCollection"]]) > 0){
  df <- read_csv(paste0(path_foringest, "/dataHorizontalFlux.csv"))
  neg_weight <- subset(df, df$recordedWeight < 0)

  if(nrow(neg_weight) > 0){warning("Negative sediment recorded") & print(neg_weight)}

}else{message("No BSNE data detected for QC")}

# checking that BOXID and StackID do not have rounding error (repeated zeros)
if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_BoxCollection"]]) && nrow(dima_data_list[["tblBSNE_BoxCollection"]]) > 0){
  # multiple zeros occur when there is a rounding error
  error_box_id   <- df %>% filter(grepl("000", BoxID))
  error_stack_id <- df %>% filter(grepl("000", StackID))

  # ID rows with duplicates WITHIN the same PrimaryKey

  dupes_in_group <- df %>%
    group_by(PrimaryKey, Height, StackID) %>%
    filter(duplicated(BoxID) | duplicated(BoxID, fromLast = TRUE)) %>%
    ungroup()

  #write.csv(dupes_in_group, paste0(path_parent, "duplicate_StackID_BoxID_height.csv"))

  # BOXID check
  if (exists("error_box_id") && !is.null(error_box_id) && nrow(error_box_id) > 0 || exists("dupes_in_group") && !is.null(dupes_in_group) && nrow(dupes_in_group) > 0) {
    warning("Potential rounding issue or duplicate BoxID found within PrimaryKey groups")

    if(nrow(error_box_id) > 0) {
      print(error_box_id)
    }

    if(nrow(dupes_in_group) > 0) {
      print(dupes_in_group)
    }
  }else{message("No potential BoxID issues found.")}

  # stack ID

  if (exists("error_stack_id") && nrow(error_stack_id) > 0) {
    warning("Potential rounding issue or duplicate StackID found within PrimaryKey groups")
    if(nrow(error_stack_id) > 0) {
      print(error_stack_id)
    }

  }else{message("No potential StackID issues found or no BSNE data detected")}

}


##############################
# DDT QC


# compare og and LDC data
if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_TrapCollection"]]) && nrow(dima_data_list[["tblBSNE_TrapCollection"]]) > 0){
  missing <- compare_ddt_to_og_data(path_parent)

}else{message("No DDT data detected for QC")}

#not null and in schema
if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_TrapCollection"]]) && nrow(dima_data_list[["tblBSNE_TrapCollection"]]) > 0){
  df <- read_csv(paste0(path_foringest, "/dataDustDeposition.csv"))
  empty_cols(df)
}else{message("No DDT data detected for QC")}

#checking the sediment weight is correctly recorded
if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_TrapCollection"]]) && nrow(dima_data_list[["tblBSNE_TrapCollection"]]) > 0){
  df <- read_csv(paste0(path_foringest, "/dataDustDeposition.csv"))

  sediment_weight_check_ddt(df)
}else{message("No DDT data detected for QC")}
# warn if negative sediment records
if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_TrapCollection"]]) && nrow(dima_data_list[["tblBSNE_TrapCollection"]]) > 0){
  df <- read_csv(paste0(path_foringest, "/dataDustDeposition.csv"))

  neg_weight <- subset(df, df$recordedWeight < 0)

  if(nrow(neg_weight) > 0){warning("Negative sediment recorded") & print(neg_weight)}
}else{message("No DDT data detected for QC")}

# checking that BOXID and StackID do not have rounding error (repeated zeros)
if(exists("dima_data_list") && !is.null(dima_data_list[["tblBSNE_TrapCollection"]]) && nrow(dima_data_list[["tblBSNE_TrapCollection"]]) > 0){
  df <- read_csv(paste0(path_foringest, "/dataDustDeposition.csv"))

  # multiple zeros occur when there is a rounding error
  error_stack_id <- df %>% filter(grepl("000", StackID))

  # ID rows with duplicates WITHIN the same PrimaryKey

  dupes_in_group <- df %>%
    group_by(PrimaryKey, StackID) %>%
    filter(duplicated(StackID) | duplicated(StackID, fromLast = TRUE)) %>%
    ungroup()

  #write.csv(dupes_in_group, paste0(path_parent, "duplicate_StackID_BoxID_height.csv"))

  # stackid check
  if (exists("error_stack_id") && !is.null(error_stack_id) && nrow(error_stack_id) > 0 || exists("dupes_in_group") && !is.null(dupes_in_group) && nrow(dupes_in_group) > 0) {
    warning("Potential rounding issue or duplicate BoxID found within PrimaryKey groups")

    if(nrow(error_stack_id) > 0) {
      print(error_stack_id)
    }

    if(nrow(dupes_in_group) > 0) {
      print(dupes_in_group)
    }
  }else{message("No potential StackID issues found or no BSNE data detected")}

}

}
