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

