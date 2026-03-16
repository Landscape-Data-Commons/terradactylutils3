
#' Flux tall vs flux original data
#'
#' QC check the observations remain the same after preparation
#'
#' @param path_parent path where For Ingest and DIMATables stored
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#'
#' @export
compare_flux_to_og_data <- function(path_parent, recursive) {

  #dataHorizontalFlux
  dir <- list.dirs(path_parent, recursive = recursive)
  ingest_folder <- dir[grep("For Ingest$", dir)]


  if (length(ingest_folder) == 0) stop("Could not find 'dataHorizontalFlux' folder.")

  flux_file <- list.files(ingest_folder[1], pattern = "dataHorizontalFlux\\.csv$", full.names = TRUE)


  all_dirs <- list.dirs(path_parent, recursive = recursive)
  data_dir <- all_dirs[grep("/DIMATables$", all_dirs)]

  if (length(data_dir) == 0) stop("Could not find 'DIMATables' folder.")

  box_file <- list.files(data_dir[1], pattern = "tblBSNE_BoxCollection", full.names = TRUE)

  if (length(flux_file) == 0 || length(box_file) == 0) {
    stop("BSNE Box data not detected")
  }

  # ingested v og
  df_flux <- read.csv(flux_file, stringsAsFactors = FALSE)
  df_box  <- read_excel(box_file)

  # now lets check a unique id, first making sure col types match
  df_flux$BoxID <- as.character(df_flux$BoxID)
  df_box$collectDate <- as.character(df_box$collectDate)
  df_box$beakerNbr <- as.character(df_box$beakerNbr)
  df_flux$beakerNbr <- as.character(df_flux$beakerNbr)

  missing_rows <- anti_join(df_box, df_flux,
                            by = c("BoxID" = "BoxID",
                                   "collectDate" = "DateVisited",
                                   "beakerNbr" = "beakerNbr",
                                   "recordedWeight" = "recordedWeight"))
  missing_rows <- missing_rows %>% dplyr::filter(SampleCompromised == FALSE)
  if(nrow(missing_rows) > 0){message("Data are missing in the new horizontal flux table.")}
  if(nrow(missing_rows) == 0){message("Data are all present in the new horizontal flux table.")}

  missing_rows
}


#' DDT tall vs DDT original data
#'
#' DDT compared to original data
#'
#' @param path_parent path where For Ingest and DIMATables stored
#' @param recursive TRUE or FALSE; TRUE when the path has multiple For Ingest folders
#'
#' @export
compare_ddt_to_og_data <- function(path_parent, recursive) {

  #dataHorizontalFlux
  dir <- list.dirs(path_parent, recursive = recursive)
  ingest_folder <- dir[grep("For Ingest$", dir)]


  if (length(ingest_folder) == 0) stop("Could not find For Ingest folder.")

  flux_file <- list.files(ingest_folder[1], pattern = "dataDustDeposition\\.csv$", full.names = TRUE)


  all_dirs <- list.dirs(path_parent, recursive = TRUE)
  data_dir <- all_dirs[grep("/DIMATables$", all_dirs)] # Ensures it ends in /data

  if (length(data_dir) == 0) stop("Could not find 'DIMATables' folder.")

  box_file <- list.files(data_dir[1], pattern = "tblBSNE_TrapCollection", full.names = TRUE)

  if (length(flux_file) == 0 || length(box_file) == 0) {
    stop("BSNE Trap data not detected")
  }

  # ingested v og
  df_ddt <- read.csv(flux_file, stringsAsFactors = FALSE)
  df_trap  <- read.csv(box_file)

  # now lets check a unique id, first making syre data types are the same
  df_trap$beakerNbr <- as.character(df_trap$beakerNbr)
  df_ddt$beakerNbr <- as.character(df_ddt$beakerNbr)
  df_trap$DateVisited <- lubridate::parse_date_time(df_trap$collectDate,
                                                    orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS","ymd HM", "mdy HM"))
  df_ddt$DateVisited <- lubridate::parse_date_time(df_ddt$DateVisited,
                                                   orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS","ymd HM", "mdy HM"))

  missing_rows <- anti_join(df_trap, df_ddt,
                            by = c("StackID" = "StackID",
                                   "DateVisited" = "DateVisited",
                                   "beakerNbr" = "beakerNbr",
                                   "recordedWeight" = "recordedWeight"))
  missing_rows <- missing_rows %>% dplyr::filter(SampleCompromised == FALSE)
  if(nrow(missing_rows) > 0){message("Data are missing in the new DDT table.")}
  if(nrow(missing_rows) == 0){message("Data are all present in the new DDT table.")}

  missing_rows
}





#' Sediment weight check MWAC
#'
#' ensure recorded sediment weight is correctly recorded
#'
#' @param df data.frame of the MWAC data in the format for the LDC
#'
#' @export
sediment_weight_check <- function(df){

  df <- df %>% mutate(actualweight = recordedWeight - emptyWeight) %>%
    mutate(weightdiff = actualweight - sedimentWeight)
  # if actualweight does not equal sedimentWeight return the PrimaryKey, BoxID and beakerNbr
  df <- df %>% dplyr::filter(weightdiff >= 0.01) %>% dplyr::select(PrimaryKey, BoxID, beakerNbr)

  if(nrow(df) >0){message("Inaccurate sediment weights recorded.")}else{message("Sediment weights are correctly recorded.")}
  df

}


#' Sediment weight check DDT
#'
#' ensure recorded sediment weight is correctly recorded
#'
#' @param df data.frame of the DDT data in the format for the LDC
#'
#' @export
sediment_weight_check_ddt <- function(df){

  df <- df %>% mutate(actualweight = recordedWeight - emptyWeight) %>%
    mutate(weightdiff = actualweight - sedimentWeight)
  # if actualweight does not equal sedimentWeight return the PrimaryKey, BoxID and beakerNbr
  df <- df %>% dplyr::filter(weightdiff >= 0.01) %>% dplyr::select(PrimaryKey, StackID, beakerNbr)

  if(nrow(df) >0){message("Inaccurate sediment weights recorded.")}else{message("Sediment weights are correctly recorded.")}
  df

}
