###############################
#' BG Check NRI
#'
#' compare the BG in PINTERCEPT to that produced by geoInd
#'
#' @param path_original_files File path to original_files folder
#' @param path_foringest File path to For Ingest folder
#' @param recursive TRUE or FALSE; TRUE if there is more than one For Ingest folder
#'
#' @return whether there is a mismatch in the BG; if there is, returns a data.frame of the PrimaryKey with mismatches
#' @export
bare_soil_comparison_nri <- function(path_original_files, path_foringest, recursive) {


  nri_folders <- data.frame(path_name = list.dirs(path = path_original_files, recursive = TRUE, full.names = TRUE))

  nri_folders <- nri_folders %>% dplyr::mutate(nri_folders = paste0(path_name))


  nri_folders <- nri_folders$nri_folders

  #initialize data frame
  bare_soil_summary <- data.frame()


  for (folder in nri_folders) {
    skip_to_next <- FALSE

    tryCatch({
      ptpath  <- file.path(folder, "PINTERCEPT.csv")


      # get data from the needed tbls
      pt <- read.csv(ptpath)

      # SoilSurface values according to terradactyl
      target_soil_values <- c("AG", "CM", "LM", "FG", "PC", "S")

      #
      pt <- rename(pt, TopCanopy = HIT1)
      detail_processed <- pt %>% mutate(BareSoil = ifelse(
        TopCanopy == "None" &
          BASAL %in% target_soil_values &
          if_all(matches("^HIT\\d+"), is_blank_or_na),
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
    all_geoindicators <- bind_rows(geo_list)

    # JOIN the two datasets to see the comparison
    final_comparison <- all_geoindicators %>%
      # Ensure PrimaryKey is the same type for joining
      mutate(PrimaryKey = as.character(PrimaryKey)) %>%
      left_join(bare_soil_summary %>% mutate(PrimaryKey = as.character(PrimaryKey)),
                by = "PrimaryKey")

    return(final_comparison)

  } else {
    return(message("No bare soil differences found"))
  }
}

