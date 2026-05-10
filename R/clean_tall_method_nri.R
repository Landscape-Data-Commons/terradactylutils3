###################################
#' Clean Tall LPI NRI
#'
#'after gathering lpi, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param lpi as a data.frame, the tall_lpi file
#' @param dataHeader as a data.frame, the dataHeader file produced from terradactylutils2::create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return updated tall file written to path_tall and a tall_lpi data frame in the console (unless saved to an object)
#'
#' @examples clean_tall_lpi(lpi = terradactyl::gather_lpi(source = source, tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_lpi_nri <- function(lpi, dataHeader, path_tall){
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


  lpi <- lpi |> tdact_remove_duplicates() |> tdact_remove_empty(datatype = "lpi")


  tall_lpi <- lpi
tall_lpi <- tall_lpi[!is.na(tall_lpi$code),]
tall_lpi <- tall_lpi[tall_lpi$code != "None",]
  pkeys <- dataHeader$PrimaryKey
  dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # making sure all codes are capital
  tall_lpi$code <- toupper(tall_lpi$code)
  tall_lpi$ProjectKey <- dataHeader$ProjectKey[match(tall_lpi$PrimaryKey, dataHeader$PrimaryKey)]

  tall_lpi <- tall_lpi %>%
    group_by(PrimaryKey, LineKey, PointNbr) %>%
    # TopCanopy moss needs to be handled differently - move to lower code and drop TopCanopy
    # make sure we don't touch SoilSurface
    mutate(has_2moss_top = any(layer == "TopCanopy" & code == "2MOSS")) %>%

  # only mutating the moss top groups
    mutate(
      layer = case_when(
        !has_2moss_top ~ layer,               # Do nothing if no 2MOSS on top
        layer == "SoilSurface" ~ layer,        # Do nothing to SoilSurface
        layer == "TopCanopy" ~ "Lower1",       # 2MOSS (TopCanopy) becomes Lower1

        # For any LowerX, extract the number, add 1, and paste it back - that way if Lower5 becomes Lower6 and we keep SoilSurface
        str_detect(layer, "Lower") ~ {
          old_num <- as.numeric(str_extract(layer, "\\d+"))
          paste0("Lower", old_num + 1)
        },

        TRUE ~ layer
      )
    ) %>%
    #
    # since 2MOSS is now Lower1, we just filter out any remaining TopCanopy rows
    # (which only exist in the non-moss groups now).
    filter(!(has_2moss_top & layer == "TopCanopy")) %>%
    # drop the new column
    select(-has_2moss_top) %>%
    ungroup()


  # we don't need to move 2MOSS if already in lower layer

  # now just make 2MOSS "M"
  #tall_lpi$code <- ifelse(tall_lpi$code == "2MOSS", "M", tall_lpi$code)

  # same for 2LICHN(1)

  tall_lpi <- tall_lpi %>%
    group_by(PrimaryKey, LineKey, PointNbr) %>%
    # TopCanopy moss needs to be handled differently - move to lower code and drop TopCanopy
    # make sure we don't touch SoilSurface
    mutate(has_2lich_top = any(layer == "TopCanopy" & code == "2LICHN")) %>%

    # only mutating the moss top groups
    mutate(
      layer = case_when(
        !has_2lich_top ~ layer,               # Do nothing if no 2MOSS on top
        layer == "SoilSurface" ~ layer,        # Do nothing to SoilSurface
        layer == "TopCanopy" ~ "Lower1",       # 2MOSS (TopCanopy) becomes Lower1

        # For any LowerX, extract the number, add 1, and paste it back - that way if Lower5 becomes Lower6 and we keep SoilSurface
        str_detect(layer, "Lower") ~ {
          old_num <- as.numeric(str_extract(layer, "\\d+"))
          paste0("Lower", old_num + 1)
        },

        TRUE ~ layer
      )
    ) %>%
    #
    # since 2MOSS is now Lower1, we just filter out any remaining TopCanopy rows
    # (which only exist in the non-moss groups now).
    filter(!(has_2lich_top & layer == "TopCanopy")) %>%
    # drop the new column
    select(-has_2lich_top) %>%
    ungroup()


  tall_lpi <- tall_lpi %>%
    group_by(PrimaryKey, LineKey, PointNbr) %>%
    # TopCanopy moss needs to be handled differently - move to lower code and drop TopCanopy
    # make sure we don't touch SoilSurface
    mutate(has_2lich_top = any(layer == "TopCanopy" & code == "2LICHN1")) %>%

    # only mutating the moss top groups
    mutate(
      layer = case_when(
        !has_2lich_top ~ layer,               # Do nothing if no 2MOSS on top
        layer == "SoilSurface" ~ layer,        # Do nothing to SoilSurface
        layer == "TopCanopy" ~ "Lower1",       # 2MOSS (TopCanopy) becomes Lower1

        # For any LowerX, extract the number, add 1, and paste it back - that way if Lower5 becomes Lower6 and we keep SoilSurface
        str_detect(layer, "Lower") ~ {
          old_num <- as.numeric(str_extract(layer, "\\d+"))
          paste0("Lower", old_num + 1)
        },

        TRUE ~ layer
      )
    ) %>%
    #
    # since 2MOSS is now Lower1, we just filter out any remaining TopCanopy rows
    # (which only exist in the non-moss groups now).
    filter(!(has_2lich_top & layer == "TopCanopy")) %>%
    # drop the new column
    select(-has_2lich_top) %>%
    ungroup()

  saveRDS(tall_lpi, file.path(path_tall, "lpi_tall.rds"))
  #write.csv(tall_lpi, file.path(path_tall, "lpi_tall.csv"), row.names = F)

  return(tall_lpi)
}
####################################



############################################
#' Clean Tall Gap NRI
#'
#'removes and adds columns to the tall_gap file produced using terradactyl::gather_gap that are necessary to produce geofiles
#'
#' @param tall_gap as a data.frame, tall gap file produced from terradactyl::gather_gap
#' @param dataHeader as a data.frame, the dataHeader file produced from terradactylutils2::create_header
#' @param tblGapHeader for NRI, this remains NULL
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return an updated tall_gap file saved to path_tall and tall_gap in the console (unless saved to an object)
#'
#' @examples clean_tall_gap(tall_gap = terradactyl::gather_gap(source = "DIMA", tblGapHeader = tblGapHeader, tblGapDetail = tblGapDetail2), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_gap_nri <- function(tall_gap, dataHeader, path_tall, tblGapHeader = NULL){

  dropcols_gap <- tall_gap  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_gap <- tall_gap[which(!duplicated(dropcols_gap)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_gap <- tall_gap |>
    transform(
      DBKey       = dataHeader$DBKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      DateVisited = dataHeader$DateVisited[match(PrimaryKey, dataHeader$PrimaryKey)],
      ProjectKey  = dataHeader$ProjectKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      Direction   = NA
    )
  # remove GapEnd NAs when there is already a good entry
  tall_gap <- tall_gap %>%
    group_by(PrimaryKey, LineKey, SeqNo) %>%
    # keep the row if:
    # 1. it is the only row in the group (n() == 1)
    # 2. OR if it is part of a group but the GapEnd is NOT NA
    filter(n() == 1 | !is.na(GapEnd)) %>%
    ungroup()

  saveRDS(tall_gap, file.path(path_tall, "gap_tall.rds"))
  #write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = F)
  return(tall_gap)
}
#####################################



#####################################
#' Clean Tall Soil Stability NRI
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
clean_tall_soil_stability_nri <- function(tall_soil_stability, dataHeader, path_tall){

  dropcols_soil_stability <- tall_soil_stability  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))

  pkeys <- dataHeader$PrimaryKey
  tall_soil_stability <- tall_soil_stability[which(!duplicated(dropcols_soil_stability)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_soil_stability <- tall_soil_stability |>
    transform(
      DBKey       = dataHeader$DBKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      ProjectKey  = dataHeader$ProjectKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      Hydro       = FALSE
    )
  saveRDS(tall_soil_stability, file.path(path_tall, "soil_stability_tall.rds"))
  #write.csv(tall_soil_stability, file.path(path_tall, "soil_stability_tall.csv"), row.names = F)
  return(tall_soil_stability)
}
##################################


########################################
#' Clean Tall Species Richness NRI
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
clean_tall_species_inventory_nri <- function(tall_species, dataHeader, path_tall){

  dropcols_species <- tall_species  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_species <- tall_species[which(!duplicated(dropcols_species)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_species <- tall_species |>
    transform(
      DBKey       = dataHeader$DBKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      ProjectKey  = dataHeader$ProjectKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      Direction   = NA
    )
  saveRDS(tall_species, file.path(path_tall, "species_inventory_tall.rds"))
 #write.csv(tall_species, file.path(path_tall, "species_inventory_tall.csv"), row.names = F)

  return(tall_species)
}
########################################

########################################
#' Clean Tall Height NRI
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
clean_tall_height_nri <- function(tall_height, dataHeader, tblLPIHeader, path_tall){

  dropcols_height <- tall_height  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "heightList")))
  pkeys <- dataHeader$PrimaryKey
  tall_height <- tall_height[which(!duplicated(dropcols_height)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_height <- tall_height |>
    transform(
      DBKey          = dataHeader$DBKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      ProjectKey     = dataHeader$ProjectKey[match(PrimaryKey, dataHeader$PrimaryKey)],
      FormType       = NA,
      source         = "NRI",
      DateVisited    = dataHeader$DateVisited[match(PrimaryKey, dataHeader$PrimaryKey)],
      DateLoadedInDb = Sys.Date(),
      FormDate       = dataHeader$DateVisited[match(PrimaryKey, dataHeader$PrimaryKey)]
    )

  saveRDS(tall_height, file.path(path_tall, "height_tall.rds"))
  #write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = F)

  return(tall_height)
}
##################################

