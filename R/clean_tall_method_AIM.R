
###################################
#' Clean Tall LPI AIM
#'
#'after gathering lpi, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param tall_lpi as a data.frame, the tall_lpi file
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return updated tall file written to path_tall and a tall_lpi data frame in the console (unless saved to an object)
#'
#' @examples clean_tall_lpi(lpi = terradactyl::gather_lpi(source = source, tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
#' @export
clean_tall_lpi_aim <- function(tall_lpi, path_tall){


dropcols_lpi <- tall_lpi %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] %>%
  dplyr::filter(PrimaryKey %in% pkeys) %>% unique()

# Set classes #can we just add this to terra?
## date fields
lpi <- tall_lpi
if (any(class(lpi) %in% c("POSIXct", "POSIXt"))) {
  change_vars <- names(lpi)[do.call(rbind, vapply(lpi,
                                                  class))[, 1] %in% c("POSIXct", "POSIXt")]
  lpi <- dplyr::mutate_at(lpi, dplyr::vars(change_vars),
                          dplyr::funs(as.character))
}
## text field
# reorder so that primary key is leftmost column
# adding DBKey
lpi$DBKey <- header$DBKey[match(lpi$PrimaryKey,header$PrimaryKey)] # adding outside of terra


lpi <- lpi %>%
  dplyr::select(PrimaryKey, DBKey, LineKey, tidyselect::everything())

# Drop rows with no data
lpi <- lpi %>%
  dplyr::filter(!(is.na(LineKey) &
                    is.na(layer) &
                    is.na(code) &
                    is.na(ShrubShape) &
                    is.na(PointNbr)))


lpi <- lpi %>% tdact_remove_duplicates() %>% tdact_remove_empty(datatype = "lpi")


tall_lpi <- lpi

#dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
# we want to keep the DBKey and DateLoadedInDb
dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c( "rid", "DateModified", "SpeciesList")))

tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] %>%
  dplyr::filter(PrimaryKey %in% pkeys) %>% unique()

tall_lpi$source <- rep(source)
tall_lpi$ProjectKey <- rep(projectkey)
tall_lpi$DateLoadedInDb <- todaysDate
tall_lpi$SpeciesState <- rep("BLM_AIM") # should this be the species state from header??
tall_lpi$DBKey <- header$DBKey[match(tall_lpi$PrimaryKey,header$PrimaryKey)] # adding outside of terra
tall_lpi$ShowCheckbox <- NA
tall_lpi$code<- trimws(tall_lpi$code)
tall_lpi$SpeciesState <- NULL
saveRDS(tall_lpi, file.path(path_tall, "lpi_tall.rdata"))
write.csv(tall_lpi, file.path(path_tall, "lpi_tall.csv"), row.names = F)

}
