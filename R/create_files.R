


###############################################
#' Create species list from DIMA Tables
#'
#'Creates a species list using the information from species tables in the DIMA
#'
#' @param tblSpeciesGeneric as a data.frame tblSpeciesGeneric from DIMA tables
#' @param tblSpecies as a data.frame tblSpecies from DIMA tables
#' @param projectkey list of the unique ProjectKeys, which can be found in the dataHeader created using terradactylutils2::create_header()
#' @param path_species_main path to where you save your species lists
#' @param species_list_NOT_created T when a species list has not been created; F if user already has a species list for geoindicator calculations
#' @param USDA_plants a data.frame of the USDA plants with the 4 letter code, GrowthHabit and Duration
#' @param speciescode the column name in the USDA plant list file that contains the four letter codes
#'
#' @return CSV(s) of a species list for each ProjectKey provided by the user
#'
#' @examples create_species_list(species_list_NOT_created = T, tblSpeciesGeneric = tblSpeciesGeneric, tblSpecies = tblSpecies, projectkey = unique(dataHeader$ProjectKey), path_species_main = paste0("D:/data_preparation_docs_used_from_06012024_04302025/data_preparation_docs_used_from_06012024_04302025/Docs for data prep/Data/species_lists/"), USDA_plants = read.csv(USDA_plants.csv), speciescode = "UpdatedSpeciesCode")
#' @export
create_species_list <- function(species_list_NOT_created,tblSpeciesGeneric, tblSpecies, projectkey, path_species_main, USDA_plants, speciescode){
  if(species_list_NOT_created){
    woody_codes <- c(1:4)
    tblSpeciesGeneric$GrowthHabit <- ifelse(tblSpeciesGeneric$GrowthHabitCode %in% woody_codes, "Woody",
                                            "NonWoody")

    tblSpeciesGeneric$GrowthHabitSub <- ifelse(tblSpeciesGeneric$GrowthHabitCode ==1, "Tree",
                                               ifelse(tblSpeciesGeneric$GrowthHabitCode ==2, "Shrub",
                                                      ifelse(tblSpeciesGeneric$GrowthHabitCode ==3, "SubShrub",
                                                             ifelse(tblSpeciesGeneric$GrowthHabitCode ==4, "Succulent",
                                                                    ifelse(tblSpeciesGeneric$GrowthHabitCode ==5, "Forb",
                                                                           ifelse(tblSpeciesGeneric$GrowthHabitCode ==6, "Graminoid", "Sedge"))))))

    tblSpeciesGeneric$Noxious <- ""
    tblSpeciesGeneric$Invasive <- NA
    tblSpeciesGeneric$UpdatedSpeciesCode <- ""
    tblSpeciesGeneric$Notes <- ""
    tblSpeciesGeneric$SpeciesState <- tblSpeciesGeneric$project
    tblSpeciesGeneric$SG_Group <- ""
    tblSpeciesGeneric$HigherTaxon<- ""
    tblSpeciesGeneric$Nonnative<- ""
    tblSpeciesGeneric$SpecialStatus<- ""
    tblSpeciesGeneric$Photosynthesis<- ""
    tblSpeciesGeneric$PJ	<- ""
    tblSpeciesGeneric$CurrentPLANTSCode <- ""

    generic_keep <- tblSpeciesGeneric %>% dplyr::select(SpeciesCode,ScientificName, Family, GrowthHabit, GrowthHabitSub, Duration,
                                                        Noxious, Invasive,UpdatedSpeciesCode, Notes,
                                                        SpeciesState,SG_Group,HigherTaxon, Nonnative, SpecialStatus, Photosynthesis, PJ, CurrentPLANTSCode)

    tblSpecies$GrowthHabit <- ifelse(tblSpecies$GrowthHabitCode %in% woody_codes, "Woody",
                                     "NonWoody")

    tblSpecies$GrowthHabitSub <- ifelse(tblSpecies$GrowthHabitCode ==1, "Tree",
                                        ifelse(tblSpecies$GrowthHabitCode ==2, "Shrub",
                                               ifelse(tblSpecies$GrowthHabitCode ==3, "SubShrub",
                                                      ifelse(tblSpecies$GrowthHabitCode ==4, "Succulent",
                                                             ifelse(tblSpecies$GrowthHabitCode ==5, "Forb",
                                                                    ifelse(tblSpecies$GrowthHabitCode ==6, "Graminoid", "Sedge"))))))

    tblSpecies$Noxious <- ""
    tblSpecies$Invasive <- ifelse(tblSpecies$Invasive == 0, FALSE,
                                  ifelse(tblSpecies$Invasive == 1, TRUE,  NA ))
    tblSpecies$UpdatedSpeciesCode <- USDA_plants$UpdatedSpeciesCode[match(tblSpecies$SpeciesCode, USDA_plants$SpeciesCode)]
    tblSpecies$Notes<- ""
    tblSpecies$SpeciesState<- tblSpecies$project
    tblSpecies$SG_Group<- ""
    tblSpecies$HigherTaxon<- ""
    tblSpecies$Nonnative<- ""
    tblSpecies$SpecialStatus<- ""
    tblSpecies$Photosynthesis<- ""
    tblSpecies$PJ	<- ""
    tblSpecies$CurrentPLANTSCode <- tblSpecies$UpdatedSpeciesCode


    species_keep <- tblSpecies %>% dplyr::select(SpeciesCode,ScientificName, Family, GrowthHabit, GrowthHabitSub,Duration,
                                                 Noxious, Invasive,UpdatedSpeciesCode, Notes,
                                                 SpeciesState,SG_Group,HigherTaxon, Nonnative, SpecialStatus, Photosynthesis, PJ, CurrentPLANTSCode)
    #for correct geoindicator calculations, the species list must have the updated species code in the species code column, having all the attributes described
    `%notin%` <- Negate(`%in%`)
    updated_species <- species_keep[!is.na(species_keep$UpdatedSpeciesCode), ]
    updated_species <- updated_species|> filter(updated_species$UpdatedSpeciesCode %notin% updated_species$SpeciesCode)

    if(nrow(updated_species) > 0){
      updated_species$SpeciesCode <- updated_species$UpdatedSpeciesCode
      species_keep <- rbind(species_keep, updated_species)}

    species <- rbind(species_keep, generic_keep)

    for(projkey in projectkey){
      #path_species <- "D:/data_preparation_docs_used_from_06012024_04302025/data_preparation_docs_used_from_06012024_04302025/Docs for data prep/Data/species_lists/species_"
      projsp <- species |> subset(species$SpeciesState == projkey)
      write.csv(projsp, paste0(path_species_main,"species_",  projkey, ".csv"), row.names = F)

    }
  }else{
    print("User selected that species list is already created, thus not using DIMA files to produce a species list")
  }
}
###############################################








################################################
#' Create header
#'
#'creates the header table used to produce all of the tall tables
#'
#' @param path_tall a file path where your tall data will be stored, that is saved within path_parent, where path_parent is the path with the dima exports file for the project and export files (tall, for ingest and QC files)
#' @param tblPlots tblPlots from the DIMA tables read in as a data.frame
#' @param todaysDate today's date
#' @param source source type such as "DIMA" or "Terradat"
#' @param by_species_key whether the SpeciesState in the header differentiates by state (T) or by ProjectKey(F)
#'
#' @return  RDS and CSV of header saved to the tall file directory (path_tall) as well as a data.frame in your console (unless set to an object) with the name dataHeader
#'
#' @examples create_header(path_tall = file.path(path_parent, "Tall"), tblPlots = tblPlots, todaysDate = format(Sys.Date(), "%m/%d/%Y"), source = "DIMA", by_species_key = FALSE)
#' @export
create_header <- function (path_tall,tblPlots,todaysDate, source,  by_species_key){
  problem_pk <- primarykey_qc$PrimaryKey[primarykey_qc$Action=="Delete"]
  tblPlots <- tblPlots |> subset(!PrimaryKey %in% problem_pk)
  dataHeader <- tblPlots |>
    rename(
      ProjectKey = project,
      DBKey = dbname,
      Latitude_NAD83 = Latitude,
      Longitude_NAD83 = Longitude,
      EcologicalSiteId = EcolSite
    )
  dataHeader$SiteID <- tblSites$SiteID[match(dataHeader$SiteKey, tblSites$SiteKey)]
  dataHeader$RecKey <- dataHeader$PlotKey

  header2 <- dataHeader

  # keeping only cols of interest
  dataHeader <- dataHeader |> dplyr::select(ProjectKey, PrimaryKey, DateVisited, Latitude_NAD83, Longitude_NAD83,
                                            DBKey, State, County, PlotID, RecKey,EcologicalSiteId)


  # adding remaining details needed for dataHeader

  dataHeader$PercentCoveredByEcoSite <- rep(NA) # leaving blank, doesn't impact calcs
  dataHeader$wkb_geometry <- rep(NA) # leaving blank, doesn't impact calcs
  dataHeader$DateLoadedInDb <- rep(todaysDate)
  dataHeader$source <- rep(source)
  # for species join to work properly, the SpecieState needs to be the projectkey
  # unless the project actually is distinguishing the species by state
  if(by_species_key == TRUE){
    dataHeader$SpeciesState <- dataHeader$State
  }

  if(by_species_key == FALSE){
    dataHeader$SpeciesState <- dataHeader$ProjectKey
  }
  #dataHeader$DateVisited <- as.character(dataHeader$DateVisited)

  write.csv(dataHeader, paste0(path_tall,"/header.csv"), row.names = F)
  saveRDS(dataHeader, paste0(path_tall,"/header.rdata"))

  dataHeader

}
###############################

