


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
    updated_species <- updated_species|> dplyr::filter(updated_species$UpdatedSpeciesCode %notin% updated_species$SpeciesCode)

    if(nrow(updated_species) > 0){
      updated_species$SpeciesCode <- updated_species$UpdatedSpeciesCode
      species_keep <- rbind(species_keep, updated_species)}

    species <- rbind(species_keep, generic_keep)
    #remove duplicates
    species$SpeciesCode <- trimws(toupper(as.character(species$SpeciesCode)))
    species$ScientificName <- trimws(as.character(species$ScientificName))
       species <- species |>
      dplyr::distinct(SpeciesCode, ScientificName, GrowthHabitSub, Duration, .keep_all = TRUE)

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
#' Create geoIndicators
#'
#'creates the geoIndicators table when there is only BSNE data available
#'
#' @param path_schema File path with the latest LDC schema plan
#' @param path_parent File path where the data are being saved including the For Ingest file
#'
#' @return a geoIndicators csv will indicator values NULL
#'
#' @export

create_geoind <- function(path_schema, path_parent){
  # putting back into schema
  schema <- read.csv(path_schema)

  target_schema <- schema %>% filter(Table == "geoIndicators")
  schema_columns <-target_schema$Field

  #getting the for ingest folder and header
  target_dir <- list.dirs(path_parent, full.names = TRUE, recursive = TRUE)
  ingest_folder <- target_dir[grep("For Ingest$", target_dir)]
  ingest_folder <- ingest_folder[1]
  if (length(ingest_folder) > 0) {
    file_path <- list.files(ingest_folder, pattern = "dataHeader.*\\.csv$", full.names = TRUE)[1]
    if (!is.na(file_path)) {
      # Read the data
      header <- read.csv(file_path)

      # missing schema cols
      missing_cols <- setdiff(schema_columns, names(header))

      # add missing columns with NA observations
      header[missing_cols] <- NA

      # keep only the columns defined in the schema and order them correctly
      header <- header[, schema_columns, drop = FALSE]

      # 4. Save the new CSV
      write.csv(header, file.path(ingest_folder, "geoIndicators.csv"), row.names = FALSE)
    } else {
      stop("Could not find 'dataHeader' file.")
    }
  } else {
    stop("Could not find 'ForIngest' folder.")
  }
}




###############################
#' Create header ALL
#'
#'modifies the header post gather_header depending on the source type
#'
#' @param source as a character string, the data source such as "AIM", "NRI" or "DIMA"
#' @param path_original_files path_original_files, for NRI data only
#' @param path_tall where tall data will be saved
#' @param dsn dsn for AIM data only

#'
#' @return a dataHeader that is in the expected LDC format to the tall file path
#'
#' @export
create_header_all <- function(source, path_original_files = NULL, path_tall, dsn_aim = NULL){
  if(source == "NRI"){
    dataHeader <- terradactyl::gather_header_nri(dsn = path_original_files, point_path = "POINT.csv", speciesstate = "NRI")
    dataHeader$LocationStatus <- "fuzzed" #?
    dataHeader$State <- dataHeader$STATE
    dataHeader$Latitude_NAD83 <- NA
    dataHeader$Longitude_NAD83 <- NA
    write.csv(dataHeader, paste0(path_tall,"/header.csv"), row.names = F)
    saveRDS(dataHeader, paste0(path_tall,"/header.rdata"))


  }else if (source == "BLM_AIM"){
    header <- gather_header(dsn = dsn_aim,  source = "AIM")
    #remove dups
    header <- header[which(!duplicated(header)),]
    header$DBKey <- gsub('.{15}$', '', header$DateVisited)
    header$ProjectKey <- "BLM_AIM"
    write.csv(header, file.path(path_tall, "header.csv"), row.names = F)
    saveRDS(header, file.path(path_tall, "header.rdata"))

  }else{
    dataHeader <- terradactylutils3::create_header(path_tall = path_tall, tblPlots = tblPlots, todaysDate = todaysDate, source = source,
                                                   by_species_key = FALSE)
  }
}

###############################
#' Create directories used for LDC data preparation
#'
#'creates any relevant directories depending on the source
#'
#' @param source as a character string, the data source such as "AIM", "NRI" or "DIMA"
#' @param path_parent main directory where all data will be saved
#'
#' @return folders for data preparation for the LDC, if not already created
#'
#' @export
create_dirs <- function(path_parent, source){

  #setting file path for directories that the user does not edit
  path_cache <- file.path(path_parent, "Cache")
  path_qc <- file.path(path_parent, "QC") # path where the QC data will be saved
  path_tall <- file.path(path_parent, "Tall") # path where the tall data will be saved
  path_original_files <- file.path(path_parent, "original_files")
  path_qc <- file.path(path_parent, "QC")
  DIMATables <- file.path(path_parent, "DIMATables")
  sensitive_data <- file.path(path_parent, "sensitive_data")

  # set up directories if not yet in parent folder
  if(!dir.exists(path_parent)) dir.create(path_parent)
  if(!dir.exists(path_cache)) dir.create(path_cache)
  if(!dir.exists(path_qc)) dir.create(path_qc)

  if(source == "NRI"){
    if(!dir.exists(path_original_files)) dir.create(path_original_files)
    if(!dir.exists(path_qc)) dir.create(path_qc)
    if(!dir.exists(sensitive_data)) dir.create(sensitive_data)

  }

  path_tall <- file.path(path_parent, "Tall")
  if(!dir.exists(path_tall)) dir.create(path_tall)
  path_foringest <- file.path(path_parent, "For Ingest")
  if(!dir.exists(path_foringest)) dir.create(path_foringest)
  if(source == "DIMA"){
    if(!dir.exists(DIMATables)) dir.create(DIMATables)}
}
