


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
#' @param gathered_data path where gathered data will be stored
#' @param by_species_key whether the SpeciesState in the header differentiates by state (T) or by ProjectKey(F)
#'
#' @return  RDS and CSV of header saved to the tall file directory (path_tall) as well as a data.frame in your console (unless set to an object) with the name dataHeader
#'
#' @examples create_header(path_tall = file.path(path_parent, "Tall"), tblPlots = tblPlots, todaysDate = format(Sys.Date(), "%m/%d/%Y"), source = "DIMA", by_species_key = FALSE)
#' @export
create_header <- function(path_tall, tblPlots, todaysDate, source, by_species_key = FALSE, gathered_data) {

  # Check if primarykey_qc exists, is not NULL, and has rows/elements before filtering
  if (exists("primarykey_qc") && !is.null(primarykey_qc) && length(primarykey_qc) > 0 && nrow(primarykey_qc) > 0) {
    problem_pk <- primarykey_qc$PrimaryKey[primarykey_qc$Action == "Delete"]
    tblPlots <- tblPlots |> subset(!PrimaryKey %in% problem_pk)
  }

  # 1. Define your target map (New_Name = "Old_Name")
  rename_map <- c(
    ProjectKey       = "project",
    DBKey            = "dbname",
    Latitude_NAD83   = "Latitude",
    Longitude_NAD83  = "Longitude",
    EcologicalSiteId = "EcolSite"
  )

  # 2. Start with your base dataframe
  dataHeader <- tblPlots

  # 3. Loop through and apply renames conditionally
  for (new_name in names(rename_map)) {
    old_name <- rename_map[[new_name]]

    # Check if the old column exists to be renamed
    if (old_name %in% names(dataHeader)) {

      # Check if the new name is already taken
      if (new_name %in% names(dataHeader)) {
        # Fixed to dynamically show the actual duplicate column name
        warning(paste0(new_name, " already exists; not replacing ", old_name))
      } else {
        # Rename safely using base bracket notation to avoid tidyverse overhead in a loop
        names(dataHeader)[names(dataHeader) == old_name] <- new_name
      }

    }
  }

  dataHeader$SiteID <- tblSites$SiteID[match(dataHeader$SiteKey, tblSites$SiteKey)]
  dataHeader$RecKey <- dataHeader$PlotKey

  header2 <- dataHeader

  # keeping only cols of interest
  dataHeader <- dataHeader |> dplyr::select(
    ProjectKey, PrimaryKey, DateVisited, Latitude_NAD83, Longitude_NAD83,
    DBKey, State, County, PlotID, RecKey, EcologicalSiteId
  )

  # adding remaining details needed for dataHeader
  dataHeader$PercentCoveredByEcoSite <- rep(NA) # leaving blank, doesn't impact calcs
  dataHeader$wkb_geometry <- rep(NA) # leaving blank, doesn't impact calcs
  dataHeader$DateLoadedInDb <- rep(todaysDate)
  dataHeader$source <- rep(source)

  # for species join to work properly, the SpecieState needs to be the projectkey
  # unless the project actually is distinguishing the species by state
  if (by_species_key == TRUE) {
    dataHeader$SpeciesState <- dataHeader$State
  } else {
    dataHeader$SpeciesState <- dataHeader$ProjectKey
  }

  write.csv(dataHeader, paste0(path_tall, "/header.csv"), row.names = F)
  saveRDS(dataHeader, paste0(path_tall, "/header.rdata"))
  write.csv(dataHeader, paste0(gathered_data, "/header.csv"), row.names = F)

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
#' @param gathered_data path where gathered data will be stored
#' @param path_tall where tall data will be saved
#' @param dsn dsn for AIM data only
#' @param by_species_key whether SpeciesState is by state for DIMA data

#'
#' @return a dataHeader that is in the expected LDC format to the tall file path
#'
#' @export
create_header_all <- function(source, path_original_files = NULL, path_tall, dsn = NULL, gathered_data, by_species_key){
  if(source == "NRI"){
    dataHeader <- terradactyl::gather_header_nri(dsn = path_original_files, point_path = "POINT.csv", speciesstate = "NRI")
    dataHeader$LocationStatus <- "Obscured"
    dataHeader$State <- dataHeader$STATE
    dataHeader$Latitude_NAD83 <- NA
    dataHeader$Longitude_NAD83 <- NA

    dataHeader <- dataHeader %>%
      # fix date using SURVEY
      mutate(
        temp_date = as.Date(DateVisited),
        yr = year(temp_date),

        # if NA or incorrect year, assign SURVEY as date
        DateVisited = if_else(
          is.na(temp_date) | DateVisited == "0001-01-01" | yr < 1900,
          as.character(SURVEY),
          as.character(DateVisited)
        )
      ) %>%
      # drop columns not used in LDC structure
      select(-yr, -temp_date)
    #remove NAs
    dataHeader <- dataHeader[!is.na(dataHeader$DateVisited),]
    #save
    write.csv(dataHeader, paste0(path_tall,"/header.csv"), row.names = F)
    saveRDS(dataHeader, paste0(path_tall,"/header.rdata"))
    write.csv(dataHeader, paste0(gathered_data,"/header.csv"), row.names = F)


  }else if (source == "BLM_AIM"){
    dataHeader <- gather_header(dsn = dsn,  source = "AIM")
    #remove dups
    dataHeader <- dataHeader[which(!duplicated(dataHeader)),]
    dataHeader$DBKey <- gsub('.{15}$', '', dataHeader$DateVisited)
    dataHeader$ProjectKey <- "BLM_AIM"
    if(by_species_key == TRUE){
      dataHeader$SpeciesState <- dataHeader$State
    }

    if(by_species_key == FALSE){
      dataHeader$SpeciesState <- dataHeader$ProjectKey
    }
    write.csv(dataHeader, file.path(path_tall, "header.csv"), row.names = F)
    saveRDS(dataHeader, file.path(path_tall, "header.rdata"))
    write.csv(dataHeader, paste0(gathered_data,"/header.csv"), row.names = F)

  }else{
    gathered_data <- paste0(path_parent,"/gathered_data")
    dataHeader <- terradactylutils3::create_header(path_tall = path_tall, tblPlots = tblPlots, todaysDate = todaysDate, source = source,
                                                   gathered_data = gathered_data,by_species_key = by_species_key)
  }

  # assign to global environment
  assign("dataHeader", dataHeader, envir = .GlobalEnv)
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
create_dirs <- function(path_parent, source) {

  # 1. Build a named list of paths
  paths <- list(
    path_cache          = file.path(path_parent, "Cache"),
    path_qc             = file.path(path_parent, "QC"),
    path_tall           = file.path(path_parent, "Tall"),
    path_original_files = file.path(path_parent, "original_files"),
    DIMATables          = file.path(path_parent, "DIMATables"),
    sensitive_data      = file.path(path_parent, "sensitive_data"),
    gathered_data       = file.path(path_parent, "gathered_data"),
    Tables              = file.path(path_parent, "Tables"),
    path_foringest      = file.path(path_parent, "For Ingest")
  )

  # 2. Create the directories safely
  if(!dir.exists(path_parent)) dir.create(path_parent)
  if(!dir.exists(paths$path_cache)) dir.create(paths$path_cache)
  if(!dir.exists(paths$path_qc)) dir.create(paths$path_qc)
  if(!dir.exists(paths$gathered_data)) dir.create(paths$gathered_data)

  if(source == "NRI"){
    if(!dir.exists(paths$path_original_files)) dir.create(paths$path_original_files)
    if(!dir.exists(paths$path_qc)) dir.create(paths$path_qc)
    if(!dir.exists(paths$sensitive_data)) dir.create(paths$sensitive_data)
  }

  if(!dir.exists(paths$path_tall)) dir.create(paths$path_tall)
  if(!dir.exists(paths$path_foringest)) dir.create(paths$path_foringest)

  if(source == "DIMA"){
    if(!dir.exists(paths$DIMATables)) dir.create(paths$DIMATables)
  }
  if(source == "DIMA" || source == "Other"){
    if(!dir.exists(paths$Tables)) dir.create(paths$Tables)
  }

  # Return the list of paths to the user/parent function
  return(paths)
}



###################################
#' Gather soil horizons for NRI
#'
#' create soil horizon file from NRI tables
#'
#' @param nri list of nri data frames
#' @param gathered_data path where gathered files are stored
#' @param dataHeader as dataframe dataHeader
#' @param path_schema file path to LDC schema plan
#'
#' @return gathered soil horizon table to path_tall
#'
#' @export
create_soil_horizons_nri <- function(nri, gathered_data, dataHeader, path_schema){
  SH <- nri$SOILHORIZON
  #drop duplicates
  dropcols_hf <- SH  %>% dplyr::select_if(!(names(.) %in% c("rid", "DateModified", "SpeciesList")))
  SH <- SH[which(!duplicated(dropcols_hf)),]

  na_cols <- c("HorizonKey", "HorizonName", "pH", "EC", "ClayPct", "SandPct",
               "SiltPct", "StructureGrade", "StructureSize", "StructureType",
               "StructureQuality", "Hue", "Value", "Chroma", "ColorMoistDry",
               "FragVolGravel", "FragVolCobble", "FragVolStone",
               "FragVolNodule", "FragVolDurinode")
  # Pre-create the columns
  SH[na_cols] <- NA
  # match columns to expected naming in LDC
  SH <- SH %>%
    # many cols are NA, assigning
    mutate(across(all_of(na_cols), ~NA)) %>%
    # match the remaining cols
    mutate(
      ProjectKey        = "NRI",
      DateLoadedInDb    = todaysDate,
      HorizonDepthUpper = DEPTH * 2.54,
      HorizonDepthLower = DEPTH * 2.54,
      DepthUOM          = "cm",
      Texture           = HORIZON_TEXTURE,
      TextureModifier   = TEXTURE_MODIFIER,
      Effervescence     = EFFERVESCENCE_CLASS,
      HorizonNotes      = UNUSUAL_FEATURES,
      HorizonNumber     = SEQNUM,
      source            = "NRI"
    )
  #match is failing - retrieving DateVisited
  dates <- dataHeader %>%
    select(PrimaryKey, DateVisited) %>%
    distinct(PrimaryKey, .keep_all = TRUE) # Ensures one date per Key

  #join to SH
  SH <- SH %>%
    left_join(dates, by = "PrimaryKey")

  # only keep data in schema
  schema <- read.csv(path_schema)
  schema <- schema %>% dplyr::filter(Table == "dataSoilHorizons")
  # schema column order
  ordered_cols <- schema$Field

  # reorder, keeping schema cols
  SH <- SH %>%
    dplyr::select(all_of(ordered_cols))


  write.csv(SH, paste0(gathered_data, "/soil_horizons_tall.csv"), row.names = FALSE)


}





###################################
#' Create MWAC table
#'
#' create MWAC table in the LDC format from DIMA
#'
#' @param tblBSNE_BoxCollection BSNE data from DIMA
#' @param gathered_data path where gathered files are stored
#'
#' @return processed BSNE MWAC data to the path_foringest
#'
#' @export
create_mwac <- function(tblBSNE_BoxCollection, gathered_data){

  # remove bad data
  tblBSNE_BoxCollection <- subset(tblBSNE_BoxCollection, SampleCompromised == "FALSE")
  #drop duplicates
  dropcols_hf <- tblBSNE_BoxCollection  %>% dplyr::select_if(!(names(.) %in% c("rid", "DateModified", "SpeciesList")))
  tblBSNE_BoxCollection <- tblBSNE_BoxCollection[which(!duplicated(dropcols_hf)),]
  #assign project key
  tblBSNE_BoxCollection$ProjectKey <- tblBSNE_BoxCollection$project
  # add correct rid
  tblBSNE_BoxCollection$rid <- seq(1:nrow(tblBSNE_BoxCollection))
  #tblBSNE_BoxCollection$DateEstablished <- as_date(tblBSNE_BoxCollection$DateEstablished.x)
  tblBSNE_BoxCollection$DBKey <- tblBSNE_BoxCollection$dbname
  tblBSNE_BoxCollection$DateLoadedInDb <- todaysDate
  # make sure date columns dont have TZ

  tblBSNE_BoxCollection$collectDate <- lubridate::parse_date_time(tblBSNE_BoxCollection$collectDate,
                                                                  orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS","ymd HM", "mdy HM"))

  attr(tblBSNE_BoxCollection$collectDate, "tzone") <- NULL

  tblBSNE_BoxCollection$DateEstablished <- lubridate::parse_date_time(tblBSNE_BoxCollection$DateEstablished,
                                                                      orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS","ymd HM", "mdy HM"))

  attr(tblBSNE_BoxCollection$DateEstablished, "tzone") <- NULL

  # only keep data in schema, in the order of the schema
  schema <- read.csv(path_schema)
  schema <- schema %>% dplyr::filter(Table == "dataHorizontalFlux")
  # schema column order
  ordered_cols <- schema$Field

  # reorder BSNE, keeping schema cols
  tblBSNE_BoxCollection <- tblBSNE_BoxCollection %>%
    dplyr::select(all_of(ordered_cols))


  write.csv(tblBSNE_BoxCollection, paste0(gathered_data, "/horizontalflux_tall.csv"), row.names = FALSE)

  return(tblBSNE_BoxCollection)
}





###################################
#' Create DDT table
#'
#' create DDT table in the LDC format from DIMA
#'
#' @param tblBSNE_TrapCollection BSNE data from DIMA
#' @param gathered_data path where gathered files are stored
#' @param path_schema file path for LDC schema plan
#'
#' @return processed BSNE DDT data to the path_foringest
#'
#' @export
create_ddt <- function(tblBSNE_TrapCollection, gathered_data, path_schema){

  # remove bad data
  tblBSNE_TrapCollection <- subset(tblBSNE_TrapCollection, SampleCompromised == "FALSE")
  #drop duplicates
  dropcols_ddt <- tblBSNE_TrapCollection  %>% dplyr::select_if(!(names(.) %in% c("rid", "DateModified", "SpeciesList")))
  tblBSNE_TrapCollection <- tblBSNE_TrapCollection[which(!duplicated(dropcols_ddt)),]
  #assign project key
  tblBSNE_TrapCollection$ProjectKey <- tblBSNE_TrapCollection$project
  #tblBSNE_BoxCollection$DateEstablished <- as_date(tblBSNE_BoxCollection$DateEstablished.x)
  tblBSNE_TrapCollection$DBKey <- tblBSNE_TrapCollection$dbname
  tblBSNE_TrapCollection$DateLoadedInDb <- todaysDate

  tblBSNE_TrapCollection$collectDate <- lubridate::parse_date_time(tblBSNE_TrapCollection$collectDate,
                                                                   orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS","ymd HM", "mdy HM"))

  attr(tblBSNE_TrapCollection$collectDate, "tzone") <- NULL

  tblBSNE_TrapCollection$DateEstablished <- lubridate::parse_date_time(tblBSNE_TrapCollection$DateEstablished,
                                                                       orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS","ymd HM", "mdy HM"))

  attr(tblBSNE_TrapCollection$DateEstablished, "tzone") <- NULL

  # only keep data in schema, in the order of the schema - can't use schema until Kris updates
  # schema <- read.csv(path_schema)
  # schema <- schema %>% dplyr::filter(Table == "dataDustDeposition")
  ddtschema <- read.csv(path_schema)
  ddtschema <- ddtschema %>% dplyr::filter(Table == "dataDustDeposition")

  tblBSNE_TrapCollection$DustDepositionRate <- tblBSNE_TrapCollection$sedimentWeight/(tblBSNE_TrapCollection$trapOpeningArea*0.0001)/tblBSNE_TrapCollection$daysExposed
  # # schema column order
  ordered_cols <- ddtschema$Field

  # reorder BSNE, keeping schema cols
  tblBSNE_TrapCollection <- tblBSNE_TrapCollection %>%
    dplyr::select(all_of(ordered_cols))


  write.csv(tblBSNE_TrapCollection, paste0(gathered_data, "/dustdeposition_tall.csv"), row.names = FALSE)
return(tblBSNE_TrapCollection)
}


###################################
#' Create OG AIM files
#'
#' save original AIM files
#'
#' @param path_parent path to parent folder where all data are being saved
#' @param dsn path to gdb for AIM
#'
#' @return saves files to For Ingest and DIMATables folder to be used for QC
#'
#' @export

create_aim_og_files <- function(dsn, path_parent){


# write the DIMATables
#st_layers(dsn)
DIMATables <- file.path(path_parent, "DIMATables")
if(!dir.exists(DIMATables)) dir.create(DIMATables)

tblLPIDetail <- st_read(dsn = dsn, layer = "AIM_TerrestrialTerradat__F_tblLPIDetail")
tblGapDetail <- st_read(dsn = dsn, layer = "AIM_TerrestrialTerradat__F_tblGapDetail")
tblSoilStabDetail <- st_read(dsn = dsn, layer = "AIM_TerrestrialTerradat__F_tblSoilStabDetail")
tblLPIHeader <- st_read(dsn = dsn, layer = "AIM_TerrestrialTerradat__F_tblLPIHeader")
tblLines <- st_read(dsn = dsn, layer = "AIM_TerrestrialTerradat__F_tblLines")
tblPlots <- st_read(dsn = dsn, layer = "AIM_TerrestrialTerradat__F_tblPlots")




write.csv(tblLPIDetail, paste0(DIMATables, "/tblLPIDetail.csv"))
write.csv(tblGapDetail, paste0(DIMATables, "/tblGapDetail.csv"))
write.csv(tblSoilStabDetail, paste0(DIMATables, "/tblSoilStabDetail.csv"))
write.csv(tblLPIHeader, paste0(DIMATables, "/tblLPIHeader.csv"))
write.csv(tblLines, paste0(DIMATables, "/tblLines.csv"))
write.csv(tblPlots, paste0(DIMATables, "/tblPlots.csv"))

}




###################################
#' Create geoindicators ALL
#'
#' add missing PrimaryKeys to geoIndicators
#'
#' @param path_schema path to LDC schema plan
#' @param path_foringest path to For Ingest folder
#' @param path_parent path to parent folder where all data are being saved
#' @param BSNE_only True or False, True if only BSNE data are being processed
#'
#' @return saves geoIndicators file to For Ingest having all PrimaryKeys
#'
#' @export

create_geoind_ALL <- function(path_schema, path_foringest, path_parent, BSNE_only){
#assign NA vals to geoind where indicators not calculated
if(BSNE_only){
  create_geoind(path_schema = path_schema, path_parent = path_parent)
}else{
  geoind <- read.csv(paste0(path_foringest, "/geoIndicators.csv"))
  head <- read.csv(paste0(path_foringest, "/dataHeader.csv"))

  all_pkeys <- head$PrimaryKey

  # pkeys not in the geoind file
  missing_pkey_values <- all_pkeys[!all_pkeys %in% geoind$PrimaryKey]

  if(NROW(missing_pkey_values) > 0){
    # create df of missing with NA for geoind cols
    missing_rows <- data.frame(PrimaryKey = missing_pkey_values)
    other_cols <- setdiff(names(geoind), "PrimaryKey")
    missing_rows[other_cols] <- NA
    # and recombine
    geoind <- rbind(geoind, missing_rows)


    write.csv(geoind, paste0(path_foringest,"/geoIndicators.csv"))
  }
}

}






###################################
#' Create species list AIM
#'
#' create species list from the dsn
#'
#' @param dsn dsn
#' @param example_path path to example species list with correct header
#' @param path_species path where species list are stored in LDC expected format
#'
#' @return species list from AIM dsn
#'
#' @export
create_species_list_AIM <- function(dsn, example_path, path_species){
#AIM species list

#
# # #species
splist <- st_read(dsn = dsn, layer = "AIM_TerrestrialTerradat__I_Species")


# keep only names like in example

example <- read.csv(example_path)

cols_to_keep <- names(example)

splist <- splist[, names(splist) %in% cols_to_keep]

splist$SpeciesCode <- splist$CurrentPLANTSCode

splist <- splist[!duplicated(splist$SpeciesCode),]

splist$SpeciesCode <- trimws(splist$SpeciesCode)
#
#

test <- st_read(dsn = dsn, layer = "AIM_Terrestrial__F_tblNationalPlants")
test$SpeciesCode <- test$NameCode
test <- test %>%
  dplyr::left_join(splist %>% dplyr::select(SpeciesCode, SG_Group), by = "SpeciesCode")
path_specieslist <- paste0(path_species, "BLM_AIM.csv")
write.csv(test, path_specieslist)

}



#' Create Species List from National Species List Layer RW
#'
#' @description Extracts and formats the national species list layer from an AIM Wetland
#' geodatabase, appends project-specific metadata and dummy attributes, and writes the
#' output to a standard local CSV file directory.
#'
#' @param dsn Character. The Data Source Name (typically a path to a File Geodatabase `.gdb`).
#' @param projectkey Character. The unique identifier code for the target project/state.
#' @param output_dir Character. The base directory path where the generated CSV should be saved.
#'
#' @return An invisible `data.frame` containing the formatted species list data.
#' @export
#'
#' @importFrom sf st_read
#' @importFrom dplyr mutate across
#' @importFrom readr write_csv
#' @importFrom utils head
create_species_list_RW <- function(dsn, projectkey, output_dir) {
  library(sf)
  library(dplyr)
  library(readr)

  # Read in species list layer
  sp <- sf::st_read(dsn = dsn, layer = "AIM_Wetland__S_NationalSpeciesList", quiet = TRUE)

  # Streamline column additions using mutate and across
  sp <- sp %>%
    dplyr::mutate(
      SpeciesCode        = Symbol,
      UpdatedSpeciesCode = Symbol,
      CurrentPLANTSCode  = Symbol,
      SpeciesState       = projectkey,
      # Safely initialize the non-existent columns to NA
      Noxious            = NA,
      Invasive           = NA,
      HigherTaxon        = NA,
      Nonnative          = NA,
      SpecialStatus      = NA,
      Photosynthesis     = NA,
      PJ                 = NA
    )

  # Ensure export directory exists safely
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  readr::write_csv(sp, file.path(output_dir, "species_BLM_AIM_RW.csv"))

  return((sp))
}


#' Create DIMA-Compatible Tables from RW DSN
#'
#' @description Reads Line Point Intercept (LPI) and Species Richness layers from an
#' AIM Wetland Geodatabase, transforms spatial coordinates, cleans tracking metrics,
#' validates plot keys, and outputs formatted DIMA and Tall relational tables.
#'
#' @param dsn Character. The Data Source Name (typically a path to a File Geodatabase `.gdb`).
#' @param projectkey Character. The unique identifier code for the target project/state.
#' @param path_dimatables Character. Directory path where the DIMA export CSVs will be saved.
#' @param path_tall Character. Directory path where the processed "tall" layout CSV and RDS headers will be saved.
#'
#' @return An invisible `data.frame` containing rows from `tblLines` where the `PlotKey`
#' does not successfully match an entry in `tblPlots` (empty if data integrity is clean).
#' @export
#'
#' @importFrom sf st_read st_as_sf st_transform st_coordinates st_drop_geometry st_geometry
#' @importFrom dplyr mutate select rename distinct bind_rows anti_join any_of
#' @importFrom stringr str_sub
#' @importFrom utils write.csv
create_dimatables_RW <- function(dsn, projectkey, path_dimatables, path_tall) {
  # ==========================================
  # 1. READ IN SPECIFIC NECESSARY LAYERS
  # ==========================================
  tblLPIDetail       <- sf::st_read(dsn = dsn, layer = "AIM_Wetland__F_LPIDetail", quiet = TRUE)
  tblLPIHeader       <- sf::st_read(dsn = dsn, layer = "AIM_Wetland__F_LPI", quiet = TRUE)
  tblSpecRichHeader  <- sf::st_read(dsn = dsn, layer = "AIM_Wetland__F_SpeciesInventory", quiet = TRUE)
  tblSpecRichDetail  <- sf::st_read(dsn = dsn, layer = "AIM_Wetland__F_SpecRichDetail", quiet = TRUE)

  # ==========================================
  # 2. LPI & PLOT PROCESSING
  # ==========================================
  lpi <- tblLPIHeader %>%
    dplyr::mutate(
      State                 = projectkey,
      PrimaryKey            = EvaluationID,
      Date                  = as.character(DateFormat),
      DateVisited           = as.Date(Date, format = "%Y-%m-%d"),
      FormDate              = DateVisited,
      Measure               = 1,
      LineLengthAmount      = LineLength,
      SpacingIntervalAmount = interval,
      SpacingType           = "cm",
      HeightOption          = "height",
      HeightUOM             = htinterval,
      ShowCheckbox          = "FALSE",
      CheckboxLabel         = "",
      source                = "DIMA",
      ProjectKey            = projectkey,
      SpeciesKey            = paste0("sp_", projectkey),
      DateLoadedInDb        = Sys.Date(),
      State = AdminState,
      RecKey = LineKey
    )

  # Coordinate transformations using active geometry validation
  lpi <- sf::st_as_sf(lpi)
  sf::st_geometry(lpi) <- sf::st_geometry(lpi)

  lpi <- sf::st_transform(lpi, 4326)
  lpi <- sf::st_transform(lpi, 4269)

  # Extract coordinates safely from sf matrix layout
  coords <- sf::st_coordinates(lpi)

  lpi <- lpi %>%
    dplyr::mutate(
      Latitude  = coords[, "Y"],
      Longitude = coords[, "X"],
      unique_key      = paste0(PrimaryKey, "_", LineKey)
    )

  # Deduplicate LPI records
  lpi <- lpi[!duplicated(lpi$unique_key), ]

  lpi <- lpi %>%
    dplyr::mutate(
      PlotKey = PlotID,
      LineID  = LineNumber
    )

  # ==========================================
  # 3. EXPORT TBLPLOTS & HEADER
  # ==========================================
  # Strip sf tracking properties to convert to a flat dataframe for tabular files
  tblPlots <- lpi %>%
    sf::st_drop_geometry() %>%
    dplyr::select(
      ProjectKey, PrimaryKey, PlotKey, PlotID, DateVisited, Latitude, Longitude,
      DateLoadedInDb, source, State
    )

  tblPlots <- tblPlots[!duplicated(tblPlots$PrimaryKey), ]

  tblPlots <- tblPlots %>%
    dplyr::mutate(
      SpeciesState            = projectkey,
      wkb_geometry            = NA,
      EcologicalSiteId        = NA,
      PercentCoveredByEcoSite = NA,
      SiteKey                 = projectkey,
      EcolSite = NA,
      County = NA
    )

  utils::write.csv(tblPlots, file.path(path_dimatables, "tblPlots.csv"), row.names = FALSE)

  # header <- tblPlots %>% dplyr::mutate(PlotKey = NULL)
  # header <- header[!duplicated(header$PrimaryKey), ]
  #
  # utils::write.csv(header, file.path(path_tall, "header.csv"), row.names = FALSE)
  # saveRDS(header, file.path(path_tall, "header.rdata"))

  # ==========================================
  # 4. EXPORT SITES & LPI TABLES
  # ==========================================
  tblSites <- data.frame(
    SiteKey = projectkey,
    SiteID  = projectkey,
    stringsAsFactors = FALSE
  )
  utils::write.csv(tblSites, file.path(path_dimatables, "tblSites.csv"), row.names = FALSE)

  colnames(tblLPIDetail)[colnames(tblLPIDetail) == 'EvaluationID'] <- 'PrimaryKey'
  tblLPIDetail$ShrubShape <- NA
  tblLPIDetail$SpeciesLowerHerb <- NA
  tblLPIDetail$HeightLowerHerb <- NA
  # keep only cols to prevent height issues
  lpi_keep_cols <- c(
    "PrimaryKey", # Included as a safety key for future joins
    "RecKey", "PointLoc", "PointNbr", "TopCanopy",
    "Lower1", "Lower2", "Lower3", "Lower4", "Lower5", "Lower6", "Lower7",
    "SoilSurface", "HeightTop", "HeightSurface", "HeightWoody", "HeightHerbaceous", "HeightLowerHerb",
    "HeightLower1", "HeightLower2", "HeightLower3", "HeightLower4", "HeightLower5", "HeightLower6", "HeightLower7",
    "ChkboxTop", "ChkboxSoil", "ChkboxWoody", "ChkboxHerbaceous", "ChkboxLowerHerb",
    "ChkboxLower1", "ChkboxLower2", "ChkboxLower3", "ChkboxLower4", "ChkboxLower5", "ChkboxLower6", "ChkboxLower7",
    "SpeciesWoody", "SpeciesHerbaceous", "SpeciesLowerHerb", "ShrubShape"
  )

  # loop through the list: if a column does not exist, initialize it with NA
  for (col in lpi_keep_cols) {
    if (!col %in% names(tblLPIDetail)) {
      tblLPIDetail[[col]] <- NA
    }
  }

  # select only the columns oi, dropping everything else
  tblLPIDetail <- tblLPIDetail |>
    dplyr::select(dplyr::all_of(lpi_keep_cols))
  utils::write.csv(tblLPIDetail, file.path(path_dimatables, "tblLPIDetail.csv"), row.names = FALSE)

  tblLPIHeader <- lpi %>%
    sf::st_drop_geometry() %>%
    dplyr::mutate(
      CheckboxLabel = "",
      chckbox       = NA
    )
  utils::write.csv(tblLPIHeader, file.path(path_dimatables, "tblLPIHeader.csv"), row.names = FALSE)

  # ==========================================
  # 5. EXPORT SPECIES RICHNESS TABLES
  # ==========================================
  # tblSpecRichHeader <- tblSpecRichHeader %>%
  #   sf::st_drop_geometry() %>%
  #   dplyr::mutate(
  #     RecKey       = EvaluationID,
  #     # Convert DateFormat string to true standard Date object tracking
  #     DateVisited  = as.Date(as.character(DateFormat), format = "%Y-%m-%d")
  #   )
  #
  # colnames(tblSpecRichHeader)[colnames(tblSpecRichHeader) == 'EvaluationID'] <- 'PrimaryKey'
  # tblSpecRichHeader$LineKey <- tblSpecRichHeader$PrimaryKey
  # utils::write.csv(tblSpecRichHeader, file.path(path_dimatables, "tblSpecRichHeader.csv"), row.names = FALSE)
  #
  # tblSpecRichDetail <- tblSpecRichDetail %>%
  #   sf::st_drop_geometry() %>%
  #   dplyr::mutate(
  #     RecKey = EvaluationID
  #   )
  #
  # colnames(tblSpecRichDetail)[colnames(tblSpecRichDetail) == 'EvaluationID'] <- 'PrimaryKey'
  # colnames(tblSpecRichDetail)[colnames(tblSpecRichDetail) == 'abundance']   <- 'DENSITY'
  # tblSpecRichDetail$DENSITY <- as.integer(tblSpecRichDetail$DENSITY)
  # utils::write.csv(tblSpecRichDetail, file.path(path_dimatables, "tblSpecRichDetail.csv"), row.names = FALSE)

  # ==========================================
  # 6. TBLLINES PROCESSING & INTEGRITY CHECK
  # ==========================================
  tblLines <- lpi %>%
    sf::st_drop_geometry() %>%
    # Explicitly selected PrimaryKey and DateVisited for schema validation
    dplyr::select(PlotKey, LineKey, LineID, Azimuth, PrimaryKey, DateVisited) %>%
    dplyr::mutate(
      Azimuth = dplyr::if_else(is.na(Azimuth), 999, as.numeric(Azimuth)),
      RecKey  = LineKey
    )
  #
  # lines_spin <- tblSpecRichHeader %>%
  #   dplyr::distinct(PrimaryKey, DateVisited) %>%
  #   dplyr::rename(LineKey = PrimaryKey) %>%
  #   dplyr::mutate(
  #     LineID      = LineKey,
  #     PlotKey     = stringr::str_sub(LineKey, start = 1, end = -12),
  #     Azimuth     = 999,
  #     RecKey      = LineKey,
  #     # Populate tracking keys using local metadata fallbacks
  #     PrimaryKey  = LineKey
  #   )
  #
  # lines_spin <- lines_spin %>%
  #   dplyr::select(dplyr::any_of(base::intersect(names(lines_spin), names(tblLines))))
  #
  # # Combine and drop duplicates based on the specified unique combination
  # tblLines <- tblLines %>%
  #   dplyr::bind_rows(lines_spin) %>%
  #   dplyr::distinct(LineKey, LineID, PlotKey, .keep_all = TRUE)
  #
  # # Integrity check verification matching
  # missing_plots <- tblLines %>% dplyr::anti_join(tblPlots, by = "PlotKey")
  #
  # if (nrow(missing_plots) == 0) {
  #   message("Every PlotKey in tblLines exists in tblPlots.")
  # } else {
  #   warning(paste("Found", nrow(missing_plots), "rows with missing PlotKeys. These PlotKeys must be added to tblPlots before proceeding."))
  # }

  utils::write.csv(tblLines, file.path(path_dimatables, "tblLines.csv"), row.names = FALSE)

  #return(invisible(missing_plots))
}



#' Create tables in terradactyl expected format
#'
#' @description
#' Reads core vegetation field data methods (LPI, Gap, Species Richness) from a spatial database
#' (DSN geodatabase/shapefile), CSV files, or standard R dataframes and converts them to the format
#' needed for terradactyl
#'
#' @param dsn Character. The file path to a spatial database (e.g., `.gdb` folder or folder containing shapefiles). Defaults to `NULL`.
#' @param csv_path Character. The file path to a folder containing raw `.csv` input tables. Defaults to `NULL`.
#' @param projectkey Character. Unique identifier assigned as the `ProjectKey`, `SpeciesState`, and default fallback tracker across tables.
#' @param path_tables Character. The target output directory path where finalized `.csv` tables will be saved.
#' @param tblPlots_input Character string name of the plot layer within the DSN/CSV path, or an inline dataframe object.
#' @param tblLines_input Character string name of the lines layer within the DSN/CSV path, or an inline dataframe object.
#' @param plots_map Named list. Direct mapping of required plot schema attributes to incoming column names (e.g., `list(PrimaryKey = "EvaluationID")`).
#' @param lines_map Named list. Direct mapping of required line schema attributes to incoming column names.
#' @param sites_map Named list. Optional tracking maps for sites schema attributes. Defaults to `NULL`.
#' @param tblPlots_input_alt Optional alternative plot data structures. Defaults to `NULL`.
#' @param tblSites_input Optional alternative site data structures or layer names. Defaults to `NULL`.
#' @param tblLPIHeader_input Character string layer name or dataframe for LPI header data. Defaults to `NULL`.
#' @param lpi_header_map Named list. Column mapping list for the LPI Header table. Defaults to `NULL`.
#' @param tblLPIDetail_input Character string layer name or dataframe for LPI point-level detailed data. Defaults to `NULL`.
#' @param lpi_detail_map Named list. Column mapping list for LPI point details. Defaults to `NULL`.
#' @param tblSpecRichHeader_input Character string layer name or dataframe for Species Richness headers. Defaults to `NULL`.
#' @param spec_rich_header_map Named list. Column mapping list for Species Richness headers. Defaults to `NULL`.
#' @param tblSpecRichDetail_input Character string layer name or dataframe for Species Richness detailed species counts. Defaults to `NULL`.
#' @param spec_rich_detail_map Named list. Column mapping list for Species Richness details. Defaults to `NULL`.
#' @param tblGapHeader_input Character string layer name or dataframe for Canopy/Basal Gap intercept headers. Defaults to `NULL`.
#' @param gap_header_map Named list. Column mapping list for Gap intercept headers. Defaults to `NULL`.
#' @param tblGapDetail_input Character string layer name or dataframe for localized Gap intercept measurements. Defaults to `NULL`.
#' @param gap_detail_map Named list. Column mapping list for raw Gap segments. Defaults to `NULL`.
#' @param crs_option Character. Spatial coordinate reference system projection choice. Options are `"NAD83"` (EPSG 4269) or `"WGS84"` (EPSG 4326). Defaults to `"NAD83"`.
#' @export
create_tables <- function(dsn = NULL,
                          csv_path = NULL,
                          projectkey,
                          path_tables,
                          # Required base core data inputs
                          tblPlots_input,
                          tblLines_input,
                          # Required mapping lists
                          plots_map,
                          lines_map,
                          sites_map = NULL,
                          # All structural tables are now completely optional (Default to NULL)
                          tblPlots_input_alt = NULL,
                          tblSites_input = NULL,
                          tblLPIHeader_input = NULL,
                          lpi_header_map = NULL,
                          tblLPIDetail_input = NULL,
                          lpi_detail_map = NULL,
                          tblSpecRichHeader_input = NULL,
                          spec_rich_header_map = NULL,
                          tblSpecRichDetail_input = NULL,
                          spec_rich_detail_map = NULL,
                          tblGapHeader_input = NULL,
                          gap_header_map = NULL,
                          tblGapDetail_input = NULL,
                          gap_detail_map = NULL,
                          crs_option = "NAD83") {

  library(dplyr)
  library(sf)
  library(utils)

  # --- Helper function for robust date conversion ---
  smart_date <- function(date_vec) {
    if (is.null(date_vec)) return(as.character(NA))
    parsed_date <- suppressWarnings(as.Date(as.character(date_vec)))
    if (all(is.na(parsed_date)) && is.numeric(suppressWarnings(as.numeric(na.omit(date_vec))))) {
      parsed_date <- suppressWarnings(as.Date(as.numeric(date_vec), origin = "1899-12-30"))
    }
    return(format(parsed_date, "%Y-%m-%d"))
  }

  # --- Helper function to build structured schema out of messy data ---
  build_schema_table <- function(raw_input, layer_name, mapping_list, required_cols) {
    df <- NULL
    if (!is.null(dsn)) {
      df <- suppressWarnings(tryCatch(sf::st_read(dsn = dsn, layer = layer_name, quiet = TRUE), error = function(e) NULL))
    } else if (!is.null(csv_path)) {
      file_target <- file.path(csv_path, paste0(raw_input, ".csv"))
      if (file.exists(file_target)) df <- utils::read.csv(file_target, stringsAsFactors = FALSE)
    } else if (is.data.frame(raw_input)) {
      df <- raw_input
    }

    if (is.null(df)) return(NULL)
    if (inherits(df, "sf")) df <- sf::st_drop_geometry(df)

    out_df <- data.frame(matrix(ncol = length(required_cols), nrow = nrow(df)))
    colnames(out_df) <- required_cols

    if ("PrimaryKey" %in% names(mapping_list)) {
      pkey_col <- mapping_list[["PrimaryKey"]]
      if (!is.null(pkey_col) && pkey_col %in% names(df)) {
        out_df$PrimaryKey <- df[[pkey_col]]
      }
    }

    for (req_col in required_cols) {
      if (req_col %in% names(mapping_list)) {
        source_col <- mapping_list[[req_col]]
        if (!is.null(source_col) && source_col %in% names(df)) {
          out_df[[req_col]] <- df[[source_col]]
        }
      }
    }
    return(out_df)
  }

  # ==========================================
  # 1. GENERATE TBLPLOTS
  # ==========================================
  plots_cols <- c("ProjectKey", "PlotKey", "PlotID", "DateVisited", "Latitude", "Longitude",
                  "DateLoadedInDb", "source", "State", "SpeciesState", "wkb_geometry",
                  "EcologicalSiteId", "PercentCoveredByEcoSite", "SiteKey", "EcolSite", "County", "PrimaryKey")

  if (!is.null(dsn)) {
    raw_plots <- sf::st_read(dsn = dsn, layer = tblPlots_input, quiet = TRUE)
    target_epsg <- if (toupper(crs_option) == "WGS84") 4326 else 4269
    raw_plots <- sf::st_transform(raw_plots, target_epsg)
    coords <- sf::st_coordinates(raw_plots)

    df_plots <- sf::st_drop_geometry(raw_plots)
    df_plots$calc_lat <- coords[, "Y"]
    df_plots$calc_lon <- coords[, "X"]

    plots_map$Latitude <- "calc_lat"
    plots_map$Longitude <- "calc_lon"
  } else {
    file_target <- if (!is.null(csv_path)) file.path(csv_path, paste0(tblPlots_input, ".csv")) else NULL
    df_plots <- if (!is.null(file_target) && file.exists(file_target)) utils::read.csv(file_target, stringsAsFactors = FALSE) else tblPlots_input
  }

  tblPlots <- build_schema_table(df_plots, tblPlots_input, plots_map, plots_cols)

  if (!is.null(tblPlots)) {
    tblPlots$ProjectKey      <- projectkey
    tblPlots$SpeciesState    <- projectkey
    tblPlots$DateLoadedInDb  <- format(Sys.Date(), "%Y-%m-%d")
    tblPlots$DateVisited     <- smart_date(tblPlots$DateVisited)

    tblPlots$SiteKey         <- dplyr::coalesce(as.character(tblPlots$SiteKey), projectkey)

    if ("PrimaryKey" %in% colnames(tblPlots)) {
      tblPlots <- tblPlots[!duplicated(tblPlots$PrimaryKey), ]
    }
    utils::write.csv(tblPlots, file.path(path_tables, "tblPlots.csv"), row.names = FALSE)
  }

  # ==========================================
  # 2. GENERATE TBLSITES
  # ==========================================
  sites_cols <- c("SiteKey", "SiteID")
  if (is.null(sites_map)) {
    tblSites <- data.frame(SiteKey = projectkey, SiteID = projectkey, stringsAsFactors = FALSE)
  } else {
    # FIXED: Changed "tblSites" string to tblSites_input variable
    tblSites <- build_schema_table(tblSites_input, tblSites_input, sites_map, sites_cols)
    if (!is.null(tblSites)) {
      tblSites$SiteKey <- dplyr::coalesce(tblSites$SiteKey, projectkey)
      tblSites$SiteID  <- dplyr::coalesce(tblSites$SiteID, projectkey)
    }
  }
  if (!is.null(tblSites)) utils::write.csv(tblSites, file.path(path_tables, "tblSites.csv"), row.names = FALSE)

  # ==========================================
  # 3. GENERATE TBLLINES
  # ==========================================
  lines_cols <- c("PlotKey", "LineKey", "LineID", "Azimuth", "PrimaryKey", "DateVisited", "RecKey")
  # FIXED: Changed "tblLines" string to tblLines_input variable
  tblLines <- build_schema_table(tblLines_input, tblLines_input, lines_map, lines_cols)
  if (!is.null(tblLines)) {
    tblLines$Azimuth     <- suppressWarnings(dplyr::if_else(is.na(tblLines$Azimuth), 999, as.numeric(tblLines$Azimuth)))
    tblLines$DateVisited <- smart_date(tblLines$DateVisited)
    tblLines$RecKey      <- dplyr::coalesce(as.character(tblLines$RecKey), as.character(tblLines$LineKey))
    utils::write.csv(tblLines, file.path(path_tables, "tblLines.csv"), row.names = FALSE)
  }

  # ==========================================
  # 4. CONDITIONAL METHODS & EXTRACTION BLOCKS
  # ==========================================

  # tblLPIHeader
  if (!is.null(lpi_header_map)) {
    lpi_header_cols <- c(
      "LineKey", "RecKey", "DateModified", "FormType", "FormDate", "Observer", "Recorder",
      "DataEntry", "DataErrorChecking", "Direction", "Measure", "LineLengthAmount",
      "SpacingIntervalAmount", "SpacingType", "HeightOption", "HeightUOM", "ShowCheckbox",
      "CheckboxLabel", "State", "DateVisited", "source", "ProjectKey", "SpeciesKey",
      "DateLoadedInDb", "PlotKey", "LineNumber", "PrimaryKey"
    )
    tblLPIHeader <- build_schema_table(tblLPIHeader_input, tblLPIHeader_input, lpi_header_map, lpi_header_cols)
    if (!is.null(tblLPIHeader)) {
      tblLPIHeader$Measure        <- 1
      tblLPIHeader$ProjectKey     <- projectkey
      tblLPIHeader$SpeciesKey     <- paste0("sp_", projectkey)
      tblLPIHeader$DateLoadedInDb <- format(Sys.Date(), "%Y-%m-%d")
      tblLPIHeader$DateVisited    <- smart_date(tblLPIHeader$DateVisited)
      tblLPIHeader$FormDate       <- tblLPIHeader$DateVisited
      tblLPIHeader$RecKey         <- dplyr::coalesce(as.character(tblLPIHeader$RecKey), as.character(tblLPIHeader$LineKey))
      utils::write.csv(tblLPIHeader, file.path(path_tables, "tblLPIHeader.csv"), row.names = FALSE)
    }
  }

  # tblLPIDetail
  if (!is.null(lpi_detail_map)) {
    lpi_detail_cols <- c(
      "PrimaryKey", "RecKey", "PointLoc", "PointNbr", "TopCanopy", "Lower1", "Lower2", "Lower3", "Lower4",
      "SoilSurface", "HeightTop", "ChkboxTop", "ChkboxLower1", "ChkboxLower2", "ChkboxLower3", "ChkboxLower4",
      "ChkboxSoil", "HeightLower1", "HeightLower2", "HeightLower3", "HeightLower4", "HeightSurface",
      "HeightWoody", "HeightHerbaceous", "ShrubShape", "SpeciesWoody", "SpeciesHerbaceous", "ChkboxWoody",
      "ChkboxHerbaceous", "Lower5", "Lower6", "Lower7", "ChkboxLower5", "ChkboxLower6", "ChkboxLower7",
      "HeightLower5", "HeightLower6", "HeightLower7", "SpeciesLowerHerb", "HeightLowerHerb", "ChkboxLowerHerb"
    )
    # FIXED: Changed "tblLPIDetail" string to tblLPIDetail_input variable
    tblLPIDetail <- build_schema_table(tblLPIDetail_input, tblLPIDetail_input, lpi_detail_map, lpi_detail_cols)
    if (!is.null(tblLPIDetail)) utils::write.csv(tblLPIDetail, file.path(path_tables, "tblLPIDetail.csv"), row.names = FALSE)
  }

  # tblSpecRichHeader
  if (!is.null(spec_rich_header_map)) {
    sr_header_cols <- c(
      "LineKey", "RecKey", "DateModified", "FormType", "FormDate", "Observer", "Recorder", "DataEntry",
      "DataErrorChecking", "SpecRichMethod", "SpecRichMeasure", "SpecRichNbrSubPlots", "SpecRich1Container",
      "SpecRich1Shape", "SpecRich1Dim1", "SpecRich1Dim2", "SpecRich1Area", "SpecRich2Container", "SpecRich2Shape",
      "SpecRich2Dim1", "SpecRich2Dim2", "SpecRich2Area", "SpecRich3Container", "SpecRich3Shape", "SpecRich3Dim1",
      "SpecRich3Dim2", "SpecRich3Area", "SpecRich4Container", "SpecRich4Shape", "SpecRich4Dim1", "SpecRich4Dim2",
      "SpecRich4Area", "SpecRich5Container", "SpecRich5Shape", "SpecRich5Dim1", "SpecRich5Dim2", "SpecRich5Area",
      "SpecRich6Container", "SpecRich6Shape", "SpecRich6Dim1", "SpecRich6Dim2", "SpecRich6Area", "Notes", "plotVisitKey", "PrimaryKey"
    )
    # FIXED: Changed "tblSpecRichHeader" string to tblSpecRichHeader_input variable
    tblSpecRichHeader <- build_schema_table(tblSpecRichHeader_input, tblSpecRichHeader_input, spec_rich_header_map, sr_header_cols)
    if (!is.null(tblSpecRichHeader)) utils::write.csv(tblSpecRichHeader, file.path(path_tables, "tblSpecRichHeader.csv"), row.names = FALSE)
  }

  # tblSpecRichDetail
  if (!is.null(spec_rich_detail_map)) {
    sr_detail_cols <- c("RecKey", "subPlotID", "subPlotDesc", "SpeciesCount", "SpeciesList", "PrimaryKey")
    # FIXED: Changed "tblSpecRichDetail" string to tblSpecRichDetail_input variable
    tblSpecRichDetail <- build_schema_table(tblSpecRichDetail_input, tblSpecRichDetail_input, spec_rich_detail_map, sr_detail_cols)
    if (!is.null(tblSpecRichDetail)) utils::write.csv(tblSpecRichDetail, file.path(path_tables, "tblSpecRichDetail.csv"), row.names = FALSE)
  }

  # tblGapHeader
  if (!is.null(gap_header_map)) {
    gap_header_cols <- c(
      "LineKey", "RecKey", "DateModified", "FormType", "FormDate", "Observer", "Recorder", "DataEntry",
      "DataErrorChecking", "Direction", "Measure", "LineLengthAmount", "GapMin", "GapData", "PerennialsCanopy",
      "AnnualGrassesCanopy", "AnnualForbsCanopy", "OtherCanopy", "sumCanCat1", "sumCanCat2", "sumCanCat3",
      "sumCanCat4", "pctCanCat1", "pctCanCat2", "pctCanCat3", "pctCanCat4", "sumBasCat1", "sumBasCat2",
      "sumBasCat3", "sumBasCat4", "pctBasCat1", "pctBasCat2", "pctBasCat3", "pctBasCat4", "Notes",
      "NoCanopyGaps", "NoBasalGaps", "PerennialsBasal", "AnnualGrassesBasal", "AnnualForbsBasal", "OtherBasal", "PrimaryKey"
    )
    # FIXED: Changed "tblGapHeader" string to tblGapHeader_input variable
    tblGapHeader <- build_schema_table(tblGapHeader_input, tblGapHeader_input, gap_header_map, gap_header_cols)
    if (!is.null(tblGapHeader)) utils::write.csv(tblGapHeader, file.path(path_tables, "tblGapHeader.csv"), row.names = FALSE)
  }

  # tblGapDetail
  if (!is.null(gap_detail_map)) {
    gap_detail_cols <- c("RecKey", "SeqNo", "RecType", "GapStart", "GapEnd", "Gap", "PrimaryKey")
    # FIXED: Changed "tblGapDetail" string to tblGapDetail_input variable
    tblGapDetail <- build_schema_table(tblGapDetail_input, tblGapDetail_input, gap_detail_map, gap_detail_cols)
    if (!is.null(tblGapDetail)) utils::write.csv(tblGapDetail, file.path(path_tables, "tblGapDetail.csv"), row.names = FALSE)
  }

  message("Table rendering batch finished successfully!")
}
