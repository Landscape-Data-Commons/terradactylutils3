library(devtools)
use_package("tidyr")
use_package("dplyr")
use_package("stringr")
use_package("lubridate")
use_package("tidyselect")
use_package("magrittr")

#########################################
#' Assign keys
#'
#' Assigns all of the keys (primarykey, reckey, linekey, dbkey) to the DIMA tables. When using, ensure that the files are structured in the path_project folder as: "project"/"file(s) titled with the name of the location within that project with _ date data were received with format %Y-%m-%d"/"all the files exported from DIMA with the typical DIMA export naming such as tblGapHeader". For instance, my file structure could be NWERN/dima_exports/NWERN_HAFB/NWERN_HAFB_10062025/tbl...csv. path_qc, DIMATables, path_tall,path_original_files must be objects saved to your environment that are the file paths where outputs will be stored
#'
#' @param path_project folder in path_parent where data for preparation are stored where path_parent is the path where dima exports file for project is stored and where export files (tall, for ingest and QC files) will be stored
#' @param format the format that your tblPlots FormDate is in
#' @param noteformat the format that your tblPlotNotes NoteDate is in
#' @param nonlineformat the format that your nonline data DateRecorded is in
#' @param non_line_tables tables without numeric data in a list
#'
#' @return R data with PrimaryKey, LineKey, PlotKey and RecKey assigned to each plot as well as R data with QC information about PrimaryKey assignment
#' @export
#'
#' @examples assign_keys(path_project = "D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/dima_exports/", format = "%m/%d/%Y", noteformat = "%m/%d/%Y",nonlineformat = "%m/%d/%Y",non_line_tables = c("tblPlots", "tblLines", "tblSites") )
assign_keys <- function(path_project, format, noteformat, nonlineformat,non_line_tables){
  # get list of all export files
  dima_export_files <- data.frame(file_path = list.files(path = path_project,
                                                         pattern = ".csv",
                                                         recursive = T,
                                                         include.dirs = T)) |>
    tidyr::separate_wider_delim(
      file_path,
      "/",
      names = c("project", "dbname", "table"),
      cols_remove = FALSE)







  # this code reads all CSVs at once, appends and assigns table name
  # read all DIMA types and append
  all_dimas <- lapply(X = unique(dima_export_files$table),
                      FUN = function(X) {
                        # read each file associated with a data_type
                        file_list <- dima_export_files$file_path[dima_export_files$table==X]
                        # read the all files associated with a particular DIMA table type and append
                        data <- do.call(rbind, lapply(X = file_list,
                                                      FUN = function(X) {
                                                        print(X)
                                                        data <- read.csv(paste0(path_project,X),
                                                                         # change blanks to NA
                                                                         na.strings = c("", "NA")) |>
                                                          # add file path
                                                          dplyr::mutate(file_path = X) |>
                                                          # join to dima_export file to get project, dbname, and table name
                                                          dplyr::left_join(dima_export_files) |>
                                                          dplyr::select(-c(file_path, table))
                                                      })
                        )
                      })

  #name all of the tables in the all_dimas list
  names(all_dimas) <- unique(dima_export_files$table) |> stringr::str_remove(".csv")



  # the primary key is assigned from the large (appended) CSV with name from table assigned to each observation - however
  # this is done in multiple parts depending on the table type

  # create PrimaryKeys by joining PlotKey to DateVisited. We first have to join to the header tables, then the detail tables
  header_tables <- lapply(X = all_dimas[names(all_dimas) |> stringr::str_detect("Header")],
                          function(X){
                            # if there is already a PlotKey, no need to do anything, otherwise we need to join PlotKey to the table via tblLines
                            if(!"PlotKey" %in% names(X)){
                              data_pk <- dplyr::left_join(
                                X,
                                all_dimas$tblLines |>
                                  dplyr::select(PlotKey, LineKey, project, dbname)|> dplyr::distinct(),
                                relationship = "many-to-one")
                            }else{
                              data_pk <- X
                            }
                            # Now generate Primarykey based on PlotKey and FormDate
                            data_pk <- data_pk |>
                              dplyr::mutate(
                                #Format FormDate
                                DateVisited = as.Date(FormDate, format = format), #format is the current format of FormDate (e.g., format = "%m/%d/%Y")
                                PrimaryKey = paste0(PlotKey, DateVisited),
                                FormDate = as.Date(FormDate, format = format))
                          })


  # join header and detail tables to add PrimaryKey
  detail_list <- names(all_dimas)[names(all_dimas) |> stringr::str_detect("Detail")]
  detail_tables <- lapply(
    # we will work one method at a time through the list
    X = detail_list,
    function(X){
      # we need to find the associated header table
      tblDetail <- all_dimas[[X]]
      tblHeader <- header_tables[[X |> stringr::str_replace(pattern = "Detail",
                                                            replacement = "Header")]]

      # if tblHeader exists, proceed with join
      if(!is.null(tblHeader)){
        data_pk <- dplyr::left_join(
          # join detail table to header
          tblDetail,
          tblHeader %>%
            dplyr::select_if(names(tblHeader) %in% c("PlotKey", "LineKey", "RecKey", "FormDate", "PrimaryKey", "DateVisited", "project", "dbname")),
          relationship = "many-to-one")
      }else{
        print(paste("No header for table", X, "No join performed. Check that this is expected"))
        all_dimas[[X]]
      }
    })

  names(detail_tables) <- detail_list

  # merge the detail and header tables together
  detail_header <- c(detail_tables, header_tables)

  # we also need to get PrimaryKey information into the non-Line based data
  no_lines_tables <- all_dimas[!names(all_dimas) |> stringr::str_detect("Header|Detail")] |> names()
  data_no_lines <- lapply(X = no_lines_tables,
                          function(X){
                            # For tblPlotsNotes, create a PrimaryKey from PlotKey and NoteDate
                            if(X=="tblPlotNotes"){
                              data <- all_dimas[[X]] |> dplyr::mutate(
                                DateVisited = as.Date(NoteDate, format = noteformat),
                                NoteDate = as.Date(NoteDate, format = noteformat),
                                PrimaryKey = paste0(PlotKey, DateVisited))
                            }else
                              # For tblPlotHistory, create a PrimaryKey from PlotKey and DateRecorded
                              if(X=="tblPlotHistory"){
                                data <- all_dimas[[X]] |> dplyr::mutate(
                                  DateVisited = as.Date(DateRecorded, format = nonlineformat),
                                  DateRecorded = as.Date(DateRecorded, format = nonlineformat),
                                  PrimaryKey = paste0(PlotKey, DateVisited))
                              }else
                                # For tblSoilPits, create add a PlotKey and DateVisited. We'll join PrimaryKey later for all plots
                                if(X=="tblSoilPits"){
                                  data <- all_dimas[[X]] |> dplyr::mutate(
                                    DateRecorded = as.Date(DateRecorded, format = nonlineformat)
                                  )
                                }else
                                  # For tblSoilPitHorizons, first join with tblSoilPits, then
                                  # add PlotKey and DateVisited
                                  if(X=="tblSoilPitHorizons"){
                                    data <- dplyr::left_join(all_dimas[[X]],
                                                             all_dimas$tblSoilPits |>
                                                               dplyr::select(PlotKey, DateRecorded, SoilKey, project, dbname))|>
                                      dplyr::mutate(DateRecorded = as.Date(DateRecorded, format = nonlineformat))
                                  }else{
                                    all_dimas[[X]]
                                  }
                          })

  names(data_no_lines) <- no_lines_tables

  # Plots, Lines, SoilPits, and SoilPit Horizons all need PrimaryKeys that correspond with visit of the PlotKey
  table_plots<- non_line_tables # list of nonnumeric tables in data; could include c("tblPlots", "tblLines", "tblSites", "tblSoilPits", "tblSoilPitHorizons")


  # get all of the unique method PrimaryKeys
  unique_pks <- do.call(rbind,
                        lapply(X = names(detail_header),
                               FUN = function(X){
                                 print(X)
                                 # If PlotKey exists, we'll merge
                                 if("PlotKey" %in% names(detail_header[[X]])){

                                   data <-detail_header[[X]] |>
                                     dplyr::select(PlotKey, PrimaryKey, DateVisited, project, dbname) |>
                                     dplyr::mutate(method = X) |>
                                     dplyr::distinct()
                                 }else{
                                   message(paste("No PlotKeys found in table", X, ". This table will be dropped from output"))
                                 }
                               })
  ) |>
    # make sure the methods are distinct, regardless of Header or Detail
    dplyr::mutate(method = method |> stringr::str_remove_all(
      pattern = "Detail|Header|tbl"
    )) |> dplyr::distinct()

  # join to table_plots
  plots_pks <- lapply(X = table_plots,
                      function(X){
                        print(X)
                        data <- data_no_lines[[X]] |>
                          dplyr::left_join(unique_pks |>
                                             # remove method
                                             dplyr::select(-method) |>
                                             dplyr::distinct(),
                                           relationship = "many-to-many")
                      })
  names(plots_pks) <- table_plots






  # QC checking all tables have date and pkey assigned


  # put all the tables together
  all_dimas_pks <- c(plots_pks, data_no_lines[!names(data_no_lines) %in% table_plots], detail_header)

  # QC
  # First, check that all tables have a PrimaryKey and DateVisited assigned
  primarykey_check <- do.call(
    rbind,lapply(X = names(all_dimas_pks),
                 function(X){
                   data <- all_dimas_pks[[X]]
                   data <- data.frame(table = X) |>
                     dplyr::mutate(primarykey_check = dplyr::if_else(
                       "PrimaryKey" %in% colnames(all_dimas_pks[[X]]),
                       "Yes", "No")
                     )
                 })
  )

  # Print out the problem tables
  if(nrow(primarykey_check[primarykey_check$primarykey_check=="No"&!primarykey_check$table %in%
                           c("tblSites", "tblSpecies", "tblSpeciesGeneric", "tblNestedFreqSpeciesSummary",
                             "tblNestedFreqSpeciesDetail"),])>0){
    primarykey_check[primarykey_check$primarykey_check=="No"&!primarykey_check$table %in%
                       c("tblSites", "tblSpecies", "tblSpeciesGeneric", "tblNestedFreqSpeciesSummary",
                         "tblNestedFreqSpeciesDetail"),]
  }else{
    print("All PrimaryKeys assigned")
  }







  # assign pkeys to details and compares by pkey 
  # QC PrimaryKeys and DateVisited
  # First we'll see how identify any PrimaryKey issues (e.g., NA, orphaned records)
  pk_date_check <- all_dimas_pks$tblPlots |>
    dplyr::select(PlotKey, PrimaryKey, DateVisited, dbname, project) |>
    # add method
    dplyr::mutate(method = "tblPlots")|>
    dplyr::distinct()|>
    # join to transect data observations
    dplyr::bind_rows(unique_pks)|>
    # make wider so we can compare by PrimaryKey
    # add a value row
    dplyr::mutate(values = "yes") |>
    tidyr::pivot_wider(names_from = method,
                       values_from = values,
                       values_fill = "no")




  # Identify PrimaryKeys where date visits are close to each other--this could mean that unique plots are improperly assigned
  pk_date_check <- pk_date_check |> dplyr::group_by(PlotKey) |>
    dplyr::arrange(desc(DateVisited)) |>
    dplyr::mutate(ClosestDateVisited = dplyr::lead(DateVisited))|>
    dplyr::mutate(DaysDiff = difftime(DateVisited, ClosestDateVisited, units = "days") |>
                    # convert to numeric days
                    stringr::str_remove(" days") |> as.numeric()) |>
    dplyr::ungroup()|>

    # add Notes and Action
    dplyr::mutate(Notes = dplyr::case_when( DaysDiff<=7 ~ "Visit within 7 days",
                                            DaysDiff>7 & DaysDiff<=30 ~ "Visit within 7-30 days",
                                            DaysDiff>7 & DaysDiff<=30 ~ "Visit within 7-30 days",
                                            DaysDiff>30 & DaysDiff<=60 ~ "Visit within 30-60 days",
                                            DaysDiff>60 & DaysDiff<=275 ~ "Visit within 30-275 days"),
                  # recommend action
                  Action = dplyr::case_when(DaysDiff>7 & DaysDiff<=275 ~ "Confirm date visited",
                                            DaysDiff<=7 ~ "Consider grouping date visits"))|>
    # add PlotID information back in to help users trouble shoot
    dplyr::left_join(all_dimas_pks[["tblPlots"]]|> dplyr::select(PrimaryKey, PlotKey, PlotID) |> dplyr::distinct() |> subset(!is.na(PrimaryKey)))







  # code removes NA and generic plots - orphaned records are identified, but deletion is handled within the gather function
  # Flag generic plots and orphaned records for deletion
  pk_date_check <- pk_date_check |>
    # Make a note of the issue
    dplyr::mutate(Notes = dplyr::case_when(is.na(PlotKey) ~ "Orphan records",
                                           PlotKey %in% c("123123123", "999999999") ~ "Generic plots",
                                           .default = Notes),
                  # recommend action
                  Action = dplyr::case_when(is.na(PlotKey) ~ "Delete",
                                            PlotKey %in% c("123123123", "999999999") ~ "Delete",
                                            .default = Action),
                  DataOwnerResponse = NA)
  # Save files for QC
  saveRDS(all_dimas, file.path(path_qc, "all_dimas.Rdata"))
  saveRDS(all_dimas_pks,   file.path(path_qc, "all_dimas_pks.Rdata"))
  write.csv(pk_date_check, file.path(path_qc, "primarykey_date_check.csv"), row.names=FALSE)
  write.csv(pk_date_check |> subset(!is.na(Action)),
            paste0(path_qc,"/primarykey_resolve_", Sys.Date(), ".csv"), row.names=FALSE)
  for(i in names(all_dimas)){
    write.csv(all_dimas[[i]], paste0(DIMATables,"/",i,".csv"))
  }

}
###############################################




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
#' @export
#'
#' @examples create_species_list(species_list_NOT_created = T, tblSpeciesGeneric = tblSpeciesGeneric, tblSpecies = tblSpecies, projectkey = unique(dataHeader$ProjectKey), path_species_main = paste0("D:/data_preparation_docs_used_from_06012024_04302025/data_preparation_docs_used_from_06012024_04302025/Docs for data prep/Data/species_lists/"), USDA_plants = read.csv(USDA_plants.csv), speciescode = "UpdatedSpeciesCode")
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

  generic_keep <- tblSpeciesGeneric %>% dplyr::select(SpeciesCode,ScientificName, Family, GrowthHabit, GrowthHabitSub, Duration,
                                                      Noxious, Invasive,UpdatedSpeciesCode, Notes,
                                                      SpeciesState,SG_Group)

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
  tblSpecies$Notes <- ""
  tblSpecies$SpeciesState <- tblSpecies$project
  tblSpecies$SG_Group <- ""

  species_keep <- tblSpecies %>% dplyr::select(SpeciesCode,ScientificName, Family, GrowthHabit, GrowthHabitSub,Duration,
                                               Noxious, Invasive,UpdatedSpeciesCode, Notes,
                                               SpeciesState,SG_Group)
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





###############################################
#' DIMA table QC
#'
#'checks the data produced from assign_keys, including checks for missingness and missing coordinates. Problem PrimaryKeys are removed. Thus, prior to running, check with the data owner about the PrimaryKeys with NAs or that were sampled within a certain number of days to ensure removing the problem PrimaryKeys is desired.
#'
#' @param dima_data_list as an object, all_dimas_pks.RDS from the assign_keys function
#' @param primarykey_qc as an object, primarykey_resolve_date.csv from assign_keys function
#' @param path_qc path where the QC data will be saved
#'
#' @return CSVs of the QC related to the PrimaryKey assignment in the assigned QC folder
#' @export
#'
#' @examples dima_table_qc(dima_data_list = readRDS("QC/all_dima_pks.Rdata"), primarykey_qc = read.csv(paste0("QC/primarykey_resolve_", date_pkey_qc_run, ".csv")), path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
dima_table_qc <- function(dima_data_list, primarykey_qc, path_qc){
  # we've already identified a few plots as problematic while generating the PrimaryKey, let's remove those
  problem_pk <- primarykey_qc$PrimaryKey[primarykey_qc$Action=="Delete"]

  # check lat/longs
  coord_qc <- dima_data_list[["tblPlots"]] |> subset(is.na(Latitude)|is.na(Longitude)|Latitude==0|Longitude==0) |>
    # remove previously identified PrimaryKeys
    subset(!PrimaryKey %in% problem_pk) |>
    dplyr::select(project, dbname, PlotKey, PlotID, PrimaryKey, Latitude, Longitude) |>
    tidyr::pivot_longer(cols = -c(project, dbname, PlotKey, PlotID, PrimaryKey,),
                        names_to = "Field",
                        values_to = "n_missing") |>
    dplyr::mutate(n_missing = 1,
                  Notes = "Coordinates missing 0 or missing",
                  Action = "Populate or delete plot")





  # check for missingness of observations
  # check for NAs in observations
  missingness <- do.call(rbind,lapply(X = names(dima_data_list),

                                      function(X){
                                        data <- dima_data_list[[X]]

                                        # for tables with PrimaryKeys, check for NAs in columns
                                        if("PrimaryKey" %in% colnames(data)){
                                          # remove previously identified PrimaryKeys
                                          data <- data|>
                                            subset(!PrimaryKey %in% problem_pk)

                                          # identify number of missing rows per field
                                          missingness <- data|>
                                            dplyr::group_by(project, dbname, PrimaryKey) |>
                                            dplyr::summarise(dplyr::across(dplyr::everything(), ~ sum(is.na(.x))))|>
                                            dplyr::ungroup()

                                          # pivot longer so we can summarize
                                          missingness_tall <-  missingness |>
                                            tidyr::pivot_longer(cols = -c("project", "dbname", "PrimaryKey"),
                                                                names_to = "Field",
                                                                values_to = "n_missing")

                                          missingness_summary <- missingness_tall |>
                                            dplyr::group_by(project, dbname,Field) |>
                                            dplyr::summarise(
                                              avg_missing = mean(n_missing),
                                              n_records = dplyr::n()

                                            ) |> dplyr::ungroup()

                                          # join back to tall table
                                          missingness_tall <- missingness_tall |>
                                            dplyr::left_join(missingness_summary) |>
                                            # add interpreation. If the number missing > standard deviation, we'll flag that
                                            dplyr::mutate(
                                              anomaly = (n_missing-avg_missing),
                                              prop_missing = n_missing/n_records
                                            ) |>

                                            # add table identifier
                                            dplyr::mutate(table = X)
                                        }

                                      })
  )

  # Add notes based on the importance of fields
  missingness_notes <- missingness |>
    # add in PlotID info
    dplyr::left_join(dima_data_list[["tblPlots"]] |> dplyr::select(PrimaryKey, PlotID, PlotKey)) |>
    # subset where this is no anomaly
    subset(anomaly!=0) |>
    # subset where there are no missing values
    subset(n_missing>0) |>
    # dplyr::left_join(read.csv("table_fields_importance.csv",
    #                           na.strings = c("", "NA"))) |>
    # # join in the coord_qc table for a comprehensive report
    dplyr::bind_rows(coord_qc)|>
    dplyr::arrange(Notes, Action) |>

    # rearrange for readability
    dplyr::relocate(project, dbname,  PlotKey, PlotID, PrimaryKey) |> # removed table, as third obs until get importance csv

    # add a data owner response column
    dplyr::mutate(DataOwnerResponse = NA)


  SWBC_check <- missingness_notes

  write.csv(SWBC_check, file.path(path_qc, "SWBC_DIMA_check_all.csv"), row.names = F)
  write.csv(SWBC_check |> subset(!is.na(Action)), file.path(path_qc, "SWBC_DIMA_check_resolve.csv"), row.names = F)

}
################################################



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
#' @export
#'
#' @examples create_header(path_tall = file.path(path_parent, "Tall"), tblPlots = tblPlots, todaysDate = format(Sys.Date(), "%m/%d/%Y"), source = "DIMA", by_species_key = FALSE)
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
                                            DBKey, State, PlotID, RecKey,EcologicalSiteId)


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
            names(_) |>
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



###################################
#' Clean Tall LPI
#'
#'after gathering lpi, this function makes adjustments to the tall table that are necessary to produce geofiles and the data prepared for the LDC
#'
#' @param lpi as a data.frame, the tall_lpi file
#' @param dataHeader as a data.frame, the dataHeader file produced from terradactylutils2::create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return updated tall file written to path_tall and a tall_lpi data frame in the console (unless saved to an object)
#' @export
#'
#' @examples clean_tall_lpi(lpi = terradactyl::gather_lpi(source = source, tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
clean_tall_lpi <- function(lpi, dataHeader, path_tall){
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

  pkeys <- dataHeader$PrimaryKey
  dropcols_lpi <- tall_lpi  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  tall_lpi <- tall_lpi[which(!duplicated(dropcols_lpi)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # making sure all codes are capital
  tall_lpi$code <- toupper(tall_lpi$code)
  tall_lpi$ProjectKey <- dataHeader$ProjectKey[match(tall_lpi$PrimaryKey, dataHeader$PrimaryKey)]
tall_lpi$PointLoc <- as.integer(tall_lpi$PointLoc)

  saveRDS(tall_lpi, file.path(path_tall, "lpi_tall.rdata"))
  write.csv(tall_lpi, file.path(path_tall, "lpi_tall.csv"), row.names = F)

  tall_lpi
}
####################################



####################################
#' Tall LPI QC
#'
#'after the lpi tall table is prepared for terradactylutils2::geofiles() using terradactylutils2::clean_tall_lpi(), this function produces the tall_lpi file QC checks
#'
#' @param cleaned_tall_lpi as a data.frame, the tall lpi data that has been through terradactylutils2::clean_tall_lpi
#' @param speciescode the column name in the USDA plant list file that contains the four letter codes
#' @param USDA_plants a data.frame of the USDA plants with the 4 letter code, GrowthHabit and Duration
#' @param tblLPIDetail as a data.frame, the tblLPIDetail from the DIMA tables
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV containing information about the QC of the tall lpi data saved to the designated QC file
#' @export
#'
#' @examples tall_lpi_qc(cleaned_tall_lpi = cleaned_tall_lpi, speciescode = "UpdatedSpeciesCode", tblLPIDetail = tblLPIDetail, USDA_plants = read.csv("D:/modifying_data_prep_script_10032025/2004-2023_ceap_species_list.csv") , path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC")))
tall_lpi_qc <- function(cleaned_tall_lpi, speciescode, USDA_plants, tblLPIDetail, path_qc){
  # list two letter codes and compare to terradat
tall_lpi <- cleaned_tall_lpi
  #get two letter codes
  two_letter <- tall_lpi[nchar(tall_lpi$code) <= 2, ]

  # only keep the unique codes
  #two_letter <- two_letter[!duplicated(two_letter$code),]

  # this is the list currently used in terradactyl for two letter codes
  terra_two_letter <- c("L","HL", "AM", "DN", "ER", "HT", "NL","AL","DS","D","LC","M","WL", "CY","EL",
                        "W","WA","RF","R","GR","ST","CB","BY","VL","AG","CM","LM","FG","PC",
                        "BR","S")
  # determine whether the tall lpi code is associated with the terradactyl two letter codes, if not provide feedback
  two_letter$tl_error <- ifelse(two_letter$code %in% terra_two_letter, 0, 1)
  two_letter$Notes <- ifelse(two_letter$tl_error == 1,
                             "Two letter codes present that are not associated with terradactyl codes", NA)
  two_letter$Action = ifelse(two_letter$tl_error == 1, "Check with project manager to determine what code represents", NA)
  two_letter <- two_letter |> dplyr::select(PrimaryKey, DBKey, LineKey, RecKey, layer, code, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  two_letter <- two_letter[!is.na(two_letter$Notes),]



  # check same number of unique codes as og data
  # get the unique codes from the original data table
  og_codes <- c(tblLPIDetail$TopCanopy, tblLPIDetail$Lower1, tblLPIDetail$Lower2, tblLPIDetail$Lower3,
                tblLPIDetail$Lower4,tblLPIDetail$Lower5, tblLPIDetail$Lower6, tblLPIDetail$Lower7,
                tblLPIDetail$SoilSurface)
  og_codes <- unique(og_codes)

  #determine whether the tall lpi code is a code from the original data
  tall_lpi_codes <- tall_lpi

  tall_lpi_codes$add_codes <- ifelse(tall_lpi_codes$code %in% og_codes, 0, 1)

  # provide feedback where tall lpi codes are not in the original data
  tall_lpi_codes$Notes <- ifelse(tall_lpi_codes$add_codes == 1,
                                 "Codes present that are not in the original data", NA)

  tall_lpi_codes$Action <- ifelse(tall_lpi_codes$add_codes == 1,
                                  "Determine whether code addition was intentional", NA)
  tall_lpi_codes <- tall_lpi_codes |> dplyr::select(PrimaryKey, DBKey, LineKey, RecKey, layer, code, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  tall_lpi_codes <- tall_lpi_codes[!is.na(tall_lpi_codes$Notes),]




  # looking for soil surface codes that are not terradactyl accepted soil surface codes
  # get the unique two letter soil surface codes from the tall lpi
  ss <- tall_lpi |> filter(layer == "SoilSurface")
  ss <- ss[nchar(ss$code) <= 2, ]
  #ss <- ss[!duplicated(ss$code),]
  # these are the two letter surface codes used in terradactyl
  terra_two_letter_surf <- c("DS","D","LC","M", "CY", "EL",
                             "W","WA","RF","R","GR","ST","CB","BY","VL","AG","CM","LM","FG","PC",
                             "BR","S")

  # determine whether the tall lpi surface code is one of the codes from terradactyl

  ss$add_codes <- ifelse(ss$code %in% terra_two_letter_surf, 0, 1)

  # provide feedback where the tall lpi surface code is not associated with the terradactyl codes
  ss$Notes <- ifelse( ss$add_codes == 1,
                      "Soil surface codes present that are not associated with terradactyl", NA)

  ss$Action <- ifelse(ss$add_codes == 1,
                      "Check with the project manager to determine what the code represents", NA)
  ss <- ss |> dplyr::select(PrimaryKey, DBKey, LineKey, RecKey, layer, code, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  ss <- ss[!is.na(ss$Notes),]



  ## identifying where the tall lpi codes are not a USDA plant code
  #get the accepted USDA plant codes
  USDA_plant_codes <- USDA_plants[,paste0(speciescode)]


  # checking that the tall_lpi codes are in the USDA database
  tall_lpi_plant_codes <- tall_lpi[nchar(tall_lpi$code) > 2, ]


  tall_lpi_plant_codes$usda_code <- ifelse(tall_lpi_plant_codes$code %in% USDA_plant_codes, 0, 1)

  # providing feedback for the tall lpi codes that are not in the USDA plant code list
  tall_lpi_plant_codes$Notes <- ifelse( tall_lpi_plant_codes$usda_code == 1,
                                        "Codes present that are not an accepted USDA plant code", NA)
  tall_lpi_plant_codes$Action <- ifelse(tall_lpi_plant_codes$usda_code ==1,
                                        "If not unknown code, confirm with project manager the correct USDA plant code or species attributes", NA)
  tall_lpi_plant_codes <- tall_lpi_plant_codes |> dplyr::select(PrimaryKey, DBKey, LineKey, RecKey, layer, code, Notes, Action)

  # joining multiple tall lpi tables was machine space expensive - only keeping the plots with feedback for later joining
  tall_lpi_plant_codes <- tall_lpi_plant_codes[!is.na(tall_lpi_plant_codes$Notes),]


  # joining the errors for the tall lpi data

  tall_lpi_code_check <-  rbind(two_letter, tall_lpi_codes) %>%
    rbind(., ss) %>% rbind(., tall_lpi_plant_codes)

  # exporting to the QC folder
  write.csv(tall_lpi_code_check, file.path(path_qc, "tall_lpi_code_check.csv"), row.names = FALSE)

  select_me <- c("PrimaryKey", "LineKey", "RecKey","TopCanopy", "SoilSurface")
  og_layers <- tblLPIDetail |> dplyr:: select( all_of(select_me), contains("Lower") & !contains("Chk")& !contains("Height")& !contains("Species"))
  og_layers <- gather(og_layers, layer, code, -PrimaryKey, -LineKey, -RecKey)
  og_layers <- og_layers |> dplyr::filter(code != "None", !is.na(code))

  tall_lpi_layer_codes <- tall_lpi |> dplyr::select(PrimaryKey, LineKey, RecKey,layer, code)
  tall_lpi_layer_codes$LineKey <- as.numeric(tall_lpi_layer_codes$LineKey)
  missing_in_tall_lpi <- dplyr::setdiff(og_layers, tall_lpi_layer_codes)
  missing_in_tall_lpi <- as.data.frame(missing_in_tall_lpi)
  if(nrow(missing_in_tall_lpi) > 0){
    missing_in_tall_lpi$Notes <- "The specific hit (layer and code) in tall lpi does not match the original data"
    missing_in_tall_lpi$Action <- "Determine why gather or cleaning is changing the original data"

  }

  missing_in_og <- dplyr::setdiff(tall_lpi_layer_codes, og_layers)
  missing_in_og <- as.data.frame(missing_in_og)
  if(nrow(missing_in_og) > 0){
    missing_in_og$Notes <- "The specific hit (layer and code) in original data does not match or is missing from the tall lpi data"
    missing_in_og$Action <- "Determine why gather or cleaning is changing the tall data"

  }

  if(length(missing_in_og) ==  length(missing_in_tall_lpi)){
    missing_layer_codes <- rbind(missing_in_tall_lpi, missing_in_og)
  }
  if(length(missing_in_og) >  length(missing_in_tall_lpi)){
    missing_layer_codes <- missing_in_og
  }

  if(length(missing_in_og) <  length(missing_in_tall_lpi)){
    missing_layer_codes <- missing_in_tall_lpi
  }

  missing_layer_codes <- as.data.frame(missing_layer_codes)

  if(nrow(missing_layer_codes) > 0){
    missing_layer_codes <- missing_layer_codes |> filter_all(any_vars(duplicated(.)))
  }

  write.csv(missing_layer_codes, file.path(path_qc, "differing_layer_codes_check.csv"), row.names = F)

}
############################################



############################################
#' Clean Tall Gap
#'
#'removes and adds columns to the tall_gap file produced using terradactyl::gather_gap that are (not) necessary to produce geofiles
#'
#' @param tall_gap as a data.frame, tall gap file produced from terradactyl::gather_gap()
#' @param dataHeader as a data.frame, the dataHeader file produced from terradactylutils2::create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return an updated tall_gap file saved to path_tall and tall_gap in the console (unless saved to an object)
#' @export
#'
#' @examples clean_tall_gap(tall_gap = terradactyl::gather_gap(source = "DIMA", tblGapHeader = tblGapHeader, tblGapDetail = tblGapDetail2), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
clean_tall_gap <- function(tall_gap, dataHeader, path_tall){

  dropcols_gap <- tall_gap  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_gap <- tall_gap[which(!duplicated(dropcols_gap)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_gap$DBKey <- dataHeader$DBKey[match(tall_gap$PrimaryKey, dataHeader$PrimaryKey)]

  tall_gap$DateVisited <- tblGapHeader$DateVisited[match(tall_gap$PrimaryKey, tblGapHeader$PrimaryKey)]
  #tall_gap$DateVisited <- as.character(tall_gap$DateVisited)

  tall_gap$Direction <- tblGapHeader$Direction[match(tall_gap$PrimaryKey, tblGapHeader$PrimaryKey)]
  #match
  tall_gap$ProjectKey <- dataHeader$ProjectKey[match(tall_gap$PrimaryKey, dataHeader$PrimaryKey)]

  saveRDS(tall_gap, file.path(path_tall, "gap_tall.rdata"))
  write.csv(tall_gap, file.path(path_tall, "gap_tall.csv"), row.names = F)
  tall_gap
}
#####################################



#####################################
#' Tall Gap QC
#'
#' produces QC information using the tall_gap file in the format for running terradactylutils2::geofiles() created using terradactylutils2::clean_tall_gap()
#'
#' @param cleaned_tall_gap the tall_gap file that has been through terradactylutils2::clean_tall_gap()
#' @param tblGapDetail the tblGapDetail file from DIMA tables
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with QC information about that tall_gap file saved to the QC folder specified
#' @export
#'
#' @examples gap_qc(cleaned_tall_gap = cleaned_tall_gap, tblGapDetail = tblGapDetail, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
tall_gap_qc <- function(cleaned_tall_gap, tblGapDetail, path_qc){
tall_gap <- cleaned_tall_gap
  # function(tblGapDetail, tall_gap)
  ### gap QC
  # checking that the tall and og GapStart data match
  tall_gap_start <- tall_gap |> dplyr::select(PrimaryKey, LineKey, RecKey,GapStart)
  og_gap_start <- tblGapDetail |> dplyr::select(PrimaryKey, LineKey, RecKey,GapStart)

  tall_gap_start_differ <- dplyr::setdiff(og_gap_start, tall_gap_start)
  if(nrow(tall_gap_start_differ) > 0){
    tall_gap_start_differ$Notes <- "There is a GapStart in the tall data that differs from the original data"
    tall_gap_start_differ$Action <- "Determine why gather or clean functions are altering the original GapStart"

  }

  og_gap_start_differ <- dplyr::setdiff(tall_gap_start, og_gap_start)
  if(nrow(og_gap_start_differ) > 0){
    og_gap_start_differ$Notes <- "There is a GapStart in the original data that differs from the tall tables"
    og_gap_start_differ$Action <- "Determine why gather or clean functions are altering the tall GapStart"

  }


  gap_start_errors <- rbind(tall_gap_start_differ, og_gap_start_differ)

  if(nrow(gap_start_errors) > 0){
    gap_start_errors <- gap_start_errors |> filter_all(any_vars(duplicated(.)))
  }


  # checking the GapStart is not NA
  no_start <- tall_gap_start[is.na(tall_gap_start$GapStart),] #
  if(nrow(no_start) > 0){
    no_start$Notes <- "The GapStart for the line is NA"
    no_start$Action <- "Work with project manager to determine whether line needs removed"
  }
  gap_start_errors <- rbind(gap_start_errors, no_start)

  write.csv(gap_start_errors, file.path(path_qc, "GapStart_check.csv"), row.names = F)

  # checking max and min
  tall_gap_gaps <- tall_gap |> dplyr::select(PrimaryKey, LineKey, RecKey,Gap)
  og_gap_gaps <- tblGapDetail |> dplyr::select(PrimaryKey, LineKey, RecKey,Gap)

  max_tall_gap <- slice_max(tall_gap_gaps, Gap, by = c('PrimaryKey', 'LineKey','RecKey'))
  max_og_gap <- slice_max(og_gap_gaps, Gap, by = c('PrimaryKey', 'LineKey','RecKey'))


  max_gap_error_tall <- dplyr::setdiff(max_og_gap, max_tall_gap)
  if(nrow(max_gap_error_tall) > 0){
    max_gap_error_tall$Notes <- "There is a Gap in the tall data that differs from the original data"
    max_gap_error_tall$Action <- "Determine why gather or clean functions are altering the original Gap"

  }

  max_gap_error_og <- dplyr::setdiff(max_tall_gap, max_og_gap)
  if(nrow(max_gap_error_og) > 0){
    max_gap_error_og$Notes <- "There is a Gap in the original data that differs from the tall tables"
    max_gap_error_og$Action <- "Determine why gather or clean functions are altering the tall Gap"

  }


  max_gap_errors <- rbind(max_gap_error_tall, max_gap_error_og)

  if(nrow(max_gap_errors) > 0){
    max_gap_errors <- max_gap_errors |> filter_all(any_vars(duplicated(.)))
  }




  min_tall_gap <- slice_min(tall_gap_gaps, Gap, by = c('PrimaryKey', 'LineKey','RecKey'))
  min_og_gap <- slice_min(og_gap_gaps, Gap, by = c('PrimaryKey', 'LineKey','RecKey'))


  min_gap_error_tall <- dplyr::setdiff(min_og_gap, min_tall_gap)
  if(nrow(min_gap_error_tall) > 0){
    min_gap_error_tall$Notes <- "There is a Gap in the tall data that differs from the original data"
    min_gap_error_tall$Action <- "Determine why gather or clean functions are altering the original Gap"

  }

  min_gap_error_og <- dplyr::setdiff(min_tall_gap, min_og_gap)
  if(nrow(min_gap_error_og) > 0){
    min_gap_error_og$Notes <- "There is a Gap in the original data that differs from the tall tables"
    min_gap_error_og$Action <- "Determine why gather or clean functions are altering the tall Gap"

  }


  min_gap_errors <- rbind(min_gap_error_tall, min_gap_error_og)

  if(nrow(min_gap_errors) > 0){
    min_gap_errors <- min_gap_errors |> filter_all(any_vars(duplicated(.)))
  }


  gap_errors <- rbind(max_gap_errors, min_gap_errors)

  ## checking for negatives or NAs
  neg_gap <- tall_gap_gaps |> filter(Gap < 0)
  if(nrow(neg_gap) > 0){
    neg_gap$Notes <- "There are negative gaps present"
    neg_gap$Action <- "Determine if the gap should be positive or work with project manager to determine whether line needs removed"
  }
  gap_errors <- rbind(gap_errors, neg_gap)

  write.csv(gap_errors, file.path(path_qc, "Gap_check.csv"), row.names = F)

  # GapEnd errors
  tall_gap_end <- tall_gap |> dplyr::select(PrimaryKey, LineKey, RecKey,GapEnd)

  no_end <- tall_gap_end[is.na(tall_gap_end$GapEnd),]

  if(nrow(no_end) > 0){
    no_end$Notes <- "The GapEnd is NA"
    no_end$Action <- "Work with project manager to determine whether line needs removed"
  }

  write.csv(no_end, file.path(path_qc, "GapEnd_check.csv"), row.names = F)

}
#####################################



#####################################
#' Clean Tall Soil Stability
#'
#'adds and removes columns to the data produced from terradactyl::gather_soil_stability that are (not) necessary to run terradactylutils2::geofiles()
#'
#' @param tall_soil_stability file produced from terradactyl::gather_soil_stability
#' @param dataHeader dataHeader produced from terradactylutils2::create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return a CSV saved to the specified path_tall and a tall_soil_stability data frame in the console (unless saved to an object)
#' @export
#'
#' @examples clean_tall_soil_stability(tall_soil_stability = terradactyl::gather_soil_stability(source = source, tblSoilStabDetail = tblSoilStabDetail, tblSoilStabHeader = tblSoilStabHeader), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
clean_tall_soil_stability <- function(tall_soil_stability, dataHeader, path_tall){

  dropcols_soil_stability <- tall_soil_stability  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))

  pkeys <- dataHeader$PrimaryKey
  tall_soil_stability <- tall_soil_stability[which(!duplicated(dropcols_soil_stability)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_soil_stability$DBKey <- dataHeader$DBKey[match(tall_soil_stability$PrimaryKey, dataHeader$PrimaryKey)]
  tall_soil_stability$Hydro <- rep(FALSE)
  #tall_soil_stability$DateVisited <- as.character(tall_soil_stability$DateVisited)
  #rename
  tall_soil_stability <- tall_soil_stability |>
    rename(
      ProjectKey = project
    )
  saveRDS(tall_soil_stability, file.path(path_tall, "soil_stability_tall.rdata"))
  write.csv(tall_soil_stability, file.path(path_tall, "soil_stability_tall.csv"), row.names = F)
  tall_soil_stability
}
##################################




##################################
#' Tall Soil Stability QC
#'
#'produces QC information using the tall_soil_stability file prepared for terradactylutils2::geofiles() using terradactylutils2::clean_tall_soil_stability()
#'
#' @param tblSoilStabDetail tblSoilStabDetail from the DIMA tables
#' @param cleaned_tall_soil_stability tall_soil_stability created using terradactylutils2::clean_tall_soil_stability()
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with information about the tall soil stability QC
#' @export
#'
#' @examples soil_stability_qc(tblSoilStabDetail = tblSoilStabDetail, cleaned_tall_soil_stability = cleaned_tall_soil_stability, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
tall_soil_stability_qc <- function(tblSoilStabDetail, cleaned_tall_soil_stability, path_qc){
tall_soil_stability <- cleaned_tall_soil_stability
  # SS rating errors
  ss_og_rating <- tblSoilStabDetail |> dplyr::select(contains("Rating"),  RecKey) |>
    gather("Position", "Rating"  , -RecKey)
  ss_og_rating$Position <- gsub("^.{0,6}", "", ss_og_rating$Position)


  ss_og_rating_2 <- tblSoilStabDetail |> dplyr::select(contains("Pos"), RecKey) |>
    gather("Position", "Pos"  , -RecKey)
  ss_og_rating_2$Position <- gsub("^.{0,3}", "", ss_og_rating_2$Position)

  ss_og_rating <- merge(ss_og_rating, ss_og_rating_2)

  ss_og_rating <- ss_og_rating[!is.na(ss_og_rating$Rating),]
  ss_og_rating <- ss_og_rating[!is.na(ss_og_rating$Pos),]

  ss_tall_rating <- tall_soil_stability |> dplyr::select(RecKey, Pos, Rating, Position)

  # checking max and min



  max_tall_rating <- slice_max(ss_tall_rating, Rating, by = c('Position', 'Pos','RecKey'))
  max_og_rating <- slice_max(ss_og_rating, Rating, by = c('Position', 'Pos','RecKey'))

  max_tall_rating$Position <- as.character(max_tall_rating$Position)
  max_og_rating$Position <- as.character(max_og_rating$Position)
  max_og_rating$Pos <- as.character(max_og_rating$Pos)
  max_tall_rating$Pos <- as.character(max_tall_rating$Pos)

  max_rating_error_tall <- dplyr::setdiff(max_og_rating, max_tall_rating)
  if(nrow(max_rating_error_tall) > 0){
    max_rating_error_tall$Notes <- "There is a rating in the tall data that differs from the original data"
    max_rating_error_tall$Action <- "Determine why gather or clean functions are altering the original rating"

  }

  max_rating_error_og <- dplyr::setdiff(max_tall_rating, max_og_rating)
  if(nrow(max_rating_error_og) > 0){
    max_rating_error_og$Notes <- "There is a rating in the original data that differs from the tall tables"
    max_rating_error_og$Action <- "Determine why gather or clean functions are altering the tall rating"

  }


  max_rating_errors <- rbind(max_rating_error_tall, max_rating_error_og)

  if(nrow(max_rating_errors) > 0){
    max_rating_errors <- max_rating_errors |> filter_all(any_vars(duplicated(.)))
  }




  min_tall_rating <- slice_min(ss_tall_rating, Rating, by = c('Position', 'Pos','RecKey'))
  min_og_rating <- slice_min(ss_og_rating, Rating, by = c('Position', 'Pos','RecKey'))

  min_tall_rating$Position <- as.character(min_tall_rating$Position)
  min_og_rating$Position <- as.character(min_og_rating$Position)
  min_og_rating$Pos <- as.character(min_og_rating$Pos)

  min_tall_rating$Pos <- as.character(min_tall_rating$Pos)

  min_rating_error_tall <- dplyr::setdiff(min_og_rating, min_tall_rating)
  if(nrow(min_rating_error_tall) > 0){
    min_rating_error_tall$Notes <- "There is a rating in the tall data that differs from the original data"
    min_rating_error_tall$Action <- "Determine why gather or clean functions are altering the original rating"

  }

  min_rating_error_og <- dplyr::setdiff(min_tall_rating, min_og_rating)
  if(nrow(min_rating_error_og) > 0){
    min_rating_error_og$Notes <- "There is a rating in the original data that differs from the tall tables"
    min_rating_error_og$Action <- "Determine why gather or clean functions are altering the tall rating"

  }


  min_rating_errors <- rbind(min_rating_error_tall, min_rating_error_og)

  if(nrow(min_rating_errors) > 0){
    min_rating_errors <- min_rating_errors |> filter_all(any_vars(duplicated(.)))
  }


  rating_errors <- rbind(max_rating_errors, min_rating_errors)




  # SS shouldn't be more than 6 in raw and calcd

  ss_raw_six <- ss_og_rating |> filter(Rating >6)
  if(nrow(ss_raw_six) > 0){
    ss_raw_six$Notes <- "There is a rating in the original soil stability data that is greater than 6"
    ss_raw_six$Action <- "Work with the project manager to determine if the rating should be removed"

  }


  ss_calcd_six <- ss_tall_rating |> filter(Rating > 6)
  if(nrow(ss_calcd_six) > 0){
    ss_calcd_six$Notes <- "There is a rating in the tall soil stability data that is greater than 6"
    ss_calcd_six$Action <- "Work with the project manager to determine if the rating should be removed"

  }

  ss_six <- rbind(ss_raw_six, ss_calcd_six)

  if(nrow(ss_six) > 0){
    ss_six <- ss_six |> filter_all(any_vars(duplicated(.)))
  }



  ss_rating_errors <- rbind(rating_errors, ss_six)

  # write CSV
  write.csv(ss_rating_errors,   file.path(path_qc, "soil_stability_rating_check.csv"), row.names = F)


  # veg cover classes
  ss_og_veg <- tblSoilStabDetail |> dplyr::select(contains("Veg"),  RecKey) |>
    gather("Position", "Veg"  , -RecKey)
  ss_og_veg$Position <- gsub("^.{0,3}", "", ss_og_veg$Position)


  ss_og_veg_2 <- tblSoilStabDetail |> dplyr::select(contains("Pos"), RecKey) |>
    gather("Position", "Pos"  , -RecKey)
  ss_og_veg_2$Position <- gsub("^.{0,3}", "", ss_og_veg_2$Position)

  ss_og_veg <- merge(ss_og_veg, ss_og_veg_2)

  ss_og_veg <- ss_og_veg[!is.na(ss_og_veg$Veg),]
  ss_og_veg <- ss_og_veg[!is.na(ss_og_veg$Pos),]

  ss_tall_veg <- tall_soil_stability |> dplyr::select(RecKey, Pos, Veg, Position)


  ss_og_veg$Position <- as.character(ss_og_veg$Position)
  ss_tall_veg$Position <- as.character(ss_tall_veg$Position)
  ss_og_veg$Pos <- as.character(ss_og_veg$Pos)
  ss_tall_veg$Pos <- as.character(ss_tall_veg$Pos)

  veg_error_tall <- dplyr::setdiff(ss_og_veg, ss_tall_veg)
  if(nrow(veg_error_tall) > 0){
    veg_error_tall$Notes <- "There is a Veg record in the tall data that differs from the original data"
    veg_error_tall$Action <- "Determine why gather or clean functions are altering the original Veg"

  }

  veg_error_og <- dplyr::setdiff(ss_tall_veg, ss_og_veg)
  if(nrow(veg_error_og) > 0){
    veg_error_og$Notes <- "There is a Veg record in the original data that differs from the tall tables"
    veg_error_og$Action <- "Determine why gather or clean functions are altering the tall Veg"

  }


  veg_errors <- rbind(veg_error_tall, veg_error_og)

  if(nrow(veg_errors) > 0){
    veg_errors <-veg_errors |> filter_all(any_vars(duplicated(.)))
  }

  write.csv(veg_errors, file.path(path_qc, "soil_stability_Veg_check.csv"), row.names = F)


}
########################################




########################################
#' Clean Tall Species Richness
#'
#'adds or removes columns from the  tall_species_richness file produced with terradactyl::gather_species_richness() that are (not) necessary to run terradactylutils2::geofiles()
#'
#' @param tall_species tall_species file produced from terradactyl::gather_species_richness()
#' @param dataHeader dataHeader file produced from create_header()
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'
#' @return a CSV saved to the speficied path_tall and the updated tall_species_richness saved to the R enviornment
#' @export
#'
#' @examples clean_tall_species(tall_species = gather_species_inventory(source = "DIMA", tblSpecRichDetail = tblSpecRichDetail, tblSpecRichHeader = tblSpecRichHeader), dataHeader = dataHeader, path_tall = file.path(path_parent, "Tall"))
clean_tall_species <- function(tall_species, dataHeader, path_tall){

  dropcols_species <- tall_species  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "SpeciesList")))
  pkeys <- dataHeader$PrimaryKey
  tall_species <- tall_species[which(!duplicated(dropcols_species)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_species$DBKey <- dataHeader$DBKey[match(tall_species$PrimaryKey, dataHeader$PrimaryKey)]
  tall_species$Direction <- tblSpecRichHeader$Direction[match(tall_species$PrimaryKey, tblSpecRichHeader$PrimaryKey)]
  #tall_species$DateVisited <- as.character(tall_species$DateVisited)
  tall_species <- tall_species |>
    rename(
      ProjectKey = project
    )

  saveRDS(tall_species, file.path(path_tall, "species_inventory_tall.rdata"))
  write.csv(tall_species, file.path(path_tall, "species_inventory_tall.csv"), row.names = F)

  tall_species
}
########################################



########################################
#' Clean Tall Height
#'
#' adds and removes columns (not) necessary to run terradactylutils2::geofiles() for the the file produced using terradactyl::gather_height()
#'
#' @param tall_height as a data.frame, the tall_file produced from terradactyl::gather_height()
#' @param dataHeader as data.frame, dataHeader file produced from terradactylutils2::create_header()
#' @param tblLPIHeader as data.frame, tblLPIHeader from the DIMA tables
#' @param path_tall where all tall files from terradactyl::gather_... were saved
#'@param source data type
#'@param todaysDate today's date
#'
#' @return a CSV saved to the specified path_tall and an updated tall_height file saved to the console(unless saved to an object)
#' @export
#'
#' @examples clean_tall_height(tall_height = gather_height(source = "DIMA", tblLPIDetail = tblLPIDetail, tblLPIHeader = tblLPIHeader), dataHeader = dataHeader, tblLPIHeader = tblLPIHeader,  source = DIMA, todaysDate = todaysDate, path_tall = file.path(path_parent, "Tall"))
clean_tall_height <- function(tall_height, dataHeader, tblLPIHeader,  source,todaysDate, path_tall){

  dropcols_height <- tall_height  %>% dplyr::select_if(!(names(.) %in% c("DateLoadedInDB", "DBKey", "rid", "DateModified", "heightList")))
  pkeys <- dataHeader$PrimaryKey
  tall_height <- tall_height[which(!duplicated(dropcols_height)),] |>
    dplyr::filter(PrimaryKey %in% pkeys) |> unique()
  # add back in cols that are currently being removed with the function
  tall_height$DBKey <- dataHeader$DBKey[match(tall_height$PrimaryKey, dataHeader$PrimaryKey)]
  tall_height$ProjectKey <- dataHeader$ProjectKey[match(tall_height$PrimaryKey, dataHeader$PrimaryKey)]
  tall_height$FormType <- tblLPIHeader$FormType[match(tall_height$PrimaryKey, tblLPIHeader$PrimaryKey)]
  tall_height$source <- rep(source)
  tall_height$DateVisited <-tblLPIHeader$DateVisited[match(tall_height$PrimaryKey, tblLPIHeader$PrimaryKey)]
  #tall_height$DateVisited <- as.Date(tall_height$DateVisited, format = format)
  tall_height$DateLoadedInDb <- rep(todaysDate) #
  tall_height$FormDate <- tblLPIHeader$FormDate[match(tall_height$PrimaryKey, tblLPIHeader$PrimaryKey)]
tall_height$PointLoc <- as.integer(tall_height$PointLoc)

  saveRDS(tall_height, file.path(path_tall, "height_tall.rdata"))
  write.csv(tall_height, file.path(path_tall, "height_tall.csv"), row.names = F)

  tall_height
}
##################################



##################################
#' Tall Height QC
#'
#' produces QC information using the tall_height file produced from terradactylutils2::clean_tall_height()
#'
#' @param tblLPIDetail as a data.frame, tblLPIDetail from the DIMATables
#' @param cleaned_tall_height as a data.frame, the tall_height file produced from terradactylutils2::clean_tall_height()
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV file with QC information about the height data saved to the specified path_qc
#' @export
#'
#' @examples height_qc(tblLPIDetail = tblLPIDetail, cleaned_tall_height = cleaned_tall_height, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
tall_height_qc <- function(tblLPIDetail, cleaned_tall_height, path_qc){
  ### HGT QC
  # checking heights are the same in the original and tall data
tall_height <- cleaned_tall_height
  heights_og <- tblLPIDetail |> dplyr::select(PrimaryKey, LineKey, PointNbr, HeightWoody, HeightHerbaceous)

  heights_og <- gather(heights_og, "type", "Height", -PrimaryKey, -LineKey,-PointNbr)
  heights_og$type <- gsub("^.{0,6}", "", heights_og$type)

  heights_og$type <- tolower(heights_og$type)

  heights_og <- heights_og[!is.na(heights_og$Height),]
  tall_height_max <- tall_height |> dplyr::select(PrimaryKey, LineKey, PointNbr, type, Height)

  max_tall_Height <- slice_max(tall_height_max, Height, by = c('PrimaryKey', 'LineKey','PointNbr'))
  max_og_Height <- slice_max(heights_og, Height, by = c('PrimaryKey', 'LineKey','PointNbr'))


  max_Height_error_tall <- dplyr::setdiff(max_og_Height, max_tall_Height)
  if(nrow(max_Height_error_tall) > 0){
    max_Height_error_tall$Notes <- "There is a max Height in the tall data that differs from the original data"
    max_Height_error_tall$Action <- "Determine why gather or clean functions are altering the original Height"

  }

  max_Height_error_og <- dplyr::setdiff(max_tall_Height, max_og_Height)
  if(nrow(max_Height_error_og) > 0){
    max_Height_error_og$Notes <- "There is a max Height in the original data that differs from the tall tables"
    max_Height_error_og$Action <- "Determine why gather or clean functions are altering the tall Height"

  }


  max_Height_errors <- rbind(max_Height_error_tall, max_Height_error_og)

  if(nrow(max_Height_errors) > 0){
    max_Height_errors <- max_Height_errors |> filter_all(any_vars(duplicated(.)))
  }




  min_tall_Height <- slice_min(tall_height_max, Height, by = c('PrimaryKey', 'LineKey','PointNbr'))
  min_og_Height <- slice_min(heights_og, Height, by = c('PrimaryKey', 'LineKey','PointNbr'))

  min_Height_error_tall <- dplyr::setdiff(min_og_Height, min_tall_Height)
  if(nrow(min_Height_error_tall) > 0){
    min_Height_error_tall$Notes <- "There is a min Height in the tall data that differs from the original data"
    min_Height_error_tall$Action <- "Determine why gather or clean functions are altering the original Height"

  }

  min_Height_error_og <- dplyr::setdiff(min_tall_Height, min_og_Height)
  if(nrow(min_Height_error_og) > 0){
    min_Height_error_og$Notes <- "There is a min Height in the original data that differs from the tall tables"
    min_Height_error_og$Action <- "Determine why gather or clean functions are altering the tall Height"

  }


  min_Height_errors <- rbind(min_Height_error_tall, min_Height_error_og)

  if(nrow(min_Height_errors) > 0){
    min_Height_errors <- min_Height_errors |> filter_all(any_vars(duplicated(.)))
  }


  Height_errors <- rbind(max_Height_errors, min_Height_errors)

  write.csv(Height_errors, file.path(path_qc, "Height_check.csv"), row.names = F)

}
#########################################



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



########################################
#' Translate Tall Data into LDC Schema
#'
#'helper function for translate_coremethods that uses a schema to translate the tall data sets into the LDC format (column class and name). This is an updated version of translate_schema from terradactyl_Utils that does not require user to input projkey.
#'
#' @param data any cleaned tall data
#' @param datatype the data type in the tall table such as lpi
#' @param schema the LDC schema describing the characteristics of the columns in the tall tables on the LDC
#' @param dropcols T or F describing whether to drop columns that are not used
#' @param verbose T or F describing whether to return commentary
#'
#' @return rewrites the tall tables in path_tall in the LDC format
#' @export
#'
#' @examples translate_schema2(schema = schema,datatype = "dataHeight",dropcols = TRUE, verbose = TRUE)
#' @noRd
translate_schema2 <- function(data, datatype,schema,dropcols = T, verbose = T){

  ### standardize names
  # colnames(matrix)[colnames(matrix) == fromcol] <- "terradactylAlias"
  # colnames(matrix)[colnames(matrix) == tocol] <- "ToColumn"

  ### process the incoming matrix by assigning actions to take at each row
  matrix_processed1 <-
    schema |>
    dplyr::filter(Table == datatype) |>
    dplyr::mutate(
      Field <- stringr::str_trim(Field, side = "both"),
    ) |>
    dplyr::filter(Field != "" | terradactylAlias != "") |>
    dplyr::select(terradactylAlias, Field)

  matrix_processed <- matrix_processed1 |>
    dplyr::mutate(
      DropColumn = matrix_processed1$terradactylAlias != "" & matrix_processed1$Field == "",
      AddColumn = matrix_processed1$Field != "" & matrix_processed1$terradactylAlias == "",
      ChangeColumn = matrix_processed1$Field != "" & matrix_processed1$terradactylAlias != "" & matrix_processed1$Field != matrix_processed1$terradactylAlias,
      NoAction = matrix_processed1$Field == matrix_processed1$terradactylAlias & !AddColumn & !DropColumn,
      Checksum = AddColumn + DropColumn + ChangeColumn + NoAction,
    )

  ## check for errors (if errors are here the function is not working)
  errors <-
    matrix_processed |>
    dplyr::filter(Checksum != 1)

  if(nrow(errors) > 0) {print("Errors found in translation matrix. Debug function.")
    return(errors)}

  ChangeColumn <-
    matrix_processed |>
    dplyr::filter(ChangeColumn)

  AddColumn <-
    matrix_processed |>
    dplyr::filter(AddColumn)

  DropColumn <-
    matrix_processed |>
    dplyr::filter(DropColumn)

  ## run translation and add data
  outdata <- data |>
    dplyr::rename_at(
      ChangeColumn$terradactylAlias, ~ ChangeColumn$Field) |>
    `is.na<-`(AddColumn$Field |> unique())

  # # drop columns from prior schema if enabled
  # if(dropcols){
  #   outdata <- outdata |>
  #     dplyr::select_if(!colnames(.) %in% DropColumn$terradactylAlias)
  # }

  # select only the tables in the out schema
  goodnames <- matrix_processed |> dplyr::filter(Field != "") |> dplyr::pull(Field)

  if(verbose) {
    print(paste("Returning", length(goodnames), "columns"))
    print(dplyr::all_of(goodnames))
  }

  outdata <- outdata |>
    dplyr::select(dplyr::all_of(goodnames))


  #outdata$ProjectKey <- projectkey
  #
  # # return messages if verbose
  # if(verbose) {
  #   print(paste0(nrow(ChangeColumn), " columns renamed"))
  #   print(ChangeColumn[,c("terradactylAlias", "Field")])}
  #
  # if(verbose) {
  #   print(paste0(nrow(AddColumn), " columns added"))
  #   print(AddColumn$Field)}
  #
  # if(verbose & dropcols) {
  #   print(paste0(nrow(DropColumn), " columns removed"))
  #   print(DropColumn$terradactylAlias)
  # }

  return(outdata)
}
#############################################


#############################################
#' Translate Core Methods
#'
#' produces the tall tables in a format for the LDC using the tall data produced from terradactylutils2::clean_tall_"method"() and a schema
#'
#' @param path_tall path to the tall files produced from terradactylutils2::clean_tall_"method"()
#' @param path_out where to write the files for ingest
#' @param path_schema file path to a schema used to adjust the tall files
#' @param verbose T or F describing whether to return commentary
#'
#' @return updated CSV of the tall files that are written to path_out (typically, a For Ingest directory)
#' @export
#'
#' @examples translate_coremethods2(path_tall = file.path(path_parent, "Tall"),path_out = path_foringest,path_schema = path_schema,verbose = T)
translate_coremethods2 <- function(path_tall, path_out, path_schema,  verbose = F){

  schema <- read.csv(path_schema)

  if(file.exists(file.path(path_tall, "header.Rdata"))){
    print("Translating header data")
    header   <- readRDS(file.path(path_tall, "header.Rdata"))
    dataHeader <- header |>
      translate_schema2(schema = schema,
                        #projectkey = projectkey,
                        datatype = "dataHeader",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataHeader, file.path(path_out, "dataHeader.csv"), row.names = F)
  } else {
    stop("Header data not found. Unable to translate data")
  }

  if(file.exists(file.path(path_tall, "lpi_tall.Rdata"))){
    print("Translating LPI data")
    tall_lpi <- readRDS(file.path(path_tall, "lpi_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataLPI <- tall_lpi |>
      translate_schema2(schema = schema,
                        #projectkey = projectkey,
                        datatype = "dataLPI",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataLPI, file.path(path_out, "dataLPI.csv"), row.names = F)
  } else {
    print("LPI data not found")
  }

  if(file.exists(file.path(path_tall, "height_tall.Rdata"))){
    print("Translating height data")
    tall_ht  <- readRDS(file.path(path_tall, "height_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataHeight <- tall_ht |>
      translate_schema2(schema = schema,
                        # projectkey = projectkey,
                        datatype = "dataHeight",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataHeight, file.path(path_out, "dataHeight.csv"), row.names = F)
  } else {
    print("Height data not found")
  }

  if(file.exists(file.path(path_tall, "species_inventory_tall.Rdata"))){
    print("Translating species inventory data")
    tall_sr  <- readRDS(file.path(path_tall, "species_inventory_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataSpeciesInventory <- tall_sr |>
      translate_schema2(schema = schema,
                        # projectkey = projectkey,
                        datatype = "dataSpeciesInventory",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataSpeciesInventory, file.path(path_out, "dataSpeciesInventory.csv"), row.names = F)
  } else {
    print("Species inventory data not found")
  }


  if(file.exists(file.path(path_tall, "soil_stability_tall.Rdata"))){
    print("Translating soil stability data")
    tall_ss  <- readRDS(file.path(path_tall, "soil_stability_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataSoilStability <- tall_ss |>
      translate_schema2(schema = schema,
                        #  projectkey = projectkey,
                        datatype = "dataSoilStability",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataSoilStability, file.path(path_out, "dataSoilStability.csv"), row.names = F)
  } else {
    print("Soil stability data not found")
  }

  if(file.exists(file.path(path_tall, "gap_tall.Rdata"))){
    print("Translating canopy gap data")
    tall_gap <- readRDS(file.path(path_tall, "gap_tall.Rdata")) |>
      dplyr::left_join(dataHeader |> dplyr::select(PrimaryKey, DateVisited))
    dataGap <- tall_gap |>
      translate_schema2(schema = schema,
                        #    projectkey = projectkey,
                        datatype = "dataGap",
                        dropcols = TRUE,
                        verbose = TRUE)
    write.csv(dataGap, file.path(path_out, "dataGap.csv"), row.names = F)
  } else {
    print("Gap data not found")
  }

}
##################################



##################################
#' Geofiles
#'
#' creates the geoIndicators and geoSpecies files and writes them to the path_foringest using the path_tall data that was produced using translate_coremethods2. Updated from translate_coremethods from terradactyl_Utils to not require projkey.
#'
#' @param path_foringest path where data for ingest will be exported
#' @param path_tall path to the tall files produced from terradactylutils2::clean_tall_"method"()
#' @param header as a data.frame, the tall header file in path_tall
#' @param path_specieslist path to species lists including the ProjectKey
#' @param path_template path to an indicator list using graminoid identifiers, currently used while certain agencies use GRASS
#' @param doGSP TRUE unless user does not want a geoSpecies file produced
#'
#' @return geoSpecies and geoIndicators file written to the path_foringest
#' @export
#'
#' @examples geofiles(path_foringest = path_foringest,path_tall = file.path(path_parent, "Tall"),header = tall_header, path_specieslist =  paste0(path_species,  projkey, ".csv"),path_template = template)
geofiles <- function(path_foringest,path_tall,header,path_specieslist, path_template,doGSP){
  if(file.exists(file.path(path_tall, "lpi_tall.Rdata"))) {
    l <- lpi_calc(
      lpi_tall = file.path(path_tall, "lpi_tall.rdata"),
      header = header,
      source = "DIMA",
      species_file = path_specieslist,
      dsn = path_template
    )
    l_graminoid <- lpi_calc_graminoid(lpi_tall = file.path(path_tall, "lpi_tall.rdata"),
                                      header = header,
                                      source = "DIMA", # i do not want it to read from the species state tbl
                                      species_file = path_specieslist,
                                      dsn = path_template)

if(nrow(l_graminoid) > 0){
    l <- dplyr::left_join(l, l_graminoid|> tidyr::pivot_wider(names_from = "indicator", values_from = "percent"))
}

  } else {
    l <- NULL
  }

  if(file.exists(file.path(path_tall, "gap_tall.Rdata"))){ #
    g <- gap_calc(
      gap_tall = file.path(path_tall, "gap_tall.rdata"),
      header = header
    )
  } else {
    g <- NULL
  }

  if(file.exists(file.path(path_tall, "height_tall.Rdata"))){
    h <- height_calc(
      height_tall = file.path(path_tall, "height_tall.rdata"),
      header = header,
      source = "DIMA",
      species_file = path_specieslist
    )
  } else {
    h <- NULL
  }

  if(file.exists(file.path(path_tall, "species_inventory_tall.Rdata"))){
    sr <- spp_inventory_calc(
      header = header,
      spp_inventory_tall = file.path(path_tall, "species_inventory_tall.rdata"),
      species_file = path_specieslist,
      source = "DIMA"
    )
  } else {
    sr <- NULL
  }
  if(file.exists(file.path(path_tall, "soil_stability_tall.Rdata"))){
    ss <- soil_stability_calc(header = header,
                              soil_stability_tall = file.path(path_tall, "soil_stability_tall.rdata"))
  } else {
    ss <- NULL
  }

  if(file.exists(file.path(path_tall, "rangelandhealth_tall.Rdata"))){
    rh <- tall_rangelandhealth # There is no indicator calculation for RH, just a gather; I structured it like this to preserve the symmetry.
  } else {
    rh <- NULL
  }

  all_indicators <- header
  all_indicators <- header
  if(file.exists(file.path(path_tall, "lpi_tall.Rdata"))) {
    all_indicators <- all_indicators %>% dplyr::left_join(., l)}
  if(file.exists(file.path(path_tall, "gap_tall.Rdata"))) {
    all_indicators <- all_indicators %>% dplyr::left_join(., g) }
  if(file.exists(file.path(path_tall, "height_tall.Rdata"))) {
    all_indicators <- all_indicators %>% dplyr::left_join(., h)  }
  if(file.exists(file.path(path_tall, "species_inventory_tall.Rdata"))) {
    all_indicators <- all_indicators %>% dplyr::left_join(., sr)}
  if(file.exists(file.path(path_tall, "soil_stability_tall.Rdata"))) {
    all_indicators <- all_indicators %>% dplyr::left_join(., ss)}
  if(file.exists(file.path(path_tall, "rangelandhealth_tall.Rdata"))) {
    all_indicators <- all_indicators %>% dplyr::left_join(., rh)}

  all_indicators_dropcols <- all_indicators %>%
    dplyr::select_if(!names(.) %in% c("DBKey", "DateLoadedInDb", "rid", "SpeciesList"))
  all_indicators_unique <- all_indicators[which(!duplicated(all_indicators_dropcols)),]

  i <- terradactylutils3::add_indicator_columns(template = template,
                             source = "DIMA",
                             all_indicators = all_indicators_unique,
                             prefixes_to_zero = c("AH", "FH", "NumSpp"))

  i2 <- i
  if(file.exists(file.path(path_tall, "lpi_tall.Rdata"))) {
  i2$BareSoil <- i2$BareSoilCover
    }

  colnames(i2) = gsub("Grass", "Graminoid", colnames(i2))

  i3 <- subset(i2, select=which(!duplicated(names(i2))))

  schema <- read.csv(path_schema)
  geoInd <- i3 |>
    translate_schema2(schema = schema,
                      #    projectkey = projectkey,
                      datatype = "geoIndicators",
                      dropcols = TRUE,
                      verbose = TRUE)


  write.csv(geoInd, file = file.path(path_foringest, "geoIndicators.csv"), row.names = F)





  if(doGSP){
    a <- accumulated_species(
      lpi_tall =
        if(file.exists(file.path(path_tall, "lpi_tall.rdata"))){
          file.path(path_tall, "lpi_tall.rdata")
        } else {
          NULL
        },
      height_tall =
        if(file.exists(file.path(path_tall, "height_tall.rdata"))){
          file.path(path_tall, "height_tall.rdata")
        } else {
          NULL
        },
      spp_inventory_tall =
        if(file.exists(file.path(path_tall, "species_inventory_tall.rdata"))){
          file.path(path_tall, "species_inventory_tall.rdata")
        } else {
          NULL
        },
      header = file.path(path_tall, "header.rdata"),
      species_file = path_specieslist,
      dead = F,
      source = "DIMA")

    header = readRDS(file.path(path_tall, "header.rdata"))
    a2 <- a |>
      dplyr::left_join(header |> dplyr::select(PrimaryKey, DateVisited)) |>
      dplyr::filter(!(is.na(AH_SpeciesCover) & is.na(AH_SpeciesCover_n) &
                        is.na(Hgt_Species_Avg) & is.na(Hgt_Species_Avg_n)))
    a2$DBKey <- header$DBKey[match(a2$PrimaryKey, header$PrimaryKey)]
    a2$ProjectKey <- header$ProjectKey[match(a2$PrimaryKey, header$PrimaryKey)]

    a2$DateLoadedInDb <- rep(todaysDate)
    schema <- distinct(schema)
    a2 <- a2|>   translate_schema2(schema = schema,
                                   #projectkey = projectkey,
                                   datatype = "geoSpecies",
                                   dropcols = TRUE,
                                   verbose = TRUE)




    write.csv(a2, file.path(path_foringest, "geoSpecies.csv"), row.names = F)


  }

}
#######################################



#######################################
#' geoIndicators QC
#'
#' produces a CSV with QC information about the geoIndicators table
#'
#' @param path_foringest where geoIndicators data was saved
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with QC information about the geoIndicators table
#' @export
#'
#' @examples geoind_qc(path_foringest = path_foringest, path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
geoind_qc <- function(path_foringest, path_qc){

  geoind <- read.csv(paste0(path_foringest, "/geoIndicators.csv"))

  dat_selected <- geoind[sapply(geoind, is.numeric)]
  geoind_NA <- dat_selected |>
    summarise(across(everything(), ~ sum(is.na(.x)))) |> mutate(number_obs_in_geoind = nrow(dat_selected))

  geoind_NA <- gather(geoind_NA, col_name, number_NAs, -number_obs_in_geoind)
  geoind_NA <- geoind_NA[,c(2,3,1)]


  geoind_zero <- dat_selected |> summarise(across(everything(), ~ sum(.x, na.rm = T) ))
  geoind_zero <- gather(geoind_zero, col_name, sum_for_entire_column)

  geoind_check <-  left_join(geoind_NA, geoind_zero, by = "col_name")

  geoind_check$Notes <- ifelse(geoind_check$sum_for_entire_column == 0, "Every observation for this column is zero",
                               "")

  geoind_check$Action <- ifelse(geoind_check$sum_for_entire_column == 0,
                                "If this is unexpected, review the template has the desired column followed by the geofiles calculations",
                                "")


  write.csv(geoind_NA, file.path(path_qc, "geoind_number_NAs_zeros_per_numeric_column.csv"), row.names = FALSE)

  # make sure core are not adding up to more than 100
  #bare_tf <- geoind |> mutate(bare_tf = BareSoil + TotalFoliarCover)
  gaps <- geoind

  gaps$Notes <- ifelse(gaps$GapCover_25_plus > 100.01, "Sum of GapCover is greater than 100%", "")

  # putting in an action for all rows for now, at the end, only the problem observation remain and
  #will be seen by the user

  gaps$Action <- "Determine if rounding error and work with project manager to decide if plot needs removed"

  gaps <- gaps |> dplyr::select(ProjectKey, PrimaryKey, Notes, Action)



  total_cover <- geoind |> mutate(FH_BareSoil = BareSoil) |> mutate(FH_TotalFoliarCover = TotalFoliarCover)
  total_cover <- total_cover |>
    select(matches("FH_"), ProjectKey, PrimaryKey)
  total_cover<- subset(total_cover, select=-c(FH_TotalLitterCover)) #? not to be included
  #total_cover$total_cover <- rowSums(total_cover)
  total_cover <- total_cover |> mutate(total_cover=rowSums(select(total_cover,-ProjectKey, -PrimaryKey), na.rm = T))
  total_cover$Notes <- ifelse(total_cover$total_cover > 100.01, "Sum of FH cover is greater than 100%", "")
  # putting in an action for all rows for now, at the end, only the problem observation remain and
  #will be seen by the user

  total_cover$Action <- "Determine if rounding error and work with project manager to decide if plot needs removed"

  total_cover <- total_cover |> dplyr::select(ProjectKey, PrimaryKey, Notes, Action )

  cover_errors <- rbind(gaps, total_cover)

  cover_errors <- cover_errors |> filter(Notes != "")

  write.csv(cover_errors, file.path(path_qc, "geoind_total_and_gap_cover_check.csv"), row.names = FALSE)

}
#################################



#################################
#' geoSpecies QC
#'
#'produces a CSV with information about the QC of the geoSpecies file
#'
#' @param path_foringest path to where geoSpecies file is stored
#' @param USDA_plants as a data.frame, a file containing the accepted USDA codes with a code, GrowthHabit and Duration column
#' @param speciescode the name of the column with the 4 letter USDA codes in the USDA_plants file
#' @param path_qc path where the QC data will be saved
#'
#' @return a CSV with information about the QC of the geoSpecies file
#' @export
#'
#' @examples geospecies_qc(path_foringest = path_foringest, USDA_plants = read.csv("D:/modifying_data_prep_script_10032025/2004-2023_ceap_species_list.csv"), speciescode = "UpdatedSpeciesCode", path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
geospecies_qc <- function(path_foringest, USDA_plants, speciescode, path_qc){

  a2 <- read.csv(paste0(path_foringest, "/geoSpecies.csv"))

  # list species < 4 char
  issue_codes_char <- a2[nchar(a2$Species) <= 3, ]

  if(nrow(issue_codes_char >0)){
    issue_codes_char$Notes <- "The species code is less than the expected 4 characters"
    issue_codes_char <- issue_codes_char |> dplyr::select(ProjectKey, PrimaryKey, Species, Notes)
  }

  #list any with NA GH or duration
  issue_codes_GH <- a2[is.na(a2$GrowthHabit),]
  if(nrow(issue_codes_GH >0)){
    issue_codes_GH$Notes <- "The GrowthHabit is NA"
    issue_codes_GH <- issue_codes_GH |> dplyr::select(ProjectKey, PrimaryKey, Species, Notes)
  }

  issue_codes_D <- a2[is.na(a2$Duration),]

  if(nrow(issue_codes_D >0)){
    issue_codes_D$Notes <- "The Duration is NA"
    issue_codes_D <- issue_codes_D |> dplyr::select(ProjectKey, PrimaryKey, Species, Notes)
  }

  # checking that the geospecies codes are in the USDA database
  USDA_plant_codes <- USDA_plants[,paste0(speciescode)]
  `%notin%` <- Negate(`%in%`)
  incorrect_code_sp <- a2[a2$code %notin% USDA_plant_codes,]

  if(nrow(incorrect_code_sp >0)){
    incorrect_code_sp$Notes <- "Species code not associated with a USDA plant code"
    incorrect_code_sp <- incorrect_code_sp |> dplyr::select(ProjectKey, PrimaryKey, Species, Notes)
  }


  incorrect_codes <-  rbind(issue_codes_char, issue_codes_GH) %>%
    rbind(., issue_codes_D) %>% rbind(., incorrect_code_sp)

  if(nrow(incorrect_codes >0)){
    incorrect_codes$Action <- "Work with the project manager to determine the correct species code and attributes"
  }

  write.csv(incorrect_codes, file.path(path_qc, "geoSpecies_code_check.csv"), row.names = FALSE)

}
##############################################



##############################################
#' Format for ingest files
#'
#' updates the files in the path_foringest path to have the correct DBKey and DateLoadedInDb
#'
#' @param path_foringest path where data for ingest are saved
#' @param DateLoadedInDb in standard date format, the date you are running the code
#'
#' @return CSVs of the for ingest files saved to the specified path_foringest
#' @export
#'
#' @examples db_info(path_foringest = path_foringest,  DateLoadedInDb = format(Sys.Date(), "%m/%d/%Y"))
db_info <- function(path_foringest, DateLoadedInDb){

  # read in data
  header <- read.csv(paste0(path_foringest, "/dataHeader.csv"))
  ind <- read.csv(paste0(path_foringest, "/geoIndicators.csv"))
  if(file.exists(file.path(path_foringest, "/dataLPI.csv"))) {
    LPI <- read.csv(paste0(path_foringest, "/dataLPI.csv"))}
  if(file.exists(file.path(path_foringest, "/dataGap.csv"))){
    gap <- read.csv(paste0(path_foringest, "/dataGap.csv"))}
  if(file.exists(file.path(path_foringest, "/dataSoilStability.csv"))){
    ss <- read.csv(paste0(path_foringest, "/dataSoilStability.csv"))}
  if(file.exists(file.path(path_foringest, "/dataHeight.csv"))) {
    hgt <- read.csv(paste0(path_foringest, "/dataHeight.csv"))}
  if(file.exists(file.path(path_foringest, "/geoSpecies.csv"))){
    sp <- read.csv(paste0(path_foringest, "/geoSpecies.csv"))}
  if(file.exists(file.path(path_foringest, "/dataSpeciesInventory.csv"))){
    spin <- read.csv(paste0(path_foringest, "/dataSpeciesInventory.csv"))}


  header$DateLoadedInDb <- rep(todaysDate)
  ind$DateLoadedInDb <- header$DateLoadedInDb[match(ind$PrimaryKey, header$PrimaryKey)]

  if(file.exists(file.path(path_foringest, "/dataLPI.csv"))) {
    LPI$DateLoadedInDb <- header$DateLoadedInDb[match(LPI$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/dataGap.csv"))) {
    gap$DateLoadedInDb <- header$DateLoadedInDb[match(gap$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/dataHeight.csv"))) {
    hgt$DateLoadedInDb <- header$DateLoadedInDb[match(hgt$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/dataSoilStability.csv"))) {
    ss$DateLoadedInDb <- header$DateLoadedInDb[match(ss$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/geoSpecies.csv"))) {
    sp$DateLoadedInDb <- header$DateLoadedInDb[match(sp$PrimaryKey, header$PrimaryKey)]}
  if(file.exists(file.path(path_foringest, "/dataSpeciesInventory.csv"))) {
    spin$DateLoadedInDb <- header$DateLoadedInDb[match(spin$PrimaryKey, header$PrimaryKey)]}



  write.csv(header,paste0(path_foringest,"/dataHeader.csv"), row.names=FALSE)
  write.csv(ind,paste0(path_foringest,"/geoIndicators.csv"), row.names=FALSE)

  if(file.exists(file.path(path_foringest, "/dataLPI.csv"))) {
    write.csv(LPI,paste0(path_foringest,"/dataLPI.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataGap.csv"))) {
    write.csv(gap,paste0(path_foringest,"/dataGap.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataHeight.csv"))) {
    write.csv(hgt,paste0(path_foringest,"/dataHeight.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataSoilStability.csv"))) {
    write.csv(ss,paste0(path_foringest,"/dataSoilStability.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/geoSpecies.csv"))) {
    write.csv(sp,paste0(path_foringest,"/geoSpecies.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataSpeciesInventory.csv"))) {
    write.csv(spin,paste0(path_foringest,"/dataSpeciesInventory.csv"), row.names=FALSE)}



}
##############################################












