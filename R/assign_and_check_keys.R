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
#'
#' @examples assign_keys(path_project = "D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/dima_exports/", format = "%m/%d/%Y", noteformat = "%m/%d/%Y",nonlineformat = "%m/%d/%Y",non_line_tables = c("tblPlots", "tblLines", "tblSites") )
#' @export
assign_keys <- function(path_project, non_line_tables){
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

  # read all DIMA types and append
  all_dimas <- lapply(X = unique(dima_export_files$table),
                      FUN = function(X) {
                        # read each file associated with a data_type
                        file_list <- dima_export_files$file_path[dima_export_files$table==X]

                        # --- HARMONIZED COMBINATION BLOCK ---
                        raw_df_list <- lapply(X = file_list, FUN = function(X_file) {
                          print(X_file)
                          read.csv(paste0(path_project, X_file), na.strings = c("", "NA")) |>
                            dplyr::mutate(file_path = X_file) |>
                            dplyr::left_join(dima_export_files, by = "file_path") |>
                            dplyr::select(-c(file_path, table))
                        })

                        # Find intersection & capture missing plot key variants
                        common_cols <- purrr::reduce(lapply(raw_df_list, colnames), intersect)
                        all_cols_present <- unique(unlist(lapply(raw_df_list, colnames)))
                        critical_keys_to_keep <- all_cols_present[stringr::str_detect(all_cols_present, "(?i)plot.*key")]
                        final_cols_to_retain <- unique(c(common_cols, critical_keys_to_keep))

                        # Safe padding of structural discrepancies
                        harmonized_df_list <- lapply(raw_df_list, function(df) {
                          missing_keys <- setdiff(final_cols_to_retain, colnames(df))
                          if (length(missing_keys) > 0) {
                            for (mk in missing_keys) df[[mk]] <- NA
                          }
                          return(df[, final_cols_to_retain, drop = FALSE])
                        })

                        data <- do.call(rbind, harmonized_df_list)
                        return(data)
                        # --- END HARMONIZED BLOCK ---
                      })

  #name all of the tables in the all_dimas list
  names(all_dimas) <- unique(dima_export_files$table) |> stringr::str_remove(".csv")

  # the primary key is assigned from the large (appended) CSV with name from table assigned to each observation - however
  # this is done in multiple parts depending on the table type

  # create PrimaryKeys by joining PlotKey to DateVisited. We first have to join to the header tables, then the detail tables
  header_tables <- lapply(X = all_dimas[names(all_dimas) |> stringr::str_detect("Header")],
                          function(X){
                            # If there is already a PlotKey, no need to do anything, otherwise join via tblLines
                            if(!"PlotKey" %in% names(X)){
                              data_pk <- dplyr::left_join(
                                X |> dplyr::mutate(LineKey = as.character(LineKey)),
                                all_dimas$tblLines |>
                                  dplyr::mutate(LineKey = as.character(LineKey)) |>
                                  dplyr::select(PlotKey, LineKey, project, dbname) |>
                                  dplyr::distinct(),
                                relationship = "many-to-one")
                            } else {
                              data_pk <- X
                            }

                            # PARSE EXCEL TEXT DATES USING BASE R ORIGIN
                            data_pk <- data_pk |>
                              dplyr::mutate(
                                # 1. Test if the value is purely a number (like "45716")
                                is_numeric_date = !is.na(suppressWarnings(as.numeric(FormDate))),

                                # 2. Convert based on whether it's a number or actual text
                                DateVisited = dplyr::case_when(
                                  is_numeric_date ~ as.Date(suppressWarnings(as.numeric(FormDate)), origin = "1899-12-30"),
                                  .default = as.Date(suppressWarnings(
                                    lubridate::parse_date_time(FormDate,
                                                               orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS", "ymd HM", "mdy HM"))
                                  ))
                                ),

                                # 3. Build your primary key and clean up temporary helper column
                                PrimaryKey = paste0(PlotKey, DateVisited),
                                FormDate = DateVisited
                              ) |>
                              dplyr::select(-is_numeric_date) # Drop the helper column
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
          # Force RecKey to character in the Detail table
          tblDetail %>%
            dplyr::mutate(RecKey = as.character(RecKey)),

          # Force RecKey to character in the Header table
          tblHeader %>%
            dplyr::mutate(RecKey = as.character(RecKey)) %>%
            dplyr::select_if(names(.) %in% c("PlotKey", "LineKey", "RecKey", "FormDate", "PrimaryKey", "DateVisited", "project", "dbname")),

          relationship = "many-to-one"
        )
      }else{
        print(paste("No header for table", X, "No join performed. Check that this is expected"))
        all_dimas[[X]]
      }
    })

  names(detail_tables) <- detail_list

  # merge the detail and header tables together
  detail_header <- c(detail_tables, header_tables)

  # we also need to get PrimaryKey information into the non-Line based data
  no_lines_tables <- all_dimas[!names(all_dimas) |> stringr::str_detect("Header|Detail|Box|Stack")] |> names()
  data_no_lines <- lapply(X = no_lines_tables,
                          function(X){
                            # For tblPlotsNotes, create a PrimaryKey from PlotKey and NoteDate
                            if(X=="tblPlotNotes"){
                              data <- all_dimas[[X]] |> dplyr::mutate(
                                DateVisited = lubridate::parse_date_time(NoteDate,
                                                                         orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS")),
                                DateVisited = as.Date(DateVisited), # Ensure it's a Date object, not POSIXct
                                PrimaryKey = paste0(PlotKey, DateVisited),
                                NoteDate = DateVisited # Keeping them synced

                              )
                            }else
                              # For tblPlotHistory, create a PrimaryKey from PlotKey and DateRecorded
                              if(X=="tblPlotHistory"){
                                data <- all_dimas[[X]] |> dplyr::mutate(

                                  DateVisited = lubridate::parse_date_time(DateRecorded,
                                                                           orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS")),
                                  DateVisited = as.Date(DateVisited), # Ensure it's a Date object, not POSIXct
                                  PrimaryKey = paste0(PlotKey, DateVisited),
                                  DateRecorded = DateVisited # Keeping them synced
                                )
                              }else
                                # For tblSoilPits, create add a PlotKey and DateVisited. We'll join PrimaryKey later for all plots
                                if(X=="tblSoilPits"){
                                  data <- all_dimas[[X]] |> dplyr::mutate(
                                    DateRecorded = lubridate::parse_date_time(DateRecorded,
                                                                              orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS")),
                                    DateRecorded = as.Date(DateRecorded), # Ensure it's a Date object, not POSIXct

                                  )
                                }else
                                  # For tblSoilPitHorizons, first join with tblSoilPits, then
                                  # add PlotKey and DateVisited
                                  if(X=="tblSoilPitHorizons"){
                                    data <- dplyr::left_join(all_dimas[[X]],
                                                             all_dimas$tblSoilPits |>
                                                               dplyr::select(PlotKey, DateRecorded, SoilKey, project, dbname))|>
                                      dplyr::mutate(

                                        DateRecorded = lubridate::parse_date_time(DateRecorded,
                                                                                  orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS")),
                                        DateRecorded = as.Date(DateRecorded), # Ensure it's a Date object, not POSIXct

                                      )
                                  }else{
                                    all_dimas[[X]]
                                  }
                          })

  names(data_no_lines) <- no_lines_tables

  # Plots, Lines, SoilPits, and SoilPit Horizons all need PrimaryKeys that correspond with visit of the PlotKey
  table_plots<- non_line_tables # list of nonnumeric tables in data; could include c("tblPlots", "tblLines", "tblSites", "tblSoilPits", "tblSoilPitHorizons")


  # MWAC

  mwac <- names(all_dimas)[stringr::str_detect(names(all_dimas), "BoxCollection")]


  MWAC_tables <- lapply(mwac, function(pkey) {

    # collection data
    X <- all_dimas[[pkey]]

    # join with Box to get StackID, then join Stack to get PlotKey from the all_dimas list
    data_pk <- X %>%
      dplyr::left_join(
        all_dimas[["tblBSNE_Box"]] %>%
          dplyr::select(-dbname, -project, -Notes, -DateEstablished),
        by = "BoxID",
        relationship = "many-to-many"
      ) %>%
      dplyr::left_join(
        all_dimas[["tblBSNE_Stack"]] %>%
          dplyr::select(-dbname, -project, -Notes),
        by = "StackID",
        relationship = "many-to-many"
      )

    # create PrimaryKey
    data_pk <- data_pk %>%
      dplyr::mutate(
        # 1. Check if collectDate is a raw Excel numeric string
        is_numeric_date = !is.na(suppressWarnings(as.numeric(collectDate))),

        # 2. Parse safely: use origin if numeric, use lubridate if text
        ParsedDate = dplyr::if_else(
          is_numeric_date,
          as.Date(as.numeric(collectDate), origin = "1899-12-30"),
          as.Date(suppressWarnings(
            lubridate::parse_date_time(collectDate,
                                       orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS", "ymd HM", "mdy HM"))
          ))
        ),

        # 3. Assign to both variables and create the PrimaryKey
        collectDate = ParsedDate,
        DateVisited = ParsedDate,
        PrimaryKey  = paste0(PlotKey, DateVisited)
      ) %>%
      dplyr::select(-is_numeric_date, -ParsedDate) # Clean up temporary helper columns

    return(data_pk)
  })

  # original table names back to the list
  names(MWAC_tables) <- mwac



  # DDT

  ddt <- names(all_dimas)[stringr::str_detect(names(all_dimas), "TrapCollection")]


  ddt_tables <- lapply(ddt, function(pkey) {

    # collection data
    X <- all_dimas[[pkey]]

    # join with Box to get StackID, then join Stack to get PlotKey from the all_dimas list
    data_pk <- X %>%
      dplyr::left_join(
        all_dimas[["tblBSNE_Stack"]] %>%
          dplyr::select(-dbname, -project, -Notes),
        by = "StackID",
        relationship = "many-to-many"
      )

    # create PrimaryKey
    data_pk <- data_pk %>%
      dplyr::mutate(
        # 1. Check if collectDate is a raw Excel numeric string
        is_numeric_date = !is.na(suppressWarnings(as.numeric(collectDate))),

        # 2. Parse safely: use origin if numeric, use lubridate if text
        ParsedDate = dplyr::if_else(
          is_numeric_date,
          as.Date(as.numeric(collectDate), origin = "1899-12-30"),
          as.Date(suppressWarnings(
            lubridate::parse_date_time(collectDate,
                                       orders = c("ymd", "mdy", "dmy", "ymd HMS", "mdy HMS", "ymd HM", "mdy HM"))
          ))
        ),

        # 3. Assign to both variables and create the PrimaryKey
        collectDate = ParsedDate,
        DateVisited = ParsedDate,
        PrimaryKey  = paste0(PlotKey, DateVisited)
      ) %>%
      dplyr::select(-is_numeric_date, -ParsedDate) # Clean up temporary helper columns

    return(data_pk)
  })

  # original table names back to the list
  names(ddt_tables) <- ddt




  # merge with detail and header
  detail_header <- c(detail_header, MWAC_tables, ddt_tables)


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
#'
#' @examples dima_table_qc(dima_data_list = readRDS("QC/all_dima_pks.Rdata"), primarykey_qc = read.csv(paste0("QC/primarykey_resolve_", date_pkey_qc_run, ".csv")), path_qc = file.path("D:/modifying_data_prep_script_10032025/NWERN_HAFB_10132025/QC"))
#' @export
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



###############################################
#' NRI table QC
#'
#'checks the data produced from assign_keys, including checks for missingness and missing coordinates. Problem PrimaryKeys are removed. Thus, prior to running, check with the data owner about the PrimaryKeys with NAs or that were sampled within a certain number of days to ensure removing the problem PrimaryKeys is desired.
#'
#' @param nri as an object, read_nri_text output
#' @param path_qc path where the QC data will be saved
#'
#' @return CSVs of the QC related to the PrimaryKey assignment in the assigned QC folder
#'
#' @export
nri_table_qc <- function(nri, path_qc){



  # check for missingness of observations
  # check for NAs in observations
  missingness <- do.call(rbind,lapply(X = names(nri),

                                      function(X){
                                        data <- nri[[X]]

                                        # for tables with PrimaryKeys, check for NAs in columns
                                        if("PrimaryKey" %in% colnames(data)){

                                          # identify number of missing rows per field
                                          missingness <- data|>
                                            dplyr::group_by(PrimaryKey) |>
                                            dplyr::summarise(dplyr::across(dplyr::everything(), ~ sum(is.na(.x))))|>
                                            dplyr::ungroup()

                                          # pivot longer so we can summarize
                                          missingness_tall <-  missingness |>
                                            tidyr::pivot_longer(cols = -c( "PrimaryKey"),
                                                                names_to = "Field",
                                                                values_to = "n_missing")

                                          missingness_summary <- missingness_tall |>
                                            dplyr::group_by(PrimaryKey,Field) |>
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
    dplyr::left_join(nri[["POINTCOORDINATES"]] |> dplyr::select(PrimaryKey)) |>
    # subset where this is no anomaly
    subset(anomaly!=0) |>
    # subset where there are no missing values
    subset(n_missing>0) |>
    # dplyr::left_join(read.csv("table_fields_importance.csv",
    #                           na.strings = c("", "NA"))) |>
    # # join in the coord_qc table for a comprehensive report
    #dplyr::bind_rows(coord_qc)|>
    #dplyr::arrange(Notes, Action) |>

    # rearrange for readability
    dplyr::relocate( PrimaryKey) |> # removed table, as third obs until get importance csv

    # add a data owner response column
    dplyr::mutate(DataOwnerResponse = NA)


  SWBC_check <- missingness_notes

  write.csv(SWBC_check, file.path(path_qc, "SWBC_DIMA_check_all.csv"), row.names = F)
  #write.csv(SWBC_check |> subset(!is.na(Action)), file.path(path_qc, "SWBC_DIMA_check_resolve.csv"), row.names = F)

}
################################################


#' Assign and QC Keys depending on the source
#'
#' Assigns PrimaryKey and other relevant database keys depending on the data source,
#' writes raw and processed tables, and executes quality control checks.
#'
#' @param dsn Character string. The path to the data source network or directory. Only applicable if source is "NRI". Defaults to NULL.
#' @param source Character string. The data source type; options include "NRI", "BLM_AIM", "DIMA", or "Other".
#' @param sensitive_data Character string. Path to the folder containing NRI sensitive data (only used if source is "NRI").
#' @param pkey_assigned Logical. Indicates whether PrimaryKeys have already been assigned. Controls DIMA branching logic. Defaults to NULL.
#' @param path_original_files Character string. Path to the directory where raw or original files (Rdata/CSVs) should be saved. Defaults to NULL.
#' @param path_qc Character string. Path to the directory where QC files and reports will be saved. Defaults to NULL.
#' @param path_project Character string. Path to the project folder containing the source datasets (e.g., DIMA tables/tblPlots). Defaults to NULL.
#' @param non_line_table_list Character vector. A list of tables that do not contain line/transect data. Only used for DIMA key assignments. Defaults to NULL.
#' @param date_pkey_qc_run Character string. A date string (e.g., "YYYYMMDD") used to dynamically look up or name the DIMA PrimaryKey resolution CSV. Defaults to NULL.
#'
#' @export
assign_keys_all <- function(dsn = NULL,
                            source,
                            sensitive_data = NULL,
                            pkey_assigned = NULL,
                            path_original_files = NULL,
                            path_qc = NULL,
                            path_project = NULL,
                            DIMATables = NULL,
                            non_line_table_list = NULL,
                            date_pkey_qc_run = NULL) {

  if (source == "NRI") {

    # add field names to package
    table_name <- terradactyl::table_name(nri_path = dsn)

    # Read NRI tables in and apply names
    data_list <- lapply(X = table_name, function(X) {
      print(X)
      # read all files for the table and merge
      data <- terradactyl::read_nri_text(
        dsn = dsn,
        table_name = X,
        DBKey = "auto",
        GL_schema_path = "D:/LDC_data_10012025/NRI/og_data/Grazing_Land_Data_Guide.xlsx"
      )
      return(data)
    })

    # add names to list elements
    names(data_list) <- toupper(table_name)

    data_list <- terradactyl::assign_pkey_nri(data_list = data_list, sensitive_data = sensitive_data)

    # save original file as Rdata
    saveRDS(data_list, paste0(path_original_files, "/NRI_raw_2024.Rdata"))

    # Write all tables to CSV
    up_tab <- toupper(table_name)
    sapply(
      X = up_tab,
      function(X) {
        print(X)
        dat <- as.data.frame(data_list[[X]])
        dat$DBKey <- basename(dsn)
        dat <- dat %>% dplyr::distinct()
        write.csv(dat, paste0(path_original_files, "/", toupper(X), ".csv"), row.names = FALSE)
      })

    # QC
    terradactylutils3::nri_table_qc(nri = data_list, path_qc = path_qc)

    return(data_list)

  } else if (source == "BLM_AIM") {

    message("PrimaryKey assigned by BLM")

  } else if (source %in% c("DIMA", "Other") & is.null(pkey_assigned)) {

    ## function to assign keys (project, PrimaryKey, dbname, RecKey, LineKey) to each data file
    terradactylutils3::assign_keys(path_project = path_project, non_line_tables = non_line_table_list)

    # using data produced from assign_keys function to produce QC files to review
    data_list <- readRDS(paste0(path_qc, "/all_dimas_pks.Rdata"))

    terradactylutils3::dima_table_qc(
      dima_data_list = data_list,
      primarykey_qc = read.csv(paste0(path_qc, "/primarykey_resolve_", date_pkey_qc_run, ".csv")),
      path_qc = path_qc
    )
    return(data_list)

  } else if (source == "DIMA" & !is.null(pkey_assigned)) {

    # =========================================================================
    # NEW: Scan and rename user-defined primary key column to 'PrimaryKey'
    # =========================================================================
    message(paste0("Scanning files to replace '", pkey_assigned, "' with 'PrimaryKey'..."))

    # Gather all CSV files across BOTH path_project and DIMATables
    all_project_files <- list.files(path_project, pattern = "\\.csv$", full.names = TRUE)
    all_dima_files    <- list.files(DIMATables, pattern = "\\.csv$", full.names = TRUE)
    files_to_rename   <- unique(c(all_project_files, all_dima_files))

    for (file_path in files_to_rename) {
      dat <- tryCatch(read.csv(file_path), error = function(e) return(NULL))
      if (is.null(dat)) next

      # Check if the user-specified column exists (case-insensitive check)
      matching_col <- names(dat)[tolower(names(dat)) == tolower(pkey_assigned)]

      if (length(matching_col) > 0) {
        # Rename the matching column to PrimaryKey
        names(dat)[names(dat) == matching_col] <- "PrimaryKey"
        # Overwrite the original file with the updated column name
        write.csv(dat, file_path, row.names = FALSE)
      }
    }

    # =========================================================================
    # CONTINUATION: Date Discrepancy QC and Data Import
    # =========================================================================

    # 1. Load tblPlots from path_project to get baseline PrimaryKeys and coordinates
    tbl_plots_path <- list.files(path_project, pattern = "tblPlots", full.names = TRUE)
    if(length(tbl_plots_path) == 0) stop("tblPlots file not found in path_project.")

    tblPlots <- if(grepl("\\.csv$", tbl_plots_path[1], ignore.case = TRUE)) {
      read.csv(tbl_plots_path[1])
    } else {
      readRDS(tbl_plots_path[1])
    }

    plots_base <- tblPlots %>%
      dplyr::select(PrimaryKey, Latitude, Longitude) %>%
      dplyr::distinct()

    # 2. Scan all CSV files in DIMATables for DateVisited and PrimaryKey
    all_files <- list.files(DIMATables, pattern = "\\.csv$", full.names = TRUE)

    scan_results <- lapply(all_files, function(file_path) {
      dat <- tryCatch(read.csv(file_path), error = function(e) return(NULL))
      if (is.null(dat)) return(NULL)

      # Standardize column naming for evaluation
      names(dat) <- toupper(names(dat))

      if ("PRIMARYKEY" %in% names(dat) & "DATEVISITED" %in% names(dat)) {
        res <- dat %>%
          dplyr::select(PrimaryKey = PRIMARYKEY, DateVisited = DATEVISITED) %>%
          dplyr::distinct() %>%
          dplyr::mutate(
            file_name = basename(file_path),
            from_target_method = ifelse(grepl("LPI|Gap|SpeciesInventory", file_name, ignore.case = TRUE), "Yes", "No")
          )
        return(res)
      }
      return(NULL)
    })

    # Combine scanned combinations
    scanned_df <- dplyr::bind_rows(scan_results)

    if (nrow(scanned_df) > 0) {
      # 3. Calculate date differences within 1 year (365 days) for identical PrimaryKeys
      scanned_df$DateVisited <- as.Date(scanned_df$DateVisited)

      date_qc_report <- scanned_df %>%
        dplyr::group_by(PrimaryKey) %>%
        dplyr::reframe(
          Date_1 = rep(DateVisited, each = dplyr::n()),
          Date_2 = rep(DateVisited, times = dplyr::n()),
          File_1 = rep(file_name, each = dplyr::n()),
          File_2 = rep(file_name, times = dplyr::n()),
          From_Target_1 = rep(from_target_method, each = dplyr::n()),
          From_Target_2 = rep(from_target_method, times = dplyr::n())
        ) %>%
        dplyr::filter(File_1 != File_2) %>%
        dplyr::mutate(Date_Diff_Days = as.numeric(abs(difftime(Date_1, Date_2, units = "days")))) %>%
        dplyr::filter(Date_Diff_Days <= 365 & Date_Diff_Days > 0) %>%
        dplyr::left_join(plots_base, by = "PrimaryKey") %>%
        dplyr::distinct()

      # Save to QC path instead of returning it
      write.csv(date_qc_report, paste0(path_qc, "/date_discrepancy_report.csv"), row.names = FALSE)
    } else {
      message("No matching tables with PrimaryKey and DateVisited found to evaluate for QC.")
    }

    # 4. Read all tables from DIMATables path to build and return the dima_data_list
    csv_files <- list.files(path = DIMATables, pattern = "\\.csv$", full.names = TRUE)

    data_list <- csv_files |>
      purrr::map(readr::read_csv, show_col_types = FALSE) |>
      purrr::set_names(tools::file_path_sans_ext(basename(csv_files)))

    return(data_list)
  }
}


#' Quality Control Check for Primary Keys and Visit Dates
#'
#' @description
#' Compares primary keys across plot data and unique data observations to flag
#' orphaned records, generic plot IDs, and consecutive plots visited within a short
#' window. Generates troubleshooting logs for data owners.
#'
#' @param all_dimas_pks A named list of data frames containing DIMA tables. Must
#'   include `tblPlots`.
#' @param unique_pks A data frame of compiled unique primary keys from data observation tables.
#' @param path_qc Character. The directory path where output QC CSV files will be written.
#'
#' @return A data frame containing the complete primary key and date check report.
#'
#' @importFrom dplyr select mutate distinct bind_rows group_by arrange lead ungroup case_when left_join
#' @importFrom tidyr pivot_wider
#'
#' @export
check_pk_and_visit_dates <- function(all_dimas_pks, unique_pks, path_qc) {

  # 1. Verification Check
  if (!"tblPlots" %in% names(all_dimas_pks)) {
    stop("The 'all_dimas_pks' list must contain a 'tblPlots' data frame.")
  }
  if (!dir.exists(path_qc)) {
    dir.create(path_qc, recursive = TRUE)
  }

  message("--- Running PrimaryKey and DateVisited QC Check ---")

  # 2. Join tblPlots to unique data observations and pivot wider to map presence
  pk_date_check <- all_dimas_pks$tblPlots %>%
    dplyr::select(PlotKey, PrimaryKey, DateVisited) %>%
    dplyr::mutate(method = "tblPlots") %>%
    dplyr::distinct() %>%
    dplyr::bind_rows(unique_pks) %>%
    dplyr::mutate(values = "yes") %>%
    tidyr::pivot_wider(
      names_from = method,
      values_from = values,
      values_fill = "no"
    )

  # 3. Track temporal spacing between consecutive plot visits under the same PlotKey
  pk_date_check <- pk_date_check %>%
    dplyr::group_by(PlotKey) %>%
    dplyr::arrange(desc(DateVisited), .by_group = TRUE) %>%
    dplyr::mutate(
      ClosestDateVisited = dplyr::lead(DateVisited),
      # Calculate differences in days explicitly and cast safely to numeric
      DaysDiff = as.numeric(difftime(DateVisited, ClosestDateVisited, units = "days"))
    ) %>%
    dplyr::ungroup() %>%
    # Assign Notes and recommended Actions based on time windows
    dplyr::mutate(
      Notes = dplyr::case_when(
        DaysDiff <= 7 ~ "Visit within 7 days",
        DaysDiff > 7  & DaysDiff <= 30  ~ "Visit within 7-30 days",
        DaysDiff > 30 & DaysDiff <= 60  ~ "Visit within 30-60 days",
        DaysDiff > 60 & DaysDiff <= 275 ~ "Visit within 30-275 days",
        .default = NA_character_
      ),
      Action = dplyr::case_when(
        DaysDiff > 7 & DaysDiff <= 275 ~ "Confirm date visited",
        DaysDiff <= 7 ~ "Consider grouping date visits",
        .default = NA_character_
      )
    ) %>%
    # Bring PlotID back in to help users troubleshoot specific sites
    dplyr::left_join(
      all_dimas_pks[["tblPlots"]] %>%
        dplyr::select(PrimaryKey, PlotKey, PlotID) %>%
        dplyr::distinct() %>%
        dplyr::filter(!is.na(PrimaryKey)),
      by = c("PrimaryKey", "PlotKey")
    )

  # 4. Flag orphaned records and generic junk placeholder plots
  pk_date_check <- pk_date_check %>%
    dplyr::mutate(
      Notes = dplyr::case_when(
        is.na(PlotKey) ~ "Orphan records",
        PlotKey %in% c("123123123", "999999999") ~ "Generic plots",
        .default = Notes
      ),
      Action = dplyr::case_when(
        is.na(PlotKey) ~ "Delete",
        PlotKey %in% c("123123123", "999999999") ~ "Delete",
        .default = Action
      ),
      DataOwnerResponse = NA_character_
    )

  # 5. Output reports to CSV
  main_report_path <- file.path(path_qc, "primarykey_date_check.csv")
  action_report_path <- file.path(path_qc, paste0("primarykey_resolve_", Sys.Date(), ".csv"))

  write.csv(pk_date_check, main_report_path, row.names = FALSE)

  action_needed_df <- pk_date_check %>% dplyr::filter(!is.na(Action))
  write.csv(action_needed_df, action_report_path, row.names = FALSE)

  message("QC Complete. Logs saved to:\n - ", main_report_path, "\n - ", action_report_path)

  return(pk_date_check)
}
