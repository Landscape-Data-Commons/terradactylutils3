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
#'
#' @examples translate_schema2(schema = schema,datatype = "dataHeight",dropcols = TRUE, verbose = TRUE)
#' @noRd
#' @export
translate_schema2 <- function(data,
                              datatype,
                              schema,
                              dropcols = TRUE,
                              verbose = TRUE){

  #### Sanitization ------------------------------------------------------------
  ##### Schema -----------------------------------------------------------------
  ### standardize names
  # colnames(matrix)[colnames(matrix) == fromcol] <- "terradactylAlias"
  # colnames(matrix)[colnames(matrix) == tocol] <- "ToColumn"

  ### process the incoming matrix by assigning actions to take at each row
  matrix_processed <- dplyr::filter(.data = schema,
                                    Table == datatype) |>
    dplyr::mutate(.data = _,
                  Field <- stringr::str_trim(string = Field,
                                             side = "both")) |>
    dplyr::filter(.data = _,
                  Field != "" | terradactylAlias != "") |>
    dplyr::select(.data = _,
                  tidyselect::all_of(x = c("terradactylAlias",
                                           "Field"))) |>
    dplyr::mutate(.data = _,
                  DropColumn = terradactylAlias != "" & Field == "",
                  AddColumn = Field != "" & terradactylAlias == "",
                  ChangeColumn = Field != "" & terradactylAlias != "" & Field != terradactylAlias,
                  NoAction = Field == terradactylAlias & !AddColumn & !DropColumn,
                  Error = (AddColumn + DropColumn + ChangeColumn + NoAction) != 1)

  # Check for errors!
  if(sum(matrix_processed$Error) > 0) {
    warning("Errors found in translation matrix. Returning diagnostic information.")
    return(errors)
  }

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
  outdata <- dplyr::rename_at(.tbl = data,
                              .vars = ChangeColumn$terradactylAlias,
                              .funs = ~ ChangeColumn$Field) |>
    `is.na<-`(AddColumn$Field |> unique())


  # select only the tables in the out schema
  goodnames <- dplyr::filter(.data = matrix_processed,
                             Field != "") |>
    dplyr::pull(.data = _,
                Field)

  if (verbose) {
    message(paste("Returning the following columns/variables:",
                  paste(goodnames,
                        collapse = ", ")))
  }

  # This was an all_of() in the past, but that was brittle.
  # Now we use an any_of() and then inform the user about the missing variables
  outdata <- dplyr::select(.data = outdata,
                           tidyselect::any_of(x = goodnames))

  missing_names <- setdiff(x = goodnames,
                           y = names(outdata))
  if (length(missing_names) > 0) {
    if (verbose) {
      message(paste("The following variables are still missing and will be added, populated with the value NA:",
                    paste(missing_names,
                          sep = ", ")))
    }
    for (current_missing_name in missing_names) {
      outdata[[current_missing_name]] <- NA
    }
  }

  # making sure in the correct order after selecting missing names
  outdata <- outdata |>
    dplyr::select(tidyselect::all_of(goodnames))

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

##############################################
#' Format for ingest files
#'
#' updates the files in the path_foringest path to have the correct DBKey and DateLoadedInDb
#'
#' @param path_foringest path where data for ingest are saved
#' @param DateLoadedInDb in standard date format, the date you are running the code
#' @param DBKey_date the date that will be associated with the DBKey. For DIMAs, this is the date the data were received in Y-m-d format
#'
#' @return CSVs of the for ingest files saved to the specified path_foringest
#'
#' @examples db_info(path_foringest = path_foringest,  DateLoadedInDb = format(Sys.Date(), "%m/%d/%Y"))
#' @export
db_info <- function(path_foringest, DateLoadedInDb, DBKey_date){

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
  if(file.exists(file.path(path_foringest, "/dataHorizontalFlux.csv"))){
    hf <- read.csv(paste0(path_foringest, "/dataHorizontalFlux.csv"))}
  if(file.exists(file.path(path_foringest, "/dataDustDeposition.csv"))){
    ddt <- read.csv(paste0(path_foringest, "/dataDustDeposition.csv"))}
  if(file.exists(file.path(path_foringest, "/dataSoilHorizons.csv"))){
    sh <- read.csv(paste0(path_foringest, "/dataSoilHorizons.csv"))}


  header$DateLoadedInDb <- rep(todaysDate)
  header$DBKey <- paste0(header$ProjectKey, DBKey_date)
  ind$DateLoadedInDb <- header$DateLoadedInDb[match(ind$PrimaryKey, header$PrimaryKey)]
  ind$DBKey <- header$DBKey[match(ind$PrimaryKey, header$PrimaryKey)]

  if(file.exists(file.path(path_foringest, "/dataLPI.csv"))) {
    LPI$DateLoadedInDb <- header$DateLoadedInDb[match(LPI$PrimaryKey, header$PrimaryKey)]
  LPI$DBKey <- header$DBKey[match(LPI$PrimaryKey, header$PrimaryKey)]}

  if(file.exists(file.path(path_foringest, "/dataGap.csv"))) {
    gap$DateLoadedInDb <- header$DateLoadedInDb[match(gap$PrimaryKey, header$PrimaryKey)]
gap$DBKey <- header$DBKey[match(gap$PrimaryKey, header$PrimaryKey)]}

  if(file.exists(file.path(path_foringest, "/dataHeight.csv"))) {
    hgt$DateLoadedInDb <- header$DateLoadedInDb[match(hgt$PrimaryKey, header$PrimaryKey)]
  hgt$DBKey <- header$DBKey[match(hgt$PrimaryKey, header$PrimaryKey)]}

  if(file.exists(file.path(path_foringest, "/dataSoilStability.csv"))) {
    ss$DateLoadedInDb <- header$DateLoadedInDb[match(ss$PrimaryKey, header$PrimaryKey)]
  ss$DBKey <- header$DBKey[match(ss$PrimaryKey, header$PrimaryKey)]}

  if(file.exists(file.path(path_foringest, "/geoSpecies.csv"))) {
    sp$DateLoadedInDb <- header$DateLoadedInDb[match(sp$PrimaryKey, header$PrimaryKey)]
  sp$DBKey <- header$DBKey[match(sp$PrimaryKey, header$PrimaryKey)]}

  if(file.exists(file.path(path_foringest, "/dataSpeciesInventory.csv"))) {
    spin$DateLoadedInDb <- header$DateLoadedInDb[match(spin$PrimaryKey, header$PrimaryKey)]
  spin$DBKey <- header$DBKey[match(spin$PrimaryKey, header$PrimaryKey)]}


  if(file.exists(file.path(path_foringest, "/dataHorizontalFlux.csv"))) {
    hf$DateLoadedInDb <- header$DateLoadedInDb[match(hf$PrimaryKey, header$PrimaryKey)]
    hf$DBKey <- header$DBKey[match(hf$PrimaryKey, header$PrimaryKey)]}

  if(file.exists(file.path(path_foringest, "/dataDustDeposition.csv"))) {
    ddt$DateLoadedInDb <- header$DateLoadedInDb[match(ddt$PrimaryKey, header$PrimaryKey)]
    ddt$DBKey <- header$DBKey[match(ddt$PrimaryKey, header$PrimaryKey)]}

   if(file.exists(file.path(path_foringest, "/dataSoilHorizons.csv"))) {
    sh$DateLoadedInDb <- header$DateLoadedInDb[match(sh$PrimaryKey, header$PrimaryKey)]
    sh$DBKey <- header$DBKey[match(sh$PrimaryKey, header$PrimaryKey)]}



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
  if(file.exists(file.path(path_foringest, "/dataDustDeposition.csv"))) {
    write.csv(ddt,paste0(path_foringest,"/dataDustDeposition.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataHorizontalFlux.csv"))) {
    write.csv(hf,paste0(path_foringest,"/dataHorizontalFlux.csv"), row.names=FALSE)}
  if(file.exists(file.path(path_foringest, "/dataSoilHorizons.csv"))) {
    write.csv(sh,paste0(path_foringest,"/dataSoilHorizons.csv"), row.names=FALSE)}


}
##############################################




##############################################
#' Separate by ProjectKey for LDC Ingester
#'
#' separates the Projects into their own For Ingest folder
#'
#' @param path_foringest path where data for ingest are saved
#'
#' @return For Ingest folders for each ProjectKey
#'
#' @export

separate_foringest_by_projkey <- function(path_foringest){
message("Multiple ProjectKeys detected; check For_Ingest_by_projkey folder for data separated by ProjectKeys")
path_foringest_split <- file.path(path_foringest, "For_Ingest_by_projkey")
if (!dir.exists(path_foringest_split)) dir.create(path_foringest_split)

# bind those in the For Ingest folder
#select for ingest folder
all_csv_paths <- list.files(path = path_foringest, recursive = TRUE,
                            pattern = "\\.csv$", full.names = TRUE)

all_csv_paths <- all_csv_paths[grepl("For Ingest", all_csv_paths)]

#get the groups of data (evident in csv name)
dfs <- data.frame(all_csv_paths)
dfs$name <- sub(".*\\/", "", dfs$all_csv_paths) #used to match any character sequence up to and including the last forward slash in a string


# bind csvs of same group

list_of_dfs <- list()

names <- unique(dfs$name)

for (current_name in names){

  #get file names
  files_to_read <- dfs %>%
    dplyr::filter(name == !!current_name) %>% #!! unquoting
    dplyr::pull(all_csv_paths) #pull converts col to vect


  # Initialize data frame
  combined_dat <- read.csv(files_to_read[1]) #get the first df in so we can do for loop

  # join the rest of the files with another for loop
  # using full join because it handles the diff cols in geo files
  for(path in files_to_read[-1]){
    df <- read.csv(path)
    combined_dat <- bind_rows(combined_dat, df)
  }


  list_of_dfs[[current_name]] <- combined_dat
}


# combine and then update the DBKey column

# Apply the function to each data frame in the list
list_of_dfs <- lapply(X = list_of_dfs[names(list_of_dfs)],
                      FUN = function(X) {
                        X$DBKey <- paste0(X$ProjectKey,"_", date )
                        X
                      })

for(proj in projectkey) {

  # Create the project directory
  path_foringest_proj <- file.path(path_foringest_split, proj)
  if (!dir.exists(path_foringest_proj)) dir.create(path_foringest_proj, recursive = TRUE)

  # Iterate over names of the list
  lapply(names(list_of_dfs), function(name) {

    #get df
    df <- list_of_dfs[[name]]

    # filter for proj
    df <- df[df$ProjectKey == proj, ]

    write.csv(df, file.path(path_foringest_proj, paste0(name, ".csv")), row.names = FALSE)
  })
}

}
