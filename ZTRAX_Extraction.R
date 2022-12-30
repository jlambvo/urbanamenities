##########==========##########==========##########==========##########==========
## Name: ZTRAX hedonics dataset
## Author: Jonathan Lamb
## Created: March 2022
##
## Purpose: These are supporting programs to automatic the extraction of
## ZTRAX data and merge with other sources to generate a clean database
## using externally-defined configuration files that specify variables
## and study areas to include. Automatic data cleaning requires an external
## script that is specified in the main user script.

# ZTRAX data are stored as a relational database between numerous tables in
# two branches (ZTrans and ZAsmt). An excel layout file maps specific fields to their 
# parent tables and column numbers. This R script references the layout to automate  
# construction of a sample ZTrax database merging selected fields from both branches from 
# a subset of counties, applying processing rules to each table as they are read. 

##======================== Dependencies =====================================##

library(tidyverse)
library(data.table)
library(readxl)
library(fst)
library(tidylog)
library(parallel)
library(furrr)

##============ Import functions for ZTRAX layout sheets =====================##

# Parse an external definition of fields to include. Expects an
# .xlsx with a sheet named for each main dataset (ZAsmt and ZTrans),
# a header row with the names of tables to pull from, and any number
# of fields in each column.

load_ztrax_includes <- function(path) {
  
  message("Loading ZTRAX variable include list...")
  
  includePath <- ztraxIncludeFile
  include <- excel_sheets(includePath) %>% 
    map(function(x) read_excel(includePath,sheet = x))
  names(include) <- excel_sheets(includePath)
  
  return(include)
  
}

# Import a multi-sheet excel file with a mapping of table and field
# names to a column number. Table names are truncated to remove "ut"
# for easier parsing and matching. NOTE that "DateType" carries through
# a typo from the provided file, should be "DataType" 

load_ztrax_layout <- function(path) {
  
  message("Loading ZTRAX data layout...")
  
  layoutPath <- file.path(path,"ZTRAX_layout.xlsx")
  layout <- excel_sheets(layoutPath) %>% 
    map(function(x) 
          read_excel(layoutPath, 
                     sheet = x, 
                     range = cell_cols("A:D")) %>% 
          na.omit() %>%
          mutate(TableName = str_sub(TableName, 3,),
                 DateType = map_type(DateType)) %>%
          split(.$TableName)
       ) 
  names(layout) <- excel_sheets(layoutPath)
  
  return(layout)
  
}

# Retrieves a list of county-level FIPS codes that make up
# the study area.

loadStudyArea <- function(path = sourcePath) {
  
  message("Loading study area...")
  
  unlist(
    read_excel(
      file.path(sourcePath,"ZTRAX_studyarea.xlsx"),
      col_names=FALSE)
  )
  
}

# Maps original specified SQL data types to R primitives for specifying
# columns, since fread() can detect the wrong type depending on the sample

map_type <- function(rawType) {
  
  mapType <- c(
    "uniqueidentifier" = "character", 
    "bigint" = "numeric", 
    "char" = "character", 
    "varchar" = "character",  
    "date" = "Date", 
    "smallint" = "integer",      
    "int" = "integer",
    "money" = "numeric", 
    "decimal" = "double",
    "tinyint" = "integer"
  )
  
  return(mapType[rawType])
  
}

# Return a list of column numbers and type pairs for the corresponding fields
# in the specified table and dataset ("ZTrans" or "ZAsmt"). Expects arguments
# e.g. ("ZTrans","Main",include,layout)

get_column_ids <- function(dataset, 
                           table, 
                           include = load_ztrax_includes(sourcePath), 
                           layout = load_ztrax_layout(sourcePath)) {
  
  message("Looking up columns IDs...")
  
  match <- layout[[dataset]][[table]] %>%
    filter(
      FieldName %in% include[[dataset]][[table]] %>%
        na.omit()
    )
  
  setNames(match$DateType, paste("V",match$column_id,sep=""))
  
}

# Look up column name(s) by variable number(s), accommodates variables
# in arbitrary order.

get_column_name <- function(dataset, table, col) {
  
  message("Mapping column IDs to names...")
  
  layout <-  load_ztrax_layout(sourcePath)
  i <- str_sub(col, 2) %>% as.numeric()
  layout[[dataset]][[table]]$FieldName[i]
  
}

##======================= ZTrax Import Functions ============================##
#
# This series of functions is used recursively to automate the construction
# of a sample dataset for a specified set of FIPS areas and variables. 
#
#============================================================================##

# Returns a specific ZTrans or ZAsmt table for a specified state, and applies
# any specified re-processing and cleaning functions. By default will dynamically
# ready in an external variable list to include but can be overriden with
# specified fields. 

import_ztrax_table <- function(state, 
                               dataset, 
                               table, 
                               vintage = "20181230",
                               include = load_ztrax_includes(sourcePath), 
                               layout = load_ztrax_layout(sourcePath)) {
  
  message(paste("Getting", dataset, state, table))
  
  # Generate arguments for reading files and get the
  # current data, adding historical if needed.

  cols <- get_column_ids(dataset, table, include, layout)
  paths <- generate_ztrax_paths(state, vintage, dataset, table)
  data <- get_file_or_archive(paths, cols)
  
  if (dataset == "ZAsmt"){
      message("Looking for historic")
      hist <- get_historic(paths, cols)
      data <- bind_rows(data, hist)
  }

  # Rename columns from the "include" spreadsheet
  colnames(data) <- colnames(data) %>% get_column_name(dataset,table,.)

  # Attempt to automatically run any processing for this table.
  f <- paste("proc", dataset, table, sep = "_")
  apply_rule(data, f)
}

# This function generates paths and names used for reading in files.
# NOTE: The archives appear to have been compressed on Windows 
# where directory structure resulted in backslashes, so we need
# special handling for escapes. 

generate_ztrax_paths <- function(state, 
                                 vintage,
                                 dataset, 
                                 table) {
  
  file <- paste0(table, ".txt")
  archive <- paste0(state, ".zip")
  
  list(
    "target" = file.path(ztraxPath, vintage, state, paste(dataset, file, sep = "\\")),
    "archive" = file.path(ztraxPath, vintage, archive),
    "fileListed" = paste(dataset, file, sep = "\\"),
    "fileCMD" = paste(dataset, file, sep = r"(\\\\)"),
    "historicTarget" = file.path(ztraxPath, historical, state, "ZAsmt", file),
    "historicArchive" = file.path(ztraxPath, historical, archive)
  )
  
}

# Function to locate relevant file(s) for the data table checking
# for an already compressed file, and finally an archive.

get_file_or_archive <- function(paths, cols) {
  
  message(paths$target )
  
  if (paths$target %>% file.exists()) {
    
    message("Found inflated data...")
    fread_ztrax(file = paths$target, 
                select = cols)
  }
  
  else {
    message("No inflated data found, attempting to unzip...")
    fread_ztrax(cmd = paste("unzip -p", 
                            paths$archive, 
                            paths$fileCMD),
               select = cols
               )
  }
  
}

# fread only allows either 'file' ors 'cmd' argument to be used, 
# so this wrapper function passes the appropriate arg while
# preserving other specifications.

fread_ztrax <- function(...) {
  message("reading..")
  raw <-fread(...,
              nrows = Inf,
              sep = '|',
              header = FALSE,
              stringsAsFactors = FALSE,
              na.strings = "",
              fill = TRUE,
              nThread = 4,
              showProgress = TRUE,
              quote = "")
  
}

# Historical assessor data is archived separately from current
# year records, and includes only six tables. This function looks 
# for available historic files in a specified directory.

get_historic <- function(paths, cols) {
  
  inflated <- file.exists(paths$historicTarget)
  archived <- file.exists(paths$historicArchive)
  
  if (inflated){
    message("Found uncompressed assessor data.")
    paths$target <- paths$historicTarget
    get_file_or_archive(paths, cols)
  }
  
  else if (archived) {
    tableNames <- unzip(paths$historicArchive, list = TRUE)$Name
    if (paths$fileListed %in% tableNames) {
      paths$archive <- paths$historicArchive
      get_file_or_archive(paths, cols)
    }
    else {
      message("Historic archive found but missing table...")
    }
  }
  
  else {
    message("No historic found...")
  }
  
}

# Wrapper function attempts to apply a processing rule matching a 
# dynamically generated string to a table. See section "Filtering 
# and cleaning helper functions"

apply_rule <- function(df, funcName) {
  
  funcName <- funcName %>% tolower()
  message(paste("Checking for ",funcName))
  
  f <- tryCatch(
    match.fun(funcName),
    error = function(cond) message("No rule found, keeping raw...")
  )
  
  if(is.function(f)){
    
    df <- tryCatch(
        df <- f(df),
        error = function(cond) message(paste("Error applying rule: ",cond)),
        finally = return(df)
      )
  }
  
  else {
    return(df)
  }

}

# Iterate and extract desired transaction and assessor tables 
# for the specified state FIPS code, merging into a single data table,
# which can be overridden by specifying mergTables = FALSE

import_ztrax_state <- function(state,
                               vintage = "20181230",
                               include = load_ztrax_includes(sourcePath),
                               layout = load_ztrax_layout(sourcePath),
                               mergeTables = TRUE) {
  
  extract <- names(include) %>%
    map(function(x) {
      extract <- Reduce(function(i,j) {
        message("Attempting to merge tables...")
        i[j, (colnames(j)) := mget(colnames(j)), 
          on = key(j), 
          all = FALSE]
      },
      map(names(include[[x]]), 
          function(y) {
            import_ztrax_table(state, 
                               x, 
                               y, 
                               vintage, 
                               include, 
                               layout)
            })) %>%
      {if(x == "ZTrans"){apply_rule(., 'proc_ztrans')}
      else if(x == "ZAsmt"){apply_rule(., 'proc_zasmt')}}
    })
  
  if(mergeTables == TRUE){
    merge_trans_asmt(extract[[1]], extract[[2]]) %>% apply_rule('proc_state')
  }
  
  return(extract)
  
}

import_ztrax_dataset <- function(state,
                                 dataset,
                                 vintage,
                                 include,
                                 layout) {
  
  dataset <- map(names(include), 
                 function(table) {
                   import_ztrax_table(state, 
                                      dataset, 
                                      table, 
                                      vintage, 
                                      include, 
                                      layout)
                 }) %>%
    Reduce(merge.data.table, 
           by = key(y), 
           all = FALSE)
  
}

# Join extracts of the main two ZTRAX tables for a state containing transaction
# and assessment records, using ImportParcelID as the primary key and attempting
# to match by year, using either the nearest available assessment record, or can
# specify +/- Inf to match forward or backward from the next available (although
# leading or trailing entries will be left unmatched).

merge_trans_asmt <- function(trans, 
                             asmt, 
                             roll = "nearest"
                             ){
  
  message(paste("Merging transaction and assessment tables by ", roll))
  trans[asmt,
        on = .(ImportParcelID, Year),
        (colnames(asmt)) := mget(colnames(asmt)),
        roll = roll,
        rollends = c(TRUE,TRUE)
  ] 

}