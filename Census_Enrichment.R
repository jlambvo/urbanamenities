##########==========##########==========##########==========##########==========
## Name: ZTRAX Census Enrichment
## Author: Jonathan Lamb
## Created: Oct 2022
##
## Purpose: This set of functions supports merging a base ZTRAX data extract
## with measures of green space based on high resolution land cover data from 
## EPA EnviroAtlas. The functions below depend on two input files in the 
## working directory with the below default object names:
##

library(tidycensus)
census_api_key('', overwrite = TRUE)

# ----------- Decennenial Census and ACS 5-year Tables-------------------------#

acsTables <- c(
  
  #---- Demographics  ----- #'
   Race             = 'B02001',  # RACE
   Age              = 'B01002',  # MEDIAN AGE BY SEX
   HouseholdType    = 'B11001',  # HOUSEHOLD TYPE (INCLUDING LIVING ALONE) i.e. family vs non-family
   Education        = 'B15003',  # EDUCATIONAL ATTAINMENT FOR THE POPULATION 25 YEARS AND OVER
   
   #---- Economic ----- #
   MedianIncome     = 'B19013',  # MEDIAN HOUSEHOLD INCOME
   PublicAssistance = 'B19058',  # PUBLIC ASSISTANCE INCOME OR FOOD STAMPS/SNAP IN THE PAST 12 MONTHS FOR HOUSEHOLDS (only 2018-2020)
   Tenure           = 'B25003',  # TENURE
   CommuteMode      = 'B08301',  # MEANS OF TRANSPORTATION TO WORK
   TravelTime       = 'B08303',  # TRAVEL TIME TO WORK
   
   #---- Social capital ----- #
   
   Nonprofit        = 'B24080',  # SEX BY CLASS OF WORKER FOR THE CIVILIAN EMPLOYED POPULATION 16 YEARS AND OVER (for non-profit work)
   Mobility         = 'B07201'   # GEOGRAPHICAL MOBILITY IN THE PAST YEAR FOR CURRENT RESIDENCE--METROPOLITAN STATISTICAL AREA LEVEL IN THE UNITED STATES

)

# There is an error fetching table P1 in 2010, so constructing
# this one manually. 
decVariables <- c(
  
  Total    = 'P010001',
  OneRace  = 'P010002',
  White    = 'P010003',
  Black    = 'P010004',
  Ind      = 'P010005',
  Asian    = 'P010006',
  Pacific  = 'P010007',
  Other    = 'P010008',
  TwoPlus  = 'P010009'
  
)

acsdesc <- tidycensus::load_variables(2013, 
                                      dataset = "acs5", 
                                      cache = TRUE) %>% setDT

# ------------------- Get and format data ------------------------#

# Returns a data.table for the variable named in the acsTables list
# as a percentage of the total population in each blockgroup-year.
# If multiple rows are specified, the population is inclusive 
# (e.g. for all education levels or age groups above some level). A
# column name argument can be optionally supplied.

add_acs_percent <- function(transData, state, var, rows, name = NULL){
  
   acs <- get_acs_var(transData, state, acsTables[var], rows)
   aggregate_functions <- list(estimate = sum, summary_est = mean)
   
   acs <- acs[, mapply(function(f, x) as.list(f(x)), 
                       aggregate_functions, 
                       .SD), 
              by = c('blockgroup', 'Year'),
              .SDcols = names(aggregate_functions)]
   
   acs[, (var) := estimate / summary_est]
   
   comment(acs[[var]]) = get_acs_comment(acsTables[var], rows)
   
   if (!is.null(name)) {
     setnames(acs, var, name)
   }
   
   left_join_table(transData, 
                   acs[, -c('estimate', 'summary_est')],
                   keys = c('blockgroup'))
   
}

# As above, but returns the median of the supplied row(s).
# TODO This is intended to compute a population weighted median 
# and so should should only be used with single rows for now.  

add_acs_median <- function(transData, state, var, rows, name = NULL){

  acs <- get_acs_var(transData, state, acsTables[var], rows)
  
  acs <- acs[, lapply(.SD, median),
      by = c('blockgroup', 'Year'),
      .SDcols = 'estimate']

  comment(acs$estimate) = get_acs_comment(acsTables[var], rows)
  
  if (is.null(name)) {
   name <- var
  }
  setnames(acs, 'estimate', name)

  left_join_table(transData, 
                  acs[, -c('estimate', 'summary_est')],
                  keys = c('blockgroup'))
  
}

# Returns a data.table with specified rows by census table number, 
# across years and geographies in the the references transaction data
# (assumes columns for "State" and "County").

get_acs_var <- function(transData, 
                        state,
                        table, 
                        rows, 
                        years = 2013:2019){
  
  counties = unique(transData$County)
  
  map_dfr(setNames(years, years),
          .id = 'Year',
          function(year){
            get_acs(variables = paste0(table, "_", sprintf("%03s",rows)),
                    cache_table = TRUE,
                    geography = 'block group', 
                    state = state,
                    county = counties, 
                    year = year,
                    geometry = FALSE,
                    summary_var = paste0(table, "_001")) %>% 
              setDT %>%
              apply_rule('proc_acs_table')
          }) %>%
    setcolorder(c('blockgroup','Year')) %>%
    setkey(blockgroup, Year)
}

# ------------------- Transform and re-code vars ------------------------#

proc_acs_table <- function(dt){
  
  message(paste("Processing", names(dt)[3]))
  
  dt %>%
    {
      .[, blockgroup := factor(GEOID)]
      .[, c('GEOID', 'NAME') := NULL]
    } %>%
    
    type.convert()
  
}

# Helper function to construct a descriptive annotation based on the
# census variable concepts and names.

get_acs_comment <- function(table, row, desc = acsdesc){
  
  row <- min(row)
  table <- paste0(table, "_", sprintf("%03s",row))
  labels <- desc[grepl(table, name)]$label
  concepts <- desc[grepl(table, name)]$concept
  description <- paste(concepts,
                    str_extract(labels, '[^!!]*$'))
  
  return(description)
  
}