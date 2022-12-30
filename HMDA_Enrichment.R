##########==========##########==========##########==========##########==========
## Name: ZTRAX HMDA Enrichment
## Author: Jonathan Lamb
## Created: Oct 2022
##
## Purpose: This set of functions supports merging a base ZTRAX data extract
## with HMDA records to impute the buyer's race/ethnicity and other
## demographic information. 

##======================== Dependencies =====================================##

#library(reclin) # WARNING This conflict with leaflet via identical()

##======================== Import data =====================================##

# Aggregates and merges loan records and reporting institution data for the
# specified state. Uses default file names by default. Tested for 2007 onward
# only, pre-2007 may need additional handling.

import_hmda <- function(state, 
                        recordsPattern = "records_labels.zip",
                        institutionsPattern= "panel.zip") {
  
  records <- aggregate_hmda(state, 
                            recordsPattern, 
                            clean_hmda_records
                            )
  
  institutions <- aggregate_hmda(state, 
                                 institutionsPattern, 
                                 clean_hmda_institutions
                                 )
  
  records[institutions, 
          (colnames(institutions)) := mget(colnames(institutions)), 
          on = "RespondentID", 
          all = FALSE
          ]

  return(records)
  
}


# HMDA data is split into annual releases, so this function reads, cleans,
# and concatenates data files found in the working directory matching
# a supplied name pattern (e.g. the default loan records file with plain
# text labels ends in "records_labels.zip"). The specified data cleaning
# function is applied as each file is read.

aggregate_hmda <- function(state, pattern, clean) {
  
  archives <- dir(file.path(hmdaPath, state), pattern = paste0("*", pattern))
  message(archives[1])
  message(file.path(state, archives[1]))
  
  archives %>% 
    map(~fread(
      cmd = paste0('unzip -p ', file.path(hmdaPath, state, .))
    ) %>% clean()
    ) %>%
   reduce(rbind, fill=TRUE)
}


##============================= HMDA Cleaning Functions  ================================##

clean_hmda_records <- function(dt) {
  
 message(paste("Processing HMDA records data for ", dt$as_of_year[1]))

  keep <- c("as_of_year",
            "respondent_id",
            "minority_population",
            "applicant_race_name_1",
            "applicant_ethnicity_name",
            "applicant_income_000s",
            "loan_amount_000s",
            "loan_purpose",
            "action_taken",
            "state_code",
            "county_code",
            "census_tract_number"
           )
  
  renamed <- c("Year",
            "RespondentID",
            "MinorityPercent",
            "DerivedRace",
            "DerivedEthnicity",
            "Income",
            "LoanAmount",
            "LoanPurpose",
            "ActionTaken",
            "StateCode",
            "CountyCode",
            "CensusTract")

  dt[, ..keep] %>%
    
    {
      setnames(.,  old = keep, new = renamed)
    } %>%
    
    {
      # Concatenate full census tract number to match ZTRAX
      .[, CensusTract := paste0(StateCode,
                                sprintf("%03s",CountyCode),
                                sprintf("%06s",sub("\\.", "", CensusTract))
                               )] %>% 
      .[, c("StateCode","CountyCode") := NULL]
    } %>%
    
    {
      # Filter to activity in study area
      .[substr(CensusTract,0,5) %in% studyArea]
    } %>%
    
    {
      # Convert order of magnitude to match ZTRAX
      .[, LoanAmount := LoanAmount * 1000]
      .[, Income := Income * 1000]
    }
  
}


clean_hmda_institutions <- function(dt) {
  
  message(paste("Processing HMDA institution data for ", dt$'Activity Year'[1]))
  
  dt %>%

    {
      # Naming scheme changes after 2009 to include "<Variable> (Panel)" which is
      # modified to match columns when binding data tables together.
      setnames(., str_remove(colnames(.), "\\s\\(Panel\\)"))
    } %>%

    {
     # Rename to match the main HMDA records files and ZTRAX 
      setnames(., 
               old = c("Respondent Identification Number",
                       "Respondent ID",
                       "Activity Year",
                       "Respondent Name"), 
               new = c("RespondentID",
                       "RespondentID",
                       "Year",
                       "LenderName"), 
               skip_absent = TRUE)
    } %>%

    {
     # ZTRAX matching procedure requires only the year and institution name,
     # respondent ID is used as key to merge with main HMDA records
      .[, c("RespondentID","LenderName", "Year")]
    }

}

##====================== Merge with transaction data  ========================##


# Repurposed from Kelsey O'Hollaren:
# "Methodology for fuzzy matching using reclin package:
# https://cran.r-project.org/web/packages/reclin/vignettes/introduction_to_reclin.html
# Blocking variable should be one with no errors. In this case use censustract
# and LoanAmount."

ztrax_hmda_merge <- function(ztrax, 
                             hmda,
                             blocking = c("CensusTract", "LoanAmount", "Year"),
                             compareby = c("LenderName", "LoanAmount"),
                             comparator = jaro_winkler, 
                             threshold = .5){
  
  # Temporary tract number for matching if needed
  ztrax[, "CensusTract" := substr(blockgroup,0,11)] 
  
  linked <- pair_blocking(ztrax, hmda, blocking) %>%
    compare_pairs(by= compareby, default_comparator = comparator(0.9)) %>%
    select_threshold("LenderName", var = "threshold", threshold = threshold) %>%
    #select_n_to_m("LenderName", var = "ntom", threshold = threshold) %>%
    link(all_x = TRUE, all_y = FALSE) %>%
    setDT() %>%
    .[!is.na(TransId)]
  
  # Remove 
  ztrax[, "CensusTract" := NULL]
  
  return(linked)
  
}

