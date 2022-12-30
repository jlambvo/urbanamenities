##=============== Filtering and cleaning helper functions ===================##
#
# These functions are used to automate processing of individual data
# tables as they are parsed. Each table attempts to call a processing
# function following the pattern <proc_dataset_table> (lowercase), e.g. 
# "proc_ztrans_main" (any unmatched tables are passed through). 
#
##===========================================================================#

library(lubridate)
library(geosphere)

##======================= ZTRAX Transaction ============================##

# Some filtering follows suggestions for "high confidence" of fair market
# value as described by Nolte et al. (2021) and documented in 
# https://placeslab.org/ztrax/

proc_ztrans_main <- function(dt) {

  message("Processing transactions main table...")
  
  # Load an index to calculate money in constant dollars
  cpi <- importCPI()
  cpi2021 <- cpi[J(2021)]$CPI
  
  dt %>% setkey(., TransId, FIPS) %>%
    
    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    } %>%
    
    {
      message("Omitting missing date...")
      .[!is.na(DocumentDate),]
    } %>%
    
    {
      message("Extracting year for keying...")
      .[,Year := year(DocumentDate)]
    } %>%
    
    {
      message("Restricting to post 2014")
      .[Year >= 2014]
    } %>%
    
    {
      message("Rounding loan amount to match with HMDA...")
      .[, LoanAmount := round(LoanAmount / 1000) * 1000]
    } %>%
    # 
    # { UNUSED for MN
    #   message("Omitting outliers and missing sales amounts...")
    #     # In MSP many entries appear with a sales price of exactly 500,
    #     # some with normal looking loan amounts. Since buyer and seller 
    #     # names are the same on many (or they involve rapid rotations) these
    #     # are assumed to be refinances or property transfers.
    #     .[!is.na(SalesPriceAmount)] %>% 
    #     .[SalesPriceAmount > 1001] %>%
    #     .[SalesPriceAmount < quantile(SalesPriceAmount, .999)]
    # } %>%
    
    {
      message("Converting money values to 2021 USD...")
      left_join_table(., cpi, 'Year') %>%
        .[, SalesPriceAmount2021 := SalesPriceAmount * ..cpi2021 / CPI] %>%
        .[, LoanAmount2021 := LoanAmount * cpi2021 / CPI]
    } %>%
    
    {
      message("Recoding intra family transfer flags...")
      .[is.na(IntraFamilyTransferFlag), IntraFamilyTransferFlag := FALSE] %>%
      .[IntraFamilyTransferFlag == "Y", IntraFamilyTransferFlag := TRUE] %>%
      .[, IntraFamilyTransferFlag := as.logical(IntraFamilyTransferFlag)]
    } %>%
    
    {
      message("Factoring property use code...")
      .[, PropertyUseStndCode := factor(PropertyUseStndCode)]
      .[PropertyUseStndCode %in% c("SR",NA)]
    } %>%

    {
      message("Filtering document type...")
      .[, DocumentTypeStndCode := factor(DocumentTypeStndCode)] #%>%
        .[DocumentTypeStndCode %in% c('WRDE',
                                      'SLVL',
                                      'SPWD',
                                      'ADDE',
                                      'ASSL',
                                      'BSDE',
                                      'CDDE',
                                      'CFTR',
                                      'CHDE',
                                      'CPDE',
                                      'CTSL',
                                      'DEED',
                                      'EXCH',
                                      'FDDE',
                                      'IDDE',
                                      'JTDE',
                                      'LDCT',
                                      'LWDE',
                                      'OTHR',
                                      'PRDE',
                                      'PTDE',
                                      'RFDE',
                                      'SFLC',
                                      'VLDE'
        )]
    } %>%
    
    {
      message("Filtering data class type...")
      .[, DataClassStndCode := factor(DataClassStndCode)] #%>%
         .[DataClassStndCode %in% c("D","H")]
    } %>%

    unique(., by = c("DocumentDate",
                     "SalesPriceAmount"))

}

proc_ztrans_propertyinfo <- function(dt) {
  
  message("Processing transction property info...")
  
  dt %>% setkey(., TransId, FIPS) %>%
    
    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    } %>%
    
    {
      message("Omitting missing parcel ID")
      .[!is.na(ImportParcelID),]
    } %>%
    
    {
      message("Omitting with missing census tract and block...")
      .[!is.na(PropertyAddressCensusTractAndBlock),]
    } %>%
    
    {
      message("Subsetting to only single property sequence number...")
      .[
        .[, .I[max(PropertySequenceNumber, na.rm=TRUE) == 1],
          by = ImportParcelID]$V1
      ] %>% setkey(., TransId, FIPS)
    } %>%

    {
      message("Generating block group code")
      .[,PropertyAddressCensusTractAndBlock :=
          sub("\\.", "", PropertyAddressCensusTractAndBlock)] %>%
        .[,blockgroup := 
            PropertyAddressCensusTractAndBlock %>% 
            substring(0,12) %>% 
            factor()]
    } %>%
    
    { 
      message("Adding distance to city center(s) in km...")
      .[, CBD_d := distHaversine(matrix(c(PropertyAddressLongitude, 
                                        PropertyAddressLatitude), 
                                      ncol = 2),
                               c(-93.258133, 44.986656)) / 1000] # Minneapolis
    } %>%
    
    {
      message("Removing unneeded transaction main columns")
      .[,c("PropertySequenceNumber") := NULL]
    }
}

proc_ztrans_buyername <- function(dt) {
  
  message("Processing transction buyer name...")
  dt %>% setkey(., TransId, FIPS) %>%
    
    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    } %>%
    
    {
      message("Concatenating multiple names...")
      .[, .(BuyerIndividualFullName = 
              paste(BuyerIndividualFullName, collapse=",")), 
        by = .(TransId,FIPS)]
    } 
  
}

proc_ztrans_buyernamedescriptioncode <- function(dt) {
  
  message("Processing transaction buyer ID..")
  dt %>% setkey(., TransId, FIPS) %>%
    
    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    } %>%
    
    {
      message("Removing duplicates...")
      unique(.)
    } %>%
    
    {
      message("Converting buyer code to factor")
      .[,BuyerDescriptionStndCode := as.factor(BuyerDescriptionStndCode)]
    } %>%
    
    {
      message("Filtering buyers tagged as not individuals, retaining NA...")
      .[BuyerDescriptionStndCode == "ID",]
    } 
  
}

proc_ztrans_buyermailaddress <- function(dt) {
  
  message("Processing buyer mailing address")
  dt %>% setkey(., TransId, FIPS) %>%
    
    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    } %>%
    
    {
      message("Generating block group")
      .[,BuyerMailAddressCensusTractAndBlock := 
          sub("\\.", "", BuyerMailAddressCensusTractAndBlock)] %>%
        .[,buyerBlockgroup := 
            BuyerMailAddressCensusTractAndBlock %>% 
            substring(0,12) %>%
            factor()
          ]
    }
}

proc_ztrans_sellername <- function(dt) {
  
  message("Processing seller names")
  dt %>% setkey(., TransId, FIPS) %>%
    
    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    } %>%
    
    {
      message("Concatenating multiple names...")
      .[, .(SellerIndividualFullName = 
              paste(SellerIndividualFullName, collapse=",")), 
        by = .(TransId,FIPS)]
    }
}

proc_ztrans <- function(dt){
  
  dt %>%
    
    {
      message("Dropping unmatched obs from merge missing parcel ID")
      .[!is.na(ImportParcelID)]
    } %>%
    
    {
      message("Dropping unneeded columns")
      .[, c("BuyerDescriptionStndCode") := NULL]
    } %>%
  
  setkey(., ImportParcelID, Year)
  
}

##======================== ZTrax Assessment ============================##


proc_zasmt_main <- function(dt) {
  
  message("Processing assessment main table")
  dt %>% setkey(., RowID, FIPS) %>%
    
    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    }  %>%
    
    {
      message("Generating assessment block group code")
      .[, PropertyAddressCensusTractAndBlock := 
          sub("\\.", "", PropertyAddressCensusTractAndBlock)] %>%
        .[, blockgroup := 
            PropertyAddressCensusTractAndBlock %>% 
            substring(0,12) %>%
            factor()]
    } %>%
    
    {
      message("Recoding lot size to acres...")
      .[, LotSize := pmin(LotSizeAcres,
                          LotSizeSquareFeet / 43560,
                          na.rm = TRUE)] %>%
        .[, c('LotSizeAcres','LotSizeSquareFeet') := NULL] %>%
        .[LotSize < quantile(LotSize, .999, na.rm = T)]
    } %>%
    
    {
      message("Converting assessment extract date...")

      .[,ExtractDate := parse_date_time(
                                          ExtractDate,
                                          orders=c("%m%Y","ymd")
                                         )]
    } %>%

    {
      message("Extracting year for keying...")
      .[, Year := year(ExtractDate)]
    }
  
}

proc_zasmt_value <- function(dt) {
  
  message("Processing assessment value table")
  dt %>% setkey(., RowID, FIPS) %>%

    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    } 
}

proc_zasmt_building <- function(dt) {
  
  message("Processing assessment building table")
  dt %>%  setkey(., RowID, FIPS) %>%
    
    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    } %>%
    
    {
      message("Converting property use code to factor...")
      .[,PropertyLandUseStndCode := as.factor(PropertyLandUseStndCode)]
    } %>%
    
    {
      message("Filtering building records to residential only...")
      .[str_sub(PropertyLandUseStndCode) == "RR101",]
    } %>%
    
    {
      message("Taking minimum of bathroom count fields...")
      .[, TotalBathrooms := pmin(TotalActualBathCount, 
                                 TotalCalculatedBathCount, 
                                 na.rm=TRUE)]
    } %>%
    
    {
      message("Imputing effective age from original age if NA...")
      .[, EffectiveYearBuilt := pmax(YearBuilt, 
                                     EffectiveYearBuilt, 
                                     na.rm=TRUE)]
    } %>%
    
    {
      message("Generating age from year or effective year built if provided...")
      .[, Age := 2022 - pmax(YearBuilt, 
                             EffectiveYearBuilt, 
                             na.rm = TRUE)]
    } %>%
    
    {
      message("Dropping unneeded columns...")
      .[, c("PropertyLandUseStndCode",
            "TotalActualBathCount",
            "TotalCalculatedBathCount"
            ) := NULL]
    }
    
}

proc_zasmt_buildingareas <- function(dt) {
  
  # Prioritized list of areas to impute
  # livable square footage from (based on MSP)
  
  priority <- c('BAJ' = 1,   # Adjusted Building Area
                'BAL' = 2,   # Living Building Area
                'BAF' = 3,   # Finished Building Area
                'BLF' = 4,   # Living Building Area Finished
                'BAH' = 5,   # Building Gross Area
                'BAE' = 6,   # Effective Building Area
                'BAG' = 7,   # Gross Building Area
                'BAB' = 8,   # Base Building  Area
                'BAT' = 9   # Total Building Area
  )   
  
  keep <- c('RowID',
            'FIPS',
            'BuildingAreaSqFt')
  
  message("Processing assessment building areas table")
  dt %>% setkey(., RowID, BuildingAreaSequenceNumber, FIPS) %>%
    
    {
      message("Filtering to study area FIPS...")
      .[FIPS %in% studyArea,]
    } %>%
    
    {
      message("Filtering to best guess at recorded livable area...")
      .[.[ , .I[which.min(priority[BuildingAreaStndCode])], by = RowID]$V1] 
      
    } %>%
    
    {
      message("Cleaning up area columns...")
      .[, ..keep] 
    }
  
}

proc_zasmt_name <- function(dt){
  
  message("Processing assessment name table...")
  dt %>% setkey(., RowID) %>%
    
    {
      message("Concatenating multiple owner names...")
      .[, .(Name = 
              paste(Name, collapse=",")), 
        by = RowID]
    } 
}

proc_zasmt <- function(dt) {
  
  toFill <- c(
    'BuildingAreaSqFt',
    'Age',
    'TotalBedrooms',
    'TotalBathrooms',
    'MortgageLenderName',
    'ArchitecturalStyleStndCode',
    'NoOfStories',
    'BuildingQualityStndCode'
  )
  
  dt %>%
    
  setkey(., ImportParcelID, Year) %>%

    {
      # Fills NAs forward from the most recent observation, then
      # performs in reverse to fill any leading NAs.
      
      message("Filling missing building data from other years...")
      .[, (toFill) := lapply(.SD, function(x) zoo::na.locf(x, 
                                                           na.rm = FALSE, 
                                                           fromLast = TRUE)
      ),
      .SDcols = toFill,
      by="ImportParcelID"
      ]
    }
  
} 

##======================== Combined ZTRAX ============================##


proc_state <- function(dt) {
  
    dt %>%
      
    {
      message("Reverting year to transaction date...")
      .[, Year := year(DocumentDate)]
    } %>%
    
    setcolorder(.,c("ImportParcelID","Year","DocumentDate","ExtractDate"))
}

##======================== EPA EnviroAtlas ============================##

proc_epa_green <- function(dt){
  
  keep <- c('bgrp',
            'Green_P',
            'Imp_P',
            'MFor_P'
            )
  
  renamed <- c('blockgroup',
              'Green_P',
              'Imp_P',
              'Tree_P'
              )
  
  percent <-  c('Green_P', 'Imp_P', 'Tree_P')
  
  dt[, ..keep] %>%

    {
      message('Renaming EnviroAtlas commmunity blockgroup greenspace vars...')
      setnames(., keep, renamed)
    } %>%
  
    {
       message('Converting EnviroAtlas blockgroup to factors...')
      .[, blockgroup := factor(blockgroup)]
    } %>%
      
    {
        message('Transforming percent to decimal...')
      .[, (percent) := lapply(.SD, `/`, 100), .SDcols = percent]
    }

}

proc_epa_water <- function(dt){
  
  keep <- c('bgrp',
            'RB15_LABGP',
            'RB50_LABGP'
           )
  
  renamed <- c('blockgroup',
               'Water15_P',
               'Water50_P'
              )
  
  percent <-  c('Water15_P', 'Water50_P')
  
  dt[, ..keep] %>%
    
    {
      setnames(., keep, renamed)
    } %>%
    
    {
     .[, blockgroup := factor(blockgroup)]
    } %>%
    
    {
      message('Transforming percent to decimal...')
      .[, (percent) := lapply(.SD, `/`, 100), .SDcols = percent]
    }
  
}

proc_epa_edu <- function(dt){
  
  keep <- c('bgrp',
            'Day_Count',
            'K12_Count'
           )
  
  renamed <- c('blockgroup',
               'Daycare_n',
               'K12_n'
              )
  
  dt[, ..keep] %>%
    
    {
      setnames(., keep, renamed)
    } %>%
    
    {
      .[, blockgroup := factor(blockgroup)]
    }
  
}



##======================== Convenience Functions ============================##

# Returns an inflation coefficient for constant 2021 dollars using a 
# provided year price index data.table or default imported file.  
inflator <- function(year, index = cpi) {
  
  year <- as.numeric(as.character(year))
  sapply(year, function(y)
    index[J(2021)]$Index / index[J(y)]$Index
  )
  
}

# Read in CPI file. This uses the Bureau of Labor Statistics' (BLS) Consumer 
# Price Index for all Urban Consumers Retroactive Series (R-CPI-U-RS), as used
# by the Census Bureau for calculating constant dollers.
# <https://www.census.gov/topics/income-poverty/income/guidance/
#    current-vs-constant-dollars.html>

importCPI <- function(path = file.path(ztraxPath, "R_CPI_U_RS.xlsx")) {

  cpi <- read_xlsx(path , skip = 2) %>% setDT() %>% na.omit() %>%
    setnames(., colnames(.), c("Year","CPI")) %>%
    .[, Year := as.numeric(Year)] %>%
    setkey(Year)
  
}

# Helper function 
at_least <- function(vars, n = 1){
  
  N <- sum(!is.na(vars))
  
  return(c(N, N >= n))
  
}

`%notin%` <- Negate(`%in%`)

# Helper function to perform a left-join of to data tables
left_join_table <- function(x, y, keys) {
  
  x[y,
    on = keys,
    (colnames(y)) := mget(colnames(y))
  ] 
  
}

# Helper function to perform a left-join of to data tables
left_join_table_roll <- function(x, y, on) {
  
  x[y,
    on = on,
    (colnames(y)) := mget(colnames(y)),
    roll = +Inf
  ] 
  
}

# Helper function maps 
get_FIPS_from_alpha <- function(state, rank = NULL, county = NULL){
  
  fips <- read.csv(file.path(geoPath, paste0(state,"_FIPS.csv")),
                   header = TRUE) %>% setDT
  
  if (is.null(county) & is.null(rank)){
    return(fips)
  }
  else if(!is.null(county)){
    return(fips[County == county]$FIPS)
  }
  else if(!is.null(rank)){
    return(fips[as.numeric(rank)]$FIPS)
  }
  else {
    return(NULL)
  }
  
}

get_county_from_alpha <- function(state, code){
  
  fips <- read.csv(file.path(geoPath, paste0(state,"_FIPS.csv")),
                   header = TRUE) %>% setDT
  
  return(fips[code]$County)
  
}
  
# ZTRAX Building area codes for reference:
  
# APT	Apartment
# ATC	Attic
# ATF	Attic Finished
# ATR	Atrium
# ATU	Attic Unfinished
# BAB	Base Building Area
# BAE	Effective Building Area
# BAF	Finished Building Area
# BAG	Gross Building Area
# BAH	Heated Building Area
# BAJ	Adjusted Building Area
# BAL	Living Building Area
# BAP	Perimeter Building Area
# BAQ	Manufacturing
# BAT	Total Building Area
# BFD	Basement Full Daylight
# BKN	Breakfast Nook
# BLF	Living Building Area Finished
# BLU	Living Building Area Unfinished
# BNY	Balcony/Overhang
# BPD	Basement Partial Daylight
# BRZ	Breezeway
# BSF	Basement Full
# BSH	Basement Finished
# BSN	No Basement
# BSP	Basement Partial
# BSR	Bonus Room
# BSU	Basement Unfinished
# BSY	Basement
# CEL	Cellar
# CPY	Canopy/Awning
# CP	Carport
# DEN	Family Room/Den
# EXR	Exercise Room
# GAP	Garage Apartment
# GEP	Glass Enclosed Porch
# GMR	Game Room/Recreation
# GR	Garage
# GTM	Great Room
# HBY	Hobby/Craft/Sewing
# HMT	Media Room/Home Theater
# LAI	Lanai
# LBR	Study/Library
# LBY	Lobby/Vestibule
# LFG	Loft Garage
# LFT	Loft
# LNR	Laundry Room
# MEZ	Mezzanine
# MUD	Mud Room
# OFF	Office
# POR	Portico (Drive Under)
# PRC	Covered Porch
# PRE	Enclosed Porch
# PRH	Porch
# PRO	Open Porch
# PRS	Screened Porch
# RMU	Sun Room
# SIT	Sitting Room
# ST1	Floor 1
# ST1F	Floor 1 Finished
# ST2	Floor 2
# ST3	Floor 3
# ST4	Floor 4
# ST5	Floor 5
# ST6	Floor 6
# ST7	Floor 7
# ST8	Floor 8
# ST9	Floor 9
# STH	Half Story, 3/4 Story
# STL	Lower Level
# STU	Upper Story
# USF	Upper Story Finished
# USU	Upper Story Unfinised
# UTL	Utility/Storage Room

