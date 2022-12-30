##########==========##########==========##########==========##########==========
## Name: Minnesota eCRV Transaction & Met Council Parcels
## Author: Jonathan Lamb
## Created: November 2022
##
## Purpose: These functions construct an transaction database alternative to 
## ZTRAX for the state of Minnesota, using two sources: MN eCRVs and parcel
## data aggregated by the MSP Metropolitan Council 
##
## The MN Dept. of Revenue requires an electronic certificate of value (eCRV) 
## for all property sales over $3,000, see:
## (https://www.revenue.state.mn.us/electronic-certificate-real-estate-value-ecrv)
##
## eCRV records are archived as individual XML files that must be parsed and 
## aggregated. Many of these appear to be exported with special characters that  
## require special handling. Records also cover the entire state, to this script 
## includes functions to pre-filter to the study area and arms-length transactions.
##
## The Met Council collates parcel data including geographic boundaries and 
## tax information for the 7-county MSP area. Although a unified schema is used,
## each county reports differently and so fields have uneven coverage. See
## (https://gisdata.mn.gov/dataset/us-mn-state-metrogis-plan-regional-parcels).
## 
## Because dwelling characteristics are not reported by Hennepin County in the 
## public dataset, I merge ZTRAX Assessment data by county parcel ID.  
##

##======================== Dependencies =====================================##


library(xml2)
library(parallel)
library(furrr)
library(tidyverse)
library(data.table)
library(lubridate)
library(stringdist)

##============= Included eCRV field descriptions ============================##

# 'PropertyForm/County',                    # Numeric county code of the purchased property
# 'PropertyForm/Parcels/.../ParcelId',      # =County parcel ID of the purchased property
# 'salesAgreementForm/deedContractDate',    # Reported date of sale
# 'PropertyForm/mnPropertyAddress/.../',    # Full street address of the purchased property
# 'salesAgreementForm/totPurchaseAmt',      # Total purchase price paid
# 'salesAgreementForm/downPmtEquity',       # Down payment amount if any
# 'salesAgreementForm/financeType',         # Cash, mortgage, etc.
# 'salesAgreementForm/sellerPdPts',         # Points, interest, and other closing costs paid by seller
# 'buyersForm/individuals/...',             # Full name of the buyer(s)
# 'buyersForm/individuals/...',             # Address provided for buyer (typically same as the property)
# 'sellersForm/individuals/...',            # Full name of the seller(s)
# 'sellersForm/individuals/...',            # Address provided for seller (may not be new residence)
# 'salesAgreementForm/personalPropertyIncludedInTotal', # Whether price included personal property 
# 'salesAgreementForm/receivedInTrade',     # Property was received wholly or partly in trase
# 'salesAgreementForm/likeKindExchange',    # Received under "like kind" exchange
# 'supplementaryForm/relatedInd',           # Is the buyer related to the seller
# 'supplementaryForm/governmentInd',        # Is buyer or seller a government entity
# 'supplementaryForm/nameChangedInd',       # Was even primarily a change in name on deed?
# 'supplementaryForm/nonListedInd',         # Was the property not publicly advertised?
# 'supplementaryForm/buyerAppraisalAmt',    # Private appraisal reported by buyer (0 includes unknown)
# 'supplementaryForm/sellerAppraisalAmt'    # Private appraisal reported by seller (0 includes unknown)

##======================== eCRV Import =====================================##

import_ecrv <- function(path = ecrvPath,
                        pattern = '.zip') {
  
  weeks <- list.files(path, pattern)
  recordPattern <- "<us.mn.state.mdor.ecrv.extract.form.EcrvForm>*.*?</us.mn.state.mdor.ecrv.extract.form.EcrvForm>"
  whitelist <- "[^0-9a-zA-Z@. <> /=\"\"()-]"
  
  plan('multisession', workers = detectCores() - 1)
  
  records <- future_map(weeks, 
                        function(week) {
                          
                          message(paste0("Parsing ", week))
                          archive <- file.path(path, week)
                          
                          # Some archives have erroneous files so retrieve
                          # a list of only record .xml files.
                          files <- unzip(archive, list = TRUE)$Names %>% 
                            .[grepl("xml", .)]
                          
                          # Use the shell to unzip the specified files and
                          # pipe directly into object in memory. Combine single
                          # raw string object because of special characters,
                          # manually split into XML documents.
                          records <- system(paste("unzip -p", archive,files),
                                            intern = TRUE) %>% 
                            paste(collapse = "") %>%
                            gsub(whitelist, "", .) %>%
                            str_extract_all(recordPattern) %>% 
                            unlist()
                          
                          import_ecrv_week(records)
                          
                        },
                        .progress = TRUE) %>%
   rbindlist %>% 
   setDT() %>%
   apply_rule('proc_ecrv')
  
  return(records)
  
}

import_ecrv_week <- function(records){
  
  week <- map_dfr(records, 
          .id = 'Record',
          function(i){
            i <- (read_xml(charToRaw(i), 
                           encoding = "ISO-Latin-1") %>% 
                    as_list())[[1]]
            
            if (prefilter_ecrv(i) == TRUE){
              import_ecrv_xml(i)
            }
  })
  
  return(week)
 
}

import_ecrv_xml <- function(record){
  
  `header`   <- record$headerForm
  `property` <- record$propertyForm
  `parcel` <- record$propertyForm$parcels$us.mn.state.mdor.ecrv.extract.form.ParcelForm
  `address` <- record$propertyForm$mnPropertyAddresses$us.mn.state.mdor.ecrv.extract.form.PropertyAddressForm
  `contract` <- record$salesAgreementForm
  `buyer` <- record$buyersForm$individuals$us.mn.state.mdor.ecrv.extract.form.StandardBuyerSellerForm
  `seller` <- record$sellersForm$individuals$us.mn.state.mdor.ecrv.extract.form.StandardBuyerSellerForm
  `supplementary` <- record$supplementaryForm
  `use` <- record$propertyForm$plannedUses$us.mn.state.mdor.ecrv.extract.form.PlannedUseForm
  
  ecrvNames  <- list(ID               = header$crvNumberId,
                     CountyCode       = property$county,        
                     ParcelID         = parcel$parcelId,          
                     ContractDate     = contract$deedContractDate,      
                     PropertyAddress  = paste(address$street1,
                                              address$street2,
                                              collapse = " "),   
                     SalePrice        = contract$totPurchaseAmt,         
                     Downpayment      = contract$downPmtEquity,       
                     FinanceType      = contract$financeType,       
                     SellerPaid       = contract$sellerPdPts,        
                     BuyerName        = paste(buyer$firstName,
                                              buyer$middleName,
                                              buyer$lastName,
                                              collapse = " "),
                     BuyerAddress     = paste(buyer$addressLine1,
                                              buyer$addressLine2,
                                              buyer$city,
                                              buyer$stateOrProvince,
                                              collapse = " "),      
                     SellerName       =  paste(seller$firstName,
                                               seller$middleName,
                                               seller$lastName,
                                               collapse = " "),       
                     SellerAddress    = paste(seller$addressLine1,
                                              seller$addressLine2,
                                              seller$city,
                                              seller$stateOrProvince,
                                              collapse = " "),     
                     PersonalProperty = contract$personalPropertyIncludedInTotal,  
                     InTrade          = contract$receivedInTrade,           
                     InKind           = contract$likeKindExchange, 
                     UseCode1         = min(use$tier1Cde[[1]], use$propertyTypeCode[[1]]),
                     UseCode2         = min(use$tier2Cde[[1]], use$propertyUseCode[[1]]),
                     Related          = supplementary$relatedInd,          
                     GovernmentSale   = supplementary$governmentInd,   
                     NameChangeOnly   = supplementary$nameChangeInd,    
                     Unlisted         = supplementary$nonListedInd,         
                     BuyerAppraisal   = supplementary$buyerAppraisalAmt,    
                     SellerAppraisal  = supplementary$sellerAppraisalAmt 
  ) %>% unlist %>% as.list() %>% data.frame
  
  return(ecrvNames)
  
}

# Pre-filter to include only records for single family residences purchased
# by individuals as a primary residence, and only sales of single parcels. 
  
prefilter_ecrv <- function(record){
  
  check <- (  get_FIPS_from_alpha(27, record$propertyForm$county) %in% studyArea &
              length(record$buyersForm$individuals) > 0 &
              record$propertyForm$principalResidence == TRUE &
              length(record$propertyForm$parcels) == 1 &
              record$salesAgreementForm$buyerPartInterest == FALSE)
  
  return(check)
}

proc_ecrv <- function(dt) {
  
  logical <- c('PersonalProperty',
               'InTrade',
               'InKind',
               'Related',
               'GovernmentSale',
               'NameChangeOnly',
               'Unlisted')
  
  factor <- c('UseCode1',
              'UseCode2',
              'FinanceType')
  
  # Load an index to calculate money in constant dollars
  cpi <- importCPI()
  cpi2021 <- cpi[J(2021)]$CPI
  
  dt %>% type.convert(as.is = TRUE) %>%
    {
      message("Filtering to records within study area...")
      .[, FIPS := get_FIPS_from_alpha(27, CountyCode)]
      .[FIPS %in% studyArea]
    } %>%
    
    {
      message("Recoding county code to name..")
      .[, County := get_county_from_alpha(27, CountyCode)]
    }
    
    {
      message("Converting column classes....")
      .[, ParcelID := gsub("[[:punct:]]", "", ParcelID)]
      .[, (logical) := lapply(.SD, as.logical), .SDcols = logical]
      .[, (factor) :=  lapply(.SD, factor), .SDcols = factor]
      .[, ContractDate := parse_date_time(ContractDate, orders=c("%m%Y","ymd"))]
    } %>%
    
    {
      message("Dropping non-market transcactions... ")
      .[(InKind + InTrade + Related + GovernmentSale + NameChangeOnly + Unlisted) == 0] %>%
      .[UseCode1 %in% c("RES, RESID") | UseCode2 %in% c("SINGLEFAM", "HOUSESGL", "TOWNHOUSE","DUPLEX")]
    } %>%
    
    {
      message("Generating year column and filtering to study period...")
      .[, Year := year(ContractDate)][
        Year %in% 2015:2019
      ]
    } %>%
    
    {
      message("Extracting month of sale...")
      .[, Month = month(ContractDate)]
    }
    
    {
      message("Filtering outlier sale prices....")
      .[SalePrice < quantile(.$SalePrice, .999) & SalePrice > 0]
    }
    
    {
      message("Converting money values to 2021 USD...")
      merge(., cpi, by = 'Year') %>%
        .[, SalePrice2021 := (SalePrice * cpi2021) / CPI] %>%
        .[, LoanAmount := SalePrice - Downpayment] %>%
        .[, LoanAmount2021 := LoanAmount * cpi2021 / CPI]
    } %>% setkey(., ParcelID, Year)

}

##======================== Met Council MSP Parcels ===========================##


add_mn_parcels <- function(ecrv, parcels) {
  
  unmatched <- ecrv[ParcelID %notin% parcels$ParcelID]
  
  ecrvp <- list(
    merge(ecrv, 
          mnParcels, 
          by = c('County', 'ParcelID'), 
          all = FALSE),
    merge_ecrv_parcels_on_address(unmatched, mnParcels)) %>%
    rbindlist(use.name = TRUE, fill = TRUE) %>%
    apply_rule('proc_ecrv_parcels') %>%
    ecrvp[, Year := year(ContractDate)]
  
}

merge_ecrv_parcels_on_address <- function(crv, parcels) {
  
  message('Creating temporary column subset of crv records...')
  .crv <- crv[PropertyAddress != ""]
  
  message('Extracting crv street number...')
  .crv[ , `:=`(crvStreetNumber = word(PropertyAddress),
               crvStreetName = word(PropertyAddress, start = 2, end = -1) %>%
                 toupper())]
  
  message("Finding close matches...")
  nmatch <- .crv[parcels, on = .(County == County,
                                 crvStreetNumber == parcelStreetNumber),
                 nomatch = NULL]
  
  message("Attempting to match name")
  match <- nmatch[nmatch[, .I[which.min(stringdist::stringdist(crvStreetName, 
                                                               parcelStreetName))], 
                         by = PropertyAddress]$V1][
                           , -c('crvStreetNumber', 
                                'crvStreetName', 
                                'parcelStreetName', 
                                'i.ParcelID')]
  
  return(match)
  
}

proc_ecrv_parcels <- function(dt) {
  
  keep <- c('ID',
            'ParcelID',
            'Year',
            'ContractDate',
            'PropertyAddress',
            'County',
            'SalePrice',
            'Downpayment',
            'FinanceType',
            'SellerPaid',
            'BuyerName',
            'OwnerName',
            'SellerName',
            'PersonalProperty',
            'UseCode2',
            'ParcelSize',
            'HomeStyle',
            'FinSqFt',
            'ParcelYearBuilt',
            'LastParcelSaleDate',
            'LastParcelSalePrice',
            'SchoolDst',
            'Watershed',
            'shape')
  
  dt %>% 
    {
      message("Dropping unneeded columns from merged parcel and crv datatable...")
      .[, ..keep]
    } %>%
    
    {
      message("Setting column classes...")
      .[, SellerPaid := as.numeric(SellerPaid)]
    } %>%
    
    {
      message("Calculating age...")
      .[, Age := 2022 - ParcelYearBuilt]
    } %>%
    
    {
      message("Filtering implausible values")
      .[LivingSqFt < quantile(msp$LivingSqFt, .999)]
    } %>%
    
    {
      message("Recoding zeros to NA for property attributes...")
      .[LivingSqFt == 0] <- NA
      .[ParcelSize == 0] <- NA
    } %>%
    
    {
      message("Mapping to block group")
      add_block_groups(.)
    }
  
}


##======================== Merge w/ZTRAX ====================================##

# ZTRAX assessor data includes some fields such as dwelling characteristics 
# not provided by some counties (namely Hennepin). This function attempts to
# match records by parcel number to ZTRAX records to augment observations. 

add_ztrax_asmt <- function(ecrv, asmt) {
  
  ecrv[asmt,
      on = c(ParcelID = "UnformattedAssessorParcelNumber", blockgroup = "blockgroup", Year = "Year"),
      (colnames(asmt)) := mget(colnames(asmt)),
      roll = +Inf]
  ecrv[, Year := year(ContractDate)]
  ecrv[, LivingSqFt := min(FinSqFt, BuildingAreaSqFt)]
  
}
