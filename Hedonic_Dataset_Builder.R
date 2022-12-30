##########==========##########==========##########==========##########==========
## Name: ZTRAX hedonics dataset
## Author: Jonathan Lamb
## Created: July
##
## Purpose: This script creates an enriched dataset of one or more metropolitan 
## areas for hedonic price analysis of urban amenities. Supporting source files
## combine public transaction and assessor data, U.S. Census, EPA EnviroAtlas,
## Open StreetMaps, and other sources described in the README.
##
##===========================================================================##

##========================== Data paths ======================================##

# Setup environment for data files and output

working <- "~/Documents/Dissertation"
sourcePath <- file.path(working, "Programs")
ztraxPath <- file.path(working, "ZTRAX")
ztraxIncludeFile <- file.path(sourcePath, "ZTRAX_include.xlsx")
epaPath <- file.path(working, "EnviroAtlas")
landcoverPath <- file.path(working, "Landcover")
hmdaPath <- file.path(working, "HMDA")
censusPath <- file.path(working, "Census")
osmPath <- file.path(working, "OSM")
geoPath <- file.path(working, "Geo")
ecrvPath <- file.path(working, "eCRV")
historical <- "Historical" # folder (if any) within ztraxPath

##======================== Dependencies =====================================##

# This script makes use of functions to automate extracting and cleaning
# data for analysis, organized into several files.

source(file.path(sourcePath, "ZTRAX_Extraction.R"))
source(file.path(sourcePath, "Cleaning_Functions.R"))
source(file.path(sourcePath, "eCRV_Extraction.R")) 
source(file.path(sourcePath, "Landcover_Enrichment.R"))
source(file.path(sourcePath, "Census_Enrichment.R"))
source(file.path(sourcePath, "Geo_Enrichment.R"))
source(file.path(sourcePath,"HMDA_Enrichment.R")) 

##===================== Create ZTRAX extracts ================================##
##  
##  Build the base ZTRAX extract with variables and FIPS selected in the 
##  "include" and "studyArea" excel files. This only needs to be done once!
##  
##============================================================================##

# Initialize the study area as a list of FIPS codes
studyArea <- loadStudyArea()

# Construct the base ztrax databases for a state by FIPS code, with option
# to leave transaction and properties unmerged (should only need to be
# run once unless included fields change)
ztrax_msp <- import_ztrax_state(27, mergeTables = FALSE)

# Save base import with the vintage specified
write.fst(ztrax_msp[[1]],file.path(ztraxPath, "extracts/27_Trans_20181230b.fst"))
write.fst(ztrax_msp[[2]],file.path(ztraxPath, "extracts/27_Asmt_20181230b.fst"))


##===================== Create eCRV Extract (MN Only) ======================##
##  
##   Build a base transaction extract for MN from Dept. of Revenue sales 
##   records and parcel data published by the Metropolitan Council 
##
##==========================================================================##

# Parse weekly extracts and return as a single cleaned data.table (should
# only need to be run once).
msp <- import_ecrv()
msp <- write.fst(msp, file.path(ecrvPath, "Extracts/ecrv_studyarea.fst"))

# Read in and clean MN parcels supplied by the Metropolitan Council (should 
# only need to be run once)
mnParcels <- import_mn_parcels()
saveRDS(mnParcels, file.path(geoPath, '27_parcels.rds'))

# Add parcel boundaries to MN ecrv extract
mnParcels <- readRDS(file.path(geoPath, '27_parcels.rds'))
msp <- read.fst(file.path(ecrvPath, "Extracts/ecrv_studyarea.fst"), as.data.table = TRUE)
msp <- add_mn_parcels(msp, mnParcels)
saveRDS(msp, file.path(ecrvPath, 'Extracts/ecrv_parcels.rds'))

# Add ZTRAX assessment data to ecrv extract for additional building attributes
msp <- readRDS(file.path(ecrvPath, 'Extracts/ecrv_parcels.rds'))
prop <- read.fst(file.path(ztraxPath, "extracts/27_Asmt_20181230b.fst"), as.data.table = TRUE)
msp <- add_ztrax_asmt(msp, prop)
saveRDS(msp, file.path(geoPath, 'ecrv_parcels_asmt.rds'))



##===================== Enrich with HMDA Data ===============================##
##  
##  Use supporting functions in HMDA_Enrichment to attempt to match each
##  record in a ZTRAX extract with the race and other selected demographics 
##  of the buyer.
##
##===========================================================================##




##=================== Enrich with OpenStreetMaps ============================##
##  
##  Uses supporting functions in OSM_Enrichment to augment each record in a
##  base ZTRAX extract with geographic and point of interest data.  
##  
##===========================================================================##

#  Extract OSM for specified years and layers (only need to do this once for
# the final dataset)
osm <- import_osm(years = '2022')
saveRDS(osm, file.path(osmPath, 'osm_msp_2022.rds'))

# Open a previous OSM extraction
osm <- readRDS(file.path(osmPath, 'osm_msp_2022.rds'))

# Generate feature subsets from main OSM
features <- create_feature_sets(osm)

# Add logical indicators for sites close to shoreline of water bodies
add_water_buffer(msp, osm, d = 100)
add_water_buffer(msp, osm, d = 175)

# Add logical indicator for sites lose to major highways, presuming noise 
# is a disamenity. According to DoT, sound levels near highways are 70-80dB at
# 15 meters (https://highways.dot.gov/public-roads/julyaugust-2003/living-noise)
# and urban daytime is about 50dB. Each 6dB reduction requires a doubling of 
# distance, so 15 * 2^5 = 480 meters would reduce highway noise to below
# background level. 
add_highway_buffer(msp, osm, d = 480)

# Add count of features within a walkable shed of 
add_feature_count(msp, features[['thirdplaces']], name = 'nThirdPlaces2', d = 1000)
add_feature_count(msp, features[['economic']], name = 'nEconomic', d = 1000)
add_feature_count(msp, features[['transit']], name = 'nTransit', d = 1000)


# Add distance to closest instance of a feature type
add_feature_distance(msp, features$townhall, 'Townhall')
add_feature_distance(msp, features$highway, 'Highway')
add_feature_distance(msp, features$water, 'Water')
add_feature_distance(msp, features$industrial, 'Industrial')
add_feature_distance(msp, features$car, 'Transit')


# Map to high school attendance area
add_school_areas(msp)

# Save enriched state transactions
saveRDS(msp, file.path(ecrvPath, "Extracts/msp_ecrv_features.rds"))

##==================== Enrich with Census Data ==============================##
##  
##  Uses supporting functions in Census_Enrichment to augment 
##  
##===========================================================================##

add_acs_percent(msp, state = 'MN', 'Education', 22:25, name = 'BG_Bachelors')
add_acs_percent(msp, 'MN', 'HouseholdType', 2, name = 'BG_FamilyHouseholds')
add_acs_percent(msp, 'MN', "Race", 2, name = 'BG_White')
add_acs_median(msp, 'MN', "MedianIncome", 1, name = 'BG_Income')
add_acs_percent(msp, 'MN', "Tenure", 2, name = 'BG_Own')
add_acs_percent(msp, 'MN', "Mobility", 3, name = 'BG_Moved')
add_acs_median(msp, 'MN', "Age", 1, name = 'BG_Age')
add_acs_median(msp, 'MN', "TravelTime", 1, name = 'BG_CommuteTime')
add_acs_percent(msp, 'MN', "Nonprofit", 6, name = 'BG_Nonprofit')
add_acs_median(msp, 'MN', "Race", 1, name = 'BG_Population')
msp[, BG_PopDensity := BG_Population / units::set_units(st_area(bg_geom), km^2)]
  
saveRDS(msp, file.path(ecrvPath, "Extracts/msp_ecrv_feature_acs.rds"))


##================== Enrich with EPA EnviroAtlas Data =======================##
##  
##  Use supporting functions in EPA_Enrichment to augment each record in a
##  base ZTRAX extract with calculations for green/blue space and other 
##  community data from the EPA EnviroAtlas. 
##  
##===========================================================================##

# Import and merge blockgroup-level data from the EnviroAtlas community data.

epa <-  import_EPA_bg_data('MSPMN')
left_join_table(msp, epa, keys = 'blockgroup')

# Import the meter-scale land cover GeoTIFF for the study area(s)

mulcTif <- file.path(epaPath, "MSPMN_MULC_tif","MSPMN_MULC.tif")
rsgaTif <- file.path(landcoverPath, "tcma_lc_finalv1.tif")
mulc <- read_stars(rsgaTif, proxy = T)

connTif <- file.path(epaPath, "MSPMN_Conn","MSPMN_Conn.tif")
conn <- read_stars(connTif, proxy = T)

# Filter records to those jointly within the MSP MSA and the urbanized
# area extent defined by the U.S. census. Note that this will coerce
# the data.table into an SF object. 

mspua <- read_rds(file.path(epaPath,"mspua"))
msp <- filter_to_ua(msp, ua = mspua) 

# Calculate green space metric within buffer areas for selected records. These
# return a data.frame with only the TransId and calculation. 

green50 <-  msp %>% compute_raster_slice(radius = 50) %>% make_density_column
saveRDS(green50, file.path(landcoverPath, "green50.rds"))

green100 <-  msp %>% compute_raster_slice(radius = 100) %>% make_density_column(ringInner = 50)
saveRDS(green100, file.path(landcoverPath, "green100.rds"))

green250 <-  msp %>% compute_raster_slice(radius = 250) %>% make_density_column(ringInner = 100)
saveRDS(green250, "green250")

green500 <- msp %>% compute_raster_slice(radius = 500) %>% make_density_column(ringInner = 250)
saveRDS(green500, "green500")

green100_cum <- integrate_density(green50, green100, r1 = 50, r2 = 100)
green250_cum <- integrate_density(green50, green100, r1 = 50, r2 = 100) 
green500_cum <- integrate_density(green50, green100, r1 = 50, r2 = 100)


msp[, `:=` (green50 = green50,
            green100 = green100,
            green250 = green250,
            green500 = green500,
            tree50 = tree50,
            tree100 = tree100,
            tree250 = tree250,
            tree500 = tree500)]

landcoverTypes = c(6, 7, 10)

tree50 <-  msp %>% compute_raster_slice(radius = 50) %>% make_density_column(covertypes = landcoverTypes)
saveRDS(green50, file.path(landcoverPath, "tree50.rds"))

tree100 <-  msp %>% compute_raster_slice(radius = 100) %>% make_density_column(ringInner = 50, covertypes = landcoverTypes)
saveRDS(green100, file.path(landcoverPath, "tree100.rds"))

tree250 <-  msp %>% compute_raster_slice(radius = 250) %>% make_density_column(ringInner = 100, covertypes = landcoverTypes)
saveRDS(green250, "tree250")

tree500 <- msp %>% compute_raster_slice(radius = 500) %>% make_density_column(ringInner = 250, covertypes = landcoverTypes)
saveRDS(green500, "tree500")

saveRDS(msp, file.path(ecrvPath, "Extracts/msp_ecrv_feature_acs_lc.rds"))
