##########==========##########==========##########==========##########==========
## Name: ZTRAX Geo Enrichment
## Author: Jonathan Lamb
## Created: Nov 2022
##
## Purpose: This set of functions supports merging base transaction data
## with geographic vector features and boundaries from OpenStreetMaps and 
## other GIS sources. 
##

library(osmextract)
library(osmdata)
library(stars)
library(leaflet)
library(leafem)
library(data.table)

##========================== import pbf archives ============================##

# Tag categories to read in as columns from imported pbf layers

osmtags <- c("amenity", 
             "natural",
             "water",
             "cuisine",
             "historic",
             "playground",
             "shop", 
             "public_transport", 
             "sport")

# Loads OSM pbf for the specified state, layers, and years
# as panel data in "long" format, from archives provided by
# https://download.geofabrik.de/. Assumes default file naming 
# convention except for renaming "latest" to the current year.

import_osm <- function(osmdir = osmPath,
                       state = "minnesota",
                       layers = c('points','multipolygons', 'lines'),
                       tags = osmtags,
                       years = "2022",
                       ua = readRDS(file.path(epaPath,"mspua"))) {
 
  map_dfr(years,
          function(year){
            import_osm_year(osmdir, state, layers, tags, year, ua)
          })
  
}

# Loads OSM data for a single state and year with specified layers,
# returned as a single dataframe

import_osm_year <- function(osmdir,
                            state,
                            layers,
                            tags,
                            year,
                            ua) {
  
  filename <-  paste0(state, "-", paste0(substr(year, 3, 4), "0101"), ".osm.pbf")
  path <- file.path(osmdir, filename)
 
  map_dfr(layers,
          function(layer){
            oe_read(path,
                    layer = layer,
                    extra_tags = tags)
          }) %>% 
    mutate(Year = year) %>%
    st_transform(crs = 26915) %>%
    filter_osm_geo(ua)
  
}

## ==================== Filter to study area ======================= ##

# Subset to features within the urbanized area as defined by the U.S. Census, 
# to match coverage of the EnviroAtlas MULC (not necessary if using the 
# 2015 MULC from RSGA.
#
# Uses workaround for invalid geometry when filtering geography. Attempted
# automatic geometry fix st_make_valid() does not seem to resolve issues.

filter_osm_geo <- function(osm, ua) {

  sf::sf_use_s2(FALSE)   # Workaround for invalid geometry
  osm <- osm[st_intersects(ua, osm) %>% unlist(), ]
  
}

## ==================== Variables of interest ======================= ##

## Variables of interest as instruments for social and economic capital,
## organized by tag and value. These may overlap. Wrapped in a function
## to easily support externalization.

#   amenities                # Social   Economic   Note
#     'place_of_worship',    #    .
#     'cafe',                #    .
#     'library',             #    .
#     'public_bookcase',     #    .
#     'community_centre',    #    .
#     'social_centre',       #    .
#     'school',              #    .       .
#     'college',             #    .       .
#     'university',          #    .       .
#     'townhall',            #    .       .      Could represent local CBD
#     'childcare'            #            . 
#   
#   tourism 
#     'museum',              #    .
#     'gallery',             #    .
#     'artwork'              #    .
#   
#   landuse
#     'commercial',          #           Proximity to work and consumption
#     'retail',              #           .
#     'office'               #           .
#   
#   natural
#     'water'                #           For water buffer area
#   
#   leisure 
#     'sports_centre',       #    .      
#     'park'                 #    .      
#   
#   building
#     'church'               #    .     Some missing from place_of_worship
# 
#   Sport - Any              #    .       May have too many features


## ==================== OSM feature subsets ======================= ##


# Returns a named list of feature subsets for simplified 
# geometric operations and referencing.

create_feature_sets <- function(osm){ 
  
  list(
    
    neighborhoods = osm[osm$place %in% 'neighbourhood',],
    
    economic = osm[osm$landuse %in% c('commercial',
                                     'office', 
                                     'retail') |
                     osm$building %in% c('commercial',
                                         'retail',
                                         'office',
                                         'service') |
                     !is.na(osm$shop),],
    
    industrial = osm[osm$landuse %in% c('industrial'),],
    
    townhalls = osm[osm$amenity %in% "townhall",],
    
    water = osm[osm$natural %in% c('water'),],
    
    highway = osm[osm$highway %in% c('motorway','trunk'),],
    
    thirdplaces = osm[osm$amenity %in% c('place_of_worship',
                                           'cafe',
                                           'library',
                                           'community_centre',
                                           'school') |
                        osm$tourism %in% c('museum','gallery') |
                        osm$leisure %in% c('sports_centre') |
                        osm$building %in% c('church'),],
    
    transit = osm[osm$public_transport %in% c('platform', 'station'),],
    
    park = osm[osm$leisure %in% c('park'),],
    
    car = osm[osm$highway %in% c('motorway_link', 'trunk_link'),]
    
  )
  
}

##====================== ZTRAX handling ===========================##

# Returns a simple features dataframe of just site locations
# for geometric operations, translating lat/long as text columns
# or existing simple features.

get_sites <- function(transData) {
  
  if(is.null(transData$shape)){
    
    latlong <- c("PropertyAddressLongitude", 
                 "PropertyAddressLatitude")
    
    sites <- st_as_sf(transData[, ..latlong], 
                      coords = latlong,
                      remove = TRUE,
                      crs = 4326) %>%
      st_transform(crs = 26915)
  } 
  
  else{
    sites <- transData$shape
  }
  
  return(sites)
  
}

## ================ Map to census geometry ======================= ##

# For a data.table containing lat/lot or spatial geometry, 

add_block_groups <- function(dt, 
                             state = 27, 
                             counties = unique(dt$County), 
                             vintage = 2010) {
  
  bg <- (tigris::block_groups(state, counties, year = vintage) %>%
    st_transform(crs = 26915))[, c('GEOID10', 'geometry')]
  
  sites <- get_sites(dt)
  n <- st_nearest_feature(sites, bg)
  dt[, blockgroup := bg$GEOID10[n]]
  dt[, bg_geom := bg$geometry[n]]
  set_units(dt$bg_geom, km^2)
}

## ================ Distance to features  ======================= ##

add_feature_distance <- function(transData, feature, colname = ''){
  
  message(paste("Calculating distance to", colname))
  colname <- paste0(colname, '_d')
  sites <- get_sites(transData)
  feature <- st_union(feature)
  transData[, (colname) := as.numeric(st_distance(sites, feature)) + 1]
  
}

## ========================== Feature count =========================== ##

add_feature_count <- function(transData, 
                           features,
                           d = 1000,
                           name = NULL) {
  
  if (is.null(name)){
    name = features
  }
  colname = name

  sites <- get_sites(transData) %>% st_buffer(dist = d)
  walkable <- st_contains(sites, features) %>%
    lapply(length) %>% unlist()
  
  transData[, (name) := walkable]
  
}

## ==================== Construct feature buffer ======================= ##

# Generic function generates a buffer around a specified subset of 
# features and adds a logical vector to a property transaction table 
# indicating if the site is in the buffer.

add_feature_buffer <- function(transData, features, name = 'buffer', d) {
  
  colname <- paste0(name, d)
  buffer <- st_buffer(features, dist = d) %>% st_union
  
  sites <- get_sites(transData) %>% 
    st_intersects(buffer, .) %>%
    unlist
  
  transData[, (colname) := (.I %in% sites) * 1]
  
}

# Shortcuts to add a column to indicate transactions close to specific
# types of geographic features

add_water_buffer <- function(transData, features = osm, d) {
  
  message("Adding indicator for sites in buffer area of bodies of water...")
  water <- features[features$natural %in% 'water',]$geometry
  add_feature_buffer(transData, water, name = 'water', d)
  
}

add_highway_buffer <- function(transData, features = osm, d) {
  
  message("Adding indicator for sites in buffer area of major highways...")
  highway <- features[features$highway %in% c('motorway','trunk'),]$geometry
  add_feature_buffer(transData, highway, name = 'highway', d)
  
}

## ==================== Add School Boundaries ======================= ##

add_school_areas <- function(transData) {
  
  attendance <- read_school_area()
  sites <- get_sites(transData)
  n <- st_nearest_feature(sites, attendance)
  transData[, highschool := attendance$highshoolArea[n]]
  
}

read_school_area <- function() {
  
  attendance <- st_read(file.path(geoPath, 
                                  'gpkg_bdry_school_attendance_areas',
                                  'bdry_school_attendance_areas.gpkg')) %>%
    st_set_crs(26915) %>%
    group_by(HIGH_NAME) %>% 
    summarize(geom = st_union(geom))
  
  colnames(attendance) <- c('highshoolArea', 'geom')
  
  return(attendance)
  
}

## ==================== Augment Open Parcel Data ========================= ##


import_mn_parcels <- function() {
  
  path <- file.path(geoPath, 'gpkg_plan_parcels_open', 'plan_parcels_open.gpkg')
  
  parcels <- st_read(path) %>% 
    setDT() %>% 
    apply_rule('proc_mn_parcels')
  
}

proc_mn_parcels <- function(dt) {
  
  CBD <- st_sfc(st_point(c(-93.258133, 44.986656)), 
                crs = 4326) %>% 
    st_transform(crs = 26915)
  
  old <- c('co_name',
           'county_pin',
           'anumber',
           'st_name',
           'owner_name',
           'acres_poly',
           'home_style',
           'fin_sq_ft',
           'year_built',
           'sale_date',
           'sale_value',
           'school_dst',
           'wshd_dst')
  
  new <- c('County',
           'ParcelID',
           'ParcelStreetNumber',
           'ParcelStreetName',
           'OwnerName',
           'ParcelSize',
           'HomeStyle',
           'FinSqFt',
           'ParcelYearBuilt',
           'LastParcelSaleDate',
           'LastParcelSalePrice',
           'SchoolDst',
           'Watershed')
  
  keep <- c('ParcelID',
            'ParcelAddress',
            new,
            'shape')
  
  dt %>% 
    
    {
      message("Filtering parcels to study area...")
      .[substr(state_pin,0,5) %in% studyArea] 
    } %>%
    
    {
      message("Renaming columns... ")
      setnames(., old, new)
    } %>%
    
    {
      message("Dropping parcels with multiple entries...")
      .[!duplicated(ParcelID)]
    } %>%
    
    {
      message("Concatenating address fields...")
      .[, ParcelAddress := paste(ParcelStreetNumber,
                                 ParcelStreetName,
                                 st_pre_dir,
                                 st_pos_typ,
                                 st_pos_dir) %>%
          gsub("NA","", .) %>%
          gsub("\\s+", " ", .) %>%
          abbreviate_street()]
    } %>%
    
    {
      message("Creating lat long for geo manipulations...")
      .[, latlong := st_centroid(shape)]
    } %>%
    
    {
      message("Calculating distance to Minneapolis center...")
      .[, CBD_d := (st_distance(latlong, CBD) / 1000)]
    }
    
    {
      message("Subsetting columns...")
      .[, ..keep]
    }
  
}

# Helper function to normalize addresses following USPS abbreviations,
# for matching across datasets.

abbreviate_street <- function(s, codes = file.path(sourcePath, 
                                                "streetAbbr.csv")) {
  s <- toupper(s)
  abbr <- read.csv(codes) %>% setDT
  
  for (i in 1:nrow(abbr)){
    s <- gsub(abbr$Suffix[i], abbr$Abbreviation[i], s)
  }
  
  return(s)
}

## ======================== Test visualization ========================= ##

# leaflet(options = leafletOptions(preferCanvas = TRUE)) %>%
#   addProviderTiles(providers$CartoDB.DarkMatterNoLabels) %>%
#   addFeatures(st_as_sf(x) %>% st_transform(crs = 4326),
#               color = 'blue')