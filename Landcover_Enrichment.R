##########==========##########==========##########==========##########==========
## Name: ZTRAX EPA Enrichment
## Author: Jonathan Lamb
## Created: Oct 2022
##
## Purpose: This set of functions supports merging a base ZTRAX data extract
## with measures of green space based on high resolution land cover data from 
## EPA EnviroAtlas. The functions below depend on two input files in the 
## working directory with the below default object names:
##
##   mulc = A stars_proxy reference to the meter-scale urban land cover (MULC)
##          GeoTIFF for the study area, generated for ~30 metro areas by the EPA,
##          e.g. https://gaftp.epa.gov/epadatacommons/ORD/EnviroAtlas/MSPMN_MULC_tif.zip
##        
##  mspua = TIGRIS shape file for the urbanized area (UA) of the study area.
##          MULC coverage appears to be limited to the UA extent rather than
##          all counties that comprise the MSA, so we use this to filter obs
##          outside the land cover data.
##
##  The EPA MULC tiff uses a slightly simplified set of NLCD cover codes.
##  To calculate green space. metrics, pixels belonging to one or more 
##  codes are included:
##
## 00 = 'Unclassified', 
## 10 = 'Water',
## 20 = 'Impervious Surface',
## 30 = 'Soil and Barren',
## 40 = 'Trees and Forest',
## 70 = 'Grass and Herbaceous',
## 80 = 'Agriculture',
## 91 = 'Woody Wetland',
## 92 = 'Emergent Wetland'
##
##  The 2015 RSGA 1-meter land cover file uses an almost identical set of
##  classifications These are stored as factors rather than numerical. 
##
## 01 = Grass/Shrub
## 02 = Bare Soil  
## 03 = Buildings
## 04 = Roads/Paved Surfaces
## 05 = Lakes/Ponds
## 06 = Deciduous Tree Canopy
## 07 = Coniferous Tree Canopy
## 08 = Agriculture 
## 09 = Emergent Wetland
## 10 = Forested/Shrub Wetland
## 11 = River 
## 12 = Extraction

##======================== Dependencies =====================================##

library(stars)
library(parallel)
library(furrr)

##======================== Helper Functions =================================##

# MULC coverage is limited to the intersection of MSA counties and  the 
# urbanized area (UA) as defined by the U.S. census, so this function 
# filters observations to the TIGER shape file for the study area UA, and 
# returns it as a simple features (sf) object.

filter_to_ua <- function(ztransData, ua = mspua){
  
  message("Filtering to urbanized area...")
  
  sites <- get_sites(transData)
  inUA <- st_contains(ua, sites) %>% unlist()
  sites <- sites[inUA, ]
  
  return(sites)
  
}

# Generates buffer geometry around a ZTRAX simple features object
# with the specified radius, and returns a sf object with only the
# buffer and TransId for later re-joining. 

buffer_points <- function(transDataSF, radius = 500){


  message("Generating buffers...")

  buffer <- transDataSF[, c('ID', 'latlong')] %>%
    st_buffer(dist = radius)

  return(buffer)

}

make_density_cols <- function(bands = c(50,100,250,500), 
                              coverTypes = c(1, 6, 7, 9, 10)) {
  
  Reduce(x = bands,
         init = 0,
         f = make_density_column())

  
}

# Returns a data.frame with a column of calculated measure of green space
# density within a geometric buffer around each observation. Because the 
# buffer is uniform for all observations, we create a single raster 
# mask to use rather than intersect polygonal geometry for every obs. This
# function attempts to make use of parallel processing through furrr, which
# may have inconsistent results on different OS environments.

make_density_column <- function(latlongs, 
                                stars = mulc,
                                radius = 500,
                                ringInner = 0,
                                covertypes = c(1, 6, 7, 9, 10)){
  
  
  buffer <- compute_raster_slice(latlongs, stars, radius)
  dist <- buffer[[1]]$nXSize / 2
  mask <- make_mask_ring(ringInner, dist)
  name <- paste0('green',dist)
  
  message("Calculating green metric for each transaction...")
  plan('multisession', workers = detectCores() - 1)
  d <- future_map(buffer,
                  possibly(compute_local_density, otherwise = NA),
                  covertypes = covertypes,
                  mask = mask,
                  .progress = TRUE) %>% unlist()
  return(d)
  
}

# Function to calculate greenness of a single buffer area.
# Because the MULC is very large (on the order of 5 billion pixels), this
# references a stars_proxy object, reading in only values matching the bounding
# box of the buffer an then using a pre-generated raster mask to filter the
# pixels in a circular area. 

compute_local_density <- function(io,
                                  tif = rsgaTif,
                                  covertypes = c(1, 6, 7, 9, 10),
                                  mask = NULL){
  
  
  landcover <- (((read_stars(tif, RasterIO = io)) %>%
                   st_as_stars()) %in% covertypes * 1)[[1]] %>% as.matrix()
  
  if (!is.null(mask)) {
    landcover <- landcover * mask
  }
  
  green <- sum(landcover)
  totalArea = if(!is.null(mask)){sum(mask)} 
  else{length(landcover)}
  
  greenness <- sum(landcover) / totalArea
  
  return(greenness)
  
}

# Returns the cropped raster covering the buffer area and a
# filtered raster of pixels matching supplied codes.

get_raster <- function(area, 
                       stars = mulc, 
                       tif = rsgaTif,
                       covertypes = c(1, 6, 7, 9, 10),
                       mask = NULL){
  
  area <- st_geometry(area)
  bound <- attr(stars, "dimensions")
  bb <- attr(area, "bbox")
  io <- list(nXOff = bb$xmin - bound$x$offset,
             nYOff = bound$y$offset - bb$ymax,
             nXSize = bb$xmax - bb$xmin,
             nYSize = bb$ymax - bb$ymin)
  
  buffer <- ((read_stars(tif, RasterIO = io)) %>%
               st_as_stars())
  
  green <- (buffer %in% covertypes * 1)
  
  return(list(buffer, green))
  
}

# Computes rectangular coordinates to read from a proxy raster
# that fits the specified radius around a point. This ends up being
# much faster than generating a circular buffer.

compute_raster_slice <- function(transDataSF, 
                                 stars = mulc,
                                 radius = 500){
  
  message("Computing bounds to read from landcover raster...")
  extent <- attr(stars, "dimensions")
  
  slices <- lapply(transDataSF$latlong, function(i) {
  
    x <- i[1]
    y <- i[2]
  
    io <- list(nXOff = x - (radius) - extent$x$offset,
               nYOff = extent$y$offset - (y + (radius)),
               nXSize = radius * 2,
               nYSize = radius * 2)
  })
  
  return(slices)
  
}



# Returns a circular raster mask at 1m resolution to recycle for fast 
# filtering of large raster data rather than repeatedly calculate 
# polygonal intersections.

make_mask <- function(radius){
  
  mask <- (st_point(c(0,0)) %>% 
             st_buffer(dist = radius) %>% 
             st_sfc() %>% 
             st_as_stars(nx = radius * 2, ny = radius * 2, crs = 4326))$values %>%
    as.matrix()
  
  return(mask)
}

# Returns a ring with inner and outer radii r1 and r2 to construct concentric
# bands around a point.

make_mask_ring <- function(r1, r2){

  inner <- st_point(c(0,0)) %>% 
    st_buffer(dist = r1) 

  outer <- st_point(c(0,0)) %>% 
    st_buffer(dist = r2)

  ring <- (st_difference(outer, inner) %>% st_combine %>%
             st_as_stars(nx = r2 * 2, ny = r2 * 2, crs = 4326))$values %>%
    as.matrix()

  return(ring)
}

# Returns the cumulative density of a ring and the nested area. 

integrate_density <- function(d1, d2, r1, r2){
  
  a1 <- pi * r1^2
  a2 <- pi * r2^2 - a1
  w1 <- a1 / (a1 + a2)
  w2 <- a2 / (a1 + a2)
  
  d <- d1 * w1 + d2 * w2
  
  return(d)
  
}

# Imports and combines selected tables from the pre-calculated EPA EnviroAtlas 
# blockgrou-level community data. Adding tables requires a corresponding cleaning
# rule that at the very least renames 'GEOID' to 'blockgroup'

import_EPA_bg_data <- function(msa){
  
  data <- data.table(file = c(paste0(msa, '_LCSum.csv'),
                              paste0(msa, '_RB_LC.csv'),
                              paste0(msa, '_EduLowGS.csv')),
                     rule = c('proc_epa_green',
                              'proc_epa_water',
                              'proc_epa_edu'))
  
  import <- map(1:nrow(data), 
                function(i) {fread(file.path(epaPath,
                                             paste0(msa,'_metrics_Mar2018_CSV_Shapes'),
                                             data[i]$file)) %>%
                    apply_rule(data[i]$rule)
                }
  ) %>% reduce(cbind) %>%
    .[, .SD, .SDcols = unique(names(.))]
  
  return(import)
  
}