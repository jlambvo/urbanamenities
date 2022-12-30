# urbanamenities

This is a set of programs used in my dissertation research to construct a residential real estate transaction database for hedonic price analysis of different kinds of urban amenities, with a focus on the Minneapolis-St. Paul metro area. This workflow harmonizes data from several sources with national coverage for the U.S. and is intended to be easily repurposed to other cities, with the exception of a MN-specific workflow that utilizes high quality public data unique to that state:

1. **Zillow ZTRAX** is used to create the foundational transaction and assessment data. This dataset aggregates county-level public records and other proprietary datasets covering most of the U.S. from the 1990s onward. Access was free for academic research but the program was unfortunately terminated in 2022 and so interested researchers will need previous license and archived dataset to use it. 
3. **OpenStreetMaps (OSM)** is a crowdsourced geospatial feature dataset with vector shape data and classifications covering much of the globe. This script uses bulk extracts downloaded from https://download.geofabrik.de/
4. **U.S. Census & ACS** provides neighborhood demographic characteristics at a blockgroup level. Access is open but requires users to request a unique API key. 
5. **EPA EnviroAtlas** is a database of environmental and community healthy data constructed by the EPA, including 1m-resolution landcover data and derivative data products for about 30 metro areas.
6. **HMDA Mortgage Data** is a publically accessible archive of national mortage applicant characteristics that must be reported by lenders to the Consumer Financial Protection Bureau. 
7. **Electronic Certificates of Real-Estate Value (eCRV) (MN Only)** are required to be filed with the MN Dept. of Revenue for all property transactions over $3,000, and are a publicly accessible alternative to ZTRAX for this state.
8. **Minneapolis-St. Paul Metropolitan Council (MN Only)** supplies a variety of data unique to the Twin Cities including parcel boundaries, tax information, transit data, and school attendance boundaries for the 7-county metro area. 
9. **University of Minnesota Remote Sensing and Geospatial Analysis Laboratory (MN Only)** provides a refined version of the 1m-resolution landcover data used by the EPA EnviroAtlas and with slightly expanded spatial coverage. Similar products may be found in other regions. 

Functions for constructing the combined dataset are organized into multiple source files and externalized inputs. The eventual goal is to convert this to a package that can be easily used on any system. In the meantime, interested users will need to take care that paths and folder structures are updated to reflect the local environment. The source is organized as follows:

1. **Hedonic_Dataset_Builder.R** is the main script used to create a transaction data extract and augment it with other data. This loads all necessary source files and dependencies, and defines paths to data files. The aim is to provide a single function call for most steps. 
2. **ZTRAX_Extraction.R** provides functions to create a data table of transaction events merged with assessment data that matches most closely by year. ZTRAX data is distributed as numerous tables that make up a relational database, broken across multiple vintages, divided between transaction and assessor data. This script pulls variables from tables specied in **ZTRAX_include.xlsx**, cleans, and merges them into a single data.table (or optionally as a list of separate transaction and assessment data.tables). 
3. **Cleaning_Functions.R** supports an automated cleaning pipeline for extracted data. Each function applies filtering, recoding, reshaping, transformations, etc. appropriate to the provided table. These are intended to be called by name as an argument to the function "apply_rule()" which I use to iteratively clean each ZTRAX table before merging into the final dataset, and to provide feedback via the console. For example, records are filtered  wherever possible to the county FIPS codes provided in **ZTRAX_sutdyarea.xlsx** to minimize the size of tables being combined. 
4. **Census Enrichment.R** includes functions to retrieve neighborhood characteristics from ACS 5-year estimates, matched to records by year and blockgroup. Currently supports values expressed as a percentage of total population for a range of rows (i.e. a range of education levels) or median of specified rows (typically for a single row value). 
5. **Landcover_Enrichment.R** computes density of landcover matching supplied codes in a buffer around each transaction event from a high-resolution landcover file. The 1m-resolution raster used in this analysis is too large to fit in memory, so segments are iteratively read from a virtualized raster. This is configured to use multiple cores via purrr.
6. **Geo_Enrichment.R** imports and filters an OpenStreetMaps extract (assumed to be in .pbf format), create subsets of features by tag, and provides functions to compute measures for each transaction such as distance to or count of feature types like highways, bodies of water, etc. 
7. **HMDA_Encrichment.R** imports and cleans HMDA records for the study area defined in **ZTRAX_studyarea.xlsx** and provides a function that attempts to match observations to ZTRAX transaction events based on loan size, lender name, and census tract, based on a procedure develope by Billings (2019). 
8. **eCRV_Extraction.R** provides functions for an alternative to ZTRAX to construct transaction events and assessor information from eCRV records archived by the MN Dept. of Revenue. This provides more exhaustive and consisteny coverage than ZTRAX but is available only to the end of 2014. Additional functions import and match eCRV records to parcel information and geographic boundaries suppled by the MSP Met Council. 
