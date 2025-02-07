library(janitor)
library(glue)
library(readr)
library(hydrofabric)
library(hfsubsetR)
library(maps)
library(mapdata)
library(rnaturalearth)
library(rnaturalearthdata)
library(sf)
library(data.table)

# Hydrofabric-specific processing used in RaFTS

read_hfab_layers <- function(path_gpkg, layers=NULL){
  #' @title Read the hydrofabric file gpkg for each layer
  #' @param path_gpkg The filepath of the geopackage for a hydrofabric domain
  #' @param layers The hydrofabric layers of interest. Default NULL means all.
  #' Other options include any of the following inside
  #' `c("divides", "flowpaths", "network", "nexus")`
  #' @seealso proc_attr_hf
  #' @export

  hfab_ls <- list()
  if (fileext == 'gpkg') {
    # Define layers
    if(is.null(layers)){
      layers <- sf::st_layers(dsn = path_gpkg)$name
    }
    for (lyr in layers){
      #hfab_ls[[lyr]] <- sf::read_sf(fp_cat,layer=lyr)
      hfab_ls[[lyr]] <- proc.attr.hydfab::read_hfab_layer(path_gpkg,layer=lyr)
    }
  } else {
    stop("# TODO add in the type of hydrofabric file to read based on extension")
  }
 return(hfab_ls)
}


read_hfab_layer <- function(path_gpkg, layer){
  #' @title Read a hydrofabric layer
  #' @details Performs a vpu value correction in case of AK labeled as 'hi'.
  #' Designed during hydrofabric v2.2
  #' @param path_gpkg The filepath of the geopackage for a hydrofabric domain
  #' @param layer The layer of interest. Options include
  #' 'divides','flowpaths','network','nexus'
  #' @export

  hfab <- sf::st_read(path_gpkg,layer = layer) %>% base::suppressWarnings()

  gpkg_fname <- base::basename(path_gpkg)
  if(base::grepl("ak",gpkg_fname) &&
     'vpu' %in% base::names(hfab)){
    if(!base::all('ak' %in% hfab$vpu)){
      warning(glue::glue("Expecting the {gpkg_fname} vpu column to be 'ak'.
                         EDITING the vpu!! "))
      hfab$vpu <- 'ak'
    }
  }
  return(hfab)
}


map_hfab_oconus_sources_wrap <- function( dt_need_hf, hfab_srce_map,
                                          col_lat = 'latitude',
                                          col_lon= 'longitude'){
  #' @title Match the hydrofabric OCONUS geopackage locations to missing hydrofabric data
  #' @param dt_need_hf data.table of locations that need a hydrofabric id. Must include lat/lon cols
  #' @param hfab_srce_map OCONUS postal code ids (colname 'domain') mapped to paths to respective gpkg files
  #' @param col_lat The latitude column name inside `dt_need_hf`
  #' @param col_lon The longitude column name inside `dt_need_hf`
  #' @seealso `retr_state_terr_postal()`
  #' @export

  expected_colnames <- c(col_lat, col_lon)
  if (!base::all(expected_colnames %in% base::names(dt_need_hf))){
    miss_cols <- expected_colnames[base::which(!expected_colnames %in%
                                                 base::names(dt_need_hf))]
    stop(glue::glue("dt_need_hf missing expected column names:
                    {paste0(miss_cols, collapse = ',')}"))
  }

  if(base::nrow(dt_need_hf)>0){
    post_ids <- list()
    for(i in base::nrow(dt_need_hf)){
      lat <- dt_need_hf[[col_lat]][i]
      lon <- dt_need_hf[[col_lon]][i]
      if(base::any(base::is.na(base::c(lat,lon)))){
        warning(glue::glue("Lat/Lon unavailable for {gage_id}"))
      } else {
        # Figure out which geopackage to use based on lat/lon
        postal_id <- proc.attr.hydfab::retr_state_terr_postal(lat=lat,lon=lon)
        post_ids[[i]] <- postal_id
      }
    }
    post_ids <- base::lapply(1:base::nrow(dt_need_hf), function(i)
      proc.attr.hydfab::retr_state_terr_postal(lat=dt_need_hf$latitude[i],
                                               lon=dt_need_hf$longitude[i])) %>%
      base::unlist()

    dt_need_hf$domain <- post_ids

    dt_need_hf <- base::merge(dt_need_hf, hfab_srce_map, by = "domain",
                              all.x=TRUE,all.y=FALSE,sort=FALSE)
  }
  return(dt_need_hf)
}

# Function to get state or territory name
retr_state_terr_postal <- function(lat, lon) {
  #' @title Retrieve postal id of state or territory
  #' @param lat latitude in epsg 4326
  #' @param lon longitude in epsg 4326
  #' @export

  # Load the natural earth data for states and territories
  us_states <- rnaturalearth::ne_states(country = "United States of America", returnclass = "sf")

  # Create a point from the given latitude and longitude
  point <- sf::st_sfc(sf::st_point(c(lon, lat)), crs = sf::st_crs(us_states))

  # Check which state or territory the point belongs to
  state_or_territory <- us_states[sf::st_intersects(point, us_states, sparse = FALSE),]

  # Double check with a buffer (e.g. 21.48203, -157.84589 in HI but not recognized )
  if(nrow(state_or_territory) == 0){
    buffer <- st_buffer(point, dist = 1000)
    state_or_territory <- us_states[sf::st_intersects(buffer, us_states, sparse = FALSE),]
  }

  # Otherwise check to see if this is a US territory
  if(nrow(state_or_territory)==0){
    world <- rnaturalearth::ne_countries(scale='medium', returnclass = "sf")
    point <- st_sfc(st_point(c(lon, lat)), crs = st_crs(world))
    state_or_territory <- world[st_intersects(point, world, sparse = FALSE),]
  }
  # Retrieve the postal ID that we use in mapping to gpkg files
  postal_id <- state_or_territory$postal
  if(length(postal_id) == 0){
    postal_id <- NA
  }

  # Return the postal ID of the state or territory
  return(postal_id)
}

custom_hfab_id <- function(df){
  #' @title Build a custom hydrofabric id that is unique to place
  #' @param df The dataframe for a specific location corresponding to the
  #' hydrofabric 'network'
  #' @export
  cstm_id <- paste0(df$vpu,"-",df$id) %>% unique()
  if(length(cstm_id)>1){
    stop("More than one custom id for hydrofabric. This shouldn't happen.")
  }

  return(cstm_id)
}

# ------------------

retr_hfab_id_usgs_gage <- function(gage_id,ntwk){
  #' @title NEEDS MORE WORK Retrieve the hydrofabric ID from the network via the USGS gage_id
  #' @details Must provide the appropriate network! E.g. specific to domain,
  #' such as Alaska
  #' @param gage_id The USGS identifier
  #' @param ntwk data.frame network layer from the appropriate hydrofabric
  # TODO FIX THIS!! hfsubsetR::find_origin returns the same thing for different gages:
  #.   "15493000" "15056210" "15294005" when ntwk comes from ak_nextgen.gpkg

  if (!base::grepl("USGS", gage_id) && !base::grepl("GAGE",gage_id)){
    gid_char <- glue::glue('USGS-{gage_id}') # Create format for hl_uri
  } else { # already formatted in a form that works for hl_uri
    gid_char <- gage_id
  }

  # -------- Grab the hydrofabric ids using the hl_uri ---------
  hf_simple_ids <- hfsubsetR::find_origin(network=ntwk, id = gid_char,type = "hl_uri")
  sub_loc <- ntwk[ntwk$id==hf_simple_ids$id,] %>% unique()

  hf_id <- proc.attr.hydfab::custom_hfab_id(sub_loc)

  return(hf_id)
}
# ------ Grab the hydrofabric ids via a lat/lon search -------
retr_hfab_id_coords <- function(path_gpkg, ntwk, epsg_domn,lon,lat,
                                epsg_coords = 4326){
  #' @title Retrieve hydrofabric IDs via coordinates
  #' @details Assumes the hydrofabric domain (e.g. AK, HI) is already determined
  #' via the path_gpkg
  #' @param path_gpkg Path to the hydrofabric geopackage file
  #' @param ntwk The network hydrofabric layer
  #' @param epsg_domn The EPSG of the hydrofabric domain of interest
  #' @param lon Longitude
  #' @param lat Latitude
  #' @param epsg_coords The EPSG code corresponding to `lat` and `lon`. Default 4326
  #' @export
  #'
  # Identify the point of interest, converted into the hydrofabric domain's CRS
  if(!is.na(epsg_domn)){
    pt <-  sf::st_transform(sf::st_sfc(sf::st_point(base::c(lon,lat)),
                                       crs = epsg_coords),epsg_domn)
  } else {
    stop("PROBLEM: the epsg_domn has not been defined.")
  }


  # Extract flowpath_id for intersecting divide
  origin <-  sf::read_sf(path_gpkg,layer= "divides",
                         wkt_filter = sf::st_as_text(pt))$id

  if(base::length(origin)==0){
    warning(glue::glue("This lon/lat does not exist in the hydrofabric!
                             {paste0(lon,',',lat)} for gage_id {gage_id}"))
    hf_id <- NA
  } else {
    # Subset network based on the origin id:
    sub_ntwk <- ntwk[ntwk$id == origin,]
    # Generate the customized unique identifier
    hf_id <- proc.attr.hydfab::custom_hfab_id(sub_ntwk)
  }
  return(hf_id)
}

