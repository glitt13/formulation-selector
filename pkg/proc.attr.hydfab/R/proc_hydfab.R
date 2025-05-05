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
library(yaml)
library(dplyr)
library(tidyr)

# Standardize data to featureID & featureSource
std_feat_id <- function(df, name_featureSource = c("COMID","custom_hfuid")[1],
                        col_featureID = NULL, vals_featureID = NULL){
  #' @title Generate the standardized format of featureID and featureSource
  #' @param df The data.frame of interest containing an identifier
  #' @param name_featureSource The expected name of the feature source. Default "COMID" expected for CONUS, but OCONUS will use "custom_hfuid"
  #' @param col_featureID The column name inside `df` containing featureID of interest. Must provide if `vals_featureID` empty.
  #' @param vals_featureID The unique identifiers to populate featureID of interest. Must provide if `col_featureID` empty.
  #' @seealso \link[proc.attr.hydfab]{retr_attr_new}
  #' @export

  # Check expected featureSource names
  allowed_names <- c("COMID","custom_hfuid")
  if(!name_featureSource %in% allowed_names){
    str_allowed <- base::paste0(allowed_names,collapse='\n')
    warning(paste0("The name_featureSource {name_featureSource} is not in",
            "the list of expected featureSource names: ", str_allowed,
             "STRONGLY RECONSIDER THIS CHOICE!!"))
  }
  if(base::any(base::grepl("featureID",base::names(df)))){
    # nothing to do here, the featureID column already exists
    print("TODO Consider if logic should change in std_feat_id around the featureID column assignment")
  } else if(!base::is.null(col_featureID)){
    df$featureID <- df[[col_featureID]]
  } else if (!base::is.null(vals_featureID)){
    df$featureID <- vals_featureID
  } else {
    stop("Must provide either col_featureID or vals_featureID")
  }
  # Add in the featureSource for non-NA featureID columns
  if(!"featureSource" %in% base::names(df) && base::nrow(df)>0){
    df$featureSource <- NA
  }
  df$featureSource[base::which(!base::is.na(df$featureID))] <- name_featureSource

  return(df)
}


parse_hfab_oconus_config <- function(path_oconus_config){
  #' @title Parse the OCONUS hydrofabric metadata
  #' @details Reads the hydrofabric OCONUS mappings of domain-id in postal_code
  #' format and the expected CRS of each hydrofabric file as of v2.2
  #' @param path_oconus_config filepath to the yaml configuration file
  #' @export
  #'
  if(!base::file.exists(path_oconus_config)){
    stop(glue::glue("The `path_oconus_config` does not exist.
    Please reconsider how this path is defined in the attribute config file:
    {path_oconus_config}"))
  }
  cfig <- yaml::read_yaml(path_oconus_config)
  dir_base_hfab <- cfig$dir_base_hfab
  srces <- base::names(cfig)[base::grep("source_map",base::names(cfig))]


  dt_hfab_map <- data.table::rbindlist(base::lapply(srces, function(x)
                                    base::data.frame(cfig[[x]])))
  dt_hfab_map$path <- base::lapply(dt_hfab_map$path, function(x) glue::glue(x)) %>%
    base::unlist()

  if (base::formals(hfsubsetR::get_subset)$hf_version != "2.2"){
    warning("The hydrofabric has been updated.
            Check proc.attr.hydfab package file hfab_oconus_map.yaml to see if
            its mappings are still valid for the new hydrofabric version.
            Modify this if statement once confirmed.")
  }

  # Read in the 'standard' domain name and crs for oconus hydrofabric
  dir_base <- system.file("extdata",package="proc.attr.hydfab")
  path_cfig_oconus_std <- file.path(dir_base,"hfab_oconus_map.yaml")
  cfig_std <- yaml::read_yaml(path_cfig_oconus_std)
  # Process in the same order as 'sources'
  df_std_oconus_meta <- data.table::rbindlist(base::lapply(srces, function(x)
    base::data.frame(cfig_std[[x]])))

  dt_hfab_map <- base::cbind(dt_hfab_map,df_std_oconus_meta)

  if(base::any(names(dt_hfab_map) == 'crs')){
    # the `crs` column can conflict with dataframes returned by
    # proc.attr.hydfab::read_noaa_hads_sites
    dt_hfab_map <- dt_hfab_map %>% dplyr::rename(crs_hfab='crs')
  }

  return(dt_hfab_map)
}

# Hydrofabric-specific processing used in RaFTS
read_hfab_layers <- function(path_gpkg, layers=NULL){
  #' @title Read the hydrofabric file gpkg for each layer
  #' @param path_gpkg The filepath of the geopackage for a hydrofabric domain
  #' @param layers The hydrofabric layers of interest. Default NULL means all.
  #' Other options include any of the following inside
  #' `c("divides", "flowpaths", "network", "nexus")`
  #' @seealso \link[proc.attr.hydfab]{proc_attr_hf}
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
              This should be fixed with hydrofabric v3.0.
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
  #' @seealso \link[proc.attr.hydfab]{retr_state_terr_postal}
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

        warning(glue::glue("Lat/Lon unavailable for
                           {paste0(names(dt_need_hf),collapse='|')}
                           {paste0(dt_need_hf[i,],collapse='|')}"))
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
  if(base::nrow(state_or_territory) == 0){
    buffer <- sf::st_buffer(point, dist = 1000)
    state_or_territory <- us_states[sf::st_intersects(buffer, us_states, sparse = FALSE),]
  }

  # Otherwise check to see if this is a US territory
  if(base::nrow(state_or_territory)==0){
    world <- rnaturalearth::ne_countries(scale='medium', returnclass = "sf")
    point <- sf::st_sfc(sf::st_point(c(lon, lat)), crs = sf::st_crs(world))
    state_or_territory <- world[sf::st_intersects(point, world, sparse = FALSE),]
  }
  # Retrieve the postal ID that we use in mapping to gpkg files
  postal_id <- state_or_territory$postal
  if(length(postal_id) == 0){
    postal_id <- NA
  }

  # Return the postal ID of the state or territory
  return(postal_id)
}

custom_hf_id <- function(df, col_vpu = "vpu",col_id = "divide_id"){
  #' @title Build a custom hydrofabric id that is unique to place across all domains
  #' @param df The dataframe for a specific location corresponding to the
  #' @param col_vpu The column in df representing the vpu
  #' @param col_id The column in df representing the hydrofabric id
  #' hydrofabric 'network'. Default 'divide_id' e.g. cat-447 avoids the
  #' common possibility of NA values in the 'id' column
  #' @export

  # Remove NA values
  idxs_na <- base::c(base::which(base::is.na(df[[col_vpu]])),
                     base::which(base::is.na(df[[col_id]])) ) %>% base::unique()
  if(base::length(idxs_na)>0){
    df <- df[-idxs_na,]
  }

  cstm_id <- paste0(df[[col_vpu]],"-",df[[col_id]]) %>% unique()
  if(base::any(base::duplicated(cstm_id))){
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

  hf_id <- proc.attr.hydfab::custom_hf_id(sub_loc)

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
  if(!base::is.na(epsg_domn)){
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
                       {paste0(lon,',',lat)}
                       from {path_gpkg}"))
    hf_id <- NA
  } else {
    # Subset network based on the origin id:
    sub_ntwk <- ntwk[ntwk$id == origin,] %>% base::unique()
    # Generate the customized unique identifier
    hf_id <- proc.attr.hydfab::custom_hf_id(sub_ntwk)
  }
  return(hf_id)
}

retr_comids_coords <- function(df, col_lat = "latitude",
                                   col_lon="longitude",col_crs = NULL,
                                   crs=4326){
  #' @title Iterate over each coordinate in a dataframe to retrieve comid
  #' @param df Dataframe containing lat/lon columns
  #' @param col_lat The column name for the latitude data
  #' @param col_lon The column name for the longitude data
  #' @param col_crs Default NULL, column name for the CRS. If NULL, `crs` arg
  #' will be used.
  #' @param crs The CRS to use when `col_crs` is NULL. Default 4326.
  #' @export
  # TODO update once discover_nhdplus_id() can handle batch processing
  # https://github.com/DOI-USGS/nhdplusTools/issues/417
  # The limit is 400 hits per hour for a single IP address

  ls_comids <- list()
  for(idx in 1:base::nrow(df)){
    if(!base::is.null(col_crs)){
      crs_val <- df[[col_crs]][idx]
    } else {
      crs_val <- crs
    }

    # Create the sfc object with CRS
    geom_pt <- sf::st_as_sf(df[idx,], coords = c('longitude', 'latitude'),
                            crs = crs_val)

    ls_comids[[idx]] <- nhdplusTools::discover_nhdplus_id(geom_pt)

    if(base::length(ls_comids[[idx]])==0){
      # Assign NA to keep dimensionality consistent
      ls_comids[[idx]] <- NA
    }
  }
  comids <- base::unlist(ls_comids)
  return(comids)
}

retr_hfab_id_wrap <- function(dt_need_hf, path_oconus_hfab_config,
                              col_usgsId="usgsId",col_lon='longitude',
                              col_lat='latitude',epsg_coords=4326){
  #' @title Retrieve hydrofabric IDs wrapper
  #' @details Intended for situations when comids unavailable, generally as OCONUS
  #' @param dt_need_hf data.table of needed locations
  #' @param path_oconus_config File path to yaml config mapping of OCONUS
  #' hydrofabric locations, postal codes, and expected CRS geopackage filepaths
  #' @param col_usgsId column name inside `dt_need_hf` for the USGS gage ID
  #' @param col_lon column name inside `dt_need_hf` for longitude value
  #' @param col_lat column name inside `dt_need_hf` for latitude value
  #' @param epsg_coords The CRS for the lat/lon data inside `dt_need_hf`
  #' @seealso prep_oconus_hydroatlas.R also creates the hf_uid using custom_hf_id()
  #' @export
  # TODO  modify this given the updated hydrofabric per https://github.com/owp-spatial/hfsubsetR/issues/6

  # Parse the hydrofabric config file
  hfab_srce_map <- proc.attr.hydfab::parse_hfab_oconus_config(path_oconus_hfab_config)

  # Identify postal code and gpkg mappings:
  dt_need_hf <- proc.attr.hydfab::map_hfab_oconus_sources_wrap(dt_need_hf,
                                                               hfab_srce_map)
  col_gpkg_path <- "path" # column name inside `dt_need_hf` for hydrofabric

  # Check to ensure expected columns
  need_colnames <- base::c(col_gpkg_path,col_usgsId, col_lat, col_lon)
  if(!base::all(need_colnames %in% base::colnames(dt_need_hf))){
    need_colnames_txt <- need_colnames[base::which(!need_colnames %in%
                                                 base::names(dt_need_hf))] %>%
      base::paste0(collapse = "\n")
    stop(glue::glue("dt_need_hf does not contain the expected column
                    {need_colnames_txt}. Check map_hfab_oconus_sources_wrap()."))
  }

  # Grouping by paths so we only read in each gpkg once:
  uniq_paths <- base::unique(dt_need_hf[[col_gpkg_path]])[!is.na(unique(dt_need_hf[[col_gpkg_path]]))]
  ls_sub_gpgk_need_hf <- list()
  for (path_gpkg in uniq_paths){

    if(!base::file.exists(path_gpkg)){
      stop(glue::glue("The desired gpkg does not exist: {path_gpkg}.
                      Revisit `hfab_srce_map` mappings."))
    }

    # Read in the geopackage layers to identify location/retrieve hydrofabric ID
    ntwk <- proc.attr.hydfab::read_hfab_layer(path_gpkg=path_gpkg, layer="network")
    flowpaths <- proc.attr.hydfab::read_hfab_layer(path_gpkg=path_gpkg,layer = "flowpaths")

    sub_dt_need_gpkg <- dt_need_hf[dt_need_hf[[col_gpkg_path]] == path_gpkg,]
    ############## Try to find hydrofabric ID via USGS gage_id ##############
    hf_ids <- base::list()
    for(i in 1:base::nrow(sub_dt_need_gpkg)){
      gage_id <- sub_dt_need_gpkg[[col_usgsId]][[i]]

      if(!base::is.na(sub_dt_need_gpkg[[col_lat]][[i]])){
        # Try to find lat/lon
        epsg_domn <- sf::st_crs(flowpaths$geom)$epsg # The CRS of this hydrofabric domain
        if(base::is.na(epsg_domn)){ # Use the manual mapping CRS as plan B
          epsg_domn <- hfab_srce_map$crs_hfab[hfab_srce_map$path==path_gpkg] %>% unique()
          if(base::length(epsg_domn)!=1){
            stop(glue::glue("Need to define epsg for {path_gpkg}"))
          }
        }

        # Define the point of interest
        lon <- sub_dt_need_gpkg[[col_lon]][i]
        lat <- sub_dt_need_gpkg[[col_lat]][i]

        hf_id <- proc.attr.hydfab::retr_hfab_id_coords(path_gpkg = path_gpkg,
                                                       ntwk=ntwk,
                                                       epsg_domn = epsg_domn,
                                                       lon=lon,lat=lat,
                                                       epsg_coords =epsg_coords)

      } else if(!base::is.na(gage_id)){
        # PLACEHOLDER UNTIL gage_id retrieval is fixed
        # Grab the hf_id
        # hf_id <- proc.attr.hydfab::retr_hfab_id_usgs_gage(gage_id=gage_id,
        #                                                   ntwk=ntwk)
        warning(glue::glue("UNABLE TO DETERMINE HYDROFABRIC LOCATION FOR {sub_dt_need_gpkg$name[[i]]}"))
        hf_id <- NA

      } else {
        warning(glue::glue("UNABLE TO DETERMINE HYDROFABRIC LOCATION FOR {sub_dt_need_gpkg$name[[i]]}"))
        hf_id <- NA
      }
      hf_ids[[i]] <- hf_id
    } # End for loop over each location
    sub_dt_need_gpkg$hf_uid <- hf_ids %>% base::unlist()

    ls_sub_gpgk_need_hf[[path_gpkg]] <- sub_dt_need_gpkg
  } # End for loop over unique gpkg paths

  dt_need_hf_nomor <- data.table::rbindlist(ls_sub_gpgk_need_hf)
  sub_need_hf_nomor <- dt_need_hf_nomor %>%
    dplyr::select(base::c(col_usgsId,"hf_uid"))
  # Recombine
  dt_need_hf_mrge <- base::merge(x=dt_need_hf,
                                 y=sub_need_hf_nomor,
                                 all.x = TRUE,all.y = FALSE,by = col_usgsId)

  # Standardize the unique identifiers to the format used across formulation-selector
  dt_have_hf <- proc.attr.hydfab::std_feat_id(df=dt_need_hf_mrge,
                                              name_featureSource ="custom_hfuid",
                                              col_featureID = "hf_uid")

  return(dt_have_hf)
}



retr_hfuids <- function(loc_ids,
                        path_oconus_hfab_config, # e.g. "~/git/formulation-selector/scripts/eval_ingest/bm_test25/bm_oconus_config.yaml"
                        featureSource = c("nwissite","wqp","comid","location")[1]
  ){
  #' @title Retrieve the hydrofabric unique id
  #' @description Intended for oCONUS locations, whereas
  #' \link[proc.attr.hydfab]{retr_comids} can grab the comids from CONUS
  #' locations. Thus this function is intended to fill the gaps after
  #'  \link[proc.attr.hydfab]{retr_comids} has run.
  #' @param loc_ids The location identifier whose form is described by
  #' \code{featureSource}
  #' @param path_oconus_hfab_config Path to the config file defining where
  #' the hydrofabric files are stored.
  #' @param featureSource The \link[nhdplusTools]{get_nldi_feature}
  #' featureSource, which is attempted to be translated into the
  #' \link[dataRetrieval]{findNLDI} featureSource types. Default 'nwissite'
  #' means the argument `nwis` is used in \code{findNLDI}, which is likely the
  #' best option in the case of USGS gage ids. Other options include 'wqp',
  #' for the water quality portal (fewer locations means less data returned),
  #' 'comid', and 'location'. Location values in \code{loc_ids}
  #' would need to be defined as two points (e.g. \code{c(-115,40)})
  #'
  #' @seealso \link[proc.attr.hydfab]{retr_comids} Companion function for retrieiving comids across CONUS
  #' @seealso \link[proc.attr.hydfab]{retr_hfab_id_wrap} Specifically designed for retrieving the hydrofabric unique id, intended for oCONUS
  #' @seealso \link[proc.attr.hydfab]{proc_attr_gage_ids}
  #' @export
  # Changelog / contributions
  #. 2025-05-02 Originally created, GL

  # Find the NLDI location from an identifier or coordinates, with the goal to acquire a network geometry point
  if(base::grepl("nwissite",featureSource)){ # Cases where USGS- is not prepended
    # The nwis argument in findNLDI tends to work better than wqp, so convert USGS-{gage_id} to {gage_id}
    loc_ids_num <- base::lapply(loc_ids,function(x)
      base::gsub(pattern = "USGS-",replacement = "",x)) %>% base::unlist()
    ls_data_retr_usgs <- base::lapply(loc_ids_num, function(y)
      tryCatch(dataRetrieval::findNLDI(nwis=y)$origin,error = function(e) {
        base::data.frame(sourceName="NWIS Surface Water Sites",
                         identifier=x,comid=NA,name=NA,X=NA,Y=NA,geometry=NA)}))
  } else if (base::grepl("comid",featureSource)){
    ls_data_retr_usgs <-  base::lapply(loc_ids, function(x)
      tryCatch(dataRetrieval::findNLDI(comid=x)$origin,error = function(e) {
      base::data.frame(sourceName="NHDPlus comid",
                       identifier=x,comid=NA,name=NA,X=NA,Y=NA,geometry=NA)}))
  } else if(featureSource=='wqp'){ # The water-quality portal query doesn't include all NWIS sites
    if(!base::any(base::grepl("USGS-", loc_ids))){
      stop("Expecting loc_id format to follow 'USGS_{gage_id}'")
    } else {
      ls_data_retr_usgs <-  base::lapply(loc_ids, function(x)
        tryCatch(dataRetrieval::findNLDI(wqp=x)$origin,error = function(e) {
        base::data.frame(sourceName="Water Quality Portal",
                         identifier=x,comid=NA,name=NA,X=NA,Y=NA,geometry=NA)}))
    }
  } else if (base::grepl("loc",featureSource)){ # Working with coordinates
    # e.g. where findNLDI(location= c(-115,40))
    ls_data_retr_usgs <- base::lapply(loc_ids, function(x)
      tryCatch(dataRetrieval::findNLDI(nwis=x)$origin,error = function(e) {
        base::data.frame(sourceName="NHDPlus comid",
                         identifier=x,comid=NA,name=NA,X=NA,Y=NA,geometry=NA)}))
  } else {
    stop("Add another type of featureSource retrieval option here.")
  }

  dt_retr_usgs <- ls_data_retr_usgs %>%
    data.table::rbindlist(ignore.attr=TRUE,fill = TRUE,use.names = TRUE)
  if(base::all(base::is.na(dt_retr_usgs$X)) && base::length(loc_ids)>3){
    warning(glue::glue(
    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    All {length(loc_ids)} provided loc_ids returned NA values for locations in retr_hfuids.
    Are the provided loc_ids in the correct format? Is the featureSource
    correct? Inspect proc.attr.hydfab::retr_hfuids."))
  }

  # Generate coordinates from sf geometry
  coords <- dt_retr_usgs %>% sf::st_as_sf() %>% sf::st_coordinates()
  # Rename coordinates to latitude and longitude
  dt_retr_usgs <- dt_retr_usgs %>%
    dplyr::mutate(longitude= coords[,1], latitude = coords[,2])

  # Retrieve standardized unique location identifier, hf uid
  dt_hfuid <- proc.attr.hydfab::retr_hfab_id_wrap(dt_need_hf=dt_retr_usgs,
                                                    path_oconus_hfab_config=path_oconus_hfab_config,
                                                    col_usgsId="identifier", # specific to returned object from dataRetrieval::findNLDI
                                                    col_lon='longitude',
                                                    col_lat='latitude',
                                                    epsg_coords=4326 # presumed to be standard from dataRetrieval::findNLDI
  )
  return(dt_hfuid)
}


######## NOAA DATA SOURCES
read_noaa_nwps_gauges <- function(
    path_nwps_rpt ='https://water.noaa.gov/resources/downloads/reports/nwps_all_gauges_report.csv'
){
  #' @title Read & clean NOAA NWPS gauge metadata
  #' @param path_nwps_rpt Path to the data source
  #' @export
  d <- readr::read_csv(path_nwps_rpt) |>
    janitor::clean_names()
  return(d)
}

# Function to convert DMS to Decimal Degrees
dms_to_dd <- function(dms) {
  #' @title Internal function to convert dms to decimal degrees
  #' @seealso \link[proc.attr.hydfab]{read_noaa_hads_sites}
  parts <- base::unlist(base::strsplit(dms, " "))  # Split by space
  degrees <- base::as.numeric(parts[1])
  minutes <- base::as.numeric(parts[2])
  seconds <- base::as.numeric(parts[3])

  dd <- degrees + minutes / 60 + seconds / 3600
  return(dd)
}

read_noaa_hads_sites <- function(
    path_hads_sites='https://hads.ncep.noaa.gov/USGS/ALL_USGS-HADS_SITES.txt',
    name_map = data.frame(hads_row2 = c("5 CHR","STATION","GOES","NWS","LATITUDE","LONGITUDE","LOCATION"),
                          std_name = c("lid","usgsId","GOES","NWS_HSA","latitude","longitude","name"))){
  #' @title Read and parse NOAA HADS sites metadata
  #' @param path_hads_sites The path to NCEP NOAA HADS site locations
  #' @param name_map Mapping the default hads column names to desired names
  #' consistent with those used by NWPS in the nwps_all_gauges_report.csv and
  #' more importantly, the NWPS data retrieved via retr_noaa_gauges_meta
  #' @seealso \link[proc.attr.hydfab]{retr_noaa_gauges_meta}
  #' @export

  # Parse HADS SITES for complete site list (e.g. CSNC2)
  hads_all <- readr::read_delim(path_hads_sites,
                                lazy = TRUE,delim = "|",skip=1) #|>
  base::names(hads_all) <- base::names(hads_all) %>% base::trimws(which = 'both')
  data_bgn <- base::grep("-----",hads_all$STATION) +1 # data begin after dashes

  hads_data <- hads_all[data_bgn:nrow(hads_all),]
  mat_hads_dat <- base::sapply(names(hads_data), function(j) hads_data[[j]] %>%
                                 base::trimws(which = 'both'))

  # Now perform custom renaming:
  base::colnames(mat_hads_dat) <- name_map$std_name[match(colnames(mat_hads_dat), name_map$hads_row2)]

  # Convert to decimal degrees (!! and note the hard-coded negative for lon!!)
  dt <- mat_hads_dat %>% data.table::data.table() %>%
    dplyr::mutate(lat_dd = base::sapply(latitude, proc.attr.hydfab:::dms_to_dd),
                  lon_dd = -base::sapply(longitude, proc.attr.hydfab:::dms_to_dd))
  dt$latitude <- dt$lat_dd
  dt$longitude <- dt$lon_dd
  dt$crs <- "EPSG:4326"

  # Remove intermediate columns
  dt_clean <- dt %>% dplyr::select(-dplyr::all_of(base::c("lat_dd","lon_dd")))

  return(dt_clean)
}

retr_noaa_gauges_meta <- function(gauge_ids,
                                  gauge_url_base = "https://api.water.noaa.gov/nwps/v1/gauges",
                                  retr_ids = c("lid","usgsId","name","latitude","longitude")){
  #' @title Retrieve metadata based on a NOAA RFC gauge ID, aka lid
  #' @description Uses the NWPS api to retrieve gauge metadata
  #' @param gauge_ids list of NOAA gauge ids of interest
  #' @param gauge_url_base the base api url for NWPS
  #' @param retr_ids The desired data to retrieve from the api
  #' @seealso \link[proc.attr.hydfab]{read_noaa_hads_sites}
  #' @export
  ls_all_resp <- list()
  for(gid in gauge_ids){
    url <- file.path(gauge_url_base,gid)
    resp <- curl::curl_fetch_memory(url)

    if (resp$status_code == 200) {
      # Parse the JSON data
      data <- jsonlite::fromJSON(rawToChar(resp$content))
      dt_resp <- data.table::data.table(data.frame(data[retr_ids]))
      ls_all_resp[[url]] <- dt_resp
    } else {
      ls_all_resp[[url]] <- data.table::data.table(lid=gid)
      cat(glue::glue("Request for {gid} failed with status code:",
                     resp$status_code, "\n"))
    }
  }
  dt_all <- data.table::rbindlist(ls_all_resp,fill=TRUE)
  return(dt_all)
}

######## NOAA DATA SOURCES
read_noaa_nwps_gauges <- function(
    path_nwps_rpt ='https://water.noaa.gov/resources/downloads/reports/nwps_all_gauges_report.csv'
){
  #' @title Read & clean NOAA NWPS gauge metadata
  #' @param path_nwps_rpt Path to the data source
  #' @export
  d <- readr::read_csv('https://water.noaa.gov/resources/downloads/reports/nwps_all_gauges_report.csv') |>
    janitor::clean_names()
  return(d)
}

# Function to convert DMS to Decimal Degrees
dms_to_dd <- function(dms) {
  #' @title Internal function to convert dms to decimal degrees
  #' @seealso \link[proc.attr.hydfab]{read_noaa_hads_sites}
  parts <- base::unlist(base::strsplit(dms, " "))  # Split by space
  degrees <- base::as.numeric(parts[1])
  minutes <- base::as.numeric(parts[2])
  seconds <- base::as.numeric(parts[3])

  dd <- degrees + minutes / 60 + seconds / 3600
  return(dd)
}

read_noaa_hads_sites <- function(
    path_hads_sites='https://hads.ncep.noaa.gov/USGS/ALL_USGS-HADS_SITES.txt',
    name_map = data.frame(hads_row2 = c("5 CHR","STATION","GOES","NWS","LATITUDE","LONGITUDE","LOCATION"),
                          std_name = c("lid","usgsId","GOES","NWS_HSA","latitude","longitude","name"))){
  #' @title Read and parse NOAA HADS sites metadata
  #' @param path_hads_sites The path to NCEP NOAA HADS site locations
  #' @param name_map Mapping the default hads column names to desired names
  #' consistent with those used by NWPS in the nwps_all_gauges_report.csv and
  #' more importantly, the NWPS data retrieved via retr_noaa_gauges_meta
  #' @seealso \link[proc.attr.hydfab]{retr_noaa_gauges_meta}
  #' @export

  # Parse HADS SITES for complete site list (e.g. CSNC2)
  hads_all <- readr::read_delim(path_hads_sites,
                                lazy = TRUE,delim = "|",skip=1) #|>
  base::names(hads_all) <- base::names(hads_all) %>% base::trimws(which = 'both')
  data_bgn <- base::grep("-----",hads_all$STATION) +1 # data begin after dashes

  hads_data <- hads_all[data_bgn:nrow(hads_all),]
  mat_hads_dat <- base::sapply(names(hads_data), function(j) hads_data[[j]] %>%
                                 base::trimws(which = 'both'))

  # Now perform custom renaming:
  base::colnames(mat_hads_dat) <- name_map$std_name[match(colnames(mat_hads_dat), name_map$hads_row2)]

  # Convert to decimal degrees (!! and note the hard-coded negative for lon!!)
  dt <- mat_hads_dat %>% data.table::data.table() %>%
    dplyr::mutate(lat_dd = base::sapply(latitude, proc.attr.hydfab:::dms_to_dd),
                  lon_dd = -base::sapply(longitude, proc.attr.hydfab:::dms_to_dd))
  dt$latitude <- dt$lat_dd
  dt$longitude <- dt$lon_dd
  dt$crs <- "EPSG:4326"

  # Remove intermediate columns
  dt_clean <- dt %>% dplyr::select(-dplyr::all_of(base::c("lat_dd","lon_dd")))

  return(dt_clean)
}
