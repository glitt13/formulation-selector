#' @title Collection of functions for processing hydrofabric data with hydroatlas data
#' @description These are not fully formalized functions! Serving a quick-and-dirty
#' geospatial processing need that maps hydroatlas attributes to OCONUS
#' hydrofabrics locations across AK and PR
#' @details Functions that are internal to proc.attr.hydfab, not intended for general use
#' @seealso proc.attr.hydfab/flow/prep_oconus_hydroatlas.R, the script that calls these functions

read_hydatl_by_vpu_val <- function(vpu,dir_base_hydatl){
  #' @title Read the hydroatlas by vpu value
  #' @description Define Paths to hydroatlas data downloaded from hydroatlas website
  #'  based on the hydrofabric vpu identifiers. Make geometries valid.
  #' @param vpu The OCONUS VPU id used in the hydrofabric. Options
  if(vpu=="ak"){
    # Read in the arctic hydroatlas data
    path_hab_shp <- file.path(dir_base_hydatl,"hybas_ar_lev01-12_v1c","hybas_ar_lev12_v1c.shp")
  } else if (vpu=="prvi"){
    # Read in the north america hydroatlas data
    path_hab_shp <- file.path(dir_base_hydatl,"hybas_na_lev01-12_v1c","hybas_na_lev12_v1c.shp")
  } else {
    stop("Problem: vpu not identified.")
  }
  if(!file.exists(path_hab_shp)){
    stop(glue::glue("MUST download the hydroatlas basin dataset for {vpu} from
    https://www.hydrosheds.org/products/hydrobasins for the Arctic & North America
    and place inside {dir_base_hydatl}"))
  }

  print(glue::glue("Reading, making valid {path_hab_shp} for {vpu} vpu."))
  hab_shp <- sf::read_sf(path_hab_shp)
  # Make the geometries valid
  hab_shp_val <- sf::st_make_valid(hab_shp)

  return(hab_shp_val)
}

subset_hydatl_by_hydfab <- function(div_hfab_val,hab_shp_val,
                                    buff_dist = 50000){
  #' @title Subset the hydroatlas data to a buffered hydrofabric extent
  #' @param div_hfab_val validated hydrofabric divides of interest.
  #' Presumed to be the smaller spatial extent, sf object
  #' @param hab_shp_val validated hydroatlas basin.
  #' Presumed to be the larger spatial extent, sf object
  #' @param buff_dist The distance for a buffer around the hydrofabric to ensure
  #' full hydroatlas coverage
  extent_hfab <- sf::st_bbox(div_hfab_val)
  extent_hfab_sf <- st_as_sfc(extent_hfab)
  extent_hfab_buff <- sf::st_buffer(extent_hfab_sf, dist = buff_dist)
  hab_shp_sub <- sf::st_intersection(hab_shp_val, extent_hfab_buff)
  return(hab_shp_sub)
}

std_paths_ntrsct <- function(path_hfab){
  #' @title Standardizing the paths to hydrofabric-hydroatlas intersection data
  new_file_name <- base::gsub(pattern = ".gpkg",replacement="_all.rds",
                              x = base::basename(path_hfab))
  path_save_ls_ntrsct <- base::file.path(dir_save_base,new_file_name)
  path_save_ntrsct_gpkg <- gsub(pattern=".rds",replacement=".gpkg",
                                path_save_ls_ntrsct)

  return(list(rds=path_save_ls_ntrsct, gpkg = path_save_ntrsct_gpkg))
}

std_paths_attrs <- function(dir_base_hfab, vpu){
  #' @title standardize path to the hydroatlas attributes corresponding to
  #' each hydrofabric divide, saved as a csv
  #' @param dir_base_hfab Base directory where hydrofabric data stored
  #' @param vpu the hydrofabric vpu of interest (e.g. `'ak'` or `'prvi'`)
  path_attrs_new <- file.path(dir_base_hfab,paste0(vpu,"_hydroatlas_attrs.csv"))
  return(path_attrs_new)
}

std_path_attrs_all_parq <- function(dir_base_hfab,ls_vpus=c("ak","prvi")){
  #' @title standardize path to they hydroatlas attributes for all OCONUS
  #' hydrofabric domains
  #' @details Intended for just ak and prvi
  #' @param dir_base_hfab Base directory where hydrofabric data stored
  #' @param ls_vpu the hydrofabric vpus of interest (e.g. `c('ak','prvi')`)
  #' @export
  all_vpus <- base::paste0(base::sort(ls_vpus),collapse = "_")
  path_attrs <- base::file.path(dir_base_hfab,
                                glue::glue("hydroatlas_attributes_{all_vpus}.parquet"))
  return(path_attrs)
}


read_hfab_lyr_val <- function(path_hfab, lyr = 'divides'){
  #' @title Read the hydrofabric layer, convert to WGS 84, and make valid
  #' @param path_hfab filepath to the hydrofabric geopackage of interest
  #' @param lyr The layer to read from the geopackage, default `'divides'`
  print(glue::glue("Reading, transforming, & make valid {path_hfab}"))
  div_hfab <- sf::st_read(path_hfab,layer = lyr) # e.g. EPSG: 3338
  div_hfab_tfrm <- sf::st_transform(div_hfab, crs = 4326) # convert to 4326
  div_hfab_val <- sf::st_make_valid(div_hfab_tfrm)
  return(div_hfab_val)
}
