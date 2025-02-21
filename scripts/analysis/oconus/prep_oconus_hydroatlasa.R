#' @title Spatially weight hydroatlas attributes across hydrofabric divides
#' @author Guy Litt
#' @description Since hydrofabric divides are smaller than the hydroatlas
#' basins, we need to extract the hydroatlas attributes across each hydroatlas
#' divide. This assumes that the coarser-scales are appropriate for hydroatlas
#' divides
#' @details Intended for OCONUS processing, NWMv4.
#' Based on hydrofabric v2.2 and hydroatlas v1

library(sf)
library(dplyr)
library(proc.attr.hydfab)
library(yaml)

# Catchment attributes

dir_extdata <- system.file("extdata", package="proc.attr.hydfab")
path_hydatl_catg <- file.path(dir_extdata,"hydatl_catg_aggr.yml")

# Paths to hydrofabric geopackages:
dir_save_base <- "~/noaa/analysis/oconus_hydatl/"
if(!base::dir.exists(dir_save_base)){
  base::dir.create(dir_save_base,recursive=TRUE)
}
# Hydrofabric data downloaded from Lynker-spatial
dir_base_hfab <- "~/noaa/hydrofabric/v2.2/"
path_hfab_ak <- file.path(dir_base_hfab,"ak_nextgen.gpkg")
path_hfab_prvi <- file.path(dir_base_hfab,"prvi_nextgen.gpkg")
paths_hfab <- c(path_hfab_ak,path_hfab_prvi)

redo_intersection_analysis <- FALSE


dir_base_hydatl <- "~/noaa/data/hydroatlas/"
read_hydatl_by_vpu_val <- function(vpu){
  # Define Paths to hydroatlas data downloaded from hydroatlas website
  #. based on the hydrofabric vpu identifiers. Make geometries valid
  if(vpu=="ak"){
    # Read in the arctic hydroatlas data
    path_hab_shp <- file.path(dir_base_hydatl,"hybas_ar_lev01-12_v1c","hybas_ar_lev12_v1c.shp")
  } else if (vpu=="prvi"){
    # Read in the north america hydroatlas data
    path_hab_shp <- file.path(dir_base_hydatl,"hybas_na_lev01-12_v1c","hybas_na_lev12_v1c.shp")
  }
  print(glue::glue("Reading, making valid {path_hab_shp} for {vpu} vpu."))
  hab_shp <- sf::read_sf(path_hab_shp)
  # Make the geometries valid
  hab_shp_val <- sf::st_make_valid(hab_shp)

  return(hab_shp_val)
}

subset_hydatl_by_hydfab <- function(div_hfab_val,hab_shp_val,
                                    buff_dist = 50000){
  # Subset the hydroatlas data to a buffered hydrofabric extent
  extent_hfab <- sf::st_bbox(div_hfab_val)
  extent_hfab_sf <- st_as_sfc(extent_hfab)
  extent_hfab_buff <- sf::st_buffer(extent_hfab_sf, dist = buff_dist)
  hab_shp_sub <- sf::st_intersection(hab_shp_val, extent_hfab_buff)
  return(hab_shp_sub)
}

std_paths_ntrsct <- function(path_hfab){
  # Standardizing the paths to hydrofabric-hydroatlas intersection data
  new_file_name <- base::gsub(pattern = ".gpkg",replacement="_all.rds",
                              x = base::basename(path_hfab))
  path_save_ls_ntrsct <- base::file.path(dir_save_base,new_file_name)
  path_save_ntrsct_gpkg <- gsub(pattern=".rds",replacement=".gpkg",
                                path_save_ls_ntrsct)

  return(list(rds=path_save_ls_ntrsct, gpkg = path_save_ntrsct_gpkg))
}

std_paths_attrs <- function(dir_base_hfab, vpu = c("ak","prvi")){
  path_attrs_new <- file.path(dir_base_hfab,paste0(vpu,"_hydroatlas_attrs.csv"))
  return(path_attrs_new)
}

read_hfab_lyr_val <- function(path_hfab, lyr = 'divides'){
  # Read hydrofabric layer, transform to CRS 4326, make valid
  print(glue::glue("Reading, transforming, & make valid {path_hfab}"))
  div_hfab <- sf::st_read(path_hfab,layer = lyr) # EPSG: 3338
  div_hfab_tfrm <- sf::st_transform(div_hfab, crs = 4326) # convert to 4326
  div_hfab_val <- sf::st_make_valid(div_hfab_tfrm)
  return(div_hfab_val)
}

########### HYDROFABRIC-HYDROATLAS INTERSECTION FRACTION PROCESSING ############
# Calculates the overlap fraction of hydroatlas basins for each hydrofabric divide
#. Writes comprehensive to file as an .rds and a geopackage with two layer:
#. polygon_intersects : Situations where only a single polygon represents the hydrofabric-hydroatlas overlap
#. multipolygon_intersects: Situations where multiple polgyons represent the hydrofabric-hydroatlas overlap
#. NOTE, this processing took nearly 2 days for AK on an Apple M3 Pro, 18GB RAM
#.   (without fancy multi-core processing)
for(path_hfab in paths_hfab){

  # Define the standardized hydfab-hydatlas intersection rds and gpkg paths
  paths_ntrsct <- std_paths_ntrsct(path_hfab)
  path_save_ls_ntrsct <- paths_ntrsct$rds
  path_save_ntrsct_gpkg <- paths_ntrsct$gpkg

  if(file.exists(path_save_ls_ntrsct) && redo_intersection_analysis==FALSE){
    print(glue::glue("Skipping intersection analysis for {path_save_ls_ntrsct}"))
    next()
  }

  # hydroatlas basins shapefiles
  if(base::grepl("ak_nextgen",path_hfab)){
    vpu <- "ak"
  } else if(base::grepl("prvi_nextgen",path_hfab)){
    vpu <- "prvi"
  } else {
    print(glue::glue("Problem with {path_hfab}. Skipping!"))
  }
  # Read hydroatlas data, convert to 4326, make geoms valid
  hab_shp <- read_hydatl_by_vpu_val(vpu)

  # Read in the hydrofabric data
  div_hfab_val <- read_hfab_lyr_val(path_hfab,lyr='divides')

  # Make the geometries valid
  hab_shp_val <- sf::st_make_valid(hab_shp)

  # Subset the hydroatlas data to a buffered hydrofabric extent
  hab_shp_sub <- subset_hydatl_by_hydfab(div_hfab_val,hab_shp_val,buff_dist=50000)

  ########################          INTERSECTION         #####################
  # Loop over the hydrofabric catchments and find intersections with hydroatlas
  ls_ntrsct_ha_hfab <- list()
  ls_ntrsct_intrmediate <- list()
  ctr <- 0
  for(div_id in unique(div_hfab_val$divide_id)){
    ctr <- ctr+1
    print(glue::glue("Processing {div_id}"))
    sub_hfab <- div_hfab_val[div_hfab_val$divide_id == div_id,]
    #small_in_big <-  try(sf::st_join(sub_hfab,hab_shp_val, join = sf::st_within))

    ntrsct <- try(sf::st_intersection(sub_hfab,hab_shp_sub)) # small, big overlap
    if(!"try-error" %in% class(ntrsct)){
      ntrsct$area_intersect <- sf::st_area(ntrsct) # compute area of intersections
      sub_hfab$area_small <- sf::st_area(sub_hfab) # compute overall area of the smaller hydrofabric basin

      frac_overlaps <- ntrsct$area_intersect/sub_hfab$area_small
      ntrsct$frac_overlaps <- frac_overlaps
      ls_ntrsct_ha_hfab[[div_id]] <- ntrsct
      ls_ntrsct_intrmediate[[div_id]] <- ntrsct
    } else {
      ls_ntrsct_ha_hfab[[div_id]] <- data.frame(divide_id = div_id)
      ls_ntrsct_intrmediate[[div_id]] <- data.frame(divide_id = div_id)
    }
    # Implement intermediate file saving every 100 iterations
    if(ctr%%100==0 || ctr==nrow(div_hfab_val)){
      intermediate_file_name <- base::gsub(pattern = ".gpkg",
                                           replacement=glue::glue("_{ctr}.rds"),
                                           x = base::basename(path_hfab))
      path_save_ls_intrmediate <- base::file.path(dir_save_base,intermediate_file_name)

      base::saveRDS(ls_ntrsct_intrmediate, file = path_save_ls_intrmediate)
      ls_ntrsct_intrmediate <- list()
    }
  }

  #--------------------   Save binary file   -------------------- #
  base::saveRDS(ls_ntrsct_ha_hfab, file = path_save_ls_ntrsct)
  print(glue::glue("Wrote {path_save_ls_ntrsct}"))

  #--------------------      SAVE GPKG      -------------------- #
  # Removing empty values
  df_ntrsct <- data.table::rbindlist(ls_ntrsct_ha_hfab,fill=TRUE,ignore.attr=TRUE)
  df_ntr_rmna <- df_ntrsct[-base::which(base::is.na(df_ntrsct$frac_overlaps)),]
  st_ntr <- sf::st_as_sf(df_ntr_rmna)

  # Split the data by the different geometry types so we can write it as a gpkg
  ls_ntr <- list()
  ls_mlti <- list()
  for(i in 1:nrow(st_ntr)){
    st_smp <- try(sf::st_simplify(st_ntr[i,]))
    if("sfc_POLYGON" %in% class(st_smp$geom)){
      ls_ntr[[i]] <- st_smp
    } else {
      ls_mlti[[i]] <- st_smp
    }
  }
  st_poly <- data.table::rbindlist(ls_ntr) %>% sf::st_as_sf()
  st_mlti <- data.table::rbindlist(ls_mlti) %>% sf::st_as_sf()

  # Create geopackage with different layers, multipolygon and single polygon
  path_save_ntrsct_gpkg <- gsub(pattern=".rds",replacement=".gpkg",path_save_ls_ntrsct)
  sf::st_write(st_poly, path_save_ntrsct_gpkg, layer = "polygon_intersects",delete_layer=TRUE)
  sf::st_write(st_mlti, path_save_ntrsct_gpkg, layer = "multipolygon_intersects",delete_layer=TRUE)
}


# TODO fill in the gaps:
for(path_hfab in paths_hfab){
  if(base::grepl("ak",path_hfab)){
    df_haa <- df_haa_ak # Define the hydroatlas attribute data of interest
    vpu <- "ak"
  } else if (base::grepl("prvi", path_hfab)){
    df_haa <- df_haa_na # Define the hydroatlas attribute data of interest
    vpu <- "prvi"
  }
  hab_shp <- read_hydatl_by_vpu_val(vpu)



  # Read in the hydrofabric data
  div_hfab_val <- read_hfab_lyr_val(path_hfab,lyr='divides')
  # TODO add in hab_shp_buff


  path_ntrsct_rds <- std_paths_ntrsct(path_hfab)$rds
  ls_ntr <- readRDS(path_save_ls_ntrsct)

  need_divs <- div_hfab_val$divide_id[which(!div_hfab_val$divide_id %in% names(ls_ntr))]

  for(divid in need_divs){
    print(glue::glue("Processing {div_id}"))
    sub_hfab <- div_hfab_val[div_hfab_val$divide_id == div_id,]
    #small_in_big <-  try(sf::st_join(sub_hfab,hab_shp_val, join = sf::st_within))

    ntrsct <- try(sf::st_intersection(sub_hfab,hab_shp_sub))
  }
}


# ---------------------------------------------------------------------------- #
##########################  HYDROATLAS ATTRIBUTES  #############################
# ---------------------------------------------------------------------------- #
# GOAL: Proportionally scale hydroatlas attributes by fractional hf coverage
weighted_mean <- function(x, w) {
  sum(x * w) / sum(w)
}
# Read the global hydroatlas attributes
path_haa_global <-  file.path(dir_base_hydatl,"BasinATLAS_Data_v10.gdb","BasinATLAS_v10.gdb")
layrs_global_attrs <- sf::st_layers(path_haa_global)
high_res_lyr <- layrs_global_attrs$name[base::grep("lev12",layrs_global_attrs$name)]
df_haa_global <- sf::read_sf(path_haa_global,layer=high_res_lyr)

# Subset hydroatlas attributes to catchments of interest (e.g. AK domain)
df_haa_ak <- df_haa_global %>% dplyr::filter(HYBAS_ID %in% ak_hab_shp$HYBAS_ID)
df_haa_na <- df_haa_global %>% dplyr::filter(HYBAS_ID %in% na_hab_shp$HYBAS_ID)
rm(df_haa_global) # Remove the large global dataset from memory

hydatl_catg <- yaml::read_yaml(path_hydatl_catg)



for(path_hfab in paths_hfab){

  if(base::grepl("ak",path_hfab)){
    df_haa <- df_haa_ak # Define the hydroatlas attribute data of interest
    vpu <- "ak"
  } else if (base::grepl("prvi", path_hfab)){
    df_haa <- df_haa_na # Define the hydroatlas attribute data of interest
    vpu <- "prvi"
  }

  path_ntrsct_gpkg <- std_paths_ntrsct(path_hfab)$gpkg

  # Read in the geopackage intersection data
  st_poly <- sf::st_read(path_ntrsct_gpkg,layer = "polygon_intersects")
  st_mlti <- sf::st_read(path_ntrsct_gpkg,layer = "multipolygon_intersects")
  st_ntrsct <- base::rbind(st_poly,st_mlti)

  # Layers in the domain hydrofabric
  hfab_layrs <- sf::st_layers(path_hfab)
  # Get the hydrofabric column names
  hfab_div <- sf::st_read(path_hfab,layer = "divides")
  names_hfab_div <- names(hfab_div)

  ls_wt_mean <- list()
  for(divid in unique(st_ntrsct$divide_id)){
    sub_ntrsct <- st_ntrsct %>% subset(divide_id == divid)


    # Total coverage of hydroatlas dataset over hydrofabric divide:
    totl_coverage <- base::sum(sub_ntrsct$frac_overlaps)
    totl_area_ntrsct_km2 <- sum(sub_ntrsct$area_intersect)/1E6

    sub_df_haa <- df_haa %>% subset(HYBAS_ID %in% sub_ntrsct$HYBAS_ID)

    cols_to_average <- hydatl_catg$avg_cols
    df_cmbo <- merge(data.table::as.data.table(sub_ntrsct),
                     data.table::as.data.table(sub_df_haa),by="HYBAS_ID")
    # Weighted mean, divided by the total coverage fraction. E.g., if only
    # the weighted mean only accounts for 74% of divide id, extrapolate the
    # under-estimated fraction to the total fraction
    df_wt_mean <- df_cmbo %>%
      dplyr::summarize_at(dplyr::vars(cols_to_average),
                          list(~ weighted_mean(., frac_overlaps)/totl_coverage))
    df_wt_mean$divide_id <- divid
    df_wt_mean$totl_hydatl_locs <- nrow(sub_ntrsct)
    df_wt_mean$total_coverage <- totl_coverage
    df_wt_mean$area_ntrsct_covered_sqkm <- base::sum(sub_ntrsct$area_intersect)/1E6
    df_wt_mean$area_estimated_tot_sqkm <- df_wt_mean$area_ntrsct_covered_sqkm/totl_coverage
    ls_wt_mean[[divid]] <- df_wt_mean
    if(totl_coverage < 0.99){ # >99% hydroatlas coverage
      warning(glue::glue("Total hydroatlas coverage for {divid} only {totl_coverage}"))
    }
  }

  # Compile weighted mean attributes
  dt_wt_mean <- data.table::rbindlist(ls_wt_mean)
  path_attrs <- std_paths_attrs(dir_base_hfab, vpu = vpu)
  write.csv(dt_wt_mean,file = path_attrs)

  # Create geopackage
  dim(dt_wt_mean)

}

# TODO MERGE ATTRIBUTES WITH DIVIDE GEOM:



###########


#df_hfab_ak_attrs <- sf::st_read(path_hfab_ak,layer="divide-attributes")

# Read hydrofabric divides and convert to EPSG 4326
# div_hfab_pr <- sf::st_read(path_hfab_prvi, layer = 'divides') #"EPSG:6566"
# div_hfab_pr_tfrm <- sf::st_transform(div_hfab_pr, crs = 4326)
#
#




#
#
# for (div_id in hab_shp$HYBAS_ID){
#   sub_hab_ak <- hab_shp[ak_hab_shp$HYBAS_ID == div_id,]
#
#
#   small_in_big <- try(sf::st_join(div_hfab_ak_tfrm,sub_hab_ak, join = sf::st_within))
#   if ("try-error" %in% base::class(small_in_big)){
#     next()
#   } else {
#     stop()
#     # TODO check to see if there was any intersection
#     ak_ntrsct <- sf::st_intersection(div_hfab_val,ak_hab_shp_val) # small, big overlap
#     ak_ntrsct$area_intersect <- sf::st_area(ak_ntrsct) # compute area of intersections
#     div_hfab_val$area_small <- sf::st_area(div_hfab_val) # compute overall area of the smaller hydrofabric basin
#
#     frac_overlap <- ak_ntrsct$area_intersect/div_hfab_val$area_small
#
#     # TODO is this the correct ID col? Or should it be "HYBAS_ID"???
#     # Merge intersection areas back to the hydrofabric polygons
#     hfab_overlap <- div_hfab_val %>% dplyr::left_join(ak_ntrsct %>%
#                                                           sf::st_drop_geometry(), by = 'ID')
#
#   }
# }



# TODO identify locations that correspond with



# ----------------------------
# Reduce the hydroatlas basins to those intersecting the hydrofabric
# hab_sub_to_hf <- sf::st_filter(hab_shp_val,div_hfab_val,
#                                   .predicate=sf::st_intersects )

# Compute intersection areas:
#div_ntrsct <- sf::st_intersection(div_hfab_val,hab_shp_val)
#div_ntrsct$area_intersect <- sf::st_area(div_ntrsct)
#div_hfab_val$area_small <- sf::st_area(div_hfab_val)

# TODO is this the correct ID col? Or should it be "HYBAS_ID"???
# Merge intersection areas back to the hydrofabric polygons
#hfab_overlap <- div_hfab_val %>% dplyr::left_join(div_ntrsct %>%
#                         sf::st_drop_geometry(), by = 'ID')

# Compute overlap fraction
# hfab_overlap <- hfab_overlap %>% dplyr::mutate(
#   frac_over = as.numeric(area_intersect) / as.numeric(area_small)
# )

# TODO remove? This doesn't get used
# Find the small hydrofabric polygons within the larger hydroatlas polygons
# hfab_hab_join <- sf::st_join(div_hfab_val,hab_sub_to_hf, join = sf::st_within) # those that are entirely within
# hfab_hab_join_ntrs <- sf::st_join(div_hfab_val,hab_sub_to_hf, join = sf::st_intersects) # those that intersect

