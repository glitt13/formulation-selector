#' @title Spatially weight hydroatlas attributes across hydrofabric divides
#' @author Guy Litt
#' @description Determine the spatial coverage of each hydroatlas basin upon
#' the smaller-scale hydrofabric divide.
#' Creates spatially weighted average of each hydroatlas attribute when
#' downscaling to the hydrofabric divide.
#' This assumes that the coarser-scale hydroatlas data may be downscaled to
#'. the smaller-scaled hydrofabric divides.
#' @details Intended for OCONUS processing of AK and PRVI, NWMv4.
#' Based on hydrofabric v2.2 and hydroatlas v1, but updates may be
#' 1) MUST download the `prvi` and `ak` hydrofabric datasets from:
#' https://www.lynker-spatial.com/data?path=hydrofabric%2Fv2.2%2F
#' 2) MUST download the hydroBASINS datasets for North America and Arctic
#' from https://www.hydrosheds.org/products/hydrobasins
#' @note Relevant internal proc.attr.hydfab functions stored inside
#'  `proc.attr.hydfab/R/proc_hydatl_hydfab.R`
#'  Transforms each hydrofabric into CRS 4326 for areal weighting calculations.
#' @note Internal to the Regionalization team, outputs shared in NOAA google drive:
#' https://drive.google.com/drive/folders/1RpmXevXvhKR-DGmf_lY8nHzwkghK8LOO?usp=drive_link
# Changelog / Contributions
#. 2025-02-25 Originally created, GL

library(sf)
library(dplyr)
library(proc.attr.hydfab)
library(yaml)
library(data.table)
library(glue)

# Paths to hydrofabric geopackages:
dir_save_base <- "~/noaa/analysis/oconus_hydatl/"

# Hydrofabric data downloaded from Lynker-spatial
dir_base_hfab <- "~/noaa/hydrofabric/v2.2/"
dir_hfab_tab_dat <- "~/noaa/hydrofabric/tabular-data/" # Save the parquet file here
path_hfab_ak <- base::file.path(dir_base_hfab,"ak_nextgen.gpkg")
path_hfab_prvi <- base::file.path(dir_base_hfab,"prvi_nextgen.gpkg")
paths_hfab <- base::c(path_hfab_ak,path_hfab_prvi) # Define the paths to the hydrofabric geopackages of interest

redo_intersection_analysis <- FALSE # Should we re-run hydroatlas/hydrofabric intersection analysis if it hasn't already been run? (may take ~18 hours)
# Directory containing hydrotlas basins manually downloaded from https://www.hydrosheds.org/products/hydrobasins
dir_base_hydatl <- "~/noaa/data/hydroatlas/"
path_haa_global <-  file.path(dir_base_hydatl,"BasinATLAS_Data_v10.gdb","BasinATLAS_v10.gdb")

#------ END USER INPUT HERE
if(!base::dir.exists(dir_save_base)){
  base::dir.create(dir_save_base,recursive=TRUE)
}

dir_extdata <- system.file("extdata", package="proc.attr.hydfab")
path_hydatl_catg <- file.path(dir_extdata,"hydatl_catg_aggr.yml")

# ---------------------------------------------------------------------------- #
########## 1. HYDROFABRIC-HYDROATLAS INTERSECTION FRACTION PROCESSING ##########
# ---------------------------------------------------------------------------- #
# Calculates the overlap fraction of hydroatlas basins for each hydrofabric divide
#. Writes comprehensive to file as an .rds and a geopackage with two layer:
#. polygon_intersects : Situations where only a single polygon represents the hydrofabric-hydroatlas overlap
#. multipolygon_intersects: Situations where multiple polgyons represent the hydrofabric-hydroatlas overlap
#. NOTE, this processing took nearly 18 hours for AK on an Apple M3 Pro, 18GB RAM
#.   (without fancy multi-core processing)
for(path_hfab in paths_hfab){

  # Define the standardized hydfab-hydatlas intersection rds and gpkg paths
  paths_ntrsct <- proc.attr.hydfab:::std_paths_ntrsct(path_hfab)
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
  hab_shp <- proc.attr.hydfab:::read_hydatl_by_vpu_val(vpu,dir_base_hydatl)

  # Read in the hydrofabric data
  div_hfab_val <- proc.attr.hydfab:::read_hfab_lyr_val(path_hfab,lyr='divides')

  # Make the geometries valid
  hab_shp_val <- sf::st_make_valid(hab_shp)

  # Subset the hydroatlas data to a buffered hydrofabric extent
  hab_shp_sub <- proc.attr.hydfab:::subset_hydatl_by_hydfab(div_hfab_val,hab_shp_val,buff_dist=50000)

  # ------------------------      INTERSECTION       --------------------------
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
      ls_ntrsct_intrmediate <- list() # reset the intermediate list for future file saving
    }
  }

  #--------------------------   Save binary file   --------------------------- #
  base::saveRDS(ls_ntrsct_ha_hfab, file = path_save_ls_ntrsct)
  print(glue::glue("Wrote {path_save_ls_ntrsct}"))

  #--------------------------      SAVE GPKG      ---------------------------- #
  # Removing empty values
  df_ntrsct <- data.table::rbindlist(ls_ntrsct_ha_hfab,fill=TRUE,ignore.attr=TRUE)
  df_ntr_rmna <- df_ntrsct[base::which(!base::is.na(df_ntrsct$frac_overlaps)),]
  st_ntr <- sf::st_as_sf(df_ntr_rmna)

  # Split the data by the different geometry types so we can write it as a gpkg
  ls_ntr <- list()
  ls_mlti <- list()
  for(i in 1:base::nrow(st_ntr)){
    st_smp <- try(sf::st_simplify(st_ntr[i,]))
    if("sfc_POLYGON" %in% base::class(st_smp$geom)){
      ls_ntr[[i]] <- st_smp
    } else {
      ls_mlti[[i]] <- st_smp
    }
  }
  st_poly <- data.table::rbindlist(ls_ntr) %>% sf::st_as_sf()
  st_mlti <- data.table::rbindlist(ls_mlti) %>% sf::st_as_sf()

  # Create geopackage with different layers, multipolygon and single polygon
  path_save_ntrsct_gpkg <- base::gsub(pattern=".rds",replacement=".gpkg",path_save_ls_ntrsct)
  sf::st_write(st_poly, path_save_ntrsct_gpkg, layer = "polygon_intersects",
               delete_layer=TRUE)
  sf::st_write(st_mlti, path_save_ntrsct_gpkg,layer = "multipolygon_intersects",
               delete_layer=TRUE)
}
# ---------------------------------------------------------------------------- #
######################## 2. QA/QC: Hydrofabric Gaps? ###########################
# ---------------------------------------------------------------------------- #
# QA/QC check: Any gaps to fill from downscaling hydroatlas to hydrofabric?
for(path_hfab in paths_hfab){
  if(base::grepl("ak",path_hfab)){
    vpu <- "ak"
  } else if (base::grepl("prvi", path_hfab)){
    vpu <- "prvi"
  }
  hab_shp <- proc.attr.hydfab:::read_hydatl_by_vpu_val(vpu,dir_base_hydatl)

  # Read in the hydrofabric data
  div_hfab_val <- proc.attr.hydfab:::read_hfab_lyr_val(path_hfab,lyr='divides')

  path_ntrsct_rds <- proc.attr.hydfab:::std_paths_ntrsct(path_hfab)$rds
  ls_ntr <- base::readRDS(path_save_ls_ntrsct)

  # Which divide ids have not been accounted for with hydroatlas downscaling?:
  need_divs <- div_hfab_val$divide_id[which(!div_hfab_val$divide_id %in% names(ls_ntr))]
  if(length(need_divs) > 0){
    stop("TODO Missing some hydrofabric divides. Implement a gap-filling solution here.")
    for(divid in need_divs){
      print(glue::glue("Processing {div_id}"))
      sub_hfab <- div_hfab_val[div_hfab_val$divide_id == div_id,]
      ntrsct <- try(sf::st_intersection(sub_hfab,hab_shp_sub))
    }
  } else{
    message(glue::glue("All hydrofabric divides from {path_hfab} have been succeessfully
            mapped to hydroatlas attributes!"))
  }
}


# ---------------------------------------------------------------------------- #
######################## 3. HYDROATLAS ATTRIBUTES  #############################
# ---------------------------------------------------------------------------- #
message("Downscaling/areal averaging hydroatlas attributes to hydrofabric divides")
# Proportionally scale hydroatlas attributes by fractional hf coverage
#.  and write scaled attributes to hydrofabric gpkg (& a separate .csv)
weighted_mean <- function(x, w) {
  sum(x * w) / sum(w)
}
# Read the global hydroatlas attributes

layrs_global_attrs <- sf::st_layers(path_haa_global)
high_res_lyr <- layrs_global_attrs$name[base::grep("lev12",layrs_global_attrs$name)]
df_haa_global <- sf::read_sf(path_haa_global,layer=high_res_lyr)

# Subset hydroatlas attributes to catchments of interest (e.g. AK domain)
for(path_hfab in paths_hfab){
  # hydroatlas basins shapefiles
  if(base::grepl("ak_nextgen",path_hfab)){
    vpu <- "ak"
    ak_hab_shp <- proc.attr.hydfab:::read_hydatl_by_vpu_val(vpu,dir_base_hydatl)
  } else if(base::grepl("prvi_nextgen",path_hfab)){
    vpu <- "prvi"
    na_hab_shp <- proc.attr.hydfab:::read_hydatl_by_vpu_val(vpu,dir_base_hydatl)
  } else {
    print(glue::glue("Problem with {path_hfab}. Skipping!"))
  }
}
df_haa_ak <- df_haa_global %>% dplyr::filter(HYBAS_ID %in% ak_hab_shp$HYBAS_ID)
df_haa_na <- df_haa_global %>% dplyr::filter(HYBAS_ID %in% na_hab_shp$HYBAS_ID)
rm(df_haa_global) # Remove the large global dataset from memory

# Read in the hydroatlas categories, to identify which cols to aggregate by averaging!
hydatl_catg <- yaml::read_yaml(path_hydatl_catg)

ls_all_attrs <- list()
for(path_hfab in paths_hfab){

  if(base::grepl("ak",path_hfab)){
    df_haa <- df_haa_ak # Define the hydroatlas attribute data of interest
    vpu <- "ak"
  } else if (base::grepl("prvi", path_hfab)){
    df_haa <- df_haa_na # Define the hydroatlas attribute data of interest
    vpu <- "prvi"
  }

  path_ntrsct_gpkg <- proc.attr.hydfab:::std_paths_ntrsct(path_hfab)$gpkg

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
    totl_area_ntrsct_km2 <- base::sum(sub_ntrsct$area_intersect)/1E6

    sub_df_haa <- df_haa %>% base::subset(HYBAS_ID %in% sub_ntrsct$HYBAS_ID)

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
    df_wt_mean$vpu <- vpu
    df_wt_mean$id <- base::unique(sub_ntrsct$id)
    # Refer to proc.attr.hydfab::retr_hfab_id_wrap()
    df_wt_mean$hf_uid <- proc.attr.hydfab::custom_hf_id(df_wt_mean, col_vpu = "vpu",col_id = "id")
    df_wt_mean$totl_hydatl_locs <- nrow(sub_ntrsct)
    df_wt_mean$total_coverage <- totl_coverage
    df_wt_mean$area_ntrsct_covered_sqkm <- base::sum(sub_ntrsct$area_intersect)/1E6
    df_wt_mean$area_estimated_tot_sqkm <- df_wt_mean$area_ntrsct_covered_sqkm/totl_coverage
    ls_wt_mean[[divid]] <- df_wt_mean

    if(totl_coverage < 0.99){ # >99% hydroatlas coverage
      # Situations where the weighted mean would be an extrapolation:
      warning(glue::glue("Total hydroatlas coverage for {divid} only {totl_coverage}.
      The weighted mean values are an extrapolation!"))
    }
  }

  # Compile weighted mean attributes
  dt_wt_mean <- data.table::rbindlist(ls_wt_mean)
  # Reorder the column names
  new_cols <- c("hf_uid","divide_id","id","vpu","totl_hydatl_locs","total_coverage",
                "area_ntrsct_covered_sqkm","area_estimated_tot_sqkm")
  dt_wt_mean <- data.table::setcolorder(dt_wt_mean,
                 base::c(new_cols, base::setdiff(names(dt_wt_mean),new_cols)))

  # Create path to hydrofabric's hydroatlas attributes and write to file as .csv
  path_attrs <- proc.attr.hydfab:::std_paths_attrs(dir_base_hfab, vpu = vpu)
  utils::write.csv(dt_wt_mean,file = path_attrs)

  # Update hydrofabric geopackage with the downscaled hydroatlas attributes as a new layer
  sf::st_write(dt_wt_mean,dsn=path_hfab,layer="hydroatlas_attributes",append=FALSE)
  base::message(glue::glue("Updated {path_hfab} with hydroatlas attributes"))

  ls_all_attrs[[vpu]] <- dt_wt_mean
}

dt_all_attrs <- data.table::rbindlist(ls_all_attrs)

# Write combined OCONUS attributes as parquet:
ls_vpus <- base::unique(dt_all_attrs$vpu)
path_attrs_all_oconus <- proc.attr.hydfab:::std_path_attrs_all_parq(dir_hfab_tab_dat, ls_vpus)
arrow::write_parquet(dt_all_attrs,path_attrs_all_oconus)

