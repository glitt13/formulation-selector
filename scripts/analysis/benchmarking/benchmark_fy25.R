# Benchmarking exercises:
library(proc.attr.hydfab)
library(janitor)
library(readr)
library(hydrofabric)
library(hfsubsetR)
library(maps)
library(mapdata)

# Function to get state name



# Example usage
# lat <- 41.8781
# lon <- -87.6298
# usvi <- c(17.725496,-64.845511)
# pr <- c(-66.992208,18.122422,4326)
# lon <- pr[1]
# lat <- pr[2]
# lon <- usvi[2]
# lat <- usvi[1]
# get_state_or_territory(pr[2],pr[1])
# state_name <- get_state_or_territory(latitude, longitude)
#
# print(state_name)


gauge_ids <- c("JOPM7","RNDO2","CSNC2",
               "UCHA2","TYAA2","WLWA2",
               "ALEC2","BERU1","OAKA3",
               "CREC1","HPIC1","PRBC1",
               "WDVA1","BDCL1","PLAM6",
               "CRTN6","FDKM2","LODN4",
               "BLRM7","DUBW4","PGEN8",
               "KALI4","HWYM5","HHTM4",
               "NRWM3","CENV1","CHRN6",
               "FSSO3","DRSW1","WGCM8",
               "DLYW2","DEPI3","LARO1",
               "VDSG1","CATA1","MKHF1",
               "KLGT2","ATIT2","SDAT2",
               "COMP4","MOCP4","NGKP4",
               "HLEH1","WLUH1","WHSH1")
# ----- grab comids using a method proposed by Mike Johnson
d <- readr::read_csv('https://water.noaa.gov/resources/downloads/reports/nwps_all_gauges_report.csv') |>
  janitor::clean_names()

comids <- lapply(gauge_ids, function(gid) dplyr::filter(d, nws_shef_id == gid) |>
  sf::st_as_sf(coords = c('longitude', 'latitude'), crs = 4326) |>
  nhdplusTools::discover_nhdplus_id() )

comids <- base::lapply(comids, function(x) ifelse(is.null(x),yes = NA,x)) %>% unlist()

# TODO parse HADS SITES for complete site list (e.g. CSNC2)
hads <- readr::read_csv('https://hads.ncep.noaa.gov/USGS/ALL_USGS-HADS_SITES.txt') |>
  janitor::clean_names()

# -----
dt_meta_noaa <- proc.attr.hydfab::retr_noaa_gauges_meta(gauge_ids =gauge_ids,
                                                        retr_ids = c("lid","usgsId","name","latitude","longitude"))
# for some reason, CSNC2 is not detected in the database. USGS-07103700
dt_meta_noaa[['usgsId']][dt_meta_noaa$lid == "CSNC2"] <- "07103700"
dt_meta_noaa[['name']][dt_meta_noaa$lid == "CSNC2"] <- "Fountain Creek Near Colorado Springs CO"
dt_meta_noaa$comid <- comids

# -----
# TODO build this based on
# Mapping the geopackage file locations to their postal id & expected CRS
hfab_srce_map <- data.table::data.table(domain = c("AK","PR","VI","HI"),
           paths = c("~/noaa/hydrofabric/v2.2/ak_nextgen.gpkg",
                     "~/noaa/hydrofabric/v2.2/prvi_nextgen.gpkg",
                     "~/noaa/hydrofabric/v2.2/prvi_nextgen.gpkg",
                     "~/noaa/hydrofabric/v2.2/hi_nextgen.gpkg"),
           crs_epsg = c(3338,32161,32161,102007) # just in case can't be automatically detected with sf
           )



# ################### SEARCH FOR MISSING HYDROFABRIC IDS #######################
# TODO find hydrofabric id:
if (base::any(base::is.na(dt_meta_noaa$comid))){
  dt_need_hf <- dt_meta_noaa[base::which(base::is.na(dt_meta_noaa$comid)),]

  # Identify postal code and gpkg mappings:
  dt_need_hf <- proc.attr.hydfab::map_hfab_oconus_sources_wrap(dt_need_hf,
                                                               hfab_srce_map)


  # TODO group by geopackage, then read in that geopackage once and operate over it
  ######### Find which hydrofabric geopackages correspond to locations of interest
  # post_ids <- list()
  # for(i in base::nrow(dt_need_hf)){
  #     lat <- dt_need_hf$latitude[i]
  #     lon <- dt_need_hf$longitude[i]
  #     if(base::any(base::is.na(base::c(lat,lon)))){
  #       warning(glue::glue("Lat/Lon unavailable for {gage_id}"))
  #     } else {
  #       # Figure out which geopackage to use based on lat/lon
  #       postal_id <- get_state_or_territory(lat=lat,lon=lon)
  #       post_ids[[i]] <- postal_id
  #     }
  # }
  # post_ids <- base::lapply(1:base::nrow(dt_need_hf), function(i)
  #   get_state_or_territory(dt_need_hf$latitude[i],dt_need_hf$longitude[i])) %>%
  #   base::unlist()
  #
  # dt_need_hf$domain <- post_ids
  #
  # dt_need_hf_mrge <- base::merge(dt_need_hf, hfab_srce_map, by = "domain",
  #                                all.x=TRUE,all.y=FALSE,sort=FALSE)
  ########### Acquire hydrofabric ID



retr_hfab_id_wrap <- function(dt_need_hf, col_gpkg_path = "paths",
                              col_usgsId = "usgsId",col_lon= 'longitude',
                              col_lat= 'latitude',epsg_coords=4326
                              ){
  #' @title Retrieve hydrofabric IDs
  #' @details Intended for situations when comids unavailable, generally as OCONUS
  #' @param dt_need_hf data.table of needed
  #' @param col_gpkg_path column name inside `dt_need_hf` for hydrofabric
  #' geopackage filepaths
  #' @param col_usgsId column name inside `dt_need_hf` for the USGS gage ID
  #' @param col_lon column name inside `dt_need_hf` for longitude value
  #' @param col_lat column name inside `dt_need_hf` for latitude value
  #' @param epsg_coords The CRS for the lat/lon data insed `dt_need_hf
  #' @export

  # Check to ensure expected columns
  need_colnames <- base::c(col_gpkg_path,col_usgsId, col_lat, col_lon)
  if(!base::all(need_colnames %in% base::colnames(dt_need_hf))){
    need_colnames <- need_colnames[base::which(!need_colnames %in%
                                                 base::names(dt_need_hf))] %>%
      base::paste0(collapse = "\n")
    stop(glue::glue("dt_need_hf does not contain the expected column
                    {need_colnames}. Check map_hfab_oconus_sources_wrap()."))
  }


  # Grouping by paths so we only read in each gpkg once:
  uniq_paths <- base::unique(dt_need_hf[[col_gpkg_path]])[!is.na(unique(dt_need_hf[[col_gpkg_path]]))]
  ls_sub_gpgk_need_hf <- list()
  for (path_gpkg in uniq_paths){

    if(!base::file.exists(path_gpkg)){
      stop(glue::glue("The desired gpkg does not exist: {path_gpkg}.
                      Revisit `hfab_srce_map` mappings."))
    }

    # # Implement a correction to vpu ID for Alaska...
    # if("ak" %in% base::basename(path_gpkg)){
    #
    # }

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
        # Note that hfsubsetR v0.3.3 needs OCONUS lat/lon search added per issue # https://github.com/owp-spatial/hfsubsetR/issues/6
        # The following is a temporary (?) workaround

        # Get the coordinates
        # sub_fp$geom[[1]][c(1,2)]
        # length(sub_fp$geom[[1]])
        # sub_fp <- flowpaths[flowpaths$id == hf_ids$id,]
        epsg_domn <- sf::st_crs(flowpaths$geom)$epsg # The CRS of this hydrofabric domain
        if(base::is.na(epsg_domn)){ # Use the manual mapping CRS as plan B
          epsg_domn <- hfab_srce_map$crs_epsg[hfab_srce_map$paths==path_gpkg] %>% unique()
          if(length(epsg_domn)!=1){
            stop(glue::glue("Need to define epsg for {path_gpkg}"))
          }
        }

        # if(base::is.na(epsg_domn)){
        #   divides <- proc.attr.hydfab::read_hfab_layer(path_gpkg=path_gpkg,layer = "nexus")
        #   epsg_domn <- sf::st_crs(divides$geom)$epsg
        # }

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
        warning(glue::glue("UNABLE TO DETERMINE HYDROFABRIC LOCATION FOR {sub_dt_need_gpgk$name[[i]]}"))
        hf_id <- NA

      } else {
        warning(glue::glue("UNABLE TO DETERMINE HYDROFABRIC LOCATION FOR {sub_dt_need_gpgk$name[[i]]}"))
        hf_id <- NA
      }
      hf_ids[[i]] <- hf_id


    } # End for loop over each location
    sub_dt_need_gpkg$hf_id <- hf_ids %>% base::unlist()

    ls_sub_gpgk_need_hf[[path_gpkg]] <- sub_dt_need_gpkg
  } # End for loop over unique gpkg paths

  dt_need_hf_nomor <- data.table::rbindlist(ls_sub_gpgk_need_hf)



  return(dt_need_hf_nomor)
}
dt_have_hf <- retr_hfab_id_wrap(dt_need_hf, col_gpkg_path = "paths",
                              col_usgsId = 'usgsId',col_lon= 'longitude',
                              col_lat= 'latitude',epsg_coords=4326)

  # # TODO for NA lat/lon, search based on usgsId (USE existing)
  # proc.attr.hydfab::retr_comids(gage_ids = dt_need_hf$usgsId,
  #                               dir_db_attrs = "/Users/guylitt/noaa/data/test/oconus_attrs",
  #                               featureSource="nwissite", featureID = "USGS-{gage_id}")
  # # First, try the comid:
  # nhdplusTools::get_nhdplus()




  for(i in 1:base::nrow(dt_need_hf)){





  }

        # The mapped geopackage based on postal_ids
        gpkg <- hfab_srce_map$paths[hfab_srce_map$domain == postal_id]

        # Read in the geopackage layers to identify location/retrieve hydrofabric ID
        ntwk <- sf::st_read(gpkg,layer = "network") %>% base::suppressWarnings()






  #} # end loop over each location ID
  xy <- c(lon,lat)

  xy <- c(-149.098220, 61.618974)
  # FROM MIKE JOHNSON:
  # Create and transform pt to CRS used in AK (we hope to automate this detection)

  #divides <- sf::read_sf(gpkg,layer= "divides")




  {
    base::plot(ss$divides$geom)
    base::plot(ss$flowpaths$geom, add = TRUE, col = "blue")
    base::plot(ss$nexus$geom,     add = TRUE, col = "red", pch = 16)
  }


  #
  # if(any(is.null(dt_need_hf$usgsId))){
  #
  #
  # } else { # USE USGS-gage-id
  #   for (gage_id in dt_need_hf$usgsId){
  #
  # }

      # # TODO see what teh coordinates look like
      #
      #
      # nhdplusTools::get_nhdplus(comid="44744")
      #
      # nldi_feat <- list(featureSource = 'nwissite',
      #                   featureID = gid_char)
      # rslt <- hfsubsetR::get_subset(nldi_feature = nldi_feat,gpkg = gpkg,lyrs = "network")
      # rslt <- hfsubsetR::get_subset(id=dt_need_hf$lid[dt_need_hf$usgsId == gage_id], gpkg = gpkg,lyrs = "network")
      # rslt <- hfsubsetR::get_subset(poi_id=dt_need_hf$lid[dt_need_hf$usgsId == gage_id], gpkg = gpkg,lyrs = "network")
      # rslt <- hfsubsetR::get_subset(xy=c(lon,lat), gpkg = gpkg,lyrs = "network")
      # rslt <- hfsubsetR::get_subset(xy=c(lon,lat), gpkg = gpkg,lyrs = "flowpaths")
      # rslt <- hfsubsetR::get_subset(id=dt_need_hf$lid[dt_need_hf$usgsId == gage_id], gpkg = gpkg,lyrs = "network")
      #
      #
      # ak_fps <- sf::st_read(gpkg,layer = "flowpaths")
      # ak_divs <- sf::st_read(gpkg,layer = "divides")
      # ak_nex <- sf::st_read(gpkg,layer="nexus")
      # ak_nex
      # # Nearest point:
      # hfsubsetR::find_origin(network=ak_ntwk,id=c(lon,lat), type = "xy")
      # hfsubsetR::find_origin(network=ak_fps,id=c(lon,lat), type = "xy")
      #
      # "Gages-"
      #
      # #"Gages-11123000")
      #
      # nhdplusTools::discover_nhdplus_id(point = sf::st_sfc(sf::st_point(xy), crs = 4326))
      #
      # ls_pts
      # for(i in 1:nrow(ak_fps)){
      #   pt <- sf::st_nearest_points(c(lat,lon), ak_fps$geom)
      # }
      #
      # ak_gpkg <-
      # ak_gpkg %>% tail()
      # dta <- dplyrTailcall()dta <- dplyr::tbl(gpkg)
      #
  }


}



# TODO use id or lat/lon to figure out which OCONUS dataset contains it


# TODO use hydrofabric id as comid identifier







# Now grab attributes for these watersheds
path_attr_config <- "~/git/formulation-selector/scripts/analysis/benchmarking/oconus_attrs.yaml"
Retr_Params <- proc.attr.hydfab::attr_cfig_parse(path_attr_config)

proc.attr.hydfab::fs_attrs_miss_mlti_wrap(path_attr_config)


#%%
# PROCESS ATTRIBUTES
dt_comids <- proc.attr.hydfab:::grab_attrs_datasets_fs_wrap(Retr_Params,overwrite = FALSE)

# --------------------------- Compile attributes --------------------------- #
# Demonstration of how to retrieve attributes/comids that exist inside dir_db_attrs:
demo_example <- FALSE
if (demo_example){
  # The comids of interest
  comids <- dt_comids$featureID %>% base::unname() %>% base::unlist()

  # The attribute variables of interest
  vars <- Retr_Params$vars %>% base::unlist() %>% base::unname()

  dat_all_attrs <- proc.attr.hydfab::retrieve_attr_exst(comids, vars,
                                                        Retr_Params$paths$dir_db_attrs)
  base::rm(dat_all_attrs)

}

