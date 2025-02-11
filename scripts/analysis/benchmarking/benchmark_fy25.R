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
path_oconus_config <- "~/git/formulation-selector/scripts/eval_ingest/bm_test25/bm_oconus_config.yaml"
path_nwps_rpt  <- 'https://water.noaa.gov/resources/downloads/reports/nwps_all_gauges_report.csv'
path_hads_sites <- 'https://hads.ncep.noaa.gov/USGS/ALL_USGS-HADS_SITES.txt'
path_nwps_api <- "https://api.water.noaa.gov/nwps/v1/gauges"

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

proc.attr.hydfab::retr_comids()
proc.attr.hydfab::comid
# ----- grab comids using a method proposed by Mike Johnson


#### NWPS data
# NOTE: NWPS gauges not comprehensive
d_nwps <- proc.attr.hydfab::read_noaa_nwps_gauges(
  path_nwps_rpt =path_nwps_rpt)

comids_nwps <- base::lapply(gauge_ids,
                            function(gid) dplyr::filter(d_nwps, nws_shef_id == gid) |>
  sf::st_as_sf(coords = c('longitude', 'latitude'), crs = 4326) |>
  nhdplusTools::discover_nhdplus_id() )

##### HADS DATA RETRIEVAL/FILTERING
# NOTE: HADS data are comprehensive. Read all data.
dt_hads <- proc.attr.hydfab::read_noaa_hads_sites(
  path_hads_sites=path_hads_sites)

# Subset HADS to NOAA gauge_ids of interest
dt_hads_sub <- base::lapply(gauge_ids,
                       function(gid) dt_hads %>% dplyr::filter(lid==gid)) %>%
                        data.table::rbindlist()
# # Create geometry from lat/lon
# if(length(unique(dt_hads_noaa$crs))==1){
#   dt_hads_noaa <- dt_hads_noaa %>%
#     sf::st_as_sf(coords = c('longitude', 'latitude' ),crs=unique(dt_hads_noaa$crs))
# } else {
#   stop("Not expecting >1 crs")
# }

# comids_hads <- base::lapply(dt_hads_noaa, function(x)
#   nhdplusTools::discover_nhdplus_id(point=sf::st_as_sf(x[[c("longitude","latitude")]],
#                                                        crs=4326)) )


# Retrieve comids based on the NOAA location id, `lid``
comids_hads <- base::lapply(dt_hads_sub$lid,
            function(gid) dplyr::filter(dt_hads_sub, lid == gid) |>
              sf::st_as_sf(coords = c('longitude', 'latitude'), crs = 4326)  %>%
              nhdplusTools::discover_nhdplus_id() )
if(base::all(base::is.na(comids_hads))){
  warning("POTENTIAL PROBLEM WITH nhdplusTools::discover_nhdplus_id():
  No comids returned. This may relate to database connection limits or a bad connection.")

  # Now try to acquire comids via USGS gage Ids
  comids_hads <- base::lapply(dt_hads_sub$usgsId,function(gid)
    nhdplusTools::discover_nhdplus_id(nldi_feature = c(featureSource='nwissite',featureID=paste0("USGS-",gid))) )
  if(base::any(!base::is.na(comids_hads))){
    base::message("comids successfully discovered using USGS gage IDs")
  }
} else if (base::any(base::is.na(comids_hads))){
  # Now try to acquire comids via USGS gage Ids
  comids_hads_redo <- base::lapply(dt_hads_sub$usgsId,function(gid)
    nhdplusTools::discover_nhdplus_id(nldi_feature = c(featureSource='nwissite',featureID=paste0("USGS-",gid))) )
  if(base::any(!base::is.na(comids_hads))){
    base::message("comids successfully discovered using USGS gage IDs")
  }
  # TODO compare comids_hads and comids_hads_redo to see if any additional comids found
}



# organize the comids into NA-filled vector
comids <- base::lapply(comids_hads, function(x) base::ifelse(base::is.null(x),yes = NA,x)) %>%
  base::unlist()


# Populate standardized featureID and featureSource unique identifiers
dt_meta_noaa <- proc.attr.hydfab::std_feat_id(df=dt_hads_sub,
                                              name_featureSource ="COMID",
                                              vals_featureID = comids)




# -----
testing_nwps_api = FALSE
if(testing_nwps_api){
  dt_meta_noaa <- proc.attr.hydfab::retr_noaa_gauges_meta(gauge_ids =gauge_ids,
                                                          gauge_url_base = path_nwps_api,
                                                          retr_ids = c("lid","usgsId","name","latitude","longitude"))

  dt_meta_noaa_sub_no_coords <- dt_meta_noaa[base::which(is.na(dt_meta_noaa$latitude)),]


  dt_hads_has_sub <- base::lapply(dt_meta_noaa_sub_no_coords$lid,
                                  function(id) dt_hads %>% dplyr::filter(lid == id)) %>%
    data.table::rbindlist()

  names(dt_)


  if(base::any(base::is.na(dret_meta_noaa$latitude))){

  }

  # for some reason, CSNC2 is not detected in the database. USGS-07103700
  dt_meta_noaa[['usgsId']][dt_meta_noaa$lid == "CSNC2"] <- "07103700"
  dt_meta_noaa[['name']][dt_meta_noaa$lid == "CSNC2"] <- "Fountain Creek Near Colorado Springs CO"


  # Populate standardized featureID and featureSource unique identifiers
  dt_meta_noaa <- proc.attr.hydfab::std_feat_id(df=dt_meta_noaa,
                                                name_featureSource ="COMID",
                                                vals_featureID = comids)

}



# -----
# TODO build this map based on config file rather than hard-code it
# Mapping the geopackage file locations to their postal id & expected CRS
# hfab_srce_map <- data.table::data.table(domain = c("AK","PR","VI","HI"),
#            paths = c("~/noaa/hydrofabric/v2.2/ak_nextgen.gpkg",
#                      "~/noaa/hydrofabric/v2.2/prvi_nextgen.gpkg",
#                      "~/noaa/hydrofabric/v2.2/prvi_nextgen.gpkg",
#                      "~/noaa/hydrofabric/v2.2/hi_nextgen.gpkg"),
#            crs_epsg = c("EPSG:3338","EPSG:6566",
#                         "EPSG:6566","ESRI:102007") # just in case can't be automatically detected with sf
#            )

# ################### SEARCH FOR MISSING HYDROFABRIC IDS #######################

if (base::any(base::is.na(dt_meta_noaa$featureID))){
  dt_need_hf <- dt_meta_noaa[base::which(base::is.na(dt_meta_noaa$featureID)),]

  # Retrieve the hydrofabric IDs wrapper for needed locations
  dt_have_hf <- proc.attr.hydfab::retr_hfab_id_wrap(dt_need_hf, path_oconus_config,
                                  col_usgsId = 'usgsId',col_lon= 'longitude',
                                  col_lat= 'latitude',epsg_coords=4326)

  # Subset to the new columns of interest (plus the common ID column usgsId)
  sub_dt_have_hf <- dt_have_hf %>% dplyr::select(c("usgsId",featureID, featureSource))

  # Fill in the missing NA values for featureID and featureSource columns
  dt_meta_noaa <- dt_meta_noaa %>% dplyr::left_join(sub_dt_have_hf, by = "usgsId",
                                               suffix=c("_df1","_df2")) %>%
    dplyr::mutate(featureID = ifelse(is.na(featureID_df1),featureID_df2, featureID_df1),
    featureSource = ifelse(is.na(featureSource_df1),featureID_df2, featureID_df1)
    ) %>%
    dplyr::select(-dplyr::all_of(
      base::c("featureID_df1",'featureID_df2',
              'featureSource_df1','featureSource_df2')))
}

# df <- arrow::read_parquet("~/noaa/regionalization/data/input/attributes/comid_10023916_attrs.parquet")



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

