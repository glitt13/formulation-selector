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


# Populate standardized featureID and featureSource unique identifiers
dt_meta_noaa <- proc.attr.hydfab::std_feat_id(df=dt_meta_noaa,
                                                  name_featureSource ="COMID",
                                                  vals_featureID = comids)


# -----
# TODO build this map based on config file rather than hard-code it
# Mapping the geopackage file locations to their postal id & expected CRS
hfab_srce_map <- data.table::data.table(domain = c("AK","PR","VI","HI"),
           paths = c("~/noaa/hydrofabric/v2.2/ak_nextgen.gpkg",
                     "~/noaa/hydrofabric/v2.2/prvi_nextgen.gpkg",
                     "~/noaa/hydrofabric/v2.2/prvi_nextgen.gpkg",
                     "~/noaa/hydrofabric/v2.2/hi_nextgen.gpkg"),
           crs_epsg = c("EPSG:3338","EPSG:6566",
                        "EPSG:6566","ESRI:102007") # just in case can't be automatically detected with sf
           )

# ################### SEARCH FOR MISSING HYDROFABRIC IDS #######################

if (base::any(base::is.na(dt_meta_noaa$featureID))){
  dt_need_hf <- dt_meta_noaa[base::which(base::is.na(dt_meta_noaa$featureID)),]

  # Identify postal code and gpkg mappings:
  dt_need_hf <- proc.attr.hydfab::map_hfab_oconus_sources_wrap(dt_need_hf,
                                                               hfab_srce_map)

  # Retrieve the hydrofabric IDs wrapper for needed locations
  dt_have_hf <- retr_hfab_id_wrap(dt_need_hf, hfab_srce_map, col_gpkg_path = "paths",
                                  col_usgsId = 'usgsId',col_lon= 'longitude',
                                  col_lat= 'latitude',epsg_coords=4326)

  # Reconcile the missing data

}



# df <- arrow::read_parquet("~/noaa/regionalization/data/input/attributes/comid_10023916_attrs.parquet")

df



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

