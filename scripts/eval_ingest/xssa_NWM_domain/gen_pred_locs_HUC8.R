# proc_attr_mlti_wrap

# Call in the package
library(proc.attr.hydfab)
library(tidyverse)
library(dataRetrieval)

# Set important params (Retr_Params) ----------------------------------
home_dir <- Sys.getenv("HOME")
path_cfig_pred <- glue::glue("{home_dir}/Lauren/FSDS/formulation-selector/scripts/eval_ingest/xssa_us/xssaus_pred_config.yaml")
subsamp_n <- 20 # how is this decided? see how this impacts processing
subsamp_seed <- 432

# Read in config file
if(!base::file.exists(path_cfig_pred)){
  stop(glue::glue("The provided path_cfig_pred does not exist: {path_cfig_pred}"))
}


cfig_pred <- yaml::read_yaml(path_cfig_pred)
ds_type <- base::unlist(cfig_pred)[['ds_type']]
write_type <- base::unlist(cfig_pred)[['write_type']]
path_meta <- base::unlist(cfig_pred)[['path_meta']] # The filepath of the file that generates the list of comids used for prediction

# READ IN ATTRIBUTE CONFIG FILE
path_attr_config <- glue::glue(cfig_pred[['path_attr_config']])
cfig_attr <- yaml::read_yaml(path_attr_config)

# Defining directory paths as early as possible:
io_cfig <- cfig_attr[['file_io']]
dir_base <- glue::glue(base::unlist(io_cfig)[['dir_base']])
dir_std_base <- glue::glue(base::unlist(io_cfig)[['dir_std_base']])
dir_db_hydfab <- glue::glue(base::unlist(io_cfig)[['dir_db_hydfab']])
dir_db_attrs <- glue::glue(base::unlist(io_cfig)[['dir_db_attrs']])

# ------------------------ ATTRIBUTE CONFIGURATION --------------------------- #
hfab_cfg <- cfig_attr[['hydfab_config']]

names_hfab_cfg <- unlist(lapply(hfab_cfg, function(x) names(x)))
names_attr_sel_cfg <- unlist(lapply(cfig_attr[['attr_select']], function(x) names(x)))
s3_base <- glue::glue(base::unlist(hfab_cfg)[['s3_base']]) # s3 path containing hydrofabric-formatted attribute datasets
s3_bucket <- glue::glue(base::unlist(hfab_cfg)[['s3_bucket']]) # s3 bucket containing hydrofabric data
s3_path_hydatl <- glue::glue(unlist(cfig_attr[['attr_select']])[['s3_path_hydatl']]) # path to hydroatlas data formatted for hydrofabric

form_cfig <- cfig_attr[['formulation_metadata']]
datasets <- form_cfig[[grep("datasets",form_cfig)]]$datasets

# Additional config options
hf_cat_sel <- base::unlist(hfab_cfg)[['hf_cat_sel']]#c("total","all")[1] # total: interested in the single location's aggregated catchment data; all: all subcatchments of interest

# The names of attribute datasets of interest (e.g. 'ha_vars', 'usgs_vars', etc.)
names_attr_sel <- base::lapply(cfig_attr[['attr_select']],
                               function(x) base::names(x)[[1]]) %>% unlist()

# Generate list of standard attribute dataset names containing sublist of variable IDs
ls_vars <- names_attr_sel[grep("_vars",names_attr_sel)]
vars_ls <- base::lapply(ls_vars, function(x) base::unlist(base::lapply(cfig_attr[['attr_select']], function(y) y[[x]])))
names(vars_ls) <- ls_vars

# The attribute retrieval parameters
Retr_Params <- list(paths = list(# Note that if a path is provided, ensure the
  # name includes 'path'. Same for directory having variable name with 'dir'
  dir_db_hydfab=dir_db_hydfab,
  dir_db_attrs=dir_db_attrs,
  s3_path_hydatl = s3_path_hydatl,
  dir_std_base = dir_std_base,
  path_meta=path_meta),
  vars = vars_ls,
  datasets = datasets,
  ds_type = ds_type,
  write_type = write_type
)

# Get COMIDS -------------------------------------
home_dir <- Sys.getenv("HOME")
path_cfig_pred <- glue::glue("{home_dir}/Lauren/FSDS/formulation-selector/scripts/eval_ingest/xssa_us/xssaus_pred_config.yaml")

### intersect approach -----
file_list <- list.files(glue::glue("{home_dir}/Lauren/regionalization"), pattern = '*_hf_huc8_int.csv', full.names = TRUE)
# Remove oCONUS files, these will need to be processed separately
file_list <- file_list[!grepl("ak_", file_list)]

# Read in all these files and merge into one dataframe 
huc08_int_df <- do.call(rbind, lapply(file_list, function(file) {
  read.csv(file)
}))
huc08_int_df <- huc08_int_df %>% rename(id = hf_id) # Change this earlier on to avoid confusion

# Read in the CONUS hydrofab so we can get hydrofabr IDs/COMIDs for these nextgen hydrofabric IDs
hf_conus <- sf::st_read(glue::glue('{home_dir}/Lauren/hydrofabric/data/v2.2/conus_nextgen.gpkg'), layer = 'network')

# Subset for the IDs of interest
hf_conus <- hf_conus %>% 
  filter(id %in% huc08_int_df$id) 

# # See what is in hf_conus$id that is not in huc08_int_df$id
# hf_conus %>% 
#   filter(!id %in% huc08_int_df$id) %>% 
#   select(id) %>% 
#   distinct() %>% 
#   arrange(id)
# 
# # See vice versa
# huc08_int_df %>% 
#   filter(!id %in% hf_conus$id) %>% 
#   select(id) %>% 
#   distinct() %>% 
#   arrange(id)

# Join the two dataframes to get the COMIDs
# merged_df <- merge(huc08_int_df, hf_conus, by = "id")

# For each id, subset merged_df for the row where the hf_hydroseq is the lowest
# This allows us to get the hf_id/COMID for the most downstream stream segment
hf_conus_sub <- hf_conus %>%
  filter(id %in% huc08_int_df$id) 

hf_conus_sub <- hf_conus_sub %>%
  group_by(id) %>%
  slice(which.min(hf_hydroseq)) %>%
  ungroup()

# # Print the maximum number of times any "id" shows up in huc08_int_df
# max_count <- max(table(huc08_int_df$id))
# 
# # Identify which ones have this many entries
# ids_with_max_count <- names(which(table(huc08_int_df$id) == max_count))

comids <- unique(hf_conus_sub$hf_id)

# Grab attributes ------------------------------
# For some reason, running the following got this working
#library(future)?
#library(future.apply)
dt_site_feat <- proc_attr_mlti_wrap(comids = comids, Retr_Params = Retr_Params, 
                    lyrs = "network", overwrite = FALSE)


for(ds in datasets){
  path_nldi_out <- glue::glue(path_meta)
  
  proc.attr.hydfab::write_meta_nldi_feat(dt_site_feat=dt_site_feat,
                                         path_meta = path_nldi_out)
}
# ### hydrofabric subsetting approach -----
# # Read in the hydrofabric network table
# hf_conus <- sf::st_read(glue::glue('{home_dir}/Lauren/hydrofabric/data/v2.2/conus_nextgen.gpkg'), layer = 'network')
# 
# # Subset for those with a hl_uri that starts with "huc12"
# hf_conus <- hf_conus %>% 
#   filter(grepl("huc12", hl_uri))
# 
# # Create a new column that is huc8 (the first 8 digits of the huc12)
# hf_conus$huc8 <- substr(hf_conus$hl_uri, 7, 14)
# 
# # Confirm why a given id has multiple hf_ids (COMIDs)
# dat <- get_nhdplus(comid = c(22307225, 22307223, 22307091))
# plot(dat)
# mapview(dat)
# # Do this again in mapview but use a different color for each comid, which is discrete
# mapview(dat, zcol = "comid", legend = FALSE)+
#   mapview(conus_flowpaths_sub)
# 
# hf_conus_test_ids <- hf_conus %>% 
#   filter(hf_id %in% c(22307225, 22307223, 22307091))
# hf_conus_test_ids <- hf_conus_test_ids$id %>% unique()
# 
# # Get the flowpath layer
# conus_flowpaths <- st_read(glue::glue('{home_dir}/Lauren/hydrofabric/data/v2.2/conus_nextgen.gpkg'), layer = 'flowpaths')
# conus_flowpaths_sub <- conus_flowpaths %>% filter(id %in% hf_conus_test_ids)
# 
# mapview(conus_flowpaths_sub)
# 
# 
# hu <- get_huc(sf::st_sfc(sf::st_point(c(-73, 42)), crs = 4326),
#               type = "huc08")
# 
# download_nhd('/Users/laurenbolotin/Downloads/', c('02', "0003"), download_files = FALSE)
# 
# # Plot all 3
# plot_nhdplus(list("comid", "22307225"),
#              # streamorder = 2,
#              nhdplus_data = dat,
#              plot_config = list(basin = list(lwd = 2),
#                                 outlets = list(huc12pp = list(cex = 1.5),
#                                                comid = list(col = "green"))))
# plot_nhdplus(list(list("comid", "22307225"),
#                   list("comid", "22307223"),
#                   list("comid", "22307091")),
#              # streamorder = 2,
#              nhdplus_data = sample_data,
#              plot_config = list(basin = list(lwd = 2),
#                                 outlets = list(huc12pp = list(cex = 1.5),
#                                                comid = list(col = "green"))))