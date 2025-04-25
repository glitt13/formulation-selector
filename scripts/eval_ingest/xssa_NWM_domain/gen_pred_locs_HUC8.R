# Call in the package
library(proc.attr.hydfab)
library(tidyverse)
library(dataRetrieval)

# Set important params (Retr_Params) ----------------------------------
home_dir <- Sys.getenv("HOME")
path_cfig_pred <- glue::glue("{home_dir}/Lauren/FSDS/formulation-selector/scripts/eval_ingest/xssa_NWM_domain/xssanwm_pred_config.yaml")
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


# ------------------------ ATTRIBUTE CONFIGURATION --------------------------- #
# READ IN ATTRIBUTE CONFIG FILE
name_attr_config <- cfig_pred[['name_attr_config']]
path_attr_config <- proc.attr.hydfab::build_cfig_path(path_cfig_pred,name_attr_config)

Retr_Params <- proc.attr.hydfab::attr_cfig_parse(path_attr_config)


# Get COMIDS -------------------------------------
home_dir <- Sys.getenv("HOME")

# COMID for most downstream portion of flowpath intersecting HUC08 boundaries
lowest_hs <- read.csv(glue('{home_dir}/Lauren/regionalization/comids_lowest_hf_hydroseq.csv'))

# COMID for most upstream portion of the flowpath intersecting HUC08 boundaries
highest_hs <- read.csv(glue('{home_dir}/Lauren/regionalization/comids_highest_hf_hydroseq.csv'))

comids <- unique(highest_hs$hf_id)

# Grab attributes ------------------------------
# For some reason, running the following got this working
library(future)
library(future.apply)
dt_site_feat <- proc_attr_mlti_wrap(comids = comids, Retr_Params = Retr_Params,
                    lyrs = "network", overwrite = FALSE)
# Run this^^, run attribute transformation, and then run it again???


for(ds in datasets){
  path_nldi_out <- glue::glue(path_meta)

  proc.attr.hydfab::write_meta_nldi_feat(dt_site_feat=dt_site_feat,
                                         path_meta = path_nldi_out)
}
