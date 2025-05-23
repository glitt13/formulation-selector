#' @title Unit test attribute grabber processor
#' @description Unit testing for catchment attribute grabbing via the hydrofabric
#' @author Guy Litt \email{guy.litt@noaa.gov}
#' @note When running this script, be sure to also source tests/testthat/setup.R first
# Changelog / Contributions
#   2024-07-24 Originally created, GL
#   2024-10-03 Contributed to, LB

# unloadNamespace("proc.attr.hydfab")
suppressPackageStartupMessages(library(proc.attr.hydfab,quietly=TRUE))
suppressPackageStartupMessages(library(testthat,quietly=TRUE))
suppressPackageStartupMessages(library(mockery,quietly=TRUE))
suppressPackageStartupMessages(library(dplyr,quietly=TRUE))
suppressPackageStartupMessages(library(arrow,quietly=TRUE))
suppressPackageStartupMessages(library(hydrofabric,quietly=TRUE))
suppressPackageStartupMessages(library(data.table,quietly=TRUE))
suppressPackageStartupMessages(library(fs,quietly=TRUE))
suppressPackageStartupMessages(library(sf,quietly=TRUE))
suppressPackageStartupMessages(library(future,quietly=TRUE))
suppressPackageStartupMessages(library(future.apply,quietly=TRUE))
suppressPackageStartupMessages(library(pkgcond,quietly=TRUE))
options(arrow.unsafe_metadata = TRUE)


# TODO establish a basic config file to read in for this functionality
comid <- "18094981"#"02479560"#14138870# A small basin
s3_base <- "s3://lynker-spatial/tabular-resources"
s3_bucket <- 'lynker-spatial'
s3_path_hydatl <- glue::glue('{s3_base}/hydroATLAS/hydroatlas_vars.parquet')

# Testing variables
# ha_vars <- c('pet_mm_s01', 'cly_pc_sav', 'cly_pc_uav') # hydroatlas variables
# usgs_vars <- c('TOT_TWI','TOT_PRSNOW','TOT_POPDENS90','TOT_EWT','TOT_RECHG')

# Define data directories to a package-specific data path
dir_base <- system.file("extdata",package="proc.attr.hydfab")
# Refer to temp_dir <- tempdir() in setup.R
temp_dir <- local_temp_dir() # If running this on your own, source 'setup.R' first.
dir_db_hydfab <- file.path(temp_dir,'hfab')
path_meta <- paste0(temp_dir,"/{ds}/nldi_feat_{ds}_{ds_type}.{write_type}")
dir_db_attrs <- file.path(temp_dir,'attrs') # used for temporary attr retrieval
dir_db_attrs_pkg <- system.file("extdata","attributes_pah",package="proc.attr.hydfab")# permanent pacakage location
dir_user <- system.file("extdata","user_data_std", package="proc.attr.hydfab") # dir_user <- "~/git/fsds/pkg/proc.attr.hydfab/inst/extdata/user_data_std/"
dir_dataset <- file.path(dir_user,'xssa-mini')
path_mini_ds <- file.path(dir_dataset,'xSSA-mini_Raven_blended.nc')

ls_fs_std <- proc.attr.hydfab::proc_attr_read_gage_ids_fs(dir_dataset)

ha_vars <- c('pet_mm_s01', 'cly_pc_sav')#, 'cly_pc_uav') # hydroatlas variables
sc_vars <- c() # TODO look up variables. May need to select datasets first
usgs_vars <- c('TOT_TWI','TOT_PRSNOW')#,'TOT_POPDENS90','TOT_EWT','TOT_RECHG')

Retr_Params <- list(paths = list(dir_db_hydfab=dir_db_hydfab,
                                 dir_db_attrs=dir_db_attrs,
                                 s3_path_hydatl = s3_path_hydatl,
                                 dir_std_base = dir_user,
                                 path_meta=path_meta),
                    vars = list(usgs_vars = usgs_vars,
                                ha_vars = ha_vars),
                    datasets = 'xssa-mini',
                    write_type = 'parquet',
                    ds_type = 'training',
                    xtra_hfab = list(hf_version = "2.1.1",
                                     hfab_retr = FALSE,
                                     type='nextgen',
                                     domain='conus'
                                     ))

ignore_deprecated_tests <- TRUE # Tests built for functions now deprecated
# ---------------------------------------------------------------------------- #
#                              UNIT TESTING
# ---------------------------------------------------------------------------- #


testthat::test_that("tform_cfig_parse", {
  # Define path to yaml
  path_tform_config <- base::file.path(dir_base,"test_config","tform_config.yaml")
  # NOTE: EDIT TEST FOR build_cfig_path IF THIS ^^ CHANGES!!

  # Run the function
  parsed_config <- proc.attr.hydfab::tform_cfig_parse(path_tform_config)

  # Check that the function returns a list with expected names
  testthat::expect_type(parsed_config, "list")
  testthat::expect_named(parsed_config, c("file_io", "transform_attrs"))

  # Check transform_attrs section
  testthat::expect_identical(c("TOT_PROGLACIAL_SED_{tform_type}",
    "TOT_GLACIAL_TILL_{tform_type}","TOT_NLCD06_FOR_{tform_type}",
    "TOT_WB5100_yr_{tform_type}","TOT_HDENS_8010_{tform_type}"),
                   names(parsed_config$transform_attrs))

})

testthat::test_that("build_cfig_path",{

  path_attr_config <- base::file.path(dir_base,"test_config","attr_config.yaml")
  rslt <- proc.attr.hydfab::build_cfig_path(path_known_config = path_attr_config,
                                            path_or_name_cfig = "tform_config.yaml")
  testthat::expect_true(base::file.exists(rslt))
  testthat::expect_true(base::dirname(rslt) == base::dirname(path_attr_config))
})

testthat::test_that("attr_cfig_parse", {
  # Define path to yaml
  path_attr_config <- base::file.path(dir_base,"test_config","attr_config.yaml")
  # NOTE: EDIT TEST FOR build_cfig_path IF THIS ^^ CHANGES!!

  # Run the function
  parsed_config <- proc.attr.hydfab::attr_cfig_parse(path_attr_config)

  # Check that the function returns a list with expected names
  testthat::expect_type(parsed_config, "list")
  testthat::expect_named(parsed_config, c("paths","vars","datasets","ds_type",
                                          "write_type"))
  # The variables from transform config should also be present
  testthat::expect_equal(length(unique(parsed_config$vars$usgs_vars) ), 72)
  testthat::expect_true(any(grepl("TOT_HDENS10",parsed_config$vars$usgs_vars)))

})


testthat::test_that("map_attrs_to_dataset", {
  # Run function using variables from hydroatlas, USGS NLDI/NHDPlus
  vars <- base::c('pet_mm_s01','TOT_PPT7100_ANN', 'TOT_BASIN_AREA')
  rslt <- proc.attr.hydfab::map_attrs_to_dataset(vars)

  testthat::expect_type(rslt, "list")
  testthat::expect_equal(names(rslt), c("ha_vars","usgs_vars"))
  testthat::expect_true(length(rslt$usgs_vars) == 2)

  # Run when a variable doesn't exist:
  # Check error when mapping is incomplete
  testthat::expect_error(proc.attr.hydfab::map_attrs_to_dataset(c("A", "B","C")),
               "Total variables in should match total variables matched.")
})


# ------------------ multi-comid attribute grabbing functions -----------------
testthat::test_that("io_attr_dat",{
  path_attr_exst <- file.path(dir_base,"attributes_pah","comid_1722317_attrs.parquet")
  df_expct <- arrow::open_dataset(path_attr_exst) %>% collect() %>%
    suppressWarnings()
  rslt <- proc.attr.hydfab::io_attr_dat(
              dt_new_dat = data.frame(),path_attrs = path_attr_exst) %>%
    suppressWarnings()
  testthat::expect_identical(dim(df_expct),dim(rslt))
  testthat::expect_identical(names(df_expct),names(rslt))
  testthat::expect_false(is.factor(rslt$attribute))

  # Adding an existing value in dt_new_dat does not create a duplicated row
  dt_new_dat <- rslt[1,]
  rslt_cmbo <- proc.attr.hydfab::io_attr_dat(
    dt_new_dat = dt_new_dat,path_attrs = path_attr_exst) %>%
    suppressWarnings()

  testthat::expect_identical(dim(rslt_cmbo),dim(rslt))
  #fs::file_delete(path_attr_exst) # Delete the file we just created
})

testthat::test_that("retr_attr_new",{
  # Test retrieving multiple comids:
  comids <- c("1520007","1623207")
  need_vars <- list(usgs_vars = c("CAT_TWI","CAT_BFI"))

  rslt <- proc.attr.hydfab::retr_attr_new(comids = comids, need_vars=need_vars,
                                          path_ha = Retr_Params$paths$s3_path_hydatl)

  testthat::expect_contains(rslt[['usgs_nhdplus__v2']]$featureID,comids)
  testthat::expect_contains(rslt[['usgs_nhdplus__v2']]$attribute,need_vars$usgs_vars)
  testthat::expect_equal(base::nrow(rslt[['usgs_nhdplus__v2']]),4)

})

testthat::test_that("check_miss_attrs_comid_io",{

  comids <- c("1520007","1623207")
  need_vars <- list(usgs_vars = c("TOT_PRSNOW","TOT_TWI"))
  Retr_Params_pkg <- Retr_Params
  Retr_Params_pkg$paths$dir_db_attrs <- dir_db_attrs_pkg
  dt_all <- proc.attr.hydfab::retr_attr_new(comids = comids, need_vars=need_vars,
                                            path_ha = Retr_Params_pkg$paths$s3_path_hydatl)[['usgs_nhdplus__v2']]
  # Add in an extra usgs var that wasn't retrieved, TOT_ELEV_MAX
  attr_vars <- list(usgs_vars = c("TOT_TWI","TOT_PRSNOW","TOT_ELEV_MAX"))
  rslt <- testthat::capture_warning(proc.attr.hydfab::check_miss_attrs_comid_io(dt_all,
                                    attr_vars,
                                    dir_db_attrs_pkg))
  testthat::expect_true(base::grepl("TOT_ELEV_MAX",rslt$message))
})


testthat::test_that("proc_attr_mlti_wrap",{

  comids <- c("1520007","1623207")
  Retr_Params_pkg <- Retr_Params
  Retr_Params_pkg$paths$dir_db_attrs <- dir_db_attrs_pkg
  dt_rslt <- suppressWarnings(proc.attr.hydfab::proc_attr_mlti_wrap(comids,
                                 Retr_Params=Retr_Params_pkg,lyrs="network",
                                  overwrite=FALSE))

  testthat::expect_true("data.frame" %in% class(dt_rslt))
  testthat::expect_true(all(comids %in% dt_rslt$featureID))
  testthat::expect_true(all(unlist(Retr_Params_pkg$vars) %in% dt_rslt$attribute))
  testthat::expect_true(all(names(dt_rslt) %in% c("data_source","dl_timestamp",
                                                  "attribute","value",
                                                  "featureID","featureSource")))

})

# ------------------------ original package functions -------------------------
testthat::test_that("write_meta_nldi_feat", {
  # TODO why does the write test fail?
  dt_site_feat <- readRDS(file.path(dir_base,"nldi_site_feat.Rds"))
  path_meta <- file.path(temp_dir, 'nldi_site_feat.parquet')

  # This first checks for whether a warning is created
  if(!base::dir.exists(dirname(path_meta))){
    warn_rslt <- testthat::capture_condition(
      proc.attr.hydfab::write_meta_nldi_feat(dt_site_feat,
                                             path_meta=path_meta))
    testthat::expect_true(grepl("expect", warn_rslt$message))
  }


  rslt <- testthat::capture_condition(
    proc.attr.hydfab::write_meta_nldi_feat(dt_site_feat,
                                           path_meta=path_meta))
  testthat::expect_true(grepl(path_meta, rslt$message))


  files_exst <- base::list.files(base::dirname(path_meta))
  testthat::expect_true(base::file.exists(path_meta))

  path_meta_csv <- base::gsub(".parquet",replacement = ".csv",x=path_meta)
  rslt_csv <- testthat::capture_condition(
    proc.attr.hydfab::write_meta_nldi_feat(dt_site_feat,
                path_meta=path_meta_csv))
  testthat::expect_true(file.exists(path_meta_csv))

  path_meta_fake_ext <- base::gsub(".parquet",replacement = ".fake",x=path_meta)
  rslt_fake <- testthat::capture_condition(
    proc.attr.hydfab::write_meta_nldi_feat(dt_site_feat,
                                           path_meta=path_meta_fake_ext))

  testthat::expect_true(grepl("extension",rslt_fake$message))
})


testthat::test_that("proc_attr_std_hfsub_name standardized name generator", {
  testthat::expect_equal('hydrofab_testit_111.parquet',
               proc.attr.hydfab:::proc_attr_std_hfsub_name(111,"testit",'parquet'))

})

testthat::test_that("read_loc_data",{
  # Read in the normal gage
  good_file <- file.path(dir_base,"gage_id_example.csv")
  gage_dat <- proc.attr.hydfab::read_loc_data(loc_id_filepath = good_file,loc_id = 'gage_id',fmt='csv')
  testthat::expect_equal(ncol(gage_dat),1)
  testthat::expect_equal(colnames(gage_dat), 'gage_id')
  testthat::expect_true('character' %in% class(gage_dat$gage_id))
  testthat::expect_true(base::substring(gage_dat$gage_id[1],1,1)== "0")

  bad_file <- file.path(dir_base,"gage_id_ex_bad.parquet")
  bad_dat <- proc.attr.hydfab::read_loc_data(loc_id_filepath = bad_file,loc_id = 'gage_id', fmt = 'parquet')
  testthat::expect_true(base::substring(
    base::as.character(bad_dat$gage_id[1]),1,1)!="0")

})

testthat::test_that('proc_attr_gageids',{

  path_meta_loc <- proc.attr.hydfab:::std_path_map_loc_ids(Retr_Params$paths$dir_db_attrs)
  if(file.exists(path_meta_loc)){
    file.remove(path_meta_loc)
  }

  # test just usgs vars
  Retr_Params_usgs <- Retr_Params_ha <- Retr_Params
  Retr_Params_usgs$vars <- list(usgs_vars = usgs_vars)
  Retr_Params_usgs$paths$dir_db_attrs <- file.path(Retr_Params$paths$dir_std_base,'../attributes_pah/')
  dt_comids <- proc.attr.hydfab::proc_attr_gageids(gage_ids=ls_fs_std$gage_ids[2],
                                      featureSource=ls_fs_std$featureSource,
                                      featureID=ls_fs_std$featureID,
                                      path_save_gpkg = NULL,
                                      Retr_Params=Retr_Params_usgs,
                                      lyrs="network",overwrite=FALSE) %>%
                pkgcond::suppress_warnings()
  testthat::expect_identical(unique(dt_comids$gage_id),ls_fs_std$gage_ids[2])
  testthat::expect_true("data.frame" %in% class(dt_comids))

  # test just hydroatlas var\
  Retr_Params_ha$vars <- list(ha_vars = ha_vars)
  path_meta_loc <- proc.attr.hydfab:::std_path_map_loc_ids(Retr_Params$paths$dir_db_attrs)
  if(file.exists(path_meta_loc)){ # need to delete this to avoid problems
    # that arise from further testing (e.g. notasource)
    file.remove(path_meta_loc)
  }
  dt_comids_ha <- proc.attr.hydfab::proc_attr_gageids(gage_ids=ls_fs_std$gage_ids[2],
                                                   featureSource=ls_fs_std$featureSource,
                                                   featureID=ls_fs_std$featureID,
                                                   Retr_Params=Retr_Params_ha,
                                                   path_save_gpkg = NULL,
                                                   lyrs="network",overwrite=FALSE) %>%
                  base::suppressWarnings()
  testthat::expect_true(all(unlist(unname(Retr_Params_ha$vars)) %in% dt_comids_ha$attribute))

  # TODO figure out what's wrong here. The confusion is that it works when calling the second time, but not the first
  # # test a wrong featureSource
  # testthat::expect_error(proc.attr.hydfab::proc_attr_gageids(gage_ids=ls_fs_std$gage_ids[2],
  #                                                  featureSource='notasource',
  #                                                  featureID=ls_fs_std$featureID,
  #                                                  Retr_Params=Retr_Params,
  #                                                  lyrs="network",overwrite=FALSE),
  #                          regexp="Problem with comid database logic")

  if(file.exists(path_meta_loc)){ # need to delete this to avoid problems
    # that arise from further testing (e.g. notasource)
    file.remove(path_meta_loc)
  }
  # Expect 'skipping' this gage_id b/c NA doesn't exist
  testthat::expect_warning(proc.attr.hydfab::proc_attr_gageids(gage_ids=c(NA),
                                                              featureSource='nwissite',
                                                              featureID=ls_fs_std$featureID,
                                                              Retr_Params=Retr_Params,
                                                              path_save_gpkg = NULL,
                                                              lyrs="network",overwrite=FALSE),
                           regexp="following gage_id values did not return a comid")

})

testthat::test_that('comid_instead_of_nwissite',{
  # Use 'comid' as the featureSource in lieu of 'nwissite'
  comids_exst <- c("1520007","1623207","1638559","1722317")
  # Define path and make sure it doesn't exist
  path_save_gpkg <- file.path(temp_dir,"comid_check.gpkg")
  capt_rm <- base::file.remove(path_save_gpkg) %>% suppressWarnings()

  test_exst <- proc.attr.hydfab::proc_attr_gageids(gage_ids=comids_exst,
                                      featureSource='comid',
                                      featureID='{gage_id}',
                                      Retr_Params=Retr_Params,
                                      path_save_gpkg = path_save_gpkg,
                                      lyrs=lyrs,
                                      overwrite=overwrite)

  testthat::expect_true(base::all(comids_exst %in% test_exst$featureID))
  # Test an ID that isn't actually a comid. Make this after test_exst, since
  #. we know that path_save_gpkg has now been created
  non_comid <- "75004300004059"
  test_nonexst <- testthat::expect_warning(proc.attr.hydfab::proc_attr_gageids(gage_ids=non_comid,
                                                      featureSource='comid',
                                                      featureID='{gage_id}',
                                                      Retr_Params=Retr_Params,
                                                      path_save_gpkg = path_save_gpkg,
                                                      lyrs=lyrs,
                                                      overwrite=overwrite),
                  regexp = "Unexpected missing data")

  testthat::expect_true(base::any(base::grepl(non_comid,test_nonexst$gage_id)))
  # NOTE 20240414: Code now expects the provided comid to be returned as featureID: https://github.com/NOAA-OWP/formulation-selector/commit/5aafac9bf01b7cce9a9e7947d9fd5dec152a8286
  # testthat::expect_true(base::is.na(test_nonexst$featureID))

  # Test a mix of comid and non-comid:
  comids_mix_good_bad <- base::c(comids_exst,non_comid, "dakleta")
  test_mix <- proc.attr.hydfab::proc_attr_gageids(gage_ids=comids_mix_good_bad,
                                                  featureSource='comid',
                                                  featureID='{gage_id}',
                                                  Retr_Params=Retr_Params,
                                                  path_save_gpkg = path_save_gpkg,
                                                  lyrs=lyrs,
                                                  overwrite=overwrite) %>%
            testthat::expect_warning()
  # Ensure a non-retrievable comid generates an empty point
  testthat::expect_true(base::nrow(test_mix) == base::nrow(test_exst)+2)
  testthat::expect_true(base::is.na(test_mix$value[test_mix$gage_id == non_comid]))
})

testthat::test_that("fs_retr_nhdp_comids_geom_wrap",{
  # Testing the comid/gage_id/geometry mappings wrapper
  # UNITTEST TASKS FOR MARCH 13
  # TODO Enforce CRS 4326 across all nhdplus queries
  # TODO A multipoint reach converts to a single point
  path_save_gpkg <- file.path(temp_dir,"chck_map_gid_geom.gpkg")

  rslt_normal <- proc.attr.hydfab::fs_retr_nhdp_comids_geom_wrap(path_save_gpkg=path_save_gpkg,
                          gage_ids=ls_fs_std$gage_ids,featureSource='nwissite',
                                featureID = 'USGS-{gage_id}')
  required_cols <- c("featureID","featureSource","gage_id","geometry")
  testthat::expect_true(file.exists(path_save_gpkg))
  testthat::expect_true(all(ls_fs_std$gage_ids %in% rslt_normal$gage_id))
  testthat::expect_s3_class(rslt_normal$geometry,"sfc_POINT")
  testthat::expect_s3_class(rslt_normal,"sf")
  testthat::expect_true(all(required_cols %in% base::names(rslt_normal)))

  rm_gpkg <- file.remove(path_save_gpkg)
  comids_exst <- c("1520007","1623207","1638559","1722317")
  rslt_comid_query <- proc.attr.hydfab::fs_retr_nhdp_comids_geom_wrap(
                          path_save_gpkg=path_save_gpkg,
                          gage_ids=comids_exst,featureSource='comid',
                          featureID = '{gage_id}')

  testthat::expect_identical(nrow(rslt_comid_query),nrow(rslt_normal))
  testthat::expect_true(base::all(required_cols %in% base::names(rslt_comid_query)))
  testthat::expect_true(base::all(comids_exst %in% rslt_comid_query$gage_id))
  testthat::expect_true(base::all(rslt_comid_query$featureSource == 'comid'))
  testthat::expect_equal(sf::st_crs(rslt_comid_query$geometry)$epsg, 4326)
  rm_gpkg <- file.remove(path_save_gpkg)
})


testthat::test_that("fs_retr_nhdp_comids_geom",{
  # Test the function that retrieves geometry based on comid
  retr_geom <- proc.attr.hydfab::fs_retr_nhdp_comids_geom(
              gage_ids = ls_fs_std$gage_ids)
  testthat::expect_equal(unique(retr_geom$featureSource),
            formals(proc.attr.hydfab::fs_retr_nhdp_comids_geom)$featureSource)
  required_cols <- c("featureID","featureSource","gage_id","geometry")
  testthat::expect_equal(nrow(retr_geom), length(ls_fs_std$gage_ids))
  testthat::expect_true(all(ls_fs_std$gage_ids %in% retr_geom$gage_id))
  testthat::expect_s3_class(retr_geom$geometry,"sfc_POINT")
  testthat::expect_s3_class(retr_geom,"data.table")
  testthat::expect_true(all(required_cols %in% base::names(retr_geom)))

  bad_comid <- testthat::expect_warning(proc.attr.hydfab::fs_retr_nhdp_comids_geom(
    gage_ids = "daklsteja",featureID = "{gage_id}",featureSource = "comid"),
    regexp = "Could not retrieve geometry"
  ) %>% pkgcond::suppress_messages()
  testthat::expect_equal(sf::st_crs(bad_comid$geometry)$epsg,4326)
  testthat::expect_equal(nrow(bad_comid),1)
  testthat::expect_true(sf::st_is_empty(bad_comid$geometry))
  testthat::expect_identical(bad_comid$featureID,"daklsteja")
  testthat::expect_identical(bad_comid$featureSource,"comid")
})

testthat::test_that('check_attr_selection', {
  ## Using a config yaml
  # Test for requesting something NOT in the attr menu
  attr_cfg_path_missing <- file.path(dir_base, 'xssa_attr_config_missing_vars.yaml')
  testthat::expect_message(testthat::expect_warning(expect_equal(proc.attr.hydfab::check_attr_selection(attr_cfg_path_missing), c("TOT_TWi", "TOT_POPDENS91"))))

  # Test for only requesting vars that ARE in the attr menu
  attr_cfg_path <- file.path(dir_base, '/xssa_attr_config_all_vars_avail.yaml')
  testthat::expect_message(testthat::expect_equal(proc.attr.hydfab::check_attr_selection(attr_cfg_path), NA))


  ## Using a list of variables of interest instead of a config yaml
  # Test for requesting something NOT in the attr menu
  vars <- c('TOT_TWi', 'TOT_PRSNOW', 'TOT_EWT')
  testthat::expect_warning(testthat::expect_equal(proc.attr.hydfab::check_attr_selection(vars = vars), 'TOT_TWi'))

  # Test for only requesting vars that ARE in the attr menu
  vars <- c('TOT_TWI', 'TOT_PRSNOW', 'TOT_EWT')
  testthat::expect_equal(proc.attr.hydfab::check_attr_selection(vars = vars), NA)
})


testthat::test_that('retrieve_attr_exst', {
  comids <- c("1520007","1623207","1638559","1722317") # !!Don't change this!!
  vars <- Retr_Params$vars %>% unlist() %>% unname()

  # Run tests based on expected dims
  dat_attr_all <- suppressWarnings(proc.attr.hydfab::retrieve_attr_exst(comids,vars,dir_db_attrs_pkg))
  testthat::expect_equal(length(unique(dat_attr_all$featureID)), # TODO update datasets inside dir_db_attrs
                         length(comids))
  testthat::expect_equal(length(unique(dat_attr_all$attribute)),length(vars))

  testthat::expect_error(proc.attr.hydfab::retrieve_attr_exst(comids,
                                                              vars,
                                                              dir_db_attrs='a'))
  # Testing for No parquet files present
  capt_no_parquet <- testthat::capture_condition(proc.attr.hydfab::retrieve_attr_exst(comids,
                                                                vars,
                                                                dir_db_attrs=dirname(dirname(dir_db_attrs_pkg))))
  testthat::expect_true(grepl("parquet",capt_no_parquet$message))
  nada_var <- testthat::capture_warnings(proc.attr.hydfab::retrieve_attr_exst(comids,vars=c("TOT_TWI","naDa"),
                                              dir_db_attrs_pkg))
  testthat::expect_true(any(grepl("naDa",nada_var)))

  nada_comid <- testthat::capture_warnings(proc.attr.hydfab::retrieve_attr_exst(comids=c("1520007","1623207","nada"),vars,
                                              dir_db_attrs_pkg))
  testthat::expect_true(any(base::grepl("nada",nada_comid)))

  testthat::expect_error(proc.attr.hydfab::retrieve_attr_exst(comids,vars=c(3134,3135),
                                            dir_db_attrs_pkg))
  testthat::expect_warning(proc.attr.hydfab::retrieve_attr_exst(comids=c(3134,3135),vars,
                                            dir_db_attrs_pkg))
})



testthat::test_that("hfab_config_opt",{
  config_in <- yaml::read_yaml(file.path(dir_base, 'xssa_attr_config_all_vars_avail.yaml'))
  reqd_hfab <- c("s3_base","s3_bucket","hf_cat_sel","source")
  hfab_config <- proc.attr.hydfab::hfab_config_opt(config_in$hydfab_config,
                                                   reqd_hfab=reqd_hfab)

  testthat::expect_true(!base::any(reqd_hfab %in% names(hfab_config)))

  # A NULL hfab_retr is set to the default val in proc.attr.hydfab::proc_attr_wrap()
  hfab_cfg_edit <- config_in$hydfab_config
  names_cfg_edit <- lapply(hfab_cfg_edit, function(x) names(x)) %>% unlist()
  idx_hfab_retr <- grep("hfab_retr", names_cfg_edit)
  hfab_cfg_edit[[idx_hfab_retr]] <- list(hfab_retr = NULL)
  testthat::expect_identical(base::formals(proc.attr.hydfab::proc_attr_wrap)$hfab_retr,
                             proc.attr.hydfab::hfab_config_opt(hfab_cfg_edit,
                                                               reqd_hfab=reqd_hfab)$hfab_retr)
  # A NULL hf_version is set to the default val in proc_attr_wrap()
  hfab_cfg_hfsubsetr <- config_in$hydfab_config
  names_cfg_hfsubsetr <- lapply(hfab_cfg_hfsubsetr, function(x) names(x)) %>% unlist()
  idx_hfver <- grep("hf_version", names_cfg_hfsubsetr)
  hfab_cfg_hfsubsetr[[idx_hfver]] <- list(hf_version=NULL)

  testthat::expect_identical(base::formals(hfsubsetR::get_subset)$hf_version,
                             hfab_config_opt(hfab_cfg_hfsubsetr,
                                             reqd_hfab=reqd_hfab)$hf_version)

})

testthat::test_that("proc_attr_hf not a comid",{
  testthat::expect_error(proc.attr.hydfab::proc_attr_hf(comid="13Notacomid14",
                                                        dir_db_hydfab,
                                                        custom_name="{lyrs}_",fileext = 'gpkg',
                                                        lyrs=c('divides','network')[2],
                                                        hf_cat_sel=TRUE, overwrite=FALSE)) %>% suppress_warnings()
})


testthat::test_that("grab_attrs_datasets_fs_wrap", {
  # COPY retrieve params stored in package into temp dir for standard processing
  dir_attrs_pah <- file.path(Retr_Params$paths$dir_std_base,'../attributes_pah/')
  fs::dir_copy(dir_attrs_pah, Retr_Params$paths$dir_db_attrs,overwrite = TRUE)

  arrow::open_dataset(dir_attrs_pah) %>% names()

  # Mock `path_save_gpkg` inside `save_to_gpkg`
  mock_path_save_gpkg <- file.path(temp_dir,"unit_test.gpkg")
  mockery::stub(grab_attrs_datasets_fs_wrap, "path_save_gpkg", mock_path_save_gpkg)


  ls_comids_all <- proc.attr.hydfab::grab_attrs_datasets_fs_wrap(Retr_Params,
                                                               lyrs="network",
                                                               overwrite=FALSE) %>%
    base::suppressWarnings()
  testthat::expect_equal(names(ls_comids_all), Retr_Params$datasets)


  # Test wrong datasets name provided
  Retr_Params_bad_ds <- Retr_Params
  Retr_Params_bad_ds$datasets <- c("bad","xssa-mini")
  testthat::expect_error(
    proc.attr.hydfab::grab_attrs_datasets_fs_wrap(Retr_Params_bad_ds,
                                                    lyrs="network",
                                                    overwrite=FALSE))
  # Test when path_meta requirements not provided:
  Retr_Params_missing_meta <- Retr_Params
  Retr_Params_missing_meta$write_type <- NULL
  Retr_Params_missing_meta$ds_type <- NULL
  testthat::expect_error(
    proc.attr.hydfab::grab_attrs_datasets_fs_wrap(Retr_Params_missing_meta,
                                                  lyrs="network",
                                                  overwrite=FALSE) %>%
      base::suppressWarnings(),
    regexp = "path_meta not fully defined")

  # Test that all datasets are processed
  Retr_Params_all_ds <- Retr_Params
  Retr_Params_all_ds$datasets <- "all"
  ls_comids_all_ds <- proc.attr.hydfab::grab_attrs_datasets_fs_wrap(Retr_Params_all_ds,
                                                                      lyrs="network",
                                                                      overwrite=FALSE) %>%
                        base::suppressWarnings()
  # When 'all' datasets requested, should have the same number retrieved
  testthat::expect_equal(length(ls_comids_all_ds),
                        length(list.files(Retr_Params_all_ds$paths$dir_std_base)))

  # Test running just the dataset path - not reading in a netcdf dataset.
  Retr_Params_no_ds <- Retr_Params
  Retr_Params_no_ds$datasets <- NULL
  good_file <- file.path(dir_base,"gage_id_example.csv")
  Retr_Params_no_ds$loc_id_read$loc_id_filepath <- good_file
  Retr_Params_no_ds$loc_id_read$gage_id <- 'gage_id'
  Retr_Params_no_ds$loc_id_read$featureSource_loc <- 'nwissite'
  Retr_Params_no_ds$loc_id_read$featureID_loc <- 'USGS-{gage_id}'
  Retr_Params_no_ds$loc_id_read$fmt <- 'csv'

  dat_gid_ex <- proc.attr.hydfab::grab_attrs_datasets_fs_wrap(Retr_Params_no_ds,
                                                  lyrs="network",
                                                  path_save_gpkg_cstm = mock_path_save_gpkg,
                                                  overwrite=FALSE) %>% suppressWarnings()
  testthat::expect_equal(nrow(dat_gid_ex[[1]]),24) # this considers both usgs & hydroatlas
  dat_gf <- read.csv(good_file,colClasses ="character")
  orig_ids <- unique(as.character(dat_gf$gage_id))
  rtrn_ids <- unique(dat_gid_ex[[1]]$gage_id) # Note that "01031500" is missing. Not sure why.
  testthat::expect_true(all(rtrn_ids %in% orig_ids))
  testthat::expect_true(file.exists(mock_path_save_gpkg)) # the geopackage should have been created

})


testthat::test_that("proc_attr_hydatl", {
  exp_dat_ha <- readRDS(system.file("extdata", paste0("ha_18094981.Rds"), package="proc.attr.hydfab"))
  ha <- proc.attr.hydfab::proc_attr_hydatl(comid,s3_path_hydatl,
                                           ha_vars=c("pet_mm_s01","cly_pc_sav","cly_pc_uav"))
  # saveRDS(ha,paste0("~/git/fsds/pkg/proc.attr.hydfab/inst/extdata/ha_",comid,".Rds"))
  # Wide data expected
  testthat::expect_equal(ha,exp_dat_ha)

  # Run this with a bad s3 bucket
  testthat::expect_error(proc.attr.hydfab::proc_attr_hydatl(comid="18094981",
                                                          s3_path_hydatl ='https://s3.notabucket',
                                                          ha_vars = Retr_Params$vars$ha_vars))
})

testthat::test_that("proc_attr_usgs_nhd", {
  exp_dat <- readRDS(system.file("extdata", paste0("nhd_18094981.Rds"), package="proc.attr.hydfab"))
  order_cols <- c('COMID',"TOT_TWI","TOT_PRSNOW","TOT_POPDENS90","TOT_EWT","TOT_RECHG")
  usgs_meta <- proc.attr.hydfab::proc_attr_usgs_nhd(comid=18094981,
                   usgs_vars=c("TOT_TWI","TOT_PRSNOW","TOT_POPDENS90","TOT_EWT","TOT_RECHG")) %>%
                    data.table::setcolorder(order_cols)
  #saveRDS(usgs_meta,paste0("~/git/fsds/pkg/proc.attr.hydfab/inst/extdata/nhd_",comid,".Rds"))
  # Wide data expected
  # Check that the COMID col is returned (expected out of USGS data)
  testthat::expect_true(base::any(base::grepl("COMID",colnames(usgs_meta))))
  # Check for expected dataframe format
  testthat::expect_equal(usgs_meta,exp_dat)

})

testthat::test_that("proc_attr_exst_wrap", {
  #path_attrs,vars_ls,bucket_conn=NA
  ls_rslt <- proc.attr.hydfab::proc_attr_exst_wrap(
                                                   path_attrs=dir_db_attrs,
                                                   vars_ls=Retr_Params$vars,
                                                   bucket_conn=NA) %>%
              base::suppressWarnings()
  testthat::expect_true(all(names(ls_rslt) == c("dt_all","need_vars")))
  testthat::expect_type(ls_rslt,'list')
  testthat::expect_s3_class(ls_rslt$dt_all,'data.table')
  if(length(list.files(dir_db_attrs,pattern='parquet'))==0){
    testthat::expect_true(nrow(ls_rslt$dt_all)==0)
  }


  # Testing for a comid that doesn't exist
  new_dir <- base::tempdir()
  ls_no_comid <- proc.attr.hydfab::proc_attr_exst_wrap(
                                                      path_attrs=file.path(new_dir,'newone','file.parquet'),
                                                      vars_ls=Retr_Params$vars,
                                                      bucket_conn=NA)
  testthat::expect_true(nrow(ls_no_comid$dt_all)==0)
  # Kinda-sorta running the test, but only useful if new_dir exists
  testthat::expect_equal(dir.exists(new_dir),
                         dir.exists(file.path(new_dir,'newone')))
})

pkg_path_meta <- base::file.path(dir_db_attrs_pkg,"meta_loc","comid_featID_map.csv")
if(!base::file.exists(pkg_path_meta)){
  warning("The expected hard-coded filepath for metadata does not exist:
          {actual_path_meta}")
} else {
  base::file.remove(pkg_path_meta)
}

# Remove the gpkg files created:
filz_gpkg <- c(list.files(dir_dataset,pattern=".gpkg",full.names = TRUE),
list.files(base::gsub(pattern = "-mini",replacement="-mini-two",x=dir_dataset),pattern="gpkg",full.names=TRUE))

rm_gpkg <- file.remove(filz_gpkg)

# TODO unit testing for fs_attrs_miss_wrap()
# testthat::test_that("fs_attrs_miss_wrap",{
#   path_attr_config <- file.path(dir_base,"xssa_attr_config_all_vars_avail.yaml")
#   rslt <- proc.attr.hydfab::fs_attrs_miss_wrap(path_attr_config)
#
#
# })
# Read in data of expected format
if (!ignore_deprecated_tests){
  # proc_attr_wrap deprecated as of Dec, 2024
  testthat::test_that("DEPRECATED_proc_attr_wrap", {
    Retr_Params_all <- Retr_Params
    # Substitute w/ new tempdir based on setup.R
    Retr_Params$paths$dir_db_attrs <- Retr_Params$paths$dir_db_attrs %>%
      base::gsub(pattern=temp_dir,
                 replacement=local_temp_dir2() )
    Retr_Params$paths$dir_db_hydfab <- Retr_Params$paths$dir_db_hydfab %>%
      base::gsub(pattern=temp_dir,
                 replacement =local_temp_dir2() )
    Retr_Params_all$vars$ha_vars <- c("pet_mm_s01","cly_pc_sav")
    Retr_Params_all$vars$usgs_vars <-  c("TOT_TWI","TOT_PRSNOW","TOT_POPDENS90","TOT_EWT","TOT_RECHG","TOT_BFI")
    exp_dat <- readRDS(system.file("extdata", paste0("attrs_18094081.Rds"), package="proc.attr.hydfab"))
    exp_dat$attribute <- as.character(exp_dat$attribute)
    dat_all <- proc.attr.hydfab::proc_attr_wrap(comid=18094081,Retr_Params_all,
                                                lyrs='network',
                                                overwrite=TRUE )
    # How the exp_dat was originally created for unit testing
    # saveRDS(dat_all,paste0("~/git/fsds/pkg/proc.attr.hydfab/inst/extdata/attrs_18094081.Rds"))
    testthat::expect_true(dir.exists(dir_db_attrs))
    # Remove the dl_timestamp column for download timestamp and compare
    testthat::expect_equal(
      exp_dat %>% select(-dl_timestamp) %>% as.matrix(),
      dat_all %>% select(-dl_timestamp) %>% as.matrix())

    # Test when data exist in tempdir and new data do not exist
    Retr_Params_only_new <- Retr_Params
    Retr_Params_only_new$vars$usgs_vars <- c('TOT_PET')
    dat_add_pet <- suppressWarnings(proc.attr.hydfab::proc_attr_wrap(18094081,Retr_Params_only_new,
                                                                     lyrs='network',
                                                                     overwrite=FALSE ))
    testthat::expect_true(any('TOT_PET' %in% dat_add_pet$attribute))
    testthat::expect_true(any(grepl("TOT_PRSNOW", dat_add_pet$attribute)))

    # Test when some data exist in tempdir and new data needed
    Retr_Params_add <- Retr_Params
    # Sneak in the BFI variable
    Retr_Params_add$vars$usgs_vars <- c("TOT_TWI","TOT_PRSNOW","TOT_POPDENS90",
                                        "TOT_EWT","TOT_RECHG","TOT_BFI")
    dat_all_bfi <- suppressWarnings(proc.attr.hydfab::proc_attr_wrap(comid,
                                                                     Retr_Params_add,
                                                                     lyrs='network',
                                                                     overwrite=FALSE ))
    # Does the BFI var exist?
    testthat::expect_true(base::any('TOT_BFI' %in% dat_all_bfi$attribute))
    # testthat::expect_true(any(grepl("TOT_PRSNOW", dat_all_bfi$attribute)))


    # files_attrs <- file.path(Retr_Params$paths$dir_db_attrs,
    #                          list.files(Retr_Params$paths$dir_db_attrs))
    file.remove(file.path(Retr_Params$paths$dir_db_attrs,"comid_18094081_attrs.parquet"))
  })
}
