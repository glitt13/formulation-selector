#' @title Unit test hydrofabric processing
#' @author Guy Litt \email{guy.litt@noaa.gov}
#' @note When running this script, be sure to also source tests/testthat/setup.R first
# Changelog / Contributions
#   2025-02-12 Originally created, GL


suppressPackageStartupMessages(library(proc.attr.hydfab,quietly=TRUE))
suppressPackageStartupMessages(library(testthat,quietly=TRUE))
suppressPackageStartupMessages(library(dplyr,quietly=TRUE))
suppressPackageStartupMessages(library(hydrofabric,quietly=TRUE))
suppressPackageStartupMessages(library(data.table,quietly=TRUE))
suppressPackageStartupMessages(library(pkgcond,quietly=TRUE))
suppressPackageStartupMessages(library(yaml,quietly=TRUE))
suppressPackageStartupMessages(library(glue,quietly=TRUE))

# Generate objects used in multiple tests:
dt_hads <- proc.attr.hydfab::read_noaa_hads_sites() %>% suppress_messages()

test_yaml <- base::tempfile(fileext = ".yaml")
# Do not edit the spaces on the left side of this text!
base::writeLines(
  'dir_base_hfab: "~/noaa/hydrofabric/v2.2"
source_map_ak:
  path: "{dir_base_hfab}/path/to/file_ak.gpkg"
source_map_hi:
  path: "{dir_base_hfab}/path/to/file_hi.gpkg"
source_map_pr:
  path: "{dir_base_hfab}/path/to/file_pr.gpkg"',
  con = test_yaml)


testthat::test_that("read_noaa_hads_sites",{

  testthat::expect_true(base::nrow(dt_hads)>10000)
  testthat::expect_true(base::ncol(dt_hads) == 8)
  testthat::expect_true("lid" %in% names(dt_hads))
  testthat::expect_true("usgsId" %in% names(dt_hads))
  testthat::expect_true("latitude" %in% names(dt_hads))
  testthat::expect_true("longitude" %in% names(dt_hads))

})

testthat::test_that("std_feat_id",{
  dt_hads_sub <-dt_hads[c(3,333,408,1200,1333),]
  comids_fake <- c(1111,2222,3333,4444,5555)

  dt_meta <- proc.attr.hydfab::std_feat_id(df=dt_hads_sub,
              name_featureSource ="COMID",
              vals_featureID = comids_fake)

  # standardized form throughout RaFTS are featureID and featureSource columns
  testthat::expect_true("featureID" %in% names(dt_meta))
  testthat::expect_true("featureSource" %in% names(dt_meta))
  testthat::expect_true(base::all(dt_meta$featureSource == "COMID")) # standardized
  testthat::expect_identical(dt_meta$featureID,comids_fake)
})

# TODO add NWPS api unit test: retr_noaa_gauges_meta

# TODO create a mock object for path_oconus_hfab_config and reading hydrofabric oconus gpkg data
# testthat::test_that("retr_hfab_id_wrap",{
#   # Retrieve the hydrofabric IDs wrapper
#   usgs_ids_oconus <- c("15056210","16704000","50147800") # AK, HI, PR
#   dt_need_hf <- dt_hads %>% dplyr::filter(usgsId %in% usgs_ids_oconus)
#
#   dt_have_hf <- proc.attr.hydfab::retr_hfab_id_wrap(dt_need_hf,path_oconus_hfab_config = ) %>%
#     pkgcond::suppress_warnings() %>% pkgcond::suppress_messages()
#
#
#   testthat::expect_true(base::nrow(dt_have_hf) == nrow(dt_need_hf))
#   expect_new_cols <- c("featureID","featureSource","crs_hfab")
#   testthat::expect_true(base::all(expect_new_cols %in% base::names(dt_have_hf)))
#
#   # columns using wb ids:
#   idxs_cstm_hfab <- grep("-wb-",dt_have_hf$featureID)
#   testthat::expect_true(all(dt_have_hf$featureSource[idxs_cstm_hfab] == "custom_hfab"))
#
#   testthat::expect_true(all(names(dt_need_hf) %in% names(dt_have_hf)))
#
# })

testthat::test_that("parse_hfab_oconus_config",{
  # Create a temporary YAML configuration file for testing
  df_map_hfab <- proc.attr.hydfab::parse_hfab_oconus_config(test_yaml)

  testthat::expect_true(base::all(c("path","domain","crs_hfab") %in%
                                    base::colnames(df_map_hfab)) )
  testthat::expect_identical(df_map_hfab$path[1],
                             "~/noaa/hydrofabric/v2.2/path/to/file_ak.gpkg")
})

testthat::test_that("retr_state_terr_postal",{
  usgs_ids_oconus <- c("15056210","16704000","50147800") # AK, HI, PR
  dt_need_hf <- dt_hads %>% dplyr::filter(usgsId %in% usgs_ids_oconus)
  # AK should return AK
  ak_rslt <- proc.attr.hydfab::retr_state_terr_postal(lat=dt_need_hf$latitude[1],
                                           lon=dt_need_hf$longitude[1])
  testthat::expect_identical(ak_rslt, "AK")
  pr_rslt <- proc.attr.hydfab::retr_state_terr_postal(lat=dt_need_hf$latitude[3],
                                                      lon=dt_need_hf$longitude[3])
  testthat::expect_identical(pr_rslt, "PR")

  # Test US virgin island
  vi_rslt <- proc.attr.hydfab::retr_state_terr_postal(lat=18.335765,
                                                      lon=-64.896335)
  testthat::expect_identical(vi_rslt,"VI")

})



testthat::test_that("map_hfab_oconus_sources_wrap",{

  usgs_ids_oconus <- c("15056210","50147800","16704000") # AK, PR, HI
  dt_need_hf <- dt_hads %>% dplyr::filter(usgsId %in% usgs_ids_oconus)

  hfab_srce_map <- proc.attr.hydfab::parse_hfab_oconus_config(test_yaml)
  dt_rslt <- proc.attr.hydfab::map_hfab_oconus_sources_wrap(dt_need_hf,
                                                hfab_srce_map,
                                           col_lat = 'latitude',
                                           col_lon= 'longitude')

  testthat::expect_true(base::nrow(dt_rslt) == base::nrow(dt_need_hf))
  testthat::expect_true(base::all(dt_rslt$path %in% hfab_srce_map$path))
  testthat::expect_true(base::all(base::names(dt_need_hf) %in%
                                    base::names(dt_rslt)))
  testthat::expect_true(base::all(base::names(hfab_srce_map) %in%
                                      base::names(dt_rslt)))

  # Expect error when lat and lon are not columns
  testthat::expect_error(
    proc.attr.hydfab::map_hfab_oconus_sources_wrap(dt_need_hf,
                                                  hfab_srce_map,
                                                  col_lat = 'lat',
                                                  col_lon= 'lon'))
})

testthat::test_that("read_hfab_layer",{
  # Create a temporary GeoPackage for testing
  temp_gpkg <- tempfile(pattern = "test_hi", fileext = ".gpkg")

  # Create a mock spatial dataset with a vpu column
  mock_data <- sf::st_sf(
    geometry = sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(1, 1))),
    vpu = c("hi", "hi")
  )

  # Write the dataset to the temporary GeoPackage
  sf::st_write(mock_data, temp_gpkg, layer = "divides", driver = "GPKG", quiet = TRUE)

  # Unit test for read_hfab_layer
  result <- proc.attr.hydfab::read_hfab_layer(temp_gpkg, "divides")

  # Check that result is an sf object
  testthat::expect_s3_class(result, "sf")

  # Check that expected columns exist
  testthat::expect_true("vpu" %in% colnames(result))

  # Check that vpu correction works
  testthat::expect_equal(unique(result$vpu), "hi")

  # Now expect a warning when there is a pattern mismatch specific to ak:
  temp_gpkg_ak <- tempfile(pattern = "test_ak", fileext = ".gpkg")
  # Create a mock spatial dataset with a vpu column
  mock_data_ak <- sf::st_sf(
    geometry = sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(1, 1))),
    vpu = c("hi", "hi")
  )

  # Write the dataset to the temporary GeoPackage
  sf::st_write(mock_data_ak, temp_gpkg_ak, layer = "divides",
               driver = "GPKG", quiet = TRUE,append=FALSE) %>% suppress_messages()

  testthat::expect_warning( proc.attr.hydfab::read_hfab_layer(temp_gpkg_ak, "divides"))


})

##### Building up mock data for retr_hfab_id_wrap unit testing
# Create a temporary GeoPackage for testing
temp_gpkg <- tempfile(pattern = "test_ak", fileext = ".gpkg")
mock_network <- sf::st_sf(
  geometry = sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(1, 1))),
  vpu = c("hi", "hi"),
  hf_id = c(1001, 1002)
)
mock_flowpaths <- mock_network

# Write datasets to the temporary GeoPackage
sf::st_write(mock_network, temp_gpkg, layer = "network", driver = "GPKG", quiet = TRUE)
sf::st_write(mock_flowpaths, temp_gpkg, layer = "flowpaths", driver = "GPKG", quiet = TRUE)
sf::st_write(mock_flowpaths, temp_gpkg, layer = "divides", driver = "GPKG", quiet = TRUE)
# Create a mock input data.table
dt_need_hf <- data.table::data.table(
  usgsId = c("001", "002"),
  longitude = c(0, 1),
  latitude = c(0, 1),
  path = temp_gpkg
)
temp_dir <- tempdir()
fn_ak <- base::gsub(pattern=".gpkg",replacement="",x= base::basename(temp_gpkg) )
base::writeLines(
glue::glue(
'dir_base_hfab: ""
source_map_ak:
  path: "{temp_dir}/{fn_ak}.gpkg"'),
  con = test_yaml)


# Unit test for retr_hfab_id_wrap
testthat::test_that("retr_hfab_id_wrap correctly retrieves hydrofabric IDs", {
  usgs_ids_oconus <- c("15056210","50147800","16704000") # AK, PR, HI
  dt_need_hf <- dt_hads %>% dplyr::filter(usgsId %in% usgs_ids_oconus)

  result <- proc.attr.hydfab::retr_hfab_id_wrap(dt_need_hf = dt_need_hf,
                              path_oconus_hfab_config = test_yaml) %>%
    pkgcond::suppress_warnings()

  # Check that result is a data.table
  testthat::expect_s3_class(result, "data.table")

  # Check that hydrofabric IDs are correctly assigned
  testthat::expect_equal(dt_need_hf$usgsId,result$usgsId)

  testthat::expect_true(base::nrow(result) == nrow(dt_need_hf))
  expect_new_cols <- c("featureID","featureSource","crs_hfab")
  testthat::expect_true(base::all(expect_new_cols %in% base::names(result)))

  # columns using wb ids:
  idxs_cstm_hfab <- base::grep("-wb-",result$featureID)
  testthat::expect_true(base::all(result$featureSource[idxs_cstm_hfab] == "custom_hfab"))

  testthat::expect_true(base::all(base::names(dt_need_hf) %in% base::names(result)))
})

# Delete the temp files
rm(test_yaml)
rm(temp_gpkg)

