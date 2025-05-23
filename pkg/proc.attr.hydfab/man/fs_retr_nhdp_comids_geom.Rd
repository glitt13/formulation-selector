% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/proc_attr_grabber.R
\name{fs_retr_nhdp_comids_geom}
\alias{fs_retr_nhdp_comids_geom}
\title{Retrieve comids & point geometry based on nldi_feature identifiers}
\usage{
fs_retr_nhdp_comids_geom(
  gage_ids,
  featureSource = "nwissite",
  featureID = "USGS-{gage_id}",
  epsg = 4326
)
}
\arguments{
\item{gage_ids}{vector of USGS gage_ids}

\item{featureSource}{The  \link[nhdplusTools]{get_nldi_feature} feature
featureSource, default 'nwissite'}

\item{featureID}{a glue-configured conversion of gage_id into a recognized
featureID for  \link[nhdplusTools]{get_nldi_feature}. E.g. if gage_id
represents exactly what the nldi_feature$featureID should be, then
featureID="{gage_id}". In other instances, conversions may be necessary,
e.g. featureID="USGS-{gage_id}". When defining featureID, it's expected
that the term 'gage_id' is used as a variable in glue syntax to create featureID
Refer to ?dataRetrieval::get_nldi_sources() for options to use with nldi_feature}

\item{epsg}{The EPSG code to use for the CRS; nhdplus defaults 4326}
}
\value{
data.table of comid and geometric point in epsg 4326
}
\description{
Retrieve comids & point geometry based on nldi_feature identifiers
}
\seealso{
\link[proc.attr.hydfab]{proc_attr_read_gage_ids_fs}

\link[proc.attr.hydfab]{fs_retr_nhdp_comids_geom_wrap}

\code{fs_algo.fs_algo_train_eval.fs_retr_nhdp_comids_geom}
}
