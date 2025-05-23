% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/proc_attr_grabber.R
\name{proc_attr_wrap}
\alias{proc_attr_wrap}
\title{DEPRECATED. Wrapper to retrieve variables when processing attributes}
\usage{
proc_attr_wrap(
  comid,
  Retr_Params,
  lyrs = "network",
  overwrite = FALSE,
  hfab_retr = FALSE
)
}
\arguments{
\item{comid}{character. The common identifier USGS location code for a surface water feature.}

\item{Retr_Params}{list. List of list structure with parameters/paths needed to acquire variables of interest}

\item{lyrs}{character. The layer names of interest from the hydrofabric gpkg. Default 'network'}

\item{overwrite}{boolean. Should the hydrofabric cloud data acquisition be redone and overwrite any local files? Default FALSE.}

\item{hfab_retr}{boolean. Should the hydrofabric geopackage data be retrieved? Default FALSE.}
}
\description{
DEPRECATED. Use \link{proc_attr_mlti_wrap} instead.
Identifies a single comid location using the hydrofabric and then
acquires user-requested variables from multiple sources. Writes all
acquired variables to a parquet file as a standard data.table format.
Re-processing runs only download data that have not yet been acquired.
}
\details{
Function returns & writes a data.table of all these fields:
featureID - e.g. USGS common identifier (default)
featureSource - e.g. "COMID" (default)
data_source - where the data came from (e.g. 'usgs_nhdplus__v2','hydroatlas__v1')
dl_timestamp - timestamp of when data were downloaded
attribute - the variable identifier used in a particular dataset
value - the value of the identifier
}
\seealso{
\link[proc.attr.hydfab]{proc_attrs_gageids}

\link[proc.attr.hydfab]{proc_attr_mlti_wrap}
}
\author{
Guy Litt \email{guy.litt@noaa.gov}
}
