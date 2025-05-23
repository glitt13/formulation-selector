% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/proc_attr_grabber.R
\name{proc_attr_usgs_nhd}
\alias{proc_attr_usgs_nhd}
\title{Retrieve USGS variables based on comid}
\usage{
proc_attr_usgs_nhd(comid, usgs_vars)
}
\arguments{
\item{comid}{character or numeric class. The common identifier USGS
location code for a surface water feature. May be multiple comids.}

\item{usgs_vars}{list class. The standardized names of NHDplus variables.}
}
\description{
Retrieve USGS variables based on comid
}
\seealso{
\link[nhdplusTools]{get_characteristics_metadata}
}
