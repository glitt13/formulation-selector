% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/proc_attr_grabber.R
\name{std_path_attrs}
\alias{std_path_attrs}
\title{standardized path to attribute parquet file}
\usage{
std_path_attrs(comid, dir_db_attrs)
}
\arguments{
\item{comid}{character. USGS COMID value of interest}

\item{dir_db_attrs}{character. Directory where attribute .parquet files live}
}
\description{
standardized path to attribute parquet file
}
\seealso{
\link[proc.attr.hydfab]{proc_attr_wrap}

\code{fs_algo.fs_algo_train_eval.fs_read_attr_comid()} python function
that reads these files
}
