library(dplyr)
library(arrow)
library(stringr)
library(ggplot2)
library(glue)
dir_dat <- "~/noaa/regionalization/data/output/algorithm_predictions/xSSA_proc_sens_wt_loc_sel_ngencerf"
fns <- list.files(dir_dat, pattern = 'pred_rf')
for(fn in fns){
  df <- arrow::read_parquet(file.path(dir_dat,fn))
  name_proc <- stringr::str_split(fn,pattern = "__")[[1]][[1]] %>%
    gsub(pattern= "pred_rf_",replacement="")
  maxval <- max(df$prediction)
  frac_pt1 <- round(length(which(df$prediction < 0.01))/nrow(df),2)
  frac_pt3 <- round(length(which(df$prediction < 0.03))/nrow(df),2)
  plot_out <- ggplot2::ggplot(df,aes(x=prediction)) +
                    geom_histogram(bins = 40) +
                    ggtitle(glue::glue("{name_proc} at ngenCERF locations \n frac below 1%: {frac_pt1} \n frac below 3%: {frac_pt3}")) +
                    xlim(0,maxval)


  print(plot_out)
}
