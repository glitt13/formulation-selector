This is a cluster-based regionalization demonstration of parameter sets using
clustering algorithms trained from the July, 2026 calibration experiments on 
URSA using hydrofabric v4. Each formulation's parameter sets were selected as
the 'best' iteration based on the equally-weighted criteria of NNSE and MAPPE metrics.

Note that this is not a production-grade example, as more calibration locations would be
needed for reasonable continental-scale parameter regionalization.

This performs cluster-based donor-receiver pairing to regionalize
parameters across CONUS HUC12 basins based on hfATLAS atttributes.
The regionalized parameters are predicted across a test-grade 
2026 August 31 hydrofabric v4 dataset. 
Refer to e.g. `regn_casam_pred_config_hf4.yaml` for the 
representation of making predictions across hydrofabric v4.

The config files herein use the predictors and static parameters generated 
by hfATLAS as the watershed attributes used for cluster prediction.
