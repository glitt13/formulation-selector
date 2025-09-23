# Background
Integration tests for the data processing pipeline using `fs_prep`, `proc.attr.hydfab` and `fs_algo` packages.

The current version of input data to run these tests is zipped inside testdata_20250901.zip.

# Steps for running the RaFTS integration test
1) Those with access to the NOAA google drive may download the integration test input 
data, `testdata_20250901/`, from the following:
https://drive.google.com/drive/folders/1JDtJKSfbmtBBp1nkBdNMwzS_EvFYkvpC?usp=drive_link

2) Once downloaded and stored locally, the paths to the integration test data must be customized inside
the `formulation-selector/tests/config/xssa/xssa_prep_config.yaml` file_io section.

3) Finally, run the integration test:
```
python path/to/git/formulation-selector/tests/test_rafts_prep_to_pred.py
```

Four tests will run, corresponding to each basic step in the core RaFTS workflow.