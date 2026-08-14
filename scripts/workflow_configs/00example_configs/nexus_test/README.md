Goal: test out a training/prediction RaFTS workflow performed on nexus rather than divides

Note hfv4 test basins from June/July don't have unique nexus ids, so using custom identifiers created in hfATLAS (combines gage_id with nexus ID)

```zsh
uv run python map_nexus_divides.py "/Users/guylitt/noaa/regionalization/data/raw/hfv4_test/hfv4_fix/" "./nexus_divide_mapping.csv"
```