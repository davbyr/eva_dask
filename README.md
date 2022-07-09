# netcdf_eva
Scripts and routines for doing parallel extreme value analysis using Dask and pyextremes

Workflow for Annual BM maxima:

1. netcdf_to_zarr.py 
2. get_annual_maxima.py
3. fit_gev_to_extremes.py
4. return_levels
