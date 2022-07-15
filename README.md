### Overview

This is a small library to help with extreme value analysis of geospatial datasets.
Chunking and parallelisation is done with the Dask library.
Extreme value analysis and fitting is done using scipy.stats.

See the nb_* files for examples of how to use the routines in eva_dask.py

`eva_dask.py` is the main file containing classes and routines.
It currently contains three classes:

1. `extremes`       - Routines for finding extremes (currently only annual maxima).
2. `model_fitting`. - Routines for fitting (currently only GEV).
3. `return_values`  - Contains routines for calculating return periods and levels, based on fitted distributions or empirically.

## Routines

Data should be geospatial data, with two horizontal dimensions and a time dimension. The time dimension should be the first dimension for these routines to work correctly. I.E. the shape of a dask data array or xarray data array should be (time, x, y). Some routines of eva_dask expect dask arrays. If you have lazily read data in using xarray, you can convert it to a dask array using something like:

```
dataset = xr.open_dataset(filename, chunks={'lat':30, 'lon':30})
data = dataset[variable_name].data
```

Or if you have eager data (loaded to memory), you can transform it to a dask array with new chunks using:

```
data = da.from_array(data, chunks=[-1, 30, 30])
```

You should always make sure that your time dimension has only one dask chunk.

## extremes class
```
'''
def annual_maxima(cls, dataset, time_dim = 'time', min_datapoints=300):
        Takes in an xarray datasets and finds the annual maxima.
        
        Args:
            dataset        : xarray.Dataset() object containing data variables
                             to analyse
            time_dim       : Name of the time dimension in the xarray dataset
            min_datapoints : Minimum number of data points for analysis, 
                             otherwise NaN

        Returns:
            returns a new grouped dataset of annual maxima. The returned 
            dataset needs to be computed (.compute() or .to_netcdf()).
        
        '''
```
