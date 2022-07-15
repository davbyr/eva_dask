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

You should always make sure that your time dimension has only one dask chunk. Also be aware that all chunking/parallelisation is done automatically using dask and the chunks specified on the xarray dataset or dask array. Returned objects are often delayed and need to be computed to bring data to memory or write to disk. This can be done using `.compute()` or `to_netcdf()` if in an xarray dataset.

### extremes class
You can find the annual maxima of all variables in an xarray dataset using `extremes.annual_maxima()`:

```
from eva_dask import extremes
maxima = extremes.annual_maxima(xarray_dataset, time_dim = 'time', min_datapoints=300)
maxima = maxima.to_netcdf(filename)
```

### model_fitting class
You can fit a gev as follows:

```
from eva_dask import model_fitting
gev_parameters = model_fitting.fit_gev_model(dask_array = data, distribution = 'genextreme', zscore_to_remove = 4, 
                                             minimum_points = 100, method='MLE')
gev_parameters = gev_parameters.compute()
```

This will return an array of equal spatial size to the input (x, y) but with the time dimension replaced by a new parameters dimension with 4 elements. The first three elements of this dimension are the GEV (shape, loc, scale). The last element is the pvalue from a ks-test, which can be used for accepting or rejecting bad fits. This routine uses dask's `map_blocks()` routine, which will apply the analysis to each chunk specified in the input array (in parallel).
