#!bin/bash/python3

import sys
import xarray
from dask.distributed import Client, LocalCluster
import xarray as xr

args = sys.argv
output_file = args[-1]
dimension = args[-2]
input_file = args[1:-2]


client = Client("tcp://127.0.0.1:34161")

datasets = [ xr.open_zarr(fn) for fn in input_file ]

datasets_concat = xr.concat(datasets, dim=dimension)
datasets_concat = datasets_concat.chunk({dimension:-1})
datasets_concat = datasets_concat.transpose('lat','lon','year')
print(datasets_concat)

datasets_concat.to_netcdf( output_file )