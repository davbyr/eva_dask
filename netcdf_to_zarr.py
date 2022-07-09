from dask.distributed import Client, LocalCluster
import xarray as xr
import sys
from datetime import datetime
import glob
import os
import pandas as pd

#~~~~~~~~~~~~   INPUTS   ~~~~~~~~~~~~~~~~#
# directory to input file(s) and file name(s)
# input_files must be a list even if its just one file
# use glob.glob() to use a wildcard.
input_file   = '<Path to filename or wildcard or list of files>'

# Define output directory for zarr file.
output_dir = '<Output directory>'

# How to chunk the data spatially. 
spatial_chunks = {'lat':25, 'lon':25}

# If a dask cluster already exists, you can provide it's IP.
# Otherwise, one will be created for you.
use_existing_ip = False
scheduler_ip = 'tcp://127.0.0.1:33997'

# Number of Dask workers to use
n_workers = 4


#~~~~~~~~~~~~   RUN   ~~~~~~~~~~~~~~~~#

# Determine if its just one or multiple files
if type(input_file) == list:
    n_files = len(input_file)
else:
    input_file = [input_file]
    n_files = 1

# Start cluster and connect a client
if use_existing_ip:
    client = Client(scheduler_ip)
else:
    cluster = LocalCluster(n_workers=n_workers)
    client = Client(cluster)

# If only one file, just convert straight to zarr
if n_files == 1:
    dataset = xr.open_dataset(input_file[0], chunks=spatial_chunks)
    output_base = os.path.basename(input_file[0])[:-3]
    dataset.to_zarr( os.path.join(output_dir, output_base) )
    
# If multiple files, loop over filenames and do them one by one, using delayed.
else:
    dataset_list = [xr.open_dataset(fn, chunks=spatial_chunks) for fn in input_file]
    for dd in range(n_files):
        
        # Get original file name for output
        output_base = os.path.basename(input_file[dd])[:-3]
        
        # Get a delayed zarr function to compute later
        dataset_list[dd].to_zarr( os.path.join(output_dir, output_base))

# Close cluster and client
client.close()

if not use_existing_ip:
    cluster.close()