import xarray as xr
from dask.distributed import Client, LocalCluster
from dask import delayed
import matplotlib.pyplot as plt
import pandas as pd
from pyextremes import EVA
from datetime import datetime, timedelta
import numpy as np
import glob
import os

input_file = glob.glob("/home/davidbyrne/disks/ssd200/wetbulb/zarr/1deg/*")
output_dir = '/home/davidbyrne/disks/ssd200/wetbulb/analysis/annual_maxima/1deg'
n_files = len(input_file)

client = Client("tcp://127.0.0.1:34885")

dataset_list = [ xr.open_zarr(fn) for fn in input_file ]

delayed_list = []

for dd in range(n_files):
    
    dataset = dataset_list[dd]
    
    start_year = int(input_file[dd][-9:-5])
    start_date = datetime(start_year, 1, 1)
    end_date   = start_date + timedelta(days = dataset.dims['time'] - 1)
    time = pd.date_range(start_date, end_date, freq='1D')
    
    dataset['time'] = time
    
    grouped = dataset.groupby('time.year')
    grouped_max = grouped.nanmax(dim='time')
    
    filebase = os.path.basename(input_file[dd])
    
    grouped_max.to_zarr( os.path.join(output_dir, filebase) )
    
client.compute(delayed_list)

client.close()