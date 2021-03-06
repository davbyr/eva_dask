from dask.distributed import Client
import dask
import dask.array as da
from dask import delayed
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import glob
from datetime import datetime as dt
from scipy.stats import genextreme as gev
from scipy.stats import genpareto as gp
import scipy.stats as stats
import sys
sys.path.append('/home/davidbyrne/code/netcdf_eva')

class model_fitting():
    
    @classmethod
    def fit_gev_model(cls, dask_array, distribution='genextreme', 
                 zscore_to_remove=np.inf, minimum_points = 100, method='MLE'):
        '''
        For fitting extreme value distributions to a map of extreme samples
        using Dask. For this to work properly, please ensure you have started 
        a Dask cluster and client. These routines make use of Dask's 
        map_blocks() function to assign processes to different workers.
        
        Args:
            dask_array       : A dask array containing 2D map of extremes 
                               data.
                               This class is excepting an array shape of 
                               (time, x, y).
                               Ensure that the array has only one chunk in the 
                               time dimension.
            distribution     : String denoting distribution. Currently only 
                               "genextreme"
            zscore_to_remove : Remove some values before analysis, if they 
                               have a larger zscore
            minimum_points   : Minimum number of data points for analysis. 
                               Otherwise nan.

        Returns:
            An uncomputed dask array of shape (4, n_x, n_y).
            The first index contains parameters (shape, loc, scale) and the 
            pvalue from a KS test.
        '''
        
        if distribution == 'genextreme':
            mapped = da.map_blocks(cls._fit_gev_model_chunk, dask_array, 
                                   zscore_to_remove,
                                   minimum_points, method)
        else:
            raise ValueError("Only option for get_fit() distribution is" \
                             "currently genextreme")
        
        return mapped
    
    @classmethod
    def _fit_gev_model_chunk(cls, chunk, zscore_to_remove, minimum_points,
                             method):
        '''
        Fit gev to an individual chunk of a dask array.
        '''
        
        # Get chunk shape and flatten chunk. 
        n_t, n_r, n_c = chunk.shape
        n_pts = n_r*n_c
        chunkF = chunk.reshape((n_t, n_pts))

        # Declare output array
        params = np.zeros((4, n_pts))*np.nan

        # Loop over each location in the flattened chunk and do a 
        # distribution fit.
        for pt in range(n_pts):
            timeseries = chunkF[:, pt]
            timeseries = timeseries[~np.isnan(timeseries)]

            if len(timeseries) > minimum_points:

                # Remove outliers
                z = np.abs( stats.zscore(timeseries, nan_policy='omit') )
                timeseries = timeseries[z<=zscore_to_remove]

                # Get the fitted params. HAve to do in try-except statement as 
                # sometimes an error is returned for infinite moments.
                try:
                    ff = gev.fit(timeseries, method=method)
                    params[:3, pt] = ff
                    # Kolmogorov-Smirnov test for fitted distribution
                    ks = stats.kstest(timeseries, 'genextreme', args=ff)
                    params[3, pt] = ks[1]
                except:
                    pass

                

        return params.reshape((4, n_r, n_c))

    @classmethod
    def AIC(cls, data, model, params):
        k = len(params)
        logLik = np.sum( model.logpdf(data, params[0], params[1], params[2]) ) 
        return 2*k - 2*logLik

class return_values():
    
    def __init__(self):
        return
    
    @classmethod
    def return_levels_distribution(cls, fitted_params, 
                               distribution, 
                               return_periods):
        '''
        Calculates return levels from an extreme value distribution fit.
        
        Args:
            fitted_params       : Array of fitted parameters of shape 
                                  (n_params, n_x, n_y)
            distribution        : scipy.stats distribution class
            return_periods      : List or array of return periods

        Returns:
            An uncomputed dask array of shape (4, n_x, n_y).
            The first index contains parameters (shape, loc, scale) and the
            pvalue from a KS test.
        '''
        return_levels = da.map_blocks(cls._return_levels_distribution_chunk, 
                                      fitted_params, distribution,
                                      return_periods)
        return return_levels

    @classmethod
    def _return_levels_distribution_chunk(cls, chunk, distribution,
                                      return_periods):
        '''
        Runs return_levels_distribution_point over an individual chunk.
        '''
        # Get input size, flatten and declare output arrays
        n_p, n_r, n_c = chunk.shape
        chunkF = chunk.reshape((n_p, n_r*n_c))
        n_pts = n_r*n_c
        n_rp = len(return_periods)
        out_array = np.zeros((n_rp, n_pts))*np.nan

        # Loop over each point in the chunk and call 
        # cls.return_levels_distribution_point
        for pt in range(n_pts):
            params = chunkF[:, pt]
            if len(params)>2:
                rl = cls._return_levels_distribution_point(params,
                                                           distribution,
                                                           return_periods)
                out_array[ :, pt] = rl

        return out_array.reshape((n_rp, n_r, n_c))
    
    @classmethod
    def _return_levels_distribution_point(cls, fitted_params, 
                                      distribution, return_periods):
        '''
        Estimate return levels for a single point. Expects params as a length
        3 tuple or array, distribution as a scipy distribution object and 
        return periods as a list of desired return periods.
        
        Calculates return levels as the inverse survival function of 
        1/(return periods)
        '''
        return_periods = np.array(return_periods)
        fitted_params = np.array(fitted_params)
        return_levels = distribution.isf(1/return_periods, fitted_params[0], 
                                         fitted_params[1], fitted_params[2])
        return return_levels
    
    @classmethod
    def return_periods_distribution(cls, fitted_params, 
                               distribution, return_periods):
        '''
        Calculates return levels from an extreme value distribution fit.
        
        Args:
            fitted_params       : Array of fitted parameters of shape 
                                  (n_params, n_x, n_y)
            distribution        : scipy.stats distribution class
            return_periods      : List or array of return periods

        Returns:
            An uncomputed dask array of shape (4, n_x, n_y).
            The first index contains parameters (shape, loc, scale) and the
            pvalue from a KS test.
        '''
        return_levels = da.map_blocks(cls._return_periods_distribution_chunk,
                                      fitted_params, distribution, 
                                      return_periods)
        return return_levels

    @classmethod
    def _return_periods_distribution_chunk(cls, chunk, distribution, 
                                       return_levels):
        '''
        Runs _return_periods_distribution_point over every point in a chunk
        array  (dask or numpy). Also requires a scipy.stats distribution class
        and list of return levels
        '''
        
        # Get shape of input chunk, flatten and initialise output arrays
        n_p, n_r, n_c= chunk.shape
        chunkF = chunk.reshape((n_p, n_r*n_c))
        n_pts = n_r*n_c
        n_rp = len(return_levels)
        out_array = np.zeros((n_rp, n_pts))*np.nan

        # Loop over every point in chunk
        for pt in range(n_pts):
            params = chunkF[:, pt]
            if len(params)>2:
                rp = cls._return_periods_distribution_point(params,
                                                            distribution, 
                                                            return_levels)
                out_array[:, pt] = rp

        return out_array.reshape((n_rp, n_r, n_c))
    
    @classmethod
    def _return_periods_distribution_point(cls, params, distribution, 
                                       return_levels, omit_above=np.inf):
        '''
        Estimate return periods for a single point. Expects params as a length 
        3 tuple or array, distribution as a scipy distribution object and 
        return levels as a list of desired return levels.
        
        Calculates return periods as the reciprocal of (1 - 
        distribution.CDF(return_levels))
        '''
        return_levels = np.array(return_levels)
        params = np.array(params)
        
        # Get return probabilities
        return_periods = 1-distribution.cdf(return_levels, params[0], 
                                            params[1], params[2])
        
        # Take the reciprical to transform probabilities into periods
        return_periods[np.isclose(return_periods,0)] = np.nan
        return_periods = 1/return_periods
        
        # Mask out any periods that are above omit_above
        return_periods[return_periods>omit_above] = np.nan
                                 
        return return_periods
        
    @classmethod
    def return_periods_poisson(cls, array, return_levels):
        
        n_rl = len(return_levels)
        return_periods = da.zeros((n_rl, array.shape[1], array.shape[2]))
        
        for rl in range(n_rl):
            rl_total = da.count_nonzero( ~np.isnan(array) , axis =0 )
            rl_count = da.count_nonzero( array >= return_levels[rl], axis=0 )
            rl_count[da.isclose(rl_count, 0)]
            return_periods[rl] = rl_total / rl_count
            
        return return_periods
    

class extremes():
    @classmethod
    def annual_maxima(cls, dataset, time_dim = 'time', min_datapoints=300):
        '''
        Takes in an xarray datasets and returns a new grouped dataset of
        annual maxima. The returned dataset needs to be computed (.compute() 
        or .to_netcdf()).
        '''
        grouped = dataset.groupby('time.year')
        grouped_count = dataset.count(dim='time')
        grouped_max = grouped.max(dim='time', skipna=True)
        annual_maxima = grouped_max.where(grouped_count >= min_datapoints)
        
        return annual_maxima.chunk({'year':-1})