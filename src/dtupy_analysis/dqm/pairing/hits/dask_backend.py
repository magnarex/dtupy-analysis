import dask.array as da
import numpy as np

from ..dask_backend import matrix_diff

def distance_matrix(df_ssl, l_i, l_j, block_size_limit = '400MB'):
    from dask.array import abs as _abs, log10 as _log10
    
    # Time distance
    d_bx    = matrix_diff(df_ssl, l_i, l_j, 'bx', block_size_limit = block_size_limit)                           # "This should be 0", come hits may come from a previous orbit, so not always true 
    d_tdc   = matrix_diff(df_ssl, l_i, l_j, 'tdc', block_size_limit = block_size_limit)
    d_tdct  = _abs(32*d_bx + d_tdc) % (32*3556)                             # We need to account for the clock reset so we use the module (32*3556) tdc times per cycle and THIS difference should be less than 32

    # Pseudo-distance in time
    d_idx  = da.abs(matrix_diff(df_ssl, l_i, l_j, 'index', f = _log10, block_size_limit = block_size_limit))
    
    # Distance in space
    d_cell = da.abs(matrix_diff(df_ssl, l_i, l_j, 'cell', block_size_limit = block_size_limit))
    
    # Now, this needs correcting the staggering.
    # For (1,2) and (3,4) we need to add 0.5 to the difference, for (2,3) we need to substract 0.5
    if l_j % 2 == 0:
        d_cell = d_cell + 0.5
    else:
        d_cell = d_cell - 0.5

    # Mask with only the real possible combinations
    mask = (d_tdct < 512) & (d_cell < 1)

    # Add up all distances
    d_total = (d_cell + d_tdct + d_idx)
    
    d_total = da.where(mask, d_total, np.inf)

    return d_total, mask

  

def pair_hits(df, l_i, l_j, block_size_limit = '400MB'):
    d_total, mask = distance_matrix(df, l_i, l_j, block_size_limit = block_size_limit)
    return get_pairs(d_total, mask)