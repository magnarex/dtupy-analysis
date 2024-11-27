import dask.array as da
import numpy as np

def matrix_diff(ddf, l_i, l_j, col, f= None, block_size_limit = '400MB'):
    # Layer j > layer i
    # row N == hit N in layer j
    # col M == hit M in layer i
    
    ddf_j = da.reshape(ddf[ddf.layer == l_j][col].to_dask_array(lengths=True), (-1,1))
    ddf_i = da.reshape(ddf[ddf.layer == l_i][col].to_dask_array(lengths=True), (1,-1))

    if f:
        ddf_i = f(ddf_i)
        ddf_j = f(ddf_j)
    
    diff = ddf_j - ddf_i
    if block_size_limit:
        return diff.rechunk(block_size_limit = block_size_limit, balance=True)
    else:
        return diff

def get_pairs(df, is_cont):
    is_cont = np.c_[da.where(is_cont)].tolist()
    is_cont = set(tuple(p) for p in is_cont)
    
    col_min  = da.stack([df.argmin(axis=0), da.arange(df.shape[1])]).T # min for each col
    col_min  = set(map(tuple, col_min.compute()))
    cont_col = col_min.intersection(is_cont)
    
    row_min  = da.stack([da.arange(df.shape[0]), df.argmin(axis=1)]).T # min for each row
    row_min  = set(map(tuple, row_min.compute()))
    cont_row = row_min.intersection(is_cont)

    
    pairs    = cont_col.intersection(cont_row)
    
    return pairs, (row_min, col_min)
