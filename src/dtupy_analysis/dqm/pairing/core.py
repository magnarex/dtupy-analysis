import .dask_backend as dask_

def matrix_diff(df, l_i, l_j, col, f = None, engine = 'dask'):
    # Layer j > layer i
    # row N == hit N in layer j
    # col M == hit M in layer i

    if engine == 'dask':
        
        return dask_.matrix_diff(df, l_i, l_j, col, f = f)