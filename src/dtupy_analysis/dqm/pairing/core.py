def matrix_diff(df, l_i, l_j, col, f = None, engine = 'dask', **kwargs):
    # Layer j > layer i
    # row N == hit N in layer j
    # col M == hit M in layer i

    if engine == 'dask':
        from .dask_backend import matrix_diff as _matrix_diff
        return _matrix_diff(df, l_i, l_j, col, f = f, **kwargs)

def distance_matrix(df_ssl, l_i, l_j, engine = 'dask', **kwargs):
    if engine == 'dask':
        from .dask_backend import distance_matrix as _distance_matrix
        return _distance_matrix(df_ssl, l_i, l_j, **kwargs)

def get_pairs(diff, engine = 'dask', **kwargs):
    if engine == 'dask':
        from .dask_backend import get_pairs as _get_pairs
        return _get_pairs(diff, **kwargs)

def pair_hits(df_ssl, l_i, l_j, engine = 'dask', **kwargs):
    if engine == 'dask':
        from .dask_backend import pair_hits as _pair_hits
        return _pair_hits(df_ssl, l_i, l_j, **kwargs)