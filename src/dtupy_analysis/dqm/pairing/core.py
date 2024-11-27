import pandas           as pd
import dask.dataframe   as dd
import dask.array as da
import numpy as np

def matrix_diff(df, l_i, l_j, col, f = None, engine = 'dask', **kwargs):
    # Layer j > layer i
    # row N == hit N in layer j
    # col M == hit M in layer i

    if   isinstance(df, dd.DataFrame):
        from .dask_backend      import matrix_diff as _matrix_diff
    elif isinstance(df, pd.DataFrame):
        from .pandas_backend    import matrix_diff as _matrix_diff
    return _matrix_diff(df, l_i, l_j, col, f = f, **kwargs)

def get_pairs(diff : 'da.Array | np.ndarray', mask, **kwargs):
    if   isinstance(diff, da.Array):
        from .dask_backend      import get_pairs as _get_pairs
    elif isinstance(diff, np.ndarray):
        from .pandas_backend    import get_pairs as _get_pairs        
    else:
        print(f'No se ha reconocido el tipo {type(diff)}')
    return _get_pairs(diff, mask, **kwargs)

def get_pair_index(pairs : 'set[tuple[int, int]]', df : 'dd.DataFrame |pd.DataFrame', l_i, l_j, **kwargs):
    if   isinstance(df, dd.DataFrame):
        raise NotImplementedError('Backend for "dask" is not available for this function, sorry :/')
        from .dask_backend      import get_pair_index as _get_pair_index
    elif isinstance(df, pd.DataFrame):
        from .pandas_backend    import pair_hits as _get_pair_index
    
    return _get_pair_index(pairs, df, l_i, l_j, **kwargs)
