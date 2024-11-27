import pandas as pd
import dask.dataframe as dd
import numpy as np
import dask.array as da


def distance_matrix(df_ssl, l_i, l_j, engine = 'pandas', **kwargs):
    if   isinstance(df_ssl, dd.DataFrame):
        from .dask_backend      import distance_matrix as _distance_matrix
    elif isinstance(df_ssl, pd.DataFrame):
        from .pandas_backend    import distance_matrix as _distance_matrix
        
    return _distance_matrix(df_ssl, l_i, l_j, **kwargs)

def pair_hits(df_ssl, l_i, l_j, engine = 'pandas', **kwargs):
    if   isinstance(df_ssl, dd.DataFrame):
        from .dask_backend      import pair_hits as _pair_hits
    elif isinstance(df_ssl, pd.DataFrame):
        from .pandas_backend    import pair_hits as _pair_hits
    
    return _pair_hits(df_ssl, l_i, l_j, **kwargs)

def pair_index(df, l_i, l_j, original = False, **kwargs):
    if   isinstance(df, dd.DataFrame):
        raise NotImplementedError('Backend for "dask" is not available for this function, sorry :/')
        from .dask_backend      import pair_index as _pair_index
    elif isinstance(df, pd.DataFrame):
        from .pandas_backend    import pair_index as _pair_index
    
    return _pair_index(df, l_i, l_j, original = original, **kwargs)
