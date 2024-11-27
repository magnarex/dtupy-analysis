from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas as pd
    from typing import Callable

def matrix_diff(df : 'pd.DataFrame', l_bot : int, l_top : int, col : str, f : 'Callable' = None, diff_by = 'layer'):
    # Layer j > layer i
    # row N == hit N in layer j
    # col M == hit M in layer i
    
    df_top = np.reshape(df[df[diff_by] == l_top][col].to_numpy(), (-1,1))
    df_bot = np.reshape(df[df[diff_by] == l_bot][col].to_numpy(), (1,-1))

    if f:
        df_top = f(df_top)
        df_bot = f(df_bot)
    
    return df_bot - df_top

def get_pairs(df, mask):
    mask = np.c_[np.where(mask)].tolist()
    mask = set(tuple(p) for p in mask)
    
    col_min  = np.stack([df.argmin(axis=0), np.arange(df.shape[1])]).T # min for each col
    col_min  = set(map(tuple, col_min))
    cont_col = col_min.intersection(mask)
    
    row_min  = np.stack([np.arange(df.shape[0]), df.argmin(axis=1)]).T # min for each row
    row_min  = set(map(tuple, row_min))
    cont_row = row_min.intersection(mask)

    
    pairs    = cont_col.intersection(cont_row)
    
    return pairs, (row_min, col_min)

def get_pair_index(pairs : 'set[tuple[int, int]]', df : 'pd.DataFrame', l_bot, l_top):
    index_pairs = {
        (df[df.layer == l_top].index.values[pair_top], df[df.layer == l_bot].index.values[pair_bot])
        for (pair_top, pair_bot) in pairs
    }
    return index_pairs
    