from typing import TYPE_CHECKING
import numpy as np

from ....dqm            import TDCTime
from ....dqm            import u
from ..pandas_backend   import matrix_diff as _matrix_diff

if TYPE_CHECKING:
    import pandas as pd
    from collections.abc import Callable

def matrix_diff(df : 'pd.DataFrame', sl_bot : int, sl_top : int, col : str, f : 'Callable' = None):
    return _matrix_diff(df, sl_bot, sl_top, col, f = f, diff_by = 'sl')

def distance_matrix(df : 'pd.DataFrame', sl_bot : int, sl_top : int):
    df_sl = df.copy().reset_index(names=['seg_idx'])
    # Time distance
    d_mt    = np.abs(matrix_diff(df_sl, sl_bot, sl_top, 'MT'   )) / df_sl[(df_sl.chi2 < 1) & (df_sl.MT > 200) & (df_sl.MT < 600)].MT.std()
    d_th    = np.abs(matrix_diff(df_sl, sl_bot, sl_top, 'theta')) / df_sl[(df_sl.chi2 < 1)].theta.std()
    d_bx0   = np.abs(matrix_diff(df_sl, sl_bot, sl_top, 'bx0'))   # "This should be 0", some hits may come from a previous orbit, so not always true
    d_idx   = np.abs(matrix_diff(df_sl, sl_bot, sl_top, 'seg_idx'))
    
    # Mask with only the real possible combinations
    mask = (d_mt < 1) & (d_th < 1)
    # TODO: Include a flag that will allow to use (d_ob == 0) as a mask for collision

    # Add up all distances
    d_total = d_mt + d_th + np.log10(np.clip(d_bx0, .1, np.inf))# + np.log10(np.clip(d_idx, 0.1, np.inf)) 
    
    # d_total = np.where(mask, d_total, np.inf)

    return d_total, mask

def pair_hits(df, l_bot, l_top):
    from ..pandas_backend import get_pairs
    d_total, mask = distance_matrix(df, l_bot, l_top)
    return get_pairs(d_total, mask)

def pair_index(df, l_bot, l_top, original = False):
    from ..pandas_backend import get_pair_index
    pairs, (row_min, col_min) = pair_hits(df, l_bot, l_top)
    if original:
        return  get_pair_index(pairs, df, l_bot, l_top),\
                (get_pair_index(row_min, df, l_bot, l_top), get_pair_index(col_min, df, l_bot, l_top)),\
                pairs,\
                (row_min, col_min)
    else:
        return  get_pair_index(pairs, df, l_bot, l_top),\
                (get_pair_index(row_min, df, l_bot, l_top), get_pair_index(col_min, df, l_bot, l_top))