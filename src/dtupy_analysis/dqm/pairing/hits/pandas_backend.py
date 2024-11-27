from typing import TYPE_CHECKING
import numpy as np

from ....dqm            import TDCTime
from ....dqm            import u
from ..pandas_backend   import matrix_diff as _matrix_diff

if TYPE_CHECKING:
    import pandas as pd
    from collections.abc import Callable

f_index_dict = {
    'ignore' : lambda x: np.zeros_like(x),
    'none'   : lambda x: 1*x,
    'std'    : lambda x: x / x.std(),
    'log'    : lambda x: np.log10(x.astype(int)),
    'pow2'   : lambda x: np.power(x.astype(int),2),
}


def matrix_diff(df : 'pd.DataFrame', sl_bot : int, sl_top : int, col : str, f : 'Callable' = None):
    return _matrix_diff(df, sl_bot, sl_top, col, f = f, diff_by = 'layer')

def distance_matrix(df : 'pd.DataFrame', l_bot : int, l_top : int, f_index = 'log'):
    df_ssl = df.copy().reset_index(names=['r_index'])
    # Time distance
    d_bx    = matrix_diff(df_ssl, l_bot, l_top, 'bx', f = np.int8)                           # "This should be 0", ssome hits may come from a previous orbit, so not always true 
    d_tdc   = matrix_diff(df_ssl, l_bot, l_top, 'tdc', f = np.int16)
    if 'orbit' in df_ssl.columns:
        d_ob = matrix_diff(df_ssl, l_bot, l_top, 'orbit', f = np.int64)
    else:
        d_ob = np.zeros_like(d_tdc)
    d_tdct  = np.abs(TDCTime(d_ob, d_bx, d_tdc).astype(int))               # We need to account for the clock reset so we use the module (32*3556) tdc times per cycle and THIS difference should be less than 32

    # Pseudo-distance in time
    d_idx  = np.abs(matrix_diff(df_ssl, l_bot, l_top, 'r_index', f = f_index_dict[f_index]))

    # Distance in space
    d_cell = matrix_diff(df_ssl, l_bot, l_top, 'cell', f = np.int8)
    
    # Same link
    if 'link' in df_ssl.columns:
        d_link = matrix_diff(df_ssl, l_bot, l_top, 'link', f = np.int8)
    else:
        # dtupy doesn't have the link information as long as I'm aware
        d_link = 0
    
    if (l_bot % 2) == 0:
        # If even bottom layer
        d_cell = np.abs(d_cell - 0.5)
    else:
        # If odd bottom layer
        d_cell = np.abs(d_cell + 0.5)

    # print(d_cell.min(), d_cell.max(), d_cell.mean(), d_cell.std())
    # Mask with only the real possible combinations
    mask = (d_tdct < u.TDC_MAXDIFF) & (d_link == 0) & (d_cell <= 0.5)
    # TODO: Include a flag that will allow to use (d_ob == 0) as a mask for collision

    # Add up all distances
    d_total = d_idx + np.where(d_tdct < u.TDC_MAXDIFF, d_tdct, np.inf) + np.where(d_cell <= 0.5, 0, np.inf) + np.where(d_link == 0, 0, np.inf)
    # leave this out beacuse I don't want to bias the selection (but this value should always be 0.5 for valid tracks)
    
    # d_total = np.where(mask, d_total, np.inf)

    return d_total, mask

def pair_hits(df, l_bot, l_top, f_index='log'):
    from ..pandas_backend import get_pairs
    d_total, mask = distance_matrix(df, l_bot, l_top, f_index=f_index)
    return get_pairs(d_total, mask)

def pair_index(df, l_bot, l_top, original = False, f_index='log'):
    from ..pandas_backend import get_pair_index
    pairs, (row_min, col_min) = pair_hits(df, l_bot, l_top, f_index=f_index)
    if original:
        return  get_pair_index(pairs, df, l_bot, l_top),\
                (get_pair_index(row_min, df, l_bot, l_top), get_pair_index(col_min, df, l_bot, l_top)),\
                pairs,\
                (row_min, col_min)
    else:
        return  get_pair_index(pairs, df, l_bot, l_top),\
                (get_pair_index(row_min, df, l_bot, l_top), get_pair_index(col_min, df, l_bot, l_top))
