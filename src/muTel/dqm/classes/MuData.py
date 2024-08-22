import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import itertools
import matplotlib as mpl
import matplotlib.pyplot as plt
from pathlib import Path


class MuData(object):
    def __init__(self, data):
        if isinstance(data, pd.DataFrame):
            self._data = data
        else:
            raise TypeError(f"'data' type {type(data)} is not within the allowed types: pandas.DataFrame")

        self._mule_list = {}
        self._mush_list = {}
        self._muse_list = {}
    
    def pair_mules(self):
        self._buffer = {}


    
    @staticmethod
    def _get_pairs(diff_matrix, mask = None):
        
        which_bot_min = [[i,e] for i,e in enumerate(diff_matrix.argmin(axis=1))] # Which bot hit has min disntace with top.
        which_top_min = [[e,i] for i,e in enumerate(diff_matrix.argmin(axis=0))] # Which top hit has min disntace with bot.

        pairs = set(tuple(pair) for pair in which_top_min).intersection(set(tuple(pair) for pair in which_bot_min))

        # Now we can check the indexes where the pair of hits are produced in contiguous cells.
        if mask:
            allowed_pairs  = np.c_[np.where(mask)].tolist()
            pairs = list(pairs.intersection(set(tuple(pair) for pair in allowed_pairs)))

        return pairs, which_top_min, which_bot_min
        
        
        # We check the intersection of both sets to obtain the pairs where the minimum coincides and then
        # we select only the ones where the cells were contiguous.
        
    @staticmethod
    def _plot_matrix(total_diff, pairs, which_top, which_bot):
        fig, ax = plt.subplots(1,1,figsize=(10,10))
        cm = mpl.colormaps.get_cmap('viridis_r')
        cm.set_bad('k')
        artist = ax.matshow(total_diff,cmap=cm)
        fig.colorbar(artist,ax = ax)

        ax.scatter(
                *np.c_[which_bot].T[::-1],
                marker='+',
                color='r',
                linewidth = 1,
                label   = 'col min',
                zorder = 4,
                s = 200
            )

        ax.scatter(
                *np.c_[which_top].T[::-1],
                marker='x',
                color='r',
                linewidth = 1,
                label   = 'row min',
                zorder = 4,
                s = 200
            )

        ax.scatter(
                *np.c_[list(pairs)].T[::-1],
                marker='s',
                color='r',
                linewidth = 1,
                label='selected',
                # facecolor='none'
                zorder = 5,
                s = 100
            )

        fig.legend(loc=2, ncols=3)
        fig.tight_layout()
        
        return fig, ax

    @staticmethod
    def make_test_matrix(N,A,b=1):
        diff = (1 - np.eye(N)) * np.random.uniform(0,1,(N,N))
        diff -= A*sum([
            (np.random.uniform(0,1,(N,N))*np.eye(N,k=k)
            +
            np.random.uniform(0,1,(N,N))*np.eye(N,k=-k))*np.exp(-b*k)
            for k in range(1,N+1)
        ],start=np.zeros((N,N)))
        
        return diff

if __name__ == '__main__':
    from muTel.dqm.classes.MuFile import MuFile    
    table_path = '/afs/ciemat.es/user/m/martialc/public/muTel_v4/muTel/data/sxa5_data.parquet'
    file = MuFile(table_path)
    data = MuData(next(file))
    diff = MuData.make_test_matrix(50,10,0.1)
    p, t, b = MuData._get_pairs(diff)
    fig, ax = MuData._plot_matrix(diff, p, t, b)
    fig.savefig('fig.png')