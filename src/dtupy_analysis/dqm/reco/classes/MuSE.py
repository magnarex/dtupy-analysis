import asyncio

import numpy as np
import pandas as pd

from .PattID            import PattID
from ...core.classes.TDCTime import TDCTime
from ...core.units      import (TDCTIME_UNIT    ,
                                BX_UNIT         ,
                                BXTIME_UNIT     ,
                                CELL_HEIGHT     ,
                                CELL_WIDTH      )


class MuSE(object):
    __module__ = 'dtupy_analysis.dqm.reco'
    def __init__(self, segment, data, nlayers=4) -> None:
        self._nlayers = nlayers
        self._seg  = segment.copy(deep=True)
        self._seg_idx = segment.name
        
        drop_cols = ['sl', 'station', 'link', 'obdt_ctr', 'obdt_type', 'channel']
        
        # Create the DataFrame with the data from each hit
        self._data = data.loc[segment.seg]\
                         .reset_index()\
                         .rename(columns = {'index' : 'df_index'})\
                         .set_index('layer')\
                         .drop(filter(lambda col: col in data.columns, drop_cols), axis = 1)\
                         .sort_index(ascending=False)
                         
        self._data['cell_global'] = self.data.cell - self.cell_offset
        self._data['wire_x'     ] = CELL_WIDTH*self.data['cell_global']

        self._data['wire_z'     ] = (self.data.index - nlayers/2 - .5) * CELL_HEIGHT
        if 'orbit' in self.data.columns:
            self._data['time'       ] = self.data.apply(lambda row: TDCTime(row.orbit, row.bx, row.tdc), axis = 1)
        else:
            self._data['time'       ] = self.data.apply(lambda row: TDCTime(row.bx, row.tdc), axis = 1)
        # Once we have TDCTime, we can compute other quantities
        self._data['time_cor'   ] = self.data.time - self.t0_bx
        try:
            self._pattID = PattID(''.join(['R' if rel == 1 else 'L' if rel == -1 else 'X' for rel in self.cell_diff]), missing = self.missing)
        except Exception as e:
            # display(self._seg)
            # display(self._data)
            print(''.join(['R' if rel == 1 else 'L' if rel == -1 else 'X' for rel in self.cell_diff]))
            raise e
        
        self._stats = None
        
        
    def __str__(self) -> str:
        return f'{self.__class__.__name__}({self.lnl}) @ (st: {self.st}, sl: {self.sl})'
    
    def __repr__(self):
        return self.data.__repr__()
    
    def _repr_html_(self):
        return self.flatten()._repr_html_()
    
    @property
    def nlayers(self):
        return self._nlayers
    
    @property
    def cell_offset(self):
        return pd.Series(
            np.repeat(
                [[-0.5, 0]],
                np.ceil(self.nlayers/2),
                axis=0
            ).ravel()[:self.nlayers],
            index = sorted(self.layers)
        ).sort_index(ascending=False) - 1
    
    @property
    def cell_diff(self):
        return np.nan_to_num(np.diff(2*(self.df.cell - self.cell_offset).to_numpy()))
    
    @property
    def pattID(self):
        return self._pattID
    
    @property
    def data(self):
        data = self._data
        data['sl'] = self.sl
        data['st'] = self.st
        return data
    @property
    def seg(self):
        return self._seg
    
    @property
    def hits(self):
        return self._seg.hits
    
    @property
    def lnl(self):
        return self._seg.lnl
    
    @property
    def layers(self):
        return np.arange(1, self.nlayers+1)
    
    @property
    def sl(self):
        return self._seg.sl
    
    @property
    def st(self):
        return self._seg.st
    
    @property
    def nhits(self):
        return len(self.data)
    
    @property
    def bx0(self):
        return self.data.bx.min()
    
    @property
    def t0_bx(self):
        return self.data.time.min().with_tdc(0)
    
    @property
    def df(self):
        df = self._data.copy().astype(float)
        
        for l in self.layers:
            if l not in df.index:
                df.loc[l] = np.nan
        
        df['wire_z'     ] = (df.index - len(self.layers)/2 - .5) * CELL_HEIGHT
        
        return df.sort_index(ascending=False)
    
    def __getattr__(self, name):
        if name in self.df.columns:
            return self.df[name]
        # elif name in dir(self.pattID):
        #     return getattr(self.pattID, name)
        else:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
    
    @property
    def lnl(self):
        return self.pattID.lnl
    
    @property
    def lats(self):
        return self.pattID.lats
    
    @property
    def patt(self):
        return self.pattID.patt
    
    @property
    def missing(self):
        return np.isnan(self.time_cor)

    def fit(self, v_drift = None, t0 = None, do_plot = False, verbose=0):
        import warnings
        from .MuSeg import MuSeg
        
        stats = []
        if do_plot:
            import matplotlib.pyplot as plt
            fig, axes = plt.subplots(len(self.lats), 1, figsize=(10, 2*len(self.lats)))
            axes = axes.ravel()
        else:
            axes = [None]*len(self.lats)
        for ax, lat in zip(axes, self.lats):
            model = MuSeg(self, lat, v_drift = v_drift, t0 = t0)
            try:
                with warnings.catch_warnings(action='ignore'):
                    args, cov = model.fit()
            except RuntimeError:
                continue
            model.args = args
            if do_plot: model.plot(ax)
            stats.append({'latsID' : lat,'theta': model.theta, 'x0': model.x0, 'v': model.v_drift, 't0': model.t0, 'chi2': model.chi2, 'res' : model.res})
       
        if do_plot:
            plt.tight_layout()
            plt.show()
        
        stats = pd.DataFrame(stats)
        if (stats.chi2 == stats.chi2.min()).sum() > 1:
            msg = 'Multiple minima found in segment {}, adding T0 bias to chi2'.format(self._seg_idx)
            if verbose > 0: warnings.warn(msg)
            goodness_score = stats.chi2 + np.log10(np.abs(stats.t0))
            best_fit = stats.loc[[goodness_score.idxmin()]]
        else:
            best_fit = stats.loc[[stats.chi2.idxmin()]]
            
        self._stats = stats
        
        self._best_fit = best_fit.drop(columns=['res'])
        
        return stats
    
    async def async_fit(self, v_drift = None, t0 = None):
        return await asyncio.to_thread(self.fit, v_drift = v_drift, t0 = t0)
    
    @property
    def is_fitted(self):
        return self._stats is not None
    
    def flatten(self):
        new_dict = {}
        new_dict['seg_idx'] = np.uint64(self._seg_idx)
        # new_dict['hits'] = [tuple(self.data.df_index.astype(np.uint64).values.tolist())]
        # new_dict['nhits'] = self.nhits
        new_dict['st'] = np.uint8(self.st)
        new_dict['sl'] = np.uint8(self.sl)
        new_dict['bx0'] = np.uint64(int(self.t0_bx))

        for l, data in self.df.iterrows():
            new_dict[f't{l}'] = float(data['time_cor'])

        new_dict['pattID'] = str(self.pattID)
        if self.stats is not None:
            df = pd.concat([pd.DataFrame([new_dict]), self.best_fit.reset_index(drop=True).drop(columns=['seg_idx'])], axis=1).astype({'latsID':str})
        else:
            df = pd.DataFrame([new_dict]).reset_index(drop=True)
        
        return df.astype({'pattID':'category'})

    @property
    def stats(self):
        return self._stats
    @property
    def best_fit(self):
        return self._best_fit