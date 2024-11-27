from typing import TYPE_CHECKING
from numbers import Number
import asyncio

import numpy as np

import dtupy_analysis.dqm.pairing.hits as pairing

if TYPE_CHECKING:
    from .MuData import MuData
    import pandas as pd

class MuSL:
    __module__ = 'dtupy_analysis.dqm.core'
    def __init__(self, data : 'MuData', station, sl, engine = 'dask'):
        self._engine = engine
        self._data = data
        self._station = station
        self._sl = sl
        
        self._pairs = {}
        self._tryads = {}
        self._tetrads = {}
    
    @property
    def engine(self):
        return self._engine
    
    @classmethod
    def from_dask(self, data : 'MuData', station : int, sl : int):
        return MuSL(data, station, sl, engine='dask')
        
    @classmethod
    def from_pandas(self, data : 'MuData', station : int, sl : int):
        return MuSL(data, station, sl, engine='pandas')

    @property
    def data(self) -> 'pd.DataFrame | dd.DataFrame':
        return self._data[(self._data.sl == self.sl) & (self._data.station == self.station)]
    
    @property
    def sl(self):
        return self._sl
    @property
    def station(self):
        return self._station
    
    @property
    def layers(self):
        return sorted(set(self.data.layer.unique().tolist()))
    
    @property
    def pairs(self):
        return self._pairs
    @property
    def lwl(self):
        return sorted(list(self.pairs.keys()), reverse=True)
    
    def _repr_html_(self):
        return self.data._repr_html_()
    
    def __str__(self):
        return f'{self.__class__.__name__} @ (st: {self.station}, sl: {self.sl})'
    
    def pair_hits(self, f_index = 'log', debug = False):
        for (l_i, l_j) in zip(self.layers[:-1], self.layers[1:]):
            id = pairing.LwL(l_i, l_j)
            if debug: print(f'Pairing layers {id} @ ST{self.station}SL{self.sl}')
            pairs, _ = pairing.pair_index(self.data, l_i, l_j, f_index=f_index)
            self._pairs[id]= dict(pairs)
        return self

    def yoke_pairs(self):
        # TODO: asegurarse de que han sido emparejadas
        # Recorrer todas las parejas de filas hechas para convertirlas en tríadas
        for lwl in self.lwl[:-1]:
            top_j = list(self.pairs[lwl  ].keys()  )
            top_i = list(self.pairs[lwl  ].values())
            bot_j = list(self.pairs[lwl-1].keys()  )

            idx_top, idx_bot = np.where((np.c_[top_i] - np.r_[bot_j] ) == 0)
            tryad = self._tryads.get(lwl*(lwl-1), dict())
            self._tryads[lwl*(lwl-1)] = tryad | {
                i : [
                    top_j[m_top],
                    self.pairs[lwl]  .pop(top_j[m_top]),
                    self.pairs[lwl-1].pop(bot_j[n_bot]),
                ]
                for i, (m_top, n_bot) in enumerate(zip(idx_top, idx_bot))
            }
        return

    def yoke_tryads(self):
        for lwlwl, tryad in self.tryads.items():
            lwl = lwlwl % 4 # gets id for missing 2-group
            if len(tryad) * len(self.pairs[lwl]) == 0: continue
            
            if lwlwl > lwl:
                # 3-group is aligned left
                top = np.array(list(tryad.values()))[:,-1]
                bot = list(self.pairs[lwl].keys())
            else:
                # 3-group is aligned right
                top = list(self.pairs[lwl].values())
                bot = np.array(list(tryad.values()))[:,0]
                
            idx_top, idx_bot = np.where((np.c_[top] - np.r_[bot] ) == 0)
            for i, (m_top, n_bot)  in enumerate(zip(idx_top, idx_bot)):
                if lwlwl > lwl:
                # 3-group is aligned left
                    trace = [*tryad.pop(m_top), self.pairs[lwl].pop(bot[n_bot])]
                else:
                # 3-group is aligned right
                    trace = [self.pairs[lwl].pop(top[m_top]), *tryad.pop(n_bot)]
                self._tetrads[i] = trace
    
    async def async_pair_hits(self):
        result = await asyncio.to_thread(self.pair_hits)        
        return result
    
    async def async_yoke_pairs(self):
        result = await asyncio.to_thread(self.yoke_pairs)        
        return result
    
    async def async_yoke_tryads(self):
        result = await asyncio.to_thread(self.yoke_tryads)        
        return result
    
    def make_segments(self, f_index='log'):
        self.pair_hits(f_index=f_index)
        self.yoke_pairs()
        self.yoke_tryads()
        return self

    async def async_make_segments(self):
        await self.async_pair_hits()
        await self.async_yoke_pairs()
        await self.async_yoke_tryads()
        return self
    
    @property
    def tryads(self):
        return self._tryads
    @property
    def tetrads(self):
        return self._tetrads
    @property
    def segments(self):
        return {pairing.LnL(*self.layers) : self._tetrads} | self._tryads
