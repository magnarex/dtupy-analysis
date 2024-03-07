import muTel.utils.meta as meta
from muTel.dqm.classes.MuLE import MuLE, MuSH

import logging
import pandas as pd
import numpy as np
import sys

_MuData_logger = logging.Logger('MuON')
_MuData_logger.addHandler(logging.StreamHandler(sys.stdout))


# MuSE significa "Muon Superlayer Event", pero suena como "musa" en inglés

class MuSEType(type):
    def __repr__(self):
        return self.__name__
    
class MuSE(object, metaclass = MuSEType):
    __slots__ = ['_patternID','_data','_mules','_mushs','_new_mule','_new_mush','_id','_eventnr','_sl']
    def __init__(self, id = None) -> None:
        self._patternID = 'XXX'
        self._data = None

        self._new_mule = False
        self._new_mush = False

        self._mules = []
        self._mushs = []

        self._id = id
        self._eventnr = None
        self._sl = None

    def set_and_assert(self, mule : MuLE | MuSH):
        if self._eventnr is not None:
            try:
                assert self._eventnr == mule.eventnr
            except AssertionError:
                raise Exception(f'Hay hits de distintos eventos en este MuLE.')
        else:
            self._eventnr = mule.eventnr

        if self._sl is not None:
            try:
                assert self._sl == mule.sl
            except AssertionError:
                raise Exception(f'Hay hits de distintos eventos en este MuLE.')
        else:
            self._sl = mule.sl


    def add_mule(self, mule : MuLE):
        self.set_and_assert(mule)
        self._mules = self.mules + [mule]
        self._new_mule = True
        self.update_pattID()
        return self

    def add_mush(self, mush : MuSH):
        self.set_and_assert(mush)
        self._mushs = self.mushs + [mush]
        self._new_mush = True
        self.update_pattID()

        return self


    def update_pattID(self):
        if self._new_mule:
            mule = self.mules[-1]
            pattID_iloc = -int(mule.id[-1])
            patternID = mule.patternID

            pattID = list(self.patternID)
            pattID[pattID_iloc] = patternID
            self._patternID = ''.join(pattID)

        if (len(self.mules) == 1) and (len(self.mushs) == 1) and (self._new_mule or self._new_mush):
            df = self.df
            mush = self.mushs[-1]
            if mush.layer == 4:
                cell_diff = (df[df.layer == 2].cell - df[df.layer == 4].cell).values[0]
                pattID_iloc = 0                    
                    
            elif mush.layer == 1:
                cell_diff = (df[df.layer == 1].cell - df[df.layer == 3].cell).values[0]
                pattID_iloc = -1

            if   cell_diff ==  0: return self
            elif cell_diff == -1: patternID = 'L'
            elif cell_diff ==  1: patternID = 'R'
                    
            pattID = list(self.patternID)
            pattID[pattID_iloc] = patternID
            pattID[1]           = patternID
            self._patternID = ''.join(pattID)


    @property
    def layer(self):
        return self.data.layer
    
    @property
    def sl(self):
        return self.data.sl.iloc[0]

    @property
    def eventnr(self):
        return self._eventnr

    @property
    def full_data(self):
        data = pd.concat(
            [
                *list(map(lambda mule: mule.data, self.mules)),
                *list(map(lambda mush: mush.data, self.mushs))
            ])\
            .drop_duplicates()\
            .sort_values('layer',ascending=False)
            # .set_index('layer')\
            # .reindex(meta.layers)\
            # .sort_index(ascending=False)
            # .reset_index()\
            # .astype(meta.muse_data_type_dict)
        
        data['MuSEId'] = f'{self.eventnr}{self.id}'

        return data
                    
    @property
    def data(self):
        return self.full_data.drop(['EventNr','sl'],axis='columns')


    @property
    def nhits(self):
        if len(self.mules) <= 1:
           return 2*len(self.mules) + len(self.mushs)
        else:
            return len(self.mules) + 1
    
    @property
    def mules(self):
        return self._mules
    
    @property
    def mushs(self):
        return self._mushs
    
    @property
    def patternID(self):
        return self._patternID

    @property
    def allow_recon(self):
        return self.nhits >= 3

    @property
    def id(self):
        return self._id
    
    @property
    def ref_layer(self):
        return self.data.dropna().layer.values[0]
    
    @property
    def ref_cell(self):
        return self.data.dropna().cell.values[0]
    
    @property
    def cell_diff(self):
        return self.data.cell.values - self.ref_cell
    




    # @property
    # def df(self):
    #     t_d = []
    #     for layer in meta.layers:
    #         if layer in self.layer.tolist(): t_d.append(self.data[self.data.layer == layer].DriftTime.iloc[0])
    #         else: t_d.append(np.nan)
        
    #     return pd.Series(
    #         data=dict(zip(
    #             ['EventNr','sl','nhits',*[f't{l}' for l in meta.layers], 'patternID', 'MuSE'],
    #             [self.eventnr, self.sl, self.nhits, *t_d, self.patternID, self]
    #             ))
    #     ).to_frame().transpose()
    
    @property
    def df(self):
        return self.data
            
    
    # @property
    # def theta(self):
    #     return self._theta
    
    # @property
    # def x0(self):
    #     return self._x0
    
    # def set_pars(self,pars):
    #     self._x0, self._theta = pars