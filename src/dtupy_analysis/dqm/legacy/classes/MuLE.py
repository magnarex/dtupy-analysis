import logging
import pandas as pd
import numpy as np
import sys
import muTel.utils.meta as meta
from typing import List

_MuData_logger = logging.Logger('MuLE')
_MuData_logger.addHandler(logging.StreamHandler(sys.stdout))


# MuLE significa "Muon Layer Event", pero suena como "mula" en inglés

class MuHitType(type):
    def __repr__(self):
        return self.__name__
    
class MuLE(object, metaclass = MuHitType):
    __slots__ = ['_data', '_complete','_hit_d_max', '_patternID', '_id', '_cell_d', '_within_d', '_hit_d']
    def __init__(self) -> None:
        self._data = None
        self._complete = False
        self._hit_d_max = np.inf
        self._patternID = 'X'
        self._id = None


    @property
    def df(self):
        return self.data

    @property
    def layer(self) -> list:
        '''
        Propiedad que dice las layers en las que se encuentran estos hits.
        '''
        return self.data.layer
    
    @property
    def sl(self) -> list:
        '''
        Propiedad que guarda los datos en un DataFrame. Es read-only.
        '''
        try:
            assert np.unique(self.data.sl).size == 1

        except AssertionError:
            raise ValueError('Se han mezclado hits de superlayers distintas.')
        
        return np.unique(self.data.sl)[0]
    
    @property
    def data(self) -> pd.DataFrame:
        '''
        Propiedad que guarda los datos en un DataFrame. Es read-only.
        '''
        return self._data
    
    def add_hit(self, hit_df : pd.DataFrame | pd.Series | List[pd.DataFrame | pd.Series]):
        '''
        Añadimos un hit al Layer Event en forma de fila de un pd.DataFrame
        '''
        if self._complete:
            raise ValueError('No se ha podido añadir el evento. Ya se ha alcanzado el número máximo de hits (2).')

        if isinstance(hit_df, pd.DataFrame | pd.Series | List[pd.DataFrame | pd.Series]):
            if self._data is None:
                self._data = hit_df
            else:
                self._data = pd.concat([self._data, hit_df]).sort_values('layer',ascending=False)

        elif isinstance(hit_df, pd.DataFrame | pd.Series | List[pd.DataFrame | pd.Series]):
            if self._data is None:
                self._data = pd.concat(hit_df).sort_values('layer',ascending=False)
            else:
                self._data = pd.concat([self._data, *hit_df]).sort_values('layer',ascending=False)
        
        else:
            raise ValueError('No se ha podido añadir el evento. El tipo de self.data no es correcto, sólo\
                             se permite "pd.DataFrame" o "pd.Series".')
   
        if len(self.data) == 2: self.complete = True
        elif len(self.data) > 2:
            raise ValueError('Se han intentado añadir más de 2 eventos a un mismo MuLE.')
            
        
        return self
    
    @property
    def cell_d(self):
        if not hasattr(self,'_cell_d'):
            self._calc_cell_d()
        try:
            return self._cell_d
        except AttributeError:
            return self._cell_d
            
    def _calc_cell_d(self):
        if not self._complete:
            raise ValueError('No se puede calcular la disntacia hasta que no haya 2 hits.')
        else:
            # El pd.DataFrame está ordenado en orden descendente de layer, así que hay que
            # cambiarle el signo.
            cell_diff = np.diff(self.data.cell.values)

            # Miramos la capa inferior para saber el staggering y corregirlo.
            if (self.layer.min() % 2 == 0):
                self._cell_d = cell_diff - 0.5
            else:
                self._cell_d = cell_diff + 0.5
        
    @property
    def hit_d(self):
        if not hasattr(self,'_hit_d'):
            self._calc_hit_d()
        try:
            return self._hit_d
        except AttributeError:
            return self._hit_d

    def _calc_hit_d(self):
        self._hit_d = np.diff(self.data.hit.values)
        self.check_if_within_d()

    @property
    def within_d(self):
        return self._within_d

    @property
    def hit_d_max(self):
        return self._hit_d_max
    
    @hit_d_max.setter
    def hit_d_max(self,value):
        self._hit_d_max = value
        self.check_if_within_d()
        
    @property
    def total_d(self):
        return np.abs(self.cell_d) + np.abs(self.hit_d)
        
    def check_if_within_d(self):
        self._within_d = self.hit_d < self.hit_d_max

    @property
    def id(self):
        '''
        Propiedad que dice qué tipo de MuLE es, guarda la misma información que
        self.layer, pero en formato que pueda ser usado con MuEvent.
        '''
        return self._id
    
    @property
    def patternID(self) -> str:
        if self.cell_d < 0:
            return 'L'
        else:
            return 'R'

    @property
    def complete(self):
        return self._complete
    @complete.setter
    def complete(self,value):
        if value:
            self._id = 'w'.join(map(lambda x: str(x), sorted(self.layer,reverse=True)))
        
        self._complete = value
        
    @property
    def eventnr(self):
        return np.int32(self.data.EventNr.values[0])


class MuSH(MuLE, metaclass = MuHitType):

    def __init__(self) -> None:
        super().__init__()


    def add_hit(self, hit_df : (pd.DataFrame | pd.Series)):
        '''
        Añadimos un hit al Standalone Hit en forma de fila de un pd.DataFrame
        '''
        if self._complete:
            raise ValueError('No se ha podido añadir el evento. Ya se ha alcanzado el número máximo de hits.')

        if not isinstance(hit_df, (pd.DataFrame | pd.Series)):
            print(hit_df,type(hit_df))
            raise ValueError('No se ha podido añadir el evento. El tipo de hit_df no es correcto, sólo\
                              se permite "pd.DataFrame" o "pd.Series".')


        if self._data is None:
            self._data = hit_df
        else:
            raise ValueError('No se ha podido añadir el evento. El tipo de self.data no es correcto, sólo\
                             se permite "pd.DataFrame" o "pd.Series".')
   
        if len(self.data) == 1:
            self.complete = True  
        return self
    
    @property
    def complete(self):
        return self._complete
    
    @complete.setter
    def complete(self,value):
        if value:
            self._id = str(self.layer)
        self._complete = value 
            
    @property
    def patternID(self) -> str:
        return 'X'

    @property
    def layer(self) -> list:
        '''
        Propiedad que dice las layers en las que se encuentran estos hits.
        '''
        try:
            return self.data.layer.values[0]
        except AttributeError:
            return self.data.layer.values[0]





if __name__ == '__main__':
    from muTel.dqm.classes.MuData import MuData
    muon_data = MuData.from_run(588)
    data = muon_data[73]
    

