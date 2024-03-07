import muTel.utils.meta as meta
from muTel.utils.data import id_list

from muTel.dqm.classes.MuLE import MuLE, MuSH
from muTel.dqm.classes.MuSE import MuSE
from muTel.utils.units import Time
import concurrent.futures

from IPython.display import HTML, display

from copy import deepcopy

from collections.abc import Iterable, Generator

from timeit import default_timer as timer
import logging
import pandas
import pandas as pd
import numpy as np
import dask.dataframe
import dask.dataframe as dd
import dask.array as da
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

import sys

_MuData_logger = logging.Logger('MuEvent')
_MuData_logger.addHandler(logging.StreamHandler(sys.stdout))


class MuEventType(type):
    def __repr__(self): 
        return self.__name__
    
    


class MuSEGenerator(Generator):
    def __init__(self,n_init = 0):
        self.n = n_init

    def send(self, ignored_arg):
        if self.n < len(id_list):
            return_value = MuSE(id=id_list[self.n])
            self.n = self.n + 1
            return return_value
        else:
            raise NotImplementedError('No está contemplado que haya más de 702 MuSEs por evento.')

    def throw(self, type=None, value=None, traceback=None):
        raise StopIteration

    







class MuEvent(object, metaclass = MuEventType):
    __slots__ = ('_muses', '_mushs', '_mules', '_data', '_run', '_date', '_eventnr', '_debug','_muse_generator')

    def __str__(self):
        return f'{self.__class__.__name__}({self.eventnr}@{self.run})'
    

    def __init__(
        self,
        data        : (dask.dataframe.DataFrame | pandas.DataFrame) = None,
        run         : int                                           = None,
        date        : datetime.datetime                             = None,
        debug       : bool                                          = False
    ) -> None:
        #_________________________________________________________________
        #                INICIALIZACIÓN DE LAS PROPIEDADES
        #‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
        self._debug  = debug
        self._run   = run
        self._date  = date

        self._eventnr   = None
        self.data = data

        #_________________________________________________________________
        #               INICIALIZACIÓN DE LOS GENERADORES
        #‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
        self._muse_generator = MuSEGenerator()



        #_________________________________________________________________
        #         INICIALIZACIÓN DE LAS VARIABLES DE LAS TRAZAS
        #‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
        self._mules = {
            sl_i : {}
            for sl_i in meta.superlayers
        }

        self._mushs = {
            sl_i : {}
            for sl_i in meta.superlayers
        }

        self._muses = {
            sl_i : []
            for sl_i in meta.superlayers
        }


    def __setstate__(self,d):
        d['_muse_generator'] = MuSEGenerator(d['n_init'])
        del d['n_init']

        [setattr(self,slot,val) for slot, val in d.items()]

        

    def __getstate__(self):
        d = {slot : getattr(self,slot) for slot in self.__slots__}
        del d['_muse_generator']
        d['n_init'] = self._muse_generator.n
        return d









    @staticmethod
    def analyse_event_static(event):
        return MuEvent.find_muses_static(MuEvent.pair_hits_static(event))

    @classmethod
    def from_data(cls, event_data, run = None, date = None, do_pairs = False, do_muses = False, debug= False):
        event = MuEvent(data = event_data, run = run, date = date, debug=debug)

        if not isinstance(event_data, pd.DataFrame): print(f'WARNING: No es pandas.DataFrame, es un {type(event_data)}')

        if do_pairs or do_muses: event = MuEvent.pair_hits_static(event, debug=debug)
        if do_muses: event = MuEvent.find_muses_static(event,debug=debug)

        return event
    
    @classmethod
    def from_MuData(cls, mudata : 'MuData', do_pairs = False, do_muses = False, debug=False):
        return MuEvent.from_data(mudata.df, run = mudata.run, date = mudata.date, do_pairs=do_pairs, do_muses=do_muses, debug=debug)

    @property
    def data(self) -> pd.DataFrame:
        return self._data
    
    @data.setter
    def data(self, data : pd.DataFrame):
        try:
            eventnr = data.EventNr.unique()
            assert eventnr.size == 1
                    
        except AssertionError:
            raise ValueError('No puede haber hits con EventNr distintos en los datos del evento.')
        
        self._eventnr = eventnr[0]
        # data = data.drop('EventNr',axis='columns')
        self._data = data

        
        
    @property
    def eventnr(self) -> int:
        return  self._eventnr
    
    @property
    def mules(self) -> dict[dict[MuLE]]:
        return self._mules
    
    @property
    def mushs(self) -> dict[dict[MuSH]]:
        return self._mushs
    
    @property
    def muses(self) -> dict[list[MuLE]]:
        return self._muses
    
    @property
    def nhits(self) -> int:
        return len(self.data)
            
        
    def pair_hits(self, debug=False, timeit=False):
        if timeit: t_i = timer()
        event = self.pair_hits_static(self, debug=debug, timeit=timeit)
        if timeit:
            t_f = timer()
            print('#================================================================================#')
            print(f'Se ha tardado {Time(t_f-t_i)} en emparejar los {self.nhits} hits.')
            print('#================================================================================#')
        return event

    @staticmethod
    def pair_hits_static(event : 'MuEvent', debug=False, timeit=False):
        for sl_i in meta.superlayers:
            if timeit:
                print('#================================================================================#')
                print(f'#                                 SUPERLAYER {sl_i}                                   #')
                print('#================================================================================#')
                t_i = timer()
            event._mules[sl_i] = MuEvent.pair_sl_hits_static(event, sl_i, debug=debug, timeit=timeit)
            if timeit: t_mule = timer()
            event._mushs[sl_i] = MuEvent.find_mush_in_sl_static(event, sl_i, debug=debug)
            
            if timeit:
                t_mush = timer()
                nhits = len(event.data[event.data.sl==sl_i])
                nmush = sum([len(l_i) for l_i in event.mushs[sl_i].values()])
                print(f'Se ha tardado {Time(t_mule-t_i)} en emparejar los {nhits} hits de la SL{sl_i}.')
                print(f'Se ha tardado {Time(t_mush-t_mule)} en recoger los {nmush} MuSH de la SL{sl_i}.')
                    
        
        # if timeit: print('#================================================================================#')
        return event

    @staticmethod
    def find_mush_in_sl_static(event : 'MuEvent', sl, debug=False, timeit=False):
        data_sl = event.data[event.data.sl == sl]
        mules_sl = sum(event.mules[sl].values(), start=[])

        if len(mules_sl) == 0:
            used_index = []
        else:
            used_index = np.r_[*[mule.df.index.values for mule in mules_sl]]
                
        mush_dict = {layer_i : [] for layer_i in meta.layers}

        for i,row in data_sl.loc[~data_sl.index.isin(used_index)].iterrows():
            mush_dict[row.layer].append(MuSH().add_hit(data_sl.loc[i].to_frame().transpose()))

        return mush_dict


    @staticmethod
    def pair_sl_hits_static(event : 'MuEvent', sl, debug= False, timeit=False):
        mule_dict = {}
        
        for layer_i, layer_j in zip(meta.layers[:-1],meta.layers[1:]):
            mule_dict[f'{layer_i}w{layer_j}'] = MuEvent.pair_sll_hits_static(event, sl, layer_i, layer_j, debug = debug, timeit=timeit)
        return mule_dict
    

    @staticmethod
    def pair_sll_hits_static(
            event       : 'MuEvent',
            sl          : int, 
            layer_i     : int, 
            layer_j     : int, 
            hit_d_coef  : float = meta.hit_d_coef,
            timeit      : bool  = False, 
            debug       : bool  = False
        ):
        

        # Primero tenemos que asegurarnos de que layer_j > layer_i.
        if layer_j > layer_i:
            # Orden normal: layer_j > layer_i
            pass
        elif layer_j < layer_i:
            # Orden inverso: layer_i > layer_j
            l_ = layer_j
            layer_j = layer_i
            layer_i = l_
        # De esta forma layer_j siempre será la superior y layer_i la inferior.
        
        # Calculamos la distancia máxima que permitimos.
        n_events = len(event.data[event.data.sl == sl])

        
        hit_d_max = np.ceil(hit_d_coef*n_events).astype(int)
        if debug: display(f'Distancia máxima permitida: {hit_d_max:d} ({hit_d_coef} x {n_events:d})')


        # Seleccionamos las capas que queremos emparejar.
        hits_layer_j = event.data[(event.data.layer == layer_j) & (event.data.sl == sl)]
        hits_layer_i = event.data[(event.data.layer == layer_i) & (event.data.sl == sl)]
          
        
        if debug:
            print(f'N_i: {len(hits_layer_i):d}\nN_j: {len(hits_layer_j):d}')
            display(hits_layer_j)
            display(hits_layer_i)

        if timeit: t_i = timer()
        pairs = MuEvent.layer_pairing(hits_layer_i, hits_layer_j, debug = debug)
        if timeit: t_pairs = timer()
        
        # Una vez tenemos los hits emparejados, extraemos toda la información de cada uno de ellos y la
        # guardamos en un pd.DataFrame.
        mule_list = []
        for li_k, lj_k in pairs:
            mule_k = MuLE()
            
            # Añadimos las capas al objeto MuLE.
            mule_k.add_hit(hits_layer_i.iloc[[li_k]])
            mule_k.add_hit(hits_layer_j.iloc[[lj_k]])
            mule_k.hit_d_max = hit_d_max

            mule_list.append(mule_k)
        if timeit:
            t_mule = timer()
            print(f'El algoritmo de emparejamiento ha tardado {Time(t_pairs-t_i)}.')
            print(f'Se ha tardado {Time(t_mule-t_pairs)} en formar todos los MuLEs.')
            print('#--------------------------------------------------------------------------------#')

        return mule_list


    @staticmethod
    def layer_pairing(
            hits_layer_i      : pd.DataFrame | pd.Series,
            hits_layer_j      : pd.DataFrame | pd.Series,
            debug             : bool = False):
        """
        Define el algoritmo de emparejamiento por layers empleado. Éste usa la mínima distancia en hits para
        emparejar hits en layers consecutivas.

        
        Variables
        ---------
            - hits_layer_i : pd.DataFrame | pd.Series
                Datos de la layer inferior (menor número).

            - hits_layer_j : pd.DataFrame | pd.Series
                Datos de la layer superior (mayor número).

            - debug : bool
                Flag que indica si queremos que se ejecute el código de depuración.

            
        Returns
        -------
            - event : p
            Una slice de los datos con el EventNr indicado.
        """
        if debug:
            print(f'Tipo de hits_layer_i: {type(hits_layer_i)}')
            print(f'Tipo de hits_layer_j: {type(hits_layer_j)}')

        if (hits_layer_i.index.size == 0) or (hits_layer_j.index.size == 0):
            return []

        try:
            layer_i = int(hits_layer_i.layer.iloc[0])
            layer_j = int(hits_layer_j.layer.iloc[0])
        except IndexError as err:
            display(hits_layer_i)
            display(hits_layer_j)
            raise err
        layer_sep = np.abs(layer_j - layer_i)



        # Calculamos unas pseudo-distancias en hits y celdas.
        hit_diff = np.r_[hits_layer_j.hit] - np.c_[hits_layer_i.hit]
        cell_diff = np.r_[hits_layer_j.cell] - np.c_[hits_layer_i.cell]
        if debug: print(f'Distancia en hits:\n {hit_diff}\nDistancia en celdas:\n {cell_diff}')
        


        # Ajustamos la distancia en celdas según el staggering cuando son resta de capas a distancias
        # impares.
        if layer_sep % 2 == 1:
            if (layer_i % 2 == 0):
                cell_diff = cell_diff + 0.5
            else:
                cell_diff = cell_diff - 0.5
            

        # Miramos los hits que son en celdas contiguas.
        is_cont   = np.where(np.abs(cell_diff) <= layer_sep/2)
        
        # Según la distancia total, comprobamos que el mínimo por filas y columnas coincidan, ya que de esta
        # manera nos aseguramos de que no usamos más de una vez el mismo hit para hacer parejas.
        total_diff = np.abs(hit_diff) + np.abs(cell_diff)
        if total_diff.size == 0: return []

        where_j_min = [[i,e] for i,e in enumerate(total_diff.argmin(axis=1))] # Mínimo de cada fila.
        where_i_min = [[e,i] for i,e in enumerate(total_diff.argmin(axis=0))] # Mínimo de cada columna.

        # Buscamos coincidencias entre los dos y que sean celdas contiguas.
        pairs = [
            pair
            for pair in where_i_min
            if (pair in where_j_min)
            and (pair in np.c_[is_cont].tolist())
        ]
    
    
        return pairs


    # Debajo todo lo que involucra juntar MuLEs y MuSHs para crear MuSEs.

    def find_muses(self,debug=False):
        return MuEvent.find_muses_static(self,debug=debug)
    
    def yoke_mules(self,debug=False):
        return MuEvent.yoke_mules_static(self,debug=debug)
    
    def mush_mules(self,debug=False):
        return MuEvent.mush_mules_static(self,debug=debug)

    @staticmethod
    def find_muses_static(event : 'MuEvent', debug=False):
        for sl_i in meta.superlayers:
            event._muses[sl_i] = event._muses[sl_i] + MuEvent.find_muses_sl_static(event, sl_i, debug=debug)
        return event

    @staticmethod
    def yoke_mules_static(event : 'MuEvent',debug=False):
        for sl_i in meta.superlayers:
            event._muses[sl_i] = event._muses[sl_i] + MuEvent.yoke_mules_sl_static(event, sl_i, debug=debug)
        return event

    @staticmethod
    def mush_mules_static(event : 'MuEvent', debug=False):
        for sl_i in meta.superlayers:
            event._muses[sl_i] = event._muses[sl_i] + event.mush_mules_sl_static(event, sl_i, debug=debug)
        return event
    
    @staticmethod
    def find_muses_sl_static(event : 'MuEvent', sl, debug=False):
        '''
        Con esta función tomamos los objetos MuLE y MuSH y los juntamos para recrear las
        trazas del MuON en la superlayer indicada.
        '''
        muse_list = []
        muse_list = muse_list + MuEvent.yoke_mules_sl_static(event, sl, debug=debug)
        muse_list = muse_list + MuEvent.mush_mules_sl_static(event, sl, debug=debug)
        muse_list = muse_list + MuEvent.dump_muses_sl_static(event, sl, debug=debug)


        return muse_list

    @staticmethod
    def yoke_mules_sl_static(event : 'MuEvent', sl, debug=False):
        mules = event.mules[sl]
        muse_list = []

        for i in range(len(mules['3w2'])):
            mule_3w2 = mules['3w2'].pop(0)
            # hit_3 = mule_3w2.data.hit.loc[mule_3w2.data.layer == 3]
            # hit_2 = mule_3w2.data.hit.loc[mule_3w2.data.layer == 2]

            muse_i = event.new_muse()
            muse_i.add_mule(mule_3w2)

            # match_in_2w1 = list(map(lambda mule_2w1: mule_2w1.data.index.isin(mule_3w2.data.index).any(), mules['2w1']))
            match_in_2w1 = [mule_2w1.data.index.isin(mule_3w2.data.index).any() for mule_2w1 in mules['2w1']]
            if True in match_in_2w1:
                i_2w1 = int(np.where(match_in_2w1)[0])
                mule_2w1 = mules['2w1'].pop(i_2w1)
                muse_i.add_mule(mule_2w1)


            # match_in_4w3 = list(map(lambda mule_4w3: mule_4w3.data.index.isin(mule_3w2.data.index).any(), mules['4w3']))
            match_in_4w3 = [mule_4w3.data.index.isin(mule_3w2.data.index).any() for mule_4w3 in mules['4w3']]
            if True in match_in_4w3:
                i_4w3 = int(np.where(match_in_4w3)[0])
                mule_4w3 = mules['4w3'].pop(i_4w3)
                muse_i.add_mule(mule_4w3)

            muse_list.append(muse_i)
        
        return muse_list

    
    @staticmethod
    def mush_mules_sl_static(event : 'MuEvent', sl, debug = False):
        muse_list = []
        
        # Unimos las que tienen un hit suelto en la layer 1 primero
        mush_1 = event.mushs[sl][1]
        mules_4w3 = event.mules[sl]['4w3']

        if (len(mush_1) > 0) & (len(mules_4w3) > 0):

            # hits_1 = pd.concat(list(map(lambda mush: mush.data, mush_1)))
            hits_1 = pd.concat([mush.data for mush in mush_1])
            # hits_4w3 = pd.concat(list(map(lambda mule: mule.data[mule.data.layer == 3], mules_4w3))) # Esto nos dará los hits en la layer 3
            hits_4w3 = pd.concat([mule.data[mule.data.layer == 3] for mule in mules_4w3]) # Esto nos dará los hits en la layer 3

            pairs_431 = MuEvent.layer_pairing(
                hits_layer_i = hits_1,
                hits_layer_j = hits_4w3,
                debug=debug
            )
            removed_j = np.zeros(len(hits_4w3))
            removed_i = np.zeros(len(hits_1))

            for li_k, lj_k in pairs_431:
                muse_k = event.new_muse()
                muse_k.add_mule(mules_4w3.pop(lj_k - removed_j[:lj_k].sum(dtype=int)))
                muse_k.add_mush(mush_1.pop(li_k - removed_i[:li_k].sum(dtype=int)))
                muse_list.append(muse_k)


                removed_i[li_k] = 1
                removed_j[lj_k] = 1
        
        # Ahora unimos las que tienen un hit suelto en la layer 4
        mush_4 = event.mushs[sl][4]
        mules_2w1 = event.mules[sl]['2w1']

        if (len(mush_4) > 0) & (len(mules_2w1) > 0):

            hits_4 = pd.concat([mush.data for mush in mush_4])
            hits_2w1 = pd.concat([mule.data[mule.data.layer == 2] for mule in mules_2w1]) # Esto nos dará los hits en la layer 2

            pairs_421 = MuEvent.layer_pairing(
                hits_layer_i = hits_2w1,
                hits_layer_j = hits_4,
                debug=debug
            )
            removed_j = np.zeros(len(hits_4))
            removed_i = np.zeros(len(hits_2w1))

            for li_k, lj_k in pairs_421:
                muse_k = event.new_muse()
                muse_k.add_mush(mush_4.pop(lj_k - removed_j[:lj_k].sum(dtype=int)))
                muse_k.add_mule(mules_2w1.pop(li_k - removed_i[:li_k].sum(dtype=int)))
                muse_list.append(muse_k)


                removed_i[li_k] = 1
                removed_j[lj_k] = 1
        
        return muse_list

    
    @staticmethod
    def dump_muses_sl_static(event : 'MuEvent', sl : int, debug = False):
        muse_list = []

        # Popeamos todas las MuLEs que haya y las convertimos en MuSEs
        for key, mule in event.mules[sl].items():
            for i in range(len(mule)):
                muse_list.append(event.new_muse().add_mule(mule.pop(0)))
        
        # Popeamos todas las MuSHs que haya y las convertimos en MuSEs
        for layer, mush in event.mushs[sl].items():
            for i in range(len(mush)):
                muse_list.append(event.new_muse().add_mush(mush.pop(0)))
        
        return muse_list


    @property
    def all_muses(self) -> list[MuSE]:
        return sum(self.muses.values(),start=[])

    @property
    def n4(self) -> int:
        return self._calc_ni(4)
    @property
    def n3(self) -> int:
        return self._calc_ni(3)
    @property
    def n2(self) -> int:
        return self._calc_ni(2)
    @property
    def n1(self) -> int:
        return self._calc_ni(1)
    
    def _calc_ni(self, i : int) -> int:
        return int(np.sum([muse.nhits == i for muse in self.all_muses]))

    @property
    def run(self) -> int:
        return self._run



    def new_muse(self) -> MuSE:
        return next(self._muse_generator)

    # @property
    # def df(self):
    #     return pd.concat(
    #         list(map(lambda muse: muse.df, self.all_muses)),
    #         ignore_index=True
    #     ).set_index('EventNr')
    

    @staticmethod
    def get_df_static(event : 'MuEvent') -> pd.DataFrame:
        return pd.concat(
            [muse.full_data for muse in event.all_muses],
            ignore_index=True
        ).set_index('EventNr')
        


    @staticmethod
    def save_with_pqwriter(event, pqwriter):
        try:
            pqwriter.write_table(
                pa.Table.from_pandas(
                    pd.concat([
                        muse.full_data
                        for muse in event.all_muses
                    ])\
                    .astype(meta.data_type_dict | {
                        'EventId'   : 'category',
                        'MuSEId'    : 'category'
                    })
                )
            )
        except ValueError as e:
            print(event.eventnr,flush=True)
            exit(1)









if __name__ == '__main__':
    from muTel.dqm.classes.MuData import MuData
    muon_data = MuData.from_run(588)
    data = muon_data[73]
    event = MuEvent()
    event.data = data
    event.pair_hits()

    # print(list(map(lambda mule: list(mule.hits.hit.values), event.mule[1])))
