from collections.abc import Iterable
from copy import deepcopy
import itertools

class Buffer(object):
    _buffer = None
    _template = None
    
    def __init__(self,*args,**kwargs):
        self.build()

    def clear(self):
        self._buffer = deepcopy(self._template)
        
    def dump(self,writer):
        pass
    
    def fill(self,value):
        pass
    
    def build(self):
        self._template = {None : []}
        self.clear()
    
        
    def __del__(self):
        self.clear()
        
class PairingBuffer(Buffer):
    def __init__(self,
                 stations       : 'int | Iterable[int]' = 1,
                 superlayers    : 'int | Iterable[int]' = 4,
                 layers         : 'int | Iterable[int]' = 4
                ):
        self.stations       = stations
        self.superlayers    = superlayers
        self.layers         = layers
        
        super().__init__() # Builds buffer
        
    def build(self):
        self._template = {
            st : {
                sl : {l : [] for l in self.layers}
                for sl in self.superlayers
            }
            for st in self.stations
        }
        self.clear()
            
            
    @property
    def stations(self):
        if hasattr(self,'_stations'):
            return self._stations
        else:
            return list(range(1,self.nstations+1))
    @stations.setter
    def stations(self, value):
        if isinstance(value, int):
            self._nstations = value
            
        elif isinstance(value, Iterable):
            self._stations  = value
            self._nstations = len(value)

    @property
    def nstations(self):
        if hasattr(self,'_nstations'):
            return self._nstations
        elif hasattr(self,'_stations'):
            return len(self._stations)
        else:
            raise AttributeError('No stations and no nstations, what have you done?')

    @property
    def superlayers(self):
        if hasattr(self,'_superlayers'):
            return self._superlayers
        else:
            return list(range(1,self.nsuperlayers+1))
    @superlayers.setter
    def superlayers(self, value):
        if isinstance(value, int):
            self._nsuperlayers = value
        elif isinstance(value, Iterable):
            self._superlayers  = value
            self._nsuperlayers = len(value)

    @property
    def nsuperlayers(self):
        if hasattr(self,'_nsuperlayers'):
            return self._nsuperlayers
        elif hasattr(self,'_superlayers'):
            return len(self._superlayers)
        else:
            raise AttributeError('No superlayers and no nsuperlayers, what have you done?')

    @property
    def layers(self):
        if hasattr(self,'_layers'):
            return self._layers
        else:
            return list(range(1,self.nlayers+1))
    @layers.setter
    def layers(self, value):
        if isinstance(value, int):
            self._nlayers = value
        elif isinstance(value, Iterable):
            self._layers  = value
            self._nlayers = len(value)

    @property
    def nlayers(self):
        if hasattr(self,'_nlayers'):
            return self._nlayers
        elif hasattr(self,'_layers'):
            return len(self._layers)
        else:
            raise AttributeError('No layers and no nlayers, what have you done?')



class Pairing(PairingBuffer):
    def __init__(self,
                 stations       : 'int | Iterable[int]' = 1,
                 superlayers    : 'int | Iterable[int]' = 4,
                 layers         : 'int | Iterable[int]' = 4
                ):
        super().__init__(stations, superlayers, layers)

    def __enter__(self):
        self._buffer = PairingBuffer(self.stations,self.superlayers,self.layers)
        return self._buffer
    
    def __exit__(self, type, value, traceback):
        del self._buffer


if __name__ == '__main__':
    with Pairing(stations=[2],superlayers=[1,3],layers=4) as pairs:
        print(pairs.nstations, pairs.stations)
        print(pairs.nsuperlayers, pairs.superlayers)
        print(pairs.nlayers, pairs.layers)
        
        print(pairs._buffer)