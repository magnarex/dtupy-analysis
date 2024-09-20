
class MuSL:
    def __init__(self, mudata, station, sl, engine = 'dask'):
        self._engine = engine
        self._mudata = mudata
        self._station = station
        self._sl = sl
        
        self._pairs = {}
    
    @property
    def engine(self):
        return self._engine
    
    @classmethod
    def from_dask(self, mudata, station, sl):
        return MuSL(mudata, station, sl, engine='dask')
    
    @property
    def data(self):
        return self._mudata.data[(self._mudata.data.sl == self.sl) & (self._mudata.data.station == self.station)]
    
    @property
    def sl(self):
        return self._sl
    @property
    def station(self):
        return self._station
    
    def pair_hits(self):
        pass
        