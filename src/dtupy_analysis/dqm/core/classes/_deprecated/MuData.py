import dask.dataframe as dd

from dtupy_analysis.utils.paths import get_file, data_directory
from dtupy_analysis.dqm.classes.MuSL import MuSL

class MuData:
    __allowed_engines = {'dask'}
    """
    Data handler
    """
    def __init__(self, engine = 'dask'):
        if engine in self.__allowed_engines:
            self._engine = engine
        else:
            raise ValueError(f"Specified engine is not allowed, please choose one from: {self.__allowed_engines}")
    
    @property
    def data(self):
        if self.engine == 'dask':
            return self.ddf
        elif self.engine == 'pandas':
            return self.df
    
    @property
    def df(self):
        if self.engine == 'dask':
            # WARNING: doing this is highly discouraged.
            return self.ddf.compute()
    
    @property
    def ddf(self) -> dd.DataFrame:
        if self.engine == 'dask':
            return self._ddf
        else:
            return None

    @property
    def engine(self):
        return self._engine
    
    @classmethod
    def from_parquet(self, path, engine='dask'):
        """
        Load data from parquet. Default uses dask.dataframe.
        """
        instance = MuData(engine=engine)
        
        path = get_file(path, data_directory, ['.parquet'])
        
        if engine == 'dask':
            instance._ddf = dd.read_parquet(path,
                    # engine              = 'pyarrow' ,
                    split_row_groups    =  True     ,
                )
            
        return instance

    def get_ssl(self, station, sl):
        """
        Get all data from a (station, sl) pair
        """
        if self.engine == 'dask':
            return MuSL.from_dask(self, station = station, sl = sl)
    
    
