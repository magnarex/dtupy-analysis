import dask.dataframe as dd

from dtupy_analysis.utils.paths import get_file, data_directory
from dtupy_analysis.dqm.classes.MuSL import MuSL

class MuData:
    """
    Data handler
    """
    def __init__(self, engine = 'dask'):
        self._engine = engine
    
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
                    engine              = 'pyarrow' ,
                    # dtype_backend       = "pyarrow" ,
                    filesystem          = 'arrow'   ,
                    split_row_groups    =  True     ,
                )
            
        return instance

    def get_ssl(self, station, sl):
        """
        Get all data from a (station, sl) pair
        """
        if self.engine == 'dask':
            return MuSL.from_dask(self.data, station = station, sl = sl)
    
    
