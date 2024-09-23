from   collections.abc  import Generator
import pathlib

import pyarrow          as pa
import pyarrow.parquet  as pq
import dask.dataframe   as dd
import pandas           as pd

from .MuData import MuData
from dtupy_analysis.utils.paths import  get_file,\
                                        data_directory

class MuFile:
    """
    Class that handles the IO.
    """
    __allowed_engines__ = {'dask'}
    def __init__(self, data, engine):
        if engine in self.__allowed_engines__:
            self._engine = engine
        else:
            raise ValueError(f"'engine' {engine} is not supported, please use one of: {self.__allowed_engines__}")
        
        if engine == 'dask':
            self._ddf = data
    
    @property
    def data(self):
        if self.engine == 'dask':
            return self._ddf
        
    @property
    def engine(self):
        return self._engine
    
    # @classmethod
    # def to_batches(cls, path : 'str | pathlib.Path', batch_size : int = 10000):
    #     """
    #     Reads a parquet file in batches and provides an object that allows user to handle
    #     the mapping manually.
    #     """
    #     path = get_file(path, data_directory, ['.parquet'])
    #     return MuFileBatches(path, batch_size=batch_size)
    
    @classmethod
    def to_dask(cls, path : 'str | pathlib.Path', partition_size : int = 10000):
        """
        Reads a parquet file in batches, needs mapping.
        """
        
        path = get_file(path, data_directory, ['.parquet'])
        data = dd.read_parquet(path,
                        # engine              = 'pyarrow' ,
                        split_row_groups    =  True     ,
                        blocksize           = partition_size
                    )
        file = cls(data, 'dask')
        return MuData.from_dask(file)

        

    
class MuFileBatches(MuFile):
    __backend__ = "pandas"
    def __init__(self, parquet_path : 'str | pathlib.Path', batch_size : int = 10000):
        self._parquet = pq.ParquetFile(parquet_path)
        self._batch_size = batch_size
        self._batches = None        
            
    def __len__(self) -> int:
        return self.parquet.scan_contents('index')
    
    def __next__(self) -> pd.DataFrame:
        if self._batches:
            pass
        else:
            self._batches = self.parquet.iter_batches(
                batch_size = self.batch_size,
                use_pandas_metadata=True,
                use_threads=True
            )

        return pa.Table.from_batches([next(self.batches)]).to_pandas().set_index('index')
        
    def __iter__(self) -> 'MuFile':
        self._batches = self.parquet.iter_batches(
            batch_size = self.batch_size,
            use_pandas_metadata=True,
            use_threads=True
        )
        return self
    
    def __getitem__(self,var):
        _buffer = []
        for chunk in self:
            _buffer.append(chunk[var])
        return pd.concat(_buffer)
    
    def __getattr__(self,attr):
        if attr in self._parquet.schema.names:
            return self[attr]
        else:
            super().__getitem__(attr)
    
    def map(self, function):
        for chunk in self:
            yield function(chunk)
    
    
    @property
    def batch_size(self) -> int:
        return self._batch_size
    @property
    def parquet(self) -> 'pq.ParquetFile':
        return self._parquet
    @property
    def batches(self) -> 'None | Generator[pa.RecordBatch]':
        return self._batches