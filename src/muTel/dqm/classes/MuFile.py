import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import pathlib
from collections.abc import Generator

class MuFile(object):
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