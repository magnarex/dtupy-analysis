# Muon Track Enveloper
from typing import TYPE_CHECKING

import pyarrow as pa
import pandas  as pd

from ....core.classes.MuFile import MuFile

if TYPE_CHECKING:
    from typing import Generator
    from .MuSegData import MuSegData


class MuTE(MuFile):
    __module__ = 'dtupy_analysis.dqm.pairing.segments'
    def __init__(self, fits_path, batch_size = 1000, f_overlap = 0.3):
        super().__init__(fits_path, batch_size = batch_size, f_overlap = f_overlap, suffix='.fits')

    def __iter__(self) -> 'Generator[MuSegData]':
        from .MuSegData import MuSegData
        if self._batches is None:
            self._batches = self.parquet.iter_batches(
                batch_size = self.batch_size,
                use_pandas_metadata=True,
                use_threads=True
            )
        for batch in self.batches:            
            self._data = pa.Table.from_batches([batch]).to_pandas().set_index('seg_idx')
            
            if self.memory.data is None:
                pass
            else:
                self._data = pd.concat([self.memory.data, self._data])
            
            self.memory.data = self._data.tail(self.overlap)
            yield MuSegData.from_pandas(self)
