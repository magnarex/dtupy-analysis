# Muon Segment & Grouping Object
from typing import TYPE_CHECKING
import asyncio

import numpy            as np
import pandas           as pd
import pyarrow          as pa
import pyarrow.parquet  as pq

from ....utils.ipython  import is_notebook
from ...pairing.hits    import LnL
from ..                 import MuSE
from ...core            import units as u

if TYPE_CHECKING:
    from pandas import DataFrame
    import dtupy_analysis.dqm as dqm
    from .SegDF import SegDF
        
class MuSGO(object):
    __module__ = 'dtupy_analysis.dqm.reco'
    """Muon Segment & Grouping Object"""
    __datatypes__ = {
        'seg_idx' : pa.uint64(),
        'sl'      : pa.uint8(),
        'pattID'  : pa.dictionary(pa.uint8(), pa.string()),
        'latsID'  : pa.dictionary(pa.uint8(), pa.string()),
    }
    def __init__(self, data, seg : 'SegDF', min_hits = 4, verbose=0) -> None:
        import warnings
        self._data = data
        self._min_hits = min_hits
        self._verbose = verbose
        
        # We need to reduce segments, we accomplish this by cheking
        # which ones are present in the current data
        segs  = seg.get_segs(data)
        segs = segs[segs.nhits >= min_hits]
        if verbose > 0:
            if len(segs) == 0:
                warnings.warn('Empty MuSGO!')
            elif len(segs) > len(segs.seg.drop_duplicates()):
                # Check for duplicates to ensure a correct behavior of the pairing algorithm.
                # Duplicates in this stage may indicate a problem rooting from dqm.pairing!
                warnings.warn('Segments are not unique!')
        self._init_muses(segs)
        self._stats = None
    
    def _init_muses(self, segs):
        if len(segs) > 0:
            self._muses = segs.apply(self.make_muse, axis=1)
        else:
            self._muses = []
        del self._data
    
    def make_muse(self, row):
        muse = MuSE(row, self._data)
        # print(muse._seg)
        return muse
    
    @property
    def segments(self) -> 'DataFrame':
        segs = []
        for muse in self.muses.to_list():
            segs.append(muse._seg.to_frame().T)
        return pd.concat(segs).copy(deep=True)

    @property
    def data(self) -> 'dqm.MuData':
        return pd.concat([m.data for m in self.muses])
    
    def __iter__(self):
        return (muse for muse in self.muses)
    
    def __len__(self):
        return len(self._segments)
    
    @property
    def muses(self) -> 'list[dqm.reco.MuSE]':
        return self._muses

    def _repr_html_(self):
        return self.segments._repr_html_()

    def fit(self, v_drift = u.V_DRIFT, t0 = None, do_plot = False, do_bar = False, verbose=0):
        verbose = max(self._verbose, verbose)
        if do_bar:
            try:
                if is_notebook():
                    from tqdm.notebook import tqdm
                else:
                    from tqdm import tqdm
            except ImportError:
                from tqdm import tqdm
        else:
            tqdm = lambda x: x

        for muse in tqdm(self.muses):
            muse.fit(v_drift = v_drift, t0 = t0, do_plot = do_plot, verbose = verbose)
        
        return self.stats
    
    @property
    def stats(self) -> 'DataFrame':
        best_fit = []
        for muse in self.muses:
            bf = muse.best_fit
            bf['seg_idx'] = muse._seg_idx
            best_fit.append(bf)
        return pd.concat(best_fit, ignore_index=True).drop_duplicates('seg_idx').copy(deep=True)

    def flatten(self):
        df = pd.concat([muse.flatten() for muse in self.muses], ignore_index=True)\
                    .drop_duplicates('seg_idx')\
                    .astype({'pattID':'category'})

        if 'latsID' in df.columns: df = df.astype({'latsID':'category'})
        return df.copy(deep=True)
    
    def export(self, path, pqwriter = None, skip_rows = None):
        from pathlib import Path      
        from ....utils.paths import get_with_default,\
                                    data_directory

        df = self.flatten().astype({'pattID':str, 'latsID':str})
        if skip_rows is None: skip_rows = np.zeros_like(df.index, dtype = bool)
        
        table = pa.Table.from_pandas(df[~skip_rows], preserve_index = False)
        schema = pa.schema({
            field.name : self.__datatypes__.get(field.name, field.type)
            for field in table.schema
        })
        
        if pqwriter is None:
            output = get_with_default(Path(path).with_suffix('.fits'), data_directory)
            pqwriter = pq.ParquetWriter(output, schema)
        
        table = table.cast(schema)
        pqwriter.write_table(table)
        
        return pqwriter


    async def async_fit(self, v_drift = u.V_DRIFT, t0 = None):
        tasks = []

        for muse in self.muses:
            tasks.append(muse.async_fit(v_drift = v_drift, t0 = t0))
  
        await asyncio.gather(*tasks)
        return self.stats
