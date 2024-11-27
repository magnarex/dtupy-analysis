import pathlib
import multiprocessing
from   typing           import TYPE_CHECKING

import numpy            as np
import pyarrow          as pa
import pyarrow.parquet  as pq
import pandas           as pd

from ....utils.paths    import  get_file,\
                                data_directory,\
                                get_with_default
from ....utils.ipython  import  is_notebook
from ....utils.memory   import  Memory

if TYPE_CHECKING:
    from .                 import MuData
    from .                 import MuSL
    from collections.abc   import Generator
    from .MuData           import MuData



try:
    from alive_progress import alive_bar
    _do_bar = True
except ImportError:
    alive_bar = None
    _do_bar = False

class MuFile(object):
    __module__ = 'dtupy_analysis.dqm.core'
    """
    Class that handles the IO.
    """

    def __init__(self,
                parquet_path : 'str | pathlib.Path',
                batch_size   : int   = 10000,
                f_overlap    : float = 0.3,
                suffix       : str   = '.parquet'
            ):
        self._path       = get_file(parquet_path, data_directory, [suffix])
        self._batch_size = batch_size
        self._f_overlap  = f_overlap
        self._overlap    = int(np.ceil(batch_size * f_overlap))
        
        # Inicializamos las variables
        self._parquet   = None
        self._batches   = None   
        self._data      = None 
        self._memory    = Memory(data = None)
        self._segments  = None
        
    def __enter__(self):
        self._parquet = pq.ParquetFile(self._path)
        self._engine = 'pandas'
        return self
    
    def __exit__(self, exc_type : Exception, exc_val, exc_tb):
        if exc_type:
            raise exc_type(exc_val).with_traceback(exc_tb)

        self._parquet.close()
        return self._parquet.closed
    
    def __len__(self) -> int:
        return self.parquet.scan_contents('index')
            
    def __iter__(self) -> 'Generator[MuData]':
        from .MuData import MuData
        if self._batches is None:
            self._batches = self.parquet.iter_batches(
                batch_size = self.batch_size,
                use_pandas_metadata=True,
                use_threads=True
            )
        for batch in self.batches:            
            self._data = pa.Table.from_batches([batch]).to_pandas().set_index('index')
            
            if self.memory.data is None:
                pass
            else:
                self._data = pd.concat([self.memory.data, self._data])
            
            self.memory.data = self._data.tail(self.overlap)
            yield MuData.from_pandas(self)
    
    def __getitem__(self,var):
        _buffer = []
        for chunk in self:
            _buffer.append(chunk[var])
        return pd.concat(_buffer)
    
    def __getattr__(self,attr):
        if attr in self._parquet.schema.names:
            return self[attr]
    
    def map(self, function):
        for chunk in self:
            yield function(chunk)
    
    
    def calc_segments(self, ncores = None, nmax = None, f_index = 'log', debug = False):
        def update_bar(bar):
            def callback(result):
                bar.update(1)
            return callback
        try:
            if is_notebook():
                from tqdm.notebook import tqdm
            else:
                from tqdm import tqdm
        except:
            from tqdm import tqdm
            
        with multiprocessing.Pool(ncores)               as exe    ,\
             tqdm(total=1, desc='Submitted', unit = 'task(s)')   as sub_bar,\
             tqdm(total=1, desc='Completed', unit = 'task(s)')   as com_bar:
            
            results = []
            
            for i, mudata in enumerate(self):
                if i == nmax: break
                if debug: print(f'Submitting {i}', flush = True)
                
                result = exe.apply_async(
                    mudata.make_segments,
                    kwds = {'f_index': f_index},
                    callback = update_bar(com_bar)
                )
                
                results.append(result)
                sub_bar.update(1)
            
            sub_bar.total = len(results)
            com_bar.total = len(results)
            
            sub_bar.refresh()
            com_bar.refresh()
            
            exe.close()
            # print('\nAll tasks submitted!', flush=True)
            
            exe.join()
            # print('\nAll tasks finished!', flush=True)
        
        # Post-procesamos los resultados
        with tqdm(total=len(results), desc='Post-processed', unit = 'task(s)')   as proc_bar:
            from ...reco.classes.PattID import  PattID

            dict_segments = {
                    'st'    : [],
                    'sl'    : [],
                    'lnl'   : [],
                    'seg'   : [],
                    'nhits' : [],
                    # 'pattID': [],
                }
            for i in range(len(results)):
                r = results.pop(0).get()
                for musl in r:
                    musl : MuSL; st = musl.station; sl = musl.sl
                    for lnl, segs in musl.segments.items():
                        for seg in segs.values():
                            if seg in dict_segments['seg']: continue
                            dict_segments['st'   ].append(st       )
                            dict_segments['sl'   ].append(sl       )
                            dict_segments['lnl'  ].append(str(lnl) )
                            dict_segments['seg'  ].append(seg      )
                            dict_segments['nhits'].append(len(seg) )
                            
                            # # Esto es una chapuza
                            # if len(seg) == 4:
                            #     d = musl.data.loc[seg].set_index('layer').sort_index(ascending=False)
                            #     global_cell = d.cell + pd.Series((0,0.5,0,0.5), index=d.index)
                            #     pattID = ''.join(['R' if cell_diff > 0 else 'L' for cell_diff in global_cell.diff().dropna()])
                            #     dict_segments['pattID'].append(pattID)
                            # else:
                            #     dict_segments['pattID'].append('N/A')
                proc_bar.update(1)    
            
            self._segments = pd.DataFrame(dict_segments).astype({
                'st'    : np.int8,
                'sl'    : np.uint8,
                'lnl'   : 'category',
                'nhits' : np.uint8,
            })
              
    def export_segments(self, path = None, **kwargs):
        if path is None:
            path = self.path.with_suffix('.seg')
        else:
            path = get_with_default(pathlib.Path(path).with_suffix('.seg'), data_directory)
        
        if self.segments is None:
            self.calc_segments(**kwargs)
        
        with open(path, '+wb') as f:
            # Guardar como parquet
            self._segments.to_parquet(f)
        
    
    @property
    def segments(self):
        return self._segments
    @property
    def batch_size(self) -> int:
        return self._batch_size
    @property
    def parquet(self) -> 'pq.ParquetFile':
        return self._parquet
    @property
    def batches(self) -> 'None | Generator[pa.RecordBatch]':
        return self._batches
    @property
    def path(self):
        return self._path
    @property
    def data(self) -> pd.DataFrame:
        return self._data
    @property
    def f_overlap(self):
        return self._f_overlap
    @property
    def overlap(self):
        return self._overlap
    @property
    def memory(self) -> Memory:
        return self._memory
