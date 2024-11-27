from typing import TYPE_CHECKING
import multiprocessing
import sys
from multiprocessing import Pool
import gc

import pandas as pd
import pyarrow.parquet  as pq
import pyarrow          as pa
import numpy            as np

from ...core.classes.MuFile import MuFile
from ....utils.paths    import get_file,\
                               data_directory,\
                               get_with_default
from .MuSGO             import MuSGO
from .SegDF             import SegDF

if TYPE_CHECKING:
    import pathlib

class MuSTIO(MuFile):
    """Muon Segment Track I/O"""
    _data_types = {
        'seg_idx'   : pa.uint64(),
        'sl'        : pa.uint8(),
        'pattID'    : pa.dictionary(pa.uint8(), pa.string()),
        'latsID'    : pa.dictionary(pa.uint8(), pa.string()),
    }

    
    
    def __init__(self,
                data_path   : 'str | pathlib.Path'                  ,
                seg_path    : 'str | pathlib.Path | None'   = None  ,
                batch_size  : int                           = 1000 ,
                f_overlap   : float                         = 0.3   
            ):
        super().__init__(data_path, batch_size, f_overlap)
        
        if seg_path is None: seg_path = data_path
        
        try:
            self._path_seg = get_file(seg_path, data_directory, ['.seg'])
        except FileNotFoundError:
            self._path_seg = None
        
        self._segments = None
        # self._used     = None
        self.memory.hits = []
        
    def __enter__(self):
        self._parquet = pq.ParquetFile(self._path)
        self._engine = 'pandas'
        if self._path_seg is not None: self._segments = SegDF.read_parquet(self._path_seg)
        return self
    
    @property
    def segments(self):
        return self._segments

    @property
    def unused_segments(self):
        return self.segments[np.isin(self.segments.seg, self.used, invert=True)]
    
    def fit(self, output_path, min_hits = 4, ncores = 12, nmax = None, debug = False, verbose=0):
        if ncores is None:
            ncores = multiprocessing.cpu_count()
        else:
            ncores = min(ncores, multiprocessing.cpu_count())
        self._pqwriter = None
        self._schema = None
        self._used = set()
        
        def update_bar(self, com_bar, proc_bar):
            def callback(result):
                try:
                    com_bar.update(1)
                    
                    idx = set(result.seg_idx.astype(int))
                    unused = idx - self._used
                    
                    if debug: print(f'{i:10d}: Writing {len(unused)} segments to disk')
                    result = result[result.seg_idx.isin(unused)]
                    
                    table = pa.Table.from_pandas(result, preserve_index = False)
                    
                    if self._pqwriter is None:
                        self._schema = pa.schema({field.name : self._data_types.get(field.name, field.type) for field in table.schema})
                        self._pqwriter = pq.ParquetWriter(output_path, self._schema)
                    
                    table = table.cast(self._schema)
                    self._pqwriter.write_table(table)
                    
                    self._used.update(idx)
                    
                    del result
                    gc.collect()
                    
                    proc_bar.update(1)
                except Exception as e:
                    print(f'Some went wrong here: {e}')
            return callback
        try:
            from ....utils.ipython import is_notebook
            if is_notebook():
                from tqdm.notebook import tqdm
            else:
                from tqdm import tqdm
        except:
            from tqdm import tqdm
        
        
        
        with Pool(ncores) as exe,\
             tqdm(total= nmax if nmax else 1, desc='Submitted',      unit = 'task(s)', dynamic_ncols=True)  as sub_bar,\
             tqdm(total= nmax if nmax else 1, desc='Completed',      unit = 'task(s)', dynamic_ncols=True)  as com_bar,\
             tqdm(total= nmax if nmax else 1, desc='Post-processed', unit = 'task(s)', dynamic_ncols=True)  as proc_bar:
            
            for i, data in enumerate(self):
                
                # TODO: Implement a way to skip already fitted segments, probably will need a multiprocessing.Manager
                if i == nmax: break
                segs = self.segments.get_segs(data)
                if len(segs) == 0:
                    com_bar.update(1)
                    proc_bar.update(1)
                    if debug: print(f'Skipping {i}, no segments found', flush = True)
                    continue
                if debug: print(f'\nSubmitting {i}', flush = True)
                exe.apply_async(self.task,
                    args=(data, segs),
                    kwds=dict(min_hits=min_hits, verbose=verbose, debug=debug),
                    callback = update_bar(self, com_bar, proc_bar),
                    error_callback=self.exception_handler
                )
                
                sub_bar.update()
            if not nmax:
                sub_bar.total  = (i+1)
                com_bar.total  = (i+1)
                proc_bar.total = (i+1)
        
                sub_bar.refresh()
                com_bar.refresh()
                proc_bar.refresh()
                
            exe.close()
            exe.join()
            
            if self._pqwriter: self._pqwriter.close()
    
    @staticmethod
    def exception_handler(e):
        if isinstance(e, KeyboardInterrupt):
            pass
        else:
            _, _, tb = sys.exc_info()
            print(f'Error: {e.with_traceback(tb)}')
            
    @staticmethod
    def task(data, segments, min_hits = 4, verbose=0, debug=False):
        if debug: print(f'Fitting {len(segments)} segments')
        musgo = MuSGO(data, segments, min_hits = min_hits, verbose=verbose)
        if debug: print(f'MuSGO has been made successfully!')
        musgo.fit(verbose=verbose)
        if debug: print(f'MuSGO has been fitted successfully!')
        return musgo.flatten().astype({'pattID':'category', 'latsID':'category'})


    
# define schema too
# self._pqwriter = pq.ParquetWriter(self.output_path, self._schema)
# table = pa.Table.from_pydict(self.buffer)
# _size = table.num_rows
# if self._schema: table = table.cast(self._schema)
# if not self.pqwriter: self._pqwriter = pq.ParquetWriter(self.output_path, table.schema)
# self.pqwriter.write_table(table)
# self._lines_read += _size
# self._reset_buffer()

            