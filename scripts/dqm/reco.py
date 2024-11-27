#!/usr/bin/env python3
"""
This script handles the reconstruction of the segments.
"""

import importlib.util
import sys
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('seg_path'      ,
    help        = 'which segment file to reco (.seg; secretly a .parquet).'
)
parser.add_argument('out_path'      , 
    default     = None              ,
    nargs       = '?'               ,
    help        = 'output file that will contain the segment information (.fits; secretly a .parquet).'
)
parser.add_argument('--batch-size'  ,
    dest        = 'batch_size'      ,
    default     = 1e3               ,
    help        = 'size (in rows) of every new chunk that will be read from the data file. By default, 1e3. '
                  'Have in mind that final task will use (1 + overlap) * batch_size.'
)
parser.add_argument('--overlap'     ,
    dest        = 'f_overlap'       ,
    default     = 0.3               ,
    help        = 'fraction of the chunk that will be kept for the next iteration. By default, 0.3 (i.e. 30%%).'
)
parser.add_argument('--ncores', '-n',
    dest        = 'ncores'          ,
    type        = int               ,
    help        = 'number of processes (CPU cores) that the script will use at a time. By default, 12 at most.'
)
parser.add_argument('--min-hits',
    dest        = 'min_hits'            ,
    type        = int               ,
    default     = 4                 ,
    help        = 'minimum number of hits for a segment to be processed. By default, 4 hits per segment.'
)

parser.add_argument('--nmax',
    dest        = 'nmax'            ,
    type        = int               ,
    default     = None              ,
    help        = 'number of chunks to be processed. By default, all of them.'
)
parser.add_argument('--debug'       ,
    dest        = 'debug'           ,
    action      = 'store_true'      ,
    help        = "flag used to enable the debugging features."
)
parser.add_argument('-d', '--data'  ,
    dest        = 'data_path'       ,
    default     = None              ,
    help        = "path to the data file (.parquet). By default, the same same as segments."
)
parser.add_argument('-v', '--verbose',
    dest        = 'verbose'         ,
    action      = 'count'           ,
    default     = 0                 ,
    help        = 'increase output verbosity; can be used multiple times, e.g., -v, -vv, -vvv.'
)





args = parser.parse_args()

# Importing muTel package from anywhere
parent = '/'.join(str(Path(__file__).resolve()).split('/')[:-3]) # porque está tres niveles por debajo de la carpeta de instalación
loc = parent+'/src/dtupy_analysis/__init__.py'
src = 'dtupy_analysis'
spec = importlib.util.spec_from_file_location(src, loc)
foo = importlib.util.module_from_spec(spec)
sys.modules[src] = foo
spec.loader.exec_module(foo)

from dtupy_analysis.dqm.reco import MuSTIO
from dtupy_analysis.utils.paths import get_file, data_directory, get_with_default

seg_path = get_file(args.seg_path, data_directory, ['.seg'])

if args.out_path is None:
    out_path = seg_path.with_suffix('.fits')
else:
    out_path  = get_with_default(Path(args.out_path).with_suffix('.fits'), data_directory)

if args.nmax:
    out_path = out_path.with_stem(f'{out_path.stem}_{args.nmax}')

if args.data_path is None:
    data_path = seg_path.with_suffix('.parquet')
else:
    data_path  = get_with_default(Path(args.data_path).with_suffix('.parquet'), data_directory)

if __name__ == '__main__':
    with MuSTIO(data_path, seg_path, batch_size = args.batch_size, f_overlap=args.f_overlap) as mustio:
        mustio.fit(out_path,
                   min_hits=args.min_hits,
                   nmax=args.nmax,
                   ncores=args.ncores,
                   debug=args.debug,
                   verbose=args.verbose
                )