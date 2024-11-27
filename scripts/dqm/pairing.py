#!/usr/bin/env python3
"""
This script handles the pairing of the segments.
"""

import importlib.util
import sys
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('src_path'      ,
    help        = 'which data file to build segments from (.parquet).'
)
parser.add_argument('out_path'      , 
    default     = None              ,
    nargs       = '?'               ,
    help        = 'output file that will contain the segment information (.seg; secretly a .parquet).'
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
    help        = 'number of processes (CPU cores) that the script will use at a time. By default, all of them.'
)
parser.add_argument('--nmax',
    dest        = 'nmax'            ,
    type        = int               ,
    default     = None              ,
    help        = 'number of segments to be processed. By default, all of them.'
)
parser.add_argument('--f-index', '-f',
    dest        = 'f_index'          ,
    type        = str                ,
    default     = 'log'              ,
    help        = 'function to use for index difference computation. By default, "log".'
)
parser.add_argument('--debug'       ,
    dest        = 'debug'           ,
    action      = 'store_true'      ,
    help        = "flag used to enable the debugging features."
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

from dtupy_analysis.dqm import MuFile
from dtupy_analysis.utils.paths import get_file, data_directory, get_with_default

src_path = get_file(args.src_path, data_directory, ['.parquet'])

if args.out_path is None:
    out_path = src_path.with_suffix('.seg')
else:
    out_path  = get_with_default(Path(args.out_path).with_suffix('.seg'), data_directory)

if args.nmax:
    out_path = out_path.with_stem(f'{out_path.stem}_{args.nmax}')

with MuFile(src_path, batch_size=args.batch_size, f_overlap=args.f_overlap) as mufile:
    mufile.calc_segments(ncores = args.ncores, nmax = args.nmax, f_index = args.f_index, debug = args.debug)
    mufile.export_segments(out_path, debug = args.debug)