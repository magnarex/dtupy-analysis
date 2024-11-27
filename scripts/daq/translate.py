#!/usr/bin/env python3
"""
Translate a file from lines of bytes to a tabular format (.parquet).
"""

import importlib.util
import sys
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('src_path'      ,
    help        = 'which file structured in lines of bytes to translate.'
)
parser.add_argument('out_path'      , 
    help        = 'output file that will have table format (.parquet).'
)
parser.add_argument('-l'  , 
    dest        = 'language'        ,
    required    = True              ,
    choices     = ['it', 'la', 'es'],
    help        = 'language from which the input file will be translate.'
)
parser.add_argument('--cfg'         ,
    dest        = 'cfg_path'        ,
    default     = None              ,
    help        = 'configuration file (.yaml) that will be used for the translation.'
)
parser.add_argument('--buffer-size' ,
    dest        = 'buffer_size'     ,
    default     = 1e5               ,
    help        = 'maximum size for the buffer (in lines). By default, 1e5.'
)
parser.add_argument('--debug'       ,
    dest        = 'debug'           ,
    action      = 'store_true'      ,
    help        = """
                    flag used to enable the debugging features. If `alive_progress` is installed,
                    this will make a progress bar appear in terminal.
                  """
)
parser.add_argument('--verbose', '-v',
    dest        = 'verbose'          ,
    action      = 'store_true'       ,
    help        = 'flag used to enable verbosity'
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

from dtupy_analysis.daq import Translator


# Running the translator
if   (args.language.lower() == 'it') and (args.cfg_path is not None): transr = Translator.from_it(args.cfg_path)
elif (args.language.lower() == 'la') and (args.cfg_path is not None): transr = Translator.from_la(args.cfg_path)
elif (args.language.lower() == 'es')                                : transr = Translator.from_es(args.cfg_path)
else: raise ValueError(f"No language configuration for ('{args.language.lower()}') is wrong. Maybe missing the --cfg argument?")


transr.translate(args.src_path, args.out_path,
    max_buffer  = args.buffer_size  ,
    debug       = args.debug        ,
    verbose     = args.verbose
)