import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import importlib.util
import sys
import argparse


parser = argparse.ArgumentParser(description="Translate a file from lines of bytes to a tabular format (.parquet).")
parser.add_argument('--src'         , 
    dest        = 'src_path'        ,
    required    = True              ,
    help        = 'table-like file (.parquet) that will be used to plot.'
)
parser.add_argument('--out'         , 
    dest        = 'fig_dir'         ,
    default     = None              ,
    help        = 'directory where to save the plots.'
)
parser.add_argument('--range'       , 
    dest        = 'range'           ,
    default     = None              ,
    nargs       = 2                 ,
    help        = 'directory where to save the plots.'
)
parser.add_argument('--nbins'       , 
    dest        = 'nbins'           ,
    default     = 100               ,
    help        = 'directory where to save the plots.'
)

args = parser.parse_args()



# Importing muTel package from anywhere
parent = '/'.join(__file__.split('/')[:-3]) # porque está tres niveles por debajo de la carpeta de instalación
loc = parent+'/src/muTel/__init__.py'
src = 'muTel'
spec = importlib.util.spec_from_file_location(src, loc)
foo = importlib.util.module_from_spec(spec)
sys.modules[src] = foo
spec.loader.exec_module(foo)

from muTel.utils.paths import data_directory, get_file




parquet_file = get_file(args.src_path,data_directory,['.parquet'])

if args.fig_dir is None:
    fig_dir = Path(parent+'/figs') / parquet_file.stem
else:
    fig_dir = args.fig_dir


df = pd.read_parquet(parquet_file)
cols = df.columns

for col in cols:
    fig, ax = plt.subplots()
    nbins = args.nbins
    range = args.range
    
    if len(df[col].unique()) < nbins:
        nbins = len(df[col].unique())
    if range == None :
        range = (df[col].min(), df[col].max())
    
    ax.hist(df[col], range = range, bins = nbins)
    
    fig_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(fig_dir/Path('{col}.png'.format(col=col)))
    del fig