<<<<<<< HEAD
import pandas as pd
import matplotlib.pyplot as plt
=======
"""
Use this script to plot a series of standard plots that will provide useful insight
into the status and performance of the readout electronics and processing of raw
data.
"""
>>>>>>> First local commit
from pathlib import Path
import importlib.util
import sys
import argparse
<<<<<<< HEAD
import numpy as np

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
parser.add_argument('--cfg'         , 
    dest        = 'hist_cfg'        ,
    default     = 'default_hist'    ,
    help        = 'configuration for the plots.'
)
parser.add_argument('--std-noise'   , 
    dest        = 'std_noise'       ,
    default     = 1.5               ,
    help        = 'configuration for the plots.'
)



args = parser.parse_args()



# Importing muTel package from anywhere
parent = '/'.join(__file__.split('/')[:-3]) # porque está tres niveles por debajo de la carpeta de instalación
=======
import itertools

import pandas as pd
import matplotlib as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import numpy.random as random


# Importing muTel package from anywhere
parent = '/'.join(str(Path(__file__).resolve()).split('/')[:-3]) # porque está tres niveles por debajo de la carpeta de instalación
>>>>>>> First local commit
loc = parent+'/src/muTel/__init__.py'
src = 'muTel'
spec = importlib.util.spec_from_file_location(src, loc)
foo = importlib.util.module_from_spec(spec)
sys.modules[src] = foo
spec.loader.exec_module(foo)

<<<<<<< HEAD
from muTel.utils.paths import data_directory, get_file, load_yaml, config_directory



parquet_file = get_file(args.src_path,data_directory,['.parquet'])

if args.fig_dir is None:
    fig_dir = Path(parent+'/figs') / parquet_file.stem
else:
    fig_dir = args.fig_dir
fig_dir.mkdir(parents=True, exist_ok=True)


hist_cfg_dict = load_yaml(args.hist_cfg, config_directory / Path('daq'))
cols = list(hist_cfg_dict.keys())

df = pd.read_parquet(parquet_file,columns=cols)

for col in cols:    
    hist_cfg = hist_cfg_dict.get(col,{})
    
    if not hist_cfg.get('figsize', None): hist_cfg['figsize']  = None
    
    
    fig, ax = plt.subplots(figsize=hist_cfg['figsize'])
    ax : plt.Axes
    
    if not hist_cfg.get('bins', None): hist_cfg['bins']  = len(df[col].unique())
    if not hist_cfg.get('range',None): hist_cfg['range'] = (df[col].min(), df[col].max()+1)

    step = np.abs(np.diff(hist_cfg['range'])/hist_cfg['bins']).astype(int)[0]
    # edges = np.linspace(*hist_cfg['range'], hist_cfg['bins']+1)
    mids = np.arange(*hist_cfg['range'], step)
    # cts, _ = np.histogram(df[col], range = hist_cfg['range'], bins = hist_cfg['bins'])
    
    cts, edges, _ = ax.hist(df[col], range = hist_cfg['range'], bins = hist_cfg['bins'])

    
    # ax.bar(mids, cts,width=step)
    if hist_cfg['bins'] < 20: ax.set_xticks(mids, mids)
    
    ax.set_xticks(ax.get_xticks()+0.5*step,ax.get_xticklabels())
    ax.set_xlim(*hist_cfg['range'] + 0.5*step*np.r_[-1,1])
    
    if col == 'bx': print(step, hist_cfg['range'] + 0.5*step*np.r_[-1,1], ax.get_xlim())
    
    
    if hist_cfg.get('label', None):
        ax.set_xlabel(hist_cfg['label'].format(col=col), fontsize = 12)
    ax.set_ylabel('Counts, cts (arb. units.)', fontsize = 12)
    ax.set_title(col, fontsize=14)
    
    fig.savefig(fig_dir/Path('{col}.png'.format(col=col)),dpi=300)
    del fig



# PLOT DEL ANCHO DE LA DISTRIBUCIÓN TEMPORAL POR CANAL
if ('channel' in cols) & ('tdc' in cols):
    
    # BAR PLOT
    std_by_ch = df.groupby('channel')['tdc'].std()
    
    fig, ax = plt.subplots()
    ax : plt.Axes
    ax.bar(std_by_ch.index, std_by_ch.values)
    ax.set_xlabel('Channel', fontsize = 12)
    ax.set_ylabel('Width of TDC distribution, $\sigma (tdc)$ (arb. units.)', fontsize = 12)
    
    glob_std = df.tdc.std()
    mean_std = std_by_ch.mean()
    
    noisy_ch = std_by_ch[std_by_ch/mean_std >= args.std_noise]
    ax.axhline(mean_std,zorder=5,color='r',linestyle='dashed', label='Mean TDC deviation per channel')
    ax.axhline(glob_std,zorder=5,color='k',linestyle='dashed', label = 'TDC deviation')
    
    if len(noisy_ch) > 0:
        print('\n')
        print(f'Global std           : {glob_std:3.2f}')
        print(f'Mean std per channel : {mean_std:3.2f}')
        
        print(f'\nChannels with higher-than-average std: {noisy_ch.index.to_list()}')
        for ch, std in noisy_ch.items():
            print(f' ch {ch:3d} : {std:5.2f} ({std/std_by_ch.mean():3.1f}̄σ)')
    else:
        print('\nNo noisy channels in sample! :D')
        
    plt.legend()
    fig.savefig(fig_dir/Path('TDCsig_vs_ch.png'))
    del fig
    
    
    # 2D HIST
    fig, ax = plt.subplots(figsize=(6,9))
    ax : plt.Axes
    h = ax.hist2d(df['tdc'], df['channel'],
              range = [hist_cfg_dict['tdc']['range'], hist_cfg_dict['channel']['range']],
              bins  = [hist_cfg_dict['tdc']['bins' ], hist_cfg_dict['channel']['bins' ]],
              density   = True
            )
    plt.colorbar(h[3],ax=ax)
    ax.set_xlabel('TDC, tdc (arb. units.)', fontsize = 12)
    ax.set_ylabel('Channel, ch', fontsize = 12)
    fig.savefig(fig_dir/Path('TDC_vs_ch_2d.png'))
    del fig

print('\n')
=======
import muTel.daqplotlib.plots  as daq_plots
import muTel.daqplotlib.config as daq_config
from   muTel.utils.paths import (
    data_directory,
    get_file,
    load_yaml,
    config_directory
)

def main(src_path, fig_dir, hist_cfg):
    parquet_file = get_file(src_path,data_directory,['.parquet'])

    if fig_dir is None:
        fig_dir = Path(parent+'/figs') / parquet_file.stem
    else:
        fig_dir = Path(fig_dir)
    fig_dir.mkdir(parents=True, exist_ok=True)


    plot_cfg = daq_config.PlotConfig.from_yaml(hist_cfg)

    fields = list(plot_cfg.keys())


    df = pd.read_parquet(parquet_file, columns=fields)

    for link, data in df.groupby('link'):
        print(f'Plotting for link {link}')
        link_dir = fig_dir/Path(f'link_{str(link).zfill(2)}')
        link_dir.mkdir(parents=True, exist_ok=True)

        # VARIABLE DISTRIBUTIONS
        hist_dir = link_dir/Path('var_dist')
        hist_dir.mkdir(parents=True, exist_ok=True)
        for var in fields:
            try:
                with daq_plots.Hist(data, var, plot_cfg = plot_cfg, cms_rlabel=f'link {link}') as plot:
                    plot.fig.savefig(hist_dir/Path(f'{var}.png'),dpi=300)
            except Exception as err:
                print(var)
                raise err
            
        # TDC time Hist
        with daq_plots.Hist(32*data.bx + data.tdc, cms_rlabel=f'link {link}') as plot:        
            plot.ax.set_xlabel('TDC time, (arb. uts.)')
            plot.fig.savefig(hist_dir/Path(f'TDCtime.png'),dpi=300)
        
                
        # PLOT DEL ANCHO DE LA DISTRIBUCIÓN TEMPORAL POR CANAL
        #     BAR PLOT
        # with daq_plots.TDCstd_vs_ch(data, plot_cfg = hist_cfg_dict, debug=True) as (fig, ax): 
        #     fig.savefig(link_dir/Path('TDCstd_vs_ch.png'))
        
        
        if np.isin(['channel', 'tdc'], fields).all():
            with daq_plots.BX2D(data, 'channel', plot_cfg = plot_cfg, cms_rlabel=f'link {link}') as plot:
                plot.fig.savefig(link_dir/Path('BX2D_vs_ch.png'))
                plot.inspect_bx (link_dir/Path('BX2D_vs_ch.png'))        
            
        if np.isin(['obdt_ctr', 'tdc'], fields).all():
            # 2D HIST
            with daq_plots.BX2D(data, 'obdt_ctr', plot_cfg = plot_cfg, cms_rlabel=f'link {link}') as plot:
                plot.fig.savefig(link_dir/Path('BX2D_vs_OBDTctr.png'))
                plot.inspect_bx (link_dir/Path('BX2D_vs_OBDTctr.png'))
        
        if np.isin(['tdc'], fields).all():
            # 2D HIST
            with daq_plots.BX2D(data, 'tdc', plot_cfg = plot_cfg, cms_rlabel=f'link {link}') as plot:
                plot.ax.yaxis.grid(True, which='both', color = 'k', linestyle='dotted', linewidth=1)
                plot.fig.savefig(link_dir/Path('BX2D_vs_tdc.png'))
                plot.inspect_bx (link_dir/Path('BX2D_vs_tdc.png'))


        if np.isin(['station', 'sl', 'cell', 'tdc'], fields).all():
            for station in df.station.unique():
                station_dir = link_dir/Path(f'station_{str(station).zfill(2)}')
                station_dir.mkdir(parents=True, exist_ok=True)
                
                for sl in df[df.station == station].sl.unique():
                    sl_dir = station_dir/Path(f'sl_{str(sl).zfill(2)}')
                    sl_dir.mkdir(parents=True, exist_ok=True)
                    
                    label = f'link {link} station {station} sl {sl}'
                    df_ssl = df[(df.station == station) & (df.sl == sl)]
                    with daq_plots.BX2D(df_ssl, 'cell', plot_cfg = plot_cfg, cms_rlabel=label) as plot:
                        plot.ax.yaxis.grid(True, which='both', color = 'k', linestyle='dotted', linewidth=1)
                        plot.fig.savefig(sl_dir/Path(f'BX2D_vs_cell.png'))
                        plot.inspect_bx (sl_dir/Path(f'BX2D_vs_cell.png'))

        
        print('\n')
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('src_path'      , 
        help        = 'table-like file (.parquet) that will be used to plot.'
    )
    parser.add_argument('fig_dir'       ,
        nargs       = '?'               ,
        default     = None              ,
        help        = 'directory where to save the plots.'
    )

    parser.add_argument('--cfg'         , 
        dest        = 'hist_cfg'        ,
        default     = 'default'         ,
        help        = 'configuration file for the plots. Default is "default".'
    )

    main(**parser.parse_args())
>>>>>>> First local commit
