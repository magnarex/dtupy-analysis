#!/usr/bin/env python3
"""
Use this script to plot a series of standard plots that will provide useful insight
into the status and performance of the readout electronics and processing of raw
data.
"""
from pathlib import Path
import importlib.util
import sys
import argparse
import itertools

import pandas as pd
import matplotlib as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import numpy.random as random


# Importing muTel package from anywhere
parent = '/'.join(str(Path(__file__).resolve()).split('/')[:-3]) # porque está tres niveles por debajo de la carpeta de instalación
loc = parent+'/src/dtupy_analysis/__init__.py'
src = 'dtupy_analysis'
spec = importlib.util.spec_from_file_location(src, loc)
foo = importlib.util.module_from_spec(spec)
sys.modules[src] = foo
spec.loader.exec_module(foo)

import dtupy_analysis.daqplotlib.plots  as daq_plots
import dtupy_analysis.daqplotlib.config as daq_config
from   dtupy_analysis.utils.paths import (
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
    
    fig_dir = fig_dir / 'daq'
    
    fig_dir.mkdir(parents=True, exist_ok=True)


    plot_cfg = daq_config.PlotConfig.from_yaml(hist_cfg)

    fields = list(plot_cfg.keys())


    df = pd.read_parquet(parquet_file)
    if 'link' not in df.columns:
        df['link'] = 0
        
    for link, data in df.groupby('link'):
        print(f'Plotting for link {link}')
        link_dir = fig_dir/Path(f'link_{str(link).zfill(2)}')
        link_dir.mkdir(parents=True, exist_ok=True)
                
        # VARIABLE DISTRIBUTIONS
        hist_dir = link_dir/Path('var_dist')
        hist_dir.mkdir(parents=True, exist_ok=True)
        for var in fields:
            if var not in data.columns: continue
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
                
        if np.isin(['obdt_ch', 'tdc'], fields).all():
            with daq_plots.BX2D(data, 'obdt_ch', plot_cfg = plot_cfg, cms_rlabel=f'link {link}') as plot:
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
            for station in data.station.unique():
                station_dir = link_dir/Path(f'station_{str(station).zfill(2)}')
                station_dir.mkdir(parents=True, exist_ok=True)
                
                for sl in data[data.station == station].sl.unique():
                    sl_dir = station_dir/Path(f'sl_{str(sl).zfill(2)}')
                    sl_dir.mkdir(parents=True, exist_ok=True)
                    
                    label = f'link {link} station {station} sl {sl}'
                    df_ssl = data[(data.station == station) & (data.sl == sl)]
                    with daq_plots.BX2D(df_ssl, 'cell', plot_cfg = plot_cfg, cms_rlabel=label) as plot:
                        plot.ax.yaxis.grid(True, which='both', color = 'k', linestyle='dotted', linewidth=1)
                        plot.fig.savefig(sl_dir/Path(f'BX2D_vs_cell.png'))
                        plot.inspect_bx (sl_dir/Path(f'BX2D_vs_cell.png'))
                        
                    with daq_plots.Hist2D(df_ssl, 'cell', 'layer', plot_cfg = plot_cfg, cms_rlabel=label, figsize=(15, 8), log_c = True) as plot:
                        plot.ax.yaxis.grid(True, which='both', color = 'k', linestyle='dotted', linewidth=1)
                        plot.fig.savefig(sl_dir/Path(f'occupancy_mb{station}_sl{sl}.png'))
                    
                    if np.isin(['channel', 'tdc'], fields).all():
                        with daq_plots.BX2D(df_ssl, 'channel', plot_cfg = plot_cfg, cms_rlabel=f'link {link}') as plot:
                            plot.fig.savefig(sl_dir/Path('BX2D_vs_ch.png'))
                            plot.inspect_bx (sl_dir/Path('BX2D_vs_ch.png'))
                            
                    if np.isin(['obdt_ch', 'tdc'], fields).all():
                        with daq_plots.BX2D(df_ssl, 'obdt_ch', plot_cfg = plot_cfg, cms_rlabel=f'link {link}') as plot:
                            plot.fig.savefig(sl_dir/Path('BX2D_vs_ch.png'))
                            plot.inspect_bx (sl_dir/Path('BX2D_vs_ch.png'))
        
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

    main(**vars(parser.parse_args()))
    
