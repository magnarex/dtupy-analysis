#!/usr/bin/env python3
"""
Use this script to plot a series of standard plots that will provide useful insight
into the status and performance of the reconstruction.
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

import dtupy_analysis.daqplotlib  as dpl
from   dtupy_analysis.utils.paths import (
    data_directory,
    get_file,
)

def main(src_path, fig_dir, hist_cfg):
    parquet_file = get_file(src_path,data_directory, ['.fits'])

    if fig_dir is None:
        fig_dir = Path(parent+'/figs') / parquet_file.stem
    else:
        fig_dir = Path(fig_dir)
    
    fig_dir = fig_dir / 'dqm'
    
    fig_dir.mkdir(parents=True, exist_ok=True)


    plot_cfg = dpl.config.PlotConfig.from_yaml(hist_cfg)

    fields = list(plot_cfg.keys())


    df = pd.read_parquet(parquet_file)
    
    with dpl.plots.Hist(df, var = 'theta') as plot:
        plot.fig.savefig(fig_dir / "theta.png", dpi=300)
    
    with dpl.plots.Hist(df, var = 'x0') as plot:
        plot.fig.savefig(fig_dir / "x0.png", dpi=300)

    with dpl.plots.Hist2D(df, 'x0', 'theta', figsize=(15,8), bins = (17*2, None), log_c=True) as plot:
        plot.ax.tick_params(axis='y', which='minor', left=False, labelleft=False, right=False)
        plot.ax.grid(which='both',axis='x')
        plot.ax.grid(which='major',axis='y')
        plot.fig.savefig(fig_dir / "theta_vs_x0_0p5.png", dpi=300)

    with dpl.plots.Hist2D(df, 'x0', 'theta', figsize=(15,8), bins = (17*10, None), log_c=True) as plot:
        plot.ax.tick_params(axis='y', which='minor', left=False, labelleft=False, right=False)
        plot.ax.grid(which='both',axis='x')
        plot.ax.grid(which='major',axis='y')
        plot.fig.savefig(fig_dir / "theta_vs_x0_0p1.png", dpi=300)

    with dpl.plots.PattLats2D(df, log_c=True) as plot:
        plot.fig.savefig(fig_dir / "pattID_vs_latsID.png", dpi=300)

    with dpl.plots.HistByPatt(df, 'x0') as plot:
        plot.fig.savefig(fig_dir / "x0_by_PattID.png", dpi=300)

    with dpl.plots.HistByPatt(df[df.chi2<1], 'x0') as plot:
        plot.fig.savefig(fig_dir / "x0_by_PattID_chi2lt1.png", dpi=300)

    with dpl.plots.HistByPatt(df, 'theta') as plot:
        plot.fig.savefig(fig_dir / "theta_by_PattID.png", dpi=300)

    with dpl.plots.HistByPatt(df[df.chi2<1], 'theta') as plot:
        plot.fig.savefig(fig_dir / "theta_by_PattID_chi2lt1.png", dpi=300)



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
    
