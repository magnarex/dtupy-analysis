from typing import Any
from pathlib import Path

import matplotlib        as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import mplhep            as hep
import pandas

from dtupy_analysis.utils.paths import config_directory
from dtupy_analysis.utils.docs import is_documented_by
from ..config import PlotConfig

class Plot:
    """
    A class used to represent a Plot using the `matplotlib` library.
        
    """
    
    def __init__(self,
            data,
            *args,
            plot_cfg = None,
            figsize  = None,
            label_fs = 25,
            tick_pad = 10,
            **kwargs
        ):
        """
        Parameters
        ----------
        data : `~pandas.DataFrame` or `~pandas.Series`
            A DataFrame instance containing the data to be plotted. Can also be a `pandas.Series`.
        plot_cfg : `~dtupy_analysis.daqplotlib.config.PlotConfig`, optional
            An instance of class `~dtupy_analysis.daqplotlib.config.PlotConfig` to set the following plot settings.
            - `bins` : int
            - `range` : `list` [`int`, `int`]
            - `label` : `str`, may contain `{col}`
            - `log` : `bool`
        *args : tuple
            Additional args that will be passed to the `plot` method.
        **kwargs : dict, optional
            Keyword arguments that will be passed to the `plot` method.
        
        Other Parameters
        ----------------    
        figsize : (float, float), default [6.4, 4.8]
            Width, height in inches of Figure. Inhereted from matplotlib.
        label_fs : float, default 25
            Font size for axis labels.
        tick_pad : float, default 10
            Distance in points between tick and label. Inhereted from matplotlib.
            
        """
        self.init_figure(figsize=figsize)

        self._data = data
        self._args = args
        self._kwargs = kwargs
        if plot_cfg is None:
            self._plot_cfg = PlotConfig.from_yaml("default")
        else:
            self._plot_cfg = plot_cfg
        
        # Default ax params
        self.ax.tick_params(axis='both', which='major', pad=tick_pad)
        self.ax.set_xlabel(self.ax.get_xlabel(), fontsize = label_fs)
        self.ax.set_ylabel(self.ax.get_ylabel(), fontsize = label_fs)
        plt.tight_layout()

    def init_figure(self, figsize = None):
        self._fig, self._ax = plt.subplots(1,1, figsize=figsize)
        
    def __enter__(self):
        """
        Handles the creation in the context manager. If data is not None, this will
        automatically plot the figure.
        
        Returns
        -------
        self : dtupy_analysis.daqplotlib.plots.Plot
            An instances of this class.
        """
        if not (self.data is None): self.plot(*self._args, **self._kwargs)
        
        return self
        
        
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Handles the destruction in the context manager. This will ensure that the
        figure is closed upon exiting the `with` statement.
        """
        plt.close(self._fig)
        

    def plot(self, data, *args, **kwargs):
        """
        Method used for drawing the data in `~self.ax`. This is a placeholder and should
        be modified in a subclass for obtaining different plots.
        
        Arguments
        ---------
        data : `~pandas.DataFrame` or `~pandas.Series`
            The set of data that will be used for drawing the plot
        *args : tuple
            Positional arguments required by the method.
        **kwargs : dict
            Keyword arguments used by the method.
        """
        # Plot data...
        pass
    
    #----------------------
    # Write-only attributes
    #----------------------
    
    @property
    def ax(self) -> plt.Axes:
        """
        Get the `~matplotlib.axis.Axis` where the plot will be drawn.
        """
        return self._ax
    
    @property
    def fig(self) -> plt.Figure:
        """
        Get the `~matplotlib.figure.Figure` where the plot will be drawn.
        """
        return self._fig

    @property
    def data(self) -> 'pandas.DataFrame':
        """
        Get the dataset that will be used by the `self.plot` method.
        """
        return self._data

    @property
    def plot_cfg(self) -> 'PlotConfig':
        """
        Get the plot configuration that will be used by the `self.plot` method.
        """
        return self._plot_cfg


        
class CMSPlot(Plot):
    """
    A subclass of `dtupy_analysis.daqplotlib.plots.Plot` that provides support for the recommended
    CMS style using the package `~mplhep`.
    
    """

    def __init__(self,
            data,
            *args,
            plot_cfg = None,
            label_fs = 25,
            tick_pad = 10,
            cms_label = 'Private Work',
            cms_loc   = 0,
            cms_data  = True,
            cms_rlabel = '',
            cms_exp = False,
            cms_pad = 0,    
            **kwargs
        ):
        """      
        Parameters
        ----------
        data : `~pandas.DataFrame` or `~pandas.Series`
            A DataFrame instance containing the data to be plotted. Can also be a `pandas.Series`.
        plot_cfg : `~dtupy_analysis.daqplotlib.config.PlotConfig`, optional
            An instance of class `~dtupy_analysis.daqplotlib.config.PlotConfig` to set the following plot settings.
            - `bins` : int
            - `range` : `list` [`int`, `int`]
            - `label` : `str`, may contain `{col}`
            - `log` : `bool`
        
        Other Parameters
        ----------------
        figsize : (float, float), default [6.4, 4.8]
            Width, height in inches of Figure. Inhereted from matplotlib.
        label_fs : float, default 25
            Font size for axis labels.
        tick_pad : float, default 10
            Distance in points between tick and label. Inhereted from matplotlib.
        cms_label : str, default 'Private Work'
            Text to append after <exp> (Simulation) <label>. Typically “Preliminary”,
            “Supplementary”, “Private Work” or “Work in Progress”. Argument will be
            passed to `~mplhep.cms.label` as `label`.
        cms_loc : int, default 0
            Label position of `exp_text` label:
                - 0 : Above axes, left aligned
                - 1 : Top left corner
                - 2 : Top left corner, multiline
                - 3 : Split EXP above axes, rest of label in top left corner”
                - 4 : (1) Top left corner, but align “rlabel” underneath.
            Argument will be passed to `~mplhep.cms.label` as `loc`.
        cms_data : bool, default True
            Prevents prepending “Simulation” to experiment label. Argument will be
            passed to `~mplhep.cms.label` as `data`.
        cms_rlabel : str, default ''
            String to manually set right-hand label text. Argument will be passed
            to `~mplhep.cms.label` as `rlabel`.
        cms_exp : str, default False
            If a string, it will be appended to the CMS label; if False, just "CMS" will
            be shown. Argument will be passed to `~mplhep.cms.label` as `exp`.
        cms_pad : float, default 0
            Additional padding from axes border in units of axes fraction size.
            Argument will be passed to `~mplhep.cms.label` as `pad`.
        *args : tuple
            Additional args that will be passed to the `plot` method.
        **kwargs : dict, optional
            Styling arguments to override given those given by `plot_cfg`, those who
            are not plot styling, will be passed to `plot`.
        """
        hep.style.use("CMS")
        
        # Define 10-color scheme as sugested in:
        # https://cms-analysis.docs.cern.ch/guidelines/plotting/colors/#categorical-data-eg-1d-stackplots
        from cycler import cycler
        mpl.rcParams['axes.prop_cycle'] = cycler('color',
            ['#3f90da',
             '#ffa90e',
             '#bd1f01',
             '#94a4a2',
             '#832db6',
             '#a96b59',
             '#e76300',
             '#b9ac70',
             '#717581',
             '#92dadd']
        )
        
        
        
        super().__init__(
            data,
            *args,
            plot_cfg = plot_cfg,
            label_fs = label_fs,
            tick_pad = tick_pad,
            **kwargs
        )
        
        self._cms_kwargs = dict(
            cms_label  = cms_label                                      ,
            cms_loc    = cms_loc                                        ,
            cms_data   = cms_data                                       ,
            cms_rlabel = cms_rlabel                                     ,
            cms_exp    = ('CMS' if (not cms_exp) else cms_exp if ('CMS' in cms_exp) else 'CMS ' + cms_exp)   ,
            cms_pad    = cms_pad                                        ,
        )
        
        hep.cms.label(**{key[4:] : value for (key, value) in self.cms_kwargs.items()}, ax=self.ax) 
    @property
    def cms_kwargs(self) -> dict[str, Any]:
        """
        Get the keyword arguments that will be passed to `~mplhep.cms.label`.
        """
        return self._cms_kwargs
if __name__ == '__main__':
    with CMSPlot() as (fig, ax):
        ax.plot(1,1)
        fig.savefig('/nfs/cms/martialc/DTUpgrade/figs/test.png')
        
        