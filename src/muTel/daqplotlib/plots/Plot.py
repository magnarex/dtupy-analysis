from typing import Any

import matplotlib        as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import mplhep            as hep
import pandas

from muTel.utils.docs import is_documented_by
from ..config import PlotConfig

class Plot:
    """
    A class used to represent a Plot using the `matplotlib` library.
    
    Attributes
    ----------
    fig : `~matplotlib.figure.Figure`
        The Figure instance in which the Plot is drawn.
    ax : `~matplotlib.axes.Axes`
        The Axes instance in which the Plot is drawn.
    data : `~pandas.DataFrame` or `~panda.Series`
        A set of data that will be used by the `plot` method to draw the plot.
    plot_cfg : `muTel.daqplotlib.config.PlotConfig`
        A default configuration for drawing the plots.
    
    Methods
    -------
    plot(data, *args, **kwargs)
        Draw the given plot in the Figure's Axis.
    """
    
    def __init__(self,
            data,
            *args,
            plot_cfg = PlotConfig({}),
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
        plot_cfg : `~muTel.daqplotlib.config.PlotConfig`, optional
            An instance of class `~muTel.daqplotlib.config.PlotConfig` to set the following plot settings.
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
        self._fig, self._ax = plt.subplots(1,1, figsize=figsize)
        self._data = data
        self._args = args
        self._kwargs = kwargs
        self._plot_cfg = plot_cfg
        
        # Default ax params
        self.ax.tick_params(axis='both', which='major', pad=tick_pad)
        self.ax.set_xlabel(self.ax.get_xlabel(), fontsize = label_fs)
        self.ax.set_ylabel(self.ax.get_ylabel(), fontsize = label_fs)

        
        
    def __enter__(self):
        """
        Handles the creation in the context manager. If data is not None, this will
        automatically plot the figure.
        
        Returns
        -------
        self : muTel.daqplotlib.plots.Plot
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
    A subclass of `muTel.daqplotlib.plots.Plot` that provides support for the recommended
    CMS style using the package `~mplhep`.
    
    Attributes
    ----------
    fig : `~matplotlib.figure.Figure`
        The Figure instance in which the Plot is drawn.
    ax : `~matplotlib.axes.Axes`
        The Axes instance in which the Plot is drawn.
    data : `~pandas.DataFrame` or `~panda.Series`
        A set of data that will be used by the `plot` method to draw the plot.
    plot_cfg : `muTel.daqplotlib.config.PlotConfig`
        A default configuration for drawing the plots.
    cms_kwargs : `dict`
        A dictionary containing all the parameters for the CMS styling methods
        used by the constructor or otherwise.
    
    Methods
    -------
    plot(data, *args, **kwargs)
        Draw the given plot in the Figure's Axis.
    """

    def __init__(self,
            data,
            *args,
            plot_cfg = PlotConfig({}),
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
        plot_cfg : `~muTel.daqplotlib.config.PlotConfig`, optional
            An instance of class `~muTel.daqplotlib.config.PlotConfig` to set the following plot settings.
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
        super().__init__(
            data,
            *args,
            plot_cfg = plot_cfg,
            label_fs = label_fs,
            tick_pad = tick_pad,
            **kwargs
        )
        
        self._cms_kwargs = dict(
            label  = cms_label                                      ,
            loc    = cms_loc                                        ,
            data   = cms_data                                       ,
            rlabel = cms_rlabel                                     ,
            exp    = ('CMS' if not cms_exp else 'CMS ' + cms_exp)   ,
            pad    = cms_pad                                        ,
        )
        
        hep.cms.label(**self.cms_kwargs) 
          
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
        
        