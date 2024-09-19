import matplotlib as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import pandas
import numpy as np

from ..Plot import CMSPlot

class Hist2D(CMSPlot):
    """
    Subclass of ``muTel.daqplotlib.plots.CMSPlot`` that plots a ``matplotlib.pyplot.hist2d`` in CMS style.
    
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
    plot(var_x, var_y, bx_window, debug=False, **kwargs)
        Plots a 2D histogram using the data given in the construction of the instance.
        
    See Also
    --------
    - muTel.daqplotlib.plots.CMSPlot
    """
    
    def __init__(self, *args, cms_pad = 0.05, cmap = 'cividis', **kwargs):
        """
        Parameters
        ----------
        data : `~pandas.DataFrame` or `~pandas.Series`
            A DataFrame instance containing the data to be plotted. Can also be a `pandas.Series`.
        plot_cfg : `~muTel.daqplotlib.config.PlotConfig`, optional
            An instance of class `~muTel.daqplotlib.config.PlotConfig` to set the following plot settings.
            - `bins` : int
            - `range` : `list` [`int`, `int`]
            - `label` : `str`, may contain ``{col}``
            - `log` : `bool`
        *args : tuple
            Positional args that will be passed to the `plot` method.
        **kwargs : dict [str, ]
            Styling arguments to override given those given by `plot_cfg`, those who
            are not plot styling, will be passed to `plot`.
        
        Other Parameters
        ----------------
        cmap : str, default 'cividis'
            Colormap that will be passed to ``matplotlib.pyplot.hist2d`` as ``cmap``.
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
        cms_pad : float, default 0.05
            Additional padding from axes border in units of axes fraction size.
            Argument will be passed to `~mplhep.cms.label` as `pad`.
        
        See Also
        --------
        - muTel.daqplotlib.plots.CMSPlot
        
        """
        super().__init__(*args, cms_pad = cms_pad, cmap = cmap, **kwargs)
    
    def plot(self, var_x, var_y, cmap = 'cividis', log_c = False, **kwargs):
        """
        Plots a 2D histogram using the data given in the construction of the instance.
        
        Parameters
        ----------
        var_x, var_y : str or `pandas.Series`
            If `var_x` (`var_y`) type is ``str``, it will fetch the column with that given name from `data`, in case
            the type is a ``pandas.Series``, it will be used as is. The `var_x` (`var_y`) will be plotted along the
            X-axis (Y-axis).
        log_c : bool, default False
            Turn on logarithmic scaling for the colormap.
        cmap : str, default 'cividis'
            Colormap that will be passed to ``matplotlib.pyplot.hist2d`` as ``cmap``.
        **kwargs : dict [str, ]
            Styling arguments to override given those given by `plot_cfg`.
        
        """

        range = kwargs.pop('range', [None, None])
        bins  = kwargs.pop('bins' , [None, None])
            
        if isinstance(var_x, str):
            hx_data  = self.data[var_x]
            if range[0] is None: range[0] = self.plot_cfg[var_x].get('range', None)
            if bins [0] is None: bins [0] = self.plot_cfg[var_x].get('bins' , None)
        else:
            hx_data  = var_x
        
        if isinstance(var_y, str):
            hy_data  = self.data[var_y]
            if range[1] is None: range[1] = self.plot_cfg[var_y].get('range', None)
            if bins [1] is None: bins [1] = self.plot_cfg[var_y].get('bins' , None)
        else:
            hy_data  = var_y

        if not ('norm' in kwargs.keys()):
            kwargs['norm'] = None if not log_c else mpl.colors.LogNorm(vmin=1)
                
        # Plot 2D Histogram
        self.h = h = self.ax.hist2d(hx_data, hy_data,
                range       = range,
                bins        = bins,
                cmap        = cmap,
                **kwargs
            )
        
        if isinstance(bins[0], int) & (not range[0] is None):
            hx_ticks = np.arange(*range[0])
            if bins[0] < 20: self.ax.set_xticks(hx_ticks, hx_ticks)
        
        hx_ticks = self.ax.get_xticks()
        if (hx_ticks[1]-hx_ticks[0]) <= 10:
            hx_minor_ticks = np.arange(*range[0])
            self.ax.set_xticks(hx_minor_ticks, minor=True)
            
        if isinstance(bins[1], int) & (not range[1] is None):
            hy_ticks = np.arange(*range[1])
            if bins[1] < 20: self.ax.set_yticks(hy_ticks, hy_ticks)
        
        hy_ticks = self.ax.get_yticks()
        if (hy_ticks[1]-hy_ticks[0]) <= 10:
            hy_minor_ticks = np.arange(*range[1])
            self.ax.set_yticks(hy_minor_ticks, minor=True)


        # Make colorbar from colormap
        plt.colorbar(h[3], ax=self.ax)
        self.fig.get_axes()[-1].set_ylabel('Events', fontsize=20)

        return h

