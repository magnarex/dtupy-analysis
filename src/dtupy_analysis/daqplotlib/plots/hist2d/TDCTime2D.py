import matplotlib as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import pandas
import numpy as np

from .Hist2D import Hist2D

class TDCTime2D(Hist2D):
    """
    A subclass of ``dtupy_analysis.daqplotlib.plots.Hist2D``, plots a variable against the
    BX number on the upper X-axis and the TDC time on the lower X-axis. 
    
    Attributes
    ----------
    var : str
        Variable that will plotted against BX.
    fig : `~matplotlib.figure.Figure`
        The Figure instance in which the Plot is drawn.
    ax : `~matplotlib.axes.Axes`
        The Axes instance in which the Plot is drawn.
    data : `~pandas.DataFrame` or `~panda.Series`
        A set of data that will be used by the `plot` method to draw the plot.
    plot_cfg : `dtupy_analysis.daqplotlib.config.PlotConfig`
        A default configuration for drawing the plots.
    cms_kwargs : `dict`
        A dictionary containing all the parameters for the CMS styling methods
        used by the constructor or otherwise.

    Methods
    -------
    plot(var, bin_width, **kwargs)
        Plots a 2D histogram using the data given in the construction of the instance.        
        
    See Also
    --------
    - dtupy_analysis.daqplotlib.plots.Hist2D
    """
    
    def __init__(self, *args, **kwargs):
        """
        Parameters
        ----------
        data : `~pandas.DataFrame` or `~pandas.Series`
            A DataFrame instance containing the data to be plotted. Can also be a `pandas.Series`.
        plot_cfg : `~dtupy_analysis.daqplotlib.config.PlotConfig`, optional
            An instance of class `~dtupy_analysis.daqplotlib.config.PlotConfig` to set the following plot settings.
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
        - dtupy_analysis.daqplotlib.plots.Hist2D
        """
        kwargs['cms_pad'] = 0.05
        if 'cms_rlabel' in kwargs.keys():
            cms_rlabel = kwargs.pop('cms_rlabel')
        
        super().__init__(*args, **kwargs)
        
        self.fig.suptitle(cms_rlabel, y = 0.96, x=0.95, ha = 'right', style = 'italic')
    
    def plot(self, var, bx_centre = None, bx_window = 5, **kwargs):
        """
        Plots a 2D histogram using the data given in the construction of the instance.
        
        Parameters
        ----------
        var : str
            Variable that will plotted against BX
        bx_centre : int, optional
            Central BX of the plot.
        bx_window : int, default 5
            Size, in BX, of the X-axis range.
        **kwargs : dict [str, ]
            Styling arguments to override given those given by `plot_cfg`.
        
        Other Parameters
        ----------------
        range : (float, float), optional
            The leftmost and rightmost edges of the bins along the Y-axis,
            the range in the X-axis must be set with `bx_centre` and `bx_window`. This
            overrides the value from `plot_cfg`.
        bins : None or int, optional
            Number of bins in the Y-axis. This overrides the value from `plot_cfg`.
        
        Notes
        -----
        The number of bins in the X-axis is fixed, but the range may be modified through
        `bx_centre` and `bx_window`. This is so for better readability.

        """
        hy_range = kwargs.pop('range', [None, None])
        hy_bins  = kwargs.pop('bins' , [None, None])
                    
        if isinstance(var, str):
            hy_data  = self.data[var]
            if hy_range is None: hy_range = self.plot_cfg[var].get('range', None)
            if hy_bins  is None: hy_bins  = self.plot_cfg[var].get('bins' , None)
        else:
            hy_data  = var
        
        # Compute TDC time from bx and TDC
        if bx_centre is None:
            bx_hist, bx_edges = np.histogram(self.data['bx'], self.plot_cfg['bx']['bins'], self.plot_cfg['bx']['range'])
            max_bx = bx_edges[bx_hist.argmax()]
        else:
            max_bx = bx_centre
        std_bx = (bx_window-1)//2
        range_bx = np.array([max(0, (max_bx - std_bx)), min(3556, (max_bx + std_bx + 1))], dtype=int)
        
        tdc_time = 32*self.data['bx'].to_numpy(dtype=int) + self.data['tdc'].to_numpy(dtype=int)
        tdc_time_range = 32*np.array(range_bx)
        xbins = range(tdc_time_range[0], tdc_time_range[1]+1 , 1)
        
        
        # Adjust X grid to bunch crossings
        self.ax.set_xticks(xbins[::32])
        self.ax.set_xticks(xbins[:: 4], minor=True)
        self.ax.xaxis.grid(True, color = 'k', linestyle='dashed', linewidth=1)
        
        # BX axis
        ax_bx = self.ax.twiny()
        ax_bx.set_xlim(*range_bx)
        ax_bx.set_xticks(range(range_bx[0], range_bx[1]+1))
        
        h = super().plot(tdc_time, hy_data,
                     range = [tdc_time_range   , hy_range],
                     bins  = [32*bx_window     , hy_bins ],
                     log_c = True,
                     **kwargs
                    )
        
        

        # Format colorbar from colormap
        self.fig.get_axes()[-1].set_ylabel('Events', fontsize=20)
        
        # Set axis labels
        self.ax.set_xlabel('TDC time (arb. units.)')
        self.ax.set_ylabel(self.plot_cfg[var].label.format(var=var))
