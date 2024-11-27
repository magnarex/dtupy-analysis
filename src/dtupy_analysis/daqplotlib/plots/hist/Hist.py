import matplotlib        as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy             as np
import pandas


from ..Plot import CMSPlot
from ...config import FieldConfig

class Hist(CMSPlot):
    """
    Subclass of ``dtupy_analysis.daqplotlib.plots.CMSPlot`` that plots a ``matplotlib.pyplot.hist`` in CMS style.
    
    Attributes
    ----------
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
    plot(var=None, debug=False, **kwargs)
        Plots a histogram using the data given in the construction of the instance.
        
    See Also
    --------
    - dtupy_analysis.daqplotlib.plots.CMSPlot
    """
    def __init__(self, data, *args, cms_loc = 2, **kwargs):
        """
        Constructor of the 1D histogram.
        
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
            Positional args that will be passed to the `plot` method.
        **kwargs : dict [str, ]
            Styling arguments to override given those given by `plot_cfg`, those who
            are not plot styling, will be passed to `plot`.
        
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
        cms_loc : int, default 2
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
        """
        super().__init__(data, *args, cms_loc = cms_loc, **kwargs)
        formatter = mticker.ScalarFormatter(useMathText=True)
        formatter.set_powerlimits((-3,3))
        self.ax.yaxis.set_major_formatter(formatter)

    def plot(self, var = None, debug = False, **kwargs):
        """
        Parameters
        ----------
        var : str, optional
            If `var` is set, it will fetch the column `data[var]` (assumes `data` is a ``pandas.DataFrame``) else, it
            will plot `data` (assumes `data` is a ``pandas.Series``).
        debug : bool, default False
            If ``True``, turns on debugging messages.
        **kwargs : dict [str, ]
            Styling arguments to override given those given by `plot_cfg`, those who
            are not plot styling, will be passed to `plot`.
        
        """
        if var is None: 
            df : pandas.Series = self.data
            hist_cfg = FieldConfig('None', {})
        else:
            df : pandas.Series = self.data[var]
            hist_cfg = self.plot_cfg[var]
        
        
        bins    = kwargs.pop('bins' , hist_cfg.get('bins' , len(df.unique()))           )
        range   = kwargs.pop('range', hist_cfg.get('range', (df.min(), df.max()+1))     )
        log     = kwargs.pop('log'  , hist_cfg.get('log'  , False)                      )
        label   = kwargs.pop('label', hist_cfg.get('label', 'var') if var is None else \
                                      hist_cfg.get('label', '{var}')                    )        
        
        step = np.abs(range[1] - range[0])/bins
        mids = np.arange(*range, step)
        
        cts, edges, _ = self.ax.hist(df,
                                     range = range, 
                                     bins  = bins,
                                     log   = log,
                                     **kwargs
                                    )

        if bins < 20: self.ax.set_xticks(mids, mids)
        
        self.ax.set_xticks(self.ax.get_xticks()+0.5*step, self.ax.get_xticklabels())
        
        self.ax.set_xlim(*range + 0.5*step*np.r_[-1,1])
        self.ax.set_ylim(self.ax.get_ylim()*np.r_[1,1.2])   
        
        self.ax.set_xlabel(label.format(var=var) if not (var is None) else label)
        self.ax.set_ylabel('Events')
        
        plt.tight_layout()


