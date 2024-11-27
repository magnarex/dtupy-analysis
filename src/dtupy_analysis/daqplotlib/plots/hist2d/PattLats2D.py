import matplotlib as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import pandas
import numpy as np

from .Hist2D import Hist2D

class PattLats2D(Hist2D):
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
        - muTel.daqplotlib.plots.Hist2D
        
        """
        super().__init__(*args, cms_pad = cms_pad, cmap = cmap, **kwargs)
    
    def plot(self, cmap = 'cividis', log_c = False, **kwargs):
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

        pattID_cat = self.data['pattID'].astype('category').cat
        latsID_cat = self.data['latsID'].astype('category').cat

        hist2d = super().plot(pattID_cat.codes.values, latsID_cat.codes.values,
           bins=[len(pattID_cat.categories), len(latsID_cat.categories)],
           range=((0, len(pattID_cat.categories)), (0, len(latsID_cat.categories))),
           cmap=cmap,
           log_c=log_c
        )
        
        # hist2d = self.ax.hist2d(pattID_cat.codes.values, latsID_cat.codes.values,
        #    bins=[len(pattID_cat.categories), len(latsID_cat.categories)],
        #    range=((0, len(pattID_cat.categories)), (0, len(latsID_cat.categories))),
        #    cmap='cividis',
        #    norm = norm,
        #    **kwargs
        # )
         
        
        self.ax.set_xticks(np.arange(len(pattID_cat.categories))+0.5, pattID_cat.categories)
        self.ax.set_yticks(np.arange(len(latsID_cat.categories))+0.5, latsID_cat.categories)

        plt.tight_layout()
        
