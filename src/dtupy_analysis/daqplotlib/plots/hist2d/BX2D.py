from pathlib import Path

import matplotlib as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import pandas
import numpy as np

from .Hist2D    import Hist2D
from .TDCTime2D import TDCTime2D


class BX2D(Hist2D):
    """
    A subclass of ``dtupy_analysis.daqplotlib.plots.Hist2D``, plots a variable against the BX
    number on the X-axis. 
    
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
    inspect_bx(savefig=None, bx_window=5)
        Searches the BX distribution and produces a plot centered around the filled
        regions.
        
    See Also
    --------
    - dtupy_analysis.daqplotlib.plots.Hist2D
    """
    
    def __init__(self, data, *args, cms_pad = 0, figsize = [14, 7], **kwargs):
        """
        Parameters
        ----------
        data : `~pandas.DataFrame` or `~pandas.Series`
            A DataFrame instance containing the data to be plotted. Can also be a `pandas.Series`.
        var : str
            Variable that will plotted against BX
        plot_cfg : `~dtupy_analysis.daqplotlib.config.PlotConfig`, optional
            An instance of class `~dtupy_analysis.daqplotlib.config.PlotConfig` to set the following plot settings.
            - `bins` : int
            - `range` : `list` [`int`, `int`]
            - `label` : `str`, may contain `{col}`
            - `log` : `bool`
        bin_width : int, default 3
            Number of BX per bin.
        *args : tuple
            Positional args that will be passed to the `plot` method.
        **kwargs : dict [str, ]
            Styling arguments to override given those given by `plot_cfg`, those who
            are not plot styling, will be passed to `plot`.
        
        Other Parameters
        ----------------
        cmap : str, default 'cividis'
            Colormap that will be passed to ``matplotlib.pyplot.hist2d`` as ``cmap``.
        figsize : (float, float), default [14, 7]
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
            
        See Also
        --------
        - dtupy_analysis.daqplotlib.plots.Hist2D
        """

        super().__init__(data, *args, cms_pad = cms_pad, figsize = figsize, **kwargs)
    
    def plot(self, var, bin_width = 3, **kwargs):
        """
        Plots a 2D histogram using the data given in the construction of the instance.
        
        Parameters
        ----------
        var : str
            Variable that will plotted against BX
        bin_width : int, default 3
            Number of BX per bin.
        **kwargs : dict [str, ]
            Styling arguments to override given those given by `plot_cfg`.
            
        Notes
        -----
        For some reason, binning below a width of 3 bx results in some bins
        disappearing. Have this in mind when changing the `bin_width` variable.

        """
        self.var = var
        bx_range = self.plot_cfg['bx']['range']
        
        log_c = kwargs.pop('log_c', True)
        
        bx_bins  = np.arange(bx_range[0], bx_range[1]+1,bin_width)
        if bx_bins[-1] >= bx_range[1]:
            bx_bins[-1] = bx_range[1]
        else:
            bx_bins = np.r_[bx_bins, bx_range[1]]
            
        super().plot('bx', var,
                         range = [bx_range, None],
                         bins  = [bx_bins , None],
                         log_c = log_c,
                         **kwargs
                        )
        
        # self.fig.tight_layout()
        self.ax.margins(x=0, tight=True)
        self.fig.get_axes()[-1].margins(x=0, tight=True)
        
        # Grid
        self.ax.yaxis.grid(True, color = 'k', linestyle='dotted', linewidth=1)
        

        # Set axis labels
        self.ax.set_xlabel('Bunch crossing (arb. units.)')
        self.ax.set_ylabel(self.plot_cfg[var].label.format(var=var))
        
        plt.tight_layout()

        
    def inspect_bx(self, savefig = None, bx_window = 5, threshold = 0.05):
        """
        Scan the BX distributions to find the non-empty bins and plot a zoomed hist
        around these in TDC time (32·BX+TDC).
        
        Paramaters
        ----------
        savefig : str or Path, optional
            Path to the destination folder and anme where the figure will be saved. The
            name will be appended ``_bx{BX}`` where ``{BX}`` will be replaced with the
            BX around which the plot is centered.
        bx_window : int, default 5
            Total range of the X-axis. This will result in :math:`(bx_window-1)/2` BXs
            on each side of the central BX.
        """
        # Flatten hist in bx dimension and get the bins that are filled
        hist2d_flat = self.h[0].sum(axis=1)
        filled_bx = self.h[1][:-1][hist2d_flat > 0]
        filled_cts = hist2d_flat[hist2d_flat > 0]
        
        cms_kwargs = {key:val for key, val in self.cms_kwargs.items() if key != 'cms_pad'}
        
        skip_bx = []
        for i, bx in enumerate(filled_bx):
            if bx in skip_bx:
                continue
            elif filled_cts[i]/filled_cts.sum() < threshold:
                continue
            else:
                with TDCTime2D(self.data, self.var, plot_cfg = self.plot_cfg, bx_centre = bx, **cms_kwargs) as bx_plot:
                    bx_plot.ax.yaxis.grid(True, which = 'both', color = 'k', linestyle='dotted', linewidth=1)
                    
                    
                    if isinstance(savefig, (str, Path)):
                        path = Path(savefig)
                        # Make figures subfolder
                        fig_path = savefig.with_stem(f'{path.stem}_bx').with_suffix('')
                        fig_path.mkdir(parents=True, exist_ok=True)

                        bx_plot.fig.savefig(fig_path / Path(f'{path.stem}_bx{bx}{path.suffix}'))
                
                # This needs some fixing
                # Add bins that will fall in the same window so there isn't overlap
                [skip_bx.append(bx_i) for bx_i in filled_bx if bx_i <= bx + bx_window//2]



            