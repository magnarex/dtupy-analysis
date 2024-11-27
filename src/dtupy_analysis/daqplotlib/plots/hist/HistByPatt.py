import matplotlib           as mpl; mpl.use('Agg')
import matplotlib.gridspec  as gridspec
import matplotlib.pyplot    as plt
import matplotlib.ticker    as mticker
import numpy                as np
import pandas
import mplhep               as hep



from .Hist import Hist
from ...config import FieldConfig
from ....utils.paths import load_yaml
from ....dqm.reco.classes.PattID import PattID


class HistByPatt(Hist):
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
    
    __patt_dict__ = load_yaml(PattID.__patt_path__)
    
    def __init__(self, data, *args, cms_loc = 0, **kwargs):
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
        super().__init__(data, *args, cms_loc = cms_loc, figsize=(24, 8), **kwargs)
        self.ax.remove()

        npatts = len(self.__patt_dict__)
        self.gs = gridspec.GridSpec(npatts, 2, width_ratios=[1, 2])
        
        # Crear el subplot único a la derecha
        self.ax_right = self.fig.add_subplot(self.gs[1:, 1])
        hep.cms.label(ax=self.ax_right, **{key[4:] : value for (key, value) in self.cms_kwargs.items()}) 
        
        # Crear los subplots a la izquierda
        self.axes = [self.fig.add_subplot(self.gs[i, 0], sharex = self.ax_right) for i in range(len(self.__patt_dict__))]
        [ax.sharey(self.axes[0]) for ax in self.axes[1:]]            

        
        
    def plot(self, var, reverse = True, add_plot=None, **kwargs):       
        hist_cfg = self.plot_cfg[var]
        df = self.data[var]
        
        bins    = kwargs.pop('bins' , hist_cfg.get('bins' , len(df.unique()))           )
        range   = kwargs.pop('range', hist_cfg.get('range', (df.min(), df.max()+1))     )
        log     = kwargs.pop('log'  , hist_cfg.get('log'  , False)                      )
        label   = kwargs.pop('label', hist_cfg.get('label', 'var') if var is None else \
                                      hist_cfg.get('label', '{var}')                    )        
        
        step = np.abs(range[1] - range[0])/bins
        mids = np.arange(*range, step)

        # group by pattID
        pattIDs = []; data = []; w = []; N_list = []
        for pattID in self.__patt_dict__.keys():
            if isinstance(var, str):
                data_patt = self.data[self.data.pattID == pattID][var]
            else:
                data_patt = var(self.data[df.pattID == pattID])
            N = len(data_patt)
            
            if N == 0:
                continue
            else:
                data.append(data_patt)
                w.append(np.ones(N) / N)
                pattIDs.append(pattID)
                N_list.append(N)

        # Order by counts
        ordered_idx     = [zipped[0][1] for zipped in zip(sorted(zip(N_list, np.arange(len(N_list)).tolist()), reverse=reverse))]
        ordered_data    = [data[i] for i in ordered_idx]
        colors  = [f'C{i}' for i in np.arange(len(ordered_idx))]
        # Plot each
        for i, c in zip(ordered_idx, colors):
            ax = self.axes[i]
            ax.hist(data[i],
                bins=bins,
                histtype='stepfilled',
                range=range,
                log=log, color=c,
                label = pattIDs[i]
            )
            ax.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
            ax.tick_params(axis='y', which='both', left=True, labelleft=False)
            ax.grid(which='minor',axis='x')
            ax.grid(which='major',axis='x',lw=1, color='k')
            if add_plot is not None: add_plot(ax)
            
            ax.set_ylabel(pattIDs[i])
            
        ax = self.axes[-1]
        # Configurar el último subplot a la izquierda
        ax.tick_params(axis='x', which='both', bottom=True, top=False, labelbottom=True)
        if label is not None: ax.set_xlabel(label)


        # Configurar el subplot único a la derecha
        self.ax_right.hist(ordered_data,
            bins        = bins,
            histtype    ='barstacked',
            range       = range,
            log         = log,
            color       = colors,
        )
        self.ax_right.grid(which='major',axis='y')
        self.ax_right.grid(which='minor',axis='x')
        self.ax_right.grid(which='major',axis='x',lw=1, color='k')
        self.fig.legend(ncols=len(pattIDs))
        if label is not None: self.ax_right.set_xlabel(label)
        self.ax_right.set_ylabel('Frequency')
