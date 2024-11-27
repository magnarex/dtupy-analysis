"""
=========
DAQ Plots
=========

This module is based in matplotlib and all the classes defined here have a `fig` attribute and an
`ax` attribute, which are matplotlib objects.

The module is organized by the matplotlib.pyplot function used to produce the plots, these are
the following:

======================= =====================================
matplotlib
function
----------------------- -------------------------------------
`hist`                  `Hist`
`hist2d`                `Hist2D`, `BX2D`, `TDCTime2D`
======================= =====================================

All these classes are derived from `CMSPlot` which is itself a subclass of `Plot`.

"""

# Base Classes
from .Plot                  import Plot, CMSPlot

# plt.hist2d
from .hist2d.Hist2D         import Hist2D
from .hist2d.TDCTime2D      import TDCTime2D
from .hist2d.BX2D           import BX2D
from .hist2d.PattLats2D     import PattLats2D

# plt.bar
from .bar.TDCstd_vs_ch      import TDCstd_vs_ch

# plt.hist
from .hist.Hist             import Hist
from .hist.HistByPatt       import HistByPatt