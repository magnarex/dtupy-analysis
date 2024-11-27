"""
====================
DAQ Plotting Library
====================

Sub-package of `dtupy_analysis` used for plotting with a standard style derived from
the CMS style guidlines for python available in this tutorial
https://indico.cern.ch/event/1398843/#1-plotting-tools-and-recommend.

It also enables defining a ``.yaml`` file with the default parameters (or custom)
for the plot style (bins, range, name of the field, label, ...).

Structure
---------
The package is divided in two modules: `plots` for plotting and `config` to handle
the plot styling configuration.

=========== ===========================================
Module
----------- -------------------------------------------
`plots`     This module contains all the default plot
            types used and more classes may be added
            using the `CMSPlot` object as a template.
`config`    This module handles the styling of the
            plots and reading the config ``.yaml``
            files.
=========== ===========================================

"""

from . import plots
from . import config
