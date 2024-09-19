"""
======================
Plotting Configuration
======================

The main class is `PlotConfig`\, which allows reading from a ``.yaml`` file. It
also defines the `Config` class, which is a wrapper for a dictionary so that
its items may be accessed as attributes and returns a default value.

=============== ========================================================
Config
--------------- --------------------------------------------------------
`Config`        Class implementing a dictionary
                with items that can be accessed as attributes
`FieldConfig`   Subclass of `Config` that represents the configuration
                for a variable (field).
`PlotConfig`    Subclass of `Config` where each value is a `FieldConfig`
                 instance
=============== ========================================================

"""

from .Config import Config, FieldConfig, PlotConfig