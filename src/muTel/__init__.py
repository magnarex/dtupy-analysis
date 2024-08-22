<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> cd7ff81b9f591768977cc27777949b747be49a37
"""
=====
muTel
=====
A package for all things **Muon Drift Tube** related.

This package is divided in subpackages that are mostly stand-alone
and comprise different parts of the Data Acquisiton (DAQ), Data
Quality Monitoring (DQM) and Data Analysis.

## Structure

=============== ======================================================
Module
--------------- ------------------------------------------------------
`daq`           Data Acquisition (DAQ) sub-package, oriented towards
                the translation of the data produced by a Slow Control
                Box to a compact table-like format (`.parquet`).
`daqplotlib`    Plotting sub-package that uses the `.parquet` files
                produced with `muTel.daq` to generate a series of
                plots used for monitoring and debugging.
`dqm`           Data Quality Monitoring (DQM) sub-package that handles
                the reconstruction of the data in the `.parquet` files
                (*work in progress*).                
`utils`         This module provides miscellaneous functionalities
                to the package that are used across many sub-packages.
=============== ======================================================

"""
<<<<<<< HEAD
>>>>>>> First local commit
=======
>>>>>>> cd7ff81b9f591768977cc27777949b747be49a37
from .utils import parent