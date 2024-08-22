"""
==============================
Data Acquisition (DAQ) Library
==============================

The `muTel.daq` library offers support for handling the raw (binary)
data read from the Slow Control Box output. It allows to convert this
format into a tabular format (`.parquet`).

The main class that handles this behaviour is the `Translator`, which
takes a subclass of `Language` to convert the bits into python objects.

The current available languages are:
- Italian

## Structure

=========== ==================================================
Module
----------- --------------------------------------------------
`utils`     This module provides miscellaneous functionalities
            to the package that are used internally only by
            the ``muTel.daq`` sub-package.
=========== ==================================================

## Classes
### Translator
The `Translator` class will produce a series of fields from the data in
the raw files.

=========== =============== ===========================================
Field       Type            
----------- --------------- -------------------------------------------
index       ``uint64``      Index of the hit in the output table 
obdt_type   ``uint8``       `0` for OBDT Theta and `1` for OBDT Phi
obdt_ctr    ``uint8``       Connector in the OBDT connected to the link
station     ``int8``        Station in the CMS DT system
sl          ``uint8``       Superlayer in the given station
layer       ``uint8``       Layer in the given superlayer
cell        ``uint``        Cell in the given layer
=========== =============== ===========================================

These fields are all which are needed for locating an individual hit in
the detector.

### Languages

The subclasses of `Language` tell the `Translator` how each word of bits
has to be chopped and transformed into Python objects. Each field needs
three parameters: `mask`, `position` and `type`.

#### Italian

=========== =========== =============== ===========
Field       Type        Position        Mask        
----------- ----------- --------------- -----------
channel     ``uint8``   0               0xFF      
bx          ``uint16``  8               0xFFF               
tdc         ``uint8``   20              0b11111      
link        ``uint8``   60              0xF             
=========== =========== =============== =========

"""

from .classes.Translator import *