import muTel.utils.meta as meta
from muTel.utils.data import display_df, data_path
from muTel.utils.multiprocessing import data_to_events

from copy import deepcopy
import logging
import pandas as pd
import numpy as np
import sys

from collections.abc import Iterable


_MuTel_logger = logging.Logger('MuTel')
_MuTel_logger.addHandler(logging.StreamHandler(sys.stdout))



class MuTelType(type):
    def __repr__(self):
        return self.__name__
    

class MuTel(object, metaclass = MuTelType):
    def __init__(self, data):
        self._events = None