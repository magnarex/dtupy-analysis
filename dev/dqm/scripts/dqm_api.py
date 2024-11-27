import sys, os
sys.path.insert(1, '/nfs/cms/martialc/DTUpgrade/src')


import dtupy_analysis
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import pyarrow as pa
import pyarrow.parquet as pq
import dask

import dtupy_analysis.dqm as dqm
from dtupy_analysis.dqm.pairing import matrix_diff, distance_matrix, pair_hits

file = '/nfs/cms/martialc/DTUpgrade/data/testpulse_240802.parquet'
with dqm.MuFile(file) as file:
    for mudata in file:
        musl = mudata.get_ssl(1,1)
        d, mask = distance_matrix(musl.data, 1, 2)
        print(type(d))
        
        
        break