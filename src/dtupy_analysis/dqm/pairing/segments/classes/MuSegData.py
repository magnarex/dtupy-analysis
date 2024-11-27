import re

import pandas as pd

from ....core.classes.MuData import MuData

class MuSegData(MuData):
    def __init__(self, file, data : 'pd.DataFrame', engine = 'pandas'):
        super().__init__(file, data, engine=engine)
        
        for t_i in filter(lambda x: re.match(r'\At[1-9]\d*', x), self._df.columns):
            self._df[t_i] = self._df[t_i] - self._df['t0']

        self._df['MT1'] = 0.5 * (self._df.t1 + self._df.t3) + self._df.t2
        self._df['MT2'] = 0.5 * (self._df.t2 + self._df.t4) + self._df.t3
        self._df['MT']  = 0.5 * (self._df.MT1 + self._df.MT2)

        self._df['NT1'] = 0.5 * (self._df.t1 + self._df.t2)
        self._df['NT2'] = 0.5 * (self._df.t3 + self._df.t4)
        self._df['NT']  = 0.5 * (self._df.NT2 - self._df.NT1)

        