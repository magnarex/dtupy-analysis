import itertools
import numpy as np

from ...pairing.hits.classes.LnL      import LnL
from ....utils.paths import load_yaml, config_directory


class PattID(object):
    __module__ = 'dtupy_analysis.dqm.reco'
    __patt_path__ = config_directory / 'dqm/reco/pattern'
    __allowed_relID__ = ['L', 'R', 'X']
    
    
    def __init__(self, *pattern, missing = None):
        self._missing = missing
        if len(pattern) == 1:
            pattID = pattern[0]
            if isinstance(pattID, str):
                pattID : str
                pattern = [relID for relID in pattID.upper()]
            else:
                raise TypeError(f'Argument is type {type(pattID)}! Only str is accepted as a single argument, e.g. LLR, XLR, etc.')
        else:
            if isinstance(pattern[0], str):
                pattern = [pat.upper() for pat in pattern]
            elif isinstance(pattern[0], int):
                pattern = ['X' if diff == 0 else 'L' if diff < 0 else 'R' for diff in pattern]
        
        
        self._patt = pattern
        
        try:
            assert np.isin(self.patt, self.__allowed_relID__).all()
        except AssertionError:
            raise ValueError(f"Relative position of cells must be left ('L'), right ('R') or unknown ('X'), but {self.pattID} was given!")

        self._calc_lnl()
        
    def _calc_lnl(self):
        self._lnl = LnL(len(self) + 1)
        # print(self.lnl)
        for i, relID in enumerate(self.patt):
            if relID == 'X':
                missing = len(self) + 1 - i
                # print(f'{self.lnl} / {LnL(missing, missing-1)}')
                self._lnl /= LnL(missing, missing-1)
        
    @property
    def patt(self):
        return self._patt
    
    @property
    def pattID(self):
        return ''.join(self.patt)
    
    @property
    def lnl(self):
        return self._lnl
    
    def __len__(self):
        return len(self.patt)
    
    def __repr__(self):
        return str(self)
    
    def __str__(self):
        return self.pattID
    
    def __iter__(self):
        comp_pattIDs = [''.join(patt) for patt in itertools.product(*[(p,) if p != 'X' else ['L', 'R'] for p in self.patt])]
        comp_latIDs = sum([load_yaml(self.__patt_path__)[pattID]['lats'] for pattID in comp_pattIDs], start = [])
        comp_latIDs = set(map(lambda lats: ''.join([lat if not is_missing else 'X' for (lat, is_missing) in zip(lats, self.missing)]), comp_latIDs))
        return iter([LatsID(lats, missing = self.missing) for lats in comp_latIDs])
    
    @property
    def lats(self):
        return list(self)
    
    @property
    def missing(self):
        return self._missing

class LatsID(object):
    __module__ = 'dtupy_analysis.dqm.reco'
    __allowed_relID__ = ['L', 'R', 'X']
    __relID_coef__ = {
        'L' : -1,
        'R' :  1,
        'X' :  np.nan
    }
    
    def __init__(self, *lats, missing = None):
        if len(lats) == 1:
            latsID = lats[0]
            if isinstance(latsID, str):
                latsID : str
                lats = [relID for relID in latsID.upper()]
            else:
                raise TypeError(f'Argument is type {type(latsID)}! Only str is accepted as a single argument, e.g. LLLR, LLRR, etc.')

        
        try:
            assert np.isin(lats, self.__allowed_relID__).all()
        except AssertionError:
            raise ValueError(f"Relative position of cells must be left ('L') or right ('R'), but {self.latID} was given!")
        
        self._missing = missing
        if not (missing is None):
            try:
                assert len(lats) == len(missing)
            except AssertionError:
                raise ValueError(f'Missing layers must be a bool array with the same size as layer number. {len(lats)} layers but len(missing) is {len(missing)}!')
            lats = ['X' if is_missing else lat for (lat, is_missing) in zip(lats, missing)]
        
        self._lats = lats
    @property
    def missing(self):
        return self._missing
    @property
    def lats(self):
        return self._lats
    
    @property
    def latsID(self):
        return ''.join(self.lats)
    
    def __array__(self, dtype=None):
        return np.array([self.__relID_coef__[lat] for lat in self.lats])
    
    def __str__(self):
        return self.latsID
    
    def __repr__(self):
        return str(self)
    
    def __mul__(self, other):
        return self.__array__() * other
    
    def __truediv__(self, other):
        return self.__array__() / other
    
    def __add__(self, other):
        return self.__array__() + other
    
    def __sub__(self, other):
        return self.__array__() - other
    
    def __neg__(self):
        return ~self
    
    def __invert__(self):
        return LatsID(*['L' if lat == 'R' else 'R' for lat in self.lats])
    
    def __getitem__(self, key):
        return self.__array__()[key]