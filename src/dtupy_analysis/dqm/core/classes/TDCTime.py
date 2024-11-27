from numbers import Number

from ..units import TDCTIME_UNIT, BX_UNIT, BXTIME_UNIT, OB_UNIT

import numpy as np

class TDCTime(object):
    __module__ = 'dtupy_analysis.dqm.core'

    def __init__(self, ob = 0, bx = 0, tdc = 0):
        self._ob  = np.int64(ob)
        self._bx  = np.int16(bx)
        self._tdc = np.int8(tdc)
        
    # def __array__(self, dtype=np.int64):
    #     return np.array(self.__int__(), dtype=dtype)
    
    def with_tdc(self, tdc):
        return TDCTime(self.ob, self.bx, tdc)
    
    def with_bx(self, bx):
        return TDCTime(self.ob, bx, self.tdc)
    
    def with_orbit(self, ob):
        return TDCTime(ob, self.bx, self.tdc)
    
    @property
    def ob(self):
        return self._ob
    
    @property
    def bx(self):
        return self._bx
    
    @property
    def tdc(self):
        return self._tdc
    
    @property
    def tdctime(self):
        return ((self.ob) * OB_UNIT + self.bx) * BX_UNIT + self.tdc
    
    @property
    def ns(self):
        return self.tdctime * TDCTIME_UNIT
    
    def __add__(self, other):
        tdc = (self.tdc + other.tdc) % BX_UNIT
        bx  = (self.bx + other.bx + (self.tdc + other.tdc) // BX_UNIT) % OB_UNIT
        ob  = self.ob + other.ob + (self.bx + other.bx + (self.tdc + other.tdc) // BX_UNIT) // OB_UNIT
        
        return TDCTimeDelta(ob, bx, tdc)
    
    def __sub__(self, other):
        tdc = (self.tdc - other.tdc) % BX_UNIT
        bx  = (self.bx - other.bx + (self.tdc - other.tdc) // BX_UNIT) % OB_UNIT
        ob  = self.ob - other.ob + (self.bx - other.bx + (self.tdc - other.tdc) // BX_UNIT) // OB_UNIT
        
        return TDCTimeDelta(ob, bx, tdc)
    
    def __int__(self):
        return int(self.tdctime)
    
    def __float__(self):
        return float(self.ns)
    
    def __str__(self):
        return f'{self.__class__.__name__}(ob = {self.ob}, bx = {self.bx}, tdc = {self.tdc}, ns = {self.ns})'
    
    def __repr__(self):
        return str(self)
    
    def __gt__(self, other):
        if isinstance(other, self.__class__):
            if self.ob > other.ob:
                return True
            elif self.ob == other.ob:
                if self.bx > other.bx:
                    return True
                elif self.bx == other.bx:
                    return self.tdc > other.tdc
                else:
                    return False
            else:
                return False
        elif isinstance(other, Number):
            return self.ns > other
        else:
            raise NotImplementedError(f'Comparison (>) between {self.__class__.__name__} and {other.__class__.__name__} is not implemented!')

    def __lt__(self, other):
        if isinstance(other, self.__class__):
            if self.ob < other.ob:
                return True
            elif self.ob == other.ob:
                if self.bx < other.bx:
                    return True
                elif self.bx == other.bx:
                    return self.tdc < other.tdc
                else:
                    return False
            else:
                return False
        
        elif isinstance(other, Number):
            return self.ns < other
        
        else:
            raise NotImplementedError(f'Comparison between (<) {self.__class__.__name__} and {other.__class__.__name__} is not implemented!')
    
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.ob == other.ob) and (self.bx == other.bx) and (self.tdc == other.tdc)
        
        elif isinstance(other, Number):
            return self.ns == other
        
        else:
            raise NotImplementedError(f'Comparison between {self.__class__.__name__} and {other.__class__.__name__} is not implemented!')

    def astype(self, dtype):
        if isinstance(self.tdctime, np.ndarray):
            if (dtype is int) | (dtype is np.int_):
                return self.tdctime.astype(dtype)
            elif (dtype is float) | (dtype is np.float_):
                return self.ns.astype(dtype)
            else:
                raise TypeError(f"Unsupported dtype: {dtype}")

    def _repr_html_(self):
        return repr(self)
    
    def __ge__(self, other):
        return self.__gt__(other) or self.__eq__(other)
    
    def __le__(self, other):
        return self.__lt__(other) or self.__eq__(other)


class TDCTimeDelta(TDCTime):
    def __init__(self, ob = 0, bx = 0, tdc = 0):
        super().__init__(ob, bx, tdc)
    
    def __str__(self):
        if isinstance(self.ns, Number):
            return f'{float(self):.2f} ns'
        else:
            with np.printoptions(formatter={'all' : lambda x: f'{float(x):.2f} ns'}):
                return str(self.ns)
    
    def __repr__(self):
        if isinstance(self.ns, Number):
            return f'{float(self):.2f}'
        else:
            with np.printoptions(formatter={'all' : lambda x: f'{float(x):.2f}'}):
                return str(self.ns)
    
    