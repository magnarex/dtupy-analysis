from dataclasses import dataclass, field
from numpy import log10, power, floor, round
from typing import Optional

@dataclass(repr=False, init=False)
class PhysicalMagnitude(object):
    _magnitude_order    : None          = field(repr= False, default = None)
    _mantissa           : None          = field(repr= False, default = None)

    __order_dict = {
        -4  :   'p',
        -3  :   'n',
        -2  :   'μ',
        -1  :   'm',
        0   :   '' ,
        1   :   'k',
        2   :   'M',
        3   :   'G',
        4   :   'T',
        5   :   'P'
    }
    def __init__(self, value : float, precision : int = 2, unit_symbol : str = 'u', magnitude_name : Optional[str] = None):
        self.value = value
        self._precision = precision
        self._unit_symbol = unit_symbol
        self._magnitude_name = magnitude_name

    def __repr__(self):
        return '{value}{order}{units}'.format(
            value = round(power(10,self.mantissa),self.precision),
            order = self.__order_dict[self.magnitude_order],
            units = self.unit_symbol
        )

    @property
    def magnitude_order(self):
        if self._magnitude_order is None:
            return self._calc_order()[1]
        else:
            return self._magnitude_order
        
    @property
    def mantissa(self):
        if self._mantissa is None:
            return self._calc_order()[0]
        else:
            return self._mantissa
    
    def _calc_order(self):
        val_log = log10(self.value)/3

        self._magnitude_order = order = int(floor(val_log))
        self._mantissa = mantissa = 3*(val_log-order)

        return mantissa, order

    @property
    def unit_symbol(self):
        return self._unit_symbol
    @unit_symbol.setter
    def unit_symbol(self,value):
        self._unit_symbol = value
    
    @property
    def magnitude_name(self):
        if self._magnitude_name is None:
            return self.__class__.__name__
        else:
            return self._magnitude_name
    
    @magnitude_name.setter
    def magnitude_name(self, value : (type(None) | str)):
        self._magnitude_name = value

    @property
    def precision(self):
        return self._precision
    
    def __add__(self, other : 'PhysicalMagnitude'):
        try:
            assert (self.unit_symbol == other.unit_symbol) & (self.__class__.__name__ == other.__class__.__name__)
        except AssertionError:
            raise ValueError('No se pueden sumar dos magnitudes distintas entre ellas.')
        return PhysicalMagnitude(
            value           = self.value + other.value,
            unit_symbol     = self.unit_symbol,
            magnitude_name  = self.magnitude_name,
            precision       = min(self.precision,other.precision)
            )
    
    
    def __sub__(self, other : 'PhysicalMagnitude'):
        try:
            assert (self.unit_symbol == other.unit_symbol) & (self.__class__.__name__ == other.__class__.__name__)
        except AssertionError:
            raise ValueError('No se pueden restar dos magnitudes distintas entre ellas.')
        return PhysicalMagnitude(
            value           = self.value - other.value,
            unit_symbol     = self.unit_symbol,
            magnitude_name  = self.magnitude_name,
            precision       = min(self.precision,other.precision)
            )


    def __mul__(self, other : 'PhysicalMagnitude'):
        return PhysicalMagnitude(
            value           = self.value * other.value,
            unit_symbol     = '·'.join([self.unit_symbol,other.unit_symbol]),
            magnitude_name  = '*'.join([self.magnitude_name,other.magnitude_name]),
            precision       = min(self.precision,other.precision)
            )
    
    def __truediv__(self, other : 'PhysicalMagnitude'):
        if (self.unit_symbol == other.unit_symbol) & (self.__class__.__name__ == other.__class__.__name__):
            symbol  = ''
            name    = 'Scalar'
        else:
            symbol  = '/'.join([self.unit_symbol,other.unit_symbol])
            name    = '/'.join([self.magnitude_name,other.magnitude_name])
        
        return PhysicalMagnitude(
            value           = self.value / other.value,
            unit_symbol     = symbol,
            magnitude_name  = name,
            precision       = min(self.precision,other.precision)
            )

    def __eq__(self, other : 'PhysicalMagnitude'):
        return (self.value == other.value) & (self.unit_symbol == other.unit_symbol)



@dataclass(init=False, repr=False)
class Time(PhysicalMagnitude):
    def __init__(self,value,precision = 2):
        super().__init__(
            value           = value,
            precision       = precision,
            unit_symbol     = 's',
        )


    def __repr__(self):
        if self.magnitude_order == 1:

            return '{value_m}m {value_s}s'.format(
                value_m = int(self.value // 60),
                value_s = round(self.value %  60, self.precision)
            )
        elif self.magnitude_order >= 2:
            rest = self.value % 3600
            return '{value_h}h {value_m}m {value_s}s'.format(
                value_h = int(self.value // 3600),
                value_m = int(rest // 60),
                value_s = round(rest %  60, self.precision)
            )
        else:
            return super().__repr__()

    @property
    def magnitude_order(self):
        if self._magnitude_order is None:
            return self._calc_order()[1]
        else:
            return self._magnitude_order
        
    @property
    def mantissa(self):
        if self._mantissa is None:
            return self._calc_order()[0]
        else:
            return self._mantissa
    
    def _calc_order(self):  
        if self.value > 60:
            val_log = log10(self.value)/log10(60)
            self._magnitude_order = order = min(2, int(floor(val_log)))
            self._mantissa = mantissa = val_log-order
        else:
            val_log = log10(self.value)/3
            self._magnitude_order = order = int(floor(val_log))
            self._mantissa = mantissa = 3*(val_log-order)

        return mantissa, order




@dataclass(init=False, repr=False)
class Distance(PhysicalMagnitude):
    def __init__(self,value,precision = 2):
        super().__init__(
            value           = value,
            precision       = precision,
            unit_symbol     = 'm',
        )




if __name__ == '__main__':
    val1 = Time(6223)
    print(val1)

    # val1 = Distance(0.1)
    # val2 = Time(245e-9)
    # print(f'{val1} / {val2} = {val1 / val2}')

    # val1 = Distance(10)
    # val2 = Distance(5)
    # print(f'{val1} / {val2} = {val1 / val2}')