from typing import TYPE_CHECKING

import numpy as np
import pandas as pd


if TYPE_CHECKING:
    from numbers            import Number
    from collections.abc    import Iterable


class LnL(object):
    __module__ = 'dtupy_analysis.dqm.pairing.hits'
    def __init__(self, *layers):
        if len(layers) == 1:
            arg = layers[0]
            if isinstance(arg, str):
                arg : str
                layers = [int(layer) for layer in arg.split('w')]
            elif isinstance(arg, int):
                arg : int
                layers = np.arange(arg) + 1
            else:
                raise TypeError(f'Argument is type {type(arg)}! Only str is accepted as a single argument, e.g. 3w2, 4w3w2, etc.')
        self._layers = np.array(sorted(layers, reverse=True), dtype=int)
        try:
            assert (np.diff(self._layers) == -1).all()
        except AssertionError:
            raise ValueError("Layer numbers must be consecutive, no skips allowed!")
    @property
    def top(self):
        return self.layers.max()
    @property
    def bot(self):
        return self.layers.min()
    @property
    def layers(self):
        return self._layers
    @property
    def id(self):
        return 'w'.join(self.layers.astype(str))
    def __str__(self):
        return self.id
    def __repr__(self):
        return str(self)
    def __repr_html__(self):
        return str(self)
    
    def __add__(self, other):
        if isinstance(other, int):
            return LnL(*(self.layers + other))
        else:
            raise TypeError("Addition takes only integers!")
    def __sub__(self, other):
        if self.bot == 1:
            print("WARNING: This is already the lowest layer pair, can't go lower than 2w1!")
            return LnL(*self.layers)
        elif isinstance(other, int):
            return LnL(*(self.layers - other))
        else:
            raise TypeError("Subtraction takes only integers!")
    def __contains__(self, other):
        if isinstance(other, Number):
            other = str(int(other))
        elif isinstance(other, str):
            pass
        else:
            raise TypeError(f'Type of {other} is {type(other)}: only Number or str are accepted.')
        
        return other in str(self)
    def __eq__(self, other):
        return str(self).__eq__(str(other))
    def __ne__(self, other):
        return str(self).__ne__(str(other))
    def __lt__(self, other):
        return str(self).__lt__(str(other))
    def __gt__(self, other):
        return str(self).__gt__(str(other))
    def __le__(self, other):
        return str(self).__le__(str(other))
    def __ge__(self, other):
        return str(self).__ge__(str(other))
    
    def __hash__(self):
        return self.id.__hash__()
    def __mul__(self, other):
        if max(self.bot, other.bot) == min(self.top, other.top):
            return LnL(*np.unique(np.r_[self.layers, other.layers]))
        else:
            raise ArithmeticError("Can't add up layer pairs that are not adjacent!")
    def __truediv__(self, other):
        if np.isin(other.layers, self.layers).all():
            if other.top == self.top:
                return LnL(*np.setdiff1d(self.layers, other.layers), other.bot)
            else:
                return LnL(*np.setdiff1d(self.layers, other.layers), other.top)
        else:
            raise ArithmeticError("Can't subtract layer pairs that are not present!")
    def __mod__(self, other):
        try:
            left  = (other in self.layers.tolist())
            right = (1 in self.layers.tolist())
            
            assert left | right
        except AssertionError:
            raise NotImplementedError("LnL object must be en an extreme, module is not implemented for intermediate layer groups")
        if other >= self.top:
            missing = set(range(1,other + 1)).difference(set(self.layers))
            
            if left:
                return LnL(*missing.union({self.bot}))
            else:
                return LnL(*missing.union({self.top}))
        else:
            return 0

    def __iter__(self):
        return iter(self.layers.tolist())

    def __len__(self):
        return len(self.layers)
    
    def complete(self, seg : 'Iterable', module = None):
        if module is None:
            module = self.top
        comp = self % module
        for l in comp[:-1]:
            seg = np.insert(seg, module - l, -1)
        
        return seg.astype(int)

    def __getitem__(self, key):
        return self.layers[key]    
    
class LwL(LnL):
    def __init__(self,*layers):
        if len(layers) != 2:
            raise ValueError(f"lwlwl expected 2 layers, {len(layers)} were given")
        super().__init__(*layers)

    
class LwLwL(LnL):
    def __init__(self,*layers):
        if len(layers) != 3:
            raise ValueError(f"lwlwl expected 3 layers, {len(layers)} were given")
        super().__init__(*layers)
    