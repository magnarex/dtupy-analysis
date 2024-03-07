import pandas as pd
from muTel.dqm.classes.MuData import MuData
from muTel.utils.meta import superlayers
import logging

class FilterType(type):
    def __repr__(self):
        return self.__name__
    pass


class Filter(object,metaclass=FilterType):
    __cfg__ = 'filter'
    def __init__(self, *args,**kwargs):
        self.args = args
        self.kwargs = kwargs
    
    def filter(self, target : 'MuData')-> pd.DataFrame:
        '''
        Toma de argumentos el objeto sobre el que se aplica el filtro
        y devuelve el objeto modificado.
        '''
        return target

    pass


class Drop(Filter,metaclass=FilterType):
    def __init__(self, cols):
        super().__init__(cols)
        self.cols = cols

    def filter(self,target):
        drop_cols = set(target.df.columns)-set(self.cols)
        return target.df.drop(drop_cols,axis=1)


class DropTrigger(Filter,metaclass=FilterType):
    '''
    Descarta el canal del trigger.
    '''
    def __init__(self):
        super().__init__()
    
    def filter(self,target):
        df = target.df
        return df[df['channel'] != 0]


class TimeFrame(Filter,metaclass=FilterType):
    def __init__(self,tmin,tmax):
        super().__init__(tmin,tmax)
        self.tmin = tmin
        self.tmax = tmax

    def filter(self,target):
        df  = target.df
        return df[(df['DriftTime'] < self.tmax)&(df['DriftTime'] > self.tmin)]



class SLStudy(Filter,metaclass=FilterType):
    '''
    Filtra una superlayer según los valores de las otras tres. La función
    f guarda la condición lógica que se le requerirá a los datos.
    '''
    def __init__(self, f, sl=None,**kwargs):

        # En caso de indicar una función preestablecida,
        # se carga del diccionario.
        if isinstance(f,str):
            from muTel.utils.conditionals import f_dict
            try:
                self.f = f_dict[f]
            except KeyError:
                print(f'La función {f} no está definida en el diccionario.')
        else:
            self.f = f


        # Superlayer de estudio
        self.sl = sl
        
        # En estos atributos se guardan las variables de la función f.
        self.kwargs = kwargs
        
        super().__init__(sl, f, kwargs)
    
    def filter(self,target):
        return self.f(target,self.sl,**self.kwargs)
    


if __name__ == '__main__':
    # print(type(Drop))
    print(Drop(['a']).__cfg__)
    print(Drop.__cfg__)