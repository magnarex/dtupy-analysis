import pandas as pd

class FilterType(type):
    def __repr__(self):
        return self.__name__
    pass


class Filter(object,metaclass=FilterType):
    def __init__(self, *args,**kwargs):
        self.__cfg__ = 'filter'
        self.args = args
        self.kwargs = kwargs
    
    def filter(self, target : pd.DataFrame)-> pd.DataFrame:
        '''
        Toma de argumentos el objeto sobre el que se aplica el filtro
        y devuelve el objeto modificado.
        '''
        # return target
        pass
    pass


class Drop(Filter,metaclass=FilterType):
    def __init__(self, cols):
        super().__init__(cols)
        self.cols = cols

    def filter(self,target):
        return target.drop(self.cols,axis=1)


class DropTrigger(Filter,metaclass=FilterType):
    '''
    Descarta el canal del trigger.
    '''

    def __init__(self):
        super().__init__()
    
    def filter(self,target):
        return target[target['channel'] != 0]


class SLStudy(Filter,metaclass=FilterType):
    '''
    Filtra una superlayer según los valores de las otras tres. La función
    f guarda la condición lógica que se le requerirá a los datos.
    '''
    def __init__(self, SL_st, f):
        super().__init__(SL_st, f)
    
    def filter(self,target):
        pass



if __name__ == '__main__':
    # print(type(Drop))
    # print(Drop.__cfg__)
    pass