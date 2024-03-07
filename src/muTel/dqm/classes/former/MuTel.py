import threading
import logging
import sys

from muTel.utils.threading import ThreadLoop
from muTel.dqm.classes.MuData import MuData, MuSL


class MuTelType(type):
    def __repr__(self):
        return self.__name__

class MuTel(object,metaclass=MuTelType):
    """
    Una clase que representa el telescopio entero.

    Attributes
    ----------
    - data : muTel.dqm.classes.MuData
        Medidas del detector que se quieren reconstruir. Cuando se le asigna un valor,
        crea cada una de las superlayers.
    
    - sl : dict(int : muTel.dqm.classes.SLRecon)
        Diccionario que contiene todos los superlayers del telescopio. Se le puede asignar
        una lista para crear las superlayers indicadas.
    
    - fit_4hits : bool
        Valor que indica si debe ajustar las trazas de 4 hits.
    
    - fit_3hits : bool
        Valor que indica si debe ajustar las trazas de 3 hits.

    - _logger : logging.Logger
        Objeto que lleva el log de la instancia.
    
    - debug : bool
        Indica si se deberían mostrar los mensajes del log por consola.
        
    Methods
    -------
    - fit_traces:
        
    """

    def __init__(self, data : MuData, sl = [1,2,3,4],debug=False):
        #___________________________
        # INICIALIZACIÓN DEL LOGGER
        #‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
        self._logger = logging.Logger('MuTel')
        self._logger.addHandler(logging.StreamHandler(sys.stdout))
        self.debug = debug

        #___________________________________
        # INICIALIZACIÓN DE LAS SUPERLAYERS
        #‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
        self.data = data
        self.sl = sl

        #_________________________________
        # CONFIGURACIÓN DEL AJUSTE
        #‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
        self.fit_4hits = True
        self.fit_3hits = True
    


    #=====================================================================
    # ESTADO DE DEBUG
    #=====================================================================

    @property
    def debug(self):
        return self._debug
    @debug.setter
    def debug(self,val):
        if isinstance(val,bool):
            if val:
                self._logger.setLevel(logging.DEBUG)
            else:
                self._logger.setLevel(logging.CRITICAL)
        elif isinstance(val,int):
            self._logger.setLevel(val)
        else:
            raise TypeError(f'El valor debe ser un booleano y se le ha pasado un {type(val)}')


    #=====================================================================
    # ASIGNACIÓN DE LOS DATOS
    #=====================================================================

    @property
    def data(self):
        return self._data  
    @data.setter
    def data(self,val):
        self._data = val
        if hasattr(self,'_sl'):
            self.sl = self._sl.keys()


    #=====================================================================
    # CREACIÓN DE LAS SUPERLAYERS
    #=====================================================================
    
    @property
    def sl(self):
        return self._sl
    
    @sl.setter
    def sl(self, val : list):
        self._sl = ThreadLoop(val, self._set_sl)
    
    
    def _set_sl(self,key):
        self._logger.debug(f'Creando MuRecon de la SL{key}')
        return self.data.to_SL(key)


    #=====================================================================
    # CONFIGURACIÓN DEL AJUSTE DE LAS TRAZAS
    #=====================================================================
    
    @property
    def fit_4hits(self):
        return self._fit_4hits
    @fit_4hits.setter
    def fit_4hits(self,val):
        if isinstance(val,bool):
            self._fit_4hits = val
        else:
            raise TypeError(f'Sólo pueden usarse valores booleanos y esto es {type(val)}')

    @property
    def fit_3hits(self):
        return self._fit_3hits
    @fit_4hits.setter
    def fit_3hits(self,val):
        if isinstance(val,bool):
            self._fit_3hits = val
        else:
            raise TypeError(f'Sólo pueden usarse valores booleanos y esto es {type(val)}')


    #=====================================================================
    # AJUSTE DE LAS TRAZAS
    #=====================================================================

    @property
    def fits(self):
        if not hasattr(self,'_fits'):
            fits = self.fit_traces()
        
        return self._fits
        
    
    def fit_traces(self, eventnr = None, sample = None, plot=False, redo=False):
        """
        Interfaz de usuario para obtener el ajuste de las trazas. Puede también
        forzar el cálculo de las trazas usando la opción 'redo'.

        Variables
        ---------
        - redo : bool
            Indica si se fuerza el cálculo de las trazas. Si es False, devuelve
            el valor guardado en el objeto MuTel en caso de que ya haya sido
            calculado.

        """

        fits = ThreadLoop(
            self.sl.keys(),
            self.fit_traces_sl,
            n4 = self.fit_4hits, 
            n3 = self.fit_3hits, 
            eventnr = eventnr, 
            sample = sample, 
            plot=plot
        )

        return fits


    def fit_traces_sl(self,key, n4 = True, n3 = True, eventnr = None, sample = None, plot=False):
        return self.sl[key].fit_traces(n4 = n4, n3 = n3, eventnr = eventnr, sample = sample, plot=plot)

        

if __name__ == '__main__':
    pass

