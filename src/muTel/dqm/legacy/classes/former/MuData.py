from muTel.utils.meta import *
from muTel.utils.config import load_cfg
from muTel.utils.data import display_df, data_path
from muTel.utils.fitting import fit_hist, fit_model, fit_f_track, f_track


from IPython.display import HTML, display

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.collections import PatchCollection

from scipy.stats import chisquare
import plotly.graph_objs as go
from copy import deepcopy

from collections.abc import Iterable

import lmfit
from lmfit.models import GaussianModel
import logging
import pandas as pd
import numpy as np
import json
import sys
import itertools

_MuData_logger = logging.Logger('MuData')
_MuData_logger.addHandler(logging.StreamHandler(sys.stdout))






class MuDataType(type):
    def __repr__(self):
        return self.__name__
    


#--------------------------------------------------------------------------------------------------------------------------
class MuData(object,metaclass=MuDataType):
    """
    Una clase que representa un conjunto de datos.


    Variables
    ---------
    - df : muTel.dqm.classes.MuData
        Medidas del detector que se quieren reconstruir.
    
    - run : dict(int : muTel.dqm.classes.SLRecon)
        Diccionario que contiene todos los superlayers del telescopio. Se le puede asignar
        una lista para crear las superlayers indicadas.
        
    - debug : bool
        Indica si se deberían mostrar los mensajes del log por consola.
        

    Methods
    -------
    - Métodos de utilidad:
        - copy                  : Produce una copia profunda del objeto.

        - __len__               : Da el número de eventos distintos dentro de los datos.

        - _repr_html_           : Representación del objeto para Jupyter Notebooks.

        - __add__               : Define el comportamiento del operador suma "+".

        - __getitem__           : Define el comportamiento de objeto[int].

    - Generadores de objetos:
        - from_path             : Genera un objeto a partir del path a un archivo.

        - from_run              : Genera un objeto a partir de una run buscando su archivo correspondiente
                                  en el directorio por defecto MuData._data_path ([...]/MuTel/data/).
        
    - Filtrado de los datos:
        - add filter            : Método para añadir filtros basados en muTel.dqm.classes.Filter

        - get_filter_by_type    : Método para obtener los filtros del mismo tipo que se han aplicado a los datos.
    
        - clean                 : Aplica una serie de filtros por defecto para limpiar los datos.
    
    - Manejo de los datos:
        - _get_cells            : Devuelve un DataFrame con la celda activada en cada evento correspondiente al mínimo
                                  tiempo de deriva en cada capa. La usa la propiedad 'cells' para devolver su valor.
        
        - get_drifttimes        : Devuelve el mínimo tiempo de deriva de cada evento por cada supercapa y capa.

        - display_event         : Representación bonita de los datos correspondientes al evento indicado.
    
        
    Properties
    ----------
    - df                        : DataFrame que contiene los datos. Se define en la creación del objeto.

    - run                       : Información sobre la run en la que se tomaron los datos. Se define en la creación del objeto.

    - debug                     : Indica el estado del logger del objeto. Se define en la creación del objeto pero se puede
                                modificar.

    - Nevents                   : Número de eventos distintos dentro de los datos.

    
    Class Attributes
    ----------------
    - _data_path                : Indica el lugar donde se busca por defecto al invocar from_run.

    """
    _data_path = data_path

    def __init__(self,df=None,run=None,debug=False):
        #__________________________________
        # INICIALIZACIÓN DE LAS PROPIEDADES
        #‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
        self.debug = debug
        self._run = run
        self._df = df

        #______________________________
        # INICIALIZACIÓN DE LOS FILTROS
        #‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
        self.filters = {}

    
    #=====================================================================
    # MÉTODOS DE UTILIDAD
    #=====================================================================
    """
    Definimos una serie de métodos que nos van a servir para manejar los objetos generados
    por la clase guardándolos, leyéndolos y duplicándolos.
    """

    def copy(self) -> 'MuData':
        """
        Un simple método que devuelve una copia real del objeto.
        """
        return deepcopy(self)

    def __len__(self) -> int:
        """
        Sobreescribimos el método '__len__' para que al hacer "len(objeto)" nos devuelva en número de eventos
        individuales que hay en los datos. Tiene la misma información que la propiedad Nevents.
        """
        return self.Nevents

    def _repr_html_(self):
        """
        Función para definir el comportamiento que tiene cuando se pinta en un Jupyter Notebook.
        """
        return self.df.head(5).to_html()
    
    def __add__(self : 'MuData', other : 'MuData') -> 'MuData':
        """
        Sobreescribimos el comportamiento que tiene el operador '+' sobre este tipo de objetos. De esta manera,
        al sumar objetos MuData, se crea un objeto nuevo que contiene los datos de los dos. Se crea un nuevo índice
        de EventNr donde acaba el del primero de los objetos.
        """
        if self.run == other.run:
            raise RuntimeWarning(
                'Estás sumando datos con la misma run, no vas a saber identificarlos '
                'después. Sería recomendable que les dieras valores distintos a "run".'
            )
        
        # Cogemos los datos de las dos clases
        self_df = self.df
        other_df = self.df

        # Cogemos los valores de las runs de cada objeto y los metemos como columnas nuevas.
        self_df['run'] = self.run
        other_df['run'] = other.run
        
        last_idx = self_df.index.values.max()           # Número del último evento del primer objeto

        # Le asignamos un nuevo EventNr a cada evento del segundo objeto para que no haya EventNr duplicados
        # cuando juntemos todos los datos.
        other_df = other_df.rename(index = {idx : idx + last_idx + 1 for idx in np.unique(other_df.index.values)})

        new_run = set([self.run,other.run])             # La nueva run será un set (para que no haya duplicados) con las runs
        new_df = pd.concat([self_df,other_df], axis=0)  # Juntamos los datos de los dos una vez cambiado el EventNr y añadida
                                                        # la run como columna
        
        # Creamos un objeto nuevo con estos datos y lo devolvemos como resultado.
        return MuData(new_df,run=new_run)

    def __getitem__(self, eventnr : int):
        """
        Sobreescribe el comportamiento de '__getitem__' para que objeto[eventnr] devuelva la entrada
        de objeto.df correspondiente con el EventNr indicado.

        Variables
        ---------
            - eventnr : Índice entero del evento que se quiere recuperar.
        
        Returns
        -------
            - event   : Una slice de los datos con el EventNr indicado.
        """

        if eventnr not in self.df.index:
            raise ValueError(f'El evento {eventnr} no existe.')
        
        event = self.df.loc[eventnr]
        return event


    #=====================================================================
    # RUTINAS DE CREACIÓN DE OBJETOS (CLASS LEVEL)
    #=====================================================================

    @classmethod
    def from_path(cls, path : str, run : int or set = None, debug : bool = False) -> 'MuData':
        """
        Constructor de objetos a través del path al archivo csv/txt/dat con columnas con encabezado.
        

        Variables
        ---------
        - cls : muTel.dqm.classes.MuData
            Clase del objeto.

        - path : str | FilePath | algo tipo open
            Path al archivo de donde se van a leer los datos.

        - run : int | set
            Información sobre la run de la que se están leyendo los datos. Puede ser un int o un set.

        - debug : bool
            Variable que asigna el estado del logger.
        
        
        Returns
        -------
        - objeto : muTel.dqm.classes.MuData
            Objeto creado con los datos en el path indicado.
        """
        df = pd.read_csv(path).set_index('EventNr')
        return cls(df,run=run,debug=debug).clean()
    
    @classmethod
    def from_run(cls, run : int, clean = True, debug : bool = False) -> 'MuData':
        """
        Constructor de objetos a través del la run, buscando los archivos en el directorio _data_path,
        que por defecto ([...]/MuTel/data/), donde parent es el directorio de instalación del paquete.
        Leerá los archivos que tengan nombre MuonData_{run}.txt.
        

        Variables
        ---------
        - cls : muTel.dqm.classes.MuData
            Clase del objeto.

        - run : int | set
            Run de la cual debe leer los datos.

        - debug : bool
            Variable que asigna el estado del logger.
        
        
        Returns
        -------
        - objeto : muTel.dqm.classes.MuData
            Objeto creado con los datos de la run indicada.
        """

        df = pd.read_csv(f'{cls._data_path}/MuonData_{run}.txt').set_index('EventNr')
        if clean:
            return cls(df,run=run,debug=debug).clean()
        else:
            return cls(df,run=run,debug=debug)

    
    #=====================================================================
    # RUTINAS DE CREACIÓN DE OBJETOS (CLASS LEVEL)
    #=====================================================================

    def to_SL(self, sl : int, clean = True) -> 'MuData':
        """
        
        

        Variables
        ---------
        - cls : muTel.dqm.classes.MuData
            Clase del objeto.

        - run : int | set
            Run de la cual debe leer los datos.

        - debug : bool
            Variable que asigna el estado del logger.
        
        
        Returns
        -------
        - objeto : muTel.dqm.classes.MuData
            Objeto creado con los datos de la run indicada.
        """

        if clean:
            return MuSL(data=self, sl=sl, debug=self.debug).clean()
        else:
            return MuSL(data=self, sl=sl, debug=self.debug)



    #=====================================================================
    # PROPIEDADES
    #=====================================================================

    @property
    def df(self) -> pd.DataFrame:
        '''
        Propiedad que guarda los datos en un DataFrame. Es read-only.
        '''
        return self._df

    @property
    def run(self) -> (int or Iterable):
        '''
        Propiedad que guarda la información sobre la run. Es read-only.
        '''
        return self._run

    @property
    def Nevents(self):
        '''
        Propiedad que devuelve el número de eventos distintos en los datos. Es read-only.
        '''
        return self.eventnr.size
    
    @property
    def eventnr(self):
        '''
        Propiedad que devuelve el número de eventos distintos en los datos. Es read-only.
        '''
        return np.unique(self.df.index)
    
    @property
    def cells(self):
        '''
        Propiedad que devuelve las celdas de los hits que con el menor tiempo de deriva de
        cada capa. Es read-only.
        '''
        return self._get_cells(self.df)
    
    @staticmethod
    def _get_cells(df):
        '''
        Función que usa la propiedad 'cells' para conocer su valor.

        Returns
        -------
        - cells : pd.DataFrame
            DataFrame con las celdas de los hits que con el menor tiempo de deriva de cada capa.
        '''

        # Primero, agrupamos según el EventNr, superlayer y layer
        # En cada uno, escogemos el que tenga menor tiempo de deriva y devolvemos su celda
        # Luego, agrupamos todos los índices (EventNr, sl, layer) y convertimos el tercer nivel
        # de índices (layer) en columnas.
        # Finalmente, eliminamos el tercer nivel de índices, ya que lo acabamos de gastar

        cells = df.groupby(['EventNr','sl','layer'])\
                 .min('DriftTime')[['cell']]\
                 .stack().unstack(level=2)\
                 .droplevel(2)
        
        return cells


    #=====================================================================
    # ESTADO DE DEBUG
    #=====================================================================

    @property
    def debug(self):
        '''
        Propiedad que asigna el estado del logger.
        '''
        return self._debug
    @debug.setter
    def debug(self,val):
        '''
        Función que define la asignación de valores a la propiedad 'debug'. Ajusta el nivel del logger
        según el valor indicado.

        Variables
        ---------
        - val : bool | int
            Nivel al que se tiene que ajustar el logger. True lo pone en DEBUG y False en CRITICAl.
        '''
        if isinstance(val,bool):
            if val:
                _MuData_logger.setLevel(logging.DEBUG)
            else:
                _MuData_logger.setLevel(logging.CRITICAL)
        elif isinstance(val,int):
            _MuData_logger.setLevel(val)
        else:
            raise TypeError(f'El valor debe ser un booleano y se le ha pasado un {type(val)}')
        self._debug = val

    #=====================================================================
    # FILTRADO DE LOS DATOS
    #=====================================================================

    def add_filter(self, new_filter : 'muTel.dqm.classes.Filter', alias : str or None = None):
        '''
        Función que permite añadir un filtro a los datos. Los filtros usados se guardan
        en un atributo dentro de la clase llamado "filters". De este modo, se puede consultar
        el historial de filtros aplicados.

        Variables
        ---------
        - new_filter : muTel.dqm.classes.Filter
            Filtro nuevo que aplicar a los datos.
        
        - alias : str
            Nombre con el que agregarlo al diccionario de los filtros.
        '''


        
        new_filter_type = type(new_filter).__name__             # Cogemos el tipo de filtro que se va a aplicar
        _MuData_logger.debug(
            f' Añadiendo filtro {new_filter_type} a un objeto {type(self).__name__}'
        )
        
        if alias is None:
            # Si no tiene alias, vamos a ver cuántos filtros de este tipo se han aplicado ya y crear
            # un alias con el nombre del tipo de filtro y su ordinal.
            filter_types = [type(filter_i).__name__ for filter_i in self.filters.values()]
            
            _MuData_logger.debug(
                f' Filtros existentes:'+
                ''.join(
                    [
                        f'\n\t{alias_i}\t\t:\t{filter_types[i]}'
                        for i, alias_i in enumerate(self.filters.keys())
                    ]
                )
            )
            
            # Contamos el número de filtros del mismo tipo
            Nfits = sum([
                    1 
                    if filter_type_i == new_filter_type
                    else 
                    0 
                    for filter_type_i in filter_types
            ]) 

            _MuData_logger.debug(
                f' Existen ya {Nfits} filtros {new_filter_type}'
            )

            # Creamos el alias
            alias = f'{new_filter_type}_{str(Nfits+1).zfill(2):2}'

            _MuData_logger.debug(
                f' Guardando el nuevo filtro {new_filter_type} '
                f'bajo el alias {alias}'
            )

        else:
            # Si se ha especificado un alias, lo aplicamos directamente y nos aseguramos de
            # que sea un string.
            alias = str(alias)

        
        self.filters[alias] = (new_filter)          # Guardamos el filtro en el diccionario.
        self._df = new_filter.filter(self)          # Aplicamos el filtro a los datos y los sobreescribimos.

        _MuData_logger.debug(
            '\n\n\n'
        )

    def get_filter_by_type(self, filter_type : str):
        '''
        Función que permite tomar los filtros aplicados según su tipo. Devuelve todos los filtros
        del tipo indicado.

        Variables
        ---------
        - filter_type : str
            Nombre del tipo de filtro que se desea buscar.
        
            
        Returns
        -------
        - filters : list[muTel.dqm.classes.Filters]
        '''
        filters = [
                    filter                                      # Devuelve el filtro
                    for filter in self.filters.values()         # Para cada filtro en el diccionario de filtros
                    if filter_type == type(filter).__name__     # Si el nombre de su tipo de filtro coincide con el indicado.
                ]
        
        return filters


    #=====================================================================
    # SELECCIÓN DE EVENTOS
    #=====================================================================

    def display_event(self, eventnr : int, t_min : float = 0, t_max : float = 1500) -> None:
        '''
        Función que muestra un DataFrame con un formato bonito conteniendo los datos del evento indicado.
        Está formateado para ser visto dentro de un Jupyter Notebook.

        Variables
        ---------
        - eventnr : int
            Número del evento que se desea mostrar.
        
        - t_min : float
            Tiempo mínimo que se espera que tengan los datos para la representación de las barras.

        - t_max : float:
            Tiempo máximo que se espera que tengan los datos para la representación de las barras.
        '''
        

        # Se toman los datos del evento indicado.
        # Descartamos las columnas ['GEO','hit','channel','EventNr'].
        # Ordenamos los datos por sl, layer y celda.
        # Le damos formado a los tiempos de deriva.
        # Escondemos el índice.
        # Mostramos barras indicando el tiempo de deriva de cada
        # celda según los límites pasados a la función
        # Le ponemos un título que indique el evento seleccionado.
        # Centramos los textos de todas las celdas.
        styled_df = self[eventnr]\
            .drop(['GEO','hit','channel','EventNr'], axis='columns')\
            .sort_values(['sl','layer','cell'])\
            .style.format({
                'DriftTime' : '{:20.2f} ns'
            })\
            .hide(axis="index")\
            .bar(
                subset=["DriftTime",],
                color='#8e82fe',
                vmin = t_min,
                vmax = t_max
            )\
            .set_caption(f'<b><p style="font-size:20px">Event {eventnr}</p></b>')\
            .set_table_styles([dict(selector='th', props=[('text-align', 'center')])])

        display(styled_df)
        return

    def get_drifttimes(self, sl : int, layer : int) -> pd.DataFrame:
        '''
        Función que devuelve los tiempos de deriva de todos los hits de una superlayer y layer para cada evento.

        Variables
        ---------
        - sl : int
            Superlayer donde se quieren tomar los tiempos de deriva.
        
        - layer : int
            Layer de la superlayer donde se toman los tiempos de deriva.
        '''

        # Cogemos los datos
        df = self.df

        # Seleccionamos la superlayer y layer y tomamos los tiempos de deriva
        # Renombramos la columna como t{layer}.
        dt_layer = df[(df['sl'] == sl) & (df['layer'] == layer)]['DriftTime']\
                   .rename(f't{layer}')
        return dt_layer


    #=====================================================================
    # LIMPIADO DE LOS DATOS
    #=====================================================================

    def clean(self, config : dict = {}) -> 'MuData':
        '''
        Función que aplica dos filtros para limpiar los datos. El primero, quita las columnas que no vamos
        a usar. El segundo, elimina el canal del Trigger.

        Variables
        ---------
        - config : dict
            Diccionario con la configuración que usa cada filtro. Por defecto, está vacío, así que
            los filtros cargan la configuración definida por defecto en el archivo de configuración
            de los filtros ([...]/MuTel/src/MuTel/dqm/config/filters.json)
        
        Returns
        -------
        - self : muTel.dqm.classes.MuData
            Devuelve el propio objeto
        '''
        from muTel.dqm.classes.Filters import Drop, DropTrigger

        # Aplicamos el filtro que elimina las columnas irrelevantes
        self.add_filter(load_cfg(Drop,config_name=config.get('Drop','default')))

        # Aplicamos el filtro que elimina el canal del Trigger
        self.add_filter(load_cfg(DropTrigger))

        return self



#--------------------------------------------------------------------------------------------------------------------------
class MuSL(MuData,metaclass=MuDataType):
    """
    Una clase que representa una superlayer del detector. Es una subclase de muTel.dqm.classes.MuData.


    Variables
    ---------
    - df : muTel.dqm.classes.MuData
        Medidas del detector que se quieren reconstruir.
    
    - run : dict(int : muTel.dqm.classes.SLRecon)
        Diccionario que contiene todos los superlayers del telescopio. Se le puede asignar
        una lista para crear las superlayers indicadas.

    - sl : int
        Superlayer que va a representar el objeto.
        
    - debug : bool
        Indica si se deberían mostrar los mensajes del log por consola.
        

    Methods
    -------
    - Métodos de utilidad:
        - copy                  : Produce una copia profunda del objeto.

        - __len__               : Da el número de eventos distintos dentro de los datos.

        - _repr_html_           : Representación del objeto para Jupyter Notebooks.

        - __add__               : Define el comportamiento del operador suma "+".

        - __getitem__           : Define el comportamiento de objeto[int].

    - Generadores de objetos:
        - from_path             : Genera un objeto a partir del path a un archivo.

        - from_run              : Genera un objeto a partir de una run buscando su archivo correspondiente
                                  en el directorio por defecto MuData._data_path ([...]/MuTel/data/).
        
    - Filtrado de los datos:
        - add filter            : Método para añadir filtros basados en muTel.dqm.classes.Filter

        - get_filter_by_type    : Método para obtener los filtros del mismo tipo que se han aplicado a los datos.
    
        - clean                 : Aplica una serie de filtros por defecto para limpiar los datos.
    
    - Manejo de los datos:
        - _get_cells            : Devuelve un DataFrame con la celda activada en cada evento correspondiente al mínimo
                                  tiempo de deriva en cada capa. La usa la propiedad 'cells' para devolver su valor.
        
        - get_drifttimes        : Devuelve el mínimo tiempo de deriva de cada evento por cada supercapa y capa.

        - display_event         : Representación bonita de los datos correspondientes al evento indicado.
    
        
    Properties
    ----------
    - df                        : DataFrame que contiene los datos. Se define en la creación del objeto.

    - run                       : Información sobre la run en la que se tomaron los datos. Se define en la creación del objeto.

    - debug                     : Indica el estado del logger del objeto. Se define en la creación del objeto pero se puede
                                modificar.

    - Nevents                   : Número de eventos distintos dentro de los datos.

    
    Class Attributes
    ----------------
    - _data_path                : Indica el lugar donde se busca por defecto al invocar from_run.

    """



    def __init__(self, data = None, sl = None, debug = False):
        super().__init__(df = data.df, run = data.run)
        self.debug = debug
        self._sl = sl
        self._src = data

        self.set_timebox()
        self.T0_corr = 0
        self.Tmax_corr = 0


    # =====================================================================
    # SUPER CHARGING
    # =====================================================================


    def __getitem__(self, eventnr : int):
        
        if eventnr not in self.df.index:
            raise ValueError(f'El evento {eventnr} no existe.')

        item = self.df.loc[eventnr]


        return item[item.sl == self.sl].drop('sl', axis=1).sort_values(['layer','cell','DriftTime'])



    # =====================================================================
    # RUTINAS DE CREACIÓN DE OBJETOS (CLASS LEVEL)
    # =====================================================================

    @classmethod
    def from_path(cls, path : str, sl : int, run : int = None, debug: bool = False) -> 'MuData':
        data = MuData.from_path(path,run,debug=debug)
        return cls(data = data, sl = sl, debug= debug).clean()
    
    @classmethod
    def from_run(cls, run : int, sl : int, debug : bool = False) -> 'MuData':
        """
        Lee los archivos desde el path por defecto a los datos (MuData._data_path).
        """
        data = MuData.from_run(run, debug=debug)
        return cls(data = data, sl = sl, debug= debug).clean()



    # =====================================================================
    # RUTINAS DE CREACIÓN DE OBJETOS (OBJECT LEVEL)
    # =====================================================================

    def sample(self, sample = None, eventnr = None, idx = None):
        
        if (sample is None) & (eventnr is None) & (idx is None):
            raise ValueError('Uno de entre "sample", "eventnr" o "idx debe ser distinto de None.')
        
        elif isinstance(sample,int) & (eventnr is None) & (idx is None):
            idx = np.random.choice(self.eventnr,size=sample)
        
        elif isinstance(eventnr,Iterable) & (sample is None) & (idx is None):
            isin = np.isin(eventnr,self.eventnr)
            idx = eventnr[isin]
        elif isinstance(eventnr,int) & (sample is None) & (idx is None):
            idx = eventnr
        
        elif isinstance(idx,(int,Iterable)) & (sample is None) & (eventnr is None):
            idx = self.eventnr[idx]
        
        elif (not sample is None) & (not eventnr is None):
            raise ValueError('No se pueden asignar "sample" y "eventnr" a la vez.')
        
        else:
            raise ValueError('"sample" sólo puede ser None o int.')
                
        df = self.df.loc[idx]
        return MuSL(MuData(df,run=self.run), sl=self.sl, debug=self.debug).clean()


    # =====================================================================
    # SUPERLAYER
    # =====================================================================

    @property
    def sl(self):
        return self._sl
    @sl.setter
    def sl(self,val):
        self._sl = val

    @property
    def run(self):
        return self._src.run

    @property
    def cells(self):
        return self._get_cells(self.df).loc[:,self.sl,:]
    
    @property
    def df(self):
        return self._src.df

    
    @property
    def src(self):
        return self._src

    # =====================================================================
    # TIMEBOX
    # =====================================================================

    @property
    def T0(self):
        return self._T0 + self._T0_corr * self._dT0
    @T0.setter
    def T0(self,val):
        self._T0 = val
    
    @property
    def dT0(self):
        return self._dT0
    @dT0.setter
    def dT0(self,val):
        self._dT0 = val
    
    @property
    def T0_corr(self):
        return self._T0_corr
    @T0_corr.setter
    def T0_corr(self,val):
        self._T0_corr = val
    

    @property
    def Tmax(self):
        return self._Tmax + self._Tmax_corr * self._dTmax
    @Tmax.setter
    def Tmax(self,val):
        self._Tmax = val
    
    @property
    def dTmax(self):
        return self._dTmax
    @dTmax.setter
    def dTmax(self,val):
        self._dTmax = val
    
    @property
    def Tmax_corr(self):
        return self._Tmax_corr
    @Tmax_corr.setter
    def Tmax_corr(self,val):
        self._Tmax_corr = val

    def set_timebox(self, **kwargs):
        T0_fit, Tmax_fit = self.fit_timebox(self.df['DriftTime'],**kwargs)

        self.T0  = T0_fit.params['center']
        self.dT0 = T0_fit.params['sigma']

        self.Tmax  = Tmax_fit.params['center']
        self.dTmax = Tmax_fit.params['sigma']

        return T0_fit, Tmax_fit

    @staticmethod
    def fit_timebox(dts, model = GaussianModel, bins = 80, T0_range = (600,800), Tmax_range=(1000,1200)):
        # dts = self.df['DriftTime']
        cts, edges = np.histogram(dts, bins=bins, range = (T0_range[0],Tmax_range[1]))
        mids = (edges[1:]+edges[:-1])/2
        dcts = np.gradient(cts,edges[1]-edges[0])

        hist_range = (T0_range[0],Tmax_range[1])
        T0_fit = fit_model(
            mids,
            dcts,
            hist_range=hist_range,
            fit_range=T0_range,
            par_range=T0_range,
            model = model()
        )

        Tmax_fit = fit_model(
            mids,
            dcts,
            hist_range=hist_range,
            fit_range=Tmax_range,
            par_range=Tmax_range,
            model = model()
        )


        return T0_fit, Tmax_fit

    # =====================================================================
    # TIEMPOS DE DERIVA
    # =====================================================================

    @property
    def drifttimes(self):
        if not hasattr(self,'_drifttimes'):
            self._drifttimes = self._set_drifttimes()
        dts = self._drifttimes
        vals = np.ma.array(dts.values, mask=np.isnan(dts.values))
        cut = (self.T0 <= vals).all(axis=1) & (vals <= self.Tmax).all(axis=1)
        return dts.iloc[cut]

    def _set_drifttimes(self):
        t_list = [self.get_drifttimes(i).to_frame() for i in layers]
        df = pd.concat(t_list,axis=1)
        # df['nhits'] = len(layers) - df.isna().sum(axis=1)
        df = df.reindex(sorted(df.columns,reverse=True), axis=1)
        return df
   
    def get_drifttimes(self, layer : int, all : bool = False) -> pd.DataFrame:
        dt = super().get_drifttimes(self.sl, layer)
        if all:
            return dt
        else:
            return dt.groupby('EventNr').min()

    
    # =====================================================================
    # MEANTIMERS
    # =====================================================================

    @property
    def mt1(self):
        if not hasattr(self,'_mt1'):
            self._set_meantimers()
        return self._mt1
    
    @property
    def mt2(self):
        if not hasattr(self,'_mt2'):
            self._set_meantimers()
        return self._mt2
    
    @property
    def meantimers(self):
        if not hasattr(self,'_meantimers'):
            self._set_meantimers()

        
        df = pd.concat([self.mt1,self.mt2],axis=1)
        df.columns = ['MT1','MT2']
        return df
    
    def _set_meantimers(self):
        dts = self.drifttimes
        self._mt1 = (dts.t1 + dts.t3)/2 + dts.t2 - 2*self.T0
        self._mt2 = (dts.t2 + dts.t4)/2 + dts.t3 - 2*self.T0

    
    # =====================================================================
    # VELOCIDAD DE DERIVA
    # =====================================================================

    @property
    def vdrift(self):
        if not hasattr(self,'_vdrift'):
            self._set_vdrift()
        return self._vdrift
    
    @vdrift.setter
    def vdrift(self,val):
        self._vdrift = val
    
    @property
    def dvdrift(self):
        if not hasattr(self,'_vdrift'):
            self._set_vdrift()
        return self._dvdrift
    
    @dvdrift.setter
    def dvdrift(self,val):
        self._dvdrift = val
    
    def _set_vdrift(self,range=(300,450),plot=False):
        
        fit_mt1 = fit_hist(
            self.mt1,
            hist_range=range,
            model = GaussianModel(),
            plot=plot
        )
        mt1 = fit_mt1.params['center']
        dmt1 = fit_mt1.params['sigma']

        fit_mt2 = fit_hist(
            self.mt2,
            hist_range=range,
            model = GaussianModel(),
            plot=plot
        )
        mt2 = fit_mt2.params['center']
        dmt2 = fit_mt2.params['sigma']

        mts = np.r_[mt1,mt2]
        dmts = np.r_[dmt1,dmt2]
        w = 1/dmts**2
        mt_mean = np.sum(mts*w)/np.sum(w)
        dmt_mean = np.sqrt(1/np.sum(w))
        

        vdrift = cell_width/2/mt_mean
        dvdrift = vdrift*dmt_mean/mt_mean

        self.vdrift = vdrift
        self.dvdrift = dvdrift

        return vdrift, dvdrift
    
    
    # =====================================================================
    # RECONOCIMIENTO DE PATRÓN DE INCIDENCIA
    # =====================================================================

    @property
    def pattern(self):
        if not hasattr(self,'_pattern'):
            self._pattern = self._calc_pattern()
        return self._pattern
    
    def _calc_pattern(self,sample=None):
        where3n4 = self.drifttimes.notna().sum(axis=1) >= 3

        cells = self.cells.loc[where3n4.index[where3n4]]
        cells.columns = [len(layers)*['layer'], [int(i) for i in cells.columns.values]]
        cells = cells.reindex(sorted(cells.columns,reverse=True), axis=1)
        cells['ref_layer'] = cells['layer'].notna().idxmax(axis=1)

        if sample is None:
            pass

        elif isinstance(sample, int):
            cells = cells.sample(sample)

        else:
            raise ValueError('"sample" sólo puede ser None o int.')
        
        return cells.apply(self._layers_to_pattern,axis=1).dropna()
        
    @staticmethod
    def _layers_to_pattern(ser):
    
        # Según la existencia de un hit en la capa 4, escogemos la capa de referencia.
        # Calculamos la celda de referencia.
        # Calculamos las posiciones relativas de los hits con respecto al superior.
        cells = ser['layer'][[4,3,2,1]].to_numpy()
        nhits = (~np.isnan(cells)).sum()
        if nhits == 4:
            where_nan = 0
        elif nhits == 3:
            where_nan = 4-np.isnan(cells).argmax()
        else:
            return

        
        ref_layer = int(ser['ref_layer'])
        ref_cell = int(ser['layer'][ref_layer])
        rel_cells = cells - ref_cell
        diff = 2*np.diff(cells) + np.r_[ 1,-1, 1]
        diff = np.where(np.isnan(diff),0,diff).astype(int)

        
        
        # Con la ayuda de este diccionario, sabemos si se encuentran a la dcha o izda
        # de la capa anterior.

        pattID = MuSL._diff_to_ID(diff)
        
        if '#' in pattID:
            return
        

        result = pd.Series(
            [nhits, ref_layer,ref_cell,pattID,rel_cells,where_nan],
            index = ['nhits','ref_layer','ref_cell','pattID','rel_cells','mis_cell']
        )
        return result
    
    @staticmethod
    def _diff_to_ID(diff):
            ID = ''
            for i in diff:
                if i == -1.:
                    ID += 'L'
                elif i == 1.:
                    ID += 'R'
                elif i == 0:
                    ID += 'X'
                else:
                    ID +='#'
            return ID


    # =====================================================================
    # AJUSTE DE LAS TRAZAS
    # =====================================================================

    @staticmethod
    def get_lat(pattID,mis_cell):
        if 'X' in pattID:
            mis_idx = int(4 - mis_cell)
            lat_list = [lat_i[:mis_idx] + 'X' + lat_i[mis_idx+1:] for lat_i in all_lats]
            lat_list = list(np.unique(lat_list))
        else:
            lat_list = patt_dict[pattID]['lats']
        
        # print(lat_list)
        return lat_list

    @property
    def results(self):
        if not hasattr(self,'_fits'):
            patt = self.pattern
            idx = list(set(patt.index).intersection(self.meantimers.index))
            mts  = self.meantimers.loc[idx]
            dts  = self.drifttimes.loc[idx]
            return pd.concat([dts,mts,patt.loc[idx]],axis=1)
        else:
            fits = self.fits
            patt = self.pattern
            recon = pd.concat([patt,fits],axis=1).dropna()
            idx = list(set(recon.index).intersection(self.meantimers.index))
            mts  = self.meantimers.loc[idx]
            dts  = self.drifttimes.loc[idx]
            return pd.concat([dts,mts,recon.loc[idx]],axis=1)
        
    @property
    def fits(self):
        if not hasattr(self,'_fits'):
            self._fits = self._calc_fits(self.results)
        return self._fits

    def fit_traces(self, n4 = True, n3 = True, eventnr = None, sample = None, plot=False):
        
        if (sample is None) & (eventnr is None):
            results = self.results
        else:
            results = self.sample(sample=sample,eventnr=eventnr).results
        
        fits = self._calc_fits(results, n4=n4, n3=n3, plot=plot)

        if (sample is None) & (eventnr is None):
            self._fits = fits

        return fits
        
    def _calc_fits(self, results, n4 = True, n3 = True, plot=False):
        where = np.zeros_like(results.index)
        if n4: where += (results.nhits == 4)
        if n3: where += (results.nhits == 3)
        idx = where.index[where == 1]
        
        results = results.loc[idx]

        fits = results.apply(lambda ser: self._fit_trace(ser,plot=plot), axis=1)
        return fits

    def _fit_trace(self,ser,plot=False,log=True):
        # Fit Best Track

        # Usamos la función "get_lat" para obtener todas las posibles lateralidades para un 
        # patrón de incidencia
        lat_list = self.get_lat(ser['pattID'],ser['mis_cell'])    

        # Consultamos el diccionario que identifica las lateralidades con sus coeficientes
        # Usamos stack para ponerlo como una matriz con tantas filas como lateralidades posibles
        # y tantas columnas como capas (l4,l3,l2,l1).
        coef = np.stack(list(map(
            lambda x: lat_dict[x],
            [lat_i.replace('X','R') for lat_i in lat_list]
        )))


        vdrift = self.vdrift
        n_rel = np.array(ser['rel_cells'])  # Están en orden inverso (l4,l3,l2,l1)
        n_ref = ser['ref_cell']
        t = ser[['t4','t3','t2','t1']].to_numpy() - self.T0
        sl_ref_height = sl_height[-self.sl]


        # Diferentes posiciones según las posibles lateralidades
        x_array = layer_offset + cell_width*(n_ref + n_rel) + coef*t*vdrift
        x_array = x_array.astype(float)
        is_nan = np.isnan(x_array).any(axis=0)
        y_i = wire_height + sl_ref_height
        
        if log:
            logging.debug(
                f'\n#===============================#'+
                f'\n|  Ajustando EventNr{ser.name}  |'+
                f'\n#===============================#'+
                f'\n\tVelocidad: {vdrift*1e3:.2f} μm/ns'+
                f'\n\tPatrón de incidencia: {ser.pattID}'+
                f'\n\tPosibles lateralidades:'+
                '\n\t\t'.join(lat_list)+
                f'\nCelda de incidencia: {n_ref}'
            )
        
              
        result_list = []
        stat_list = []
        
        if plot:
            fig,axes = plt.subplots(coef.shape[0],1,figsize=(13,2*coef.shape[0]),sharex=True)
            # fig.subplots_adjust(0,0,1,1,0,0)
            axes = axes.ravel()
        for i,x_i in enumerate(x_array):
            if plot:
                    ax_i = axes[i]
                    ax_i.set_title(f'{lat_list[i]}')
                    ax_i.set_ylim(wire_height.min()-cell_height/2-sheet_height,wire_height.max()+cell_height/2+sheet_height)
                    ax_i.set_xlim(0,(ncells+0.5)*cell_width)

                    # ax_i.set_xlim((n_ref-2)*cell_width,(n_ref+1)*cell_width)
                    ax_i.plot(
                            x_i,
                            wire_height,
                            linestyle='none',
                            marker='x',
                            color='xkcd:bright blue',
                            zorder = 1
                        )
                    self.plot_cells(ax=ax_i,ref_cells = n_ref+n_rel)
            try:
                args_i, r2_i, chi2_i = fit_f_track(x_i[~is_nan],y_i[~is_nan])   
                res_i = x_i-f_track(y_i,*args_i)
                result_list.append([*args_i,r2_i,chi2_i,res_i])
                stat_list.append(chi2_i)

                if plot:
                    
                    y_plt = np.linspace(wire_height.min()-cell_height,wire_height.max()+cell_height,21)
                    ax_i.plot(f_track(y_plt,*args_i),y_plt,linestyle='dashed')
                
            except RuntimeError:
                return
            if log:
                logging.debug(
                    f'\n'+
                    f'\n‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾'+
                    f'\n\t\t\tLATERALIDAD: {lat_list[i]}'+
                    f'\n____________________________________________________'+
                    f'\n\tR2:   {r2_i:.2f}'+
                    f'\n\tchi2: {chi2_i:.2f}'
                )
        iloc = np.argmin(stat_list)

        if plot:
            # fig.suptitle(f'Combinación escogida: {lat_list[iloc]}')
            axes[iloc].set_title(f'{lat_list[iloc]}',fontweight='bold')
            plt.xlabel('Position (mm)',fontsize=14)
            plt.show()
        
        if log:
            logging.debug(
                f'\n'+
                f'\n____________________________________________________'+
                f'\n\t\t\tLATERALIDAD ESCOGIDA: {lat_list[iloc]}'+
                f'\n‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾'+
                f'\n\tR2:   {result_list[iloc][-3]:.2f}'+
                f'\n\tchi2: {result_list[iloc][-2]:.2f}'+
                f'\n\n'
            ) 
        return pd.Series([*result_list[iloc],lat_list[iloc]],index=['theta','x0','r2','chi2','res','latID'])

    @staticmethod
    def plot_cells(ax : plt.Axes = None,ref_cells=None,offset=0):
        ax.grid(False)
        cell_linestyle = dict(
            lw=0.5,
            color='k',
            zorder=-1
        )

        wire_linestyle = dict(
            marker='o',
            linestyle='none',
            color='k',
            zorder=-1,
            markersize=3
        )

        if ax is None:
            ax = plt.gca()


        # Pintamos las líneas que separan cada layer
        ax.set_xlim(-10,(ncells+0.5)*cell_width+10)
        cell_heights = np.r_[2*cell_height,wire_height-0.5*cell_height]
        # ax.set_ylim(cell_heights[-1]-sheet_height,0)
        ax.hlines(
            cell_heights,
            xmin = cell_width*np.r_[0,0,0,0,0.5],
            xmax = cell_width*(ncells + np.r_[0,0.5,.5,.5,.5]),
            **cell_linestyle
        )


        # Calculamos la posición de las paredes de las celdas y los hilos
        x_cell = (np.arange(ncells+1))*cell_width + layer_offset.reshape(4,1) + 0.5*cell_width
        x_wire = x_cell[:,:-1] + 0.5*cell_width
        
        # Pintamos la posición de los hilos
        ax.plot(
            x_wire,
            wire_height - offset,
            **wire_linestyle,
            )

        # Pintamos las paredes de las celdas
        ax.vlines(
                x_cell,
                ymin=np.ones_like(x_cell) * cell_heights[:-1].reshape(4,1) - offset,
                ymax = np.ones_like(x_cell) * cell_heights[1:].reshape(4,1) - offset,
                **cell_linestyle
            )

        # Coloreamos las celdas para distinguirlas del fondo
        patch_list = []
        x_patch = x_wire.ravel() - 0.5*cell_width
        y_patch = (np.ones_like(x_wire)*wire_height.reshape(4,1) - 0.5*cell_height).ravel() - offset
        for xy_p in zip(x_patch,y_patch):
            patch_list.append(patches.Rectangle(
                xy_p,
                cell_width,
                cell_height,
            ))
        ax.add_collection(PatchCollection(patch_list,fc = 'xkcd:ecru',zorder=-3))

        #Coloreamos las celdas activadas de otro color
        if not ref_cells is None:
            x_cell = layer_offset + cell_width*ref_cells - 0.5*cell_width
            y_cell = wire_height - 0.5*cell_height - offset
            xy_cells = zip(x_cell.ravel(),y_cell.ravel())
            patch_list = []
            for xy_cell in xy_cells:
                patch_list.append(patches.Rectangle(
                        xy_cell,
                        cell_width,
                        cell_height,
                    ))
            ax.add_collection(PatchCollection(patch_list, fc='xkcd:amber',zorder=-2))
        
        
        
        return
    

    #=====================================================================
    # ESTADÍSTICA DE LAS LATERALIDADES
    #=====================================================================

    @property
    def lat_hist(self):
        return self._calc_patt_lat(self)

    @staticmethod
    def _calc_patt_lat(sl):
        fits = sl.fits
        patt = sl.pattern
        recon = pd.concat([patt,fits],axis=1).dropna()
        gb = recon.groupby('pattID').apply(lambda grp: [grp[grp['latID'] == lat_i].index.size for lat_i in all_lats])
        return gb.to_dict()


# if __name__ == '__main__':
    # from muTel.dqm.classes.filters import Drop
    # from muTel.utils.config import load_cfg
    # muon_data = MuData(588,sl=1,nhits=4)
    # display(muon_data)
    # muon_data.add_filter(load_cfg(Drop))