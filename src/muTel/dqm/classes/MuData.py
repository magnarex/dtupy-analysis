import muTel.utils.meta as meta
from muTel.utils.data import display_df, data_path

from copy import deepcopy
import logging
import pandas
import pandas as pd
import pyarrow.parquet as pq
import dask.dataframe
import dask.dataframe as dd
import dask.array as da
import numpy as np
import sys
import datetime

from collections.abc import Iterable


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

    def __init__(
            self,
            data        : (dask.dataframe.DataFrame | pandas.DataFrame) = None,
            run         : int                                           = None,
            date        : datetime.datetime                             = None,
            npartitions : int                                           = None,
            debug       : bool                                          = False
        ):
        #_________________________________________________________________
        #                 INICIALIZACIÓN DE LAS PROPIEDADES
        #‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
        self._debug  = debug
        self._run   = run
        self._date  = date


        self._data  = data
        if isinstance(data,dd.DataFrame) and npartitions is not None:
            self.npartitions = npartitions
    
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
        if isinstance(self.data,dd.DataFrame):
            return self.data.to_html()

        elif isinstance(self.data,pd.DataFrame):
            return self.data.iloc[np.r_[0:5, -5:0]].to_html()
        
    
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

        if isinstance(eventnr, int):
            event_list = [eventnr]
        elif isinstance(eventnr, (tuple | list | set)):
            event_list = eventnr
        else:
            raise TypeError(f"Wrong type for {type(eventnr)} in '__getitem__'. Only 'int' or '(tuple | list | set)' are allowed.")
        
        
        if not self.data.EventNr.isin(event_list).compute().any():      
            raise ValueError(f'El evento {eventnr} no existe.')

        
        return self.sample(eventnr=event_list)

    #=====================================================================
    # RUTINAS DE CREACIÓN DE OBJETOS (CLASS LEVEL)
    #=====================================================================

    # Estos son para el formato viejo de Jesús de los datos (csv)

    @classmethod
    def from_jpath(
        cls,
        path    : str,
        run     : int | set    = None,
        clean   : bool          = True,
        debug   : bool          = False
    ) -> 'MuData':
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
        
        df = pd.read_csv(path,index_col='Unnamed: 0')
        if clean: df = MuData.clean(df)

        return cls(df,run=run,debug=debug)
    
    @classmethod
    def from_jrun(
        cls,
        run     : int,
        clean   : bool = True,
        debug   : bool = False
    ) -> 'MuData':
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

        return MuData.from_jpath(
            f'{cls._data_path}/MuonData_{run}.txt',
            run = run,
            clean = clean,
            debug=debug
            )


    # Estos son para el formato nuevo de los datos (parquet)

    @classmethod
    def from_path(
        cls,
        path        : str,
        npartitions : int           = 5,
        clean       : bool          = True,
        debug       : bool          = False
    ) -> 'MuData':
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
        
        ddf = dd.read_parquet(
            path,
            engine='pyarrow',
            index="__null_dask_index__",
            calculate_divisions=True
        ).drop('chamber', axis = 'columns') #Sólo hay una cámara, no aporta información

        meta = pq.read_table(path).schema.metadata

        run = int(meta[b'run'])
        date = meta[b'date'].decode('utf-8')


        if date == 'NA':
            date = None
        else:
            date = datetime.datetime(date)

        if clean:
            ddf = MuData.clean(ddf)

        obj = cls(ddf, npartitions = npartitions, run=run, date = date, debug=debug)

        

        return obj
    
    @classmethod
    def from_datadir(
        cls,
        filename    : str,
        npartitions : int   = 5,
        clean       : bool  = True,
        extension   : str   = 'parquet',
        debug       : bool  = False
    ) -> 'MuData':
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

        path = f'{cls._data_path}/{filename}.{extension}'
        return MuData.from_path(path,npartitions=npartitions,clean=clean,debug=debug)
    
    
    
    @classmethod 
    def from_mortadelo(
        cls,
        stream      : 'stream from Mortadelo',
        run         : int | None                = None,
        date        : datetime.datetime | None  = None,
        debug       : bool                      = False
    ):
        
        df = pd.DataFrame(stream)[['bctr','SL','ly','wi','valid_ro','valid_trg','tdc']]
        df = df.rename(columns = {'bctr' : 'EventNr','SL' : 'sl', 'ly' : 'layer', 'wi' : 'cell', 'tdc' : 'TDCTime'})
        obj = cls(df, run=run, date = date, debug=debug)
        return obj
    #=====================================================================
    # LIMPIADO DE LOS DATOS
    #=====================================================================

    @staticmethod
    def clean(data) -> 'MuData':
        '''
        Función que elimina el canal del Trigger.

        Returns
        -------
        - self : muTel.dqm.classes.MuData
            Devuelve el propio objeto
        '''
        return data[data.channel != 0].astype(meta.data_type_dict)             

    def repartition(self, npartitions = 10):

        ddf = self.ddf

        max_enr = ddf.EventNr.max()
        divs_enr = np.floor(np.arange(npartitions+1)/npartitions*max_enr).astype(np.int32)

        divs_idx = list(map(lambda x: ddf.index.compute()[ddf.EventNr==x].min(), divs_enr))  
        divs_idx[0] = ddf.divisions[0]
        divs_idx[-1] = ddf.divisions[-1]


        return ddf.repartition(divisions=tuple(divs_idx))

    #=====================================================================
    # PROPIEDADES
    #=====================================================================

    @property
    def data(self):
        return self._data

    @property
    def ddf(self) -> dask.dataframe.DataFrame:
        '''
        Propiedad que guarda los datos en un DataFrame. Es read-only.
        '''
        if isinstance(self.data, dd.DataFrame):
            return self.data
        else:
            return

    @property
    def df(self) -> pd.DataFrame:
        '''
        Propiedad que guarda los datos en un DataFrame. Es read-only.
        '''
        if isinstance(self.data, dd.DataFrame):
            return self.data.compute()
        elif isinstance(self.data, pd.DataFrame):
            return self.data
        else:
            return
    
    @df.setter
    def df(self,value):
        if self.ddf is None:
            self._df = value
        else:
            raise ValueError("Can't assign a value to 'MuData.df' because there exists 'MuData.ddf'.")

    @property
    def eventnr(self):
        return np.unique(self.df.index.values).size
    
    @property
    def run(self):
        return self._run
    
    @property
    def npartitions(self):
        if self.ddf is None:
            return None
        else:
            return self.ddf.npartitions
    
    @npartitions.setter
    def npartitions(self,value):
        if self.ddf is None:
            raise AttributeError('No se puede asignar número de particiones porque no es un DataFrame de Dask.')
        else:
            self._data = self.repartition(value)
        

    @property
    def date(self):
        return self._date
    
    @property
    def memory_usage(self):
        prefix_dict = {
            0 : '',
            1 : 'k',
            2 : 'M',
            3 : 'G'
        }
        mem_log = da.log10(self.ddf.memory_usage(deep=True).sum())/3

        return f'{da.power(1000,mem_log-da.floor(mem_log)):.2f} {prefix_dict[int(mem_log)]}B'

    @property
    def debug(self):
        return self._debug



    # =====================================================================
    # RUTINAS DE CREACIÓN DE OBJETOS (OBJECT LEVEL)
    # =====================================================================

    def sample(self, sample_size = None, eventnr = None):
        
        if (sample_size is None) & (eventnr is None):
            raise ValueError('Uno de entre "sample", "eventnr" o "idx debe ser distinto de None.')
        
        elif isinstance(sample_size,int) & (eventnr is None):
            eventnr = np.random.choice(self.ddf.EventNr.unique(),sample_size)
            
        elif isinstance(eventnr,(Iterable | int)) & (sample_size is None):
            pass
        
        elif (not sample_size is None) & (not eventnr is None):
            raise ValueError('No se pueden asignar "sample" y "eventnr" a la vez.')
        
        else:
            raise ValueError('"sample" sólo puede ser None o int.')
        
        return MuData(self.ddf.set_index('EventNr').loc[eventnr].reset_index(), run=self.run, date = self.date, debug=self.debug)
