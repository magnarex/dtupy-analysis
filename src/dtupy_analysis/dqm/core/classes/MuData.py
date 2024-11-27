import asyncio

import dask.dataframe   as dd
import pandas           as pd

from ....utils.paths    import get_file, data_directory
from .MuSL              import MuSL
from .MuFile            import MuFile



class MuData(object):
    __module__ = 'dtupy_analysis.dqm.core'
    __allowed_engines = {'pandas'}
    """
    Data handler
    """
    def __init__(self, file, data : 'pd.DataFrame', engine = 'pandas'):
        if engine in self.__allowed_engines:
            self._engine = engine
        else:
            raise ValueError(f"Specified engine is not allowed, please choose one from: {self.__allowed_engines}")
        
        self._file = file
        
        self._df = data
        
    @property
    def file(self) -> MuFile:
        return self._file
    
    @property
    def data(self) -> "pd.DataFrame | dd.DataFrame":
        if self.engine == 'dask':
            return self.ddf
        elif self.engine == 'pandas':
            return self.df
    
    @property
    def df(self) -> pd.DataFrame:
        """
        Return a pandas.DataFrame with the data.
        """
        return self._df
    
    @property
    def engine(self) -> str:
        return self._engine.lower()
    
    
    @classmethod
    def from_pandas(cls, file : "MuFile") -> "MuData":
        """
        Load data from MuFile with "pandas" as engine.
        """
        
        return cls(file, file.data, engine='pandas')


    def get_ssl(self, station, sl) -> 'MuSL':
        """
        Get all data from a (station, sl) pair
        """
        return MuSL.from_pandas(self, station = station, sl = sl)
    
    # Magic methods to function as a DataFrame
    def __getitem__(self, arg):
        return self.data[arg]
    def __eq__(self, other):
        return self.data.__eq__(other)
    def __ne__(self, other):
        return self.data.__ne__(other)
    def __lt__(self, other):
        return self.data.__lt__(other)
    def __gt__(self, other):
        return self.data.__gt__(other)
    def __le__(self, other):
        return self.data.__le__(other)
    def __ge__(self, other):
        return self.data.__ge__(other)
    def __and__(self, other):
        return self.data.__and__(other)
    def __or__(self, other):
        return self.data.__or__(other)
    
    def __getattr__(self, attr):
        if attr in self.data.columns:
            return self.data[attr]
        else:
            try:
                return getattr(self.data, attr)
            except Exception as e:
                raise e
    
    def _repr_html_(self):
        return self.data._repr_html_()
    
    def pair_ssl(self, st, sl):
        musl = self.get_ssl(st, sl)
        musl.pair_hits()
        return musl
    
    def pair_hits(self):
        musl = []
        for st in self.stations:
            # For every station
            for sl in self[self.station == st].sl.unique():
                musl.append(self.pair_ssl(st, sl))
        return musl
    async def async_process_segments(self):
        tasks = []
        for st in self.stations:
            # For every station
            for sl in self[self.station == st].sl.unique():
                musl = self.get_ssl(st, sl)
                tasks.append(musl.async_make_segments())
        results = await asyncio.gather(*tasks)  # Espera a que todas terminen
        return results
    
    def make_segments(self, f_index = 'log'):
        tasks = []
        for st in self.stations:
            # For every station
            for sl in self[self.station == st].sl.unique():
                musl = self.get_ssl(st, sl)
                tasks.append(musl.make_segments(f_index=f_index))
        return tasks


    def process_segments(self):
        # Ejecuta el bucle de eventos asíncrono
        return asyncio.run(self.async_process_segments())
    
    def pair_ssl(self, st, sl):
        musl = self.get_ssl(st, sl)
        musl.pair_hits()
        return musl
    
    @property
    def superlayers(self):
        return self.sl.unique()
    @property
    def stations(self):
        return self.station.unique()
    
    # Hay que personalizar la serialización porque los objetos ParquetFile no son
    # serializables.
    # Personaliza la serialización para evitar serializar el objeto ParquetFile
    def __getstate__(self):
        # Devuelve el estado sin el objeto `parquet_file`, solo el path
        state = self.__dict__.copy()
        state['_file'] = None  # No serializar el objeto ParquetFile
        state['_file_path'] = self.file.path
        state['_file_bs'] = self.file.batch_size
        state['_file_fo'] = self.file.f_overlap
        return state

    # Personaliza la deserialización para volver a abrir el archivo
    def __setstate__(self, state):
        # Restaurar el estado y volver a abrir el archivo parquet
        self.__dict__.update(state)
        self._file = MuFile(state.pop('_file_path'), batch_size=state.pop('_file_bs'), f_overlap=state.pop('_file_fo'))  # Reabrir el archivo al deserializar
    
    # @staticmethod
    # def _plot_matrix(total_diff, pairs, which_top, which_bot):
    #     fig, ax = plt.subplots(1,1,figsize=(10,10))
    #     cm = mpl.colormaps.get_cmap('viridis_r')
    #     cm.set_bad('k')
    #     artist = ax.matshow(total_diff,cmap=cm)
    #     fig.colorbar(artist,ax = ax)

    #     ax.scatter(
    #             *np.c_[which_bot].T[::-1],
    #             marker='+',
    #             color='r',
    #             linewidth = 1,
    #             label   = 'col min',
    #             zorder = 4,
    #             s = 200
    #         )

    #     ax.scatter(
    #             *np.c_[which_top].T[::-1],
    #             marker='x',
    #             color='r',
    #             linewidth = 1,
    #             label   = 'row min',
    #             zorder = 4,
    #             s = 200
    #         )

    #     ax.scatter(
    #             *np.c_[list(pairs)].T[::-1],
    #             marker='s',
    #             color='r',
    #             linewidth = 1,
    #             label='selected',
    #             # facecolor='none'
    #             zorder = 5,
    #             s = 100
    #         )

    #     fig.legend(loc=2, ncols=3)
    #     fig.tight_layout()
        
    #     return fig, ax

    # @staticmethod
    # def make_test_matrix(N,A,b=1):
    #     diff = (1 - np.eye(N)) * np.random.uniform(0,1,(N,N))
    #     diff -= A*sum([
    #         (np.random.uniform(0,1,(N,N))*np.eye(N,k=k)
    #         +
    #         np.random.uniform(0,1,(N,N))*np.eye(N,k=-k))*np.exp(-b*k)
    #         for k in range(1,N+1)
    #     ],start=np.zeros((N,N)))
        
    #     return diff

# class MuDataBatches(MuData):
#     __allowed_engines = {'pandas'}

#     def __init__(self, mufile : MuFileBatches, engine = 'pandas'):
#         super().__init__(mufile, engine=engine)