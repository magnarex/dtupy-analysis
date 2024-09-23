import dask.dataframe   as dd
import pandas           as pd

from dtupy_analysis.utils.paths import get_file, data_directory
from dtupy_analysis.dqm.classes.MuSL import MuSL
from .MuFile import MuFile



class MuData:
    __allowed_engines = {'dask'}
    """
    Data handler
    """
    def __init__(self, file : MuFile, engine = 'dask'):
        if engine in self.__allowed_engines:
            self._engine = engine
        else:
            raise ValueError(f"Specified engine is not allowed, please choose one from: {self.__allowed_engines}")

        self._file = file
        
    @property
    def file(self) -> MuFile:
        return self._file
    
    @property
    def data(self) -> "pd.DataFrame | dd.DataFrame":
        return self.file.data
    
    @property
    def df(self) -> pd.DataFrame:
        """
        Return a pandas.DataFrame with the data.
        
        **WARNING:** This is highly discouraged when working with engine "dask" since
        this will load the full dataset into memory. Use with precaution.
        """
        if self.engine == 'dask':
            # WARNING: doing this is highly discouraged.
            return self.ddf.compute()
    
    @property
    def ddf(self) -> dd.DataFrame:
        """
        If engine is "dask", this will return the corresponding dask.DataFrame.
        """
        if self.engine == 'dask':
            return self._ddf
        else:
            return None

    @property
    def engine(self) -> str:
        return self._engine.lower()
    
    @classmethod
    def from_dask(cls, file : MuFile) -> "MuData":
        """
        Load data from MuFile with "dask" as engine. Default uses dask.dataframe.
        """
        return cls(file, engine='dask')

    def get_ssl(self, station, sl):
        """
        Get all data from a (station, sl) pair
        """
        if self.engine == 'dask':
            return MuSL.from_dask(self, station = station, sl = sl)
    
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

