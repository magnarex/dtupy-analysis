import pathlib

from ...utils.paths import load_yaml, config_directory

class Config:
    """
    This is a wrapper for a `dict` whose values may be accessed as if the keys were also
    attributes.
        
    Methods
    -------
    get(key, default=None)
        Proxy for `cfg.get`(key, default)
    
    Note
    ----
    Items in `cfg` may also be accessed as ``Config.<key>`` or ``Config[<key>]`` or using
    the method `get(<key>)`.
    
    Take in mind this has been achieved adding some syntactic sugar to the built-in
    methods `__getitem__` and `__getattr__`, this last one being called only if
    `__getattribute__` fails.
    
    Examples
    --------
    >>> foo = {'a' : 1, 'b' : 2}
    >>> bar = Config(foo)
    >>> print(bar.a)
    1
    >>> print(bar['b'])
    2
    >>> print(bar.get('c'))
    None
    >>> print(bar.get('c', default=3))
    3
    """
    
    def __init__(self, cfg : 'dict[str, ]'):
        """
        Parameters
        ----------
        cfg : dict
            Dictionary to wrap.
        """
        self._cfg = cfg
        
    def values(self):
        """
        Get `cfg.values()`.
        """
        return self.cfg.values()
    def keys(self):
        """
        Get `cfg.keys()`.
        """
        return self.cfg.keys()
    
    def get(self, key, default = None):
        """
        Gets an the value of `key` with `default` as default. Wrapper for `cfg.get`.
        
        Parameters
        ----------
        key : str
            Key of the item in `cfg`.
        default : Any, optional
            Default value that will be returned in case `key` doesn't exist.
        
        Returns
        -------
        value : Any
            Value corresponding to the item with `key` as key or `default` in case `key`
            doesn't exist in `cfg`.
        """
        if key is None: key = str(key) # Fix to prevent from crashing with key == None
        
        if key in self.keys():
            return self.cfg[key]
        else:
            return default

    def __getitem__(self, key : str):
        return self.get(key)
    
    def __getattr__(self, name: str):
        return self[name]
    
    @property
    def cfg(self) -> 'dict[str, ]':
        """
        Get the dictionary used in the instance construction.
        """
        return object.__getattribute__(self, '_cfg')

    def __repr__(self):
        return f'<muTel.daqplotlib.config.{self.__class__.__name__} at {hex(id(self))}>'
    def __str__(self):
        return self.__repr__()

class FieldConfig(Config):
    """
    A subclass of ``Config``. This represents the configuration for a field or column of
    data.
        
    Notes
    ----
    The supported keys are:
    - bins : ``int``
    - range : [``float``, ``float``]
    - label : ``str``
    - log : bool
    These keys will be used by the ``plot`` method in ``muTel.daqplotlib.Plot`` (and its
    subclasses) to style the plots and compute some internal variables.
    """
    
    def __init__(self, name : str, cfg: dict[str, ]):
        """
        Parameters
        ----------
        name : str
            Name of the field.
        cfg : dict
            Dictionary to wrap.
        """
        super().__init__(cfg)
        self._name = name
        
    @property
    def name(self) -> str:
        """
        Get the name of the field (or data column).
        """
        return self._name
    
    def __setitem__(self, key : str, value):
        self.cfg[key] = value

    
class PlotConfig(Config):
    """
    This class represents the configuration for a ``muTel.daqplotlib.plots.Plot`` (and its
    subclasses). Its a subclass of ``Config`` where each item in the `cfg` attribute
    is a ``FieldConfig`` instance.
    
    Methods
    -------
    from_yaml   
    """
    
    def __init__(self, cfg : 'dict[str, dict[str, ]]'):
        """
        Parameters
        ----------
        cfg : dict [str, dict]
            Dictionary with items ``str`` : ``dict`` where each dict will be converted to ``FieldConfig``.
        """
        cfg : 'dict[str, FieldConfig]' = {key : FieldConfig(key, _cfg) for key, _cfg in cfg.items()}
        super().__init__(cfg)

    def __getitem__(self, key : str) -> 'FieldConfig':
        return self.get(key, FieldConfig(key, {}))
            
    @classmethod
    def from_yaml(cls, path : 'str | pathlib.Path' = 'default') -> 'PlotConfig':
        """
        Class method to build an instance from a ``.yaml`` file in the path given. It first checks the path given and
        in case this fails, it will check the default directory using the default cfg directory (``config_directory``)
        defined in ``muTel.utils.paths``as ``<config_directory>/daq``.
        
        Parameters
        ----------
        path : str or ``pathlib.Path``, default 'default'
            Path to the ``.yaml`` file with the configuration.
        
        Returns
        -------
        plot_cfg : `PlotConfig`
            The `PlotConfig` instance built from the dictionary in ``.yaml`` format.
        """
        cfg = load_yaml(path, config_directory / pathlib.Path('daq'))
        return cls(cfg)