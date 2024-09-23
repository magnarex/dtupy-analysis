from typing import TYPE_CHECKING
from collections.abc import Iterable
from pathlib import Path
import itertools
from copy import deepcopy

import pyarrow as pa
import pyarrow.parquet as pq

from .Language import Italian, Latin
from ..utils.parsing import obdt2int
from ...utils.paths import load_yaml, config_directory, get_file, data_directory, get_with_default

if TYPE_CHECKING:
    import io
    import pathlib
    import pyarrow
    import pyarrow.parquet
    from   .Language import Language

try:
    from alive_progress import alive_bar
    _do_bar = True
except ImportError:
    alive_bar = None
    _do_bar = False



    

class Translator(object):
    """
    Class that handles the loop for translating every row in a literal ``.txt`` file.
    
    Attributes
    ----------
    language : `Language`
        The subclass of `Language` used in translation.
    buffer : dict [str, Iterable]
    valid_links : dict[int, str]
        A dictionary containing the connections between links and OBDT types.
    pqwriter : pyarrow.parquet.ParquetWriter
        Pyarrow object that will handle the writing in ``.parquet`` format.
    output_path : pathlib.Path
        Path to the output ``.parquet`` file.
    cfg_name : str
        Name of the config ``.yaml`` file.
    translator : `Translator`
        Instance of the `Translator` class that will handle the writing.
    
    Methods
    -------
    translate(src_path, out_path, max_buffer=1e5, debug=False, verbose=False)
        Translate `src_path` to a ``.parquet`` file at `out_path`.
    from_it(cfg_path)
        Class method to create an instance of `Translator` with `language=Italian`.
    """
    
    _default_schema = {
        'index_t'   : 'uint64' ,
        'obdt_type' : 'uint8'  ,
        'obdt_ctr'  : 'uint8'  ,
        'station'   : 'int8'   ,
        'sl'        : 'uint8'  ,
        'layer'     : 'uint8'  ,
        'cell'      : 'uint8'  ,

    }
        
    def __init__(self, language : 'Language', cfg_path):
        self._language = language
        self._cfg_path = Path(cfg_path)
        self._cfg = load_yaml(cfg_path, config_directory / Path('daq/mapping'))
        self._valid_links = {link : set(obdt.keys()) for link, obdt in self._cfg['connectors'].items()}
        # self._translator[link][channel] = [station, sl, layer, cell]
        self._build_translator()
        
        
        
        self._buffer = {
            field_name : []
            for field_name in self._default_schema.keys()
        } | {
            field_name  : []
            for field_name in language.fields.keys()
        }
        
        self._buffer = dict(sorted(self._buffer.items()))
        
        self._empty_buffer = deepcopy(self._buffer)
        self._buffer_size = 0
        self._lines_read = 0
        self._lines_failed = 0
        
        if language.schema:
            self._schema = Translator._parse_schema(self._default_schema | language.schema)
        else:
            self._schema = None
            
        self._pqwriter = None
        self._output_path = None
            
    @staticmethod
    def _parse_schema(schema):
        """
        Transform `schema` into a ``pyarrow.Schema``.
        
        Parameters
        ----------
        schema : dict [str, str]
            Dictionary with the name of the fields and the name of their corresponding Pyarrow data type [1]_.
        
        Returns
        -------
        ``pyarrow.Schema``
        
        References
        ----------
        .. [1] See https://arrow.apache.org/docs/python/api/datatypes.html#factory-functions
        """
        return pa.schema([(field, eval(f'pa.{dtype}()')) for field, dtype in sorted(schema.items())])
   
    def _reset_buffer(self):
        """
        Resets the buffer using `_empty_buffer` as a template. In turn, `_empty_buffer` is
        constructed from ``Translator._default_schema`` and `language.fields`.
        """
        self._buffer = deepcopy(self._empty_buffer)
        self._buffer_size = 0
        
        return

    def _build_translator(self):
        """
        This function builds the translator dictionary from the config file.
        The translator is stored in `_translator` which may be accesed through
        the `translator` property.
        """
        
        cfg = self._cfg
        translator = {}
        
        for link, obdt in cfg['links'].items():
            translator[link] = {}
            for obdt_ctr, (station, sl, sl_ctr) in cfg['connectors'][link].items():
                
                # OBDT type | OBDT connector | Station | Superlayer | Layer | Cell (sslc)
                
                # IMPORTANT: IF YOU WANT TO CHANGE THE COLUMNS OF THE OUTPUT READ THIS
                # This are the lists that the translator will get when calling self.lookup_sllc, if you want to add a column
                # in the output file, this is the thing you wanna edit. You must also edit Translator._default_schema to add
                # the data type of the new column.
                # self.lookup_sllc(fields) -> self._translator[link][channel] -> sllc 
                
                sslc = [
                    [*obdt2int(obdt, obdt_ctr), station, sl, *wl[::-1]]                    # Fields returned by link & channel
                    for wl in list(itertools.product(cfg['cells'][sl_ctr], cfg['layers'])) # Unpack cells and layers (cartesian product)
                ]
                
                translator[link] = translator[link] | dict(zip(cfg['obdt'][obdt][obdt_ctr],sslc))
                
        self._translator =  translator
        return

    def translate(self, src_path : 'str | Path', out_path : 'str | Path', max_buffer : int = 1e5, debug : bool = False, verbose : bool = False):
        """
        Public method to handle the translation of ``.txt`` files into ``.parquet`` files. This will start a loop over the lines if the file at
        `src_path` that may be interrupted via a ``KeyboardInterrupt`` exception at any point, this will save the current state of the
        translation to the output file to allow partial translation.
        
        Parameters
        ----------
        src_path : str or pathlib.Path
            Path to the ``.txt`` file to be translated.
        out_path : str or pathlib.Path
            Path to the ``.parquet`` file that will be written.
        
        Other Parameters
        ----------------
        max_buffer : int, default 100000
            Maximum size the buffer will reach before dumping into disk.
        debug : bool, default False
            Turn on or off the debugging messages.
        verbose : bool, default False
            Turn on or off the verbality of the main loop.
        
        """
        if Path(src_path).suffix == '':
            src_path = get_file(src_path, data_directory, ['.txt'], debug=True)
        else:
            src_path = get_file(src_path, data_directory)
            
        out_path = get_with_default(Path(out_path).with_suffix('.parquet'), data_directory)
        self._output_path = Path(out_path)

        if self._schema: self._pqwriter = pq.ParquetWriter(self.output_path, self._schema)
        
        if _do_bar:
            with open(src_path, 'r') as file, alive_bar() as bar:
                self._main_loop(file,max_buffer=max_buffer,bar=bar, debug = debug, verbose=verbose)
        else:
            with open(src_path, 'r') as file:
                self._main_loop(file,max_buffer=max_buffer, debug = debug, verbose=verbose)
        
        print('\nLines processed: {read:10d} / {total:<10d} ({ratio:6.2f}%)'.format(
            read    = self._lines_read                          ,
            total   = self._lines_read  + self._lines_failed    ,
            ratio  = self._lines_read/(self._lines_read  + self._lines_failed)*100  ,
        ))
        
        
        self.pqwriter.close()
        self._pqwriter = None
    
    def _main_loop(self, file : 'io.TextIOWrapper', max_buffer : int, bar = None, debug : bool = False, verbose : bool = False) -> 'tuple[bool, int]':
        """
        Begins the loop over the input file lines.
        
        Parameters
        ----------
        file : io.TextIOWrapper
            File that will be translated.
        max_buffer : int
            Maximum size the buffer will reach before dumping to disk.
        
        Other Parameters
        ----------------
        bar : default None
            If ``alive_progress`` this will get the returned object from ``alive_progress.alive_bar`` and will display
            a dynamic progress bar in the terminal.
        debug : bool, default False
            Turn on or off the debugging messages.
        verbose : bool, default False
            Turn on or off the verbality of the main loop. This will print the expected keys for a KeyError, useful for debugging
            the configuration of the dataset.
        
        Returns
        -------
        bool
            Whether the file has been fully read or not.
        int
            Index of last line read.
        
        Raises
        ------
        For any other exception appart from ``KeyboardInterrupt`` and ``KeyError``, this raises the exception as it normally would.
        """
        print('Entering translator loop...')
        try:
            for i, line in enumerate(file):
                try:
                    fields = self._translate_word(int(line)) | {'index_t' : i}
                    
                    self._update_buffer(fields)
                    
                    if self._buffer_size == max_buffer: self._dump_buffer()
                    
                    del line
                except KeyError as err:
                    fields = self.language(int(line))
                    link   = fields['link']
                    ch     = fields['channel']
                    if debug & verbose:
                        print(f"KeyError:"
                              f"\tlink : {link:2d} ({str(link in self._translator             .keys()):>5s})"
                              f"\tch   : {ch  :3d} ({str(ch   in self._translator.get(link,{}).keys()):>5s})"
                              f"\t{self._translator.get(link,{}).get(ch,'')}"
                            )
                    if not (self._translator.get(link,{}).get(ch, None) is None): raise err
                        
                    self._lines_failed += 1
                    continue
                except Exception as err:
                    # print(self.buffer)
                    raise err
                finally:
                    if bar: bar()
        except KeyboardInterrupt:
            self._dump_buffer()
            return False, i
            
        self._dump_buffer()
        return True, i
    
    def _translate_word(self,word):
        """
        This calls `language` to translate a word read from the source file.
        
        Parameters
        ----------
        word : int
            Word that will be translated using `language`.
        
        Returns
        -------
        dict [str, ]
            A dictionary with every field's value
            
        Raises
        ------
        KeyError
            This means the translated `word` doesn't match with any registered channel, probably noise.
            
        """
        fields = self.language(word)
        
        sllc   = dict(zip(['obdt_type', 'obdt_ctr', 'station', 'sl', 'layer', 'cell'], self._lookup_sllc(fields)))
        return fields | sllc
    
    def _lookup_sllc(self,fields : 'dict[str, ]') -> 'tuple':
        """
        Check if the values of dictionary `fields` correspond to any registered channel.
        
        Returns
        -------
        tuple
            A ``tuple`` with the translate fields for (link, connector, channel) as defined in `_translator`.
        
        Raises
        ------
        KeyError
            This means the `fields` don't match with any registered channel, probably noise.

        """
        return self._translator[fields['link']][fields['channel']]
    
    
    def _update_buffer(self,fields):
        """
        Append field values to buffer.
        
        Parameters
        ----------
        fields : dict [str, ]
            Translated word to add to buffer.
        
        Returns
        -------
        bool
            Returns ``True`` just in case.
        """
        for field_name, value in fields.items():
            self._buffer[field_name].append(value)

        self._buffer_size += 1
        return True
    
    def _dump_buffer(self):
        """
        Dump the buffer to disk and reset it.
        """
        print(f'Dumping {self._buffer_size} lines...')
        
        table = pa.Table.from_pydict(self.buffer)
        _size = table.num_rows
        if self._schema: table = table.cast(self._schema)
        if not self.pqwriter: self._pqwriter = pq.ParquetWriter(self.output_path, table.schema)
        self.pqwriter.write_table(table)
        self._lines_read += _size
        self._reset_buffer()
    
    
    @classmethod
    def from_it(cls, cfg_path : 'str | pathlib.Path'):
        """
        Construct instance with `Italian` as default language. This is for the new FW update.
        """
        return cls(Italian(), cfg_path)
    @classmethod
    def from_la(cls, cfg_path : 'str | pathlib.Path'):
        """
        Construct instance with `Latin` as default language. This is for the older FW versions.
        It used to be called `Italian` in older versions of the package.
        """
        return cls(Latin(), cfg_path)
    
    @property
    def language(self) -> 'Language':
        """
        Get the `Language` instance used for the translation.
        """
        return self._language
    @property
    def buffer(self) -> 'dict[str, Iterable]':
        """
        Get the current state of the buffer.
        
        WARNING: This may reach size #fields x (max buffer size), so keep this in mind before printing.
        """
        return self._buffer
    @property
    def valid_links(self) -> 'dict[int, str]':
        """
        Get the relation between link ({1,2,...}) and OBDT type ({theta, phi}).
        """
        return self._valid_links
    @property
    def pqwriter(self) -> 'pyarrow.parquet.ParquetWriter':
        """
        Get the ``ParqueWriter`` that will be used for writing.
        """
        return self._pqwriter
    @property
    def output_path(self) -> Path:
        """
        Get the path to the output file.
        """
        return self._output_path
    @property
    def cfg_name(self) -> str:
        """
        Get the name of the config file.
        """
        return self._cfg_path.stem
    @property
    def translator(self) -> dict[int, dict[int, list]]:
        """
        Get the dictionary used for translation.
        """
        return self._translator
