from typing import TYPE_CHECKING
from collections.abc import Iterable
from pathlib import Path
import itertools
from copy import deepcopy
import struct

import pyarrow as pa
import pyarrow.parquet as pq

from .Language import Italian, Latin, Spanish
from ..utils.parsing import obdt2int
from ...utils.paths import load_yaml, config_directory, get_file, data_directory, get_with_default
from ...utils.docs import is_documented_by

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
    _default_schema = {
        'index'     : 'uint64' ,
    }

    def __init__(self, language : 'Language'):
        self._language = language
        self._buffer = {
            field_name : []
            for field_name in self._default_schema.keys()
        } | {
            field_name  : []
            for field_name in language.schema.keys()
        }
        
        self._buffer = dict(sorted(self._buffer.items()))
        
        self._empty_buffer = deepcopy(self._buffer)
        self._buffer_size = 0
        self._lines_read = 0
        self._lines_failed = 0
        
        if language.schema:
            self._schema = Translator._parse_schema(self._default_schema | language.schema)
        else:
            self._schema = self._default_schema
            
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

    def translate(self,
        src_path : 'str | Path', 
        out_path : 'str | Path', 
        max_buffer : int = 1e5,
        binary_file : bool = False,
        close_pqwriter : bool = True,
        debug : bool = False, 
        verbose : bool = False
    ):
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
        from_dtupy : bool, default False
            Whether the input file is in the format of the dtupy files.
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

        if (self._schema is not None) & (self._pqwriter is None):
            self._pqwriter = pq.ParquetWriter(self.output_path, self._schema)
        
        if _do_bar:
            with open(src_path, 'r' + 'b' if binary_file else 'r') as file, alive_bar() as bar:
                self._main_loop(file,max_buffer=max_buffer, bar=bar, debug = debug, verbose=verbose)
        else:
            with open(src_path, 'r' + 'b' if binary_file else 'r') as file:
                self._main_loop(file,max_buffer=max_buffer, debug = debug, verbose=verbose)
        
        print('\nLines processed: {read:10d} / {total:<10d} ({ratio:6.2f}%)'.format(
            read    = self._lines_read                          ,
            total   = self._lines_read  + self._lines_failed    ,
            ratio  = self._lines_read/(self._lines_read  + self._lines_failed)*100  ,
        ))
        
        if close_pqwriter:
            self.pqwriter.close()
            self._pqwriter = None

    def error_handler(self, func, ignore_keyerror = False, debug=False, verbose=False):
        def wrapper(word, *args, **kwargs):
            try:
                return func(word, *args, **kwargs)
            except KeyError as e:
                if ignore_keyerror:
                    self._lines_failed += 1
                    return None
                else:
                    raise e
            except Exception as e:
                print(f"Unexpected error: {e}")
        return wrapper

    def _main_loop(self,
        file            : 'io.TextIOWrapper'    ,
        max_buffer      : int                   ,
        bar                    = None           ,
        ignore_keyerror : bool = False          ,
        debug           : bool = False          ,
        verbose         : bool = False          ,
    ) -> 'tuple[bool, int]':
        """
        Begins the loop over the input file lines.
        
        Parameters
        ----------
        file : io.TextIOWrapper
            File that will be translated.
        max_buffer : int
            Maximum size the buffer will reach before dumping to disk.
        from_dtupy : bool, default False
            Whether the input file is in the format of the dtupy files.
        
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
                fields = self.translate_word(line, 
                    ignore_keyerror = ignore_keyerror,
                    debug           = debug,
                    verbose         = verbose,
                )
                if fields is None: continue
                                
                self._update_buffer(fields | {'index' : i})
                
                if self._buffer_size == max_buffer: self._dump_buffer()
                
                del line
                
                if bar: bar()
        except KeyboardInterrupt:
            self._dump_buffer()
            return False, i
        
        self._dump_buffer()
        return True, i

    def translate_word(self, word, ignore_keyerror=True, debug=False, verbose=False):
        return self.error_handler(self._translate_word, ignore_keyerror=ignore_keyerror, debug = debug, verbose=verbose)(word)

    def _translate_word(self, word):
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
        return self.language(word)

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
        return Translator_sxa5(Italian(), cfg_path)
    
    @classmethod
    def from_la(cls, cfg_path : 'str | pathlib.Path'):
        """
        Construct instance with `Latin` as default language. This is for the older FW versions.
        It used to be called `Italian` in older versions of the package.
        """
        return Translator_sxa5(Latin(), cfg_path)
    
    @classmethod
    def from_es(cls, cfg_path : 'str | pathlib.Path' = None):
        """
        Construct instance with `Latin` as default language. This is for the older FW versions.
        It used to be called `Italian` in older versions of the package.
        """
        return Translator_dtupy(Spanish(), cfg_path)

    
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


class Translator_dtupy(Translator):
    def __init__(self,
                 language : 'Language',
                 cfg_path = None,
                 data_width = 65,
                 words_per_row = 3,
                 timestamp_width = 28,
                 start_time = None
            ):
        
        if cfg_path is not None:
            self._cfg_path = Path(cfg_path)
            self._cfg = load_yaml(cfg_path, config_directory / Path('daq/mapping'))
            self._build_translator()
            self._default_schema = self._default_schema | {'obdt_type' : 'uint8','obdt_ctr' : 'uint8', 'link' : 'uint8'}
        else:
            self._cfg_path = None
            self._cfg = None
            self._translator = None
        
        super().__init__(language)
        
        self._words_per_row = words_per_row
        self._word_size = data_width + 1 + timestamp_width
        self._data_width = data_width
        self._timestamp_width = timestamp_width
        self._start_time = start_time
        self._invalid_lines = 0
    def _build_translator(self):
        """
        This function builds the translator dictionary from the config file.
        The translator is stored in `_translator` which may be accesed through
        the `translator` property.
        """
        
        cfg = self._cfg
        translator = {}
        
        for link, obdt in cfg['links'].items():
            for obdt_ctr, (station, sl, sl_ctr) in cfg['connectors'][link].items():
                if station not in translator.keys(): translator[station] = {}
                if sl not in translator[station].keys(): translator[station][sl] = {}
                
                for cell in cfg['cells'][sl_ctr]:
                    translator[station][sl][cell] = [*obdt2int(obdt, obdt_ctr), link, sl_ctr]
                
                
        self._translator =  translator
        return

    @is_documented_by(Translator.translate)
    def translate(self,
        src_path : 'str | Path', 
        out_path : 'str | Path', 
        max_buffer : int = 1e5,
        debug : bool = False, 
        verbose : bool = False
    ):
        dump_path = Path(src_path)
        for dump_file in dump_path.parent.glob(f'{dump_path.stem}_*.bin'):
            super().translate(dump_file, out_path,
                max_buffer = max_buffer,
                binary_file = True,
                close_pqwriter=False,
                debug = debug,
                verbose = verbose
            )
        
        print('Lines failed: {0} error / {1} invalid / {2} total'.format(
            self._lines_failed - self._invalid_lines,
            self._invalid_lines,
            self._lines_failed,
        ))

        if self._pqwriter:
            self.pqwriter.close()
            self._pqwriter = None
        
    @is_documented_by(Translator._translate_word)
    def _translate_word(self, word):
        fields = self.language(word >> (self._timestamp_width + 1)) | self.language.timestamp(word)
        
        fields['layer'] += 1
        fields['station'] += 1
        
        if self.translator is not None:
            obdt_type, obdt_ctr, link, sl_ctr = self.translator[fields['station']][fields['sl']][fields['cell']]
            fields['obdt_type'] = obdt_type
            fields['obdt_ctr'] = obdt_ctr
            fields['link'] = link
            
        return fields


    def _main_loop(self,
        file            : 'io.TextIOWrapper'    ,
        max_buffer      : int                   ,
        bar                    = None           ,
        ignore_keyerror : bool = False          ,
        debug           : bool = False          ,
        verbose         : bool = False          ,
    ) -> 'tuple[bool, int]':
        print('Entering translator loop...')
        try:
            i = 0
            while True:
                try:
                    store_time = struct.unpack('>I', file.read(4))[0]/1000
                    if self._start_time is not None: store_time += self._start_time
                    
                    line = sum([struct.unpack('>I', file.read(4))[0] << (32*i) for i in range(self._words_per_row)])
                
                except struct.error:
                    # No more lines to read
                    break
                
                fields = self.translate_word(line, 
                    ignore_keyerror = ignore_keyerror,
                    debug           = debug,
                    verbose         = verbose,
                )
                
                if fields is None: continue
                elif not (fields['valid_trig'] & fields['valid_ro']):
                    self._invalid_lines += 1
                    self._lines_failed  += 1
                    continue
                
                # print(fields); exit()
                self._update_buffer(fields | {'index' : i, 'time' : store_time})
                
                if self._buffer_size == max_buffer: self._dump_buffer()
                
                del line
                
                if bar: bar()
                i += 1
        except KeyboardInterrupt:
            self._dump_buffer()
            return False, i

        self._dump_buffer()
        return True, i

    def error_handler(self, func, debug=False, verbose=False, **deco_kwargs):
        def wrapper(word, *args, **kwargs):
            try:
                return func(word, *args, **kwargs)
            except KeyError as e:
                self._lines_failed += 1
                self._lines_failed += 1                
                
                return None
            except Exception as e:
                print(f"Unexpected error: {e}")
        return wrapper



class Translator_sxa5(Translator):
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
        'index'     : 'uint64' ,
        'obdt_type' : 'uint8'  ,
        'obdt_ctr'  : 'uint8'  ,
        'station'   : 'int8'   ,
        'sl'        : 'uint8'  ,
        'layer'     : 'uint8'  ,
        'cell'      : 'uint8'  ,
    }
        
    def __init__(self, language : 'Language', cfg_path):
        super().__init__(language)
        
        self._cfg_path = Path(cfg_path)
        self._cfg = load_yaml(cfg_path, config_directory / Path('daq/mapping'))
        self._valid_links = {link : set(obdt.keys()) for link, obdt in self._cfg['connectors'].items()}
        self._build_translator()
   


    
    
    def _translate_word(self, word):
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
        fields = self.language(int(word))
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
    

    def error_handler(self, func, debug=False, verbose=False, **deco_kwargs):
        def wrapper(word, *args, **kwargs):
            try:
                return func(word, *args, **kwargs)
            except KeyError as e:
                self._lines_failed += 1
                self._lines_failed += 1
                fields = self.language(int(word))
                link   = fields['link']
                ch     = fields['channel']
                
                if debug & verbose:
                    print(f"KeyError:"
                            f"\tlink : {link:2d} ({str(link in self._translator             .keys()):>5s})"
                            f"\tch   : {ch  :3d} ({str(ch   in self._translator.get(link,{}).keys()):>5s})"
                            f"\t{self._translator.get(link,{}).get(ch,'')}"
                        )
                if not (self._translator.get(link,{}).get(ch, None) is None): raise e
                
                return None
            except Exception as e:
                print(f"Unexpected error: {e}")
        return wrapper
