from muTel.utils.doc import is_documented_by
from muTel.utils.paths import load_yaml, config_directory, get_file, data_directory, get_with_default

from collections.abc import Iterable
from pathlib import Path
import itertools
from copy import deepcopy
import pyarrow as pa
import pyarrow.parquet as pq




class Language(object):
    def __init__(self, id = 'default', fields = {}, schema = None):
        self._fields = fields
        self._id     = id
        self._schema = schema
    # =================================
    # OBJECT ATTRIBUTES
    # =================================
    # READ-ONLY
    # -------------------
    @property
    def fields(self):
        """
        Mapping of the fields in (mask, right_shift) format for Translator's parse method.
        """
        return self._fields
    @property
    def id(self):
        """
        Shortened name to refer to this language.
        """
        return self._id
    @property
    def schema(self) -> 'None | dict[str,str]':
        """
        Dictionary containing the data types of the fields.
        """
        return self._schema
    # =================================
    # OBJECT METHODS
    # =================================
    def translate(
            self,
            word    : 'int | bytes'
        ) -> Iterable:
        """
        This function reads a word and returns a dictionary of the fields codified
        in the word.

        Parameters
        ----------
        word : int
            Word to apply bit-wise operations.
        
        Returns
        -------
        fields : dict[field name] : field value
            dictionary with all fields
        """
        # return link, tdc, bx, ch
        return {field_name : Language.parse(word, *field) for field_name, field in self.fields.items()}

    # =================================
    # STATIC METHODS
    # =================================
    @staticmethod
    def parse(
            word    : 'int | bytes'     , 
            mask    : bin               ,
            pos     : int               ,
        ) -> tuple:
        """
        This function extracts some bits from a word.

        Parameters
        ----------
        word : int
            Word to apply bit-wise operations.

        mask : bin or hex
            A binary mask to apply to the word.
            
        pos : int
            Number of bits to shift the word to the right, i.e., the position of the
            first bits that should be read.
        
        Returns
        -------
        bits
            Bits that have been extracted from the word.
        """
        
        
        # return link, tdc, bx, ch
        return (word >> pos) & mask
    
    
    # =================================
    # OBJECT METHODS
    # =================================
    @is_documented_by(translate)
    def __call__(
            self    : 'Language'  ,
            word    : 'int | bytes' ,        
        ) -> Iterable:
        
        return self.translate(word) 
    
class Italian(Language):
    _default_fields = {
        'channel'   : (0xFF,       0),    # CHANNEL
        'bx'        : (0xFFF,      8),    # BX
        'tdc'       : (0b11111,   20),    # TDC
        'link'      : (0xF,       60)     # LINK
    }
    _default_schema = {
        'channel'   : 'uint8',    # CHANNEL
        'bx'        : 'uint16',   # BX
        'tdc'       : 'uint8',    # TDC
        'link'      : 'uint8'     # LINK
    }
    
    def __init__(self):
        super().__init__('it', Italian._default_fields, Italian._default_schema)

class Translator(object):
    _default_schema = {
        'index'     : 'uint64',
        'station'   : 'int8',
        'sl'        : 'uint8',
        'layer'     : 'uint8',
        'cell'      : 'uint8'
    }
    @staticmethod
    def parse_schema(schema):
        return pa.schema([(field, eval(f'pa.{dtype}()')) for field, dtype in sorted(schema.items())])
    
    def __init__(self, language : Language, mapping_path):
        self._language = language
        self._cfg_name = Path(mapping_path).stem
        self._cfg = load_yaml(mapping_path, config_directory / Path('daq/mapping'))
        self._valid_links = {link : set(obdt.keys()) for link, obdt in self._cfg['connectors'].items()}
        # self._translator[link][channel] = [station, sl, layer, cell]
        self.build_translator()
        
        
        
        self._buffer = {
            'index'     : [],
            'station'   : [],
            'sl'        : [],
            'layer'     : [],
            'cell'      : [], 
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
            self._schema = Translator.parse_schema(self._default_schema | language.schema)
        else:
            self._schema = None
            
        self._pqwriter = None
        self._output_path = None
            
        
        
    def reset_buffer(self):
        self._buffer = deepcopy(self._empty_buffer)
        self._buffer_size = 0


    def build_translator(self):
        cfg = self._cfg
        translator = {}
        
        for link, obdt in cfg['links'].items():
            translator[link] = {}
            for obdt_ctr, (station, sl, sl_ctr) in cfg['connectors'][link].items():
                # Station Superlayer Layer Cell
                sslc = [[station, sl, *wl[::-1]] for wl in list(itertools.product(cfg['cells'][sl_ctr], cfg['layers']))]
                translator[link] = translator[link] | dict(zip(cfg['obdt'][obdt][obdt_ctr],sslc))
                
        self._translator =  translator
    
    def translate(self, src_path, out_path, max_buffer = 1e5, debug = False):
        if Path(src_path).suffix == '':
            src_path = get_file(src_path, data_directory, ['.txt'], debug=True)
        else:
            src_path = get_file(src_path, data_directory)
            
        out_path = get_with_default(Path(out_path).with_suffix('.parquet'), data_directory)
        self._output_path = Path(out_path)

        if self._schema: self._pqwriter = pq.ParquetWriter(self.output_path, self._schema)
        
        if debug:
            from alive_progress import alive_bar
            
            # print('Getting number of entries...',end=' ')
            # with open(src_path, "rb") as f:
            #     nentries = sum(1 for _ in f)
            # print('Success!')

            with open(src_path, 'r') as file, alive_bar() as bar:
                self.main_loop(file,max_buffer=max_buffer,bar=bar)
        else:
            with open(src_path, 'r') as file:
                self.main_loop(file,max_buffer=max_buffer)
        
        print('\nLines processed: {read:10d} / {total:<10d} ({ratio:6.2f}%)'.format(
            read    = self._lines_read                          ,
            total   = self._lines_read  + self._lines_failed    ,
            ratio  = self._lines_read/(self._lines_read  + self._lines_failed)*100  ,
        ))
        
        
        self.pqwriter.close()
        self._pqwriter = None
    
    def main_loop(self, file, max_buffer, bar = None):
        print('Entering translator loop...')
        try:
            for i, line in enumerate(file):
                try:
                    fields = self.translate_word(int(line)) | {'index' : i}
                    
                    self.update_buffer(fields)
                    
                    if self._buffer_size == max_buffer: self.dump_buffer()
                    
                    del line
                except KeyError as err:
                    self._lines_failed += 1
                    continue
                except Exception as err:
                    print(self.buffer)
                    raise err
                finally:
                    if bar: bar()
        except KeyboardInterrupt:
            self.dump_buffer()
            return False, i
            
        self.dump_buffer()
        return True, i
    
    def translate_word(self,word):
        fields = self.language(word)
        
        sllc   = dict(zip(['station', 'sl', 'layer', 'cell'], self.lookup_sllc(fields)))
        return fields | sllc
    
    def lookup_sllc(self,fields):
        return self._translator[fields['link']][fields['channel']]
    
    
    def update_buffer(self,fields):
        for field_name, value in fields.items():
            self._buffer[field_name].append(value)
        self._buffer_size += 1
        return True
    
    def dump_buffer(self):
        print(f'Dumping {self._buffer_size} lines...')
        table = pa.Table.from_pydict(self.buffer)
        _size = table.num_rows
        if self._schema: table = table.cast(self._schema)
        if not self.pqwriter: self._pqwriter = pq.ParquetWriter(self.output_path, table.schema)
        self.pqwriter.write_table(table)
        self._lines_read += _size
        self.reset_buffer()
    
    
    @classmethod
    def from_it(cls,mapping_path):
        return cls(Italian(),mapping_path)
    
    @property
    def language(self) -> Language:
        return self._language
    @property
    def buffer(self):
        return self._buffer
    @property
    def valid_links(self):
        return self._valid_links
    @property
    def pqwriter(self):
        return self._pqwriter
    @property
    def output_path(self) -> Path:
        return self._output_path
    @property
    def cfg_name(self) -> str:
        return self._cfg_name
if __name__ == '__main__':
    lang = Italian()
    print(lang(5764607523173461363))
    print(lang(11529215049700534640))
    
    # exit()
    # sxa_path = '/afs/ciemat.es/user/m/martialc/public/muTel_v4/muTel/data/sxa5_data.txt'
    # out_path = '/afs/ciemat.es/user/m/martialc/public/muTel_v4/muTel/data/sxa5_data.parquet'
    sxa_path = '/afs/cern.ch/user/r/redondo/work/public/sxa5/testpulse_theta_2.txt'
    out_path = '/afs/ciemat.es/user/m/martialc/public/muTel_v4/muTel/data/testpulse_theta_2.parquet'
    transr = Translator.from_it('testpulse_theta_2')
    transr.translate(sxa_path, out_path, max_buffer=1e5, debug=True)
