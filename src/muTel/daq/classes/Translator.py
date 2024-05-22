from muTel.utils.doc import is_documented_by
from muTel.utils.paths import load_yaml, config_directory

from collections.abc import Iterable
from pathlib import Path
import itertools
from copy import deepcopy
import pyarrow as pa
import pyarrow.parquet as pq
from alive_progress import alive_bar




class Language(object):
    def __init__(self, fields = {}):
        self._fields = fields

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
    
    def __init__(self):
        super().__init__(self._default_fields)

class Translator(object):
    def __init__(self, language, mapping_path):
        self._language = language
        self._cfg = load_yaml(mapping_path, config_directory / Path('daq/mapping'))
        self._valid_links = {link : set(obdt.keys()) for link, obdt in self._cfg['connectors'].items()}
        # self._translator[link][channel] = [station, sl, layer, cell]
        self.build_translator()
        
        
        
        self._buffer = {
            'station'   : [],
            'sl'        : [],
            'layer'     : [],
            'cell'      : [], 
        }
        self._buffer = self._buffer | {field_name : [] for field_name in language.fields.keys()}
        self._empty_buffer = deepcopy(self._buffer)
        self._buffer_size = 0
        
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
    
    def translate(self, src_path, out_path, max_buffer = 1e5):
        with open(src_path, "rb") as f:
            nentries = sum(1 for _ in f)
        
        self._output_path = out_path
        
        with open(src_path, 'r') as file, alive_bar(nentries,refresh_secs=.1) as bar:
            for line in file:
                try:
                    fields = self.translate_word(int(line))
                    
                    self.update_buffer(fields)
                    
                    bar()
                    if self._buffer_size > max_buffer: self.dump_buffer()

                except KeyboardInterrupt:
                    break
                except KeyError:
                    bar()
                    continue
                except Exception as err:
                    print(self.buffer)
                    raise err

        self.pqwriter.close()
        self._pqwriter = None
        
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
        table = pa.Table.from_pydict(self.buffer)
        if not self.pqwriter: self._pqwriter = pq.ParquetWriter(self.output_path, table.schema)
        self.pqwriter.write_table(table)
    
        self.reset_buffer()
    
    
    @classmethod
    def from_it(cls,mapping_path):
        return cls(Italian(),mapping_path)
    
    @property
    def language(self):
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
    def output_path(self):
        return self._output_path
if __name__ == '__main__':
    lang = Italian()
    print(lang(5764607523173461363))
    
    sxa_path = '/afs/ciemat.es/user/m/martialc/public/muTel_v4/muTel/data/sxa5_data.txt'
    out_path = '/afs/ciemat.es/user/m/martialc/public/muTel_v4/muTel/data/test.parquet'
    transr = Translator.from_it('sxa5')
    transr.translate(sxa_path, out_path, max_buffer=1e5)
