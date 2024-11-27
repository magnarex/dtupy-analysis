from dtupy_analysis.utils.docs import is_documented_by
from collections.abc import Iterable


class Language(object):
    """
    This class' task is the conversion of a word of bits into a series of fields using two parameters:
    - mask : A mask of ones the size of the field.
    - pos : The position of the first bit of the field (being pos=0 the leftmost bit).
    
    It also needs a "schema", which is a dictionary with the type of every field that will be used to
    to cast the ``pyarrow.Table`` into the correct schema in order to write the ``.parquet`` file.
    
    You may call a `Language` instance on an int to translate it into a dict with every field.
    
    Attributes
    ----------
    fields : dict [str, (int, int)]
        A dictionary containing the mask and position of each field in a word.
    id : str
        An abbreviated class name. Defaults to the first two letters of the class name in lowercase.
    schema : dict [str, str]
        A dictionary with the type to which each field will be cast.
    
    Methods
    -------
    parse(word, mask, pos)
        Extracts the bits from `word` located in `pos` using `mask`.
    translate(word)
        Returns a dict with the value for each field as read from `word`.
    """
    
    def __init__(self, id : str = None, fields : 'dict[str, (int, int)]' = {}, schema : 'dict[str,str]' = None):
        """
        Paramaters
        ----------
        id : str, optional
            The shortened name of the class.
        fields : dict [str, (int, int)]
            A dictionary with the name of the fields as the keys and a tuple ``(mask, position)`` where\
            ``mask`` should be a binary mask (e.g. 0xFF, 0b111) and ``position`` the position of the\
            first bit, where the leftmost bit has ``position=0``.
        schema : dict [str, str]
            A dictionary with the same keys as `fields` and where the values are strings with the\
            name of the Pyarrow data types [1]_.
            
        References
        ----------
        .. [1] https://arrow.apache.org/docs/python/api/datatypes.html#factory-functions
        """
        self._fields = fields
        self._id     = id
        self._schema = schema
    
    # =================================
    # OBJECT ATTRIBUTES
    # =================================
    # READ-ONLY
    # -------------------
    @property
    def fields(self) -> 'dict[str, (int, int)]':
        """
        Get the mapping of the fields in (mask, position) format for usage in Translator's parse method.
        """
        return self._fields
    @property
    def id(self) -> str:
        """
        Get the shortened name to refer to this language.
        """
        if self._id is None:
            return self.__class__.__name__[:2].lower()
        else:
            return self._id
    @property
    def schema(self) -> 'None | dict[str,str]':
        """
        Get the dictionary containing the data types of every field.
        """
        return self._schema
    
    # =================================
    # OBJECT METHODS
    # =================================
    def translate(
            self,
            word    : 'int | bytes',
            dictionary : 'dict[str, (int, int)]' = None
        ) -> Iterable:
        """
        This function reads a word and returns a dictionary of the fields codified
        in the word.

        Parameters
        ----------
        word : int
            A word to extract the fields from.
        
        Returns
        -------
        fields : dict[field name] : field value
            Dictionary with the value of every field as read from `word`.
        """
        
        if dictionary is None: dictionary = self.fields
        return {field_name : Language.parse(word, *field) for field_name, field in dictionary.items()}

    # =================================
    # STATIC METHODS
    # =================================
    @staticmethod
    def parse(
            word    : int               , 
            mask    : int               ,
            pos     : int               ,
        ) -> int:
        """
        This function extracts some bits from a word.

        Parameters
        ----------
        word : int
            Word to split into fields.

        mask : int
            A binary mask to apply to the word.
            
        pos : int
            Number of bits to shift the word to the right, i.e., the position of the
            first bits that should be read.
        
        Returns
        -------
        bits : int
            Bits that have been extracted from the word.
        """
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

class Latin(Language):
    """
    Sub-class of ``Language`` that has the following fields:
    
    =========== =========== =============== ===========
    Field       Type        Position        Mask        
    ----------- ----------- --------------- -----------
    channel     ``uint8``   0               0xFF      
    bx          ``uint16``  8               0xFFF               
    tdc         ``uint8``   20              0b11111      
    link        ``uint8``   60              0xF             
    =========== =========== =============== =========
    """
    
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
        super().__init__('la', Latin._default_fields, Latin._default_schema)

class Italian(Language):
    """
    Sub-class of ``Language`` that has the following fields:
    
    =========== =========== =============== ===========
    Field       Type        Position        Mask        
    ----------- ----------- --------------- -----------
    channel     ``uint8``   0               0xFF      
    bx          ``uint16``  8               0xFFF               
    tdc         ``uint8``   20              0b11111 
    orbit       ``uint16``  32              0xFFFF
    link        ``uint8``   58              0b111111           
    =========== =========== =============== =========
    """
    
    _default_fields = {
        'channel'   : (0xFF,       0),    # CHANNEL
        'bx'        : (0xFFF,      8),    # BX
        'tdc'       : (0b11111,   20),    # TDC
        'orbit'     : (0xFFFF,    32),    # ORBIT
        'link'      : (0b111111,  58)     # LINK
    }
    _default_schema = {
        'channel'   : 'uint8',    # CHANNEL
        'bx'        : 'uint16',   # BX
        'tdc'       : 'uint8',    # TDC
        'orbit'     : 'uint16',   # ORBIT   
        'link'      : 'uint8'     # LINK
    }
    
    def __init__(self):
        super().__init__('it', Italian._default_fields, Italian._default_schema)

class Spanish(Language):
    """
    Sub-class of ``Language`` that has the following fields:
    
    =========== =========== =============== ===========
    Field       Type        Position        Mask        
    ----------- ----------- --------------- -----------
    obdt_ch     ``uint8``   0               0xFF      
    obdt_bx     ``uint16``  8               0xFFF               
    obdt_tdc    ``uint8``   20              0b11111 
    valid       ``bool``    25              0b1
    bx          ``uint16``  26              0xFFF
    tdc         ``uint8``   38              0b11111
    wheel       ``int8``    43              0b111
    sector      ``int8``    46              0b1111
    station     ``int8``    50              0b11
    sl          ``uint8``   52              0b11
    layer       ``uint8``   54              0b11
    cell        ``uint8``   56              0b1111111
    valid_ro    ``bool``    63              0b1
    valid_trig  ``bool``    64              0b1
    =========== =========== =============== =========
    """
    
    _default_fields = {
        'obdt_ch'   : (0xFF,       0),    # CHANNEL
        'obdt_bx'   : (0xFFF,      8),    # BX
        'obdt_tdc'  : (0b11111,   20),    # TDC
        'valid'     : (0b1,       25),    # VALID
        'bx'        : (0xFFF,     26),    # BX_EXT
        'tdc'       : (0b11111,   38),    # TDC_EXT
        'wheel'     : (0b111,     43),    # WHEEL
        'sector'    : (0xF,       46),    # SECTOR
        'station'   : (0b11,      50),    # STATION
        'sl'        : (0b11,      52),    # SL
        'layer'     : (0b11,      54),    # LAYER
        'cell'      : (0b1111111, 56),    # CELL
        'valid_ro'  : (0b1,       63),    # VALID_RO
        'valid_trig': (0b1,       64),    # VALID_TRIG
    }
    _default_schema = {
        'time'      : 'float64'         , # TIME
        'ts_cc'     : 'uint8'           , # TS_CC
        'ts_bx'     : 'uint16'          , # TS_BX
        'ts_oc'     : 'uint16'          , # TS_OC
        'obdt_ch'   : 'uint8'           , # CHANNEL
        'obdt_bx'   : 'uint16'          , # BX
        'obdt_tdc'  : 'uint8'           , # TDC
        'valid'     : 'uint8'           , # VALID
        'bx'        : 'uint16'          , # BX_EXT
        'tdc'       : 'uint8'           , # TDC_EXT
        'wheel'     : 'int8'            , # WHEEL
        'sector'    : 'int8'            , # SECTOR
        'station'   : 'int8'            , # STATION
        'sl'        : 'uint8'           , # SL
        'layer'     : 'uint8'           , # LAYER
        'cell'      : 'uint8'           , # CELL
        'valid_ro'  : 'uint8'           , # VALID_RO
        'valid_trig': 'uint8'           , # VALID_TRIG
    }
    def __init__(self):
        super().__init__('es', Spanish._default_fields, Spanish._default_schema)

    def timestamp(self, word):
        ts_dict = {
            'ts_cc' : (0xF,     0),
            'ts_bx' : (0xFFF,   4),
            'ts_oc' : (0xFFF,  16),
        }
        return self.translate(word, ts_dict)