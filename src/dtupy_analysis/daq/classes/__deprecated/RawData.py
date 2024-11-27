import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import itertools
import pyarrow as pa
import pyarrow.parquet as pq
import gc
import numpy as np
from copy import deepcopy
from alive_progress import alive_bar
from pprint import pprint

theta = {
    'a': [33,   34,   31,   30,   35,   28,   32,   29,   26,   24,   27,   5,    25,   3,    4,    0   ],
    'b': [2,    1,    94,   7,    9,    155,  63,   65,   153,  17,   16,   20,   18,   19,   22,   21  ],
    'c': [157,  151,  150,  227,  11,   8,    6,    10,   154,  152,  226,  146,  147,  122,  120,  116 ],
    'd': [224,  133,  118,  119,  117,  115,  101,  100,  102,  99,   107,  105,  103,  106,  214,  211 ],
    'e': [210,  218,  220,  225,  215,  104,  199,  213,  212,  201,  202,  196,  198,  200,  121,  124 ],
    'f': [45,   194,  125,  126,  123,  139,  140,  144,  148,  149,  222,  193,  203,  192,  223,  46  ],
    'g': [216,  197,  195,  54,   47,   49,   44,   48,   38,   108,  109,  111,  110,  36,   112,  113 ],
    'h': [37,   114,  132,  128,  129,  130,  127,  131,  51,   50,   208,  52,   53,   41,   39,   205 ],
    'i': [23,   14,   12,   15,   13,   221,  219,  217,  207,  209,  93,   206,  62,   69,   80,   79  ],
    'j': [76,   204,  42,   59,   40,   43,   180,  181,  178,  176,  177,  179,  162,  160,  182,  161 ],
    'k': [135,  183,  138,  163,  185,  184,  167,  164,  145,  137,  134,  136,  170,  142,  143,  141 ],
    'l': [171,  156,  158,  159,  66,   90,   92,   91,   64,   68,   95,   97,   70,   173,  96,   98  ],
    'm': [72,   67,   165,  71,   61,   73,   82,   81,   83,   75,   78,   166,  168,  77,   56,   74  ],
    'n': [169,  58,   60,   57,   86,   55,   85,   87,   89,   172,  174,  187,  175,  84,   88,   186 ],
    'o': [191,  190,  188,  189,  239,  239,  239,  239,  239,  239,  239,  239,  239,  239,  239,  239 ],
}

# dict[channel] = obdt_connector
theta_translator = {}
for connector, channels in theta.items():
    for channel in channels:
        theta_translator[int(channel)] = connector

# dict[obdt_connector] = list(channels)
phi = {
    'a': [158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 204, 205, 206, 207 ],
    'b': [ 62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77 ],
    'c': [110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125 ],
    'd': [186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201 ],
    'e': [224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239 ],
    'f': [126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141 ],
    'g': [ 46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61 ],
    'h': [ 78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93 ],
    'i': [170, 171, 172, 173, 174, 175, 176, 177, 178, 178, 180, 181, 182, 183, 184, 185 ],
    'j': [202, 203,   0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13 ],
    'k': [142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157 ],
    'l': [ 30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45 ],
    'm': [ 94,  95,  96,  97,  98,  99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109 ],
    'n': [208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223 ],
    'o': [ 14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29 ],
    }

# dict[channel] = obdt_connector
phi_translator = {}
for connector, channels in phi.items():
    for channel in channels:
        phi_translator[int(channel)] = connector

# dict[link][obdt_connector] = (station, sl, sl_connector)
link_obdtc_to_slc = {
    5 :{
        'a': [1, 1, '4A'],
        'b': [1, 1, '3B'],
        'c': [1, 1, '3A'],
        'd': [1, 1, '2B'],
        'e': [1, 1, '2A'],
        'f': [1, 1, '1B'],
        'g': [1, 1, '1A'],
        'h': [1, 3, '1A'],
        'i': [1, 3, '4A'],
        'j': [1, 3, '3B'],
        'k': [1, 3, '3A'],
        'l': [1, 3, '2B'],
        'm': [1, 3, '2A'],
        'n': [1, 3, '1B'],
    },
    2 :{
        'o': [1, 1, '4B'],
    }
}


# dict[link][obdt_connector] = list(channels)
link_dict = {
    2 : phi,
    5 : theta
}


# dict[link][channel] = obdt_connector
link_translator = {
    2 : phi_translator,
    5 : theta_translator
}



layer_order = [4,2,3,1]
# dict[sl_connector] = list(wires)
connector_to_wire = {
    '1A' : [ 0,  1,  2,  3],
    '1B' : [ 4,  5,  6,  7],
    '2A' : [ 8,  9, 10, 11],
    '2B' : [12, 13, 14, 15],
    '3A' : [16, 17, 18, 19],
    '3B' : [20, 21, 22, 23],
    '4A' : [24, 25, 26, 27],
    '4B' : [28, 29, 30, 31]
}



# dict[link][obdt][channel] = (layer, wire)
connector_translator = {}
for link, obdt_to_sl in link_obdtc_to_slc.items():
    connector_translator[link] = {}
    for obdt_c, (_,_,sl_c) in obdt_to_sl.items():
        if sl_c is None:
            connector_translator[link][obdt_c] = {None : (None, None)}
        else:
            connector_translator[link][obdt_c] = dict(zip(
                link_dict[link][obdt_c],
                [wl[::-1] for wl in list(itertools.product(connector_to_wire[sl_c], layer_order))]
            ))