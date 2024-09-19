import os
import json
import numpy as np
import pandas as pd
import itertools
# token laptop: 2678d4463c3c471fbb051fea50561272
# token pc: 0ca788318dd5416595d71d496db61e20
# parent = '/afs/ciemat.es/user/m/martialc/public/muTel/'
# parent = 'J:/public/muTel/'
parent = os.path.abspath(__file__).replace("\\",'/').split('src/muTel')[0]

superlayers = [1,2,3,4]
layers = [4,3,2,1] # (en orden de arriba a abajo)

ns = 1e-9


vdrift = 55e-3 #FIXME: mm/ns?
cell_height = 13 # mm
cell_width = 2*21 # mm
ncells = 16

sl_gap = 193 ##mm
sl_gap = 0
sheet_height = 1.5 #mm
sl_width = 2*sheet_height+len(layers) * cell_height

# sl_height = -(2*sheet_height+4*cell_height)*np.array([0,1,0,1]) - sl_gap*np.array([0,0,1,1])
sl_height = np.zeros(4) # Sistema de referencia local (a nivel de SL)

wire_height = -sheet_height-cell_height*np.arange(len(layers))-cell_height*0.5

wire_height = cell_height*np.r_[1.5,0.5,-0.5,-1.5]



# layer_offset = -cell_width*np.array((0,0.5,0,0.5))  # (l1,l2,l3,l4)
layer_offset = -cell_width*np.array((0.5,0,0.5,0))  # (l4,l3,l2,l1)   # Hace que el mínimo de x de la layer 4 sea 0

# with open(f'{parent}/src/muTel/dqm/config/muTel/pattern.json','r') as file:
#     patt_dict = json.load(file)

# with open(f'{parent}/src/muTel/dqm/config/muTel/laterality.json','r') as file:
#     lat_dict = json.load(file)

all_lats = list(map(lambda x: ''.join(x), itertools.product('LR',repeat=len(layers))))


# Usado para determinar la distancia máxima en hits en el algoritmo de emparejamiento.
hit_d_coef = 0.2



# Data Type dict, se puede usar para dar formato a los datos de los dataframes
data_type_dict = {
    'EventNr'   : np.int32,
    'GEO'       : np.int8,
    'hit'       : np.int16,
    'channel'   : np.int8,
    'sl'        : np.int8,
    'layer'     : np.int8,
    'cell'      : np.int8,
    'TDCtime'   : np.int16,
    'DriftTime' : float
}

muse_data_type_dict = {
    'GEO'       : pd.Int8Dtype(),
    'EventNr'   : pd.Int32Dtype(),
    'hit'       : pd.Int16Dtype(),
    'channel'   : pd.Int8Dtype(),
    'sl'        : pd.Int8Dtype(),
    'cell'      : pd.Int8Dtype(),
    'TDCtime'   : pd.Int16Dtype()
}