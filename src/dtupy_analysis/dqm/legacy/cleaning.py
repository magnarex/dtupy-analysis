import numpy as np
import pandas as pd
from muTel.utils.data import read_muon, lookup

def reindex(df):
    '''
    Toma un DataFrame de pandas como el que devuelve la función read_muon
    del módulo muTel.utils.data y comprime las coordenadas en un mapa con un
    sólo índice.
    '''

    '''
    TODO:   Hacerlo fila a fila de los datos es muy ineficiente porque son muchas,
            hay que cambiarlo para hacerlo por cada fila del mapa. Esto se puede
            hacer iteranco con un bucle for en las filas del mapa.

    '''
    
    hit_pos = df[lookup.index.names]
    hit_pos = hit_pos.apply(reindex_row , axis=1).astype(int)
    return hit_pos

def reindex_row(row):
    try:
        return int(lookup.loc[tuple(row)]['index'])
    except KeyError:
        return None




def main():
    from muTel.utils.data import read_muon
    # print(chamber_df)
    muon_df = read_muon(588,sl=1,nhits=4)
    print(reindex(muon_df))




if __name__ == '__main__':
    main()