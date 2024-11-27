from muTel.utils.meta import superlayers, layers
import pandas as pd
import numpy as np
import logging
from IPython.display import display

def f_4hits(df,sl):
    sl_slice = df[df['sl'] == sl].drop(['sl'],axis='columns')
    cond = sl_slice.groupby('EventNr').apply(lambda grp: (set(grp.layer) == layers)and(grp.layer.size==4))
    idx = cond[cond==True].index
    return df.loc[idx]


def f_4hits_inclusive(target, sl,**kwargs):
    logging.info(f'Estudiando SL {sl}')
    
    return f_4hits(target.df,sl)

def f_4hits_exclusive(target,sl,**kwargs):
    df = target.df
    for sl in superlayers:
        df = f_4hits(df,sl)
    return df

def f_3hits(df,sl):
    sl_slice = df[df['sl'] == sl].drop(['sl'],axis='columns')
    cond_3set =lambda grp: len(set(layers) - set(grp.layer)) == 1
    cond = sl_slice\
        .groupby('EventNr')\
        .apply(lambda grp: 
            (
                cond_3set(grp)
            )and(
                grp.layer.size==3
            )
        )
    idx = cond[cond==True].index
    return df.loc[idx]


def f_3n4hits(target,sl,**kwargs):
    df = target.df
    return pd.concat([f_4hits(df,sl),f_3hits(df,sl)])

f_dict = {
    '4hits_in' : f_4hits_inclusive,
    '4hits_ex' : f_4hits_exclusive,
    '3n4hits'  : f_3n4hits
}


