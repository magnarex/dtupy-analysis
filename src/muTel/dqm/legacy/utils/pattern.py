import json
from muTel import parent
from itertools import product
import numpy as np

with open(f'{parent}/src/muTel/dqm/cfg/muTel/pattern.json','r') as file:
    pattern_dict = json.load(file)

with open(f'{parent}/src/muTel/dqm/cfg/muTel/laterality.json','r') as file:
    laterality_dict = json.load(file)

def lats4patt(pattern_id, missing_hits):
    if 'X' in pattern_id:
        pats = [''.join(pat) for pat in product(*[pat_i.replace('X','LR') for pat_i in pattern_id])]
        
        lats = set()
        [[      lats.add(''.join(np.where(missing_hits,'X',list(lat))))
                for lat in pattern_dict[pat]['lats']
            ]   for pat in pats
        ]
        
            
        return lats
        
    else:
        return pattern_dict[pattern_id]['lats']


if __name__ == '__main__':
    from pprint import pprint
    # pprint(pattern_dict)
    # pprint(laterality_dict)
    print(lats4patt('RLX',[0,0,0,1]))