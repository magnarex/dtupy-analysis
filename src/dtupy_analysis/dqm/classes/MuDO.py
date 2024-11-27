from dtupy_analysis.utils.paths import load_yaml, config_directory
from pathlib import Path
import numpy as np


class MuDO:
    """
    Muon Detector Object
    """
    def __init__(self):
        pass
    
    def get_sl(self, kind):
        if kind in ['theta', 'phi']:
            return self._ssll[kind]
        else:
            raise ValueError("'kind' must be one of 'theta' or 'phi'")

    @classmethod
    def from_mapping(self, cfg_path):
        cfg = load_yaml(cfg_path, config_directory / Path('daq/mapping'))
        
        instance = MuDO()
        instance._ssll = {'phi' : [], 'theta' : []}
        
        instance._layers = cfg['layers']
        
        for (link, obdt_type) in cfg['links'].items():
            link_list = []
            for ssl in np.unique(np.stack(list(cfg['connectors'][link].values()))[:,:-1], axis=0):
                instance.ssll[obdt_type] += [[*ssl, layer] for layer in self.layers]
            
            