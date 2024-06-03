from muTel.dqm.classes.MuSL import MuSL
from pathlib import Path
from muTel.utils.paths import parent_directory, load_yaml


class MuChamber(object):
    _current_chamber = None
    def __init__(self,
        chamber
    ):
        self._chamber = chamber
        self._superlayers = {}
        self.set_current(self)
        
    #=====================================================================
    # INIT PROPERTIES
    #=====================================================================
    @property
    def chamber(self):
        return self._chamber
    @property
    def superlayers(self):
        return self._superlayers
    @property
    def sl(self):
        return self._superlayers
    
        
    #=====================================================================
    # CLASS CONSTRUCTION
    #=====================================================================
    @classmethod
    def from_preset(cls, chamber, preset, cfg = None):
        '''
        Will look for preset in parent/dqm/cfg/{cfg}/chambers.yaml and parent/src/muTel/dqm/cfg/{cfg}/chambers.yaml
        '''
        try:
            if not cfg:
                cfg_path = Path('cfg')
            else:
                cfg_path = Path(f'cfg/{cfg}')
                
            return MuChamber.from_config(chamber, preset, parent_directory / cfg_path)
        except IOError:
            pass
        
        try:
            if not cfg:
                cfg_path = Path('src/muTel/dqm/cfg')
            else:
                cfg_path = Path(f'src/muTel/dqm/cfg/{cfg}')
            return MuChamber.from_config(chamber, preset, parent_directory / cfg_path)
        except IOError:
            pass
    
    @classmethod
    def from_config(cls, chamber, preset, config_path):
        '''
        Will look for config in config_path
        '''
        chamber_cfg = load_yaml(config_path / Path('chambers'))[preset]
        chamber = cls(chamber)
        for sl, sl_cfg in chamber_cfg.items():
            musl = MuSL.from_config(sl, sl_cfg['preset'], config_path)
            musl.set_z0(sl_cfg.get('z0', 0))
            musl.set_vdrift(sl_cfg.get('vdrift', 55e-3))    # Default vdrift is 55 um/ns (55e-3 mm/ns)
            musl.set_t0(sl_cfg.get('t0', None))             
            musl._default_t0 = sl_cfg.get('default_t0', 0)
            
            chamber.add_sl(musl)
            
        return chamber
    #=====================================================================
    # INSTANCE METHODS
    #=====================================================================
    def add_sl(self, sl : MuSL):
        sl.at(chamber = self.chamber)
        self._superlayers[sl.sl] = sl
    
    @classmethod
    def set_current(cls,chamber):
        cls._current_chamber = chamber
    
    @classmethod
    def get_current(cls):
        return cls._current_chamber
    
    #=====================================================================
    # METHOD OVERLOAD   
    #=====================================================================
    def __str__(self):
        return f'{self.__class__.__name__}({self.chamber})'
    def __repr__(self):
        return str(self)
    def __getitem__(self,value):
        return self.sl[value]
    
    
if __name__ == '__main__':
    mutel = MuChamber.from_preset('muTel','default','muTel')
    print(mutel.sl)