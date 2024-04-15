import yaml
from muTel.utils.paths import load_yaml, parent_directory
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt



class MuSL(object):
    def __init__(self,
        sl,
        orientation,
        layers,
        cells,
        cell_width,
        cell_height,
        sl_height,
        station     = None,
        chamber     = None,
        vdrift      = None,
        t0          = None,
        default_t0  = 0
    ):
        self._sl            = sl
        self._orientation   = orientation
        self._cells         = cells
        self._cell_width    = cell_width
        self._cell_height   = cell_height
        self._layers        = layers
        self._chamber       = chamber
        self._station       = station
        self._sl_height     = sl_height
        self._x0            = 0
        self._y0            = 0
        self._z0            = 0
        self._vdrift        = vdrift
        self._t0            = t0
        self._default_t0    = default_t0
        # Compute relative positions
        self._cell_shift    = np.c_[np.array(np.ceil(layers/2).astype(int)*[0,0.5])[:layers]] 
        # self._cell_shift   = np.c_[np.array(np.ceil(layers/2).astype(int)*[0.5,0])[:layers]] # Asuming top layer is shifted inwards
        self._cell_offset   = cell_width*self._cell_shift # Asuming top layer is shifted outwards
        
        # Wire position
        self._wire_z            = np.c_[cell_height*((layers-1)/2 - np.arange(layers))]
        self._wire_x            = cell_width*(0.5 + np.arange(cells)) + self._cell_offset
        
        # Cell walls
        self._wall_x           = cell_width*np.arange(cells+1) + self._cell_offset
        self._wall_z           = cell_height*(layers/2 - np.arange(layers+1))
        self._wall_z_top       = np.repeat(self._wire_z + 0.5*cell_height, cells+1)
        self._wall_z_bottom    = np.repeat(self._wire_z - 0.5*cell_height, cells+1)
        self._wall_x_bottom    = np.array(layers*[0]+[0.5])*cell_width
        self._wall_x_top       = np.array([0]+layers*[0.5])*cell_width + cells*cell_width
        
        #SL size
        self._width     = cells*cell_width
        self._height    = layers*cell_height
        
        #Geometric parametres
        self._phi_max   = np.arctan(self._cell_width/2/self._cell_height)
        
    #=====================================================================
    # INIT PROPERTIES
    #=====================================================================
    @property
    def sl(self):
        return self._sl
    @property
    def orientation(self):
        return self._orientation
    @property
    def layers(self):
        return self._layers
    @property
    def cells(self):
        return self._cells
    @property
    def cell_width(self):
        return self._cell_width
    @property
    def cell_height(self):
        return self._cell_height
    @property
    def chamber(self):
        return self._chamber
    @property
    def station(self):
        return self._station
    @property
    def sl_height(self):
        return self._sl_height
    @property
    def x0(self):
        return self._x0
    @property
    def y0(self):
        return self._y0
    @property
    def z0(self):
        return self._z0
    @property
    def vdrift(self):
        return self._vdrift
    @property
    def t0(self):
        if self._t0 is None:
            return self._default_t0
        else:
            return self._t0
    @property
    def default_t0(self):
        return self._default_t0
    #=====================================================================
    # CLASS CONSTRUCTION
    #=====================================================================
    @classmethod
    def from_preset(cls, sl, preset, cfg = None):
        '''
        Will look for preset in parent/dqm/cfg/{cfg}/superlayers.yaml and parent/src/muTel/dqm/cfg/{cfg}/superlayers.yaml
        '''
        try:
            if not cfg:
                cfg_path = Path('cfg')
            else:
                cfg_path = Path(f'cfg/{cfg}')
                
            return MuSL.from_config(sl, preset, parent_directory / cfg_path)
        except IOError:
            pass
        
        try:
            if not cfg:
                cfg_path = Path('src/muTel/dqm/cfg')
            else:
                cfg_path = Path(f'src/muTel/dqm/cfg/{cfg}')
            return MuSL.from_config(sl, preset, parent_directory / cfg_path)
        except IOError:
            pass
    @classmethod
    def from_config(cls, sl, preset, config_path):
        '''
        Will look for config in config_path
        '''
        
        sl_cfg = load_yaml(config_path / Path('superlayers'))[preset]
        
        return cls(sl,**sl_cfg)

    #=====================================================================
    # CLASS METHOD OVERLOAD
    #=====================================================================
    def __str__(self):
        string = f'SL{self.sl} ({self.orientation})'
        
        if not self.chamber:
            return string
        if (self.station is None) and (self.chamber is not None):
            return ' @ '.join([string, str(self.chamber)])
        else:
            return ' @ '.join([string, '.'.join([str(self.station),str(self.chamber)])])

    def __repr__(self):
        return str(self)

    #=====================================================================
    # INSTANCE PROPERTIES
    #=====================================================================
    @property
    def cell_shift(self):
        return self._cell_shift
    @property
    def cell_offset(self):
        return self._cell_offset
    @property
    def wire_z(self):
        return self._wire_z
    @property
    def wire_x(self):
        return self._wire_x
    @property
    def wall_z_bottom(self):
        return self._wall_z_bottom
    @property
    def wall_x_bottom(self):
        return self._wall_x_bottom
    @property
    def wall_z_top(self):
        return self._wall_z_top
    @property
    def wall_x_top(self):
        return self._wall_x_top
    @property
    def wall_z(self):
        return self._wall_z
    @property
    def wall_x(self):
        return self._wall_x
    @property
    def width(self):
        return self._width
    @property
    def height(self):
        return self._height
    @property
    def phi_max(self):
        return self._phi_max
    #=====================================================================
    # INSTANCE METHODS
    #=====================================================================

    def at(self, sl = None, station = None, chamber = None):
        if sl:      self._sl        = sl
        if station: self._station   = station
        if chamber: self._chamber   = chamber
    
    def set_offset(self, x = None, y = None, z = None):
        if x: self._x0 = x
        if y: self._y0 = y
        if z: self._z0 = z
    def set_x0(self, x):
        self.set_offset(x = x)
    def set_y0(self, y):
        self.set_offset(y = y)
    def set_z0(self, z):
        self.set_offset(z = z)
    
    def set_vdrift(self,vdrift):
        self._vdrift = vdrift
    def set_t0(self,t0):
        self._t0 = t0

    def plot_sl(self,ax : 'plt.Axes' = None, ratio = 1/22):
        if not ax:
            fig, ax = plt.subplots(1,1,figsize=(ratio*self.width,ratio*self.height))
        else:
            fig = ax.get_figure()
            
        ax.plot(self.wire_x,np.tile(self.wire_z, (1,self.cells)),linestyle = 'none', marker = 'o', color = 'k', zorder=-1)
        ax.vlines(self.wall_x,self.wall_z_bottom,self.wall_z_top)
        ax.hlines(self.wall_z,self.wall_x_bottom,self.wall_x_top)
        ax.set_xlim(
            -0.1*self.cell_width,
            (self.cells+0.6)*self.cell_width
        )
        ax.set_ylim(
            (-self.layers/2-0.1)  *self.cell_height,
            ( self.layers/2+0.1)  *self.cell_height
        )
        return fig, ax
    
if __name__ == '__main__':
    import matplotlib.pyplot as plt
    sl = MuSL.from_preset(1, 'SN', 'muTel')
    print(sl)    