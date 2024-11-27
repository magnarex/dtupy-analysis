import yaml
from pathlib import Path

import numpy                as np
import matplotlib.pyplot    as plt

from ...utils.paths import config_directory, load_yaml
# The schema is top down, meaning LRL is {4w3: L, 3w2 : R, 2w1: L} and
# LRLR is {4 : L, 3 : R, 2 : L, 1 : R}

pat_dict = load_yaml('dqm/reco/pattern.yaml', config_directory)

def get_fits(lat_coef, wire_x, v_drift = None, t0 = None):
    '''
    Returns a tuple of:
     - fit: The function used by curve_fit
     - f_z: The function used with (z, theta, x0)
     - f_t: The function used with (t, *v_drift, *t0)
        
    The parametres with * are only required depending on whether they were given to the function
    in the first place.
    '''
        
    return  (get_fit(lat_coef, wire_x, v_drift = v_drift, t0 = t0),
            get_z(lat_coef, wire_x, v_drift = v_drift, t0 = t0),
            get_t(lat_coef, wire_x, v_drift = v_drift, t0 = t0))

def get_fit(lat_coef, wire_x, v_drift = None, t0 = None):
    default_v_drift = not (v_drift is None)
    default_t0      = not (t0 is None)
    
    if (not default_v_drift) & (not default_t0):
        def f_fit(z, theta, x0, v_drift, t0):
            return lat_coef/v_drift*(z*np.tan(theta)+x0-wire_x) + t0
        
    elif (default_v_drift) & (not default_t0):
        def f_fit(z, theta, x0, t0):
            return lat_coef/v_drift*(z*np.tan(theta)+x0-wire_x) + t0
        
    elif (not default_v_drift) & (default_t0):
        def f_fit(z, theta, x0, v_drift):
            return lat_coef/v_drift*(z*np.tan(theta)+x0-wire_x) + t0   
            
    else:
        def f_fit(z, theta, x0):
            return lat_coef/v_drift*(z*np.tan(theta)+x0-wire_x) + t0
    
    return f_fit

def get_z(lat_coef, wire_x, v_drift = None, t0 = None):
    def f_z(z, theta, x0):
        return z*np.tan(theta)+x0
    return f_z

def get_t(lat_coef, wire_x, v_drift = None, t0 = None):
    default_v_drift = not (v_drift is None)
    default_t0      = not (t0 is None)

    if (not default_v_drift) & (not default_t0):
        # print('Getting f_t with v_drift and t0 as free parameters')
        def f_t(t, v_drift, t0):
            return lat_coef*v_drift*(t - t0) + wire_x

    elif (default_v_drift) & (not default_t0):
        # print('Getting f_t with t0 as a free parameter')
        def f_t(t, t0):
            return lat_coef*v_drift*(t - t0) + wire_x

    elif (not default_v_drift) & (default_t0):
        # print('Getting f_t with v_drift as a free parameter')
        def f_t(t, v_drift):
            return lat_coef*v_drift*(t - t0) + wire_x

    else:
        # print('Getting f_t with v_drift and t0 as fixed parameters')
        def f_t(t):
            return lat_coef*v_drift*(t - t0) + wire_x
        
    return f_t

def plot_cells(muse, ax = None):
    from ...dqm         import units as u
    nlayers = len(muse.df)
    x_ref = muse.df.wire_x.loc[4]
    if np.isnan(x_ref): x_ref = muse.df.wire_x.loc[2]
    
    if ax is None:
        ax = plt.gca()
    heights = muse.df.wire_z
    wires_x = 0.5*u.CELL_WIDTH*np.r_[*[(2*np.arange(i+1) - i) for i in range(nlayers)]] + x_ref
    wires_y = np.r_[*[np.repeat(heights.iloc[i],i+1) for i in range(nlayers)]]
    ax.plot(
        wires_x,
        wires_y,
        linestyle='none',
        marker = 'o',
        color = 'k'
    )
    ax.plot(
        muse.df.wire_x,
        heights,
        linestyle='none',
        marker = 'o',
        color = 'green'
    )
    wall_x = 0.5*u.CELL_WIDTH*np.r_[*[(2*np.arange(i+2) - i) for i in range(nlayers)]] + x_ref - 0.5*u.CELL_WIDTH
    wall_y = np.r_[*[np.repeat(heights.iloc[i],i+2) for i in range(nlayers)]]
    ax.vlines(wall_x, wall_y - u.CELL_HEIGHT/2, wall_y + u.CELL_HEIGHT/2, 'k', lw = 1)

    ceil_x = np.array([0.5*u.CELL_WIDTH*(i+1) for i in range(nlayers)])
    ceil_x = np.r_[ceil_x, ceil_x[-1]]
    
    ax.hlines(np.r_[heights, heights.iloc[-1]-u.CELL_HEIGHT] + 0.5*u.CELL_HEIGHT, x_ref +  ceil_x, x_ref - ceil_x, 'k', lw = 1)
    try:
        ax.set_xlim(np.min(wall_x) - .2*u.CELL_WIDTH, np.max(wall_x) + .2*u.CELL_WIDTH)
    except ValueError:
        pass
    ax.set_ylim(np.min(heights) - .7*u.CELL_HEIGHT, np.max(heights) + .7*u.CELL_HEIGHT)