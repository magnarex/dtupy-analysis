from typing import TYPE_CHECKING

import numpy                    as np
import matplotlib.pyplot        as plt

from ...core    import units    as u

from ..         import get_fits, get_fit, get_z, get_t

if TYPE_CHECKING:
    from . import MuSE

class MuSeg(object):
    __module__ = 'dtupy_analysis.dqm.reco'
    def __init__(self, muse : 'MuSE', lat_coef, v_drift = None, t0 = None):
        # Read-only attribute
        self._muse      = muse
        self._lat_coef  = lat_coef
        self._v_drift   = v_drift
        self._t0        = t0
        
        self._args      = None
        self._cov       = None
    @property
    def t(self):
        return self.muse.time_cor.to_numpy()[~self.muse.missing]
    @property
    def z(self):
        return self.muse.wire_z.to_numpy()[~self.muse.missing]
    @property
    def muse(self):
        return self._muse
    @property
    def lat_coef(self):
        return self._lat_coef[~self.muse.missing]
    @property
    def wire_x(self):
        return self.muse.wire_x.to_numpy()[~self.muse.missing]
    @property
    def f_fit(self):
        return get_fit(self.lat_coef, self.wire_x, self._v_drift, self._t0)
    @property
    def f_z(self):
        return get_z(self.lat_coef, self.wire_x, self._v_drift, self._t0)
    @property
    def f_t(self):
        return get_t(self.lat_coef, self.wire_x, self._v_drift, self._t0)

    
    @property
    def bounds(self):
        # if (self.muse.nlayers % 2) == 0:
        #     xmin = min(self.muse.df.wire_x.loc[self.muse.nlayers//2], self.muse.df.wire_x.loc[self.muse.nlayers//2+1])
        #     xmax = max(self.muse.df.wire_x.loc[self.muse.nlayers//2], self.muse.df.wire_x.loc[self.muse.nlayers//2+1])
        # else:
        xmin = self.muse.df.wire_x.loc[self.muse.nlayers//2+1] - (self.muse.nlayers//2+1)*u.CELL_WIDTH
        xmax = self.muse.df.wire_x.loc[self.muse.nlayers//2+1] + (self.muse.nlayers//2+1)*u.CELL_WIDTH
        if (not self.default_v_drift) & (not self.default_t0):
            bounds = (
                        [-np.pi/2, xmin, 0     , -np.inf      ],
                        [np.pi/2, xmax, 100e-3, self.t.min() ]
                    )
        elif (self.default_v_drift) & (not self.default_t0):
            t0_min = self.t.max()-(u.CELL_WIDTH/self.default_v_drift)
            bounds = (
                        [-np.pi/2, 0, min(-1e-9, t0_min)],
                        [np.pi/2, u.CELL_WIDTH*u.MAX_CELLS, self.t.min() ]
                    )
        elif (not self.default_v_drift) & (self.default_t0):
            bounds = (
                        [-np.pi/2, xmin, 0      ],
                        [np.pi/2, xmax, 100e-3 ]
                    )
        else:
            bounds = (
                        [-np.pi/2, xmin ],
                        [np.pi/2, xmax ]
                    )
        return bounds

    @property
    def p0(self):
        dx = self.wire_x[0] - self.wire_x[-1]
        dz = (self.muse.data.index.values[0] - self.muse.data.index.values[-1]) * u.CELL_HEIGHT
        # theta0 = np.arctan(dx/dz)
        theta0 = 0
        xmean =  (self.bounds[0][1] + self.bounds[1][1])/2
        
        if (not self.default_v_drift) & (not self.default_t0):
            p0 = [theta0, xmean, u.V_DRIFT, 0   ]
        elif (self.default_v_drift) & (not self.default_t0):
            t0_0 =  (self.bounds[0][-1] + self.bounds[1][-1])/2
            p0 = [theta0, xmean, t0_0   ]
        elif (not self.default_v_drift) & (self.default_t0):
            p0 = [theta0, xmean, u.V_DRIFT]
        else:
            p0 = [theta0, xmean]
        return p0

    def fit(self):
        from scipy.optimize import curve_fit
        try:
            args, cov = curve_fit(self.f_fit, self.z, self.t,
                                    bounds  = self.bounds,
                                    p0      = self.p0
                                )
        except RuntimeError as e:
            raise e
        except ValueError as e:
            print(self.p0, self.bounds)
            raise e
        
        self.args = args
        self._cov  = cov
        
        
        return args, cov
    
    def plot(self, ax = None):
        from ..core import plot_cells
        if ax is None:
            ax = plt.gca()
        plot_cells(self.muse, ax = ax)
        
        if self.is_fitted:
            _, f_z, f_t = get_fits(self.lat_coef, self.wire_x, self._v_drift, self._t0)
            
            ax.set_title(f'{self.latsID} : {self.chi2:.2e}')
            ax.plot(f_t(self.t, *self.args[2:]), self.z, 'x')
            ylim = ax.get_ylim()

            z_plot = np.linspace(ylim[0]*1.5, ylim[1]*1.5, 10)
            ax.plot(f_z(z_plot, *self.args[:2]), z_plot, '--')

            ax.set_ylim(*ylim)
            ax.set_ylabel('z [mm]')
            ax.set_xlabel('x [mm]')


        
    @property
    def latsID(self):
        return str(self._lat_coef)
    @property
    def theta(self):
        if self.is_fitted: return self.args[0]
    @property
    def x0(self):
        if self.is_fitted: return self.args[1]
    @property
    def t0(self):
        if self.default_t0:
            return self._t0
        else:
            if self.is_fitted: return self.args[-1]
        
    @property
    def v_drift(self):
        if self.default_v_drift:
            return self._v_drift
        else:
            if self.is_fitted: return self.args[2]

    @property
    def is_fitted(self):
        return not (self.args is None)
    
    @property
    def default_t0(self):
        return not (self._t0 is None)
    @property
    def default_v_drift(self):
        return not (self._v_drift is None)
    
    @property
    def args(self):
        return self._args
    @args.setter
    def args(self, values):
        self._args = args = values
        x_dt  = self.f_t(self.t, *args[2:])
        x_fit = self.f_z(self.z, *args[:2])            
        
        self._res  = res  = x_fit - x_dt
        chi2 = np.sum((res)**2)
        
        # TODO: Maybe penalize inf chi2 is 0?
        self._chi2 = chi2
        # if chi2 == 0:
        #     self._chi2 = np.inf
        # else:
        #     self._chi2 = chi2

    
    @property
    def res(self):
        if self.is_fitted: return self._res
    @property
    def chi2(self):
        if self.is_fitted: return self._chi2