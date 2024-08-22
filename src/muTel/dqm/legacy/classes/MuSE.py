import muTel.utils.meta as meta
from muTel.dqm.classes.MuLE import MuLE, MuSH
from muTel.dqm.classes.MuChamber import MuChamber
from muTel.dqm.utils.pattern import pattern_dict, lats4patt
from muTel.dqm.utils.fitting import fit_thx0t0, fit_thx0, lat2coef, coef2lat
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import sys
from pprint import pprint
_MuData_logger = logging.Logger('MuON')
_MuData_logger.addHandler(logging.StreamHandler(sys.stdout))


# MuSE significa "Muon Superlayer Event", pero suena como "musa" en inglés

class MuSEType(type):
    def __repr__(self):
        return self.__name__
    
class MuSE(object, metaclass = MuSEType):
    __slots__ = ['_pattID','_data','_mules','_mushs','_new_mule','_new_mush','_id','_eventnr','_sl','_phi','_x0','_latID','_t0','_res','_chi2','_x0_dev','_t0_dev']
    def __init__(self, id = None) -> None:
        self._pattID = 'XXX'
        self._data = None

        self._new_mule = False
        self._new_mush = False

        self._mules = []
        self._mushs = []

        self._id = id
        self._eventnr = None
        self._sl = None
        
        # Trace attributes
        self._t0        = None
        self._phi       = None
        self._x0        = None
        self._res       = None
        self._chi2      = None
        self._x0_dev    = None
        self._t0_dev    = None

    def set_and_assert(self, mule : MuLE | MuSH):
        if self._eventnr is not None:
            try:
                assert self._eventnr == mule.eventnr
            except AssertionError:
                raise Exception(f'Hay hits de distintos eventos en este MuLE.')
        else:
            self._eventnr = mule.eventnr

        if self._sl is not None:
            try:
                assert self._sl == mule.sl
            except AssertionError:
                raise Exception(f'Hay hits de distintos eventos en este MuLE.')
        else:
            self._sl = mule.sl


    def add_mule(self, mule : MuLE):
        self.set_and_assert(mule)
        self._mules = self.mules + [mule]
        self._new_mule = True
        self.update_pattID()
        return self

    def add_mush(self, mush : MuSH):
        self.set_and_assert(mush)
        self._mushs = self.mushs + [mush]
        self._new_mush = True
        self.update_pattID()

        return self


    def update_pattID(self):
        if self._new_mule:
            mule = self.mules[-1]
            pattID_iloc = -int(mule.id[-1])
            patternID = mule.patternID

            pattID = list(self.pattID)
            pattID[pattID_iloc] = patternID
            self._pattID = ''.join(pattID)

        if (len(self.mules) == 1) and (len(self.mushs) == 1) and (self._new_mule or self._new_mush):
            df = self.df
            mush = self.mushs[-1]
            if mush.layer == 4:
                cell_diff = (df[df.layer == 2].cell - df[df.layer == 4].cell).values[0]
                pattID_iloc = 0                    
                    
            elif mush.layer == 1:
                cell_diff = (df[df.layer == 1].cell - df[df.layer == 3].cell).values[0]
                pattID_iloc = -1

            if   cell_diff ==  0: return self
            elif cell_diff == -1: patternID = 'L'
            elif cell_diff ==  1: patternID = 'R'
                    
            pattID = list(self.pattID)
            pattID[pattID_iloc] = patternID
            pattID[1]           = patternID
            self._pattID = ''.join(pattID)


    @property
    def layer(self):
        return self.data.layer
    
    @property
    def sl(self):
        if self._sl is None:
            sl_values = self.full_data.sl.values
            return sl_values[~np.isnan(sl_values)][0]
        else:
            return self._sl

    @property
    def eventnr(self):
        return self._eventnr

    @property
    def full_data(self):
        if self._data is None:
            data = pd.concat(
                [
                    *list(map(lambda mule: mule.data, self.mules)),
                    *list(map(lambda mush: mush.data, self.mushs))
                ])\
                .drop_duplicates()\
                .sort_values('layer',ascending=False)
                # .set_index('layer')\
                # .reindex(meta.layers)\
                # .sort_index(ascending=False)
                # .reset_index()\
                # .astype(meta.muse_data_type_dict)
            
            data['MuSEId'] = f'{self.eventnr}{self.id}'

            return data
        else:
            return self._data
                    
    @property
    def data(self):
        if self._data is None:
            return self.full_data.drop(['EventNr','sl'],axis='columns')
        else:
            return self._data.dropna()


    @property
    def nhits(self):
        if (len(self._mules) + len(self._mushs)) > 0:
            if len(self.mules) <= 1:
                return 2*len(self.mules) + len(self.mushs)
            else:
                return len(self.mules) + 1
        else:
            return len(self.data)
    
    @property
    def mules(self):
        return self._mules
    
    @property
    def mushs(self):
        return self._mushs
    
    @property
    def pattID(self):
        return self._pattID

    @property
    def allow_recon(self):
        return self.nhits >= 3

    @property
    def id(self):
        return self._id
    
    @property
    def ref_layer(self):
        return self.data.dropna().layer.values[0]
    
    @property
    def ref_cell(self):
        return self.data.dropna().cell.values[0]
    
    @property
    def cell_diff(self):
        return self.data.cell.values - self.ref_cell
    
    @property
    def chamber(self):
        return MuChamber.get_current()

    @property
    def musl(self):
        return self.chamber[self.sl]
    
    @property
    def cell(self):
        return self.full_data.cell

    @property
    def dt(self):
        return self.full_data.DriftTime
    
    
    def calc_pattID(self):
        diff2pat_dict = {1  : 'R',
                        -1 : 'L'}
        cells = self.cell + self.musl.cell_shift.ravel()
        cell_diff = np.diff(2*cells).astype(int)
        return ''.join([diff2pat_dict.get(diff,'X') for diff in cell_diff])
    
    # @property
    # def df(self):
    #     t_d = []
    #     for layer in meta.layers:
    #         if layer in self.layer.tolist(): t_d.append(self.data[self.data.layer == layer].DriftTime.iloc[0])
    #         else: t_d.append(np.nan)
        
    #     return pd.Series(
    #         data=dict(zip(
    #             ['EventNr','sl','nhits',*[f't{l}' for l in meta.layers], 'patternID', 'MuSE'],
    #             [self.eventnr, self.sl, self.nhits, *t_d, self.patternID, self]
    #             ))
    #     ).to_frame().transpose()
    
    
    
    @property
    def df(self):
        return self.data
            
    def fit(self,maxfev=10000,do_plot=True):
        if (self.nhits == 4):
            return self.fit_4hits(maxfev=maxfev, do_plot=do_plot)
        elif (self.nhits == 3):
            return self.fit_3hits(maxfev=maxfev, do_plot=do_plot)
    
    def fit_4hits(self,maxfev=10000,do_plot = False):
        dt  = self.dt.values
        z   = self.musl.wire_z.ravel()
        v   = self.musl.vdrift
        xn  = (self.cell+self.musl.cell_shift.ravel()-0.5)*self.musl.cell_width # The origin is in the leftmost side of the first cell on the top layer
        xn = xn.values
        xn_0 = np.nanmean(xn)
        
                
        bounds      = [
                        [-self.musl.phi_max, -1.5*self.musl.cell_width,                     np.nanmax(dt[1:-1])-self.musl.cell_width/2/v   ], # The middle ones to better restrain the angle
                        [ self.musl.phi_max, (self.musl.cells+1.5)*self.musl.cell_width,    np.nanmax(dt[1:-1])                            ],
                    ]
        p0          =   [0,                  xn_0,                                          max(self.musl.default_t0,bounds[0][2])   ]
        
        fit_result_list = []
        for lat_id in lats4patt(self.pattID,np.isnan(dt)):
            c = lat2coef(lat_id)
            (phi,x0,t0), res, chi2, param_dev = fit_thx0t0(z,c,xn,v,dt,p0=p0,bounds=bounds,maxfev=maxfev)
            fit_result = {
                'latID'         : lat_id,
                'phi'           : phi,
                'x0'            : x0,
                't0'            : t0,
                'res'           : res,
                'chi2'          : chi2,
                'x0_dev'        : param_dev[1],
                't0_dev'        : param_dev[2],
            }
            if do_plot:
                x_obvs = xn + c*v*(dt-t0)
                x_expc = x0 + z*np.tan(phi)
                
                fig, ax = self.musl.plot_sl()
                ax.plot(
                    x_obvs,
                    z,
                    linestyle='none',
                    marker = 'x',
                    label='fitted'
                )
                ax.plot(
                    x_expc,
                    z,
                    linestyle='solid',
                    marker = 'none',
                    label='trace'
                )
                ax.plot(
                    (dt-self.musl.default_t0)*c*v+xn,
                    z,
                    linestyle='none',
                    marker = 'o',
                    label = 'default',
                    mfc='none',
                    color='red'
                )
                
                ax.plot(
                    xn,
                    z,
                    linestyle='none',
                    marker = 'o',
                    color = 'green'
                )
                patches = [
                    ax.add_patch(Rectangle(
                        (x-self.musl.cell_width/2, y-self.musl.cell_height/2),
                        self.musl.cell_width,
                        self.musl.cell_height,
                        zorder = -3,
                        facecolor = 'xkcd:dull yellow'
                    ))
                    for x,y in zip(xn,z)
                ]
                ax.legend()
                
                fig.suptitle(f'{lat_id} (chi2 = {chi2:.3f})')
                fit_result['fig'] = fig

            fit_result_list.append(fit_result)
            
            
            
        
        fit_df = pd.DataFrame(fit_result_list)
        best_fit = fit_df[fit_df.chi2 == fit_df.chi2.min()]
        for attr in best_fit.columns:
            if attr == 'fig': continue
            setattr(self, f'_{attr}', best_fit[attr].values[0])
            
        return fit_df.set_index('latID').sort_values('chi2')
        
    
    def fit_3hits(self,maxfev=10000,do_plot = False):
        dt  = self.dt.values
        z   = self.musl.wire_z.ravel()
        xn  = (self.cell+self.musl.cell_shift.ravel()-0.5)*self.musl.cell_width # The origin is in the leftmost side of the first cell on the top layer
        xn = xn.values
        xn_0 = np.nanmean(xn)
        v   = self.musl.vdrift
        t0  = self.musl.t0
        
                
        p0          =   [0,                  xn_0,                                        ]
        bounds      = [
                        [-self.musl.phi_max, -1.5*self.musl.cell_width,                   ],
                        [ self.musl.phi_max, (self.musl.cells+1.5)*self.musl.cell_width,  ],
                    ]
        
        fit_result_list = []
        for lat_id in lats4patt(self.pattID,np.isnan(dt)):
            c = lat2coef(lat_id)
            (phi,x0), res, chi2, param_dev = fit_thx0(z,c,xn,v,t0,dt,p0=p0,bounds=bounds,maxfev=maxfev)
            fit_result = {
                'latID'         : lat_id,
                'phi'           : phi,
                'x0'            : x0,
                't0'            : t0,
                'res'           : res,
                'chi2'          : chi2,
                'x0_dev'        : param_dev[1],
                't0_dev'        : t0-self.musl.default_t0,
            }
            if do_plot:
                x_obvs = xn + c*v*(dt-t0)
                x_expc = x0 + z*np.tan(phi)
                
                fig, ax = self.musl.plot_sl()
                ax.plot(
                    x_obvs,
                    z,
                    linestyle='none',
                    marker = 'x',
                    label='fitted'
                )
                ax.plot(
                    x_expc,
                    z,
                    linestyle='solid',
                    marker = 'none',
                    label='trace'
                )
                ax.plot(
                    xn,
                    z,
                    linestyle='none',
                    marker = 'o',
                    color = 'green'
                )
                patches = [
                    ax.add_patch(Rectangle(
                        (x-self.musl.cell_width/2, y-self.musl.cell_height/2),
                        self.musl.cell_width,
                        self.musl.cell_height,
                        zorder = -3,
                        facecolor = 'xkcd:dull yellow'
                    ))
                    for x,y in zip(xn,z)
                ]
                ax.legend()
                
                fig.suptitle(f'{lat_id} (chi2 = {chi2:.3f})')
                fit_result['fig'] = fig

            fit_result_list.append(fit_result)
            
            
            
        
        fit_df = pd.DataFrame(fit_result_list)
        best_fit = fit_df[fit_df.chi2 == fit_df.chi2.min()]
        for attr in best_fit.columns:
            if attr == 'fig': continue
            setattr(self, f'_{attr}', best_fit[attr].values[0])
            
        return fit_df.set_index('latID').sort_values('chi2')
    
    
    
    @classmethod
    def from_musedata(cls,muse_data, do_fit = False, do_plot = False):
        id = muse_data.MuSEId.unique()
        try:
            assert len(id) == 1
        except AssertionError:
            raise ValueError('The dataframe has different Events mixed up!')
        muse = MuSE(id)
        muse._sl = muse_data.sl.dropna().unique()[0]
        muse._data = muse_data.set_index('layer').reindex(range(1,muse.musl.layers+1)).sort_index(ascending=False)
        muse._pattID = muse.calc_pattID()
        if do_fit: muse.fit(do_plot = do_plot)
        return muse
    # @property
    # def theta(self):
    #     return self._theta
    
    # @property
    # def x0(self):
    #     return self._x0
    
    # def set_pars(self,pars):
    #     self._x0, self._theta = pars