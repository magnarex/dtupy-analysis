import numpy as np
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt


def heights_to_drifttime(z, c, xn, theta, x0, t0, v):
    return c/v*(z*np.tan(theta)+x0-xn)+t0

def h2dt(z, c, xn, theta, x0, t0, v):
    return heights_to_drifttime(z, c, xn, theta, x0, t0, v)


def h2dt_zcxnv(x,theta,x0,t0):
    '''
    Heights to drifttime where z is a matrix with rows z, c, xn and v.
    '''
    z   = x[0]
    c   = x[1]
    xn  = x[2]
    v   = x[3]
    return h2dt(z,c,xn,theta,x0,t0,v)





def fit_thx0t0(z,c,xn,v,dt, p0 = None, bounds = None, maxfev = 10000):
    '''
    Fit to obtain theta, x0, t0.
    '''
    
    not_na = np.where(~np.isnan(dt))[0]
    
    # for i in range(len(p0)):
    #     print(f'{bounds[0][i]} < {p0[i]} < {bounds[1][i]}')
    
    z_arr = np.zeros((4,dt.size))
    z_arr[0] = z 
    z_arr[1] = c 
    z_arr[2] = xn
    z_arr[3] = v
    
    args, cov = curve_fit(h2dt_zcxnv,z_arr[:,not_na],dt[not_na],
                                        p0 = p0,
                                        bounds = bounds,
                                        maxfev = maxfev
                                    )
    theta, x0, t0 = args
    
    param_dev = np.array(args) - np.array(p0)
    
    x_obvs = xn + c*v*(dt-t0)
    x_expc = x0 + z*np.tan(theta)
    res = x_obvs - x_expc
    chi2 = np.nansum(res**2/np.abs(x_expc))    
        
    
    return (theta,x0,t0), res, chi2, param_dev

def h2dt_zcxnvt0(x,theta,x0):
    '''
    Heights to drifttime where z is a matrix with rows z, c, xn and v.
    '''
    z   = x[0]
    c   = x[1]
    xn  = x[2]
    v   = x[3]
    t0  = x[4]
    
    return h2dt(z,c,xn,theta,x0,t0,v)



def fit_thx0(z,c,xn,v,t0,dt, p0 = None, bounds = None, maxfev = 10000):
    '''
    Fit to obtain theta, x0, t0.
    '''
    
    not_na = np.where(~np.isnan(dt))[0]
    
    # for i in range(len(p0)):
    #     print(f'{bounds[0][i]} < {p0[i]} < {bounds[1][i]}')
    
    z_arr = np.zeros((5,dt.size))
    z_arr[0] = z 
    z_arr[1] = c 
    z_arr[2] = xn
    z_arr[3] = v
    z_arr[4] = t0
    
    args, cov = curve_fit(h2dt_zcxnvt0,z_arr[:,not_na],dt[not_na],
                                        p0 = p0,
                                        bounds = bounds,
                                        maxfev = maxfev
                                    )
    theta, x0 = args
    
    param_dev = np.array(args) - np.array(p0)
    
    x_obvs = xn + c*v*(dt-t0)
    x_expc = x0 + z*np.tan(theta)
    res = x_obvs - x_expc
    chi2 = np.nansum(res**2/np.abs(x_expc))    
        
    
    return (theta,x0), res, chi2, param_dev


def lat2coef(lat):
    coef_dict = {'L' : -1,
                 'R' :  1}
    return np.array([coef_dict.get(lat_i,1) for lat_i in lat])

def coef2lat(coef):
    lat_dict =  {-1  : 'L',
                  1  : 'R'}
    return ''.join([lat_dict.get(coef_i,'X') for coef_i in coef])






    
if __name__ == '__main__':
    width       = 42
    height      = 13
    v           =  55e-3

    lat_id_list = ['LLRL', 'RLRL', 'LLRR', 'RLRR']
    dt          = np.array([ 736.71875,  1060.15625,     725.78125,      935.9375    ])
    z           = np.array([ 19.5,       6.5,            -6.5,           -19.5       ])
    cells       = np.array([ 14,         14,             14,             14          ], dtype=np.int8)
    cell_shift  = np.array([ 0.,         0.5,            0.,             0.5         ])
    xn          = (cells + cell_shift - cells[0])*width

    p0          = [0, 0, 710]
    bounds      = [
                    [-width/height, -width/2, 0],
                    [ width/height,  width/2, np.inf],
                ]

    for lat_id in lat_id_list:
        c = lat2coef(lat_id)
        (phi,x0,t0), res, chi2, param_dev = fit_thx0t0(z,c,v,xn,dt,p0=p0,bounds=bounds)
        print(
            f'{lat_id}\n'
            f'  v_drift:\t{v*1e3:2.2f} um/ns\n'
            f'  t0:     \t{t0:3.1f} ns\n'
            f'  phi:    \t{theta/np.pi:.2f}π ({180*theta/np.pi:.2f}º)\n'
            f'  x0:     \t{x0/width:.2f}·width\n'
            f'  chi2:   \t{chi2:.3f}\n'
        )
        