import numpy as np
from scipy.optimize import curve_fit
import pandas as pd
import matplotlib.pyplot as plt

def fit_T0(ser, tf_filter = None, nbins=100,
           axes=None,T0_corr=1.2):
    if isinstance(axes,list):
        for i,ax in enumerate(axes):
            if len(ax.get_lines()) == 0:
                break
    else:
        ax = None
    

    if tf_filter is None:
        t_min = ser.DriftTime.min()
        t_max = ser.DriftTime.max()
    else:
        t_min = tf_filter.tmin
        t_max = tf_filter.tmax

    def test_func(x, a, mu, sigma):
        return a * np.exp(-1/(2*sigma**2)*(x-mu)**2)

    time = ser.DriftTime.to_numpy()
    values, bins = np.histogram(time,range=(t_min,t_max),bins=nbins)
    grad = np.gradient(values,(t_max-t_min)/nbins)
    mids = (bins[:-1]+bins[1:])/2

    
    params, params_covariance = curve_fit(test_func, mids,grad, p0=[400, 700, 100])
    a, mu, sigma = params

    T0 = pd.Series([mu,abs(sigma)],index=['T0','dT0'])
    T0['T0'] = T0['T0'] - T0_corr*T0['dT0']

    
    if isinstance(ax,plt.Axes):
        ax.step(mids,grad,where='mid')
        x_plot = np.linspace(t_min,t_max,3*nbins)
        ax.plot(x_plot,test_func(x_plot,*params))
        ax.axvline(T0['T0'],linestyle='dashed',color='k')
        ax.set_title(f'SL {i+1}')
            


    return T0


