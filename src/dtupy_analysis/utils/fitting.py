from lmfit.models import GaussianModel
from lmfit import CompositeModel
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import linregress, chisquare

def fit_model(
    x,
    y,
    model,
    hist_range=(300,450),
    par_range = None,
    fit_range = None,
    pars=None,
    plot=False
):
    if par_range is None: par_range = hist_range
    if fit_range is None: fit_range = hist_range

    if not (isinstance(model,CompositeModel)) & (pars is None):
        where_par = (par_range[0] < x) & (x < par_range[1])
        pars = model.guess(y[where_par],x[where_par])
    else:
        pars = pars

    where_fit = (fit_range[0] < x) & (x < fit_range[1])
    fit = model.fit(y[where_fit],pars,x=x[where_fit],max_nfev =20000)


    if plot:
        fig,ax = plt.subplots(1,1)
        ax.step(x,y,color='C0',label='datos')
        ax.step(x[where_fit],fit.best_fit,color='k',label='best fit')
        plt.legend()
        fig.show()

    return fit

def fit_hist(
    x,
    bins=30,
    **kwargs
):
    cts, edges = np.histogram(x,bins=bins, range=kwargs['hist_range'])
    mids = (edges[1:]+edges[:-1])/2

    return fit_model(
        x=mids,
        y=cts,
        **kwargs
    )


def f_track(y, theta, x0):
    return y*np.tan(theta) + x0

def fit_f_track(x,y):
    'theta, x0, R2, chi2'
    slope, x0, r, p, se = linregress(y,x)
    theta = np.arctan(slope)
    chi2,_ = chisquare(x,f_track(y,theta,x0))
    return (theta, x0), r**2, chi2

