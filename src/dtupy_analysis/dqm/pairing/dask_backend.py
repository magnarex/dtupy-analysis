import dask.array as da

def matrix_diff(ddf, l_i, l_j, col, f= None):
    # Layer j > layer i
    # row N == hit N in layer j
    # col M == hit M in layer i
    
    ddf_j = da.reshape(ddf[ddf.layer == l_j][col].to_dask_array(lengths=True), (-1,1))
    ddf_i = da.reshape(ddf[ddf.layer == l_i][col].to_dask_array(lengths=True), (1,-1))

    if f:
        ddf_i = f(ddf_i)
        ddf_j = f(ddf_j)
    
    return ddf_j - ddf_i
