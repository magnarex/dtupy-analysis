def is_notebook():
    try:
        # Verifica si `get_ipython` existe y si el tipo de shell es de Jupyter
        from IPython import get_ipython
        if 'IPKernelApp' in get_ipython().config:  # `IPKernelApp` indica un kernel de Jupyter
            return True
        else:
            return False
    except ImportError:
        # Si IPython no está disponible, no estamos en un notebook
        return False