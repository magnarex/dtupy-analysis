import threading
from functools import wraps
import time

#TODO: No almacenar los resultados del loop dentro del objeto MuTel, sino indicar al getter que haga un diccionario



class SLThread(threading.Thread):
    def __init__(self,**kwargs):
        # execute the base constructor
        threading.Thread.__init__(self,**kwargs)
        self.result = None
        # set a default value
 
    # function executed in a new thread
    def run(self,**kwargs):
        # block for a moment
        
        # store data in an instance variable
        try:
            if self._target:
                self.result = self._target(*self._args, **self._kwargs)
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs


# def TSiter():
#     def decorator(func):
#         """
#         Cada una de las iteraciones del loop que asigna los valores del parámetro. TS viene de Threading Setter.
#         """
#         @wraps(func)
#         def __set_iter__(self, lock, key, *args, **kwargs):
#             func_eval = func(self,*args,**kwargs)

#             with lock:
#                 self._logger.debug(f'Actualizando diccionario {set_attr_name} con la key {key}...')
#                 local_value = getattr(self, set_attr_name)
#                 local_value[key] = func_eval
#                 setattr(self,f'_{set_attr_name}',local_value)

#                 self._logger.debug(f'Llegando a la barrera con la key {key}')

#             return 
        
#         return __set_iter__
#     return decorator

def ThreadLoop(keys,func,name='Thread',*args,**kwargs):
    # setattr(self,f'_{set_attr_name}',{})
    # keys = getattr(self,key_attr)

    threads = {}

    for i, key in enumerate(keys):
        # kwargs_i = {kw : arg[key] for kw,arg in kwargs.items()}
        
        # self._logger.debug(f'Iniciando thread {name}_{str(i).zfill(2)}')
        thread = SLThread(
            target = func, name = f'{name}_{str(i).zfill(2)}',
            kwargs = dict(
                key     = key
            ) | kwargs
        )
        threads[key] = thread
        thread.start()

    for key, thread in threads.items():
        thread.join()

    result = {key : thread.result for key, thread in threads.items()}

    return result



# class SLproperty:
#     def __init__(
#         self=None,
#         fget=None,
#         fset=None,
#         fdel=None,
#         fthread=None,
#         doc=None
#     ):
#         """Attributes of 'SLproperty'
#         fget
#             function to be used for getting 
#             an attribute value
#         fset
#             function to be used for setting 
#             an attribute value
#         fdel
#             function to be used for deleting 
#             an attribute
#         fiter
#             function to be used for iterating
#             through the setting loop
#         doc
#             the docstring
#         """
        
#         self.fget = fget
#         self.fset = fset
#         self.fdel = fdel
#         self.fthread = fthread

#         if doc is None and fget is not None:
#             doc = fget.__doc__

#     def __get__(self, obj, objtype=None):
#         if obj is None:
#             return self
#         if self.fget is None:
#             raise AttributeError("unreadable attribute")
#         return self.fget(obj)

#     def __set__(self, obj, value):
#         if self.fset is None:
#             raise AttributeError("can't set attribute")
#         self.fset(obj, value)

#     def __delete__(self, obj):
#         if self.fdel is None:
#             raise AttributeError("can't delete attribute")
#         self.fdel(obj)

#     def __call__(self, obj, *args,**kwargs):
#         if self.fthread is None:
#             raise AttributeError("can't calc")
#         self.fthread(self,*args,**kwargs)
        










    
#     def getter(self, fget):
#         return type(self)(fget, self.fset, self.fdel, self.fthread, self.__doc__)

#     def setter(self, fset):
#         return type(self)(self.fget, fset, self.fdel, self.fthread, self.__doc__)

#     def deleter(self, fdel):
#         return type(self)(self.fget, self.fset, fdel, self.fthread, self.__doc__)
    
#     def threading(self,fthread):
#         return type(self)(self.fget, self.fset, self.fdel, fthread, self.__doc__)
    
    

