# import matplotlib.pyplot as plt 
# from collections.abc import ABC, abstractmethod

# def extract_kwarg(kwargs,target,default=None):
#     val = kwargs.get(target, default)
#     if target in kwargs:
#         del kwargs[target]
#     return kwargs, val 

# class ReferenceFrame:
#     def __init__(self):
#         pass

# class Plot(abc):
#     @abstractmethod
#     def __init__(self,**kwargs):
#         # make a specific kind of plot (a spatial plot, a cmd, etc)
#         pass

#     @staticmethod
#     def plot(data, **kwargs):
#         pass

#     @abstractmethod
#     def plot(self,fig, ax, **kwargs):
#         # draw this plot onto the given fig and ax
#         pass


# # class Plotter()