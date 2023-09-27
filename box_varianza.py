#!/usr/bin/env python
# coding: utf-8

# #### Box varianza. Funziona solo con satpy dalla 0.41 in poi, quando è stato inserito nel lettore modis L2 il dataset mod05

# In[1]:


from glob import glob
import satpy
import xarray as xr
from pyresample.geometry import AreaDefinition, create_area_def, SwathDefinition
from datetime import datetime
from mast import utils


# In[2]:


scene = glob('/home/bornagain/mounting_point/osvaldo/storage/WORK/DATA/MODIS/MOD05_L2/2020/**/*.hdf',recursive=True)


# In[3]:


len(scene)


# In[5]:


area_id = 'thule'
description = 'Thule ice box'
proj_id = 'thule'
center = (-67.8,76.37)
#center = (-68,76)
radius = (1000,1000)
shape = (19,19)
resolution =1000

proj_dict = {'proj': 'tmerc', 'lat_0': center[1], 'lon_0': center[0] , 'lat_ts':center[1], 'a': 6371228.0, 'units': 'm'}

center = xr.DataArray([center[0], center[1]], attrs={"units": "degrees"})
area_def = AreaDefinition.from_area_of_interest(area_id, proj_dict, shape, center, resolution)

datasets=[]


# In[6]:


def run_cycle(element):
    try:
        scn = satpy.Scene([element], reader='modis_l2')
        scn.load(['water_vapor_infrared'])
        local_scn = scn.resample(area_def, nprocs=16, radius_of_influence=10000)
        local_scn_xr= local_scn.to_xarray_dataset()
        local_scn_xr=local_scn_xr.expand_dims(time=[local_scn_xr.start_time])
        datasets.append(local_scn_xr)
    except(ValueError):
            #print(Exception)
            pass


# #### Ciclo sequenziale

# In[7]:


#get_ipython().run_cell_magic('time', '', 'for element in scene:\n    run_cycle(element)\n')


# #### Ciclo parallelizzato
%%time
utils.parallel.parallel_CPU(run_cycle,scene)
# In[9]:


print(len(datasets))


# In[ ]:
