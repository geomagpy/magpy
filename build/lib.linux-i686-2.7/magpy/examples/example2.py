#!/usr/bin/env python
"""
MagPy - Plot analyzed DI data
"""

from magpy.stream import *   
from magpy.transfer import *

abspath = 'WIC-A2-FGE_S025_01.txt'
absst = read(path_or_url=abspath)
#absst = absst.remove_outlier(timerange=timerange,keys=['x','y','z','var1','var2','var3','var4','var5','dx','dy','dz','df'])
#absst = absst.remove_flagged()
absst = absst.sorting()

#['x','y','z']: 'Absolute values')
#['dx','dy','dz']: 'Base values')
#['var1','var2','var3','var4','var5']: 'Collimation angles')
#['df']: 'Delta F')
#['str1']: Observer
#['str2']: Instrument

absst.plot(['dx','dy','dz','var1','var2','var3','var4','var5','df'],plottitle='All')

# Filter by instrument
st = absst.extract('str2','T10A')
st.plot(['dx','dy','dz','var1','var2','var3','var4','var5','df'],plottitle='All T10A')

# Filter by observer
st = absst.extract('str1','Leichter')
st.plot(['dx','dy','dz','var1','var2','var3','var4','var5','df'],plottitle='All by Leichter')

st = absst.extract('str1','Berger')
st.plot(['dx','dy','dz','var1','var2','var3','var4','var5','df'],plottitle='All by Berger  ')

