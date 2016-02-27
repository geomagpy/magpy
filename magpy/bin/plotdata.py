#!/usr/bin/env python
"""
MagPy - Automatically analyze absolute values from CobsServer - WIK analysis
"""

import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')

# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from magpy.stream import *

starttime = '2013-06-16'
endtime = '2013-06-20'
#lemi25 = read(
gdas = read(path_or_url='/home/leon/Dropbox/Daten/Magnetism/FGE-WIC/IAGA/*',starttime=starttime, endtime=endtime)
#fge = read(path_or_url='/home/leon/Dropbox/Daten/Magnetism/FGE-WIC/data/*',starttime=starttime, endtime=endtime)
pos1 = read(path_or_url='/srv/data/GS-W-30/raw/pos1*',starttime=starttime, endtime=endtime)
#lemi = read(path_or_url='/srv/data/GS-W-30/raw/lemi*',starttime=starttime, endtime=endtime)
#env = read(path_or_url='/srv/data/GS-W-30/raw/env*',starttime=starttime, endtime=endtime)

pos1 = pos1.filter(filter_type='gauss',filter_width=timedelta(minutes=1))
pos1.plot(['f'])
#lemi.plot(['x','y','z','f','t1'])

stdiff = subtractStreams(gdas,pos1,keys=['f']) # Stream_a gets modified - stdiff = st1mod...

stdiff.plot(['f'])
#env.plot(['x'])

