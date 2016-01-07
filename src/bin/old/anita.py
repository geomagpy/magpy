#!/usr/bin/env python
"""
MagPy - WIK analysis
"""
# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from stream import *

basispath = r'/home/leon/Dropbox/Daten/Magnetism/'

stDIDD = read(path_or_url=os.path.join(basispath,'DIDD-WIK','*'),starttime='2012-08-01', endtime='2012-09-01')
stDIDD.plot(['x','y','z','f'])
