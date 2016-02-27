#!/usr/bin/env python
"""
MagPy - Radon Analysis 
Automatically analyze Radon measurements from ZAMG FTPServer
"""

import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')

from magpy.stream import *   
from magpy.transfer import *

# Write BLV data
# ----------------
stream = read('/home/leon/Dropbox/Daten/Magnetism/DI-WIC/data/WIC-A2-FGE_S025_01.txt')
#stream = read('/home/leon/Dropbox/Daten/Magnetism/DI-WIK/data/WIK-D-LEMI_1_BE_01.txt')
stream.header['StationID'] = 'WIC'
stream.write('/home/leon/CronScripts/', format_type='BLV',fitfunc='poly',fitdegree=1)
x=1/0

# Read IMF data
# ----------------
stream = read('JUL2913.WIC')
stream.plot(['x'])

# Write IMF data
# ----------------
print "Reading data"
stream = read('gdas-aw_2013-07-29.txt')
print "Length: ", len(stream)
print stream.header
stream.header['StationID'] = 'WIC'
stream.header['DataAcquisitionLatitude'] = '48.2'
stream.header['DataAcquisitionLongitude'] = '15.3'
stream.header['DataSensorAzimuth'] = 3.4
print stream.header
stream.write('/home/leon/CronScripts/', format_type='IMF')
