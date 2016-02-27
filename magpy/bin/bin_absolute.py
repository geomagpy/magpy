#!/usr/bin/env python
"""
Example program to analyze a single absolute file
"""

import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')

from magpy.stream import *
from magpy.absolutes import *

import MySQLdb

#scalarstr = read('/home/leon/leon/Observatory/Messdaten/Data-Magnetism/GEM_download 2013-07-25/GEM_AS-A6-A5_2013-07-18.txt')
#scalarstr.plot(['f'])


date = '2013-10-23'
time = '07:17:00'

print "Reading absolute file..."
time = time.replace(':','-')
#abstr = absRead(path_or_url='/home/leon/Dropbox/Daten/Magnetism/DI-WIC/Comparison/'+date+'_'+time+'_A2_WIC.txt')
abstr = absRead(path_or_url='/home/leon/Dropbox/Daten/Magnetism/DI-WIC/raw/'+date+'_'+time+'_A2_WIC.txt')

print "Reading variometer data and interploating it ..."
variostr = read('/home/leon/Dropbox/Daten/Magnetism/FGE-WIC/IAGA/gdas-aw_'+date+'.txt')
vafunc = variostr.interpol(['x','y','z'])

print "Reading scalar data and interploating it ..."
scalarstr = read('/home/leon/Dropbox/Daten/Magnetism/FGE-WIC/IAGA/gdas-aw_'+date+'.txt')
#scalarstr = read('/srv/data/GS-W-30/raw/POS1_N432_0001_'+date+'.bin')
scfunc = scalarstr.interpol(['f'])

#variostr.plot(['x','y','z'])
scalarstr.plot(['f'])

print "Inserting variovalues at corresponding times into the DI object"
abstr = abstr._insert_function_values(vafunc)

print "Inserting scalar values at corresponding times into the DI object"
deltaF = 0
abstr = abstr._insert_function_values(scfunc,funckeys=['f'],offset=deltaF)

print "Determining DI values"
result = abstr.calcabsolutes(incstart=45,xstart=20000,ystart=0,unit='deg',printresults=True,debugmode=False)

print "Result before redating: ",result
resultstream = DataStream()
resultstream.add(result)

#resultnew = abstr.variocorr('2013-07-16T12:00:00',variostr,resultstream)
#print "Result: ",resultnew

# Use pmag to plt stereoplot with DI and alpha 95
