#!/usr/bin/env python
"""
MagPy - WIK analysis
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from core.magpy_stream import *
from core.magpy_absolutes import *

# general definitions
mainpath = r'/home/leon/Dropbox/Daten/Magnetism/'
year = 2009
"""
# --------------------
# create working files
# --------------------
# here we read raw data from the instruments, flag them,
# merge them with additional parameters like temperature
# and save them to the working directory

# Start with DIDD values and read yearly fractions
# 1. Get data
st1 = pmRead(path_or_url=os.path.join(mainpath,'DIDD-WIK','*'),starttime= str(year)+'-01-01', endtime=str(year+1)+'-01-01')
# 2. Merge auxilliary data
aux1 = pmRead(path_or_url=os.path.join(mainpath,'TEMP-WIK','Schacht*'))
aux1 = aux1.date_offset(-timedelta(hours=2)) # correcting times e.g. MET to UTC
aux1 = aux1.filtered(filter_type='gauss',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30),respect_flags=True)
Tserialnr = aux1.header['InstrumentSerialNum']
stDIDD = mergeStreams(st1,aux1,keys=['t1','var1'])
# 3. Flagging list (last updated 07.9.2012 by leon)
stDIDD = stDIDD.flag_stream('x',3,"Water income",datetime(2009,2,28,16,35,0,0),datetime(2009,5,9,8,21,0,0))
stDIDD = stDIDD.flag_stream('y',3,"Water income",datetime(2009,2,28,16,35,0,0),datetime(2009,5,9,8,21,0,0))
stDIDD = stDIDD.flag_stream('z',3,"Water income",datetime(2009,2,28,16,35,0,0),datetime(2009,5,9,8,21,0,0))
stDIDD = stDIDD.flag_stream('f',3,"Water income",datetime(2009,2,28,16,35,0,0),datetime(2009,5,9,8,21,0,0))
#
stDIDD = stDIDD.flag_stream('x',3,"Mowing lawn",datetime(2009,12,1,6,24,0,0),datetime(2009,12,1,7,58,0,0))
stDIDD = stDIDD.flag_stream('y',3,"Mowing lawn",datetime(2009,12,1,6,24,0,0),datetime(2009,12,1,7,58,0,0))
stDIDD = stDIDD.flag_stream('z',3,"Mowing lawn",datetime(2009,12,1,6,24,0,0),datetime(2009,12,1,7,58,0,0))
stDIDD = stDIDD.flag_stream('f',3,"Mowing lawn",datetime(2009,12,1,6,24,0,0),datetime(2009,12,1,7,58,0,0))
#
stDIDD = stDIDD.flag_stream('x',3,"Mowing lawn",datetime(2009,12,1,10,57,0,0),datetime(2009,12,1,11,12,0,0))
stDIDD = stDIDD.flag_stream('y',3,"Mowing lawn",datetime(2009,12,1,10,57,0,0),datetime(2009,12,1,11,12,0,0))
stDIDD = stDIDD.flag_stream('z',3,"Mowing lawn",datetime(2009,12,1,10,57,0,0),datetime(2009,12,1,11,12,0,0))
stDIDD = stDIDD.flag_stream('f',3,"Mowing lawn",datetime(2009,12,1,10,57,0,0),datetime(2009,12,1,11,12,0,0))
#

# 4. Provide Meta information (last updated 07.9.2012 by leon)
headers = stDIDD.header
headers['Instrument'] = 'DIDD'
headers['InstrumentSerialNum'] = 'not known'
headers['InstrumentOrientation'] = 'xyz'
headers['Azimuth'] = '0 deg'
headers['Tilt'] = '0 deg'
headers['InstrumentPeer'] = 'Shaft'
headers['InstrumentDataLogger'] = 'Magrec1.0'
headers['ProvidedComp'] = 'xyzf'
headers['ProvidedInterval'] = 'min'
headers['ProvidedType'] = 'variation'
headers['DigitalSamplingInterval'] = '8 sec'
headers['DigitalFilter'] = 'Gauss 45sec'
headers['Latitude (WGS84)'] = '48.265'
headers['Longitude (WGS84)'] = '16.318'
headers['Elevation (NN)'] = '400 m'
headers['IAGAcode'] = 'WIK'
headers['Station'] = 'Cobenzl'
headers['Institution'] = 'Zentralanstalt fuer Meteorologie und Geodynamik'
headers['WebInfo'] = 'http://www.wiki.at'
headers['T-Instrument'] = 'External-USB'
headers['T-InstrumentSerialNum'] = str(Tserialnr)
stDIDD.header = headers
# 5. Save all to the working directory
stDIDD.pmwrite(os.path.join(mainpath,'DIDD-WIK','data'),filenamebegins='DIDD_',format_type='PYCDF')



# OPT values : read yearly fractions
# 1. Get data
stD = pmRead(path_or_url=os.path.join(mainpath,'OPT-WIK','D_2009_0*'))
stH = pmRead(path_or_url=os.path.join(mainpath,'OPT-WIK','H_2009_0*'))
stZ = pmRead(path_or_url=os.path.join(mainpath,'OPT-WIK','Z_2009_0*'))
stF = pmRead(path_or_url=os.path.join(mainpath,'OPT-WIK','F_2009_0*'))
stHZ = mergeStreams(stH,stZ)
stHDZ = mergeStreams(stHZ,stD)
stOPT = mergeStreams(stHDZ,stF)
stOPT = stOPT._convertstream('hdz2xyz')
# 2. Merge auxilliary data
# currently still empty
# 3. Flagging list (last updated 07.9.2012 by leon)
# currently still empty
headers = stOPT.header
headers['Instrument'] = 'Optical'
headers['InstrumentSerialNum'] = 'not known'
headers['InstrumentOrientation'] = 'hdz'
headers['Azimuth'] = 'not known'
headers['Tilt'] = '0 deg'
headers['InstrumentPeer'] = 'Basement-West'
headers['InstrumentDataLogger'] = 'Optical'
headers['ProvidedComp'] = 'xyzf'
headers['ProvidedInterval'] = 'hour'
headers['ProvidedType'] = 'baseline corrected'
headers['DigitalSamplingInterval'] = 'None'
headers['DigitalFilter'] = 'None'
headers['Latitude (WGS84)'] = '48.265'
headers['Longitude (WGS84)'] = '16.318'
headers['Elevation (NN)'] = '400 m'
headers['IAGAcode'] = 'WIK'
headers['Station'] = 'Cobenzl'
headers['Institution'] = 'Zentralanstalt fuer Meteorologie und Geodynamik'
headers['WebInfo'] = 'http://www.wiki.at'
headers['T-Instrument'] = ''
headers['T-InstrumentSerialNum'] = ''
# 5. Save all to the worjing directory
stOPT.pmwrite(os.path.join(mainpath),filenamebegins='OPT_',format_type='PYCDF')
"""
# PMAG values : read yearly fractions
# 1. Get data
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK','origfiles2009','CO0903*'),starttime=str(year)+'-01-01', endtime=str(year+1)+'-01-01')
# Add Meta information
headers = stPMAG.header
headers['Instrument'] = 'ELSEC820'
headers['InstrumentSerialNum'] = 'not known'
headers['InstrumentOrientation'] = 'None'
headers['Azimuth'] = ''
headers['Tilt'] = ''
headers['InstrumentPeer'] = 'F pillar'
headers['InstrumentDataLogger'] = 'ELSEC'
headers['ProvidedComp'] = 'f'
headers['ProvidedInterval'] = '10 sec'
headers['ProvidedType'] = 'intensity'
headers['DigitalSamplingInterval'] = '10 sec'
headers['DigitalFilter'] = 'None'
headers['Latitude (WGS84)'] = '48.265'
headers['Longitude (WGS84)'] = '16.318'
headers['Elevation (NN)'] = '400 m'
headers['IAGAcode'] = 'WIK'
headers['Station'] = 'Cobenzl'
headers['Institution'] = 'Zentralanstalt fuer Meteorologie und Geodynamik'
headers['WebInfo'] = 'http://www.wiki.at'
headers['T-Instrument'] = ''
headers['T-InstrumentSerialNum'] = ''
stPMAG.header = headers
#
# 2. Flagging list (last updated 07.9.2012 by leon)
# currently still empty
# 3. Remove outliers
stPMAG = stPMAG.routlier()
# 4. Save all to the worjing directory
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',mode='replace',format_type='PYCDF')

