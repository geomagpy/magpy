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
year = 2011

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
stDIDD = stDIDD.flag_stream('x',3,"Mowing",datetime(2011,6,3,7,57,0,0),datetime(2011,6,3,9,02,0,0))
stDIDD = stDIDD.flag_stream('y',3,"Mowing",datetime(2011,6,3,7,57,0,0),datetime(2011,6,3,9,02,0,0))
stDIDD = stDIDD.flag_stream('z',3,"Mowing",datetime(2011,6,3,7,57,0,0),datetime(2011,6,3,9,02,0,0))
stDIDD = stDIDD.flag_stream('f',3,"Mowing",datetime(2011,6,3,7,57,0,0),datetime(2011,6,3,9,02,0,0))
#
stDIDD = stDIDD.flag_stream('x',3,"unknown",datetime(2011,7,14,18,32,0,0),datetime(2011,7,14,18,34,0,0))
stDIDD = stDIDD.flag_stream('y',3,"unknown",datetime(2011,7,14,18,32,0,0),datetime(2011,7,14,18,34,0,0))
stDIDD = stDIDD.flag_stream('z',3,"unknown",datetime(2011,7,14,18,32,0,0),datetime(2011,7,14,18,34,0,0))
stDIDD = stDIDD.flag_stream('f',3,"unknown",datetime(2011,7,14,18,32,0,0),datetime(2011,7,14,18,34,0,0))
#
stDIDD = stDIDD.flag_stream('x',3,"Mowing lawn",datetime(2011,9,14,05,43,0,0),datetime(2011,9,14,6,41,0,0))
stDIDD = stDIDD.flag_stream('y',3,"Mowing lawn",datetime(2011,9,14,05,43,0,0),datetime(2011,9,14,6,41,0,0))
stDIDD = stDIDD.flag_stream('z',3,"Mowing lawn",datetime(2011,9,14,05,43,0,0),datetime(2011,9,14,6,41,0,0))
stDIDD = stDIDD.flag_stream('f',3,"Mowing lawn",datetime(2011,9,14,05,43,0,0),datetime(2011,9,14,6,41,0,0))
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
# 5. Save all to the worjing directory
stDIDD.pmwrite(os.path.join(mainpath,'DIDD-WIK','data'),filenamebegins='DIDD_',format_type='PYCDF')


# LEMI values and read yearly fractions
# 1. Get data
st2 = pmRead(path_or_url=os.path.join(mainpath,'LEMI-WIK','*'),starttime= str(year)+'-01-01', endtime=str(year+1)+'-01-01')
# 2. Merge auxilliary data
aux2 = pmRead(path_or_url=os.path.join(mainpath,'TEMP-WIK','Vario*'))
aux2 = aux2.date_offset(-timedelta(hours=2)) # correcting times e.g. MET to UTC
aux2 = aux2.filtered(filter_type='gauss',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30),respect_flags=True)
Tserialnr = aux2.header['InstrumentSerialNum']
stLEMI = mergeStreams(st2,aux2,keys=['t1','var1'])
# 3. Flagging list (last updated 07.9.2012 by leon)
stLEMI = stLEMI.flag_stream('x',3,"Mowing",datetime(2011,6,3,7,57,0,0),datetime(2011,6,3,9,02,0,0))
stLEMI = stLEMI.flag_stream('y',3,"Mowing",datetime(2011,6,3,7,57,0,0),datetime(2011,6,3,9,02,0,0))
stLEMI = stLEMI.flag_stream('z',3,"Mowing",datetime(2011,6,3,7,57,0,0),datetime(2011,6,3,9,02,0,0))
stLEMI = stLEMI.flag_stream('f',3,"Mowing",datetime(2011,6,3,7,57,0,0),datetime(2011,6,3,9,02,0,0))
#
stLEMI = stLEMI.flag_stream('x',3,"unknown",datetime(2011,7,14,18,32,0,0),datetime(2011,7,14,18,34,0,0))
stLEMI = stLEMI.flag_stream('y',3,"unknown",datetime(2011,7,14,18,32,0,0),datetime(2011,7,14,18,34,0,0))
stLEMI = stLEMI.flag_stream('z',3,"unknown",datetime(2011,7,14,18,32,0,0),datetime(2011,7,14,18,34,0,0))
stLEMI = stLEMI.flag_stream('f',3,"unknown",datetime(2011,7,14,18,32,0,0),datetime(2011,7,14,18,34,0,0))
#
stLEMI = stLEMI.flag_stream('x',3,"Mowing lawn",datetime(2011,9,14,05,43,0,0),datetime(2011,9,14,6,41,0,0))
stLEMI = stLEMI.flag_stream('y',3,"Mowing lawn",datetime(2011,9,14,05,43,0,0),datetime(2011,9,14,6,41,0,0))
stLEMI = stLEMI.flag_stream('z',3,"Mowing lawn",datetime(2011,9,14,05,43,0,0),datetime(2011,9,14,6,41,0,0))
stLEMI = stLEMI.flag_stream('f',3,"Mowing lawn",datetime(2011,9,14,05,43,0,0),datetime(2011,9,14,6,41,0,0))
# 4. Add Meta information
headers = stLEMI.header
headers['Instrument'] = 'Lemi025'
headers['InstrumentSerialNum'] = 'not known'
headers['InstrumentOrientation'] = 'hdz'
headers['Azimuth'] = '3.3 deg'
headers['Tilt'] = '0 deg'
headers['InstrumentPeer'] = 'Basement-East'
headers['InstrumentDataLogger'] = 'Lemi025'
headers['ProvidedComp'] = 'xyzf'
headers['ProvidedInterval'] = 'min'
headers['ProvidedType'] = 'variation'
headers['DigitalSamplingInterval'] = '0.00625 sec'
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
stLEMI.header = headers
# 5. Save all to the working directory
stLEMI.pmwrite(os.path.join(mainpath,'LEMI-WIK','data'),filenamebegins='LEMI_',format_type='PYCDF')

"""
# PMAG values : read yearly fractions
# 1. Get data
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'01','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
#
# Add Meta information
headers = stPMAG.header
headers['Instrument'] = 'ELSEC820'
headers['InstrumentSerialNum'] = 'not known'
headers['InstrumentOrientation'] = 'None'
headers['Azimuth'] = ''
headers['Tilt'] = ''
headers['InstrumentPeer'] = 'F pillar'
headers['InstrumentDataLogger'] = 'ELSEC and FieldPoint'
headers['ProvidedComp'] = 'f'
headers['ProvidedInterval'] = '10 sec'
headers['ProvidedType'] = 'intensity'
headers['DigitalSamplingInterval'] = '10 sec'
headers['DigitalFilter'] = 'None'
headers['Latitude (WGS84)'] = '48'
headers['Longitude (WGS84)'] = '13'
headers['Elevation (NN)'] = '400 m'
headers['IAGAcode'] = 'WIK'
headers['Station'] = 'Cobenzl'
headers['Institution'] = 'Zentralanstalt fuer Meteorologie und Geodynamik'
headers['WebInfo'] = 'http://www.wiki.at'
headers['T-Instrument'] = ''
headers['T-InstrumentSerialNum'] = ''
stPMAG.header = headers
#
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'02','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'03','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'04','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'05','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'06','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'07','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'08','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'09','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'10','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'11','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK',str(year),'12','*'),starttime= str(year)+'-01-01', endtime=str(year)+'-02-01')
# currently no flags
stPMAG = stPMAG.routlier()
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')

"""
