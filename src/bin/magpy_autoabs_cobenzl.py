#!/usr/bin/env python
"""
MagPy - Automatically analyze absolute values from CobsServer - WIK analysis
"""

import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')

# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from stream import *
from absolutes import *
matplotlib.use('Agg')

# Some definitions
absolutedatalocation = "ftp://data@conrad-observatory.at:data2COBS@94.136.40.103/cobenzlabs/"
absindentifier = 'D_WIK.txt'
# check os and vary the basepath variable
if os.name == 'posix':
    print "Running on Linux or similar OS"
    basepath = '/home/leon/Dropbox'
else:
    print "Windows system"
    basepath = 'e:\dropbox\My Dropbox'

lemipath = os.path.join(basepath,'Daten','Magnetism','LEMI-WIK','data','*')
diddpath = os.path.join(basepath,'Daten','Magnetism','DIDD-WIK','data','*')
archivepath = os.path.join(basepath,'Daten','Magnetism','DI-WIK','raw')
writeresultpath = os.path.join(basepath,'Daten','Magnetism','DI-WIK','data')
send_notification_to = ['roman.leonhardt@zamg.ac.at','barbara.leichter@zamg.ac.at','andrea.draxler@gmx.at']
#send_notification_to = ['roman.leonhardt@zamg.ac.at']

# ToDo: add counter for logfile length and only send mail if new data was added or errors are happening
start = datetime.utcnow()-timedelta(days=365)

print "Analyzing absolute data for Lemi"

# Do it for the Lemi
abslemi = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=3.3, beta=0.0, absidentifier=absindentifier, variopath=lemipath, scalarpath=diddpath)
# write the data
abslemi.write(writeresultpath,coverage='all',mode='replace',filenamebegins='WIK-D-LEMI_1_BE_01')
# make plot covering one year back from now
abslemi = read(path_or_url=os.path.join(writeresultpath,'WIK-D-LEMI_1_BE_01.txt'))
abslemi = abslemi.remove_outlier(timerange=timedelta(days=60),keys=['x','y','z','f','dx','dy','dz'],threshold=1.5)
abslemi.write(writeresultpath,coverage='all',filenamebegins='WIK-D-LEMI_1_BE_01')
abslemi= abslemi.trim(starttime=start)
abslemi = abslemi.remove_flagged()
abslemi.plot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from LEMI", outfile=os.path.join(basepath,'Daten','Magnetism','DI-WIK','AutoAnalysisLemi'))

print "Analyzing absolutes data for dIdD and moving DI-data to archive"

# Repeat for DIDD but write new logfile and move succesfully analyzed files from the server to the archive
absdidd = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=0.0, beta=0.0, absidentifier=absindentifier, variopath=diddpath, scalarpath=diddpath, archivepath=archivepath)
absdidd.write(writeresultpath,coverage='all',mode='replace',filenamebegins='WIK-D-DIDD_3121331_S_01')
absdidd = read(path_or_url=os.path.join(writeresultpath,'WIK-D-DIDD_3121331_S_01.txt'))
absdidd = absdidd.remove_outlier(timerange=timedelta(days=60),keys=['x','y','z','f','dx','dy','dz'],threshold=1.5)
absdidd.write(writeresultpath,coverage='all',filenamebegins='WIK-D-DIDD_3121331_S_01')
absdidd = absdidd.trim(starttime=start)
absdidd = absdidd.remove_flagged()
absdidd.plot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from DIDD", outfile=os.path.join(basepath,'Daten','Magnetism','DI-WIK','AutoAnalysisDIDD'))

# Calculate differences to easily identify errors in one instrument
absdiff = subtractStreams(absdidd,abslemi,keys=['x','y','z','f']) # Stream_a gets modified - stdiff = st1mod...
absdiff.plot(['x','y','z','f'],plottitle = "Differences of absolute values", outfile=os.path.join(basepath,'Daten','Magnetism','DI-WIK','AutoAnalysisDifferences'))

# Use new mail function to send log and plot
msg = 'Successfully analyzed files were transferred to archive directory\n\nControl the graph - still erroneous data is usually related to wrong azimuth marks\nThis data has to be uploaded manually again for correction\nPlease check the remaining files on the server for errors -> see attached logfile for hints\n\nCheers, Your Analysis-Robot'
send_mail('roman_leonhardt@web.de', send_notification_to, text=msg, files=['magpy.log',os.path.join(basepath,'Daten','Magnetism','DI-WIK','AutoAnalysisDIDD.png'),os.path.join(basepath,'Daten','Magnetism','DI-WIK','AutoAnalysisLemi.png'),os.path.join(basepath,'Daten','Magnetism','DI-WIK','AutoAnalysisDifferences.png')], smtpserver='smtp.web.de',user="roman_leonhardt",pwd="2kippen")

