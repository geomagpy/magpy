#!/usr/bin/env python
"""
MagPy - Automatically analyze absolute values from CobsServer - WIK analysis
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from core.magpy_stream import *
from core.magpy_absolutes import *

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
archivepath = os.path.join(basepath,'Daten','Magnetism','ABSOLUTE-RAW')
writeresultpath = os.path.join(basepath,'Daten','Magnetism','ABSOLUTE-RAW','data')
#send_notification_to = ['roman.leonhardt@zamg.ac.at','barbara.leichter@zamg.ac.at','andrea.draxler@gmx.at']
send_notification_to = ['roman.leonhardt@zamg.ac.at']

# ToDo: add counter for logfile length and only send mail if new data was added or errors are happening 

# Do it for the Lemi
abslemi = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=3.3, beta=0.0, absidentifier=absindentifier, variopath=lemipath, scalarpath=diddpath)
# write the data
abslemi.pmwrite(writeresultpath,coverage='all',mode='replace',filenamebegins='absolutes_lemi')
# make plot covering one year back from now
start = datetime.utcnow()-timedelta(days=365)
abslemi = pmRead(path_or_url=os.path.join(writeresultpath,'absolutes_lemi.txt'),starttime=start)
abslemi.pmplot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from LEMI", outfile="AutoAnalysisLemi")

# Repeat for DIDD but write new logfile and move succesfully analyzed files from the server to the archive
absdidd = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=0.0, beta=0.0, absidentifier=absindentifier, variopath=diddpath, scalarpath=diddpath, archivepath=archivepath)
absdidd.pmwrite(writeresultpath,coverage='all',mode='replace',filenamebegins='absolutes_didd')
absdidd = pmRead(path_or_url=os.path.join(writeresultpath,'absolutes_didd.txt'),starttime=start)
absdidd.pmplot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from DIDD", outfile="AutoAnalysisDIDD")

# Calculate differences to easily identify errors in one instrument
absdiff = subtractStreams(absdidd,abslemi,keys=['x','y','z','f']) # Stream_a gets modified - stdiff = st1mod...
absdiff.pmplot(['x','y','z','f'],plottitle = "Differences of absolute values", outfile="AutoAnalysisDifferences")

# Use new mail function to send log and plot
msg = 'Successfully analyzed files were transferred to archive directory\n\nControl the graph - still erroneous data is usually related to wrong azimuth marks\nThis data has to be uploaded manually again for correction\nPlease check the remaining files on the server for errors -> see attached logfile for hints\n\nCheers, Your Analysis-Robot'
send_mail('roman_leonhardt@web.de', send_notification_to, text=msg, files=['magpy.log','AutoAnalysisDIDD.png','AutoAnalysisLemi.png','AutoAnalysisDifferences.png'], smtpserver='smtp.web.de',user="roman_leonhardt",pwd="2kippen")

