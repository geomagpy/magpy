#!/usr/bin/env python

import sys
sys.path.append('/home/leon/Software/magpy/trunk/src')

from stream import *
from absolutes import *
from transfer import *

import MySQLdb

# Some definitions
absolutedatalocation = "ftp://data@conrad-observatory.at:data2COBS@94.136.40.103/cobenzlabs/"

# check os and vary the basepath variable
if os.name == 'posix':
    print "Running on Linux or similar OS"
    basepath = '/home/leon/Dropbox'
else:
    print "Windows system"
    basepath = 'e:\dropbox\My Dropbox'

diddpath = os.path.join(basepath,'Daten','Magnetism','DIDD-WIK','data','*')
fgepath = os.path.join(basepath,'Daten','Magnetism','FGE-WIC','IAGA','*')
lemipath = os.path.join('/srv','data','GS-W-30','raw','LEMI*')
pospath = os.path.join('/srv','data','GS-W-30','raw','POS*')
archivepath = os.path.join(basepath,'Daten','Magnetism','DI-WIC','raw')
rawpath = os.path.join(basepath,'Daten','Magnetism','DI-WIC')
writeresultpath = os.path.join(basepath,'Daten','Magnetism','DI-WIC','data')
#send_notification_to = ['roman.leonhardt@zamg.ac.at','m-i.herzog@a1.net']
send_notification_to = ['roman.leonhardt@zamg.ac.at']

absindentifier = 'A2_WIC.txt'
start = datetime.utcnow()-timedelta(days=365)

# ToDo: add counter for logfile length and only send mail if new data was added or errors are happening
print "Analyzing absolutes data for dIdD"
# Repeat for DIDD but write new logfile and move succesfully analyzed files from the server to the archive
absdidd = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=0.0, beta=0.0, deltaF=-170.3, absidentifier=absindentifier, variopath=diddpath, scalarpath=diddpath)
if absdidd:
    absdidd.write(writeresultpath,coverage='all',mode='replace',filenamebegins='absolutes_didd_A2')
    absdidd = read(path_or_url=os.path.join(writeresultpath,'absolutes_didd_A2.txt'))
    #absdidd = absdidd.remove_outlier(timerange=timedelta(days=60),keys=['x','y','z','f','dx','dy','dz'],threshold=1.5)
    absdidd.write(writeresultpath,coverage='all',filenamebegins='absolutes_didd_A2')
    absdidd = absdidd.trim(starttime=start)
    #absdidd = absdidd.remove_flagged()
    absdidd.plot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from DIDD", outfile=os.path.join(rawpath,"AutoAnalysisDIDD"))

print "Analyzing absolutes data for dIdD"
# Repeat for DIDD but write new logfile and move succesfully analyzed files from the server to the archive
#abslemi = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=3.35, beta=0.0, deltaF=0.0, absidentifier=absindentifier, variopath=lemipath, scalarpath=pospath)
#if abslemi:
#    abslemi.write(writeresultpath,coverage='all',mode='replace',filenamebegins='absolutes_lemi_A2')
#    abslemi = read(path_or_url=os.path.join(writeresultpath,'absolutes_lemi_A2.txt'))
#    abslemi = abslemi.remove_outlier(timerange=timedelta(days=60),keys=['x','y','z','f','dx','dy','dz'],threshold=1.5)
#    abslemi.write(writeresultpath,coverage='all',filenamebegins='absolutes_lemi_A2')
#    abslemi = abslemi.trim(starttime=start)
#    abslemi = abslemi.remove_flagged()
#    abslemi.plot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from LEMI", outfile=os.path.join(rawpath,"AutoAnalysisLEMI"))

print "Analyzing absolutes data for FGE"
# Do it for the FGE
absfge = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=3.35, beta=0.0, debugmode=True, absidentifier=absindentifier, variopath=fgepath, scalarpath=fgepath, archivepath=archivepath)
if absfge:
    absfge.write(writeresultpath,coverage='all',mode='replace',filenamebegins='absolutes_fge_A2')
    absfge = read(path_or_url=os.path.join(writeresultpath,'absolutes_fge_A2.txt'))
    #absfge = absfge.remove_outlier(timerange=timedelta(days=60),keys=['x','y','z','f','dx','dy','dz'],threshold=1.5)
    absfge.write(writeresultpath,coverage='all',filenamebegins='absolutes_fge_A2')
    absfge= absfge.trim(starttime=start)
    #absfge = absfge.remove_flagged()
    absfge.plot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from FGE", outfile=os.path.join(rawpath,"AutoAnalysisFge"))

    # Calculate differences to easily identify errors in one instrument
    absdiff = subtractStreams(absdidd,absfge,keys=['x','y','z','f']) # Stream_a gets modified - stdiff = st1mod...
    absdiff.plot(['x','y','z','f'],plottitle = "Differences of absolute values", outfile="AutoAnalysisDifferences")

# test whether new data is analyzed
import commands
ret = commands.getoutput('wc -l magpy.log') # check whether paths are ok or switch to full paths
linenum = int(ret.split()[0])

print linenum

if linenum > 10:
    # Use new mail function to send log and plot
    msg = 'Successfully analyzed files were transferred to archive directory\n\nControl the graph - still erroneous data is usually related to wrong azimuth marks\nThis data has to be uploaded manually again for correction\nPlease check the remaining files on the server for errors -> see attached logfile for hints\n\nCheers, Your Analysis-Robot'
    #send_mail('roman_leonhardt@web.de', send_notification_to, text=msg, files=['magpy.log','AutoAnalysisDIDD.png','AutoAnalysisLemi.png','AutoAnalysisDifferences.png'], smtpserver='smtp.web.de',user="roman_leonhardt",pwd="2kippen")

absindentifier = 'H1_WIC.txt'
start = datetime.utcnow()-timedelta(days=365)

# ToDo: add counter for logfile length and only send mail if new data was added or errors are happening
print "Analyzing absolutes data for dIdD"

absdidd = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=0.0, beta=0.0, deltaF=-170.3, absidentifier=absindentifier, variopath=diddpath, scalarpath=diddpath)
if absdidd:
    absdidd.write(writeresultpath,coverage='all',mode='replace',filenamebegins='absolutes_didd_H1')
    absdidd = read(path_or_url=os.path.join(writeresultpath,'absolutes_didd_H1.txt'))
    absdidd = absdidd.remove_outlier(timerange=timedelta(days=60),keys=['x','y','z','f','dx','dy','dz'],threshold=1.5)
    absdidd.write(writeresultpath,coverage='all',filenamebegins='absolutes_didd_H1')
    absdidd = absdidd.trim(starttime=start)
    absdidd = absdidd.remove_flagged()
    absdidd.plot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from DIDD", outfile=os.path.join(rawpath,"AutoAnalysisDIDDH1"))


print "Analyzing absolutes data for FGE"
absfge = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=3.35, beta=0.0, debugmode=True, absidentifier=absindentifier, variopath=fgepath, scalarpath=fgepath, archivepath=archivepath)
if absfge:
    absfge.write(writeresultpath,coverage='all',mode='replace',filenamebegins='absolutes_fge_H1')
    absfge = read(path_or_url=os.path.join(writeresultpath,'absolutes_fge_H1.txt'))
    absfge = absfge.remove_outlier(timerange=timedelta(days=60),keys=['x','y','z','f','dx','dy','dz'],threshold=1.5)
    absfge.write(writeresultpath,coverage='all',filenamebegins='absolutes_fge_H1')
    absfge= absfge.trim(starttime=start)
    absfge = absfge.remove_flagged()
    absfge.plot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from FGE", outfile=os.path.join(rawpath,"AutoAnalysisFgeH1"))

# Calculate differences to easily identify errors in one instrument
absdiff = subtractStreams(absdidd,absfge,keys=['x','y','z','f']) # Stream_a gets modified - stdiff = st1mod...
absdiff.plot(['x','y','z','f'],plottitle = "Differences of absolute values", outfile="AutoAnalysisDifferencesH1")
