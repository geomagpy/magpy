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
year = 2010

print datetime.utcnow()
st = pmRead(path_or_url=os.path.join(mainpath,'DIDD-WIK','*'),starttime= str(year)+'-07-20', endtime=str(year)+'-07-31')
st.pmplot(['x','y'],plottype='continuous')
st.pmwrite(os.path.join(mainpath,'DIDD-WIK','data'),filenamebegins='DIDD_',format_type='PYCDF')
st.header.clear()
st = pmRead(path_or_url=os.path.join(mainpath,'DIDD-WIK','data','*'),starttime= str(year)+'-07-20', endtime=str(year)+'-07-31')
st.pmplot(['x','y','t1'],plottype='continuous')
print datetime.utcnow()

x=1/0
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
stDIDD = mergeStreams(st1,aux1,keys=['t1','var1'])
# 3. Flagging list (last updated 07.9.2012 by leon)
# currently still empty
# 4. Save all to the worjing directory
stDIDD.pmwrite(os.path.join(mainpath,'DIDD-WIK','data'),filenamebegins='DIDD_',format_type='PYCDF')


# LEMI values and read yearly fractions
# 1. Get data
st2 = pmRead(path_or_url=os.path.join(mainpath,'LEMI-WIK','*'),starttime= str(year)+'-01-01', endtime=str(year+1)+'-01-01')
# 2. Merge auxilliary data
aux2 = pmRead(path_or_url=os.path.join(mainpath,'TEMP-WIK','Vario*'))
aux2 = aux2.date_offset(-timedelta(hours=2)) # correcting times e.g. MET to UTC
aux2 = aux2.filtered(filter_type='gauss',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30),respect_flags=True)
stLEMI = mergeStreams(st2,aux2,keys=['t1','var1'])
# 3. Flagging list (last updated 07.9.2012 by leon)
# currently still empty
# 4. Save all to the worjing directory
stLEMI.pmwrite(os.path.join(mainpath,'LEMI-WIK','data'),filenamebegins='LEMI_',format_type='PYCDF')


# PMAG values : read yearly fractions
# 1. Get data
stPMAG = pmRead(path_or_url=os.path.join(mainpath,'PMAG-WIK','*'),starttime= str(year)+'-01-01', endtime=str(year+1)+'-01-01')
# 2. Flagging list (last updated 07.9.2012 by leon)
# currently still empty
# 3. Remove outliers
stPMAG = stPMAG.routlier()
# 4. Save all to the worjing directory
stPMAG.pmwrite(os.path.join(mainpath,'PMAG-WIK','data'),filenamebegins='PMAG_',format_type='PYCDF')

x=1/0






basepath = '/home/leon'
path = r'/home/leon/Dropbox/Projects/SpaceWeather/Conrad Observatorium/outdoormessungen'

basispath = r'/home/leon/Dropbox/Projects/SpaceWeather/Conrad Observatorium/Basismessungen'

st = pmRead(path_or_url=os.path.join(path,'outdoorsurvey_2012-08-*.csv'))
st = st.routlier()
st = st.remove_flagged()
st.pmplot(['f'],plottitle = "Ex 1 - Plot")
bas = pmRead(path_or_url=os.path.join(basispath,'*'))
bas = bas.routlier()
bas = bas.remove_flagged()
bas.pmplot(['f'],plottitle = "Basis")
newst = subtractStreams(st,bas,keys='f')
newst.pmplot(['f'],plottitle = "Sub")
newst.pmwrite(path,coverage='all',filenamebegins='outdoorsurvey_red_2012-08-02')
x = 1/0

# Example for the analysis of WIK data

# 
# 1)   Absolute Values
#
# Analyze Absolute measurments
logging.info("----- Analyzing Absolute measurements -----")
#absanapath = os.path.normpath('f:\\Absolute-Cobenzl\\Analysis')
#absarchpath = os.path.normpath('f:\\Absolute-Cobenzl\\Archive')
#absdidd = pmRead(path_or_url=os.path.normpath('..\\dat\\output\\absolutes\\absolutes_didd.txt'))
#absdidd = analyzeAbsFiles(path_or_url=absanapath, alpha=0.0, beta=0.0, variopath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'), scalarpath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'), archivepath=absarchpath)
#absdidd.pmwrite('..\\dat\\output\\absolutes\\',coverage='all',mode='replace',filenamebegins='absolutes_didd')
#absdidd.pmplot(['x','y','z'],plottitle = "Analysis of absolute values")
#abslemi = analyzeAbsFiles(path_or_url=absanapath, alpha=3.3, beta=0.0, variopath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\LEMI\\*'), scalarpath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'), archivepath=absarchpath)
#abslemi.pmwrite('..\\dat\\output\\absolutes\\',coverage='all',mode='replace',filenamebegins='absolutes_lemi')
#abslemi.pmplot(['x','y','z'],plottitle = "Analysis of absolute values")


#absdidd = pmRead(path_or_url=os.path.join('..','dat','output','absolutes','absolutes_didd.txt'))
#abslemi = pmRead(path_or_url=os.path.join('..','dat','output','absolutes','absolutes_lemi.txt'))
#absdidd.pmplot(['x','y','z'],plottitle = "Analysis of absolute values - DIDD")
#abslemi.pmplot(['x','y','z'],plottitle = "Analysis of absolute values - LEMI")

#absdiff = subtractStreams(absdidd,abslemi,keys=['x','y','z','f']) # Stream_a gets modified - stdiff = st1mod...
#absdiff.pmplot(['x','y','z','f'],plottitle = "Differences of absolute values")

abslemi = analyzeAbsFiles(path_or_url="ftp://data@conrad-observatory.at:data2COBS@94.136.40.103/cobenzlabs/2011-11-21_13-51-42_AbsoluteMeas.txt", alpha=3.3, beta=0.0, variopath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\LEMI\\*'), scalarpath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'))


#abslemi = analyzeAbsFiles(path_or_url="ftp://94.136.40.103:21",alpha=3.3, beta=0.0, variopath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\LEMI\\*'), scalarpath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'))
#abslemi = analyzeAbsFiles(path_or_url="ftp://data@conrad-observatory.at:data2COBS@94.136.40.103/cobenzlabs/", alpha=3.3, beta=0.0, variopath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\LEMI\\*'), scalarpath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'))

# Do it for the Lemi
abslemi = analyzeAbsFiles(path_or_url="ftp://data@conrad-observatory.at:data2COBS@94.136.40.103/cobenzlabs/", alpha=3.3, beta=0.0, variopath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\LEMI\\*'), scalarpath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'))
# write the data
abslemi.pmwrite(os.path.join('..','dat','absolutes'),coverage='all',mode='replace',filenamebegins='absolutes_lemi')
# make plot covering one year back from now
start = datetime.utcnow()-timedelta(days=365)
abslemi = pmRead(path_or_url=os.path.join('..','dat','absolutes','absolutes_lemi.txt'),starttime=start)
abslemi.pmplot(['x','y','z'],plottitle = "Analysis of absolute values - LEMI", outfile="AutoAnalysisLemi")

# Repeat for DIDD but write new logfile
absdidd = analyzeAbsFiles(path_or_url="ftp://data@conrad-observatory.at:data2COBS@94.136.40.103/cobenzlabs/", alpha=0.0, beta=0.0, variopath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'), scalarpath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'), archivepath=os.path.normpath('f:\\Absolute-Cobenzl\\Archive'))
absdidd.pmwrite(os.path.join('..','dat','absolutes'),coverage='all',mode='replace',filenamebegins='absolutes_didd')
absdidd = pmRead(path_or_url=os.path.join('..','dat','absolutes','absolutes_lemi.txt'),starttime=start)
absdidd.pmplot(['x','y','z'],plottitle = "Analysis of absolute values - DIDD", outfile="AutoAnalysisDIDD")
# Use new mail function to send log and plot
msg = 'Successfully analyzed files were transferred to archive directory\n\nControl the graph - still erroneous data is usually related to wrong azimuth marks\nThis data has to be uploaded manually again for correction\nPlease check the remaining files on the server for errors -> see attached logfile for hints\n\nCheers, Your Analysis-Robot'
send_mail('roman_leonhardt@web.de', ['roman.leonhardt@zamg.ac.at'], text=msg, files=['magpy.log','AutoAnalysisDIDD.png','AutoAnalysisLemi.png'], smtpserver='smtp.web.de',user="roman_leonhardt",pwd="2kippen")

plt.show()

test = 1/0


# Script for automatic procedure using cron job
logging.info("----- Analyzing Absolute measurements -----")
# - a) Download all files to analysis folder using transfer package (or access database)

# - b) Analyse data for lemi , keep data in analysis folder 
absanapath = os.path.normpath('f:\\Absolute-Cobenzl\\Analysis') # try to use ftp path 
abslemi = analyzeAbsFiles(path_or_url=absanapath, alpha=3.3, beta=0.0, variopath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\LEMI\\*'), scalarpath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'))
# - c) Analyse data for didd , move data to archive folder
absarchpath = os.path.normpath('f:\\Absolute-Cobenzl\\Archive')
absdidd = analyzeAbsFiles(path_or_url=absanapath, alpha=0.0, beta=0.0, variopath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'), scalarpath=os.path.normpath('f:\\Vario-Cobenzl\\dIdD-System\\*'), archivepath=absarchpath)
# - d) Eventually upload AbsPlots to status page of homepage (or send by mail with (f))

# - e) Upload erroneous files to server again (requires a correction possibility

# - f) Send logging info to observer





# 2) Analyze data





pmagpath = 'f:\\Vario-Cobenzl\\dIdD-System\\pmag\\2011\\01\\*'
# see magpy_gsm19
st = pmRead(path_or_url=os.path.normpath(pmagpath))
st.pmplot(['f'],plottitle = "Ex 1 - Plot")

st = st.routlier()
st = st.remove_flagged()

st.pmplot(['f'],plottitle = "Ex 1 - Plot")


# write
st2 = pmRead(path_or_url=os.path.normpath(diddpath))
st2.pmplot(['f'],plottitle = "Ex 1 - Plot")

st2 = st2.routlier()

stdiff = subtractStreams(st,st2,keys=['f']) # Stream_a gets modified - stdiff = st1mod...
#plot

# flag

