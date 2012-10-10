#!/usr/bin/env python
"""
MagPy - WIK analysis
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from core.magpy_stream import *
from core.magpy_absolutes import *
from core.magpy_transfer import *

basispath = r'/home/leon/Dropbox/Daten/Magnetism'


stRADON = pmRead(path_or_url='ftp://trmsoe:mgt.trms!@www.zamg.ac.at/data/radon/')
print stRADON
stRADON.pmplot(['x','t1','var1'],padding=0.2)
stRADON.powerspectrum('x')


x=1/0

stOPT = pmRead(path_or_url=os.path.join(basispath,'OPT-WIK','data','*'),starttime='2009-01-01', endtime='2009-05-31')
#Flag the f column without data
stOPT = stOPT.flag_stream('f',3,"System failure",datetime(2009,2,9,15,0,0,0),datetime(2009,2,16,13,0,0,0))
stOPT = stOPT.remove_flagged()
stOPT = stOPT._convertstream('xyz2hdz')
# Offsets to VVDM Pear
stOPT = stOPT.offset({'x': -6, 'z': 11}) 
stOPT = stOPT.delta_f()
stOPT.pmplot(['x','y','z','f','df'])

stPMAG = pmRead(path_or_url=os.path.join(basispath,'PMAG-WIK','data','*'),starttime='2009-01-01', endtime='2009-05-31')
stPMAG = stPMAG.remove_flagged()
stPMAG.pmplot(['f'])
stPMAG = stPMAG.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stdiff = subtractStreams(stOPT,stPMAG,keys=['f']) # Stream_a gets modified - stdiff = st1mod...
stdiff.pmplot(['f'])

x= 1/0


#absDIDD = pmRead(path_or_url=os.path.join(basispath,'ABSOLUTE-RAW','data','absolutes_didd.txt'))
#absDIDD.pmplot(['dx','dy','dz'])

#stDIDDdat = pmRead(path_or_url=os.path.join(basispath,'DIDD-WIK','data','*'),starttime='2009-01-01', endtime='2009-05-31')
stDIDDmod = pmRead(path_or_url=os.path.join(basispath,'DIDD-WIK','preliminary','*'),starttime='2009-01-01', endtime='2009-05-31')
#stDIDDmod = stDIDDdat.remove_flagged()
#absDIDD = pmRead(path_or_url=os.path.join(basispath,'ABSOLUTE-RAW','data','absolutes_didd.txt'))
#stDIDDmod = stDIDDmod.baseline(absDIDD,fitfunc='poly',fitdegree=5,plotbaseline=True)
stDIDDmod = stDIDDmod.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stDIDDmod = stDIDDmod._convertstream('xyz2hdz')
stDIDDmod.pmplot(['x','y','z','f'])
stdiff = subtractStreams(stDIDDmod,stOPT,keys=['x','y','z','f']) # Stream_a gets modified - stdiff = st1mod...
stdiff.pmplot(['x','y','z','f'])
hvals = stdiff._get_column('x')
dvals = stdiff._get_column('y')
zvals = stdiff._get_column('z')
fvals = stdiff._get_column('f')
hlst = [elem for elem in hvals if not isnan(elem)]
dlst = [elem for elem in dvals if not isnan(elem)]
zlst = [elem for elem in zvals if not isnan(elem)]
flst = [elem for elem in fvals if not isnan(elem)]
deltaH = np.median(hlst)
deltaD = np.median(dlst)
deltaZ = np.median(zlst)
deltaF = np.median(flst)
print "Delta H to main pear: %f" % deltaH
print "Delta D to main pear: %f" % deltaD
print "Delta Z to main pear: %f" % deltaZ
print "Delta F to main pear: %f" % deltaF
#offsetdict = {'x': deltaH, 'z': deltaZ, 'f': deltaF}
#stOPT = stOPT.offset(offsetdict)
#stOPT.pmplot(['x','y','z','f'])


x=1/0

# Annotation of plots
# ----------------
stDIDD = pmRead(path_or_url=os.path.join(basispath,'DIDD-WIK','data','*'),starttime='2009-12-01', endtime='2009-12-2')
stDIDD.pmplot(['x','y','z','f'],annotate=True)
stDIDD = stDIDD.remove_flagged()
stDIDD.pmplot(['x','y','z','f'])
# Check for storm onsets
stDIDD = pmRead(path_or_url=os.path.join(basispath,'DIDD-WIK','data','*'),starttime='2011-9-9', endtime='2011-9-14')
stDIDD = stDIDD._convertstream('xyz2hdz')
stDIDD = stDIDD.aic_calc('x',timerange=timedelta(hours=2))
stDIDD = stDIDD.differentiate(keys=['var2'],put2keys=['var3'])
stDIDD = stDIDD.eventlogger('var3',[20,30,50],'>',None,True)
stfilt = stDIDD.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stfilt = stfilt._get_k(key='var2',put2key='var4',scale=[0,70,140,210,280,350,420,490,560])
stfilt.header['col-var4'] = 'Cobs k_-index'
stDIDD = mergeStreams(stDIDD,stfilt,key=['var4'])
stDIDD.pmplot(['x','var2','var3','var4'],bartrange=0.02,symbollist = ['-','-','-','z'],plottitle = "Storm onsets and local variation index", annotate=True)

x=1/0

# Read Radon data
# ----------------
stRADON = pmRead(path_or_url='ftp://trmsoe:mgt.trms!@www.zamg.ac.at/data/radon/')
stRADON.pmplot(['x','t1','var1'],padding=0.2)
stRADON.powerspectrum('x')

x = 1/0


# reading WDC data directly from the WDC
# ----------------
stWDC = pmRead(path_or_url='ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc')
#HDZ: stWDC = pmRead(path_or_url='ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/lrv2011.wdc')
stWDC.pmplot(['x','y','z','f'])

x=1/0

# write WDC Format
# ----------------
stDIDD = pmRead(path_or_url=os.path.join(basispath,'DIDD-WIK','preliminary','*'),starttime='2012-09-01', endtime='2012-10-01')
stDIDD = stDIDD.flag_stream('x',3,"Water income",datetime(2012,9,28,15,35,0,0),datetime(2012,9,28,17,21,0,0))
stDIDD = stDIDD.remove_flagged()
stDIDD = stDIDD.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stDIDD.pmwrite(os.path.join(basispath),filenamebegins='wik',format_type='WDC',coverage='month',dateformat='%Y%m')
# ToDo: WDC minute filter

x=1/0

# write IAGA2002 Format
# ----------------
stDIDD = pmRead(path_or_url=os.path.join(basispath,'DIDD-WIK','preliminary','*'),starttime='2012-09-01', endtime='2012-09-02')
stDIDD.pmwrite(os.path.join(basispath),filenamebegins='IAGA_',format_type='IAGA')

x=1/0

# write monthly files
# ----------------
stDIDD = pmRead(path_or_url=os.path.join(basispath,'DIDD-WIK','preliminary','*'),starttime='2012-09-01', endtime='2012-10-2')
stDIDD.pmwrite(os.path.join(basispath),filenamebegins='Mon_',format_type='PYCDF',dateformat="%Y-%m",coverage='month')
# Works for DIDD when read again: Test this function for all other file formats as well
stDIDD = pmRead(path_or_url=os.path.join(basispath,'Mon_*'),starttime='2012-09-01', endtime='2012-10-2')
stDIDD.pmplot(['x','y','z','f'])

x=1/0

stLEMI = pmRead(path_or_url=os.path.join(basispath,'LEMI-WIK','data','*'),starttime='2011-01-01', endtime='2011-03-31')
stLEMI = stLEMI.remove_flagged()
absLEMI = pmRead(path_or_url=os.path.join(basispath,'ABSOLUTE-RAW','data','absolutes_lemi.txt'))
stLEMI = stLEMI.rotation(alpha=3.3)
stLEMI = stLEMI.baseline(absLEMI,knotstep=0.05,plotbaseline=True)
stLEMI = stLEMI.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stDIDD = pmRead(path_or_url=os.path.join(basispath,'DIDD-WIK','data','*'),starttime='2011-01-01', endtime='2011-03-31')
stDIDD = stDIDD.remove_flagged()
absDIDD = pmRead(path_or_url=os.path.join(basispath,'ABSOLUTE-RAW','data','absolutes_didd.txt'))
stDIDD = stDIDD.baseline(absDIDD,knotstep=0.05,plotbaseline=True)
stDIDD = stDIDD.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stLEMI.pmplot(['x','y','z','f'])
stDIDD.pmplot(['x','y','z','f'])
stdiff = subtractStreams(stDIDD,stLEMI,keys=['x','y','z','f']) # Stream_a gets modified - stdiff = st1mod...
stdiff.pmplot(['x','y','z','f'])

x=1/0

# Read optical data files
# ----------------
basispath = r'/home/leon/Dropbox/Daten/Magnetism/OPT-WIK'
stD = pmRead(path_or_url=os.path.join(basispath,'D_2009_0*'))
stH = pmRead(path_or_url=os.path.join(basispath,'H_2009_0*'))
stZ = pmRead(path_or_url=os.path.join(basispath,'Z_2009_0*'))
stF = pmRead(path_or_url=os.path.join(basispath,'F_2009_0*'))
stHZ = mergeStreams(stH,stZ)
stHDZ = mergeStreams(stHZ,stD)
stOPT = mergeStreams(stHDZ,stF)
stOPT = stOPT._convertstream('hdz2xyz')
stOPT.pmplot(['x','y','z','f'])


# Read binary data from the GDAS logger
# ----------------

basispath = r'/home/leon/Dropbox/Daten/Magnetism/DataFormats'
infile = os.path.join(basispath,'WIC-GDAS','WIC_v1_20120906.sec')
st = pmRead(path_or_url=infile)
st.pmplot(['x','y','z'],outfile="test.png")

ftpdatatransfer(localpath='',filestr="test.png", ftppath='/data', myproxy='www.zamg.ac.at',port=21, login='trmsoe', passwd='mgt.trms!',logfile = 'ltest.txt')

st2 = pmRead(path_or_url='ftp://trmsoe:mgt.trms!@www.zamg.ac.at/data/WIC_v1_min_20120906.bin')
st2.pmplot(['x','y','z','f'])


x=1/0


# Read data from field surveys
# ----------------

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

