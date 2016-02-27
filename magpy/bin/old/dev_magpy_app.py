#!/usr/bin/env python
"""
MagPy - Example Applications: Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 22.05.2012)
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from magpy.stream import *

#
# Reading data and plotting
#
logging.info("----- Now starting Example 1 -----")
# using relative paths, absolute paths are straightforward
st = read(path_or_url=os.path.join('..','dat','didd','*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!! TODO: If time range is not part of available data range, an error message should be shown
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!! TODO: Add padding option to plots
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
st.plot(['x','y','z'],plottitle = "Ex 1 - Plot")

#
# Writing data 
#
logging.info("----- Now starting Example 2 -----")
st = read(path_or_url=os.path.join('..','dat','gdas-bgs','*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
st.plot(['x'],plottitle = "Ex 2 - Saving - not filtered")
st.write(os.path.join('..','dat','output','cdf'),format_type='PYCDF')

#
# Reading and writing various formats
#
logging.info("----- Now starting Example 3 -----")
st = read(path_or_url=os.path.join(r'..','dat','didd','*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
st.plot(['x','y','z','f'],plottitle = "Ex 3 - Formats - didd")
st = read(path_or_url=os.path.join(r'..','dat','lemi025','*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
st.plot(['x','y','z'],plottitle = "Ex 3 - Formats - lemi")
#st = read(path_or_url=os.path.join(r'..','dat','iaga02','*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
#st.plot(['x','y','z'],plottitle = "Ex 3 - Formats - iaga02")
st = read(path_or_url=os.path.join(r'..','dat','dtu','FHB*.sec'))
st.plot(['x','y','z'],plottitle = "Ex 3 - Formats - DTU ")
st = read(path_or_url=os.path.join(r'..','dat','output','cdf','*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
st.plot(['x','y','z','t1'],plottitle = "Ex 3 - Formats - gdas-bgs")
st = read(path_or_url=os.path.join(r'..','dat','pmag','*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
st.plot(['f'],plottitle = "Ex 3 - Formats - pmag")
st.write(os.path.join('..','dat','output','txt'),format_type='PYSTR')
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## ToDo: include iaga02 output from old version and intermagnet as well
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


#
# Reading cdf data and filtering
#
logging.info("----- Now starting Example 4 -----")
st = read(path_or_url=os.path.join('..','dat','output','cdf','*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
#st = read(path_or_url=os.path.join('..','dat','dtu','GDH4_20091215.cdf'))
st.plot(['x','y','z'],plottitle = "Ex 3 - Filter - seconds (not filtered)")
st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
st.write(os.path.join('..','dat','output','txt'),format_type='PYSTR',filenameends='-min.txt')
st.plot(['x','y','z'],plottitle = "Ex 3 - Filter - minutes")
st = st.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
st.plot(['x','y','z'],plottitle = "Ex 3 - Filter - hours")
st.write(os.path.join('..','dat','output','txt'),format_type='PYSTR',filenameends='-hou.txt')

#
# Intensity values, Outlier removal and auto-flagging
#
logging.info("----- Now starting Example 5 -----")
st = read(path_or_url=os.path.join('..','dat','pmag','*') ,starttime='2011-9-4',endtime='2011-9-5')
st.plot(['f'],plottitle = "Ex 5 - F vals with outliers")
st = st.remove_outlier()
stmod = st.remove_flagged()
stmod.plot(['f'],plottitle = "Ex 5 - Outliers removed")
stmod.write(os.path.join('..','dat','output','txt'),format_type='PYSTR',filenamebegins='FlagPMAG-')


#
# Flagging  
#
logging.info("----- Now starting Example 6 - Flagging -----")
st = read(path_or_url=os.path.join('..','dat','output','cdf','didd','*') ,starttime='2011-9-4',endtime='2011-9-5')
st = st.flag_stream('f',3,"Maintenance",'2011-9-4T10:00:00',datetime(2011,9,4,13,0,0,0))
stmod = st.remove_flagged()
stmod.write(os.path.join('..','dat','output','txt'),format_type='PYSTR',filenamebegins='FlagDIDD-')
stmod = stmod.get_gaps(key='f')
stmod.plot(['x','y','z','f','var3'],plottitle = "Ex 6 - Flagging")

#
# Smoothing and interpolating data
#
logging.info("----- Now starting Example 7 - Smoothing -----")
st = read(path_or_url=os.path.join('..','dat','lemi025','*'),starttime='2011-09-7',endtime=datetime(2011,9,9))
st.plot(['x'],plottitle = "Ex 7 - Before smoothing")
st = st.smooth(['x'],window_len=21)
st.plot(['x'],plottitle = "Ex 7 - After smoothing")
func = st.interpol(['x','y','z'])
st.plot(['x'],function=func,plottitle = "Ex 7 - After smoothing and interpolation")

#
# Storm analysis and derivatives 
#
logging.info("----- Now starting Example 8 - Storm analysis -----")
st = read(path_or_url=os.path.join('..','dat','didd','*'),starttime='2011-9-8',endtime='2011-9-14')
st = st._convertstream('xyz2hdz')
st = st.aic_calc('x',timerange=timedelta(hours=1))
st = st.differentiate(keys=['var2'],put2keys=['var3'])
st.plot(['x','y','z','var2'],plottitle = "Ex 8 - AIC Analysis")
stfilt = st.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stfilt = stfilt._get_k(key='var2',put2key='var4',scale=[0,100,200,300,400,500,600,700,800])
st = mergeStreams(st,stfilt,key=['var4'])
st.plot(['x','var2','var3','var4'],bartrange=0.02,symbollist = ['-','-','-','z'],plottitle = "Ex 8 - Storm onsets and local variation index")
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## ToDo: Check FMI method, include harmonic fit
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#fmi = st.k_fmi(fitdegree=2)
#fmi = st.k_fmi(fitfunc='spline',knotstep=0.4)
#st.plot(['x','var2','var3','t2'],symbollist = ['-','-','-','z'])
#

#
# Basic spectral investigation 
#
logging.info("----- Now starting Example 9 - Spectral analysis -----")
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## ToDo: Improve the functions, include PSD
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
st.spectrogram('x',dbscale=True)
#st.powerspectrum('x')

#
# Auxiliary data
#
logging.info("----- Now starting Example 10 - Auxiliary Temperature data -----")
# Temperature measurements and corrections to time columns
aux = read(path_or_url=os.path.join('..','dat','auxiliary','temp','Schacht*'))
aux = aux.date_offset(-timedelta(hours=2)) # correcting times e.g. MET to UTC
aux = aux.filtered(filter_type='gauss',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30),respect_flags=True)
func = aux.interpol(['t1','t2','var1'])
aux.plot(['t1','t2','var1'],function=func,plottitle = "Ex 10 - Reading/Analyzing auxiliary data")

# 
# Merging data streams and filling of missing values
#
# Using Aux data
logging.info("----- Now starting Example 11a - Merging auxiliary T data and variometer data -----")
st = read(path_or_url=os.path.join('..','dat','didd','*'))
newst = mergeStreams(st,aux,keys=['t1','var1'])
newst.write(os.path.join('..','dat','output','cdf','didd'),filenameends='_didd_min',format_type='PYCDF')
newst.plot(['x','y','z','t1','var1'],symbollist = ['-','-','-','-','-'],plottype='continuous',plottitle = "Ex 11a - Merge Vario and T data")

# Merging primary and secondary data
logging.info("----- Now starting Example 11b - Filling missing values from secondary instruments -----")
## Step A) Preparing DemoStream-1 with gaps
st1 = read(path_or_url=os.path.join('..','dat','output','txt','FlagDIDD-*'))
st1.plot(['x','f'],plottitle = "Ex 11b - Primary data set with some flagged records")
## Step B) Preparing DemoStream-2
st2 = read(path_or_url=os.path.join('..','dat','output','txt','FlagPMAG-*'))
st2 = st2.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
st2.plot(['f'], plottitle = "Ex 11b - Secondary data set")
## Step C) Determining average offset - median -> should be known
stdiff = subtractStreams(st1,st2,keys=['f']) # Stream_a gets modified - stdiff = st1mod...
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## ToDo: Trim required - check why
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
stdiff = stdiff.trim(starttime=datetime(2011,9,4,00,02),endtime=datetime(2011,9,4,23,58))
stdiff.plot(['f'], plottitle = "Ex 11b - Differences of both scalar instruments")
stdiff.spectrogram('f',dbscale=True)
offset = np.median(stdiff._maskNAN(stdiff._get_column('f')))
## Step D) Merging f column of stream 2 to stream 1 (reloaded) with average offset - median
st1 = read(path_or_url=os.path.join('..','dat','output','txt','FlagDIDD-*'))
mergest = mergeStreams(st1,st2,keys=['f'],offset=offset,comment='Pmag')
mergest.write(os.path.join('..','dat','output','txt'),format_type='PYSTR',filenamebegins='MergedDIDD-PMAG_')
mergest.plot(['x','f'], plottitle = "Ex 11b - Merged F values in stream")



# Absolute Values
# ----------------------------------------
from magpy.absolutes import *

# 
# Absolute Values
#
# Analyze Absolute measurments
logging.info("----- Now starting Example 12 - Analyzing Absolute measurements -----")
abso = analyzeAbsFiles(path_or_url=os.path.join('..','dat','absolutes','raw'), alpha=3.3, beta=0.0, variopath=os.path.join('..','dat','lemi025','*'), scalarpath=os.path.join('..','dat','didd','*'))
abso.write(os.path.join('..','dat','output','absolutes'),coverage='all',mode='replace',filenamebegins='absolutes_lemi')
abso.plot(['x','y','z'],plottitle = "Ex 12 - Analysis of absolute values")

# Baseline calculation and correction
logging.info("----- Now starting Example 13 - Obtaining baselines -----")
abslemi = read(path_or_url=os.path.join('..','dat','absolutes','absolutes_lemi.txt'))
func = abslemi.fit(['dx','dy','dz'],fitfunc='spline',knotstep=0.05)
abslemi.plot(['dx','dy','dz'],function=func, plottitle = "Ex 13 - Baseline values and spline fit")
lemi = read(path_or_url=os.path.join('..','dat','lemi025','*'),starttime='2011-09-1',endtime='2011-9-30')
lemi = lemi.rotation(alpha=3.3,beta=0.0)
lemi = lemi.baseline(abslemi,knotstep=0.05,plotbaseline=True)
lemi.plot(['x','y','z'], plottitle = "Ex 13 - Baseline corrected data")


#
# An example for an almost automated typical analysis in 16 lines
#
logging.info("----- Now starting Example 14 - Full example -----")
# 1. Load variometer data
va = read(path_or_url=os.path.join('..','dat','lemi025','*'))
# 2. Eventually combine with auxiliary data
aux = read(path_or_url=os.path.join('..','dat','auxiliary','temp','Vario*'))
vanew = mergeStreams(va,aux,keys=['t1','var1'])
# 3. Eventually Clean Data - Do flagging
# see example 6
# 4. Save modified variometer data
vanew.write(os.path.join('..','dat','output','cdf','lemi'),filenameends='_lemi_min',format_type='PYCDF')
##vanew = read(path_or_url=os.path.join('..','dat','output','cdf','lemi','Test*'))
# 5. Load scalar data
sc = read(path_or_url=os.path.join('..','dat','pmag','*'))
# 6. Eventually combine with auxiliary data
# 7. Eventually Clean Data - Do flagging and apply filters for comparions with vario
sc = sc.remove_outlier()
sc = sc.remove_flagged()
sc = sc.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
# 8. Save eventually modified scalar data
sc.write(os.path.join('..','dat','output','cdf','pmag'),filenameends='_pmag_min',format_type='PYCDF')
##sc = read(path_or_url=os.path.join('..','dat','output','cdf','pmag','*'))
# 9. Eventually merge variometer and scalar data (add pillar offset to f value)
priminst = mergeStreams(vanew,sc,keys=['f'])
# 10. Anaylze absolute data with modified variometer and scalar data (dont forget to remove flagged data first)
#see example 12
# 11. Calculate baseline and baseline correction
abslemi = read(path_or_url=os.path.join('..','dat','absolutes','absolutes_lemi.txt'))
func = abslemi.fit(['dx','dy','dz'],fitfunc='spline',knotstep=0.05)
priminst = priminst.rotation(alpha=3.3,beta=0.0)
priminst = priminst.baseline(abslemi,knotstep=0.05,plotbaseline=True)
priminst.plot(['x','y','z','f','t1'])
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## ToDo: var columns of vario (e.g. humidity) are overwritten by absolute vals 
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# 12. Save corrected data
priminst.write(os.path.join('..','dat','output','cdf','lemi'),filenamebegins='Basecorr-',filenameends='_lemi_min',format_type='PYCDF')
##vamerge = read(path_or_url=os.path.join('..','dat','output','cdf','lemi','Basecorr*'))
# 13. Compare data sets (dF, different varios, etc)
priminst = priminst.delta_f()
priminst.plot(['x','y','z','f','df'])
# 14. (Eventually merge data to fill gaps) hmmm....
#see example 11b
# 15. Apply filters to create min, hours, day, month files
#see example 4
# 16. Do fancy analysis things (indicies, strom onsets, spectral, etc)


#
# Some Tests and ideas
#
# Stacking
# --------------------------
# see also Heinz-Peters suggestions

#
# Baseline Stability
# --------------------------
# code written for old version - include here

#
# Getting orientation angles
# --------------------------
#Two approaches:
# 1) using baselines
# 2) using variometers, baselinecorrected, absolutes without variometer correction
# ---------------
#baslemi1 = read(path_or_url=os.path.join(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_lemi_alpha3.3.txt'))
#baslemi2 = read(path_or_url=os.path.join(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_didd.txt'))
#newst = subtractStreams(baslemi1,baslemi2,keys=['x','y','z'])
#newst = newst.trim(starttime=datetime(2010,7,10,00,02),endtime=datetime(2011,10,1,23,58))
#newst.plot(['x','y','z'])

#testarray = np.array(baslemi1)
#print testarray[1][2]
#print testarray.ndim

# Baseline Correction and RotationMatrix
# ---------------
# alpha and beta describe the rotation matrix (alpha is the horizontal angle (D) and beta the vertical)
#didd = read(path_or_url=os.path.join('g:\Vario-Cobenzl\dIdD-System\*'),starttime='2011-01-1',endtime='2011-12-31')
#basdidd = read(path_or_url=os.path.join(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_didd.txt'))
#lemi = read(path_or_url=os.path.join('g:\Vario-Cobenzl\dIdD-System\LEMI\*'),starttime='2011-01-1',endtime='2011-12-31')
#baslemi = read(path_or_url=os.path.join(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
#lemi = lemi.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
#didd = didd.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
#lemi = lemi.rotation(alpha=3.3,beta=0.0)
#didd = didd.baseline(basdidd,knotstep=0.05,plotbaseline=True)
#lemi = lemi.baseline(baslemi,knotstep=0.05,plotbaseline=True)

#didd.plot(['x','y','z'])
#lemi.plot(['x','y','z'])
#newst = subtractStreams(didd,lemi,keys=['x','y','z'],getmeans=True)
# for some reason first and last points are not subtracted
#newst = newst.trim(starttime=datetime(2011,1,1,02,00),endtime=datetime(2011,12,30,22,00))
#newst = newst.remove_outlier(keys=['x','y','z'])
#print "Mean x: %f" % np.mean(newst._get_column('x'))
#print "Mean y: %f" % np.mean(newst._get_column('y'))
#print "Mean z: %f" % np.mean(newst._get_column('z'))

#newst.plot(['x','y','z'])



