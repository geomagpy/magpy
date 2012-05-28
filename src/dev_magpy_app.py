#!/usr/bin/env python
"""
MagPy - Example Applications: Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 22.05.2012)
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from dev_magpy_stream import *

#
# Reading data and plotting
#
logging.info("----- Now starting Example 1 -----")
# relative path
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\didd\\*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!! TODO: If time range is not part of available data range, an error message should be shown
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!! TODO: Add padding option to plots
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#st.pmplot(['x','y','z'],plottitle = "Ex 1 - Plot")

#
# Writing data 
#
logging.info("----- Now starting Example 2 -----")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\gdas-bgs\\*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
#st.pmplot(['x'],plottitle = "Ex 2 - Saving - not filtered")
#st.pmwrite('..\\dat\\output\\cdf',format_type='PYCDF')

#
# Reading and writing various formats
#
logging.info("----- Now starting Example 3 -----")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\didd\\*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
#st.pmplot(['x','y','z','f'],plottitle = "Ex 3 - Formats - didd")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\lemi025\\*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
#st.pmplot(['x','y','z'],plottitle = "Ex 3 - Formats - lemi")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\iaga02\\*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
#st.pmplot(['x','y','z'],plottitle = "Ex 3 - Formats - iaga02")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\output\\cdf\\*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
#st.pmplot(['x','y','z','t1'],plottitle = "Ex 3 - Formats - gdas-bgs")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\pmag\\*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
#st.pmplot(['f'],plottitle = "Ex 3 - Formats - pmag")
#st.pmwrite('..\\dat\\output\\txt',format_type='PYSTR')


#
# Reading cdf data and filtering
#
logging.info("----- Now starting Example 4 -----")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\output\\cdf\\*'),starttime='2011-09-8',endtime=datetime(2011,9,9))
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\dtu\\GDH4_20091215.cdf'))
#st.pmplot(['x','y','z'],plottitle = "Ex 3 - Filter - seconds (not filtered)")
#st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
#st.pmwrite('..\\dat\\output\\txt',format_type='PYSTR',filenameends='-min.txt')
#st.pmplot(['x','y','z'],plottitle = "Ex 3 - Filter - minutes")
#st = st.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
#st.pmplot(['x','y','z'],plottitle = "Ex 3 - Filter - hours")
#st.pmwrite('..\\dat\\output\\txt',format_type='PYSTR',filenameends='-hou.txt')

#
# Intensity values, Outlier removal and auto-flagging
#
logging.info("----- Now starting Example 5 -----")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\pmag\\*'),starttime='2011-09-7',endtime=datetime(2011,9,9))
#st.pmplot(['f'],plottitle = "Ex 5 - F vals with outliers")
#st = st.routlier()
#stmod = st.remove_flagged()
#stmod.pmplot(['f'],plottitle = "Ex 5 - Outliers removed")

#
# Flagging - not working correctly yet - check other variables 
#
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!! TODO: check variables etc
# !!! TODO: flagging of f also removes timestamps (set to nan) and thus other components
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
logging.info("----- Now starting Example 6 - Flagging -----")
st = pmRead(path_or_url=os.path.normpath('..\\dat\\lemi025\\*'),starttime='2011-09-7',endtime=datetime(2011,9,9))
st = st.flag_stream('x',3,"Moaing",datetime(2011,9,8,12,0,0,0),datetime(2011,9,8,13,0,0,0))
stmod = st.remove_flagged()
#stmod = stmod.get_gaps(gapvariable=True)
stmod.pmwrite('..\\dat\\output\\txt',format_type='PYSTR')
stmod.pmplot(['x'],plottitle = "Ex 6 - Flagging")

#
# Smoothing and interpolating data
#
logging.info("----- Now starting Example 7 - Smoothing -----")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\lemi025\\*'),starttime='2011-09-7',endtime=datetime(2011,9,9))
#st.pmplot(['x'],plottitle = "Ex 7 - Before smoothing")
#st = st.smooth(['x'],window_len=21)
#st.pmplot(['x'],plottitle = "Ex 7 - After smoothing")
#func = st.interpol(['x','y','z'])
#st.pmplot(['x'],function=func,plottitle = "Ex 7 - After smoothing and interpolation")

#
# Storm analysis and fit functions 
#
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!! TODO: Use H for estimation and calculate index
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
logging.info("----- Now starting Example 8 - Storm analysis -----")
st = pmRead(path_or_url=os.path.normpath('..\\dat\\didd\\*'),starttime='2011-9-8',endtime='2011-9-14')
func = st.fit(['x','y','z'],fitfunc='spline',knotstep=0.1)
st = st.aic_calc('x',timerange=timedelta(hours=1))
st.pmplot(['x','y','z','var2'],function=func,plottitle = "Ex 8 - AIC Analysis")
#fmi = st.k_fmi(fitdegree=2)
#fmi = st.k_fmi(fitfunc='spline',knotstep=0.4)
#col = st._get_column('var2')
#st = st._put_column(col,'y')
#st = st.differentiate()
#st.pmplot(['x','var2','dy','t2'],symbollist = ['-','-','-','z'])
#

#
# Basic spectral investigation 
#
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!! TODO: Develop these functions
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
logging.info("----- Now starting Example 9 - Storm analysis -----")
st.spectrogram('x')
#st.powerspectrum('x')

#
# Auxiliary data
#
logging.info("----- Now starting Example 10 - Auxiliary Temperature data -----")
# Temperature measurements and corrections to time columns
#aux = pmRead(path_or_url=os.path.normpath('..\\dat\\auxiliary\\temp\\Schacht*'))
#aux = aux.date_offset(-timedelta(hours=2)) # correcting times e.g. MET to UTC
#aux = aux.filtered(filter_type='gauss',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30),respect_flags=True)
#func = aux.interpol(['t1','t2','var1'])
#aux.pmplot(['t1','t2','var1'],function=func,plottitle = "Ex 10 - Reading/Analyzing auxiliary data")

# 
# Merging data streams and filling of missing values
#
# Using Aux data
logging.info("----- Now starting Example 11a - Merging auxiliary T data and variometer data -----")
#st = pmRead(path_or_url=os.path.normpath('..\\dat\\didd\\*'))
#newst = mergeStreams(st,aux,keys=['t1','var1'],plottitle = "Ex 11a - Merge Vario and T data")
#newst.pmwrite('..\\dat\\output\\cdf\\didd',filenameends='_didd_min',format_type='PYCDF')
#newst.pmplot(['x','y','z','t1','var1'],symbollist = ['-','-','-','-','-'],plottype='continuous')
#st.spectrogram('x')

# Merging primary and secondary data
logging.info("----- Now starting Example 11b - Filling missing values from secondary instruments -----")
## Preparing DemoStream-1 with gaps
st1 = pmRead(path_or_url=os.path.normpath('..\\dat\\output\\cdf\\didd\\*') ,starttime='2011-9-4',endtime='2011-9-5')
st1 = st1.flag_stream('f',3,"Bycicle",'2011-9-4T10:00:00',datetime(2011,9,4,13,0,0,0))
st1mod = st1.remove_flagged()
st1mod.pmplot(['x','f'],plottitle = "Ex 11b - Primary data set with some flagged records")
## Preparing DemoStream-2
st2 = pmRead(path_or_url=os.path.normpath('..\\dat\\pmag\\*') ,starttime='2011-9-4',endtime='2011-9-5')
st2 = st2.routlier()
st2mod = st2.remove_flagged()
st2mod = st2mod.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
st2mod.pmplot(['f'], plottitle = "Ex 11b - Secondary data set")
logging.info("----- Example 10b - Determining average offset -----")
## Determining average offset - median -> should be known
stdiff = subtractStreams(st1mod,st2mod,keys=['f']) # Stream a gets modified - stdiff = st1mod...
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## ToDo: Trim required - check why
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
stdiff = stdiff.trim(starttime=datetime(2011,9,4,00,02),endtime=datetime(2011,9,4,23,58))
stdiff.pmplot(['f'], plottitle = "Ex 11b - differences")
offset = np.median(stdiff._maskNAN(stdiff._get_column('f')))
## Merging Stream 2 to stream 1 average offset - median
st1new = st1.remove_flagged() # reload st1
st1.pmplot(['x','f'],plottitle = "Ex 10b - Primary data set with some flagged records")
mergest = mergeStreams(st1new,st2mod,keys=['f'],offset=offset)
mergest.pmplot(['x','f'], plottitle = "Ex 10b - offsets")
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## ToDo: Correct flagging before continuing here
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!



# Absolute Values
# ----------------------------------------
from dev_magpy_absolutes import *

# 
# Absolute Values
#
# Using Aux data
logging.info("----- Now starting Example 12 - Analyzing Absolute measurements -----")

#
# Absolute Values
# ---------------
#st = pmRead(path_or_url=os.path.normpath(r'e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*'),starttime='2011-9-1',endtime='2011-9-02')
#st = pmRead(path_or_url=os.path.normpath(r'f:\Vario-Cobenzl\dIdD-System\*'),starttime='2011-8-20',endtime='2011-8-21')
#bas = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_didd.txt'))
#bas.pmplot(['x','y','z'])
#func = bas.fit(['dx','dy','dz'],fitfunc='spline',knotstep=0.05)
#bas.pmplot(['dx','dy','dz'],function=func)

#st.pmplot(['x','y','z'])
#st = st.baseline(bas,knotstep=0.05,plotbaseline=True)
#st.pmplot(['x','y','z'])


#stle = pmRead(path_or_url=os.path.normpath(r'f:\Vario-Cobenzl\dIdD-System\LEMI\*'),starttime='2011-8-20',endtime='2011-8-21')
#basle = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
#stle = stle.baseline(basle,knotstep=0.05,plotbaseline=True)
#stle.pmplot(['x','y','z'])


# Baseline Correction and RotationMatrix
# ---------------
# alpha and beta describe the rotation matrix (alpha is the horizontal angle (D) and beta the vertical)
#didd = pmRead(path_or_url=os.path.normpath('g:\Vario-Cobenzl\dIdD-System\*'),starttime='2011-01-1',endtime='2011-12-31')
#basdidd = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_didd.txt'))
#lemi = pmRead(path_or_url=os.path.normpath('g:\Vario-Cobenzl\dIdD-System\LEMI\*'),starttime='2011-01-1',endtime='2011-12-31')
#baslemi = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
#lemi = lemi.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
#didd = didd.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
#lemi = lemi.rotation(alpha=3.3,beta=0.0)
#didd = didd.baseline(basdidd,knotstep=0.05,plotbaseline=True)
#lemi = lemi.baseline(baslemi,knotstep=0.05,plotbaseline=True)

#didd.pmplot(['x','y','z'])
#lemi.pmplot(['x','y','z'])
#newst = subtractStreams(didd,lemi,keys=['x','y','z'],getmeans=True)
# for some reason first and last points are not subtracted
#newst = newst.trim(starttime=datetime(2011,1,1,02,00),endtime=datetime(2011,12,30,22,00))
#newst = newst.routlier(keys=['x','y','z'])
#print "Mean x: %f" % np.mean(newst._get_column('x'))
#print "Mean y: %f" % np.mean(newst._get_column('y'))
#print "Mean z: %f" % np.mean(newst._get_column('z'))

#newst.pmplot(['x','y','z'])

# DTU data
# ---------------
#dtust = pmRead(path_or_url=os.path.normpath(r'g:\VirtualBox\Ny mappe\GDH4_20091215.cdf'))
#st = pmRead(path_or_url=os.path.normpath(r'g:\VirtualBox\Ny mappe\FHB*.sec'))
#st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
#st = st.aic_calc('x',timerange=timedelta(hours=1))
#col = st._get_column('var2')
#st = st._put_column(col,'y')
#st = st.differentiate()
#st.pmplot(['x','var2','dy'],symbollist = ['-','-','-'])
#dtust = dtust.filtered(filter_type='linear',filter_width=timedelta(hours=1))
#dtust.pmplot(['x','y','z'])

# Comparison of baseline calculated with and without correct orientation of sensor
# ---------------
baslemi1 = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_lemi_alpha3.3.txt'))
baslemi2 = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_didd.txt'))
newst = subtractStreams(baslemi1,baslemi2,keys=['x','y','z'])
newst = newst.trim(starttime=datetime(2010,7,10,00,02),endtime=datetime(2011,10,1,23,58))
newst.pmplot(['x','y','z'])

testarray = np.array(baslemi1)
print testarray[1][2]
print testarray.ndim
# Testing new funcs
#lemi = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\lemi\*'),starttime='2010-7-17',endtime='2010-7-18')
#baslemi = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
#lemi = lemi.rotation(alpha=3.30,beta=-4.5)
#lemi = lemi.baseline(baslemi,knotstep=0.05,plotbaseline=True)
#lemi.pmplot(['x','y','z'])


#st = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*'),starttime='2011-9-1',endtime='2011-9-30')
#st.pmplot(['x','y','z'])
#newst = subtractStreams(bas,st,keys=['x','y','z'])
#newst.pmplot(['x','y','z'])
