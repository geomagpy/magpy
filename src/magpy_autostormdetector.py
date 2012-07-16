#!/usr/bin/env python
"""
MagPy - Example Applications: Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 22.05.2012)
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from core.magpy_stream import *


# Take care: changed flagkeylist from 8 to 12 - see whether this affects anything else

# ----------------------------------------
# ---- Storm analysis and derivatives ----
# ----------------------------------------

#Some definitions
starttime = '2011-9-8' # replace by datetime.utcnow-timedelta(days=3)
endtime='2011-9-14' # datetime.replace by utcnow()
variopath = os.path.join('..','dat','didd','*')
scalarpath = os.path.join('..','dat','didd','*')

starttime = '2011-3-1' # replace by datetime.utcnow-timedelta(days=3)
endtime='2011-3-31' # datetime.replace by utcnow()
variopath = os.path.normpath(r'f:\Vario-Cobenzl\dIdD-System\LEMI\*')
scalarpath = os.path.normpath(r'f:\Vario-Cobenzl\dIdD-System\*')

#
# Read Variometer data
sva = pmRead(path_or_url=variopath,starttime=starttime,endtime=endtime)
# For pure Variodata read Scalar data and merge (ToDO: Insert pear offset !!
ssc = pmRead(path_or_url=scalarpath,starttime=starttime,endtime=endtime)
ssc = ssc.routlier()
ssc = ssc.remove_flagged()
ssc = ssc.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
sinst = mergeStreams(sva,ssc,keys=['f'])


# Baseline correction
abslemi = pmRead(path_or_url=os.path.join('..','dat','absolutes','absolutes_lemi.txt'))
func = abslemi.fit(['dx','dy','dz'],fitfunc='spline',knotstep=0.05)
sinst = sinst.rotation(alpha=3.3,beta=0.0)
sinst = sinst.baseline(abslemi,knotstep=0.05)
sinst = sinst._convertstream('xyz2hdz')
sinst.pmplot(['x','y','z','f'])


#sinst.spectrogram('x',wlen=60,dbscale=True)
sinst = sinst.aic_calc('x',timerange=timedelta(hours=1))
sinst = sinst.differentiate(keys=['var2'],put2keys=['var3'])
sinst.eventlogger('var3',[20,30,50],'>')
stfilt = sinst.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stfilt = stfilt._get_k(key='var2',put2key='var4',scale=[0,70,140,210,280,350,420,490,560])
stfilt.header['col-var4'] = 'Cobs k_-index'
sinst = mergeStreams(sinst,stfilt,key=['var4'])

sinst.pmplot(['x','var2','var3','var4'],bartrange=0.02,symbollist = ['-','-','-','z'],plottitle = "Ex 8 - Storm onsets and local variation index")

