#!/usr/bin/env python
"""
MagPy - Example Applications: Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 22.05.2012)
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from core.magpy_stream import *


# Take care: changed flagkeylist from 8 to 12 - see whether this affects anything else

#
# Storm analysis and derivatives 
#
logging.info("----- Now starting Example 8 - Storm analysis -----")
st = pmRead(path_or_url=os.path.join('..','dat','didd','*'),starttime='2011-9-8',endtime='2011-9-14')
st = st._convertstream('xyz2hdz')
#st.spectrogram('x',wlen=60,dbscale=True)
st = st.aic_calc('x',timerange=timedelta(hours=1))
st = st.differentiate(keys=['var2'],put2keys=['var3'])
st.eventlogger(['var3'],[15,30,45],'>')
stfilt = st.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stfilt = stfilt._get_k(key='var2',put2key='var4',scale=[0,70,140,210,280,350,420,490,560])
st = mergeStreams(st,stfilt,key=['var4'])

st.pmplot(['x','var2','var3','var4'],bartrange=0.02,symbollist = ['-','-','-','z'],plottitle = "Ex 8 - Storm onsets and local variation index")

