 #!/usr/bin/env python
"""
MagPy - Example Applications: Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 22.05.2012)
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from magpy.stream import *


# Take care: changed flagkeylist from 8 to 12 - see whether this affects anything else

# ----------------------------------------
# ---- Storm analysis and derivatives ----
# ----------------------------------------

#Some definitions
#endtime = datetime.utcnow()
endtime=datetime.strptime('2012-10-10',"%Y-%m-%d")
starttime = endtime - timedelta(days=10)
basepath = "/home/leon/Dropbox/Daten/Magnetism"
variopath = os.path.join(basepath,'DIDD-WIK','preliminary','*')
#
# Read Variometer data
stDIDD = read(path_or_url=variopath,starttime=starttime,endtime=endtime)
stDIDD = stDIDD._convertstream('xyz2hdz')

#sinst.spectrogram('x',wlen=600)
stDIDD = stDIDD.aic_calc('x',timerange=timedelta(hours=0.5))
stDIDD = stDIDD.differentiate(keys=['var2'],put2keys=['var3'])
stDIDDfilt = stDIDD.extract('var1',200,'>')
stDIDDnew = stDIDDfilt.eventlogger('var3',[30,40,60],'>',addcomment=True)
try:
    stDIDD = mergeStreams(stDIDD,stDIDDnew ,key=['comment'])
except:
    pass
stfilt = stDIDD.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stfilt = stfilt._get_k(key='var2',put2key='var4',scale=[-50,10,70,130,180,230,280,320,360])
stfilt.header['col-var4'] = 'Cobs k_-index'
stDIDD = mergeStreams(stDIDD,stfilt,key=['var4'])

#stDIDD.plot(['x','var1','var2','var3'],bartrange=0.02,symbollist = ['-','-','-','-'],annotate=True, plottitle = "Ex 8 - Storm onsets and local variation index")
stDIDD.plot(['x','var1','var2','var3','var4'],bartrange=0.02,symbollist = ['-','-','-','-','z'],annotate=True, confinex=True, plottitle = "Ex 8 - Storm onsets and local variation index")

