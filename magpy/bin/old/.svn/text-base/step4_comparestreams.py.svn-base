#!/usr/bin/env python
"""
MagPy - Automatically analyze absolute values from CobsServer - WIK analysis
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from stream import *
from absolutes import *

# Some definitions
mainpath = r'/home/leon/Dropbox/Daten/Magnetism/'

stOPT = read(path_or_url=os.path.join(mainpath,'OPT-WIK','data','*'))
stOPT.plot(['x','y','z'],plottitle="Optical",outfile='tmp.png')

plt.show()

stDIDD =  read(path_or_url=os.path.join(mainpath,'DIDD-WIK','data','*'),starttime='2009-01-01',endtime='2009-06-01')
stDIDDmod = stDIDD.remove_flagged()
# Save this data to the working folder! and use it for baseline calculation
absDIDD = read(path_or_url=os.path.join(mainpath,'ABSOLUTE-RAW','data','absolutes_didd.txt'))
stDIDDmod = stDIDDmod.baseline(absDIDD,knotstep=0.05,plotbaseline=True)
# Filter hour values
stDIDDhou = stDIDDmod.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
stDIDDhou.plot(['x','y','z'],plottitle="DIDD",outfile='tmp.png')


# Calculate differences to easily identify errors in one instrument
diff = subtractStreams(stOPT,stDIDDhou,keys=['x','y','z','f']) # Stream_a gets modified - stdiff = st1mod...
diff.plot(['x','y','z','f'],plottitle = "Differences of DIDD and optical", outfile="AnalysisDifferences")

plt.show()
