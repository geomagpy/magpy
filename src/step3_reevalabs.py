#!/usr/bin/env python
"""
MagPy - Automatically analyze absolute values from CobsServer - WIK analysis
"""

# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from core.magpy_stream import *
from core.magpy_absolutes import *

# Some definitions
mainpath = r'/home/leon/Dropbox/Daten/Magnetism/'

absolutedatalocation = os.path.join(mainpath,'ABSOLUTE-RAW')
absindentifier = 'AbsoluteMeas.txt' # repeat with D_WIK or just use .txt?

lemipath = os.path.join(mainpath,'LEMI-WIK','data','*')
diddpath = os.path.join(mainpath,'DIDD-WIK','data','*')

writeresultpath = os.path.join(mainpath,'ABSOLUTE-RAW','data')



# Do it for the Lemi
abslemi = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=3.3, beta=0.0, absidentifier=absindentifier, variopath=lemipath, scalarpath=diddpath)
#abslemi.header.clear()
# write the data
abslemi.pmwrite(writeresultpath,coverage='all',mode='replace',filenamebegins='absolutes_lemi')
# make plot covering one year back from now
#start = datetime.utcnow()-timedelta(days=365)
#abslemi = pmRead(path_or_url=os.path.join(writeresultpath,'absolutes_lemi.txt'),starttime=start)
#abslemi.pmplot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from LEMI", outfile="AnalysisLemi")

# Repeat for DIDD but write new logfile and move succesfully analyzed files from the server to the archive
absdidd = analyzeAbsFiles(path_or_url=absolutedatalocation, alpha=0.0, beta=0.0, absidentifier=absindentifier, variopath=diddpath, scalarpath=diddpath)
#absdidd.header.clear()
absdidd.pmwrite(writeresultpath,coverage='all',mode='replace',filenamebegins='absolutes_didd')
#absdidd = pmRead(path_or_url=os.path.join(writeresultpath,'absolutes_didd.txt'),starttime=start)
#absdidd.pmplot(['x','y','z'],plottitle = "Analysis of absolute values - Using variocorr. data from DIDD", outfile="AnalysisDIDD")

del absdidd
# Calculate differences to easily identify errors in one instrument
absdidd = pmRead(path_or_url=os.path.join(writeresultpath,'absolutes_didd.txt'))
abslemi = pmRead(path_or_url=os.path.join(writeresultpath,'absolutes_lemi.txt'))
absdidd.pmplot(['x','y','z'])
absdiff = subtractStreams(absdidd,abslemi,keys=['x','y','z','f']) # Stream_a gets modified - stdiff = st1mod...
absdiff.pmplot(['x','y','z','f'],plottitle = "Differences of absolute values")
#plt.show()


