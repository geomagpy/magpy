#!/usr/bin/env python
"""
MagPy - Example Applications for analyzing data from the IAGA WS 2012: Written by Roman Leonhardt 2012
Version 1.0 (from 06.2012)
"""

# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from core.magpy_stream import *
# Absolute Values
# ----------------------------------------
from core.magpy_absolutes import *

basispath = r'/home/leon/Dropbox/Daten/Magnetism/PlayGround'

# Data not available any more 
#hf = pmRead(path_or_url=os.path.normpath('..\\dat\\spain\\Data SanFernando\\07062012.uhf'),starttime=datetime(2012,6,7,00,10),endtime=datetime(2012,6,7,00,14))
#hf.pmplot(['x','y','z'],plottitle = "HF data data")
#hf.powerspectrum('x')
#hf.spectrogram(['x'],wlen=5,dbscale=True,title='Magson - spectra')


# Reading LEMI measurement from the Conrad Observatory. Plotting data, PSD and Spectrogram
# -----------------------------------------------------------------
hf = pmRead(path_or_url=os.path.join(basispath, 'LEMI-HF','20090705v.txt'),starttime=datetime(2009,7,5,03,00),endtime=datetime(2009,7,5,04,00))
hf.pmplot(['x','y','z'],debugmode=True,confinex=True,plottitle = "HF data from the Conrad Observatory")
hf.powerspectrum('x',debugmode=True)
hf.spectrogram(['x'],wlen=10,dbscale=True,title='LEMI at COBS - HF-spectra')


# Reading LEMI measurement of the IAGA-Spain Ws. Plotting data and PSD
# -----------------------------------------------------------------
li = pmRead(path_or_url=os.path.join(basispath, 'SPAIN-IAGA-LEMI','*'),starttime=datetime(2012,6,5,17,00))
li.pmplot(['x','y','z'],confinex=True,plottitle = "Andriys data - IAGA Workshop 2012 Spain")
li.powerspectrum('x')
li.spectrogram(['x'],wlen=60,dbscale=True,title='LEMI at IAGA-Spain - HF-spectra')

# Reading MAGSON measurement of the IAGA-Spain Ws. Plotting data and PSD
# -----------------------------------------------------------------
ua = pmRead(path_or_url=os.path.join(basispath, 'SPAIN-IAGA-MAGSON','*.dat'))
ua = ua._convertstream('hdz2xyz')
ua.pmplot(['x','y','z','t1','t2'],confinex=True,plottitle = "Ulis and Sandras data")
ua.spectrogram(['x','y'],wlen=60,title='Magson - spectra')


# Reading SFS DMI-FGE source data. Applying preliminary (linear) baselinecorrection using workshop measurement.
# Rotation data from SFS (FEG is HDZ oriented)
# -----------------------------------------------------------------
st = pmRead(path_or_url=os.path.join(basispath,'SFS','*.dmi2'),dataformat='SFDMI')
abssp = pmRead(path_or_url=os.path.join(basispath,'SPAIN-IAGA-ABS','absolutes_spain.txt'))
st = st.rotation(alpha=-2.0,beta=0.0)
st = st.baseline(abssp,fitfunc='poly',fitdegree=1,plotbaseline=True)
st.pmplot(['x','y','z','t1','t2'],confinex=True,plottitle = "San Fernando data")
#st.spectrogram(['x','y'],wlen=60,title='San Fernando')


#ua = ua.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
#st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))


# Comparing vectorial data from LEMI (IAGA-ws Spain) and the SFS main instrument (DMI-FGE)
# ------------------------------------------------------------------
stdiff = subtractStreams(st,li,keys=['x','y','z'])
stdiff = stdiff.trim(starttime=datetime(2012,6,5,18,00),endtime=datetime(2012,6,6,6,00))
stdiff.pmplot(['x','y','z','t1'],confinex=True,plottitle = "Diffs between LEMI and FGE")
stdiff.spectrogram(['z'],wlen=60)


# Scalar values of the GSM90 from SFS
# ------------------------------------------------------------------
stf = pmRead(path_or_url=os.path.join(basispath,'SFS','*.gsm'),dataformat='SFGSM')
stf.pmplot(['f'],confinex=True,plottitle = "Scalar values of the GSM90 from SFS")


# Analyzing absolute measurements and creating an absolute mean file
# ------------------------------------------------------------------
#abso = analyzeAbsFiles(path_or_url=os.path.join(basispath,'SPAIN-IAGA-ABS'), alpha=-2.0, beta=0.0, variopath=os.path.join(basispath,'SFS','*.dmi2'), scalarpath=os.path.join(basispath,'SFS','*.gsm'), absidentifier='*CobsMeas.txt', printresults=True)
#abso.pmwrite(os.path.join(basispath,'SPAIN-IAGA-ABS'),coverage='all',mode='replace',filenamebegins='absolutes_spain')

