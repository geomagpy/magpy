#!/usr/bin/env python
"""
MagPy - Example Applications: Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 22.05.2012)
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from core.magpy_stream import *
# Absolute Values
# ----------------------------------------
from core.magpy_absolutes import *


hf = pmRead(path_or_url=os.path.normpath('..\\dat\\spain\\Data SanFernando\\07062012.uhf'),starttime=datetime(2012,6,7,00,10),endtime=datetime(2012,6,7,00,14))
hf.pmplot(['x','y','z'],plottitle = "HF data data")
hf.powerspectrum('x')
hf.spectrogram(['x'],wlen=5,dbscale=True,title='Magson - spectra')

hf = pmRead(path_or_url=os.path.normpath('..\\dat\\lemi-HF\\20090705v.txt'),starttime=datetime(2009,7,5,03,00),endtime=datetime(2009,7,5,04,00))
hf.pmplot(['x','y','z'],debugmode=True,plottitle = "HF data")
hf.powerspectrum('x',debugmode=True)
hf.spectrogram(['x'],wlen=10,dbscale=True,title='HF - spectra')


li = pmRead(path_or_url=os.path.normpath('..\\dat\\spain\\lemi data\\*'),starttime=datetime(2012,6,5,17,00))
li.pmplot(['x','y','z'],plottitle = "Andriys data")
li.powerspectrum('x')

ua = pmRead(path_or_url=os.path.normpath('..\\dat\\spain\\*.dat'))
ua = ua._convertstream('hdz2xyz')
ua.pmplot(['x','y','z','t1','t2'],plottitle = "Ulis and Sandras data")
#ua.spectrogram(['x','y'],wlen=60,title='Magson - spectra')

st = pmRead(path_or_url=os.path.normpath('..\\dat\\spain\\*.dmi2'),dataformat='SFDMI')
abssp = pmRead(path_or_url=os.path.normpath(r'e:\\leon\\Programme\\Python\\TestFiles\\Spain\\Abs\\absolutes_spain.txt'))
st = st.rotation(alpha=-2.0,beta=0.0)
st = st.baseline(abssp,fitfunc='poly',fitdegree=1,plotbaseline=True)
st.pmplot(['x','y','z','t1','t2'],plottitle = "San Fernando data")
#st.spectrogram(['x','y'],wlen=60,title='San Fernando')


#ua = ua.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
#st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))

stdiff = subtractStreams(st,li,keys=['x','y','z'])
stdiff = stdiff.trim(starttime=datetime(2012,6,5,18,00),endtime=datetime(2012,6,6,6,00))
stdiff.pmplot(['x','y','z','t1'],plottitle = "Diffs")
stdiff.spectrogram(['z'],wlen=60)

stf = pmRead(path_or_url=os.path.normpath('..\\dat\\spain\\*.gsm'),dataformat='SFGSM')
stf.pmplot(['f'],plottitle = "Ex 1 - Plot")

abso = analyzeAbsFiles(path_or_url=os.path.normpath('e:\\leon\\Programme\\Python\\TestFiles\\Spain\\Abs'), alpha=-2.0, beta=0.0, variopath=os.path.normpath('..\\dat\\spain\\*.dmi2'), scalarpath=os.path.normpath('..\\dat\\spain\\*.gsm'), absidentifier='*CobsMeas.txt', printresults=True)
abso.pmwrite('e:\\leon\\Programme\\Python\\TestFiles\\Spain\\Abs\\',coverage='all',mode='replace',filenamebegins='absolutes_spain')

