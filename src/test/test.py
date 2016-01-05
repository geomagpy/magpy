#!/usr/bin/env python
"""
MagPy - WIK analysis
"""


# Non-corrected Variometer and Scalar Data
# ----------------------------------------
from stream import *
from absolutes import *
from transfer import *
from mpplot import *

print "Testing internet connection ..."

print "Starting with library test"
print "Supported formats are:"


# If internet connection available then access WDC
print "WDC formats ..."
# reading WDC data directly from the WDC
# ----------------
stWDC = read(path_or_url='ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc')
#HDZ: stWDC = read(path_or_url='ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/lrv2011.wdc')
stWDC.plot(['x','y','z','f'])

# Testing functions

# Use every function, check content of results (e.g. len) and check log file for error msg

# read an example cdf
x=1/0
