#!/usr/bin/env python
"""
MagPy-General: Standard pymag package containing the following classes:
Written by Roman Leonhardt, Rachel Bailey 2011/2012/2013/2014
Written by Roman Leonhardt, Rachel Bailey, Mojca Miklavec 2015/2016
Version 0.3 (starting May 2016)
License:
https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import logging
import os
import sys
import tempfile


# ----------------------------------------------------------------------------
# Part 1: Import routines for packages
# ----------------------------------------------------------------------------

logpygen = ''           # temporary logger variable
badimports = []         # List of missing packages
nasacdfdir = "c:\CDF Distribution\cdf33_1-dist\lib"

# Logging
# ---------
# Select the user's home directory (platform independent) or environment path
if "MAGPY_LOG_PATH" in os.environ:
    path_to_log = os.environ["MAGPY_LOG_PATH"]
    if not os.path.exists(path_to_log):
        os.makedirs(path_to_log)
else:
    path_to_log = tempfile.gettempdir()

def setup_logger(name, warninglevel=logging.WARNING, logfilepath=path_to_log,
                 logformat='%(asctime)s %(levelname)s - %(name)-6s - %(message)s'):
    """Basic setup function to create a standard logging config. Default output
    is to file in /tmp/dir."""


    logfile=os.path.join(logfilepath,'magpy.log')
    # Check file permission/existance
    if not os.path.isfile(logfile):
        pass
    else:
        if os.access(logfile, os.W_OK):
            pass
        else:
            for count in range (1,100):
                logfile=os.path.join(logfilepath,'magpy{:02}.log'.format(count))
                value = os.access(logfile, os.W_OK)
                if value or not os.path.isfile(logfile):
                    count = 100
                    break
    try:
        logging.basicConfig(filename=logfile,
                        filemode='w',
                        format=logformat,
                        level=logging.INFO)
    except:
        logging.basicConfig(format=logformat,
                        level=logging.INFO)
    logger = logging.getLogger(name)
    # Define a Handler which writes "setLevel" messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(warninglevel)
    logger.addHandler(console)

    return logger

# Package loggers to identify info/problem source
logger = setup_logger(__name__)
# DEPRECATED: replaced by individual module loggers, delete these when sure they're no longer needed:
loggerabs = logging.getLogger('abs')
loggertransfer = logging.getLogger('transf')
loggerdatabase = logging.getLogger('db')
loggerstream = logging.getLogger('stream')
loggerlib = logging.getLogger('lib')
loggerplot = logging.getLogger('plot')

# Special loggers for event notification
stormlogger = logging.getLogger('stream')

logger.info("Initiating MagPy...")

from magpy.version import __version__
logger.info("MagPy version "+str(__version__))
magpyversion = __version__

# Standard packages
# -----------------
try:
    import csv
    import pickle
    import types
    import struct
    import re
    import time, string, os, shutil
    import locale
    import copy as cp
    import fnmatch
    from tempfile import NamedTemporaryFile
    import warnings
    from glob import glob, iglob, has_magic
    from itertools import groupby
    import operator # used for stereoplot legend
    from operator import itemgetter
    # The following packages are not identically available for python3
    try:                # python2
        import copy_reg as copyreg
    except ImportError: # python3
        import copyreg as copyreg
    # Python 2 and 3: alternative 4
    try:
        from urllib.parse import urlparse, urlencode
        from urllib.request import urlopen, Request, ProxyHandler, install_opener, build_opener
        from urllib.error import HTTPError
    except ImportError:
        from urlparse import urlparse
        from urllib import urlencode
        from urllib2 import urlopen, Request, HTTPError, ProxyHandler, install_opener, build_opener
    """
    try:                # python2
        import urllib2
    except ImportError: # python3
        import urllib.request
    """
    try:                # python2
        import thread
    except ImportError: # python3
        import _thread
    try:                # python2
        from StringIO import StringIO
        pyvers = 2
    except ImportError: # python 3
        from io import StringIO
        pyvers = 3
except ImportError as e:
    logpygen += "CRITICAL MagPy initiation ImportError: standard packages.\n"
    badimports.append(e)

# operating system
try:
    PLATFORM = sys.platform
    logger.info("Running on platform: {}".format(PLATFORM))
except:
    PLATFORM = 'unkown'

# Matplotlib
# ----------
try:
    import matplotlib
    gui_env = ['TKAgg','GTKAgg','Qt4Agg','WXAgg','Agg']

    try:
        if not os.isatty(sys.stdout.fileno()):   # checks if stdout is connected to a terminal (if not, cron is starting the job)
            logger.info("No terminal connected - assuming cron job and using Agg for matplotlib")
            gui_env = ['Agg','TKAgg','GTKAgg','Qt4Agg','WXAgg']
            matplotlib.use('Agg') # For using cron
    except:
        logger.warning("Problems with identfying cron job - windows system?")
        pass
except ImportError as e:
    logpygen += "CRITICAL MagPy initiation ImportError: problem with matplotlib.\n"
    badimports.append(e)

try:
    version = matplotlib.__version__.replace('svn', '')
    try:
        version = map(int, version.replace("rc","").split("."))
        MATPLOTLIB_VERSION = list(version)
    except:
        version = version.strip("rc")
        MATPLOTLIB_VERSION = version
    logger.info("Loaded Matplotlib - Version %s" % str(MATPLOTLIB_VERSION))

    for gui in gui_env:
        try:
            logger.info("Testing backend {}".format(gui))
            matplotlib.use(gui,warn=False, force=True)
            from matplotlib import pyplot as plt
            break
        except:
            continue
    logger.info("Using backend: {}".format(matplotlib.get_backend()))

    from matplotlib.colors import Normalize
    from matplotlib.widgets import RectangleSelector, RadioButtons
    #from matplotlib.colorbar import ColorbarBase
    from matplotlib import mlab
    from matplotlib.dates import date2num, num2date
    import matplotlib.cm as cm
    from pylab import *
    from datetime import datetime, timedelta
except ImportError as e:
    logpygen += "CRITICAL MagPy initiation ImportError with matplotlib package. Please install to proceed.\n"
    logpygen += " ... if installed please check the permissions on .matplotlib in your homedirectory.\n"
    badimports.append(e)

# Numpy & SciPy
# -------------
try:
    logger.info("Loading Numpy and SciPy...")
    import numpy as np
    import scipy as sp
    from scipy import interpolate
    from scipy import stats
    from scipy import signal
    from scipy.interpolate import UnivariateSpline
    from scipy.ndimage import filters
    import scipy.optimize as op
    import math
except ImportError as e:
    logpygen += "CRITICAL MagPy initiation ImportError: Python numpy-scipy required - please install to proceed.\n"
    badimports.append(e)

# NetCDF
# ------
try:
    #print("Loading Netcdf4 support ...")
    from netCDF4 import Dataset
except ImportError as e:
    #logpygen += "MagPy initiation ImportError: NetCDF not available.\n"
    #logpygen += "... if you want to use NetCDF format support please install a current version.\n"
    #badimports.append(e)
    pass

# NASACDF - SpacePy
# -----------------
def findpath(name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return root
try:
    logger.info("Loading SpacePy package cdf support ...")
    try:
        # check for windows
        nasacdfdir = findpath('libcdf.dll','C:\CDF_Distribution') ## new path since nasaCDF3.6
        if not nasacdfdir:
            nasacdfdir = findpath('libcdf.dll','C:\CDF Distribution')
        if nasacdfdir:
            os.environ["CDF_LIB"] =str(nasacdfdir)
            logger.info("Using CDF lib in %s" % nasacdfdir)
            try:
                import spacepy.pycdf as cdf
                logger.info("... success")
            except KeyError as e:
                # Probably running at boot time - spacepy HOMEDRIVE cannot be detected
                badimports.append(e)
            except:
                logger.info("... Could not import spacepy")
                pass
        else:
            # create exception and try linux
            x=1/0
    except:
        os.putenv("CDF_LIB", "/usr/local/cdf/lib")
        logger.info("using CDF lib in /usr/local/cdf")
        ### If files (with tt_2000) have been generated with an outdated leapsecondtable
        ### an exception will occur - to prevent that:
        ### 1. make sure to use a actual leapsecond table - update cdf regularly
        ### 2. temporarly set cdf_validate environment variable to no
        # This is how option 2 is included TODO -- add this to initialization options
        # as an update of cdf is the way to go and not just deactivating the error message
        os.putenv("CDF_VALIDATE", "no")
        logger.info("deactivating cdf validation")
        try:
            import spacepy.pycdf as cdf
            logger.info("... success")
        except KeyError as e:
            # Probably running at boot time - spacepy HOMEDRIVE cannot be detected
            badimports.append(e)
        except:
            logger.info("... Could not import spacepy")
            pass
except ImportError as e:
    logpygen += "MagPy initiation ImportError: NASA cdf not available.\n"
    logpygen += "... if you want to use NASA CDF format support please install a current version.\n"
    badimports.append(e)

if logpygen == '':
    logpygen = "OK"
else:
    logger.info(logpygen)
    logger.info("Missing packages:")
    for item in badimports:
        logger.info(item)
    logger.info("Moving on anyway...")

### Some Python3/2 compatibility code
### taken from http://www.rfk.id.au/blog/entry/preparing-pyenchant-for-python-3/
try:
    unicode = unicode
    # 'unicode' exists, must be Python 2
    str = str
    unicode = unicode
    bytes = str
    basestring = basestring
except NameError:
    # 'unicode' is undefined, must be Python 3
    str = str
    unicode = str
    bytes = bytes
    basestring = (str,bytes)

# Storing function - http://bytes.com/topic/python/answers/552476-why-cant-you-pickle-instancemethods#edit2155350
# by Steven Bethard
# Used here to pickle baseline functions from header and store it in a cdf key.
# Not really a transparent method but working nicely. Underlying functional parameters to reconstruct the fit
# are stored as well but would require a link to the absolute data.
def _pickle_method(method):
    func_name = method.__func__.__name__
    obj = method.__self__
    cls = method.__self__.__class__
    return _unpickle_method, (func_name, obj, cls)

def _unpickle_method(func_name, obj, cls):
    for cls in cls.mro():
       try:
           func = cls.__dict__[func_name]
       except KeyError:
           pass
       else:
           break
    return func.__get__(obj, cls)

copyreg.pickle(types.MethodType, _pickle_method, _unpickle_method)

# ----------------------------------------------------------------------------
# Part 2: Define Dictionaries
# ----------------------------------------------------------------------------

# Keys available in DataStream Object:
KEYLIST = [     'time',         # Timestamp (date2num object)
                'x',            # X or I component of magnetic field (float)
                'y',            # Y or D component of magnetic field (float)
                'z',            # Z component of magnetic field (float)
                'f',            # Magnetic field strength (float)
                't1',           # Temperature variable (e.g. ambient temp) (float)
                't2',           # Secondary temperature variable (e.g. sensor temp) (float)
                'var1',         # Extra variable #1 (float)
                'var2',         # Extra variable #2 (float)
                'var3',         # Extra variable #3 (float)
                'var4',         # Extra variable #4 (float)
                'var5',         # Extra variable #5 (float)
                'dx',           # Errors in X (float)
                'dy',           # Errors in Y (float)
                'dz',           # Errors in Z (float)
                'df',           # Errors in F (float)
                'str1',         # Extra string variable #1 (str)
                'str2',         # Extra string variable #2 (str)
                'str3',         # Extra string variable #3 (str)
                'str4',         # Extra string variable #4 (str)
                'flag',         # Variable for flags. (str='0000000000000000-')
                'comment',      # Space for comments on flags (str)
                'typ',          # Type of data (str='xyzf')
                'sectime'       # Secondary time variable (date2num)
            ]

NUMKEYLIST = KEYLIST[1:16]
# Empty key values at initiation of stream:
KEYINITDICT = {'time':0,'x':float('nan'),'y':float('nan'),'z':float('nan'),'f':float('nan'),
                't1':float('nan'),'t2':float('nan'),'var1':float('nan'),'var2':float('nan'),
                'var3':float('nan'),'var4':float('nan'),'var5':float('nan'),'dx':float('nan'),
                'dy':float('nan'),'dz':float('nan'),'df':float('nan'),'str1':'-','str2':'-',
                'str3':'-','str4':'-','flag':'0000000000000000-','comment':'-','typ':'xyzf',
                'sectime':float('nan')}
FLAGKEYLIST = KEYLIST[:16]
# KEYLIST[:8] # only primary values with time
# KEYLIST[1:8] # only primary values without time

# Formats supported by MagPy read function:
PYMAG_SUPPORTED_FORMATS = {
                'IAGA':['rw','IAGA 2002 text format'],
                'WDC':['rw','World Data Centre format'],
                'IMF':['rw', 'Intermagnet Format'],
                'IAF':['rw', 'Intermagnet archive Format'],
                'IMAGCDF':['rw','Intermagnet CDF Format'],
                'BLV':['rw','Baseline format Intermagnet'],
                'IYFV':['rw','Yearly mean format Intermagnet'],
                'DKA':['rw', 'K value format Intermagnet'],
                'DIDD':['rw','Output format from MinGeo DIDD'],
                'GSM19':['r', 'Output format from GSM19 magnetometer'],
                'JSON':['rw', 'JavaScript Object Notation'],
                'LEMIHF':['r', 'LEMI text format data'],
                'LEMIBIN':['r','Current LEMI binary data format'],
                'LEMIBIN1':['r','Deprecated LEMI binary format at WIC'],
                'OPT':['r', 'Optical hourly data from WIK'],
                'PMAG1':['r','Deprecated ELSEC from WIK'],
                'PMAG2':['r', 'Current ELSEC from WIK'],
                'GDASA1':['r', 'GDAS binary format'],
                'GDASB1':['r', 'GDAS text format'],
                'RMRCS':['r', 'RCS data output from Richards perl scripts'],
                'RCS':['r', 'RCS raw output'],
                'METEO':['r', 'Winklbauer METEO files'],
                'NEIC':['r', 'WGET data from USGS - NEIC'],
                'LNM':['r', 'Thies Laser-Disdrometer'],
                'IWT':['r', 'IWT Tiltmeter data'],
                'LIPPGRAV':['r', 'Lippmann Tiltmeter data'],
                'GRAVSG':['r', 'GWR TSF data'],
                'CR800':['r', 'CR800 datalogger'],
                'IONO':['r', 'IM806 Ionometer'],
                'RADON':['r', 'single channel analyser gamma data'],
                'USBLOG':['r', 'USB temperature logger'],
                #'SERSIN':['r', '?'],
                #'SERMUL':['r', '?'],
                'PYSTR':['rw', 'MagPy full ascii'],
                'AUTODIF':['r', 'Deprecated - AutoDIF ouput data'],
                'AUTODIF_FREAD':['r', 'Deprecated - Special format for AutoDIF read-in'],
                'PYCDF':['rw', 'MagPy CDF variant'],
                'PYBIN':['r', 'MagPy own binary format'],
                'PYASCII':['rw', 'MagPy basic ASCII'],
                'POS1TXT':['r', 'POS-1 text format output data'],
                'POS1':['r', 'POS-1 binary output at WIC'],
                'PMB':['r', 'POS pmb file'],
                'QSPIN':['r', 'QSPIN ascii output'],
                #'PYNC':['r', 'MagPy NetCDF variant (too be developed)'],
                #'DTU1':['r', 'ASCII Data from the DTUs FGE systems'],
                #'BDV1':['r', 'Budkov GDAS data variant'],
                'GFZKP':['r', 'GeoForschungsZentrum KP-Index format'],
                'NOAAACE':['r', 'NOAA ACE satellite data format'],
                'PHA':['r', 'Potentially Hazardous Asteroids (PHAs) from the International Astronomical Unions Minor Planet Center, (json, incomplete)'],
                'NETCDF':['r', 'NetCDF4 format, NOAA DSCOVR satellite data archive format'],
                'LATEX':['w','LateX data'],
                'CS':['r','Cesium G823'],
                #'SFDMI':['r', 'San Fernando variometer'],
                #'SFGSM':['r', 'San Fernando GSM90'],
                'UNKOWN':['-','Unknown']
                        }
"""
PYMAG_SUPPORTED_FORMATS = {
                'IAGA':'rw',         # IAGA 2002 text format
                'WDC':'rw',          # World Data Centre format
                'IMF':'rw',          # Intermagnet Format
                'IAF':'rw',          # Intermagnet archive Format
                'IMAGCDF',      # Intermagnet CDF Format
                'BLV',          # Baseline format Intermagnet
                'IYFV',         # Yearly mean format Intermagnet
                'DKA',          # K value format Intermagnet
                'DIDD',         # Output format from DIDD
                'GSM19',        # Output format from GSM19 magnetometer
                'JSON',         # JavaScript Object Notation
                'LEMIHF',       # LEMI text format data
                'LEMIBIN',      # Current LEMI binary data format at WIC
                'LEMIBIN1',     # Deprecated LEMI binary format at WIC
                'OPT',          # Optical hourly data from WIK
                'PMAG1',        # Deprecated ELSEC from WIK
                'PMAG2',        # Current ELSEC from WIK
                'GDASA1',       # ?
                'GDASB1',       # ?
                'RMRCS',        # RCS data output from Richards perl scripts
                'RCS',        # RCS data output from Richards perl scripts
                'METEO',        # RCS data output in METEO files
                'NEIC',        # WGET data from USGS - NEIC
                'LNM',          # LaserNiederschlagsMonitor files
                'IWT',          # Tiltmeter data files at cobs
                'LIPPGRAV',     # Lippmann Tiltmeter data files at cobs
                'CR800',        # Data from the CR800 datalogger
                'IONO',         # Data from IM806 Ionometer
                'RADON',        # ?
                'USBLOG',       # ?
                'SERSIN',       # ?
                'SERMUL',       # ?
                'PYSTR',        # MagPy full ascii
                'AUTODIF',      # AutoDIF ouput data
                'AUTODIF_FREAD',# Special format for AutoDIF read-in
                'PYCDF',        # MagPy CDF variant
                'PYBIN',        # MagPy own format
                'PYASCII',      # MagPy basic ASCII
                'POS1TXT',      # POS-1 text format output data
                'POS1',         # POS-1 binary output at WIC
                'PMB',          # POS pmb output
                'QSPIN',        # QSpin output
                'PYNC',         # MagPy NetCDF variant (too be developed)
                'DTU1',         # ASCII Data from the DTU's FGE systems
                'SFDMI',        # ?
                'SFGSM',        # ?
                'BDV1',         # ?
                'GFZKP',        # GeoForschungsZentrum KP-Index format
                'NOAAACE',      # NOAA ACE satellite data format
                'LATEX',        # LateX data
                'CS',           # ?
                'UNKOWN'        # 'Unknown'?
                        }
"""

# ----------------------------------------------------------------------------
#  Part 3: Example files for easy access and tests
# ----------------------------------------------------------------------------

from pkg_resources import resource_filename
example1 = resource_filename('magpy', 'examples/example1.cdf')  #ImagCDF
example2 = resource_filename('magpy', 'examples/example2.bin')  #IAF
example3 = resource_filename('magpy', 'examples/2015-03-25_08-18-00_A2_WIC.txt')
example4 = resource_filename('magpy', 'examples/example4.txt')  #MagPy Str with basevalues
example5 = resource_filename('magpy', 'examples/example5.cdf')  #MagPy CDF

# ----------------------------------------------------------------------------
#  Part 4: Main classes -- DataStream, LineStruct and
#      PyMagLog (To be removed)
# ----------------------------------------------------------------------------

class DataStream(object):
    """
    Creates a list object from input files /url data
    data is organized in columns

    keys are column identifier:
    key in keys: see KEYLIST

    A note on headers:
    ALWAYS INITIATE STREAM WITH >>> stream = DataStream([],{}).

    All available methods:
    ----------------------------

    - stream.ext(self, columnstructure): # new version of extend function for column operations
    - stream.add(self, datlst):
    - stream.clear_header(self):
    - stream.extend(self,datlst,header):
    - stream.union(self,column):
    - stream.findtime(self,time):
    - stream._find_t_limits(self):
    - stream._print_key_headers(self):
    - stream._get_key_headers(self,**kwargs):
    - stream.sorting(self):
    - stream._get_line(self, key, value):
    - stream._remove_lines(self, key, value):
    - stream._remove_columns(self, keys):
    - stream._get_column(self, key):
    - stream._put_column(self, column, key, **kwargs):
    - stream._move_column(self, key, put2key):
    - stream._clear_column(self, key):
    - stream._reduce_stream(self, pointlimit=100000):
    - stream._aic(self, signal, k, debugmode=None):
    - stream._get_k(self, **kwargs):
    - stream._get_k_float(self, value, **kwargs):
    - stream._get_max(self, key, returntime=False):
    - stream._get_min(self, key, returntime=False):
    - stream._gf(self, t, tau):
    - stream._hf(self, p, x):
    - stream._residual_func(self, func, y):
    - stream._tau(self, period):
    - stream._convertstream(self, coordinate, **kwargs):
    - stream._det_trange(self, period):
    - stream._is_number(self, s):
    - stream._normalize(self, column):
    - stream._testtime(self, time):
    - stream._drop_nans(self, key):
    - stream.aic_calc(self, key, **kwargs):
    - stream.baseline(self, absolutestream, **kwargs):
    - stream.bindetector(self,key,text=None,**kwargs):
    - stream.calc_f(self, **kwargs):
    - stream.cut(self,length,kind=0,order=0):
    - stream.dailymeans(self):
    - stream.date_offset(self, offset):
    - stream.delta_f(self, **kwargs):
    - stream.dict2stream(self,dictkey='DataBaseValues')
    - stream.differentiate(self, **kwargs):
    - stream.eventlogger(self, key, values, compare=None, stringvalues=None, addcomment=None, debugmode=None):
    - stream.extract(self, key, value, compare=None, debugmode=None):
    - stream.extrapolate(self, start, end):
    - stream.filter(self, **kwargs):
    - stream.fit(self, keys, **kwargs):
    - stream.flag_outlier(self, **kwargs):
    - stream.flag_stream(self, key, flag, comment, startdate, enddate=None, samplingrate):
    - stream.func2stream(self,function,**kwargs):
    - stream.func_add(self,function,**kwargs):
    - stream.func_subtract(self,function,**kwargs):
    - stream.get_gaps(self, **kwargs):
    - stream.get_sampling_period(self):
    - stream.samplingrate(self, **kwargs):
    - stream.integrate(self, **kwargs):
    - stream.interpol(self, keys, **kwargs):
    - stream.k_fmi(self, **kwargs):
    - stream.mean(self, key, **kwargs):
    - stream.multiply(self, factors):
    - stream.offset(self, offsets):
    - stream.randomdrop(self, percentage=None, fixed_indicies=None):
    - stream.remove(self, starttime=starttime, endtime=endtime):
    - stream.remove_flagged(self, **kwargs):
    - stream.resample(self, keys, **kwargs):
    - stream.rotation(self,**kwargs):
    - stream.scale_correction(self, keys, scales, **kwargs):
    - stream.smooth(self, keys, **kwargs):
    - stream.steadyrise(self, key, timewindow, **kwargs):
    - stream.stream2dict(self,dictkey='DataBaseValues')
    - stream.stream2flaglist(self, userange=True, flagnumber=None, keystoflag=None, sensorid=None, comment=None)
    - stream.trim(self, starttime=None, endtime=None, newway=False):
    - stream.variometercorrection(self, variopath, thedate, **kwargs):
    - stream.write(self, filepath, **kwargs):


    Application methods:
    ----------------------------

    - stream.aic_calc(key) -- returns stream (with !var2! filled with aic values)
    - stream.baseline() -- calculates baseline correction for input stream (datastream)
    - stream.dailymeans() -- for DI stream - obtains variometer corrected means fo basevalues
    - stream.date_offset() -- Corrects the time column of the selected stream by the offst
    - stream.delta_f() -- Calculates the difference of x+y+z to f
    - stream.differentiate() -- returns stream (with !dx!,!dy!,!dz!,!df! filled by derivatives)
    - stream.extrapolate() -- read absolute stream and extrapolate the data
    - stream.fit(keys) -- returns function
    - stream.filter() -- returns stream (changes sampling_period; in case of fmi ...)
    - stream.find_offset(stream_a, stream_b) -- Finds offset of two data streams. (Not optimised.)
    - stream.flag_stream() -- Add flags to specific times or time ranges
    - stream.func2stream() -- Combine stream and function (add, subtract, etc)
    - stream.func_add() -- Add a function to the selected values of the data stream
    - stream.func_subtract() -- Subtract a function from the selected values of the data stream
    - stream.get_gaps() -- Takes the dominant sample frequency and fills non-existing time steps
    - stream.get_sampling_period() -- returns the dominant sampling frequency in unit ! days !
    - stream.integrate() -- returns stream (integrated vals at !dx!,!dy!,!dz!,!df!)
    - stream.interpol(keys) -- returns function
    - stream.k_fmi() -- Calculating k values following the fmi approach
    - stream.linestruct2ndarray() -- converts linestrcut data to ndarray. should be avoided
    - stream.mean() -- Calculates mean values for the specified key, Nan's are regarded for
    - stream.offset() -- Apply constant offsets to elements of the datastream
    - stream.plot() -- plot keys from stream
    - stream.powerspectrum() -- Calculating the power spectrum following the numpy fft example
    - stream.remove_flagged() -- returns stream (removes data from stream according to flags)
    - stream.resample(period) -- Resample stream to given sampling period.
    - stream.rotation() -- Rotation matrix for rotating x,y,z to new coordinate system xs,ys,zs
    - stream.selectkeys(keys) -- ndarray: remove all data except for provided keys (and flag/comment)
    - stream.smooth(key) -- smooth the data using a window with requested size
    - stream.spectrogram() -- Creates a spectrogram plot of selected keys
    - stream.stream2flaglist() -- make flaglist out of stream
    - stream.trim() -- returns stream within new time frame
    - stream.use_sectime() -- Swap between primary and secondary time (if sectime is available)
    - stream.variometercorrection() -- Obtain average DI values at certain timestep(s)
    - stream.write() -- Writing Stream to a file

    Supporting INTERNAL methods:
    ----------------------------

    A. Standard functions and overrides for list like objects
    - self.clear_header(self) -- Clears headers
    - self.extend(self,datlst,header) -- Extends stream object
    - self.sorting(self) -- Sorts object

    B. Internal Methods I: Line & column functions
    - self._get_column(key) -- returns a numpy array of selected columns from Stream
    - self._put_column(key) -- adds a column to a Stream
    - self._move_column(key, put2key) -- moves one column to another key
    - self._clear_column(key) -- clears a column to a Stream
    - self._get_line(self, key, value) -- returns a LineStruct element corresponding to the first occurence of value within the selected key
    - self._reduce_stream(self) -- Reduces stream below a certain limit.
    - self._remove_lines(self, key, value) -- removes lines with value within the selected key
    - self.findtime(self,time) -- returns index and line for which time equals self.time

    B. Internal Methods II: Data manipulation functions
    - self._aic(self, signal, k, debugmode=None) -- returns float -- determines Akaki Information Criterion for a specific index k
    - self._get_k(self, **kwargs) -- Calculates the k value according to the Bartels scale
    - self._get_k_float(self, value, **kwargs) -- Like _get_k, but for testing single values and not full stream keys (used in filtered function)
    - self._gf(self, t, tau):  -- Gauss function
    - self._hf(self, p, x) -- Harmonic function
    - self._residual_func(self, func, y) -- residual of the harmonic function
    - self._tau(self, period) -- low pass filter with -3db point at period in sec (e.g. 120 sec)

    B. Internal Methods III: General utility & NaN handlers
    - self._convertstream(self, coordinate, **kwargs) -- Convert coordinates of x,y,z columns in stream
    - self._det_trange(self, period) -- starting with coefficients above 1%
    - self._find_t_limits(self) -- return times of first and last stream data points
    - self._testtime(time) -- returns datetime object
    - self._get_min(key) -- returns float
    - self._get_max(key) -- returns float
    - self._normalize(column) -- returns list,float,float -- normalizes selected column to range 0,1
    - nan_helper(self, y) -- Helper to handle indices and logical indices of NaNs
    - self._print_key_headers(self) -- Prints keys in datastream with variable and unit.
    - self._get_key_headers(self) -- Returns keys in datastream.
    - self._drop_nans(self, key) -- Helper to drop lines with NaNs in any of the selected keys.
    - self._is_number(self, s) -- ?

    Supporting EXTERNAL methods:
    ----------------------------

    Useful functions:
    - array2stream -- returns a data stream  -- converts a list of arrays to a datastream
    - linestruct2ndarray -- returns a data ndarray  -- converts a old linestruct format
    - denormalize -- returns list -- (column,startvalue,endvalue) denormalizes selected column from range 0,1 ro sv,ev
    - find_nearest(array, value) -- find point in array closest to value
    - maskNAN(column) -- Tests for NAN values in array and usually masks them
    - nearestPow2(x) -- Find power of two nearest to x

*********************************************************************
    Standard function description format:

    DEFINITION:
        Description of function purpose and usage.

    PARAMETERS:
    Variables:
        - variable:     (type) Description.
    Kwargs:
        - variable:     (type) Description.

    RETURNS:
        - variable:     (type) Description.

    EXAMPLE:
        >>> alldata = mergeStreams(pos_stream, lemi_stream, keys=['x','y','z'])

    APPLICATION:
        Code for simple application.

*********************************************************************
    Standard file description format:

Path:                   *path*     (magpy.acquisition.pos1protocol)
Part of package:        *package*  (acquisition)
Type:                   *type*     (type of file/package)

PURPOSE:
        Description...

CONTAINS:
        *ThisClass:     (Class)
                        What is this class for?
        thisFunction:   (Func) Description

DEPENDENCIES:
        List all non-standard packages required for file.
        + paths of all MagPy package dependencies.

CALLED BY:
        Path to magpy packages that call this part, e.g. magpy.bin.acquisition

*********************************************************************
    """

    KEYLIST = [ 'time',         # Timestamp (date2num object)
                'x',            # X or I component of magnetic field (float)
                'y',            # Y or D component of magnetic field (float)
                'z',            # Z component of magnetic field (float)
                'f',            # Magnetic field strength (float)
                't1',           # Temperature variable (e.g. ambient temp) (float)
                't2',           # Secondary temperature variable (e.g. sensor temp) (float)
                'var1',         # Extra variable #1 (float)
                'var2',         # Extra variable #2 (float)
                'var3',         # Extra variable #3 (float)
                'var4',         # Extra variable #4 (float)
                'var5',         # Extra variable #5 (float)
                'dx',           # Errors in X (float)
                'dy',           # Errors in Y (float)
                'dz',           # Errors in Z (float)
                'df',           # Errors in F (float)
                'str1',         # Extra string variable #1 (str)
                'str2',         # Extra string variable #2 (str)
                'str3',         # Extra string variable #3 (str)
                'str4',         # Extra string variable #4 (str)
                'flag',         # Variable for flags. (str='0000000000000000-')
                'comment',      # Space for comments on flags (str)
                'typ',          # Type of data (str='xyzf')
                'sectime'       # Secondary time variable (date2num)
                ]
    NUMKEYLIST = KEYLIST[1:16]

    def __init__(self, container=None, header={},ndarray=None):
        if container is None:
            container = []
        self.container = container
        if ndarray is None:
            ndarray = np.array([np.asarray([]) for elem in KEYLIST])
        self.ndarray = ndarray ## Test this! -> for better memory efficiency
        #if header is None:
        #    header = {'Test':'Well, it works'}
            #header = {}
        self.header = header
        #for key in KEYLIST:
        #    setattr(self,key,np.asarray([]))
        #self.header = {'Test':'Well, it works'}
        self.progress = 0

    # ------------------------------------------------------------------------
    # A. Standard functions and overrides for list like objects
    # ------------------------------------------------------------------------

    def ext(self, columnstructure): # new version of extend function for column operations
        """
        the extend and add functions must be replaced in case of
        speed optimization
        """
        for key in KEYLIST:
            self.container.key = np.append(self.container.key, columnstructure.key, 1)

    def add(self, datlst):
        #try:
        assert isinstance(self.container, (list, tuple))
        self.container.append(datlst)
        #except:
        #    print list(self.container).append(datlst)


    def length(self):
        #try:
        if len(self.ndarray[0]) > 0:
            ll = [len(elem) for elem in self.ndarray]
            return ll
        else:
            try: ## might fail if LineStruct is empty (no time)
                if len(self) == 1 and np.isnan(self[0].time):
                    return [0]
                else:
                    return [len(self)]
            except:
                return [0]

    def replace(self, datlst):
        # Replace in stream
        # - replace value with existing data
        # Method was used by K calc - replaced by internal method there
        newself = DataStream()
        assert isinstance(self.container, (list, tuple))
        ti = list(self._get_column('time'))
        try:
           ind = ti.index(datlst.time)
        except ValueError:
           self = self.add(datlst)
           return self
        except:
           return self
        li = [elem for elem in self]
        del li[ind]
        del ti[ind]
        li.append(datlst)
        return DataStream(li,self.header)

    def copy(self):
        """
        DESCRIPTION:
           method for copying content of a stream to a new stream
        APPLICATION:
           for non-destructive methods
        """
        #print self.container
        #assert isinstance(self.container, (list, tuple))
        co = DataStream()
        #co.header = self.header
        newheader = {}
        for el in self.header:
            newheader[el] = self.header[el]

        array = [[] for el in KEYLIST]
        if len(self.ndarray[0])> 0:
            for ind, key in enumerate(KEYLIST):
                liste = []
                for val in self.ndarray[ind]: ## This is necessary to really copy the content
                    liste.append(val)
                array[ind] = np.asarray(liste)
            co.container = [LineStruct()]
        else:
            for el in self:
                li = LineStruct()
                for key in KEYLIST:
                    if key == 'time':
                        li.time = el.time
                    else:
                        #exec('li.'+key+' = el.'+key)
                        elkey = getattr(el,key)
                        setattr(li, key, elkey)
                co.add(li)

        return DataStream(co.container,newheader,np.asarray(array, dtype=object))


    def __str__(self):
        return str(self.container)

    def __repr__(self):
        return str(self.container)

    def __getitem__(self, var):
        try:
            if var in NUMKEYLIST:
                return self.ndarray[self.KEYLIST.index(var)].astype(np.float64)
            else:
                return self.ndarray[self.KEYLIST.index(var)]
        except:
            return self.container.__getitem__(var)

    def __setitem__(self, var, value):
        self.ndarray[self.KEYLIST.index(var)] = value

    def __len__(self):
        return len(self.container)

    def clear_header(self):
        """
        Remove header information
        """
        self.header = {}

    def extend(self,datlst,header,ndarray):
        array = [[] for key in KEYLIST]
        self.container.extend(datlst)
        self.header = header
        # Some initial check if any data set except timecolumn is contained
        datalength = len(ndarray)

        #t1 = datetime.utcnow()
        if pyvers and pyvers == 2:
                ch1 = '-'.encode('utf-8') # not working with py3
                ch2 = ''.encode('utf-8')
        else:
                ch1 = '-'
                ch2 = ''

        try:
            test = []

            for col in ndarray:
                col = np.array(list(col))
                #print (np.array(list(col)).dtype)
                if col.dtype in ['float64','float32','int32','int64']:
                    try:
                        x = np.asarray(col)[~np.isnan(col)]
                    except: # fallback 1 -> should not needed any more
                        #print ("Fallback1")
                        x = np.asarray([elem for elem in col if not np.isnan(elem)]) 
                else:
                    #y = np.asarray(col)[col!='-']
                    #x = np.asarray(y)[y!='']
                    y = np.asarray(col)[col!=ch1]
                    x = np.asarray(y)[y!=ch2]
                test.append(x)
            test = np.asarray(test)
        except:
            # print ("Fallback -- pretty slowly")
            #print ("Fallback2")
            test = [[elem for elem in col if not elem in [ch1,ch2]] for col in ndarray]
        #t2 = datetime.utcnow()
        #print (t2-t1)

        emptycnt = [len(el) for el in test if len(el) > 0]

        if self.ndarray.size == 0:
            self.ndarray = ndarray
        elif len(emptycnt) == 1:
            print("Tyring to extend with empty data set")
            #self.ndarray = np.asarray((list(self.ndarray)).extend(list(ndarray)))
        else:
            for idx,elem in enumerate(self.ndarray):
                if len(ndarray[idx]) > 0:
                    if len(self.ndarray[idx]) > 0 and len(self.ndarray[0]) > 0:
                        array[idx] = np.append(self.ndarray[idx], ndarray[idx]).astype(object)
                        #array[idx] = np.append(self.ndarray[idx], ndarray[idx],1).astype(object)
                    elif len(self.ndarray[0]) > 0: # only time axis present so far but no data within this elem
                        fill = ['-']
                        key = KEYLIST[idx]
                        if key in NUMKEYLIST or key=='sectime':
                            fill = [float('nan')]
                        nullvals = np.asarray(fill * len(self.ndarray[0]))
                        #array[idx] = np.append(nullvals, ndarray[idx],1).astype(object)
                        array[idx] = np.append(nullvals, ndarray[idx]).astype(object)
                    else:
                        array[idx] = ndarray[idx].astype(object)
            self.ndarray = np.asarray(array)

    def union(self,column):
        seen = set()
        seen_add = seen.add
        return [ x for x in column if not (x in seen or seen_add(x))]


    def removeduplicates(self):
        """
        DESCRIPTION:
            Identify duplicate time stamps and remove all data.
            Lines with first occurence are kept.

       """
        # get duplicates in time column
        def list_duplicates(seq):
            seen = set()
            seen_add = seen.add
            return [idx for idx,item in enumerate(seq) if item in seen or seen_add(item)]

        if not len(self.ndarray[0]) > 0:
            print ("removeduplicates: works only with ndarrays")
            return

        duplicateindicies = list_duplicates(self.ndarray[0])

        array = [[] for key in KEYLIST]
        for idx, elem in enumerate(self.ndarray):
            if len(elem) > 0:
                newelem = np.delete(elem, duplicateindicies)
                array[idx] = newelem

        return DataStream(self, self.header, np.asarray(array))


    def start(self, dateformt=None):
        st,et = self._find_t_limits()
        return st

    def end(self, dateformt=None):
        st,et = self._find_t_limits()
        return et

    def findtime(self,time,**kwargs):
        """
        DEFINITION:
            Find a line within the container which contains the selected time step
            or the first line following this timestep (since 0.3.99 using mode 'argmax')
        VARIABLES:
            startidx    (int) index to start search with (speeding up)
            endidx      (int) index to end search with (speeding up)
            mode        (string) define search mode (fastest would be 'argmax')

        RETURNS:
            The index position of the line and the line itself
        """
        startidx = kwargs.get('startidx')
        endidx = kwargs.get('endidx')
        mode = kwargs.get('mode')

        #try:
        #    from bisect import bisect
        #except ImportError:
        #    print("Import error")

        st = date2num(self._testtime(time))
        if len(self.ndarray[0]) > 0:
            if startidx and endidx:
                ticol = self.ndarray[0][startidx:endidx]
            elif startidx:
                ticol = self.ndarray[0][startidx:]
            elif endidx:
                ticol = self.ndarray[0][:endidx]
            else:
                ticol = self.ndarray[0]
            try:
                if mode =='argmax':
                    ## much faster since 0.3.99 (used in flag_stream)
                    indexes = [np.argmax(ticol>=st)]
                else:
                    ## the following method is used until 0.3.98
                    indexes = [i for i,x in enumerate(ticol) if x == st]    ### FASTER
                # Other methods
                # #############
                #indexes = [i for i,x in enumerate(ticol) if np.allclose(x,st,rtol=1e-14,atol=1e-17)]  # if the two time equal within about 0.7 milliseconds
                #indexes = [bisect(ticol, st)]   ## SELECTS ONLY INDEX WHERE VALUE SHOULD BE inserted
                #indexes = [ticol.index(st)]
                #print("findtime", indexes)
                if not len(indexes) == 0:
                    if startidx:
                        retindex = indexes[0] + startidx
                    else:
                        retindex = indexes[0]
                    #print("Findtime index:",retindex)
                    return retindex, LineStruct()
                else:
                    return 0, []
                #return list(self.ndarray[0]).index(st), LineStruct()
            except:
                logger.warning("findtime: Didn't find selected time - returning 0")
                return 0, []
        for index, line in enumerate(self):
            if line.time == st:
                return index, line
        logger.warning("findtime: Didn't find selected time - returning 0")
        return 0, []

    def _find_t_limits(self):
        """
        DEFINITION:
            Find start and end times in stream.
        RETURNS:
            Two datetime objects, start and end.
        """

        if len(self.ndarray[0]) > 0:
            t_start = num2date(np.min(self.ndarray[0].astype(float))).replace(tzinfo=None)
            t_end = num2date(np.max(self.ndarray[0].astype(float))).replace(tzinfo=None)
        else:
            try: # old type
                t_start = num2date(self[0].time).replace(tzinfo=None)
                t_end = num2date(self[-1].time).replace(tzinfo=None)
            except: # empty
                t_start,t_end = None,None

        return t_start, t_end

    def _print_key_headers(self):
        print("%10s : %22s : %28s" % ("MAGPY KEY", "VARIABLE", "UNIT"))
        for key in FLAGKEYLIST[1:]:
            try:
                header = self.header['col-'+key]
            except:
                header = None
            try:
                unit = self.header['unit-col-'+key]
            except:
                unit = None
            print("%10s : %22s : %28s" % (key, header, unit))


    def _get_key_headers(self,**kwargs):
        """
    DEFINITION:
        get a list of existing numerical keys in stream.

    PARAMETERS:
    kwargs:
        - limit:        (int) limit the lenght of the list
        - numerical:    (bool) if True, select only numerical keys
    RETURNS:
        - keylist:      (list) a list like ['x','y','z']

    EXAMPLE:
        >>> data_stream._get_key_headers(limit=1)
        """

        limit = kwargs.get('limit')
        numerical = kwargs.get('numerical')

        if numerical:
            TESTLIST = FLAGKEYLIST
        else:
            TESTLIST = KEYLIST

        keylist = []
        """
        for key in FLAGKEYLIST[1:]:
            try:
                header = self.header['col-'+key]
                try:
                    unit = self.header['unit-col-'+key]
                except:
                    unit = None
                keylist.append(key)
            except:
                header = None
        """

        if not len(keylist) > 0:  # e.g. Testing ndarray
            for ind,elem in enumerate(self.ndarray): # use the long way
                if len(elem) > 0 and ind < len(TESTLIST):
                    if not TESTLIST[ind] == 'time':
                        keylist.append(TESTLIST[ind])

        if not len(keylist) > 0:  # e.g. header col-? does not contain any info
            #for key in FLAGKEYLIST[1:]: # use the long way
            for key in TESTLIST[1:]: # use the long way
                col = self._get_column(key)
                if len(col) > 0:
                    #if not len(col) == 1 and not ( # maybe add something to prevent reading empty LineStructs)
                    if len(col) == 1:
                        if col[0] in ['-',float(nan),'']:
                            pass
                    else:
                        keylist.append(key)

        if limit and len(keylist) > limit:
            keylist = keylist[:limit]

        return keylist


    def _get_key_names(self):
        """
        DESCRIPTION:
            get the variable names for each key
        APPLICATION:
            keydict = self._get_key_names()
        """
        keydict = {}
        for key in KEYLIST:
            kname = self.header.get('col-'+key)
            keydict[kname] = key
        return keydict


    def dropempty(self):
        """
        DESCRIPTION:
            Drop empty arrays from ndarray and store their positions
        """
        if not len(self.ndarray[0]) > 0:
            return self.ndarray, np.asarray([])
        newndarray = []
        indexarray = []
        for ind,elem in enumerate(self.ndarray):
            if len(elem) > 0:
               newndarray.append(np.asarray(elem).astype(object))
               indexarray.append(ind)
        keylist = [el for ind,el in enumerate(KEYLIST) if ind in indexarray]
        return np.asarray(newndarray), keylist

    def fillempty(self, ndarray, keylist):
        """
        DESCRIPTION:
            Fills empty arrays into ndarray at all position of KEYLIST not provided in keylist
        """
        if not len(ndarray[0]) > 0:
            return self

        if len(self.ndarray) == KEYLIST:
            return self

        lst = list(ndarray)
        for i,key in enumerate(KEYLIST):
            if not key in keylist:
                lst.insert(i,[])

        newndarray = np.asarray(lst)
        return newndarray

    def sorting(self):
        """
        Sorting data according to time (maybe generalize that to some key)
        """
        try: # old LineStruct part
            liste = sorted(self.container, key=lambda tmp: tmp.time)
        except:
            pass

        if len(self.ndarray[0]) > 0:
            self.ndarray, keylst = self.dropempty()
            #self.ndarray = self.ndarray[:, np.argsort(self.ndarray[0])] # does not work if some rows have a different length)
            ind =  np.argsort(self.ndarray[0])
            for i,el in enumerate(self.ndarray):
                if len(el) == len(ind):
                    self.ndarray[i] = el[ind]
                else:
                    #print("Sorting: key %s has the wrong length - replacing row with NaNs" % KEYLIST[i])
                    logger.warning("Sorting: key %s has the wrong length - replacing row with NaNs" % KEYLIST[i])
                    logger.warning("len(t-axis)=%d len(%s)=%d" % (len(self.ndarray[0]), KEYLIST[i], len(self.ndarray[i])))
                    self.ndarray[i] = np.empty(len(self.ndarray[0])) * np.nan

            self.ndarray = self.fillempty(self.ndarray,keylst)
            for idx,el in enumerate(self.ndarray):
                self.ndarray[idx] = np.asarray(self.ndarray[idx]).astype(object)
        else:
            self.ndarray = self.ndarray

        return DataStream(liste, self.header, self.ndarray)

    # ------------------------------------------------------------------------
    # B. Internal Methods: Line & column functions
    # ------------------------------------------------------------------------

    def _get_line(self, key, value):
        """
        returns a LineStruct elemt corresponding to the first occurence of value within the selected key
        e.g.
        st = st._get_line('time',734555.3442) will return the line with time 7...
        """
        if not key in KEYLIST:
            raise ValueError("Column key not valid")

        lines = [elem for elem in self if eval('elem.'+key) == value]

        return lines[0]

    def _take_columns(self, keys):
        """
        DEFINITION:
            extract selected columns of the given keys (Old LineStruct format - decrapted)
        """

        resultstream = DataStream()

        for elem in self:
            line = LineStruct()
            line.time = elem.time
            resultstream.add(line)
        resultstream.header = {}

        for key in keys:
            if not key in KEYLIST:
                pass
            elif not key == 'time':
                col = self._get_column(key)
                #print key, len(col)
                try:
                    resultstream.header['col-'+key] = self.header['col-'+key]
                except:
                    pass
                try:
                    resultstream.header['unit-col-'+key] = self.header['unit-col-'+key]
                except:
                    pass
                resultstream = resultstream._put_column(col,key)

        return resultstream



    def _remove_lines(self, key, value):
        """
        removes lines with value within the selected key
        e.g.
        st = st._remove_lines('time',734555.3442) will return the line with time 7...
        """
        if not key in KEYLIST:
            raise ValueError("Column key not valid")

        lst = [elem for elem in self if not eval('elem.'+key) == value]

        return DataStream(lst, self.header)


    def _get_column(self, key):
        """
        Returns a numpy array of selected column from Stream
        Example:
        columnx = datastream._get_column('x')
        """

        if not key in KEYLIST:
            raise ValueError("Column key not valid")

        # Speeded up this technique:


        ind = KEYLIST.index(key)

        if len(self.ndarray[0]) > 0:
            try:
                col = self[key]
            except:
                col = self.ndarray[ind]
            return col

        # Check for initialization value
        #testval = self[0][ind]
        # if testval == KEYINITDICT[key] or isnan(testval):
        #    return np.asarray([])
        try:
            col = np.asarray([row[ind] for row in self])
            #get the first ten elements and test whether nan is there -- why ??
            """
            try: # in case of string....
                novalfound = True
                for ele in col[:10]:
                    if not isnan(ele):
                        novalfound = False
                if novalfound:
                    return np.asarray([])
            except:
                return col
            """
            return col
        except:
            return np.asarray([])


    def _put_column(self, column, key, **kwargs):
        """
    DEFINITION:
        adds a column to a Stream
    PARAMETERS:
        column:        (array) single list with data with equal length as stream
        key:           (key) key to which the data is written
    Kwargs:
        columnname:    (string) define a name
        columnunit:    (string) define a unit
    RETURNS:
        - DataStream object

    EXAMPLE:
        >>>  stream = stream._put_column(res, 't2', columnname='Rain',columnunit='mm in 1h')
        """
        #init = kwargs.get('init')
        #if init>0:
        #    for i in range init:
        #    self.add(float('NaN'))
        columnname = kwargs.get('columnname')
        columnunit = kwargs.get('columnunit')

        if not key in KEYLIST:
            raise ValueError("Column key not valid")
        if len(self.ndarray[0]) > 0:
            ind = KEYLIST.index(key)
            self.ndarray[ind] = np.asarray(column)
        else:
            if not len(column) == len(self):
                raise ValueError("Column length does not fit Datastream")
            for idx, elem in enumerate(self):
                setattr(elem, key, column[idx])

        if not columnname:
            try: # TODO correct that
                if eval('self.header["col-%s"]' % key) == '':
                    exec('self.header["col-%s"] = "%s"' % (key, key))
            except:
                pass
        else:
            exec('self.header["col-%s"] = "%s"' % (key, columnname))

        if not columnunit:
            try: # TODO correct that
                if eval('self.header["unit-col-%s"]' % key) == '':
                    exec('self.header["unit-col-%s"] = "arb"' % (key))
            except:
                pass
        else:
            exec('self.header["unit-col-%s"] = "%s"' % (key, columnunit))

        return self

    def _move_column(self, key, put2key):
        '''
    DEFINITION:
        Move column of key "key" to key "put2key".
        Simples.

    PARAMETERS:
    Variables:
        - key:          (str) Key to be moved.
        - put2key:      (str) Key for 'key' to be moved to.

    RETURNS:
        - stream:       (DataStream) DataStream object.

    EXAMPLE:
        >>> data_stream._move_column('f', 'var1')
        '''

        if not key in KEYLIST:
            logger.error("_move_column: Column key %s not valid!" % key)
        if key == 'time':
            logger.error("_move_column: Cannot move time column!")
        if not put2key in KEYLIST:
            logger.error("_move_column: Column key %s (to move %s to) is not valid!" % (put2key,key))
        if len(self.ndarray[0]) > 0:
            col = self._get_column(key)
            self =self._put_column(col,put2key)
            return self

        try:
            for i, elem in enumerate(self):
                exec('elem.'+put2key+' = '+'elem.'+key)
                if key in NUMKEYLIST:
                    setattr(elem, key, float("NaN"))
                    #exec('elem.'+key+' = float("NaN")')
                else:
                    setattr(elem, key, "-")
                    #exec('elem.'+key+' = "-"')
            try:
                exec('self.header["col-%s"] = self.header["col-%s"]' % (put2key, key))
                exec('self.header["unit-col-%s"] = self.header["unit-col-%s"]' % (put2key, key))
                exec('self.header["col-%s"] = None' % (key))
                exec('self.header["unit-col-%s"] = None' % (key))
            except:
                logger.error("_move_column: Error updating headers.")
            logger.info("_move_column: Column %s moved to column %s." % (key, put2key))
        except:
            logger.error("_move_column: It's an error.")

        return self


    def _drop_column(self,key):
        """
        remove a column of a Stream
        """
        ind = KEYLIST.index(key)

        if len(self.ndarray[0]) > 0:

            try:
                self.ndarray[ind] = np.asarray([])
            except:
                # Some array don't allow that, shape error e.g. PYSTRING -> then use this
                array = [np.asarray(el) if idx is not ind else np.asarray([]) for idx,el in enumerate(self.ndarray)]
                self.ndarray = np.asarray(array)


            colkey = "col-%s" % key
            colunitkey = "unit-col-%s" % key
            try:
                self.header.pop(colkey, None)
                self.header.pop(colunitkey, None)
            except:
                print("_drop_column: Error while dropping header info")
        else:
           print("No data available  or LineStruct type (not supported)")

        return self

    def _clear_column(self, key):
        """
        remove a column to a Stream
        """
        #init = kwargs.get('init')
        #if init>0:
        #    for i in range init:
        #    self.add(float('NaN'))

        if not key in KEYLIST:
            raise ValueError("Column key not valid")
        for idx, elem in enumerate(self):
            if key in NUMKEYLIST:
                setattr(elem, key, float("NaN"))
                #exec('elem.'+key+' = float("NaN")')
            else:
                setattr(elem, key, "-")
                #exec('elem.'+key+' = "-"')

        return self

    def _reduce_stream(self, pointlimit=100000):
        """
    DEFINITION:
        Reduces size of stream by picking for plotting methods to save memory
        when plotting large data sets.
        Does NOT filter or smooth!
        This function purely removes data points (rows) in a
        periodic fashion until size is <100000 data points.
        (Point limit can also be defined.)

    PARAMETERS:
    Kwargs:
        - pointlimit:   (int) Max number of points to include in stream. Default is 100000.

    RETURNS:
        - DataStream:   (DataStream) New stream reduced to below pointlimit.

    EXAMPLE:
        >>> lessdata = ten_Hz_data._reduce_stream(pointlimit=500000)

        """

        size = len(self)
        div = size/pointlimit
        divisor = math.ceil(div)
        count = 0.
        lst = []

        if divisor > 1.:
            for elem in self:
                if count%divisor == 0.:
                    lst.append(elem)
                count += 1.
        else:
            logger.warning("_reduce_stream: Stream size (%s) is already below pointlimit (%s)." % (size,pointlimit))
            return self

        logger.info("_reduce_stream: Stream size reduced from %s to %s points." % (size,len(lst)))

        return DataStream(lst, self.header)


    def _remove_nancolumns(self):
        """
    DEFINITION:
        Remove any columsn soley filled with nan values

    APPLICATION:
        called by plot methods in mpplot


    RETURNS:
        - DataStream:   (DataStream) New stream reduced to below pointlimit.

        """
        array = [[] for key in KEYLIST]
        if len(self.ndarray[0]) > 0:
            for idx, elem in enumerate(self.ndarray):
                if len(self.ndarray[idx]) > 0 and KEYLIST[idx] in NUMKEYLIST:
                    lst = list(self.ndarray[idx])
                    #print KEYLIST[idx],lst[0]
                    if lst[1:] == lst[:-1] and np.isnan(float(lst[0])):
                        array[idx] = np.asarray([])
                    else:
                        array[idx] = self.ndarray[idx]
                else:
                    array[idx] = self.ndarray[idx]
        else:
            pass
        return DataStream(self,self.header,np.asarray(array))


    # ------------------------------------------------------------------------
    # B. Internal Methods: Data manipulation functions
    # ------------------------------------------------------------------------

    def _aic(self, signal, k, debugmode=None):
        try:
            aicval = (k-1)* np.log(np.var(signal[:k]))+(len(signal)-k-1)*np.log(np.var(signal[k:]))
        except:
            if debugmode:
                logger.debug('_AIC: could not evaluate AIC at index position %i' % (k))
            pass
        return aicval


    def harmfit(self,nt, val, fitdegree):
        # method for harminic fit according to Phil McFadden's fortran program
        """
    DEFINITION:
        Method for harmonic fit according to Phil McFadden's fortran program
        Used by k-value determination

    PARAMETERS:
    Kwargs:
        - nt:           (list) Normalized time array.
        - val:          (list) Value list.
        - fitdegree:    (int) hramonic degree default is 5.

    RETURNS:
        - newval:       (array) an array with fitted values of length(val).

    EXAMPLE:
        >>> f_fit = self.harmfit(nt,val, 5)

        """
        N = len(nt)
        coeff = (val[-1]-val[0]) /(nt[-1]-nt[0])
        newval = [elem-coeff*(nt[i]-nt[0]) for i, elem in enumerate(val)]
        ReVal = []
        ImVal = []

        for h in range(0,fitdegree):
            ReVal.append(newval[0])
            ImVal.append(0.0)
            angle = -h*(2.0*np.pi/N)
            for i in range(1,len(newval)):
                si = np.sin(i*angle)
                co = np.cos(i*angle)
                ReVal[h] = ReVal[h] + newval[i]*co
                ImVal[h] = ImVal[h] + newval[i]*si

        #print "Parameter:", len(newval)
        #print len(ReVal), ReVal
        angle = 2.0*np.pi*(float(N-1)/float(N))/(nt[-1]-nt[0])
        harmval = []
        for i,elem in enumerate(newval):
            harmval.append(ReVal[0])
            angle2 = (nt[i]-nt[0])*angle
            for h in range(1,fitdegree):
                si = np.sin(h*angle2)
                co = np.cos(h*angle2)
                harmval[i] = harmval[i]+(2.0*(ReVal[h]*co-ImVal[h]*si))
            harmval[i] = harmval[i]/float(N)+coeff*(nt[i]-nt[0])

        return np.asarray(harmval)

    def _get_max(self, key, returntime=False):
        if not key in KEYLIST[:16]:
            raise ValueError("Column key not valid")
        key_ind = KEYLIST.index(key)
        t_ind = KEYLIST.index('time')

        if len(self.ndarray[0]) > 0:
            result = np.nanmax(self.ndarray[key_ind].astype(float))
            ind = np.nanargmax(self.ndarray[key_ind].astype(float))
            tresult = self.ndarray[t_ind][ind]
        else:
            elem = max(self, key=lambda tmp: eval('tmp.'+key))
            result = eval('elem.'+key)
            tresult = elem.time

        if returntime:
            return result, tresult
        else:
            return result


    def _get_min(self, key, returntime=False):
        if not key in KEYLIST[:16]:
            raise ValueError("Column key not valid")
        key_ind = KEYLIST.index(key)
        t_ind = KEYLIST.index('time')

        if len(self.ndarray[0]) > 0:
            result = np.nanmin(self.ndarray[key_ind].astype(float))
            ind = np.nanargmin(self.ndarray[key_ind].astype(float))
            tresult = self.ndarray[t_ind][ind]
        else:
            elem = min(self, key=lambda tmp: eval('tmp.'+key))
            result = eval('elem.'+key)
            tresult = elem.time

        if returntime:
            return result, tresult
        else:
            return result

    def amplitude(self,key):
        """
        DESCRIPTION:
             calculates maximum-minimum difference of the keys timeseries
        REQUIRES:
             _get_column()
        RETURNS:
             float: difference between maximum and minimim value in time range
        APPLICATION
             amp = stream.amplitude('x')
        """
        ts = self._get_column(key).astype(float)
        ts = ts[~np.isnan(ts)]
        maxts = np.max(ts)
        mints = np.min(ts)
        return maxts-mints

    def _gf(self, t, tau):
        """
        Gauss function
        """
        return np.exp(-((t/tau)*(t/tau))/2)


    def _hf(self, p, x):
        """
        Harmonic function
        """
        hf = p[0]*cos(2*pi/p[1]*x+p[2]) + p[3]*x + p[4] # Target function
        return hf


    def _residual_func(self, func, y):
        """
        residual of the harmonic function
        """
        return y - func


    def _tau(self, period, fac=0.83255461):
        """
        low pass filter with -3db point at period in sec (e.g. 120 sec)
        1. convert period from seconds to days as used in daytime
        2. return tau (in unit "day")
        - The value of 0.83255461 is obtained for -3db (see IAGA Guide)
        """
        per = period/(3600*24)
        return fac*per/(2*np.pi)


    # ------------------------------------------------------------------------
    # B. Internal Methods: General utility & NaN handlers
    # ------------------------------------------------------------------------

    def _convertstream(self, coordinate, **kwargs):
        """
      DESCRIPTION:
        Convert coordinates of x,y,z columns in other
        coordinate system:
        - xyz2hdz
        - xyz2idf
        - hdz2xyz
        - idf2xyz
        Helper method which call the tranformation routines
      APPLICATION:
        used by k_fmi, variocorrection

        """
        ext = ''
        if len(self.ndarray[4]) > 0:
            ext = 'F'
        if len(self.ndarray[KEYLIST.index('df')]) > 0:
            ext = 'G'

        if len(self.ndarray[0]) > 0:
            if coordinate == 'xyz2hdz':
                self = self.xyz2hdz()
                self.header['DataComponents'] = 'HDZ'+ext
            elif coordinate == 'xyz2idf':
                self = self.xyz2idf()
                self.header['DataComponents'] = 'IDF'+ext
            elif coordinate == 'hdz2xyz':
                self = self.hdz2xyz()
                self.header['DataComponents'] = 'XYZ'+ext
            elif coordinate == 'idf2xyz':
                self = self.idf2xyz()
                self.header['DataComponents'] = 'XYZ'+ext
            elif coordinate == 'idf2hdz':
                self = self.idf2xyz()
                self = self.xyz2hdz()
                self.header['DataComponents'] = 'HDZ'+ext
            elif coordinate == 'hdz2idf':
                self = self.hdz2xyz()
                self = self.xyz2idf()
                self.header['DataComponents'] = 'IDF'+ext
            else:
                print("_convertstream: unkown coordinate transform")
            return self

        keep_header = kwargs.get('keep_header')
        outstream = DataStream()
        for elem in self:
            row=LineStruct()
            exec('row = elem.'+coordinate+'(unit="deg")')
            row.typ = ''.join((list(coordinate))[4:])+'f'
            outstream.add(row)

        if not keep_header:
            outstream.header['col-x'] = (list(coordinate))[4]
            outstream.header['col-y'] = (list(coordinate))[5]
            outstream.header['col-z'] = (list(coordinate))[6]
            if (list(coordinate))[4] in ['i','d']:
                outstream.header['unit-col-x'] = 'deg'
            else:
                outstream.header['unit-col-x'] = 'nT'
            if (list(coordinate))[5] in ['i','d']:
                outstream.header['unit-col-y'] = 'deg'
            else:
                outstream.header['unit-col-y'] = 'nT'
            if (list(coordinate))[6] in ['i','d']:
                outstream.header['unit-col-z'] = 'deg'
            else:
                outstream.header['unit-col-z'] = 'nT'

        return DataStream(outstream,outstream.header)

    def _delete(self,index):
        """
      DESCRIPTION:
        Helper method to delete all values at a specific index or range of indicies
        from the ndarray
      APPLICTAION:
        Used by k_fmi with individual indicies
        """
        for i,array in enumerate(self.ndarray):
            if isinstance( index, (int,long) ):
                if len(array) > index:
                    self.ndarray[i] = np.delete(self.ndarray[i],index)
            else:
                self.ndarray[i] = np.delete(self.ndarray[i],index)
        return self

    def _append(self,stream):
        """
      DESCRIPTION:
        Helper method to append values from another stream to
        a ndarray. Append only to columns already filled in self.
      APPLICTAION:
        Used by k_fmi
        """
        for i,array in enumerate(self):
            if len(array) > 0:
                self.ndarray[i] = np.append(self.ndarray[i],stream.ndarray[i])
        return self


    def _det_trange(self, period):
        """
        starting with coefficients above 1%
        is now returning a timedelta object
        """
        return np.sqrt(-np.log(0.01)*2)*self._tau(period)


    def _is_number(self, s):
        """
        Test whether s is a number
        """
        if s in ['',None]:
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False


    def _normalize(self, column):
        """
        normalizes the given column to range [0:1]
        """
        normcol = []
        column = column.astype(float)
        maxval = np.max(column)
        minval = np.min(column)
        for elem in column:
            normcol.append((elem-minval)/(maxval-minval))

        return normcol, minval, maxval


    def _testtime(self, time):
        """
        Check the date/time input and returns a datetime object if valid:

        ! Use UTC times !

        - accepted are the following inputs:
        1) absolute time: as provided by date2num
        2) strings: 2011-11-22 or 2011-11-22T11:11:00
        3) datetime objects by datetime.datetime e.g. (datetime(2011,11,22,11,11,00)

        """

        if isinstance(time, float) or isinstance(time, int):
            try:
                timeobj = num2date(time).replace(tzinfo=None)
            except:
                raise TypeError
        elif isinstance(time, str): # test for str only in Python 3 should be basestring for 2.x
            try:
                timeobj = datetime.strptime(time,"%Y-%m-%d")
            except:
                try:
                    timeobj = datetime.strptime(time,"%Y-%m-%dT%H:%M:%S")
                except:
                    try:
                        timeobj = datetime.strptime(time,"%Y-%m-%d %H:%M:%S.%f")
                    except:
                        try:
                            timeobj = datetime.strptime(time,"%Y-%m-%dT%H:%M:%S.%f")
                        except:
                            try:
                                timeobj = datetime.strptime(time,"%Y-%m-%d %H:%M:%S")
                            except:
                                try:
                                    # Not happy with that but necessary to deal
                                    # with old 1000000 micro second bug
                                    timearray = time.split('.')
                                    if timearray[1] == '1000000':
                                        timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")+timedelta(seconds=1)
                                    else:
                                        # This would be wrong but leads always to a TypeError
                                        timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")
                                except:
                                    try:
                                        timeobj = num2date(float(time)).replace(tzinfo=None)
                                    except:
                                        raise TypeError
        elif not isinstance(time, datetime):
            raise TypeError
        else:
            timeobj = time

        return timeobj


    def _drop_nans(self, key):
        """
    DEFINITION:
        Helper to drop all lines when NaNs or INFs are found within the selected key

    RETURNS:
        - DataStream:   (DataStream object) a new data stream object with out identified lines.

    EXAMPLE:
        >>> newstream = stream._drop_nans('x')

    APPLICATION:
        used for plotting and fitting of data

        """
        array = [np.asarray([]) for elem in KEYLIST]
        if len(self.ndarray[0]) > 0 and key in NUMKEYLIST:
            ind = KEYLIST.index(key)
            #indicieslst = [i for i,el in enumerate(self.ndarray[ind].astype(float)) if np.isnan(el) or np.isinf(el)]
            ar = np.asarray(self.ndarray[ind]).astype(float)
            indicieslst = []
            for i,el in enumerate(ar):
                if np.isnan(el) or np.isinf(el):
                    indicieslst.append(i)
            searchlist = ['time']
            searchlist.extend(NUMKEYLIST)
            for index,tkey in enumerate(searchlist):
                if len(self.ndarray[index])>0:   # Time column !!! -> index+1
                    array[index] = np.delete(self.ndarray[index], indicieslst)
                #elif len(self.ndarray[index+1])>0:
                #    array[index+1] = self.ndarray[index+1]

            newst = [LineStruct()]
        else:
            newst = [elem for elem in self if not isnan(eval('elem.'+key)) and not isinf(eval('elem.'+key))]

        return DataStream(newst,self.header,np.asarray(array))


    def _select_keys(self, keys):
        """
      DESCRIPTION
        Non-destructive method to select provided keys from Data stream.
      APPLICATION:
        streamxy = streamyxzf._select_keys(['x','y'])
        """
        result = self.copy()

        try:
            if not len(keys) > 0:
                return self
        except:
            return self

        """
        print ("sel", keys)
        if not 'time' in keys:
            keys.append('time')
        print ("sel", keys)
        """

        ndarray = [[] for key in KEYLIST]
        ndarray = np.asarray([np.asarray(elem) if KEYLIST[idx] in keys or KEYLIST[idx] == 'time' else np.asarray([]) for idx,elem in enumerate(result.ndarray)])

        return DataStream([LineStruct()],result.header,ndarray)


    def _select_timerange(self, starttime=None, endtime=None, maxidx=-1):
        """
      DESCRIPTION
        Non-destructive method to select a certain time range from a stream.
        Similar to trim, leaving the original stream unchanged however.
      APPLICATION:
        Used by write
        """
        ndarray = [[] for key in KEYLIST]

        # Use a different technique
        # copy all data to array and then delete everything below and above

        #t1 = datetime.utcnow()

        #ndarray = self.ndarray
        startindices = []
        endindices = []
        if starttime:
            starttime = self._testtime(starttime)
            if self.ndarray[0].size > 0:   # time column present
                if maxidx > 0:
                    idx = (np.abs(self.ndarray[0][:maxidx]-date2num(starttime))).argmin()
                else:
                    idx = (np.abs(self.ndarray[0]-date2num(starttime))).argmin()
                # Trim should start at point >= starttime, so check:
                if self.ndarray[0][idx] < date2num(starttime):
                    idx += 1
                startindices = list(range(0,idx))
        if endtime:
            endtime = self._testtime(endtime)
            if self.ndarray[0].size > 0:   # time column present
                #print "select timerange", maxidx
                if maxidx > 0: # truncate the ndarray
                    #print maxidx
                    #tr = self.ndarray[0][:maxidx].astype(float)
                    idx = 1 + (np.abs(self.ndarray[0][:maxidx].astype(float)-date2num(endtime))).argmin() # get the nearest index to endtime and add 1 (to get lenghts correctly)
                else:
                    idx = 1 + (np.abs(self.ndarray[0].astype(float)-date2num(endtime))).argmin() # get the nearest index to endtime and add 1 (to get lenghts correctly)
                if idx >= len(self.ndarray[0]): ## prevent too large idx values
                    idx = len(self.ndarray[0]) # - 1
                try: # using try so that this test is passed in case of idx == len(self.ndarray)
                    endnum = date2num(endtime)
                    #print ("Value now", idx, self.ndarray[0][idx], date2num(endtime))
                    if self.ndarray[0][idx] > endnum and self.ndarray[0][idx-1] < endnum:
                        # case 1: value at idx is larger, value at idx-1 is smaller -> use idx
                        pass
                    elif self.ndarray[0][idx] == endnum:
                        # case 2: value at idx is endnum -> use idx
                        pass
                    elif not self.ndarray[0][idx] <= endnum:
                        # case 3: value at idx-1 equals endnum -> use idx-1
                        idx -= 1
                    #print ("Value now b", idx, self.ndarray[0][idx], date2num(endtime))
                    #if not self.ndarray[0][idx] <= date2num(endtime):
                    #    # Make sure that last value is either identical to endtime (if existing or one index larger)
                    #    # This is important as from this index on, data is removed
                    #    idx -= 1
                    #    print ("Value now", idx, self.ndarray[0][idx], date2num(endtime))
                    #    print ("Value now", idx, self.ndarray[0][idx+1], date2num(endtime))
                except:
                    pass
                endindices = list(range(idx,len(self.ndarray[0])))

        indices = startindices + endindices

        #t2 = datetime.utcnow()
        #print "_select_timerange - getting t range needed:", t2-t1

        if len(startindices) > 0:
            st = startindices[-1]+1
        else:
            st = 0
        if len(endindices) > 0:
            ed = endindices[0]
        else:
            ed = len(self.ndarray[0])

        for i in range(len(self.ndarray)):
            ndarray[i] = self.ndarray[i][st:ed]   ## This is the correct length

        #t3 = datetime.utcnow()
        #print "_select_timerange - deleting :", t3-t2

        return np.asarray(ndarray)

    # ------------------------------------------------------------------------
    # C. Application methods
    #           (in alphabetical order)
    # ------------------------------------------------------------------------

    def aic_calc(self, key, **kwargs):
        """
    DEFINITION:
        Picking storm onsets using the Akaike Information Criterion (AIC) picker
        - extract one dimensional array from DataStream (e.g. H) -> signal
        - take the first k values of the signal and calculates variance and log
        - plus the rest of the signal (variance and log)
        NOTE: Best results come from evaluating two data series - one with original
        data, one of same data with AIC timerange offset by timerange/2 to cover
        any signals that may occur at the points between evaluations.

    PARAMETERS:
    Variables:
        - key:          (str) Key to check. Needs to be an element of KEYLIST.
    Kwargs:
        - timerange:    (timedelta object) defines the length of the time window
                        examined by the aic iteration. (default: timedelta(hours=1).)
        - aic2key:      (str) defines the key of the column where to save the aic values
                        (default = var2).
        - aicmin2key:   (str) defines the key of the column where to save the aic minimum val
                        (default: key = var1.)
        - aicminstack:  (bool) if true, aicmin values are added to previously present column values.

    RETURNS:
        - self:         (DataStream object) Stream with results in default var1 + var2 keys.

    EXAMPLE:
        >>> stream = stream.aic_calc('x',timerange=timedelta(hours=0.5))

    APPLICATION:
        from magpy.stream import read
        stream = read(datapath)
        stream = stream.aic_calc('x',timerange=timedelta(hours=0.5))
        stream = stream.differentiate(keys=['var2'],put2keys=['var3'])
        stream_filt = stream.extract('var1',200,'>')
        stream_new = stream_file.eventlogger('var3',[30,40,60],'>',addcomment=True)
        stream = mergeStreams(stream,stream_new,key='comment')
        """
        timerange = kwargs.get('timerange')
        aic2key = kwargs.get('aic2key')
        aicmin2key = kwargs.get('aicmin2key')
        aicminstack = kwargs.get('aicminstack')

        if not timerange:
            timerange = timedelta(hours=1)
        if not aic2key:
            aic2key = 'var2'
        if not aicmin2key:
            aicmin2key = 'var1'

        t = self._get_column('time')
        signal = self._get_column(key)
        #Clear the projected results column
        array = []
        aic2ind = KEYLIST.index(aic2key)
        self = self._clear_column(aic2key)
        if len(self.ndarray[0]) > 0.:
            self.ndarray[aic2ind] = np.empty((len(self.ndarray[0],)))
            self.ndarray[aic2ind][:] = np.NAN
        # get sampling interval for normalization - need seconds data to test that
        sp = self.get_sampling_period()*24*60
        # corrcet approach
        iprev = 0
        iend = 0

        while iend < len(t)-1:
            istart = iprev
            ta, iend = find_nearest(np.asarray(t), date2num(num2date(t[istart]).replace(tzinfo=None) + timerange))
            if iend == istart:
                 iend += 60 # approx for minute files and 1 hour timedelta (used when no data available in time range) should be valid for any other time range as well
            else:
                currsequence = signal[istart:iend]
                aicarray = []
                for idx, el in enumerate(currsequence):
                    if idx > 1 and idx < len(currsequence):
                        # CALCULATE AIC
                        aicval = self._aic(currsequence, idx)/timerange.seconds*3600 # *sp Normalize to sampling rate and timerange
                        if len(self.ndarray[0]) > 0:
                            self.ndarray[aic2ind][idx+istart] = aicval
                        else:
                            exec('self[idx+istart].'+ aic2key +' = aicval')
                        if not isnan(aicval):
                            aicarray.append(aicval)
                        # store start value - aic: is a measure for the significance of information change
                        #if idx == 2:
                        #    aicstart = aicval
                        #self[idx+istart].var5 = aicstart-aicval
                maxaic = np.max(aicarray)
                # determine the relative amplitude as well
                cnt = 0
                for idx, el in enumerate(currsequence):
                    if idx > 1 and idx < len(currsequence):
                        # TODO: this does not yet work with ndarrays
                        try:
                            if aicminstack:
                                if not eval('isnan(self[idx+istart].'+aicmin2key+')'):
                                    exec('self[idx+istart].'+ aicmin2key +' += (-aicarray[cnt] + maxaic)')
                                else:
                                    exec('self[idx+istart].'+ aicmin2key +' = (-aicarray[cnt] + maxaic)')
                            else:
                                exec('self[idx+istart].'+ aicmin2key +' = (-aicarray[cnt] + maxaic)')
                                exec('self[idx+istart].'+ aicmin2key +' = maxaic')
                            cnt = cnt+1
                        except:
                            msg = "number of counts does not fit usually because of nans"
            iprev = iend

        self.header['col-var2'] = 'aic'

        return self


    def baseline(self, absolutedata, **kwargs):
        """
      DESCRIPTION:
        calculates baseline correction for input stream (datastream)
        Uses available baseline values from the provided absolute file
        Special cases:
        1) Absolute data covers the full time range of the stream:
            -> Absolute data is extrapolated by duplicating the last and first entry at "extradays" offset
            -> desired function is calculated
        2) No Absolute data for the end of the stream:
            -> like 1: Absolute data is extrapolated by duplicating the last entry at "extradays" offset or end of stream
            -> and info message is created, if timedifference exceeds the "extraday" arg then a warning will be send
        2) No Absolute data for the beginning of the stream:
            -> like 2: Absolute data is extrapolated by duplicating the first entry at "extradays" offset or beginning o stream
            -> and info message is created, if timedifference exceeds the "extraday" arg then a warning will be send
      VARIABLES:
        required:
          didata          (DataStream) containing DI data- usually obtained by absolutes.absoluteAnalysis()

        keywords:
          plotbaseline    (bool/string) will plot a baselineplot (if a valid path is provided
                                        to file otherwise to to screen- requires mpplot
          extradays       (int) days to which the absolutedata is exteded prior and after start and endtime
          ##plotfilename    (string) if plotbaseline is selected, the outputplot is send to this file
          fitfunc         (string) see fit
          fitdegree       (int) see fit
          knotstep        (int) see fit
          keys            (list) keys which contain the basevalues (default) is ['dx','dy','dz']

      APPLICATION:
          func  = data.baseline(didata,knotstep=0.1,plotbaseline=True)
          # fixed time range
          func  = data.baseline(didata,startabs='2015-02-01',endabs='2015-08-24',extradays=0)
        OR:
          funclist = []
          funclist.append(rawdata.baseline(basevalues, extradays=0, fitfunc='poly',
                          fitdegree=1,startabs='2009-01-01',endabs='2009-03-22'))
          funclist.append(rawdata.baseline(basevalues, extradays=0, fitfunc='poly',
                          fitdegree=1,startabs='2009-03-22',endabs='2009-06-27'))
          funclist.append(rawdata.baseline(basevalues, extradays=0, fitfunc='spline',
                          knotstep=0.2,startabs='2009-06-27',endabs='2010-02-01'))

        stabilitytest (bool)
        """

        keys = kwargs.get('keys')
        fitfunc = kwargs.get('fitfunc')
        fitdegree = kwargs.get('fitdegree')
        knotstep = kwargs.get('knotstep')
        extradays = kwargs.get('extradays',15)
        plotbaseline = kwargs.get('plotbaseline')
        plotfilename = kwargs.get('plotfilename')
        startabs =  kwargs.get('startabs')
        endabs =  kwargs.get('endabs')

        orgstartabs = None
        orgendabs = None

        #if not extradays:
        #    extradays = 15
        if not fitfunc:
            fitfunc = self.header.get('DataAbsFunc')
            if not fitfunc:
                fitfunc = 'spline'
        if not fitdegree:
            fitdegree = self.header.get('DataAbsDegree')
            if not fitdegree:
                fitdegree = 5
        if not knotstep:
            knotstep = self.header.get('DataAbsKnots')
            if not knotstep:
                knotstep = 0.3
        if not keys:
            keys = ['dx','dy','dz']

        if len(self.ndarray[0]) > 0:
            ndtype = True
            starttime = np.min(self.ndarray[0])
            endtime = np.max(self.ndarray[0])
        else:
            starttime = self[0].time
            endtime = self[-1].time

        fixstart,fixend = False,False
        if startabs:
            startabs = date2num(self._testtime(startabs))
            orgstartabs = startabs
            fixstart = True
        if endabs:
            endabs = date2num(self._testtime(endabs))
            orgendabs = endabs
            fixend = True

        pierlong = absolutedata.header.get('DataAcquisitionLongitude','')
        pierlat = absolutedata.header.get('DataAcquisitionLatitude','')
        pierel = absolutedata.header.get('DataElevation','')
        pierlocref = absolutedata.header.get('DataAcquisitionReference','')
        pierelref = absolutedata.header.get('DataElevationRef','')
        #self.header['DataAbsFunc'] = fitfunc
        #self.header['DataAbsDegree'] = fitdegree
        #self.header['DataAbsKnots'] = knotstep
        #self.header['DataAbsDate'] = datetime.strftime(datetime.utcnow(),'%Y-%m-%d %H:%M:%S')

        usestepinbetween = False # for better extrapolation

        logger.info(' --- Start baseline-correction at %s' % str(datetime.now()))

        absolutestream  = absolutedata.copy()

        #print("Baseline", absolutestream.length())
        absolutestream = absolutestream.remove_flagged()
        #print("Baseline", absolutestream.length())
        #print("Baseline", absolutestream.ndarray[0])

        absndtype = False
        if len(absolutestream.ndarray[0]) > 0:
            #print ("HERE1: adopting time range absolutes - before {} {}".format(startabs, endabs))
            absolutestream.ndarray[0] = absolutestream.ndarray[0].astype(float)
            absndtype = True
            if not np.min(absolutestream.ndarray[0]) < endtime:
                logger.warning("Baseline: Last measurement prior to beginning of absolute measurements ")
            abst = absolutestream.ndarray[0]
            if not startabs or startabs < np.min(absolutestream.ndarray[0]):
                startabs = np.min(absolutestream.ndarray[0])
            if not endabs or endabs > np.max(absolutestream.ndarray[0]):
                endabs = np.max(absolutestream.ndarray[0])
        else:
            # 1) test whether absolutes are in the selected absolute data stream
            if absolutestream[0].time == 0 or absolutestream[0].time == float('nan'):
                raise ValueError ("Baseline: Input stream needs to contain absolute data ")
            # 2) check whether enddate is within abs time range or larger:
            if not absolutestream[0].time-1 < endtime:
                logger.warning("Baseline: Last measurement prior to beginning of absolute measurements ")
            abst = absolutestream._get_column('time')
            startabs = absolutestream[0].time
            endabs = absolutestream[-1].time

        # Initialze orgstartabd and orgendabs if not yet provided: orgabs values will be added to DataAbsInfo
        if not orgstartabs:
            orgstartabs = startabs
        if not orgendabs:
            orgendabs = endabs

        #print ("HERE2a: Time range absolutes  - {} {} {} {}".format(startabs, endabs, num2date(startabs), num2date(endabs)))
        #print ("HERE2b: Time range datastream - {} {}".format(starttime, endtime))

        # 3) check time ranges of stream and absolute values:
        if startabs > starttime:
            #print ('HERE2c: First absolute value measured after beginning of stream')
            #logger.warning('Baseline: First absolute value measured after beginning of stream - duplicating first abs value at beginning of time series')
            #if fixstart:
            #
            #absolutestream.add(absolutestream[0])
            #absolutestream[-1].time = starttime
            #absolutestream.sorting()
            logger.info('Baseline: %d days without absolutes at the beginning of the stream' % int(np.floor(np.min(abst)-starttime)))
        if endabs < endtime:
            logger.info("Baseline: Last absolute measurement before end of stream - extrapolating baseline")
            if num2date(endabs).replace(tzinfo=None) + timedelta(days=extradays) < num2date(endtime).replace(tzinfo=None):
                usestepinbetween = True
                if not fixend:
                    logger.warning("Baseline: Well... thats an adventurous extrapolation, but as you wish...")

        starttime = num2date(starttime).replace(tzinfo=None)
        endtime = num2date(endtime).replace(tzinfo=None)


        # 4) get standard time rang of one year and extradays at start and end
        #           test whether absstream covers this time range including extradays
        # ###########
        #  get boundaries
        # ###########
        extrapolate = False
        # upper
        if fixend:
            #absolutestream = absolutestream.trim(endtime=endabs)  # should I trim here already - leon ??
            # time range long enough
            baseendtime = endabs+extradays
            if baseendtime < orgendabs:
                baseendtime = orgendabs
            extrapolate = True
        else:
            baseendtime = date2num(endtime+timedelta(days=1))
            extrapolate = True
        #if endabs >= date2num(endtime)+extradays:
        #    # time range long enough
        #    baseendtime = date2num(endtime)+extradays
        # lower
        if fixstart:
            #absolutestream = absolutestream.trim(starttime=startabs)  # should I trim here already - leon ??
            basestarttime = startabs-extradays
            if basestarttime > orgstartabs:
                basestarttime = orgstartabs
            extrapolate = True
        else:
            # not long enough
            #basestarttime = date2num(starttime)
            basestarttime = startabs-extradays
            extrapolate = True
            if baseendtime - (366.+2*extradays) > startabs:
                # time range long enough
                basestarttime =  baseendtime-(366.+2*extradays)

        baseendtime = num2date(baseendtime).replace(tzinfo=None)
        basestarttime = num2date(basestarttime).replace(tzinfo=None)

        #print ("HERE3a: basestart and end", basestarttime, baseendtime)

        # Don't use trim here
        #bas = absolutestream.trim(starttime=basestarttime,endtime=baseendtime)
        basarray = absolutestream._select_timerange(starttime=basestarttime,endtime=baseendtime)
        bas = DataStream([LineStruct()],absolutestream.header,basarray)

        #print ("HERE3b: length of selected absolutes: ", bas.length()[0])

        if extrapolate: # and not extradays == 0:
            bas = bas.extrapolate(basestarttime,baseendtime)

        #keys = ['dx','dy','dz']
        try:
            logger.info("Fitting Baseline between: {a} and {b}".format(a=str(num2date(np.min(bas.ndarray[0]))),b=str(num2date(np.max(bas.ndarray[0])))))
            #print ("Baseline", bas.length(), keys)
            #for elem in bas.ndarray:
            #    print elem
            func = bas.fit(keys,fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep)
        except:
            print ("Baseline: Error when determining fit - Enough data point to satisfy fit complexity?")
            logger.error("Baseline: Error when determining fit - Not enough data point to satisfy fit complexity? N = {}".format(bas.length()))
            return None

        #if len(keys) == 3:
        #    ix = KEYLIST.index(keys[0])
        #    iy = KEYLIST.index(keys[1])
        #    iz = KEYLIST.index(keys[2])
        # get the function in some readable equation
        #self.header['DataAbsDataT'] = bas.ndarray[0],bas.ndarray[ix],bas.ndarray[iy],bas.ndarray[iz]]

        if plotbaseline:
            #check whether plotbaseline is valid path or bool
            try:
                try:
                    import magpy.mpplot as mp
                except ImportError:
                    print ("baseline: Could not load package mpplot")
                if plotfilename:
                    mp.plot(bas,variables=['dx','dy','dz'],padding = [5,0.005,5], symbollist = ['o','o','o'],function=func,plottitle='Absolute data',outfile=plotfilename)
                else:
                    mp.plot(bas,variables=['dx','dy','dz'],padding = [5,0.005,5], symbollist = ['o','o','o'],function=func,plottitle='Absolute data')
            except:
                print("using the internal plotting routine requires mpplot to be imported as mp")

        keystr = '_'.join(keys)
        pierlong = absolutedata.header.get('DataAcquisitionLongitude','')
        pierlat = absolutedata.header.get('DataAcquisitionLatitude','')
        pierel = absolutedata.header.get('DataElevation','')
        pierlocref = absolutedata.header.get('DataLocationReference','')
        pierelref = absolutedata.header.get('DataElevationRef','')
        if not pierlong == '' and not pierlat == '' and not pierel == '':
            absinfostring = '_'.join(map(str,[orgstartabs,orgendabs,extradays,fitfunc,fitdegree,knotstep,keystr,pierlong,pierlat,pierlocref,pierel,pierelref]))
        else:
            absinfostring = '_'.join(map(str,[orgstartabs,orgendabs,extradays,fitfunc,fitdegree,knotstep,keystr]))
        existingabsinfo = self.header.get('DataAbsInfo','').replace(', EPSG',' EPSG').split(',')
        if not existingabsinfo[0] == '':
            existingabsinfo.append(absinfostring)
        else:
            existingabsinfo = [absinfostring]

        # Get minimum and maximum times out of existing absinfostream
        minstarttime=100000000.0
        maxendtime=0.0
        for el in existingabsinfo:
            ele = el.split('_')
            mintime = float(ele[0])
            maxtime = float(ele[1])
            if minstarttime > mintime:
                minstarttime = mintime
            if maxendtime < maxtime:
                maxendtime = maxtime
        exabsstring = ','.join(existingabsinfo)
        self.header['DataAbsInfo'] = exabsstring  # 735582.0_735978.0_0_spline_5_0.3_dx_dy_dz

        #print ("HERE5a:", minstarttime, maxendtime, absolutestream.length()[0])
        bas2save = absolutestream.trim(starttime=minstarttime,endtime=maxendtime)
        tmpdict = bas2save.stream2dict()
        #print ("HERE5b:", bas2save.length()[0])
        self.header['DataBaseValues'] = tmpdict['DataBaseValues']
        #self.header['DataAbsMinTime'] = func[1] #num2date(func[1]).replace(tzinfo=None)
        #self.header['DataAbsMaxTime'] = func[2] #num2date(func[2]).replace(tzinfo=None)
        #self.header['DataAbsFunctionObject'] = func

        #else:
        #    self = self.func_add(func)

        logger.info(' --- Finished baseline-correction at %s' % str(datetime.now()))

        #for key in self.header:
        #    if key.startswith('DataAbs'):
        #        print(key, self.header[key])

        return func


    def stream2dict(self, keys=['dx','dy','dz'], dictkey='DataBaseValues'):
        """
        DESCRIPTION:
            Method to convert stream contents into a list and assign this to a dictionary.
            You can use this method to directly store magnetic basevalues along with
            data time series (e.g. using NasaCDF). Multilayer storage as supported by NetCDF
            might provide better options to combine both data sets in one file.
        PARAMETERS:
            stream		(DataStream) data containing e.g. basevalues
            keys		(list of keys) keys which are going to be stored
            dictkey		(string) name of the dictionaries key
        RETURNS:
            dict		(dictionary) with name dictkey
        APPLICATION:
            >>> d = absdata.stream2dict(['dx','dy','dz'],'DataBaseValues')
            >>> d = neicdata.stream2dict(['f','str3'],'Earthquakes')
        """

        if not self.length()[0] > 0:
            return {}

        if not len(keys) > 0:
            return {}

        d = {}
        keylst = ['time']
        keylst.extend(keys)

        array,headline,addline = [],[],[]
        for key in keylst:
            try:
                pos = KEYLIST.index(key)
            except ValueError:
                pos = -1
            if pos in range(0,len(KEYLIST)):
                headline.append(key)
                if not key == 'time':
                    addline.append(self.header.get('col-'+key))
                else:
                    addline.append(self.header.get('DataID'))
                column = self.ndarray[pos]
                array.append(column)

        rowlst = np.transpose(np.asarray(array)).astype(object)
        fulllst = np.insert(rowlst,0,np.asarray(addline).astype(object),axis=0) ##could be used to store column names and id in time column
        fulllst = np.insert(fulllst,0,np.asarray(headline).astype(object),axis=0)
        d[dictkey] = fulllst
        return d


    def dict2stream(self,dictkey='DataBaseValues'):
        """
        DESCRIPTION:
            Method to convert the list stored in stream.header['DataBaseValue']
            to an absolute stream.
        PARAMETERS:
            stream		(DataStream) stream with variation data
            dictkey		(string) ususally 'DataBaseValues'
        RETURNS:
            stream		(DataStream) containing values of header info
        APPLICATION:
            >>> absstream = stream.dict2stream(header['DataBaseValues'])
        """

        lst = self.header.get(dictkey)

        if not type(lst) in (list,tuple,np.ndarray):
            print("dict2stream: no list,tuple,array found in provided header key")
            return DataStream()

        if len(lst) == 0:
            print("dict2stream: list is empty")
            return DataStream()

        array = [[] for el in KEYLIST]

        headerinfo = lst[0]
        addinfo = lst[1]
        data = lst[2:]
        #print(headerinfo,addinfo)
        collst = np.transpose(np.asarray(data)).astype(object)
        #print(collst)

        for idx,key in enumerate(headerinfo):
            pos = KEYLIST.index(key)
            array[pos] = collst[idx]

        return DataStream([LineStruct()], {}, np.asarray(array))


    def baselineAdvanced(self, absdata, baselist, **kwargs):
        """
        DESCRIPTION:
            reads stream, didata and baseline list
            -> save separate monthly cdf's for each baseline input
            -> Filename contains date of baseline jump
        RETURNS:
            list of header and ndarray -> this is necessary for datastreams
        """
        sensid = kwargs.get('sensorid')
        plotbaseline = kwargs.get('plotbaseline')

        data = self.copy()
        # Get start and endtime of stream
        ts,te = data._find_t_limits()
        # Get start and endtime of di data
        tabss,tabse = absdata._find_t_limits()
        # Some checks
        if tabss > te or tabse < ts:
            print ("baselineAdvanced: No DI data for selected stream available -aborting")
            return False
        if tabss > ts:
            print ("baselineAdvanced: DI data does not cover the time range of stream - trimming stream")
            data = data.trim(starttime=tabss)
        if tabse < te:
            print ("baselineAdvanced: DI data does not cover the time range of stream - trimming stream")
            data = data.trim(endtime=tabse)
        # Getting relevant baseline info
        sensid = self.header.get('SensorID','')
        if sensid == '':
            print ("baselineAdvanced: No SensorID in header info - provide by option sensorid='XXX'")
            return False
        indlist = [ind for ind, elem in enumerate(baselist[0]) if elem == sensid]
        #print "writeBC", indlist
        senslist = [[el for idx,el in enumerate(elem) if idx in indlist] for elem in baselist]
        #print "writeBC", senslist
        #print "writeBC", senslist[1]
        if not len(senslist) > 0:
            print ("baselineAdvanced: Did not find any valid baseline parameters for selected sensor")
            return False
        # get index of starttime closest before
        beforeinds = [[ind,np.abs(date2num(ts)-elem)] for ind, elem in enumerate(senslist[1]) if elem < date2num(ts)]
        #print "writeBC", beforeinds
        minl = [el[1] for el in beforeinds]
        #print "writeBC minl", minl
        startind = beforeinds[minl.index(np.min(minl))][0]
        #print "writeBC", startind
        vallist = [[el for idx,el in enumerate(elem) if idx == startind] for elem in senslist]
        #print vallist
        validinds = [ind for ind, elem in enumerate(senslist[1]) if elem >= date2num(ts) and elem <= date2num(te)]
        #print "writeBC inds", validinds
        vallist2 = [[el for idx,el in enumerate(elem) if idx in validinds] for elem in senslist]
        #print vallist2
        if len(vallist2[0]) > 0:
            resultlist = []
            for idx, elem in enumerate(vallist):
                addelem = vallist2[idx]
                print(elem, addelem)
                elem.extend(addelem)
                resultlist.append(elem)
        else:
            resultlist = vallist

        print("baselineAdvanced: inds", resultlist)

        # Select appropriate time ranges from stream
        if not len(resultlist[0]) > 0:
            print ("baselineAdvanced: Did not find any valid baseline parameters for selected sensor")
            return False
        streamlist = []
        dictlist = []
        resultlist = np.asarray(resultlist)
        vals = resultlist.transpose()
        for idx, elem in enumerate(vals):
            #print "writeBC running", elem
            mintime = float(elem[1])
            maxtime = float(elem[2])
            array = data._select_timerange(starttime=mintime, endtime=maxtime)
            stream = DataStream(data,data.header,array)
            baselinefunc = stream.baseline(absdata,startabs=mintime,endabs=maxtime, fitfunc=elem[3],fitdegree=int(elem[4]),knotstep=float(elem[5]),plotbaseline=plotbaseline)
            #stream = stream.bc()
            #exec('stream'+str(idx)+'= DataStream(stream,stream.header,stream.ndarray)')
            dicthead = stream.header
            #dictlist.append(dicthead.copy()) # Note: append just adds a pointer to content - use copy
            #streamlist.append([dicthead.copy(),stream.ndarray])
            streamlist.append([DataStream([LineStruct()],dicthead.copy(),stream.ndarray),baselinefunc])

        #print "Streamlist", streamlist
        #print len(dicthead),dictlist
        return streamlist


    def bc(self, function=None, ctype=None, alpha=0.0,level='preliminary'):
        """
        DEFINITION:
            Method to obtain baseline corrected data. By default flagged data is removed
            before baseline correction.
            Requires DataAbs values in the datastreams header.
            The function object is transferred to keys x,y,z, please note that the baseline function
            is stored in HDZ format (H:nT, D:0.0000 deg, Z: nT).
            By default the bc method requires HDZ oriented variometer data. If XYZ data is provided,
            or any other orientation, please provided rotation angles to transform this data into HDZ.
            Example:  For XYZ data please add the option alpha=DeclinationAtYourSite in a
                      float format of 0.00000 deg
        PARAMETERS:
            function      (function object) provide the function directly - not from header
            ctype         (string) one of 'fff', 'fdf', 'ddf' - denoting nT components 'f' and degree 'd'
            alpha/beta    (floats) provide rotation angles for the variometer data to be applied
                                   before correction - data is rotated back after correction
        """
        logger.debug("BC: Performing baseline correction: Requires HEZ data.")
        logger.debug("    H magnetic North, E magnetic East, Z vertical downwards, all in nT.")

        pierdata = False
        absinfostring = self.header.get('DataAbsInfo')
        absvalues = self.header.get('DataBaseValues')
        func = self.header.get('DataAbsFunctionObject')
        datatype = self.header.get('DataType')

        if datatype == 'BC':
            print ("BC: dataset is already baseline corrected - returning")
            return self

        bcdata = self.copy()

        logger.debug("BC: Components of stream: {}".format(self.header.get('DataComponents')))
        logger.debug("BC: baseline adoption information: {}".format(absinfostring))

        if absinfostring and type(absvalues) in [list,np.ndarray,tuple]:
            #print("BC: Found baseline adoption information in meta data - correcting")
            absinfostring = absinfostring.replace(', EPSG',' EPSG')
            absinfostring = absinfostring.replace(',EPSG',' EPSG')
            absinfostring = absinfostring.replace(', epsg',' EPSG')
            absinfostring = absinfostring.replace(',epsg',' EPSG')
            absinfolist = absinfostring.split(',')
            funclist = []
            for absinfo in absinfolist:
                #print("BC: TODO repeat correction several times and check header info")
                # extract baseline data
                absstream = bcdata.dict2stream()
                #print("BC: abstream length", absstream.length()[0])
                parameter = absinfo.split('_')
                #print("BC:", parameter, len(parameter))
                funckeys = parameter[6:9]
                if len(parameter) >= 14:
                    #extract pier information
                    pierdata = True
                    pierlon = float(parameter[9])
                    pierlat = float(parameter[10])
                    pierlocref = parameter[11]
                    pierel = float(parameter[12])
                    pierelref =  parameter[13]
                #print("BC", num2date(float(parameter[0])))
                #print("BC", num2date(float(parameter[1])))
                if not funckeys == ['df']:
                    func = bcdata.baseline(absstream, startabs=float(parameter[0]), endabs=float(parameter[1]), extradays=int(parameter[2]), fitfunc=parameter[3], fitdegree=int(parameter[4]), knotstep=float(parameter[5]), keys=funckeys)
                    if 'dx' in funckeys:
                        func[0]['fx'] = func[0]['fdx']
                        func[0]['fy'] = func[0]['fdy']
                        func[0]['fz'] = func[0]['fdz']
                        func[0].pop('fdx', None)
                        func[0].pop('fdy', None)
                        func[0].pop('fdz', None)
                        keys = ['x','y','z']
                    elif 'x' in funckeys:
                        keys = ['x','y','z']
                    else:
                        print("BC: could not interpret BaseLineFunctionObject - returning")
                        return self
                    funclist.append(func)

            #print ("BC: Found a list of functions:", funclist)
            bcdata = bcdata.func2stream(funclist,mode='addbaseline',keys=keys)
            bcdata.header['col-x'] = 'H'
            bcdata.header['unit-col-x'] = 'nT'
            bcdata.header['col-y'] = 'D'
            bcdata.header['unit-col-y'] = 'deg'
            datacomp = bcdata.header.get('DataComponents','')
            if len(datacomp) == 4:
                bcdata.header['DataComponents'] = 'HDZ'+datacomp[3]
            else:
                bcdata.header['DataComponents'] = 'HDZ'

            # Add BC mark to datatype - data is baseline corrected
            bcdata.header['DataType'] = 'BC'

            # Update location data from absinfo
            if pierdata:
                self.header['DataAcquisitionLongitude'] = pierlon
                self.header['DataAcquisitionLatitude'] = pierlat
                self.header['DataLocationReference'] = pierlocref
                self.header['DataElevation'] = pierel
                self.header['DataElevationRef'] = pierelref

            return bcdata

        elif func:
            # 1.) move content of basevalue function to columns 'x','y','z'?
            try:
                func[0]['fx'] = func[0]['fdx']
                func[0]['fy'] = func[0]['fdy']
                func[0]['fz'] = func[0]['fdz']
                func[0].pop('fdx', None)
                func[0].pop('fdy', None)
                func[0].pop('fdz', None)
                keys = ['x','y','z']
            except:
                print("BC: could not interpret BaseLineFunctionObject - returning")
                return self

            # 2.) eventually transform self - check header['DataComponents']
            if ctype == 'fff':
                pass
            elif ctype == 'ddf':
                pass
            else:
                pass
            #eventually use other information like absolute path, and function parameter
            #for key in self.header:
            #    if key.startswith('DataAbs'):
            #        print key, self.header[key]

            # drop all lines with nan values in either x or y and if x=0 add some 0.00001 because of arctan(y/x)
            #print len(self.ndarray[0])
            #for elem in self.ndarray[1]:
            #    if np.isnan(elem) or elem == 0.0:
            #        print "Found", elem
            #self = self._drop_nans('x')
            #self = self._drop_nans('y')
            #print len(self.ndarray[0])

            bcdata = bcdata.func2stream(func,mode='addbaseline',keys=['x','y','z'])
            bcdata.header['col-x'] = 'H'
            bcdata.header['unit-col-x'] = 'nT'
            bcdata.header['col-y'] = 'D'
            bcdata.header['unit-col-y'] = 'deg'
            bcdata.header['DataComponents'] = 'HDZ'
            return bcdata

        else:
            print("BC: No data for correction available - header needs to contain DataAbsFunctionObject")
            return self


    def bindetector(self,key,flagnum=1,keystoflag=['x'],sensorid=None,text=None,**kwargs):
        """
        DEFINITION:
            Function to detect changes between 0 and 1 and create a flaglist for zero or one states
        PARAMETERS:
            key:           (key) key to investigate
            flagnum:        (int) integer between 0 and 4, default is 0
            keystoflag:	   (list) list of keys to be flagged
            sensorid:	   (string) sensorid for flaglist, default is sensorid of self
            text:          (string) text to be added to comments/stdout,
                                    will be extended by on/off
        Kwargs:
            markallon:     (BOOL) add comment to all ons
            markalloff:    (BOOL) add comment to all offs
            onvalue:       (float) critical value to determin on stage (default = 0.99)
        RETURNS:
            - flaglist

        EXAMPLE:
            >>>  flaglist = stream.bindetector('z',0,'x',SensorID,'Maintanence switch for rain bucket',markallon=True)
        """
        markallon = kwargs.get('markallon')
        markalloff = kwargs.get('markalloff')
        onvalue = kwargs.get('onvalue')

        if not markallon and not markalloff:
            markallon = True
        if not onvalue:
            onvalue = 0.99
        if not sensorid:
            sensorid = self.header.get('SensorID')

        if not len(self.ndarray[0]) > 0:
            print ("bindetector: No ndarray data found - aborting")
            return self

        moddate = datetime.utcnow()
        ind = KEYLIST.index(key)
        startstate = self.ndarray[ind][0]
        flaglist=[]
        # Find switching states (Joe Kington: http://stackoverflow.com/questions/4494404/find-large-number-of-consecutive-values-fulfilling-condition-in-a-numpy-array)
        d = np.diff(self.ndarray[ind])
        idx, = d.nonzero()
        idx += 1

        if markallon:
            if not text:
                text = 'on'
            if self.ndarray[ind][0]:
                # If the start of condition is True prepend a 0
                idx = np.r_[0, idx]
            if self.ndarray[ind][-1]:
                # If the end of condition is True, append the length of the array
                idx = np.r_[idx, self.ndarray[ind].size] # Edit
            # Reshape the result into two columns
            #print("Bindetector", idx, idx.size)
            idx.shape = (-1,2)
            for start,stop in idx:
                stop = stop-1
                for elem in keystoflag:
                    flagline = [num2date(self.ndarray[0][start]).replace(tzinfo=None),num2date(self.ndarray[0][stop]).replace(tzinfo=None),elem,int(flagnum),text,sensorid,moddate]
                    flaglist.append(flagline)
        if markalloff:
            if not text:
                text = 'off'
            if not self.ndarray[ind][0]:
                # If the start of condition is True prepend a 0
                idx = np.r_[0, idx]
            if not self.ndarray[ind][-1]:
                # If the end of condition is True, append the length of the array
                idx = np.r_[idx, self.ndarray[ind].size] # Edit
            # Reshape the result into two columns
            idx.shape = (-1,2)
            for start,stop in idx:
                stop = stop-1
                for elem in keystoflag:
                    flagline = [num2date(self.ndarray[0][start]).replace(tzinfo=None),num2date(self.ndarray[0][stop]).replace(tzinfo=None),elem,int(flagid),text,sensorid,moddate]
                    flaglist.append(flagline)

        return flaglist

    def calc_f(self, **kwargs):
        """
        DEFINITION:
            Calculates the f form  x^2+y^2+z^2. If delta F is present, then by default
            this value is added as well
        PARAMETERS:
         Kwargs:
            - offset:     (array) containing three elements [xoffset,yoffset,zoffset],
            - skipdelta   (bool)  id selecetd then an existing delta f is not accounted for
        RETURNS:
            - DataStream with f and, if given, offset corrected xyz values

        EXAMPLES:
            >>> fstream = stream.calc_f()
            >>> fstream = stream.calc_f(offset=[20000,0,43000])
        """

        # Take care: if there is only 0.1 nT accuracy then there will be a similar noise in the deltaF signal

        offset = kwargs.get('offset')
        skipdelta = kwargs.get('skipdelta')

        if not offset:
            offset = [0,0,0]
        else:
            if not len(offset) == 3:
                logger.error('calc_f: offset with wrong dimension given - needs to contain a three dim array like [a,b,c] - returning stream without changes')
                return self

        ndtype = False
        try:
            if len(self.ndarray[0]) > 0:
                ndtype = True
            elif len(self) > 1:
                ndtype = False
            else:
                logger.error('calc_f: empty stream - aborting')
                return self
        except:
            logger.error('calc_f: inapropriate data provided - aborting')
            return self

        logger.info('calc_f: --- Calculating f started at %s ' % str(datetime.now()))

        if ndtype:
            inddf = KEYLIST.index('df')
            indf = KEYLIST.index('f')
            indx = KEYLIST.index('x')
            indy = KEYLIST.index('y')
            indz = KEYLIST.index('z')
            if len(self.ndarray[inddf]) > 0 and not skipdelta:
                df = self.ndarray[inddf].astype(float)
            else:
                df = np.asarray([0.0]*len(self.ndarray[indx]))
            x2 = ((self.ndarray[indx]+offset[0])**2).astype(float)
            y2 = ((self.ndarray[indy]+offset[1])**2).astype(float)
            z2 = ((self.ndarray[indz]+offset[2])**2).astype(float)
            self.ndarray[indf] = np.sqrt(x2+y2+z2) + df
        else:
            for elem in self:
                elem.f = np.sqrt((elem.x+offset[0])**2+(elem.y+offset[1])**2+(elem.z+offset[2])**2)

        self.header['col-f'] = 'f'
        self.header['unit-col-f'] = 'nT'

        logger.info('calc_f: --- Calculating f finished at %s ' % str(datetime.now()))

        return self


    def compensation(self, **kwargs):
        """
        DEFINITION:
            Method for magnetic variometer data:
            Applies eventually present compensation field values in the header
            to the vector x,y,z.
            Compensation fields are provided in mirco Tesla (according to LEMI data).
            Please note that any additional provided "DataDeltaValues" are also applied
            by default (to avoid use option skipdelta=True). 
            Calculation:
            
            This method uses header information data.header[''].
            After successfull application data.header['DeltaValuesApplied']
            is set to 1.

        PARAMETERS:
         Kwargs:
            - skipdelta   (bool)  if True then DataDeltaValues are ignored
        RETURNS:
            - DataStream with compensation values appliesd to xyz values
            - original dataStream if no compensation values are found

        EXAMPLES:
            >>> compstream = stream.compensation()
        """

        skipdelta = kwargs.get('skipdelta')

        if not self.length()[0] > 0:
            return self
 
        stream = self.copy()
        logger.info("compensation: applying compensation field values to variometer data ...")
        deltas = stream.header.get('DataDeltaValues','')
        if not skipdelta and not deltas=='':
            logger.info("compensation: applying delta values from header['DataDeltaValues'] first")
            stream = stream.offset(deltas)
            stream.header['DataDeltaValuesApplied'] = 1

        offdict = {}
        xcomp = stream.header.get('DataCompensationX','0')
        ycomp = stream.header.get('DataCompensationY','0')
        zcomp = stream.header.get('DataCompensationZ','0')
        if not float(xcomp)==0.:
            offdict['x'] = -1*float(xcomp)*1000.
        if not float(ycomp)==0.:
            offdict['y'] = -1*float(ycomp)*1000.
        if not float(zcomp)==0.:
            offdict['z'] = -1*float(zcomp)*1000.
        logger.info(' -- applying compensation fields: x={}, y={}, z={}'.format(xcomp,ycomp,zcomp))
        if len(offdict) > 0:
            stream = stream.offset(offdict)
            stream.header['DataDeltaValuesApplied'] = 1

        return stream


    def cut(self,length,kind=0,order=0):
        """
        DEFINITION:
            cut returns the selected amount of lines from datastreams
        PARAMETER:
            stream    :    datastream
            length    :    provide the amount of lines to be returned (default: percent of stream length)
            kind      :    define the kind of length parameter  
                           = 0 (default): length is given in percent
                           = 1: length is given in number of lines
            order     :    define from which side   
                           = 0 (default): the last amount of lines are returned
                           = 1: lines are counted from the beginning 
        VERSION:
            added in MagPy 0.4.6
        APPLICATION:
            # length of stream: 86400
            cutstream = stream.cut(50) 
            # length of cutstream: 43200
        """
        stream = self.copy()
        if length <= 0:
            print ("get_last: length needs to be > 0")
            return stream
        if kind == 0:
            if length > 100:
                length = 100
            amount = int(stream.length()[0]*length/100.)
        else:
            if length > stream.length()[0]:
                return stream
            else:
                amount = length
        for idx,el in enumerate(stream.ndarray):
            if len(el) >= amount:
                if order == 0:
                    nel = el[-amount:]
                else:
                    nel = el[:amount]
                stream.ndarray[idx] = nel
        return stream


    def dailymeans(self, keys=['x','y','z','f'], offset = 0.5, keepposition=False, **kwargs):
        """
    DEFINITION:
        Calculates daily means of xyz components and their standard deviations. By default
        numpy's mean and std methods are applied even if only two data sets are available.

        TODO ---
        If less then three data sets are provided, twice the difference between two values
        is used as an conservative proxy of uncertainty. I only on value is available, then
        the maximum uncertainty of the collection is assumed. This behavior can be changed
        by keyword arguments.
        TODO ---

        An outputstream is generated which containes basevalues in columns
        x,y,z and uncertainty values in dx,dy,dz
        if only a single values is available, dx,dy,dz contain the average uncertainties
        of the full data set
        time column contains the average time of the measurement

    PARAMETERS:
    Variables
    	- keys: 	(list) provide up to four keys which are used in columns x,y,z
        - offset:       (float) offset in timeunit days (0 to 0.999) default is 0.5, some test might use 0
    Kwargs:
        - none

    RETURNS:
        - stream:       (DataStream object) with daily means and standard deviation

    EXAMPLE:
        >>> means = didata.dailymeans(keys=['dx','dy','dz'])

    APPLICATION:
        >>> means = didata.dailymeans(keys=['dx','dy','dz'])
        >>> mp.plot(means,['x','y','z'],errorbars=True, symbollist=['o','o','o'])

        """

        percentage = 90
        keys = keys[:4]
        poslst,deltaposlst = [],[]
        deltakeys = ['dx','dy','dz','df']

        for key in keys:
            poslst.append(KEYLIST.index(key))
        for idx,pos in enumerate(poslst):
            deltaposlst.append(KEYLIST.index(deltakeys[idx]))

        if not len(self.ndarray[0]) > 0:
            return self

        array = [[] for el in KEYLIST]
        data = self.copy()
        data = data.removeduplicates()
        timecol = np.floor(data.ndarray[0])
        tmpdatelst = np.asarray(list(set(list(timecol))))
        for day in tmpdatelst:
            sel = data._select_timerange(starttime=day,endtime=day+1)
            """
        #for idx,day in enumerate(daylst):
            #sel = final._select_timerange(starttime=np.round(day), endtime=np.round(day)+1)
            """
            #print (len(sel))
            sttmp = DataStream([LineStruct()],{},sel)
            array[0].append(day+offset)
            for idx, pos in enumerate(poslst):
                #if len(sttmp.ndarray[idx+1]) > 0:
                if not keepposition:
                    array[idx+1].append(sttmp.mean(KEYLIST[pos],percentage=percentage))
                else:
                    array[pos].append(sttmp.mean(KEYLIST[pos],percentage=percentage))
                #print ("Check", KEYLIST[pos], idx+1, len(sttmp._get_column(KEYLIST[pos])),sttmp._get_column(KEYLIST[pos]),sttmp.mean(KEYLIST[pos],percentage=percentage))
                """
            #array[0].append(day+0.5)
            #for idx,pos in enumerate(poslst):
                array[idx+1].append(np.mean(sel[pos],percentage=percentage))
                """
                data.header['col-'+KEYLIST[idx+1]] = '{}'.format(self.header.get('col-'+KEYLIST[pos]))
                data.header['unit-col-'+KEYLIST[idx+1]] = '{}'.format(self.header.get('unit-col-'+KEYLIST[pos]))
                diff = pos-idx
            if not keepposition:
              for idx,dpos in enumerate(deltaposlst):
                #if len(sttmp.ndarray[idx]) > 0:
                me,std = sttmp.mean(KEYLIST[idx+diff],percentage=percentage, std=True)
                array[dpos].append(std)
                #array[dpos].append(np.std(sel[idx+diff]))
                data.header['col-'+KEYLIST[dpos]] = 'sigma {}'.format(self.header.get('col-'+KEYLIST[idx+diff]))
                data.header['unit-col-'+KEYLIST[dpos]] = '{}'.format(self.header.get('unit-col-'+KEYLIST[idx+diff]))
        data.header['DataFormat'] = 'MagPyDailyMean'

        array = [np.asarray(el) for el in array]
        retstream = DataStream([LineStruct()],data.header,np.asarray(array))
        retstream = retstream.sorting()
        return retstream


    def date_offset(self, offset):
        """
    IMPORTANT:
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        THIS METHOD IS NOT SUPPORTED ANY MORE. PLEASE USE
        self.offset({'time':timedelta(seconds=1000)}) INSTEAD
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    DEFINITION:
        Corrects the time column of the selected stream by the offst
        offset is a timedelta object (e.g. timedelta(hours=1))

    PARAMETERS:
    Variables:
        - offset:       (timedelta object) Offset to apply to stream.
    Kwargs:
        - None

    RETURNS:
        - stream:       (DataStream object) Stream with offset applied.

    EXAMPLE:
        >>> data = data.offset(timedelta(minutes=3))

    APPLICATION:
        """

        header = self.header
        newstream = DataStream()
        array = self.ndarray

        if len(ndarray[0]) > 0:
            ndtype = True
            secsperday = 3600*24
            ndarray[0] = ndarray[0] + offset.total_seconds/secsperday

        for elem in self:
            newtime = num2date(elem.time).replace(tzinfo=None) + offset
            elem.sectime = elem.time
            elem.time = date2num(newtime)
            newstream.add(elem)

        logger.info('date_offset: Corrected time column by %s sec' % str(offset.total_seconds))

        return DataStream(newstream,header,array)


    def delta_f(self, **kwargs):
        """
        DESCRIPTION:
            Calculates the difference of x+y+z to f and puts the result to the df column

        PARAMETER:
            keywords:
            :type offset: float
            :param offset: constant offset to f values
            :type digits: int
            :param digits: number of digits to be rounded (should equal the input precision)
        """

        # Take care: if there is only 0.1 nT accurracy then there will be a similar noise in the deltaF signal

        offset = kwargs.get('offset')
        digits = kwargs.get('digits')
        if not offset:
            offset = 0
        if not digits:
            digits = 8

        logger.info('--- Calculating delta f started at %s ' % str(datetime.now()))

        try:
            syst = self.header['DataComponents']
        except:
            syst = None


        ind = KEYLIST.index("df")
        indx = KEYLIST.index("x")
        indy = KEYLIST.index("y")
        indz = KEYLIST.index("z")
        indf = KEYLIST.index("f")
        if len(self.ndarray[0])>0 and len(self.ndarray[indx])>0 and len(self.ndarray[indy])>0 and len(self.ndarray[indz])>0 and len(self.ndarray[indf])>0:
            # requires x,y,z and f
            arx = self.ndarray[indx]**2
            ary = self.ndarray[indy]**2
            arz = self.ndarray[indz]**2
            if syst in ['HDZ','hdz','HDZF','hdzf','HDZS','hdzs','HDZG','hdzg']:
                print("deltaF: found HDZ orientation")
                ary = np.asarray([0]*len(self.ndarray[indy]))
            sumar = list(arx+ary+arz)
            sqr = np.sqrt(np.asarray(sumar))
            self.ndarray[ind] = sqr - (self.ndarray[indf] + offset)
        else:
            for elem in self:
                elem.df = round(np.sqrt(elem.x**2+elem.y**2+elem.z**2),digits) - (elem.f + offset)

        self.header['col-df'] = 'delta f'
        self.header['unit-col-df'] = 'nT'

        logger.info('--- Calculating delta f finished at %s ' % str(datetime.now()))

        return self


    def f_from_df(self, **kwargs):
        """
        DESCRIPTION:
            Calculates the f from the difference of x+y+z and df

        PARAMETER:
            keywords:
            :type offset: float
            :param offset: constant offset to f values
            :type digits: int
            :param digits: number of digits to be rounded (should equal the input precision)
        """

        # Take care: if there is only 0.1 nT accurracy then there will be a similar noise in the deltaF signal

        offset = kwargs.get('offset')
        digits = kwargs.get('digits')
        if not offset:
            offset = 0.
        if not digits:
            digits = 8

        logger.info('--- Calculating f started at %s ' % str(datetime.now()))

        try:
            syst = self.header['DataComponents']
        except:
            syst = None


        ind = KEYLIST.index("df")
        indx = KEYLIST.index("x")
        indy = KEYLIST.index("y")
        indz = KEYLIST.index("z")
        indf = KEYLIST.index("f")
        if len(self.ndarray[0])>0 and len(self.ndarray[indx])>0 and len(self.ndarray[indy])>0 and len(self.ndarray[indz])>0 and len(self.ndarray[ind])>0:
            # requires x,y,z and f
            arx = self.ndarray[indx]**2
            ary = self.ndarray[indy]**2
            arz = self.ndarray[indz]**2
            if syst in ['HDZ','hdz','HDZF','hdzf','HDZS','hdzs','HDZG','hdzg']:
                print("deltaF: found HDZ orientation")
                ary = np.asarray([0]*len(self.ndarray[indy]))
            sumar = list(arx+ary+arz)
            sqr = np.sqrt(np.asarray(sumar))
            self.ndarray[indf] = sqr - (self.ndarray[ind] + offset)
        else:
            for elem in self:
                elem.f = round(np.sqrt(elem.x**2+elem.y**2+elem.z**2),digits) - (elem.df + offset)

        self.header['col-f'] = 'f'
        self.header['unit-col-f'] = 'nT'

        logger.info('--- Calculating f finished at %s ' % str(datetime.now()))

        return self


    def differentiate(self, **kwargs):
        """
    DEFINITION:
        Method to differentiate all columns with respect to time.
        -- Using successive gradients

    PARAMETERS:
    Variables:
        keys: (list - default ['x','y','z','f'] provide limited key-list
        put2key
        - keys:         (list) Provide limited key-list. default = ['x','y','z','f']
        - put2keys:     (type) Provide keys to put differentiated keys to.
                        Default = ['dx','dy','dz','df']
    Kwargs:

    RETURNS:
        - stream:       (DataStream) Differentiated data stream, x values in dx, etc..

    EXAMPLE:
        >>> stream = stream.differentiate(keys=['f'],put2keys=['df'])

    APPLICATION:
        """

        logger.info('differentiate: Calculating derivative started.')

        keys = kwargs.get('keys')
        put2keys = kwargs.get('put2keys')
        if not keys:
            keys = ['x','y','z','f']
        if not put2keys:
            put2keys = ['dx','dy','dz','df']

        if len(keys) != len(put2keys):
            logger.error('Amount of columns read must be equal to outputcolumns')
            return self

        stream = self.copy()

        ndtype = False
        if len(stream.ndarray[0]) > 0:
            t = stream.ndarray[0].astype(float)
            ndtype = True
        else:
            t = stream._get_column('time')

        for i, key in enumerate(keys):
            if ndtype:
                ind = KEYLIST.index(key)
                val = stream.ndarray[ind].astype(float)
            else:
                val = stream._get_column(key)
            dval = np.gradient(np.asarray(val))
            stream._put_column(dval, put2keys[i])
            stream.header['col-'+put2keys[i]] = r"d%s vs dt" % (key)

        logger.info('--- derivative obtained at %s ' % str(datetime.now()))
        return stream


    def DWT_calc(self,key='x',wavelet='db4',level=3,plot=False,outfile=None,
                window=5):
        """
    DEFINITION:
        Discrete wavelet transform (DWT) method of analysing a magnetic signal
        to pick out SSCs. This method was taken from Hafez (2013): "Systematic examination
        of the geomagnetic storm sudden commencement using multi resolution analysis."
        (NOTE: PyWavelets package must be installed for this method. It should be applied
        to 1s data - otherwise the sample window should be changed.)

        METHOD:
        1. Use the 4th-order Daubechies wavelet filter to calculate the 1st to 3rd details
           (D1, D2, D3) of the geomagnetic signal. This is applied to a sliding window of
           five samples.
        2. The 3rd detail (D3) samples are squared to evaluate the magnitude.
        3. The sample window (5) is averaged to avoid ripple effects. (This means the
           returned stream will have ~1/5 the size of the original.)

    PARAMETERS:
    Variables:
        - key:          (str) Apply DWT to this key. Default 'x' due to SSCs dominating
                        the horizontal component.
        - wavelet:      (str) Type of filter to use. Default 'db4' (4th-order Daubechies
                        wavelet filter) according to Hafez (2013).
        - level:        (int) Decomposition level. Will calculate details down to this level.
                        Default 3, also Hafez (2013).
        - plot:         (bool) If True, will display a plot of A3, D1, D2 and D3.
        - outfile:      (str) If given, will plot will be saved to 'outfile' path.
        - window:       (int) Length of sample window. Default 5, i.e. 5s with second data.

    RETURNS:
        - DWT_stream:   (DataStream object) A stream containing the following:
                        'x': A_n (approximation function)
                        'var1': D1 (first detail)
                        'var2': D2 (second detail)
                        'var3': D3 (third detail)
                        ... will have to be changed if higher details are required.

    EXAMPLE:
        >>> DWT_stream = stream.DWT_calc(plot=True)

    APPLICATION:
        # Storm detection using detail 3 (D3 = var3):
        from magpy.stream import *
        stream = read('LEMI_1s_Data_2014-02-15.cdf')    # 2014-02-15 is a good storm example
        DWT_stream = stream.DWT_calc(plot=True)
        Da_min = 0.0005 # nT^2 (minimum amplitude of D3 for storm detection)
        Dp_min = 40 # seconds (minimum period of Da > Da_min for storm detection)
        detection = False
        for row in DWT_stream:
            if row.var3 >= Da_min and detection == False:
                timepin = row.time
                detection = True
            elif row.var3 < Da_min and detection == True:
                duration = (num2date(row.time) - num2date(timepin)).seconds
                if duration >= Dp_min:
                    print "Storm detected!"
                    print duration, num2date(timepin)
                detection = False
        """

        # Import required package PyWavelets:
        # http://www.pybytes.com/pywavelets/index.html
        import pywt

        # 1a. Grab array from stream
        data = self._get_column(key)
        t_ind = KEYLIST.index('time')

        #DWT_stream = DataStream([],{})
        DWT_stream = DataStream()
        headers = DWT_stream.header
        array = [[] for key in KEYLIST]
        x_ind = KEYLIST.index('x')
        dx_ind = KEYLIST.index('dx')
        var1_ind = KEYLIST.index('var1')
        var2_ind = KEYLIST.index('var2')
        var3_ind = KEYLIST.index('var3')
        i = 0
        logger.info("DWT_calc: Starting Discrete Wavelet Transform of key %s." % key)

        # 1b. Loop for sliding window
        while True:
            if i >= (len(data)-window):
                break

            #row = LineStruct()
            # Take the values in the middle of the window (not exact but changes are
            # not extreme over standard 5s window)
            #row.time = self[i+window/2].time
            array[t_ind].append(self.ndarray[t_ind][i+window/2])
            data_cut = data[i:i+window]
            #row.x = sum(data_cut)/float(window)
            array[x_ind].append(sum(data_cut)/float(window))

            # 1c. Calculate wavelet transform coefficients
            # Wavedec produces results in form: [cA_n, cD_n, cD_n-1, ..., cD2, cD1]
            # (cA_n is a list of coefficients for an approximation for the nth order.
            # All cD_n are coefficients for details n --> 1.)
            coeffs = pywt.wavedec(data_cut, wavelet, level=level)

            # 1d. Calculate approximation and detail functions from coefficients
            take = len(data_cut)        # (Length of fn from coeffs = length of original data)
            functions = []
            approx = True
            for item in coeffs:
                if approx:
                    part = 'a'  # Calculate approximation function
                else:
                    part = 'd'  # Calculate detail function
                function = pywt.upcoef(part, item, wavelet, level=level, take=take)
                functions.append(function)
                approx = False

            # 2. Square the results
            fin_fns = []
            for item in functions:
                item_sq = [j**2 for j in item]
                # 3. Average over the window
                val = sum(item_sq)/window
                fin_fns.append(val)

            # TODO: This is hard-wired for level=3.
            #row.dx, row.var1, row.var2, row.var3 = fin_fns
            array[dx_ind].append(fin_fns[0])
            array[var1_ind].append(fin_fns[3])
            array[var2_ind].append(fin_fns[2])
            array[var3_ind].append(fin_fns[1])

            #DWT_stream.add(row)
            i += window

        logger.info("DWT_calc: Finished DWT.")

        DWT_stream.header['col-x'] = 'A3'
        DWT_stream.header['unit-col-x'] = 'nT^2'
        DWT_stream.header['col-var1'] = 'D1'
        DWT_stream.header['unit-col-var1'] = 'nT^2'
        DWT_stream.header['col-var2'] = 'D2'
        DWT_stream.header['unit-col-var2'] = 'nT^2'
        DWT_stream.header['col-var3'] = 'D3'
        DWT_stream.header['unit-col-var3'] = 'nT^2'

        # Plot stream:
        if plot == True:
            date = datetime.strftime(num2date(self.ndarray[0][0]),'%Y-%m-%d')
            logger.info('DWT_calc: Plotting data...')
            if outfile:
                DWT_stream.plot(['x','var1','var2','var3'],
                                plottitle="DWT Decomposition of %s (%s)" % (key,date),
                                outfile=outfile)
            else:
                DWT_stream.plot(['x','var1','var2','var3'],
                                plottitle="DWT Decomposition of %s (%s)" % (key,date))

        #return DWT_stream
        return DataStream([LineStruct()], headers, np.asarray(array))


    def eventlogger(self, key, values, compare=None, stringvalues=None, addcomment=None, debugmode=None):
        """
        read stream and log data of which key meets the criteria
        maybe combine with extract

        Required:
        :type key: string
        :param key: provide the key to be examined
        :type values: list
        :param values: provide a list of three values
        :type values: list
        :param values: provide a list of three values
        Optional:
        :type compare: string
        :param compare: ">, <, ==, !="
        :type stringvalues: list
        :param stringvalues: provide a list of exactly the same length as values with the respective comments
        :type addcomment: bool
        :param addcomment: if true add the stringvalues to the comment line of the datastream

        :type debugmode: bool
        :param debugmode: provide more information

        example:
        compare is string like ">, <, ==, !="
        st.eventlogger(['var3'],[15,20,30],'>')
        """
        assert type(values) == list

        if not compare:
            compare = '=='
        if not compare in ['<','>','<=','>=','==','!=']:
            logger.warning('Eventlogger: wrong value for compare: needs to be among <,>,<=,>=,==,!=')
            return self
        if not stringvalues:
            stringvalues = ['Minor storm onset','Moderate storm onset','Major storm onset']
        else:
            assert type(stringvalues) == list
        if not len(stringvalues) == len(values):
            logger.warning('Eventlogger: Provided comments do not match amount of values')
            return self

        for elem in self:
            #evaluationstring = 'elem.' + key + ' ' + compare + ' ' + str(values[0])
            if eval('elem.'+key+' '+compare+' '+str(values[2])):
                stormlogger.warning('%s at %s' % (stringvalues[2],num2date(elem.time).replace(tzinfo=None)))
                if addcomment:
                    if elem.comment == '-':
                        elem.comment = stringvalues[2]
                    else:
                        elem.comment += ', ' + stringvalues[2]
            elif eval('elem.'+key+' '+compare+' '+str(values[1])):
                stormlogger.warning('%s at %s' % (stringvalues[1],num2date(elem.time).replace(tzinfo=None)))
                if addcomment:
                    if elem.comment == '-':
                        elem.comment = stringvalues[1]
                    else:
                        elem.comment += ', ' + stringvalues[1]
            elif eval('elem.'+key+' '+compare+' '+str(values[0])):
                stormlogger.warning('%s at %s' % (stringvalues[0],num2date(elem.time).replace(tzinfo=None)))
                if addcomment:
                    if elem.comment == '-':
                        elem.comment = stringvalues[0]
                    else:
                        elem.comment += ', ' + stringvalues[0]

        return self


    def extract(self, key, value, compare=None, debugmode=None):
        """
        DEFINITION:
            Read stream and extract data of the selected key which meets the choosen criteria

        PARAMETERS:
        Variables:
            - key:      (str) streams key e.g. 'x'.
            - value:    (str/float/int) any selected input which should be tested for
                        special note: if value is in brackets, then the term is evaluated
                        e.g. value="('int(elem.time)')" selects all points at 0:00
                        Important: this only works for compare = '=='
         Kwargs:
            - compare:  (str) criteria, one out of ">=", "<=",">", "<", "==", "!=", default is '=='
            - debugmode:(bool) if true several additional outputs will be created

        RETURNS:
            - DataStream with selected values only

        EXAMPLES:
            >>> extractedstream = stream.extract('x',20000,'>')
            >>> extractedstream = stream.extract('str1','Berger')
        """

        if not compare:
            compare = '=='
        if not compare in [">=", "<=",">", "<", "==", "!=", 'like']:
            logger.info('--- Extract: Please provide proper compare parameter ">=", "<=",">", "<", "==", "like" or "!=" ')
            return self

        if value in ['',None]:
            return self

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True

        ind = KEYLIST.index(key)

        stream = self.copy()

        if not self._is_number(value):
            if value.startswith('(') and value.endswith(')') and compare == '==':
                logger.info("extract: Selected special functional type -equality defined by difference less then 10 exp-6")
                if ndtype:
                    val = eval(value[1:-1])
                    indexar = np.where((np.abs(stream.ndarray[ind]-val)) < 0.000001)[0]
                else:
                    val = value[1:-1]
                    liste = []
                    for elem in self:
                        if abs(eval('elem.'+key) - eval(val)) < 0.000001:
                            liste.append(elem)
                    return DataStream(liste,self.header)
            else:
                #print "Found String", ndtype
                too = '"' + str(value) + '"'
                if ndtype:
                    if compare == 'like':
                        indexar = np.asarray([i for i, s in enumerate(stream.ndarray[ind]) if str(value) in s])
                    else:
                        #print stream.ndarray[ind]
                        searchclause = 'stream.ndarray[ind] '+ compare + ' ' + too
                        #print searchclause, ind, key
                        indexar = eval('np.where('+searchclause+')[0]')
                    #print indexar, len(indexar)
        else:
            too = str(value)
            if ndtype:
                searchclause = 'stream.ndarray[ind].astype(float) '+ compare + ' ' + too
                with np.errstate(invalid='ignore'):
                    indexar = eval('np.where('+searchclause+')[0]')

        if ndtype:
            for ind,el in enumerate(stream.ndarray):
                if len(stream.ndarray[ind]) > 0:
                    ar = [stream.ndarray[ind][i] for i in indexar]
                    stream.ndarray[ind] = np.asarray(ar).astype(object)
            return stream
        else:
            liste = [elem for elem in self if eval('elem.'+key+' '+ compare + ' ' + too)]
            return DataStream(liste,self.header,self.ndarray)

    def extract2(self, keys, get='>', func=None, debugmode=None):
        """
        DEFINITION:
            Read stream and extract data of the selected keys which meets the choosen criteria

        PARAMETERS:
        Variables:
            - keys:     (list) keylist like ['x','f'].
            - func:     a function object
         Kwargs:
            - get:  (str) criteria, one out of ">=", "<=",">", "<", "==", "!=", default is '=='
            - debugmode:(bool) if true several additional outputs will be created

        RETURNS:
            - DataStream with selected values only

        EXAMPLES:
            >>> extractedstream = stream.extract('x',20000,'>')
            >>> extractedstream = stream.extract('str1','Berger')
        """

        if not get:
            get = '=='
        if not get in [">=", "<=",">", "<", "==", "!=", 'like']:
            print ('--- Extract: Please provide proper compare parameter ">=", "<=",">", "<", "==", "like" or "!=" ')
            return self

        stream = self.copy()

        def func(x):
            y = 1/(0.2*exp(0.06/(x/10000.))) + 2.5
            return y

        xpos = KEYLIST.index(keys[0])
        ypos = KEYLIST.index(keys[1])
        x = stream.ndarray[xpos].astype(float)
        y = stream.ndarray[ypos].astype(float)

        idxlist = []
        for idx,val in enumerate(x):
            ythreshold = func(val)
            test = eval('y[idx] '+ get + ' ' + str(ythreshold))
            #print (val, 'y[idx] '+ get + ' ' + str(ythreshold))
            if test:
                idxlist.append(idx)

        array = [[] for key in KEYLIST]
        for i,key in enumerate(KEYLIST):
            for idx in idxlist:
                if len(stream.ndarray[i]) > 0:
                    array[i].append(stream.ndarray[i][idx])
            array[i] = np.asarray(array[i])
        print ("Length of list", len(idxlist))

        return DataStream([LineStruct()], stream.header,np.asarray(array))


    def extrapolate(self, start, end):
        """
      DESCRIPTION:
        Reads stream output of absolute analysis and extrapolate the data
        current method (too be improved if necessary):
        - repeat the last and first input with baseline values at disered start and end time
        Hereby and functional fit (e.g. spline or polynom is forced towards a quasi-stable baseline evolution).
        The principle asumption of this technique is that the base values are constant on average.
      APPLICATION:
        is used by stream.baseline
        """

        ltime = date2num(end) # + timedelta(days=1))
        ftime = date2num(start) # - timedelta(days=1))
        array = [[] for key in KEYLIST]

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True
            firsttime = np.min(self.ndarray[0])
            lasttime = np.max(self.ndarray[0])
            # Find the last element with baseline values - assuming a sorted array
            inddx = KEYLIST.index('dx')
            lastind=len(self.ndarray[0])-1

            #print("Extrapolate", self.ndarray,len(self.ndarray[inddx]), self.ndarray[inddx], self.ndarray[inddx][lastind])
            while np.isnan(float(self.ndarray[inddx][lastind])):
                lastind = lastind-1
            firstind=0
            while np.isnan(float(self.ndarray[inddx][firstind])):
                firstind = firstind+1

            #print "extrapolate", num2date(ftime), num2date(ltime), ftime, ltime
            for idx,elem in enumerate(self.ndarray):
                if len(elem) > 0:
                    array[idx] = self.ndarray[idx]
                    if idx == 0:
                        array[idx] = np.append(array[idx],ftime)
                        array[idx] = np.append(array[idx],ltime)
                        #array[idx] = np.append(self.ndarray[idx],ftime)
                        #array[idx] = np.append(self.ndarray[idx],ltime)
                    else:
                        array[idx] = np.append(array[idx],array[idx][firstind])
                        array[idx] = np.append(array[idx],array[idx][lastind])
                        #array[idx] = np.append(self.ndarray[idx],self.ndarray[idx][firstind])
                        #array[idx] = np.append(self.ndarray[idx],self.ndarray[idx][lastind])
            indar = np.argsort(array[0])
            array = [el[indar].astype(object) if len(el)>0 else np.asarray([]) for el in array]
        else:
            if self.length()[0] < 2:
                return self
            firstelem = self[0]
            lastelem = self[-1]
            # Find the last element with baseline values
            i = 1
            while isnan(lastelem.dx):
                lastelem = self[-i]
                i = i +1

            line = LineStruct()
            for key in KEYLIST:
                if key == 'time':
                    line.time = ftime
                else:
                    exec('line.'+key+' = firstelem.'+key)
            self.add(line)
            line = LineStruct()
            for key in KEYLIST:
                if key == 'time':
                    line.time = ltime
                else:
                    exec('line.'+key+' = lastelem.'+key)
            self.add(line)


        stream = DataStream(self,self.header,np.asarray(array))
        #print "extra", stream.ndarray
        #print "extra", stream.length()

        #stream = stream.sorting()

        return stream

        #return DataStream(self,self.header,self.ndarray)


    def filter(self,**kwargs):
        """
        DEFINITION:
            Uses a selected window to filter the datastream - similar to the smooth function.
            (take a look at the Scipy Cookbook/Signal Smooth)
            This method is based on the convolution of a scaled window with the signal.
            The signal is prepared by introducing reflected copies of the signal
            (with the window size) in both ends so that transient parts are minimized
            in the begining and end part of the output signal.
            This function is approximately twice as fast as the previous version.
            Difference: Gaps of the stream a filled by time steps with NaNs in the data columns
            By default missing values are interpolated if more than 90 percent of data is present
            within the window range. This is used to comply with INTERMAGNET rules. Set option
            conservative to False to avoid this.

        PARAMETERS:
        Kwargs:
            - keys:             (list) List of keys to smooth
            - filter_type:      (string) name of the window. One of
                                'flat','barthann','bartlett','blackman','blackmanharris','bohman',
                                'boxcar','cosine','flattop','hamming','hann','nuttall',
                                'parzen','triang','gaussian','wiener','spline','butterworth'
                                See http://docs.scipy.org/doc/scipy/reference/signal.html
            - filter_width:     (timedelta) window width of the filter
            - resample_period:  (int) resampling interval in seconds (e.g. 1 for one second data)
                                 leave blank for standard filters as it will be automatically selected
            - noresample:       (bool) if True the data set is resampled at filter_width positions
            - missingdata:      (string) define how to deal with missing data
                                          'conservative' (default): no filtering
                                          'interpolate': interpolate if less than 10% are missing
                                          'mean': use mean if less than 10% are missing'
            - conservative:     (bool) if True than no interpolation is performed
            - autofill:         (list) of keys: provide a keylist for which nan values are linearly interpolated before filtering - use with care, might be useful if you have low resolution parameters asociated with main values like (humidity etc)
            - resampleoffset:   (timedelta) if provided the offset will be added to resamples starttime
            - resamplemode:     (string) if 'fast' then fast resampling is used
            - testplot:         (bool) provides a plot of unfiltered and filtered data for each key if true
            - dontfillgaps:     (bool) if true, get_gaps will not be conducted - much faster but requires the absence of data gaps (including time step)

        RETURNS:
            - self:             (DataStream) containing the filtered signal within the selected columns

        EXAMPLE:
            >>> nice_data = bad_data.filter(keys=['x','y','z'])
            or
            >>> nice_data = bad_data.filter(filter_type='gaussian',filter_width=timedelta(hours=1))

        APPLICATION:

        TODO:
            !!A proper and correct treatment of gaps within the dataset to be filtered is missing!!

        """

        # ########################
        # Kwargs and definitions
        # ########################
        filterlist = ['flat','barthann','bartlett','blackman','blackmanharris','bohman',
                'boxcar','cosine','flattop','hamming','hann','nuttall','parzen','triang',
                'gaussian','wiener','spline','butterworth']

        # To be added
        #kaiser(M, beta[, sym])         Return a Kaiser window.
        #slepian(M, width[, sym])       Return a digital Slepian (DPSS) window.
        #chebwin(M, at[, sym])  Return a Dolph-Chebyshev window.
        # see http://docs.scipy.org/doc/scipy/reference/signal.html

        keys = kwargs.get('keys')
        filter_type = kwargs.get('filter_type')
        filter_width = kwargs.get('filter_width')
        resample_period = kwargs.get('resample_period')
        filter_offset = kwargs.get('filter_offset')
        noresample = kwargs.get('noresample')
        resamplemode = kwargs.get('resamplemode')
        resamplestart = kwargs.get('resamplestart')
        resampleoffset = kwargs.get('resampleoffset')
        testplot = kwargs.get('testplot')
        autofill = kwargs.get('autofill')
        dontfillgaps = kwargs.get('dontfillgaps')
        fillgaps = kwargs.get('fillgaps')
        debugmode = kwargs.get('debugmode')
        conservative =  kwargs.get('conservative')
        missingdata =  kwargs.get('missingdata')

        sr = self.samplingrate()

        if not keys:
            keys = self._get_key_headers(numerical=True)
        if not filter_width and not resample_period:
            if sr < 0.5: # use 1 second filter with 0.3 Hz cut off as default
                filter_width = timedelta(seconds=3.33333333)
                resample_period = 1.0
            else: # use 1 minute filter with 0.008 Hz cut off as default
                filter_width = timedelta(minutes=2)
                resample_period = 60.0
        if not filter_width: # resample_period obviously provided - use nyquist
                filter_width = timedelta(seconds=2*resample_period)
        if not resample_period: # filter_width obviously provided... use filter_width as period
            resample_period = filter_width.total_seconds()
            # Fall back for old data
            if filter_width == timedelta(seconds=1):
                filter_width = timedelta(seconds=3.3)
                resample_period = 1.0
        if not noresample:
            resample = True
        else:
            resample = False
        if not autofill:
            autofill = []
        else:
            if not isinstance(autofill, (list, tuple)):
                print("Autofill need to be a keylist")
                return
        if not resamplemode:
            resamplefast = False
        else:
            if resamplemode == 'fast':
                resamplefast = True
            else:
                resamplefast = False
        if not debugmode:
            debugmode = None
        if not filter_type:
            filter_type = 'gaussian'
        if resamplestart:
            print("##############  Warning ##############")
            print("option RESAMPLESTART is not used any more. Switch to resampleoffset for modifying time steps")
        if not missingdata:
            missingdata = 'conservative'

        ndtype = False

        # ########################
        # Basic validity checks and window size definitions
        # ########################
        if not filter_type in filterlist:
            logger.error("smooth: Window is none of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman', etc")
            logger.debug("smooth: You entered non-existing filter type -  %s  - " % filter_type)
            return self

        #print self.length()[0]
        if not self.length()[0] > 1:
            logger.error("Filter: stream needs to contain data - returning.")
            return self

        if debugmode:
            print("Starting length:", self.length())

        #if not dontfillgaps:   ### changed--- now using dont fill gaps as default
        if fillgaps:
            self = self.get_gaps()
            if debugmode:
                print("length after getting gaps:", len(self))

        window_period = filter_width.total_seconds()
        si = timedelta(seconds=self.get_sampling_period()*24*3600)

        sampling_period = si.days*24*3600 + si.seconds + np.round(si.microseconds/1000000.0,2)

        if debugmode:
            print("Timedelta and sampling period:", si, sampling_period)

        # window_len defines the window size in data points assuming the major sampling period to be valid for the dataset
        if filter_type == 'gaussian':
            # For a gaussian fit
            window_len = np.round((window_period/sampling_period))
            #print (window_period,sampling_period,window_len)
            # Window length needs to be odd number:
            if window_len % 2 == 0:
                window_len = window_len +1
            std = 0.83255461*window_len/(2*np.pi)
            trangetmp = self._det_trange(window_period)*24*3600
            if trangetmp < 1:
                trange = np.round(trangetmp,3)
            else:
                trange = timedelta(seconds=(self._det_trange(window_period)*24*3600)).seconds
            if debugmode:
                print("Window character: ", window_len, std, trange)
        else:
            window_len = np.round(window_period/sampling_period)
            if window_len % 2:
                window_len = window_len+1
            trange = window_period/2

        if sampling_period >= window_period:
            logger.warning("Filter: Sampling period is equal or larger then projected filter window - returning.")
            return self

        # ########################
        # Reading data of each selected column in stream
        # ########################

        if len(self.ndarray[0])>0:
            t = self.ndarray[0]
            ndtype = True
        else:
            t = self._get_column('time')

        if debugmode:
            print("Length time column:", len(t))

        for key in keys:
            if debugmode:
                print ("Start filtering for", key)
            if not key in KEYLIST:
                logger.error("Column key %s not valid." % key)
            keyindex = KEYLIST.index(key)
            if len(self.ndarray[keyindex])>0:
                v = self.ndarray[keyindex]
            else:
                v = self._get_column(key)

            # INTERMAGNET 90 percent rule: interpolate missing values if less than 10 percent are missing
            #if not conservative or missingdata in ['interpolate','mean']:
            if missingdata in ['interpolate','mean']:
                fill = 'mean'
                try:
                    if missingdata == 'interpolate':
                        fill = missingdate
                    else:
                        fill = 'mean'
                except:
                    fill = 'mean'
                v = self.missingvalue(v,np.round(window_period/sampling_period),fill=fill) # using ratio here and not _len

            if key in autofill:
                logger.warning("Filter: key %s has been selected for linear interpolation before filtering." % key)
                logger.warning("Filter: I guess you know what you are doing...")
                nans, x= nan_helper(v)
                v[nans]= interp(x(nans), x(~nans), v[~nans])

            # Make sure that we are dealing with numbers
            v = np.array(list(map(float, v)))

            if v.ndim != 1:
                logger.error("Filter: Only accepts 1 dimensional arrays.")
            if window_len<3:
                logger.error("Filter: Window lenght defined by filter_width needs to cover at least three data points")

            if debugmode:
                print("Treating k:", key, v.size)

            if v.size >= window_len:
                #print ("Check:", v, len(v), window_len)
                s=np.r_[v[int(window_len)-1:0:-1],v,v[-1:-int(window_len):-1]]

                if filter_type == 'gaussian':
                    w = signal.gaussian(window_len, std=std)
                    y=np.convolve(w/w.sum(),s,mode='valid')
                    res = y[(int(window_len/2)):(len(v)+int(window_len/2))]
                elif filter_type == 'wiener':
                    res = signal.wiener(v, int(window_len), noise=0.5)
                elif filter_type == 'butterworth':
                    dt = 800./float(len(v))
                    nyf = 0.5/dt
                    b, a = signal.butter(4, 1.5/nyf)
                    res = signal.filtfilt(b, a, v)
                elif filter_type == 'spline':
                    res = UnivariateSpline(t, v, s=240)
                elif filter_type == 'flat':
                    w=np.ones(int(window_len),'d')
                    s = np.ma.masked_invalid(s)
                    y=np.convolve(w/w.sum(),s,mode='valid') #'valid')
                    res = y[(int(window_len/2)-1):(len(v)+int(window_len/2)-1)]
                else:
                    w = eval('signal.'+filter_type+'(window_len)')
                    y=np.convolve(w/w.sum(),s,mode='valid')
                    res = y[(int(window_len/2)):(len(v)+int(window_len/2))]

                if testplot == True:
                    fig, ax1 = plt.subplots(1,1, figsize=(10,4))
                    ax1.plot(t, v, 'b.-', linewidth=2, label = 'raw data')
                    ax1.plot(t, res, 'r.-', linewidth=2, label = filter_type)
                    plt.show()

                if ndtype:
                    self.ndarray[keyindex] = res
                else:
                    self._put_column(res,key)

        if resample:
            if debugmode:
                print("Resampling: ", keys)
            self = self.resample(keys,period=resample_period,fast=resamplefast,offset=resampleoffset)
            self.header['DataSamplingRate'] = str(resample_period) + ' sec'

        # ########################
        # Update header information
        # ########################
        passband = filter_width.total_seconds()
        #print ("passband", 1/passband)
        #self.header['DataSamplingFilter'] = filter_type + ' - ' + str(trange) + ' sec'
        self.header['DataSamplingFilter'] = filter_type + ' - ' + str(1.0/float(passband)) + ' Hz'

        return self



    def nfilter(self, **kwargs):
        """
    DEFINITION:
        Code for simple application, filtering function.
        Returns stream with filtered data with sampling period of
        filter_width.

    PARAMETERS:
    Variables:
        - variable:     (type) Description.
    Kwargs:
        - filter_type:  (str) Options: gaussian, linear or special. Default = gaussian.
        - filter_width: (timedelta object) Default = timedelta(minutes=1)
        - filter_offset:        (timedelta object) Default=0
        - gauss_win:    (int) Default = 1.86506 (corresponds to +/-45 sec in case of min or 45 min in case of hour).
        - fmi_initial_data:     (DataStream containing dH values (dx)) Default=[].

    RETURNS:
        - stream:       (DataStream object) Stream containing filtered data.

    EXAMPLE:
        >>> stream_filtered = stream.filter(filter_width=timedelta(minutes=3))

    APPLICATION:

        """

        return self.filter(**kwargs)


    def fit(self, keys, **kwargs):
        """
    DEFINITION:
        Code for fitting data. Please note: if nans are present in any of the selected keys
        the whole line is dropped before fitting.

    PARAMETERS:
    Variables:
        - keys:         (list) Provide a list of keys to be fitted (e.g. ['x','y','z'].
    Kwargs:
        - fitfunc:      (str) Options: 'poly', 'harmonic', 'spline', default='spline'
        - timerange:    (timedelta object) Default = timedelta(hours=1)
        - fitdegree:    (float) Default=5
        - knotstep:     (float < 0.5) determines the amount of knots: amount = 1/knotstep ---> VERY smooth 0.1 | NOT VERY SMOOTH 0.001
        - flag:         (bool).

    RETURNS:
        - function object:      (list) func = [functionkeylist, sv, ev]


    EXAMPLE:
        >>> func = stream.fit(['x'])

    APPLICATION:

        """

        # Defaults:
        fitfunc = kwargs.get('fitfunc')
        fitdegree = kwargs.get('fitdegree')
        knotstep = kwargs.get('knotstep')
        starttime = kwargs.get('starttime')
        endtime = kwargs.get('endtime')
        if not fitfunc:
            fitfunc = 'spline'
        if not fitdegree:
            fitdegree = 5
        if not knotstep:
            knotstep = 0.01

        defaulttime = 0
        if not starttime:
            starttime = self._find_t_limits()[0]
        if not endtime:
            endtime = self._find_t_limits()[1]
        if starttime == self._find_t_limits()[0]:
            defaulttime += 1
        if endtime == self._find_t_limits()[1]:
            defaulttime += 1

        if knotstep >= 0.5:
            raise ValueError("Knotstep needs to be smaller than 0.5")

        functionkeylist = {}

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype=True

        tok = True

        fitstream = self.copy()
        if not defaulttime == 2: # TODO if applied to full stream, one point at the end is missing
            fitstream = fitstream.trim(starttime=starttime, endtime=endtime)

        for key in keys:
            tmpst = fitstream._drop_nans(key)
            if ndtype:
                t = tmpst.ndarray[0]
            else:
                t = tmpst._get_column('time')
            if len(t) < 1:
                tok = False
                break

            nt,sv,ev = fitstream._normalize(t)

            #newlist = []
            #for kkk in nt:
            #    if kkk not in newlist:
            #        newlist.append(kkk)
            #    else:
            #        newlist.append(kkk+0.00001)
            #nt = newlist
            #nt = np.sort(np.asarray(nt))
            #print "NT", nt
            sp = fitstream.get_sampling_period()
            if sp == 0:  ## if no dominant sampling period can be identified then use minutes
                sp = 0.0177083333256
            if not key in KEYLIST[1:16]:
                raise ValueError("Column key not valid")
            if ndtype:
                ind = KEYLIST.index(key)
                val = tmpst.ndarray[ind]
            else:
                val = tmpst._get_column(key)

            # interplolate NaN values
            #nans, xxx= nan_helper(val)
            #val[nans]= np.interp(xxx(nans), xxx(~nans), val[~nans])
            #print np.min(nt), np.max(nt), sp, len(self)
            # normalized sampling rate
            sp = sp/(ev-sv) # should be the best?
            #sp = (ev-sv)/len(val) # does not work
            x = arange(np.min(nt),np.max(nt),sp)
            if len(val)>1 and fitfunc == 'spline':
                try:
                    #logger.error('Interpolation: Testing knots (knotsteps = {}), (len(val) = {}'.format(knotstep, len(val)))
                    knots = np.array(arange(np.min(nt)+knotstep,np.max(nt)-knotstep,knotstep))
                    if len(knots) > len(val):
                        knotstep = knotstep*4
                        knots = np.array(arange(np.min(nt)+knotstep,np.max(nt)-knotstep,knotstep))
                        logger.warning('Too many knots in spline for available data. Please check amount of fitted data in time range. Trying to reduce resolution ...')
                    ti = interpolate.splrep(nt, val, k=3, s=0, t=knots)
                except:
                    logger.error('Value error in fit function - likely reason: no valid numbers or too few numbers for fit: len(knots)={} > len(val)={}? '.format(len(knots),len(val)))
                    print ("Checking", key, len(val), val, sp, knotstep, len(knots))
                    raise ValueError("Value error in fit function - not enough data or invalid numbers")
                    return
                #print nt, val, len(knots), knots
                #ti = interpolate.interp1d(nt, val, kind='cubic')
                #print "X", x, np.min(nt),np.max(nt),sp
                #print "TI", ti
                f_fit = interpolate.splev(x,ti)
            elif len(val)>1 and fitfunc == 'poly':
                logger.debug('Selected polynomial fit - amount of data: %d, time steps: %d, degree of fit: %d' % (len(nt), len(val), fitdegree))
                ti = polyfit(nt, val, fitdegree)
                f_fit = polyval(ti,x)
            elif len(val)>1 and fitfunc == 'harmonic':
                logger.debug('Selected harmonic fit - using inverse fourier transform')
                f_fit = self.harmfit(nt, val, fitdegree)
                # Don't use resampled list for harmonic time series
                x = nt
            elif len(val)<=1:
                logger.warning('Fit: No valid data for key {}'.format(key))
                break
                #return
            else:
                logger.warning('Fit: function not valid')
                return
            exec('f'+key+' = interpolate.interp1d(x, f_fit, bounds_error=False)')
            exec('functionkeylist["f'+key+'"] = f'+key)

        if tok:
            func = [functionkeylist, sv, ev]
        else:
            func = [functionkeylist, 0, 0]
        return func

    def extractflags(self):
        """
    DEFINITION:
        Extracts flags asociated with the provided DataStream object
        (as obtained by flaggedstream = stream.flag_outlier())

    PARAMETERS:
    Variables:
        None
    RETURNS:
        - flaglist:     (list) a flaglist of type [st,et,key,flagnumber,commentarray[idx],sensorid,now]

    EXAMPLE:
        >>> flaglist = stream.extractflags()
        """
        sensorid = self.header.get('SensorID','')
        now = datetime.utcnow()
        flaglist = []

        flpos = KEYLIST.index('flag')
        compos = KEYLIST.index('comment')
        flags = self.ndarray[flpos]
        comments = self.ndarray[compos]
        if not len(flags) > 0 or not len(comments) > 0:
            return flaglist

        uniqueflags = self.union(flags)
        uniquecomments = self.union(comments)

        # 1. Extract relevant keys from uniqueflags
        print ("extractflags: Unique Flags -", uniqueflags)
        print ("extractflags: Unique Comments -", uniquecomments)
        # zeroflag = ''
        keylist = []
        for elem in uniqueflags:
            if not elem in ['','-']:
                #print (elem)
                for idx,el in enumerate(elem):
                    if not el == '-' and el in ['0','1','2','3','4','5','6']:
                        keylist.append(NUMKEYLIST[idx-1])
        # 2. Cycle through keys and extract comments
        if not len(keylist) > 0:
            return flaglist

        keylist = self.union(np.asarray(keylist))

        for key in keylist:
            indexflag = KEYLIST.index(key)
            for comment in uniquecomments:
                flagindicies = []
                for idx, elem in enumerate(comments):
                    if not elem == '' and elem == comment:
                        #print ("ELEM", elem)
                        flagindicies.append(idx)
                # 2. get consecutive groups
                for k, g in groupby(enumerate(flagindicies), lambda ix: ix[0] - ix[1]):
                    try:
                        consecutives = list(map(itemgetter(1), g))
                        st = num2date(self.ndarray[0][consecutives[0]]).replace(tzinfo=None)
                        et = num2date(self.ndarray[0][consecutives[-1]]).replace(tzinfo=None)
                        flagnumber = int(flags[consecutives[0]][indexflag])
                        flaglist.append([st,et,key,flagnumber,comment,sensorid,now])
                    except:
                        print ("extractflags: error when extracting flaglist")

        return flaglist


    def flagfast(self,indexarray,flag, comment,keys=None):
        """
    DEFINITION:
        Add a flag to specific indicies of the streams ndarray.

    PARAMETERS:
    Variables:
        - keys:         (list) Optional: list of keys to mark ['x','y','z']
        - flag:         (int) 0 ok, 1 remove, 2 force ok, 3 force remove,
                        4 merged from other instrument
        - comment:      (str) The reason for flag
        - indexarray:   (array) indicies of the datapoint(s) to mark

    RETURNS:
        - DataStream:   Input stream with flags and comments.

    EXAMPLE:
        >>> data = data.flagfast([155],'3','Lawnmower',['x','y','z'])

    APPLICATION:
        """

        print("Adding flags .... ")
        # Define Defaultflag
        flagls = [str('-') for elem in FLAGKEYLIST]
        defaultflag = ''

        # Get new flag
        newflagls = []
        if not keys:
            for idx,key in enumerate(FLAGKEYLIST): # Flag all existing data
                if len(self.ndarray[idx]) > 0:
                    newflagls.append(str(flag))
                else:
                    newflagls.append('-')
            newflag = ''.join(newflagls)
        else:
            for idx,key in enumerate(FLAGKEYLIST): # Only key column
                if len(self.ndarray[idx]) > 0 and FLAGKEYLIST[idx] in keys:
                    newflagls.append(str(flag))
                else:
                    newflagls.append('-')
            newflag = ''.join(newflagls)

        flagarray, commentarray = [],[]
        flagindex = KEYLIST.index('flag')
        commentindex = KEYLIST.index('comment')

        # create a predefined list
        # ########################
        # a) get existing flags and comments or create empty lists
        if len(self.ndarray[flagindex]) > 0:
            flagarray = self.ndarray[flagindex].astype(object)
        else:
            flagarray = [''] * len(self.ndarray[0])
        if len(self.ndarray[commentindex]) > 0:
            commentarray = self.ndarray[commentindex].astype(object)
        else:
            commentarray = [''] * len(self.ndarray[0])
        # b) insert new info
        for i in indexarray:
            flagarray[i] = newflag
            commentarray[i] = comment

        commentarray = np.asarray(commentarray, dtype='object')
        flagarray = np.asarray(flagarray, dtype='object')

        flagnum = KEYLIST.index('flag')
        commentnum = KEYLIST.index('comment')
        self.ndarray[flagnum] = flagarray
        self.ndarray[commentnum] = commentarray
        #print "... finished"
        return self


    def flag_range(self, **kwargs):
        """
    DEFINITION:
        Flags data within time range or data exceeding a certain threshold
        Coding : 0 take, 1 remove, 2 force take, 3 force remove

    PARAMETERS:
    Variables:
        - None.
    Kwargs:
        - keys:         (list) List of keys to check for criteria. Default = all numerical
                            please note: for using above and below criteria only one element
                            need to be provided (e.g. ['x']
        - text          (string) comment
        - flagnum       (int) Flagid
        - keystoflag:   (list) List of keys to flag. Default = all numerical
        - below:        (float) flag data of key below this numerical value.
        - above:        (float) flag data of key exceeding this numerical value.
        - starttime:    (datetime Object)
        - endtime:      (datetime Object)
    RETURNS:
        - flaglist:     (list) flagging information - use stream.flag(flaglist) to add to stream

    EXAMPLE:

        >>> fllist = stream.flag_range(keys=['x'], above=80)

    APPLICATION:
        """

        keys = kwargs.get('keys')
        above = kwargs.get('above')
        below = kwargs.get('below')
        starttime = kwargs.get('starttime')
        endtime = kwargs.get('endtime')
        text = kwargs.get('text')
        flagnum = kwargs.get('flagnum')
        keystoflag = kwargs.get('keystoflag')


        sensorid = self.header.get('SensorID')
        moddate = datetime.utcnow()
        flaglist=[]
        if not keystoflag:
            keystoflag = self._get_key_headers(numerical=True)
        if not flagnum:
            flagnum = 0

        if not len(self.ndarray[0]) > 0:
            print ("flag_range: No data available - aborting")
            return flaglist

        if not len(keys) == 1:
            if above or below:
                print ("flag_range: for using thresholds above and below only a single key needs to be provided")
                print ("  -- ignoring given above and below values")
                below = False
                above = False

        # test validity of starttime and endtime

        trimmedstream = self.copy()
        if starttime and endtime:
            trimmedstream = self._select_timerange(starttime=starttime,endtime=endtime)
            trimmedstream = DataStream([LineStruct()],self.header,trimmedstream)
        elif starttime:
            trimmedstream = self._select_timerange(starttime=starttime)
            trimmedstream = DataStream([LineStruct()],self.header,trimmedstream)
        elif endtime:
            trimmedstream = self._select_timerange(endtime=endtime)
            trimmedstream = DataStream([LineStruct()],self.header,trimmedstream)

        if not above and not below:
            # return flags for all data in trimmed stream
            for elem in keystoflag:
                flagline = [num2date(trimmedstream.ndarray[0][0]).replace(tzinfo=None),num2date(trimmedstream.ndarray[0][-1]).replace(tzinfo=None),elem,int(flagnum),text,sensorid,moddate]
                flaglist.append(flagline)
            return flaglist

        if above and below:
            # TODO create True/False list and then follow the bin detector example
            ind = KEYLIST.index(keys[0])
            trueindicies = (trimmedstream.ndarray[ind] > above) & (trimmedstream.ndarray[ind] < below)

            d = np.diff(trueindicies)
            idx, = d.nonzero()
            idx += 1

            if not text:
                text = 'outside of range {} to {}'.format(below,above)
            if trueindicies[0]:
                # If the start of condition is True prepend a 0
                idx = np.r_[0, idx]
            if trueindicies[-1]:
                # If the end of condition is True, append the length of the array
                idx = np.r_[idx, trimmedstream.ndarray[ind].size] # Edit
            # Reshape the result into two columns
            idx.shape = (-1,2)

            for start,stop in idx:
                stop = stop-1
                for elem in keystoflag:
                    flagline = [num2date(trimmedstream.ndarray[0][start]).replace(tzinfo=None),num2date(trimmedstream.ndarray[0][stop]).replace(tzinfo=None),elem,int(flagnum),text,sensorid,moddate]
                    flaglist.append(flagline)
        elif above:
            # TODO create True/False list and then follow the bin detector example
            ind = KEYLIST.index(keys[0])
            trueindicies = trimmedstream.ndarray[ind] > above

            d = np.diff(trueindicies)
            idx, = d.nonzero()
            idx += 1

            if not text:
                text = 'exceeding {}'.format(above)
            if trueindicies[0]:
                # If the start of condition is True prepend a 0
                idx = np.r_[0, idx]
            if trueindicies[-1]:
                # If the end of condition is True, append the length of the array
                idx = np.r_[idx, trimmedstream.ndarray[ind].size] # Edit
            # Reshape the result into two columns
            idx.shape = (-1,2)

            for start,stop in idx:
                stop = stop-1
                for elem in keystoflag:
                    flagline = [num2date(trimmedstream.ndarray[0][start]).replace(tzinfo=None),num2date(trimmedstream.ndarray[0][stop]).replace(tzinfo=None),elem,int(flagnum),text,sensorid,moddate]
                    flaglist.append(flagline)
        elif below:
            # TODO create True/False the other way round
            ind = KEYLIST.index(keys[0])
            truefalse = trimmedstream.ndarray[ind] < below

            d = np.diff(truefalse)
            idx, = d.nonzero()
            idx += 1

            if not text:
                text = 'below {}'.format(below)
            if truefalse[0]:
                # If the start of condition is True prepend a 0
                idx = np.r_[0, idx]
            if truefalse[-1]:
                # If the end of condition is True, append the length of the array
                idx = np.r_[idx, trimmedstream.ndarray[ind].size] # Edit
            # Reshape the result into two columns
            idx.shape = (-1,2)

            for start,stop in idx:
                stop = stop-1
                for elem in keystoflag:
                    flagline = [num2date(trimmedstream.ndarray[0][start]).replace(tzinfo=None),num2date(trimmedstream.ndarray[0][stop]).replace(tzinfo=None),elem,int(flagnum),text,sensorid,moddate]
                    flaglist.append(flagline)

        return flaglist

    def flag_outlier(self, **kwargs):
        """
    DEFINITION:
        Flags outliers in data, using quartiles.
        Coding : 0 take, 1 remove, 2 force take, 3 force remove
        Example:
        0000000, 0001000, etc
        012 = take f, automatically removed v, and force use of other
        300 = force remove f, take v, and take other

    PARAMETERS:
    Variables:
        - None.
    Kwargs:
        - keys:         	(list) List of keys to evaluate. Default = all numerical
        - threshold:   		(float) Determines threshold for outliers.
                        	1.5 = standard
                        	5 = weak condition, keeps storm onsets in (default)
                        	4 = a useful comprimise to be used in automatic analysis.
        - timerange:    	(timedelta Object) Time range. Default = samlingrate(sec)*600
        - stdout:        	prints removed values to stdout
        - returnflaglist	(bool) if True, a flaglist is returned instead of stream
        - markall       	(bool) default is False. If True, all components (provided keys)
                                 are flagged even if outlier is only detected in one. Useful for
                                 vectorial data 
    RETURNS:
        - stream:       (DataStream Object) Stream with flagged data.

    EXAMPLE:

        >>> stream.flag_outlier(keys=['x','y','z'], threshold=2)

    APPLICATION:
        """

        # Defaults:
        timerange = kwargs.get('timerange')
        threshold = kwargs.get('threshold')
        keys = kwargs.get('keys')
        markall = kwargs.get('markall')
        stdout = kwargs.get('stdout')
        returnflaglist = kwargs.get('returnflaglist')

        sr = self.samplingrate()
        flagtimeprev = 0
        startflagtime = 0

        if not timerange:
            sr = self.samplingrate()
            timerange = timedelta(seconds=sr*600)
        if not keys:
            keys = self._get_key_headers(numerical=True)
        if not threshold:
            threshold = 5.0

        cdate = datetime.utcnow().replace(tzinfo=None)
        sensorid = self.header.get('SensorID','')
        flaglist = []

        # Position of flag in flagstring
        # f (intensity): pos 0
        # x,y,z (vector): pos 1
        # other (vector): pos 2

        if not len(self.ndarray[0]) > 0:
            logger.info('flag_outlier: No ndarray - starting old remove_outlier method.')
            self = self.remove_outlier(keys=keys,threshold=threshold,timerange=timerange,stdout=stdout,markall=markall)
            return self

        logger.info('flag_outlier: Starting outlier identification...')

        flagidx = KEYLIST.index('flag')
        commentidx = KEYLIST.index('comment')
        if not len(self.ndarray[flagidx]) > 0:
            self.ndarray[flagidx] = [''] * len(self.ndarray[0])
        else:
            self.ndarray[flagidx] = self.ndarray[flagidx].astype(object)
        if not len(self.ndarray[commentidx]) > 0:
            self.ndarray[commentidx] = [''] * len(self.ndarray[0])
        else:
            self.ndarray[commentidx] = self.ndarray[commentidx].astype(object)

        # get a poslist of all keys - used for markall
        flagposls = [FLAGKEYLIST.index(key) for key in keys]
        # Start here with for key in keys:
        for key in keys:
            flagpos = FLAGKEYLIST.index(key)
            if not len(self.ndarray[flagpos]) > 0:
                print("Flag_outlier: No data for key %s - skipping" % key)
                break

            print ("-------------------------")
            print ("Dealing with key:", key)

            st = 0
            et = len(self.ndarray[0])
            incrt = int(timerange.total_seconds()/sr)
            if incrt == 0:
                print("Flag_outlier: check timerange ... seems to be smaller as sampling rate")
                break
            at = incrt

            while st < et:
                idxst = st
                idxat = at
                st = at
                at += incrt
                if idxat > et:
                    idxat = et

                #print key, idxst, idxat
                selcol = self.ndarray[flagpos][idxst:idxat].astype(float)
                selcol = selcol[~np.isnan(selcol)]

                if len(selcol) > 0:
                    try:
                        q1 = stats.scoreatpercentile(selcol,16)
                        q3 = stats.scoreatpercentile(selcol,84)
                        iqd = q3-q1
                        md = np.median(selcol)
                        if iqd == 0:
                            iqd = 0.000001
                        whisker = threshold*iqd
                        #print key, md, iqd, whisker
                    except:
                        try:
                            md = np.median(selcol)
                            whisker = md*0.005
                        except:
                            logger.warning("remove_outlier: Eliminate outliers produced a problem: please check.")
                            pass

                    #print md, whisker, np.asarray(selcol)
                    for elem in range(idxst,idxat):
                        #print flagpos, elem
                        if not md-whisker < self.ndarray[flagpos][elem] < md+whisker and not np.isnan(self.ndarray[flagpos][elem]):
                            #print "Found:", key, self.ndarray[flagpos][elem]
                            #if key == 'df':
                            #    x = 1/0
                            try:
                                if not self.ndarray[flagidx][elem] == '':
                                    #print "Got here", self.ndarray[flagidx][elem]
                                    newflagls = list(self.ndarray[flagidx][elem])
                                    #print newflagls
                                    if newflagls[flagpos] == '-':
                                        newflagls[flagpos] = 0
                                    if not int(newflagls[flagpos]) > 1:
                                        newflagls[flagpos] = '1'
                                    if markall:
                                        for p in flagposls:
                                            if not newflagls[p] > 1:
                                                newflagls[p] = '1'
                                    newflag = ''.join(newflagls)
                                else:
                                    x=1/0 # Force except
                            except:
                                newflagls = []
                                for idx,el in enumerate(FLAGKEYLIST): # Only key column
                                    if idx == flagpos:
                                        newflagls.append('1')
                                    else:
                                        newflagls.append('-')
                                if markall:
                                    for p in flagposls:
                                        newflagls[p] = '1'
                                newflag = ''.join(newflagls)

                            self.ndarray[flagidx][elem] = newflag
                            #print self.ndarray[flagidx][elem]
                            commline = "aof - threshold: {a}, window: {b} sec".format(a=str(threshold), b=str(timerange.total_seconds()))
                            self.ndarray[commentidx][elem] = commline
                            infoline = "flag_outlier: at {a} - removed {b} (= {c})".format(a=str(self.ndarray[0][elem]), b=key, c=self.ndarray[flagpos][elem])
                            logger.info(infoline)
                            #[starttime,endtime,key,flagid,flagcomment]
                            flagtime = self.ndarray[0][elem]

                            if markall:
                                # if not flagtime and key and commline in flaglist
                                for fkey in keys:
                                    ls = [flagtime,flagtime,fkey,1,commline]
                                    if not ls in flaglist:
                                        flaglist.append(ls)
                            else:
                                flaglist.append([flagtime,flagtime,key,1,commline])
                            if stdout:
                                print(infoline)
                        else:
                            try:
                                if not self.ndarray[flagidx][elem] == '':
                                    pass
                                else:
                                    x=1/0 # Not elegant but working
                            except:
                                self.ndarray[flagidx][elem] = ''
                                self.ndarray[commentidx][elem] = ''

        self.ndarray[flagidx] = np.asarray(self.ndarray[flagidx])
        self.ndarray[commentidx] = np.asarray(self.ndarray[commentidx])

        logger.info('flag_outlier: Outlier flagging finished.')

        ## METHOD WHICH SORTS/COMBINES THE FLAGLIST
        #print("flag_outlier",flaglist)
        # Combine subsequent time steps with identical flags to one flag range
        newlist = []
        srday = sr/(3600.*24.)

        # Keep it simple - no cleaning here - just produce new format
        if len(flaglist)>0:
            #flaglist = sorted(flaglist, key=lambda x: x[0])
            for line in flaglist: 
                newlist.append([num2date(line[0]).replace(tzinfo=None),num2date(line[1]).replace(tzinfo=None),line[2],line[3],line[4],sensorid,cdate])
        else:
            newlist = []

        #newlist = self.flaglistclean(newlist)

        """
        # requires a sorted list
        if len(flaglist)>0:
          # Different keys are not regarded for here (until 0.4.6)
          # 1. Extract all flag for individual keys first
          for key in keys:
            templist = [l for l in flaglist if l[2] == key]
            fllist = sorted(templist, key=lambda x: x[0])
            #flaglist = sorted(flaglist, key=lambda x: x[0])
            # Startvalue of endtime is firsttime
            etprev = fllist[0][1]
            prevline = fllist[0]
            for line in fllist:
                st = line[0]
                et = line[1]
                diff1 = (et-etprev)       # end time diff between current flag and last flag
                diff2 = (st-etprev)       # diff between current start and last end
                srunc = srday+0.01*srday  # sampling rate with uncertainty
                if diff1 < srunc or diff2 < srunc:
                    # subsequent time step found -> changing et in line
                    prevline[1] = et
                else:
                    newlist.append([num2date(prevline[0]).replace(tzinfo=None),num2date(prevline[1]).replace(tzinfo=None),prevline[2],prevline[3],prevline[4],sensorid,cdate])
                    prevline = line
                etprev = et
            #save current content of prevline with new et
            newlist.append([num2date(prevline[0]).replace(tzinfo=None),num2date(prevline[1]).replace(tzinfo=None),prevline[2],prevline[3],prevline[4],sensorid,cdate])
        else:
            newlist = []
        """

        if returnflaglist:
            return newlist

        return self

    def flag(self, flaglist, removeduplicates=False, debug=False):
        """
    DEFINITION:
        Apply flaglist to stream. A flaglist typically looks like:
        [starttime,endtime,key,flagid,flagcomment]
        starttime and endtime are provided as datetime objects
        key exists in KEYLIST
        flagid is a integer number between 0 and 4
        comment is a string of less then 100 characters

    PARAMETERS:
        - flaglist:             (list) as obtained by mpplots plotFlag, database db2flaglist

    RETURNS:
        - DataStream:   flagged version of stream.

    EXAMPLE:
        >>> flaglist = db.db2flaglist(db,sensorid_data)
        >>> data = data.flag(flaglist)
        """
        self.progress = 0

        # get time range of stream:
        st,et = self._find_t_limits()
        st = date2num(st)
        et = date2num(et)

        lenfl = len(flaglist)
        logger.info("Flag: Found flaglist of length {}".format(lenfl))
        flaglist = [line for line in flaglist if date2num(self._testtime(line[1])) >= st]
        flaglist = [line for line in flaglist if date2num(self._testtime(line[0])) <= et]
        # Sort flaglist accoring to startdate (used to speed up flagging procedure)
        flaglist.sort()

        ## Cleanup flaglist -- remove all inputs with duplicate start and endtime
        ## (use only last input)
        #print("1",flaglist)
        def flagclean(flaglist):
            ## Cleanup flaglist -- remove all inputs with duplicate start and endtime
            ## (use only last input)
            indicies = []
            for line in flaglist:
                inds = [ind for ind,elem in enumerate(flaglist) if elem[0] == line[0] and elem[1] == line[1] and elem[2] == line[2]]
                if len(inds) > 0:
                    index = inds[-1]
                    indicies.append(index)
            uniqueidx = (list(set(indicies)))
            uniqueidx.sort()
            #print(uniqueidx)
            flaglist = [elem for idx, elem in enumerate(flaglist) if idx in uniqueidx]

            return flaglist

        if removeduplicates:
            flaglist = flagclean(flaglist)

        lenfl = len(flaglist)
        logger.info("Flag: Relevant flags: {}".format(lenfl))

        ## Determinig sampling rate for nearby flagging
        sr = self.samplingrate()

        if lenfl > 0:
            for i in range(lenfl):
                self.progress = (float(i)/float(lenfl)*100.)
                if removeduplicates or debug or lenfl > 100:
                    if i == int(lenfl/5.):
                        print("Flag: 20 percent done")
                    if i == int(lenfl/5.*2.):
                        print("Flag: 40 percent done")
                    if i == int(lenfl/5.*3.):
                        print("Flag: 60 percent done")
                    if i == int(lenfl/5.*4.):
                        print("Flag: 80 percent done")
                fs = date2num(self._testtime(flaglist[i][0]))
                fe = date2num(self._testtime(flaglist[i][1]))
                if st < fs and et < fs and st < fe and et < fe:
                    pass
                elif  st > fs and et > fs and st > fe and et > fe:
                    pass
                else:
                    valid_chars='-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
                    flaglist[i][4] = ''.join([e for e in list(flaglist[i][4]) if e in list(valid_chars)])
                    keys = flaglist[i][2].split('_')
                    for key in keys:
                        self = self.flag_stream(key,int(flaglist[i][3]),flaglist[i][4],flaglist[i][0],flaglist[i][1],samplingrate = sr,debug=debug)

        return self

    def flagliststats(self,flaglist, intensive=False, output='stdout'):
        """
        DESCRIPTION:
            Provides some information on flag statistics
        PARAMETER:
            flaglist   (list) flaglist to be investigated
        APPLICTAION:
            flaglist = db2flaglist(db,'all')
            self.flagliststats(flaglist)
        """
        amountlist = []
        outputt = '##########################################\n'
        outputt += '           Flaglist statistics            \n'
        outputt += '##########################################\n'
        outputt += '\n'
        outputt += 'A) Total contents: {}\n'.format(len(flaglist))
        outputt += '\n'
        outputt += 'B) Content for each ID:\n'
        #print (flaglist[0], len(flaglist[0]))
        if len(flaglist[0]) > 6:
            ids = [el[5] for el in flaglist]
            uniquenames = list(set(ids))
        for name in uniquenames:
            amount = len([el[0] for el in flaglist if el[5] == name])
            amountlist.append([name,amount])
            if intensive:
                flagli = [el for el in flaglist if el[5] == name]
                index = [el[3] for el in flagli]
                uniqueindicies = list(set(index))
                reasons = [el[4] for el in flagli]
                uniquereasons = list(set(reasons))
                intensiveinfo = []
                for reason in uniquereasons:
                    num = len([el for el in flagli if reason == el[4]])
                    intensiveinfo.append([reason,num])
                intensiveinfo = sorted(intensiveinfo,key=lambda x: x[1])
                intensiveinfo = ["{} : {}\n".format(e[0],e[1]) for e in intensiveinfo]
                amountlist[-1].append(intensiveinfo)
        amountlist = sorted(amountlist,key=lambda x: x[1])
        for el in amountlist:
            outputt += "Dataset: {} \t Amount: {}\n".format(el[0],el[1])
            if intensive:
                for ele in el[2]:
                    outputt += "   {}".format(ele)
        if output=='stdout':
            print (outputt)
        return outputt

    def flaglistclean(self,flaglist):
        """
        DESCRIPTION:
            identify and remove duplicates from flaglist, only the latest inputs are used
            start, endtime and key are used to identfy duplicates
        PARAMETER:
            flaglist   (list) flaglist to be investigated
        APPLICTAION:
            stream = DataStream()
            flaglist = db2flaglist(db,'all')
            flaglistwithoutduplicates = stream.flaglistclean(flaglist)
        """

        # first step - remove all duplicates
        testflaglist = ['____'.join([str(date2num(elem[0])),str(date2num(elem[1])),str(elem[2]),str(elem[3]),str(elem[4]),str(elem[5]),str(date2num(elem[6]))]) for elem in flaglist]
        uniques,indi = np.unique(testflaglist,return_index=True)
        flaglist = [flaglist[idx] for idx in indi]

        # second step - remove all inputs without components
        flaglist = [elem for elem in flaglist if not elem[2] == '']

        ## Cleanup flaglist -- remove all inputs with duplicate start and endtime
        ## (use only last input)
        indicies = []
        for ti, line in enumerate(flaglist):
            if len(line) > 5:
                inds = [ind for ind,elem in enumerate(flaglist) if elem[0] == line[0] and elem[1] == line[1] and elem[2] == line[2] and elem[5] == line[5]]
            else:
                inds = [ind for ind,elem in enumerate(flaglist) if elem[0] == line[0] and elem[1] == line[1] and elem[2] == line[2]]
            if len(inds) > 1:
                # get inputs dates for all duplicates and select the latest
                dates = [[flaglist[dupind][-1], dupind] for dupind in inds]
                indicies.append(sorted(dates)[-1][1])
            else:
                index = inds[-1]
                indicies.append(index)

        uniqueidx = (list(set(indicies)))
        print ("flaglistclean: found {} unique inputs".format(len(uniqueidx)))
        uniqueidx.sort()
        flaglist = [flaglist[idx] for idx in uniqueidx]

        return flaglist


    def stream2flaglist(self, userange=True, flagnumber=None, keystoflag=None, sensorid=None, comment=None):
        """
        DESCRIPTION:
            Constructs a flaglist input dependent on the content of stream
        PARAMETER:
            comment    (key or string) if key (or comma separted list of keys) are
                       found, then the content of this column is used (first input
            flagnumber (int) integer number between 0 and 4
            userange   (bool) if False, each stream line results in a flag,
                              if True the full time range is marked

        """
        ### identify any given gaps and flag time ranges regarding gaps
        if not comment:
            print("stream2flag: you need to provide either a key or a text comment. (e.g. 'str1,str2' or 'Flagged'")
            return []
        if not flagnumber:
            flagnumber = 0
        if not keystoflag:
            print("stream2flag: you need to provide a list of keys to which you apply the flags (e.g. ['x','z']")
            return []
        if not sensorid:
            print("stream2flag: you need to provide a sensorid")
            return []

        commentarray = np.asarray([])
        uselist = False

        if comment in KEYLIST:
            pos = KEYLIST.index(comment)
            if userange:
                comment = self.ndarray[pos][0]
            else:
                uselist = True
                commentarray = self.ndarray[pos]
        else:
            lst,poslst = [],[]
            commentlist = comment.split(',')
            try:
                for commkey in commentlist:
                    if commkey in KEYLIST:
                        #print(commkey)
                        pos = KEYLIST.index(commkey)
                        if userange:
                            lst.append(str(self.ndarray[pos][0]))
                        else:
                            poslst.append(pos)
                    else:
                        # Throw exception
                        x= 1/0
                if userange:
                    comment = ' : '.join(lst)
                else:
                    uselist = True
                    resultarray = []
                    for pos in poslst:
                        resultarray.append(self.ndarray[pos])
                    resultarray = np.transpose(np.asarray(resultarray))
                    commentarray = [''.join(str(lst)) for lst in resultarray]
            except:
                #comment remains unchanged
                pass

        now = datetime.utcnow()
        res = []
        if userange:
            st = np.min(self.ndarray[0])
            et = np.max(self.ndarray[0])
            st = num2date(float(st)).replace(tzinfo=None)
            et = num2date(float(et)).replace(tzinfo=None)
            for key in keystoflag:
                res.append([st,et,key,flagnumber,comment,sensorid,now])
        else:
            for idx,st in enumerate(self.ndarray[0]):
                for key in keystoflag:
                    st = num2date(float(st)).replace(tzinfo=None)
                    if uselist:
                        res.append([st,st,key,flagnumber,commentarray[idx],sensorid,now])
                    else:
                        res.append([st,st,key,flagnumber,comment,sensorid,now])
        return res


    def flaglistmod(self, mode='select', flaglist=[], parameter='key', value=None, newvalue=None, starttime=None, endtime=None):
        """
        DEFINITION:
            Select/Replace/Delete information in flaglist
            parameters are key, flagnumber, comment, startdate, enddate=None
            mode delete: if only starttime and endtime are provided then all data inbetween is removed,
                         if parameter and value are provided this data is removed, eventuall
                         only between start and endtime
        APPLICTAION

        """
        num = 0
        # convert start and end to correct format
        if parameter == 'key':
            num = 2
        elif parameter == 'flagnumber':
            num = 3
        elif parameter == 'comment':
            num = 4
        elif parameter == 'sensorid':
            num = 5

        if mode in ['select','replace'] or (mode=='delete' and value):
            if starttime:
                starttime = self._testtime(starttime)
                flaglist = [elem for elem in flaglist if elem[1] > starttime]
            if endtime:
                endtime = self._testtime(endtime)
                flaglist = [elem for elem in flaglist if elem[0] < endtime]
        elif mode == 'delete' and not value:
            print ("Only deleting")
            flaglist1, flaglist2 = [],[]
            if starttime:
                starttime = self._testtime(starttime)
                flaglist1 = [elem for elem in flaglist if elem[1] < starttime]
            if endtime:
                endtime = self._testtime(endtime)
                flaglist2 = [elem for elem in flaglist if elem[0] > endtime]
            flaglist1.extend(flaglist2)
            flaglist = flaglist1

        if mode == 'select':
            if num>0 and value:
                if num == 4:
                    flaglist = [elem for elem in flaglist if elem[num].find(value) > 0]
                elif num == 3:
                    flaglist = [elem for elem in flaglist if elem[num] == int(value)]
                else:
                    flaglist = [elem for elem in flaglist if elem[num] == value]
        elif mode == 'replace':
            if num>0 and value:
                for idx, elem in enumerate(flaglist):
                    if num == 4:
                        if elem[num].find(value) >= 0:
                            flaglist[idx][num] = newvalue
                    elif num == 3:
                        if elem[num] == int(value):
                            flaglist[idx][num] = int(newvalue)
                    else:
                        if elem[num] == value:
                            flaglist[idx][num] = newvalue
        elif mode == 'delete':
            if num>0 and value:
                if num == 4:
                    flaglist = [elem for elem in flaglist if elem[num].find(value) < 0]
                elif num == 3:
                    flaglist = [elem for elem in flaglist if not elem[num] == int(value)]
                else:
                    flaglist = [elem for elem in flaglist if not elem[num] == value]

        return flaglist


    def flaglistadd(self, flaglist, sensorid, keys, flagnumber, comment, startdate, enddate=None):
        """
        DEFINITION:
            Add a specific input to a flaglist
            Flaglist elements look like
            [st,et,key,flagnumber,comment,sensorid,now]

        APPLICATION:
            newflaglist = stream.flaglistadd(oldflaglist,sensorid, keys, flagnumber, comment, startdate, enddate)
        """
        # convert start and end to correct format
        st = self._testtime(startdate)
        if enddate:
            et = self._testtime(enddate)
        else:
            et = st
        now = datetime.utcnow()
        if keys in ['all','All','ALL']:
            keys = KEYLIST
        for key in keys:
            flagelem = [st,et,key,flagnumber,comment,sensorid,now]
            exists = [elem for elem in flaglist if elem[:5] == flagelem[:5]]
            if len(exists) == 0:
                flaglist.append(flagelem)
            else:
                print ("flaglistadd: Flag already exists")
        return flaglist

    def flag_stream(self, key, flag, comment, startdate, enddate=None, samplingrate=0., debug=False):
        """
    DEFINITION:
        Add flags to specific times or time ranges (if enddate is provided).

    PARAMETERS:
    Variables:
        - key:          (str) Column to apply flag to, e.g. 'x'
        - flag:         (int) 0 ok, 1 remove, 2 force ok, 3 force remove,
                        4 merged from other instrument
        - comment:      (str) The reason for flag
        - startdate:    (datetime object) the date of the (first) datapoint to remove
    Kwargs:
        - enddate:      (datetime object) the enddate of a time range to be flagged
        - samplingrate: (float) in seconds, needs to be provided for effective nearby search

    RETURNS:
        - DataStream:   Input stream with flags and comments.

    EXAMPLE:
        >>> data = data.flag_stream('x',0,'Lawnmower',flag1,flag1_end)

    APPLICATION:
        """

        # TODO:
        # make flag_stream to accept keylists -> much faser for multiple column data

        sr = samplingrate

        if not key in KEYLIST:
            logger.error("flag_stream: %s is not a valid key." % key)
            return self
        if not flag in [0,1,2,3,4]:
            logger.error("flag_stream: %s is not a valid flag." % flag)
            return self

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True
        elif not len(self) > 0:
            return DataStream()

        startdate = self._testtime(startdate)

        if not enddate:
            # Set enddate to startdat
            # Hereby flag nearest might be used later
            enddate = startdate
            """
            start = date2num(startdate)
            check_startdate, val = self.findtime(start)
            if check_startdate == 0:
                logger.info("flag_stream: No data at given date for flag. Finding nearest data point.")
                if ndtype:
                    time = self.ndarray[0]
                else:
                    time = self._get_column('time')
                #print start, len(time)
                new_endtime, index = find_nearest(time, start)
                if new_endtime > start:
                    startdate = num2date(start)
                    enddate = num2date(new_endtime)
                else:
                    startdate = num2date(new_endtime)
                    enddate = num2date(start)
            else:
                enddate = startdate
            """
        else:
            enddate = self._testtime(enddate)

        ### ######## IF STARTDATE == ENDDATE
        ### MODIFYED TO STARTDATE-Samplingrate/3, ENDDATE + Samplingrate/3
        ### Taking 1/3 is arbitrary.
        ### This helps to apply flagging info to any higher resolution record
        ### which does not contain the exact time stamp.
        ### You are likely exclude more data then necessary.
        ### Flag the high resolution data set to avoid that.

        def rangeExtend(startdate,enddate,samplingrate,divisor=3):
            if startdate == enddate:
                startdate = startdate-timedelta(seconds=samplingrate/divisor)
                enddate = enddate+timedelta(seconds=samplingrate/divisor)
                start = date2num(startdate)
                end = date2num(enddate)
                return start,end
            else:
                start = date2num(startdate)
                end = date2num(enddate)
                return start,end

        pos = FLAGKEYLIST.index(key)

        if debug:
            print("Flag",startdate, enddate)

        start = date2num(startdate)
        end = date2num(enddate)
        mint = np.min(self.ndarray[0])
        maxt = np.max(self.ndarray[0])

        if start < mint and end < mint:
            st = 0
            ed = 0
        elif start > maxt and end > maxt:
            st = 0
            ed = 0
        else:
            ### Modified to use nearest value to be flagged if flagtimes
            ### overlap with streams timerange
            ### find_nearest is probably very slowly...
            ### Using startidx values to speed up the process at least for later data
            # Get start and end indicies:
            if debug:
                ti1 = datetime.utcnow()
            st, ls = self.findtime(startdate,mode='argmax')
            # st is the starttime, ls ?   -- modification allow to provide key list!!
            if debug:
                ti2 = datetime.utcnow()
                print ("Findtime duration", ti2-ti1)

            #if debug:
            #    ti1 = datetime.utcnow()
            #    testls = nonzero(self.ndarray[0]==startdate)
            #    ti2 = datetime.utcnow()
            #    print ("Findtime duration -alternative", ti2-ti1)

            if st == 0:
                #print("Flag_stream: slowly start",st)
                if not sr == 0:
                    # Determine sampling rate if not done yet
                    start,end = rangeExtend(startdate,enddate,sr)
                    ls,st = find_nearest(self.ndarray[0],start)
            sti = st-2
            if sti < 0:
                sti = 0
            ed, le = self.findtime(enddate,startidx=sti,mode='argmax')
            if ed == 0:
                #print("Flag_stream: slowly end",ed)
                if not sr == 0:
                    # Determine sampling rate if not done yet
                    start,end = rangeExtend(startdate,enddate,sr)
                    le, ed = find_nearest(self.ndarray[0],end) ### TODO use startundex here as well
            if ed == len(self.ndarray[0]):
                ed = ed-1
            # Create a defaultflag
            defaultflag = ['-' for el in FLAGKEYLIST]
            if debug:
                ti3 = datetime.utcnow()
                print ("Full Findtime duration", ti3-ti1)
                print("flagging", st, ed)

        if ndtype:
            array = [[] for el in KEYLIST]
            flagind = KEYLIST.index('flag')
            commentind = KEYLIST.index('comment')
            # Check whether flag and comment are exisiting - if not create empty
            if not len(self.ndarray[flagind]) > 0:
                array[flagind] = [''] * len(self.ndarray[0])
            else:
                array[flagind] = list(self.ndarray[flagind])
            if not len(self.ndarray[commentind]) > 0:
                array[commentind] = [''] * len(self.ndarray[0])
            else:
                array[commentind] = list(self.ndarray[commentind])
            # Now either modify existing or add new flag

            if st==0 and ed==0:
                pass
            else:
                t3a = datetime.utcnow()
                for i in range(st,ed+1):
                    #if self.ndarray[flagind][i] == '' or self.ndarray[flagind][i] == '-':
                    if array[flagind][i] == '' or array[flagind][i] == '-':
                        flagls = defaultflag
                    else:
                        flagls = list(array[flagind][i])
                    # if existing flaglistlength is shorter, because new columns where added later to ndarray
                    if len(flagls) < pos:
                        flagls.extend(['-' for j in range(pos+1-flagls)])
                    flagls[pos] = str(flag)
                    array[flagind][i] = ''.join(flagls)
                    array[commentind][i] = comment

            self.ndarray[flagind] = np.array(array[flagind], dtype=np.object)
            self.ndarray[commentind] = np.array(array[commentind], dtype=np.object)

            # up to 0.3.98 the following code was used (~10 times slower)
            # further significant speed up requires some structural changes: 
            #   1. use keylist here
            #self.ndarray[flagind] = np.asarray(array[flagind]).astype(object)
            #self.ndarray[commentind] = np.asarray(array[commentind]).astype(object)

        else:
            for elem in self:
                if elem.time >= start and elem.time <= end:
                    fllist = list(elem.flag)
                    if not len(fllist) > 1:
                        fllist = defaultflag
                    fllist[pos] = str(flag)
                    elem.flag=''.join(fllist)
                    elem.comment = comment
        
        if flag == 1 or flag == 3 and debug:
            if enddate:
                #print ("flag_stream: Flagged data from %s to %s -> (%s)" % (startdate.isoformat(),enddate.isoformat(),comment))
                try:
                    logger.info("flag_stream: Flagged data from %s to %s -> (%s)" % (startdate.isoformat().encode('ascii','ignore'),enddate.isoformat().encode('ascii','ignore'),comment.encode('ascii','ignore')))
                except:
                    pass
            else:
                try:
                    logger.info("flag_stream: Flagged data at %s -> (%s)" % (startdate.isoformat().encode('ascii','ignore'),comment.encode('ascii','ignore')))
                except:
                    pass

        return self

    def simplebasevalue2stream(self,basevalue,**kwargs):
        """
      DESCRIPTION:
        simple baselvalue correction using a simple basevalue list

      PARAMETERS:
        basevalue       (list): [baseH,baseD,baseZ]
        keys            (list): default = 'x','y','z'

      APPLICTAION:
        used by stream.baseline

        """
        mode = kwargs.get('mode')
        keys = ['x','y','z']

        # Changed that - 49 sec before, no less then 2 secs
        if not len(self.ndarray[0]) > 0:
            print("simplebasevalue2stream: requires ndarray")
            return self

        #1. calculate function value for each data time step
        array = [[] for key in KEYLIST]
        array[0] = self.ndarray[0]
        # get x array for baseline
        #indx = KEYLIST.index('x')
        for key in KEYLIST:
            ind = KEYLIST.index(key)
            if key in keys: # new
                #print keys.index(key)
                ar = self.ndarray[ind].astype(float)
                if key == 'y':
                    #indx = KEYLIST.index('x')
                    #Hv + Hb;   Db + atan2(y,H_corr)    Zb + Zv
                    #print type(self.ndarray[ind]), key, self.ndarray[ind]
                    array[ind] = np.arctan2(np.asarray(list(ar)),np.asarray(list(arrayx)))*180./np.pi + basevalue[keys.index(key)]
                    self.header['col-y'] = 'd'
                    self.header['unit-col-y'] = 'deg'
                else:
                    array[ind] = ar + basevalue[keys.index(key)]
                    if key == 'x': # remember this for correct y determination
                        arrayx = array[ind]
            else: # new
                if len(self.ndarray[ind]) > 0:
                    array[ind] = self.ndarray[ind].astype(object)

        self.header['DataComponents'] = 'HDZ'
        return DataStream(self,self.header,np.asarray(array))


    def func2stream(self,funclist,**kwargs):
        """
      DESCRIPTION:
        combine data stream and functions obtained by fitting and interpolation. Possible combination
        modes are 'add' (default), subtract 'sub', divide 'div' and 'multiply'. Furthermore, the
        function values can replace the original values at the given timesteps of the stream

      PARAMETERS:
        funclist        (list of functions): required - each function is an output of stream.fit or stream.interpol
        #function        (function): required - output of stream.fit or stream.interpol
        keys            (list): default = 'x','y','z'
        mode            (string): one of 'add','sub','div','multiply','values' - default = 'add'

      APPLICTAION:
        used by stream.baseline

        """
        keys = kwargs.get('keys')
        fkeys = kwargs.get('fkeys')
        mode = kwargs.get('mode')
        if not keys:
            keys = ['x','y','z']
        if not mode:
            mode = 'add'
        if fkeys and not len(fkeys) == len(keys):
            fkeys=None
            logger.warning("func2stream: provided fkeys do not match keys")

        if isinstance(funclist[0], dict):
            funct = [funclist]
        else:
            funct = funclist   # TODO: cycle through list

        totalarray = [[] for key in KEYLIST]
        posstr = KEYLIST.index('str1')
        testx = []

        for function in funct:
            if not function:
                return self
            # Changed that - 49 sec before, no less then 2 secs
            if not len(self.ndarray[0]) > 0:
                print("func2stream: requires ndarray - trying old LineStruct functions")
                if mode == 'add':
                    return self.func_add(function, keys=keys)
                elif mode == 'sub':
                    return self.func_subtract(function, keys=keys)
                else:
                    return self

            #1. calculate function value for each data time step
            array = [[] for key in KEYLIST]
            array[0] = self.ndarray[0]
            dis_done = False
            # get x array for baseline
            #indx = KEYLIST.index('x')
            #arrayx = self.ndarray[indx].astype(float)
            functimearray = (self.ndarray[0].astype(float)-function[1])/(function[2]-function[1])
            #print functimearray
            validkey = False
            for key in KEYLIST:
                ind = KEYLIST.index(key)
                if key in keys: # new
                    keyind = keys.index(key)
                    if fkeys:
                        fkey = fkeys[keyind]
                    else:
                        fkey = key
                    ar = np.asarray(self.ndarray[ind]).astype(float)
                    validkey = True
                    #try:
                    #    test = function[0]['f'+key](functimearray)
                    #    validkey = True
                    #except:
                    #    validkey = False
                    #    array[ind] = ar
                    if mode == 'add' and validkey:
                        print ("here", ar, function[0]['f'+fkey](functimearray))
                        array[ind] = ar + function[0]['f'+fkey](functimearray)
                    elif mode == 'addbaseline' and validkey:
                        if key == 'y':
                            #indx = KEYLIST.index('x')
                            #Hv + Hb;   Db + atan2(y,H_corr)    Zb + Zv
                            #print type(self.ndarray[ind]), key, self.ndarray[ind]
                            array[ind] = np.arctan2(np.asarray(list(ar)),np.asarray(list(arrayx)))*180./np.pi + function[0]['f'+fkey](functimearray)
                            self.header['col-y'] = 'd'
                            self.header['unit-col-y'] = 'deg'
                        else:
                            #print("func2stream", function, function[0], function[0]['f'+key],functimearray)
                            array[ind] = ar + function[0]['f'+fkey](functimearray)
                            if len(array[posstr]) == 0:
                                #print ("Assigned values to str1: function {}".format(function[1]))
                                array[posstr] = ['c']*len(ar)
                            if len(testx) > 0 and not dis_done:
                                # identify change from number to nan
                                # add discontinuity marker there
                                #print ("Here", testx)
                                prevel = np.nan
                                for idx, el in enumerate(testx):
                                    if not np.isnan(prevel) and np.isnan(el):
                                        array[posstr][idx] = 'd'
                                        #print ("Modified str1 at {}".format(idx))
                                        break
                                    prevel = el
                                dis_done = True
                            if key == 'x': # remember this for correct y determination
                                arrayx = array[ind]
                                testx = function[0]['f'+fkey](functimearray)
                            if key == 'dx': # use this column to test if delta values are already provided
                                testx = function[0]['f'+fkey](functimearray)
                    elif mode in ['sub','subtract'] and validkey:
                        array[ind] = ar - function[0]['f'+fkey](functimearray)
                    elif mode == 'values' and validkey:
                        array[ind] = function[0]['f'+fkey](functimearray)
                    elif mode == 'div' and validkey:
                        array[ind] = ar / function[0]['f'+fkey](functimearray)
                    elif mode == 'multiply' and validkey:
                        array[ind] = ar * function[0]['f'+fkey](functimearray)
                    else:
                        print("func2stream: mode not recognized")
                else: # new
                    if len(self.ndarray[ind]) > 0:
                        array[ind] = np.asarray(self.ndarray[ind]).astype(object)

            #print ("Check", array[posstr])
            for idx, col in enumerate(array):
                if len(totalarray[idx]) > 0 and not idx == 0:
                    totalcol = totalarray[idx]
                    for j,el in enumerate(col):
                        if idx < len(NUMKEYLIST)+1 and not np.isnan(el) and np.isnan(totalcol[j]):
                            totalarray[idx][j] = array[idx][j]
                        if idx > len(NUMKEYLIST) and not el == 'c' and totalcol[j] == 'c':
                            totalarray[idx][j] = 'd'
                else:
                    totalarray[idx] = array[idx]

        return DataStream(self,self.header,np.asarray(totalarray))


    def func_add(self,funclist,**kwargs):
        """
        Add a function to the selected values of the data stream -> e.g. get baseline
        Optional:
        keys (default = 'x','y','z')
        """
        keys = kwargs.get('keys')
        mode = kwargs.get('mode')
        if not keys:
            keys = ['x','y','z']
        if not mode:
            mode = 'add'

        if isinstance(funclist[0], dict):
            funct = [funclist]
        else:
            funct = funclist
        function = funct[0]  # Direct call of old version only accepts single function

        # Changed that - 49 sec before, no less then 2 secs
        if len(self.ndarray[0]) > 0:
            #1. calculate function value for each data time step
            array = [[] for key in KEYLIST]
            array[0] = self.ndarray[0]
            functimearray = (self.ndarray[0].astype(float)-function[1])/(function[2]-function[1])
            #print functimearray
            for key in keys:
                ind = KEYLIST.index(key)
                if mode == 'add':
                    array[ind] = self.ndarray[ind] + function[0]['f'+key](functimearray)
                elif mode == 'sub':
                    array[ind] = self.ndarray[ind] - function[0]['f'+key](functimearray)
                elif mode == 'values':
                    array[ind] = function[0]['f'+key](functimearray)
                elif mode == 'div':
                    array[ind] = self.ndarray[ind] / function[0]['f'+key](functimearray)
                elif mode == 'multiply':
                    array[ind] = self.ndarray[ind] * function[0]['f'+key](functimearray)
                else:
                    print("func2stream: mode not recognized")

            return DataStream(self,self.header,np.asarray(array))

        for elem in self:
            # check whether time step is in function range
            if function[1] <= elem.time <= function[2]:
                functime = (elem.time-function[1])/(function[2]-function[1])
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        raise ValueError("Column key not valid")
                    fkey = 'f'+key
                    exec('keyval = elem.'+key)
                    if fkey in function[0] and not isnan(keyval):
                        try:
                            newval = keyval + function[0][fkey](functime)
                        except:
                            newval = float('nan')
                        exec('elem.'+key+' = newval')
                    else:
                        pass
            else:
                pass

        return self


    def func_subtract(self,funclist,**kwargs):
        """
        Subtract a function from the selected values of the data stream -> e.g. obtain Residuals
        Optional:
        keys (default = 'x','y','z')
        :type order int
        :param order : 0 -> stream - function; 1 -> function - stream
        """
        keys = kwargs.get('keys')
        order = kwargs.get('order')

        st = DataStream()
        st = self.copy()

        if isinstance(funclist[0], dict):
            funct = [funclist]
        else:
            funct = funclist
        function = funct[0]  # Direct call of old version only accepts single function

        """
        for el in self:
            li = LineStruct()
            li.time = el.time
            li.x = el.x
            li.y = el.y
            li.z = el.z
            st.add(li)
        """
        if not order:
            order = 0

        if not keys:
            keys = ['x','y','z']

        for elem in st:
            # check whether time step is in function range
            if function[1] <= elem.time <= function[2]:
                functime = (elem.time-function[1])/(function[2]-function[1])
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        raise ValueError("Column key not valid")
                    fkey = 'f'+key
                    exec('keyval = elem.'+key)
                    if fkey in function[0] and not isnan(keyval):
                        try:
                            if order == 0:
                                newval = keyval - function[0][fkey](functime)
                            else:
                                newval = function[0][fkey](functime) - keyval
                        except:
                            newval = float('nan')
                        exec('elem.'+key+' = newval')
                    else:
                        pass
            else:
                pass

        return st


    def get_gaps(self, **kwargs):
        """
    DEFINITION:
        Takes the dominant sample frequency and fills nan into non-existing time steps:
        This function provides the basis for discontinuous plots and gap analysis and proper filtering.

    PARAMETERS:
    Variables:
        ---
    Kwargs:
        - accuracy:     (float) time relative to a day - default 1 sec
        - gapvariable:  (string) - refering to stream column - default='var5' - This column
                         is overwritten with 0 (data) and 1 (no data).
        - key:          (string) - refering to a data column e.g. key='x'. If given then all NaN values with existing time steps are also marked by '1' in the gapvariable line for this key

    RETURNS:
        - stream:       (Datastream)

    EXAMPLE:
        >>> stream_with_gaps_filled = stream_with_aps.get_gaps(['f'])

    APPLICATION:
        used by nfilter() for correct filtering
    CHANGES:
        Last updated and tested with nfilter function by leon 2014-07-22
        """

        accuracy = kwargs.get('accuracy')
        key = kwargs.get('key')
        gapvariable = kwargs.get('gapvariable')
        debug = kwargs.get('debug')

        if key in KEYLIST:
            gapvariable = True

        if not gapvariable:
            gapvariable = 'var5'

        if not self.length()[0] > 1:
            print ("get_gaps: Stream does not contain data - aborting")
            return self

        # Better use get_sampling period as samplingrate is rounded
        #spr = self.get_sampling_period()
        #newsps = newsp*3600.0*24.0
        newsps = self.samplingrate()
        newsp = newsps/3600.0/24.0

        if not accuracy:
            accuracy = 0.9/(3600.0*24.0) # one second relative to day
            accuracy = 0.05*newsp # 5 percent of samplingrate

        if newsps < 0.9 and not accuracy:
            accuracy = (newsps-(newsps*0.1))/(3600.0*24.0)

        logger.info('--- Starting filling gaps with NANs at %s ' % (str(datetime.now())))

        stream = self.copy()
        prevtime = 0

        ndtype = False
        if len(stream.ndarray[0]) > 0:
            maxtime = stream.ndarray[0][-1]
            mintime = stream.ndarray[0][0]
            length = len(stream.ndarray[0])
            sourcetime = stream.ndarray[0]
            ndtype = True
        else:
            mintime = self[0].time
            maxtime = self[-1].time

        if debug:
            print("Time range:", mintime, maxtime)
            print("Length, samp_per and accuracy:", self.length()[0], newsps, accuracy)

        shift = 0
        if ndtype:
            # Get time diff and expected count
            timediff = maxtime - mintime
            expN = int(round(timediff/newsp))+1
            if debug:
                print("Expected length vs actual length:", expN, length)
            if expN == len(sourcetime):
                # Found the expected amount of time steps - no gaps
                logger.info("get_gaps: No gaps found - Returning")
                return stream
            else:
                # correct way (will be used by default) - does not use any accuracy value
                #projtime = np.linspace(mintime, maxtime, num=expN, endpoint=True)
                #print("proj:", projtime, len(projtime))
                # find values or projtime, which are not in sourcetime
                #dif = setdiff1d(projtime,sourcetime, assume_unique=True)
                #print (dif, len(dif))
                #print (len(dif),len(sourcetime),len(projtime))
                diff = sourcetime[1:] - sourcetime[:-1]
                num_fills = np.round(diff / newsp) - 1
                getdiffids = np.where(diff > newsp+accuracy)[0]
                logger.info("get_gaps: Found gaps - Filling nans to them")
                if debug:
                    print ("Here", diff, num_fills, newsp, getdiffids)
                missingt = []
                # Get critical differences and number of missing steps
                for i in getdiffids:
                    #print (i,  sourcetime[i-1], sourcetime[i], sourcetime[i+1])
                    nf = num_fills[i]
                    # if nf is larger than zero then get append the missing time steps to missingt list
                    if nf > 0:
                        for n in range(int(nf)): # add n+1 * samplingrate for each missing value
                            missingt.append(sourcetime[i]+(n+1)*newsp)

                print ("Filling {} gaps".format(len(missingt)))

                # Cycle through stream and append nans to each column for missing time steps
                nans = [np.nan] * len(missingt)
                empts = [''] * len(missingt)
                gaps = [0.0] * len(missingt)
                for idx,elem in enumerate(stream.ndarray):
                    if idx == 0:
                        # append missingt list to array element
                        elem = list(elem)
                        lenelem = len(elem)
                        elem.extend(missingt)
                        stream.ndarray[idx] = np.asarray(elem).astype(object)
                    elif len(elem) > 0:
                        # append nans list to array element
                        elem = list(elem)
                        if KEYLIST[idx] in NUMKEYLIST or KEYLIST[idx] == 'sectime':
                            elem.extend(nans)
                        else:
                            elem.extend(empts)
                        stream.ndarray[idx] = np.asarray(elem).astype(object)
                    elif KEYLIST[idx] == gapvariable:
                        # append nans list to array element
                        elem = [1.0]*lenelem
                        elem.extend(gaps)
                        stream.ndarray[idx] = np.asarray(elem).astype(object)
            return stream.sorting()

        else:
            stream = DataStream()
            for elem in self:
                if abs((prevtime+newsp) - elem.time) > accuracy and not prevtime == 0:
                    currtime = num2date(prevtime)+timedelta(seconds=newsps)
                    while currtime <= num2date(elem.time):
                        newline = LineStruct()
                        exec('newline.'+gapvariable+' = 1.0')
                        newline.time = date2num(currtime)
                        stream.add(newline)
                        currtime += timedelta(seconds=newsps)
                else:
                    exec('elem.'+gapvariable+' = 0.0')
                    if key in KEYLIST:
                        if isnan(eval('elem.'+key)):
                            exec('elem.'+gapvariable+' = 1.0')
                    stream.add(elem)
                prevtime = elem.time

        logger.info('--- Filling gaps finished at %s ' % (str(datetime.now())))
        if debugmode:
            print("Ending:", stream[0].time, stream[-1].time)

        return stream.sorting()


    def get_rotationangle(self, xcompensation=0,keys=['x','y','z'],**kwargs):
        """
        DESCRIPTION:
            "Estimating" the rotation angle towards a magnetic coordinate system
            assuming z to be vertical down. Please note: You need to provide a
            complete horizontal vector including either the x compensation field
            or if not available an annual estimate of the vector. This method can be used
            to determine reorientation characteristics in order to accurately apply
            HDZ optimzed basevalue calculations.
        RETURNS:
            rotangle   (float) The estimated rotation angle in degree
        """
        annualmeans = kwargs.get('annualmeans')

        #1. get vector from data
        # x = y*tan(dec)
        if not keys:
            keys = ['x','y','z']

        if not len(keys) == 3:
            logger.error('get_rotation: provided keylist need to have three components.')
            return stream #self

        logger.info('get_rotation: Determining rotation angle towards a magnetic coordinate system assuming z to be vertical down.')

        ind1 = KEYLIST.index(keys[0])
        ind2 = KEYLIST.index(keys[1])
        ind3 = KEYLIST.index(keys[2])

        if len(self.ndarray[0]) > 0:
            if len(self.ndarray[ind1]) > 0 and len(self.ndarray[ind2]) > 0 and len(self.ndarray[ind3]) > 0:
                # get mean disregarding nans
                xl = [el for el in self.ndarray[ind1] if not np.isnan(el)]
                yl = [el for el in self.ndarray[ind2] if not np.isnan(el)]
                if annualmeans:
                    meanx = annualmeans[0]
                else:
                    meanx = np.mean(xl)+xcompensation
                meany = np.mean(yl)
                # get rotation angle so that meany == 0
                print ("Rotation",meanx, meany)
                #zeroy = meanx*np.sin(ra)+meany*np.cos(ra)
                #-meany/meanx = np.tan(ra)
                rotangle = np.arctan2(-meany,meanx) * (180.) / np.pi

        logger.info('getrotation: Rotation angle determined: {} deg'.format(rotangle))

        return rotangle


    def get_sampling_period(self):
        """
        returns the dominant sampling frequency in unit ! days !

        for time savings, this function only tests the first 1000 elements
        """

        # For proper applictation - duplicates are removed
        self = self.removeduplicates()

        if len(self.ndarray[0]) > 0:
            timecol = self.ndarray[0].astype(float)
        else:
            timecol= self._get_column('time')

        # New way:
        if len(timecol) > 1:
            diffs = np.asarray(timecol[1:]-timecol[:-1])
            diffs = diffs[~np.isnan(diffs)]
            me = np.median(diffs)
            st = np.std(diffs)
            diffs = [el for el in diffs if el <= me+2*st and el >= me-2*st]
            return np.median(diffs)
        else:
            return 0.0

        """
        timedifflist = [[0,0]]
        timediff = 0
        if len(timecol) <= 1000:
            testrange = len(timecol)
        else:
            testrange = 1000

        print "Get_sampling_rate", np.asarray(timecol[1:]-timecol[:-1])
        print "Get_sampling_rate", np.median(np.asarray(timecol[1:]-timecol[:-1]))*3600.*24.


        for idx, val in enumerate(timecol[:testrange]):
            if idx > 1 and not isnan(val):
                timediff = np.round((val-timeprev),7)
                found = 0
                for tel in timedifflist:
                    if tel[1] == timediff:
                        tel[0] = tel[0]+1
                        found = 1
                if found == 0:
                    timedifflist.append([1,timediff])
            timeprev = val

        #print self
        if not len(timedifflist) == 0:
            timedifflist.sort(key=lambda x: int(x[0]))
            # get the most often found timediff
            domtd = timedifflist[-1][1]
        else:
            logger.error("get_sampling_period: unkown problem - returning 0")
            domtd = 0

        if not domtd == 0:
            return domtd
        else:
            try:
                return timedifflist[-2][1]
            except:
                logger.error("get_sampling_period: could not identify dominant sampling rate")
                return 0
        """

    def samplingrate(self, **kwargs):
        """
        DEFINITION:
            returns a rounded value of the sampling rate
            in seconds
            and updates the header information
        """
        # XXX include that in the stream reading process....
        digits = kwargs.get('digits')
        notrounded = kwargs.get('notrounded')

        if not digits:
            digits = 1

        if not self.length()[0] > 1:
            return 0.0

        sr = self.get_sampling_period()*24*3600
        unit = ' sec'

        val = sr

        # Create a suitable rounding function:
        # Use simple rounds if sr > 60 secs
        # Check accuracy for sr < 10 secs (three digits:
        #       if abs(sr-round(sr,0)) * 1000 e.g. (1.002 -> 2, 0.998 -> 2)
        if sr < 0.05:
            for i in range(0,5):
                multi = 10**i
                srfloor = np.floor(sr*multi)
                if srfloor >= 1:
                    # found multiplicator
                    # now determine significance taking into account three more digits
                    digs = np.floor(np.abs(sr*multi-srfloor)*1000)
                    if digs<5: # round to zero
                        val = np.round(srfloor/multi,1)
                    else:
                        val = np.round(sr,5)
                    break
        elif sr < 59:
            for i in range(0,3):
                multi = 10**i
                srfloor = np.floor(sr*multi)
                if srfloor >= 1:
                    # found multiplicator
                    # now determine significance taking into account three more digits
                    digs = np.floor(np.abs(sr*multi-srfloor)*1000)
                    if digs<5: # round to zero
                        val = np.round(srfloor/multi,1)
                    else:
                        val = np.round(sr,3)
                    break
        else:
            val = np.round(sr,1)
        """
        if np.round(sr*10.,0) == 0:
            val = np.round(sr,2)
            #unit = ' Hz'
        elif np.round(sr,0) == 0:
            if 0.09 < sr < 0.11:
                val = np.round(sr,digits)
            else:
                val = np.round(sr,2)
                #unit = ' Hz'
        else:
            val = np.round(sr,0)
        """
        if notrounded:
            val = sr

        self.header['DataSamplingRate'] = str(val) + unit

        return val

    def integrate(self, **kwargs):
        """
        DESCRIPTION:
            Method to integrate selected columns respect to time.
            -- Using scipy.integrate.cumtrapz
        VARIABLES:
            optional:
            keys: (list - default ['x','y','z','f'] provide limited key-list
        """


        logger.info('--- Integrating started at %s ' % str(datetime.now()))

        keys = kwargs.get('keys')
        if not keys:
            keys = ['x','y','z']

        array = [[] for key in KEYLIST]
        ndtype = False
        if len(self.ndarray[0])>0:
            ndtype = True
            t = self.ndarray[0]
            array[0] = t
        else:
            t = self._get_column('time')
        for key in keys:
            if ndtype:
                ind = KEYLIST.index(key)
                val = self.ndarray[ind]
                array[ind] = np.asarray(val)
            else:
                val = self._get_column(key)
            dval = sp.integrate.cumtrapz(np.asarray(val),t)
            dval = np.insert(dval, 0, 0) # Prepend 0 to maintain original length
            if ndtype:
                ind = KEYLIST.index('d'+key)
                array[ind] = np.asarray(dval)
            else:
                self._put_column(dval, 'd'+key)

        self.ndarray = np.asarray(array)
        logger.info('--- integration finished at %s ' % str(datetime.now()))
        return self


    def interpol(self, keys, **kwargs):
        """
    DEFINITION:
        Uses Numpy interpolate.interp1d to interpolate streams.

    PARAMETERS:
    Variables:
        - keys:         (list) List of keys to interpolate.
    Kwargs:
        - kind:         (str) type of interpolation. Options:
                        linear = linear - Default
                        slinear = spline (first order)
                        quadratic = spline (second order)
                        cubic = spline (third order)
                        nearest = ?
                        zero = ?
        (TODO: add these?)
        - timerange:    (timedelta object) default=timedelta(hours=1).
        - fitdegree:    (float) default=4.
        - knotstep:     (float < 0.5) determines the amount of knots:
                        amount = 1/knotstep ---> VERY smooth 0.1 | NOT VERY SMOOTH 0.001

    RETURNS:
        - func:         (list) Contains the following:
                        list[0]:        (dict) {'f+key': interpolate function}
                        list[1]:        (float) date2num value of minimum timestamp
                        list[2]:        (float) date2num value of maximum timestamp

    EXAMPLE:
        >>> int_data = pos_data.interpol(['f'])

    APPLICATION:
        """

        kind = kwargs.get('kind')

        if not kind:
            kind = 'linear'

        if kind not in ['linear','slinear','quadratic','cubic','nearest','zero']:
            logger.warning("interpol: Interpolation kind %s not valid. Using linear interpolation instead." % kind)
            kind = 'linear'

        ndtype = False
        if len(self.ndarray[0]) > 0:
            t = self.ndarray[0]
            ndtype = True
        else:
            t = self._get_column('time')
        nt,sv,ev = self._normalize(t)
        sp = self.get_sampling_period()
        functionkeylist = {}

        logger.info("interpol: Interpolating stream with %s interpolation." % kind)

        for key in keys:
            if not key in NUMKEYLIST:
                logger.error("interpol: Column key not valid!")
            if ndtype:
                ind = KEYLIST.index(key)
                val = self.ndarray[ind].astype(float)
            else:
                val = self._get_column(key)
            # interplolate NaN values
            nans, xxx= nan_helper(val)
            try: # Try to interpolate nan values
                val[nans]= np.interp(xxx(nans), xxx(~nans), val[~nans])
            except:
                #val[nans]=int(nan)
                pass
            if len(val)>1:
                exec('f'+key+' = interpolate.interp1d(nt, val, kind)')
                exec('functionkeylist["f'+key+'"] = f'+key)
            else:
                logger.warning("interpol: interpolation of zero length data set - wont work.")
                pass

        logger.info("interpol: Interpolation complete.")

        func = [functionkeylist, sv, ev]

        return func
    
    
    def interpolate_nans(self, keys):
        """"
    DEFINITION: 
        Provides a simple linear nan interpolator that returns the interpolated
        data in the stream. Uses method that is already present elsewhere, e.g.
        in filter, for easy and quick access.
    
    PARAMETERS:
        - keys:         List of keys to interpolate.
        
    RETURNS:
        - stream:       Original stream with nans replaced by linear interpolation.
        """
    
        for key in keys:
            if key not in NUMKEYLIST:
                logger.error("interpolate_nans: {} is an invalid key! Cannot interpolate.".format(key))
            y = self._get_column(key)
            nans, x = nan_helper(y)
            y[nans] = np.interp(x(nans), x(~nans), y[~nans])
            self._put_column(y, key)
            logger.info("interpolate_nans: Replaced nans in {} with linearly interpolated values.".format(key))
        return self
    

    def k_extend(self, **kwargs):
        """
      DESCRIPTION:
        Extending the k_scale from 9 to 28 values as used for the GFZ kp value
        """
        k9_level = kwargs.get('k9_level')

        if not k9_level:
            if 'StationK9' in self.header:
                # 1. Check header info
                k9_level = self.header['StationK9']
            else:
                # 2. Set Potsdam default
                k9_level = 500
        fortscale = [0,7.5,15,30,60,105,180,300,495,750]
        k_scale = [float(k9_level)*elem/750.0 for elem in fortscale]

        newlst = []
        klst = [0.,0.33,0.66,1.,1.33,1.66,2.,2.33,2.66,3.,3.33,3.66,4.,4.33,4.66,5.,5.33,5.66,6.,6.33,6.66,7.,7.33,7.66,8.,8.33,8.66,9.]
        for idx,elem in enumerate(k_scale):
            if idx > 0:
                diff = elem - k_scale[idx-1]
                newlst.append(elem-2*diff/3)
                newlst.append(elem-diff/3)
            newlst.append(elem)

        indvar1 = KEYLIST.index('var1')
        indvar2 = KEYLIST.index('var2')
        ar = []
        for elem in self.ndarray[indvar2]:
            for count,val in enumerate(newlst):
                if elem > val:
                    k = klst[count]
            ar.append(k)
        self.ndarray[indvar1] = np.asarray(ar)

        return self


    def k_fmi(self, **kwargs):
        """
      DESCRIPTION:
        Calculating k values following the fmi approach. The method uses three major steps:
        Firstly, the record is eventually filtered to minute data, outliers are removed
        (using default options) and gaps are interpolated. Ideally, these steps have been
        contucted before, which allows for complete control of these steps.
        Secondly, the last 27 hours are investigated. Starting from the last record, the last
        three hour segment is taken and the fmi approach is applied. Finally, the provided
        stream is analyzed from the beginning. Definite values are thus produced for the
        previous day after 3:00 am (depending on n - see below).
        The FMI method:
        The provided data stream is checked and converted to xyz data. Investigated are the
        horizontal components. In a first run k values are calculated by simply determining
        the max/min difference of the minute variation data within the three hour segements.
        This is done for both horizontal components and the maximum difference is selected.
        Using the transformation table related to the Niemegk scale the k values are calculated.
        Based on these k values, a first estimate of the quiet daily variation (Sr) is obtained.
        Hourly means with extended time ranges (30min + m + n) are obtained for each x.5 hour.
        m refers to 120 minutes (0-3a.m., 21-24p.m.), 60 minutes (3-6, 18-21) or 0 minutes.
        n is determined by k**3.3.

        xyz within the code always refers to the coordinate system of the sensor and not to any geomagnetic reference.

        By default it is assumed that the provided stream comes from a hdz oriented instrument.
        For xyz (or any other) orientation use the option checky=True to investigate both horizontal components.
        If the stream contains absolute data, the option hcomp = True transforms the stream to hdz.

        The following steps are performed:
        1. Asserts: Signal covers at least 24 hours, sampling rate minute or second
        2. Produce filtered minute signal, check for gaps, eventually interpolate (done by filter/sm algorythm) - needs some improvements
        3. from the last value contained get 3 hour segments and calculate max, min and max-min

        kwargs support the following keywords:
            - k9_level  (float) the value for which k9 is defined, all other values a linearly approximated
            - magnetic latitude  (float) another way to define the k scale
            - timerange (timedelta obsject) default=timedelta(hours=1)
            - fitdegree (float)  default=5
            - knotstep (float < 0.5) determines the amount of knots: amount = 1/knotstep ---> VERY smooth 0.1 | NOT VERY SMOOTH 0.001
            - flag
      PARAMETER:
        k9_level        (int) define the Observatories K9 Level. If not provided then firstly
                              the header information is scanned for a 'StationK9' input. If not
                              successful a K9 of 500 nT is assumend.
        """
        plot = kwargs.get('plot')
        debug = kwargs.get('debug')
        hcomp = kwargs.get('hcomp')
        fitdegree = kwargs.get('fitdegree')
        fitfunc=kwargs.get('fitfunc')
        magnetic_latitude = kwargs.get('magnetic_latitude')
        k9_level = kwargs.get('k9_level')
        checky = kwargs.get('checky')  # used for xyz data if True then the y component is checked as well

        if not fitfunc:
            fitfunc = 'harmonic'
        if not fitdegree:
            fitdegree = 5
        if not k9_level:
            if 'StationK9' in self.header:
                # 1. Check header info
                k9_level = self.header['StationK9']
            else:
                # 2. Set Potsdam default
                k9_level = 500

        # Some basics:
        startinghours = [0,3,6,9,12,15,18,21]
        mlist = [120,60,0,0,0,0,60,120]

        #ngkscale = [0,5,10,20,40,70,120,200,330,500]

        fortscale = [0,7.5,15,30,60,105,180,300,495,750]
        k_scale = [float(k9_level)*elem/750.0 for elem in fortscale]

        # calculate local scale from magnetic latitude (inclination):
        # important: how to do that - what is the latitudinal relationship, how to transfer the scale,
        # it is frequently mentioned to be quasi-log but it is not a simple Log scale
        # func can be fitted reasonably well by
        # func[a_] := Exp[0.8308663199145958 + 0.7894060396483681 k -  0.021250627459823503 k^2]

        kstream = DataStream()

        logger.info('--- Starting k value calculation: %s ' % (str(datetime.now())))

        # Non destructive - using a coyp of the supplied stream
        stream = self.copy()

        # ############################################
        # ##           Step 1           ##############
        # ##   ------------------------ ##############
        # ##  preparing data:           ##############
        # ##  - check sampling/length   ##############
        # ##  - check type (xyz etc)    ##############
        # ##  - check removing outliers ##############
        # ##  - eventually filter       ##############
        # ##  - interpolate/fill gaps   ##############
        # ############################################

        # removing outliers
        if debug:
            print("Removing outliers")
        stream = stream.flag_outlier(keys=['x','y','z'],threshold=6.) # Weak conditions
        stream = stream.remove_flagged()

        sr = stream.samplingrate()
        if debug:
            print("Sampling rate", sr)

        if sr > 65:
            print("Algorythm requires minute or higher resolution - aborting")
            return DataStream()
        if sr <= 0.9:
            print("Data appears to be below 1 second resolution - filtering to seconds first")
            stream = stream.nfilter(filter_width=timedelta(seconds=1))
            sr = stream.samplingrate()
        if 0.9 < sr < 55:
            print("Data appears to be below 1 minute resolution - filtering to minutes")
            stream = stream.nfilter(filter_width=timedelta(minutes=1))
        else:
            pass
            # get_gaps - put nans to missing data
            # then replace nans with interpolated values
            #nans, x= nan_helper(v)
            # v[nans]= interp(x(nans), x(~nans), v[~nans])

        ndtype = True
        if len(stream.ndarray[0]) > 0:
            ndtype = True
            timediff = np.max(stream.ndarray[0]) - np.min(stream.ndarray[0])
            indtyp = KEYLIST.index('typ')
            try:
                gettyp = stream.ndarray[indtyp][0]
            except:
                gettyp = 'xyzf'
            print("ndtype - Timeseries ending at:", num2date(np.max(stream.ndarray[0])))
        else:
            timediff = stream[-1].time - stream[0].time
            gettyp = stream[0].typ
            print("LineStruct - Timeseries ending at:", num2date(stream[-1].time))

        print("Coverage in days:", timediff)
        if timediff < 1.1:   # 1 corresponds to 24 hours
            print("not enough time covered - aborting")
            return

        if debug:
            print("Typ:", gettyp)

        # Transform the coordinate system to XYZ, asuming a hdz orientation.
        fmistream = stream
        if gettyp == 'idff':
            fmistream = stream._convertstream('idf2xyz',keep_header=True)
        elif gettyp == 'hdzf':
            fmistream = stream._convertstream('hdz2xyz',keep_header=True)
        elif not gettyp == 'xyzf':
            print("Unkown type of data - please provide xyzf, idff, hdzf -aborting")
            return

        # By default use H for determination
        if debug:
            print("converting data to hdz - only analyze h")
            print("This is applicable in case of baselinecorrected data")
        # TODO Important currently we are only using x (or x and y)

        if hcomp:
            print("Please note: H comp requires that columns xyz contain baseline corrected values")
            fmistream = fmistream._convertstream('xyz2hdz',keep_header=True)
        elif 'DataAbsFunctionObject' in fmistream.header:
            print("Found Baseline function")
            pass # to a bc correction and
            checky = True
        else:
            # If variation data use maximum from x and y
            checky = True


        # ############################################
        # ##           Step 2           ##############
        # ##   ------------------------ ##############
        # ##  some functions            ##############
        # ############################################

        def klist2stream(klist, kvalstream=DataStream() ,ndtype=True):
            """
            Internal method to convert a k value list to a stream
            """
            #emptystream = DataStream()

            if len(kvalstream.ndarray[0]) > 0:
                kexists = True
                #ti = list(li.ndarray[0])
                #print "Previous k", li.ndarray
            elif len(kvalstream) > 0:
                kexists = True
                #li = [elem for elem in kvalstream]
                #ti = [elem.time for elem in kvalstream]
            else:
                kexists = False
                array = [[] for key in KEYLIST]
                #li = DataStream()
            indvar1 = KEYLIST.index('var1')
            indvar2 = KEYLIST.index('var2')
            indvar3 = KEYLIST.index('var3')

            if ndtype:
                #array = [[] for key in KEYLIST]
                for kline in klist:
                    time = kline[0]
                    if kexists:
                        try:
                            ind = list(kvalstream.ndarray[0]).index(time)
                            #print "Found time at index", ind
                            #if kvalstream.ndarray[indvar3][ind] < quality lower
                            kvalstream = kvalstream._delete(ind)
                        except:
                            pass
                        kvalstream.ndarray[0] = np.append(kvalstream.ndarray[0],kline[0])
                        kvalstream.ndarray[indvar1] = np.append(kvalstream.ndarray[indvar1],kline[1])
                        kvalstream.ndarray[indvar2] = np.append(kvalstream.ndarray[indvar2],kline[2])
                        kvalstream.ndarray[indvar3] = np.append(kvalstream.ndarray[indvar3],kline[3])
                    else:
                        # put data to kvalstream
                        array[0].append(kline[0])
                        array[indvar1].append(kline[1])
                        array[indvar2].append(kline[2])
                        array[indvar3].append(kline[3]) # Quality parameter - containg time coverage
                                                         # High quality replaces low quality
                if not kexists:
                    array[0] = np.asarray(array[0])
                    array[indvar1] = np.asarray(array[indvar1])
                    array[indvar2] = np.asarray(array[indvar2])
                    kvalstream.ndarray = np.asarray(array)

                return kvalstream


        def maxmink(datastream, cdlist, index, k_scale, ndtype=True, **kwargs):
            # function returns 3 hour k values for a 24 hour minute time series
            # The following function is used several times on different !!!!! 24h !!!!!!!  timeseries
            #         (with and without removal of daily-quiet signals)
            checky = kwargs.get('checky')

            xmaxval = 0
            xminval = 0
            ymaxval = 0
            yminval = 0
            deltaday = 0
            klist = []
            for j in range(0,8):
                if debug:
                    print("Loop Test", j, index, num2date(cdlist[index])-timedelta(days=deltaday))
                #t7 = datetime.utcnow()

                #threehours = datastream.extract("time", date2num(num2date(cdlist[index])-timedelta(days=deltaday)), "<")

                et = date2num(num2date(cdlist[index])-timedelta(days=deltaday))
                index = index - 1
                if index < 0:
                    index = 7
                    deltaday += 1
                if debug:
                    print("Start", num2date(cdlist[index])-timedelta(days=deltaday))
                #threehours = threehours.extract("time", date2num(num2date(cdlist[index])-timedelta(days=deltaday)), ">=")

                st = date2num(num2date(cdlist[index])-timedelta(days=deltaday))
                ar = datastream._select_timerange(starttime=st, endtime=et)
                threehours = DataStream([LineStruct()],{},ar)
                #print("ET",st,et)
                #t8 = datetime.utcnow()
                #print("Extracting time needed:", t8-t7)

                if ndtype:
                    len3hours = len(threehours.ndarray[0])
                else:
                    len3hours = len(threehours)
                if debug:
                    print("Length of three hour segment", len3hours)
                if len3hours > 0:
                    if ndtype:
                        indx = KEYLIST.index('x')
                        indy = KEYLIST.index('y')
                        colx = threehours.ndarray[indx]
                    else:
                        colx = threehours._get_column('x')
                    colx = [elem for elem in colx if not isnan(elem)]
                    if len(colx) > 0:
                        xmaxval = max(colx)
                        xminval = min(colx)
                    else:
                        ymaxval = 0.0
                        yminval = 0.0
                    if checky:
                        if ndtype:
                            coly = threehours.ndarray[indy]
                        else:
                            coly = threehours._get_column('y')
                        coly = [elem for elem in coly if not isnan(elem)]
                        ymaxval = max(coly)
                        yminval = min(coly)
                    else:
                        ymaxval = 0.0
                        yminval = 0.0
                    maxmindiff = max([xmaxval-xminval, ymaxval-yminval])
                    k = np.nan
                    for count,val  in enumerate(k_scale):
                        if maxmindiff > val:
                            k = count
                    if np.isnan(k):
                        maxmindiff = np.nan
                    if debug:
                        print("Extrema", k, maxmindiff, xmaxval, xminval, ymaxval, yminval)
                    # create a k-value list
                else:
                    k = np.nan
                    maxmindiff = np.nan
                ti = date2num(num2date(cdlist[index])-timedelta(days=deltaday)+timedelta(minutes=90))
                klist.append([ti,k,maxmindiff,1])

            return klist

        def fmimeans(datastream, laststep, kvalstream, ndtype=True):
            # function returns 3 hour k values for a 24 hour minute time series
            deltaday = 0
            hmlist = []
            meanstream = DataStream()

            lasthour = num2date(laststep).replace(minute=0, second=0, microsecond=0)
            for j in range(0,24):
                #if debug:
                #    print "Loop Test", j
                # last hour
                index = lasthour.hour
                index = index - 1
                if index < 0:
                    index = 23
                #if debug:
                #print index
                meanat = lasthour - timedelta(minutes=30)

                #get m (using index)
                #if debug:
                #print int(np.floor(index/3.))
                m = mlist[int(np.floor(index/3.))]
                #if debug:
                #print "m:", m
                #get n
                # test: find nearest kval from kvalstream
                idx = (np.abs(kvalstream.ndarray[0].astype(float)-date2num(meanat))).argmin()
                kval = kvalstream.ndarray[KEYLIST.index('var1')][idx]
                if not np.isnan(kval):
                    n = kval**3.3
                else:
                    n = 0

                # extract meanat +/- (30+m+n)
                valrange = datastream.extract("time", date2num(meanat+timedelta(minutes=30)+timedelta(minutes=m)+timedelta(minutes=n)), "<")
                valrange = valrange.extract("time", date2num(meanat-timedelta(minutes=30)-timedelta(minutes=m)-timedelta(minutes=n)), ">=")
                #if debug:
                #print "Length of Sequence", len(valrange), num2date(valrange[0].time), num2date(valrange[-1].time)
                if ndtype:
                    firsttime = np.min(datastream.ndarray[0])
                else:
                    firsttime = datastream[0].time
                if not firsttime < date2num(meanat-timedelta(minutes=30)-timedelta(minutes=m)-timedelta(minutes=n)):
                    print("##############################################")
                    print(" careful - datastream not long enough for correct k determination")
                    print("##############################################")
                    print("Hourly means not correctly determinable for day", meanat)
                    print("as the extended time range is not reached")
                    print("----------------------------------------------")
                    kvalstream.ndarray[KEYLIST.index('var3')][idx] = 0.5
                    #return meanstream
                # Now get the means
                meanx = valrange.mean('x')
                meany = valrange.mean('y')
                meanz = valrange.mean('z')
                hmlist.append([date2num(meanat),meanx,meany,meanz])
                # Describe why we are duplicating values at the end and the beginning!!
                # Was that necessary for the polyfit??
                if j == 0:
                    hmlist.append([date2num(meanat+timedelta(minutes=30)+timedelta(minutes=m)+timedelta(minutes=n)),meanx,meany,meanz])
                if j == 23:
                    hmlist.append([date2num(meanat-timedelta(minutes=30)-timedelta(minutes=m)-timedelta(minutes=n)),meanx,meany,meanz])

                lasthour = lasthour - timedelta(hours=1)

            if ndtype:
                array = [[] for key in KEYLIST]
                indx = KEYLIST.index('x')
                indy = KEYLIST.index('y')
                indz = KEYLIST.index('z')
                array[0] =  np.asarray([elem[0] for elem in hmlist])
                array[indx] =  np.asarray([elem[1] for elem in hmlist])
                array[indy] =  np.asarray([elem[2] for elem in hmlist])
                array[indz] =  np.asarray([elem[3] for elem in hmlist])
                meanstream.ndarray = np.asarray(array)
            else:
                for elem in sorted(hmlist):
                    line = LineStruct()
                    line.time = elem[0]
                    line.x = elem[1]
                    line.y = elem[2]
                    line.z = elem[3]
                    meanstream.add(line)

            #print klist
            return meanstream.sorting()

        # ############################################
        # ##           Step 2           ##############
        # ##   ------------------------ ##############
        # ##  analyze last 24 h:        ##############
        # ##  - get last day            ##############
        # ##  - get last 3hour segment  ##############
        # ##  - run backwards           ##############
        # ##  - calc fmi:               ##############
        # ##    - 1. get max/min deviation ###########
        # ##    - 2. use this k to get sr  ###########
        # ##    - 3. calc k with sr reduced ##########
        # ##    - 4. recalc sr              ##########
        # ##    - 5. final k                ##########
        # ############################################

        if ndtype:
            currentdate = num2date(np.max(fmistream.ndarray[0])).replace(tzinfo=None)
            lastdate = currentdate
            d = currentdate.date()
            currentdate = datetime.combine(d, datetime.min.time())
        else:
            currentdate = num2date(fmistream[-1].time).replace(tzinfo=None)
            lastdate = currentdate
            d = currentdate.date()
            currentdate = datetime.combine(d, datetime.min.time())

        print("Last effective time series ending at day", currentdate)

        print(" -----------------------------------------------------")
        print(" ------------- Starting backward analysis ------------")
        print(" --------------- beginning at last time --------------")

        # selecting reduced time range!!!
        t1 = datetime.utcnow()
        array = fmistream._select_timerange(starttime=currentdate-timedelta(days=2))
        fmitstream = DataStream([LineStruct()],fmistream.header,array)

        cdlist = [date2num(currentdate.replace(hour=elem)) for elem in startinghours]
        #print("Daily list", cdlist, currentdate)
        t2 = datetime.utcnow()
        print("Step0 needed:", t2-t1)

        #ta, i = find_nearest(np.asarray(cdlist), date2num(lastdate-timedelta(minutes=90)))
        ta, i = find_nearest(np.asarray(cdlist), date2num(lastdate))
        if i < 7:
            i=i+1
        else:
            i=0
            cdlist = [el+1 for el in cdlist]
        #print("Nearest three hour mark", num2date(ta), i, np.asarray(cdlist))

        if plot:
            import magpy.mpplot as mp
            fmistream.plot(noshow=True, plottitle="0")

        # 1. get a backward 24 hour calculation from the last record
        klist = maxmink(fmitstream,cdlist,i,k_scale)
        #print(klist, i)
        kstream = klist2stream(klist, kstream)

        t3 = datetime.utcnow()
        print("Step1 needed:", t3-t2)

        # 2. a) now get the hourly means with extended time ranges (sr function)
        hmean = fmimeans(fmitstream,date2num(lastdate),kstream)
        func = hmean.fit(['x','y','z'],fitfunc='harmonic',fitdegree=5)
        if plot:
            hmean.plot(function=func,noshow=True, plottitle="1: SR function")

        # 2. b) subtract sr from original record
        #redfmi = fmistream.func_subtract(func)
        redfmi = fmistream.func2stream(func,mode='sub')

        if plot:
            redfmi.plot(noshow=True, plottitle="1: reduced")
            fmistream.plot(noshow=True, plottitle="1")

        t4 = datetime.utcnow()
        print("Step2 needed:", t4-t3)

        # 3. recalc k
        klist = maxmink(redfmi,cdlist,i,k_scale)
        kstream = klist2stream(klist, kstream)
        #print ("3.", num2date(kstream.ndarray[0]))

        t5 = datetime.utcnow()
        print("Step3 needed:", t5-t4)

        # 4. recalc sr and subtract
        finalhmean = fmimeans(fmitstream,date2num(lastdate),kstream)
        finalfunc = finalhmean.fit(['x','y','z'],fitfunc='harmonic',fitdegree=5)
        firedfmi = fmistream.func2stream(finalfunc,mode='sub')

        if plot:
            mp.plot(finalhmean,['x','y','z'],function=finalfunc,noshow=True, plottitle="2: SR function")
            #finalhmean.plot(['x','y','z'],function=finalfunc,noshow=True, plottitle="2: SR function")
            firedfmi.plot(['x','y','z'],noshow=True, plottitle="2: reduced")
            fmitstream.plot(['x','y','z'],plottitle="2")

        t6 = datetime.utcnow()
        print("Step4 needed:", t6-t5)

        # 5. final k
        klist = maxmink(firedfmi,cdlist,i,k_scale)
        kstream = klist2stream(klist, kstream)

        #print ("Last", num2date(kstream.ndarray[0]))
        t7 = datetime.utcnow()
        print("Step5 needed:", t7-t6)

        # ############################################
        # ##           Step 3           ##############
        # ##   ------------------------ ##############
        # ##  analyze from beginning:   ##############
        # ##  - get first record        ##############
        # ##  - from day to day         ##############
        # ##  - run backwards           ##############
        # ##  - calc fmi:               ##############
        # ##    - 1. get max/min deviation ###########
        # ##    - 2. use this k to get sr  ###########
        # ##    - 3. calc k with sr reduced ##########
        # ##    - 4. recalc sr              ##########
        # ##    - 5. final k                ##########
        # ############################################
        print(" -----------------------------------------------------")
        print(" ------------- Starting forward analysis -------------")
        print(" -----------------  from first date ------------------")

        if ndtype:
            st = np.min(fmistream.ndarray[0])
        else:
            st = fmistream[0].time

        startday = int(np.floor(st))
        for daynum in range(1,int(timediff)+1):
            currentdate = num2date(startday+daynum)
            print("Running daily chunks forward until ", currentdate)
            # selecting reduced time range!!!
            array = fmistream._select_timerange(starttime=currentdate-timedelta(days=3),endtime=currentdate+timedelta(days=1))
            fmitstream = DataStream([LineStruct()],fmistream.header,array)

            cdlist = [date2num(currentdate.replace(hour=elem)) for elem in startinghours]
            #print "Daily list", cdlist

            # 1. get a backward 24 hour calculation from the last record
            klist = maxmink(fmitstream,cdlist,0,k_scale)
            #print("forward", klist)
            kstream = klist2stream(klist, kstream)
            # 2. a) now get the hourly means with extended time ranges (sr function)
            hmean = fmimeans(fmitstream,startday+daynum,kstream)
            if ndtype:
                lenhmean = len(hmean.ndarray[0])
            else:
                lenhmean = len(hmean)
            if not lenhmean == 0: # Length 0 if not enough data for full extended mean value calc
                func = hmean.fit(['x','y','z'],fitfunc='harmonic',fitdegree=5)
                #hmean.plot(function=func,noshow=True)
                if not func[0] == {}:
                    if plot:
                        fmistream.plot(noshow=True)
                    # 2. b) subtract sr from original record
                    redfmi = fmitstream.func2stream(func,mode='sub')
                    # 3. recalc k
                    klist = maxmink(redfmi,cdlist,0,k_scale)
                    kstream = klist2stream(klist, kstream)
                    #print klist
                    # 4. recalc sr and subtract
                    finalhmean = fmimeans(fmitstream,startday+daynum,kstream)
                    finalfunc = finalhmean.fit(['x','y','z'],fitfunc='harmonic',fitdegree=5)
                    firedfmi = fmistream.func2stream(finalfunc,mode='sub')
                    if plot:
                        finalhmean.plot(['x','y','z'],noshow=True, function=finalfunc, plottitle="2")
                        firedfmi.plot(['x','y','z'],noshow=True, plottitle="2: reduced")
                        fmitstream.plot(['x','y','z'], plottitle="2: fmistream")
                    # 5. final k
                    klist = maxmink(firedfmi,cdlist,0,k_scale)
                    kstream = klist2stream(klist, kstream)
                    #print "Final", klist

        #print kstream.ndarray, klist

        kstream = kstream.sorting()
        kstream.header['col-var1'] = 'K'
        kstream.header['col-var2'] = 'C'
        kstream.header['col-var3'] = 'Quality'
        #print ("Test",kstream.ndarray)

        return DataStream([LineStruct()],kstream.header,kstream.ndarray)

        """
        outstream = DataStream()
        lst = [[elem.time,elem.var1,elem.var2] for elem in kstream]
        for el in sorted(lst):
            line = LineStruct()
            line.time = el[0]
            line.var1 = el[1]
            line.var2 = el[2]
            outstream.add(line)

        return outstream
        """



    def linestruct2ndarray(self):
        """
    DEFINITION:
        Converts linestruct data to ndarray.
    RETURNS:
        - self with ndarray filled
    EXAMPLE:
        >>> data = data.linestruct2ndarray()

    APPLICATION:
        """
        def checkEqual3(lst):
            return lst[1:] == lst[:-1]

        array = [np.asarray([]) for elem in KEYLIST]

        keys = self._get_key_headers()

        t = np.asarray(self._get_column('time'))
        array[0] = t
        for key in keys:
            ind = KEYLIST.index(key)
            col = self._get_column(key)
            if len(col) > 0:
                if not False in checkEqual3(col) and col[0] == '-':
                    col = np.asarray([])
                array[ind] = col
            else:
                array[ind] = []

        array = np.asarray(array)
        steam = DataStream()
        stream = [LineStruct()]
        return DataStream(stream,self.header,array)



    def mean(self, key, **kwargs):
        """
    DEFINITION:
        Calculates mean values for the specified key, Nan's are regarded for.
        Means are only calculated if more then "amount" in percent are non-nan's
        Returns a float if successful or NaN.

    PARAMETERS:
    Variables:
        - key:          (KEYLIST) element of Keylist like 'x' .
    Kwargs:
        - percentage:   (int) Define required percentage of non-nan values, if not
                               met that nan will be returned. Default is 95 (%)
        - meanfunction: (string) accepts 'mean' and 'median'. Default is 'mean'
        - std:          (bool) if true, the standard deviation is returned as well

    RETURNS:
        - mean/median(, std) (float)
    EXAMPLE:
        >>> meanx = datastream.mean('x',meanfunction='median',percentage=90)

    APPLICATION:
        stream = read(datapath)
        mean = stream.mean('f')
        median = stream.mean('f',meanfunction='median')
        stddev = stream.mean('f',std=True)
        """
        percentage = kwargs.get('percentage')
        meanfunction = kwargs.get('meanfunction')
        std = kwargs.get('std')

        if not meanfunction:
            meanfunction = 'mean'
        if not percentage:
            percentage = 95
        if not std:
            std = False

        ndtype = False
        if len(self.ndarray[0])>0:
            ndtype = True
        elif len(self) > 0:
            pass
        else:
            logger.error('mean: empty stream - aborting')
            if std:
                return float("NaN"), float("NaN")
            else:
                return float("NaN")

        if not isinstance( percentage, (int,long)):
            logger.error("mean: Percentage needs to be an integer!")
        if not key in KEYLIST[:16]:
            logger.error("mean: Column key not valid!")

        if ndtype:
            ind = KEYLIST.index(key)
            length = len(self.ndarray[0])
            self.ndarray[ind] = np.asarray(self.ndarray[ind])
            ar = self.ndarray[ind].astype(float)
            ar = ar[~np.isnan(ar)]
        else:
            ar = [getattr(elem,key) for elem in self if not isnan(getattr(elem,key))]
            length = float(len(self))
        div = float(len(ar))/length*100.0

        if div >= percentage:
            if std:
                return eval('np.'+meanfunction+'(ar)'), np.std(ar)
            else:
                return eval('np.'+meanfunction+'(ar)')
        else:
            logger.info('mean: Too many nans in column {}, exceeding {} percent'.format(key,percentage))
            if std:
                return float("NaN"), float("NaN")
            else:
                return float("NaN")

    def missingvalue(self,v,window_len,threshold=0.9,fill='mean'):
        """
        DESCRIPTION
            fills missing values either with means or interpolated values
        PARAMETER:
            v: 			(np.array) single column of ndarray
            window_len: 	(int) length of window to check threshold
            threshold: 	        (float) minimum percentage of available data e.g. 0.9 - 90 precent
            fill: 	        (string) 'mean' or 'interpolation'
        RETURNS:
            ndarray - single column
        """
        try:
            v_rest = np.array([])
            v = v.astype(float)
            n_split = len(v)/float(window_len)
            if not n_split == int(n_split):
                el = int(int(n_split)*window_len)
                v_rest = v[el:]
                v = v[:el]
            spli = np.split(v,int(len(v)/window_len))
            if len(v_rest) > 0:
                spli.append(v_rest)
            newar = np.array([])
            for idx,ar in enumerate(spli):
                nans, x = nan_helper(ar)
                if len(ar[~nans]) >= threshold*len(ar):
                    if fill == 'mean':
                        ar[nans]= np.nanmean(ar)
                    else:
                        ar[nans]= interp(x(nans), x(~nans), ar[~nans])
                newar = np.concatenate((newar,ar))
            v = newar
        except:
            print ("Filter: could not split stream in equal parts for interpolation - switching to conservative mode")

        return v

    def MODWT_calc(self,key='x',wavelet='haar',level=1,plot=False,outfile=None,
                window=5):
        """
    DEFINITION:
        Multiple Overlap Discrete wavelet transform (MODWT) method of analysing a magnetic signal
        to pick out SSCs. This method was taken from Hafez (2013b): "Geomagnetic Sudden
        Commencement Automatic Detection via MODWT"
        (NOTE: PyWavelets package must be installed for this method. It should be applied
        to 1s data - otherwise the sample window and detection levels should be changed.)

        METHOD:
        1. Use the Haar wavelet filter to calculate the 1st and 2nd details
           of the geomagnetic signal.
        2. The 1st detail (D1) samples are squared to evaluate the magnitude.
        3. The sample window (5) is averaged to avoid ripple effects. (This means the
           returned stream will have ~1/5 the size of the original.)

    PARAMETERS:
    Variables:
        - key:          (str) Apply MODWT to this key. Default 'x' due to SSCs dominating
                        the horizontal component.
        - wavelet:	(str) Type of filter to use. Default 'db4' (4th-order Daubechies
                        wavelet filter) according to Hafez (2013).
        - level:	(int) Decomposition level. Will calculate details down to this level.
                        Default 3, also Hafez (2013).
        - plot:	        (bool) If True, will display a plot of A3, D1, D2 and D3.
        - outfile:      (str) If given, will plot will be saved to 'outfile' path.
        - window:       (int) Length of sample window. Default 5, i.e. 5s with second data.

    RETURNS:
        - MODWT_stream: 	(DataStream object) A stream containing the following:
                        'x': A_n (approximation function)
                        'var1': D1 (first detail)
                        'var2': D2 (second detail)
                        ...
                        'var3': D3 (third detail)
                        ...

    EXAMPLE:
        >>> DWT_stream = stream.DWT_calc(plot=True)

    APPLICATION:
        # Storm detection using detail 3 (D3 = var3):
        from magpy.stream import *
        stream = read('LEMI_1s_Data_2014-02-15.cdf')	# 2014-02-15 is a good storm example
        MODWT_stream = stream.MODWT_calc(plot=True)
        Da_min = 0.0005 # nT^2 (minimum amplitude of D3 for storm detection)
        Dp_min = 40 # seconds (minimum period of Da > Da_min for storm detection)
        detection = False
        for row in MODWT_stream:
            if row.var3 >= Da_min and detection == False:
                timepin = row.time
                detection = True
            elif row.var3 < Da_min and detection == True:
                duration = (num2date(row.time) - num2date(timepin)).seconds
                if duration >= Dp_min:
                    print "Storm detected!"
                    print duration, num2date(timepin)
                detection = False
        """

        # Import required package PyWavelets:
        # http://www.pybytes.com/pywavelets/index.html
        import pywt

        # 1a. Grab array from stream
        data = self._get_column(key)
        t_ind = KEYLIST.index('time')

        #MODWT_stream = DataStream([],{})
        MODWT_stream = DataStream()
        headers = MODWT_stream.header
        array = [[] for key in KEYLIST]
        x_ind = KEYLIST.index('x')
        dx_ind = KEYLIST.index('dx')
        var1_ind = KEYLIST.index('var1')
        var2_ind = KEYLIST.index('var2')
        var3_ind = KEYLIST.index('var3')
        var4_ind = KEYLIST.index('var4')
        var5_ind = KEYLIST.index('var5')
        dy_ind = KEYLIST.index('dy')
        i = 0
        logger.info("MODWT_calc: Starting Discrete Wavelet Transform of key %s." % key)

        if len(data) % 2 == 1:
            data = data[0:-1]

        # Results have format:
        # (cAn, cDn), ..., (cA2, cD2), (cA1, cD1)
        coeffs = pywt.swt(data, wavelet, level)
        acoeffs, dcoeffs = [], []
        for i in xrange(level):
            (a, d) = coeffs[i]
            acoeffs.append(a)
            dcoeffs.append(d)

        for i, item in enumerate(dcoeffs):
            dcoeffs[i] = [j**2 for j in item]

        # 1b. Loop for sliding window
        while True:
            if i >= (len(data)-window):
                break

            # Take the values in the middle of the window (not exact but changes are
            # not extreme over standard 5s window)
            array[t_ind].append(self.ndarray[t_ind][i+window/2])
            data_cut = data[i:i+window]
            array[x_ind].append(sum(data_cut)/float(window))

            a_cut = acoeffs[0][i:i+window]
            array[dx_ind].append(sum(a_cut)/float(window))
            for j in xrange(level):
                d_cut = dcoeffs[-(j+1)][i:i+window]
                if j <= 5:
                    key = 'var'+str(j+1)
                    array[KEYLIST.index(key)].append(sum(d_cut)/float(window))
                elif 5 < j <= 7:
                    if j == 6:
                        key = 'dy'
                    elif j == 7:
                        key = 'dz'
                    array[KEYLIST.index(key)].append(sum(d_cut)/float(window))

            i += window

        logger.info("MODWT_calc: Finished MODWT.")

        MODWT_stream.header['col-x'] = 'A3'
        MODWT_stream.header['unit-col-x'] = 'nT^2'
        MODWT_stream.header['col-var1'] = 'D1'
        MODWT_stream.header['unit-col-var1'] = 'nT^2'
        MODWT_stream.header['col-var2'] = 'D2'
        MODWT_stream.header['unit-col-var2'] = 'nT^2'
        MODWT_stream.header['col-var3'] = 'D3'
        MODWT_stream.header['unit-col-var3'] = 'nT^2'
        MODWT_stream.header['col-var4'] = 'D4'
        MODWT_stream.header['unit-col-var4'] = 'nT^2'
        MODWT_stream.header['col-var5'] = 'D5'
        MODWT_stream.header['unit-col-var5'] = 'nT^2'
        MODWT_stream.header['col-dy'] = 'D6'
        MODWT_stream.header['unit-col-dy'] = 'nT^2'

        # Plot stream:
        if plot == True:
            date = datetime.strftime(num2date(self.ndarray[0][0]),'%Y-%m-%d')
            logger.info('MODWT_calc: Plotting data...')
            if outfile:
                MODWT_stream.plot(['x','var1','var2','var3'],
                                plottitle="MODWT Decomposition of %s (%s)" % (key,date),
                                outfile=outfile)
            else:
                MODWT_stream.plot(['x','var1','var2','var3'],
                                plottitle="MODWT Decomposition of %s (%s)" % (key,date))

        for key in KEYLIST:
            array[KEYLIST.index(key)] = np.asarray(array[KEYLIST.index(key)])

        return DataStream([LineStruct()], headers, np.asarray(array))


    def multiply(self, factors, square=False):
        """
    DEFINITION:
        A function to multiply the datastream, should one ever have the need to.
        Scale value correction for example.

    PARAMETERS:
    Variables:
        - factors:      (dict) Dictionary of multiplcation factors with keys to apply to
                        e.g. {'x': -1, 'f': 2}
    Kwargs:
        - square:       (bool) If True, key will be squared by the factor.

    RETURNS:
        - self:         (DataStream) Multiplied datastream.

    EXAMPLE:
        >>> data.multiply({'x':-1})

    APPLICATION:

        """
        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True

        for key in factors:
            if key in KEYLIST:
                if ndtype:
                    ind = KEYLIST.index(key)
                    val = self.ndarray[ind]
                else:
                    val = self._get_column(key)
                if key == 'time':
                    logger.error("factor: Multiplying time? That's just plain silly.")
                else:
                    if square == False:
                        newval = [elem * factors[key] for elem in val]
                        logger.info('factor: Multiplied column %s by %s.' % (key, factors[key]))
                    else:
                        newval = [elem ** factors[key] for elem in val]
                        logger.info('factor: Multiplied column %s by %s.' % (key, factors[key]))
                if ndtype:
                    self.ndarray[ind] = np.asarray(newval)
                else:
                    self = self._put_column(newval, key)
            else:
                logger.warning("factor: Key '%s' not in keylist." % key)

        return self


    def obspyspectrogram(self, data, samp_rate, per_lap=0.9, wlen=None, log=False,
                    outfile=None, fmt=None, axes=None, dbscale=False,
                    mult=8.0, cmap=None, zorder=None, title=None, show=True,
                    sphinx=False, clip=[0.0, 1.0]):

        #TODO: Discuss with Ramon which kind of window should be used (cos^2(2*pi (t/T)))
        """
        Function taken from ObsPy
        Computes and plots spectrogram of the input data.
        :param data: Input data
        :type samp_rate: float
        :param samp_rate: Samplerate in Hz
        :type per_lap: float
        :param per_lap: Percentage of overlap of sliding window, ranging from 0
            to 1. High overlaps take a long time to compute.
        :type wlen: int or float
        :param wlen: Window length for fft in seconds. If this parameter is too
            small, the calculation will take forever.
        :type log: bool
        :param log: Logarithmic frequency axis if True, linear frequency axis
            otherwise.
        :type outfile: String
        :param outfile: String for the filename of output file, if None
            interactive plotting is activated.
        :type fmt: String
        :param fmt: Format of image to save
        :type axes: :class:`matplotlib.axes.Axes`
        :param axes: Plot into given axes, this deactivates the fmt and
            outfile option.
        :type dbscale: bool
        :param dbscale: If True 10 * log10 of color values is taken, if False the
            sqrt is taken.
        :type mult: float
        :param mult: Pad zeros to lengh mult * wlen. This will make the spectrogram
            smoother. Available for matplotlib > 0.99.0.
        :type cmap: :class:`matplotlib.colors.Colormap`
        :param cmap: Specify a custom colormap instance
        :type zorder: float
        :param zorder: Specify the zorder of the plot. Only of importance if other
            plots in the same axes are executed.
        :type title: String
        :param title: Set the plot title
        :type show: bool
        :param show: Do not call `plt.show()` at end of routine. That way, further
            modifications can be done to the figure before showing it.
        :type sphinx: bool
        :param sphinx: Internal flag used for API doc generation, default False
        :type clip: [float, float]
        :param clip: adjust colormap to clip at lower and/or upper end. The given
            percentages of the amplitude range (linear or logarithmic depending
            on option `dbscale`) are clipped.
        """

        # enforce float for samp_rate
        samp_rate = float(samp_rate)

        # set wlen from samp_rate if not specified otherwise
        if not wlen:
            wlen = samp_rate / 100.

        npts = len(data)

        # nfft needs to be an integer, otherwise a deprecation will be raised
        #XXX add condition for too many windows => calculation takes for ever
        nfft = int(nearestPow2(wlen * samp_rate))

        if nfft > npts:
            nfft = int(nearestPow2(npts / 8.0))

        if mult != None:
            mult = int(nearestPow2(mult))
            mult = mult * nfft

        nlap = int(nfft * float(per_lap))

        data = data - data.mean()
        end = npts / samp_rate

        # Here we call not plt.specgram as this already produces a plot
        # matplotlib.mlab.specgram should be faster as it computes only the
        # arrays
        # XXX mlab.specgram uses fft, would be better and faster use rfft

        if MATPLOTLIB_VERSION >= [0, 99, 0]:
            specgram, freq, time = mlab.specgram(data, Fs=samp_rate, NFFT=nfft,
                                                  pad_to=mult, noverlap=nlap)
        else:
            specgram, freq, time = mlab.specgram(data, Fs=samp_rate,
                                                    NFFT=nfft, noverlap=nlap)

        # db scale and remove zero/offset for amplitude
        if dbscale:
            specgram = 10 * np.log10(specgram[1:, :])
        else:
            specgram = np.sqrt(specgram[1:, :])

        freq = freq[1:]

        vmin, vmax = clip

        if vmin < 0 or vmax > 1 or vmin >= vmax:
            msg = "Invalid parameters for clip option."
            raise ValueError(msg)

        _range = float(specgram.max() - specgram.min())
        vmin = specgram.min() + vmin * _range
        vmax = specgram.min() + vmax * _range
        norm = Normalize(vmin, vmax, clip=True)

        if not axes:
            fig = plt.figure()
            ax = fig.add_subplot(111)
        else:
            ax = axes

        # calculate half bin width
        halfbin_time = (time[1] - time[0]) / 2.0
        halfbin_freq = (freq[1] - freq[0]) / 2.0

        if log:
            # pcolor expects one bin more at the right end
            freq = np.concatenate((freq, [freq[-1] + 2 * halfbin_freq]))
            time = np.concatenate((time, [time[-1] + 2 * halfbin_time]))
            # center bin
            time -= halfbin_time
            freq -= halfbin_freq
            # pcolormesh issue was fixed in matplotlib r5716 (2008-07-07)
            # inbetween tags 0.98.2 and 0.98.3
            # see:
            #  - http://matplotlib.svn.sourceforge.net/viewvc/...
            #    matplotlib?revision=5716&view=revision
            #  - http://matplotlib.sourceforge.net/_static/CHANGELOG

            if MATPLOTLIB_VERSION >= [0, 98, 3]:
                # Log scaling for frequency values (y-axis)
                ax.set_yscale('log')
                # Plot times
                ax.pcolormesh(time, freq, specgram, cmap=cmap, zorder=zorder,
                              norm=norm)
            else:
                X, Y = np.meshgrid(time, freq)
                ax.pcolor(X, Y, specgram, cmap=cmap, zorder=zorder, norm=norm)
                ax.semilogy()
        else:
            # this method is much much faster!
            specgram = np.flipud(specgram)
            # center bin
            extent = (time[0] - halfbin_time, time[-1] + halfbin_time,
                      freq[0] - halfbin_freq, freq[-1] + halfbin_freq)
            ax.imshow(specgram, interpolation="nearest", extent=extent,
                      cmap=cmap, zorder=zorder)

        # set correct way of axis, whitespace before and after with window
        # length
        ax.axis('tight')
        ax.set_xlim(0, end)
        ax.grid(False)

        if axes:
            return ax

        ax.set_xlabel('Time [s]')
        ax.set_ylabel('Frequency [Hz]')
        if title:
            ax.set_title(title)

        if not sphinx:
            # ignoring all NumPy warnings during plot
            temp = np.geterr()
            np.seterr(all='ignore')
            plt.draw()
            np.seterr(**temp)

        if outfile:
            if fmt:
                fig.savefig(outfile, format=fmt)
            else:
                fig.savefig(outfile)
        elif show:
            plt.show()
        else:
            return fig


    def offset(self, offsets, **kwargs):
        """
    DEFINITION:
        Apply constant offsets to elements of the datastream

    PARAMETERS:
    Variables:
        - offsets:      (dict) Dictionary of offsets with keys to apply to
                        e.g. {'time': timedelta(hours=1), 'x': 4.2, 'f': -1.34242}
                        Important: Time offsets have to be timedelta objects
    Kwargs:
        - starttime:    (Datetime object) Start time to apply offsets
        - endtime :     (Datetime object) End time to apply offsets

    RETURNS:
        - variable:     (type) Description.

    EXAMPLE:
        >>> data.offset({'x':7.5})
        or
        >>> data.offset({'x':7.5},starttime='2015-11-21 13:33:00',starttime='2015-11-23 12:22:00')

    APPLICATION:

        """
        endtime = kwargs.get('endtime')
        starttime = kwargs.get('starttime')
        comment = kwargs.get('comment')

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype =True
            tcol = self.ndarray[0]
        else:
            tcol = self._get_column('time')

        if not len(tcol) > 0:
            logger.error("offset: No data found - aborting")
            return self

        stidx = 0
        edidx = len(tcol)
        if starttime:
            st = date2num(self._testtime(starttime))
            # get index number of first element >= starttime in timecol
            stidxlst = np.where(tcol >= st)[0]
            if not len(stidxlst) > 0:
                return self   ## stream ends before starttime
            stidx = stidxlst[0]
        if endtime:
            ed = date2num(self._testtime(endtime))
            # get index number of last element <= endtime in timecol
            edidxlst = np.where(tcol <= ed)[0]
            if not len(edidxlst) > 0:
                return self   ## stream begins after endtime
            edidx = (edidxlst[-1]) + 1

        if comment and not comment == '':
            if len(self.ndarray[0]) > 0:
                commpos = KEYLIST.index('comment')
                flagpos = KEYLIST.index('flag')
                commcol = self.ndarray[commpos]
            else:
                commcol = self._get_column('comment')
            if not len(commcol) == len(tcol):
                commcol = [''] * len(tcol)
            if not len(self.ndarray[flagpos]) == len(tcol):
                fllist = ['0' for el in FLAGKEYLIST]
                fllist.append('-')
                fl = ''.join(fllist)
                self.ndarray[flagpos] = [fl] * len(tcol)
            for idx,el in enumerate(commcol):
                if idx >= stidx and idx <= edidx:
                    if not el == '':
                        commcol[idx] = comment + ', ' + el
                    else:
                        commcol[idx] = comment
                else:
                    commcol[idx] = el
            print("offset", len(commcol), len(tcol))
            self.ndarray[commpos] = commcol

        for key in offsets:
            if key in KEYLIST:
                if ndtype:
                    ind = KEYLIST.index(key)
                    val = self.ndarray[ind]
                else:
                    val = self._get_column(key)
                val = val[stidx:edidx]
                if key == 'time':
                    secperday = 24*3600
                    try:
                        os = offsets[key].total_seconds()/secperday
                    except:
                        try:
                            exec('os = '+offsets[key]+'.total_seconds()/secperday')
                        except:
                            print("offset: error with time offset - check provided timedelta")
                            break
                    val = val + os
                    #print num2date(val[0]).replace(tzinfo=None)
                    #print num2date(val[0]).replace(tzinfo=None) + offsets[key]
                    #newval = [date2num(num2date(elem).replace(tzinfo=None) + offsets[key]) for elem in val]
                    logger.info('offset: Corrected time column by %s sec' % str(offsets[key]))
                else:
                    val = val + offsets[key]
                    #newval = [elem + offsets[key] for elem in val]
                    logger.info('offset: Corrected column %s by %.3f' % (key, offsets[key]))
                if ndtype:
                    self.ndarray[ind][stidx:edidx] = val
                else:
                    nval = self._get_column(key) # repeated extraction of column - could be optimzed but usage of LineStruct will not be supported in future
                    nval[stidx:edidx] = val
                    self = self._put_column(nval, key)
            else:
                logger.error("offset: Key '%s' not in keylist." % key)

        return self


    def plot(self, keys=None, debugmode=None, **kwargs):
        """
    DEFINITION:
        Code for plotting one dataset. Consult mpplot.plot() and .plotStreams() for more
        details.

    EXAMPLE:
        >>> cs1_data.plot(['f'],
                outfile = 'frequenz.png',
                specialdict = {'f':[44184.8,44185.8]},
                plottitle = 'Station Graz - Feldstaerke 05.08.2013',
                bgcolor='white')
        """

        import magpy.mpplot as mp
        if keys == None:
            keys = []
        mp.plot(self, variables=keys, **kwargs)


    def powerspectrum(self, key, debugmode=None, outfile=None, fmt=None, axes=None, title=None,**kwargs):
        """
    DEFINITION:
        Calculating the power spectrum
        following the numpy fft example

    PARAMETERS:
    Variables:
        - key:          (str) Key to analyse
    Kwargs:
        - axes:         (?) ?
        - debugmode:    (bool) Variable to show steps
        - fmt:          (str) Format of outfile, e.g. "png"
        - outfile:      (str) Filename to save plot to
        - title:        (str) Title to display on plot
        - marks:        (dict) add some text to the plot
        - returndata:   (bool) return freq and asd
        - freqlevel:    (float) print noise level at that frequency

    RETURNS:
        - plot:         (matplotlib plot) A plot of the powerspectrum

    EXAMPLE:
        >>> data_stream.powerspectrum('x')

    APPLICATION:
        >>> from magpy.stream import read
        1. Requires DataStream object:
        >>> data_path = '/usr/lib/python2.7/magpy/examples/*'
        >>> data = read(path_or_url=data_path,
                        starttime='2013-06-10 00:00:00',
                        endtime='2013-06-11 00:00:00')
        2. Call for data stream:
        >>> data.powerspectrum('f',
                        title='PSD of f', marks={'day':0.000011574},
                        outfile='ps.png')
        """
        if debugmode:
            print("Start powerspectrum at %s" % datetime.utcnow())

        noshow = kwargs.get('noshow')
        returndata = kwargs.get('returndata')
        marks = kwargs.get('marks')
        freqlevel = kwargs.get('freqlevel')

        if noshow:
            show = False
        else:
            show = True

        dt = self.get_sampling_period()*24*3600

        if not len(self) > 0:
            logger.error("Powerspectrum: Stream of zero length -- aborting")
            raise Exception("Can't analyse stream of zero length!")

        t = np.asarray(self._get_column('time'))
        val = np.asarray(self._get_column(key))
        mint = np.min(t)
        tnew, valnew = [],[]

        nfft = int(nearestPow2(len(t)))
        #print "NFFT:", nfft

        if nfft > len(t):
            nfft = int(nearestPow2(len(t) / 2.0))

        #print "NFFT now:", nfft

        for idx, elem in enumerate(val):
            if not isnan(elem):
                tnew.append((t[idx]-mint)*24*3600)
                valnew.append(elem)

        tnew = np.asarray(tnew)
        valnew = np.asarray(valnew)

        if debugmode:
            print("Extracted data for powerspectrum at %s" % datetime.utcnow())

        #freq = np.fft.fftfreq(tnew.shape[-1],dt)
        #freq = freq[range(len(tnew)/2)] # one side frequency range
        #freq = freq[1:]
        #print "Maximum frequency:", max(freq)
        #s = np.fft.fft(valnew)
        #s = s[range(len(valnew)/2)] # one side data range
        #s = s[1:]
        #ps = np.real(s*np.conjugate(s))

        if not axes:
            fig = plt.figure()
            ax = fig.add_subplot(111)
        else:
            ax = axes

        psdm = mlab.psd(valnew, nfft, 1/dt)
        asdm = np.sqrt(psdm[0])
        freqm = psdm[1]

        ax.loglog(freqm, asdm,'b-')

        #print "Maximum frequency:", max(freqm)

        if freqlevel:
            val, idx = find_nearest(freqm, freqlevel)
            print("Maximum Noise Level at %s Hz: %s" % (val,asdm[idx]))

        if not marks:
            pass
        else:
            for elem in marks:
                ax.annotate(elem, xy=(marks[elem],min(asdm)),
                                xytext=(marks[elem],max(asdm)-(max(asdm)-min(asdm))*0.3),
                                bbox=dict(boxstyle="round", fc="0.95", alpha=0.6),
                                arrowprops=dict(arrowstyle="->",
                                shrinkA=0, shrinkB=1,
                                connectionstyle="angle,angleA=0,angleB=90,rad=10"))

        try:
            unit = self.header['unit-col-'+key]
        except:
            unit = 'unit'

        ax.set_xlabel('Frequency [Hz]')
        ax.set_ylabel(('Amplitude spectral density [%s/sqrt(Hz)]') % unit)
        if title:
            ax.set_title(title)

        if debugmode:
            print("Finished powerspectrum at %s" % datetime.utcnow())

        if outfile:
            if fmt:
                fig.savefig(outfile, format=fmt)
            else:
                fig.savefig(outfile)
        elif returndata:
            return freqm, asdm
        elif show:
            plt.show()
        else:
            return fig


    def randomdrop(self,percentage=None,fixed_indicies=None):
                """
                DESCRIPTION:
                    Method to randomly drop one line from data. If percentage is
                    given, then lines according to this percentage are dropped.
                    This corresponds to a jackknife and d-jackknife respectively.
                PARAMETER:
                    percentage     (float)  provide a percentage value to be dropped (1-99)
                    fixed_indicies (list)   e.g. [0,1] provide a list of indicies
                                            which will not be dropped
                RETURNS:
                    DataStream
                APPLICATION:
                    >>> newstream = stream.randomdrop(percentage=10,fixed_indicies=[0,len(means.ndarray[0])-1])
                """
                import random
                def makeDrippingBucket(lst):
                    bucket = lst
                    if len(bucket) == 0:
                        return []
                    else:
                        random_index = random.randrange(0,len(bucket))
                        del bucket[random_index]
                        return bucket

                if len(self.ndarray[0]) < 1:
                    return self
                if percentage:
                    if percentage > 99:
                        percentage = 99
                    if percentage < 1:
                        percentage = 1
                ns = self.copy()
                if fixed_indicies:
                    # TODO assert list
                    pass
                if not percentage:
                    newlen = len(ns.ndarray[0]) -1
                else:
                    newlen = int(np.round(len(ns.ndarray[0])-len(ns.ndarray[0])*percentage/100.,0))
                # Index list of stream
                indexlst = [idx for idx, el in enumerate(ns.ndarray[0])]
                #print len(indexlst), newlen
                while len(indexlst) > newlen:
                    indexlst = makeDrippingBucket(indexlst)
                    if fixed_indicies:
                        for el in fixed_indicies:
                            if not el in indexlst:
                                indexlst.append(el)
                #print "Here", len(indexlst)
                for idx,ar in enumerate(ns.ndarray):
                    if len(ar) > 0:
                        #print ar, indexlst
                        newar = ar[indexlst]
                        ns.ndarray[idx] = newar
                return ns


    def remove(self, starttime=None, endtime=None):
        """
    DEFINITION:
        Removing dates inside of range between start- and endtime.
        (Does the exact opposite of self.trim().)

    PARAMETERS:
    Variables:
        - starttime:    (datetime/str) Start of period to trim with
        - endtime:      (datetime/str) End of period to trim to

    RETURNS:
        - stream:       (DataStream object) Stream with data between
                        starttime and endtime removed.

    EXAMPLE:
        >>> data = data.trim(starttime, endtime)

    APPLICATION:
        """

        if starttime and endtime:
            if self._testtime(starttime) > self._testtime(endtime):
                logger.error('Trim: Starttime (%s) is larger than endtime (%s).' % (starttime,endtime))
                raise ValueError("Starttime is larger than endtime.")

        logger.info('Remove: Started from %s to %s' % (starttime,endtime))

        cutstream = DataStream()
        cutstream.header = self.header
        cutstream.ndarray = self.ndarray
        starttime = self._testtime(starttime)
        endtime = self._testtime(endtime)
        stval = 0

        if len(cutstream.ndarray[0]) > 0:
            timearray = self.ndarray[0]
            st = (np.abs(timearray.astype(float)-date2num(starttime))).argmin() - 1
            ed = (np.abs(timearray.astype(float)-date2num(endtime))).argmin() + 1
            if starttime < num2date(cutstream.ndarray[0][0]):
                st = 0
            if endtime > num2date(cutstream.ndarray[0][-1]):
                ed = len(cutstream.ndarray[0])
            dropind = [i for i in range(st,ed)]
            for index,key in enumerate(KEYLIST):
                if len(cutstream.ndarray[index])>0:
                    cutstream.ndarray[index] = np.delete(cutstream.ndarray[index], dropind)
        else:
            for idx, elem in enumerate(self):
                newline = LineStruct()
                if not isnan(elem.time):
                    newline.time = elem.time
                    if elem.time <= date2num(starttime) or elem.time > date2num(endtime):
                        for key in KEYLIST:
                            exec('newline.'+key+' = elem.'+key)
                    cutstream.add(newline)

        return cutstream


    def remove_flagged(self, **kwargs):
        """
    DEFINITION:
        remove flagged data from stream:
        Flagged values are replaced by NAN values. Therefore the stream's length is not changed.
        Flags are defined by integers (0 normal, 1 automatically marked, 2 to be kept,
        3 to be removed, 4 special)
    PARAMETERS:
        Kwargs:
            - keys:             (list) keys (string list e.g. 'f') default=FLAGKEYLIST
            - flaglist:         (list) default=[1,3] defines integer codes to be removed
    RETURNS:
        - stream:               (DataStream Object) Stream with flagged data replaced by NAN.

    EXAMPLE:
        >>> newstream = stream.remove_flagged()

    APPLICATION:
        """

        # Defaults:
        flaglist = kwargs.get('flaglist')
        keys = kwargs.get('keys')

        if not flaglist:
            flaglist = [1,3]
        if not keys:
            keys = FLAGKEYLIST

        # Converting elements of flaglist to strings
        flaglist = [str(fl) for fl in flaglist]
        array = self.ndarray
        ndtype = False
        if len(self.ndarray[0]) > 0:
            flagind = KEYLIST.index('flag')
            commind = KEYLIST.index('comment')
            ndtype = True

        for key in keys:
            pos = KEYLIST.index(key)
            liste = []
            emptyelem = LineStruct()
            if ndtype:
                # get indicies of all non-empty flag contents
                indlst = [i for i,el in enumerate(self.ndarray[flagind]) if not el in ['','-']]
                for i in indlst:
                    try:
                        #if len(array[pos]) > 0:
                        flagls = list(self.ndarray[flagind][i])
                        flag = flagls[pos]
                        if flag in flaglist:
                           array[pos][i] = float("nan")
                    except:
                        #print("stream remove_flagged: index error: indlst {}, pos {}, length flag colum {}".format(len(indlst), pos, len(self.ndarray[flagind])))
                        pass
                liste = [LineStruct()]
            else:
                for elem in self:
                    fllst = list(elem.flag)
                    try: # test whether useful flag is present: flaglst length changed during the program development
                        flag = int(fllst[pos])
                    except:
                        flag = 0
                    if not flag in flaglist:
                        liste.append(elem)
                    else:
                        setattr(elem, key, float("nan"))
                        #exec('elem.'+key+' = float("nan")')
                        liste.append(elem)

        #liste = [elem for elem in self if not elem.flag[pos] in flaglist]

        if ndtype:
            #-> Necessary to consider shape (e.g.BLV data)
            newar = [np.asarray([]) for el in KEYLIST]
            for idx,el in enumerate(array):
                if idx == flagind:
                    pass 
                elif idx == commind:
                    pass
                else:
                    newar[idx] = array[idx]
        else:
            newar = list(self.ndarray)

            # Drop contents of flag and comment column -> didn't work for BLV data because of shape
            # changed for 0.3.99
            #array[flagind] = np.asarray([])
            #array[commind] = np.asarray([])

        return DataStream(liste, self.header,np.asarray(newar))



    def remove_outlier(self, **kwargs):
        """
    DEFINITION:
        Flags outliers in data, uses quartiles.
        Notes: Position of flag in flagstring:
        f (intensity): pos 0
        x,y,z (vector): pos 1
        other (vector): pos 2
        Position of flag in flagstring
        x : pos 0
        y : pos 1
        z : pos 2
        f : pos 3
        t1 : pos 4
        t2 : pos 5
        var1 : pos 6
        var2: pos 7
        Coding : 0 take, 1 remove, 2 force take, 3 force remove
        Example:
        0000000, 0001000, etc
        012 = take f, automatically removed v, and force use of other
        300 = force remove f, take v, and take other

    PARAMETERS:
    Variables:
        - None.
    Kwargs:
        - keys:         (list) List of keys to evaluate. Default=['f']
        - threshold:    (float) Determines threshold for outliers.
                        1.5 = standard
                        5 = keeps storm onsets in
                        4 = Default as comprimise.
        - timerange:    (timedelta Object) Time range. Default = timedelta(hours=1)
        - markall :     marks all data except forcing has already been applied
        - stdout:        prints removed values to stdout
    RETURNS:
        - stream:       (DataStream Object) Stream with flagged data.

    EXAMPLE:

        >>> stream.remove_outlier(keys=['x','y','z'], threshold=2)

    APPLICATION:
        """
        # Defaults:
        timerange = kwargs.get('timerange')
        threshold = kwargs.get('threshold')
        keys = kwargs.get('keys')
        markall = kwargs.get('markall')
        stdout = kwargs.get('stdout')
        if not timerange:
            timerange = timedelta(hours=1)
        if not keys:
            keys = ['f']
        if not threshold:
            threshold = 4.0
        if not stdout:
            stdout = False
        # Position of flag in flagstring
        # f (intensity): pos 0
        # x,y,z (vector): pos 1
        # other (vector): pos 2

        logger.info('remove_outlier: Starting outlier removal.')

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True
            arraytime = self.ndarray[0]
            flagind = KEYLIST.index('flag')
            commentind = KEYLIST.index('comment')
            print ("Found ndarray - using flag_outlier instead")
            return self.flag_outlier(**kwargs)

        elif len(self) > 1:
            arraytime = self._get_column('time')
        else:
            logger.warning('remove_outlier: No data - Stopping outlier removal.')
            return self

        # Working non-destructive
        restream = self.copy()

        # Start here with for key in keys:
        for key in keys:
            flagpos = FLAGKEYLIST.index(key)

            st,et = self._find_t_limits()
            st = date2num(st)
            et = date2num(et)
            at = date2num((num2date(st).replace(tzinfo=None)) + timerange)
            incrt = at-st

            newst = DataStream()
            while st < et:
                tmpar, idxst = find_nearest(arraytime,st)
                tmpar, idxat = find_nearest(arraytime,at)
                if idxat == len(arraytime)-1:
                    idxat = len(arraytime)
                st = at
                at += incrt

                if ndtype:
                    ind = KEYLIST.index(key)
                    lstpart = self.ndarray[ind][idxst:idxat].astype(float)
                    print(lstpart)
                    print(np.isnan(lstpart))
                    selcol = lstpart[~np.isnan(lstpart)]
                else:
                    lstpart = self[idxst:idxat]
                    # changed at 28.08.2014
                    #selcol = [eval('row.'+key) for row in lstpart]
                    selcol = [eval('row.'+key) for row in lstpart if not isnan(eval('row.'+key))]


                try:
                    q1 = stats.scoreatpercentile(selcol,25)
                    q3 = stats.scoreatpercentile(selcol,75)
                    iqd = q3-q1
                    md = np.median(selcol)
                    whisker = threshold*iqd
                except:
                    try:
                        md = np.median(selcol)
                        whisker = md*0.005
                    except:
                        logger.warning("remove_outlier: Eliminate outliers produced a problem: please check.")
                        pass

                if ndtype:
                    # XXX DOES NOT WORK, TODO
                    for i in range(idxst,idxat):
                        if row.flag == '' or row.flag == '0000000000000000-' or row.flag == '-' or row.flag == '-0000000000000000':
                            row.flag = '-' * len(FLAGKEYLIST)
                        if row.comment == '-':
                            row.comment = ''
                else:
                    for elem in lstpart:
                        row = LineStruct()
                        row = elem
                        if row.flag == '' or row.flag == '0000000000000000-' or row.flag == '-' or row.flag == '-0000000000000000':
                            #row.flag = '0000000000000000-'
                            row.flag = '-----------------'
                        if row.comment == '-':
                            row.comment = ''
                        if isNumber(row.flag): # if somehow the flag has been transfered to a number - create a string again
                            num = str(int(row.flag))[:-1]
                            row.flag = num+'-'
                        if not md-whisker < eval('elem.'+key) < md+whisker:
                            fllist = list(row.flag)
                            #print "Found", key
                            if len(fllist) >= flagpos:
                                fllist = np.asarray(fllist, dtype=object)
                                if not fllist[flagpos] in [1,2,3,4] :
                                    if markall:
                                        #print "mark"
                                        fl = []
                                        for j,f in enumerate(FLAGKEYLIST):
                                            if f in keys:
                                                fl.append('1')
                                            else:
                                                fl.append('-')
                                        for idx, el in enumerate(fllist):
                                            if el in [1,2,3,4]:
                                                fl[idx] = el
                                        fllist = fl
                                    fllist[flagpos] = '1'
                                    row.flag=''.join(fllist)
                                    row.comment = "aof - threshold: %s, window: %s sec" % (str(threshold), str(timerange.total_seconds()))
                                    #print row.flag, key
                                    if not isnan(eval('elem.'+key)):
                                        infoline = "remove_outlier: at %s - removed %s (= %f)" % (str(num2date(elem.time)),key, eval('elem.'+key))
                                        logger.info(infoline)
                                        if stdout:
                                            print(infoline)
                        else:
                            fllist = list(row.flag)
                            if len(fllist) >= flagpos:
                                if row.flag == '':
                                    pass
                                elif fllist[flagpos] == '-':
                                    testlst = [el for el in fllist if el in ['0','1','2','3','4']]
                                    if not len(testlst) > 0:
                                        row.flag = ''
                                else:
                                    pass
                        newst.add(row)

        logger.info('remove_outlier: Outlier removal finished.')

        if ndtype:
            return restream
        else:
            return DataStream(newst, self.header, self.ndarray)


    def resample(self, keys, debugmode=False,**kwargs):
        """
    DEFINITION:
        Uses Numpy interpolate.interp1d to resample stream to requested period.

        Two methods:
           fast: is only valid if time stamps at which resampling is conducted are part of the 
                 original time series. e.g. org = second (58,59,0,1,2) resampled at 0
           slow: general method if time stamps for resampling are not contained (e.g. 58.23, 59.24, 0.23,...) 
                 resampled at 0
    PARAMETERS:
    Variables:
        - keys:         (list) keys to be resampled.
    Kwargs:
        - period:       (float) sampling period in seconds, e.g. 5s (0.2 Hz).
        - fast:         (bool) use fast approximation
        - startperiod:  (integer) starttime in sec (e.g. 60 each minute, 900 each quarter hour
        - offset:       (integer) starttime in sec (e.g. 60 each minute, 900 each quarter hour

    RETURNS:
        - stream:       (DataStream object) Stream containing resampled data.

    EXAMPLE:
        >>> resampled_stream = pos_data.resample(['f'],period=1)

    APPLICATION:
        """

        period = kwargs.get('period')
        fast = kwargs.get('fast')
        offset = kwargs.get('offset')

        if not period:
            period = 60.

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True

        sp = self.samplingrate()

        logger.info("resample: Resampling stream of sampling period %s to period %s." % (sp,period))

        logger.info("resample: Resampling keys %s " % (','.join(keys)))

        # Determine the minimum time
        t_min,t_max = self._find_t_limits()
        t_start = t_min

        if offset:
            t_min = ceil_dt(t_min,period)
            if t_min - offset > t_start:
                t_min = t_min -offset
            else:
                t_min = t_min +offset
            startperiod, line = self.findtime(t_min)
        else:
            t_min = ceil_dt(t_min,period)
            startperiod, line = self.findtime(t_min)

        if fast:   # To be done if timesteps are at period timesteps
            try:
                logger.info("resample: Using fast algorithm.")
                si = timedelta(seconds=sp)
                sampling_period = si.seconds

                if period <= sampling_period:
                    logger.warning("resample: Resampling period must be larger or equal than original sampling period.")
                    return self

                if debugmode:
                    print ("Trying fast algorythm")
                    print ("Projected period and Sampling period:", period, sampling_period)
                if not line == [] or ndtype: # or (ndtype and not line == []):
                    xx = int(np.round(period/sampling_period))
                    if ndtype:
                        newstream = DataStream([LineStruct()],{},np.asarray([]))
                        newstream.header = self.header
                        lst = []
                        for ind,elem in enumerate(self.ndarray):
                            if debugmode:
                                print ("dealing with column", ind, elem)
                            if len(elem) > 0:
                                lst.append(np.asarray(elem[startperiod::xx]))
                            else:
                                lst.append(np.asarray([]))
                        newstream.ndarray = np.asarray(lst)
                    else:
                        newstream = DataStream([],{},np.asarray([[] for el in KEYLIST]))
                        newstream.header = self.header
                        for line in self[startperiod::xx]:
                            newstream.add(line)
                    newstream.header['DataSamplingRate'] = str(period) + ' sec'
                    return newstream
                logger.warning("resample: Fast resampling failed - switching to slow mode")
            except:
                logger.warning("resample: Fast resampling failed - switching to slow mode")
                pass

        # This is done if timesteps are not at period intervals
        # -----------------------------------------------------

        if debugmode:
            print ("General -slow- resampling")
        # Create a list containing time steps
        #t_max = num2date(self._get_max('time'))
        t_list = []
        time = t_min
        while time <= t_max:
           t_list.append(date2num(time))
           time = time + timedelta(seconds=period)

        # Compare length of new time list with old timelist
        # multiplicator is used to check whether nan value is at the corresponding position of the orgdata file - used for not yet completely but sufficiently correct missing value treatment
        if not len(t_list) > 0:
            return DataStream()
        multiplicator = float(self.length()[0])/float(len(t_list))
        logger.info("resample a: {},{},{}".format(float(self.length()[0]), float(len(t_list)),startperiod))

        #print ("Times:", self.ndarray[0][0],self.ndarray[0][-1],t_list[0],t_list[-1]) 
        stwithnan = self.copy()

        # What is this good for (leon 17.04.2019)???
        tmp = self.trim(starttime=736011.58337400458,endtime=736011.59721099539)
        logger.info("resample test: {}".format(tmp.ndarray))

        #tcol = stwithnan.ndarray[0]

        res_stream = DataStream()
        res_stream.header = self.header
        array=[np.asarray([]) for elem in KEYLIST]
        if ndtype:
            array[0] = np.asarray(t_list)
            res_stream.add(LineStruct())
        else:
            for item in t_list:
                row = LineStruct()
                row.time = item
                res_stream.add(row)

        for key in keys:
            if debugmode:
                print ("Resampling:", key)
            if key not in KEYLIST[1:16]:
                logger.warning("resample: Key %s not supported!" % key)

            index = KEYLIST.index(key)
            try:
                #print (len(self._get_column(key)), multiplicator)
                int_data = self.interpol([key],kind='linear')#'cubic')
                int_func = int_data[0]['f'+key]
                int_min = int_data[1]
                int_max = int_data[2]

                key_list = []
                for ind, item in enumerate(t_list):
                    # normalized time range between 0 and 1
                    functime = (item - int_min)/(int_max - int_min)
                    # check whether original value is np.nan (as interpol method does not account for that)
                    # exact but slowly: idx = np.abs(tcol-item).argmin()
                    #                   orgval = stwithnan.ndarray[index][idx]
                    # reduce the index range as below
                    if ndtype:
                        if int(ind*multiplicator) <= len(self.ndarray[index]):
                            #orgval = self.ndarray[index][int(ind*multiplicator)]
                            estimate = False
                            # Please note: here a two techniques (exact and estimate)
                            # Speeddiff (example data set (500000 data points)
                            # Exact:    7.55 sec (including one minute filter)
                            # Estimate: 7.15 sec
                            if estimate:
                                orgval = stwithnan.ndarray[index][int(ind*multiplicator+startperiod)] # + offset
                            else:
                                # Exact solution:
                                mv = int(ind*multiplicator+startperiod)
                                stv = mv-int(20*multiplicator)
                                if stv < 0:
                                    stv = 0
                                etv = mv+int(20*multiplicator)
                                if etv >= len(self.ndarray[index]):
                                    etv = len(self.ndarray[index])
                                subar = stwithnan.ndarray[0][stv:etv]
                                idx = (np.abs(subar-item)).argmin()
                                #subar = stwithnan.ndarray[index][stv:etv]
                                orgval = stwithnan.ndarray[index][stv+idx] # + offset
                                #if item > 736011.58337400458 and item < 736011.59721099539:
                                #   print ("Found", item, stv+idx, idx, orgval)
                                #if np.isnan(orgval):
                                #    print (stv+idx, stv, etv)
                        else:
                            print("Check Resampling method")
                            orgval = 1.0
                    else:
                        orgval = getattr(stwithnan[int(ind*multiplicator+startperiod)],key)
                    tempval = np.nan
                    # Not a safe fix, but appears to cover decimal leftover problems
                    # (e.g. functime = 1.0000000014, which raises an error)
                    if functime > 1.0:
                        functime = 1.0
                    if not isnan(orgval):
                        tempval = int_func(functime)
                    key_list.append(float(tempval))

                if ndtype:
                    array[index] = np.asarray(key_list)
                else:
                    res_stream._put_column(key_list,key)
            except:
                logger.error("resample: Error interpolating stream. Stream either too large or no data for selected key")

        res_stream.ndarray = np.asarray(array)

        logger.info("resample: Data resampling complete.")
        #return DataStream(res_stream,self.headers)
        res_stream.header['DataSamplingRate'] = str(period) + ' sec'
        return res_stream


    def rotation(self,**kwargs):
        """
    DEFINITION:
        Rotation matrix for rotating x,y,z to new coordinate system xs,ys,zs using angles alpha and beta

    PARAMETERS:
    Variables:
    Kwargs:
        - alpha:        (float) The horizontal rotation in degrees
        - beta:         (float) The vertical rotation in degrees
        - keys:         (list) provide an alternative vector to rotate - default is ['x','y','z']
                               keys are only supported from 1.0 onwards (ndarray)

    RETURNS:
        - self:         (DataStream) The rotated stream

    EXAMPLE:
        >>> data.rotation(alpha=2.74)

    APPLICATION:

        """

        unit = kwargs.get('unit')
        alpha = kwargs.get('alpha')
        beta = kwargs.get('beta')
        keys =  kwargs.get('keys')

        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        if not alpha:
            alpha = 0.
        if not beta:
            beta = 0.
        if not keys:
            keys = ['x','y','z']

        if not len(keys) == 3:
            logger.error('rotation: provided keylist need to have three components.')
            return self

        logger.info('rotation: Applying rotation matrix.')

        """
        a[0][0] = cos(p)*cos(b);
        a[0][1] = -sin(b);
        a[0][2] = sin(p)*cos(b);
        a[1][0] = cos(p)*sin(b);
        a[1][1] = cos(b);
        a[1][2] = sin(p)*sin(b);
        a[2][0] = -sin(p);
        a[2][1] = 0.0;
        a[2][2] = cos(p);

        xyz.l = ortho.l*a[0][0]+ortho.m*a[0][1]+ortho.n*a[0][2];
        xyz.m = ortho.l*a[1][0]+ortho.m*a[1][1]+ortho.n*a[1][2];
        xyz.n = ortho.l*a[2][0]+ortho.m*a[2][1]+ortho.n*a[2][2];
        """
        ind1 = KEYLIST.index(keys[0])
        ind2 = KEYLIST.index(keys[1])
        ind3 = KEYLIST.index(keys[2])

        if len(self.ndarray[0]) > 0:
            if len(self.ndarray[ind1]) > 0 and len(self.ndarray[ind2]) > 0 and len(self.ndarray[ind3]) > 0:
                ra = np.pi*alpha/(180.*ang_fac)
                rb = np.pi*beta/(180.*ang_fac)
                self.ndarray[ind1] = self.ndarray[ind1].astype(float)*np.cos(rb)*np.cos(ra)-self.ndarray[ind2].astype(float)*np.sin(ra)+self.ndarray[ind3].astype(float)*np.sin(rb)*np.cos(ra)
                self.ndarray[ind2] = self.ndarray[ind1].astype(float)*np.cos(rb)*np.sin(ra)+self.ndarray[ind2].astype(float)*np.cos(ra)+self.ndarray[ind3].astype(float)*np.sin(rb)*np.sin(ra)
                self.ndarray[ind3] = -self.ndarray[ind1].astype(float)*np.sin(rb)+self.ndarray[ind3].astype(float)*np.cos(rb)

        """
        for elem in self:
            ra = np.pi*alpha/(180.*ang_fac)
            rb = np.pi*beta/(180.*ang_fac)
            # Testing the conservation of f ##### Error corrected in May 2014 by leon
            #fbefore = sqrt(elem.x**2+elem.y**2+elem.z**2)
            xs = elem.x*np.cos(rb)*np.cos(ra)-elem.y*np.sin(ra)+elem.z*np.sin(rb)*np.cos(ra)
            ys = elem.x*np.cos(rb)*np.sin(ra)+elem.y*np.cos(ra)+elem.z*np.sin(rb)*np.sin(ra)
            zs = -elem.x*np.sin(rb)+elem.z*np.cos(rb)
            #fafter = sqrt(xs**2+ys**2+zs**2)
            #print "f:", fbefore,fafter,fbefore-fafter

            elem.x = xs
            elem.y = ys
            elem.z = zs
        """
        logger.info('rotation: Finished reorientation.')

        return self


    def scale_correction(self, keys, scales, **kwargs):
        """
        DEFINITION:
            multiplies the selected keys by the given scale values
        PARAMETERS:
         Kwargs:
            - offset:  (array) containing constant offsets for the given keys
        RETURNS:
            - DataStream

        EXAMPLES:
            >>> stream = stream.scale_correction(['x','y','z'],[1,0.988,1])
        """

        print("Function will be removed - use e.g. self.multiply({'y': 0.988}) instead")

        # Take care: if there is only 0.1 nT accurracy then there will be a similar noise in the deltaF signal

        offset = kwargs.get('offset')
        if not offset:
            offset = [0]*len(keys)
        else:
            if not len(offset) == len(keys):
                logger.error('scale_correction: offset with wrong dimension given - needs to have the same length as given keys - returning stream without changes')
                return self

        try:
            assert len(self) > 0
        except:
            logger.error('scale_correction: empty stream - aborting')
            return self

        offsetlst = []
        for key in KEYLIST:
            if key in keys:
                pos = keys.index(key)
                offsetlst.append(offset[pos])
            else:
                offsetlst.append(0.0)

        logger.info('scale_correction:  --- Scale correction started at %s ' % str(datetime.now()))
        for elem in self:
            for i,key in enumerate(keys):
                exec('elem.'+key+' = (elem.'+key+'+offset[i]) * scales[i]')

        scalelst = []
        for key in KEYLIST:
            if key in keys:
                pos = keys.index(key)
                scalelst.append(scales[pos])
            else:
                scalelst.append(1.)

        #print '_'.join(map(str,offsetlst)), scalelst
        self.header['DataScaleValues'] = '_'.join(map(str,scalelst))
        self.header['DataOffsets'] = '_'.join(map(str,offsetlst))

        logger.info('scale_correction:  --- Scale correction finished at %s ' % str(datetime.now()))

        return self


    def selectkeys(self, keys, **kwargs):
        """
    DEFINITION:
        Take data stream and remove all except the provided keys from ndarray
    RETURNS:
        - self:         (DataStream) with ndarray limited to keys

    EXAMPLE:
        >>> keydata = fulldata.selectkeys(['x','y','z'])

    APPLICATION:

        """
        noflags = kwargs.get('noflags')

        stream = self.copy()

        if not 'time' in keys:
            ti = ['time']
            ti.extend(keys)
            keys = ti

        if len(stream.ndarray[0]) > 0:
            # Check for flagging and comment column
            if not noflags:
                flagidx = KEYLIST.index('flag')
                commentidx = KEYLIST.index('comment')
                if len(stream.ndarray[flagidx]) > 0:
                    keys.append('flag')
                if len(stream.ndarray[commentidx]) > 0:
                    keys.append('comment')

            # Remove all missing
            for idx, elem in enumerate(stream.ndarray):
                if not KEYLIST[idx] in keys:
                    stream.ndarray[idx] = np.asarray([])
            return stream
        else:
            return stream

    def smooth(self, keys=None, **kwargs):
        """
    DEFINITION:
        Smooth the data using a window with requested size.
        (taken from Cookbook/Signal Smooth)
        This method is based on the convolution of a scaled window with the signal.
        The signal is prepared by introducing reflected copies of the signal
        (with the window size) in both ends so that transient parts are minimized
        in the begining and end part of the output signal.

    PARAMETERS:
    Variables:
        - keys:         (list) List of keys to smooth
    Kwargs:
        - window_len:   (int,odd) dimension of the smoothing window
        - window:       (str) the type of window from 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'. A flat window will produce a moving average smoothing.
        (See also:
        numpy.hanning, numpy.hamming, numpy.bartlett, numpy.blackman, numpy.convolve
        scipy.signal.lfilter)

    RETURNS:
        - self:         (DataStream) The smoothed signal

    EXAMPLE:
        >>> nice_data = bad_data.smooth(['x','y','z'])
        or
        >>> t=linspace(-2,2,0.1)
        >>> x=sin(t)+randn(len(t))*0.1
        >>> y=smooth(x)

    APPLICATION:

    TODO:
        the window parameter could be the window itself if an array instead of a string
        """
        # Defaults:
        window_len = kwargs.get('window_len')
        window = kwargs.get('window')
        if not window_len:
            window_len = 11
        if not window:
            window='hanning'
        if not keys:
            keys=self._get_key_headers(numerical=True)

        window_len = int(window_len)

        ndtype = False
        if len(self.ndarray[0])>0:
            ndtype = True

        logger.info('smooth: Start smoothing (%s window, width %d) at %s' % (window, window_len, str(datetime.now())))

        for key in keys:
            if key in NUMKEYLIST:

                if ndtype:
                    ind = KEYLIST.index(key)
                    x = self.ndarray[ind]
                else:
                    x = self._get_column(key)
                x = maskNAN(x)

                if x.ndim != 1:
                    logger.error("smooth: Only accepts 1 dimensional arrays.")
                if x.size < window_len:
                    print(x.size, window_len)
                    logger.error("smooth: Input vector needs to be bigger than window size.")
                if window_len<3:
                    return x
                if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
                    logger.error("smooth: Window is none of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'")
                    logger.debug("smooth: You entered string %s as a window." % window)

                s=np.r_[x[window_len-1:0:-1],x,x[-1:-window_len:-1]]
                #print(len(s))
                if window == 'flat': #moving average
                    w=np.ones(window_len,'d')
                else:
                    w=eval('np.'+window+'(window_len)')

                y=np.convolve(w/w.sum(),s,mode='valid')

                if ndtype:
                    self.ndarray[ind] = np.asarray(y[(int(window_len/2)):(len(x)+int(window_len/2))])
                else:
                    self._put_column(y[(int(window_len/2)):(len(x)+int(window_len/2))],key)
            else:
                logger.error("Column key %s not valid." % key)


        logger.info('smooth: Finished smoothing at %s' % (str(datetime.now())))

        return self


    def spectrogram(self, keys, per_lap=0.9, wlen=None, log=False,
                    outfile=None, fmt=None, axes=None, dbscale=False,
                    mult=8.0, cmap=None, zorder=None, title=None, show=True,
                    sphinx=False, clip=[0.0, 1.0], **kwargs):
        """
        Creates a spectrogram plot of selected keys.
        Parameter description at function obspyspectrogram

        keywords:
        samp_rate_multiplicator: to change the frequency relative to one day (default value is Hz - 24*3600)
        samp_rate_multiplicator : sampling rate give as days -> multiplied by x to create Hz, etc: default 24, which means 1/3600 Hz
        """
        samp_rate_multiplicator = kwargs.get('samp_rate_multiplicator')

        if not samp_rate_multiplicator:
            samp_rate_multiplicator = 24*3600

        t = self._get_column('time')

        if not len(t) > 0:
            logger.error('Spectrogram: stream of zero length -- aborting')
            return

        for key in keys:
            val = self._get_column(key)
            val = maskNAN(val)
            dt = self.get_sampling_period()*(samp_rate_multiplicator)
            Fs = float(1.0/dt)
            self.obspyspectrogram(val,Fs, per_lap=per_lap, wlen=wlen, log=log,
                    outfile=outfile, fmt=fmt, axes=axes, dbscale=dbscale,
                    mult=mult, cmap=cmap, zorder=zorder, title=title, show=show,
                    sphinx=sphinx, clip=clip)

    def steadyrise(self, key, timewindow, **kwargs):
        """
        DEFINITION:
            Method determines the absolute increase within a data column
            and a selected time window
            neglecting any resets and decreasing trends
            - used for analyzing some rain senors
        PARAMETERS:
            key:           (key) column on which the process is performed
            timewindow:    (timedelta) define the window e.g. timedelta(minutes=15)
        Kwargs:
            sensitivitylevel:    (float) define a difference which two successive
                                         points need to exceed to be used
                                         (useful if you have some numeric noise)

        RETURNS:
            - column:   (array) column with length of th stream
                                   containing timewindow blocks of stacked data.

        EXAMPLE:
            >>>  col = stream.steadyrise('t1', timedelta(minutes=60),sensitivitylevel=0.002)


        """
        sensitivitylevel = kwargs.get('sensitivitylevel')

        prevval = 9999999999999.0
        stacked = 0.0
        count = 0
        rescol = []
        testcol = []

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True


        ind = KEYLIST.index(key)
        if ndtype and len(self.ndarray[ind]) > 0:
            startt = num2date(np.min(self.ndarray[0]))
            for idx,val in enumerate(self.ndarray[ind]):
                if num2date(self.ndarray[0][idx]) < startt+timewindow:
                    if prevval < val:
                        diff = val-prevval
                        if not sensitivitylevel:
                            stacked += val-prevval
                        elif diff > sensitivitylevel:
                            stacked += val-prevval
                    count += 1
                else:
                    for i in range(count+1):
                        rescol.append(stacked)
                    count = 0
                    # now put that results back to a column
                    startt = startt+timewindow
                    stacked = 0.0
                prevval = val

        elif not ndtype:
            startt = num2date(self[0].time)
            for elem in self:
                testcol.append(elem)
                if num2date(elem.time) < startt+timewindow:
                    val = eval('elem.'+key)
                    if prevval < val:
                        diff = val-prevval
                        if not sensitivitylevel:
                            stacked += val-prevval
                        elif diff > sensitivitylevel:
                            stacked += val-prevval
                    count += 1
                else:
                    for i in range(count+1):
                        rescol.append(stacked)
                    count = 0
                    # now put that results back to a column
                    startt = startt+timewindow
                    val = eval('elem.'+key)
                    stacked = 0.0
                prevval = val

        else:
            print("steadyrise: no data found in selected column %s" % key)
            return np.asarray([])
        # Finally fill the end
        for i in range(count):
            rescol.append(stacked)

        if not len(rescol) == len(self) and not len(rescol) == len(self.ndarray[0]) :
            logger.error('steadrise: An error leading to unequal lengths has been encountered')
            return []

        return np.asarray(rescol)

    def stereoplot(self, **kwargs):
        """
            DEFINITION:
                plots a dec and inc values in stereographic projection
                will abort if no idff typ is provided
                full circles denote positive inclinations, open negative

            PARAMETERS:
            variable:
                - stream                (DataStream) a magpy datastream object
            kwargs:
                - focus:                (string) defines the plot area - can be either:
                                            all - -90 to 90 deg inc, 360 deg dec (default)
                                            q1 - first quadrant
                                            q2 - first quadrant
                                            q3 - first quadrant
                                            q4 - first quadrant
                                            data - focus on data (if angular spread is less then 10 deg
                - groups                (KEY) - key of keylist which defines color of points
                                             (e.g. ('str2') in absolutes to select
                                             different colors for different instruments
                - legend                (bool) - draws legend only if groups is given - default True
                - legendposition    (string) - draws the legend at chosen position (e.g. "upper right", "lower center") - default is "lower left"
                - labellimit        (integer)- maximum length of label in legend
                - noshow:               (bool) don't call show at the end, just returns figure handle
                - outfile:              (string) to save the figure, if path is not existing it will be created
                - gridcolor:    (string) Define grid color e.g. '0.5' greyscale, 'r' red, etc
                - savedpi:              (integer) resolution
                - figure:               (bool) True for GUI

            REQUIRES:
                - package operator for color selection

            RETURNS:
                - plot

            ToDo:
                - add alpha 95 calc

            EXAMPLE:
                >>> stream.stereoplot(focus='data',groups='str2')

            """
        focus = kwargs.get('focus')
        groups = kwargs.get('groups')
        bgcolor  = kwargs.get('bgcolor')
        colorlist = kwargs.get('colorlist')
        outfile = kwargs.get('outfile')
        savedpi = kwargs.get('savedpi')
        gridinccolor = kwargs.get('gridinccolor')
        griddeccolor = kwargs.get('griddeccolor')
        noshow = kwargs.get('noshow')
        legend = kwargs.get('legend')
        legendposition = kwargs.get('legendposition')
        labellimit = kwargs.get('labellimit')
        figure = kwargs.get('figure')

        if not colorlist:
            colorlist = ['b','r','g','c','m','y','k']
        if not bgcolor:
            bgcolor = '#d5de9c'
        if not griddeccolor:
            griddeccolor = '#316931'
        if not gridinccolor:
            gridinccolor = '#316931'
        if not savedpi:
            savedpi = 80
        if not focus:
            focus = 'all'
        if not legend:
            legend = 'True'
        if not labellimit:
            labellimit = 11
        if not legendposition:
            legendposition = "lower left"

        if not self[0].typ == 'idff':
            logger.error('Stereoplot: you need to provide idf data')
            return

        inc = self._get_column('x')
        dec = self._get_column('y')

        col = ['']
        if groups:
            sel = self._get_column(groups)
            col = list(set(list(sel)))
            if len(col) > 7:
                col = col[:7]

        if not len(dec) == len(inc):
            logger.error('Stereoplot: check you data file - unequal inc and dec data?')
            return

        if not figure:
            fig = plt.figure()
        else:
            fig = figure
        ax = plt.gca()
        ax.cla() # clear things for fresh plot
        ax.set_aspect('equal')
        ax.set_xticklabels([])
        ax.set_yticklabels([])
        ax.set_xticks([])
        ax.set_yticks([])
        # Define koordinates:
        basic1=plt.Circle((0,0),90,color=bgcolor,fill=True)
        basic1a=plt.Circle((0,0),90,color=gridinccolor,fill=False)
        basic2=plt.Circle((0,0),30,color=gridinccolor,fill=False,linestyle='dotted')
        basic3=plt.Circle((0,0),60,color=gridinccolor,fill=False,linestyle='dotted')
        basic4=plt.Line2D([0,0],[-90,90],color=griddeccolor,linestyle='dashed')
        basic5=plt.Line2D([-90,90],[0,0],color=griddeccolor,linestyle='dashed')
        fig.gca().add_artist(basic1)
        fig.gca().add_artist(basic1a)
        fig.gca().add_artist(basic2)
        fig.gca().add_artist(basic3)
        fig.gca().add_artist(basic4)
        fig.gca().add_artist(basic5)

        for j in range(len(col)):
            color = colorlist[j]

            xpos,ypos,xneg,yneg,xabs,y = [],[],[],[],[],[]
            for i,el in enumerate(inc):
                if groups:
                    if sel[i] == col[j]:
                        coinc = 90-np.abs(el)
                        sindec = np.sin(np.pi/180*dec[i])
                        cosdec = np.cos(np.pi/180*dec[i])
                        xabs.append(coinc*sindec)
                        y.append(coinc*cosdec)
                        if el < 0:
                            xneg.append(coinc*sindec)
                            yneg.append(coinc*cosdec)
                        else:
                            xpos.append(coinc*sindec)
                            ypos.append(coinc*cosdec)
                else:
                    coinc = 90-np.abs(el)
                    sindec = np.sin(np.pi/180*dec[i])
                    cosdec = np.cos(np.pi/180*dec[i])
                    xabs.append(coinc*sindec)
                    y.append(coinc*cosdec)
                    if el < 0:
                        xneg.append(coinc*sindec)
                        yneg.append(coinc*cosdec)
                    else:
                        xpos.append(coinc*sindec)
                        ypos.append(coinc*cosdec)


            xmax = np.ceil(max(xabs))
            xmin = np.floor(min(xabs))
            xdif = xmax-xmin
            ymax = np.ceil(max(y))
            ymin = np.floor(min(y))
            ydif = ymax-ymin
            maxdif = max([xdif,ydif])
            mindec = np.floor(min(dec))
            maxdec = np.ceil(max(dec))
            mininc = np.floor(min(np.abs(inc)))
            maxinc = np.ceil(max(np.abs(inc)))

            if focus == 'data' and maxdif <= 10:
                # decs
                startdec = mindec
                decline,inclst = [],[]
                startinc = mininc
                incline = []
                while startdec <= maxdec:
                    xl = 90*np.sin(np.pi/180*startdec)
                    yl = 90*np.cos(np.pi/180*startdec)
                    decline.append([xl,yl,startdec])
                    startdec = startdec+1
                while startinc <= maxinc:
                    inclst.append(90-np.abs(startinc))
                    startinc = startinc+1

            if focus == 'all':
                ax.set_xlim((-90,90))
                ax.set_ylim((-90,90))
            if focus == 'q1':
                ax.set_xlim((0,90))
                ax.set_ylim((0,90))
            if focus == 'q2':
                ax.set_xlim((-90,0))
                ax.set_ylim((0,90))
            if focus == 'q3':
                ax.set_xlim((-90,0))
                ax.set_ylim((-90,0))
            if focus == 'q4':
                ax.set_xlim((0,90))
                ax.set_ylim((-90,0))
            if focus == 'data':
                ax.set_xlim((xmin,xmax))
                ax.set_ylim((ymin,ymax))
                #ax.annotate('Test', xy=(1.2, 25.2))
            ax.plot(xpos,ypos,'o',color=color, label=col[j][:labellimit])
            ax.plot(xneg,yneg,'o',color='white')
            ax.annotate('60', xy=(0, 30))
            ax.annotate('30', xy=(0, 60))
            ax.annotate('0', xy=(0, 90))
            ax.annotate('90', xy=(90, 0))
            ax.annotate('180', xy=(0, -90))
            ax.annotate('270', xy=(-90, 0))

        if focus == 'data' and maxdif <= 10:
            for elem in decline:
                pline = plt.Line2D([0,elem[0]],[0,elem[1]],color=griddeccolor,linestyle='dotted')
                xa = elem[0]/elem[1]*((ymax - ymin)/2+ymin)
                ya = (ymax - ymin)/2 + ymin
                annotext = "D:%i" % int(elem[2])
                ax.annotate(annotext, xy=(xa,ya))
                fig.gca().add_artist(pline)
            for elem in inclst:
                pcirc = plt.Circle((0,0),elem,color=gridinccolor,fill=False,linestyle='dotted')
                xa = (xmax-xmin)/2 + xmin
                ya = sqrt((elem*elem)-(xa*xa))
                annotext = "I:%i" % int(90-elem)
                ax.annotate(annotext, xy=(xa,ya))
                fig.gca().add_artist(pcirc)

        if groups and legend:
            handles, labels = ax.get_legend_handles_labels()
            hl = sorted(zip(handles, labels),key=operator.itemgetter(1))
            handles2, labels2 = zip(*hl)
            ax.legend(handles2, labels2, loc=legendposition)

        # 5. SAVE TO FILE (or show)
        if figure:
            return ax
        if outfile:
            path = os.path.split(outfile)[0]
            if not path == '':
                if not os.path.exists(path):
                    os.makedirs(path)
            if fmt:
                fig.savefig(outfile, format=fmt, dpi=savedpi)
            else:
                fig.savefig(outfile, dpi=savedpi)
        elif noshow:
            return fig
        else:
            plt.show()


    def trim(self, starttime=None, endtime=None, newway=False):
        """
    DEFINITION:
        Removing dates outside of range between start- and endtime.
        Returned stream has range starttime <= range < endtime.

    PARAMETERS:
    Variables:
        - starttime:    (datetime/str) Start of period to trim with
        - endtime:      (datetime/str) End of period to trim to
    Kwargs:
        - newway:       (bool) Testing method for non-destructive trimming

    RETURNS:
        - stream:       (DataStream object) Trimmed stream

    EXAMPLE:
        >>> data = data.trim(starttime, endtime)

    APPLICATION:
        """

        if starttime and endtime:
            if self._testtime(starttime) > self._testtime(endtime):
                logger.error('Trim: Starttime (%s) is larger than endtime (%s).' % (starttime,endtime))
                raise ValueError("Starttime is larger than endtime.")

        logger.info('Trim: Started from %s to %s' % (starttime,endtime))

        ndtype = False
        if self.ndarray[0].size > 0:
            ndtype = True
            self.container = [LineStruct()]

        #-ndarrray---------------------------------------
        if not newway:
            newarray = list(self.ndarray) # Converting array to list - better for append and  other item function (because its not type sensitive)
        else:
            newstream = self.copy()
            newarray = list(newstream.ndarray)
        if starttime:
            starttime = self._testtime(starttime)
            if newarray[0].size > 0:   # time column present
                idx = (np.abs(newarray[0].astype(float)-date2num(starttime))).argmin()
                # Trim should start at point >= starttime, so check:
                if newarray[0][idx] < date2num(starttime):
                    idx += 1
                for i in range(len(newarray)):
                    if len(newarray[i]) >= idx:
                        newarray[i] =  newarray[i][idx:]

        if endtime:
            endtime = self._testtime(endtime)
            if newarray[0].size > 0:   # time column present
                idx = 1 + (np.abs(newarray[0].astype(float)-date2num(endtime))).argmin() # get the nearest index to endtime and add 1 (to get lenghts correctly)
                #idx = 1+ (np.abs(self.ndarray[0]-date2num(endtime))).argmin() # get the nearest index to endtime
                if idx >= len(newarray[0]): ## prevent too large idx values
                    idx = len(newarray[0]) - 1
                while True:
                    if not float(newarray[0][idx]) < date2num(endtime) and idx != 0: # Make sure that last value is smaller than endtime
                        idx -= 1
                    else:
                        break

                #self.ndarray = list(self.ndarray)
                for i in range(len(newarray)):
                    length = len(newarray[i])
                    if length >= idx:
                        newarray[i] = newarray[i][:idx+1]
        newarray = np.asarray(newarray)
        #-ndarrray---------------------------------------


        #--------------------------------------------------
        if newway and not ndtype:
        # Non-destructive trimming of stream
            trimmedstream = DataStream()
            trimmedstream.header = self.header
            starttime = self._testtime(starttime)
            endtime = self._testtime(endtime)
            stval = 0
            for idx, elem in enumerate(self):
                newline = LineStruct()
                if not isnan(elem.time):
                    if elem.time >= date2num(starttime) and elem.time < date2num(endtime):
                        newline.time = elem.time
                        for key in KEYLIST:
                            exec('newline.'+key+' = elem.'+key)
                        trimmedstream.add(newline)

            return trimmedstream
        #--------------------------------------------------

        if not ndtype:
            stream = DataStream()

            if starttime:
                # check starttime input
                starttime = self._testtime(starttime)
                stval = 0
                for idx, elem in enumerate(self):
                    if not isnan(elem.time):
                        if num2date(elem.time).replace(tzinfo=None) > starttime.replace(tzinfo=None):
                            #stval = idx-1 # changed because of latex output
                            stval = idx
                            break
                if stval < 0:
                    stval = 0
                self.container = self.container[stval:]

            # remove data prior to endtime input
            if endtime:
                # check endtime input
                endtime = self._testtime(endtime)
                edval = len(self)
                for idx, elem in enumerate(self):
                    if not isnan(elem.time):
                        if num2date(elem.time).replace(tzinfo=None) > endtime.replace(tzinfo=None):
                            edval = idx
                            #edval = idx-1
                            break
                self.container = self.container[:edval]

        if ndtype:
            return DataStream(self.container,self.header,newarray)
        else:
            return DataStream(self.container,self.header,self.ndarray)


    def use_sectime(self, swap=False):
        """
        DEFINITION:
            Drop primary time stamp and replace by secondary time stamp if available.
            If swap is True, then primary time stamp is moved to secondary column (and
            not dropped).
        """
        if not 'sectime' in self._get_key_headers():
            logger.warning("use_sectime: did not find secondary time column in the streams keylist - returning unmodified timeseries")
            return self

        # Non destructive
        stream = self.copy()
        pos = KEYLIST.index('sectime')
        tcol = stream.ndarray[0]
        stream = stream._move_column('sectime','time')
        if swap:
            stream = stream._put_column(tcol,'sectime')
        else:
            stream = stream._drop_column('sectime')

        return stream


    def variometercorrection(self, variopath, thedate, **kwargs):
        """
        DEFINITION:
            ##### THS METHOD IS USELESS....
            ##### Either select a certain time in absolute calculation (TODO)
            ##### or calculate daily means of basevalues which ar already corrected for
            ##### variotion --- leon 2016-03

            Function to perform a variometercorrection of an absresult stream
            towards the given datetime using the given variometer stream.
            Returns a new absresult object with new datetime and corrected values
        APPLICATION:
            Useful to compare various absolute measurement e.g. form one day and analyse their
            differences after correcting them to a single spot in time.
        PARAMETERS:
         Variables:
            - variodata:   (DataStream) data to be used for reduction
            - endtime:     (datetime/str) End of period to trim to
         Kwargs:
            - funckeys:    (list) keys of the variometerfile which are interpolated and used
            - nomagorient: (bool) indicates that variometerdata is NOT in magnetic
                                  coordinates (hez) - Method will then use header info
                                  in DataRotationAlpha and Beta

        RETURNS:
            - stream:   (DataStream object) absolute stream - corrected

        EXAMPLE:
            >>> newabsdata = absdata.variometercorrection(starttime, endtime)

        APPLICATION:
        """
        funckeys = kwargs.get('funckeys')
        offset = kwargs.get('offset')
        nomagorient = kwargs.get('nomagorient')
        if not offset:
            offset = 0.0

        dateform = "%Y-%m-%d"

        def getfuncvals(variofunc,day):
            # Put the following to a function
            functime = (date2num(day)-variofunc[1])/(variofunc[2]-variofunc[1])
            #print(functime, day, date2num(day),variofunc[1],variofunc[2])
            refval = []
            for key in funckeys:
                if key in ['x','y','z']:
                    refval.append(variofunc[0]['f'+key](functime))
            return refval

        # Return results within a new streamobject containing only
        # the average values and its uncertainties
        resultstream = DataStream()

        # Check for ndtype:
        ndtype = False
        if len(self.ndarray[0]) > 0:
            timecol = self.ndarray[0]
            ndtype = True
            typus = self.header.get('DataComponents')
            try:
                typus = typus.lower()[:3]
            except:
                typus = ''
        else:
            timecol = self._get_column('time')
            try:
                typus = self[0].typ[:3]
            except:
                typus = ''
        # 1 Convert absresult - idff to xyz    ---- NOT NECESSARY
        # test stream type (xyz, idf or hdz?)
        # TODO add the end check whether streams are modified!!!!!!!!!!
        #print("Variometercorrection", typus)
        absstream = self.copy()
        absstream = absstream.removeduplicates()

        # 2 Convert datetime to number
        # check whether thedate is a time (then use this time every day)
        # or a full date
        datelist = []
        try:
            # Check whether provided thedate is a date with time
            datelist = [self._testtime(thedate)]
            print("Variometercorrection: using correction to single provided datetime", datelist[0])
        except:
            try:
                # Check whether provided thedate is only time
                tmpdatelst = [datetime.date(num2date(elem)) for elem in timecol]
                tmpdatelst = list(set(tmpdatelst))
                dummydatedt = self._testtime('2016-11-22T'+thedate)
                datelist = [datetime.combine(elem, datetime.time(dummydatedt)) for elem in tmpdatelst]
            except:
                print("Variometercorrection: Could not interpret the provided date/time - aborting - used dateformat should be either 12:00:00 or 2016-11-22 12:00:00 - provided:", thedate)
                return self

        if len(datelist) == 1:
            print("Variometercorrection: Transforming all provided absolute data towards", datelist[0])
        elif len(datelist) > 1:
            print("Variometercorrection: Correcting all absolute data of individual days towards time", datetime.strftime(datelist[0],"%H:%M:%S"))
        else:
            print("Variometercorrection: No correction date found - aborting")
            return self

        for day in datelist:
            print("Variocorrection: dealing with {}".format(day))
            # 1. Select the appropriate values from self
            if len(datelist) == 1:
                usedabsdata = absstream
                st, et = absstream._find_t_limits()
            else:
                st = str(datetime.date(day))
                et = str(datetime.date(day+timedelta(days=1)))
                usedndarray = absstream._select_timerange(starttime=st, endtime=et)
                usedabsdata = DataStream([LineStruct()],self.header,usedndarray)
            #print(date, num2date(usedabsdata.ndarray[0]))
            # 2. Read variation data for respective date
            vario = read(variopath, starttime=st, endtime=et)
            print("Variocorrection: loaded {} data points".format(vario.length()[0]))
            #print("Variocorrection: Please note - we are assuming that the provided variometerdata records the field in magnetic coordinates in nT (e.g. HEZ). In case of geographic xyz records one can activate a kwarg: takes provided rotation angle or (if not existing) the declination value of abs data")
            # 3. Check DataComponents: we need pure variation data
            comps = vario.header.get('DataComponents')
            try:
                comps = comps.lower()[:3]
            except:
                comps = ''
            if comps in ['xyz','idf','hdz']:
                # Data is already in geographic coordinates
                # Rotate back
                if not comps == 'xyz':
                    vario = vario._convertstream(comps+'2xyz')
                nomagorient = True
            else:
                nomagorient = False
            # 4. TODO TEST! Eventually rotate the data to hez
            if nomagorient:
                rotaangle = vario.header.get('DataRotationAlpha')
                rotbangle = vario.header.get('DataRotationBeta')
                #print("Angles", rotaangle, rotbangle)
                try:
                    rotaangle = float(rotaangle)
                    rotbangle = float(rotbangle)
                except:
                    pass
                if rotaangle in [None,np.nan,0.0]:
                    print("Variocorrection: Did not find DataRotationAlpha in header assuming xyz and rotation by minus declination")
                    rotaangle = -np.mean(usedabsdata.ndarray[2])
                else:
                    try:
                        rotaangle = float(rotaangle)
                    except:
                        rotaangle = 0.
                if not rotbangle in [None,'Null',np.nan,0.0]:
                    try:
                        rotbangle = float(rotbangle)
                    except:
                        rotbangle = 0.
                print("Variocorrection: Rotating data by {a} and {b}".format(a=rotaangle,b=rotbangle))
                vario = vario.rotation(alpha=rotaangle,beta=rotbangle)
            if vario.length()[0] > 1 and len(usedabsdata.ndarray[0]) > 0:
                variost, varioet = vario._find_t_limits()
                # 4. Interpolating variation data
                if not funckeys:
                    funckeys = []
                    keys = vario._get_key_headers(numerical=True)
                    for key in keys:
                        if key in ['x','y','z','f']:
                            funckeys.append(key)
                variofunc = vario.interpol(funckeys)

                refvals = getfuncvals(variofunc,day)

                for idx,abstime in enumerate(usedabsdata.ndarray[0]):
                    variovalsatabstime = getfuncvals(variofunc,num2date(abstime))
                    diffs= np.asarray(refvals)-np.asarray(variovalsatabstime)

                """
                    if key == 'y':
                        #refy = np.arctan2(np.asarray(list(ar)),np.asarray(list(arrayx)))*180./np.pi + function[0]['f'+key](functime)
                        pass
                    elif key in ['x','z']:
                        pass
                    else:
                        pass
                #refvals = funcattime(variofunc,date)
                # 5. Get variofunc data for selected date and each usedabsdata
                #for abstime in usedabsdata.ndarray[0]:
                #    if variost
                #absst, abset = usedabsdata._find_t_limits()
                """
                """
                    if key == 'y':
                        #indx = KEYLIST.index('x')
                        #Hv + Hb;   Db + atan2(y,H_corr)    Zb + Zv
                        #print type(self.ndarray[ind]), key, self.ndarray[ind]
                        array[ind] = np.arctan2(np.asarray(list(ar)),np.asarray(list(arrayx)))*180./np.pi + function[0]['f'+key](functimearray)
                        self.header['col-y'] = 'd'
                        self.header['unit-col-y'] = 'deg'
                    else:
                        print("func2stream", function, function[0], function[0]['f'+key],functimearray)
                        array[ind] = ar + function[0]['f'+key](functimearray)
                        if key == 'x': # remember this for correct y determination
                            arrayx = array[ind]
                """

        """
        for date in datelist:
            newvallists=[]
            for elem in absstream:
                # if elem.time == date:
                    # if value existis in function:
                        # calnewvalues and append to lists
            # calc means from lists
            # append means to new stream


            # 4 Test whether variostream covers the timerange between the abstream value(s) and the datetime
            if function[1] <= elem.time <= function[2] and function[1] <= newdate <= function[2]:
                valatorgtime = (elem.time-function[1])/(function[2]-function[1])
                valatnewtime = (newdate-function[1])/(function[2]-function[1])
                elem.time = newdate
                for key in funckeys:
                    if not key in KEYLIST[1:15]:
                        raise ValueError, "Column key not valid"
                    fkey = 'f'+key
                    if fkey in function[0]:
                        try:
                            orgval = float(function[0][fkey](valatorgtime))
                            newval = float(function[0][fkey](valatnewtime))
                            diff = orgval - newval
                        except:
                            logger.error("variometercorrection: error in assigning new values")
                            return
                        exec('elem.'+key+' = elem.'+key+' - diff')
                    else:
                        pass
            else:
                logger.warning("variometercorrection: Variometer stream does not cover the projected time range")
                pass

        # 5 Convert absresult - xyzf to idff
        absstream = absstream._convertstream('xyz2idf')

        return absstream
        """

    def _write_format(self, format_type, filenamebegins, filenameends, coverage, dateformat,year):
        """
        DEFINITION:
            Helper method to determine suggested write filenames.
            Reads format_type and header info of self -> returns specifications
        RETURNS:
            filenamebegins
            filenameends
            coverage
            dateformat
        """

        # Preconfigure some fileformats - can be overwritten by keywords
        if format_type == 'IMF':
            dateformat = '%b%d%y'
            try:
                extension = (self.header.get('StationID','')).lower()
            except:
                extension = 'txt'
            filenameends = '.'+extension
            coverage = 'day'
        if format_type == 'IAF':
            try:
                filenamebegins = (self.header.get('StationIAGAcode','')).upper()
            except:
                filenamebegins = 'XXX'
            dateformat = '%y%b'
            extension = 'BIN'
            coverage = 'month'
            filenameends = '.'+extension
        if format_type == 'IYFV':
            if not filenameends or filenameends=='.cdf':
                head = self.header
                code = head.get('StationIAGAcode','')
                if not code == '':
                    filenameends = '.'+code.upper()
                else:
                    filenameends = '.XXX'
            if not filenamebegins:
                filenamebegins = 'YEARMEAN'
            dateformat = 'None'
            coverage = 'year'
        if format_type == 'IAGA':
            dateformat = '%Y%m%d'
            coverage = 'day'
            head = self.header
            if not filenamebegins:
                code = head.get('StationIAGAcode','')
                if code == '':
                    code = head.get('StationID','')
                if not code == '':
                    filenamebegins = code.lower()[:3]
            if not filenameends or filenameends=='.cdf':
                samprate = float(str(head.get('DataSamplingRate','0')).replace('sec','').strip())
                plevel = head.get('DataPublicationLevel',0)
                if int(samprate) == 1:
                    middle = 'sec'
                elif int(samprate) == 60:
                    middle = 'min'
                elif int(samprate) == 3600:
                    middle = 'hou'
                else:
                    middle = 'lol'
                if plevel == 4:
                    fed = 'd'+middle+'.'+middle
                elif plevel == 3:
                    fed = 'q'+middle+'.'+middle
                elif plevel == 2:
                    fed = 'p'+middle+'.'+middle
                else:
                    fed = 'v'+middle+'.'+middle
                filenameends = fed

        if format_type == 'IMAGCDF':
            begin = (self.header.get('StationIAGAcode','')).lower()
            if begin == '':
                begin = (self.header.get('StationID','XYZ')).lower()
            publevel = str(self.header.get('DataPublicationLevel',0))
            samprate = float(str(self.header.get('DataSamplingRate','0')).replace('sec','').strip())
            if coverage == 'year':
                dfor = '%Y'
            elif coverage == 'month':
                dfor = '%Y%m'
            else:
                dfor = '%Y%m%d'
            if int(samprate) == 1:
                dateformat = dfor
                middle = '_000000_PT1S_'
            elif int(samprate) == 60:
                dateformat = dfor
                middle = '_0000_PT1M_'
            elif int(samprate) == 3600:
                dateformat = dfor
                middle = '_00_PT1H_'
            elif int(samprate) == 86400:
                dateformat = dfor
                middle = '_PT1D_'
            elif int(samprate) > 30000000:
                dateformat = '%Y'
                middle = '_PT1Y_'
            elif int(samprate) > 2400000:
                dateformat = '%Y%m'
                middle = '_PT1M_'
            else:
                dateformat = '%Y%m%d'
                middle = 'unknown'
            filenamebegins = begin+'_'
            filenameends = middle+publevel+'.cdf'
        if format_type == 'BLV':
            if len(self.ndarray[0]) > 0:
                lt = max(self.ndarray[0].astype(float))
            else:
                lt = self[-1].time
            if year:
                blvyear = str(year)
            else:
                blvyear = datetime.strftime(num2date(lt).replace(tzinfo=None),'%Y')
            try:
                filenamebegins = (self.header['StationID']).upper()+blvyear
            except:
                filenamebegins = 'XXX'+blvyear
            filenameends = '.blv'
            coverage = 'all'

        if not format_type:
            format_type = 'PYCDF'
        if not dateformat:
            dateformat = '%Y-%m-%d' # or %Y-%m-%dT%H or %Y-%m or %Y or %Y
        if not coverage:
            coverage = 'day' #timedelta(days=1)
        if not filenamebegins:
            filenamebegins = ''
        if not filenameends and not filenameends == '':
            # Extension for cdf files is automatically attached
            if format_type in ['PYCDF','IMAGCDF']:
                filenameends = ''
            else:
                filenameends = '.txt'

        return format_type, filenamebegins, filenameends, coverage, dateformat


    def write(self, filepath, compression=5, **kwargs):
        """
    DEFINITION:
        Code for simple application: write Stream to a file.

    PARAMETERS:
      Variables:
        - filepath:     (str) Providing path/filename for saving.
      Kwargs:
        - coverage:     (str/timedelta) day files or hour or month or year or all - default day.
                        'month','year','all',etc., otherwise timedelta object
        - dateformat:   (str) outformat of date in filename (e.g. "%Y-%m-%d" -> "2011-11-22".
        - filenamebegins:       (str) providing the begin of savename (e.g. "WIK_").
        - filenameends:         (str) providing the end of savename (e.g. ".min").
        - format_type:  (str) Which format - default pystr.
                        Current supported formats: PYSTR, PYCDF, IAGA, WDC, DIDD,
                                PMAG1, PMAG2, DTU1,  GDASA1, RMRCS, AUTODIF_FREAD,
                                USBLOG, CR800, LATEX
        - keys:         (list) Keys to write to file.
        - mode:         (str) Mode for handling existing files/data in files.
                        Options: append, overwrite, replace, skip
        [- period:      (str) Supports hour, day, month, year, all - default day.]
        [--> Where is this?]
        - wformat:      (str) outputformat.

    SPECIFIC FORMAT INSTRUCTIONS:
        format_type='IAGA'
        ------------------
           *General:
            The meta information provided within the header of each IAGA file is automatically 
            generated from the header information provided along with the following keys 
            (define by stream.header[key]):
            - Obligatory: StationInstitution, StationName, StationIAGAcode (or StationID),
                        DataElevation, DataSensorOrientation, DataDigitalSampling
            - Optional:   SensorID, DataPublicationDate, DataComments, DataConversion, StationK9, 
                          SecondarySensorID (F sensor), StationMeans (used for 'Approx H') 
            - Header input "IntervalType": can either be provided by using key 'DataIntervalType'
                          or is automatically created from DataSamplingRate.
                          Filter details as contained in DataSamplingFilter are added to the 
                          commentary part
            - Header input "Geodetic Longitude and Latitude":
                          - defined with keys 'DataAcquisitionLatitude','DataAcquisitionLongitude'
                          - if an EPSG code is provided in key 'DataLocationReference'
                            this code is used to convert Lat and Long into the WGS84 system
                            e.g. stream.header['DataLocationReference'] = 'M34, EPSG: ' 

           *Specific parameters:
            - useg          (Bool) if F is available, and G not yet caluclated: calculate G (deltaF) and
                                   use it within the IAGA output file

           *Example:

        format_type='IMF'
        ------------------
           *Specific parameters:
            - version       (str) file version
            - gin           (gin) information node code
            - datatype      (str) R: reported, A: adjusted, Q: quasi-definit, D: definite
            - kvals         (Datastream) contains K value for iaf storage
            - comment       (string) some comment, currently used in IYFV
            - kind          (string) one of 'A' (all), 'Q' quiet days, 'D' disturbed days,
                                 currently used in IYFV
        format_type='IMAGCDF'
        ------------------
           *General:
            - Header input "Geodetic Longitude and Latitude": see format_type='IAGA'

           *Specific parameters:
            - addflags      (BOOL) add flags to IMAGCDF output if True
                    
        format_type='BLV'
        ------------------
           *Specific parameters:
            - absinfo       (str) parameter of DataAbsInfo
            - fitfunc       (str) fit function for baselinefit
            - fitdegree
            - knotstep
            - extradays
            - year          (int) year
            - meanh         (float) annual mean of H component
            - meanf         (float) annual mean of F component
            - deltaF        (float) given deltaF value between pier and f position
            - diff          (DataStream) diff (deltaF) between vario and scalar

    RETURNS:
        - ...           (bool) True if successful.

    EXAMPLE:
        >>> stream.write('/home/user/data',
                        format_type='IAGA')

    APPLICATION:

        """
        format_type = kwargs.get('format_type')
        filenamebegins = kwargs.get('filenamebegins')
        filenameends = kwargs.get('filenameends')
        dateformat = kwargs.get('dateformat')
        coverage = kwargs.get('coverage')
        mode = kwargs.get('mode')
        #period = kwargs.get('period')          # TODO
        #offsets = kwargs.get('offsets')        # retired? TODO
        keys = kwargs.get('keys')
        absinfo = kwargs.get('absinfo')
        fitfunc = kwargs.get('fitfunc')
        fitdegree = kwargs.get('fitdegree')
        knotstep = kwargs.get('knotstep')
        extradays = kwargs.get('extradays')
        year = kwargs.get('year')
        meanh = kwargs.get('meanh')
        meanf = kwargs.get('meanf')
        deltaF = kwargs.get('deltaF')
        diff = kwargs.get('diff')
        baseparam =  kwargs.get('baseparam')
        version = kwargs.get('version')
        gin = kwargs.get('gin')
        datatype = kwargs.get('datatype')
        kvals = kwargs.get('kvals')
        kind = kwargs.get('kind')
        comment = kwargs.get('comment')
        useg = kwargs.get('useg')
        skipcompression = kwargs.get('skipcompression')
        debug = kwargs.get('debug')
        addflags = kwargs.get('addflags')

        success = True

        #compression: provide compression factor for CDF data: 0 no compression, 9 high compression

        t1 = datetime.utcnow()

        if not format_type in PYMAG_SUPPORTED_FORMATS:
            if not format_type:
                format_type = 'PYSTR'
            else:
                logger.warning('write: Output format not supported.')
                return False
        else:
            if not 'w' in PYMAG_SUPPORTED_FORMATS[format_type][0]:
                logger.warning('write: Selected format does not support write methods.')
                return False

        format_type, filenamebegins, filenameends, coverage, dateformat = self._write_format(format_type, filenamebegins, filenameends, coverage, dateformat, year)

        if not mode:
            mode= 'overwrite'

        if len(self) < 1 and len(self.ndarray[0]) < 1:
            logger.error('write: Stream is empty!')
            raise Exception("Can't write an empty stream to file!")

        ndtype = False
        if len(self.ndarray[0]) > 0:
            self.ndarray[0] = self.ndarray[0].astype(float)
            # remove all data from array where time is not numeric
            #1. get indicies of nonnumerics in ndarray[0]
            nonnumlist = np.asarray([idx for idx,elem in enumerate(self.ndarray[0]) if np.isnan(elem)])
            #2. delete them
            if len(nonnumlist) > 0:
                print("write: Found NaNs in time column - deleting them", nonnumlist)
                print(self.ndarray[0])
                for idx, elem in enumerate(self.ndarray):
                    self.ndarray[idx] = np.delete(self.ndarray[idx],nonnumlist)

            starttime = datetime.strptime(datetime.strftime(num2date(float(self.ndarray[0][0])).replace(tzinfo=None),'%Y-%m-%d'),'%Y-%m-%d')
            try:
                lasttime = num2date(float(self.ndarray[0][-1])).replace(tzinfo=None)
            except:
                lasttime = num2date(float(self.ndarray[0][-2])).replace(tzinfo=None)
            ndtype = True
        else:
            starttime = datetime.strptime(datetime.strftime(num2date(self[0].time).replace(tzinfo=None),'%Y-%m-%d'),'%Y-%m-%d')
            lasttime = num2date(self[-1].time).replace(tzinfo=None)

        t2 = datetime.utcnow()

        # divide stream in parts according to coverage and save them
        newst = DataStream()
        if coverage == 'month':
            #starttime = datetime.strptime(datetime.strftime(num2date(self[0].time).replace(tzinfo=None),'%Y-%m-%d'),'%Y-%m-%d')
            cmonth = int(datetime.strftime(starttime,'%m')) + 1
            cyear = int(datetime.strftime(starttime,'%Y'))
            if cmonth == 13:
               cmonth = 1
               cyear = cyear + 1
            monthstr = str(cyear) + '-' + str(cmonth) + '-' + '1T00:00:00'
            endtime = datetime.strptime(monthstr,'%Y-%m-%dT%H:%M:%S')
            while starttime < lasttime:
                if ndtype:
                    lst = []
                    ndarray=self._select_timerange(starttime=starttime, endtime=endtime)
                else:
                    lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                    ndarray = np.asarray([])
                newst = DataStream(lst,self.header,ndarray)
                filename = filenamebegins + datetime.strftime(starttime,dateformat) + filenameends
                # remove any eventually existing null byte
                filename = filename.replace('\x00','')
                if len(lst) > 0 or len(ndarray[0]) > 0:
                    success = writeFormat(newst, os.path.join(filepath,filename),format_type,mode=mode,keys=keys,kvals=kvals,skipcompression=skipcompression,compression=compression, addflags=addflags)
                starttime = endtime
                # get next endtime
                cmonth = int(datetime.strftime(starttime,'%m')) + 1
                cyear = int(datetime.strftime(starttime,'%Y'))
                if cmonth == 13:
                   cmonth = 1
                   cyear = cyear + 1
                monthstr = str(cyear) + '-' + str(cmonth) + '-' + '1T00:00:00'
                endtime = datetime.strptime(monthstr,'%Y-%m-%dT%H:%M:%S')
        elif coverage == 'year':
            #print ("write: Saving yearly data")
            cyear = int(datetime.strftime(starttime,'%Y'))
            cyear = cyear + 1
            yearstr = str(cyear) + '-01-01T00:00:00'
            endtime = datetime.strptime(yearstr,'%Y-%m-%dT%H:%M:%S')
            while starttime < lasttime:
                ndarray=self._select_timerange(starttime=starttime, endtime=endtime)
                newst = DataStream([LineStruct()],self.header,ndarray)
                if not dateformat == 'None':
                    dat = datetime.strftime(starttime,dateformat)
                else:
                    dat = ''
                filename = filenamebegins + dat + filenameends
                # remove any eventually existing null byte
                filename = filename.replace('\x00','')

                if len(ndarray[0]) > 0:
                    success = writeFormat(newst, os.path.join(filepath,filename),format_type,mode=mode,keys=keys,kvals=kvals,kind=kind,comment=comment,skipcompression=skipcompression,compression=compression, addflags=addflags)
                # get next endtime
                starttime = endtime
                cyear = cyear + 1
                yearstr = str(cyear) + '-01-01T00:00:00'
                endtime = datetime.strptime(yearstr,'%Y-%m-%dT%H:%M:%S')
        elif not coverage == 'all':
            #starttime = datetime.strptime(datetime.strftime(num2date(self[0].time).replace(tzinfo=None),'%Y-%m-%d'),'%Y-%m-%d')
            if coverage == 'hour':
                cov = timedelta(hours=1)
            else:
                cov = timedelta(days=1)
            dailystream = self.copy()
            maxidx = -1
            endtime = starttime + cov
            while starttime < lasttime:
                #lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                #newst = DataStream(lst,self.header)
                t3 = datetime.utcnow()
                #print "write - writing day:", t3

                if ndtype:
                    lst = []
                    # non-destructive
                    #print "write: start and end", starttime, endtime
                    #print "write", dailystream.length()
                    #ndarray=self._select_timerange(starttime=starttime, endtime=endtime)
                    #print starttime, endtime, coverage
                    #print "Maxidx", maxidx
                    ndarray=dailystream._select_timerange(starttime=starttime, endtime=endtime, maxidx=maxidx)
                    #print "write", len(ndarray), len(ndarray[0])
                    if len(ndarray[0]) > 0:
                        #maxidx = len(ndarray[0])*2 ## That does not work for few seconds of first day and full coverage of all other days
                        dailystream.ndarray = np.asarray([array[(len(ndarray[0])-1):] for array in dailystream.ndarray])
                        #print dailystream.length()
                    #print len(ndarray), len(ndarray[0]), len(ndarray[1]), len(ndarray[3])
                else:
                    lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                    ndarray = np.asarray([np.asarray([]) for key in KEYLIST])

                t4 = datetime.utcnow()
                #print "write - selecting time range needs:", t4-t3

                newst = DataStream(lst,self.header,ndarray)
                filename = str(filenamebegins) + str(datetime.strftime(starttime,dateformat)) + str(filenameends)
                # remove any eventually existing null byte
                filename = filename.replace('\x00','')

                if format_type == 'IMF':
                    filename = filename.upper()

                if debug:
                    print ("Writing data:", os.path.join(filepath,filename))

                if len(lst) > 0 or ndtype:
                    if len(newst.ndarray[0]) > 0 or len(newst) > 1:
                        logger.info('write: writing %s' % filename)
                        #print("Here", num2date(newst.ndarray[0][0]), newst.ndarray)
                        success = writeFormat(newst, os.path.join(filepath,filename),format_type,mode=mode,keys=keys,version=version,gin=gin,datatype=datatype, useg=useg,skipcompression=skipcompression,compression=compression, addflags=addflags)
                starttime = endtime
                endtime = endtime + cov

                t5 = datetime.utcnow()
                #print "write - written:", t5-t3
                #print "write - End:", t5

        else:
            filename = filenamebegins + filenameends
            # remove any eventually existing null byte
            filename = filename.replace('\x00','')
            if debug:
                print ("Writing file:", filename)
            success = writeFormat(self, os.path.join(filepath,filename),format_type,mode=mode,keys=keys,absinfo=absinfo,fitfunc=fitfunc,fitdegree=fitdegree, knotstep=knotstep,meanh=meanh,meanf=meanf,deltaF=deltaF,diff=diff,baseparam=baseparam, year=year,extradays=extradays,skipcompression=skipcompression,compression=compression, addflags=addflags)

        return success


    def idf2xyz(self,**kwargs):
        """
      DEFINITION:
        Converts inclination, declination, intensity (idf) data to xyz (i,d in 0.00000 deg (or gon)), f in nT
        Working only for ndarrays
      PARAMETERS:
        optional keywords:
        unit               (string) can be deg or gon
        """
        unit = kwargs.get('unit')
        keys = kwargs.get('keys')

        if not len(self.ndarray[0]) > 0:
            print("idf2xyz: no data found")
        if not keys:
            keys = ['x','y','z']
        if not len(keys) == 3:
            print("idf2xyz: invalid keys provided")

        indx = KEYLIST.index(keys[0])
        indy = KEYLIST.index(keys[1])
        indz = KEYLIST.index(keys[2])
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        dc = self.ndarray[indy].astype(float)*np.pi/(180.*ang_fac)
        ic = self.ndarray[indx].astype(float)*np.pi/(180.*ang_fac)
        self.ndarray[indx] = self.ndarray[indz].astype(float)*np.cos(dc)*np.cos(ic)
        self.ndarray[indy] = self.ndarray[indz].astype(float)*np.sin(dc)*np.cos(ic)
        self.ndarray[indz] = self.ndarray[indz].astype(float)*np.sin(ic)
        self.header['col-x'] = 'X'
        self.header['col-y'] = 'Y'
        self.header['col-z'] = 'Z'
        self.header['unit-col-x'] = 'nT'
        self.header['unit-col-y'] = 'nT'
        self.header['unit-col-z'] = 'nT'
        self.header['DataComponents'] = self.header['DataComponents'].replace('IDF','XYZ')

        return self

    def xyz2idf(self,**kwargs):
        """
      DEFINITION:
        Converts x,y,z (all in nT) to inclination, declination, intensity (idf)
              (i,d in 0.00000 deg (or gon)), f in nT
        Working only for ndarrays
      PARAMETERS:
        optional keywords:
        unit               (string) can be deg or gon
        """
        keys = kwargs.get('keys')
        if not len(self.ndarray[0]) > 0:
            print("xyz2idf: no data found")
        if not keys:
            keys = ['x','y','z']
        if not len(keys) == 3:
            print("xyz2idf: invalid keys provided")

        indx = KEYLIST.index(keys[0])
        indy = KEYLIST.index(keys[1])
        indz = KEYLIST.index(keys[2])

        unit = kwargs.get('unit')
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        h = np.sqrt(self.ndarray[indx].astype(float)**2 + self.ndarray[indy].astype(float)**2)
        i = (180.*ang_fac)/np.pi * np.arctan2(self.ndarray[indz].astype(float), h)
        d = (180.*ang_fac)/np.pi * np.arctan2(self.ndarray[indy].astype(float), self.ndarray[indx].astype(float))
        f = np.sqrt(self.ndarray[indx].astype(float)**2+self.ndarray[indy].astype(float)**2+self.ndarray[indz].astype(float)**2)
        self.ndarray[indx] = i
        self.ndarray[indy] = d
        self.ndarray[indz] = f
        self.header['col-x'] = 'I'
        self.header['col-y'] = 'D'
        self.header['col-z'] = 'F'
        self.header['unit-col-x'] = 'deg'
        self.header['unit-col-y'] = 'deg'
        self.header['unit-col-z'] = 'nT'
        self.header['DataComponents'] = self.header['DataComponents'].replace('XYZ','IDF')
        return self

    def xyz2hdz(self,**kwargs):
        """
      DEFINITION:
        Converts x,y,z (all in nT) to horizontal, declination, z (hdz)
              (d in 0.00000 deg (or gon)), h,z in nT
        Working only for ndarrays
      PARAMETERS:
        optional keywords:
        unit               (string) can be deg or gon
        """
        keys = kwargs.get('keys')
        if not len(self.ndarray[0]) > 0:
            print("xyz2hdz: no data found")
        if not keys:
            keys = ['x','y','z']
        if not len(keys) == 3:
            print("xyz2hdz: invalid keys provided")
        indx = KEYLIST.index(keys[0])
        indy = KEYLIST.index(keys[1])
        indz = KEYLIST.index(keys[2])

        unit = kwargs.get('unit')
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        h = np.sqrt(self.ndarray[indx].astype(float)**2 + self.ndarray[indy].astype(float)**2)
        d = (180.*ang_fac) / np.pi * np.arctan2(self.ndarray[indy].astype(float), self.ndarray[indx].astype(float))
        self.ndarray[indx] = h
        self.ndarray[indy] = d
        #dH = dX*X/sqrt(X^2 + Y^2) + dY*Y/sqrt(X^2 + Y^2)
        #dD = 180/Pi*(dY*X/(X^2 + Y^2) - dX*Y/(X^2 + Y^2))
        self.header['col-x'] = 'H'
        self.header['col-y'] = 'D'
        self.header['unit-col-x'] = 'nT'
        self.header['unit-col-y'] = 'deg'
        self.header['DataComponents'] = self.header['DataComponents'].replace('XYZ','HDZ')
        return self

    def hdz2xyz(self,**kwargs):
        """
      DEFINITION:
        Converts h,d,z (h,z in nT, d in deg) to xyz
        Working only for ndarrays
      PARAMETERS:
        optional keywords:
        unit               (string) can be deg or gon
        keys               (list) list of three keys which hold h,d,z values
        """
        keys = kwargs.get('keys')
        if not len(self.ndarray[0]) > 0:
            print("hdz2xyz: no data found")
        if not keys:
            keys = ['x','y','z']
        if not len(keys) == 3:
            print("hdz2xyz: invalid keys provided")

        indx = KEYLIST.index(keys[0])
        indy = KEYLIST.index(keys[1])
        indz = KEYLIST.index(keys[2])

        unit = kwargs.get('unit')
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.

        dc = self.ndarray[indy].astype(float)*np.pi/(180.*ang_fac)
        prevxcol = self.ndarray[indx].astype(float)
        self.ndarray[indx] = prevxcol * (np.cos(dc))
        self.ndarray[indy] = prevxcol * (np.sin(dc))
        #self.ndarray[indx] = self.ndarray[indx].astype(float) /np.sqrt((np.tan(dc))**2 + 1)
        #self.ndarray[indy] = np.sqrt(self.ndarray[indx].astype(float)**2 - xtmp**2)
        #print self.ndarray[indy]
        #self.ndarray[indx] = xtmp

        self.header['col-x'] = 'X'
        self.header['col-y'] = 'Y'
        self.header['col-z'] = 'Z'
        self.header['unit-col-x'] = 'nT'
        self.header['unit-col-y'] = 'nT'
        self.header['unit-col-z'] = 'nT'
        self.header['DataComponents'] = self.header['DataComponents'].replace('HDZ','XYZ')

        return DataStream(self,self.header,self.ndarray)


class PyMagLog(object):
    """
    Looging class for warning messages and analysis steps.
    logger and warnings are lists of strings.
    They contain full text information for file and screen output
    """
    def __init__(self, logger=[], warnings=[], process=[], proc_count=0):
        self.logger = logger
        self.warnings = warnings
        self.process = process
        self.proc_count = proc_count

    def __getitem__(self, key):
        return self.key

    def addwarn(self, warnmsg):
        self.warnings.append(warnmsg)

    def addlog(self, logmsg):
        self.logger.append(logmsg)

    def addpro(self, promsg):
        self.process.append(promsg)

    def clearpro(self):
        process = []

    def clearlog(self):
        logger = []

    def clearwarn(self):
        warnings = []

    def addcount(self, num, maxnum):
        """
        creates an integer number relative to maxnum ranging from 0 to 100
        assuming num starting at zero
        """
        self.proc_count = int(np.round(num*100/maxnum))

    def clearcount(self):
        self.proc_count = 0

    def _removeduplicates(self,content):
        return list(set(content))

    """
    def sendLogByMail(self,loglist,**kwargs):
        smtpserver = kwargs.get('smtpserver')
        sender = kwargs.get('sender')
        user = kwargs.get('user')
        pwd = kwargs.get('pwd')
        destination = kwargs.get('destination')
        subject = kwargs.get('subject')

        if not smtpserver:
            smtpserver = 'smtp.internet.at'
        if not sender:
           sender = 'frau.musterfrau@internet.at'
        if not destination:
            destination = ['fuer.mich@my.institution.at']
        if not user:
            user = "FrauMusterfrau"
        if not pwd:
            pwd = "HelloWorld"
        if not subject:
            subject= 'MagPy Log from %s' % datetime.utcnow()

        # typical values for text_subtype are plain, html, xml
        text_subtype = 'plain'

        content = '\n'.join(''.join(line) for line in loglist)

        try:
            msg = MIMEText(content, text_subtype)
            msg['Subject']= subject
            msg['From'] = sender # some SMTP servers will do this automatically, not all
            smtp = SMTP()
            smtp.set_debuglevel(False)
            smtp.connect(smtpserver, 587)
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(user, pwd)

            try:
                smtp.sendmail(sender, destination, msg.as_string())
            finally:
                smtp.close()

        except Exception as exc:
            raise ValueError( "mail failed; %s" % str(exc) ) # give a error message
    """

    def combineWarnLog(self,warning,log):
        comlst = ['Warning:']
        comlst.extend(self._removeduplicates(warning))
        comlst.extend(['Non-critical info:'])
        comlst.extend(self._removeduplicates(log))
        return comlst


class LineStruct(object):
    def __init__(self, time=float('nan'), x=float('nan'), y=float('nan'), z=float('nan'), f=float('nan'), dx=float('nan'), dy=float('nan'), dz=float('nan'), df=float('nan'), t1=float('nan'), t2=float('nan'), var1=float('nan'), var2=float('nan'), var3=float('nan'), var4=float('nan'), var5=float('nan'), str1='-', str2='-', str3='-', str4='-', flag='0000000000000000-', comment='-', typ="xyzf", sectime=float('nan')):
        #def __init__(self):
        #- at the end of flag is important to be recognized as string
        """
        self.time=float('nan')
        self.x=float('nan')
        self.y=float('nan')
        self.z=float('nan')
        self.f=float('nan')
        self.dx=float('nan')
        self.dy=float('nan')
        self.dz=float('nan')
        self.df=float('nan')
        self.t1=float('nan')
        self.t2=float('nan')
        self.var1=float('nan')
        self.var2=float('nan')
        self.var3=float('nan')
        self.var4=float('nan')
        self.var5=float('nan')
        self.str1=''
        self.str2=''
        self.str3=''
        self.str4=''
        self.flag='0000000000000000-'
        self.comment='-'
        self.typ="xyzf"
        self.sectime=float('nan')
        """
        self.time = time
        self.x = x
        self.y = y
        self.z = z
        self.f = f
        self.dx = dx
        self.dy = dy
        self.dz = dz
        self.df = df
        self.t1 = t1
        self.t2 = t2
        self.var1 = var1
        self.var2 = var2
        self.var3 = var3
        self.var4 = var4
        self.var5 = var5
        self.str1 = str1
        self.str2 = str2
        self.str3 = str3
        self.str4 = str4
        self.flag = flag
        self.comment = comment
        self.typ = typ
        self.sectime = sectime


    def __repr__(self):
        return repr((self.time, self.x, self.y, self.z, self.f, self.dx, self.dy, self.dz, self.df, self.t1, self.t2, self.var1, self.var2, self.var3, self.var4, self.var5, self.str1, self.str2, self.str3, self.str4, self.flag, self.comment, self.typ))

    def __getitem__(self, index):
        key = KEYLIST[index]
        return getattr(self, key)

    def __setitem__(self, index, value):
        key = KEYLIST[index]
        setattr(self, key.lower(), value)

    def idf2xyz(self,**kwargs):
        """
        keyword:
        unit: (string) can be deg or gon
        """
        unit = kwargs.get('unit')
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        dc = self.y*np.pi/(180.*ang_fac)
        ic = self.x*np.pi/(180.*ang_fac)
        self.x = self.z*np.cos(dc)*np.cos(ic)
        self.y = self.z*np.sin(dc)*np.cos(ic)
        self.z = self.z*np.sin(ic)
        return self

    def xyz2idf(self,**kwargs):
        """
        keyword:
        unit: (string) can be deg or gon
        """
        unit = kwargs.get('unit')
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        h = np.sqrt(self.x**2 + self.y**2)
        i = (180.*ang_fac)/np.pi * math.atan2(self.z, h)
        d = (180.*ang_fac)/np.pi * math.atan2(self.y, self.x)
        f = np.sqrt(self.x**2+self.y**2+self.z**2)
        self.x = i
        self.y = d
        self.z = f
        return self

    def xyz2hdz(self,**kwargs):
        """
        keyword:
        unit: (string) can be deg or gon
        """
        unit = kwargs.get('unit')
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        h = np.sqrt(self.x**2 + self.y**2)
        d = (180.*ang_fac) / np.pi * math.atan2(self.y, self.x)
        self.x = h
        self.y = d
        #dH = dX*X/sqrt(X^2 + Y^2) + dY*Y/sqrt(X^2 + Y^2)
        #dD = 180/Pi*(dY*X/(X^2 + Y^2) - dX*Y/(X^2 + Y^2))
        return self

    def hdz2xyz(self,**kwargs):
        """
        keyword:
        unit: (string) can be deg or gon
        """
        unit = kwargs.get('unit')
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        dc = self.y*np.pi/(180.*ang_fac)
        xtmp = self.x /np.sqrt((np.tan(dc))**2 + 1)
        self.y = np.sqrt(self.x**2 - xtmp**2)
        self.x = xtmp
        return self


    def rotation(self,alpha=None,beta=None,**kwargs):
        """
        Rotation matrix for ratating x,y,z to new coordinate system xs,ys,zs using angles alpha and beta
        alpha is the horizontal rotation in degree, beta the vertical
        """
        unit = kwargs.get('unit')
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        ra = ni.pi*alpha/(180.*ang_fac)
        rb = ni.pi*beta/(180.*ang_fac)
        xs = self.x*np.cos(rb)*np.cos(ra)-self.y*np.sin(ra)+self.z*np.sin(rb)*np.cos(ra)
        ys = self.x*np.cos(rb)*np.sin(ra)+self.y*np.cos(ra)+self.z*np.sin(rb)*np.sin(ra)
        zs = self.x*np.sin(rb)+self.z*np.cos(rb)

        self.x = xs
        self.y = ys
        self.z = zs

        return self


# Unused classes
"""
class ColStruct(object):
    def __init__(self,length, time=float('nan'), x=float('nan'), y=float('nan'), z=float('nan'), f=float('nan'), dx=float('nan'), dy=float('nan'), dz=float('nan'), df=float('nan'), t1=float('nan'), t2=float('nan'), var1=float('nan'), var2=float('nan'), var3=float('nan'), var4=float('nan'), var5=float('nan'), str1='-', str2='-', str3='-', str4='-', flag='0000000000000000-', comment='-', typ="xyzf", sectime=float('nan')):
        #""
        Not used so far. Maybe useful for
        Speed optimization:
        Change the whole thing to column operations

        - at the end of flag is important to be recognized as string
        for column initialization use a length parameter and "lenght*[float('nan')]" or "lenght*['-']"to initialize nan-values
        #""
        self.length = length
        self.time = length*[time]
        self.x = length*[x]
        self.y = length*[y]
        self.z = length*[z]
        self.f = length*[f]
        self.dx = length*[dx]
        self.dy = length*[dy]
        self.dz = length*[dz]
        self.df = length*[df]
        self.t1 = length*[t1]
        self.t2 = length*[t2]
        self.var1 = length*[var1]
        self.var2 = length*[var2]
        self.var3 = length*[var3]
        self.var4 = length*[var4]
        self.var5 = length*[var5]
        self.str1 = length*[str1]
        self.str2 = length*[str2]
        self.str3 = length*[str3]
        self.str4 = length*[str4]
        self.flag = length*[flag]
        self.comment = length*[comment]
        self.typ = length*[typ]
        self.sectime = length*[sectime]

    def __repr__(self):
        return repr((self.time, self.x, self.y, self.z, self.f, self.dx, self.dy, self.dz, self.df, self.t1, self.t2, self.var1, self.var2, self.var3, self.var4, self.var5, self.str1, self.str2, self.str3, self.str4, self.flag, self.comment, self.typ, self.sectime))

"""

# -------------------
#  Global functions of the stream file
# -------------------


def coordinatetransform(u,v,w,kind):
    """
    DESCRIPTION:
        Transforms given values and returns [d,i,h,x,y,z,f] if successful, False if not.
        Parameter "kind" defines the type of provided values
    APPLICATION:
        list = coordinatetransform(meanx,meany,meanz,'xyz')
    """

    if not kind in ['xyz','hdz','dhz','idf']:
        return [0]*7
    if kind == 'xyz':
        h = np.sqrt(u**2 + v**2)
        i = (180.)/np.pi * np.arctan2(w, h)
        d = (180.)/np.pi * np.arctan2(v, u)
        f = np.sqrt(u**2+v**2+w**2)
        return [d,i,h,u,v,w,f]
    elif kind == 'hdz':
        dc = v*np.pi/(180.)
        xtmp = u /np.sqrt((np.tan(dc))**2 + 1)
        y = np.sqrt(u**2 - xtmp**2)
        x = xtmp
        f = np.sqrt(x**2+y**2+w**2)
        i = (180.)/np.pi * np.arctan2(w, u)
        return [v,i,u,x,y,w,f]
    elif kind == 'dhz':
        dc = u*np.pi/(180.)
        xtmp = v /np.sqrt((np.tan(dc))**2 + 1)
        y = np.sqrt(v**2 - xtmp**2)
        x = xtmp
        f = np.sqrt(h**2+w**2)
        i = (180.)/np.pi * np.arctan2(w, v)
        return [u,i,v,x,y,w,f]
    return [0]*7

def isNumber(s):
    """
    Test whether s is a number
    """
    try:
        float(s)
        return True
    except ValueError:
        return False

def find_nearest(array,value):
    """
    Find the nearest element within an array
    """

    # Eventually faster solution (minimal)
    #idx = np.searchsorted(array, value, side="left")
    #if math.fabs(value - array[idx-1]) < math.fabs(value - array[idx]):
    #    return array[idx-1], idx-1
    #else:
    #    return array[idx], idx

    idx = (np.abs(array-value)).argmin()
    return array[idx], idx


def ceil_dt(dt,seconds):
    """
    DESCRIPTION:
        Function to round time to the next time step as given by its seconds
        minute: 60 sec
        quater hour: 900 sec
        hour:   3600 sec
    PARAMETER:
        dt: (datetime object)
        seconds: (integer)
    USAGE:
        >>>print ceil_dt(datetime(2014,01,01,14,12,04),60)
        >>>2014-01-01 14:13:00
        >>>print ceil_dt(datetime(2014,01,01,14,12,04),3600)
        >>>2014-01-01 15:00:00
        >>>print ceil_dt(datetime(2014,01,01,14,7,0),60)
        >>>2014-01-01 14:07:00
    """
    #how many secs have passed this hour
    nsecs = dt.minute*60+dt.second+dt.microsecond*1e-6
    if nsecs % seconds:
        delta = (nsecs//seconds)*seconds+seconds-nsecs
        return dt + timedelta(seconds=delta)
    else:
        return dt


# ##################
# read/write functions
# ##################

def read(path_or_url=None, dataformat=None, headonly=False, **kwargs):
    """
    DEFINITION:
        The read functions tries to open the selected files. Calls on
        function _read() for help.

    PARAMETERS:
    Variables:
        - path_or_url:  (str) Path to data files in form:
                                a) c:\my\data\*
                                b) c:\my\data\thefile.txt
                                c) /home/data/*
                                d) /home/data/thefile.txt
                                e) ftp://server/directory/
                                f) ftp://server/directory/thefile.txt
                                g) http://www.thepage.at/file.tab
        - headonly:     (?) ???
    Kwargs:
        - dataformat:   (str) Format of data file. Works as auto-detection.
        - disableproxy: (bool) If True, will use urllib2.install_opener()
        - endtime:      (str/datetime object) Description.
        - starttime:    (str/datetime object) Description.

    Format specific kwargs:
        IAF:
            - resolution: (str) can be either 'day','hour','minute'(default) or 'k'

    RETURNS:
        - stream:       (DataStream object) Stream containing data in file
                        under path_or_url.

    EXAMPLE:
        >>> stream = read('/srv/archive/WIC/LEMI025/LEMI025_2014-05-05.bin')
        OR
        >>> stream = read('http://www.swpc.noaa.gov/ftpdir/lists/ace/20140507_ace_sis_5m.txt')

    APPLICATION:
    """

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debugmode = kwargs.get('debugmode')
    disableproxy = kwargs.get('disableproxy')
    skipsorting = kwargs.get('skipsorting')
    keylist = kwargs.get('keylist') # for PYBIN
    debug = kwargs.get('debug')

    if disableproxy:
        proxy_handler = ProxyHandler( {} )
        opener = build_opener(proxy_handler)
        # install this opener
        install_opener(opener)

    # 1. No path
    if not path_or_url:
        logger.error("read: File not specified.")
        raise Exception("No path given for data in read function!")

    # 2. Create DataStream
    st = DataStream([],{},np.array([[] for ke in KEYLIST]))

    # 3. Read data
    if not isinstance(path_or_url, basestring):
        # not a string - we assume a file-like object
        pass
        """
        elif path_or_url.startswith("DB:"):
        # a database table
        if
        logger.error("read: File not specified.")
        raise Exception("No path given for data in read function!")
        pathname = path_or_url
        for file in iglob(pathname):
            stp = DataStream([],{},np.array([[] for ke in KEYLIST]))
            stp = _read(file, dataformat, headonly, **kwargs) glob
        """
    elif "://" in path_or_url:
        # some URL
        # extract extension if any
        logger.info("read: Found URL to read at %s" % path_or_url)
        content = urlopen(path_or_url).read()
        if debugmode:
            print(urlopen(path_or_url).info())
        if path_or_url[-1] == '/':
            # directory
            string = content.decode('utf-8')
            for line in string.split("\n"):
                if len(line) > 1:
                    filename = (line.strip().split()[-1])
                    if debugmode:
                        print(filename)
                    content = urlopen(path_or_url+filename).read()
                    suffix = '.'+os.path.basename(path_or_url).partition('.')[2] or '.tmp'
                    #date = os.path.basename(path_or_url).partition('.')[0][-8:]
                    #date = re.findall(r'\d+',os.path.basename(path_or_url).partition('.')[0])
                    date = os.path.basename(path_or_url).partition('.')[0] # append the full filename to the temporary file
                    fname = date+suffix
                    fname = fname.strip('?').strip(':')      ## Necessary for windows
                    #fh = NamedTemporaryFile(suffix=date+suffix,delete=False)
                    fh = NamedTemporaryFile(suffix=fname,delete=False)
                    print (fh.name, suffix)
                    fh.write(content)
                    fh.close()
                    stp = _read(fh.name, dataformat, headonly, **kwargs)
                    if len(stp) > 0: # important - otherwise header is going to be deleted
                        st.extend(stp.container,stp.header,stp.ndarray)
                    os.remove(fh.name)
        else:
            # TODO !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            # check whether content is a single file or e.g. a ftp-directory
            # currently only single files are supported
            # ToDo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            suffix = '.'+os.path.basename(path_or_url).partition('.')[2] or '.tmp'
            #date = os.path.basename(path_or_url).partition('.')[0][-8:]
            #date = re.findall(r'\d+',os.path.basename(path_or_url).partition('.')[0])[0]
            date = os.path.basename(path_or_url).partition('.')[0] # append the full filename to the temporary file
            fname = date+suffix
            fname = fname.strip('?').strip(':')   ## Necessary for windows
            fh = NamedTemporaryFile(suffix=fname,delete=False)
            fh.write(content)
            fh.close()
            st = _read(fh.name, dataformat, headonly, **kwargs)
            os.remove(fh.name)
    else:
        # some file name
        pathname = path_or_url
        for filename in iglob(pathname):
            getfile = True
            if filename.endswith('.gz') or filename.endswith('.GZ'):
                ## Added gz support to read IMO compressed data directly - future option might include tarfiles
                import gzip
                print ("Found zipped file (gz) ... unpacking")
                fname = os.path.split(filename)[1]
                fname = fname.strip('.gz')
                with NamedTemporaryFile(suffix=fname,delete=False) as fh:
                    shutil.copyfileobj(gzip.open(filename), fh)
                    filename = fh.name
            if filename.endswith('.zip') or filename.endswith('.ZIP'):
                ## Added gz support to read IMO compressed data directly - future option might include tarfiles
                from zipfile import ZipFile
                print ("Found zipped file (zip) ... unpacking")
                with ZipFile(filename) as myzip:
                    fname =  myzip.namelist()[0]
                    with NamedTemporaryFile(suffix=fname,delete=False) as fh:
                        shutil.copyfileobj(myzip.open(fname), fh)
                        filename = fh.name

            theday = extractDateFromString(filename)

            try:
                if starttime:
                    if not theday[-1] >= datetime.date(st._testtime(starttime)):
                        getfile = False
                if endtime:
                    if not theday[0] <= datetime.date(st._testtime(endtime)):
                        getfile = False
            except:
                # Date format not recognised. Read all files
                logger.info("read: Unable to detect date string in filename. Reading all files...")
                #logger.warning("read: filename: {}, theday: {}".format(filename,theday))
                getfile = True

            if getfile:
                stp = DataStream([],{},np.array([[] for ke in KEYLIST]))
                try:
                    stp = _read(filename, dataformat, headonly, **kwargs)
                except:
                    stp = DataStream([],{},np.array([[] for ke in KEYLIST]))
                    logger.warning("read: File {} could not be read. Skipping ...".format(filename))
                if (len(stp) > 0 and not np.isnan(stp[0].time)) or len(stp.ndarray[0]) > 0:   # important - otherwise header is going to be deleted
                    st.extend(stp.container,stp.header,stp.ndarray)

            #del stp

        if st.length()[0] == 0:
            # try to give more specific information why the stream is empty
            if has_magic(pathname) and not glob(pathname):
                logger.error("read: No file matching file pattern: %s" % pathname)
                raise Exception("Cannot read non-existent file!")
            elif not has_magic(pathname) and not os.path.isfile(pathname):
                logger.error("read: No such file or directory: %s" % pathname)
                raise Exception("Cannot read non-existent file!")
            # Only raise error if no starttime/endtime has been set. This
            # will return an empty stream if the user chose a time window with
            # no data in it.
            # XXX: Might cause problems if the data is faulty and the user
            # set starttime/endtime. Not sure what to do in this case.
            elif not 'starttime' in kwargs and not 'endtime' in kwargs:
                logger.error("read: Cannot open file/files: %s" % pathname)
            elif 'starttime' in kwargs or 'endtime' in kwargs:
                logger.error("read: Cannot read data. Probably no data available in the time range provided!")
                raise Exception("No data available in time range")
            else:
                logger.error("read: Unknown error occurred. No data in stream!")
                raise Exception("Unknown error occurred during reading. No data in stream!")

    if headonly and (starttime or endtime):
        msg = "read: Keyword headonly cannot be combined with starttime or endtime."
        logger.error(msg)

    # Sort the input data regarding time
    if not skipsorting:
        st = st.sorting()

    # eventually trim data
    if starttime:
        st = st.trim(starttime=starttime)
    if endtime:
        st = st.trim(endtime=endtime)

    ### Define some general header information TODO - This is done already in some format libs - clean up
    st.header['DataSamplingRate'] = float("{0:.2f}".format(st.samplingrate()))

    return st


#@uncompressFile
def _read(filename, dataformat=None, headonly=False, **kwargs):
    """
    Reads a single file into a MagPy DataStream object.
    Internal function only.
    """
    debug = kwargs.get('debug')

    stream = DataStream([],{})
    format_type = None
    foundapproptiate = False
    if not dataformat:
        # auto detect format - go through all known formats in given sort order
        for format_type in PYMAG_SUPPORTED_FORMATS:
            # check format
            if debug:
                logger.info("_read: Testing format: {} ...".format(format_type))
            if isFormat(filename, format_type):
                if debug:
                    logger.info("      -- found: {}".format(format_type))
                foundapproptiate = True
                break
        if not foundapproptiate:
            temp = open(filename, 'rt').readline()
            if temp.startswith('# MagPy Absolutes'):
                logger.warning("_read: You apparently tried to open a DI object - please use the absoluteAnalysis method")
            else:
                logger.error("_read: Could not identify a suitable data format")
            return DataStream([LineStruct()],{},np.asarray([[] for el in KEYLIST]))
    else:
        # format given via argument
        dataformat = dataformat.upper()
        try:
            formats = [el for el in PYMAG_SUPPORTED_FORMATS if el == dataformat]
            format_type = formats[0]
        except IndexError:
            msg = "Format \"%s\" is not supported. Supported types: %s"
            logger.error(msg % (dataformat, ', '.join(PYMAG_SUPPORTED_FORMATS)))
            raise TypeError(msg % (dataformat, ', '.join(PYMAG_SUPPORTED_FORMATS)))

    """
    try:
        # search readFormat for given entry point
        readFormat = load_entry_point(format_ep.dist.key,
            'obspy.plugin.waveform.%s' % (format_ep.name), 'readFormat')
    except ImportError:
        msg = "Format \"%s\" is not supported. Supported types: %s"
        raise TypeError(msg % (format_ep.name,
                               ', '.join(WAVEFORM_ENTRY_POINTS)))
    """
    stream = readFormat(filename, format_type, headonly=headonly, **kwargs)

    return stream

def saveflags(mylist=None,path=None, overwrite=False):
    """
    DEFINITION:
        Save list e.g. flaglist to file using pickle.

    PARAMETERS:
    Variables:
        - path:  (str) Path to data files in form:

    RETURNS:
        - True if succesful otherwise False
    EXAMPLE:
        >>> saveflags(flaglist,'/my/path/myfile.pkl')

    """
    print("Saving flaglist ...")
    if not mylist:
        print("error 1")
        return False
    if not path:
        path = 'myfile.pkl'
    if not overwrite:
        existflag = loadflags(path)
        existflag.extend(mylist)
        mylist = existflag
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    if path.endswith('.json'):
        print(" -- using json format ")
        try:
            import json
            def dateconv(d):
                # Converter to serialize datetime objects in json
                if isinstance(d,datetime):
                    return d.__str__()
            # Convert mylist to a dictionary
            mydic = {}
            # get a list of unique sensorid
            sid = [elem[5] for elem in mylist]
            sid = list(set(sid))
            for s in sid:
                slist = [elem[0:5]+elem[6:] for elem in mylist if elem[5] == s]
                mydic[s] = slist
                ## Dictionary looks like {SensorID:[[t1,t2,xxx,xxx,],[x...]]}
            with open(path,'w',encoding='utf-8') as file:
                file.write(unicode(json.dumps(mydic,default=dateconv)))
            print("saveflags: list saved to a json file: {}".format(path))
            return True
        except:
            return False
    else:
        print(" -- using pickle")
        try:
            # TODO: check whether package is already loaded
            from pickle import dump
            dump(mylist,open(path,'wb'))
            print("saveflags: list saved to {}".format(path))
            return True
        except:
            return False

def loadflags(path=None,sensorid=None,begin=None, end=None):
    """
    DEFINITION:
        Load list e.g. flaglist from file using pickle.

    PARAMETERS:
    Variables:
        - path:  (str) Path to data files in form:
        - begin: (datetime)
        - end:   (datetime)

    RETURNS:
        - list (e.g. flaglist)
    EXAMPLE:
        >>> loadflags('/my/path/myfile.pkl')
    """
    if not path:
        return []
    if path.endswith('.json'):
        try:
            import json
            print ("Reading a json style flaglist...")
            def dateparser(dct):
                # Convert dates in dictionary to datetime objects
                for (key,value) in dct.items():
                    for i,line in enumerate(value):
                        for j,elem in enumerate(line):
                            if str(elem).count('-') + str(elem).count(':') == 4:
                                try:
                                    try:
                                        value[i][j] = datetime.strptime(elem,"%Y-%m-%d %H:%M:%S.%f")
                                    except:
                                        value[i][j] = datetime.strptime(elem,"%Y-%m-%d %H:%M:%S")
                                except:
                                    pass
                    dct[key] = value
                return dct

            if os.path.isfile(path):
                with open(path,'r') as file:
                    mydic = json.load(file,object_hook=dateparser)
                if sensorid:
                    mylist = mydic.get(sensorid,'')
                    do = [el.insert(5,sensorid) for el in mylist]
                else:
                    mylist = []
                    for s in mydic:
                        ml = mydic[s]
                        do = [el.insert(5,s) for el in ml]
                        mylist.extend(mydic[s])
                if begin:
                    mylist = [el for el in mylist if el[1] > begin]
                if end:
                    mylist = [el for el in mylist if el[0] < end]
                return mylist
            else:
                print ("Flagfile not yet existing ...")
                return []
        except:
            return []
    else:
        try:
            from pickle import load as pklload
            mylist = pklload(open(path,"rb"))
            print("loadflags: list {a} successfully loaded, found {b} inputs".format(a=path,b=len(mylist)))
            if sensorid:
                print(" - extracting data for sensor {}".format(sensorid))
                mylist = [el for el in mylist if el[5] == sensorid]
                if begin:
                    mylist = [el for el in mylist if el[1] > begin]
                if end:
                    mylist = [el for el in mylist if el[0] < end]
                #print(" -> remaining flags: {b}".format(b=len(mylist)))
            return mylist
        except:
            return []



def joinStreams(stream_a,stream_b, **kwargs):
    """
    DEFINITION:
        Copy two streams together eventually replacing already existing time steps.
    """
    logger.info('joinStreams: Start joining at %s.' % str(datetime.now()))

    # Check stream type and eventually convert them to ndarrays
    # --------------------------------------
    ndtype = False
    if len(stream_a.ndarray[0]) > 0:
        # Using ndarray and eventually convert stream_b to ndarray as well
        ndtype = True
        if not len(stream_b.ndarray[0]) > 0:
            stream_b = stream_b.linestruct2ndarray()
            if not len(stream_b.ndarray[0]) > 0:
                return stream_a
    elif len(stream_b.ndarray[0]) > 0:
        ndtype = True
        stream_a = stream_a.linestruct2ndarray()
        if not len(stream_a.ndarray[0]) > 0:
            return stream_b
    else:
        ndtype = True
        stream_a = stream_a.linestruct2ndarray()
        stream_b = stream_b.linestruct2ndarray()
        if not len(stream_a.ndarray[0]) > 0 and not len(stream_b.ndarray[0]) > 0:
            logger.error('subtractStreams: stream(s) empty - aborting subtraction.')
            return stream_a

    # non-destructive
    # --------------------------------------
    sa = stream_a.copy()
    sb = stream_b.copy()

    # Get indicies of timesteps of stream_b of which identical times are existing in stream_a-> delelte those lines
    # --------------------------------------
    # IMPORTANT: If two streams with different keys should be combined then "merge" is the method of choice
    indofb = np.nonzero(np.in1d(sb.ndarray[0], sa.ndarray[0]))[0]
    for idx,elem in enumerate(sb.ndarray):
        if len(sb.ndarray[idx]) > 0:
            sb.ndarray[idx] = np.delete(sb.ndarray[idx],indofb)

    # Now add stream_a to stream_b - regard for eventually missing column data
    # --------------------------------------
    array = [[] for key in KEYLIST]
    for idx,elem in enumerate(sb.ndarray):
        if len(sa.ndarray[idx]) > 0 and len(sb.ndarray[idx]) > 0:
            array[idx] = np.concatenate((sa.ndarray[idx],sb.ndarray[idx]))
        elif not len(sa.ndarray[idx]) > 0 and  len(sb.ndarray[idx]) > 0:
            if idx < len(NUMKEYLIST):
                fill = float('nan')
            else:
                fill = '-'
            arraya = np.asarray([fill]*len(sa.ndarray[0]))
            array[idx] = np.concatenate((arraya,sb.ndarray[idx]))
        elif len(sa.ndarray[idx]) > 0 and not len(sb.ndarray[idx]) > 0:
            if idx < len(NUMKEYLIST):
                fill = float('nan')
            else:
                fill = '-'
            arrayb = np.asarray([fill]*len(sb.ndarray[0]))
            array[idx] = np.concatenate((sa.ndarray[idx],arrayb))
        else:
            array[idx] = np.asarray([])

    stream = DataStream([LineStruct()],sa.header,np.asarray(array))

    return stream.sorting()


def appendStreams(streamlist):
    """
    DESCRIPTION:
        Appends contents of streamlist  and returns a single new stream.
        Duplicates are removed and the new stream is sorted.
    """
    array = [[] for key in KEYLIST]
    for idx,key in enumerate(KEYLIST):
        # Get tuple of array
        arlist = []
        for stream in streamlist:
            if len(stream.ndarray[idx]) > 0:
                array[idx].extend(stream.ndarray[idx])
    stream = DataStream([LineStruct()],streamlist[0].header,np.asarray(array).astype(object))
    if len(stream.ndarray[0]) > 0:
        stream = stream.removeduplicates()
        stream = stream.sorting()
        return stream
    else:
        return DataStream([LineStruct()],streamlist[0].header,np.asarray([np.asarray([]) for key in KEYLIST]))


def mergeStreams(stream_a, stream_b, **kwargs):
    """
    DEFINITION:
        Combine the contents of two data streams realtive to stream_a.
        Basically three modes are possible:
        1. Insert data from stream_b into stream_a based on timesteps of stream_a
           - if keys are provided only these specific columns are inserted into a
           - default: if data is existing in stream_a only nans are replaced
                 here flags (4) can be set and a comment "inserted from SensorID" is added
           - eventually use get_gaps to identfy missing timesteps in stream_a before
        2. Replace
           - same as insert but here all existing time series data is replaced by
             corresponding data from stream_b
        3. Drop
           - drops the whole column from stream_a and fills it with stream_b data

        The streams need to overlapp, base stream is stream_a of which the time range
        is not modfified. If you want to extend this stream by new data use the extend
        method.

        1. replace data from specific columns of stream_a with data from stream_b.
        - requires keys
        2. fill gaps in stream_a data with stream_b data without replacing any data.
        - extend = True

    PARAMETERS:
    Variables:
        - stream_a      (DataStream object) main stream
        - stream_b      (DataStream object) this stream is merged into stream_a
    Kwargs:
        - addall:       (bool) Add all elements from stream_b
        - extend:       (bool) Time range of stream b is eventually added to stream a.
                        Default False.
                        If extend = true => any existing date which is not present in stream_a
                        will be filled by stream_b
        - mode:         (string) 'insert' or 'replace' or 'drop'. drop removes stream_a column, replace will change values no matter what, insert will only replace nan's (default)
        - keys:         (list) List of keys to add from stream_b into stream_a.
        - flag:         (bool) if true, a flag will be added to each merged line (default: flagid = 4, comment = "keys ... added from sensorid b").
        - comment:      (str) Define comment to stream_b data in stream_a.

        - replace:      (bool) Allows existing stream_a values to be replaced by stream_b ones.

    RETURNS:
        - Datastream(stream_a): (DataStream) DataStream object.

    EXAMPLE:
        >>> # Joining two datasets together:
        >>> alldata = mergeStreams(lemidata, gsmdata, keys=['f'])
               # f of gsm will be added to lemi
        # inserting missing values from another stream
        >>> new_gsm = mergeStreams(gsm1, gsm2, keys=['f'], mode='insert')
               # all missing values (nans) of gsm1 will be filled by gsm2 values (if existing)


    APPLICATION:
    """
    # old (LineStruct) too be removed
    addall = kwargs.get('addall')
    replace = kwargs.get('replace')
    extend = kwargs.get('extend')


    # new
    mode = kwargs.get('mode')
    flag = kwargs.get('flag')
    keys = kwargs.get('keys')
    comment = kwargs.get('comment')
    flagid = kwargs.get('flagid')

    if not mode:
        mode = 'insert'  # other possibilities: replace, ...
    if not keys:
        keys = stream_b._get_key_headers()

    # Defining default comment
    # --------------------------------------
    headera = stream_a.header
    headerb = stream_b.header
    try:
        sensidb = headerb['SensorID']
    except:
        sensidb = 'stream_b'

    # Better: create a flaglist and apply stream.flag(flaglist) with flag 4
    if not comment:
        comment = 'keys %s added from %s' % (','.join(keys), sensidb)
    if not flagid:
        flagid = 4

    fllst = [] # flaglist

    logger.info('mergeStreams: Start mergings at %s.' % str(datetime.now()))


    # Check stream type and eventually convert them to ndarrays
    # --------------------------------------
    ndtype = False
    if len(stream_a.ndarray[0]) > 0:
        # Using ndarray and eventually convert stream_b to ndarray as well
        ndtype = True
        if not len(stream_b.ndarray[0]) > 0:
            stream_b = stream_b.linestruct2ndarray()
    elif len(stream_b.ndarray[0]) > 0:
        ndtype = True
        stream_a = stream_a.linestruct2ndarray()
    else:
        ndtype = True
        stream_a = stream_a.linestruct2ndarray()
        stream_b = stream_b.linestruct2ndarray()
        if not len(stream_a.ndarray[0]) > 0 and len(stream_b.ndarray[0]) > 0:
            logger.error('subtractStreams: stream(s) empty - aborting subtraction.')
            return stream_a

    # non-destructive
    # --------------------------------------
    sa = stream_a.copy()
    sb = stream_b.copy()
    sa = sa.removeduplicates()
    sb = sb.removeduplicates()

    # Sampling rates
    # --------------------------------------
    sampratea = sa.samplingrate()
    samprateb = sb.samplingrate()
    minsamprate = min(sampratea,samprateb)

    if ndtype:
        timea = sa.ndarray[0]
    else:
        timea = sa._get_column('time')

    # truncate b to time range of a
    # --------------------------------------
    try:
        sb = sb.trim(starttime=num2date(timea[0]).replace(tzinfo=None), endtime=num2date(timea[-1]).replace(tzinfo=None)+timedelta(seconds=samprateb),newway=True)
    except:
        print("mergeStreams: stream_a and stream_b are apparently not overlapping - returning stream_a")
        return stream_a

    if ndtype:
        timeb = sb.ndarray[0]
    else:
        timeb = sb._get_column('time')

    # keeping a - changed by leon 10/2015
    """
    # truncate a to range of b
    # --------------------------------------
    try:
        sa = sa.trim(starttime=num2date(timeb[0]).replace(tzinfo=None), endtime=num2date(timeb[-1]).replace(tzinfo=None)+timedelta(seconds=sampratea),newway=True)
    except:
        print "mergeStreams: stream_a and stream_b are apparently not overlapping - returning stream_a"
        return stream_a

    # redo timea calc after trimming
    # --------------------------------------
    if ndtype:
        timea = sa.ndarray[0]
    else:
        timea = sa._get_column('time')
    """

    # testing overlapp
    # --------------------------------------
    if not len(sb) > 0:
        print("subtractStreams: stream_a and stream_b are not overlapping - returning stream_a")
        return stream_a

    timea = maskNAN(timea)
    timeb = maskNAN(timeb)

    orgkeys = stream_a._get_key_headers()

    # master header
    # --------------------------------------
    header = sa.header
    # just add the merged sensorid
    header['SecondarySensorID'] = sensidb

    ## Speed up of unequal timesteps - limit search range
    #   - search range small (fracratio high) if t_limits are similar and data is periodic
    #   - search range large  (fracratio small) if t_limits are similar and data is periodic
    #   - fracratio = 1 means that the full stream_b data set is searched
    #   - fracratio = 20 means that +-5percent of stream_b are searched arround expected index
    #print("mergeStream", sa.length(), sb.length(), sa._find_t_limits(), sb._find_t_limits())

    fracratio = 2  # modify if start and endtime are different
    speedup = True
    if speedup and ndtype:
        ast, aet = sa._find_t_limits()
        bst, bet = sb._find_t_limits()
        uncert = (date2num(aet)-date2num(ast))*0.01
        #print ("Merge speedup", uncert, ast, aet, bst, bet)
        if not bst < ast+timedelta(minutes=uncert*24*60):
            print ("Merge: Starttime of stream_b too large")
            for indx,key in enumerate(KEYLIST):
                if key == 'time':
                   ### Changes from 2019-01-15: modified axis - originally working fine, however except for saggitarius
                   #sb.ndarray[0] = np.append(np.asarray([date2num(ast)]), sb.ndarray[0],1)
                   sb.ndarray[0] = np.append(np.asarray([date2num(ast)]), sb.ndarray[0])
                elif key == 'sectime' or key in NUMKEYLIST:
                    if not len(sb.ndarray[indx]) == 0:
                        #sb.ndarray[indx] = np.append(np.asarray([np.nan]),sb.ndarray[indx],1)
                        sb.ndarray[indx] = np.append(np.asarray([np.nan]),sb.ndarray[indx])
                else:
                    if not len(sb.ndarray[indx]) == 0:
                        #sb.ndarray[indx] = np.append(np.asarray(['']),sb.ndarray[indx],1)
                        sb.ndarray[indx] = np.append(np.asarray(['']),sb.ndarray[indx])
        if not bet > aet-timedelta(minutes=uncert*24*60):
            print ("Merge: Endtime of stream_b too small") ### Move that to merge??
            for indx,key in enumerate(KEYLIST):
                if key == 'time':
                   #sb.ndarray[0] = np.append(sb.ndarray[0], np.asarray([date2num(aet)]),1)
                   sb.ndarray[0] = np.append(sb.ndarray[0], np.asarray([date2num(aet)]))
                elif key == 'sectime' or key in NUMKEYLIST:
                    if not len(sb.ndarray[indx]) == 0:
                        #sb.ndarray[indx] = np.append(sb.ndarray[indx], np.asarray([np.nan]),1)
                        sb.ndarray[indx] = np.append(sb.ndarray[indx], np.asarray([np.nan]))
                else:
                    if not len(sb.ndarray[indx]) == 0:
                        #sb.ndarray[indx] = np.append(sb.ndarray[indx], np.asarray(['']),1)
                        sb.ndarray[indx] = np.append(sb.ndarray[indx], np.asarray(['']))
        #st,et = sb._find_t_limits()
        #print ("Merge", st, et, sb.length())
        sb = sb.get_gaps()
        fracratio = 40  # modify if start and endtime are different

        timeb = sb.ndarray[0]
        timeb = maskNAN(timeb)

    abratio = len(timea)/float(len(timeb))
    dcnt = int(len(timeb)/fracratio)
    #print ("Merge:", abratio, dcnt, len(timeb))

    timea = np.round(timea, decimals=9)
    timeb = np.round(timeb, decimals=9)
    if ndtype:
            array = [[] for key in KEYLIST]
            # Init array with keys from stream_a
            for key in orgkeys:
                keyind = KEYLIST.index(key)
                array[keyind] = sa.ndarray[keyind]
            indtib = np.nonzero(np.in1d(timeb,timea))[0]
            # If equal elements occur in time columns
            if len(indtib) > int(0.5*len(timeb)):
                print("mergeStreams: Found identical timesteps - using simple merge")
                # get tb times for all matching indicies
                #print("merge", indtib, len(indtib), len(timea), len(timeb), np.argsort(timea), np.argsort(timeb))
                tb = np.asarray([timeb[ind] for ind in indtib])
                # Get indicies of stream_a of which times are present in matching tbs
                indtia = np.nonzero(np.in1d(timea,tb))[0]
                #print("mergeStreams", tb, indtib, indtia, timea,timeb, len(indtib), len(indtia))

                if len(indtia) == len(indtib):
                    nanind = []
                    for key in keys:
                        keyind = KEYLIST.index(key)
                        #array[keyind] = sa.ndarray[keyind]
                        vala, valb = [], []
                        if len(sb.ndarray[keyind]) > 0: # stream_b values are existing
                            #print("Found sb values", key)
                            valb = [sb.ndarray[keyind][ind] for ind in indtib]
                        if len(sa.ndarray[keyind]) > 0: # stream_b values are existing
                            vala = [sa.ndarray[keyind][ind] for ind in indtia]
                        ### Change by leon in 10/2015
                        if len(array[keyind]) > 0 and not mode=='drop': # values are present
                            pass
                        else:
                            if key in NUMKEYLIST:
                                array[keyind] = np.asarray([np.nan] *len(timea))
                            else:
                                array[keyind] = np.asarray([''] *len(timea))
                            try:
                                header['col-'+key] = sb.header['col-'+key]
                                header['unit-col-'+key] = sb.header['unit-col-'+key]
                            except:
                                print ("mergeStreams: warning when assigning header values to column %s - missing head" % key)

                        if len(sb.ndarray[keyind]) > 0: # stream_b values are existing
                            for i,ind in enumerate(indtia):
                                if key in NUMKEYLIST:
                                    tester = np.isnan(array[keyind][ind])
                                else:
                                    tester = False
                                    if array[keyind][ind] == '':
                                        tester = True
                                #print ("Merge3", tester)
                                if mode == 'insert':
                                    if tester:
                                        array[keyind][ind] = valb[i]
                                    else:
                                        if len(vala) > 0:
                                            array[keyind][ind] = vala[i]
                                elif mode == 'replace':
                                    if not np.isnan(valb[i]):
                                        array[keyind][ind] = valb[i]
                                    else:
                                        if len(vala) > 0:
                                            array[keyind][ind] = vala[i]
                                else:
                                    array[keyind][ind] = valb[i]
                                if flag:
                                    ttt = num2date(array[0][ind])
                                    fllst.append([ttt,ttt,key,flagid,comment])

                    array[0] = np.asarray(sa.ndarray[0])
                    array = np.asarray(array)

            else:
                print("mergeStreams: Did not find identical timesteps - linearily interpolating stream b...")
                print("- Please note: this method needs considerably longer.")
                print("- Only data within 1/2 the sampling rate distance of stream_a timesteps is used.")
                print("- Put in the larger (higher resolution) stream as stream_a,")
                print("- otherwise you might wait an endless amount of time.")
                # interpolate b
                # TODO here it is necessary to limit the stream to numerical keys
                #sb.ndarray = np.asarray([col for idx,col in enumerate(sb.ndarray) if KEYLIST[idx] in NUMKEYLIST])
                print("  a) starting interpolation of stream_b")
                mst = datetime.utcnow()
                function = sb.interpol(keys)
                met = datetime.utcnow()
                print("     -> needed {}".format(met-mst))
                # Get a list of indicies for which timeb values are
                #   in the vicintiy of a (within half of samplingrate)
                dti = (minsamprate/24./3600.)
                print("  b) getting indicies of stream_a with stream_b values in the vicinity")
                mst = datetime.utcnow()
                #indtia = [idx for idx, el in enumerate(timea) if np.min(np.abs(timeb-el))/dti <= 1.]  # This selcetion requires most of the time
                indtia = []  ### New and faster way by limiting the search range in stream_b by a factor of 10
                check = [int(len(timea)*(100-el)/100.) for el in range(99,1,-10)]
                lentimeb = len(timeb)
                for idx, el in enumerate(timea):
                    cst = int(idx/abratio-dcnt)
                    if cst<=0:
                        cst = 0
                    cet = int(idx/abratio+dcnt)
                    if cet>=lentimeb:
                        cet=lentimeb
                    if np.min(np.abs(timeb[cst:cet]-el)/(dti)) <= 0.5:
                        indtia.append(idx)
                    if idx in check:
                        print ("     -> finished {} percent".format(idx/float(len(timea))*100.))
                indtia = np.asarray(indtia)
                met = datetime.utcnow()
                print("     -> needed {}".format(met-mst))
                # limit time range to valued covered by the interpolation function
                #print len(indtia), len(timeb), np.asarray(indtia)
                indtia = [elem for elem in indtia if function[1] < timea[elem] < function[2]]
                #t2temp = datetime.utcnow()
                #print "Timediff %s" % str(t2temp-t1temp)
                #print len(indtia), len(timeb), np.asarray(indtia)
                #print function[1], sa.ndarray[0][indtia[0]], sa.ndarray[0][indtia[-1]], function[2]
                print("  c) extracting interpolated values of stream_b")
                mst = datetime.utcnow()
                if len(function) > 0:
                    for key in keys:
                        keyind = KEYLIST.index(key)
                        #print key, keyind
                        #print len(sa.ndarray[keyind]),len(sb.ndarray[keyind]), np.asarray(indtia)
                        vala, valb = [], []
                        if len(sb.ndarray[keyind]) > 0: # and key in function:

                            valb = [float(function[0]['f'+key]((sa.ndarray[0][ind]-function[1])/(function[2]-function[1]))) for ind in indtia]
                        if len(sa.ndarray[keyind]) > 0: # and key in function:
                            vala = [sa.ndarray[keyind][ind] for ind in indtia]

                        if len(array[keyind]) > 0 and not mode=='drop': # values are present
                            pass
                        else:
                            if key in NUMKEYLIST:
                                array[keyind] = np.asarray([np.nan] *len(timea))
                            else:
                                array[keyind] = np.asarray([''] *len(timea))
                            try:
                                header['col-'+key] = sb.header['col-'+key]
                                header['unit-col-'+key] = sb.header['unit-col-'+key]
                            except:
                                print ("mergeStreams: warning when assigning header values to column %s- missing head" % key)

                        for i,ind in enumerate(indtia):
                            if key in NUMKEYLIST:
                                tester = isnan(array[keyind][ind])
                            else:
                                tester = False
                                if array[keyind][ind] == '':
                                    tester = True
                            if mode == 'insert':
                                if tester:
                                    array[keyind][ind] = valb[i]
                                else:
                                    if len(vala) > 0:
                                        array[keyind][ind] = vala[i]
                            elif mode == 'replace':
                                if not np.isnan(valb[i]):
                                    array[keyind][ind] = valb[i]
                                else:
                                    if len(vala) > 0:
                                        array[keyind][ind] = vala[i]
                            else:
                                array[keyind][ind] = valb[i]
                            """
                            if mode == 'insert' and tester:
                                array[keyind][ind] = valb[i]
                            elif mode == 'replace':
                                array[keyind][ind] = valb[i]
                            """
                            if flag:
                                ttt = num2date(array[0][ind])
                                fllst.append([ttt,ttt,key,flagid,comment])

                        met = datetime.utcnow()
                        print("     -> needed {} for {}".format(met-mst,key))

                    array[0] = np.asarray(sa.ndarray[0])
                    array = np.asarray(array)

            #try:
            #    header['SensorID'] = sa.header['SensorID']+'-'+sb.header['SensorID']
            #except:
            #    pass

            return DataStream([LineStruct()],header,array)


    sta = list(stream_a)
    stb = list(stream_b)
    if addall:
        logger.info('mergeStreams: Adding streams together not regarding for timeconstraints of data.')
        if ndtype:
            for idx,elem in enumerate(stream_a.ndarray):
                ndarray = stream_a.ndarray
                if len(elem) == 0 and len(stream_b.ndarray[idx]) > 0:
                    # print add nan's of len_a to stream a
                    # then append stream b
                    pass
                elif len(elem) > 0 and len(stream_b.ndarray[idx]) == 0:
                    # print add nan's of len_b to stream a
                    pass
                elif len(elem) == 0 and len(stream_b.ndarray[idx]) == 0:
                    # do nothing
                    pass
                else: #len(elem) > 0 and len(stream_b.ndarray[idx]) > 0:
                    # append b to a
                    pass
            newsta = DataStream(sta, headera, ndarray)
        else:
            for elem in stream_b:
                sta.append(elem)
            newsta = DataStream(sta, headera, stream_a.ndarray)
        for elem in headerb:
            try:
                headera[elem]
                ha = True
            except:
                ha = False
            if headerb[elem] and not ha:
                newsta.header[elem] = headerb[elem]
            elif headerb[elem] and ha:
                logger.warning("mergeStreams: headers both have keys for %s. Headers may be incorrect." % elem)
        newsta.sorting()
        return newsta
    elif extend:
        logger.info('mergeStreams: Extending stream a with data from b.')
        for elem in stream_b:
            if not elem.time in timea:
                sta.append(elem)
        newsta = DataStream(sta, headera)
        for elem in headerb:
            try:
                headera[elem]
                ha = True
            except:
                ha = False
            if headerb[elem] and not ha:
                newsta.header[elem] = headerb[elem]
            elif headerb[elem] and ha:
                logger.warning("mergeStreams: headers both have keys for %s. Headers may be incorrect." % elem)
        newsta.sorting()
        return newsta
    else:
        # interpolate stream_b
        # changed the following trim section to prevent removal of first input in trim method
        if stream_b[0].time == np.min(timea):
            sb = stream_b.trim(endtime=np.max(timea))
        else:
            sb = stream_b.trim(starttime=np.min(timea), endtime=np.max(timea))
        timeb = sb._get_column('time')
        timeb = maskNAN(timeb)

        function = sb.interpol(keys)

        taprev = 0
        for elem in sb:
            foundina = find_nearest(timea,elem.time)
            pos = foundina[1]
            ta = foundina[0]
            if (ta > taprev) and (np.min(timeb) <= ta <= np.max(timeb)):
                taprev = ta
                functime = (ta-function[1])/(function[2]-function[1])
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        logger.error('mergeStreams: Column key (%s) not valid.' % key)
                    #keyval = getattr(stream_a[pos], key)# should be much better
                    exec('keyval = stream_a[pos].'+key)
                    fkey = 'f'+key
                    if fkey in function[0] and (isnan(keyval) or not stream_a._is_number(keyval)):
                        newval = function[0][fkey](functime)
                        exec('stream_a['+str(pos)+'].'+key+' = float(newval) + offset')
                        exec('stream_a['+str(pos)+'].comment = comment')
                        ## Put flag 4 into the merged data if keyposition <= 8
                        flagposlst = [i for i,el in enumerate(FLAGKEYLIST) if el == key]
                        try:
                            flagpos = flagposlst[0]
                            fllist = list(stream_a[pos].flag)
                            fllist[flagpos] = '4'
                            stream_a[pos].flag=''.join(fllist)
                        except:
                            pass
                    elif fkey in function[0] and not isnan(keyval) and replace == True:
                        newval = function[0][fkey](functime)
                        exec('stream_a['+str(pos)+'].'+key+' = float(newval) + offset')
                        exec('stream_a['+str(pos)+'].comment = comment')
                        ## Put flag 4 into the merged data if keyposition <= 8
                        flagposlst = [i for i,el in enumerate(FLAGKEYLIST) if el == key]
                        try:
                            flagpos = flagposlst[0]
                            fllist = list(stream_a[pos].flag)
                            fllist[flagpos] = '4'
                            stream_a[pos].flag=''.join(fllist)
                        except:
                            pass

    logger.info('mergeStreams: Mergings finished at %s ' % str(datetime.now()))

    return DataStream(stream_a, headera)

def dms2d(dms):
        """
        DESCRIPTION:
            converts a string with degree:minutes:seconds to degree.decimals
        VARIBALES:
            dms (string) like -0:37:23 or 23:23
        """
        # 1. get sign
        sign = dms[0]
        multi = 1
        if sign == '-':
            multi = -1
            dms = dms[1:]

        dmsar = dms.split(':')
        if len(dmsar) > 3:
            print("Could not interpret dms")
            return 0.0
        val=[]
        for i in range(0,3):
            try:
                val.append(float(dmsar[i]))
            except:
                val.append(0.0)
        d = multi*(val[0]+val[1]/60.+val[2]/3600.)
        return d



def find_offset(stream1, stream2, guess_low=-60., guess_high=60.,
        deltat_step=0.1,log_chi=False,**kwargs):
    '''
    DEFINITION:
        Uses least-squares method for a rough estimate of the offset in the time
        axis of two different streams. Both streams must contain the same key, e.g. 'f'.
        GENTLE WARNING: This method is FAR FROM OPTIMISED.
                        Interpolation brings in errors, *however* does allow for
                        a more exact result.

    PARAMETERS:
    Variables:
        - stream1:      (DataStream object) First stream to compare.
        - stream2:      (DataStream object) Second stream to compare.
    Kwargs:
        - deltat_step:  (float) Time value in s to iterate over. Accuracy is higher with
                        smaller values.
        - guess_low:    (float) Low guess for offset in s. Function will iterate from here.
        - guess_high:   (float) High guess for offset in s. Function will iterate till here.
        - log_chi:      (bool) If True, log chi values.
        - plot:         (bool) Filename of plot to save chi-sq values to, e.g. "chisq.png"

    RETURNS:
        - t_offset:     (float) The offset (in seconds) calculated by least-squares method
                        of stream_b.

    EXAMPLE:
        >>> offset = find_offset(gdas_data, pos_data, guess=-30.,deltat_min = 0.1)

    APPLICATION:

    Challenge in this function:
    --> Needs to be able to compare two non harmonic signals with different sampling
        rates and a presumed time offset. The time offset may be smaller than the
        sampling rate itself.
    How to go about it:
        1. Take arrays of key to compare
        2. Resample arrays to same sampling period (or interpolate)
        3. Determine offset between two arrays
        """
    '''

    # 1. Define starting parameters:
    N_iter = 0.

    # Interpolate the function with the smaller sample period.
    # Should hopefully lower error factors.

    sp1 = stream1.get_sampling_period()
    sp2 = stream2.get_sampling_period()

    #if sp1 > sp2:
    if sp1 < sp2:
        stream_a = stream1
        stream_b = stream2
        main_a = True
    #elif sp1 < sp2:
    elif sp1 > sp2:
        stream_a = stream2
        stream_b = stream1
        main_a = False
    else:
        stream_a = stream1
        stream_b = stream2
        main_a = True

    # Important for least-squares method. Streams must have same length.
    timeb = stream_b._get_column('time')
    stime = np.min(timeb)
    etime = np.max(timeb)

    timespan = guess_high-guess_low

    # TODO: Remove this trim function. It's destructive.
    stream_a = stream_a.trim(starttime=num2date(stime).replace(tzinfo=None)+timedelta(seconds=timespan*2),
                                endtime=num2date(etime).replace(tzinfo=None)+timedelta(seconds=-timespan*2))

    mean_a = stream_a.mean('f')
    mean_b = stream_b.mean('f')
    difference = mean_a - mean_b

    # Interpolate one stream:
    # Note: higher errors with lower degree of interpolation. Highest degree possible is desirable, linear terrible.
    try:
        int_data = stream_b.interpol(['f'],kind='cubic')
    except:
        try:
            logger.warning("find_offset: Not enough memory for cubic spline. Attempting quadratic...")
            int_data = stream_b.interpol(['f'],kind='quadratic')
        except:
            logger.error("find_offset: Too much data! Cannot interpolate function with high enough accuracy.")
            return "nan"

    int_func = int_data[0]['ff']
    int_min = date2num(num2date(int_data[1])+timedelta(milliseconds=guess_low*1000.))
    int_max = date2num(num2date(int_data[2])+timedelta(milliseconds=guess_low*1000.))

    timea = stream_a._get_column('f')
    datarray_base = np.zeros((len(stream_a)))
    count = 0

    # 5. Create array of delta-f with offset times:
    for elem in stream_a:
        time = stream_a[count].time
        if time > int_min and time < int_max:
            functime = (time - int_min)/(int_max - int_min)
            tempval = stream_a[count].f - int_func(functime)
            datarray_base[count] += tempval
        count = count+1

    # 3. From data array calculate chi-squared array of null-offset as a base comparison:
    chisq_ = 0.
    for item in datarray_base:
        chisq_ = chisq_ + (item)**2.
        #chisq_ = chisq_ + (item-difference)**2.                # Correction may be needed for reasonable values.
    deltat = guess_low

    # (Write data to file for logging purposes.)
    if log_chi:
        newfile = open('chisq.txt','a')
        writestring = str(deltat)+' '+str(chisq_)+' '+str(chisq_)+' '+str(len(datarray_base))+'\n'
        newfile.write(writestring)
        newfile.close()

    # 4. Start iteration to find best chi-squared minimisation:

    logger.info("find_offset: Starting chi-squared iterations...")

    chi_lst = []
    time_lst = []
    min_lst = []
    max_lst = []
    results = []

    while True:
        deltat = deltat + deltat_step
        if deltat > guess_high: break
        N_iter = N_iter + 1.
        flag == 0.

        datarray = np.zeros((len(stream_a)))

        count = 0
        newc = 0
        int_min = float(date2num(num2date(int_data[1]) + timedelta(milliseconds=deltat*1000.)))
        int_max = float(date2num(num2date(int_data[2]) + timedelta(milliseconds=deltat*1000.)))

        for elem in stream_a:
            time = stream_a[count].time
            if time > int_min and time < int_max:
                functime = (time - int_min)/(int_max - int_min)
                tempval = stream_a[count].f - int_func(functime)
                datarray[count] += tempval
            count = count+1

        chisq = 0.
        for item in datarray:
            chisq = chisq + (item-difference)**2.

        if log_chi:
            newfile = open('chisq.txt','a')
            writestring = str(deltat)+' '+str(chisq)+' '+str(chisq_)+' '+str(len(datarray))+'\n'
            newfile.write(writestring)
            newfile.close()

        # Catch minimum:
        if chisq < chisq_:
            chisq_ = chisq
            t_offset = deltat

        chi_lst.append(chisq)
        time_lst.append(deltat)

    if plot:
        plt.plot(time_lst,chi_lst,'-')
        plt.show()

    if not main_a:
        t_offset = t_offset * (-1)

    logger.info("find_offset: Found an offset of stream_a of %s seconds." % t_offset)

    # RESULTS
    return t_offset


def diffStreams(stream_a, stream_b, **kwargs):
    """
    DESCRIPTION:
      obtain and return the differences of two stream:
    """

    ndtype_a = False
    if len(stream_a.ndarray[0]) > 0:
        ndtype_a = True

    if not ndtype_a or not len(stream_a) > 0:
        logger.error('diffStreams: stream_a empty - aborting.')
        return stream_a

    ndtype_b = False
    if len(stream_b.ndarray[0]) > 0:
        ndtype_b = True

    # 1. Amount of columns
    #if ndtype

    # 2. Line contents
    #  --- amount of lines
    #  --- differences of lines

def subtractStreams(stream_a, stream_b, **kwargs):
    '''
    DEFINITION:
        Default function will subtract stream_b from stream_a. If timesteps are different
        stream_b will be interpolated

    PARAMETERS:
    Variables:
        - stream_a:     (DataStream) First stream
        - stream_b:     (DataStream) Second stream, which is subtracted from a

    Optional:
        - keys:         (list) key list for subtraction - default: all keys present in both streams

    RETURNS:
        - difference:   (DataStream) Description.

    EXAMPLE:
        >>> diff = subtractStreams(gsm_stream, pos_stream)

    APPLICATION:


    '''

    keys = kwargs.get('keys')
    newway = kwargs.get('newway')
    getmeans = kwargs.get('getmeans')
    debug = kwargs.get('debug')

    if not keys:
        keys = stream_a._get_key_headers(numerical=True)
    keysb = stream_b._get_key_headers(numerical=True)
    keys = list(set(keys)&set(keysb))

    if not len(keys) > 0:
        print("subtractStreams: No common keys found - aborting")
        return DataStream()

    ndtype = False
    if len(stream_a.ndarray[0]) > 0:
        # Using ndarray and eventually convert stream_b to ndarray as well
        ndtype = True
        newway = True
        if not len(stream_b.ndarray[0]) > 0:
            stream_b = stream_b.linestruct2ndarray()
    elif len(stream_b.ndarray[0]) > 0:
        ndtype = True
        stream_a = stream_a.linestruct2ndarray()
    else:
        try:
            assert len(stream_a) > 0
        except:
            logger.error('subtractStreams: stream_a empty - aborting subtraction.')
            return stream_a

    logger.info('subtractStreams: Start subtracting streams.')

    headera = stream_a.header
    headerb = stream_b.header


    # non-destructive
    #print ("SA:", stream_a.length())
    #print ("SB:", stream_b.length())
    sa = stream_a.copy()
    sb = stream_b.copy()

    # Sampling rates
    sampratea = sa.samplingrate()
    samprateb = sb.samplingrate()
    minsamprate = min(sampratea,samprateb)

    if ndtype:
        timea = sa.ndarray[0]
        timea = timea.astype(float)
    else:
        timea = sa._get_column('time')

    # truncate b to time range of a
    try:
        sb = sb.trim(starttime=num2date(np.min(timea)).replace(tzinfo=None), endtime=num2date(np.max(timea)).replace(tzinfo=None)+timedelta(seconds=samprateb),newway=True)
        #sb = sb.trim(starttime=num2date(np.min(timea)).replace(tzinfo=None), endtime=num2date(np.max(timea)).replace(tzinfo=None),newway=True)
    except:
        print("subtractStreams: stream_a and stream_b are apparently not overlapping - returning stream_a")
        return stream_a

    if ndtype:
        timeb = sb.ndarray[0]
    else:
        timeb = sb._get_column('time')

    # truncate a to range of b
    try:
        sa = sa.trim(starttime=num2date(np.min(timeb.astype(float))).replace(tzinfo=None), endtime=num2date(np.max(timeb.astype(float))).replace(tzinfo=None)+timedelta(seconds=sampratea),newway=True)
        #sa = sa.trim(starttime=num2date(np.min(timeb.astype(float))).replace(tzinfo=None), endtime=num2date(np.max(timeb.astype(float))).replace(tzinfo=None),newway=True)
    except:
        print("subtractStreams: stream_a and stream_b are apparently not overlapping - returning stream_a")
        return stream_a

    if ndtype:
        timea = sa.ndarray[0]
        timea = timea.astype(float)
    else:
        timea = sa._get_column('time')

    # testing overlapp
    if not len(sb) > 0:
        print("subtractStreams: stream_a and stream_b are not overlapping - returning stream_a")
        return stream_a

    timea = maskNAN(timea)
    timeb = maskNAN(timeb)

    #print "subtractStreams: timea", timea
    #print "subtractStreams: timeb", timeb
    # Check for the following cases:
    # 1- No overlap of a and b
    # 2- a high resolution and b low resolution (tested)
    # 3- a low resolution and b high resolution (tested)
    # 4- a shorter and fully covered by b (tested)
    # 5- b shorter and fully covered by a

    if ndtype:
            logger.info('subtractStreams: Running ndtype subtraction')
            # Assuming similar time steps
            #t1s = datetime.utcnow()
            # Get indicies of stream_b of which times are present in stream_a
            array = [[] for key in KEYLIST]
            """
            try: # TODO Find a better solution here! Roman 2017
                # The try clause is not correct as searchsorted just finds
                # positions independet of agreement (works well if data is similar)
                idxB = np.argsort(timeb)
                sortedB = timeb[idxB]
                idxA = np.searchsorted(sortedB, timea)
                #print timea, timeb,len(idxA), len(idxB)
                indtib = idxB[idxA]
                print ("solution1")
            except:
                indtib = np.nonzero(np.in1d(timeb, timea))[0]
                print ("solution2")
            """
            indtib = np.nonzero(np.in1d(timeb, timea))[0]
            #print timeb[pos]
            #print ("Here", timea)
            # If equal elements occur in time columns
            if len(indtib) > int(0.5*len(timeb)):
                logger.info('subtractStreams: Found identical timesteps - using simple subtraction')
                # get tb times for all matching indicies
                tb = np.asarray([timeb[ind] for ind in indtib])
                # Get indicies of stream_a of which times are present in matching tbs
                try:
                    idxA = np.argsort(timea)
                    sortedA = timea[idxA]
                    idxB = np.searchsorted(sortedA, tb)
                    #
                    indtia = idxA[idxB]
                except:
                    indtia = np.nonzero(np.in1d(tb, timea))[0]
                #print ("subtractStreams", len(timea),len(timeb),idxA,idxB, indtia, indtib)
                #print (np.nonzero(np.in1d(timea,tb))[0])
                #idxB = np.argsort(tb)
                #sortedB = tb[idxB]
                #idxA = np.searchsorted(sortedB, timea)
                #indtia = idxB[idxA]
                if len(indtia) == len(indtib):
                    nanind = []
                    for key in keys:
                        foundnan = False
                        keyind = KEYLIST.index(key)
                        #print key, keyind, len(sa.ndarray[keyind]), len(sb.ndarray[keyind])
                        #print indtia, indtib,len(indtia), len(indtib)
                        if len(sa.ndarray[keyind]) > 0 and len(sb.ndarray[keyind]) > 0:
                            for ind in indtia:
                                try:
                                    tmp = sa.ndarray[keyind][ind]
                                except:
                                    print(ind, keyind, len(indtia), len(sa.ndarray[keyind]))
                            vala = [sa.ndarray[keyind][ind] for ind in indtia]
                            valb = [sb.ndarray[keyind][ind] for ind in indtib]
                            diff = np.asarray(vala).astype(float) - np.asarray(valb).astype(float)
                            if isnan(diff).any():
                                foundnan = True
                            if foundnan:
                                nankeys = [ind for ind,el in enumerate(diff) if isnan(el)]
                                nanind.extend(nankeys)
                            array[keyind] = diff
                    nanind = np.unique(np.asarray(nanind))
                    array[0] = np.asarray([sa.ndarray[0][ind] for ind in indtia])
                    if foundnan:
                        for ind,elem in enumerate(array):
                            if len(elem) > 0:
                                array[ind] = np.delete(np.asarray(elem), nanind)
                    array = np.asarray(array)
            else:
                if debug:
                    print("Did not find identical timesteps - linearily interpolating stream b")
                    print("- please note... this needs considerably longer")
                    print("- put in the larger (higher resolution) stream as stream_a")
                    print("- otherwise you might wait endless")
                # interpolate b
                function = sb.interpol(keys)
                #print function, len(function), keys, sa.ndarray, sb.ndarray
                # Get a list of indicies for which timeb values are
                #   in the vicintiy of a (within half of samplingrate)
                indtia = [idx for idx, el in enumerate(timea) if np.min(np.abs(timeb-el))/(minsamprate/24./3600.)*2 <= 1.]  # This selcetion requires most of the time
                # limit time range to valued covered by the interpolation function
                #print len(indtia), len(timeb), np.asarray(indtia)
                indtia = [elem for elem in indtia if function[1] < timea[elem] < function[2]]
                #t2temp = datetime.utcnow()
                #print "Timediff %s" % str(t2temp-t1temp)
                #print len(indtia), len(timeb), np.asarray(indtia)
                #print function[1], sa.ndarray[0][indtia[0]], sa.ndarray[0][indtia[-1]], function[2]
                if len(function) > 0:
                    nanind = []
                    sa.ndarray[0] = sa.ndarray[0].astype(float)
                    for key in keys:
                        foundnan = False
                        keyind = KEYLIST.index(key)
                        #print key, keyind
                        #print len(sa.ndarray[keyind]),len(sb.ndarray[keyind]), np.asarray(indtia)
                        if len(sa.ndarray[keyind]) > 0 and len(sb.ndarray[keyind]) > 0 and key in NUMKEYLIST: # and key in function:
                            #check lengths of sa.ndarray and last value of indtia
                            indtia = list(np.asarray(indtia)[np.asarray(indtia)<len(sa.ndarray[0])])
                            #print keyind, len(indtia), len(sa.ndarray[keyind]), indtia[0], indtia[-1]
                            # Convert array to float just in case
                            sa.ndarray[keyind] = sa.ndarray[keyind].astype(float)
                            #print sa.ndarray[4][indtia[-2]]
                            vala = [sa.ndarray[keyind][ind] for ind in indtia]
                            #print "VALA", np.asarray(vala)
                            valb = [float(function[0]['f'+key]((sa.ndarray[0][ind]-function[1])/(function[2]-function[1]))) for ind in indtia]
                            #print "VALB", np.asarray(valb)
                            diff = np.asarray(vala) - np.asarray(valb)
                            if isnan(diff).any():
                                foundnan = True
                            if foundnan:
                                nankeys = [ind for ind,el in enumerate(diff) if isnan(el)]
                                nanind.extend(nankeys)
                            array[keyind] = diff
                    nanind = np.unique(np.asarray(nanind))
                    array[0] = np.asarray([sa.ndarray[0][ind] for ind in indtia])
                    if foundnan:
                        for ind,elem in enumerate(array):
                            if len(elem) > 0:
                                array[ind] = np.delete(np.asarray(elem), nanind)
                    array = np.asarray(array)

            #t2e = datetime.utcnow()
            #print "Total Timediff %s" % str(t2e-t1s)
            #print array, len(array), len(array[0])

            for key in keys:
                try:
                    sa.header['col-'+key] = 'delta '+key
                except:
                    pass
                try:
                    sa.header['unit-col-'+key] = sa.header['unit-col-'+key]
                except:
                    pass
            try:
                sa.header['SensorID'] = sa.header['SensorID']+'-'+sb.header['SensorID']
            except:
                pass

            #subtractedstream = DataStream([LineStruct()],sa.header,np.asarray(array))
            #for key in keys:
            #    subtractedstream = subtractedstream._drop_nans(key)

            return DataStream([LineStruct()],sa.header,np.asarray(array))


    if np.min(timeb) < np.min(timea):
        stime = np.min(timea)
    else:
        stime = np.min(timeb)
    if np.max(timeb) > np.max(timea):
        etime = np.max(timea)
    else:
        etime = np.max(timeb)
    # if stream_b is longer than stream_a use one step after and one step before e and stime
    if etime < np.max(timeb):
        for idx, ttt in enumerate(timeb):
            if ttt > etime:
                try: # use slightly larger time range for interpolation
                    etimeb = timeb[idx+1]
                except:
                    etimeb = timeb[idx]
                break
    else:
        etimeb = etime

    if stime > np.min(timeb):
        for idx, ttt in enumerate(timeb):
            if ttt > stime:
                stimeb = timeb[idx-1]
                break
    else:
        stimeb = stime

    if (etime <= stime):
        logger.error('subtractStreams: Streams are not overlapping!')
        return stream_a


    # ----------------------------------------------
    # --------- if loop for new technique ----------
    # ----------------------------------------------
    # changes from 23 May 2014 by leon
    # modification to keep original streams unbiased
    # need to check all subsequent functions !!!
    if newway == True:
        subtractedstream = DataStream([],{},np.asarray([[] for key in KEYLIST]))
        sa = stream_a.trim(starttime=num2date(stime).replace(tzinfo=None), endtime=num2date(etime).replace(tzinfo=None)+timedelta(seconds=sampratea),newway=True)
        sb = stream_b.trim(starttime=num2date(stimeb).replace(tzinfo=None), endtime=num2date(etimeb).replace(tzinfo=None)+timedelta(seconds=samprateb),newway=True)
        samplingrate_b = sb.get_sampling_period()

        logger.info('subtractStreams (newway): Time range from %s to %s' % (num2date(stime).replace(tzinfo=None),num2date(etime).replace(tzinfo=None)))

        # Interpolate stream_b
        # --------------------
        function = sb.interpol(keys)
        taprev = 0

        # Check for the following cases:
        # 1- No overlap of a and b
        # 2- a shorter and fully covered by b
        ## Better and probably faster way: recalc b at timesteps of a
        ## subtract a-b
        if not ndtype:
            print("Running for LineStruct")
            for idx,elem in enumerate(sa):
                tb, itmp = find_nearest(timeb,elem.time)
                index = timeblst.index(tb)

                # get index of tb
                #tib = list(timeb)
                #index = tib.index(tb)

                #print tb-elem.time

                # --------------------------------------------------------------------
                # test whether data points are present within a sampling rate distance
                # and whether the timestep is within the interpolation range
                # --------------------------------------------------------------------
                if abs(tb-elem.time) < samplingrate_b  and function[1]<=elem.time<=function[2]:
                    newline = LineStruct()
                    for key in keys:
                        newline.time = elem.time
                        #valstreama = eval('elem.'+key)
                        valstreama = getattr(elem,key)
                        try:
                            valstreamb = float(function[0]['f'+key]((elem.time-function[1])/(function[2]-function[1])))
                            #realvalb = eval('stream_b[index].'+key)
                            realvalb = getattr(stream_b[index],key)
                            #if isnan(realvalb):
                            #    print "Found"
                            if isnan(valstreama) or isnan(realvalb):
                                newval = 'NAN'
                            else:
                                newval = valstreama - valstreamb
                        except:
                            newval = 'NAN'
                        setattr(newline, key, float(newval))
                        #exec('newline.'+key+' = float(newval)')
                    subtractedstream.add(newline)

        # Finally get gaps from stream_b and remove these gaps from the subtracted stream for all keys
        # TODO

        # XXX Take care: New header info replaces header information of stream a and b
        for key in keys:
            try:
                subtractedstream.header['col-'+key] = 'delta '+key
            except:
                pass
            try:
                subtractedstream.header['unit-col-'+key] = sa.header['unit-col-'+key]
            except:
                pass
        try:
            subtractedstream.header['SensorID'] = sa.header['SensorID']+'-'+sb.header['SensorID']
        except:
            pass

        return subtractedstream

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # XXX If not newway:
    # Traditional version is used which replaces stream_a with the subtracted info
    # TODO test the stablilty of newway and make it default
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    # Take only the time range of the shorter stream
    # Important for baselines: extend the absfile to start and endtime of the stream to be corrected
    stream_a = stream_a.trim(starttime=num2date(stime).replace(tzinfo=None), endtime=num2date(etime).replace(tzinfo=None))
    stream_b = stream_b.trim(starttime=num2date(stimeb).replace(tzinfo=None), endtime=num2date(etimeb).replace(tzinfo=None))

    samplingrate_b = stream_b.get_sampling_period()

    logger.info('subtractStreams: Time range from %s to %s' % (num2date(stime).replace(tzinfo=None),num2date(etime).replace(tzinfo=None)))

    # Interpolate stream_b
    function = stream_b.interpol(keys)
    taprev = 0
    for elem in stream_a:
        ta = elem.time
        if ta >= function[1]: # in records of different resolution the first element might be older then the function start
            functime = (ta-function[1])/(function[2]-function[1])
            # Do the subtraction if there is is an element within stream b within twice the sampling rate distance
            # If not wite NaN to the diffs
            tb, itmp = find_nearest(timeb,ta)
            #Test whether a time_b event exists in the vicinity of ta and whether tb is within the time_b range otherwise interpolation fails
            if ta-samplingrate_b < tb < ta+samplingrate_b and timeb[0]<ta<timeb[-1] :
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        logger.error("subtractStreams: Column key %s not valid!" % key)
                    fkey = 'f'+key
                    try:
                        if fkey in function[0] and not isnan(eval('stream_b[itmp].' + key)):
                            newval = function[0][fkey](functime)
                            exec('elem.'+key+' -= float(newval)')
                        else:
                            setattr(elem, key, float(NaN))
                            #exec('elem.'+key+' = float(NaN)')
                    except:
                        logger.warning("subtractStreams: Check why exception was thrown.")
                        setattr(elem, key, float(NaN))
                        #exec('elem.'+key+' = float(NaN)')
            else:
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        logger.error("subtractStreams: Column key %s not valid!" % key)
                    fkey = 'f'+key
                    if fkey in function[0]:
                        setattr(elem, key, float(NaN))
                        #exec('elem.'+key+' = float(NaN)')
        else: # put NaNs in cloumn if no interpolated values in b exist
            for key in keys:
                if not key in KEYLIST[1:16]:
                    logger.error("subtractStreams: Column key %s not valid!" % key)
                fkey = 'f'+key
                if fkey in function[0]:
                    setattr(elem, key, float(NaN))
                    #exec('elem.'+key+' = float(NaN)')

    try:
        headera['SensorID'] = headera['SensorID']+'-'+headerb['SensorID']
    except:
        pass
    logger.info('subtractStreams: Stream-subtraction finished.')

    return DataStream(stream_a, headera)


def stackStreams(streamlist, **kwargs): # TODO
    """
    DEFINITION:
        Stack the contents of data streams. Eventually calculate mean and uncertainty.
        Only time steps present in all data streams are stacked.

    PARAMETERS:
    Variables:
        - streamlist:   (list) list of DataStreams

    Optional:
        - keys:         (list) keys to be stacked/averaged
        - get:          (string) obtain either "sum" or "mean" of the stacked data
        - skipdate:     (bool) if True then date is not regarded in stream. To be used
                               for stacking data of different days at same time
        - uncert:       (bool) in case of get='mean' and provided that x,y,z or f keys are available
                               and 'dx','dy','dz','df' columns are empty, the latter will be filled
                               with standard deviations

    RETURNS:
        A DataStream

    EXAMPLE:
        # e.g. Getting average 3 hour K values of quiet days
        >>> meanstream = stackStreams([kvals_of_severals_days],skipdate=True,get='mean')
        # Mean variation curve of two different variometers
        >>> meanstream = stackStreams([vario1,vario2],get='mean',uncert='True')

    APPLICATION:
    """

    keys = kwargs.get('keys')
    skipdate = kwargs.get('skipdate')
    get = kwargs.get('get')
    uncert = kwargs.get('uncert')

    result = DataStream()

    if not isinstance(streamlist, (list, tuple)):
        print("stackStream: provide a list of streams to be stacked")
        return result

    if not len(streamlist[0].ndarray[0]) > 0:
        return result

    if not len(streamlist) > 1:
        return streamlist[0]

    result = streamlist[0].copy()
    result = result.removeduplicates()
    timea = result.ndarray[0]
    numday = int(result.ndarray[0][-1])
    #skipdate = True
    if skipdate:
        print("1", timea)
        timea = np.asarray([elem-numday for elem in timea])
        print("2", timea)
        #timea = np.asarray(list(set(timea)))

    if not keys:
        keys = result._get_key_headers(numerical=True)
    keys = [key for key in keys if key in NUMKEYLIST]

    sumarray = [[] for key in KEYLIST]
    for idx,stream in enumerate(streamlist):
        if idx == 0:
            pass
        else:
            #### ###########################################################
            #### Important prerequisite: timeb needs to be a subset of timea
            #### ###########################################################
            #1) truncate stream to time range of stream a
            #print stream.length()
            stream = stream.trim(starttime=np.min(result.ndarray[0]),endtime=np.max(result.ndarray[0]))
            #print stream.length()
            timeb = stream.ndarray[0]
            if skipdate:
                numday = int(stream.ndarray[0][-1])
                timeb = np.asarray([elem-numday for elem in timeb])

            idxA = np.argsort(timea)
            sortedA = timea[idxA]
            # now get all projected indicies for timeb data within the sorted timea list
            indb_a = np.searchsorted(sortedA, timeb) # indicies of each timeb record to be
                                                     # inserted into timea
            indtia = idxA[indb_a]
            """
            print timea[indtia]
            print timeb

            print "A", len(timea), pos
            # identify all indicies of b of which times are present in first stream
            timeb = stream.ndarray[0]
            if skipdate:
                numday = int(stream.ndarray[0][-1])
                timeb = np.asarray([elem-numday for elem in timeb])
            print "B", len(timeb)
            idxB = np.argsort(timeb)
            sortedB = timeb[idxB]
            inda_b = np.searchsorted(sortedB, timea) # indicies of each timea record to be
            # Drop all indicies exceeding the range of idxB
            # (all values above timeb range will get an index max(idxB) +1)

            pos = idxB[inda_b]

            print len(idxA)
            #idxA = np.asarray(list(set(idxA)))
            print len(idxA)
            # -> idxA contains all indicies of timea values present in timeb
            print idxA
            test = timea[idxA]
            idxB = np.searchsorted(sortedB, timea[idxA])
            #idxB = np.asarray(list(set(idxB)))
            # -> idxB contains all indicies of timeb values present in timea[idxA] values
            print "C", len(idxA), len(idxB)
            if not len(idxA) == len(idxB):
                print ("stackStreams: Error with lengths of streams - check algorythm")
            indtia = idxA
            indtib = idxB

            #idxA = np.searchsorted(sortedB, timea)
            #indtib = idxB[idxA]
            # identify all indicies of first stream of which times are present in current stream
            #tb = np.asarray([timeb[ind] for ind in indtib])
            # Get indicies of stream_a of which times are present in matching tbs and rewrite timea
            #idxA = np.argsort(timea)
            #sortedA = timea[idxA]
            #idxB = np.searchsorted(sortedA, tb)
            indtia = idxA[idxB]
            #timea = np.asarray([timea[ind] for ind in indtia])
            #print "D", len(indtia), len(indtib)
            # Now cycle through keys and stack data
            """
            for key in keys:
                # firstly create an array with streamlist[0] data
                keyind = KEYLIST.index(key)
                #valb = [stream.ndarray[keyind][ind] for ind in indtib]
                #print keyind, idx, indtia
                if idx == 1:
                    if len(result.ndarray[keyind]) > 0:
                        vala = [[float(result.ndarray[keyind][ind])] for ind in idxA]
                    else:
                        vala = [[]]
                else:
                    vala = [sumarray[keyind][ind] for ind in idxA]
                if len(result.ndarray[keyind]) > 0 and len(stream.ndarray[keyind]) > 0:
                    for index,ind in enumerate(indb_a):
                        vala[ind].append(stream.ndarray[keyind][index])
                #print np.asarray(vala)
                sumarray[keyind] = vala

    # Determine position of delta values
    pos = KEYLIST.index('x')
    dpos = KEYLIST.index('dx')
    dif = dpos-pos
    #print np.asarray(sumarray)

    array = [[] for key in KEYLIST]
    for idx,elem in enumerate(sumarray):
        if len(elem) > 0 and not idx in [1+dif,2+dif,3+dif,4+dif]:
            #if uncert and idx in [1,2,3,4]:
            #    array[idx+dif] = []
            for el in elem:
                if get == 'mean':
                    try:
                        val = np.nanmean(el) # numpy after 1.11
                    except:
                        val = np.mean([e for e in el if not np.isnan(e)])
                    if uncert and idx in [1,2,3,4]:
                        #print idx
                        val2 = np.std([e for e in el if not np.isnan(e)])
                        array[idx+dif].append(val2)
                else:
                    val = sum([e for e in el if not np.isnan(e)])
                array[idx].append(val)

    for idx,elem in enumerate(array):
        array[idx] = np.asarray(array[idx])
    array[0] = np.asarray([result.ndarray[0][ind] for ind in idxA])
    array = np.asarray(array)
    #print array

    return DataStream([LineStruct()],result.header,array)


def compareStreams(stream_a, stream_b):
    '''
    DEFINITION:
        Default function will compare stream_a to stream_b. If data is missing in
        a or is different, it will be filled in with that from b.
        stream_b here is the reference stream.

    PARAMETERS:
    Variables:
        - stream_a:     (DataStream) First stream
        - stream_b:     (DataStream) Second stream, which is compared to stream_a for differences

    RETURNS:
        - stream_a:     (DataStream) Description.

    EXAMPLE:
        >>> compareStreams(db_stream, pos_stream)

    APPLICATION:

    TODO:
        - Add in support for insert and replace to be optional. (Worthwhile?)

    '''

    insert = True
    replace = True

    # Do the sampling periods match?
    samplingrate_a = stream_a.get_sampling_period()
    samplingrate_b = stream_b.get_sampling_period()

    if samplingrate_a != samplingrate_b:
        logger.error('CompareStreams: Cannot compare streams with different sampling rates!')
        return stream_a

    # Do the timelines overlap?
    timea = stream_a._get_column('time')
    timeb = stream_b._get_column('time')

    if np.min(timeb) < np.min(timea):
        stime = np.min(timea)
    else:
        stime = np.min(timeb)
    if np.max(timeb) > np.max(timea):
        etime = np.max(timea)
    else:
        etime = np.max(timeb)

    if (etime <= stime):
        logger.error('compareStreams: Streams do not overlap!')
        return stream_a

    # Trim to overlapping areas:
    stream_a = stream_a.trim(starttime=num2date(stime).replace(tzinfo=None),
                                endtime=num2date(etime).replace(tzinfo=None))
    stream_b = stream_b.trim(starttime=num2date(stime).replace(tzinfo=None),
                                endtime=num2date(etime).replace(tzinfo=None))


    logger.info('compareStreams: Starting comparison...')

    # Compare value for value between the streams:

    flag_len = False

    t_a = stream_a._get_column('time')
    t_b = stream_b._get_column('time')

    # Check length:
    if len(t_a) < len(t_b):
        logger.debug("compareStreams: Missing data in main stream.")
        flag_len = True

    # If the lengths are the same, compare single values for differences:
    if not flag_len:
        for i in range(len(t_a)):
            for key in FLAGKEYLIST:
                exec('val_a = stream_a[i].'+key)
                exec('val_b = stream_b[i].'+key)
                if not isnan(val_a):
                    if val_a != val_b:
                        logger.debug("compareStreams: Data points do not match: %s and %s at time %s." % (val_a, val_b, stream_a[i].time))
                        if replace == True:
                            exec('stream_a[i].'+key+' = stream_b[i].'+key)

    # If the lengths are different, find where values are missing:
    else:
        for i in range(len(t_b)):
            if stream_a[i].time == stream_b[i].time:
                for key in FLAGKEYLIST:
                    exec('val_a = stream_a[i].'+key)
                    exec('val_b = stream_b[i].'+key)
                    if not isnan(val_a):
                        if val_a != val_b:
                            logger.debug("compareStreams: Data points do not match: %s and %s at time %s." % (val_a, val_b, stream_a[i].time))
                            if replace == True:
                                exec('stream_a[i].'+key+' = stream_b[i].'+key)
            else:       # insert row into stream_a
                logger.debug("compareStreams: Line from secondary stream missing in main stream. Timestamp: %s." % stream_b[i].time)
                if insert == True:
                    row = LineStruct()
                    stream_a.add(row)
                    for key in KEYLIST:
                        temp = stream_a._get_column(key)
                        if len(temp) > 0:
                            for j in range(i+1,len(stream_a)):
                                exec('stream_a[j].'+key+' = temp[j-1]')
                            exec('stream_a[i].'+key+' = stream_b[i].'+key)

    logger.info('compareStreams: Finished comparison!')
    return stream_a


# Some helpful methods
def array2stream(listofarrays, keystring,starttime=None,sr=None):
        """
        DESCRIPTION:
            Converts an array to a data stream
        """
        keys = keystring.split(',')
        if not len(listofarrays) > 0:
            print("Specify a list of array - aborting")
            return
        if not len(keys) == len(listofarrays):
            print("Keys do not match provided arrays - aborting")
            return
        st = DataStream()
        if not 'time' in keys:
            if not starttime:
                print("No timing information provided - aborting")
                return
            else:
                #fill time column
                val = st._testtime(starttime)
                for ind, elem in enumerate(listofarrays[0]):
                    #emptyline = [None for elem in KEYLIST[:5]]
                    #emptyline[0] = date2num(val)
                    emptyline = LineStruct() ### Upper solution is about 1.5 times faster
                    emptyline.time = date2num(val)
                    st.add(emptyline)
                    val = val+timedelta(seconds=sr)
            add = 1
        else:
            for ind, elem in enumerate(listofarrays[0]):
                #emptyline = [None for elem in KEYLIST[:5]]
                emptyline = LineStruct() ### Upper solution is about 1.5 times faster
                emptyline.time = elem
                st.add(emptyline)
            add = 0

        for ind, ar in enumerate(listofarrays):
            #print "Finished", len(ar)
            key = keys[ind]
            index = KEYLIST.index(key)
            for i,elem in enumerate(ar):
                st[i][index] = elem

        return st


def obspy2magpy(opstream, keydict={}):
    """
    Function for converting obspy streams to magpy streams.

    INPUT:
        - opstream          obspy.core.stream.Stream object
                            Obspy stream
        - keydict           dict
                            ID of obspy traces to assign to magpy keys

    OUTPUT:
        - mpstream          Stream in magpy format

    EXAMPLE:
        >>> mpst = obspy2magpy(opst, keydict={'nn.e6046.11.p0': 'x', 'nn.e6046.11.p1': 'y'})
    """
    array = [[] for key in KEYLIST]
    mpstream = DataStream()

    # Split into channels:
    datadict = {}
    for tr in opstream.traces:
        try:
            datadict[tr.id].append(tr)
        except:
            datadict[tr.id] = [tr]

    twrite = False
    tind = KEYLIST.index("time")
    fillkeys = ['var1', 'var2', 'var3', 'var4', 'var5', 'x', 'y', 'z', 'f']
    for channel in datadict:
        data = datadict[channel]
        # Sort by time:
        data.sort(key=lambda x: x.stats.starttime)

        # Assign magpy keys:
        if channel in keydict:
            ind = KEYLIST.index(keydict[channel])
        else:
            try:
                key = fillkeys.pop(0)
                print("Writing {i} data to key {k}.".format(i=channel, k=key))
            except IndexError:
                print("CAUTION! Out of available keys for data. {} will not be contained in stream.".format(key))
            ind = KEYLIST.index(key)
        mpstream.header['col-'+key] = channel
        mpstream.header['unit-col-'+key] = ''

        # Arrange in preparatory array:
        t = []
        for d in data:
            if not twrite: # Only write time array once (for multiple channels):
                _diff = d.stats.endtime.datetime - d.stats.starttime.datetime
                # Work time in milliseconds:
                diff = _diff.days*24.*60.*60.*1000. + _diff.seconds*1000. + _diff.microseconds/1000.
                numval = int(diff/1000. * d.stats.sampling_rate) + 1
                array[tind] += [date2num(d.stats.starttime.datetime + timedelta(milliseconds=x/d.stats.sampling_rate*1000.))
                             for x in range(0, numval)]
            else: # Check anyway
                if date2num(d.stats.starttime.datetime) not in array[tind]:
                    raise Exception("Time arrays do not match!") # could be handled
            array[ind] += list(d.data)
        twrite = True

    # Convert to ndarrays:
    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])

    mpstream = DataStream([], mpstream.header, np.asarray(array))

    return mpstream


def extractDateFromString(datestring):
    """
    DESCRIPTION:
       Method to identify a date within a string (usually the filename).
       It is used by most file reading procedures
    RETURNS:
       A list of datetimeobjects with first and last date (month, year)
       or the day (dailyfiles)
    APPLICATION:
       datelist = extractDateFromString(filename)
    """
    date = False
    # get day from filename (platform independent)
    localechanged = False

    try:
        splitpath = os.path.split(datestring)
        daystring = splitpath[1].split('.')[0]
    except:
        daystring = datestring

    try:
        # IMPORTANT: when interpreting %b, then the local time format is very important (OKT vc OCT)
        old_locale = locale.getlocale()
        locale.setlocale(locale.LC_TIME, ('en_US', 'UTF-8'))
        localechanged = True
    except:
        pass
    try:
        #logger.warning("Got Here2: {}".format(daystring[-7:]))
        #logger.warning("Got Here3: {}".format(datetime.strptime(str(daystring[-7:]), '%b%d%y')))
        date = datetime.strptime(daystring[-7:], '%b%d%y')
        dateform = '%b%d%y'
        if localechanged:
            locale.setlocale(locale.LC_TIME, old_locale)
    except:
        # test for day month year
        try:
            tmpdaystring = re.findall(r'\d+',daystring)[0]
        except:
            # no number whatsoever
            return False
        testunder = daystring.replace('-','').split('_')
        for i in range(len(testunder)):
            try:
                numberstr = re.findall(r'\d+',testunder[i])[0]
            except:
                numberstr = '0'
            if len(numberstr) > 4 and int(numberstr) > 100000: # There needs to be year and month
                tmpdaystring = numberstr
            elif len(numberstr) == 4 and int(numberstr) > 1900: # use year at the end of string
                tmpdaystring = numberstr

        if len(tmpdaystring) > 8:
            try: # first try whether an easy pattern can be found e.g. test12014-11-22
                match = re.search(r'\d{4}-\d{2}-\d{2}', daystring)
                date = datetime.strptime(match.group(), '%Y-%m-%d').date()
            except:  # if not use the first 8 digits
                tmpdaystring = tmpdaystring[:8]
                pass
        if len(tmpdaystring) == 8:
            try:
                dateform = '%Y%m%d'
                date = datetime.strptime(tmpdaystring,dateform)
            except:
                # log ('dateformat in filename could not be identified')
                pass
        elif len(tmpdaystring) == 6:
            try:
                dateform = '%Y%m'
                date = datetime.strptime(tmpdaystring,dateform)
                from calendar import monthrange
                datelist = [datetime.date(date), datetime.date(date + timedelta(days=monthrange(date.year,date.month)[1]-1))]
                return datelist
            except:
                # log ('dateformat in filename could not be identified')
                pass
        elif len(tmpdaystring) == 4:
            try:
                dateform = '%Y'
                date = datetime.strptime(tmpdaystring,dateform)
                date2 = datetime.strptime(str(int(tmpdaystring)+1),dateform)
                datelist = [datetime.date(date), datetime.date(date2-timedelta(days=1))]
                return datelist
            except:
                # log ('dateformat in filename could not be identified')
                pass

        if not date and len(daystring.split('_')[0]) > 8:
            try: # first try whether an easy pattern can be found e.g. test12014-11-22_00-00-00
                daystrpart = daystring.split('_')[0] # e.g. RCS
                match = re.search(r'\d{4}-\d{2}-\d{2}', daystrpart)
                date = datetime.strptime(match.group(), '%Y-%m-%d').date()
                return [date]
            except:
                pass

        if not date:
            # No Date found so far - now try last 6 elements of string () e.g. SG gravity files
            try:
                tmpdaystring = re.findall(r'\d+',daystring)[0]
                dateform = '%y%m%d'
                date = datetime.strptime(tmpdaystring[-6:],dateform)
            except:
                pass

    try:
        return [datetime.date(date)]
    except:
        return [date]


def testTimeString(time):
    """
    Check the date/time input and returns a datetime object if valid:

    ! Use UTC times !

    - accepted are the following inputs:
    1) absolute time: as provided by date2num
    2) strings: 2011-11-22 or 2011-11-22T11:11:00
    3) datetime objects by datetime.datetime e.g. (datetime(2011,11,22,11,11,00)
    """

    timeformats = ["%Y-%m-%d",
                   "%Y-%m-%dT%H:%M:%S",
                   "%Y-%m-%d %H:%M:%S.%f",
                   "%Y-%m-%dT%H:%M:%S.%f",
                   "%Y-%m-%d %H:%M:%S"
                   ]

    if isinstance(time, float) or isinstance(time, int):
        try:
            timeobj = num2date(time).replace(tzinfo=None)
        except:
            raise TypeError
    elif isinstance(time, str): # test for str only in Python 3 should be basestring for 2.x
        for i, tf in enumerate(timeformats):
            try:
                timeobj = datetime.strptime(time,tf)
                break
            except:
                j = i+1
                pass
        if j == len(timeformats):     # Loop found no matching format
            try:
                # Necessary to deal with old 1000000 micro second bug
                timearray = time.split('.')
                print(timearray)
                if len(timearray) > 1:
                    if timearray[1] == '1000000':
                        timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")+timedelta(seconds=1)
                    else:
                        # This would be wrong but leads always to a TypeError
                        timeobj = datetime.strptime(timearray[0],"%Y-%m-%d %H:%M:%S")
            except:
                raise TypeError
    elif not isinstance(time, datetime):
        raise TypeError
    else:
        timeobj = time

    return timeobj


def denormalize(column, startvalue, endvalue):
    """
    converts [0:1] back with given start and endvalue
    """
    normcol = []
    if startvalue>0:
        if endvalue < startvalue:
            raise ValueError("start and endval must be given, endval must be larger")
        else:
            for elem in column:
                normcol.append((elem*(endvalue-startvalue)) + startvalue)
    else:
        raise ValueError("start and endval must be given as absolute times")

    return normcol


def find_nearest(array, value):
    idx = (np.abs(array-value)).argmin()
    return array[idx], idx


def maskNAN(column):
    """
    Tests for NAN values in column and usually masks them
    """

    try: # Test for the presence of nan values
        column = np.asarray(column).astype(float)
        val = np.mean(column)
        numdat = True
        if isnan(val): # found at least one nan value
            for el in column:
                if not isnan(el): # at least on number is present - use masked_array
                    num_found = True
            if num_found:
                mcolumn = np.ma.masked_invalid(column)
                numdat = True
                column = mcolumn
            else:
                numdat = False
                logger.warning("NAN warning: only nan in column")
                return []
    except:
        numdat = False
        #logger.warning("Here: NAN warning: only nan in column")
        return []

    return column


def nan_helper(y):
    """Helper to handle indices and logical indices of NaNs. Taken from eat (http://stackoverflow.com/questions/6518811/interpolate-nan-values-in-a-numpy-array)

    Input:
        - y, 1d numpy array with possible NaNs
    Output:
        - nans, logical indices of NaNs
        - index, a function, with signature indices= index(logical_indices),
         to convert logical indices of NaNs to 'equivalent' indices
    Example:
        >>> # linear interpolation of NaNs
        >>> nans, x= nan_helper(y)
        >>> y[nans]= np.interp(x(nans), x(~nans), y[~nans])
    """
    y = np.asarray(y).astype(float)
    return np.isnan(y), lambda z: z.nonzero()[0]


def nearestPow2(x):
    """
    Function taken from ObsPy
    Find power of two nearest to x
    >>> nearestPow2(3)
    2.0
    >>> nearestPow2(15)
    16.0
    :type x: Float
    :param x: Number
    :rtype: Int
    :return: Nearest power of 2 to x
    """

    a = pow(2, ceil(np.log2(x)))
    b = pow(2, floor(np.log2(x)))
    if abs(a - x) < abs(b - x):
        return a
    else:
        return b


def test_time(time):
    """
        Check the date/time input and returns a datetime object if valid:

        ! Use UTC times !

        - accepted are the following inputs:
        1) absolute time: as provided by date2num
        2) strings: 2011-11-22 or 2011-11-22T11:11:00
        3) datetime objects by datetime.datetime e.g. (datetime(2011,11,22,11,11,00)

    """
    if isinstance(time, float) or isinstance(time, int):
        try:
            timeobj = num2date(time).replace(tzinfo=None)
        except:
            raise TypeError
    elif isinstance(time, str):
        try:
            timeobj = datetime.strptime(time,"%Y-%m-%d")
        except:
            try:
                timeobj = datetime.strptime(time,"%Y-%m-%dT%H:%M:%S")
            except:
                try:
                    timeobj = datetime.strptime(time,"%Y-%m-%d %H:%M:%S.%f")
                except:
                    try:
                        timeobj = datetime.strptime(time,"%Y-%m-%d %H:%M:%S")
                    except:
                        raise TypeError
    elif not isinstance(time, datetime):
        raise TypeError
    else:
        timeobj = time

    return timeobj


def LeapTime(t):
    """
    converts strings to datetime, considering leap seconds
    """ 
    nofrag, frag = t.split('.')
    nofrag_dt = time.strptime(nofrag, "%Y-%m-%dT%H:%M:%S")
    ts = datetime.fromtimestamp(time.mktime(nofrag_dt))
    dt = ts.replace(microsecond=int(frag))
    return dt


def convertGeoCoordinate(lon,lat,pro1,pro2):
    """
    DESCRIPTION:
       converts longitude latitude using the provided epsg codes
    PARAMETER:
       lon	(float) longitude
       lat	(float) latitude
       pro1	(string) epsg code for source ('epsg:32909')
       pro2	(string) epsg code for output ('epsg:4326')
    RETURNS:
       lon, lat	(floats) longitude,latitude
    APLLICATION:

    USED BY:
       writeIMAGCDF,
    """
    try:
        from pyproj import Proj, transform
        p1 = Proj(init=pro1)
        x1 = float(lon)
        y1 = float(lat)
        # projection 2: WGS 84
        p2 = Proj(init=pro2)
        # transform this point to projection 2 coordinates.
        x2, y2 = transform(p1,p2,x1,y1)
        return x2, y2
    except:
        return lon, lat

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Now import the child classes with formats etc
# Otherwise DataStream etc will not be known
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
from magpy.lib.magpy_formats import *



if __name__ == '__main__':

    import subprocess

    print()
    print("----------------------------------------------------------")
    print("TESTING: STREAM PACKAGE")
    print("THIS IS A TEST RUN OF THE MAGPY STREAM PACKAGE.")
    print("All main methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()

    print("Please enter path of a (variometer) data file for testing:")
    print("(e.g. /srv/archive/WIC/LEMI025/LEMI025_2014-05-07.bin)")
    while True:
        filepath = raw_input("> ")
        if os.path.exists(filepath):
            break
        else:
            print("Sorry, that file doesn't exist. Try again.")

    now = datetime.utcnow()
    testrun = 'streamtest_'+datetime.strftime(now,'%Y%m%d-%H%M')
    t_start_test = time.time()
    errors = {}
    print()
    print(datetime.utcnow(), "- Starting stream package test. This run: %s." % testrun)

    while True:

        # Step 1 - Read data
        try:
            teststream = read(filepath)
            print(datetime.utcnow(), "- Stream read in.")
        except Exception as excep:
            errors['read'] = str(excep)
            print(datetime.utcnow(), "--- ERROR reading stream. Aborting test.")
            break

        # Step 2 - Rotate data (why not?)
        try:
            teststream.rotation(alpha=1.0)
            print(datetime.utcnow(), "- Rotated.")
        except Exception as excep:
            errors['rotation'] = str(excep)
            print(datetime.utcnow(), "--- ERROR rotating stream.")

        # Step 3 - Offset data
        try:
            test_offset = {'x': 150, 'y': -2000, 'z': 3.2}
            teststream.offset(test_offset)
            print(datetime.utcnow(), "- Offset.")
        except Exception as excep:
            errors['offset'] = str(excep)
            print(datetime.utcnow(), "--- ERROR offsetting stream.")

        # Step 4 - Find outliers
        try:
            teststream.remove_outlier()
            print(datetime.utcnow(), "- Flagged outliers.")
        except Exception as excep:
            errors['remove_outlier'] = str(excep)
            print(datetime.utcnow(), "--- ERROR flagging outliers.")

        # Step 5 - Remove flagged
        try:
            teststream.remove_flagged()
            print(datetime.utcnow(), "- Removed flagged outliers.")
        except Exception as excep:
            errors['remove_flagged'] = str(excep)
            print(datetime.utcnow(), "--- ERROR removing flagged outliers.")

        # Step 6 - Filter
        try:
            teststream.filter()
            print(datetime.utcnow(), "- Filtered.")
        except Exception as excep:
            errors['filter'] = str(excep)
            print(datetime.utcnow(), "--- ERROR filtering.")

        # Step 7 - Write
        try:
            teststream.write('.',
                        filenamebegins='%s_' % testrun,
                        filenameends='.min',
                        dateformat='%Y-%m-%d',
                        format_type='IAGA')
            print(datetime.utcnow(), "- Data written out to file.")
        except Exception as excep:
            errors['write'] = str(excep)
            print(datetime.utcnow(), "--- ERROR writing data to file.")

        # STILL TO ADD:
        # - smooth?
        # - plot
        # - mergeStreams
        # - subtractStreams
        # - date_offset
        # - interpol
        # - fit
        # - differentiate
        # - aic_calc
        # - k_fmi
        # - integrate
        # - baseline
        # - trim
        # - resample

        # If end of routine is reached... break.
        break

    t_end_test = time.time()
    time_taken = t_end_test - t_start_test
    print(datetime.utcnow(), "- Stream testing completed in %s s. Results below." % time_taken)

    print()
    print("----------------------------------------------------------")
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(str(errors.keys()))
        print()
        print("Would you like to print the exceptions thrown?")
        excep_answer = raw_input("(Y/n) > ")
        if excep_answer.lower() == 'y':
            i = 0
            for item in errors:
                print(errors.keys()[i] + " error string:")
                print("    " + errors[errors.keys()[i]])
                i += 1
    print()
    print("Hit enter to delete temporary files. (Or type N to keep.)")
    tempfile_answer = raw_input("> ")
    if tempfile_answer.lower() != 'n':
        del_test_files = 'rm %s*' % testrun
        subprocess.call(del_test_files,shell=True)
    print()
    print("Good-bye!")
    print("----------------------------------------------------------")










































# That's all, folks!
