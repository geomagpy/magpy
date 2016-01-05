#!/usr/bin/env python
"""
MagPy-General: Standard pymag package containing the following classes:
Written by Roman Leonhardt, Rachel Bailey 2011/2012/2013/2014
Version 0.1 (from the 23.02.2012)
"""

# ----------------------------------------------------------------------------
# Part 1: Import routines for packages
# ----------------------------------------------------------------------------

logpygen = ''           # temporary loggerstream variable
badimports = []         # List of missing packages
nasacdfdir = "c:\CDF Distribution\cdf33_1-dist\lib"

print "Initiating MagPy..."

try:
    from version import __version__
except:
    from magpy.version import __version__
print "MagPy version", __version__
magpyversion = __version__

# Standard packages
# -----------------
try:
    import csv
    import pickle
    import copy_reg
    import types
    import struct
    import logging
    import sys, re
    import thread, time, string, os, shutil
    import copy as cp
    import fnmatch
    import urllib2
    from tempfile import NamedTemporaryFile
    import warnings
    from glob import glob, iglob, has_magic
    from StringIO import StringIO
    import operator # used for stereoplot legend
    from itertools import groupby
    from operator import itemgetter
except ImportError as e:
    logpygen += "CRITICAL MagPy initiation ImportError: standard packages.\n"
    badimports.append(e)

# Matplotlib
# ----------
try:
    import matplotlib
    try:
        if not os.isatty(sys.stdout.fileno()):   # checks if stdout is connected to a terminal (if not, cron is starting the job)
            print "No terminal connected - assuming cron job and using Agg for matplotlib"
            matplotlib.use('Agg') # For using cron
    except:
        print "Problems with identfying cron job - windows system?"
        pass
except ImportError as e:
    logpygen += "CRITICAL MagPy initiation ImportError: problem with matplotlib.\n"
    badimports.append(e)

try:
    version = matplotlib.__version__.replace('svn', '')
    try:
        version = map(int, version.replace("rc","").split("."))
        MATPLOTLIB_VERSION = version
    except:
        version = version.strip("rc")
        MATPLOTLIB_VERSION = version
    print "Loaded Matplotlib - Version %s" % str(MATPLOTLIB_VERSION)
    import matplotlib.pyplot as plt
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
    print "Loading Numpy and SciPy..."
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
    print "Loading Netcdf4 support ..."
    from netCDF4 import Dataset
except ImportError as e:
    logpygen += "MagPy initiation ImportError: NetCDF not available.\n"
    logpygen += "... if you want to use NetCDF format support please install a current version.\n"
    badimports.append(e)

# NASACDF - SpacePy
# -----------------
def findpath(name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return root
try:
    print "Loading SpacePy package cdf support ..."
    try:
        # check for windows
        nasacdfdir = findpath('libcdf.dll','C:\CDF Distribution')
        #print nasacdfdir
        os.putenv("CDF_LIB", nasacdfdir)
        print "trying CDF lib in %s" % nasacdfdir
        try:
            import spacepy.pycdf as cdf
        except KeyError as e:
            # Probably running at boot time - spacepy HOMEDRIVE cannot be detected
            badimports.append(e)
        except:
            print "Unexpected error"
            pass
        print "... success"
    except:
        os.putenv("CDF_LIB", "/usr/local/cdf/lib")
        print "trying CDF lib in /usr/local/cdf"
        try:
            import spacepy.pycdf as cdf
        except KeyError as e:
            # Probably running at boot time - spacepy HOMEDRIVE cannot be detected
            badimports.append(e)
        except:
            print "Unexpected error"
            pass
        print "... success"
except ImportError as e:
    logpygen += "MagPy initiation ImportError: NASA cdf not available.\n"
    logpygen += "... if you want to use NASA CDF format support please install a current version.\n"
    badimports.append(e)

# Utilities
# ---------
try:
    import smtplib
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEBase import MIMEBase
    from email.MIMEText import MIMEText
    from email.Utils import COMMASPACE, formatdate
    from email import Encoders
    #import smtplib
    from email.mime.text import MIMEText
    #from smtplib import SMTP_SSL as SMTP       # this invokes the secure SMTP protocol (port 465, uses SSL)
    from smtplib import SMTP                  # use this for standard SMTP protocol   (port 25, no encryption)
    #from email.MIMEText import MIMEText
except ImportError as e:
    logpygen += "MagPy initiation ImportError: Mailing functions not available.\n"
    badimports.append(e)

if logpygen == '':
    logpygen = "OK"
else:
    print logpygen
    print "Missing packages:"
    for item in badimports:
        print item
    print
    print "Moving on anyway..."
    #check = raw_input("Do you want to continue anyway? ")

# Logging
# ---------
# Select the home directory of the user (platform independent)
from os.path import expanduser
home = expanduser("~")
logfile = os.path.join(home,'magpy.log')

logging.basicConfig(filename=logfile,
                        filemode='w',
                        format='%(asctime)s %(levelname)-8s- %(name)-6s %(message)s',
                        level=logging.INFO)

# Define a Handler which writes "setLevel" messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.WARNING)

# Package loggers to identify info/problem source
loggerabs = logging.getLogger('abs')
loggertransfer = logging.getLogger('transf')
loggerdatabase = logging.getLogger('db')
loggerstream = logging.getLogger('stream')
loggerlib = logging.getLogger('lib')
loggerplot = logging.getLogger('plot')

# Special loggers for event notification
stormlogger = logging.getLogger('stream')

# Storing function - http://bytes.com/topic/python/answers/552476-why-cant-you-pickle-instancemethods#edit2155350
# by Steven Bethard
# Used here to pickle baseline functions from header and store it in a cdf key.
# Not really a transparent method but working nicely. Underlying functional parameters to reconstruct the fit
# are stored as well but would require a link to the absolute data.
def _pickle_method(method):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
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

copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)

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
PYMAG_SUPPORTED_FORMATS = [
                'IAGA',         # IAGA 2002 text format
                'WDC',          # World Data Centre format
                'IMF',          # Intermagnet Format
                'IAF',          # Intermagnet archive Format
                'IMAGCDF',      # Intermagnet CDF Format
                'BLV',          # Baseline format Intermagnet
                'IYFV',         # Yearly mean format Intermagnet
                'DKA',          # K value format Intermagnet
                'DIDD',         # Output format from DIDD
                'GSM19',        # Output format from GSM19 magnetometer
                'LEMIHF',       # LEMI text format data
                'LEMIBIN',      # Current LEMI binary data format at WIC
                'LEMIBIN1',     # Deprecated LEMI binary format at WIC
                'OPT',          # Optical hourly data from WIK
                'PMAG1',        # Deprecated ELSEC from WIK
                'PMAG2',        # Current ELSEC from WIK
                'GDASA1',       # ?
                'GDASB1',       # ?
                'RMRCS',        # RCS data output from Richards perl scripts
                'METEO',        # RCS data output in METEO files
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
                        ]

# ----------------------------------------------------------------------------
#  Part 3: Main classes -- DataStream, LineStruct and
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
    - stream.date_offset(self, offset):
    - stream.delta_f(self, **kwargs):
    - stream.differentiate(self, **kwargs):
    - stream.eventlogger(self, key, values, compare=None, stringvalues=None, addcomment=None, debugmode=None):
    - stream.extract(self, key, value, compare=None, debugmode=None):
    - stream.extrapolate(self, start, end):
    - stream.filter(self, **kwargs):
    - stream.fit(self, keys, **kwargs):
    - stream.flag_outlier(self, **kwargs):
    - stream.flag_stream(self, key, flag, comment, startdate, enddate=None):
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
    - stream.remove(self, starttime=starttime, endtime=endtime):
    - stream.remove_flagged(self, **kwargs):
    - stream.resample(self, keys, **kwargs):
    - stream.rotation(self,**kwargs):
    - stream.scale_correction(self, keys, scales, **kwargs):
    - stream.smooth(self, keys, **kwargs):
    - stream.steadyrise(self, key, timewindow, **kwargs):
    - stream.trim(self, starttime=None, endtime=None, newway=False):
    - stream.variometercorrection(self, variopath, thedate, **kwargs):
    - stream.write(self, filepath, **kwargs):


    Application methods:
    ----------------------------

    - stream.aic_calc(key) -- returns stream (with !var2! filled with aic values)
    - stream.baseline() -- calculates baseline correction for input stream (datastream)
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
    - stream.trim() -- returns stream within new time frame
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
            return [len(self)]

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
        co.header = self.header
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

        return DataStream(co.container,co.header,np.asarray(array, dtype=object))


    def __str__(self):
        return str(self.container)

    def __repr__(self):
        return str(self.container)

    def __getitem__(self, index):
        return self.container.__getitem__(index)

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
        if self.ndarray.size == 0:
            self.ndarray = ndarray
        else:
            for idx,elem in enumerate(self.ndarray):
                if len(ndarray[idx]) > 0:
                    if len(self.ndarray[idx]) > 0 and len(self.ndarray[0]) > 0:
                        array[idx] = np.append(self.ndarray[idx], ndarray[idx],1).astype(object)
                    elif len(self.ndarray[0]) > 0: # only time axis present so far but no data within this elem
                        fill = ['-']
                        key = KEYLIST[idx]
                        if key in NUMKEYLIST:
                            fill = [float('nan')]
                        nullvals = np.asarray(fill * len(self.ndarray[0]))
                        #print nullvals
                        array[idx] = np.append(nullvals, ndarray[idx],1).astype(object)
                    else:
                        array[idx] = ndarray[idx].astype(object)
            #self.ndarray = np.asarray((list(self.ndarray)).extend(list(ndarray)))
            self.ndarray = np.asarray(array)

    def union(self,column):
        seen = set()
        seen_add = seen.add
        return [ x for x in column if not (x in seen or seen_add(x))]


    def removeduplicates(self):
        """
        identify duplicate time stamps and remove all data
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



    def findtime(self,time):
        """
        DEFINITION:
            Find a line within the container which contains the selected time step
        RETURNS:
            The index position of the line and the line itself
        """
        st = date2num(self._testtime(time))
        if len(self.ndarray[0]) > 0:
            try:
                return list(self.ndarray[0]).index(st), LineStruct()
            except:
                return 0, []
        for index, line in enumerate(self):
            if line.time == st:
                return index, line
        loggerstream.warning("findtime: Didn't find selected time - returning 0")
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
            t_start = num2date(self[0].time).replace(tzinfo=None)
            t_end = num2date(self[-1].time).replace(tzinfo=None)

        return t_start, t_end

    def _print_key_headers(self):
        print "%10s : %22s : %28s" % ("MAGPY KEY", "VARIABLE", "UNIT")
        for key in FLAGKEYLIST[1:]:
            try:
                header = self.header['col-'+key]
            except:
                header = None
            try:
                unit = self.header['unit-col-'+key]
            except:
                unit = None
            print "%10s : %22s : %28s" % (key, header, unit)


    def _get_key_headers(self,**kwargs):
        """
    DEFINITION:
        get a list of existing numerical keys in stream.

    PARAMETERS:
    kwargs:
        - limit:        (int) limit the lenght of the list
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
                    if len(col) == 1 and col[0] in ['-',float(nan),'']:
                        pass
                    else:
                        keylist.append(key)

        if limit and len(keylist) > limit:
            keylist = keylist[:limit]

        return keylist

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
               newndarray.append(elem)
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
        liste = sorted(self.container, key=lambda tmp: tmp.time)

        if len(self.ndarray[0]) > 0:
            self.ndarray, keylst = self.dropempty()
            #self.ndarray = self.ndarray[:, np.argsort(self.ndarray[0])] # does not work if some rows have a different length)
            ind =  np.argsort(self.ndarray[0])
            for i,el in enumerate(self.ndarray):
                if len(el) == len(ind):
                    self.ndarray[i] = el[ind]
                else:
                    print self.ndarray[i]
                    print "Sorting: key %s has the wrong length - dropping this row" % KEYLIST[i]
                    print "len(t-axis)=%d len(%s)=%d" % (len(self.ndarray[0]), KEYLIST[i], len(self.ndarray[i]))
                    self.ndarray[i] = []

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
            raise ValueError, "Column key not valid"

        lines = [elem for elem in self if eval('elem.'+key) == value]

        return lines[0]

    def _take_columns(self, keys):
        """
        DEFINITION:
            removes columns of the given keys
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
            raise ValueError, "Column key not valid"

        lst = [elem for elem in self if not eval('elem.'+key) == value]

        return DataStream(lst, self.header)


    def _get_column(self, key):
        """
        returns an numpy array of selected columns from Stream
        example:
        columnx = datastream._get_column('x')
        """

        if not key in KEYLIST:
            raise ValueError, "Column key not valid"

        # Speeded up this technique:


        ind = KEYLIST.index(key)

        if len(self.ndarray[0]) > 0:
            col = self.ndarray[ind]
            return col

        # Check for initialization value
        #testval = self[0][ind]
        # if testval == KEYINITDICT[key] or isnan(testval):
        #    return np.asarray([])
        try:
            col = np.asarray([row[ind] for row in self])
            #get the first ten elements and test whether nan is there -- why ??
            try: # in case of string....
                novalfound = True
                for ele in col[:10]:
                    if not isnan(ele):
                        novalfound = False
                if novalfound:
                    return np.asarray([])
            except:
                return col
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
            raise ValueError, "Column key not valid"
        if len(self.ndarray[0]) > 0:
            ind = KEYLIST.index(key)
            self.ndarray[ind] = np.asarray(column)
        else:
            if not len(column) == len(self):
                raise ValueError, "Column length does not fit Datastream"
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
            loggerstream.error("_move_column: Column key %s not valid!" % key)
        if key == 'time':
            loggerstream.error("_move_column: Cannot move time column!")
        if not put2key in KEYLIST:
            loggerstream.error("_move_column: Column key %s (to move %s to) is not valid!" % (put2key,key))
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
                loggerstream.error("_move_column: Error updating headers.")
            loggerstream.info("_move_column: Column %s moved to column %s." % (key, put2key))
        except:
            loggerstream.error("_move_column: It's an error.")

        return self


    def _drop_column(self,key):
        """
        remove a column of a Stream
        """
        ind = KEYLIST.index(key)

        if len(self.ndarray[0]) > 0:
            self.ndarray[ind] = np.asarray([])
            colkey = "col-%s" % key
            colunitkey = "unit-col-%s" % key
            try:
                self.header.pop(colkey, None)
                self.header.pop(colunitkey, None)
            except:
                print "_drop_column: Error while dropping header info"
        else:
           print "No data available  or LineStruct type (not supported)"

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
            raise ValueError, "Column key not valid"
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
            loggerstream.warning("_reduce_stream: Stream size (%s) is already below pointlimit (%s)." % (size,pointlimit))
            return self

        loggerstream.info("_reduce_stream: Stream size reduced from %s to %s points." % (size,len(lst)))

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
                loggerstream.debug('_AIC: could not evaluate AIC at index position %i' % (k))
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
            raise ValueError, "Column key not valid"
        key_ind = KEYLIST.index(key)
        t_ind = KEYLIST.index('time')

        if len(self.ndarray[0]) > 0:
            result = np.max(self.ndarray[key_ind])
            ind = np.argmax(self.ndarray[key_ind])
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
            raise ValueError, "Column key not valid"
        key_ind = KEYLIST.index(key)
        t_ind = KEYLIST.index('time')

        if len(self.ndarray[0]) > 0:
            result = np.min(self.ndarray[key_ind])
            ind = np.argmin(self.ndarray[key_ind])
            tresult = self.ndarray[t_ind][ind]
        else:
            elem = min(self, key=lambda tmp: eval('tmp.'+key))
            result = eval('elem.'+key)
            tresult = elem.time

        if returntime:
            return result, tresult
        else:
            return result


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


    def _tau(self, period):
        """
        low pass filter with -3db point at period in sec (e.g. 120 sec)
        1. convert period from seconds to days as used in daytime
        2. return tau (in unit "day")
        """
        per = period/(3600*24)
        return 0.83255461*per/(2*np.pi)


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
        if len(self.ndarray[0]) > 0:
            if coordinate == 'xyz2hdz':
                self = self.xyz2hdz()
            elif coordinate == 'xyz2idf':
                self = self.xyz2idf()
            elif coordinate == 'hdz2xyz':
                self = self.hdz2xyz()
            elif coordinate == 'idf2xyz':
                self = self.idf2xyz()
            else:
                print "_convertstream: unkown coordinate transform"
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
            indicieslst = [i for i,el in enumerate(self.ndarray[ind].astype(float)) if np.isnan(el) or np.isinf(el)]
            for index,key in enumerate(NUMKEYLIST):
                if len(self.ndarray[index])>0:
                    array[index] = np.delete(self.ndarray[index], indicieslst)
            newst = [LineStruct()]
        else:
            newst = [elem for elem in self if not isnan(eval('elem.'+key)) and not isinf(eval('elem.'+key))]

        return DataStream(newst,self.header,np.asarray(array))


    def _select_timerange(self,starttime=None, endtime=None, maxidx=-1):
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
        startindicies = []
        endindicies = []
        if starttime:
            starttime = self._testtime(starttime)
            if self.ndarray[0].size > 0:   # time column present
                if maxidx > 0:
                    idx = (np.abs(self.ndarray[0][:maxidx]-date2num(starttime))).argmin()
                else:
                    idx = (np.abs(self.ndarray[0]-date2num(starttime))).argmin()
                startindicies = range(0,idx)
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
                    if not self.ndarray[0][idx] <= date2num(endtime): # Make sure that last value is smaller than endtime
                        #idx -= 1
                        idx -= 1
                except:
                    pass
                endindicies = range(idx,len(self.ndarray[0]))

        indicies = startindicies + endindicies

        #t2 = datetime.utcnow()
        #print "_select_timerange - getting t range needed:", t2-t1

        if len(startindicies) > 0:
            st = startindicies[-1]+1
        else:
            st = 0
        if len(endindicies) > 0:
            ed = endindicies[0]
        else:
            ed = len(self.ndarray[0])

        for i in range(len(self.ndarray)):
            ndarray[i] = self.ndarray[i][st:ed]
            #ndarray[i] =  np.delete(self.ndarray[i],indicies) # before : very slowly

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
        from magpy.stream import *
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

        #if not extradays:
        #    extradays = 15
        if not fitfunc:
            fitfunc = 'spline'
        if not fitdegree:
            fitdegree = 5
        if not knotstep:
            knotstep = 0.1
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
            fixstart = True
        if endabs:
            endabs = date2num(self._testtime(endabs))
            fixend = True

        self.header['DataAbsFunc'] = fitfunc
        self.header['DataAbsDegree'] = fitdegree
        self.header['DataAbsKnots'] = knotstep
        self.header['DataAbsDate'] = datetime.strftime(datetime.utcnow(),'%Y-%m-%d %H:%M:%S')

        usestepinbetween = False # for better extrapolation

        loggerstream.info(' --- Start baseline-correction at %s' % str(datetime.now()))

        absolutestream  = absolutedata.copy()

        absolutestream = absolutestream.remove_flagged()

        absndtype = False
        if len(absolutestream.ndarray[0]) > 0:
            absolutestream.ndarray[0] = absolutestream.ndarray[0].astype(float)
            absndtype = True
            if not np.min(absolutestream.ndarray[0]) < endtime:
                loggerstream.warning("Baseline: Last measurement prior to beginning of absolute measurements ")
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
                loggerstream.warning("Baseline: Last measurement prior to beginning of absolute measurements ")
            abst = absolutestream._get_column('time')
            startabs = absolutestream[0].time
            endabs = absolutestream[-1].time

        # 3) check time ranges of stream and absolute values:
        if startabs > starttime:
            #loggerstream.warning('Baseline: First absolute value measured after beginning of stream - duplicating first abs value at beginning of time series')
            #absolutestream.add(absolutestream[0])
            #absolutestream[-1].time = starttime
            #absolutestream.sorting()
            loggerstream.info('Baseline: %d days without absolutes at the beginning of the stream' % int(np.floor(np.min(abst)-starttime)))
        if endabs < endtime:
            loggerstream.info("Baseline: Last absolute measurement before end of stream - extrapolating baseline")
            if num2date(endabs).replace(tzinfo=None) + timedelta(days=extradays) < num2date(endtime).replace(tzinfo=None):
                usestepinbetween = True
                loggerstream.warning("Baseline: Well... thats an adventurous extrapolation, but as you wish...")

        starttime = num2date(starttime).replace(tzinfo=None)
        endtime = num2date(endtime).replace(tzinfo=None)

        # 4) get standard time rang of one year and extradays at start and end
        #           test whether absstream covers this time range including extradays
        # ###########
        #  get boundaries
        # ###########
        #print "Baseline", extradays, num2date(startabs), num2date(endabs)
        #print "Baseline", starttime, endtime
        extrapolate = False
        # upper
        if fixend:
            absolutestream = absolutestream.trim(endtime=endabs)
            # time range long enough
            baseendtime = endabs+extradays
            extrapolate = True
        else:
            baseendtime = date2num(endtime+timedelta(days=1))
            extrapolate = True
        if endabs >= date2num(endtime)+extradays:
            # time range long enough
            baseendtime = date2num(endtime)+extradays
        # lower
        if fixstart:
            absolutestream = absolutestream.trim(starttime=startabs)
            basestarttime = startabs-extradays
            #print "baseline2", num2date(basestarttime)
            extrapolate = True
        else:
            # not long enough
            basestarttime = date2num(starttime)
            extrapolate = True
        if baseendtime - (366.+2*extradays) > startabs:
            # time range long enough
            basestarttime =  baseendtime-(366.+2*extradays)

        baseendtime = num2date(baseendtime).replace(tzinfo=None)
        basestarttime = num2date(basestarttime).replace(tzinfo=None)

        bas = absolutestream.trim(starttime=basestarttime,endtime=baseendtime)
        #print "baseline3", bas._find_t_limits(), basestarttime

        if extrapolate:
            bas = bas.extrapolate(basestarttime,baseendtime)

        #print "baseline4", bas._find_t_limits()

        """
        # 4) get year of last input - remove one day to correctly interprete
        #           1.1.2000 to 1.1.2001 full year analyses
        yearenddate = datetime.strftime(endtime-timedelta(days=1),'%Y') # year of last variometer data

        print "BASE here", max(abst), date2num(endtime)

        if absndtype:
            yearar = np.asarray([elem.year for elem in num2date(absolutestream.ndarray[0])])
            lst = absolutestream.ndarray[0][yearar > endtime.year]
            #lst = absolutestream.ndarray[0][num2date(absolutestream.ndarray[0]) > endtime.year]
        else:
            lst = [elem for elem in absolutestream if datetime.strftime(num2date(elem.time),'%Y') > yearenddate]
        if len(lst) > 0:
            baseendtime = datetime.strptime(yearenddate+'-12-31', '%Y-%m-%d')
        else:
            # if not extending to next year check whether absolutes older then last stream value are present
            # in this case use last absolute value
            # if not use use the last input
            maxabstime = num2date(np.max(abst)).replace(tzinfo=None)
            if maxabstime > endtime:
                baseendtime = maxabstime
            else:
                baseendtime = endtime

        # now add the extradays to endtime
        baseendtime = baseendtime + timedelta(days=extradays)

        # endtime for baseline calc determined
        if (np.max(abst) - np.min(abst)) < 365+2*extradays:
            loggerstream.info('Baseline: Coverage of absolute values does not reach one year')
            basestarttime = num2date(startabs).replace(tzinfo=None) - timedelta(days=extradays)
        else:
            basestarttime = baseendtime-timedelta(days=(365+2*extradays))

        msg = 'Baseline: absolute data taken from %s to %s' % (basestarttime,baseendtime)
        loggerstream.debug(msg)

        bas = absolutestream.trim(starttime=basestarttime,endtime=baseendtime)

        if usestepinbetween:
            bas = bas.extrapolate(basestarttime,endtime)
        bas = bas.extrapolate(basestarttime,baseendtime)
        """
        # move the basevalues to x,y,z - so that method func_add will work directly

        #print bas.ndarray

        #keys = ['dx','dy','dz']
        try:
            print ("Fitting Baseline between: {a} and {b}".format(a=str(num2date(np.min(bas.ndarray[0]))),b=str(num2date(np.max(bas.ndarray[0])))))
            #print bas.length(), keys
            #for elem in bas.ndarray:
            #    print elem
            func = bas.fit(keys,fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep)
        except:
            print ("Baseline: Error when determining fit - Enough data point to satisfy fit complexity?")
            raise
        ix = KEYLIST.index(keys[0])
        iy = KEYLIST.index(keys[1])
        iz = KEYLIST.index(keys[2])
        # get the function in some readable equation
        #self.header['DataAbsDataT'] = bas.ndarray[0],bas.ndarray[ix],bas.ndarray[iy],bas.ndarray[iz]]

        if plotbaseline:
            #check whether plotbaseline is valid path or bool
            try:
                try:
                    import mpplot as mp
                except:
                    import magpy.mpplot as mp
                if plotfilename:
                    mp.plot(bas,variables=['dx','dy','dz'],padding = [5,0.1,5], symbollist = ['o','o','o'],function=func,plottitle='Absolute data',outfile=plotfilename)
                else:
                    mp.plot(bas,variables=['dx','dy','dz'],padding = [5,0.1,5], symbollist = ['o','o','o'],function=func,plottitle='Absolute data')
            except:
                print "using the internal plotting routine requires mpplot to be imported as mp"

        self.header['DataAbsMinTime'] = func[1] #num2date(func[1]).replace(tzinfo=None)
        self.header['DataAbsMaxTime'] = func[2] #num2date(func[2]).replace(tzinfo=None)
        self.header['DataAbsFunctionObject'] = func

        #else:
        #    self = self.func_add(func)

        loggerstream.info(' --- Finished baseline-correction at %s' % str(datetime.now()))

        for key in self.header:
            if key.startswith('DataAbs'):
                print key, self.header[key]

        return func


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
                print elem, addelem
                elem.extend(addelem)
                resultlist.append(elem)
        else:
            resultlist = vallist

        print "baselineAdvanced: inds", resultlist

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
        try:
            func = self.header['DataAbsFunctionObject']
        except:
            print "BC: No data for correction available - header needs to contain DataAbsFunctionObject"
            return self

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
            print "BC: could not interpret BaseLineFunctionObject - returning"
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

        self = self.func2stream(func,mode='addbaseline',keys=['x','y','z'])
        self.header['col-x'] = 'H'
        self.header['unit-col-x'] = 'nT'
        self.header['col-y'] = 'D'
        self.header['unit-col-y'] = 'deg'
        self.header['DataComponents'] = 'HDZ'

        return self


    def bindetector(self,key,text=None,**kwargs):
        """
        DEFINITION:
            Function to detect changes between 0 and 1
        PARAMETERS:
            text:          (string) text to be added to comments/stdout,
                                    will be extended by on/off
            key:           (key) key to investigate
        Kwargs:
            add:           (BOOL) if true add to comments
            markallon:     (BOOL) add comment to all ons
            markalloff:    (BOOL) add comment to all offs
            onvalue:       (float) critical value to determin on stage (default = 0.99)
        RETURNS:
            - DataStream object

        EXAMPLE:
            >>>  stream = stream._put_column(res, 't2', columnname='Rain',columnunit='mm in 1h')
        """
        add = kwargs.get('add')
        markallon = kwargs.get('markallon')
        markalloff = kwargs.get('markalloff')
        onvalue = kwargs.get('onvalue')

        if not text:
            text = ''
        if not onvalue:
            onvalue = 0.99

        startstate = eval('self[0].'+key)
        for elem in self:
            state = eval('elem.'+key)
            if state > onvalue:
                state = 1
            if markallon:
                if state == 1:
                    tex = text+' on'
                    if add:
                        elem.comment =  tex
            elif markalloff:
                if state == 0:
                    tex = text+' off'
                    if add:
                        elem.comment =  tex
            elif not state == startstate and (state == 1 or state == 0):
                time = datetime.strftime(num2date(elem.time), "%Y-%m-%d %H:%M:%S")
                if state == 0:
                    tex = text+' off'
                    if add:
                        elem.comment =  tex
                        print time + ': ' + tex
                else:
                    tex = text+' on'
                    if add:
                        elem.comment =  tex
                        print time + ': ' + tex
                startstate = state

        return self

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
                loggerstream.error('calc_f: offset with wrong dimension given - needs to contain a three dim array like [a,b,c] - returning stream without changes')
                return self

        ndtype = False
        try:
            if len(self.ndarray[0]) > 0:
                ndtype = True
            elif len(self) > 1:
                ndtype = False
            else:
                loggerstream.error('calc_f: empty stream - aborting')
                return self
        except:
            loggerstream.error('calc_f: inapropriate data provided - aborting')
            return self

        loggerstream.info('calc_f: --- Calculating f started at %s ' % str(datetime.now()))

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

        loggerstream.info('calc_f: --- Calculating f finished at %s ' % str(datetime.now()))

        return self


    def dailymean(self):
        """
    DEFINITION:
        If an absolutestream is provided, basevalues are taken and averaged
        An outputstream is generated which containes basevalues in columns
        x,y,z and uncertainty values in dx,dy,dz
        if only a single values is available, dx,dy,dz contain the average uncertainties
        of the full data set
        time column contains the average time of the measurement

    PARAMETERS:
    Variables:
    Kwargs:
        - None

    RETURNS:
        - stream:       (DataStream object) with daily means and standard deviation

    EXAMPLE:
        >>> data = absstream.dailymeans()

    APPLICATION:
        """

        pass


    def date_offset(self, offset):
        """
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

        loggerstream.info('date_offset: Corrected time column by %s sec' % str(offset.total_seconds))

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

        loggerstream.info('--- Calculating delta f started at %s ' % str(datetime.now()))

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
            if syst in ['HDZ','hdz','HDZF','hdzf']:
                print "deltaF: found HDZ orientation"
                ary = np.asarray([0]*len(self.ndarray[indy]))
            sumar = list(arx+ary+arz)
            sqr = np.sqrt(np.asarray(sumar))
            self.ndarray[ind] = sqr - (self.ndarray[indf] + offset)
        else:
            for elem in self:
                elem.df = round(np.sqrt(elem.x**2+elem.y**2+elem.z**2),digits) - (elem.f + offset)

        self.header['col-df'] = 'delta f'
        self.header['unit-col-df'] = 'nT'

        loggerstream.info('--- Calculating delta f finished at %s ' % str(datetime.now()))

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

        loggerstream.info('differentiate: Calculating derivative started.')

        keys = kwargs.get('keys')
        put2keys = kwargs.get('put2keys')
        if not keys:
            keys = ['x','y','z','f']
        if not put2keys:
            put2keys = ['dx','dy','dz','df']

        if len(keys) != len(put2keys):
            loggerstream.error('Amount of columns read must be equal to outputcolumns')
            return self

        ndtype = False
        if len(self.ndarray[0]) > 0:
            t = self.ndarray[0].astype(float)
            ndtype = True
        else:
            t = self._get_column('time')

        for i, key in enumerate(keys):
            if ndtype:
                ind = KEYLIST.index(key)
                val = self.ndarray[ind].astype(float)
            else:
                val = self._get_column(key)
            dval = np.gradient(np.asarray(val))
            self._put_column(dval, put2keys[i])
            self.header['col-'+put2keys[i]] = r"d%s vs dt" % (key)

        loggerstream.info('--- derivative obtained at %s ' % str(datetime.now()))
        return self


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
        loggerstream.info("DWT_calc: Starting Discrete Wavelet Transform of key %s." % key)

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
            array[var1_ind].append(fin_fns[1])
            array[var2_ind].append(fin_fns[2])
            array[var3_ind].append(fin_fns[3])

            #DWT_stream.add(row)
            i += window

        loggerstream.info("DWT_calc: Finished DWT.")

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
            date = datetime.strftime(num2date(self[0].time),'%Y-%m-%d')
            loggerstream.info('DWT_calc: Plotting data...')
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
            loggerstream.warning('Eventlogger: wrong value for compare: needs to be among <,>,<=,>=,==,!=')
            return self
        if not stringvalues:
            stringvalues = ['Minor storm onset','Moderate storm onset','Major storm onset']
        else:
            assert type(stringvalues) == list
        if not len(stringvalues) == len(values):
            loggerstream.warning('Eventlogger: Provided comments do not match amount of values')
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
        if not compare in [">=", "<=",">", "<", "==", "!="]:
            loggerstream.info('--- Extract: Please provide proper compare parameter ">=", "<=",">", "<", "==" or "!=" ')
            return self

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True

        ind = KEYLIST.index(key)

        stream = self.copy()

        if not self._is_number(value):
            if value.startswith('(') and value.endswith(')') and compare == '==':
                loggerstream.info("extract: Selected special functional type -equality defined by difference less then 10 exp-6")
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
                print "Found String", ndtype
                too = '"' + str(value) + '"'
                if ndtype:
                    print stream.ndarray[ind]
                    searchclause = 'stream.ndarray[ind] '+ compare + ' ' + too
                    print searchclause, ind, key
                    indexar = eval('np.where('+searchclause+')[0]')
                    print indexar, len(indexar)
        else:
            too = str(value)
            if ndtype:
                searchclause = 'stream.ndarray[ind].astype(float) '+ compare + ' ' + too
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

        ltime = date2num(end + timedelta(days=1))
        ftime = date2num(start - timedelta(days=1))

        ndtype = False
        if len(self.ndarray[0]) > 0:
            array = [[] for key in KEYLIST]
            ndtype = True
            firsttime = np.min(self.ndarray[0])
            lasttime = np.max(self.ndarray[0])
            # Find the last element with baseline values - assuming a sorted array
            inddx = KEYLIST.index('dx')
            lastind=len(self.ndarray[0])-1

            #print self.ndarray[inddx][lastind]
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
            array = [el[indar].astype(object) for el in array if len(el)>0]
        else:
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

        PARAMETERS:
        Kwargs:
            - keys:             (list) List of keys to smooth
            - filter_type:      (string) name of the window. One of
                                'flat','barthann','bartlett','blackman','blackmanharris','bohman',
                                'boxcar','cosine','flattop','hamming','hann','nuttall',
                                'parzen','triang','gaussian','wiener','spline','butterworth'
                                See http://docs.scipy.org/doc/scipy/reference/signal.html
            - filter_width:     (timedelta) window width of the filter
            - noresample:       (bool) if True the data set is resampled at filter_width positions
            - autofill:         (list) of keys: provide a keylist for which nan values are linearly interpolated before filtering - use with care, might be useful if you have low resolution parameters asociated with main values like (humidity etc)
            - resampleoffset:   (timedelta) if provided the offset will be added to resamples starttime
            - resamplemode:     (string) if 'fast' then fast resampling is used
            - gaussian_factor:  (float) factor to multiply filterwidth.
                                1.86506: is the ideal numerical value for IAGA recommended 45 sec filter
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
        filter_offset = kwargs.get('filter_offset')
        noresample = kwargs.get('noresample')
        resamplemode = kwargs.get('resamplemode')
        resamplestart = kwargs.get('resamplestart')
        resampleoffset = kwargs.get('resampleoffset')
        gaussian_factor = kwargs.get('gaussian_factor')
        testplot = kwargs.get('testplot')
        autofill = kwargs.get('autofill')
        dontfillgaps = kwargs.get('dontfillgaps')
        fillgaps = kwargs.get('fillgaps')
        debugmode = kwargs.get('debugmode')

        if not keys:
            keys = self._get_key_headers(numerical=True)
        if not filter_width:
            filter_width = timedelta(minutes=1)
        if not noresample:
            resample = True
        else:
            resample = False
        if not autofill:
            autofill = []
        else:
            if not isinstance(autofill, (list, tuple)):
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
        if not gaussian_factor:
            gaussian_factor = 1.86506  # optimzed for a 45 sec window with less then 1 % outside the window
                                       # 1.86506: is the ideal numeric values (IAGA recommended for 45 sec fit)
        if resamplestart:
            print "##############  Warning ##############"
            print "option RESAMPLESTART is not used any more. Switch to resampleoffset for modifying time steps"

        ndtype = False

        # ########################
        # Basic validity checks and window size definitions
        # ########################
        if not filter_type in filterlist:
            loggerstream.error("smooth: Window is none of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman', etc")
            loggerstream.debug("smooth: You entered non-existing filter type -  %s  - " % filter_type)
            return self

        #print self.length()[0]
        if not self.length()[0] > 1:
            loggerstream.error("Filter: stream needs to contain data - returning.")
            return self

        if debugmode:
            print "Starting length:", self.length()

        #if not dontfillgaps:   ### changed--- now using dont fill gaps as default
        if fillgaps:
            self = self.get_gaps()
            if debugmode:
                print "length after getting gaps:", len(self)

        window_period = filter_width.total_seconds()
        si = timedelta(seconds=self.get_sampling_period()*24*3600)
        sampling_period = si.days*24*3600 + si.seconds + np.round(si.microseconds/1000000.0,2)

        if debugmode:
            print "Timedelta and sampling period:", si, sampling_period

        # window_len defines the window size in data points assuming the major sampling period to be valid for the dataset
        if filter_type == 'gaussian':
            # For a gaussian fit
            window_len = np.round(gaussian_factor*(window_period/sampling_period))
            # Window length needs to be odd number:
            if window_len % 2 == 0:
                window_len = window_len +1
            std = 0.83255461*window_len/(2*np.pi)
            trangetmp = self._det_trange(gaussian_factor*window_period)*24*3600
            if trangetmp < 1:
                trange = np.round(trangetmp,3)
            else:
                trange = timedelta(seconds=(self._det_trange(gaussian_factor*window_period)*24*3600)).seconds
            if debugmode:
                print "Window character: ", window_len, std, trange
        else:
            window_len = np.round(window_period/sampling_period)
            if window_len % 2:
                window_len = window_len+1
            trange = window_period/2

        if sampling_period >= window_period:
            loggerstream.warning("Filter: Sampling period is equal or larger then projected filter window - returning.")
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
            print "Length time column:", len(t)

        #nanarray = [[] for key in KEYLIST]

        for key in keys:
            #print "Start filtering for", key
            if not key in KEYLIST:
                loggerstream.error("Column key %s not valid." % key)
            keyindex = KEYLIST.index(key)
            if len(self.ndarray[keyindex])>0:
                v = self.ndarray[keyindex]
            else:
                v = self._get_column(key)

            # Get indicies of NaN's
            #if key in NUMKEYLIST:
            #    nanarray[keyindex] = np.logical_not(np.isnan(v.astype(float)))

            if key in autofill:
                loggerstream.warning("Filter: key %s has been selected for linear interpolation before filtering." % key)
                loggerstream.warning("Filter: I guess you know what you are doing...")
                nans, x= nan_helper(v)
                v[nans]= interp(x(nans), x(~nans), v[~nans])

            # Make sure that we are dealing with numbers
            v = np.array(map(float, v))
            if v.ndim != 1:
                loggerstream.error("Filter: Only accepts 1 dimensional arrays.")
            if window_len<3:
                loggerstream.error("Filter: Window lenght defined by filter_width needs to cover at least three data points")

            if debugmode:
                print "Treating k:", key, v.size

            if v.size >= window_len:
                s=np.r_[v[window_len-1:0:-1],v,v[-1:-window_len:-1]]

                if filter_type == 'gaussian':
                    w = signal.gaussian(window_len, std=std)
                    y=np.convolve(w/w.sum(),s,mode='valid')
                    res = y[(int(window_len/2)):(len(v)+int(window_len/2))]
                elif filter_type == 'wiener':
                    res = signal.wiener(v, window_len, noise=0.5)
                elif filter_type == 'butterworth':
                    dt = 800/float(len(v))
                    nyf = 0.5/dt
                    b, a = signal.butter(4, 1.5/nyf)
                    res = signal.filtfilt(b, a, v)
                elif filter_type == 'spline':
                    res = UnivariateSpline(t, v, s=240)
                elif filter_type == 'flat':
                    w=np.ones(window_len,'d')
                    y=np.convolve(w/w.sum(),s,mode='valid')
                    res = y[(int(window_len/2)):(len(v)+int(window_len/2))]
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

        #print "End length:", self.length()
        #print self.ndarray

        #print nanarray

        if resample:
            if debugmode:
                print "Resampling: ", keys
            self = self.resample(keys,period=window_period,fast=resamplefast,offset=resampleoffset)
            self.header['DataSamplingRate'] = str(window_period) + ' sec'

        # ########################
        # Update header information
        # ########################
        self.header['DataSamplingFilter'] = filter_type + ' - ' + str(trange) + ' sec'

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

        """
        # Defaults:
        filter_type = kwargs.get('filter_type')
        filter_width = kwargs.get('filter_width')
        filter_offset = kwargs.get('filter_offset')
        gauss_win = kwargs.get('gauss_win')
        fmi_initial_data = kwargs.get('fmi_initial_data')
        m_fmi = kwargs.get('m_fmi')
        if not filter_type:
            filter_type = 'gauss'
        if not filter_width:
            filter_width = timedelta(minutes=1)
        if not filter_offset:
            filter_offset = 0
        if not gauss_win:
            gauss_win = 1.86506
        if not fmi_initial_data:
            fmi_initial_data=[]
        if not m_fmi:
            m_fmi = 0

        gf_fac = gauss_win
        resdataout = []

        # check whether data is valid
        if len(self) < 2:
            loggerstream.warning('filter: No valid stream provided')
            return self

        # check whether requested filter_width >= sampling interval within 1 millisecond accuracy
        si = timedelta(seconds=self.get_sampling_period()*24*3600)
        if filter_width - si <= timedelta(microseconds=1000):
            loggerstream.warning('filter: Requested filter_width does not exceed sampling interval - aborting filtering')
            return self

        loggerstream.info('filter: Start filtering.')

        starray = np.asarray(self)
        firstday = 0
        # Calculating absolute increment
        incr = date2num(datetime.strptime("2010-11-22","%Y-%m-%d")+filter_width)-date2num(datetime.strptime("2010-11-22","%Y-%m-%d"))
        if filter_offset == 0:
            offs = 0
            filter_offset = timedelta(seconds=filter_offset)
        else:
            offs = date2num(datetime.strptime("2010-11-22","%Y-%m-%d")+filter_offset)-date2num(datetime.strptime("2010-11-22","%Y-%m-%d"))

        currtime = num2date(np.floor(starray[0].time)).replace(tzinfo=None) + filter_offset

        # 2.) Define the time ranges in dependency of resolution - use non-flagged data here
        # determine time diff between successive steps for linear means (e.g. 00-59 and not 00 to 60)
        # and get the trange for filtering
        # period (see ta(period) uses 2 times the increment
        per = gf_fac*(filter_width.seconds)
        trange = self._det_trange(per)
        if filter_type == "gauss":
            tau = self._tau(per)
            trange = timedelta(seconds=(self._det_trange(per)*24*3600))
            tdiff = timedelta(seconds=0)
        if filter_type == "linear":
            trange = filter_width / 2
            tdiff = timedelta(seconds=self.get_sampling_period()*24*3600)
        if filter_type == "fmi":
            trange = filter_width / 2
            tdiff = timedelta(seconds=self.get_sampling_period()*24*3600)
            trangestruct = []
            for elem in fmi_initial_data:
                row = []
                row.append(elem.time)
                n = np.power(fmi_initial_data._get_k_float(elem.dx),3.3)
                row.append(n)
                trangestruct.append(row)
            mint = 99999
            for i in range (len(trangestruct)):
                if np.abs(trangestruct[i][0] - date2num(currtime)) < mint:
                    mint = np.abs(trangestruct[i][0] - date2num(currtime))
                    ntmp = trangestruct[i][1]/3600
            trange = trange + timedelta(seconds=(m_fmi+ntmp)*24*3600)

        # 3.) Start the calculation
        nr_lines=len(starray)
        resdata = DataStream()
        #uplim = nr_lines
        lowlim = 0
        i = 0
        # open while loop with currentdata
        while ((currtime + trange - tdiff) <= num2date(starray[-1].time).replace(tzinfo=None)+tdiff):
            # a) select lower bound
            # eventually add a process counter here
            abscurrtime = date2num(currtime)
            tmt = date2num(currtime-trange)
            tpmt = date2num(currtime+trange-trange/30)
            for i in range(lowlim,nr_lines): # might produce an inaccuracy if new lowlim exceeds previous lowlim in fmi (is accounted for by fmi range recalc at the end) (and saves 20 secs per day analysis time)
                if starray[i].time >= tmt:
                    lowlim = i
                    break
            # b) select upper bound
            for i in range(lowlim,nr_lines):
                if starray[i].time >= tpmt:  #changed upper trange because of hour probs (last val missing)
                    uplim = i-1
                    break
            # c) do calc if data available
            #print "Bounds: %f - %f" % (lowlim,uplim)
            #print currtime
            resrow = LineStruct()
            resrow.time = abscurrtime
            if uplim > lowlim:
                for el in KEYLIST[:16]:
                    exec('col'+el+'=[]')
                if filter_type == "gauss":
                    normvec = []
                    # -- determine coefficients for gaussian weighting (needs time - is identical for evenly spaced data but only for that)
                    for k in range(lowlim,uplim):
                        normvec.append(self._gf(starray[k].time-abscurrtime,tau))
                    normcoeff = np.sum(normvec)
                    for k in range(lowlim,uplim):
                        nor = normvec[k-lowlim]/normcoeff
                        for el in KEYLIST[:16]:
                            # nan treatment different to linear case because normvec is already calculated and nan could not be just left out as they are already included in the weighting scheme
                            # might cause problems in case of inf and leading nan like in aic columns
                            #if not isnan(eval('starray[k].'+el))  and not isinf(eval('starray[k].'+el)):
                            exec('col'+el+'.append(starray[k].'+el+'*nor)')
                    # mask NaNs of the columns
                    exec('col'+el+' = maskNAN(col'+el+')')
                    resrow.time = abscurrtime
                    for el in KEYLIST[1:16]:
                        exec('resrow.'+el+' = np.sum(col'+el+')')
                elif filter_type == "linear" or filter_type == "fmi":
                    for k in range(lowlim,uplim):
                        for el in KEYLIST[:16]:
                            if not isnan(eval('starray[k].'+el)) and not isinf(eval('starray[k].'+el)):
                                exec('col'+el+'.append(starray[k].'+el+')')
                    resrow.time = abscurrtime
                    for el in KEYLIST[1:16]:
                        exec('resrow.'+el+' = np.mean(col'+el+')')
                    # add maxmin diffs: important for fmi
                    if starray[k].typ != 'fonly':
                        if len(colx) >0:
                            resrow.dx = np.max(colx)-np.min(colx)
                        if len(coly) >0:
                            resrow.dy = np.max(coly)-np.min(coly)
                        if len(colz) >0:
                            resrow.dz = np.max(colz)-np.min(colz)
                        if len(colf) >0:
                            resrow.df = np.max(colf)-np.min(colf)
                else:
                    loggerstream.warning("filter: Filter not recognized - aborting filtering.")
                resrow.typ = starray[0].typ
            else: # in case of removed flagged sequences - add time and leave "NaN" value in file
                resrow.time = abscurrtime

            resdata.add(resrow)

            # e) increase counter
            currtime += filter_width
            if filter_type == "fmi":
                trangeprev = trange
                mint = 99999
                for i in range (len(trangestruct)):
                    if np.abs(trangestruct[i][0] - abscurrtime) < mint:
                        mint = np.abs(trangestruct[i][0] - abscurrtime)
                        ntmp = trangestruct[i][1]/3600
                trange = filter_width/2 + timedelta(seconds=((m_fmi+ntmp)*24*3600))
                # if trange larger then 2 times the prev trange then set lowlim value to 0 (important for a)
                if trange > trangeprev*2:
                    lowlim = 0

        # Add filtering information to header:
        self.header['DataSamplingRate'] = str(filter_width.total_seconds()) + ' sec'
        self.header['DataSamplingFilter'] = filter_type + ' - ' + str(trange.total_seconds()) + ' sec'
        #self.header['DataInterval'] = str(filter_width.seconds)+' sec'

        loggerstream.info('filter: Finished filtering.')

        return DataStream(resdata,self.header)
        """

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
        if not fitfunc:
            fitfunc = 'spline'
        if not fitdegree:
            fitdegree = 5
        if not knotstep:
            knotstep = 0.01

        if knotstep >= 0.5:
            raise ValueError, "Knotstep needs to be smaller than 0.5"

        functionkeylist = {}

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype=True

        for key in keys:
            tmpst = self._drop_nans(key)
            if ndtype:
                t = tmpst.ndarray[0]
            else:
                t = tmpst._get_column('time')
            if len(t) < 1:
                break

            nt,sv,ev = self._normalize(t)

            #newlist = []
            #for kkk in nt:
            #    if kkk not in newlist:
            #        newlist.append(kkk)
            #    else:
            #        newlist.append(kkk+0.00001)
            #nt = newlist
            #nt = np.sort(np.asarray(nt))
            #print "NT", nt
            sp = self.get_sampling_period()
            if sp == 0:  ## if no dominant sampling period can be identified then use minutes
                sp = 0.0177083333256
            if not key in KEYLIST[1:16]:
                raise ValueError, "Column key not valid"
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
            #print sp
            sp = sp/(ev-sv) # should be the best?
            #print sp
            #sp = (ev-sv)/len(val) # does not work
            #print sp
            x = arange(np.min(nt),np.max(nt),sp)
            #print len(x)
            if len(val)>1 and fitfunc == 'spline':
                try:
                    #print val, sp, knotstep
                    knots = np.array(arange(np.min(nt)+knotstep,np.max(nt)-knotstep,knotstep))
                    #print knots, len(knots), len(val)
                    if len(knots) > len(val):
                        knotstep = knotstep*4
                        knots = np.array(arange(np.min(nt)+knotstep,np.max(nt)-knotstep,knotstep))
                        loggerstream.warning('Too many knots in spline for available data. Please check amount of fitted data in time range. Trying to reduce resolution ...')
                    #print nt, len(knots), len(val)
                    ti = interpolate.splrep(nt, val, k=3, s=0, t=knots)
                except:
                    loggerstream.error('Value error in fit function - likely reason: no valid numbers or too few numbers for fit')
                    raise ValueError, "Value error in fit function - not enough data or invalid numbers"
                    return
                #print nt, val, len(knots), knots
                #ti = interpolate.interp1d(nt, val, kind='cubic')
                #print "X", x, np.min(nt),np.max(nt),sp
                #print "TI", ti
                f_fit = interpolate.splev(x,ti)
            elif len(val)>1 and fitfunc == 'poly':
                loggerstream.debug('Selected polynomial fit - amount of data: %d, time steps: %d, degree of fit: %d' % (len(nt), len(val), fitdegree))
                ti = polyfit(nt, val, fitdegree)
                f_fit = polyval(ti,x)
            elif len(val)>1 and fitfunc == 'harmonic':
                loggerstream.debug('Selected harmonic fit - using inverse fourier transform')
                f_fit = self.harmfit(nt, val, fitdegree)
                # Don't use resampled list for harmonic time series
                x = nt
            elif len(val)<=1:
                loggerstream.warning('Fit: No valid data')
                return
            else:
                loggerstream.warning('Fit: function not valid')
                return
            exec('f'+key+' = interpolate.interp1d(x, f_fit, bounds_error=False)')
            exec('functionkeylist["f'+key+'"] = f'+key)

        func = [functionkeylist, sv, ev]

        return func


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

        print "Adding flags .... "
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
        - keys:         (list) List of keys to evaluate. Default = all numerical
        - threshold:    (float) Determines threshold for outliers.
                        1.5 = standard
                        5 = keeps storm onsets in
                        4 = Default as comprimise.
        - timerange:    (timedelta Object) Time range. Default = timedelta(hours=1)
        - stdout:        prints removed values to stdout
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

        sr = self.samplingrate()

        if not timerange:
            sr = self.samplingrate()
            timerange = timedelta(seconds=sr*600)
        if not keys:
            keys = self._get_key_headers(numerical=True)
        if not threshold:
            threshold = 5.0

        # Position of flag in flagstring
        # f (intensity): pos 0
        # x,y,z (vector): pos 1
        # other (vector): pos 2

        if not len(self.ndarray[0]) > 0:
            loggerstream.info('flag_outlier: No ndarray - starting remove_outlier.')
            self = self.remove_outlier(keys=keys,threshold=threshold,timerange=timerange,stdout=stdout,markall=markall)
            return self

        loggerstream.info('flag_outlier: Starting outlier removal.')

        flagidx = KEYLIST.index('flag')
        commentidx = KEYLIST.index('comment')
        if not len(self.ndarray[flagidx]) > 0:
            self.ndarray[flagidx] = [''] * len(self.ndarray[0])
        if not len(self.ndarray[commentidx]) > 0:
            self.ndarray[commentidx] = [''] * len(self.ndarray[0])

        # get a poslist of all keys - used for markall
        flagposls = [FLAGKEYLIST.index(key) for key in keys]
        # Start here with for key in keys:
        for key in keys:
            flagpos = FLAGKEYLIST.index(key)
            if not len(self.ndarray[flagpos]) > 0:
                print "Flag_outlier: No data for key %s - skipping" % key
                break
            #print key, flagpos

            st = 0
            et = len(self.ndarray[0])
            incrt = int(timerange.total_seconds()/sr)
            if incrt == 0:
                print "Flag_outlier: check timerange ... seems to be smaller as sampling rate"
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

                try:
                    q1 = stats.scoreatpercentile(selcol,16)
                    q3 = stats.scoreatpercentile(selcol,84)
                    iqd = q3-q1
                    md = np.median(selcol)
                    whisker = threshold*iqd
                    #print md, iqd, whisker
                except:
                    try:
                        md = np.median(selcol)
                        whisker = md*0.005
                    except:
                        loggerstream.warning("remove_outlier: Eliminate outliers produced a problem: please check.")
                        pass

                #print md, whisker, np.asarray(selcol)
                for elem in range(idxst,idxat):
                    #print flagpos, elem
                    if not md-whisker < self.ndarray[flagpos][elem] < md+whisker and not np.isnan(self.ndarray[flagpos][elem]):
                        #print "Found:", key, self.ndarray[flagpos][elem]
                        try:
                            if not self.ndarray[flagidx][elem] == '':
                                #print self.ndarray[flagidx][elem]
                                newflagls = list(self.ndarray[flagidx][elem])
                                #print newflagls
                                if not int(newflagls[flagpos]) > 1:
                                    newflagls[flagpos] = '1'
                                if markall:
                                    for p in flagposls:
                                        if not newflagls[p] > 1:
                                            newflagls[p] = '1'
                                newflag = ''.join(newflagls)
                                #print newflag
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
                        self.ndarray[commentidx][elem] = "aof - threshold: %s, window: %s sec" % (str(threshold), str(timerange.total_seconds()))
                        infoline = "remove_outlier: at %s - removed %s (= %f)" % (str(self.ndarray[0][elem]), key, self.ndarray[flagpos][elem])
                        loggerstream.info(infoline)
                        if stdout:
                            print infoline
                    else:
                        try:
                            if not self.ndarray[flagidx][elem] == '':
                                pass
                            else:
                                x=1/0 # Well not elegant but working
                        except:
                            self.ndarray[flagidx][elem] = ''
                            self.ndarray[commentidx][elem] = ''

        self.ndarray[flagidx] = np.asarray(self.ndarray[flagidx])
        self.ndarray[commentidx] = np.asarray(self.ndarray[commentidx])

        loggerstream.info('remove_outlier: Outlier removal finished.')

        return self

    def flag(self, flaglist):
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

        # get time range of stream:
        st,et = self._find_t_limits()
        st = date2num(st)
        et = date2num(et)

        if len(flaglist) > 0:
            #print "flag: going through flaglist", st,et
            for i in range(len(flaglist)):
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
                        self = self.flag_stream(key,int(flaglist[i][3]),flaglist[i][4],flaglist[i][0],flaglist[i][1])

        return self



    def flag_stream(self, key, flag, comment, startdate, enddate=None):
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

    RETURNS:
        - DataStream:   Input stream with flags and comments.

    EXAMPLE:
        >>> data.flag_stream('x',0,'Lawnmower',flag1,flag1_end)

    APPLICATION:
        """


        #print "starting flag_stream method"
        if not key in KEYLIST:
            loggerstream.error("flag_stream: %s is not a valid key." % key)
        if not flag in [0,1,2,3,4]:
            loggerstream.error("flag_stream: %s is not a valid flag." % flag)

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True
        elif not len(self) > 0:
            return DataStream()

        startdate = self._testtime(startdate)

        if not enddate:
            start = date2num(startdate)
            check_startdate, val = self.findtime(start)
            if check_startdate == 0:
                loggerstream.info("flag_stream: No data at given date for flag. Finding nearest data point.")
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
        else:
            enddate = self._testtime(enddate)

        pos = FLAGKEYLIST.index(key)
        #poslst = [i for i,el in enumerate(FLAGKEYLIST) if el == key]
        #pos = poslst[0]

        # Get start and end indicies:
        start = date2num(startdate)
        st, ls = self.findtime(startdate)
        end = date2num(enddate)
        ed, le = self.findtime(enddate)
        if ed == len(self.ndarray[0]):
            ed = ed-1
        # Create a defaultflag
        defaultflag = ['-' for el in FLAGKEYLIST]
        #print "flag", st, ed

        if ndtype:
            flagind = KEYLIST.index('flag')
            commentind = KEYLIST.index('comment')
            # Check whether flag and comment are exisiting - if not create empty
            if not len(self.ndarray[flagind]) > 0:
                self.ndarray[flagind] = [''] * len(self.ndarray[0])
                self.ndarray[flagind] = np.asarray(self.ndarray[flagind]).astype(object)
            if not len(self.ndarray[commentind]) > 0:
                self.ndarray[commentind] = [''] * len(self.ndarray[0])
                self.ndarray[commentind] = np.asarray(self.ndarray[commentind]).astype(object)
            # Now either modify existing or add new flag
            if not st==0 and not ed==0:
                for i in range(st,ed+1):
                    if self.ndarray[flagind][i] == '' or self.ndarray[flagind][i] == '-':
                        flagls = defaultflag
                    else:
                        flagls = list(self.ndarray[flagind][i])
                    # if existing flaglistlength is shorter, because new columns where added later to ndarray
                    if len(flagls) < pos:
                        flagls.extend(['-' for j in range(pos+1-flagls)])
                    flagls[pos] = str(flag)
                    #print "flag", ''.join(flagls), comment
                    self.ndarray[flagind][i] = ''.join(flagls)
                    self.ndarray[commentind][i] = comment
                    #print "flag2", self.ndarray[flagind][i], self.ndarray[commentind][i]
            self.ndarray[flagind] = np.asarray(self.ndarray[flagind])
            self.ndarray[commentind] = np.asarray(self.ndarray[commentind])
        else:
            for elem in self:
                if elem.time >= start and elem.time <= end:
                    fllist = list(elem.flag)
                    if not len(fllist) > 1:
                        fllist = defaultflag
                    fllist[pos] = str(flag)
                    elem.flag=''.join(fllist)
                    elem.comment = comment
        if flag == 1 or flag == 3:
            if enddate:
                #print ("flag_stream: Flagged data from %s to %s -> (%s)" % (startdate.isoformat(),enddate.isoformat(),comment))
                loggerstream.info("flag_stream: Flagged data from %s to %s -> (%s)" % (startdate.isoformat(),enddate.isoformat(),comment))
            else:
                loggerstream.info("flag_stream: Flagged data at %s -> (%s)" % (startdate.isoformat(),comment))

        #print self.ndarray[flagind][np.where(self.ndarray[flagind] != '')]
        #print self.ndarray[flagind]

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
            print "simplebasevalue2stream: requires ndarray"
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


    def func2stream(self,function,**kwargs):
        """
      DESCRIPTION:
        combine data stream and functions obtained by fitting and interpolation. Possible combination
        modes are 'add' (default), subtract 'sub', divide 'div' and 'multiply'. Furthermore, the
        function values can replace the original values at the given timesteps of the stream

      PARAMETERS:
        function        (function): required - output of stream.fit or stream.interpol
        keys            (list): default = 'x','y','z'
        mode            (string): one of 'add','sub','div','multiply','values' - default = 'add'

      APPLICTAION:
        used by stream.baseline

        """
        keys = kwargs.get('keys')
        mode = kwargs.get('mode')
        if not keys:
            keys = ['x','y','z']
        if not mode:
            mode = 'add'

        # Changed that - 49 sec before, no less then 2 secs
        if not len(self.ndarray[0]) > 0:
            print "func2stream: requires ndarray - trying old LineStruct functions"
            if mode == 'add':
                return self.func_add(function, keys=keys)
            elif mode == 'sub':
                return self.func_subtract(function, keys=keys)
            else:
                return self

        #1. calculate function value for each data time step
        array = [[] for key in KEYLIST]
        array[0] = self.ndarray[0]
        # get x array for baseline
        #indx = KEYLIST.index('x')
        #arrayx = self.ndarray[indx].astype(float)
        functimearray = (self.ndarray[0].astype(float)-function[1])/(function[2]-function[1])
        #print functimearray
        for key in KEYLIST:
            ind = KEYLIST.index(key)
            if key in keys: # new
                ar = self.ndarray[ind].astype(float)
                if mode == 'add':
                    array[ind] = ar + function[0]['f'+key](functimearray)
                elif mode == 'addbaseline':
                    if key == 'y':
                        #indx = KEYLIST.index('x')
                        #Hv + Hb;   Db + atan2(y,H_corr)    Zb + Zv
                        #print type(self.ndarray[ind]), key, self.ndarray[ind]
                        array[ind] = np.arctan2(np.asarray(list(ar)),np.asarray(list(arrayx)))*180./np.pi + function[0]['f'+key](functimearray)
                        self.header['col-y'] = 'd'
                        self.header['unit-col-y'] = 'deg'
                    else:
                        array[ind] = ar + function[0]['f'+key](functimearray)
                        if key == 'x': # remember this for correct y determination
                            arrayx = array[ind]
                elif mode == 'sub':
                    array[ind] = ar - function[0]['f'+key](functimearray)
                elif mode == 'values':
                    array[ind] = function[0]['f'+key](functimearray)
                elif mode == 'div':
                    array[ind] = ar / function[0]['f'+key](functimearray)
                elif mode == 'multiply':
                    array[ind] = ar * function[0]['f'+key](functimearray)
                else:
                    print "func2stream: mode not recognized"
            else: # new
                if len(self.ndarray[ind]) > 0:
                    array[ind] = self.ndarray[ind].astype(object)

        return DataStream(self,self.header,np.asarray(array))


    def func_add(self,function,**kwargs):
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
                    print "func2stream: mode not recognized"

            return DataStream(self,self.header,np.asarray(array))

        for elem in self:
            # check whether time step is in function range
            if function[1] <= elem.time <= function[2]:
                functime = (elem.time-function[1])/(function[2]-function[1])
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        raise ValueError, "Column key not valid"
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


    def func_subtract(self,function,**kwargs):
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
                        raise ValueError, "Column key not valid"
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
        debugmode = kwargs.get('debugmode')

        if key in KEYLIST:
            gapvariable = True

        if not gapvariable:
            gapvariable = 'var5'

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

        loggerstream.info('--- Starting filling gaps with NANs at %s ' % (str(datetime.now())))

        stream = self.copy()
        prevtime = 0

        ndtype = False
        if len(stream.ndarray[0]) > 0:
            maxtime = max(stream.ndarray[0])
            mintime = min(stream.ndarray[0])
            length = len(stream.ndarray[0])
            sourcetime = stream.ndarray[0]
            ndtype = True
        else:
            maxtime = self[-1].time

        if debugmode:
            print "Time range:", self[0].time, self[-1].time
            print "Length, samp_per and accuracy:", len(self), sp, accuracy

        shift = 0
        if ndtype:
            # Get time diff and expected count
            timediff = maxtime - mintime
            expN = int(round(timediff/newsp))+1
            if expN == len(sourcetime):
                # Found the expected amount of time steps - no gaps
                loggerstream.info("get_gaps: No gaps found - Returning")
                return stream
            else:
                diff = sourcetime[1:] - sourcetime[:-1]
                num_fills = np.round(diff / newsp) - 1
                projtime = np.linspace(mintime, maxtime, num=expN, endpoint=True)
                loggerstream.info("get_gaps: Found gaps - Filling nans to them")
                for i in np.where(diff > newsp+accuracy)[0]:
                    print i
                    nf = num_fills[i]
                    nans = [np.nan] * nf
                    for idx,elem in enumerate(stream.ndarray):
                        if idx == 0:
                            stream.ndarray[idx] = np.asarray(projtime)
                        else:
                            if len(stream.ndarray[idx]) > 0:
                                elem = list(elem)
                                elem[i+1+shift:i+1+shift] = nans
                                stream.ndarray[idx] = np.asarray(elem)
                    shift = int(shift + nf)
            return stream.sorting()

            """ # below is slow
        #print "using accuracy", newsp, accuracy*3600*24
        if ndtype:
            timediff = maxtime - mintime
            N = int(round(timediff/newsp))+1
            print "getgap", timediff, N
            # create projected time column
            projtime = np.linspace(mintime, maxtime, num=N, endpoint=True)
            #print "getgap", stream.ndarray[0]

            #print "getgap", projtime
            #print length, N
            # First drop all existing time steps from projtime
            indproj = np.nonzero(np.in1d(projtime, sourcetime))[0]
            projtime = np.delete(projtime, indproj)
            #print "Non identical time values", len(projtime)
            # Finally go through the remaining projtimes and test for accuracy
            remprojtime = np.asarray([t for t in projtime if not np.min(np.abs(sourcetime-t)) < accuracy])
            #print len(remprojtime)

            print "Test4:", datetime.utcnow()

            # Now append empty values to each ndarray
            for idx,elem in enumerate(stream.ndarray):
                if len(stream.ndarray[idx]) > 0:
                    if idx == 0:
                        stream.ndarray[idx] = np.append(stream.ndarray[idx],remprojtime)
                    else:
                        if KEYLIST[idx] in NUMKEYLIST:
                            stream.ndarray[idx] = np.append(stream.ndarray[idx],np.asarray([float('nan')]*len(remprojtime)))
                        else:
                            stream.ndarray[idx] = np.append(stream.ndarray[idx],np.asarray(['-']*len(remprojtime)))
            """
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

        loggerstream.info('--- Filling gaps finished at %s ' % (str(datetime.now())))
        if debugmode:
            print "Ending:", stream[0].time, stream[-1].time

        return stream.sorting()


    def get_sampling_period(self):
        """
        returns the dominant sampling frequency in unit ! days !

        for time savings, this function only tests the first 1000 elements
        """

        if len(self.ndarray[0]) > 0:
            timecol = self.ndarray[0].astype(float)
        else:
            timecol= self._get_column('time')

        # New way:
        if len(timecol) > 1:
            diffs = np.asarray(timecol[1:]-timecol[:-1])
            me = np.median(diffs)
            st = np.std(diffs)
            diffs = [el for el in diffs if el < me+2*st and el > me-2*st]
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
            loggerstream.error("get_sampling_period: unkown problem - returning 0")
            domtd = 0

        if not domtd == 0:
            return domtd
        else:
            try:
                return timedifflist[-2][1]
            except:
                loggerstream.error("get_sampling_period: could not identify dominant sampling rate")
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

        sr = self.get_sampling_period()*24*3600
        unit = ' sec'

        # Create a suitable rounding function:
        # Use simple rounds if sr > 60 secs
        # Check accuracy for sr < 10 secs (three digits:
        #       if abs(sr-round(sr,0)) * 1000 e.g. (1.002 -> 2, 0.998 -> 2)
        if sr < 59:
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
        Method to integrate selected columns respect to time.
        -- Using scipy.integrate.cumtrapz

        optional:
        keys: (list - default ['x','y','z','f'] provide limited key-list
        """


        loggerstream.info('--- Integrating started at %s ' % str(datetime.now()))

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
        loggerstream.info('--- integration finished at %s ' % str(datetime.now()))
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
            loggerstream.warning("interpol: Interpolation kind %s not valid. Using linear interpolation instead." % kind)
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

        loggerstream.info("interpol: Interpolating stream with %s interpolation." % kind)

        for key in keys:
            if not key in NUMKEYLIST:
                loggerstream.error("interpol: Column key not valid!")
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
                loggerstream.warning("interpol: interpolation of zero length data set - wont work.")
                pass

        loggerstream.info("interpol: Interpolation complete.")

        func = [functionkeylist, sv, ev]

        return func

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

        loggerstream.info('--- Starting k value calculation: %s ' % (str(datetime.now())))

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
            print "Removing outliers"
        stream = stream.flag_outlier(keys=['x','y','z'],threshold=6.) # Weak conditions
        stream = stream.remove_flagged()

        sr = stream.samplingrate()
        if debug:
            print "Sampling rate", sr

        if sr > 65:
            print "Algorythm requires minute or higher resolution - aborting"
            return DataStream()
        if sr <= 0.9:
            print "Data appears to be below 1 second resolution - filtering to seconds first"
            stream = stream.nfilter(filter_width=timedelta(seconds=1))
            sr = stream.samplingrate()
        if 0.9 < sr < 55:
            print "Data appears to be below 1 minute resolution - filtering to minutes"
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
            print "ndtype - Timeseries ending at:", num2date(np.max(stream.ndarray[0]))
        else:
            timediff = stream[-1].time - stream[0].time
            gettyp = stream[0].typ
            print "LineStruct - Timeseries ending at:", num2date(stream[-1].time)

        print "Coverage in days:", timediff
        if timediff < 1.1:   # 1 corresponds to 24 hours
            print "not enough time covered - aborting"
            return

        if debug:
            print "Typ:", gettyp

        # Transform the coordinate system to XYZ, asuming a hdz orientation.
        fmistream = stream
        if gettyp == 'idff':
            fmistream = stream._convertstream('idf2xyz',keep_header=True)
        elif gettyp == 'hdzf':
            fmistream = stream._convertstream('hdz2xyz',keep_header=True)
        elif not gettyp == 'xyzf':
            print "Unkown type of data - please provide xyzf, idff, hdzf -aborting"
            return

        # By default use H for determination
        if debug:
            print "converting data to hdz - only analyze h"
            print "This is applicable in case of baselinecorrected data"
        # TODO Important currently we are only using x (or x and y)

        if hcomp:
            print "Please note: H comp requires that columns xyz contain baseline corrected values"
            fmistream = fmistream._convertstream('xyz2hdz',keep_header=True)
        elif 'DataAbsFunctionObject' in fmistream.header:
            print "Found Baseline function"
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

                """
                    try:
                        print "Try"
                        ind = (li.ndarray[0]).index(time)
                        print "T Step1"
                        li = li._delete(ind)
                        print "T Step2"
                        emptystream.ndarray[0] = np.append(emptystream.ndarray[0],kline[0])
                        print "T Step3"
                        emptystream.ndarray[indvar1] = np.append(emptystream.ndarray[indvar1],kline[1])
                        print "T Step4"
                        emptystream.ndarray[indvar2] = np.append(emptystream.ndarray[indvar2],kline[2])
                    except:
                        print "Except"
                        array[0].append(kline[0])
                        array[indvar1].append(kvalstream.ndarray[indvar1],kline[1])
                        kvalstream.ndarray[indvar2] = np.append(kvalstream.ndarray[indvar2],kline[2])
                        print "E Step1", kline[0], kline[1], kline[2], kvalstream.ndarray
                        kvalstream.ndarray
                if len(emptystream.ndarray[0]) > 0:
                    emptystream = emptystream._append(li)
                    return emptystream
                else:
                    return kvalstream
            else:
                for kline in klist:
                    time = kline[0]
                    line = LineStruct()
                    line.time = kline[0]
                    line.var1 = kline[1]
                    line.var2 = kline[2]
                    try:
                        ind = ti.index(time)
                        del li[ind]
                        del ti[ind]
                        emptystream.add(line)
                    except:
                        kvalstream.add(line)

                if len(emptystream) > 0:
                    for i in li:
                        emptystream.add(i)
                    return emptystream
                else:
                    return kvalstream
                """


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
                    print "Loop Test", j, index, num2date(cdlist[index])-timedelta(days=deltaday)
                threehours = datastream.extract("time", date2num(num2date(cdlist[index])-timedelta(days=deltaday)), "<")
                index = index - 1
                if index < 0:
                    index = 7
                    deltaday += 1
                if debug:
                    print "Start", num2date(cdlist[index])-timedelta(days=deltaday)
                threehours = threehours.extract("time", date2num(num2date(cdlist[index])-timedelta(days=deltaday)), ">=")
                if ndtype:
                    len3hours = len(threehours.ndarray[0])
                else:
                    len3hours = len(threehours)
                if debug:
                    print "Length of three hour segment", len3hours
                if len3hours > 0:
                    if ndtype:
                        indx = KEYLIST.index('x')
                        indy = KEYLIST.index('y')
                        colx = threehours.ndarray[indx]
                    else:
                        colx = threehours._get_column('x')
                    colx = [elem for elem in colx if not isnan(elem)]
                    xmaxval = max(colx)
                    xminval = min(colx)
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
                    k = 9
                    for count,val  in enumerate(k_scale):
                        if maxmindiff > val:
                            k = count
                    if debug:
                        print "Extrema", k, maxmindiff, xmaxval, xminval, ymaxval, yminval
                    # create a k-value list
                else:
                    k = 9
                    maxmindiff = 0
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
                n = kval**3.3
                """
                for ind, val in enumerate(startinghours):

                    if val <= index < val+3:
                        kval = klist[ind][1]
                        kind = ind
                        if debug:
                            print "n:", kval, kval**3.3
                        n = kval**3.3
                """
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
                    print "##############################################"
                    print " careful - datastream not long enough for correct k determination"
                    print "##############################################"
                    print "Hourly means not correctly determinable for day", meanat
                    print "as the extended time range is not reached"
                    print "----------------------------------------------"
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
            currentdate.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            currentdate = num2date(fmistream[-1].time).replace(tzinfo=None)
            lastdate = currentdate
            currentdate.replace(hour=0, minute=0, second=0, microsecond=0)

        print "Last effective time series ending at day", currentdate

        print " -----------------------------------------------------"
        print " ------------- Starting backward analysis ------------"
        print " --------------- beginning at last time --------------"

        # selecting reduced time range!!!
        array = fmistream._select_timerange(starttime=currentdate-timedelta(days=2))
        fmitstream = DataStream([LineStruct()],fmistream.header,array)

        cdlist = [date2num(currentdate.replace(hour=elem)) for elem in startinghours]
        #print "Daily list", cdlist

        ta, i = find_nearest(np.asarray(cdlist), date2num(lastdate-timedelta(minutes=90)))
        print "Nearest three hour mark", num2date(ta), i

        if plot:
            import mpplot as mp
            fmistream.plot(noshow=True, plottitle="0")

        # 1. get a backward 24 hour calculation from the last record
        klist = maxmink(fmitstream,cdlist,i,k_scale)
        kstream = klist2stream(klist, kstream)

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

        # 3. recalc k
        klist = maxmink(redfmi,cdlist,i,k_scale)
        kstream = klist2stream(klist, kstream)

        # 4. recalc sr and subtract
        finalhmean = fmimeans(fmitstream,date2num(lastdate),kstream)
        finalfunc = finalhmean.fit(['x','y','z'],fitfunc='harmonic',fitdegree=5)
        firedfmi = fmistream.func2stream(finalfunc,mode='sub')

        if plot:
            mp.plot(finalhmean,['x','y','z'],function=finalfunc,noshow=True, plottitle="2: SR function")
            #finalhmean.plot(['x','y','z'],function=finalfunc,noshow=True, plottitle="2: SR function")
            firedfmi.plot(['x','y','z'],noshow=True, plottitle="2: reduced")
            fmitstream.plot(['x','y','z'],plottitle="2")

        # 5. final k
        klist = maxmink(firedfmi,cdlist,i,k_scale)
        kstream = klist2stream(klist, kstream)

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
        print " -----------------------------------------------------"
        print " ------------- Starting forward analysis -------------"
        print " -----------------  from first date ------------------"

        if ndtype:
            st = np.min(fmistream.ndarray[0])
        else:
            st = fmistream[0].time

        startday = int(np.floor(st))
        for daynum in range(1,int(timediff)+1):
            currentdate = num2date(startday+daynum)
            print "Running daily chunks forward until ", currentdate
            # selecting reduced time range!!!
            array = fmistream._select_timerange(starttime=currentdate-timedelta(days=3),endtime=currentdate+timedelta(days=1))
            fmitstream = DataStream([LineStruct()],fmistream.header,array)

            cdlist = [date2num(currentdate.replace(hour=elem)) for elem in startinghours]
            #print "Daily list", cdlist

            # 1. get a backward 24 hour calculation from the last record
            klist = maxmink(fmitstream,cdlist,0,k_scale)
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
        stddev = stream.mean('f',meanfunction='std')
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
            loggerstream.error('mean: empty stream - aborting')
            if std:
                return float("NaN"), float("NaN")
            else:
                return float("NaN")

        if not isinstance( percentage, (int,long)):
            loggerstream.error("mean: Percentage needs to be an integer!")
        if not key in KEYLIST[:16]:
            loggerstream.error("mean: Column key not valid!")

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
            print ('mean: Too many nans in column, exceeding %d percent' % percentage)
            loggerstream.warning('mean: Too many nans in column, exceeding %d percent' % percentage)
            if std:
                return float("NaN"), float("NaN")
            else:
                return float("NaN")


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
                    loggerstream.error("factor: Multiplying time? That's just plain silly.")
                else:
                    if square == False:
                        newval = [elem * factors[key] for elem in val]
                        loggerstream.info('factor: Multiplied column %s by %s.' % (key, factors[key]))
                    else:
                        newval = [elem ** factors[key] for elem in val]
                        loggerstream.info('factor: Multiplied column %s by %s.' % (key, factors[key]))
                if ndtype:
                    self.ndarray[ind] = np.asarray(newval)
                else:
                    self = self._put_column(newval, key)
            else:
                loggerstream.warning("factor: Key '%s' not in keylist." % key)

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


    def offset(self, offsets):
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

    APPLICATION:

        """
        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype =True

        for key in offsets:
            if key in KEYLIST:
                if ndtype:
                    ind = KEYLIST.index(key)
                    val = self.ndarray[ind]
                else:
                    val = self._get_column(key)
                if key == 'time':
                    secperday = 24*3600
                    try:
                        os = offsets[key].total_seconds()/secperday
                    except:
                        try:
                            exec('os = '+offsets[key]+'.total_seconds()/secperday')
                        except:
                            print "offset: error with time offset - check provided timedelta"
                            break
                    val = val + os
                    #print num2date(val[0]).replace(tzinfo=None)
                    #print num2date(val[0]).replace(tzinfo=None) + offsets[key]
                    #newval = [date2num(num2date(elem).replace(tzinfo=None) + offsets[key]) for elem in val]
                    loggerstream.info('offset: Corrected time column by %s sec' % str(offsets[key]))
                else:
                    val = val + offsets[key]
                    #newval = [elem + offsets[key] for elem in val]
                    loggerstream.info('offset: Corrected column %s by %.3f' % (key, offsets[key]))
                if ndtype:
                    self.ndarray[ind] = val
                else:
                    self = self._put_column(val, key)
            else:
                loggerstream.error("offset: Key '%s' not in keylist." % key)

        return self


    def plot(self, keys=None, debugmode=None, **kwargs):
        """
    DEFINITION:
        Code for simple application.
        Creates a simple graph of the current stream.
        In order to run matplotlib from cron one needs to include (matplotlib.use('Agg')).

    PARAMETERS:
    Variables:
        - keys:         (list) A list of the keys (str) to be plotted.
    Kwargs:
        - annotate:     (bool) Annotate data using comments
        - annoxy:       (dictionary) Define placement of annotation (in % of scale).
                        Possible parameters:
                        (called for annotated storm phases:)
                        sscx, sscy, mphx, mphy, recx, recy
        - annophases:   (bool) Annotate phase times with titles. Default False.
        - bgcolor:      (string) Define background color e.g. '0.5' greyscale, 'r' red, etc
        - colorlist:    (list - default []) Provide a ordered color list of type ['b','g']
        - confinex:     (bool) Confines tags on x-axis to shorter values.
        - errorbar:     (boolean - default False) plot dx,dy,dz,df values if True
        - function:     (func) [0] is a dictionary containing keys (e.g. fx), [1] the startvalue, [2] the endvalue
                        Plot the content of function within the plot.
        - fullday:      (boolean) - default False. Rounds first and last day two 0:00 respectively 24:00 if True
        - fmt:          (string?) format of outfile
        - grid:         (bool) show grid or not, default = True
        - gridcolor:    (string) Define grid color e.g. '0.5' greyscale, 'r' red, etc
        - labelcolor:   (string) Define grid color e.g. '0.5' greyscale, 'r' red, etc
        - noshow:       (bool) don't call show at the end, just returns figure handle
        - outfile:      string to save the figure, if path is not existing it will be created
        - padding:      (integer - default 0) Value to add to the max-min data for adjusting y-scales
        - savedpi:      (integer) resolution
        - stormphases:  (list) Should be a list with four datetime objects:
                        [0 = date of SSC/start of initial phase,
                        1 = start of main phase,
                        2 = start of recovery phase,
                        3 = end of recovery phase]
        - plotphases:   (list) List of keys of plots to shade.
        - resolution:   (int) maximum number of points to be displayed.
        - specialdict:  (dictionary) contains special information for specific plots.
                        key corresponds to the column
                        input is a list with the following parameters
                        ('None' if not used)
                        ymin
                        ymax
                        ycolor
                        bgcolor
                        grid
                        gridcolor
        - symbollist:   (string - default '-') symbol for primary plot
        - symbol_func:  (string - default '-') symbol of function plot

    RETURNS:
        - matplotlib plot.show(), or plot.save() if variable outfile defined.

    EXAMPLE:
        >>> cs1_data.plot(['f'],
                outfile = 'frequenz.png',
                specialdict = {'f':[44184.8,44185.8]},
                plottitle = 'Station Graz - Feldstaerke 05.08.2013',
                bgcolor='white')

    APPLICATION:

        """

        annotate = kwargs.get('annotate')
        annoxy = kwargs.get('annoxy')
        annophases = kwargs.get('annophases')
        bartrange = kwargs.get('bartrange') # in case of bars (z) use the following trange
        bgcolor = kwargs.get('bgcolor')
        colorlist = kwargs.get('colorlist')
        confinex = kwargs.get('confinex')
        endvalue = kwargs.get('endvalue')
        errorbar = kwargs.get('errorbar')
        figure = kwargs.get('figure')
        fmt = kwargs.get('fmt')
        function = kwargs.get('function')
        fullday = kwargs.get('fullday')
        grid = kwargs.get('grid')
        gridcolor = kwargs.get('gridcolor')
        labelcolor = kwargs.get('labelcolor')
        noshow = kwargs.get('noshow')
        outfile = kwargs.get('outfile')
        padding = kwargs.get('padding')
        plotphases = kwargs.get('plotphases')
        plottitle = kwargs.get('plottitle')
        plottype = kwargs.get('plottype')
        resolution = kwargs.get('resolution')
        savedpi = kwargs.get('savedpi')
        stormphases = kwargs.get('stormphases')
        specialdict = kwargs.get('specialdict')
        symbol_func = kwargs.get('symbol_func')
        symbollist = kwargs.get('symbollist')

        if not keys:
            keys = self._get_key_headers(limit=9,numerical=True)
            print "Plotting keys:", keys
        if not function:
            function = None
        if not plottitle:
            plottitle = None
        if not colorlist:
            colorlist = ['b','g','m','c','y','k','b','g','m','c','y','k']
        if not symbollist:
            symbollist = ['-','-','-','-','-','-','-','-','-','-','-','-','-']
        if not plottype:
            plottype = 'discontinuous' # can also be "continuous"
        if not symbol_func:
            symbol_func = '-'
        if not savedpi:
            savedpi = 80
        if not bartrange:
            bartrange = 0.06
        if not padding:
            padding = 0
        if not labelcolor:
            labelcolor = '0.2'
        if not bgcolor:
            bgcolor = '#d5de9c'
        if not gridcolor:
            gridcolor = '#316931'
        if not grid:
            grid =True
        if not resolution:
            resolution = 1296000  # 15 days of 1 second data can be maximale shown in detail, 1.5 days of 10 Hz

        myyfmt = ScalarFormatter(useOffset=False)

        # TODO first check whether all keys contain data - Otherwise the subplot amount is wrong
        # or alternatively plot empty datasets as well
        n_subplots = len(keys)

        if n_subplots < 1:
            loggerstream.error("plot: Number of keys not valid.")
            raise Exception("Need keys to plot!")
        count = 0

        if not figure:
            fig = plt.figure()
        else:
            fig = figure

        #fig = matplotlib.figure.Figure()
        lst = np.asarray([elem.comment for elem in self])
        lst2 = [elem.comment for elem in self if not elem.comment == '-']
        print "Starting", lst2, lst

        #print "Starting", self._get_column('flag')

        plotstream = self
        if resolution and len(plotstream) > resolution:
            loggerstream.info("plot: Reducing resultion ...")
            loggerstream.info("plot: Original resolution: %i" % len(plotstream))
            stepwidth = int(len(plotstream)/resolution)
            plotstream = DataStream(plotstream[::stepwidth],plotstream.header)
            loggerstream.info("plot: New resolution: %i" % len(plotstream))

        try:
            t = self.ndarray[0]
            if not len(t) > 0:
                x=1/0
            print "Found ndarray time", t
            if len(t) > resolution:
                loggerstream.info("plot: Reducing data resultion ...")
                stepwidth = int(len(t)/resolution)
                t = t[::stepwidth]
                print "New length", len(t)
            loggerstream.info("plot: Start plotting of stream with length %i" % len(self.ndarray[0]))
        except:
            t = np.asarray([row[0] for row in plotstream])
            loggerstream.info("plot: Start plotting of stream with length %i" % len(plotstream))
        for key in keys:
            if not key in KEYLIST[1:16]:
                loggerstream.error("plot: Column key (%s) not valid!" % key)
                raise Exception("Column key (%s) not valid!" % key)
            ind = KEYLIST.index(key)
            try:
                yplt = self.ndarray[ind]
                if not len(yplt) > 0:
                    x=1/0
                if len(yplt) > resolution:
                    stepwidth = int(len(yplt)/resolution)
                    yplt = yplt[::stepwidth]
            except:
                yplt = np.asarray([float(row[ind]) for row in plotstream])
            #yplt = self._get_column(key)

            # Switch between continuous and discontinuous plots
            if debugmode:
                print "column extracted at %s" % datetime.utcnow()
            if plottype == 'discontinuous':
                yplt = maskNAN(yplt)
            else:
                #yplt = yplt[numpy.logical_not(numpy.isnan(yplt))]
                nans, test = nan_helper(yplt)
                t = [t[idx] for idx, el in enumerate(yplt) if not nans[idx]]
                #t = newt
                yplt = [el for idx, el in enumerate(yplt) if not nans[idx]]

            # 1. START PLOTTING (if non-NaN data is present)

            len_val= len(yplt)
            if debugmode:
                print "Got row with %d elements" % len_val
            if len_val > 1:
                count += 1
                ax = count
                subplt = "%d%d%d" %(n_subplots,1,count)

                # 2. PREAMBLE: define plot properties
                # -- Create primary plot and define x scale and ticks/labels of subplots
                # -- If primary plot already exists, add subplot
                if count == 1:
                    ax = fig.add_subplot(subplt, axisbg=bgcolor)
                    if plottitle:
                        ax.set_title(plottitle)
                    a = ax
                else:
                    ax = fig.add_subplot(subplt, sharex=a, axisbg=bgcolor)

                timeunit = ''

                # -- If dates to be confined, set value types:
                if confinex:
                    trange = np.max(t) - np.min(t)
                    loggerstream.debug('plot: x range = %s' % str(trange))
                    #print trange
                    if trange < 0.0001: # 8 sec level
                        #set 0.5 second
                        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%S'))
                        timeunit = '[Sec]'
                    elif trange < 0.01: # 13 minute level
                        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%M:%S'))
                        timeunit = '[M:S]'
                    elif trange <= 1: # day level
                        # set 1 hour
                        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
                        timeunit = '[H:M]'
                    elif trange < 7: # 3 day level
                        if trange < 2:
                            ax.get_xaxis().set_major_locator(matplotlib.dates.HourLocator(interval=6))
                        elif trange < 5:
                            ax.get_xaxis().set_major_locator(matplotlib.dates.HourLocator(interval=12))
                        else:
                            ax.get_xaxis().set_major_locator(matplotlib.dates.WeekdayLocator(byweekday=matplotlib.dates.MO))
                        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%d.%b\n%H:%M'))
                        setp(ax.get_xticklabels(),rotation='0')
                        timeunit = '[Day-H:M]'
                    elif trange < 60: # month level
                        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%d.%b'))
                        setp(ax.get_xticklabels(),rotation='70')
                        timeunit = '[Day]'
                    elif trange < 150: # year level
                        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%d.%b\n%Y'))
                        setp(ax.get_xticklabels(),rotation='0')
                        timeunit = '[Day]'
                    elif trange < 600: # minute level
                        if trange < 300:
                            ax.get_xaxis().set_major_locator(matplotlib.dates.MonthLocator(interval=1))
                        elif trange < 420:
                            ax.get_xaxis().set_major_locator(matplotlib.dates.MonthLocator(interval=2))
                        else:
                            ax.get_xaxis().set_major_locator(matplotlib.dates.MonthLocator(interval=4))
                        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%b %Y'))
                        setp(ax.get_xticklabels(),rotation='0')
                        timeunit = '[Month]'
                    else:
                        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%Y'))
                        timeunit = '[Year]'

                # -- Set x-labels:
                if count < len(keys):
                    setp(ax.get_xticklabels(), visible=False)
                else:
                    ax.set_xlabel("Time (UTC) %s" % timeunit, color=labelcolor)

                # -- Adjust scales with padding:
                defaultpad = (np.max(yplt)-np.min(yplt))*0.05
                if defaultpad == 0.0:
                    defaultpad = 0.1
                if not padding or not padding == defaultpad:
                    padding = defaultpad
                ymin = np.min(yplt)-padding
                ymax = np.max(yplt)+padding

                if specialdict:
                    if key in specialdict:
                        paramlst = specialdict[key]
                        if not paramlst[0] == None:
                            ymin = paramlst[0]
                        if not paramlst[1] == None:
                            ymax = paramlst[1]

                # 3. PLOT EVERYTHING

                # -- Plot k-values with z (= symbol for plotting colored bars for k values)
                if symbollist[count-1] == 'z':
                    xy = range(9)
                    for num in range(len(t)):
                        if bartrange < t[num] < np.max(t)-bartrange:
                            ax.fill([t[num]-bartrange,t[num]+bartrange,t[num]+bartrange,t[num]-
                                bartrange],[0,0,yplt[num]+0.1,yplt[num]+0.1],
                                facecolor=cm.RdYlGn((9-yplt[num])/9.,1),alpha=1,edgecolor='k')
                    ax.plot_date(t,yplt,colorlist[count-1]+'|')
                else:
                    ax.plot_date(t,yplt,colorlist[count-1]+symbollist[count-1])

                # -- Plot error bars:
                if errorbar:
                    yerr = plotstream._get_column('d'+key)
                    if len(yerr) > 0:
                        ax.errorbar(t,yplt,yerr=varlist[ax+4],fmt=colorlist[count]+'o')
                    else:
                        loggerstream.warning('plot: Errorbars (d%s) not found for key %s' % (key, key))

                # -- Add grid:
                if grid:
                    ax.grid(True,color=gridcolor,linewidth=0.5)

                # -- Add annotations for flagged data:
                if annotate:
                    print "Yesss"
                    flag = plotstream._get_column('flag')
                    comm = plotstream._get_column('comment')
                    elemprev = "-"
                    try: # only do all that if column is in range of flagged elements (e.g. x,y,z,f)
                        print "Got here"
                        idxflag = FLAGKEYLIST.index(key)
                        poslst = [i for i,el in enumerate(FLAGKEYLIST) if el == key]
                        indexflag = int(poslst[0])
                        print idxflag, poslst, indexflag
                        print comm, flag
                        for idx, elem in enumerate(comm):
                            if not elem == elemprev:
                                print "Yesss, again"
                                if not elem == "-" and flag[idx][indexflag] in ['0','3']:
                                    annotecount = idx
                                    ax.annotate(r'%s' % (elem),
                                        xy=(t[idx], yplt[idx]),
                                        xycoords='data', xytext=(20, 20),
                                        textcoords='offset points',
                                        bbox=dict(boxstyle="round", fc="0.8"),
                                        arrowprops=dict(arrowstyle="->",
                                        shrinkA=0, shrinkB=1,
                                        connectionstyle="angle,angleA=0,angleB=90,rad=10"))
                                elif elem == "-" and idx > annotecount + 3: # test that one point defines the flagged range
                                    ax.annotate(r'End of %s' % (comm[idx-1]),
                                        xy=(t[idx-1], yplt[idx-1]),
                                        xycoords='data', xytext=(20, -20),
                                        textcoords='offset points',
                                        bbox=dict(boxstyle="round", fc="0.8"),
                                        arrowprops=dict(arrowstyle="->",
                                        shrinkA=0, shrinkB=1,
                                        connectionstyle="angle,angleA=0,angleB=90,rad=10"))
                            elemprev = elem
                    except:
                        if debugmode:
                            loggerstream.debug('plot: shown column beyong flagging range: assuming flag of column 0 (= time)')

                # -- Shade in areas of storm phases:
                if plotphases:
                    if not stormphases:
                        loggerstream.warning('plot: Need phase definition times in "stormphases" list variable.')
                    if len(stormphases) < 4:
                        loggerstream.warning('plot: Incorrect number of phase definition times in variable shadephases. 4 required.')
                    else:
                        t_ssc = stormphases[0]
                        t_mphase = stormphases[1]
                        t_recphase = stormphases[2]
                        t_end = stormphases[3]

                    if key in plotphases:
                        try:
                            ax.axvspan(t_ssc, t_mphase, facecolor='red', alpha=0.3, linewidth=0)
                            ax.axvspan(t_mphase, t_recphase, facecolor='yellow', alpha=0.3, linewidth=0)
                            ax.axvspan(t_recphase, t_end, facecolor='green', alpha=0.3, linewidth=0)
                        except:
                            loggerstream.error('plot: Error plotting shaded phase regions.')

                # -- Plot phase types with shaded regions:

                if not annoxy:
                    annoxy = {}
                if annophases:
                    if not stormphases:
                        loggerstream.debug('Plot: Need phase definition times in "stormphases" variable to plot phases.')
                    if len(stormphases) < 4:
                        loggerstream.debug('Plot: Incorrect number of phase definition times in variable shadephases. 4 required, %s given.' % len(stormphases))
                    else:
                        t_ssc = stormphases[0]
                        t_mphase = stormphases[1]
                        t_recphase = stormphases[2]
                        t_end = stormphases[3]

                    if key == plotphases[0]:
                        try:
                            y_auto = [0.85, 0.75, 0.70, 0.6, 0.5, 0.5, 0.5]
                            y_anno = ymin + y_auto[len(keys)-1]*(ymax-ymin)
                            tssc_anno, issc_anno = find_nearest(np.asarray(t), date2num(t_ssc))
                            yt_ssc = yplt[issc_anno]
                            if 'sscx' in annoxy:        # parameters for SSC annotation.
                                x_ssc = annoxy['sscx']
                            else:
                                x_ssc = t_ssc-timedelta(hours=2)
                            if 'sscy' in annoxy:
                                y_ssc = ymin + annoxy['sscy']*(ymax-ymin)
                            else:
                                y_ssc = y_anno
                            if 'mphx' in annoxy:        # parameters for main-phase annotation.
                                x_mph = annoxy['mphx']
                            else:
                                x_mph = t_mphase+timedelta(hours=1.5)
                            if 'mphy' in annoxy:
                                y_mph = ymin + annoxy['mphy']*(ymax-ymin)
                            else:
                                y_mph = y_anno
                            if 'recx' in annoxy:        # parameters for recovery-phase annotation.
                                x_rec = annoxy['recx']
                            else:
                                x_rec = t_recphase+timedelta(hours=1.5)
                            if 'recy' in annoxy:
                                y_rec = ymin + annoxy['recy']*(ymax-ymin)
                            else:
                                y_rec = y_anno

                            if not yt_ssc > 0.:
                                loggerstream.debug('plot: No data value at point of SSC.')
                            ax.annotate('SSC', xy=(t_ssc,yt_ssc),
                                        xytext=(x_ssc,y_ssc),
                                        bbox=dict(boxstyle="round", fc="0.95", alpha=0.6),
                                        arrowprops=dict(arrowstyle="->",
                                        shrinkA=0, shrinkB=1,
                                        connectionstyle="angle,angleA=0,angleB=90,rad=10"))
                            ax.annotate('Main\nPhase', xy=(t_mphase,y_mph), xytext=(x_mph,y_mph),
                                        bbox=dict(boxstyle="round", fc="0.95", alpha=0.6))
                            ax.annotate('Recovery\nPhase', xy=(t_recphase,y_rec),xytext=(x_rec,y_rec),
                                        bbox=dict(boxstyle="round", fc="0.95", alpha=0.6))
                        except:
                            loggerstream.error('Plot: Error annotating shaded phase regions.')

                # -- Plot given function:
                if function:
                    fkey = 'f'+key
                    if fkey in function[0]:
                        ttmp = arange(0,1,0.0001)# Get the minimum and maximum relative times
                        ax.plot_date(denormalize(ttmp,function[1],function[2]),function[0][fkey](ttmp),'r-')

                # -- Add Y-axis ticks:
                if bool((count-1) & 1):
                    ax.yaxis.tick_right()
                    ax.yaxis.set_label_position("right")

                # -- Take Y-axis labels from header information
                try:
                    ylabel = plotstream.header['col-'+key].upper()
                except:
                    ylabel = ''
                    pass
                try:
                    yunit = plotstream.header['unit-col-'+key]
                except:
                    yunit = ''
                    pass
                if not yunit == '':
                    #yunit = re.sub('[#$%&~_^\{}]', '', yunit)
                    yunit = re.sub('[#$%&~_\{}]', '', yunit)    # Allow for powers (^)
                    label = ylabel+' $['+yunit+']$'
                else:
                    label = ylabel
                ax.set_ylabel(label, color=labelcolor)
                ax.set_ylim(ymin,ymax)
                ax.get_yaxis().set_major_formatter(myyfmt)
                if fullday: # lower range is rounded at 0.01 digits to avoid full empty day plots at 75678.999993
                    ax.set_xlim(np.floor(np.round(np.min(t)*100)/100),np.floor(np.max(t)+1))

                loggerstream.info("plot: Finished plot %d at %s" % (count, datetime.utcnow()))

            # 4. END PLOTTING

            else:
                loggerstream.warning("plot: No data available for key %s" % key)


        fig.subplots_adjust(hspace=0)

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
        >>> import magpy
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
            print "Start powerspectrum at %s" % datetime.utcnow()

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
            loggerstream.error("Powerspectrum: Stream of zero length -- aborting")
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
            print "Extracted data for powerspectrum at %s" % datetime.utcnow()

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
            print "Maximum Noise Level at %s Hz: %s" % (val,asdm[idx])

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
            print "Finished powerspectrum at %s" % datetime.utcnow()

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
                loggerstream.error('Trim: Starttime (%s) is larger than endtime (%s).' % (starttime,endtime))
                raise ValueError, "Starttime is larger than endtime."

        loggerstream.info('Remove: Started from %s to %s' % (starttime,endtime))

        cutstream = DataStream()
        cutstream.header = self.header
        cutstream.ndarray = self.ndarray
        starttime = self._testtime(starttime)
        endtime = self._testtime(endtime)
        stval = 0

        if len(cutstream.ndarray[0]) > 0:
            st,ls = cutstream.findtime(starttime)
            ed,le = cutstream.findtime(endtime)
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
                        flagls = list(self.ndarray[flagind][i])
                        flag = flagls[pos]
                        if flag in flaglist:
                            array[pos][i] = float("nan")
                    except:
                        print "stream remove_flagged: index error"
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
        return DataStream(liste, self.header,array)



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

        loggerstream.info('remove_outlier: Starting outlier removal.')

        ndtype = False
        if len(self.ndarray[0]) > 0:
            ndtype = True
            arraytime = self.ndarray[0]
            flagind = KEYLIST.index('flag')
            commentind = KEYLIST.index('comment')
            print ("Use flag_outlier instead")
        elif len(self) > 1:
            arraytime = self._get_column('time')
        else:
            loggerstream.warning('remove_outlier: No data - Stopping outlier removal.')
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
                    print np.isnan(lstpart)
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
                        loggerstream.warning("remove_outlier: Eliminate outliers produced a problem: please check.")
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
                                        loggerstream.info(infoline)
                                        if stdout:
                                            print infoline
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

        loggerstream.info('remove_outlier: Outlier removal finished.')

        if ndtype:
            return restream
        else:
            return DataStream(newst, self.header, self.ndarray)


    def resample(self, keys, **kwargs):
        """
    DEFINITION:
        Uses Numpy interpolate.interp1d to resample stream to requested period.

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
        >>> resampled_stream = pos_data.resample(['f'],1)

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

        loggerstream.info("resample: Resampling stream of sampling period %s to period %s." % (sp,period))

        loggerstream.info("resample: Resampling keys %s " % (','.join(keys)))

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
                loggerstream.info("resample: Using fast algorithm.")
                si = timedelta(seconds=sp)
                sampling_period = si.seconds

                if period <= sampling_period:
                    loggerstream.warning("resample: Resampling period must be larger than original sampling period.")
                    return self

                #print "Trying fast algorythm"
                if not line == [] or ndtype: # or (ndtype and not line == []):
                    xx = int(np.round(period/sampling_period))
                    if ndtype:
                        newstream = DataStream([LineStruct()],{},np.asarray([]))
                        newstream.header = self.header
                        lst = []
                        for ind,elem in enumerate(self.ndarray):
                            #print "Now here", elem
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
                loggerstream.warning("resample: Fast resampling failed - switching to slow mode")
            except:
                loggerstream.warning("resample: Fast resampling failed - switching to slow mode")
                pass

        # This is done if timesteps are not at period intervals
        # -----------------------------------------------------

        #print "RESAMPLE Here 3"
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


        stwithnan = self.copy()
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
            #print "Resampling:", key
            if key not in KEYLIST[1:16]:
                loggerstream.warning("resample: Key %s not supported!" % key)

            index = KEYLIST.index(key)
            try:
                int_data = self.interpol([key],kind='linear')#'cubic')
                int_func = int_data[0]['f'+key]
                int_min = int_data[1]
                int_max = int_data[2]

                key_list = []
                for ind, item in enumerate(t_list):
                    #print item, ind
                    functime = (item - int_min)/(int_max - int_min)
                    #orgval = eval('self[int(ind*multiplicator)].'+key)
                    if ndtype:
                        if int(ind*multiplicator) <= len(self.ndarray[index]):
                            #orgval = self.ndarray[index][int(ind*multiplicator)]
                            orgval = stwithnan.ndarray[index][int(ind*multiplicator)]
                        else:
                            print "Check Resampling method"
                            orgval = 1.0
                    else:
                        orgval = getattr(stwithnan[int(ind*multiplicator)],key)
                    tempval = np.nan
                    # Not a safe fix, but appears to cover decimal leftover problems
                    # (e.g. functime = 1.0000000014, which raises an error)
                    if functime > 1.0:
                        functime = 1.0
                    if not isnan(orgval):
                        #print "no nan"
                        tempval = int_func(functime)
                    #print tempval, orgval
                    key_list.append(float(tempval))

                if ndtype:
                    array[index] = np.asarray(key_list)
                else:
                    res_stream._put_column(key_list,key)
            except:
                loggerstream.error("resample: Error interpolating stream. Stream either too large or no data for selected key")

        res_stream.ndarray = np.asarray(array)

        loggerstream.info("resample: Data resampling complete.")
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
            loggerstream.error('rotation: provided keylist need to have three components.')
            return self

        loggerstream.info('rotation: Applying rotation matrix.')

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

        loggerstream.info('rotation: Finished reorientation.')

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

        print "Function will be removed - use e.g. self.multiply({'y': 0.988}) instead"

        # Take care: if there is only 0.1 nT accurracy then there will be a similar noise in the deltaF signal

        offset = kwargs.get('offset')
        if not offset:
            offset = [0]*len(keys)
        else:
            if not len(offset) == len(keys):
                loggerstream.error('scale_correction: offset with wrong dimension given - needs to have the same length as given keys - returning stream without changes')
                return self

        try:
            assert len(self) > 0
        except:
            loggerstream.error('scale_correction: empty stream - aborting')
            return self

        offsetlst = []
        for key in KEYLIST:
            if key in keys:
                pos = keys.index(key)
                offsetlst.append(offset[pos])
            else:
                offsetlst.append(0.0)

        loggerstream.info('scale_correction:  --- Scale correction started at %s ' % str(datetime.now()))
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

        loggerstream.info('scale_correction:  --- Scale correction finished at %s ' % str(datetime.now()))

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

        if not 'time' in keys:
            ti = ['time']
            ti.extend(keys)
            keys = ti

        if len(self.ndarray[0]) > 0:
            # Check for flagging and comment column
            flagidx = KEYLIST.index('flag')
            commentidx = KEYLIST.index('comment')
            if len(self.ndarray[flagidx]) > 0:
                keys.append('flag')
            if len(self.ndarray[commentidx]) > 0:
                keys.append('comment')

            # Remove all missing
            for idx, elem in enumerate(self.ndarray):
                if not KEYLIST[idx] in keys:
                    self.ndarray[idx] = np.asarray([])
            return self
        else:
            return self

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

        ndtype = False
        if len(self.ndarray[0])>0:
            ndtype = True

        loggerstream.info('smooth: Start smoothing (%s window, width %d) at %s' % (window, window_len, str(datetime.now())))

        for key in keys:
            if key in NUMKEYLIST:

                if ndtype:
                    ind = KEYLIST.index(key)
                    x = self.ndarray[ind]
                else:
                    x = self._get_column(key)
                x = maskNAN(x)

                if x.ndim != 1:
                    loggerstream.error("smooth: Only accepts 1 dimensional arrays.")
                if x.size < window_len:
                    print x.size, window_len
                    loggerstream.error("smooth: Input vector needs to be bigger than window size.")
                if window_len<3:
                    return x
                if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
                    loggerstream.error("smooth: Window is none of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'")
                    loggerstream.debug("smooth: You entered string %s as a window." % window)

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
                loggerstream.error("Column key %s not valid." % key)


        loggerstream.info('smooth: Finished smoothing at %s' % (str(datetime.now())))

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
            loggerstream.error('Spectrogram: stream of zero length -- aborting')
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

        print ndtype
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
            print "steadyrise: no data found in selected column %s" % key
            return np.asarray([])
        # Finally fill the end
        for i in range(count):
            rescol.append(stacked)

        if not len(rescol) == len(self) and not len(rescol) == len(self.ndarray[0]) :
            loggerstream.error('steadrise: An error leading to unequal lengths has been encountered')
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
            loggerstream.error('Stereoplot: you need to provide idf data')
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
            loggerstream.error('Stereoplot: check you data file - unequal inc and dec data?')
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
        Removing dates outside of range between start- and endtime

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
                loggerstream.error('Trim: Starttime (%s) is larger than endtime (%s).' % (starttime,endtime))
                raise ValueError, "Starttime is larger than endtime."

        loggerstream.info('Trim: Started from %s to %s' % (starttime,endtime))

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
                for i in range(len(newarray)):
                    if len(newarray[i]) > idx:
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



    def variometercorrection(self, variopath, thedate, **kwargs):
        """
        DEFINITION:
            Function to perform a variometercorrection of an absresult stream
            towards the given datetime using the given variometer stream.
            Returns a new absresult object with new datetime and corrected values
        APPLICATION:
            Useful to compare various absolute measurement e.g. form one day and analyse their
            differences after correcting them to a single spot in time.
        PARAMETERS:
         Variables:
            - variodata: (DataStream) data to be used for reduction
            - endtime:  (datetime/str) End of period to trim to
         Kwargs:
            - funckeys: (list) keys of the variometerfile which are interpolated and used
            - usetime: (bool) use only the time part of thedate to correct to

        RETURNS:
            - stream:   (DataStream object) absolute stream - corrected

        EXAMPLE:
            >>> newabsdata = absdata.variometercorrection(starttime, endtime)

        APPLICATION:
        """
        funckeys = kwargs.get('funckeys')
        offset = kwargs.get('offset')
        usetime = kwargs.get('usetime')
        if not funckeys:
            funckeys = ['x','y','z','f']
        if not offset:
            offset = 0.0

        dateform = "%Y-%m-%d"

        # Return results within a new streamobject containing only
        # the average values and its uncertainties
        resultstream = DataStream()

        # Check for ndtype:
        ndtype = false
        if len(self.ndarray[0]) > 0:
            ndtype = True
            indtyp = KEYLIST.index('typ')
            typus = self.ndarray[inddtyp][0]
        else:
            typus = self[0].typ
        # 1 Convert absresult - idff to xyz
        # test stream type (xyz, idf or hdz?)
        # TODO add the end check whether streams are modified!!!!!!!!!!
        print typus
        if typus == 'idff':
            absstream = self._convertstream('idf2xyz')

        # 2 Convert datetime to number
        # check whether thedate is a time (then use this time every day)
        # or a full date
        if usetime:
            tmpdatelst = [datetime.date(num2date(elem.time)) for elem in absstream]
            datelist = self.union(tmpdatelst)
            datelist = [datetime.combine(elem, datetime.time(self._testtime(thedate))) for elem in datelist]
            print datelist
        else:
            datelist = [self._testtime(thedate)]

        # 3 Read and interplolate the variometer data
        start = datetime.strptime(datetime.strftime(num2date(self[0].time),dateform),dateform)
        end = datetime.strptime(datetime.strftime(num2date(self[-1].time),dateform),dateform)+timedelta(days=1)
        print start, end
        variostream = read(variopath,starttime=start, endtime=end)
        print len(variostream)
        function = variostream.interpol(funckeys)

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
                            loggerstream.error("variometercorrection: error in assigning new values")
                            return
                        exec('elem.'+key+' = elem.'+key+' - diff')
                    else:
                        pass
            else:
                loggerstream.warning("variometercorrection: Variometer stream does not cover the projected time range")
                pass

        # 5 Convert absresult - xyzf to idff
        absstream = absstream._convertstream('xyz2idf')

        return absstream
        """

    def write(self, filepath, **kwargs):
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
       --- specific functions for baseline file
        - fitfunc       (str) fit function for baselinefit
        - fitdegree
        - knotstep
        - extradays
        - year          (int) year
        - meanh         (float) annual mean of H component
        - meanf         (float) annual mean of F component
        - deltaF        (float) given deltaF value between pier and f position
        - diff          (DataStream) diff (deltaF) between vario and scalar
       --- specific functions for intermagnet file
        - version       (str) file version
        - gin           (gin) information node code
        - datatype      (str) R: reported, A: adjusted, Q: quasi-definit, D: definite
        - kvals         (Datastream) contains K value for iaf storage
        - comment       (string) some comment, currently used in IYFV
        - kind          (string) one of 'A' (all), 'Q' quiet days, 'D' disturbed days,
                                 currently used in IYFV
       --- specific functions for IAGA file
        - useg          (Bool) use delta F (G) instead of F for output



    RETURNS:
        - ...           (bool) True if successful.

    EXAMPLE:
        >>> stream.write('/home/user/data',
                        filenamebegins='WIK_',
                        filenameends='.min',
                        dateformat='%Y-%m-%d',
                        format_type='IAGA')
        (Output file = 'WIK_2013-08-10.min')

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
        success = True

        t1 = datetime.utcnow()
        #print "write - Start:", t1

        # Preconfigure some fileformats - can be overwritten by keywords
        if format_type == 'IMF':
            dateformat = '%b%d%y'
            try:
                extension = (self.header['StationID']).lower()
            except:
                extension = 'txt'
            filenameends = '.'+extension
        if format_type == 'IAF':
            try:
                filenamebegins = (self.header['StationIAGAcode']).upper()
            except:
                filenamebegins = 'XXX'
            dateformat = '%y%b'
            extension = 'BIN'
            filenameends = '.'+extension
        if format_type == 'IYFV':
            if not filenameends:
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
            format_type = 'PYSTR'
        if not format_type in PYMAG_SUPPORTED_FORMATS:
            loggerstream.warning('write: Output format not supported.')
            return
        if not dateformat:
            dateformat = '%Y-%m-%d' # or %Y-%m-%dT%H or %Y-%m or %Y or %Y
        if not coverage:
            coverage = timedelta(days=1)
        if not filenamebegins:
            filenamebegins = ''
        if not filenameends and not filenameends == '':
            # Extension for cdf files is automatically attached
            if format_type == 'PYCDF':
                filenameends = ''
            else:
                filenameends = '.txt'
        if not mode:
            mode= 'overwrite'

        if len(self) < 1 and len(self.ndarray[0]) < 1:
            loggerstream.error('write: Stream is empty!')
            raise Exception("Can't write an empty stream to file!")

        ndtype = False
        if len(self.ndarray[0]) > 0:
            self.ndarray[0] = self.ndarray[0].astype(float)
            # remove all data from array where time is not numeric
            #1. get indicies of nonnumerics in ndarray[0]
            nonnumlist = np.asarray([idx for idx,elem in enumerate(self.ndarray[0]) if np.isnan(elem)])
            #2. delete them
            if len(nonnumlist) > 0:
                print "write: Found NaNs in time column - deleting them", nonnumlist
                print self.ndarray[0]
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
        #print "write - initial selection:", t2-t1

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
                    #print "Trying to write ndarrays", ndarray
                else:
                    lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                    ndarray = np.asarray([])
                newst = DataStream(lst,self.header,ndarray)
                filename = filenamebegins + datetime.strftime(starttime,dateformat) + filenameends
                if len(lst) > 0 or len(ndarray[0]) > 0:
                    success = writeFormat(newst, os.path.join(filepath,filename),format_type,mode=mode,keys=keys,kvals=kvals)
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
                if len(ndarray[0]) > 0:
                    success = writeFormat(newst, os.path.join(filepath,filename),format_type,mode=mode,keys=keys,kvals=kvals,kind=kind,comment=comment)
                # get next endtime
                starttime = endtime
                cyear = cyear + 1
                yearstr = str(cyear) + '-01-01T00:00:00'
                endtime = datetime.strptime(yearstr,'%Y-%m-%dT%H:%M:%S')
        elif not coverage == 'all':
            #starttime = datetime.strptime(datetime.strftime(num2date(self[0].time).replace(tzinfo=None),'%Y-%m-%d'),'%Y-%m-%d')
            dailystream = self.copy()
            maxidx = -1
            endtime = starttime + coverage
            while starttime < lasttime:
                #lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                #newst = DataStream(lst,self.header)
                t3 = datetime.utcnow()
                #print "write - writing day:", t3

                if ndtype:
                    lst = []
                    # non-destructive
                    #print "write: start and end", starttime, endtime
                    #print dailystream.length()
                    #ndarray=self._select_timerange(starttime=starttime, endtime=endtime)
                    #print starttime, endtime, coverage
                    #print "Maxidx", maxidx
                    ndarray=dailystream._select_timerange(starttime=starttime, endtime=endtime, maxidx=maxidx)
                    if len(ndarray[0]) > 0:
                        maxidx = len(ndarray[0])*2
                        dailystream.ndarray = np.asarray([array[(len(ndarray[0])-1):] for array in dailystream.ndarray])
                    #print len(ndarray), len(ndarray[0]), len(ndarray[1]), len(ndarray[3])
                else:
                    lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                    ndarray = np.asarray([np.asarray([]) for key in KEYLIST])

                t4 = datetime.utcnow()
                #print "write - selecting time range needs:", t4-t3

                newst = DataStream(lst,self.header,ndarray)
                filename = filenamebegins + datetime.strftime(starttime,dateformat) + filenameends

                if format_type == 'IMF':
                    filename = filename.upper()
                if len(lst) > 0 or ndtype:
                    loggerstream.info('write: writing %s' % filename)
                    #print "Here", len(newst.ndarray[0])
                    success = writeFormat(newst, os.path.join(filepath,filename),format_type,mode=mode,keys=keys,version=version,gin=gin,datatype=datatype,useg=useg)
                starttime = endtime
                endtime = endtime + coverage

                t5 = datetime.utcnow()
                #print "write - written:", t5-t3
                #print "write - End:", t5

        else:
            filename = filenamebegins + filenameends
            success = writeFormat(self, os.path.join(filepath,filename),format_type,mode=mode,keys=keys,fitfunc=fitfunc,fitdegree=fitdegree, knotstep=knotstep,meanh=meanh,meanf=meanf,deltaF=deltaF,diff=diff,baseparam=baseparam, year=year,extradays=extradays)

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
            print "idf2xyz: no data found"
        if not keys:
            keys = ['x','y','z']
        if not len(keys) == 3:
            print "idf2xyz: invalid keys provided"

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
        self.header['DataComponents'] = 'XYZ'

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
            print "xyz2idf: no data found"
        if not keys:
            keys = ['x','y','z']
        if not len(keys) == 3:
            print "xyz2idf: invalid keys provided"

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
        self.header['DataComponents'] = 'IDF'
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
            print "xyz2hdz: no data found"
        if not keys:
            keys = ['x','y','z']
        if not len(keys) == 3:
            print "xyz2hdz: invalid keys provided"
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
        self.header['DataComponents'] = 'HDZ'
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
            print "hdz2xyz: no data found"
        if not keys:
            keys = ['x','y','z']
        if not len(keys) == 3:
            print "hdz2xyz: invalid keys provided"

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
        self.ndarray[indx] = self.ndarray[indx].astype(float) * (np.cos(dc))
        self.ndarray[indy] = self.ndarray[indx].astype(float) * (np.sin(dc))
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
        self.header['DataComponents'] = 'XYZ'

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

    def sendLogByMail(self,loglist,**kwargs):
        """
        function to send loggerstream lists by mail to the observer
        keywords:
        smtpserver
        sender
        user
        pwd
        destination
        subject
        """
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

        except Exception, exc:
            raise ValueError( "mail failed; %s" % str(exc) ) # give a error message

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

class ColStruct(object):
    def __init__(self,length, time=float('nan'), x=float('nan'), y=float('nan'), z=float('nan'), f=float('nan'), dx=float('nan'), dy=float('nan'), dz=float('nan'), df=float('nan'), t1=float('nan'), t2=float('nan'), var1=float('nan'), var2=float('nan'), var3=float('nan'), var4=float('nan'), var5=float('nan'), str1='-', str2='-', str3='-', str4='-', flag='0000000000000000-', comment='-', typ="xyzf", sectime=float('nan')):
        """
        Not used so far. Maybe useful for
        Speed optimization:
        Change the whole thing to column operations

        - at the end of flag is important to be recognized as string
        for column initialization use a length parameter and "lenght*[float('nan')]" or "lenght*['-']"to initialize nan-values
        """
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


def send_mail(send_from, send_to, **kwargs):
    """
    Function for sending mails with attachments
    """

    assert type(send_to)==list

    files = kwargs.get('files')
    user = kwargs.get('user')
    pwd = kwargs.get('pwd')
    port = kwargs.get('port')
    smtpserver = kwargs.get('smtpserver')
    subject = kwargs.get('subject')
    text = kwargs.get('text')

    if not smtpserver:
        smtpserver = 'smtp.web.de'
    if not files:
        files = []
    if not text:
        text = 'Cheers, Your Analysis-Robot'
    if not subject:
        subject = 'MagPy - Automatic Analyzer Message'
    if not port:
        port = 587

    assert type(files)==list

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    #smtp = smtplib.SMTP(server)
    smtp = SMTP()
    smtp.set_debuglevel(False)
    smtp.connect(smtpserver, port)
    smtp.ehlo()
    if port == 587:
        smtp.starttls()
    smtp.ehlo()
    if user:
        smtp.login(user, pwd)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


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

    if disableproxy:
        proxy_handler = urllib2.ProxyHandler( {} )
        opener = urllib2.build_opener(proxy_handler)
        # install this opener
        urllib2.install_opener(opener)

    # 1. No path
    if not path_or_url:
        loggerstream.error("read: File not specified.")
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
        loggerstream.error("read: File not specified.")
        raise Exception("No path given for data in read function!")
        pathname = path_or_url
        for file in iglob(pathname):
            stp = DataStream([],{},np.array([[] for ke in KEYLIST]))
            stp = _read(file, dataformat, headonly, **kwargs)
        """
    elif "://" in path_or_url:
        # some URL
        # extract extension if any
        loggerstream.info("read: Found URL to read at %s" % path_or_url)
        content = urllib2.urlopen(path_or_url).read()
        if debugmode:
            print urllib2.urlopen(path_or_url).info()
        if path_or_url[-1] == '/':
            # directory
            string = content.decode('utf-8')
            for line in string.split("\n"):
                if len(line) > 1:
                    filename = (line.strip().split()[-1])
                    if debugmode:
                        print filename
                    content = urllib2.urlopen(path_or_url+filename).read()
                    suffix = '.'+os.path.basename(path_or_url).partition('.')[2] or '.tmp'
                    #date = os.path.basename(path_or_url).partition('.')[0][-8:]
                    #date = re.findall(r'\d+',os.path.basename(path_or_url).partition('.')[0])
                    date = os.path.basename(path_or_url).partition('.')[0] # append the full filename to the temporary file
                    fh = NamedTemporaryFile(suffix=date+suffix,delete=False)
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
            fh = NamedTemporaryFile(suffix=date+suffix,delete=False)
            fh.write(content)
            fh.close()
            st = _read(fh.name, dataformat, headonly, **kwargs)
            os.remove(fh.name)
    else:
        # some file name
        pathname = path_or_url
        for file in iglob(pathname):
            stp = DataStream([],{},np.array([[] for ke in KEYLIST]))
            stp = _read(file, dataformat, headonly, **kwargs)
            #print stp.ndarray
            if (len(stp) > 0 and not np.isnan(stp[0].time)) or len(stp.ndarray[0]) > 0:   # important - otherwise header is going to be deleted
                st.extend(stp.container,stp.header,stp.ndarray)
            #del stp
        if len(st) == 0:
            # try to give more specific information why the stream is empty
            if has_magic(pathname) and not glob(pathname):
                loggerstream.error("read: Check file/pathname - No file matching pattern: %s" % pathname)
                loggerstream.error("read: No file matching file pattern: %s" % pathname)
                raise Exception("Cannot read non-existent file!")
            elif not has_magic(pathname) and not os.path.isfile(pathname):
                loggerstream.error("read: No such file or directory: %s" % pathname)
                raise Exception("Cannot read non-existent file!")
            # Only raise error if no starttime/endtime has been set. This
            # will return an empty stream if the user chose a time window with
            # no data in it.
            # XXX: Might cause problems if the data is faulty and the user
            # set starttime/endtime. Not sure what to do in this case.
            elif not 'starttime' in kwargs and not 'endtime' in kwargs:
                loggerstream.error("read: Cannot open file/files: %s" % pathname)
                raise Exception("Stream is empty!")

    if headonly and (starttime or endtime):
        msg = "read: Keyword headonly cannot be combined with starttime or endtime."
        loggerstream.error(msg)

    # Sort the input data regarding time
    if not skipsorting:
        st = st.sorting()
    # eventually trim data
    if starttime:
        st.trim(starttime=starttime)
    if endtime:
        st.trim(endtime=endtime)

    return st


#@uncompressFile
def _read(filename, dataformat=None, headonly=False, **kwargs):
    """
    Reads a single file into a MagPy DataStream object.
    Internal function only.
    """

    stream = DataStream([],{})
    format_type = None
    if not dataformat:
        # auto detect format - go through all known formats in given sort order
        for format_type in PYMAG_SUPPORTED_FORMATS:
            # check format
            if isFormat(filename, format_type):
                break
    else:
        # format given via argument
        dataformat = dataformat.upper()
        try:
            formats = [el for el in PYMAG_SUPPORTED_FORMATS if el == dataformat]
            format_type = formats[0]
        except IndexError:
            msg = "Format \"%s\" is not supported. Supported types: %s"
            loggerstream.error(msg % (dataformat, ', '.join(PYMAG_SUPPORTED_FORMATS)))
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


def joinStreams(stream_a,stream_b, **kwargs):
    """
    DEFINITION:
        Copy two streams together eventually replacing already existing time steps.
    """
    loggerstream.info('joinStreams: Start joining at %s.' % str(datetime.now()))

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
        if not len(stream_a.ndarray[0]) > 0 and not len(stream_b.ndarray[0]) > 0:
            loggerstream.error('subtractStreams: stream(s) empty - aborting subtraction.')
            return stream_a

    # non-destructive
    # --------------------------------------
    sa = stream_a.copy()
    sb = stream_b.copy()

    # Get indicies of timesteps of stream_b of which identical times are existing in stream_a-> delelte those lines
    # --------------------------------------
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

    stream = DataStream([LineStruct()],sb.header,np.asarray(array))

    return stream.sorting()

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

    loggerstream.info('mergeStreams: Start mergings at %s.' % str(datetime.now()))


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
            loggerstream.error('subtractStreams: stream(s) empty - aborting subtraction.')
            return stream_a

    # non-destructive
    # --------------------------------------
    sa = stream_a.copy()
    sb = stream_b.copy()

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
        print "mergeStreams: stream_a and stream_b are apparently not overlapping - returning stream_a"
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
        print "subtractStreams: stream_a and stream_b are not overlapping - returning stream_a"
        return stream_a

    timea = maskNAN(timea)
    timeb = maskNAN(timeb)

    orgkeys = stream_a._get_key_headers()

    # master header
    # --------------------------------------
    header = sa.header
    # just add the merged sensorid
    header['SecondarySensorID'] = sensidb

    print "mergeStream", sa.length(), sb.length(), sa._find_t_limits(), sb._find_t_limits()

    if ndtype:
            array = [[] for key in KEYLIST]
            # Init array with keys from stream_a
            for key in orgkeys:
                keyind = KEYLIST.index(key)
                array[keyind] = sa.ndarray[keyind]
            indtib = np.nonzero(np.in1d(timeb,timea))[0]
            # If equal elements occur in time columns
            if len(indtib) > int(0.5*len(timeb)):
                print "mergeStreams: Found identical timesteps - using simple merge"
                # get tb times for all matching indicies
                #print "merge", indtib, len(indtib), len(timea), len(timeb), np.argsort(timea), np.argsort(timeb)
                tb = np.asarray([timeb[ind] for ind in indtib])
                # Get indicies of stream_a of which times are present in matching tbs
                indtia = np.nonzero(np.in1d(timea,tb))[0]
                print "mergeStreams", tb, indtib, indtia, timea,timeb, len(indtib), len(indtia)

                if len(indtia) == len(indtib):
                    nanind = []
                    for key in keys:
                        keyind = KEYLIST.index(key)
                        #array[keyind] = sa.ndarray[keyind]
                        if len(sb.ndarray[keyind]) > 0: # stream_b values are existing
                            print "Found sb values", key
                            valb = [sb.ndarray[keyind][ind] for ind in indtib]
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

                        for i,ind in enumerate(indtia):
                            if key in NUMKEYLIST:
                                tester = isnan(array[keyind][ind])
                            else:
                                tester = False
                                if array[keyind][ind] == '':
                                    tester = True
                            if mode == 'insert' and tester:
                                array[keyind][ind] = valb[i]
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
                    for key in keys:
                        keyind = KEYLIST.index(key)
                        #print key, keyind
                        #print len(sa.ndarray[keyind]),len(sb.ndarray[keyind]), np.asarray(indtia)
                        if len(sb.ndarray[keyind]) > 0: # and key in function:

                            valb = [float(function[0]['f'+key]((sa.ndarray[0][ind]-function[1])/(function[2]-function[1]))) for ind in indtia]

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
                            if mode == 'insert' and tester:
                                array[keyind][ind] = valb[i]
                            elif mode == 'replace':
                                array[keyind][ind] = valb[i]
                            if flag:
                                ttt = num2date(array[0][ind])
                                fllst.append([ttt,ttt,key,flagid,comment])

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
        loggerstream.info('mergeStreams: Adding streams together not regarding for timeconstraints of data.')
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
                loggerstream.warning("mergeStreams: headers both have keys for %s. Headers may be incorrect." % elem)
        newsta.sorting()
        return newsta
    elif extend:
        loggerstream.info('mergeStreams: Extending stream a with data from b.')
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
                loggerstream.warning("mergeStreams: headers both have keys for %s. Headers may be incorrect." % elem)
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
                        loggerstream.error('mergeStreams: Column key (%s) not valid.' % key)
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

    loggerstream.info('mergeStreams: Mergings finished at %s ' % str(datetime.now()))

    return DataStream(stream_a, headera)



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
            loggerstream.warning("find_offset: Not enough memory for cubic spline. Attempting quadratic...")
            int_data = stream_b.interpol(['f'],kind='quadratic')
        except:
            loggerstream.error("find_offset: Too much data! Cannot interpolate function with high enough accuracy.")
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

    loggerstream.info("find_offset: Starting chi-squared iterations...")

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

    loggerstream.info("find_offset: Found an offset of stream_a of %s seconds." % t_offset)

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
        loggerstream.error('diffStreams: stream_a empty - aborting.')
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

    if not keys:
        keys = stream_a._get_key_headers(numerical=True)
    keysb = stream_b._get_key_headers(numerical=True)
    keys = list(set(keys)&set(keysb))

    if not len(keys) > 0:
        print "subtractStreams: No common keys found - aborting"
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
            loggerstream.error('subtractStreams: stream_a empty - aborting subtraction.')
            return stream_a

    loggerstream.info('subtractStreams: Start subtracting streams.')

    headera = stream_a.header
    headerb = stream_b.header


    # non-destructive
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
        print "subtractStreams: stream_a and stream_b are apparently not overlapping - returning stream_a"
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
        print "subtractStreams: stream_a and stream_b are apparently not overlapping - returning stream_a"
        return stream_a

    if ndtype:
        timea = sa.ndarray[0]
        timea = timea.astype(float)
    else:
        timea = sa._get_column('time')

    # testing overlapp
    if not len(sb) > 0:
        print "subtractStreams: stream_a and stream_b are not overlapping - returning stream_a"
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
            print "Running ndtype subtraction"
            # Assuming similar time steps
            #t1s = datetime.utcnow()
            # Get indicies of stream_b of which times are present in stream_a
            array = [[] for key in KEYLIST]
            try: # TODO Find a better solution here!
                # The try clause is not correct as searchsorted just finds
                # positions independet of agreement (works well if data is similar)
                idxB = np.argsort(timeb)
                sortedB = timeb[idxB]
                idxA = np.searchsorted(sortedB, timea)
                #print timea, timeb,len(idxA), len(idxB)
                indtib = idxB[idxA]
            except:
                indtib = np.nonzero(np.in1d(timeb, timea))[0]
            #print timeb[pos]
            #print timea
            #print indtib
            # If equal elements occur in time columns
            if len(indtib) > int(0.5*len(timeb)):
                print "Found identical timesteps - using simple subtraction"
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
                #print len(timea),len(timeb),idxA,idxB, indtia, indtib
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
                                    print ind, keyind, len(indtia), len(sa.ndarray[keyind])
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
                print "Did not find identical timesteps - linearily interpolating stream b"
                print "- please note... this needs considerably longer"
                print "- put in the larger (higher resolution) stream as stream_a"
                print "- otherwise you might wait endless"
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
        loggerstream.error('subtractStreams: Streams are not overlapping!')
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

        loggerstream.info('subtractStreams (newway): Time range from %s to %s' % (num2date(stime).replace(tzinfo=None),num2date(etime).replace(tzinfo=None)))

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
            print "Running for LineStruct"
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

    loggerstream.info('subtractStreams: Time range from %s to %s' % (num2date(stime).replace(tzinfo=None),num2date(etime).replace(tzinfo=None)))

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
                        loggerstream.error("subtractStreams: Column key %s not valid!" % key)
                    fkey = 'f'+key
                    try:
                        if fkey in function[0] and not isnan(eval('stream_b[itmp].' + key)):
                            newval = function[0][fkey](functime)
                            exec('elem.'+key+' -= float(newval)')
                        else:
                            setattr(elem, key, float(NaN))
                            #exec('elem.'+key+' = float(NaN)')
                    except:
                        loggerstream.warning("subtractStreams: Check why exception was thrown.")
                        setattr(elem, key, float(NaN))
                        #exec('elem.'+key+' = float(NaN)')
            else:
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        loggerstream.error("subtractStreams: Column key %s not valid!" % key)
                    fkey = 'f'+key
                    if fkey in function[0]:
                        setattr(elem, key, float(NaN))
                        #exec('elem.'+key+' = float(NaN)')
        else: # put NaNs in cloumn if no interpolated values in b exist
            for key in keys:
                if not key in KEYLIST[1:16]:
                    loggerstream.error("subtractStreams: Column key %s not valid!" % key)
                fkey = 'f'+key
                if fkey in function[0]:
                    setattr(elem, key, float(NaN))
                    #exec('elem.'+key+' = float(NaN)')

    try:
        headera['SensorID'] = headera['SensorID']+'-'+headerb['SensorID']
    except:
        pass
    loggerstream.info('subtractStreams: Stream-subtraction finished.')

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
        print "stackStream: provide a list of streams to be stacked"
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
        print "1", timea
        timea = np.asarray([elem-numday for elem in timea])
        print "2", timea
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
        loggingstream.error('CompareStreams: Cannot compare streams with different sampling rates!')
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
        loggerstream.error('compareStreams: Streams do not overlap!')
        return stream_a

    # Trim to overlapping areas:
    stream_a = stream_a.trim(starttime=num2date(stime).replace(tzinfo=None),
                                endtime=num2date(etime).replace(tzinfo=None))
    stream_b = stream_b.trim(starttime=num2date(stime).replace(tzinfo=None),
                                endtime=num2date(etime).replace(tzinfo=None))


    loggerstream.info('compareStreams: Starting comparison...')

    # Compare value for value between the streams:

    flag_len = False

    t_a = stream_a._get_column('time')
    t_b = stream_b._get_column('time')

    # Check length:
    if len(t_a) < len(t_b):
        loggerstream.debug("compareStreams: Missing data in main stream.")
        flag_len = True

    # If the lengths are the same, compare single values for differences:
    if not flag_len:
        for i in range(len(t_a)):
            for key in FLAGKEYLIST:
                exec('val_a = stream_a[i].'+key)
                exec('val_b = stream_b[i].'+key)
                if not isnan(val_a):
                    if val_a != val_b:
                        loggerstream.debug("compareStreams: Data points do not match: %s and %s at time %s." % (val_a, val_b, stream_a[i].time))
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
                            loggerstream.debug("compareStreams: Data points do not match: %s and %s at time %s." % (val_a, val_b, stream_a[i].time))
                            if replace == True:
                                exec('stream_a[i].'+key+' = stream_b[i].'+key)
            else:       # insert row into stream_a
                loggerstream.debug("compareStreams: Line from secondary stream missing in main stream. Timestamp: %s." % stream_b[i].time)
                if insert == True:
                    row = LineStruct()
                    stream_a.add(row)
                    for key in KEYLIST:
                        temp = stream_a._get_column(key)
                        if len(temp) > 0:
                            for j in range(i+1,len(stream_a)):
                                exec('stream_a[j].'+key+' = temp[j-1]')
                            exec('stream_a[i].'+key+' = stream_b[i].'+key)

    loggerstream.info('compareStreams: Finished comparison!')
    return stream_a


# Some helpful methods
def array2stream(listofarrays, keystring,starttime=None,sr=None):
        """
        DESCRIPTION:
            Converts an array to a data stream
        """
        keys = keystring.split(',')
        if not len(listofarrays) > 0:
            print "Specify a list of array - aborting"
            return
        if not len(keys) == len(listofarrays):
            print "Keys do not match provided arrays - aborting"
            return
        st = DataStream()
        if not 'time' in keys:
            if not starttime:
                print "No timing information provided - aborting"
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


def extractDateFromString(datestring):
    """
    Function to extract dates from a string (e.g. a filename within a path)
    Requires a string
    returns a datetime object)
    """
    date = False
    # get day from filename (platform independent)
    try:
        splitpath = os.path.split(datestring)
        daystring = splitpath[1].split('.')[0]
    except:
        daystring = datestring

    try:
        date = datetime.strptime(daystring[-7:], '%b%d%y')
        dateform = '%b%d%y'
        # log ('Found Dateformat of type dateform
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
            if len(numberstr) > 4:
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
            except:
                # log ('dateformat in filename could not be identified')
                pass
        elif len(tmpdaystring) == 4:
            try:
                dateform = '%Y'
                date = datetime.strptime(tmpdaystring,dateform)
            except:
                # log ('dateformat in filename could not be identified')
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
        return datetime.date(date)
    except:
        return date


def denormalize(column, startvalue, endvalue):
    """
    converts [0:1] back with given start and endvalue
    """
    normcol = []
    if startvalue>0:
        if endvalue < startvalue:
            raise ValueError, "start and endval must be given, endval must be larger"
        else:
            for elem in column:
                normcol.append((elem*(endvalue-startvalue)) + startvalue)
    else:
        raise ValueError, "start and endval must be given as absolute times"

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
                loggerstream.warning("NAN warning: only nan in column")
                return []
    except:
        numdat = False
        #loggerstream.warning("Here: NAN warning: only nan in column")
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


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Now import the child classes with formats etc
# Otherwise DataStream etc will not be known
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
from lib.magpy_formats import *



if __name__ == '__main__':

    import subprocess

    print
    print "----------------------------------------------------------"
    print "TESTING: STREAM PACKAGE"
    print "THIS IS A TEST RUN OF THE MAGPY STREAM PACKAGE."
    print "All main methods will be tested. This may take a while."
    print "A summary will be presented at the end. Any protocols"
    print "or functions with errors will be listed."
    print "----------------------------------------------------------"
    print

    print "Please enter path of a (variometer) data file for testing:"
    print "(e.g. /srv/archive/WIC/LEMI025/LEMI025_2014-05-07.bin)"
    while True:
        filepath = raw_input("> ")
        if os.path.exists(filepath):
            break
        else:
            print "Sorry, that file doesn't exist. Try again."

    now = datetime.utcnow()
    testrun = 'streamtest_'+datetime.strftime(now,'%Y%m%d-%H%M')
    t_start_test = time.time()
    errors = {}
    print
    print datetime.utcnow(), "- Starting stream package test. This run: %s." % testrun

    while True:

        # Step 1 - Read data
        try:
            teststream = read(filepath)
            print datetime.utcnow(), "- Stream read in."
        except Exception as excep:
            errors['read'] = str(excep)
            print datetime.utcnow(), "--- ERROR reading stream. Aborting test."
            break

        # Step 2 - Rotate data (why not?)
        try:
            teststream.rotation(alpha=1.0)
            print datetime.utcnow(), "- Rotated."
        except Exception as excep:
            errors['rotation'] = str(excep)
            print datetime.utcnow(), "--- ERROR rotating stream."

        # Step 3 - Offset data
        try:
            test_offset = {'x': 150, 'y': -2000, 'z': 3.2}
            teststream.offset(test_offset)
            print datetime.utcnow(), "- Offset."
        except Exception as excep:
            errors['offset'] = str(excep)
            print datetime.utcnow(), "--- ERROR offsetting stream."

        # Step 4 - Find outliers
        try:
            teststream.remove_outlier()
            print datetime.utcnow(), "- Flagged outliers."
        except Exception as excep:
            errors['remove_outlier'] = str(excep)
            print datetime.utcnow(), "--- ERROR flagging outliers."

        # Step 5 - Remove flagged
        try:
            teststream.remove_flagged()
            print datetime.utcnow(), "- Removed flagged outliers."
        except Exception as excep:
            errors['remove_flagged'] = str(excep)
            print datetime.utcnow(), "--- ERROR removing flagged outliers."

        # Step 6 - Filter
        try:
            teststream.filter()
            print datetime.utcnow(), "- Filtered."
        except Exception as excep:
            errors['filter'] = str(excep)
            print datetime.utcnow(), "--- ERROR filtering."

        # Step 7 - Write
        try:
            teststream.write('.',
                        filenamebegins='%s_' % testrun,
                        filenameends='.min',
                        dateformat='%Y-%m-%d',
                        format_type='IAGA')
            print datetime.utcnow(), "- Data written out to file."
        except Exception as excep:
            errors['write'] = str(excep)
            print datetime.utcnow(), "--- ERROR writing data to file."

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
    print datetime.utcnow(), "- Stream testing completed in %s s. Results below." % time_taken

    print
    print "----------------------------------------------------------"
    if errors == {}:
        print "0 errors! Great! :)"
    else:
        print len(errors), "errors were found in the following functions:"
        print str(errors.keys())
        print
        print "Would you like to print the exceptions thrown?"
        excep_answer = raw_input("(Y/n) > ")
        if excep_answer.lower() == 'y':
            i = 0
            for item in errors:
                print errors.keys()[i] + " error string:"
                print "    " + errors[errors.keys()[i]]
                i += 1
    print
    print "Hit enter to delete temporary files. (Or type N to keep.)"
    tempfile_answer = raw_input("> ")
    if tempfile_answer.lower() != 'n':
        del_test_files = 'rm %s*' % testrun
        subprocess.call(del_test_files,shell=True)
    print
    print "Good-bye!"
    print "----------------------------------------------------------"










































# That's all, folks!

