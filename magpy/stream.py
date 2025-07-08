#!/usr/bin/env python
"""
MagPy-General: Standard pymag package containing the following classes:
Written by Roman Leonhardt, Rachel Bailey 2011/2012/2013/2014
Written by Roman Leonhardt, Rachel Bailey, Mojca Miklavec 2015/2016
Version 0.3 (starting May 2016)
Version 2.0 (starting July 2024)
"""
# TODO - remove the following two line as soon as new packages are updated (only required for testruns (python stream.py)
import sys

import scipy.interpolate

sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
import logging
import os
import tempfile

# ----------------------------------------------------------------------------
# Part 1: Import routines for packages
# ----------------------------------------------------------------------------

logpygen = ''           # temporary logger variable
badimports = []         # List of missing packages

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
            for count in range(1,100):
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

# import default methods
from magpy.core.methods import *

# verified packages of magpy main
import copyreg as copyreg
import types
from tempfile import NamedTemporaryFile
import shutil
from glob import glob, iglob, has_magic
from urllib.request import urlopen, ProxyHandler, install_opener, build_opener
from scipy import signal, fftpack, interpolate, integrate
from scipy.spatial.transform import Rotation
import math

# not yet verified
#import numpy as np   # methods
#import copy # used only in core.activity for deepcopy of header  # methods
#import dateutil.parser as dparser   # methods
#import pickle
#import struct
#import re   # methods
#import time
#import string
#import fnmatch
#import warnings # methods
#from itertools import groupby
#from operator import itemgetter
#import operator  # used for stereoplot legend
#from urllib.parse import urlparse, urlencode
#from urllib.error import HTTPError
#import _thread
#from io import StringIO
#from urllib.request import Request
#import ssl
#ssl._create_default_https_context = ssl._create_unverified_context
#import scipy as sp
#from scipy import interpolate
#from scipy.interpolate import UnivariateSpline
#from scipy import stats
#from scipy import signal
#from scipy.ndimage import filters
#import scipy.optimize as op
#pyvers = 3
#PLATFORM = sys.platform

from pylab import *
from datetime import datetime
from datetime import timedelta


# NetCDF  # move to respective library
# ------
try:
    from netCDF4 import Dataset
except ImportError as e:
    pass

### Some Python3/2 compatibility code
### taken from http://www.rfk.id.au/blog/entry/preparing-pyenchant-for-python-3/
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
    func = None
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

"""
KEYLIST = ['time',  # Timestamp (date2num object)
                'x',  # X or I component of magnetic field (float)
                'y',  # Y or D component of magnetic field (float)
                'z',  # Z component of magnetic field (float)
                'f',  # Magnetic field strength (float)
                't1',  # Temperature variable (e.g. ambient temp) (float)
                't2',  # Secondary temperature variable (e.g. sensor temp) (float)
                'var1',  # Extra variable #1 (float)
                'var2',  # Extra variable #2 (float)
                'var3',  # Extra variable #3 (float)
                'var4',  # Extra variable #4 (float)
                'var5',  # Extra variable #5 (float)
                'dx',  # Errors in X (float)
                'dy',  # Errors in Y (float)
                'dz',  # Errors in Z (float)
                'df',  # Errors in F (float)
                'str1',  # Extra string variable #1 (str)
                'str2',  # Extra string variable #2 (str)
                'str3',  # Extra string variable #3 (str)
                'str4',  # Extra string variable #4 (str)
                'flag',  # Variable for flags. (str='0000000000000000-')
                'comment',  # Space for comments on flags (str)
                'typ',  # Type of data (str='xyzf')
                'sectime'  # Secondary time variable (date2num)
                ]
NUMKEYLIST = KEYLIST[1:16]
"""

# Empty key values at initiation of stream:
KEYINITDICT = {'time':0,'x':np.nan,'y':np.nan,'z':np.nan,'f':np.nan,
                't1':np.nan,'t2':np.nan,'var1':np.nan,'var2':np.nan,
                'var3':np.nan,'var4':np.nan,'var5':np.nan,'dx':np.nan,
                'dy':np.nan,'dz':np.nan,'df':np.nan,'str1':'-','str2':'-',
                'str3':'-','str4':'-','flag':'0000000000000000-','comment':'-','typ':'xyzf',
                'sectime':np.nan}

# Formats supported by MagPy read function:
SUPPORTED_FORMATS = {
                'IAGA':['rw','IAGA 2002 text format'],
                'WDC':['rw','World Data Centre format'],
                'IMF':['rw', 'Intermagnet Format'],
                'IAF':['rw', 'Intermagnet archive Format'],
                'BLV':['rw','IBFV2.0 Baseline format Intermagnet'],
                'BLV1_2':['rw','IBFV1.2 Baseline format Intermagnet'],
                'IYFV':['rw','Yearly mean format Intermagnet'],
                'DKA':['rw', 'K value format Intermagnet'],
                'DIDD':['rw','Output format from MinGeo DIDD'],
                'GSM19':['r', 'Output format from GSM19 magnetometer'],
                'GFZINDEXJSON':['r', 'JSON structure for indicies (i.e. Kp) at GFZ webservice'],
                'COVJSON':['rw', 'Coverage JSON'],
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
                'TSF':['r', 'iGrav and GWR TSF data - new'],
                'CR800':['r', 'CR800 datalogger'],
                'IONO':['r', 'IM806 Ionometer'],
                'RADON':['r', 'single channel analyser gamma data'],
                'USBLOG':['r', 'USB temperature logger'],
                #'SERSIN':['r', '?'],
                #'SERMUL':['r', '?'],
                'PYSTR':['rw', 'MagPy full ascii'],
                'AUTODIFABS':['r', 'Autodif format for Absolute measurents'],
                'AUTODIF':['r', 'Deprecated - AutoDIF debug data for MK2 version'],
                'AUTODIF_FREAD':['r', 'Deprecated - Special format for AutoDIF read-in'],
                'PYBIN':['r', 'MagPy own binary format'],
                'PYASCII':['rw', 'MagPy basic ASCII'],
                'POS1TXT':['r', 'POS-1 text format output data'],
                'POS1':['r', 'POS-1 binary output at WIC'],
                'PMB':['r', 'POS pmb file'],
                'QSPIN':['r', 'QSPIN ascii output'],
                #'PYNC':['r', 'MagPy NetCDF variant (too be developed)'],
                #'DTU1':['r', 'ASCII Data from the DTUs FGE systems'],
                #'BDV1':['r', 'Budkov GDAS data variant'],
                'GFZTMP':['r', 'GeoForschungsZentrum ascii format'],
                'GFZKP':['r', 'GeoForschungsZentrum KP-Index format'],
                'PREDSTORM':['r','PREDSTORM space weather prediction data format'],
                'CSV':['rw','comma-separated CSV data'],
                'IMAGCDF':['rw','Intermagnet CDF Format'],
                'GFZCDF':['r', 'GFZ CDF variant'],
                'PYCDF':['rw', 'MagPy CDF variant'],
                'NOAAACE':['r', 'NOAA ACE satellite data format'],
                'DSCOVR':['r', 'NOAA JSON format for DSCOVR data'],
                'XRAY':['r', 'GOES XRAY JSON format'],
                'NETCDF':['r', 'NetCDF4 format, NOAA DSCOVR satellite data archive format'],
                'LATEX':['w','LateX data'],
                #'SFDMI':['r', 'San Fernando variometer'],
                #'SFGSM':['r', 'San Fernando GSM90'],
                'UNKOWN':['-','Unknown']
                        }

# ----------------------------------------------------------------------------
#  Part 3: Example files for easy access and tests
# ----------------------------------------------------------------------------

from pkg_resources import resource_filename
example1 = resource_filename('magpy', 'examples/example1.zip')  #Zip compressed IAGA02
example2 = resource_filename('magpy', 'examples/example2.cdf')  #MagPy CDF with F
example3 = resource_filename('magpy', 'examples/example3.txt')  #PyStr Baseline
example4 = resource_filename('magpy', 'examples/example4.cdf')  #MagPy CDF
example5 = resource_filename('magpy', 'examples/example5.sec')  #Imag CDF
example6a = resource_filename('magpy', 'examples/example6a.txt')  #DI file
example6b = resource_filename('magpy', 'examples/example6b.txt')  #DI file
example7 = resource_filename('magpy', 'examples/example7.blv')  #IBFV2.0 file

# ----------------------------------------------------------------------------
#  Part 4: Main classes -- DataStream, LineStruct and
#      PyMagLog (To be removed)
# ----------------------------------------------------------------------------

class DataStream(object):
    """
    Creates a combination of an array and a dictionary. This object
    is then used for various methods. The array columns
    are organized by a predifined KEYLIST helping to
    asign vector, scalar and descriptive data.

    keys are column identifier:
    key in keys: see KEYLIST

    A note on headers:
    ALWAYS INITIATE STREAM WITH >>> stream = DataStream(ndarray=np.array([]),header={}).

    All available methods of the DataStream class and there test state:
    runtime test : Test should clarify the the method is running with all options and doe snot cause
                   any breakdown or unwanted effect on any other routine.
                   These test are performed by the package itself:
                   Run i.e. python3 stream.py
                   !! Obligatory test for all new methods at minor version level i.e. 5.1.x
    verification test : Hereby the internal correctness of the methods is checked usually by unittest.
                        Unittests are contained in magpy.test.verification
                        Run using: python3 verification.py
                   !! Obligatory test for all new methods at major version level i.e. 5.x
    application test : Not shown in the following. Real application with true data and in combination with
                       typical method application. Test listed in separate routine as part of magpy/test

    yes* indicates that this method is tested as part of another method
    ----------------------------

| class                |  method  |  since version  |  until version  |  runtime test  |  result verification  | manual  |  *tested by |
| ----------------------|  ------  |  -------------  |  -------------  |  ------------  |  ------------------  |---------|  ---------- |
|  **stream**           |             |         |                 |                |                  |         | |
|  DataStream           |  _aic       |  2.0.0  |                 |  yes*          |  yes*            | -       |  core.activity |
|  DataStream           |  _convertstream  |  2.0.0  |            |  yes           |  yes             | 5.2     | |
|  DataStream           |  _copy_column  |  2.0.0  |              |  yes           |  yes             | 5.1     | |
|  DataStream           |  _det_trange  |  2.0.0  |               |  yes*          |  yes             | -       |  filter |
|  DataStream           |  _drop_column  |  2.0.0  |              |  yes           |  yes             | 5.1     | |
|  DataStream           |  _find_t_limits  |  2.0.0  |  2.1.0     |  yes           |  -               | -       | |
|  DataStream           |  _get_column  |  2.0.0  |               |  yes           |  yes             | 5.1     | |
|  DataStream           |  _get_key_headers  |  2.0.0  |          |  yes           |  yes             | 5.1     | |
|  DataStream           |  _get_key_names  |  2.0.0  |            |  yes           |  yes             | 5.1     | |
|  DataStream           |  _get_max  |   2.0.0  |                 |  yes           |  yes             | 5.5     | |
|  DataStream           |  _get_min  |  2.0.0  |                  |  yes           |  yes             | 5.5     | |
|  DataStream           |  _get_variance  |  2.0.0  |             |  yes           |  yes             | 5.5     | |
|  DataStream           |  _move_column  |  2.0.0  |              |  yes           |  yes             | 5.1     | |
|  DataStream           |  _print_key_headers  |  2.0.0  |        |  yes           |  -               | 5.1     | |
|  DataStream           |  _put_column  |  2.0.0  |               |  yes           |  yes             | 5.1     | |
|  DataStream           |  _remove_nancolumns  |  2.0.0  |        |  yes*          |  yes             | 5.1     |  subtract_streams |
|  DataStream           |  _select_keys  |  2.0.0  |              |  yes           |  yes             | 5.1     | |
|  DataStream           |  _select_timerange  |  2.0.0  |         |  yes*          |  yes             | 5.1     |  write |
|  DataStream           |  _tau  |       2.0.0  |                 |  yes*          |  yes             | -       |  filter |
|  DataStream           |  add  |        2.0.0  |                 |  yes*          |  yes*            | -       |  absolutes |
|  DataStream           |  apply_deltas  |  2.0.0  |              |  yes           |  yes*            |         |  methods.data_for_di |
|  DataStream           |  aic_calc   |  2.0.0  |                 |  yes           |  yes*            | 8.2     | core.activity |
|  DataStream           |  amplitude  |  2.0.0  |                 |  yes           |  yes             | 5.5     | |
|  DataStream           |  baseline  |   2.0.0  |                 |  yes           |  yes             | 5.9,7.5 | |
|  DataStream           |  bc  |         2.0.0  |                 |  yes           |  yes             | 7.5     | |
|  DataStream           |  calc_f  |     2.0.0  |                 |  yes           |  yes             | 5.4     | |
|  DataStream           |  compensation  |  2.0.0  |              |  yes           |  yes             | 5.1     | |
|  DataStream           |  contents   |  2.0.0  |                 |  yes           |  yes             | 4.4     | |
|  DataStream           |  cut  |        2.0.0  |                 |  yes           |  yes             | 5.1     | |
|  DataStream           |  dailymeans  |  2.0.0  |                |  yes           |  yes             | 5.3     | |
|  DataStream           |  delta_f  |    2.0.0  |                 |  yes           |  yes             | 5.4     | |
|  DataStream           |  derivative   |  2.0.0  |               |  yes           |  yes             | 5.7     | |
|  DataStream           |  determine_rotationangles |  2.0.0  |    |  yes         |  yes             | 5.2     | |
|  DataStream           |  dict2stream  |  2.0.0  |               |  yes*          |  yes*            | -       |  baseline |
|  DataStream           |  dropempty  |  2.0.0  |                 |  yes*          |  yes*            | -       |  sorting |
|  DataStream           |  dwt_calc  |   2.0.0  |                 |  yes*          |  yes*            | 8.2     |  core.activity |
|  DataStream           |  end  |        2.0.0  |                 |  yes           |  yes             | 5.1     | |
|  DataStream           |  extend  |     2.0.0  |                 |  yes*          |  yes             | 5.10    |  read |
|  DataStream           |  extract  |    2.0.0  |                 |  yes           |  yes             | 5.1     | |
|  DataStream           |  extract_headerlist |    2.0.0  |       |  yes           |  -               | 8.2     |  core.activity |
|  DataStream           |  extrapolate  |  2.0.0  |               |  yes           |  yes             | 5.8     | |
|  DataStream           |  filter  |     2.0.0  |                 |  yes           |  yes             | 5.3     | |
|  DataStream           |  fillempty  |  2.0.0  |                 |  yes*          |  yes*            | -       |  sorting |
|  DataStream           |  findtime  |   2.0.0  |                 |  yes*          |  yes             | 5.1     |  resample |
|  DataStream           |  fit  |        2.0.0  |                 |  yes           |  yes             | 5.9     | |
|  DataStream           |  func2header  |  2.0.0  |               |  yes           |  yes             | 5.9     | |
|  DataStream           |  func2stream  |  2.0.0  |               |  yes           |  yes             | 5.9     | |
|  DataStream           |  get_fmi_array  |  2.0.0  |             |  yes*          |  yes*            |         |  core.activity |
|  DataStream           |  get_gaps  |   2.0.0  |                 |  yes           |  yes             | 5.3     | |
|  DataStream           |  get_key_name  |  2.0.0  |              |  yes           |  yes             | 5.1     | |
|  DataStream           |  get_key_unit  |  2.0.0  |              |  yes           |  yes             | 5.1     | |
|  DataStream           |  get_sampling_period |  2.0.0  |       |  yes*          |  yes             | -       |  samplingrate |
|  DataStream           |  harmfit  |    2.0.0  |                 |  yes*          |  yes             | -       |  fit |
|  DataStream           |  hdz2xyz  |    2.0.0  |                 |  yes*          |  yes*            | 5.2     |  _convertstream |
|  DataStream           |  idf2xyz  |    2.0.0  |                 |  yes*          |  yes*            | 5.2     |  _convertstream |
|  DataStream           |  integrate  |  2.0.0  |                 |  yes           |  no              | 5.7     | |
|  DataStream           |  interpol  |   2.0.0  |                 |  yes           |  yes             | 5.9     | |
|  DataStream           |  interpolate_nans  |  2.0.0  |          |  yes           |  yes             | 5.3,5.9 | |
|  DataStream           |  length  |     2.0.0  |                 |  yes*          |  yes             | 5.1     | |
|  DataStream           |  mean  |       2.0.0  |                 |  yes           |  yes             | 5.5     | |
|  DataStream           |  modwt_calc  |  2.0.0  |                |  yes*          |  yes*            | -       |  core.activity |
|  DataStream           |  multiply  |   2.0.0  |                 |  yes           |  yes             | 5.6     | |
|  DataStream           |  offset  |     2.0.0  |                 |  yes           |  yes             | 5.6     | |
|  DataStream           |  randomdrop  |  2.0.0  |                |  yes           |  yes             | 5.1     | |
|  DataStream           |  remove  |     2.0.0  |                 |  yes           |  yes             | 5.1     | |
|  DataStream           |  resample  |   2.0.0  |                 |  yes*          |  yes             | 5.3     |  filter |
|  DataStream           |  rotation  |   2.0.0  |                 |  yes           |  yes             | 5.2     | |
|  DataStream           |  samplingrate  |  2.0.0  |              |  yes           |  yes             | 5.1     | |
|  DataStream           |  simplebasevalue2stream |  2.0.0  |    |  yes*          |  yes*            | !       | test_absolute_analysis |
|  DataStream           |  smooth  |     2.0.0  |                 |  yes           |  yes             | 5.3     | |
|  DataStream           |  sorting  |    2.0.0  |                 |  yes*          |  no              | 5.1     |  read |
|  DataStream           |  start  |      2.0.0  |                 |  yes           |  yes             | 5.1     | |
|  DataStream           |  stats  |      2.0.0  |                 |  yes           |  yes             | 5.1     | |
|  DataStream           |  steadyrise  |  2.0.0  |                |  yes           |  not yet         |         | |
|  DataStream           |  stream2dict  |  2.0.0  |               |  yes*          |  yes*            | -       |  baseline |
|  DataStream           |  timerange  |  2.0.0  |                 |  yes           |  yes             | 5.1     | |
|  DataStream           |  trim  |       2.0.0  |                 |  yes           |  yes             | 5.1     | |
|  DataStream           |  union      |  2.0.0  |                 |  yes           |  yes             | -       | flagging |
|  DataStream           |  unique     |  2.0.0  |                 |  yes           |  yes             | 4.4     | |
|  DataStream           |  use_sectime  |  2.0.0  |               |  yes           |  yes             | 5.1     | |
|  DataStream           |  variables  |  2.0.0  |                 |  yes           |  yes             | 5.1     | |
|  DataStream           |  write  |      2.0.0  |                 |  yes           |  yes*            | 3.x     | in runtime |
|  DataStream           |  xyz2hdz  |    2.0.0  |                 |  yes*          |  yes*            | 5.2     |  _convertstream |
|  DataStream           |  xyz2idf  |    2.0.0  |                 |  yes*          |  yes*            | 5.2     |  _convertstream |
|     | determine_time_shift |  2.0.0  |              |  yes           |  yes*            | 5.10    | validity in runtime |
|     | join_streams         |       2.0.0  |                 |  yes           |  yes*            | 5.10    | validity in runtime |
|     | merge_streams        |      2.0.0  |                 |  yes           |  yes*            | 5.10    | validity in runtime |
|     | subtract_streams     |   2.0.0  |                 |  yes           |  yes*            | 5.10    | validity in runtime |
|     | append_streams       |     2.0.0  |                 |  ...           |  yes*            | 5.10    | validity in runtime |



deprecated:
    - stream._find_t_limits()
    - stream.flag_range()   -> moved to core.flagging
    - stream.flag_outlier(self, **kwargs)   -> moved to core.flagging
    - stream.remove_flagged(self, **kwargs)  -> core.flagging.apply_flags
    - stream.flag()  -> core.flagging.apply_flags
    - stream.bindetector(self,key,text=None,**kwargs):
    - stream.stream2flaglist(self, userange=True, flagnumber=None, keystoflag=None, sensorid=None, comment=None)


removed:
    - stream.extractflags()  -> moved to extract_flags in flagging and used by PYCDF read
    - stream.flagfast()      -> not useful any more - used for previous flagging plots outside xmagpy
    - stream.flaglistadd()   -> core.flagging add


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
        alldata = mergeStreams(pos_stream, lemi_stream, keys=['x','y','z'])

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

    def __init__(self, container=None, header=None,ndarray=None):

        self.KEYLIST = [ 'time',         # Timestamp (date2num object)
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
        self.NUMKEYLIST = self.KEYLIST[1:16]
        self.FLAGKEYLIST = self.KEYLIST[:16]

        # KEYLISTS for headers
        self.DATAINFOKEYLIST = ['DataID', 'SensorID', 'StationID', 'ColumnContents', 'ColumnUnits', 'DataFormat',
                                'DataMinTime', 'DataMaxTime', 'DataTimezone',
                                'DataSamplingFilter', 'DataDigitalSampling', 'DataComponents', 'DataSamplingRate',
                                'DataType',
                                'DataDeltaReferencePier', 'DataDeltaReferenceEpoch', 'DataScaleX',
                                'DataScaleY', 'DataScaleZ', 'DataScaleUsed', 'DataCompensationX',
                                'DataCompensationY', 'DataCompensationZ', 'DataSensorOrientation',
                                'DataSensorAzimuth', 'DataSensorTilt', 'DataAngularUnit', 'DataPier',
                                'DataAcquisitionLatitude', 'DataAcquisitionLongitude', 'DataLocationReference',
                                'DataElevation', 'DataElevationRef', 'DataFlagModification', 'DataAbsFunc',
                                'DataAbsDegree', 'DataAbsKnots', 'DataAbsMinTime', 'DataAbsMaxTime', 'DataAbsDate',
                                'DataRating', 'DataComments', 'DataSource', 'DataAbsFunctionObject',
                                'DataDeltaValues', 'DataDeltaValuesApplied', 'DataTerms', 'DataReferences',
                                'DataPublicationLevel', 'DataPublicationDate', 'DataStandardLevel',
                                'DataStandardName', 'DataStandardVersion', 'DataPartialStandDesc', 'DataRotationAlpha',
                                'DataRotationBeta', 'DataAbsInfo', 'DataBaseValues', 'DataArchive']

        self.SENSORSKEYLIST = ['SensorID', 'SensorName', 'SensorType', 'SensorSerialNum', 'SensorGroup',
                               'SensorDataLogger',
                               'SensorDataLoggerSerNum', 'SensorDataLoggerRevision', 'SensorDataLoggerRevisionComment',
                               'SensorDescription', 'SensorElements', 'SensorKeys', 'SensorModule', 'SensorDate',
                               'SensorRevision', 'SensorRevisionComment', 'SensorRevisionDate', 'SensorDynamicRange',
                               'SensorTimestepAccuracy', 'SensorGroupDelay', 'SensorPassband', 'SensorAttenuation',
                               'SensorRMSNoise', 'SensorSpectralNoise', 'SensorAbsoluteError', 'SensorOrthogonality',
                               'SensorVerticality', 'SensorTCoeff', 'SensorElectronicsTCoeff', 'SensorAnalogSampling',
                               'SensorResolution', 'SensorTime', 'SensorPicture', 'SensorManual']

        self.STATIONSKEYLIST = ['StationID', 'StationName', 'StationIAGAcode', 'StationInstitution', 'StationStreet',
                                'StationCity', 'StationPostalCode', 'StationCountry', 'StationWebInfo',
                                'StationEmail', 'StationDescription', 'StationK9', 'StationMeans', 'StationLongitude',
                                'StationLatitude', 'StationLocationReference', 'StationElevation',
                                'StationElevationRef', 'StationType', 'StationSince', 'StationUntil', 'StationPicture']


        self.header = header if header else {}
        self.container = container if container else []
        if ndarray is None:
            ndarray = np.array([np.asarray([]) for elem in self.KEYLIST])
        self.ndarray = ndarray
        self.progress = 0

    # ------------------------------------------------------------------------
    # A. Standard functions and overrides for list like objects
    # ------------------------------------------------------------------------


    def add(self, datlst):
        """
        DESCRIPTION
             Method is used by absoluteAnalysis, which is using the original container (list) structure
        :param datlst:
        :return:
        """
        # used by absolutAnalysis
        #try:
        assert isinstance(self.container, (list, tuple))
        self.container.append(datlst)
        #except:
        #    print list(self.container).append(datlst)


    def apply_deltas(self, debug=False):
        """
        DESCRIPTION:
           Extract content of DataDeltaValues and apply the corrections to stream.
           The header content DataDeltaValues needs to be present
           In order to extract such data from the database you might want to use
           stream.header = db.fields_to_dict(stream.header.get('DataID')).
           The apply_deltas method makes use of the stream.offset method.

        PARAMETER:
           stream:          data stream which is corrected

        RETURNS:
           a data stream with offsets applied

        APPLICATION:
           correctedstream = stream.apply_deltas()
        """

        def _get_old_deltavalues(ddvstring, debug=False):
            deltadict = {}
            deltalines = ddvstring.split(';')
            for idx, delt in enumerate(deltalines):
                if debug:
                    print("apply_deltas: Found Deltavalues {}".format(delt))
                deltdict = {}
                starttime = ''
                endtime = ''
                delts = delt.split(',')
                for el in delts:
                    dat = el.split('_')
                    name = dat[0].strip()
                    value = dat[1].strip()
                    if is_number(value):
                        value = float(value)
                    if name in ['st', 'et']:
                        value = num2date(value).replace(tzinfo=None)
                    deltdict[name] = value
                deltadict[idx] = deltdict
            return deltadict

        def _delta_dict_to_string(ddv):
            def dateconv(d):
                # Converter to serialize datetime objects in json
                if isinstance(d, datetime):
                    return d.__str__()

            return json.dumps(ddv, default=dateconv)

        def _dateparser(dct):
            # Convert dates in dictionary to datetime objects
            for (key, value) in dct.items():
                if str(value).count('-') + str(value).count(':') == 4:
                    try:
                        try:
                            value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
                        except:
                            value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    except:
                        pass
                dct[key] = value
            return dct

        def _get_new_deltavalues(ddvstring):
            return json.loads(ddvstring, object_hook=_dateparser)

        stream = self.copy()
        deltasapplied = False
        streamstart, streamend = stream.timerange()
        deltdict = {}

        deltas = stream.header.get('DataDeltaValues', '')
        if deltas == '':
            print("apply_deltas: No delta values found - returning unmodified stream")
            return stream
        if "_" in deltas:
            # old type
            deltdict = _get_old_deltavalues(deltas)
            print("apply_deltas: found old format of delta values in database - you might want to update them")
            print("              to update copy the following into the database:")
            newstr = _delta_dict_to_string(deltdict)
            print(newstr)
        elif "{" in deltas:
            deltdict = _get_new_deltavalues(deltas)

        for dd in deltdict:
            contdict = deltdict[dd]
            st = contdict.get('st', streamstart)  # is required
            et = contdict.get('et', datetime.now(timezone.utc).replace(tzinfo=None))  # will be set to now if not existing
            for key in contdict:
                key = key.strip()
                value = contdict.get(key)
                if not key in ['st', 'et']:
                    if (streamstart >= st and streamstart < et) or (streamend >= st and streamend < et):
                        if debug:
                            print("apply_deltas: key={}, value={}, starttime={}, endtime={}".format(key, value, st, et))
                        stream = stream.offset({key: value}, starttime=st, endtime=et)
                        deltasapplied = True
        if deltasapplied:
            stream.header['DataDeltaValues'] = ''
            stream.header['DataDeltaValuesApplied'] = True

        return stream


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


    def copy(self):
        """
        DESCRIPTION:
           method for copying content of a stream to a new stream
        APPLICATION:
           for non-destructive methods
        """

        co = DataStream()
        co.container = [LineStruct()]
        newheader = {}
        for el in self.header:
            newheader[el] = self.header[el]
        array = [[] for el in KEYLIST]

        if len(self.ndarray[0])> 0:
            for ind, key in enumerate(KEYLIST):
                #array[ind] = np.asarray([val for val in self.ndarray[ind]])
                array[ind] = np.copy(self.ndarray[ind])
                #print ("New", array[ind])
                #print ("Original", self.ndarray[ind])

        return DataStream(co.container,newheader,np.asarray(array, dtype=object))


    def __str__(self):
        return str(self.ndarray)


    def __repr__(self):
        return str(self.ndarray)


    def __getitem__(self, var):
        if var in self.NUMKEYLIST:
            return self.ndarray[self.KEYLIST.index(var)].astype(np.float64)
        elif var in KEYLIST:
            return self.ndarray[self.KEYLIST.index(var)]


    def __setitem__(self, var, value):
        self.ndarray[self.KEYLIST.index(var)] = value


    def __len__(self):
        return len(self.ndarray[0])


    @deprecated("Clean header is useless - just asign an empty dict to self.header")
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
                        array[idx] = np.append(self.ndarray[idx], ndarray[idx]).astype(object)
                    elif len(self.ndarray[0]) > 0: # only time axis present so far but no data within this elem
                        fill = ['-']
                        key = KEYLIST[idx]
                        if key in self.NUMKEYLIST:
                            fill = [np.nan]
                        nullvals = np.asarray(fill * len(self.ndarray[0]))
                        array[idx] = np.append(nullvals, ndarray[idx]).astype(object)
                    else:
                        array[idx] = ndarray[idx].astype(object)
            self.ndarray = np.asarray(array, dtype=object)


    def union(self,column):
        """
        Used only by flagging routine "extract_flags" to read old flagging structures contained in PYCDF files
        """
        seen = set()
        seen_add = seen.add
        return [ x for x in column if not (x in seen or seen_add(x))]

    def unique(self, key):
        """
        DESCRIPTION:
            Return unique elements for the selected column.
        EXAMPLE:
            print(data.uniques('str1'))
        """
        column = self._get_column(key)
        return self.union(column)

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

        return DataStream(header=self.header, ndarray=np.asarray(array,dtype=object))


    def start(self):
        st,et = self._find_t_limits()
        return st

    def end(self):
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

        st = testtime(time)
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
                indexes = [np.argmax(ticol>=st)]
                if not len(indexes) == 0:
                    if startidx:
                        retindex = indexes[0] + startidx
                    else:
                        retindex = indexes[0]
                    return retindex
                else:
                    return 0
            except:
                logger.warning("findtime: Didn't find selected time - returning 0")
                return 0
        logger.warning("findtime: Didn't find selected time - returning 0")
        return 0


    def _find_t_limits(self):
        # old method  - keep it for a while
        return self.timerange()


    def _print_key_headers(self):
        print("%10s : %22s : %28s" % ("MAGPY KEY", "VARIABLE", "UNIT"))
        for key in self.FLAGKEYLIST[1:]:
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
        data_stream._get_key_headers(limit=1)
        """

        limit = kwargs.get('limit')
        numerical = kwargs.get('numerical')

        if numerical:
            TESTLIST = self.FLAGKEYLIST
        else:
            TESTLIST = KEYLIST

        keylist = []

        if not len(keylist) > 0:  # e.g. Testing ndarray
            for ind,elem in enumerate(self.ndarray): # use the long way
                if len(elem) > 0 and ind < len(TESTLIST):
                    if not TESTLIST[ind] == 'time':
                        keylist.append(TESTLIST[ind])
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
        return np.asarray(newndarray, dtype=object), keylist

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

        newndarray = np.asarray(lst,dtype=object)
        return newndarray

    def sorting(self):
        """
        Sorting data according to time (maybe generalize that to some key)
        """

        if len(self.ndarray[0]) > 0:
            self.ndarray, keylst = self.dropempty()
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

        return DataStream(header=self.header, ndarray=self.ndarray)

    # ------------------------------------------------------------------------
    # B. Internal Methods: Line & column functions
    # ------------------------------------------------------------------------

    def _get_column(self, key):
        """
        Returns a numpy array of selected column from Stream
        Example:
        columnx = datastream._get_column('x')
        """

        if not key in KEYLIST:
            raise ValueError("Column key not valid")

        ind = KEYLIST.index(key)

        if len(self.ndarray[0]) > 0:
            try:
                col = self[key]
            except:
                col = self.ndarray[ind]
            return col

        # Check for initialization value
        try:
            col = np.asarray([row[ind] for row in self])
            return col
        except:
            return np.asarray([])


    def _put_column(self, column, key, columnname=None, columnunit=None):
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
        stream = stream._put_column(res, 't2', columnname='Rain',columnunit='mm in 1h')
        """

        if not key in KEYLIST:
            raise ValueError("Column key not valid")
        if len(self.ndarray[0]) > 0:
            ind = KEYLIST.index(key)
            self.ndarray[ind] = np.asarray(column)
        else:
            return self

        if not columnname:
            columnname = self.header.get('col-{}'.format(key),'')
            if not columnname:
                self.header['col-{}'.format(key)] = key
        else:
            self.header['col-{}'.format(key)] = columnname

        if not columnunit:
            columnunit =  self.header.get('unit-col-{}'.format(key),'')
            if not columnunit:
                self.header['unit-col-{}'.format(key)] = ''
        else:
            self.header['unit-col-{}'.format(key)] = columnunit

        return self


    def _copy_column(self, key, put2key):
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
        data_stream._move_column('f', 'var1')
        '''
        result = self

        if not key in KEYLIST:
            logger.error("_move_column: Column key %s not valid!" % key)
        if key == 'time':
            logger.error("_move_column: Cannot move time column!")
        if not put2key in KEYLIST:
            logger.error("_move_column: Column key %s (to move %s to) is not valid!" % (put2key,key))
        if len(self.ndarray[0]) > 0:
            col = self._get_column(key)
            result = self._put_column(col,put2key)
        return result


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
        data_stream._move_column('f', 'var1')
        '''
        result = self
        if not key in KEYLIST:
            logger.error("_move_column: Column key %s not valid!" % key)
        if key == 'time':
            logger.error("_move_column: Cannot move time column!")
        if not put2key in KEYLIST:
            logger.error("_move_column: Column key %s (to move %s to) is not valid!" % (put2key,key))
        if len(self.ndarray[0]) > 0:
            col = self._get_column(key)
            result = self._put_column(col, put2key, columnname=self.header.get('col-{}'.format(key)), columnunit=self.header.get('unit-col-{}'.format(key)))
            result = result._drop_column(key)
        return result

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
                self.ndarray = np.asarray(array,dtype=object)

            colkey = "col-{}".format(key)
            colunitkey = "unit-col-{}".format(key)
            try:
                self.header.pop(colkey, None)
                self.header.pop(colunitkey, None)
            except:
                print("_drop_column: Error while dropping header info")
        else:
           print("No data available  or LineStruct type (not supported)")

        return self


    def _remove_nancolumns(self):
        """
    DEFINITION:
        Remove any columns solely filled with nan values

    APPLICATION:
        called by plot methods in mpplot

    RETURNS:
        - DataStream:   (DataStream) New stream reduced to below point limit.

        """
        array = [[] for key in self.KEYLIST]
        arraylengths = []
        if len(self.ndarray[0]) > 0:
            for idx, elem in enumerate(self.ndarray):
                if len(self.ndarray[idx]) > 0 and self.KEYLIST[idx] in self.NUMKEYLIST and is_number(self.ndarray[idx][0]):
                    lst = list(self.ndarray[idx])
                    if np.isnan(float(lst[0])) and np.isnan(float(lst[-1])):
                        nanlen = np.count_nonzero(np.isnan(lst))
                        if nanlen == len(lst):
                            array[idx] = np.asarray([])
                        else:
                            array[idx] = self.ndarray[idx]
                    else:
                        array[idx] = self.ndarray[idx]
                elif len(self.ndarray[idx]) > 0:
                    array[idx] = self.ndarray[idx]
                if idx > 0:
                    arraylengths.append(len(array[idx]))
        else:
            pass
        res = DataStream(header=self.header,ndarray=np.asarray(array,dtype=object))
        if not arraylengths or (list(set(arraylengths))[0] == 0 and len(set(arraylengths)) == 1):
            # only empty data columns remaining in stream
            res = DataStream()
        return res


    # ------------------------------------------------------------------------
    # B. Internal Methods: Data manipulation functions
    # ------------------------------------------------------------------------

    def _aic(self, signal, k, debug=None):
        aicval = np.nan
        try:
            var1 = np.var(signal[:k])
            var2 = np.var(signal[k:])
            if var1 != 0:
                log1 = np.log(var1)
            else:
                log1 = 0
            if var2 != 0:
                log2 = np.log(var2)
            else:
                log2 = 0
            aicval = (k-1)* log1+(len(signal)-k-1)*log2
        except:
            if debug:
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
        f_fit = self.harmfit(nt,val, 5)

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
        result = None
        tresult = None

        if len(self.ndarray[0]) > 0:
            if key_ind > 0:
                ar = self.ndarray[key_ind].astype(float)
            else:
                ar = self.ndarray[key_ind]
            result = np.nanmax(ar)
            ind = np.nanargmax(ar)
            tresult = self.ndarray[t_ind][ind]

        if returntime:
            return result, tresult
        else:
            return result


    def _get_min(self, key, returntime=False):
        if not key in KEYLIST[:16]:
            raise ValueError("Column key not valid")
        key_ind = KEYLIST.index(key)
        t_ind = KEYLIST.index('time')
        result = None
        tresult = None

        if len(self.ndarray[0]) > 0:
            if key_ind > 0:
                ar = self.ndarray[key_ind].astype(float)
            else:
                ar = self.ndarray[key_ind]
            result = np.nanmin(ar)
            ind = np.nanargmin(ar)
            tresult = self.ndarray[t_ind][ind]

        if returntime:
            return result, tresult
        else:
            return result

    def _get_variance(self, key):
        if not key in self.KEYLIST[:16]:
            raise ValueError("Column key not valid")
        key_ind = self.KEYLIST.index(key)
        if len(self.ndarray[0]) > 0:
            result = np.nanvar(self.ndarray[key_ind].astype(float))
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
        if not key in self.KEYLIST[1:16]:
            raise ValueError("Column key not valid")
        ts = self._get_column(key).astype(float)
        ts = ts[~np.isnan(ts)]
        maxts = np.max(ts)
        mints = np.min(ts)
        return maxts-mints

    @deprecated("Unused - replaced by new K implementation")
    def _gf(self, t, tau):
        """
        Gauss function
        """
        return np.exp(-((t/tau)*(t/tau))/2)


    @deprecated("Unused - replaced by new K implementation")
    def _hf(self, p, x):
        """
        Harmonic function
        """
        hf = p[0]*cos(2*pi/p[1]*x+p[2]) + p[3]*x + p[4] # Target function
        return hf


    @deprecated("Unused - replaced by new K implementation")
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
        result = self
        ext = ''
        if len(self.ndarray[4]) > 0:
            ext = 'F'
        if len(self.ndarray[self.KEYLIST.index('df')]) > 0:
            ext = 'G'

        if len(self.ndarray[0]) > 0:
            if coordinate == 'xyz2hdz':
                result = result.xyz2hdz()
                result.header['DataComponents'] = 'HDZ'+ext
            elif coordinate == 'xyz2idf':
                result = result.xyz2idf()
                result.header['DataComponents'] = 'IDF'+ext
            elif coordinate == 'hdz2xyz':
                result = result.hdz2xyz()
                result.header['DataComponents'] = 'XYZ'+ext
            elif coordinate == 'idf2xyz':
                result = result.idf2xyz()
                result.header['DataComponents'] = 'XYZ'+ext
            elif coordinate == 'idf2hdz':
                result = result.idf2xyz()
                result = result.xyz2hdz()
                result.header['DataComponents'] = 'HDZ'+ext
            elif coordinate == 'hdz2idf':
                result = result.hdz2xyz()
                result = result.xyz2idf()
                result.header['DataComponents'] = 'IDF'+ext
            else:
                print("_convertstream: unkown coordinate transform")
        return result


    def _det_trange(self, period):
        """
        starting with coefficients above 1%
        is now returning a timedelta object
        """
        return np.sqrt(-np.log(0.01)*2)*self._tau(period)

    @deprecated("replaced by is_number in core.methods")
    def _is_number(self, s):
        """
        Test whether s is a number
        """
        if str(s) in ['','None',None]:
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False
        except:
            return False

    @deprecated("Use normalize from core.methods")
    def _normalize(self, column):
        """
        normalizes the given column to range [0:1]
        """
        normcol = []
        timeconv = False
        # test column contents
        if isinstance(column[0], (float,int,float64)):
            column = column.astype(float)
        elif isinstance(column[0], (datetime,datetime64)):
            column = date2num(column)
            timeconv = True
        else:
            print ("stream._normalize: column does not contain numbers")
        maxval = np.max(column)
        minval = np.min(column)
        for elem in column:
            normcol.append((elem-minval)/(maxval-minval))

        return normcol, minval, maxval

    def _drop_nans(self, key, debug=False):
        """
    DEFINITION:
        Helper to drop all lines when NaNs or INFs are found within the selected key

    RETURNS:
        - DataStream:   (DataStream object) a new data stream object with out identified lines.

    EXAMPLE:
        newstream = stream._drop_nans('x')

    APPLICATION:
        used for plotting and fitting of data

        """
        # Method only works with numerical columns and the time column
        searchlist = ['time']
        searchlist.extend(self.NUMKEYLIST)
        tstart = datetime.now(timezone.utc).replace(tzinfo=None)

        array = [np.asarray([]) for elem in self.KEYLIST]
        if len(self) > 0 and key in searchlist:
            # get the indicies with NaN's and then use numpy delete
            ind = self.KEYLIST.index(key)
            col = np.asarray(self.ndarray[ind])
            if len(col) > 0:
                if not key == 'time':
                    col = col.astype(float)
                indicieslst = np.argwhere(np.isnan(col))
                for index, tkey in enumerate(self.KEYLIST):
                    if len(self.ndarray[index]) > 0 and len(self.ndarray[index]) == len(col):
                        array[index] = np.delete(self.ndarray[index], indicieslst)
                if debug:
                    print ("_drop_nans: found {} nan values".format(len(indicieslst)))
            else:
                array[ind] = np.asarray([])
        #else:
        #    newst = [elem for elem in self if not isnan(eval('elem.'+key)) and not isinf(eval('elem.'+key))]
        if debug:
            tend = datetime.now(timezone.utc).replace(tzinfo=None)
            print("_drop_nans needed", (tend - tstart).total_seconds())

        return DataStream([LineStruct()],self.header,np.asarray(array,dtype=object))


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

        ndarray = np.asarray([np.asarray(elem) if self.KEYLIST[idx] in keys or self.KEYLIST[idx] == 'time' else np.asarray([]) for idx,elem in enumerate(result.ndarray)],dtype=object)

        return DataStream(header=result.header,ndarray=ndarray)


    def _select_timerange(self, starttime=None, endtime=None, maxidx=-1):
        """
      DESCRIPTION
        Non-destructive method to select a certain time range from a stream.
        Similar to trim, leaving the original stream unchanged however.
      APPLICATION:
        Used by write
        """
        ndarray = [[] for key in self.KEYLIST]

        startindices = []
        endindices = []
        if starttime:
            starttime = testtime(starttime)
            if isinstance(self.ndarray[0][0], np.datetime64):
                starttime = np.datetime64(starttime)
            if isinstance(self.ndarray[0][0],(float,float64)):  # in case array comes from an baseline structure
                starttime = date2num(starttime)
            if self.ndarray[0].size > 0:   # time column present
                if maxidx > 0:
                    idx = (np.abs(self.ndarray[0][:maxidx]-starttime)).argmin()
                else:
                    idx = (np.abs(self.ndarray[0]-starttime)).argmin()
                # Trim should start at point >= starttime, so check:
                if self.ndarray[0][idx] < starttime:
                    idx += 1
                startindices = list(range(0,idx))
        if endtime:
            endtime = testtime(endtime)
            if isinstance(self.ndarray[0][0], np.datetime64):
                endtime = np.datetime64(endtime)
            if isinstance(self.ndarray[0][0],(float,float64)):
                endtime = date2num(endtime)
            if self.ndarray[0].size > 0:   # time column present
                #print "select timerange", maxidx
                if maxidx > 0: # truncate the ndarray
                    #print maxidx
                    #tr = self.ndarray[0][:maxidx].astype(float)
                    idx = 1 + (np.abs(self.ndarray[0][:maxidx]-endtime)).argmin() # get the nearest index to endtime and add 1 (to get lenghts correctly)
                else:
                    idx = 1 + (np.abs(self.ndarray[0]-endtime)).argmin() # get the nearest index to endtime and add 1 (to get lenghts correctly)
                if idx >= len(self.ndarray[0]): ## prevent too large idx values
                    idx = len(self.ndarray[0]) # - 1
                try: # using try so that this test is passed in case of idx == len(self.ndarray)
                    endnum = endtime
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
                except:
                    pass
                endindices = list(range(idx,len(self.ndarray[0])))

        indices = startindices + endindices

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

        #t3 = datetime.now(timezone.utc).replace(tzinfo=None)
        #print "_select_timerange - deleting :", t3-t2

        return np.asarray(ndarray,dtype=object)

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
        stream = stream.aic_calc('x',timerange=timedelta(hours=0.5))

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
        delete = kwargs.get('delete')
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
        aic2ind = self.KEYLIST.index(aic2key)
        if len(self.ndarray[aic2ind]) > 0:
            if delete:
                print ("aic_calc: removing contents from column {} for storage of aic data".format(aic2key))
                self.ndarray[aic2ind] = np.asarray([])
            else:
                print ("aic_calc: cannot use the projected column {} for data storage as this contains data already".format(aic2key))
                return self
        if len(self.ndarray[0]) > 0.:
            self.ndarray[aic2ind] = np.empty((len(self.ndarray[0],)))
            self.ndarray[aic2ind][:] = np.nan
        # get sampling interval for normalization - need seconds data to test that
        sp = self.samplingrate()
        # correct approach
        iprev = 0
        iend = 0

        # change the following approach completely - extract ranges based on indices
        # based on time range and sampling rate determine window length in indies
        n_inds = int(timerange.total_seconds()/sp)


        while iend < len(t)-1:
            istart = iprev
            iend = istart+n_inds
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
                        if not isnan(aicval):
                            aicarray.append(aicval)
                        # store start value - aic: is a measure for the significance of information change
                        #if idx == 2:
                        #    aicstart = aicval
                        #self[idx+istart].var5 = aicstart-aicval
                maxaic = np.max(aicarray)
                # determine the relative amplitude as well
                cnt = 0
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
          extradays       (int) days to which the absolutedata is exteded prior and after start and endtime
          ##plotfilename    (string) if plotbaseline is selected, the outputplot is send to this file
          fitfunc         (string) see fit
          fitdegree       (int) see fit
          knotstep        (int) see fit
          keys            (list) keys which contain the basevalues (default) is ['dx','dy','dz']

      APPLICATION:
          func  = data.baseline(didata,knotstep=0.1)
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
        startabs =  kwargs.get('startabs')
        endabs =  kwargs.get('endabs')
        debug =  kwargs.get('debug')

        orgstartabs = None
        orgendabs = None

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
            starttime = np.min(self.ndarray[0])
            endtime = np.max(self.ndarray[0])
        else:
            print ("decrepated")
            starttime = self[0].time
            endtime = self[-1].time

        fixstart,fixend = False,False
        if startabs:
            startabs = testtime(startabs)
            orgstartabs = startabs
            fixstart = True
        if endabs:
            endabs = testtime(endabs)
            orgendabs = endabs
            fixend = True

        pierlong = absolutedata.header.get('DataAcquisitionLongitude','')
        pierlat = absolutedata.header.get('DataAcquisitionLatitude','')
        pierel = absolutedata.header.get('DataElevation','')
        pierlocref = absolutedata.header.get('DataAcquisitionReference','')
        pierelref = absolutedata.header.get('DataElevationRef','')

        usestepinbetween = False # for better extrapolation

        logger.info(' --- Start baseline-correction at %s' % str(datetime.now()))

        absolutestream  = absolutedata.copy()
        #absolutestream = absolutestream.remove_flagged()

        absndtype = False
        if len(absolutestream.ndarray[0]) > 0:
            #print ("HERE1: adopting time range absolutes - before {} {}".format(startabs, endabs))
            absndtype = True
            if not absolutestream.ndarray[0][0] < endtime:
                logger.warning("Baseline: Last measurement prior to beginning of absolute measurements ")
            abst = absolutestream.ndarray[0]
            if not startabs or startabs < np.min(absolutestream.ndarray[0]):
                startabs = absolutestream.start()
            if not endabs or endabs > np.max(absolutestream.ndarray[0]):
                endabs = absolutestream.end()
        else:
            print ("deprecated")
            # 1) test whether absolutes are in the selected absolute data stream
            if absolutestream[0].time == 0 or absolutestream[0].time == np.nan:
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

        if debug:
            print (" baseline: Time range absolutes  - {} {} {} {}".format(startabs, endabs, startabs, endabs))
            print (" baseline: Time range datastream - {} {}".format(starttime, endtime))

        # 3) check time ranges of stream and absolute values:
        if startabs > starttime:
            logger.info('Baseline: {} days without absolutes at the beginning of the stream'.format((startabs-starttime).total_seconds()/86400.))
        if endabs < endtime:
            logger.info("Baseline: Last absolute measurement before end of stream - extrapolating baseline")
            if (endabs + timedelta(days=extradays)) < endtime:
                usestepinbetween = True
                if not fixend:
                    logger.warning("Baseline: Well... thats an adventurous extrapolation, but as you wish...")

        # 4) get standard time range of one year and extradays at start and end
        #           test whether absstream covers this time range including extradays
        # ###########
        #  get boundaries
        # ###########
        extrapolate = False
        # upper
        if fixend:
            if debug:
                print (" baseline: fixend", endabs, extradays)

            # time range long enough
            baseendtime = endabs+timedelta(days=extradays)
            if baseendtime < orgendabs:
                baseendtime = orgendabs
            extrapolate = True
        else:
            baseendtime = endtime+timedelta(days=1)
            extrapolate = True
        if fixstart:
            if debug:
                print (" baseline: fixstart at {} with {} extradays".format(startabs, extradays))
            basestarttime = startabs-timedelta(days=extradays)
            if basestarttime > orgstartabs:
                basestarttime = orgstartabs
            extrapolate = True
        else:
            # not long enough
            basestarttime = startabs-timedelta(days=extradays)
            extrapolate = True
            if baseendtime - timedelta(days=(366.+2*extradays)) > startabs:
                # time range long enough
                basestarttime =  baseendtime-timedelta(days=(366.+2*extradays))
        tabs = absolutestream.copy()
        bas = tabs.trim(starttime=basestarttime,endtime=baseendtime)

        if extrapolate: # and not extradays == 0:
            if debug:
                print (" baseline: Extrapolating ", bas.length()[0])
            bas = bas.extrapolate(starttime=basestarttime,endtime=baseendtime, debug=debug)
            # Now remove duplicates - if start and end are existing they are duplicated be extrapolate
            bas = bas.removeduplicates()
            if debug:
                print ("    -> done ", bas.length()[0])

        try:
            if debug:
                print ("Adopting baseline between: {a} and {b} using {c}".format(a=np.min(bas.ndarray[0]),b=np.max(bas.ndarray[0]),c=fitfunc))
            logger.info("Fitting Baseline between: {a} and {b}".format(a=np.min(bas.ndarray[0]),b=np.max(bas.ndarray[0])))
            if debug:
                print (" - running Fit with Fitfunc {}, fitdegree {} and knotstep {}".format(fitfunc,fitdegree,knotstep))
            func = bas.fit(keys,fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep)
        except:
            print ("Baseline: Error when determining fit - Enough data point to satisfy fit complexity?")
            logger.error("Baseline: Error when determining fit - Not enough data point to satisfy fit complexity? N = {}".format(bas.length()))
            return None

        keystr = '_'.join(keys)
        pierlong = absolutedata.header.get('DataAcquisitionLongitude','')
        pierlat = absolutedata.header.get('DataAcquisitionLatitude','')
        pierel = absolutedata.header.get('DataElevation','')
        pierlocref = absolutedata.header.get('DataLocationReference','')
        pierelref = absolutedata.header.get('DataElevationRef','')
        orgstartabs = date2num(orgstartabs)
        orgendabs = date2num(orgendabs)
        if not pierlong == '' and not pierlat == '' and not pierel == '':
            absinfostring = '_'.join(map(str,[orgstartabs,orgendabs,extradays,fitfunc,fitdegree,knotstep,keystr,pierlong,pierlat,pierlocref,pierel,pierelref]))
        else:
            absinfostring = '_'.join(map(str,[orgstartabs,orgendabs,extradays,fitfunc,fitdegree,knotstep,keystr]))
        existingabsinfo = self.header.get('DataAbsInfo','').replace(', EPSG',' EPSG').split(',')
        if not existingabsinfo[0] == '':
            existingabsinfo.append(absinfostring)
        else:
            existingabsinfo = [absinfostring]

        # Get minimum and maximum times out of existing absinfostream in matplotlib.dates format
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
        # please note: maxendtime is the last absvalue. trim however uses <= and thus excludes the last value
        # therefore we add a very small timefraction to use the last time
        bas2save = absolutestream.trim(starttime=minstarttime,endtime=maxendtime+0.000000001)
        tmpdict = bas2save.stream2dict()
        self.header['DataBaseValues'] = tmpdict['DataBaseValues']

        # Get column heads of dx,dy and dz
        # default is H-base[nT],D-base[deg],Z-base[nT]
        basecomp = "HDZ"
        try:
            basecomp = "{}{}{}".format(absolutestream.header.get('col-dx')[0],absolutestream.header.get('col-dy')[0],absolutestream.header.get('col-dz')[0])
        except:
            pass
        if not basecomp == "HDZ":
            print ("     -> basevalues correspond to components {}".format(basecomp))
        self.header['DataBaseComponents'] = basecomp

        logger.info(' --- Finished baseline-correction at %s' % str(datetime.now()))

        return func


    def stream2dict(self, keys=None, dictkey='DataBaseValues'):
        """
        DESCRIPTION:
            Method to convert stream contents into a list and assign this to a dictionary.
            You can use this method to directly store magnetic basevalues along with
            data time series (e.g. using NasaCDF). Multilayer storage as supported by NetCDF
            might provide better options to combine both data sets in one file.
        PARAMETERS:
            self		(DataStream) data containing e.g. basevalues
            keys		(list of keys) keys which are going to be stored
            dictkey		(string) name of the dictionaries key
        RETURNS:
            dict		(dictionary) with name dictkey
        APPLICATION:
            d = absdata.stream2dict(['dx','dy','dz','df'],'DataBaseValues')
            d = neicdata.stream2dict(['f','str3'],'Earthquakes')
        """

        if not keys:
            keys = ['dx','dy','dz','df']
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
                pos = self.KEYLIST.index(key)
            except ValueError:
                pos = -1
            if pos in range(0,len(self.KEYLIST)):
                column = self.ndarray[pos]
                if len(column) > 0:
                    headline.append(key)
                    if not key == 'time':
                        addline.append(self.header.get('col-'+key))
                    else:
                        addline.append(self.header.get('DataID'))
                    column = self.ndarray[pos]
                    array.append(column)

        rowlst = np.transpose(np.asarray(array, dtype=object))
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
            self		(DataStream) stream with variation data
            dictkey		(string) ususally 'DataBaseValues'
        RETURNS:
            stream		(DataStream) containing values of header info
        APPLICATION:
            absstream = stream.dict2stream(header['DataBaseValues'])
        """

        lst = self.header.get(dictkey)

        if not type(lst) in (list,tuple,np.ndarray):
            print("dict2stream: no list,tuple,array found in provided header key")
            return DataStream()

        if len(lst) == 0:
            print("dict2stream: list is empty")
            return DataStream()

        array = [[] for el in self.KEYLIST]

        headerinfo = lst[0]
        addinfo = lst[1]
        data = lst[2:]
        #print(headerinfo,addinfo)
        collst = np.transpose(np.asarray(data)).astype(object)
        #print(collst)

        for idx,key in enumerate(headerinfo):
            pos = self.KEYLIST.index(key)
            array[pos] = collst[idx]

        if isinstance(array[0][0],float):
            array[0] = num2date(array[0])
        if isinstance(array[0][0],datetime):
            # convert to offset naive time array
            array[0] = [el.replace(tzinfo=None) for el in array[0]]

        return DataStream(header={}, ndarray=np.asarray(array,dtype=object))


    @deprecated("Apparently unused method - to be remove in 2.1")
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
            dicthead = stream.header
            streamlist.append([DataStream([LineStruct()],dicthead.copy(),stream.ndarray),baselinefunc])

        #print "Streamlist", streamlist
        #print len(dicthead),dictlist
        return streamlist


    def bc(self, function=None, ctype=None, alpha=0.0, beta=0.0, level='preliminary',usedf=False,debug=False):
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
            function      (list of function objects) provide functionlist directly - not from header
            ctype         (string) one of 'fff', 'fdf', 'ddf' - denoting nT components 'f' and degree 'd'
            alpha/beta    (floats) provide rotation angles for the variometer data to be applied
                                   before correction - data is rotated back after correction
        """
        logger.debug("BC: Performing baseline correction: Requires HEZ data.")
        logger.debug("    H magnetic North, E magnetic East, Z vertical downwards, all in nT.")

        if not function:
            function = []
        keys = ['x', 'y', 'z']

        pierdata = False
        absinfostring = self.header.get('DataAbsInfo')
        absvalues = self.header.get('DataBaseValues')
        func = self.header.get('DataAbsFunctionObject')
        datatype = self.header.get('DataType')
        basecomp = self.header.get('DataBaseComponents')

        if datatype == 'BC':
            print ("BC: dataset is already baseline corrected - returning")
            return self

        pierdata = {}
        bcdata = self.copy()

        logger.debug("BC: Components of stream: {}".format(self.header.get('DataComponents')))
        logger.debug("BC: baseline adoption information: {}".format(absinfostring))

        def baseline_adoption(bcdata,funclist,keys,basecomp,pierdata):
            datacomp = bcdata.header.get('DataComponents','')
            if basecomp in ['xyz','XYZ']:
                bcdata = bcdata.func2stream(funclist,mode='add',keys=keys)
                bcdata.header['col-x'] = 'X'
                bcdata.header['unit-col-x'] = 'nT'
                bcdata.header['col-y'] = 'Y'
                bcdata.header['unit-col-y'] = 'nT'
                if len(datacomp) == 4:
                    bcdata.header['DataComponents'] = 'XYZ'+datacomp[3]
                else:
                    bcdata.header['DataComponents'] = 'XYZ'
            elif basecomp in ['hdz','HDZ']:
                #required input (see Excel: #Hv + Hb;   Db + atan2(y,H_corr)    Zb + Zv)
                # thus i extract the y component, calulate H and then add y again
                # h_corr is then created in func2stream with the addbaseline option
                #ycomp = bcdata._get_column("y")
                #bcdata = bcdata.xyz2hdz()
                #print (bcdata.ndarray)
                #bcdata = bcdata._put_column(ycomp,"y")
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
            else:
                # Default: asume HDZ
                #print ("BC: Found a list of functions:", funclist)
                #ycomp = bcdata._get_column("y")
                #bcdata = bcdata.xyz2hdz()
                #bcdata = bcdata._put_column(ycomp,"y")
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
                self.header['DataAcquisitionLongitude'] = pierdata.get("pierlon")
                self.header['DataAcquisitionLatitude'] = pierdata.get("pierlat")
                self.header['DataLocationReference'] = pierdata.get("pierlocref")
                self.header['DataElevation'] = pierdata.get("pierel")
                self.header['DataElevationRef'] = pierdata.get("pierelref")

            return bcdata

        if function:
            if debug:
                print("BC: baseline adoption information provided in function list - correcting")
            keys = ['x','y','z']
            for func in function:
                func[0]['fx'] = func[0]['fdx']
                func[0]['fy'] = func[0]['fdy']
                func[0]['fz'] = func[0]['fdz']
                func[0].pop('fdx', None)
                func[0].pop('fdy', None)
                func[0].pop('fdz', None)
                if usedf and func[0].get('fdf'):
                    func[0]['ff'] = func[0]['fdf']
                    func[0].pop('fdf', None)
                    if not 'f' in keys:
                        keys.append('f')
            bcdata = baseline_adoption(bcdata,function,keys,basecomp,pierdata)
        elif absinfostring and type(absvalues) in [list,np.ndarray,tuple]:
            if debug:
                print("BC: Found baseline adoption information in meta data - recalculating baseline(s) with given parameters")
            absinfostring = absinfostring.replace(', EPSG',' EPSG')
            absinfostring = absinfostring.replace(',EPSG',' EPSG')
            absinfostring = absinfostring.replace(', epsg',' EPSG')
            absinfostring = absinfostring.replace(',epsg',' EPSG')
            absinfolist = absinfostring.split(',')
            funclist = []
            for absinfo in absinfolist:
                # extract baseline data
                absstream = bcdata.dict2stream()
                #print("BC: abstream length", absstream.length()[0])
                parameter = absinfo.split('_')
                #print("BC:", parameter, len(parameter))
                funckeys = parameter[6:9]
                if usedf and parameter[-1] == 'df' and len(parameter) == 10:
                    funckeys = parameter[6:10]
                if len(parameter) >= 14:
                    #extract pier information
                    pierdata = {}
                    pierdata["pierlon"] = float(parameter[9])
                    pierdata["pierlat"] = float(parameter[10])
                    pierdata["pierlocref"] = parameter[11]
                    pierdata["pierel"] = float(parameter[12])
                    pierdata["pierelref"] =  parameter[13]
                #print("BC", num2date(float(parameter[0])))
                #print("BC", num2date(float(parameter[1])))
                if not funckeys == ['df']:
                    if debug:
                        print ("baseline parameters", parameter)
                        print ("absdata", absstream.ndarray)
                    func = bcdata.baseline(absstream, startabs=float64(parameter[0]), endabs=float64(parameter[1]), extradays=int(float(parameter[2])), fitfunc=parameter[3], fitdegree=int(float(parameter[4])), knotstep=float(parameter[5]), keys=funckeys, debug=debug)
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
                    if 'df' in funckeys and usedf:
                        func[0]['ff'] = func[0]['fdf']
                        func[0].pop('fdf', None)
                        keys.append('f')
                    funclist.append(func)

            bcdata = baseline_adoption(bcdata,funclist,keys,basecomp,pierdata)

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

            bcdata = baseline_adoption(bcdata,func,keys,basecomp,pierdata)

        else:
            print("BC: No data for correction available - header needs to contain DataAbsFunctionObject")
            return self

        return bcdata


    @deprecated("replaced by core.flagging flag_binary")
    def bindetector(self,key,flagnum=1,keystoflag=None,sensorid=None,text=None,**kwargs):
        if not keystoflag:
            keystoflag = ['x']
        markallon = kwargs.get('markallon')
        markalloff = kwargs.get('markalloff')
        from magpy.core import flagging
        fl = flagging.flag_binary(self , key, flagtype=flagnum, keystoflag=keystoflag, sensorid=sensorid, text=text, markallon=markallon, markalloff=markalloff,
                    groups=None)
        return fl

    def calc_f(self, skipdelta = False):
        """
        DEFINITION:
            Calculates the f form  x^2+y^2+z^2. If delta F is present, then by default
            this value is considered as well.
            According to IM Technical Manual 5.0.0: F(scalar) = F(vector) - dF
        PARAMETERS:
            - skipdelta   (bool)  if selecetd then an existing delta f is not accounted for
        RETURNS:
            - DataStream with f

        EXAMPLES:
            fstream = stream.calc_f()
        """

        # Take care: if there is only 0.1 nT accuracy then there will be a similar noise in the deltaF signal

        if not len(self.ndarray[0]) > 0:
            return self

        fstream = self.copy()

        if len(self.ndarray[0]) > 0:
            inddf = self.KEYLIST.index('df')
            indf = self.KEYLIST.index('f')
            indx = self.KEYLIST.index('x')
            indy = self.KEYLIST.index('y')
            indz = self.KEYLIST.index('z')
            if len(fstream.ndarray[inddf]) > 0 and not skipdelta:
                df = fstream.ndarray[inddf].astype(float)
            else:
                df = np.asarray([0.0]*len(fstream.ndarray[indx]))
            x2 = ((fstream.ndarray[indx])**2).astype(float)
            y2 = ((fstream.ndarray[indy])**2).astype(float)
            z2 = ((fstream.ndarray[indz])**2).astype(float)
            fstream.ndarray[indf] = np.sqrt(x2+y2+z2) - df

        fstream.header['col-f'] = 'f'
        fstream.header['unit-col-f'] = 'nT'

        logger.info('calc_f: --- Calculating f finished at {}'.format(str(datetime.now())))

        return fstream


    def compensation(self, skipdelta=False, debug=False):
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
            - skipdelta   (bool)  if True then DataDeltaValues are ignored
        RETURNS:
            - DataStream with compensation values appliesd to xyz values
            - original dataStream if no compensation values are found

        EXAMPLES:
            compstream = stream.compensation()
        """

        if not self.length()[0] > 0:
            return self

        stream = self.copy()
        if debug:
            logger.info("compensation: applying compensation field values to variometer data ...")
        deltas = stream.header.get('DataDeltaValues','')
        if not skipdelta and not deltas=='':
            if debug:
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
        if debug:
            logger.info(' -- applying compensation fields: x={}, y={}, z={}'.format(xcomp,ycomp,zcomp))
        if len(offdict) > 0:
            stream = stream.offset(offdict)
            stream.header['DataDeltaValuesApplied'] = 1

        return stream

    def contents(self):
        """
        DESCRIPTION
            show the contents of the stream
        RETURNS
            will return a dictionary containing key plus column name, unit and first element
        EXAMPLE
            print(data.contents)
        :return:
        """
        variables = self.variables()
        result = {}
        for variable in variables:
            content = {}
            content['columnname'] = self.header.get('col-{}'.format(variable),'')
            content['columnunit'] = self.header.get('unit-col-{}'.format(variable),'')
            content['content'] = self._get_column(variable)[-1]
            result[variable] = content
        return result

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


    def dailymeans(self, keys=None, offset=timedelta(hours=12), keepposition=False, percentage=90, **kwargs):
        """
    DEFINITION:
        Calculates daily means of xyz components and their standard deviations. By default
        numpy's mean and std methods are applied even if only two data sets are available.

        An outputstream is generated which containes basevalues in columns
        x,y,z and uncertainty values in dx,dy,dz
        if only a single values is available, dx,dy,dz contain the average uncertainties
        of the full data set
        time column contains the average time of the measurement

    PARAMETERS:
    Variables
    	- keys: 	(list) provide up to four keys which are used in columns x,y,z
        - offset:       (float) offset as timedelta(seconds=xx)
        - percentage: (float) default is 90
    Kwargs:
        - none

    RETURNS:
        - stream:       (DataStream object) with daily means and standard deviation

    EXAMPLE:
        means = didata.dailymeans(keys=['dx','dy','dz'])

    APPLICATION:
        means = didata.dailymeans(keys=['dx','dy','dz'])
        mp.plot(means,['x','y','z'],errorbars=True, symbollist=['o','o','o'])

        """

        poslst,deltaposlst = [],[]
        deltakeys = ['dx','dy','dz','df']
        if not keys:
            keys = ['x', 'y', 'z', 'f']
        diff = 0
        keys = keys[:4]

        for key in keys:
            poslst.append(self.KEYLIST.index(key))
        for idx,pos in enumerate(poslst):
            deltaposlst.append(self.KEYLIST.index(deltakeys[idx]))

        if not len(self.ndarray[0]) > 0:
            return self

        array = [[] for el in self.KEYLIST]
        data = self.copy()
        data = data.removeduplicates()
        timecol = np.floor(date2num(data.ndarray[0]))
        tmpdatelst = np.asarray(list(set(list(timecol))))
        for day in tmpdatelst:
            sel = data._select_timerange(starttime=day,endtime=day+1)
            sttmp = DataStream(header={},ndarray=sel)
            array[0].append(num2date(day).replace(tzinfo=None)+offset)
            for idx, pos in enumerate(poslst):
                if not keepposition:
                    array[idx+1].append(sttmp.mean(self.KEYLIST[pos],percentage=percentage))
                else:
                    array[pos].append(sttmp.mean(self.KEYLIST[pos],percentage=percentage))
                data.header['col-'+self.KEYLIST[idx+1]] = '{}'.format(self.header.get('col-'+self.KEYLIST[pos]))
                data.header['unit-col-'+self.KEYLIST[idx+1]] = '{}'.format(self.header.get('unit-col-'+self.KEYLIST[pos]))
                diff = pos-idx
            if not keepposition:
              for idx,dpos in enumerate(deltaposlst):
                #if len(sttmp.ndarray[idx]) > 0:
                me,std = sttmp.mean(self.KEYLIST[idx+diff],percentage=percentage, std=True)
                array[dpos].append(std)
                data.header['col-'+self.KEYLIST[dpos]] = 'sigma {}'.format(self.header.get('col-'+self.KEYLIST[idx+diff]))
                data.header['unit-col-'+self.KEYLIST[dpos]] = '{}'.format(self.header.get('unit-col-'+self.KEYLIST[idx+diff]))
        data.header['DataFormat'] = 'MagPyDailyMean'

        array = [np.asarray(el) for el in array]
        retstream = DataStream(header=data.header,ndarray=np.asarray(array,dtype=object))
        retstream = retstream.sorting()
        return retstream


    def delta_f(self, **kwargs):
        """
        DESCRIPTION:
            Calculates the difference of vector F and scalar f and puts the result to the df column
            Calculated is dF = F(vector) - F(scalar) as defined in section 5.5 within the
            INTERMAGNET Technical Manual 5.0.0

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


        ind = self.KEYLIST.index("df")
        indx = self.KEYLIST.index("x")
        indy = self.KEYLIST.index("y")
        indz = self.KEYLIST.index("z")
        indf = self.KEYLIST.index("f")
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

            self.header['col-df'] = 'delta f'
            self.header['unit-col-df'] = 'nT'

        logger.info('--- Calculating delta f finished at %s ' % str(datetime.now()))

        return self


    @deprecated("Use the derivative method instead")
    def differentiate(self, **kwargs):
        return self.derivative(**kwargs)


    def derivative(self, **kwargs):
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
        stream = stream.derivative(keys=['f'],put2keys=['df'])

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

        if len(stream.ndarray[0]) > 0:
            t = stream.ndarray[0]

        for i, key in enumerate(keys):
            ind = self.KEYLIST.index(key)
            val = stream.ndarray[ind].astype(float64)
            dval = np.gradient(np.asarray(val))
            stream._put_column(dval, put2keys[i])
            stream.header['col-'+put2keys[i]] = r"d%s vs dt" % (key)

        logger.info('--- derivative obtained at %s ' % str(datetime.now()))
        return stream


    def dwt_calc(self,key='x',wavelet='db4',level=3,plot=False,outfile=None,
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
        DWT_stream = stream.dwt_calc(plot=True)

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
        t_ind = self.KEYLIST.index('time')

        #DWT_stream = DataStream([],{})
        DWT_stream = DataStream()
        headers = DWT_stream.header
        array = [[] for key in self.KEYLIST]
        x_ind = self.KEYLIST.index('x')
        dx_ind = self.KEYLIST.index('dx')
        var1_ind = self.KEYLIST.index('var1')
        var2_ind = self.KEYLIST.index('var2')
        var3_ind = self.KEYLIST.index('var3')
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
            array[t_ind].append(self.ndarray[t_ind][i+int(window/2)])
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
        return DataStream(header=headers, ndarray=np.asarray([np.asarray(a) for a in array],dtype=object))


    def extract(self, key, value, compare=None, debug=None):
        """
        DEFINITION:
            Read stream and extract data of the selected key which meets the choosen criteria

        PARAMETERS:
        Variables:
            - key:      (str) streams key e.g. 'x'.
            - value:    (str/float/int) any selected input which should be tested for
                        special note: if value is in brackets, then the term is evaluated
                        e.g. value="('int(1.85)')" selects all values with 1
                        Important: this only works for compare = '=='
         Kwargs:
            - compare:  (str) criteria, one out of ">=", "<=",">", "<", "==", "!=", default is '=='
            - debug:   (bool) if true several additional outputs will be created

        RETURNS:
            - DataStream with selected values only

        EXAMPLES:
            extractedstream = stream.extract('x',20000,'>')
            extractedstream = stream.extract('str1','Berger')
        """

        if not compare:
            compare = '=='
        if not compare in [">=", "<=",">", "<", "==", "!=", 'like']:
            logger.info('--- Extract: Please provide proper compare parameter ">=", "<=",">", "<", "==", "like" or "!=" ')
            return self

        if value in ['',None]:
            return self

        if not len(self.ndarray[0]) > 0:
            return self

        ind = self.KEYLIST.index(key)

        stream = self.copy()
        newarray = [[] for key in self.KEYLIST]

        if not is_number(value):
            if value.startswith('(') and value.endswith(')') and compare == '==':
                logger.info("extract: Selected special functional type -equality defined by difference less then 10 exp-6")
                val = eval(value[1:-1])
                indexar = np.where((np.abs(stream.ndarray[ind]-val)) < 0.000001)[0]
            else:
                too = '"' + str(value) + '"'
                if compare == 'like':
                    indexar = np.asarray([i for i, s in enumerate(stream.ndarray[ind]) if str(value) in s])
                else:
                    searchclause = 'stream.ndarray[ind] '+ compare + ' ' + too
                    indexar = eval('np.where('+searchclause+')[0]')
        else:
            too = str(value)
            searchclause = 'stream.ndarray[ind].astype(float) '+ compare + ' ' + too
            with np.errstate(invalid='ignore'):
                indexar = eval('np.where('+searchclause+')[0]')

        for ind,el in enumerate(stream.ndarray):
            if len(stream.ndarray[ind]) > 0:
                ar = [stream.ndarray[ind][i] for i in indexar]
                newarray[ind] = np.asarray(ar).astype(object)
        return DataStream(header=stream.header, ndarray=np.asarray(newarray, dtype=object))


    def extract_headerlist(self, element, parameter=1, year=None):
        """
        DESCRIPTION
            extract values from specific time-list header information
            i.e. alpha and beta values
            Each headers list structure is transferred into a list
            2022_3.23_1.23,2023_21.43_12.2 -> [[2022,2023],[3.23,21.43],[1.23,12.2]]
        OPTIONS
            element (header element) : the header element containing the list type
            parameter          (int) : defines the column from which the value is taken
            year               (int) : if provided, data of this year is takes, else man(year) is chosen
        APPLICATION
            used for core.activity for seek_storm application
            alpha = magdata.extract_headerlist('DataRotationAlpha')
        """
        content = self.header.get(element)
        # content = self.header.get(key)
        if content:
            contlist = content.split(',')
            vals = [el.split('_') for el in contlist]
            l = np.asarray(vals).T.astype(float64)
            if year:
                ind = np.argwhere(l == year)
            else:
                ind = np.argmax(l)
            return l[parameter][ind]
        else:
            return 0

    def extrapolate(self, starttime, endtime, method='old', force=False, debug=False):
        """
        DESCRIPTION
            Extrapolate all contents with a data stream towards given starttime and endtime.Important: this method
            will remove any non-numerical columns from the resulting data set as those cannot be extrapolated.
            Several different methods are available for extrapolation.
            Methods:
               old -- duplicates first and last point at given times
               spline -- see here https://docs.scipy.org/doc/scipy/tutorial/interpolate/extrapolation_examples.html
               linear -- uses a simple linear extrapolation method
               fourier -- uses a technique based on this work: https://gist.github.com/tartakynov/83f3cd8f44208a1856ce
            Important: any existing secondary time column will be dropped.
        PREREQUISITES
            Will raise an error of time distance between starttime respectively endtime
            towrads date in the stream is larger than the duration
            Choose "force =True" to override
        OPTIONS:
            starttime
            endtime
            method
            force - inactive
        APPLICATION
            - used by baseline method before application of the spline fit
            expst = extrapolate(trimstream,starttime=datetime(2022,11,22,7), endtime=datetime(2022,11,22,16),method='fourier')
        """
        skipst = False
        skipet = False
        indbefore, indafter = 0, 0
        xnew = []
        xs = None
        n_harm = 10  # number of harmonics in fourier method
        starttime = testtime(starttime)
        endtime = testtime(endtime)
        if not len(self) > 0:
            print ("extrapolate: Empty stream provided - aborting")
            return self
        stst, etst = self.timerange()
        duration = (etst - stst).total_seconds()
        dist1 = (stst - starttime).total_seconds()
        if starttime >= etst or endtime <= stst:
            print ("extrapolate: Time range of data set and extrapolation range do not fit - aborting")
            return self
        if dist1 < 0:
            print("extrapolate: Starttime younger then first date in stream - skipping")
            skipst = True
        dist2 = (endtime - etst).total_seconds()
        if dist2 < 0:
            print("extrapolate: Endtime older then last date in stream - skipping")
            skipet = True
        if dist1 > duration or dist1 > duration:
            print("extrapolate: raise error")
            return self
        if debug:
            print("Time diffs between start and data in seconds", dist1, dist2, duration)
        st = self.copy()
        # do some cleanups for extrapolation
        st = st._drop_column('sectime')
        st = st._remove_nancolumns()
        # Drop non-numerical columns as they cannot be extrapolated
        vars = st.variables()
        for var in vars:
            if not var in st.NUMKEYLIST:
                st = st._drop_column(var)
        samprate = st.samplingrate() * 1000000

        # get the average sampling rate of st
        # Initial time range tests done - continue with methods
        def add_boundary_knots(spline):
            """
            Add knots infinitesimally to the left and right.

            Additional intervals are added to have zero 2nd and 3rd derivatives,
            and to maintain the first derivative from whatever boundary condition
            was selected. The spline is modified in place.
            """
            # determine the slope at the left edge
            leftx = spline.x[0]
            lefty = spline(leftx)
            leftslope = spline(leftx, nu=1)

            # add a new breakpoint just to the left and use the
            # known slope to construct the PPoly coefficients.
            leftxnext = np.nextafter(leftx, leftx - 1)
            leftynext = lefty + leftslope * (leftxnext - leftx)
            leftcoeffs = np.array([0, 0, leftslope, leftynext])
            spline.extend(leftcoeffs[..., None], np.r_[leftxnext])

            # repeat with additional knots to the right
            rightx = spline.x[-1]
            righty = spline(rightx)
            rightslope = spline(rightx, nu=1)
            rightxnext = np.nextafter(rightx, rightx + 1)
            rightynext = righty + rightslope * (rightxnext - rightx)
            rightcoeffs = np.array([0, 0, rightslope, rightynext])
            spline.extend(rightcoeffs[..., None], np.r_[rightxnext])

        if method in ['spline', 'linear', 'fourier']:
            xs = st.ndarray[0].astype(np.datetime64)
            if not skipst:
                sttime = np.datetime64(starttime)
            else:
                sttime = xs[0]
            if not skipet:
                entime = np.datetime64(endtime)
            else:
                entime = xs[-1]
            # create a new time scale with sampling rate increment and give start and endtimes
            xnew = np.arange(sttime, entime, np.timedelta64(int(samprate), "us"))
            # now get the indices indbefore and indafter of xnew data just before and the first after xs
            print ("CHECKING HERE", xnew, sttime, entime)
            ab, a, b = np.intersect1d(xnew, xs, return_indices=True)
            print ("CHECKING HERE", a)
            indbefore = a[0]
            indafter = a[-1]
            if debug:
                print(len(xs), len(xnew))
        if method == 'spline':
            for i, ar in enumerate(st.ndarray):
                if i == 0:
                    if indbefore - 1 > 0:
                        ar = np.insert(ar, 0, xnew[0:indbefore - 1])
                    if indafter + 1 >= len(xs):
                        ar = np.insert(ar, -1, xnew[indafter + 1:len(xnew)])
                elif i > 0 and len(ar) > 0:
                    ar = ar.astype(float)
                    # identify and drop nans
                    nonnaninds = np.logical_not(np.isnan(ar))
                    natural = interpolate.CubicSpline(xs[nonnaninds], ar[nonnaninds], bc_type='natural')
                    add_boundary_knots(natural)
                    vals = natural(xnew)
                    if indbefore - 1 > 0:
                        ar = np.insert(ar, 0, vals[0:indbefore - 1])
                    if indafter + 1 >= len(xs):
                        ar = np.insert(ar, -1, vals[indafter + 1:len(xnew)])
                st.ndarray[i] = ar[:-1]
        elif method == 'linear':
            for i, ar in enumerate(st.ndarray):
                if i == 0:
                    if indbefore - 1 > 0:
                        ar = np.insert(ar, 0, xnew[0:indbefore - 1])
                    if indafter + 1 >= len(xs):
                        ar = np.insert(ar, -1, xnew[indafter + 1:len(xnew)])
                elif i > 0 and len(ar) > 0:
                    ar = ar.astype(float)
                    # identify and drop nans
                    nonnaninds = np.logical_not(np.isnan(ar))
                    p = np.polyfit(xs[nonnaninds].astype(float64), ar[nonnaninds], 1)  # find linear trend in x
                    vals = p[0] * xnew.astype(float64) + p[1]
                    if indbefore - 1 > 0:
                        ar = np.insert(ar, 0, vals[0:indbefore - 1])
                    if indafter + 1 >= len(xs):
                        ar = np.insert(ar, -1, vals[indafter + 1:len(xnew)])
                st.ndarray[i] = ar[:-1]
        elif method == 'fourier':
            for i, ar in enumerate(st.ndarray):
                if i == 0:
                    if indbefore - 1 > 0:
                        ar = np.insert(ar, 0, xnew[0:indbefore - 1])
                    if indafter + 1 >= len(xs):
                        ar = np.insert(ar, -1, xnew[indafter + 1:len(xnew)])
                elif i > 0 and len(ar) > 0:
                    ar = ar.astype(float)
                    # identify and drop nans
                    nonnaninds = np.logical_not(np.isnan(ar))
                    t = xs[nonnaninds].astype(float64)
                    y = ar[nonnaninds]
                    p = np.polyfit(t, y, 1)  # find linear trend in x
                    y_notrend = y - p[0] * t  # detrended x
                    y_freqdom = np.fft.fft(y_notrend)  # detrended x in frequency domain
                    f = np.fft.fftfreq(len(t))  # frequencies
                    indexes = range(len(t))
                    # sort indexes by frequency, lower -> higher
                    sorted(indexes, key=lambda i: np.absolute(f[i]))
                    # indexes.sort(key = lambda i: np.absolute(f[i]))
                    tn = xnew.astype(float64)
                    restored_sig = np.zeros(tn.size)
                    for j in indexes[:1 + n_harm * 2]:
                        ampli = np.absolute(y_freqdom[j]) / len(t)  # amplitude
                        phase = np.angle(y_freqdom[j])  # phase
                        restored_sig += ampli * np.cos(2 * np.pi * f[j] * tn + phase)
                    vals = restored_sig + p[0] * tn
                    if indbefore - 1 > 0:
                        ar = np.insert(ar, 0, vals[0:indbefore - 1])
                    if indafter + 1 >= len(xs):
                        ar = np.insert(ar, -1, vals[indafter + 1:len(xnew)])
                st.ndarray[i] = ar[:-1]
        else:
            # method 1 - old (duplicating first and last non-nan value at selected times)
            for i, ar in enumerate(st.ndarray):
                if len(ar) > 0:
                    if i == 0:
                        if not skipst:
                            ar = np.insert(ar, 0, starttime)
                        if not skipet:
                            ar = np.insert(ar, -1, endtime)
                    else:
                        ar = ar.astype(float)
                        if not skipst:
                            firstx = ar[np.isfinite(ar)][0]
                            ar = np.insert(ar, 0, firstx)
                        if not skipet:
                            lastx = ar[np.isfinite(ar)][-1]
                            ar = np.insert(ar, -1, lastx)
                    st.ndarray[i] = ar
        return st.sorting()

    def filter(self,**kwargs):
        """
        DEFINITION:
            Uses a selected window to filter the datastream - similar to the smooth function.
            (take a look at the Scipy Cookbook/Signal Smooth)
            This method is based on the convolution of a scaled window with the signal.
            The signal is prepared by introducing reflected copies of the signal
            (with the window size) in both ends so that transient parts are minimized
            in the beginning and end part of the output signal.
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
                                'parzen','triang','gaussian','wiener','butterworth'
                                See http://docs.scipy.org/doc/scipy/reference/signal.html
            - filter_width:     (timedelta) window width of the filter
            - resample_period:  (int) resampling interval in seconds (e.g. 1 for one second data)
                                 leave blank for standard filters as it will be automatically selected
            - noresample:       (bool) if True the data set is resampled at filter_width positions
            - missingdata:      (string) define how to deal with missing data
                                          'conservative' (default): no filling
                                          'interpolate': interpolate if less than 10% are missing
                                          'mean': use mean if less than 10% are missing'
            - conservative:     (bool) if True than no interpolation is performed
            - autofill:         (list) of keys: provide a keylist for which nan values are linearly interpolated before filtering - use with care, might be useful if you have low resolution parameters asociated with main values like (humidity etc)
            - resampleoffset:   (timedelta) if provided the offset will be added to resamples starttime
            - testplot:         (bool) provides a plot of unfiltered and filtered data for each key if true

        RETURNS:
            - self:             (DataStream) containing the filtered signal within the selected columns

        EXAMPLE:
            nice_data = bad_data.filter(keys=['x','y','z'])
            or
            nice_data = bad_data.filter(filter_type='gaussian',filter_width=timedelta(hours=1),resampleoffset=timedelta(minutes=30))

        APPLICATION:

        TODO:
            !!A proper and correct treatment of gaps within the dataset to be filtered is missing!!

        """

        # ########################
        # Kwargs and definitions
        # ########################
        filterlist = ['flat','barthann','bartlett','blackman','blackmanharris','bohman',
                'boxcar','cosine','flattop','hamming','hann','nuttall','parzen','triang',
                'gaussian','wiener','butterworth']

        # To be added
        #kaiser(M, beta[, sym])         Return a Kaiser window.
        #slepian(M, width[, sym])       Return a digital Slepian (DPSS) window.
        #chebwin(M, at[, sym])  Return a Dolph-Chebyshev window.
        # see http://docs.scipy.org/doc/scipy/reference/signal.html

        keys = kwargs.get('keys')
        filter_type = kwargs.get('filter_type')
        filter_width = kwargs.get('filter_width')
        resample_period = kwargs.get('resample_period')
        noresample = kwargs.get('noresample')
        resampleoffset = kwargs.get('resampleoffset')
        resamplemethod = kwargs.get('resamplemethod')
        testplot = kwargs.get('testplot')
        autofill = kwargs.get('autofill')
        debugmode = kwargs.get('debug')
        missingdata =  kwargs.get('missingdata')

        if not resamplemethod:
            resamplemethod = "3.0"

        if debugmode:
            print ("Running filter...")
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
        if not debugmode:
            debugmode = None
        if not filter_type:
            filter_type = 'gaussian'
        if not missingdata:
            missingdata = 'conservative'

        std = 0
        if debugmode:
            print ("filter: selected the following parameters: filter_type={}, filter_width={}, resample_period={}".format(filter_type,filter_width,resample_period))

        # ########################
        # Basic validity checks and window size definitions
        # ########################
        if not filter_type in filterlist:
            logger.error("filter: Window is none of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman', etc")
            logger.debug("filter: You entered non-existing filter type -  %s  - " % filter_type)
            return self

        logger.info("filter: Filtering with {} window".format(filter_type))

        #print self.length()[0]
        if not self.length()[0] > 1:
            logger.error("Filter: stream needs to contain data - returning.")
            return self

        if debugmode:
            print("Starting length:", self.length())

        # non-destructive
        fstream = self.copy()

        window_period = filter_width.total_seconds()
        si = timedelta(seconds=fstream.get_sampling_period())
        # default - rounding to 0.01 second for LF signals
        sampling_period = si.days*24*3600 + si.seconds + np.round(si.microseconds/1000000.0,2)
        if sampling_period < 0.02:
            # HF signal - 50 Hz or larger
            if debugmode:
                print ("Found HF signal - using full microsecond resolution")
            sampling_period = si.days*24*3600 + si.seconds + np.round(si.microseconds/1000000.0,6)

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
            trangetmp = fstream._det_trange(window_period)*24*3600
            if trangetmp < 1:
                trange = np.round(trangetmp,3)
            else:
                trange = timedelta(seconds=(fstream._det_trange(window_period)*24*3600)).seconds
            if debugmode:
                print("Window character: ", window_len, std, trange)
        else:
            window_len = np.round(window_period/sampling_period)
            if window_len % 2:
                window_len = window_len+1
            trange = window_period/2

        if sampling_period >= window_period:
            logger.warning("Filter: Sampling period is equal or larger then projected filter window - returning unmodified stream.")
            return self

        # ########################
        # Reading data of each selected column in stream
        # ########################

        v = []
        t = []
        if len(fstream.ndarray[0])>0:
            t = fstream.ndarray[0]

        if debugmode:
            print("Length time column:", len(t))

        window_len = int(window_len)

        for key in keys:
            if debugmode:
                print ("Start filtering for", key)
            if not key in self.KEYLIST:
                logger.error("Column key %s not valid." % key)
            keyindex = self.KEYLIST.index(key)
            if len(fstream.ndarray[keyindex])>0:
                v = fstream.ndarray[keyindex]

            # INTERMAGNET 90 percent rule: interpolate missing values if less than 10 percent are missing
            #if not conservative or missingdata in ['interpolate','mean']:
            # missingdata not yet working
            if missingdata in ['interpolate','mean']:
                v = missingvalue(v,window_len=np.round(window_period/sampling_period),fill=missingdata) # using ratio here and not _len

            if key in autofill:
                logger.warning("Filter: key %s has been selected for linear interpolation before filtering." % key)
                logger.warning("Filter: I guess you know what you are doing...")
                nans, x= nan_helper(v)
                v[nans]= np.interp(x(nans), x(~nans), v[~nans])

            # Make sure that we are dealing with numbers
            v = np.array(list(map(float, v)))

            if v.ndim != 1:
                logger.error("Filter: Only accepts 1 dimensional arrays.")
            if window_len<3:
                logger.error("Filter: Window length defined by filter_width needs to cover at least three data points")

            if debugmode:
                print("Treating k:", key, v.size)

            if v.size >= window_len:
                s=np.r_[v[int(window_len)-1:0:-1],v,v[-1:-int(window_len):-1]]

                if filter_type == 'gaussian':
                    w = signal.windows.gaussian(window_len, std=std)
                    y=np.convolve(w/w.sum(),s,mode='valid')
                    res = y[(int(window_len/2)):(len(v)+int(window_len/2))]
                elif filter_type == 'wiener':
                    res = signal.wiener(v, int(window_len), noise=0.5)
                elif filter_type == 'butterworth':
                    # TODO - check
                    # order of 4
                    # frequency limit (1 corresponds to nyquist)
                    nyf = 0.5/sampling_period
                    b, a = signal.butter(4, 1/(window_len*nyf))
                    res = signal.filtfilt(b, a, v)
                elif filter_type == 'flat':
                    w=np.ones(int(window_len),'d')
                    s = np.ma.masked_invalid(s)
                    y=np.convolve(w/w.sum(),s,mode='valid') #'valid')
                    res = y[(int(window_len/2)-1):(len(v)+int(window_len/2)-1)]
                else:
                    w = eval('signal.windows.'+filter_type+'(window_len)')
                    y=np.convolve(w/w.sum(),s,mode='valid')
                    res = y[(int(window_len/2)):(len(v)+int(window_len/2))]

                if testplot == True:
                    fig, ax1 = plt.subplots(1,1, figsize=(10,4))
                    ax1.plot(t, v, 'b.-', linewidth=2, label = 'raw data')
                    ax1.plot(t, res, 'r.-', linewidth=2, label = filter_type)
                    plt.show()
                fstream.ndarray[keyindex] = res

        if resample:
            if debugmode:
                print("Resampling: ", keys, resample_period)
                print(" length before resampling: ", len(fstream))
            fstream = fstream.resample(keys,period=resample_period,offset=resampleoffset,method=resamplemethod)
            if debugmode:
                print(" length after resampling: ", len(fstream))

        # ########################
        # Update header information
        # ########################
        passband = filter_width.total_seconds()
        #print ("passband", 1/passband)
        #self.header['DataSamplingFilter'] = filter_type + ' - ' + str(trange) + ' sec'
        fstream.header['DataSamplingFilter'] = filter_type + ' - ' + str(1.0/float(passband)) + ' Hz'

        return fstream


    def fit(self, keys, **kwargs):
        """
    DEFINITION:
        Code for fitting data. Please note: if nans are present in any of the selected keys
        the whole line is dropped before fitting.

    PARAMETERS:
    Variables:
        - keys:         (list) Provide a list of keys to be fitted (e.g. ['x','y','z'].
    Kwargs:
        - fitfunc:      (str) Options: 'poly', 'harmonic', 'least-squares', 'mean', 'spline', 'none', default='spline'
        - timerange:    (timedelta object) Default = timedelta(hours=1)
        - fitdegree:    (float) Default=5
        - knotstep:     (float < 0.5) determines the amount of knots: amount = 1/knotstep ---> VERY smooth 0.1 | NOT VERY SMOOTH 0.001
        - flag:         (bool).

    RETURNS:
        - function object:      (list) func = [functionkeylist, sv, ev]

    EXAMPLE:
        func = stream.fit(['x'])

    APPLICATION:

        """
        # Defaults:
        fitfunc = kwargs.get('fitfunc')
        fitdegree = kwargs.get('fitdegree')
        knotstep = kwargs.get('knotstep')
        starttime = kwargs.get('starttime')
        endtime = kwargs.get('endtime')
        debug = kwargs.get('debug')
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

        if debug:
            print ("Running fit", len(self), keys)
        if not len(self.ndarray[0]) > 0:
            return self

        fitstream = self.copy()
        if not defaulttime == 2: # TODO if applied to full stream, one point at the end is missing
            fitstream = fitstream.trim(starttime=starttime, endtime=endtime)

        sv = 0
        ev = 0
        for key in keys:
            if debug:
                print ("Fitting key", key)
            tmpst = fitstream._drop_nans(key)
            t = tmpst.ndarray[0]
            nt,sv,ev = normalize(t)
            sp = fitstream.get_sampling_period()/3600./24. # use days because of date2num
            if sp == 0:  ## if no dominant sampling period can be identified then use minutes
                sp = 0.0177083333256
            if not key in self.KEYLIST[1:16]:
                raise ValueError("Column key not valid")
            ind = self.KEYLIST.index(key)
            val = tmpst.ndarray[ind]

            # interpolate NaN values
            # normalized sampling rate
            sp = sp/(ev-sv) # should be the best?
            #sp = (ev-sv)/len(val) # does not work
            x = np.linspace(np.min(nt),np.max(nt),len(fitstream))
            val = np.asarray(val, dtype=float64)
            nt = np.asarray(nt, dtype=float64)

            if len(val)<=1:
                logger.warning('Fit: No valid data for key {}'.format(key))
                break
            elif fitfunc == 'spline':
                knots = []
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
                    raise ValueError("Value error in fit function - not enough data or invalid numbers")
                f_fit = interpolate.splev(x,ti)
            elif fitfunc == 'poly':
                logger.debug('Selected polynomial fit - amount of data: %d, time steps: %d, degree of fit: %d' % (len(nt), len(val), fitdegree))
                ti = polyfit(nt, val, fitdegree)
                f_fit = polyval(ti,x)
            elif fitfunc == 'mean':
                logger.debug('Selected mean fit - amount of data: {}, time steps: {}'.format(len(nt), len(val)))
                meanvalue = np.nanmean(val)
                meanval = np.asarray([meanvalue for el in val])
                ti = polyfit(nt, meanval, 1)
                f_fit = polyval(ti,x)
            elif fitfunc == 'harmonic':
                logger.debug('Selected harmonic fit - using inverse fourier transform')
                f_fit = self.harmfit(nt, val, fitdegree)
                # Don't use resampled list for harmonic time series
                x = nt
            elif fitfunc == 'least-squares':
                logger.debug('Selected linear least-squares fit')
                A = np.vstack([nt, np.ones(len(nt))]).T
                m, c, = np.linalg.lstsq(A, val,rcond=None)[0]
                f_fit = m * x + c
            elif fitfunc == 'none':
                logger.debug('Selected no fit')
                return
            else:
                logger.warning('Fit: function not valid')
                return
            #exec('f'+key+' = interpolate.interp1d(x, f_fit, bounds_error=False)')
            #exec('functionkeylist["f'+key+'"] = f'+key)
            functionkeylist['f' + key] = interpolate.interp1d(x, f_fit, bounds_error=False)

        #if tok:
        func = [functionkeylist, sv, ev, fitfunc, fitdegree, knotstep, starttime, endtime, keys]
        #func = [functionkeylist, sv, ev]
        #else:
        funcnew = {"keys":keys, "fitfunc":fitfunc,"fitdegree":fitdegree, "knotstep":knotstep, "starttime":starttime,"endtime":endtime, "functionlist":functionkeylist, "sv":sv, "ev":ev}
        #    func = [functionkeylist, 0, 0]
        return func

    @deprecated("replaced by core.flagging flag_range")
    def flag_range(self, **kwargs):

        keys = kwargs.get('keys')
        above = kwargs.get('above')
        below = kwargs.get('below')
        starttime = kwargs.get('starttime')
        endtime = kwargs.get('endtime')
        text = kwargs.get('text')
        flagnum = kwargs.get('flagnum')
        keystoflag = kwargs.get('keystoflag')
        from magpy.core import flagging
        fl = flagging.flag_range(self,keys=keys, above=above, below=below, starttime=starttime, endtime=endtime, flagtype=flagnum, labelid='002',
                   keystoflag=keystoflag, text=text)
        return fl

    @deprecated("replaced by core.flagging flag_outlier")
    def flag_outlier(self, **kwargs):
        # Defaults:
        timerange = kwargs.get('timerange')
        threshold = kwargs.get('threshold')
        keys = kwargs.get('keys')
        markall = kwargs.get('markall')
        stdout = kwargs.get('stdout')
        returnflaglist = kwargs.get('returnflaglist')

        from magpy.core import flagging
        fl = flagging.flag_outlier(self,keys=keys, threshold=threshold, timerange=timerange, markall=markall)
        return fl


    @deprecated("replaced by core.flagging apply_flags")
    def flag(self, flaglist, removeduplicates=False, debug=False):
        data = flaglist.apply_flags(self, mode='insert')
        return data


    @deprecated("replaced by core.flagging convert_to_stream")
    def stream2flaglist(self, userange=True, flagnumber=None, keystoflag=None, sensorid=None, comment=None):
        from magpy.core import flagging
        commentkeys = comment.split(',')
        fl = flagging.convert_to_flags(self, flagtype=flagnumber, labelid='030', sensorid=sensorid, keystoflag=keystoflag, commentkeys=commentkeys, groups=None)
        return fl


    def simplebasevalue2stream(self,basevalue,basecomp="HDZ",**kwargs):
        """
      DESCRIPTION:
        simple baselvalue correction using a list with three directional basevalues

      PARAMETERS:
        basevalue       (list): [baseH,baseD,baseZ]
        keys            (list): default = 'x','y','z'
        basecomp        (i.e. HDZ): HDZ will transform the stream for HDZ correction

      APPLICTAION:
        used by stream.baseline

        """
        mode = kwargs.get('mode')
        keys = ['x','y','z']

        # Changed that - 49 sec before, no less then 2 secs
        if not len(self.ndarray[0]) > 0:
            print("simplebasevalue2stream: requires ndarray")
            return self

        if not len(keys) == 3:
            print ("simplebaseline corr: wrong key length")
            return self
        arrayx = np.asarray(list(self.ndarray[self.KEYLIST.index(keys[0])])).astype(float)
        arrayy = np.asarray(list(self.ndarray[self.KEYLIST.index(keys[1])])).astype(float)
        arrayz = np.asarray(list(self.ndarray[self.KEYLIST.index(keys[2])])).astype(float)

        if basecomp in ["HDZ","hdz"]:
            print ("simplebaseline: Basevalues are provided as HDZ components")
        else:
            print ("simplebaseline: Basevalues are provided as XYZ components")

        #1. calculate function value for each data time step
        array = [[] for key in self.KEYLIST]
        array[0] = self.ndarray[0]
        for key in self.KEYLIST:
            ind = self.KEYLIST.index(key)
            if key in keys: # new
                if key == 'x' and basecomp in ["HDZ","hdz"]:
                    array[ind] = np.sqrt((arrayx + basevalue[keys.index(key)])**2 + arrayy**2)
                elif key == 'y' and basecomp in ["HDZ","hdz"]:
                    array[ind] = np.arctan2(arrayy, (arrayx + basevalue[0])) * 180. / np.pi + basevalue[keys.index(key)]
                    self.header['col-y'] = 'd'
                    self.header['unit-col-y'] = 'deg'
                else:
                    # will also be used if basevalues are not HDZ
                    array[ind] = self.ndarray[ind].astype(float) + basevalue[keys.index(key)]
            else:
                if len(self.ndarray[ind]) > 0:
                    array[ind] = self.ndarray[ind].astype(object)

        if basecomp in ["HDZ","hdz"]:
            self.header['DataComponents'] = 'HDZ'
        return DataStream(header=self.header,ndarray=np.asarray(array, dtype=object))


    def func2stream(self,funclist, keys=None, **kwargs):
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
                                  mode 'addbaseline' used by the baseline method

      APPLICTAION:
        used by stream.baseline

        """
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
            funct = funclist

        totalarray = [[] for key in self.KEYLIST]
        posstr = self.KEYLIST.index('str1')
        testx = []
        basex = np.asarray([])
        arrayx = np.asarray([])
        arrayy = np.asarray([])

        # required for addbaseline option
        if mode == 'addbaseline':
            arrayx = np.asarray(list(self.ndarray[self.KEYLIST.index(keys[0])])).astype(float)
            arrayy = np.asarray(list(self.ndarray[self.KEYLIST.index(keys[1])])).astype(float)
            arrayz = np.asarray(list(self.ndarray[self.KEYLIST.index(keys[2])])).astype(float)

        for function in funct:
            if not function:
                return self
            if not len(self.ndarray[0]) > 0:
                return self

            #1. calculate function value for each data time step
            array = [[] for key in self.KEYLIST]
            array[0] = self.ndarray[0]
            dis_done = False
            # get x array for baseline
            functimearray = (date2num(self.ndarray[0]).astype(float)-function[1])/(function[2]-function[1])
            for key in self.KEYLIST:
                validkey = False
                ind = self.KEYLIST.index(key)
                if key in keys: # new
                    keyind = keys.index(key)
                    if fkeys:
                        fkey = fkeys[keyind]
                    else:
                        fkey = key
                    ar = np.asarray(self.ndarray[ind]).astype(float)
                    try:
                        test = function[0]['f'+fkey](functimearray)
                        validkey = True
                    except:
                        pass
                    if mode == 'add' and validkey:
                        array[ind] = ar + function[0]['f'+fkey](functimearray)
                    elif mode == 'addbaseline' and validkey:
                        if key == 'x':
                            basex = function[0]['f'+fkey](functimearray)
                            array[ind] = np.sqrt((arrayx + basex) ** 2 + arrayy ** 2)
                            #print ("X", array[ind])
                        elif key == 'y':
                            array[ind] = np.arctan2(arrayy, (arrayx + basex)) * 180. / np.pi + function[0]['f'+fkey](functimearray)
                            #print ("Y", array[ind])
                            self.header['col-y'] = 'd'
                            self.header['unit-col-y'] = 'deg'
                        else:
                            # will also be used if basevalues are not HDZ
                            array[ind] = ar + function[0]['f'+fkey](functimearray)
                            if len(array[posstr]) == 0:
                                #print ("Assigned values to str1: function {}".format(function[1]))
                                array[posstr] = np.asarray(['c']*len(ar))
                            if len(basex) > 0 and not dis_done:
                                # identify change from number to nan
                                # add discontinuity marker there
                                prevel = np.nan
                                for idx, el in enumerate(basex):
                                    if not np.isnan(prevel) and np.isnan(el):
                                        array[posstr][idx] = 'd'
                                        #print ("Modified str1 at {}".format(idx))
                                        break
                                    prevel = el
                                dis_done = True
                            #if key == 'x': # remember this for correct y determination
                            #    arrayx = array[ind]
                            #    testx = function[0]['f'+fkey](functimearray)
                            if key == 'dx': # use this column to test if delta values are already provided
                                basex = function[0]['f'+fkey](functimearray)
                    elif mode in ['sub','subtract'] and validkey:
                        array[ind] = ar - function[0]['f'+fkey](functimearray)
                    elif mode == 'values' and validkey:
                        array[ind] = function[0]['f'+fkey](functimearray)
                    elif mode == 'div' and validkey:
                        array[ind] = ar / function[0]['f'+fkey](functimearray)
                    elif mode == 'multiply' and validkey:
                        array[ind] = ar * function[0]['f'+fkey](functimearray)
                    elif validkey:
                        print("func2stream: mode not recognized")
                else: # new
                    if len(self.ndarray[ind]) > 0:
                        array[ind] = np.asarray(self.ndarray[ind]).astype(object)

            for idx, col in enumerate(array):
                if len(totalarray[idx]) > 0 and not idx == 0:
                    totalcol = totalarray[idx]
                    for j,el in enumerate(col):
                        if idx < len(self.NUMKEYLIST)+1 and not np.isnan(el) and np.isnan(totalcol[j]):
                            totalarray[idx][j] = array[idx][j]
                        if idx > len(self.NUMKEYLIST) and not el == 'c' and totalcol[j] == 'c':
                            totalarray[idx][j] = 'd'
                else:
                    totalarray[idx] = array[idx]

        return DataStream(header=self.header,ndarray=np.asarray(totalarray,dtype=object))


    @deprecated("Apparently unsued - remove in 2.1")
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
            array = [[] for key in self.KEYLIST]
            array[0] = self.ndarray[0]
            functimearray = (self.ndarray[0].astype(float)-function[1])/(function[2]-function[1])
            #print functimearray
            for key in keys:
                ind = self.KEYLIST.index(key)
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

            return DataStream(self,self.header,np.asarray(array,dtype=object))

        return self


    def func2header(self,funclist,debug=False):
        """
        DESCRIPTION
            Add a list of functions into the data header
        """

        if isinstance(funclist[0], dict):
            funct = [funclist]
        else:
            funct = funclist

        self.header['DataFunctionObject'] = funct

        return self


    def get_fmi_array(self, missing_data=None, debug=False):
        """
        DESCRIPTION
            Extracts x and y lists from datastream, which are directly useable for K fmi algorythm.
            Please make sure to provide an appropriate datastream. Eventually use hdz2xyz conversion.
            Required sampling resolution is one-minute. If HF data is provided this data is filtered to
            one-minute without any missingdata option.
            This method is called by the K_fmi_index method of the core.activity modul.
        OPTIONS:
            missing_data  :  define a value to replace np.nans as used by MagPy
        RETURNS
            datalist      :  containing shifting lists of 3-day length with time, x and y
                          :  please note: x and y are returned in 0.1 nT resolution
            samplingrate  :  in seconds
            k9_limit      : if contained within the data header, else 0
        VERSION
            part of MagPy2.0.0 onwards
        APPLICTAION
            l, s, k = get_fmi_array(teststream, debug=True)
        """
        datastream = self.copy()
        sr = datastream.samplingrate()
        amount = len(datastream)
        effectivelength = amount * sr / 60.
        if debug:
            print("Found data of length {} with a sampling rate of {} sec".format(amount, sr))
        if not effectivelength >= 4319.9:
            print("datastream is too short - need three full days")
            return [[[], [], []]], 0, 0
        # check for x and y
        if not len(datastream.ndarray[1]) > 0:
            print("apparently no x data - aborting")
            return [[[], [], []]], 0, 0
        if not len(datastream.ndarray[2]) > 0:
            print("apparently no y data - aborting")
            return [[[], [], []]], 0, 0
        if sr > 60.1:
            # sampling rate needs to be minutes at least
            return [[[], []]], 0, 0
        elif sr < 59.9:
            if debug:
                print("Filtering ...")
            datastream = datastream.get_gaps()
            datastream = datastream.filter(filter_type='gaussian', filter_width=timedelta(seconds=120),
                                           resample_period=60.0)
            sr = datastream.samplingrate()
            amount = len(datastream)
            if debug:
                print(" after filtering: Data of length {} with a sampling rate of {} sec".format(amount, sr))
        datastream = datastream.get_gaps()
        k9_limit = datastream.header.get('K9_limit', 0)
        # get first beginning of day from array
        t = datastream.ndarray[0]
        x = datastream.ndarray[1]
        y = datastream.ndarray[2]
        if missing_data:
            if debug:
                print ("Replacing missing data with {}".format(missing_data))
        dayt = np.asarray([el.date() for el in t])
        u, indices, count = np.unique(dayt, return_index=True, return_counts=True)
        # get indicies for all count == 1440
        validcounts = np.argwhere(count == 1440)
        validindices = np.ndarray.flatten(indices[validcounts])

        # extrect indiceranges from validindices
        def shifting_windows(thelist, size):
            return [thelist[x:x + size] for x in range(len(thelist) - size + 1)]

        indranges = shifting_windows(validindices, 3)
        fulllist = []
        for r in indranges:
            l = r[-1] + 1440 - r[0]
            if l == 4320:
                ar = range(r[0], r[-1] + 1440)
                tr = t[ar]
                xr = x[ar].astype(float64)*10
                yr = y[ar].astype(float64)*10
                if missing_data:
                    xr = missingvalue(xr,fill='value',fillvalue=missing_data)
                    yr = missingvalue(yr, fill='value', fillvalue=missing_data)
                partlist = [tr, xr, yr]
                fulllist.append(partlist)

        return fulllist, sr, k9_limit


    def get_key_name(self,key):
        """
        DESCRIPTION
           get the content name of a specific key
           will scan header information until successful:
           (1) col-"key" names
           (2) ColumnContent header info
           (3) SensorElements header info
           if no Name for the key is found, then the key itself is returned
        APPLICATION:
           element = datastream.GetKeyName('var1')
        """
        if not key in self.KEYLIST:
            print ("key not in KEYLIST - aborting")
            return ''
        element = ''
        # One
        try:
            element = self.header.get("col-{}".format(key))
            if not element == '':
                return element
        except:
            pass

        # Two
        try:
            element = self.header.get('ColumnContents','').split(',')[self.KEYLIST.index(key)]
            if not element == '':
                return element
        except:
            pass

        # Three
        try:
            idx = self.header.get('SensorKeys','').split(',').index(key)
            element = self.header.get('SensorElements','').split(',')[idx]
            if not element == '':
                return element
        except:
            pass

        return key

    @deprecated("Replaced by get_key_name")
    def GetKeyName(self,key):
        return self.get_key_name(key)


    def get_key_unit(self,key):
        """
        DESCRIPTION
           get the content name of a specific key
           will scan header information until successful:
           (1) unit-col-"key" names
           (2) ColumnUnit header info
           if no unit for the key is found, then an empty string is returned
        APPLICATION:
           unit = datastream.GetKeyUnit('var1')
        """
        if not key in self.KEYLIST:
            print ("key not in KEYLIST - aborting")
            return ''
        unit = ''
        # One
        try:
            unit = self.header.get("unit-col-{}".format(key))
            if not unit == '':
                return unit
        except:
            pass

        # Two
        try:
            unit = self.header.get('ColumnUnits','').split(',')[self.KEYLIST.index(key)]
            if not unit == '':
                return unit
        except:
            pass

        return unit


    @deprecated("Replaced by get_key_unit")
    def GetKeyUnit(self,key):
        return self.get_key_unit(key)


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
        stream_with_gaps_filled = stream_with_gaps.get_gaps(key='f')

    APPLICATION:
    CHANGES:
        """

        accuracy = kwargs.get('accuracy')
        key = kwargs.get('key')
        gapvariable = kwargs.get('gapvariable')
        debug = kwargs.get('debug')

        if key in self.KEYLIST:
            gapvariable = True

        if not gapvariable:
            gapvariable = 'var5'

        if not self.length()[0] > 1:
            print ("get_gaps: Stream does not contain data - aborting")
            return self

        # Better use get_sampling period as samplingrate is rounded
        newsps = self.samplingrate()

        if not accuracy:
            #accuracy = 0.9/(3600.0*24.0) # one second relative to day
            accuracy = 0.05*newsps # 5 percent of samplingrate

        if newsps < 0.9 and not accuracy:
            accuracy = (newsps-(newsps*0.1))

        logger.info('--- Starting filling gaps with NANs at %s ' % (str(datetime.now())))

        stream = self.copy()
        prevtime = 0

        if not len(stream.ndarray[0]) > 0:
            return stream
        mintime,maxtime = stream._find_t_limits()
        length = len(stream.ndarray[0])
        sourcetime = stream.ndarray[0]

        if debug:
            print("Time range:", mintime, maxtime)
            print("Length, samp_per, and accuracy:", self.length()[0], newsps, accuracy)

        shift = 0
        lenelem = 0
        # Get time diff and expected count
        timediff = (maxtime - mintime).total_seconds()
        expN = int(round(timediff/newsps))+1
        if debug:
            print("get_gaps: Expected length vs actual length:", expN, length)
        if expN == len(sourcetime):
            # Found the expected amount of time steps - no gaps
            logger.info("get_gaps: No gaps found - Returning")
            return stream
        else:
            diff = (sourcetime[1:] - sourcetime[:-1])
            diff = np.array([el.total_seconds() for el in diff])
            num_fills = np.round(diff / newsps) - 1
            getdiffids = np.where(diff > newsps+accuracy)[0]
            logger.info("get_gaps: Found gaps - Filling nans to them")
            if debug:
                    print ("get_gaps:", diff, num_fills, newsps, getdiffids)
            missingt = []
            # Get critical differences and number of missing steps
            for i in getdiffids:
                    #print (i,  sourcetime[i-1], sourcetime[i], sourcetime[i+1])
                    nf = num_fills[i]
                    # if nf is larger than zero then get append the missing time steps to missingt list
                    if nf > 0:
                        for n in range(int(nf)): # add n+1 * samplingrate for each missing value
                            missingt.append(sourcetime[i]+timedelta(seconds=(n+1)*newsps))

            logger.info("Filling {} gaps".format(len(missingt)))
            if debug:
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
                        if self.KEYLIST[idx] in self.NUMKEYLIST or self.KEYLIST[idx] == 'sectime':
                            elem.extend(nans)
                        else:
                            elem.extend(empts)
                        stream.ndarray[idx] = np.asarray(elem).astype(object)
                    elif self.KEYLIST[idx] == gapvariable:
                        # append nans list to array element
                        elem = [1.0]*lenelem
                        elem.extend(gaps)
                        stream.ndarray[idx] = np.asarray(elem).astype(object)

        logger.info('--- Filling gaps finished at %s ' % (str(datetime.now())))

        return stream.sorting()


    def determine_rotationangles(data, referenceD=0.0, referenceI=None, keys=None, debug=False):
        """
        DESCRIPTION:
            "Estimating" the rotation angles alpha, beta and gamma relative to a magnetic
            coordinate system defined by expected Declination, expected Inclination
            and Intensity F, assuming F is identical. Please note: You need to provide a
            field vector with similar intensity as the reference field.
        VARIABLES:
            datastream : MagPy Datastream containing the vector data
            expectedD  : expected Declination of the reference field
            expectedI  : expected Inclination of the reference field, if Null/None than 0.0 will be returned
            keys       : the three keys in which the vector is stored
        RETURNS:
            alpha, beta : (float) The estimated rotation angles in degree
        """

        def unit_vector(vector):
            """ Returns the unit vector of the vector."""
            return vector / np.linalg.norm(vector)

        def reference_vector(D, I, F=1):
            # H = F cos(I) , X = H cos(D) , Y = H sin(D) , Z = F sin(I)
            return [np.cos(D) * np.cos(I), np.cos(I) * np.sin(D), sin(I)]

        def find_vertical_vector(vector):
            ez = np.array([0, 0, 1])
            search_vector = unit_vector(vector)
            per_vector = unit_vector(ez - np.dot(search_vector, ez) * search_vector)
            return per_vector

        def calc_rotation_matrix(v1start, v2start, v1target, v2target):
            """
            DESCRIPTION
               calculating the rotation matrix
            """

            def get_base_matrices():
                u1start = unit_vector(v1start)
                u2start = unit_vector(v2start)
                u3start = unit_vector(np.cross(u1start, u2start))
                u1target = unit_vector(v1target)
                u2target = unit_vector(v2target)
                u3target = unit_vector(np.cross(u1target, u2target))
                U = np.hstack([u1start.reshape(3, 1), u2start.reshape(3, 1), u3start.reshape(3, 1)])
                V = np.hstack([u1target.reshape(3, 1), u2target.reshape(3, 1), u3target.reshape(3, 1)])
                return U, V

            def calc_base_transition_matrix():
                return np.dot(V, np.linalg.inv(U))

            U, V = get_base_matrices()
            return calc_base_transition_matrix()

        def get_euler_rotation_angles(start_vector, target_vector, start_up_vector=None, target_up_vector=None,
                                      debug=False):
            if start_up_vector is None:
                start_up_vector = find_vertical_vector(start_vector)
            if target_up_vector is None:
                target_up_vector = find_vertical_vector(target_vector)

            rot_mat = calc_rotation_matrix(start_vector, start_up_vector, target_vector, target_up_vector)
            is_equal = np.allclose(rot_mat @ start_vector, target_vector, atol=1e-03)
            if debug:
                print(f"rot_mat @ start_look_at_vector1 == target_look_at_vector1 is {is_equal}")
            rotation = Rotation.from_matrix(rot_mat)
            return rotation.as_euler(seq="zyx", degrees=True)

        alpha = 0.0
        beta = 0.0
        if not keys:
            keys = ['x', 'y', 'z']

        if not len(keys) == 3:
            logger.error('get_rotation: provided keylist need to have three components.')
            return 0.0, 0.0

        refD = referenceD * np.pi / 180.
        if referenceI:
            refI = referenceI * np.pi / 180.

        logger.info(
            'get_rotation: Determining rotation angle towards a magnetic coordinate system assuming z to be vertical down.')

        ind1 = data.KEYLIST.index(keys[0])
        ind2 = data.KEYLIST.index(keys[1])
        ind3 = data.KEYLIST.index(keys[2])

        if len(data.ndarray[0]) > 0:
            if len(data.ndarray[ind1]) > 0 and len(data.ndarray[ind2]) > 0 and len(data.ndarray[ind3]) > 0:
                # get mean disregarding nans
                xl = [el for el in data.ndarray[ind1] if not np.isnan(el)]
                yl = [el for el in data.ndarray[ind2] if not np.isnan(el)]
                zl = [el for el in data.ndarray[ind3] if not np.isnan(el)]
                meanx = np.mean(xl)
                meany = np.mean(yl)
                meanz = np.mean(zl)
                meanh = np.sqrt(meanx * meanx + meany * meany)
                meanf = np.sqrt(meanx * meanx + meany * meany + meanz * meanz)
                meaninc = np.arcsin(meanz / meanf)
                if not referenceI:
                    refI = 0
                    meanz = 0
                meanvec = [meanx, meany, meanz]
                refvec = reference_vector(refD, refI)

                alpha, beta, gamma = get_euler_rotation_angles(np.array(meanvec), np.array(refvec))

                if debug:
                    print("get_rotation debug: means of x={}, y={}, z={} and h={}".format(meanx, meany, meanz, meanh))

        logger.info(
            'getrotation: Rotation angles determined: alpha={}deg, beta={}deg, gamma={}deg'.format(alpha, beta, gamma))

        return alpha, beta, gamma


    """
    def determine_rotationangles(self, referenceD=0.0, referenceI=None, keys = None, debug=False):
        #""
        DESCRIPTION:
            "Estimating" the rotation angle alpha and beta relative to a magnetic
            coordinate system defined by expected Declination, expected Inclination
            and Intensity F, assuming F is identical. Please note: You need to provide a
            field vector with similar intensity as the reference field. A reference inclination
            of 0 is not possible. Please provide a very small value if you need to do that.
        VARIABLES:
            datastream : MagPy Datastream containing the vector data
            expectedD  : expected Declination of the reference field
            expectedI  : expected Inclination of the reference field, if Null/None than 0.0 will be returned
            keys       : the three keys in which the vector is stored
        RETURNS:
            alpha, beta : (float) The estimated rotation angles in degree
        #""
        alpha = 0.0
        beta = 0.0
        if not keys:
            keys = ['x','y','z']

        if not len(keys) == 3:
            logger.error('get_rotation: provided keylist need to have three components.')
            return 0.0,0.0

        logger.info('get_rotation: Determining rotation angle towards a magnetic coordinate system assuming z to be vertical down.')

        ind1 = self.KEYLIST.index(keys[0])
        ind2 = self.KEYLIST.index(keys[1])
        ind3 = self.KEYLIST.index(keys[2])

        if len(self.ndarray[0]) > 0:
            if len(self.ndarray[ind1]) > 0 and len(self.ndarray[ind2]) > 0 and len(self.ndarray[ind3]) > 0:
                # get mean disregarding nans
                xl = [el for el in self.ndarray[ind1] if not np.isnan(el)]
                yl = [el for el in self.ndarray[ind2] if not np.isnan(el)]
                zl = [el for el in self.ndarray[ind3] if not np.isnan(el)]
                meanx = np.mean(xl)
                meany = np.mean(yl)
                meanz = np.mean(zl)
                meanh = np.sqrt(meanx*meanx + meany*meany)
                if debug:
                    print ("get_rotation debug: means of x={}, y={}, z={} and h={}".format(meanx,meany,meanz,meanh))
                alpha = np.arctan2(-meany,meanx) * (180.) / np.pi - referenceD
                if debug:
                    print ("get_rotation debug: alpha={}".format(alpha))
                if referenceI:
                    beta = np.arctan2(meanz,meanh) * (180.) / np.pi  - referenceI
                    if debug:
                        print ("get_rotation debug: beta={}".format(beta))
                else:
                    if debug:
                        print ("get_rotation debug: no reference Inclination given. Therefore beta={}".format(beta))

        logger.info('getrotation: Rotation angles determined: alpha={}deg, beta={}deg'.format(alpha,beta))

        return alpha, beta
    """

    @deprecated("Replaced by determine_rotationangles")
    def get_rotation(self, referenceD=0.0, referenceI=None, keys=None, debug=False):
        if not keys:
            keys = ['x', 'y', 'z']
        return self.get_rotation(referenceD=referenceD, referenceI=referenceI, keys=keys, debug=debug)


    def get_sampling_period(self):
        """
        returns the dominant sampling period in seconds

        for time savings, this function only tests the first 1000 elements
        """

        # For proper applictation - duplicates are removed
        timecol=[]

        if len(self.ndarray[0]) > 0:
            if not isinstance(self.ndarray[0][0], (datetime,datetime64)):
                timecol = np.array(num2date(self.ndarray[0]))
            else:
                timecol = np.array(self.ndarray[0])

        # New way:
        if len(timecol) > 1:
            try: # will fail for python 3.7
                newtd = np.nanmedian(np.diff(timecol))
                # also get the mean
                meand = np.nanmean(np.diff(timecol))
            except:
                newtd = np.median(np.diff(timecol))
                # also get the mean
                meand = np.mean(np.diff(timecol))
            # if the mean is significantly larger than the median (might happen in heavily
            # unevenly spaced series like DI measurements) then better use mean
            # otherwise median will return the short distance but does not reveal a weekly repetition
            if meand > newtd * 1000:
                newtd = meand
            return newtd.total_seconds()
        else:
            return 0.0

    def samplingrate(self, digits=None, notrounded=False, recalculate=False, debug=False):
        """
        DEFINITION:
            returns a rounded value of the sampling rate
            in seconds
            and updates the header information
        """

        if not digits:
            digits = 1

        if not self.length()[0] > 1:
            return 0.0

        sr = None
        if self.header.get("DataSamplingRate",""):
            src = self.header.get("DataSamplingRate")
            if is_number(src):
                sr = float(src)
            else:
                sr = float(self.header.get("DataSamplingRate").strip(" sec"))
        if not sr or recalculate:
            if debug:
                print("Recalculating samplingrate")
            sr = self.get_sampling_period()
            logger.info("sampling rate: Calculated sampling rate {}".format(sr))
            if debug:
                print("sampling rate: Calculated sampling rate {}".format(sr))

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
        if notrounded:
            val = sr

        self.header['DataSamplingRate'] = float("{0:.3f}".format(val))

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

        array = [[] for key in self.KEYLIST]
        ndtype = False
        if len(self.ndarray[0])>0:
            t = linspace(0,1,len(self.ndarray[0]))
            #t = self.ndarray[0].astype('datetime64[us]').astype(float64)/1000000.
            array[0] = self.ndarray[0]
        else:
            return self
        for key in keys:
            ind = self.KEYLIST.index(key)
            val = np.asarray(self.ndarray[ind],dtype=float)
            ninds = np.isnan(val)
            nind = np.array(range(0,len(val)))[ninds]
            msk = np.logical_not(ninds)
            mval = val[msk]
            mt = t[msk]
            array[ind] = val
            dval = integrate.cumulative_trapezoid(mval,mt)
            dval = np.insert(dval, 0, 0) # Prepend 0 to maintain original length
            for n in nind:
                dval = np.insert(dval, n, np.nan) # Insert nans at original position
            ind = self.KEYLIST.index('d'+key)
            array[ind] = np.asarray(dval)

        self.ndarray = np.asarray(array, dtype=object)
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

    RETURNS:
        - func:         (list) Contains the following:
                        list[0]:        (dict) {'f+key': interpolate function}
                        list[1]:        (float) date2num value of minimum timestamp
                        list[2]:        (float) date2num value of maximum timestamp

    EXAMPLE:
        int_func = pos_data.interpol(['f'])

    APPLICATION:
        used by resample, subtract and merge_streams, as well as some func2stream methods
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
        nt,sv,ev = normalize(t)
        sp = self.get_sampling_period()
        functionkeylist = {}
        f = {}

        logger.info("interpol: Interpolating stream with %s interpolation." % kind)

        for key in keys:
            if not key in self.NUMKEYLIST:
                logger.error("interpol: Column key not valid!")
            if ndtype:
                ind = self.KEYLIST.index(key)
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
                # Leave the following two line as an example how NOT to do it!
                # This was my own code 15 years ago and I am wondering about myself ;)
                #exec('f'+key+' = interpolate.interp1d(nt, val, kind)')
                #exec('functionkeylist["f'+key+'"] = f'+key)
                functionkeylist['f'+key] = interpolate.interp1d(nt, val, kind)
            else:
                logger.warning("interpol: interpolation of zero length data set - wont work.")
                pass

        logger.info("interpol: Interpolation complete.")

        #func = [functionkeylist, sv, ev]
        func = [functionkeylist, sv, ev, kind, None, None, self.start(), self.end(), keys]
        funcnew = {"keys":keys, "fitfunc":kind,"fitdegree":None, "knotstep":None, "starttime":self.start(),"endtime":self.end(), "functionlist":functionkeylist, "sv":sv, "ev":ev}


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
        stream = self.copy()
        # Drop columns containing only nan as they cannot be interpolated
        stream = stream._remove_nancolumns()
        newkeys = stream.variables()
        for key in keys:
            if key in newkeys:
                if key not in stream.NUMKEYLIST:
                    logger.error("interpolate_nans: {} is an invalid key! Cannot interpolate.".format(key))
                y = stream._get_column(key)
                nans, x = nan_helper(y)
                y[nans] = np.interp(x(nans), x(~nans), y[~nans])
                stream._put_column(y, key)
                logger.info("interpolate_nans: Replaced nans in {} with linearly interpolated values.".format(key))
        return stream


    @deprecated("Can not be used with new K_fmi technique")
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

        indvar1 = self.KEYLIST.index('var1')
        indvar2 = self.KEYLIST.index('var2')
        ar = []
        k = 0
        for elem in self.ndarray[indvar2]:
            for count,val in enumerate(newlst):
                if elem > val:
                    k = klst[count]
            ar.append(k)
        self.ndarray[indvar1] = np.asarray(ar)

        return self


    @deprecated("Please remove any usages of this method - linestruct is solely used internally by the absolutes package")
    def linestruct2ndarray(self):
        """
    DEFINITION:
        Converts linestruct data to ndarray.
        Requires get_column and get_key_headers on a linestruct
    RETURNS:
        - self with ndarray filled
    EXAMPLE:
        data = data.linestruct2ndarray()

    APPLICATION:
        """
        def checkEqual3(lst):
            return lst[1:] == lst[:-1]

        array = [np.asarray([]) for elem in self.KEYLIST]

        keys = self._get_key_headers()

        t = np.asarray(self._get_column('time'))
        array[0] = t
        for key in keys:
            ind = self.KEYLIST.index(key)
            col = self._get_column(key)
            if len(col) > 0:
                if not False in checkEqual3(col) and str(col[0]) == str('-'):
                    col = np.asarray([])
                array[ind] = col
            else:
                array[ind] = []

        array = np.asarray(array,dtype=object)
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
                        if key contains "time" then an average numerical time is returned
                        use num2date(value) to convert to datetime
    Kwargs:
        - percentage:   (int) Define required percentage of non-nan values, if not
                               met that nan will be returned. Default is 95 (%)
        - meanfunction: (string) accepts 'mean' and 'median'. Default is 'mean'
        - std:          (bool) if true, the standard deviation is returned as well

    RETURNS:
        - mean/median(, std) (float)
    EXAMPLE:
        meanx = datastream.mean('x',meanfunction='median',percentage=90)

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
        res = np.nan
        erro = np.nan

        if key in self.NUMKEYLIST or key.find("time") >= 0:
            col = self._get_column(key)
            if key.find("time") >= 0:
                col = date2num(col).astype(float)
            if len(col) > 0:
                nans = np.count_nonzero(np.isnan(col))
                perc = (len(col)-nans)/len(col)*100.
                if perc >= percentage:
                    col = col[~np.isnan(col)]
                    if meanfunction == 'median':
                        res = np.nanmedian(col)
                        erro = scipy.stats.median_abs_deviation(col)
                    else:
                        res = np.nanmean(col)
                        erro = np.std(col)
                else:
                    print('mean: Too many nans in column {}, exceeding {} percent ({:.1f})'.format(key, 100-percentage, 100-perc))
                    logger.info('mean: Too many nans in column {}, exceeding {} percent ({:.1f})'.format(key, 100-percentage, 100-perc))

        if key.find("time") >= 0 and np.isnan(res):
            res = num2date(res)

        if std:
            return res, erro
        else:
            return res


    def modwt_calc(self,key='x',wavelet='haar',level=1,plot=False,outfile=None,
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
        - plot (defunc):  (bool) If True, will display a plot of A3, D1, D2 and D3.
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
        DWT_stream = stream.modwt_calc()

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
        t_ind = self.KEYLIST.index('time')

        #MODWT_stream = DataStream([],{})
        MODWT_stream = DataStream()
        headers = MODWT_stream.header
        array = [[] for key in self.KEYLIST]
        x_ind = self.KEYLIST.index('x')
        dx_ind = self.KEYLIST.index('dx')
        var1_ind = self.KEYLIST.index('var1')
        var2_ind = self.KEYLIST.index('var2')
        var3_ind = self.KEYLIST.index('var3')
        var4_ind = self.KEYLIST.index('var4')
        var5_ind = self.KEYLIST.index('var5')
        dy_ind = self.KEYLIST.index('dy')
        i = 0
        logger.info("MODWT_calc: Starting Discrete Wavelet Transform of key %s." % key)

        if len(data) % 2 == 1:
            data = data[0:-1]

        # Results have format:
        # (cAn, cDn), ..., (cA2, cD2), (cA1, cD1)
        coeffs = pywt.swt(data, wavelet, level)
        acoeffs, dcoeffs = [], []
        for i in range(level):
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
            array[t_ind].append(self.ndarray[t_ind][int(i+window/2)])
            data_cut = data[i:i+window]
            array[x_ind].append(sum(data_cut)/float(window))

            a_cut = acoeffs[0][i:i+window]
            array[dx_ind].append(sum(a_cut)/float(window))
            for j in range(level):
                d_cut = dcoeffs[-(j+1)][i:i+window]
                if j <= 5:
                    key = 'var'+str(j+1)
                    array[self.KEYLIST.index(key)].append(sum(d_cut)/float(window))
                elif 5 < j <= 7:
                    if j == 6:
                        key = 'dy'
                    elif j == 7:
                        key = 'dz'
                    array[self.KEYLIST.index(key)].append(sum(d_cut)/float(window))

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

        for key in self.KEYLIST:
            array[self.KEYLIST.index(key)] = np.asarray(array[self.KEYLIST.index(key)])

        return DataStream(header=headers, ndarray=np.asarray(array,dtype=object))


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
        data.multiply({'x':-1})

    APPLICATION:

        """
        sel = self.copy()

        for key in factors:
            if key in self.KEYLIST:
                ind = self.KEYLIST.index(key)
                val = sel.ndarray[ind]
                if key == 'time':
                    logger.error("factor: Multiplying time? That's just plain silly.")
                else:
                    if square == False:
                        newval = [elem * factors[key] for elem in val]
                        logger.info('factor: Multiplied column %s by %s.' % (key, factors[key]))
                    else:
                        newval = [elem ** factors[key] for elem in val]
                        logger.info('factor: Multiplied column %s by %s.' % (key, factors[key]))
                    sel.ndarray[ind] = np.asarray(newval)
            else:
                logger.warning("factor: Key '%s' not in keylist." % key)

        return sel


    def offset(self, offsets, starttime=None, endtime=None, debug=False, **kwargs):
        """
    DEFINITION:
        Apply constant offsets to elements of the datastream

    PARAMETERS:
    Variables:
        - offsets:      (dict) Dictionary of offsets with keys to apply to
                        e.g. {'time': timedelta(hours=1), 'x': 4.2, 'f': -1.34242}
                        Important: Time offsets have to be timedelta objects
        - starttime:    (Datetime object) Start time to apply offsets
        - endtime :     (Datetime object) End time to apply offsets
    Kwargs:
        - comment :     (string) an annotation to add for the offset values

    RETURNS:
        - DataStream:   (DataStream) with offsets applied - destructive

    EXAMPLES:
        data = data.offset({'x':7.5})
        or
        data = data.offset({'x':7.5},starttime='2015-11-21 13:33:00',endtime='2015-11-23 12:22:00')

        """
        comment = kwargs.get('comment')

        if len(self.ndarray[0]) > 0:
            tcol = self.ndarray[0]
        else:
            return self

        if not len(tcol) > 0:
            logger.error("offset: No data found - aborting")
            return self

        stidx = 0
        edidx = len(tcol)
        commpos = self.KEYLIST.index('comment')
        flagpos = self.KEYLIST.index('flag')

        if starttime:
            st = testtime(starttime)
            # get index number of first element >= starttime in timecol
            stidxlst = np.where(tcol >= st)[0]
            if not len(stidxlst) > 0:
                return self   ## stream ends before starttime
            stidx = stidxlst[0]
        if endtime:
            ed = testtime(endtime)
            # get index number of last element <= endtime in timecol
            edidxlst = np.where(tcol <= ed)[0]
            if not len(edidxlst) > 0:
                return self   ## stream begins after endtime
            edidx = (edidxlst[-1]) + 1

        if comment and not comment == '':
            if len(self.ndarray[0]) > 0:
                commcol = self.ndarray[commpos]
            else:
                commcol = self._get_column('comment')
            if not len(commcol) == len(tcol):
                commcol = [''] * len(tcol)
            if not len(self.ndarray[flagpos]) == len(tcol):
                fllist = ['0' for el in self.FLAGKEYLIST]
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
            if debug:
                print("offset", len(commcol), len(tcol))
            self.ndarray[commpos] = commcol

        for key in offsets:
            if key in self.KEYLIST:
                ind = self.KEYLIST.index(key)
                val = self.ndarray[ind]
                val = val[stidx:edidx]
                if key == 'time':
                    #secperday = 24*3600
                    if not isinstance(offsets[key], basestring):
                        os = offsets[key]
                    else:
                        os = eval('{}'.format(offsets[key]))
                    val = val + os
                    logger.info('offset: Corrected time column by %s sec' % str(offsets[key]))
                else:
                    val = val + offsets[key]
                    logger.info('offset: Corrected column %s by %.3f' % (key, offsets[key]))
                self.ndarray[ind][stidx:edidx] = val
            else:
                logger.error("offset: Key '%s' not in keylist." % key)

        return self


    def plot(self, keys=None, debugmode=None, **kwargs):
        """
    DEFINITION:
        Code for plotting one dataset. Consult mpplot.plot() and .plotStreams() for more
        details.

    EXAMPLE:
        cs1_data.plot(['f'],
                outfile = 'frequenz.png',
                specialdict = {'f':[44184.8,44185.8]},
                plottitle = 'Station Graz - Feldstaerke 05.08.2013',
                bgcolor='white')
        """

        import magpy.core.plot as mp
        if keys == None:
            keys = []
        mp.tsplot(self, keys=keys, **kwargs)


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
                    newstream = stream.randomdrop(percentage=10,fixed_indicies=[0,len(means.ndarray[0])-1])
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
        data = data.trim(starttime, endtime)

    APPLICATION:
        """

        if starttime and endtime:
            if testtime(starttime) > testtime(endtime):
                logger.error('Trim: Starttime (%s) is larger than endtime (%s).' % (starttime,endtime))
                raise ValueError("Starttime is larger than endtime.")

        logger.info('Remove: Started from %s to %s' % (starttime,endtime))

        cutstream = self.copy()
        starttime = testtime(starttime)
        endtime = testtime(endtime)
        stval = 0

        if len(cutstream.ndarray[0]) > 0:
            timearray = self.ndarray[0]
            st = (np.abs(timearray-starttime)).argmin()
            ed = (np.abs(timearray-endtime)).argmin() + 1
            if starttime < cutstream.ndarray[0][0]:
                st = 0
            if endtime > cutstream.ndarray[0][-1]:
                ed = len(cutstream.ndarray[0])
            dropind = [i for i in range(st,ed)]
            for index,key in enumerate(self.KEYLIST):
                if len(cutstream.ndarray[index])>0:
                    cutstream.ndarray[index] = np.delete(cutstream.ndarray[index], dropind)

        return cutstream


    @deprecated("replaced by core.flagging apply_flags")
    def remove_flagged(self, **kwargs):
        flaglist = kwargs.get('flaglist')
        data = flaglist.apply_flags(self, mode='drop')
        return data


    def resample(self, keys, method="3.0", debugmode=False,**kwargs):
        """
    DEFINITION:
        Uses Numpy interpolate.interp1d to resample stream to requested period. An initial time step is added by extrapolation to maintain the length
        Please note: an removeduplicate command is issued as well as duplicates will falisfy the identification of nan values

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
        - startperiod:  (integer) starttime in sec (e.g. 60 each minute, 900 each quarter hour
        - offset:       (integer) starttime in sec (e.g. 60 each minute, 900 each quarter hour

    RETURNS:
        - stream:       (DataStream object) Stream containing resampled data.

    EXAMPLE:
        resampled_stream = pos_data.resample(['f'],period=1)

    APPLICATION:
        """

        t1 = datetime.now()
        period = kwargs.get('period')
        offset = kwargs.get('offset')

        if not period:
            period = 60.

        if not len(self.ndarray[0]) > 0:
            return self

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
            startperiod = self.findtime(t_min)
        else:
            t_min = ceil_dt(t_min,period)
            startperiod = self.findtime(t_min)

        # new way: get the indicies of resample timesteps
        stwithnan = self.copy()
        # remove duplicate inputs (would lead to wrong selection of validity identification windows)
        stwithnan = stwithnan.removeduplicates()

        # This is done if timesteps are not at period intervals
        # -----------------------------------------------------

        # Create a list (t_list) containing new time steps, separeted by period
        t_list = list(np.arange(t_min, t_max, timedelta(seconds=period)).astype(datetime))

        if not len(t_list) > 0:
            return DataStream()

        if method == "3.0":
            array=[np.asarray([]) for elem in self.KEYLIST]
            newtil = np.array(np.arange(t_min, t_max, timedelta(seconds=period)).astype(datetime))
            newti = date2num(newtil)
            int_data = stwithnan.interpolate_nans(keys)
            ti = date2num(int_data.ndarray[0])
            #print (newtil)
            # update keys as full nan columns might have been removed
            keys = int_data.variables()
            for key in keys:
                if key in self.KEYLIST[1:16]:
                    index = self.KEYLIST.index(key)
                    col = int_data._get_column(key)
                    func = interpolate.interp1d(ti, col, kind='linear')
                    f = func(newti)
                    orgcol = stwithnan._get_column(key)
                    nans, x = nan_helper(orgcol)
                    orgcol[~nans] = 5
                    orgcol[nans] = 10
                    nanfunc = interpolate.interp1d(ti, orgcol, kind='linear')
                    nanf = nanfunc(newti)
                    ind = np.where(nanf>6)
                    f[ind] = np.nan
                    array[index] = np.asarray(f)
            array[0] = newtil
        elif method == "2.0":
            # new and fast in MagPy2.0 , based on Fourier signal.resample (old makes use of linear)
            # not working yet
            array=[np.asarray([]) for elem in self.KEYLIST]
            t_list = list(np.arange(t_min-timedelta(seconds=period), t_max, timedelta(seconds=period)).astype(datetime))
            int_data = stwithnan.interpolate_nans(keys)
            # update keys as full nan columns might have been removed
            keys = int_data.variables()
            for key in keys:
                if key in self.KEYLIST[1:16]:
                    col = int_data._get_column(key)
                    col = np.insert(col, 0, col[0])  # insert value 1 at position 0 - as timelist is extrended- will be replaced by nan later
                    orgcol = stwithnan._get_column(key)
                    nans, x = nan_helper(orgcol)
                    orgcol[~nans] = 5
                    orgcol[nans] = 10
                    orgcol = np.insert(orgcol, 0, 10)
                    index = self.KEYLIST.index(key)
                    f, t = signal.resample(col, len(t_list), t_list)
                    #print (t)
                    nanf = signal.resample(orgcol, len(t_list))
                    # get all indices with 1 in nanf
                    ind = np.where(nanf>6)
                    f[ind] = np.nan
                    #f.insert(0, np.nan)  # insert nan if len(f) not fitting times
                    array[index] = np.asarray(f)
            array[0] = np.asarray(t_list)
            array = [ar[1:] for ar in array]
        else:
            # Compare length of new time list with old timelist
            # multiplicator is used to check whether nan value is at the corresponding position of the orgdata file - used for not yet completely but sufficiently correct missing value treatment
            multiplicator = float(stwithnan.length()[0])/float(len(t_list))
            diff = int(np.abs(stwithnan.length()[0]-len(t_list)))
            if diff < np.abs(multiplicator)*10:
                diff = int(np.abs(multiplicator)*10)
            if diff > 1000:
                # arbitrary maximum: if larger than 1000 something went wrong anyway
                # eventuallly use smaller data sets
                diff = 1000
            logger.info("resample a: {},{},{}".format(float(stwithnan.length()[0]), float(len(t_list)),startperiod))

            if debugmode:
                print("Times:", stwithnan.ndarray[0][0],stwithnan.ndarray[0][-1],t_list[0],t_list[-1])
                print("Multiplikator:", multiplicator, stwithnan.length()[0], len(t_list))
                print("Diff:", diff)

            # res stream with new t_list is used for return
            array=[np.asarray([]) for elem in self.KEYLIST]
            t0 = t_list[0] - timedelta(seconds=period)
            for key in keys:
                if debugmode:
                    print ("Resampling:", key)
                if key not in self.KEYLIST[1:16]:
                    logger.warning("resample: Key %s not supported!" % key)

                index = self.KEYLIST.index(key)
                try:
                    int_data = stwithnan.interpol([key],kind='linear')#'cubic')
                    int_func = int_data[0]['f'+key]
                    int_min = int_data[1]
                    int_max = int_data[2]
                    # add an initial value
                    t0 = t_list[0] -timedelta(seconds=period)
                    v0 = np.nan
                    if len(stwithnan.ndarray[index]) > 2 and not np.isnan(stwithnan.ndarray[index][0]) and not np.isnan(stwithnan.ndarray[index][1]):
                        ti1 = stwithnan.ndarray[0][0]
                        ti2 = stwithnan.ndarray[0][1]
                        v1 = stwithnan.ndarray[index][0]
                        v2 = stwithnan.ndarray[index][1]
                        v0 = v2 + (v2-v1)/((ti2-ti1).total_seconds())*((t0-ti2).total_seconds())
                    key_list = [v0]
                    for ind, item in enumerate(t_list):
                        # normalized time range between 0 and 1
                        #print (item, int_min, int_max)
                        functime = (date2num(item) - int_min)/(int_max - int_min)
                        if int(ind*multiplicator) <= len(self.ndarray[index]):
                            # Exact solution: (well, is not exact as actually the difference in counts should be considered for the search window (leon, 2023-05-01))
                            # but should be OK for now: change to stv = mv-diff, etv = mv+diff , diff = abs(
                            mv = int(ind*multiplicator+startperiod)
                            #stv = mv-int(20*multiplicator)
                            stv = int(mv-diff)
                            if stv < 0:
                                stv = 0
                            #etv = mv+int(20*multiplicator)
                            etv = int(mv+diff)
                            if etv >= len(stwithnan.ndarray[index]):
                                etv = len(stwithnan.ndarray[index])
                            subar = stwithnan.ndarray[0][stv:etv]
                            idx = (np.abs(subar-item)).argmin()
                            #subar = stwithnan.ndarray[index][stv:etv]
                            orgval = stwithnan.ndarray[index][stv+idx] # + offset
                        else:
                            print("Check Resampling method")
                            orgval = 1.0
                        tempval = np.nan
                        # Not a safe fix, but appears to cover decimal leftover problems
                        # (e.g. functime = 1.0000000014, which raises an error)
                        if functime > 1.0:
                            functime = 1.0
                        if not isnan(orgval):
                            tempval = int_func(functime)
                        key_list.append(float(tempval))

                    array[index] = np.asarray(key_list)
                except:
                    logger.error("resample: Error interpolating stream. Stream either too large or no data for selected key")

            t_list.insert(0, t0) # do not insert startcondition
            array[0] = np.asarray(t_list)
            # now drop first line of array
            array = [ar[1:] for ar in array]

        if debugmode:
            t2 = datetime.now()
            print ("Resample duration", (t2-t1).total_seconds())

        logger.info("resample: Data resampling complete.")
        stwithnan.header['DataSamplingRate'] = period
        return DataStream(header=stwithnan.header,ndarray=np.asarray(array,dtype=object))


    def rotation(self, alpha=0, beta=0, gamma=0, invert=False, order='ZYX', debug=False, **kwargs):
        """
    DEFINITION:
        Rotation matrix for rotating x,y,z to a new coordinate system xs,ys,zs using angles alpha (rotation within the
        horizontal plane around the z axis), beta rotation around y-axis and gamma for rotations around the x-axis.
        Please note: this function was changed in comparison to MagPy 1.x from a vector "yaw" and "pitch" rotation
        to a full 3D rotation. The new version is based on the SciPy Rotation module and uses Euler rotation
        ('zyx' by default).
    PARAMETERS:
    Variables:
        - alpha:        (float) z-axis rotation angle in degree The horizontal rotation in degrees (0,360)
        - beta:         (float) y-axis rotation in degrees (0,360)
        - gamma:        (float) x-axis rotation in degrees (0,360)
        - invert:       (BOOL) invert the rotation i.e. rotate back with the same angles
        - order:        default is 'ZYX'
    Kwargs:
        . unit:
        - keys:         (list) provide an alternative vector to rotate - default is ['x','y','z']
                               keys are only supported from 1.0 onwards (ndarray)
    RETURNS:
        - self:         (DataStream) The rotated stream
    EXAMPLE:
        # Most widely used case - rotation in the horizontal (xy) plane
        data = data.rotation(alpha=2.74)
        # Complex 3D rotation
        rotdata = data.rotation(alpha=45,beta=45,gamma=45)
        # Rotate back
        orgdata = data.rotation(alpha=45,beta=45,gamma=45, invert=True)
    APPLICATION:

        """

        unit = kwargs.get('unit')
        keys =  kwargs.get('keys')

        ang_fac = 1.
        if unit == 'gon':
            ang_fac = 360./400.
        elif unit == 'rad':
            ang_fac = 180./np.pi
        if not keys:
            keys = ['x','y','z']

        if not len(keys) == 3:
            logger.error('rotation: provided keylist need to have three components.')
            return self

        logger.info('rotation: Applying rotation matrix.')
        data = self.copy()
        # contruct vector
        x = data._get_column(keys[0]).astype(float)
        y = data._get_column(keys[1]).astype(float)
        z = data._get_column(keys[2]).astype(float)
        column_vector = np.array([x, y, z])
        # Transpose the column vector
        v = column_vector.T
        # converts angles
        alpha = alpha * ang_fac
        beta = beta * ang_fac
        gamma = gamma * ang_fac

        # construct the rotation matrix
        r = Rotation.from_euler(order, [alpha, beta, gamma], degrees=True)
        if invert:
            r = r.inv()
        if debug:
            print("stream.rotation: rotation matrix looks like:", r.as_matrix())

        result_vector = r.apply(v)
        # Transpose to column representation
        result_columns = result_vector.T

        # add rotated vector back into data set
        data._put_column(result_columns[0],keys[0])
        data._put_column(result_columns[1],keys[1])
        data._put_column(result_columns[2],keys[2])

        # Add a note to DataComments
        datacomm = data.header.get('DataComments','')
        fill = ''
        if datacomm:
            fill = ", "
        rotation_input = "rotation (alpha:{} beta:{} gamma:{}) applied in rotation_order {}".format(alpha,beta,gamma,order)
        datacomm = "{}{}{}".format(datacomm,fill,rotation_input)
        data.header['DataComments'] = datacomm
        logger.info('rotation: Finished reorientation.')

        return data


    @deprecated("Replaced by twice as fast _select_keys")
    def selectkeys(self, keys, **kwargs):
        """
    DEFINITION:
        Take data stream and remove all except the provided keys from ndarray
    RETURNS:
        - self:         (DataStream) with ndarray limited to keys

    EXAMPLE:
        keydata = fulldata.selectkeys(['x','y','z'])

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
                flagidx = self.KEYLIST.index('flag')
                commentidx = self.KEYLIST.index('comment')
                if len(stream.ndarray[flagidx]) > 0:
                    keys.append('flag')
                if len(stream.ndarray[commentidx]) > 0:
                    keys.append('comment')

            # Remove all missing
            for idx, elem in enumerate(stream.ndarray):
                if not self.KEYLIST[idx] in keys:
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
        - self:         (DataStream) The smoothed signal - destructive

    EXAMPLE:
        nice_data = bad_data.smooth(['x','y','z'])
        or
        t=linspace(-2,2,0.1)
        x=sin(t)+randn(len(t))*0.1
        y=smooth(x)

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

        if not len(self.ndarray[0])>0:
            return self

        logger.info('smooth: Start smoothing (%s window, width %d) at %s' % (window, window_len, str(datetime.now())))

        for key in keys:
            if key in self.NUMKEYLIST:
                ind = self.KEYLIST.index(key)
                x = self.ndarray[ind]
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

                self.ndarray[ind] = np.asarray(y[(int(window_len/2)):(len(x)+int(window_len/2))])
            else:
                logger.error("Column key %s not valid." % key)


        logger.info('smooth: Finished smoothing at %s' % (str(datetime.now())))

        return self


    def stats(self, format=''):
        """
        DESCRIPTION
            Provide an overview about the data set.
            Will return information on data length, sampling period, variables and important
            header contents like SensorID, StationID, DataID
        OPTIONS:
            format  :    "dir" default dictionary, "md" for markdown
        RETURN:
            dictionary
        APPLCIATION:
            d = data.stats(format='md')
        """
        resdict = {"SensorID": self.header.get('SensorID'), "Variables": self.variables(),
                   "Amount": len(self), "Samplingperiod (sec)": self.samplingrate(),
                   "StationID": self.header.get('StationID'), "DataID": self.header.get('DataID'),
                   "DataComponents": self.header.get('DataComponents'),
                   "Starttime": self.start(),
                   "Endtime": self.end()}
        if format == 'md':
            try:
                from IPython.display import display, Markdown
                head = ["| Parameter | Value |", "| ------ | ------ |"]
                list = ["| {} | {} |".format(el, resdict.get(el)) for el in resdict]
                head.extend(list)
                fulltext = ''
                for el in head:
                    fulltext += "{}\n".format(el)
                display(Markdown(fulltext))
            except:
                print(" stats: Markdown display requires IPython display")
        elif format == 'json':
            print(resdict)
        return resdict


    def steadyrise(self, key, timewindow, **kwargs):
        """
        DEFINITION:
            Method determines the absolute increase within a data column
            and a selected time window
            neglecting any resets and decreasing trends
            - used for analyzing bucket type rain sensors
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
            col = stream.steadyrise('t1', timedelta(minutes=60),sensitivitylevel=0.002)


        """
        sensitivitylevel = kwargs.get('sensitivitylevel')

        prevval = 9999999999999.0
        stacked = 0.0
        count = 0
        rescol = []

        if not len(self.ndarray[0]) > 0:
            return np.asarray([])

        ind = self.KEYLIST.index(key)
        if len(self.ndarray[ind]) > 0:
            startt = np.min(self.ndarray[0])
            for idx,val in enumerate(self.ndarray[ind]):
                if self.ndarray[0][idx] < startt+timewindow:
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
        else:
            print("steadyrise: no data found within the selected column {}}".format(key))
            return np.asarray([])
        # Finally fill the end
        for i in range(count):
            rescol.append(stacked)

        if not len(rescol) == len(self.ndarray[0]):
            logger.error('steadrise: An error leading to unequal lengths has been encountered')
            return np.asarray([])

        return np.asarray(rescol)

    def timerange(self):
        """
        DEFINITION:
            Find start and end times in stream.
        RETURNS:
            Two datetime objects, start and end.
        """
        if len(self.ndarray[0]) > 0:
            #print (self.ndarray[0][0], type(self.ndarray[0][0]))
            if isinstance(self.ndarray[0][0], datetime):
                t_start = np.min(self.ndarray[0]).replace(tzinfo=None)
                t_end = np.max(self.ndarray[0]).replace(tzinfo=None)
            elif isinstance(self.ndarray[0][0], np.datetime64):
                t_start = np.min(self.ndarray[0])
                t_end = np.max(self.ndarray[0])
            else:
                t_start = num2date(self.ndarray[0][0]).replace(tzinfo=None)
                t_end = num2date(self.ndarray[0][-1]).replace(tzinfo=None)
        else:
            try: # old type
                t_start = num2date(self[0].time).replace(tzinfo=None)
                t_end = num2date(self[-1].time).replace(tzinfo=None)
            except: # empty
                t_start,t_end = None,None

        return t_start, t_end

    def trim(self, starttime=None, endtime=None, include=False, newway=False, debug=False):
        """
    DEFINITION:
        Removing dates outside of range between start- and endtime.
        Returned stream has range starttime <= range < endtime.
        In order to get ranges like starttime <= range <= endtime use
        option 'include=True'

    PARAMETERS:
    Variables:
        - starttime:    (datetime/str) Start of period to trim with
        - endtime:      (datetime/str) End of period to trim to
        - include:      (bool) include endtime into result
    Kwargs:

    RETURNS:
        - stream:       (DataStream object) Trimmed stream

    EXAMPLE:
        data = data.trim(starttime=starttime, endtime=endtime)

    APPLICATION:
        """
        tinfo = None
        vind = None
        if starttime and endtime:
            if testtime(starttime) > testtime(endtime):
                raise ValueError("Starttime is larger than endtime.")

        timea = np.array(self.ndarray[0])
        if len(timea)>0 and not isinstance(timea[0], (datetime,datetime64)):
            # still necessary for absolutes in magpy cdf structures
            timea = np.array([num2date(el).replace(tzinfo=None) for el in self.ndarray[0]])
        if len(timea)>0:
            tinfo = timea[0].tzinfo
            if debug:
                print (" timezone info of data: ", tinfo)
        if starttime:
            starttime = testtime(starttime)
            if debug:
                print (" timezone info of starttime: ", starttime.tzinfo)
            if tinfo:
                starttime = starttime.replace(tzinfo=tinfo)
        if endtime:
            endtime = testtime(endtime)
            if tinfo:
                endtime = endtime.replace(tzinfo=tinfo)
        if include:
            sr = self.samplingrate()
            endtime = endtime+timedelta(seconds=sr)
        if starttime and endtime:
            vind = np.nonzero((timea >= starttime) & (timea < endtime))
        elif starttime:
            vind = np.nonzero(timea >= starttime)
        elif endtime:
            vind = np.nonzero(timea < endtime)
        if vind and len(vind) > 0 and len(vind[0]) > 0:
            newar = [[] for el in self.KEYLIST]
            for id, ar in enumerate(self.ndarray):
                if len(ar) > 0:
                    newar[id] = ar[list(vind[0])]
            newar = [np.asarray(el) for el in newar]
            res = DataStream([], self.header, np.asarray(newar, dtype=object))
        else:
            res = DataStream()

        return res


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
        pos = self.KEYLIST.index('sectime')
        tcol = stream.ndarray[0]
        stream = stream._copy_column('sectime','time')
        if swap:
            stream = stream._put_column(tcol,'sectime')
        else:
            stream = stream._drop_column('sectime')

        return stream

    def variables(self, **kwargs):
        """
        DEFINITION:
            get a list of existing numerical keys in stream.
            An alternative to _get_key_headers
        PARAMETERS:
        kwargs:
            - limit:        (int) limit the lenght of the list
            - numerical:    (bool) if True, select only numerical keys
        RETURNS:
            - keylist:      (list) a list like ['x','y','z']

        EXAMPLE:
            data.variables(limit=1)
            data_stream._get_key_headers(limit=1)
        """

        limit = kwargs.get('limit')
        numerical = kwargs.get('numerical')

        if numerical:
            TESTLIST = self.FLAGKEYLIST
        else:
            TESTLIST = self.KEYLIST

        keylist = []

        if not len(keylist) > 0:  # e.g. Testing ndarray
            for ind,elem in enumerate(self.ndarray): # use the long way
                if len(elem) > 0 and ind < len(TESTLIST):
                    if not TESTLIST[ind] == 'time':
                        keylist.append(TESTLIST[ind])
        if limit and len(keylist) > limit:
            keylist = keylist[:limit]

        return keylist


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
            if not coverage == 'all':
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

        if format_type == 'CSV':
            if not filenameends:
                filenameends = '.csv'

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
            elif coverage == 'hour':
                dfor = '%Y%m%d_%H'
            elif coverage == 'minute':
                dfor = '%Y%m%d_%H%M'
            else:
                dfor = '%Y%m%d'
            if int(samprate) == 1:
                dateformat = dfor
                middle = '_pt1s_'
            elif int(samprate) == 60:
                dateformat = dfor
                middle = '_pt1m_'
            elif int(samprate) == 3600:
                dateformat = dfor
                middle = '_pt1h_'
            elif int(samprate) == 86400:
                dateformat = dfor
                middle = '_p1d_'
            elif int(samprate) > 30000000:
                dateformat = '%Y'
                middle = '_p1y_'
            elif int(samprate) > 2400000:
                dateformat = '%Y%m'
                middle = '_p1m_'
            else:
                dateformat = '%Y%m%d'
                middle = 'unknown'
            filenamebegins = begin+'_'
            filenameends = middle+publevel+'.cdf'
        if format_type == 'BLV':
            if len(self.ndarray[0]) > 0:
                lt = self.end()
                if year:
                    blvyear = str(year)
                else:
                    blvyear = datetime.strftime(lt.replace(tzinfo=None),"%Y")
                filenamebegins = self.header.get('StationID','XXX').upper()+blvyear
                filenameends = '.blv'
                coverage = 'all'

        if not format_type:
            format_type = 'PYCDF'
        if not coverage:
            coverage = 'day' #timedelta(days=1)
        if not dateformat:
            dateformat = '%Y-%m-%d' # or %Y-%m-%dT%H or %Y-%m or %Y or %Y
            if coverage == "hour":
                dateformat = '%Y-%m-%dT%H'  # or %Y-%m-%dT%H or %Y-%m or %Y or %Y
            elif coverage == "month":
                dateformat = '%Y-%m' # or %Y-%m-%dT%H or %Y-%m or %Y or %Y
            elif coverage == "year":
                dateformat = '%Y'  # or %Y-%m-%dT%H or %Y-%m or %Y or %Y
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
                        other options are 'month','year','all','hour'
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
        - subdirectory: (str) can be Y, or Ym or Yj. default is none. Will create
                         subdirectories with year (Y) plus month (m) or day-of-year (j)

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
            - fillvalue     (float) define a fill value for non-existing data (default is np.nan)
            - scalar        (DataStream) provide scalar data when sampling rate is different to vector data
            - environment   (DataStream) provide environment data when sampling rate is different to vector data

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
        stream.write('/home/user/data',
                        format_type='IAGA')
        stringio = stream.write('StringIO',
                        format_type='IAGA')

    APPLICATION:

        """
        format_type = kwargs.get('format_type')
        filenamebegins = kwargs.get('filenamebegins')
        filenameends = kwargs.get('filenameends')
        dateformat = kwargs.get('dateformat')
        coverage = kwargs.get('coverage')
        mode = kwargs.get('mode')
        subdirectory = kwargs.get('subdirectory')
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
        fillvalue = kwargs.get('fillvalue')
        scalar = kwargs.get('scalar')
        temperature1 = kwargs.get('temperature1')
        temperature2 = kwargs.get('temperature2')
        headonly = kwargs.get('headonly')

        success = True

        #compression: provide compression factor for CDF data: 0 no compression, 9 high compression

        t1 = datetime.now(timezone.utc).replace(tzinfo=None)

        if not format_type in SUPPORTED_FORMATS:
            if not format_type:
                format_type = 'PYSTR'
            else:
                logger.warning('write: Output format not supported.')
                return False
        else:
            if not 'w' in SUPPORTED_FORMATS[format_type][0]:
                logger.warning('write: Selected format does not support write methods.')
                return False

        format_type, filenamebegins, filenameends, coverage, dateformat = self._write_format(format_type, filenamebegins, filenameends, coverage, dateformat, year)

        if not mode:
            mode= 'overwrite'

        if not subdirectory:
            subdirectory = ''

        if len(self) < 1 and len(self.ndarray[0]) < 1:
            logger.error('write: Stream is empty!')
            raise Exception("Can't write an empty stream to file!")

        ndtype = False
        if len(self.ndarray[0]) > 0:
            # remove all data from array where time is not valid
            #1. get nonnumerics in ndarray[0]
            nonnumbool = np.isnat(self.ndarray[0].astype(np.datetime64))
            #2. get indices
            nonnumlist = np.nonzero(nonnumbool)[0]
            #nonnumlist = np.asarray([idx for idx,elem in enumerate(self.ndarray[0]) if np.isnan(elem)])
            #3. delete them
            if len(nonnumlist) > 0:
                print("write: Found NaNs in time column - deleting them", nonnumlist)
                for idx, elem in enumerate(self.ndarray):
                    self.ndarray[idx] = np.delete(self.ndarray[idx],nonnumlist)

            starttime, lasttime = self._find_t_limits()
            # Round starttime to full day for corrcet coverage
            starttime = datetime.strptime(starttime.strftime("%Y-%m-%d"),"%Y-%m-%d")
            ndtype = True
        else:
            starttime = datetime.strptime(datetime.strftime(num2date(self[0].time).replace(tzinfo=None),'%Y-%m-%d'),'%Y-%m-%d')
            lasttime = num2date(self[-1].time).replace(tzinfo=None)

        t2 = datetime.now(timezone.utc).replace(tzinfo=None)
        if debug:
            print ("Saving data between", starttime, lasttime)

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
            while starttime <= lasttime:
                diryear = starttime.year
                dirmonth = starttime.month
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
                    writepath = os.path.join(filepath, filename)
                    if subdirectory == 'Y':
                        writepath = os.path.join(filepath, str(diryear), filename)
                    elif subdirectory == 'Ym':
                        writepath = os.path.join(filepath, str(diryear), str(dirmonth).zfill(2), filename)
                    success = writeFormat(newst, writepath,format_type,mode=mode,keys=keys,kvals=kvals,skipcompression=skipcompression,compression=compression, addflags=addflags,fillvalue=fillvalue,scalar=scalar,temperature1=temperature1,temperature2=temperature2,ebug=debug)
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
            while starttime <= lasttime:
                diryear = starttime.year
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
                    writepath = os.path.join(filepath, filename)
                    if subdirectory == 'Y':
                        writepath = os.path.join(filepath, str(diryear), filename)
                    success = writeFormat(newst, writepath,format_type,mode=mode,keys=keys,kvals=kvals,kind=kind,comment=comment,skipcompression=skipcompression,compression=compression, addflags=addflags,fillvalue=fillvalue,scalar=scalar,temperature1=temperature1,temperature2=temperature2,debug=debug)
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
            while starttime <= lasttime:
                diryear = starttime.year
                dirmonth = starttime.month
                dirdoy = starttime.strftime('%j')
                t3 = datetime.now(timezone.utc).replace(tzinfo=None)

                if ndtype:
                    lst = []
                    # non-destructive
                    ndarray=dailystream._select_timerange(starttime=starttime, endtime=endtime, maxidx=maxidx)
                    if len(ndarray[0]) > 0:
                        dailystream.ndarray = np.asarray([array[(len(ndarray[0])-1):] for array in dailystream.ndarray],dtype=object)
                else:
                    lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                    ndarray = np.asarray([np.asarray([]) for key in self.KEYLIST],dtype=object)

                t4 = datetime.now(timezone.utc).replace(tzinfo=None)
                #print "write - selecting time range needs:", t4-t3

                newst = DataStream(lst,self.header,ndarray)
                filename = str(filenamebegins) + str(datetime.strftime(starttime,dateformat)) + str(filenameends)
                # remove any eventually existing null byte
                filename = filename.replace('\x00','')

                if format_type == 'IMF':
                    filename = filename.upper()

                writepath = os.path.join(filepath,filename)
                if subdirectory == 'Y':
                    writepath = os.path.join(filepath, str(diryear), filename)
                elif subdirectory == 'Ym':
                    writepath = os.path.join(filepath, str(diryear), str(dirmonth).zfill(2), filename)
                elif subdirectory == 'Yj':
                    writepath = os.path.join(filepath, str(diryear), str(dirdoy).zfill(3), filename)

                if debug:
                    print ("Writing data:", writepath)

                if len(lst) > 0 or ndtype:
                    if len(newst.ndarray[0]) > 0 or len(newst) > 1:
                        logger.info('write: writing %s' % filename)
                        #print("Here", num2date(newst.ndarray[0][0]), newst.ndarray)
                        success = writeFormat(newst, writepath,format_type,mode=mode,keys=keys,version=version,gin=gin,datatype=datatype, useg=useg,skipcompression=skipcompression,compression=compression, addflags=addflags,fillvalue=fillvalue,scalar=scalar,temperature1=temperature1,temperature2=temperature2,headonly=headonly,kind=kind,debug=debug)
                starttime = endtime
                endtime = endtime + cov

                t5 = datetime.now(timezone.utc).replace(tzinfo=None)
                #print "write - written:", t5-t3
                #print "write - End:", t5

        else:
            filename = filenamebegins + filenameends
            # remove any eventually existing null byte
            filename = filename.replace('\x00','')
            if debug:
                print ("Writing file:", filename)
            success = writeFormat(self, os.path.join(filepath,filename),format_type,mode=mode,keys=keys,absinfo=absinfo,fitfunc=fitfunc,fitdegree=fitdegree, knotstep=knotstep,meanh=meanh,meanf=meanf,deltaF=deltaF,diff=diff,baseparam=baseparam, year=year,extradays=extradays,skipcompression=skipcompression,compression=compression, addflags=addflags,headonly=headonly,kind=kind,fillvalue=fillvalue,scalar=scalar,temperature1=temperature1,temperature2=temperature2,debug=debug)

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

        indx = self.KEYLIST.index(keys[0])
        indy = self.KEYLIST.index(keys[1])
        indz = self.KEYLIST.index(keys[2])
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
        prevcomps = self.header.get('DataComponents','')
        if not prevcomps.startswith('IDF'):
            print ("Did not find IDF in DataComponents but converting as you wish")
        self.header['DataComponents'] = prevcomps.replace('IDF','XYZ')

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

        indx = self.KEYLIST.index(keys[0])
        indy = self.KEYLIST.index(keys[1])
        indz = self.KEYLIST.index(keys[2])

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
        prevcomps = self.header.get('DataComponents','')
        if not prevcomps.startswith('XYZ'):
            print ("Did not find XYZ in DataComponents but converting as you wish")
        self.header['DataComponents'] = prevcomps.replace('XYZ','IDF')
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
        indx = self.KEYLIST.index(keys[0])
        indy = self.KEYLIST.index(keys[1])
        indz = self.KEYLIST.index(keys[2])

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
        prevcomps = self.header.get('DataComponents','')
        if not prevcomps.startswith('XYZ'):
            print ("Did not find XYZ in DataComponents but converting as you wish")
        self.header['DataComponents'] = prevcomps.replace('XYZ','HDZ')
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

        indx = self.KEYLIST.index(keys[0])
        indy = self.KEYLIST.index(keys[1])
        indz = self.KEYLIST.index(keys[2])

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
        prevcomps = self.header.get('DataComponents','')
        if not prevcomps.startswith('HDZ'):
            print ("Did not find HDZ in DataComponents but converting as you wish")
        self.header['DataComponents'] = prevcomps.replace('HDZ','XYZ')

        return DataStream(self,self.header,self.ndarray)


class LineStruct(object):
    def __init__(self, time=np.nan, x=np.nan, y=np.nan, z=np.nan, f=np.nan, dx=np.nan, dy=np.nan, dz=np.nan, df=np.nan, t1=np.nan, t2=np.nan, var1=np.nan, var2=np.nan, var3=np.nan, var4=np.nan, var5=np.nan, str1='-', str2='-', str3='-', str4='-', flag='0000000000000000-', comment='-', typ="xyzf", sectime=np.nan):
        #def __init__(self):
        #- at the end of flag is important to be recognized as string
        """
        self.time=np.nan
        self.x=np.nan
        self.y=np.nan
        self.z=np.nan
        self.f=np.nan
        self.dx=np.nan
        self.dy=np.nan
        self.dz=np.nan
        self.df=np.nan
        self.t1=np.nan
        self.t2=np.nan
        self.var1=np.nan
        self.var2=np.nan
        self.var3=np.nan
        self.var4=np.nan
        self.var5=np.nan
        self.str1=''
        self.str2=''
        self.str3=''
        self.str4=''
        self.flag='0000000000000000-'
        self.comment='-'
        self.typ="xyzf"
        self.sectime=np.nan
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
        data = self.copy()
        unit = kwargs.get('unit')
        if unit == 'gon':
            ang_fac = 400./360.
        elif unit == 'rad':
            ang_fac = np.pi/180.
        else:
            ang_fac = 1.
        xval = data.x
        yval = data.y
        zval = data.z

        ra = np.pi*alpha/(180.*ang_fac)
        rb = np.pi*beta/(180.*ang_fac)
        xs = data.x*np.cos(rb)*np.cos(ra)-data.y*np.sin(ra)+data.z*np.sin(rb)*np.cos(ra)
        ys = data.x*np.cos(rb)*np.sin(ra)+data.y*np.cos(ra)+data.z*np.sin(rb)*np.sin(ra)
        zs = data.x*np.sin(rb)+data.z*np.cos(rb)

        xs2 = xval*np.cos(rb)*np.cos(ra)-yval*np.sin(ra)+zval*np.sin(rb)*np.cos(ra)
        ys2 = xval*np.cos(rb)*np.sin(ra)+yval*np.cos(ra)+zval*np.sin(rb)*np.sin(ra)
        zs2 = xval*np.sin(rb)+zval*np.cos(rb)

        data.x = xs
        data.y = ys
        data.z = zs

        return data


# -------------------
#  Global functions of the stream file
# -------------------


# ##################
# read/write functions
# ##################

def read(path_or_url=None, starttime=None, endtime=None, dataformat=None, datecheck=True, headonly=False, **kwargs):
    """
    DEFINITION:
        The read functions tries to open the selected files. Calls on
        function _read() for help.

    PARAMETERS:
    Variables:
        - path_or_url:  (str) Path to data files in form:
                                a) c:/my/data/*"
                                b) c:/my/data/thefile.txt
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
        - datecheck:    (BOOL) default =True, will check for dates in filename and filter read with starttime/endtime range
                        - set to FALSE if no date in filename, but numbers which might be wrongly interpreted
        - select:       (str object) Select.

    Format specific kwargs:
        IAF:
            - resolution: (str) can be either 'day','hour','minute'(default) or 'k'
        IMAGCDF:
            - select:     (str object) Select specific data if more than one time columns are contained.
                          i.e. select='Scalar'

    RETURNS:
        - stream:       (DataStream object) Stream containing data in file
                        under path_or_url.

    EXAMPLE:
        stream = read('/srv/archive/WIC/LEMI025/LEMI025_2014-05-05.bin')
        OR
        stream = read('http://www.swpc.noaa.gov/ftpdir/lists/ace/20140507_ace_sis_5m.txt')

    APPLICATION:
    """

    debugmode = kwargs.get('debugmode')
    disableproxy = kwargs.get('disableproxy')
    skipsorting = kwargs.get('skipsorting')
    keylist = kwargs.get('keylist') # for PYBIN
    debug = kwargs.get('debug')

    KEYLIST =  DataStream().KEYLIST
    if starttime:
        starttime = testtime(starttime)
    if endtime:
        endtime = testtime(endtime)

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
        # Use some predefined values for webservices
        starttimestring = 'starttime'
        endtimestring = 'endtime'
        if path_or_url.find('bgs.ac.uk') > -1:
            starttimestring = 'dataStartDate'
            endtimestring = 'dataEndDate'
        # extract extension if any
        logger.info("read: Found URL to read at {}".format(path_or_url))
        if starttime or endtime:  # and start or endtime not yet contained in url
            path_or_url = dates_to_url(path_or_url, starttime, endtime, starttimestring = starttimestring, endtimestring = endtimestring, debug=debug)
        content = urlopen(path_or_url).read()
        content = content.decode('utf-8')
        if content.find('<pre>') > -1:
            """
                check whether content is coming with some html tags
            """
            def get_between(s,first,last):
                start = s.index(first) + len(first)
                end = s.index(last, start )
                return s[start:end]
            content_t = get_between(content, '<pre>', '</pre>')
            cleanr = re.compile('<.*?>')
            content = re.sub(cleanr, '', content_t)

        #print ("HERE", path_or_url)
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
                        st = join_streams(st, stp)
                        #st.extend(stp.container,stp.header,stp.ndarray)
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
            fname = fname.replace('?','').replace(':','')   ## Necessary for windows
            fh = NamedTemporaryFile(suffix=fname,delete=False,mode='w+')
            fh.write(content)
            fh.close()
            st = _read(fh.name, dataformat, headonly, **kwargs)
            os.remove(fh.name)
    else:
        # some file name
        pathname = path_or_url
        if debug:
            print ("Found a file pathname:", pathname)
        for filename in iglob(pathname):
            getfile = True
            if datecheck:
                theday = extract_date_from_string(filename)
                if debug:
                    print("Extracted dates from filename:", theday)
                try:
                    if starttime:
                        if not theday[-1] >= starttime.date():
                            getfile = False
                    if endtime:
                        if not theday[0] <= endtime.date():
                            getfile = False
                except:
                    # Date format not recognised. Read all files
                    logger.info("read: Unable to detect date string in filename. Reading all files...")
                    #logger.warning("read: filename: {}, theday: {}".format(filename,theday))
                    getfile = True

            if getfile:
                zipped = False
                if filename.endswith('.gz') or filename.endswith('.GZ'):
                    ## Added gz support to read IMO compressed data directly - future option might include tarfiles
                    import gzip
                    #print ("Found zipped file (gz) ... unpacking")
                    fname = os.path.split(filename)[1]
                    fname = fname.strip('.gz')
                    with NamedTemporaryFile(suffix=fname,delete=False) as fh:
                        shutil.copyfileobj(gzip.open(filename), fh)
                        filename = fh.name
                    zipped = True
                if filename.endswith('.zip') or filename.endswith('.ZIP'):
                    ## Added gz support to read IMO compressed data directly - future option might include tarfiles
                    from zipfile import ZipFile
                    #print ("Found zipped file (zip) ... unpacking")
                    with ZipFile(filename) as myzip:
                        fname =  myzip.namelist()[0]
                        with NamedTemporaryFile(suffix=fname,delete=False) as fh:
                            shutil.copyfileobj(myzip.open(fname), fh)
                            filename = fh.name
                    zipped = True

                stp = DataStream([],{},np.array([[] for ke in KEYLIST]))
                try:
                    stp = _read(filename, dataformat, headonly, **kwargs)
                    if zipped:
                        # delete the temporary file
                        os.remove(filename)
                except:
                    stp = DataStream([],{},np.array([[] for ke in KEYLIST]))
                    logger.warning("read: File {} could not be read. Skipping ...".format(filename))
                if len(stp) > 0 or len(stp.ndarray[0]) > 0:   # important - otherwise header is going to be deleted
                    st = join_streams(st,stp)
                    #st.extend(stp.container,stp.header,stp.ndarray) # extend will not import columns
                    # when reading with * of which one file has an empty column

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
        st = st.trim(starttime=starttime, debug=debug)
    if endtime:
        st = st.trim(endtime=endtime, debug=debug)

    ### Define some general header information TODO - This is done already in some format libs - clean up
    if len(st) > 0:
        st = st.removeduplicates()
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
    KEYLIST =  DataStream().KEYLIST

    format_type = None
    foundapproptiate = False
    if not dataformat:
        # auto detect format - go through all known formats in given sort order
        for format_type in SUPPORTED_FORMATS:
            # check format
            if debug:
                print("_read: Testing format: {} ...".format(format_type))
            if debug:
                logger.info("_read: Testing format: {} ...".format(format_type))
            if isFormat(filename, format_type):
                if debug:
                    logger.info("      -- found: {}".format(format_type))
                    print ("      -- found: {}".format(format_type))
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
            formats = [el for el in SUPPORTED_FORMATS if el == dataformat]
            format_type = formats[0]
        except IndexError:
            msg = "Format \"%s\" is not supported. Supported types: %s"
            logger.error(msg % (dataformat, ', '.join(SUPPORTED_FORMATS)))
            raise TypeError(msg % (dataformat, ', '.join(SUPPORTED_FORMATS)))

    stream = readFormat(filename, format_type, headonly=headonly, **kwargs)

    return stream

@deprecated("Replaced by core.flagging.save")
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
        saveflags(flaglist,'/my/path/myfile.pkl')

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


@deprecated("Replaced by core.flagging.load")
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
        loadflags('/my/path/myfile.pkl')
    """
    import json
    if not path:
        return []
    if path.endswith('.json'):
        try:
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


def join_streams(stream_a,stream_b, **kwargs):
    """
    DEFINITION:
        Copy two streams together eventually replacing already existing time steps.
        Data of stream_a will replace data of stream_b
    APPLICATION
        combinedstream = join_streams(stream_a,stream_b)
    """
    logger.info('join_streams: Start joining at %s.' % str(datetime.now()))
    KEYLIST =  DataStream().KEYLIST
    comment = kwargs.get('comment')

    # Check stream type and eventually convert them to ndarrays
    # --------------------------------------
    if len(stream_a.ndarray[0]) > 0:
        # Using ndarray and eventually convert stream_b to ndarray as well
        if not len(stream_b.ndarray[0]) > 0:
            return stream_a
    elif len(stream_b.ndarray[0]) > 0:
        if not len(stream_a.ndarray[0]) > 0:
            return stream_b
    else:
        if not len(stream_a.ndarray[0]) > 0 and not len(stream_b.ndarray[0]) > 0:
            logger.error('join_streams: stream(s) empty - aborting joining.')
            return stream_a

    # non-destructive
    # --------------------------------------
    sa = stream_a.copy()
    sb = stream_b.copy()

    saflags = sa.header.get("DataFlags")
    sbflags = sb.header.get("DataFlags")
    if saflags and sbflags:
        sa.header["DataFlags"] = saflags.join(sbflags)
    elif sbflags:
        sa.header["DataFlags"] = sbflags

    # Get indicies of timesteps of stream_b of which identical times are existing in stream_a-> delelte those lines
    # --------------------------------------
    # IMPORTANT: If two streams with different keys should be combined then "merge" is the method of choice
    # NEW: shape problems when removing data -> now use removeduplicates at the end
    # SHOULD WORK (already tested) as remove duplicate will keep the last value and drop earlier occurences
    #indofb = np.nonzero(np.in1d(sb.ndarray[0], sa.ndarray[0]))[0]
    #for idx,elem in enumerate(sb.ndarray):
    #    if len(sb.ndarray[idx]) > 0:
    #        sb.ndarray[idx] = np.delete(sb.ndarray[idx],indofb)

    # Now add stream_a to stream_b - regard for eventually missing column data
    # --------------------------------------
    array = [[] for key in KEYLIST]
    for idx,elem in enumerate(sb.ndarray):
        if len(sa.ndarray[idx]) > 0 and len(sb.ndarray[idx]) > 0:
            array[idx] = np.concatenate((sa.ndarray[idx],sb.ndarray[idx]))
        elif not len(sa.ndarray[idx]) > 0 and  len(sb.ndarray[idx]) > 0:
            if idx <= len(stream_a.NUMKEYLIST):
                fill = np.nan
            else:
                fill = '-'
            arraya = np.asarray([fill]*len(sa.ndarray[0]))
            array[idx] = np.concatenate((arraya,sb.ndarray[idx]))
        elif len(sa.ndarray[idx]) > 0 and not len(sb.ndarray[idx]) > 0:
            if idx <= len(stream_a.NUMKEYLIST):
                fill = np.nan
            else:
                fill = '-'
            arrayb = np.asarray([fill]*len(sb.ndarray[0]))
            array[idx] = np.concatenate((sa.ndarray[idx],arrayb))
        else:
            array[idx] = np.asarray([])

    stream = DataStream(header=sa.header, ndarray=np.asarray(array,dtype=object))
    if comment:
        oldcomment = stream.header.get("DataComments", "")
        if isinstance(comment,str):
            comment = oldcomment + comment
        else:
            comment = oldcomment + "joined {} into {} ".format(sb.header.get("DataID"),sa.header.get("DataID"))
        stream.header["DataComments"] = comment
    stream = stream.removeduplicates()

    return stream.sorting()


@deprecated("Replaced by join_streams")
def joinStreams(stream_a,stream_b, **kwargs):
    """
    DEFINITION:
        Copy two streams together eventually replacing already existing time steps.
        Data of stream_a will replace data of stream_b
    APPLICATION
        combinedstream = joinStreams(stream_a,stream_b)
    """
    logger.info('joinStreams: Start joining at %s.' % str(datetime.now()))
    KEYLIST =  DataStream().KEYLIST


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
    # NEW: shape problems when removing data -> now use removeduplicates at the end
    # SHOULD WORK (already tested) as remove duplicate will keep the last value and drop earlier occurences
    #indofb = np.nonzero(np.in1d(sb.ndarray[0], sa.ndarray[0]))[0]
    #for idx,elem in enumerate(sb.ndarray):
    #    if len(sb.ndarray[idx]) > 0:
    #        sb.ndarray[idx] = np.delete(sb.ndarray[idx],indofb)

    # Now add stream_a to stream_b - regard for eventually missing column data
    # --------------------------------------
    array = [[] for key in KEYLIST]
    for idx,elem in enumerate(sb.ndarray):
        if len(sa.ndarray[idx]) > 0 and len(sb.ndarray[idx]) > 0:
            array[idx] = np.concatenate((sa.ndarray[idx],sb.ndarray[idx]))
        elif not len(sa.ndarray[idx]) > 0 and  len(sb.ndarray[idx]) > 0:
            if idx < len(stream_a.NUMKEYLIST):
                fill = np.nan
            else:
                fill = '-'
            arraya = np.asarray([fill]*len(sa.ndarray[0]))
            array[idx] = np.concatenate((arraya,sb.ndarray[idx]))
        elif len(sa.ndarray[idx]) > 0 and not len(sb.ndarray[idx]) > 0:
            if idx < len(stream_a.NUMKEYLIST):
                fill = np.nan
            else:
                fill = '-'
            arrayb = np.asarray([fill]*len(sb.ndarray[0]))
            array[idx] = np.concatenate((sa.ndarray[idx],arrayb))
        else:
            array[idx] = np.asarray([])

    stream = DataStream([LineStruct()],sa.header,np.asarray(array,dtype=object))
    stream = stream.removeduplicates()

    return stream.sorting()

def append_streams(streamlist):
    """
    DESCRIPTION:
        Appends contents of streamlist  and returns a single new stream.
        Duplicates are removed and the new stream is sorted.
    """
    res = DataStream()
    for i,stream in enumerate(streamlist):
        if i == 0:
            res = stream
        else:
            res = join_streams(res,stream)
    return res
    #    stream = stream.removeduplicates()
    #    stream = stream.sorting()
    #    return stream
    #else:
    #    return DataStream(header=streamlist[0].header,ndarray=np.asarray([np.asarray([]) for key in KEYLIST]))

@deprecated("Replaced by append_streams")
def appendStreams(streamlist):
    return append_streams(streamlist)

def merge_streams(stream_a, stream_b, keys=None, mode='insert', **kwargs):
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
        # Joining two datasets together:
        alldata = mergeStreams(lemidata, gsmdata, keys=['f'])
               # f of gsm will be added to lemi
        # inserting missing values from another stream
        new_gsm = mergeStreams(gsm1, gsm2, keys=['f'], mode='insert')
               # all missing values (nans) of gsm1 will be filled by gsm2 values (if existing)


    APPLICATION:
    """
    t1 = datetime.now()

    flag = kwargs.get('flag')
    comment = kwargs.get('comment')
    flagid = kwargs.get('flagid')
    debug = kwargs.get('debug')

    if not keys:
        keys = DataStream().KEYLIST

    # Defining default comment
    # --------------------------------------
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


    logger.info('merge_streams: Start mergings at %s.' % str(datetime.now()))

    if not len(stream_a.ndarray[0]) > 0 or not len(stream_b.ndarray[0]) > 0:
        print ("merge_streams: one of the stream is empty - aborting")
        return stream_a

    # Sampling rates
    # --------------------------------------
    s1,e1 = stream_a._find_t_limits()
    s2,e2 = stream_b._find_t_limits()
    sr1 = stream_a.samplingrate()
    sr2 = stream_b.samplingrate()
    if not (s1 <= s2 <= e1) and not (s1 <= e2 <= e1) or not (sr1==sr2):
        print ("merge_streams: check consistency of sampling rates and time ranges - aborting")
        return stream_a

    # non-destructive
    # --------------------------------------
    sa = stream_a.copy()
    sb = stream_b.copy()
    sa = sa.get_gaps()
    sb = sb.get_gaps()
    sa = sa.removeduplicates()
    sb = sb.removeduplicates()

    timea = sa.ndarray[0]
    # truncate b to time range of a
    # --------------------------------------
    try:
        sb = sb.trim(starttime=sa.start(), endtime=sa.end(), include=True)
    except:
        print("merge_streams: stream_a and stream_b are apparently not overlapping - returning stream_a")
        return stream_a
    timeb = sb.ndarray[0]

    # testing overlapp
    # --------------------------------------
    if not len(sb) > 0:
        print("merge_streams: stream_a and stream_b are not overlapping - returning stream_a")
        return stream_a

    timea = maskNAN(timea)
    timeb = maskNAN(timeb)
    # master header
    # --------------------------------------
    header = sa.header
    # just add the merged sensorid
    header['SecondarySensorID'] = sensidb

    def subset(a1, a2, len1, len2):
        i = 0
        j = 0
        if len1 < len2:  # subset not exist
            return False
        # sorting both arrays
        a1.sort()
        a2.sort()

        while i < len2 and j < len1:  # traversing arrays
            if a1[j] < a2[i]:
                j += 1
            elif a1[j] == a2[i]:  # if equal, go to next index            j += 1
                i += 1
            elif a1[j] > a2[i]:  # if array1 element is greater
                return False
        # accept 10% differences
        return False if i < len2*0.9 else True

    if not subset(timea, timeb, len(timea), len(timeb)):
        print (" Time steps are apparently not fitting")
        return stream_a

    if debug:
        t1b = datetime.now()
        print("needed", (t1b - t1).total_seconds())

    # fill sb to the length of sa with np.nan or ''
    # get the indices of stream_b in stream_a
    if debug:
        print (len(timea), len(timeb))
    way = 2 # much quicker
    # get the first index of stream_b in stream_a
    if way == 1:
        indices = np.nonzero(np.in1d(timea, timeb))[0]
    else:
        index = np.argsort(timea)
        sorted_x = timea[index]
        sorted_index = np.searchsorted(sorted_x, timeb)
        yindex = np.take(index, sorted_index, mode="clip")
        mask = timea[yindex] != timeb
        indices = np.ma.array(yindex, mask=mask)

    array = [[] for key in DataStream().KEYLIST]
    array[0] = timea
    t2 = datetime.now()
    if debug:
        print("needed", (t2 - t1).total_seconds())
    for i,key in enumerate(DataStream().KEYLIST):
        if debug:
            print ("Dealing with ", key)
            t3 = datetime.now()
        if not 'time' in key:
            array[i] = sa._get_column(key)
            colb = sb._get_column(key)
            if len(array[i]) > 0 and len(colb) > 0  and key in keys:
                # eventually insert or replace
                if mode == 'replace':
                    np.put(np.asarray(array[i]), indices, colb)
                else:
                    # add some flagdict containing inserted ranges
                    # get indices of nan values in stream_a
                    if key in DataStream().NUMKEYLIST:
                        naninds = np.argwhere(np.isnan(array[i]))
                        naninds = naninds.flatten()
                        insind = np.intersect1d(indices, naninds)
                        if not (indices[0] == 0):
                            # stream b start after beginning of a
                            # insind are relative to the length of colb
                            np.put(np.asarray(array[i]), insind, colb)
                        else:
                            np.put(np.asarray(array[i]), insind, colb[insind])
                        # get the ranges of inserted indices
                        indgroups = group_indices(insind)
                        # from indgroups a flagging dict can be created
                        # TODO when flagging package is reviewed
                    else:
                        # no action if strings - maybe in a future version
                        pass
            elif len(colb) > 0  and key in keys:
                array[i] = np.asarray([np.nan]*len(timea))
                np.put(np.asarray(array[i]),indices,colb)
                if headerb.get('col-{}'.format(key),''):
                    header['col-{}'.format(key)] = headerb.get('col-{}'.format(key),'')
                if headerb.get('unit-col-{}'.format(key),''):
                    header['unit-col-{}'.format(key)] = headerb.get('unit-col-{}'.format(key),'')
            elif len(array[i]) > 0:
                pass
            else:
                array[i] = np.asarray([])
        if debug:
            t4 = datetime.now()
            print ("needed", (t4-t3).total_seconds())

    array = np.asarray(array, dtype=object)
    comment = header.get("DataComments","")
    comment = comment + "merged {} into {} ".format(sb.header.get("DataID"),sa.header.get("DataID"))
    header["DataComments"] = comment
    return DataStream(header=header,ndarray=array)


@deprecated("Replaced by merge_streams")
def mergeStreams(stream_a, stream_b, **kwargs):
    mode = kwargs.get('mode')
    flag = kwargs.get('flag')
    keys = kwargs.get('keys')
    comment = kwargs.get('comment')
    flagid = kwargs.get('flagid')
    return merge_streams(stream_a, stream_b, mode=mode, flag=flag, keys=keys, comment=comment, flagid=flagid)


def determine_time_shift(array1, array2, col2compare='f', method='correlate', shiftvalues = [-3750, 4250, 250], debug=False):
    """
    DESCRIPTION
        Get the time shift between two data stream by comparing an eventual signal shift within the selected column.
        The method  makes use of interpolation in order to virtually increase the sensitivity to below the sampling
        resolution. Maximum is resolution is 0.01 of the sampling period.
        Basically two methods are used:
        fft method : should be much quicker for large data sets, will return two solutions
        scipy.correlate method  : correct sign is obtained
    PREREQUISITE
        input data needs have an identical resolution and needs to cover the same range
    VARIABLES
        array1 : DataStream()
        array2 : shifted DataStream()
        col2compare : key of the column to compare
        method : determination method of the shift 'correlate' (default), 'fft' or 'all'
    RETURNS
        shift : (float) in resolution 1/100 of the time timeinterval between successive point
    APPLICATION
        shift = determine_time_shift(tstream,shifted_stream, col2compare='f')
    """
    shift = None
    s1, e1 = array1._find_t_limits()
    s2, e2 = array2._find_t_limits()
    sr1 = array1.samplingrate()
    sr2 = array2.samplingrate()
    if not (s1 == s2) or not (e1 == e2) or not (sr1 == sr2) or not len(array1) == len(array2):
        print("determine_time_shift: prerequisites not met - aborting")
        return None
    a, b, N = 0, len(array1), len(array1) * 100
    func1 = array1.interpol([col2compare])
    i1 = np.linspace(a, b, N) / b
    ar1 = func1[0].get('f{}'.format(col2compare))(i1)
    ar1 = ar1 - np.nanmean(ar1)
    func2 = array2.interpol([col2compare])
    i2 = np.linspace(a, b, N) / b
    ar2 = func2[0].get('f{}'.format(col2compare))(i2)
    ar2 = ar2 - np.nanmean(ar2)

    if method == 'fft' or method == 'all':
        A = fftpack.fft(ar1)
        B = fftpack.fft(ar2)
        Ar = -A.conjugate()
        Br = -B.conjugate()
        sol1 = np.argmax(np.abs(fftpack.ifft(Ar * B)))
        sol2 = np.argmax(np.abs(fftpack.ifft(A * Br)))
        lag = min([sol1, sol2])
        sign = 1
        if sol2 > sol1:
            sign = -1
        if debug:
            print("FFT method: shift array2 by timedelta(seconds={}) to fit array1".format(sign * lag / 100. * sr1))
        shift = sign * sol1 / 100. * sr1
    if method == 'correlate' or method == 'all':
        correlation = signal.correlate(ar1, ar2, mode="full")
        lags = signal.correlation_lags(len(ar1), len(ar2), mode="full")
        lag = lags[np.argmax(abs(correlation))]
        if debug:
            print("Correlate method: shift array2 by timedelta(seconds={}) to fit array1".format(lag / 100. * sr1))
        shift = lag / 100. * sr1
    if method == 'bruteforce':
        def parabola (x, *p):
            a,b,c =p
            y = (a+x)*(a+x)/b + c
            return y

        print ("-> Selected brute force method")
        shiftlst = np.arange(shiftvalues[0],shiftvalues[1],shiftvalues[2])
        if debug:
            print (" -> using the following steps:", shiftlst)
        minlst = []
        for shift in shiftlst:
            secs = shift/1000.
            if debug:
                print ("Shifting data for seconds:", secs)
            data = array2.copy()
            data = data.offset({'time': timedelta(seconds=secs)})
            diff = subtract_streams(array1,data)
            val,std = diff.mean(col2compare,std=True)
            minlst.append([secs,std])

        stdlst = [elem[1] for elem in minlst]
        minval = min(stdlst)
        ind = stdlst.index(minval)
        minshift = minlst[ind][0]

        if debug:
            print (stdlst)
            print ([elem[0] for elem in minlst])

        from scipy.optimize import curve_fit
        p0 = [1,1,-2]
        x = np.asarray([elem[0] for elem in minlst])
        y = np.asarray(stdlst)
        # get indicies of nan
        inds = np.where(np.isnan(y))
        y=np.delete(y,inds)
        x=np.delete(x,inds)

        coeff, cov = curve_fit(parabola, x, y, p0)
        shift = -coeff[0]
        if debug:
            print ("-----------------------------------------------------------")
            print ("Reference {} compared to {}: ".format(array1.header.get('SensorID'), array2.header.get('SensorID')))
            print ("Found minimum of {:.2f} at a time shift of {:.2f} sec for {} ".format(coeff[2], -coeff[0], array2.timerange()))
            print ("-----------------------------------------------------------")


    return shift


def subtract_streams(stream_a, stream_b, keys=None, getmeans=None, debug=False):
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
        diff = subtractStreams(gsm_stream, pos_stream)

    APPLICATION:


    '''

    #t1 = datetime.now(timezone.utc).replace(tzinfo=None)
    if not keys:
        keys = stream_a._get_key_headers(numerical=True)
    keysb = stream_b._get_key_headers(numerical=True)
    keys = list(set(keys)&set(keysb))
    KEYLIST =  DataStream().KEYLIST


    if not len(keys) > 0:
        logger.error("subtractStreams: No common keys found - aborting")
        return DataStream()

    if len(stream_a.ndarray[0]) > 0:
        pass
    elif len(stream_b.ndarray[0]) > 0:
        pass
    else:
        logger.error('subtractStreams: stream_a empty - aborting subtraction.')
        return stream_a

    logger.info('subtractStreams: Start subtracting streams.')

    # non-destructive
    sa = stream_a.copy()
    sb = stream_b.copy()
    #t2 = datetime.now(timezone.utc).replace(tzinfo=None)
    headera = sa.header
    headerb = sb.header

    # Drop empty columns or columns with empty placeholders
    sa = sa._remove_nancolumns()
    sb = sb._remove_nancolumns()

    # Sampling rates
    sampratea = float(sa.samplingrate())
    samprateb = float(sb.samplingrate())
    minsamprate = min([sampratea,samprateb])

    startat,endat = sa._find_t_limits()
    # truncate b to time range of a
    try:
        sb = sb.trim(starttime=np.datetime64(startat), endtime=np.datetime64(endat)+np.timedelta64(int(samprateb*1000), 'ms'))
    except:
        logger.error("subtractStreams: stream_a and stream_b are apparently not overlapping - returning stream_a")
        if debug:
            print("subtractStreams: stream_a and stream_b are apparently not overlapping - returning stream_a")
        return stream_a
    startbt,endbt = sb._find_t_limits()
    # truncate a to range of b
    try:
        sa = sa.trim(starttime=np.datetime64(startbt), endtime=np.datetime64(endbt)+np.timedelta64(int(sampratea*1000), 'ms'),newway=True)
    except:
        logger.error("subtractStreams: stream_a and stream_b are apparently not overlapping - returning stream_a")
        if debug:
            print("subtractStreams: stream_a and stream_b are apparently not overlapping - returning stream_a")
        return stream_a
    #t3 = datetime.now(timezone.utc).replace(tzinfo=None)
    timea = sa.ndarray[0].astype(datetime64)
    timeb = sb.ndarray[0].astype(datetime64)

    # testing overlapp
    if not sb.length()[0] > 0:
        logger.error("subtractStreams: stream_a and stream_b are not overlapping - returning stream_a")
        if debug:
            print("subtractStreams: stream_a and stream_b are not overlapping - returning stream_a")
        return stream_a

    # mask empty slots (for time columns only empty inputs are masked) - very fast
    numtimea = timea.astype(float64)
    numtimeb = timeb.astype(float64)
    numtimea = maskNAN(numtimea)
    numtimeb = maskNAN(numtimeb)

    #t4 = datetime.now(timezone.utc).replace(tzinfo=None)

    # Check for the following cases:
    # 1- No overlap of a and b (Done)
    # 2- a high resolution and b low resolution (tested)
    # 3- a low resolution and b high resolution (tested)
    # 4- a shorter and fully covered by b (tested)
    # 5- b shorter and fully covered by a
    ok = True
    if ok:
            # Assuming similar time steps
            # Get indicies of stream_b of which times are present in stream_a
            array = [[] for key in KEYLIST]
            # other way (combine both columsn) and get unique
            arr = np.unique(np.concatenate((numtimeb, numtimea)))
            #if len(arr) == len(timea) and len(arr) == len(timeb):
            #    # identical stream
            # If elements in combined a,b array are only slightly different from b columns - 40 percent
            if len(arr) < int(len(timeb)*1.4):
                logger.info('subtractStreams: Found identical timesteps - using simple subtraction')
                # get common timesteps
                numcommon = np.array(sorted(list(set(numtimea).intersection(numtimeb))))
                indtia = numtimea.searchsorted(numcommon)
                indtib = numtimeb.searchsorted(numcommon)
                #t5 = datetime.now(timezone.utc).replace(tzinfo=None)

                foundnan = False
                if len(indtia) == len(indtib):
                    nanind = []
                    for key in keys:
                        foundnan = False
                        keyind = KEYLIST.index(key)
                        if len(sa.ndarray[keyind]) > 0 and len(sb.ndarray[keyind]) > 0:
                            diff = np.asarray(sa.ndarray[keyind])[indtia].astype(float) - np.asarray(sb.ndarray[keyind])[indtib].astype(float)
                            #vala = [sa.ndarray[keyind][ind] for ind in indtia]
                            #valb = [sb.ndarray[keyind][ind] for ind in indtib]
                            #diff = np.asarray(vala).astype(float) - np.asarray(valb).astype(float)
                            if isnan(diff).any():
                                foundnan = True
                            if foundnan:
                                nankeys = [ind for ind,el in enumerate(diff) if np.isnan(el)]
                                nanind.extend(nankeys)
                            array[keyind] = diff
                    #t6 = datetime.now(timezone.utc).replace(tzinfo=None)
                    nanind = np.unique(np.asarray(nanind))
                    array[0] = np.asarray(np.asarray(sa.ndarray[0])[indtia],dtype=object)
                    if foundnan:
                        for ind,elem in enumerate(array):
                            if len(elem) > 0:
                                array[ind] = np.delete(np.asarray(elem), nanind)
                    array = np.asarray(array,dtype=object)
                    #t7 = datetime.now(timezone.utc).replace(tzinfo=None)
                    #if debug:
                    #    print("prep", (t2 - t1).total_seconds())
                    #    print("trim", (t3 - t2).total_seconds())
                    #    print("numtimes", (t4 - t3).total_seconds())
                    #    print("indicies", (t5 - t4).total_seconds())
                    #    print("times", (t6 - t5).total_seconds())
                    #    print("select vals", (t7 - t6).total_seconds())
            else:
                if debug:
                    print("Did not find identical timesteps - linearily interpolating stream b")
                    print("- please note... this needs considerably longer")
                    print("- put in the larger (higher resolution) stream as stream_a")
                    print("- otherwise you might wait endless")
                # interpolate b
                function = sb.interpol(keys)
                # determine numerical times rounded to sampling rates
                numtimeafull = sa.ndarray[0].astype('datetime64[s]').astype(float64)/minsamprate
                numtimea = np.round(numtimeafull,0)
                numtimeb = np.round(sb.ndarray[0].astype('datetime64[s]').astype(float64)/minsamprate,0)
                # now get the common indicies
                numcommon = np.array(sorted(list(set(numtimea).intersection(numtimeb))))
                indtia = numtimea.searchsorted(numcommon)
                #indtib = numtimeb.searchsorted(numcommon)
                funcstart = (np.datetime64(num2date(function[1]).replace(tzinfo=None), 's').astype(float64)/minsamprate)
                funcend =  (np.datetime64(num2date(function[2]).replace(tzinfo=None), 's').astype(float64)/minsamprate)

                # Get a list of indicies for which timeb values are
                #   in the vicintiy of a (within half of samplingrate)
                # limit time range to valued covered by the interpolation function
                indtia = [elem for elem in indtia if funcstart < numtimeafull[elem] < funcend]

                foundnan = False
                if len(function) > 0:
                    nanind = []
                    for key in keys:
                        foundnan = False
                        keyind = KEYLIST.index(key)
                        #print key, keyind
                        #print len(sa.ndarray[keyind]),len(sb.ndarray[keyind]), np.asarray(indtia)
                        if len(sa.ndarray[keyind]) > 0 and len(sb.ndarray[keyind]) > 0 and key in stream_a.NUMKEYLIST: # and key in function:
                            #check lengths of sa.ndarray and last value of indtia
                            indtia = list(np.asarray(indtia)[np.asarray(indtia)<len(sa.ndarray[0])])
                            # Convert array to float just in case
                            sa.ndarray[keyind] = sa.ndarray[keyind].astype(float)
                            # interpol still working with date2num
                            dnsa = date2num(sa.ndarray[0])
                            vala = [sa.ndarray[keyind][ind] for ind in indtia]
                            valb = [float(function[0]['f'+key]((dnsa[ind]-function[1])/(function[2]-function[1]))) for ind in indtia]
                            diff = np.asarray(vala) - np.asarray(valb)
                            if np.isnan(diff).any():
                                foundnan = True
                            if foundnan:
                                nankeys = [ind for ind,el in enumerate(diff) if np.isnan(el)]
                                nanind.extend(nankeys)
                            array[keyind] = diff
                    nanind = np.unique(np.asarray(nanind))
                    array[0] = np.asarray([sa.ndarray[0][ind] for ind in indtia])
                    if foundnan:
                        for ind,elem in enumerate(array):
                            if len(elem) > 0:
                                array[ind] = np.delete(np.asarray(elem), nanind)
                    array = np.asarray(array,dtype=object)

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

            header = sa.header
            comment = header.get("DataComments","")
            comment = comment + "subtracted {} from {} ".format(sb.header.get("DataID"), sa.header.get("DataID"))
            header["DataComments"] = comment

            return DataStream(header=header,ndarray=np.asarray(array,dtype=object))


@deprecated("Replaced by subtract_streams")
def subtractStreams(stream_a, stream_b, **kwargs):
    '''
    DEFINITION:
        Replaced by subtract_streams from 2.0.0 onwards
    '''

    keys = kwargs.get('keys')
    newway = kwargs.get('newway')
    getmeans = kwargs.get('getmeans')
    debug = kwargs.get('debug')

    return subtract_streams(stream_a, stream_b, keys=keys, getmeans=getmeans, debug=debug)


@deprecated("Replaced by core.methods.extract_date_from_string")
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
        tmpdaystring = daystring[-7:]
        date = dparser.parse(tmpdaystring[:5]+' '+tmpdaystring[5:], dayfirst=True)
        dateform = '%b%d%y'
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



# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Now import the child classes with formats etc
# Otherwise DataStream etc will not be known
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
from magpy.lib.magpy_formats import *



if __name__ == '__main__':

    import subprocess

    # #######################################################
    #                      Testing
    # #######################################################
    """
    In the following you find runtime tests of the stream package.
    Verification tests based on unittest are done separately in tests/verification.py
    The runtime testing environment below contains the following parts:
    Test set creation part, runtime tests, verification tests, and application tests
    1) creation of articfical data streams for runtime and verification tests
       - runtime tests: random test set mimicking natural data with gaps and uncertainties
       - verification tests: simple data stream with clearly deducable expected values for all methods 
    2) runtime tests are applied to a complex/random test set mimicking natural data with gaps and uncertainties - hereby 
    we test the general applicability of the methods and their stability in application
    3) verification tests: are used to test the accuarcy of all calculation rountines against expected values
    4) application tests are code testings by comparing the output with indepent codes from other developers. Such tests typically 
    require real data subjected to different software packages. If such application tests have been performed then a reference is given in 
    the testing table.
    Results of these test and whether individual methods are successfully subjected to these tests are summarized. Including new methods
    requires at least the addition of runtime and verification tests.
    """


    def create_minteststream(startdate=datetime(2022, 11, 1), addnan=True):
        c = 1000  # 4000 nan values are filled at random places to get some significant data gaps
        l = 32 * 1440
        #import scipy
        teststream = DataStream()
        array = [[] for el in DataStream().KEYLIST]
        win = signal.windows.hann(60)
        a = np.random.uniform(20950, 21000, size=int(l / 2))
        b = np.random.uniform(20950, 21050, size=int(l / 2))
        x = signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        if addnan:
            x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
        array[1] = x[1440:-1440]
        a = np.random.uniform(1950, 2000, size=int(l / 2))
        b = np.random.uniform(1900, 2050, size=int(l / 2))
        y = signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        if addnan:
            y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
        array[2] = y[1440:-1440]
        a = np.random.uniform(44300, 44400, size=l)
        z = signal.convolve(a, win, mode='same') / sum(win)
        array[3] = z[1440:-1440]
        array[4] = np.sqrt((x * x) + (y * y) + (z * z))[1440:-1440]
        array[0] = np.asarray([startdate + timedelta(minutes=i) for i in range(0, len(array[1]))])
        array[KEYLIST.index('sectime')] = np.asarray(
            [startdate + timedelta(minutes=i) for i in range(0, len(array[1]))]) + timedelta(minutes=15)
        teststream = DataStream(header={'SensorID': 'Test_0001_0001'}, ndarray=np.asarray(array, dtype=object))
        minstream = teststream.filter()
        teststream.header['col-x'] = 'X'
        teststream.header['col-y'] = 'Y'
        teststream.header['col-z'] = 'Z'
        teststream.header['col-f'] = 'F'
        teststream.header['unit-col-x'] = 'nT'
        teststream.header['unit-col-y'] = 'nT'
        teststream.header['unit-col-z'] = 'nT'
        teststream.header['unit-col-f'] = 'nT'
        return teststream


    def create_secteststream(startdate=datetime(2022, 11, 21)):
        # Create a random data signal with some nan values in x and z
        c = 1000  # 1000 nan values are filled at random places
        array = [[] for el in DataStream().KEYLIST]
        x = np.random.uniform(20950, 21000, size=(72, 1))
        x = np.tile(x, (1, 60 * 60)).flatten()
        x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
        array[1] = x
        y = np.random.uniform(1950, 2000, size=(72, 1))
        array[2] = np.tile(y, (1, 60 * 60)).flatten()
        z = np.random.uniform(44350, 44400, size=(72, 1))
        z = np.tile(z, (1, 60 * 60)).flatten()
        z.ravel()[np.random.choice(z.size, c, replace=False)] = np.nan
        array[3] = z
        array[0] = np.asarray([datetime(2022, 11, 21) + timedelta(seconds=i) for i in range(0, 3 * 86400)])
        array[DataStream().KEYLIST.index('sectime')] = np.asarray(
            [datetime(2022, 11, 21) + timedelta(seconds=i) for i in range(0, 3 * 86400)]) + timedelta(minutes=15)
        # array[KEYLIST.index('str1')] = ["xxx"]*len(z)
        teststream = DataStream([], {'SensorID': 'Test_0001_0001'}, np.asarray(array, dtype=object))
        teststream.header['col-x'] = 'X'
        teststream.header['col-y'] = 'Y'
        teststream.header['col-z'] = 'Z'
        teststream.header['unit-col-x'] = 'nT'
        teststream.header['unit-col-y'] = 'nT'
        teststream.header['unit-col-z'] = 'nT'
        return teststream


    teststream = create_secteststream()
    fstream = DataStream()
    trimstream = DataStream()
    orgstream = DataStream()
    rotstream = DataStream()
    intstream = DataStream()
    filtstream = DataStream()
    func = None
    # Do indents correctly already
    ok = True
    errors = {}
    successes = {}
    if ok:
        testrun = 'streamtestfile'
        t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)
        while True:
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                sr = teststream.samplingrate()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['samplingrate'] = (
                    "Version: {}, samplingrate={}: {}".format(magpyversion, sr, (te - ts).total_seconds()))
            except Exception as excep:
                errors['samplingrate'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR determining sampling rate/get_sample_period.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                orgstream = teststream.copy()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['copy'] = ("Version: {}, copy: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['copy'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR copy stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                nonanstream = teststream._drop_nans('x', debug=True)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_drop_nans'] = (
                    "Version: {}, _drop_nans: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['_drop_nans'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR _drop_nans of stream.")
            print (teststream)
            nancolstream = teststream._remove_nancolumns()
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                nancolstream = teststream._remove_nancolumns()
                print ("Length with nan: {}, length without nan: {}".format(len(teststream),len(nancolstream)))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_remove_nancolumns'] = (
                    "Version: {}, _remove_nancolumns: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['_remove_nancolumns'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR _remove_nancolumns stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                trimstream = teststream.trim(starttime=datetime(2022, 11, 22, 6), endtime=datetime(2022, 11, 22, 10))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['trim'] = ("Version: {}, trim: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['trim'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR trim stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                fstream = teststream.calc_f()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['calc_f'] = ("Version: {}, calc_f: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['calc_f'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR calc_f of stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                a = fstream._get_column('f')
                # insert a array of fitting length into a specified key
                fstream._put_column(a, 'var1', columnname='Myval', columnunit='roman')
                # move a specific column into another specified key
                fstream._copy_column('var1', 'var2')
                fstream._move_column('var1', 'var4')
                fstream._drop_column('var2')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_copymovedropput_column'] = (
                    "Version: {}, _copymovedropput_column: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['_copymovedropput_column'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR when applying column modifications.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                filterlist = ['flat', 'barthann', 'bartlett', 'blackman', 'blackmanharris', 'bohman',
                              'boxcar', 'cosine', 'flattop', 'hamming', 'hann', 'nuttall', 'parzen', 'triang',
                              'gaussian', 'wiener', 'butterworth']
                for filter_type in filterlist:
                    tts = datetime.now(timezone.utc).replace(tzinfo=None)
                    tmpstream = trimstream.filter(filter_type=filter_type, filter_width=timedelta(hours=3),
                                                  missingdata='interpolate', debug=False)
                    tte = datetime.now(timezone.utc).replace(tzinfo=None)
                    print("Running filter {} needed {}: Length: {}".format(filter_type, (tte - tts).total_seconds(),
                                                                           len(tmpstream)))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['filter'] = ("Version: {}, filter: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['filter'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR filtering or resampling stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                filtstream = teststream.filter(missingdata='interpolate', debug=False)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['filter2'] = ("Version: {}, filter2: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['filter2'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR filtering 1-sec stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                smoothlist = ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']
                for smooth_type in smoothlist:
                    tts = datetime.now(timezone.utc).replace(tzinfo=None)
                    smstream = trimstream.copy()
                    tmpstream = smstream.smooth(keys=['x'], window_len=5, window=smooth_type, debug=False)
                    tte = datetime.now(timezone.utc).replace(tzinfo=None)
                    print("Running smooth {} needed {}: Length: {}".format(smooth_type, (tte - tts).total_seconds(),
                                                                           len(tmpstream)))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['smooth'] = ("Version: {}, smooth: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['smooth'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR smoothing stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                rotstream = teststream.rotation(alpha=1.0)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['rotation'] = ("Version: {}, rotation: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['rotation'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR rotating stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                offstream1 = teststream.offset({'x': 150, 'y': -2000, 'z': 3.2})
                offstream2 = teststream.offset({'time': 'timedelta(seconds=-0.33)'}, starttime='2022-01-01',
                                               endtime='2023-01-01')
                teststream = orgstream.copy() # offset is destructive - revoke the original data
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['offset'] = ("Version: {}, offset: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['offset'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR offsetting stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                fstream = teststream.calc_f()
                teststream.header["DataDeltaValues"] = '{"0": {"st": "1971-11-22 00:00:00", "f": -1.48, "time": "timedelta(seconds=-3.0)", "et": "2018-01-01 00:00:00"}, "1": {"st": "2018-01-01 00:00:00", "f": -1.571, "time": "timedelta(seconds=-3.0)", "et": "2018-09-14 12:00:00"}, "2": {"st": "2018-09-14 12:00:00", "f": -1.571, "time": "timedelta(seconds=1.50)", "et": "2019-01-01 00:00:00"}, "3": {"st": "2019-01-01 00:00:00", "f": -1.631, "time": "timedelta(seconds=-0.30)", "et": "2020-01-01 00:00:00"}, "4": {"st": "2020-01-01 00:00:00", "f": -1.616, "time": "timedelta(seconds=-0.28)", "et": "2021-01-01 00:00:00"}, "5": {"st": "2021-01-01 00:00:00", "f": -1.609, "time": "timedelta(seconds=-0.28)", "et": "2022-01-01 00:00:00"}, "6": {"st": "2022-01-01 00:00:00", "f": -1.655, "time": "timedelta(seconds=-0.33)", "et": "2023-01-01 00:00:00"}, "7": {"st": "2023-01-01 00:00:00", "f": -1.729, "time": "timedelta(seconds=-0.28)"}}'
                res = fstream.apply_deltas()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['apply_deltas'] = (
                    "Version: {}, apply_deltas: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['apply_deltas'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with apply_deltas to stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                rotstream = rotstream.get_gaps()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['get_gaps'] = ("Version: {}, get_gaps: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['get_gaps'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR get_gaps of stream.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                starttime1, endtime1 = teststream._find_t_limits()
                starttime2, endtime2 = teststream.timerange()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['timerange'] = (
                    "Version: {}, timerange: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['timerange'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with timerange")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                keys1 = teststream._get_key_headers()
                keys2 = teststream.variables()
                print("Printing keys, variables and units:")
                teststream._print_key_headers()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_get_key_headers'] = (
                    "Version: {}, _get_key_headers: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['_get_key_headers'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _get_key_headers")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                keys = teststream.integrate()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['integrate'] = ("Version: {}, integrate: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['integrate'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with integrate")
            try:
                intstream = teststream.copy()
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                func = intstream.interpol(['x', 'y'])
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['interpol'] = (
                    "Version: {}, interpolation: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['interpol'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with interpolation")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                trimstream = trimstream.interpolate_nans(keys=['x'])
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['interpolate_nans'] = (
                    "Version: {}, interpolate_nans: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['interpolate_nans'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with interpolate_nans")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                intstream = intstream.func2header(func)
                intstream = intstream.func2stream(func, ['x', 'y'], mode='values')
                func_to_file(func, "/tmp/deleteme.json")
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['functiontools'] = (
                    "Version: {}, functiontools: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['functiontools'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with functiontools")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                xxx = teststream.multiply({'x': -1})
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['multiply'] = ("Version: {}, multiply: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['multiply'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with multiply")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                xxx = teststream.randomdrop(percentage=50, fixed_indicies=None)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['randomdrop'] = (
                    "Version: {}, randomdrop: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['randomdrop'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with randomdrop")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                red = teststream.extract('x', 20985, compare='<')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['extract'] = ("Version: {}, extract: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['extract'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with extract")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                d = teststream.contents()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['contents'] = ("Version: {}, contents: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['contents'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with contents")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                d = teststream.stats()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['stats'] = ("Version: {}, stats: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['stats'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with stats")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                xxx = teststream.cut(50, kind=0, order=0)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['cut'] = ("Version: {}, cut: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['cut'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with cut")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                xxx = teststream.dailymeans(keys=['x', 'y', 'z'])
                print("dailymeans:", len(xxx))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['daileymeans'] = (
                    "Version: {}, dailymeans: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['dailymeans'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with dailymeans")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                trimstream = teststream.trim(starttime="2022-11-22T09:00:00", endtime="2022-11-22T14:00:00")
                for method in ['linear', 'spline', 'fourier', 'old']:
                    print(" Testing extrapolation with {} method".format(method))
                    expst = trimstream.extrapolate(starttime=datetime(2022, 11, 22, 7),
                                                   endtime=datetime(2022, 11, 22, 20), method=method)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['extrapolate'] = (
                    "Version: {}, extrapolate: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['extrapolate'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with extrapolation")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                trimstream = teststream.trim(starttime="2022-11-22T09:00:00", endtime="2022-11-22T14:00:00")
                for fitoption in ['poly', 'harmonic', 'least-squares', 'mean', 'spline']:
                    print(" Testing fitting with {} method".format(fitoption))
                    func = trimstream.fit(['x'], fitfunc=fitoption, fitdegree=5, knotstep=0.2)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['fit'] = ("Version: {}, fit: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['fit'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with fit")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                xstream = trimstream._select_keys(keys=['x'])
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['_select_keys'] = (
                    "Version: {}, _select_keys: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['_select_keys'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with _select_keys")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                alpha, beta, gamma = trimstream.determine_rotationangles()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['determine_rotationangles'] = (
                    "Version: {}, determine_rotationangles: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['determine_rotationangles'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with determine_rotationangles")
            try:
                sectest = orgstream.copy()
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                uniq = sectest.union(np.asarray([1,1,2,2,3,3,3,3,3]))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                print("Test Union:", uniq)
                successes['union'] = (
                    "Version: {}, union: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['union'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with union")
            try:
                sectest = orgstream.copy()
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                uniq = sectest.unique('time')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['unique'] = (
                    "Version: {}, unique: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['unique'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with unique")
            try:
                sectest = orgstream.copy()
                s1, e1 = sectest._find_t_limits()
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                sectest = sectest.use_sectime()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                s2, e2 = sectest._find_t_limits()
                print(s2, e2)
                print("Switched to second time. Projected difference is 15 min, obtaioned are {} min".format(
                    (s2 - s1).total_seconds() / 60.))
                successes['use_sectime'] = (
                    "Version: {}, use_sectime: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['use_sectime'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with use_sectime")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                print("Key x: {} {}".format(trimstream.get_key_name('x'), trimstream.get_key_unit('x')))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['get_key_'] = ("Version: {}, get_key_: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['get_key_'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with get_key_name or unit")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                amp = trimstream.amplitude('x')
                var = trimstream._get_variance('x')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['guimethods'] = (
                    "Version: {}, guimethods: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['guimethods'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with guimethods")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                convertstream = teststream.copy()
                convlist = ['xyz2hdz', 'hdz2xyz', 'xyz2idf', 'idf2xyz']
                print(
                    "Conversion testrun starting with comps {:.2f}, {:.2f}, {:.2f}".format(convertstream.ndarray[1][0],
                                                                                           convertstream.ndarray[2][0],
                                                                                           convertstream.ndarray[3][0]))
                for conv in convlist:
                    convertstream = convertstream._convertstream(conv)
                    print("Conversion {} results in comps {:.2f}, {:.2f}, {:.2f}".format(conv,
                                                                                         convertstream.ndarray[1][0],
                                                                                         convertstream.ndarray[2][0],
                                                                                         convertstream.ndarray[3][0]))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['conversion'] = (
                    "Version: {}, conversion: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['conversion'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with conversion")
            try:
                wstream = create_minteststream(startdate=datetime(2022, 11, 1), addnan=False)
                wstream = wstream.trim(datetime(2022, 11, 1),datetime(2022, 11, 2))
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                """
                untested here because of different data requirements (tests within format libraries): 
                IAF, BLV, BLV1_2, IYFV, DKA
                """
                wformats = ['CSV', 'IAGA', 'IMAGCDF', 'IMF', 'PYSTR', 'PYASCII', 'PYCDF','WDC','DIDD','LATEX','COVJSON']
                # will not work for monthly files, IAF etc
                for idx, f in enumerate(wformats):
                    print("testing wformat", f)
                    wstream.write('.',
                                     filenamebegins='{}_{}_'.format(testrun, idx),
                                     filenameends='.tst',
                                     dateformat='%Y-%m-%d',
                                     format_type=f)
                    print("Writing as {} successful".format(f))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['write'] = ("Version: {}, write: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['write'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR writing data to file.")
            print(" TESTING MULTIPLE STREAM METHODS")
            data2 = create_minteststream(startdate=datetime(2022, 11, 15), addnan=False)
            data1 = create_minteststream(startdate=datetime(2022, 11, 1), addnan=False)
            data1.ndarray[1][31000:33000] = np.nan
            data2.ndarray[1][5000:9000] = np.nan
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                jst = join_streams(data1, data2, comment=True)
                print("check lenght of joind stream (expected 63360)", len(jst))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['join_streams'] = (
                    "Version: {}, join_streams: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['join_streams'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with join_streams")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                mst = merge_streams(data1, data2, mode='insert')
                print("check content in data1 data gap", np.nanmean(mst.ndarray[1][31000:33000]))
                print("check content in data2 (should be similar as above, not exactly the same)",
                      np.nanmean(mst.ndarray[1][8000:10000]))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['merge_streams'] = (
                    "Version: {}, merge_streams: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['merge_streams'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with merge_streams")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                sst = subtract_streams(data1, data2)
                print("check values before data gap (expected ~ 16)", np.nanmean(sst.ndarray[1][0:4000]))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['subtract_streams'] = (
                    "Version: {}, subract_streams: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['subtract_streams'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with subtract_streams")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                shifted_data1 = data1.copy()
                shifted_data1 = shifted_data1.use_sectime()
                data1 = data1.trim("2022-11-22T00:00:00", "2022-11-23T00:00:00")
                shifted_data1 = shifted_data1.trim("2022-11-22T00:00:00", "2022-11-23T00:00:00")
                shift = determine_time_shift(data1, shifted_data1, col2compare='f', debug=True)
                print("check shift: should be -15 min: ", np.round(shift / 60., 0))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['determine_time_shift'] = (
                    "Version: {}, determine_time_shift: {}".format(magpyversion, (te - ts).total_seconds()))
                shift = determine_time_shift(data1, shifted_data1, col2compare='f', method='bruteforce',
                                             shiftvalues=[-1500000, -500000, 10000], debug=True)
            except Exception as excep:
                errors['determine_time_shift'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with determine_time_shift")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                data1 = read("https://cobs.zamg.ac.at/gsa/webservice/query.php?id=WIC")
                data2 = read("https://cobs.zamg.ac.at/gsa/webservice/query.php?id=WIC", starttime='2024-08-01',
                            endtime='2024-08-02')
                data3 = read(
                    "https://imag-data-staging.bgs.ac.uk/GIN_V1/GINServices?Request=GetData&observatoryIagaCode=WIC&publicationState=adjusted&samplesPerDay=minute&format=iaga2002",
                    starttime='2024-08-01', endtime='2024-08-02', debug=True)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['webservice_read'] = (
                    "Version: {}, webservice_read: {}".format(magpyversion, (te - ts).total_seconds()))
            except Exception as excep:
                errors['webservice_read'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with webservice_read")

            """
            Unused or moved elswhere:
        - obspy2magpy              -> core.conversions (untested)

    TODO: check validity of gaussian filter (as period treatment might have changed from day to second treatment) 


            """
            # If end of routine is reached... break.
            break

        t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
        time_taken = t_end_test - t_start_test
        print(datetime.now(timezone.utc).replace(tzinfo=None),
              "- Stream testing completed in {} s. Results below.".format(time_taken.total_seconds()))

        print()
        print("----------------------------------------------------------")
        del_test_files = 'rm {}*'.format(testrun)
        subprocess.call(del_test_files, shell=True)
        del_test_files = 'rm {}*'.format(testrun.upper())
        subprocess.call(del_test_files, shell=True)
        del_test_files = 'rm xyz_2022*'
        subprocess.call(del_test_files, shell=True)
        if errors == {}:
            print("0 errors! Great! :)")
        else:
            print(len(errors), "errors were found in the following functions:")
            print(" {}".format(errors.keys()))
            print()
            for item in errors:
                print(item + " error string:")
                print("    " + errors.get(item))
        print()
        print("Good-bye!")
        print("----------------------------------------------------------")

# That's all, folks!
