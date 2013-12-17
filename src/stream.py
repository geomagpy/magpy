#!/usr/bin/env python
"""
MagPy-General: Standard pymag package containing the following classes:
Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 23.02.2012)
"""

# ----------------------------------------------------------------------------
# Part 1: Import routines for packages
# ----------------------------------------------------------------------------

logpygen = '' 		# loggerstream variable
netcdf = True 		# Export routines for netcdf
spacecdf = True 	# Export routines for Nasa cdf
mailingfunc = True 	# E-mail notifications
nasacdfdir = "c:\CDF Distribution\cdf33_1-dist\lib"

# Standard packages
# -----------------
try:
    import csv
    import pickle
    import struct
    import logging
    import sys, re
    import thread, time, string, os, shutil
    import copy
    import fnmatch
    import urllib2
    from tempfile import NamedTemporaryFile
    import warnings
    from glob import glob, iglob, has_magic
    from StringIO import StringIO
    import operator # used for stereoplot legend
except ImportError:
    print "Init MagPy: Critical Import failure: Python numpy-scipy required - please install to proceed"

# Matplotlib
# ----------
try:
    import matplotlib
    if not os.isatty(sys.stdout.fileno()):   # checks if stdout is connected to a terminal (if not, cron is starting the job)
        print "No terminal connected - assuming cron job and using Agg for matplotlib"
        matplotlib.use('Agg') # For using cron
except:
    print "Prob with matplotlib"

try:
    version = matplotlib.__version__.replace('svn', '')
    version = map(int, version.strip("rc").split("."))
    MATPLOTLIB_VERSION = version
    print "Loaded Matplotlib - Version %s" % str(MATPLOTLIB_VERSION)
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize 
    #from matplotlib.colorbar import ColorbarBase 
    from matplotlib import mlab 
    from matplotlib.dates import date2num, num2date
    import matplotlib.cm as cm
    from pylab import *
    from datetime import datetime, timedelta
except ImportError:
    logpygen += "Init MagPy: Critical Import failure: Python matplotlib required - please install to proceed\n"

# Numpy & SciPy
# -------------
try:
    import numpy as np
    import scipy as sp
    from scipy import interpolate
    from scipy import stats
    import math
except ImportError:
    logpygen += "Init MagPy: Critical Import failure: Python numpy-scipy required - please install to proceed\n"

# NetCDF
# ------
try:
    print "Loading Netcdf4 support ..."
    from netCDF4 import Dataset
    print "success"
except ImportError:
    netcdf = False
    print " -failed- "
    logpygen += "Init MagPy: Import failure: Netcdf not available\n"
    pass

# NASACDF - SpacePy
# -----------------
try:
    print "Loading Spacepy package cdf support ..."
    try:
        os.putenv("CDF_LIB", nasacdfdir)
        print "trying CDF lib at %s" % nasacdfdir
        import spacepy.pycdf as cdf
        print "... success"
    except:
        os.putenv("CDF_LIB", "/usr/local/cdf/lib")
        print "trying CDF lib in /usr/local/cdf"
        import spacepy.pycdf as cdf      
        print "... success"
except:
    logpygen += "Init MagPy: Import failure: Nasa cdf not available\n"
    print " -failed- check spacepy package"
    pass

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
except:
    mailingfunc = False
    logpygen += "pymag-general: Import failure: Mailing functions not available\n"
    pass

if logpygen == '':
    logpygen = "OK"


# Logging
# ---------

from os.path import expanduser  # select the home directory of the user - platform independent
home = expanduser("~")
logfile = os.path.join(home,'magpy.log')

logging.basicConfig(filename=logfile,
			filemode='w',
			format='%(asctime)s %(levelname)-8s- %(name)-6s %(message)s',
			level=logging.INFO)

# define a Handler which writes "setLevel" messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.WARNING)

# Package loggers to identify info/problem source
loggerabs = logging.getLogger('abs')
loggertransfer = logging.getLogger('transf')
loggerdatabase = logging.getLogger('db')
loggerstream = logging.getLogger('stream')
loggerlib = logging.getLogger('lib')

# Special loggers for event notification
stormlogger = logging.getLogger('stream')

# ----------------------------------------------------------------------------
# Part 2: Define Dictionaries
# ----------------------------------------------------------------------------

KEYLIST = ['time','x','y','z','f','t1','t2','var1','var2','var3','var4','var5','dx','dy','dz',
		'df','str1','str2','str3','str4','flag','comment','typ','sectime']
KEYINITDICT = {'time':0,'x':float('nan'),'y':float('nan'),'z':float('nan'),'f':float('nan'),'t1':float('nan'),'t2':float('nan'),
		'var1':float('nan'),'var2':float('nan'),'var3':float('nan'),'var4':float('nan'),'var5':float('nan'),'dx':float('nan'),
		'dy':float('nan'),'dz':float('nan'),'df':float('nan'),'str1':'-','str2':'-','str3':'-','str4':'-','flag':'0000000000000000-',
		'comment':'-','typ':'xyzf','sectime':float('nan')}
FLAGKEYLIST = KEYLIST[:16]
# KEYLIST[:8] # only primary values with time
# KEYLIST[1:8] # only primary values without time

PYMAG_SUPPORTED_FORMATS = ['IAGA', 'WDC', 'DIDD', 'GSM19', 'LEMIHF', 'LEMIBIN', 'LEMIBIN2',
				'OPT', 'PMAG1', 'PMAG2', 'GDASA1', 'GDASB1','RMRCS', 
				'CR800','RADON', 'USBLOG', 'SERSIN', 'SERMUL', 'PYSTR',
				'AUTODIF', 'AUTODIF_FREAD', 'PYCDF', 'PYBIN', 'POS1TXT', 
				'POS1', 'PYNC', 'DTU1', 'SFDMI', 'SFGSM', 'BDV1', 'GFZKP', 
				'NOAAACE','LATEX','CS','UNKOWN']

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

    Application methods:
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
    - stream.func_add() -- Add a function to the selected values of the data stream
    - stream.func_subtract() -- Subtract a function from the selected values of the data stream
    - stream.get_gaps() -- Takes the dominant sample frequency and fills non-existing time steps
    - stream.get_sampling_period() -- returns the dominant sampling frequency in unit ! days !
    - stream.integrate() -- returns stream (integrated vals at !dx!,!dy!,!dz!,!df!)
    - stream.interpol(keys) -- returns function
    - stream.k_fmi() -- Calculating k values following the fmi approach
    - stream.mean() -- Calculates mean values for the specified key, Nan's are regarded for
    - stream.obspyspectrogram() -- Computes and plots spectrogram of the input data
    - stream.offset() -- Apply constant offsets to elements of the datastream
    - stream.plot() -- plot keys from stream
    - stream.pmspectrogram(keys)
    - stream.powerspectrum() -- Calculating the power spectrum following the numpy fft example
    - stream.remove_flagged() -- returns stream (removes data from stream according to flags)
    - stream.remove_outlier() -- returns stream (adds flags and comments)
    - stream.resample(period) -- Resample stream to given sampling period.
    - stream.rotation() -- Rotation matrix for rotating x,y,z to new coordinate system xs,ys,zs
    - stream.smooth(key) -- smooth the data using a window with requested size
    - stream.spectrogram() -- Creates a spectrogram plot of selected keys
    - stream.trim() -- returns stream within new time frame
    - stream.write() -- Writing Stream to a file

    Supporting internal methods are:

    A. Standard functions and overrides for list like objects
    - self.clear_header(self) -- Clears headers
    - self.extend(self,datlst,header) -- Extends stream object
    - self._print_key_headers(self) -- Prints keys in datastream with variable and unit.
    - self.sorting(self) -- Sorts object

    B. Internal Methods I: Line & column functions
    - self._get_column(key) -- returns a numpy array of selected columns from Stream
    - self._put_column(key) -- adds a column to a Stream
    - self._move_column(key, put2key) -- moves one column to another key
    - self._clear_column(key) -- clears a column to a Stream
    - self._get_line(self, key, value) -- returns a LineStruct element corresponding to the first occurence of value within the selected key
    - self._reduce_stream(self) -- Reduces stream below a certain limit.
    - self._remove_lines(self, key, value) -- removes lines with value within the selected key

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
    - self._find_nearest(self, array, value) -- find point in array closest to value
    - self._testtime(time) -- returns datetime object
    - self._get_min(key) -- returns float
    - self._get_max(key) -- returns float
    - self._nearestPow2(self, x) -- Find power of two nearest to x 
    - self._normalize(column) -- returns list,float,float -- normalizes selected column to range 0,1
    - self._denormalize -- returns list -- (column,startvalue,endvalue) denormalizes selected column from range 0,1 ro sv,ev
    - self._maskNAN(self, column) -- Tests for NAN values in column and usually masks them
    - self._nan_helper(self, y) -- Helper to handle indices and logical indices of NaNs
    - self._nearestPow2(self, x) -- Find power of two nearest to x
    - self._drop_nans(self, key) -- Helper to drop lines with NaNs in any of the selected keys.
    - self._is_number(self, s) -- ?
    
    Standard function description format:

    DEFINITION:
        Description of function purpose and usage.

    PARAMETERS:
    Variables:
        - variable: 	(type) Description.
    Kwargs:
        - variable: 	(type) Description.

    RETURNS:
        - variable: 	(type) Description.

    EXAMPLE:
        >>> alldata = mergeStreams(pos_stream, lemi_stream, keys=['x','y','z'])

    APPLICATION:
        Code for simple application.

    """

    def __init__(self, container=None, header={}):
        if container is None:
            container = []
        self.container = container
        #if header is None:
        #    header = {'Test':'Well, it works'}
            #header = {}
        self.header = header
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

    def extend(self,datlst,header):
        self.container.extend(datlst)
        self.header = header

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
	get a list of existing keys in stream.

    PARAMETERS:
    kwargs:
	- limit:	(int) limit the lenght of the list
    RETURNS:
        - keylist: 	(array) a list like ['x','y','z']

    EXAMPLE:
        >>> data_stream._get_key_headers(limit=1)
	"""

        limit = kwargs.get('limit')

        keylist = []
        for key in FLAGKEYLIST[1:]:
            try:
                header = self.header['col-'+key]
		keylist.append(key)
            except:
                header = None
            try:
                unit = self.header['unit-col-'+key]
            except:
                unit = None
        if limit and len(keylist) > limit:
            keylist = keylist[:limit]

        return keylist

    def sorting(self):
        """
        Sorting data according to time (maybe generalize that to some key)
        """
        liste = sorted(self.container, key=lambda tmp: tmp.time)
        return DataStream(liste, self.header)

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

        # Get only columns which contain different data from initialization
        count = 0
        col = []
        for elem in self:
            try: # Testing whether elem.key is a float including nan to determine diffs to initialization
                nantest = float(eval('elem.'+key))
                if eval('elem.'+key) != KEYINITDICT[key] and not isnan(eval('elem.'+key)):
                    count = count+1
            except:
                if eval('elem.'+key) != KEYINITDICT[key]:
                    count = count+1
                pass
            col.append(eval('elem.'+key))
            
        if count > 0:
            return np.asarray(col)
        else:
            return np.asarray([])

        #return np.asarray([eval('elem.'+key) for elem in self])


    def _put_column(self, column, key, **kwargs):
        """
        adds a column to a Stream
        """
        #init = kwargs.get('init')
        #if init>0:
        #    for i in range init:
        #    self.add(float('NaN'))

        if not key in KEYLIST:
            raise ValueError, "Column key not valid"
        if not len(column) == len(self):
            raise ValueError, "Column length does not fit Datastream"
        for idx, elem in enumerate(self):
            exec('elem.'+key+' = column[idx]')
            
        return self

    def _move_column(self, key, put2key):
	'''
    DEFINITION:
	Move column of key "key" to key "put2key".
	Simples.

    PARAMETERS:
    Variables:
        - key: 		(str) Key to be moved.
        - put2key: 	(str) Key for 'key' to be moved to.

    RETURNS:
        - stream: 	(DataStream) DataStream object.

    EXAMPLE:
        >>> data_stream._move_column('f', 'var1')
	'''

        if not key in KEYLIST:
            loggerstream.error("_move_column: Column key %s not valid!" % key)
        if key == 'time':
            loggerstream.error("_move_column: Cannot move time column!")
        if not put2key in KEYLIST:
            loggerstream.error("_move_column: Column key %s (to move %s to) is not valid!" % (put2key,key))
        try:
            for i, elem in enumerate(self):
                exec('elem.'+put2key+' = '+'elem.'+key)
                if key in ['x','y','z','f','dx','dy','dz','df','var1','var2','var3','var4']:
	            exec('elem.'+key+' = float("NaN")')
                else:
                    exec('elem.'+key+' = "-"')
            try:
                exec('self.header["col-%s"] = self.header["col-%s"]' % (put2key, key))
                exec('self.header["unit-col-%s"] = self.header["unit-col-%s"]' % (put2key, key))
                exec('self.header["col-%s"] = ""' % (key))
                exec('self.header["unit-col-%s"] = ""' % (key))
            except:
                loggerstream.error("_move_column: Error updating headers.")
            loggerstream.info("_move_column: Column %s moved to column %s." % (key, put2key))
        except:
            loggerstream.error("_move_column: It's an error.")

        return self


    def _clear_column(self, key):
        """
        adds a column to a Stream
        """
        #init = kwargs.get('init')
        #if init>0:
        #    for i in range init:
        #    self.add(float('NaN'))

        if not key in KEYLIST:
            raise ValueError, "Column key not valid"
        for idx, elem in enumerate(self):
            if key in ['x','y','z','f','dx','dy','dz','df','var1','var2','var3','var4']:
                exec('elem.'+key+' = float("NaN")')
            else:
                exec('elem.'+key+' = "-"')
                   
        return self

    def _reduce_stream(self, **kwargs):
        """
    DEFINITION:
        Reduces size of stream for plotting methods to save memory
        when plotting large data sets.
        Does NOT filter or smooth!
        This function purely removes data points (rows) in a 
        periodic fashion until size is <100000 data points.
	(Point limit can also be defined.)

    PARAMETERS:
    Kwargs:
        - pointlimit: 	(int) Max number of points to include in stream. Default is 100000.

    RETURNS:
        - DataStream: 	(DataStream) New stream reduced to below pointlimit.

    EXAMPLE:
        >>> lessdata = ten_Hz_data._reduce_stream(pointlimit=500000)

        """

	pointlimit = kwargs.get('pointlimit')

        if not pointlimit:
            pointlimit = 100000      

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

        loggingstream.info("_reduce_stream: Stream size reduced from %s to %s points." % (size,len(lst)))

        return DataStream(lst, self.header)


    # ------------------------------------------------------------------------
    # B. Internal Methods: Data manipulation functions
    # ------------------------------------------------------------------------

    def _aic(self, signal, k, debugmode=None):
        try:
            aicval = k* np.log(np.var(signal[:k]))+(len(signal)-k-1)*np.log(np.var(signal[k:]))
        except:
            if debugmode:
                loggerstream.debug('_AIC: could not evaluate AIC at index position %i' % (k))
            pass               
        return aicval


    def _get_k(self, **kwargs):
        """
        Calculates the k value according to the Bartels scale
        Requires alpha to be set correctly (default: alpha = 1 valid for Niemegk)
        default Range nT 0 5 10 20 40 70 120 200 330 500
        default KValue   0 1  2  3  4  5   6   7   8   9
        key: defines the column to write k values (default is t2)
        """

        key = kwargs.get('key')
        put2key = kwargs.get('put2key')
        puthead = kwargs.get('puthead')
        putunit = kwargs.get('putunit')
        alpha = kwargs.get('alpha')
        scale = kwargs.get('scale')
        if not alpha:
            alpha = 1
        if not key:
            key = 'dx'
        if not put2key:
            put2key = 't2'
        if not scale:
            scale = [5,10,20,40,70,120,200,330,500] # Bartles scale

        k = 9
        outstream = DataStream()
        for elem in self:
            exec('dH = elem.'+key)
            if dH < scale[8]*alpha:
                k = 8
            if dH < scale[7]*alpha:
                k = 7
            if dH < scale[6]*alpha:
                k = 6
            if dH < scale[5]*alpha:
                k = 5
            if dH < scale[4]*alpha:
                k = 4
            if dH < scale[3]*alpha:
                k = 3
            if dH < scale[2]*alpha:
                k = 2
            if dH < scale[1]*alpha:
                k = 1
            if dH < scale[0]*alpha:
                k = 0
            exec('elem.'+put2key+' = k')
            outstream.add(elem)

        return outstream


    def _get_k_float(self, value, **kwargs):
        """
        Like _get_k, but for testing single values and not full stream keys (used in filtered function)
        Calculates the k value according to the Bartels scale
        Requires alpha to be set correctly (default: alpha = 1 valid for Niemegk)
        default Range nT 0 5 10 20 40 70 120 200 330 500
        default KValue   0 1  2  3  4  5   6   7   8   9
        """

        puthead = kwargs.get('puthead')
        putunit = kwargs.get('putunit')
        alpha = kwargs.get('alpha')
        scale = kwargs.get('scale')
        if not alpha:
            alpha = 1
        if not scale:
            scale = [5,10,20,40,70,120,200,330,500] # Bartles scale

        k = 9
        if value < scale[8]*alpha:
            k = 8
        if value < scale[7]*alpha:
            k = 7
        if value < scale[6]*alpha:
            k = 6
        if value < scale[5]*alpha:
            k = 5
        if value < scale[4]*alpha:
            k = 4
        if value < scale[3]*alpha:
            k = 3
        if value < scale[2]*alpha:
            k = 2
        if value < scale[1]*alpha:
            k = 1
        if value < scale[0]*alpha:
            k = 0

        return k


    def _get_max(self, key):
        if not key in KEYLIST[:16]:
            raise ValueError, "Column key not valid"
        elem = max(self, key=lambda tmp: eval('tmp.'+key))
        return eval('elem.'+key)


    def _get_min(self, key):
        if not key in KEYLIST[:16]:
            raise ValueError, "Column key not valid"
        elem = min(self, key=lambda tmp: eval('tmp.'+key))
        return eval('elem.'+key)


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
        Convert coordinates of x,y,z columns in stream
        coordinate:
        - xyz2hdz
        - xyz2idf
        - hdz2xyz
        - idf2xyz
        
        """
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


    def _denormalize(self, column, startvalue, endvalue):
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


    def _det_trange(self, period):
        """
        starting with coefficients above 1%
        is now returning a timedelta object
        """
        return np.sqrt(-np.log(0.01)*2)*self._tau(period)


    def _find_nearest(self, array, value):
        idx=(np.abs(array-value)).argmin()
        return array[idx], idx

        
    def _is_number(self, s):
        """
        Test whether s is a number
        """
        try:
            float(s)
            return True
        except ValueError:
            return False


    def _maskNAN(self, column):
        """
        Tests for NAN values in column and usually masks them
        """
        
        try: # Test for the presence of nan values
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


    def _nan_helper(self, y):
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
        return np.isnan(y), lambda z: z.nonzero()[0]


    def _nearestPow2(self, x): 
        """
        Function taken from ObsPy
        Find power of two nearest to x 
        >>> _nearestPow2(3) 
        2.0 
        >>> _nearestPow2(15) 
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


    def _normalize(self, column):
        """
        normalizes the given column to range [0:1]
        """
        normcol = []
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


    def _drop_nans(self, key):
        """Helper to drop lines with NaNs in any of the selected keys.

        """
        #print key
        #keylst = [eval('elem.'+key) for elem in self]
        #print keylst
        newst = [elem for elem in self if not isnan(eval('elem.'+key)) and not isinf(eval('elem.'+key))]
        return DataStream(newst,self.header)


    # ------------------------------------------------------------------------
    # C. Application methods
    # 		(in alphabetical order)
    # ------------------------------------------------------------------------

    def aic_calc(self, key, **kwargs):
        """
        Picking storm onsets using the Akaike Information Criterion (AIC) picker
        - extract one dimensional array from DataStream (e.g. H) -> signal
        - take the first k values of the signal and calculates variance and log
        - plus the rest of the signal (variance and log)

        Required:
        :type key: string
        :param key: needs to be an element of KEYLIST

        Optional keywords:
        :type timerange: timedelta object
        :param timerange: defines the length of the time window examined by the aic iteration
                        default: timedelta(hours=1)
        :type aic2key: string 
        :param aic2key: defines the key of the column where to save the aic values (default = var2)
        :type aicmin2key: string
        :param aicmin2key: defines the key of the column where to save the aic minimum val
                        default: key = var1
        :type aicminstack: bool
        :param aicminstack: if true, aicmin values are added to previously present column values
                        
        Example:
        
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
        self = self._clear_column(aic2key)
        # get sampling interval for normalization - need seconds data to test that
        sp = self.get_sampling_period()*24*60
        # corrcet approach
        iprev = 0
        iend = 0

        while iend < len(t)-1:
            istart = iprev
            ta, iend = self._find_nearest(np.asarray(t), date2num(num2date(t[istart]).replace(tzinfo=None) + timerange))
            if iend == istart:
                 iend += 60 # approx for minute files and 1 hour timedelta (used when no data available in time range) should be valid for any other time range as well
            else:
                currsequence = signal[istart:iend]
                aicarray = []
                for idx, el in enumerate(currsequence):
                    if idx > 1 and idx < len(currsequence):
                        aicval = self._aic(currsequence, idx)/timerange.seconds*3600 # *sp Normailze to sampling rate and timerange
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

    def baseline(self, absolutestream, **kwargs):
        """
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
  
        keywords:
        :type plotbaseline: bool 
        :param plotbaseline: if true plot a baselineplot 
        :type extradays: int 
        :param extradays: days to which the absolutedata is exteded prior and after start and endtime
        :type plotfilename: string 
        :param plotfilename: if plotbaseline is selected, the outputplot is send to this file
        :type fitfunc: string
        :param fitfunc: see fit
        :type fitdegree: int 
        :param fitdegree: see fit
        :type knotstep: int 
        :param knotstep: see fit

        stabilitytest (bool)
        """
        fitfunc = kwargs.get('fitfunc')
        fitdegree = kwargs.get('fitdegree')
        knotstep = kwargs.get('knotstep')
        extradays = kwargs.get('extradays')
        plotbaseline = kwargs.get('plotbaseline')
        plotfilename = kwargs.get('plotfilename')
        returnfunction = kwargs.get('returnfunction')

        if not extradays:
            extradays = 15
        if not fitfunc:
            fitfunc = 'spline'
        if not fitdegree:
            fitdegree = 5
        if not knotstep:
            knotstep = 0.05


        starttime = self[0].time
        endtime = self[-1].time

        usestepinbetween = False # for better extrapolation

        loggerstream.info(' --- Start baseline-correction at %s' % str(datetime.now()))

        # 1) test whether absolutes are in the selected absolute data stream
        if absolutestream[0].time == 0:
            raise ValueError ("Baseline: Input stream needs to contain absolute data ")

        # 2) check whether enddate is within abs time range or larger:
        if not absolutestream[0].time-1 < endtime:
            loggerstream.warning("Baseline: Last measurement prior to beginning of absolute measurements ")
            
        # 3) check time ranges of stream and absolute values:
        abst = absolutestream._get_column('time')
        if np.min(abst) > starttime:
            loggerstream.info('Baseline: First absolute value measured after beginning of stream - duplicating first abs value at beginning of time series')
            #absolutestream.add(absolutestream[0])
            #absolutestream[-1].time = starttime
            #absolutestream.sorting()
            loggerstream.info('Baseline: %d days without absolutes at the beginning of the stream' % int(np.floor(np.min(abst)-starttime)))         
        if np.max(abst) < endtime:
            loggerstream.info("Baseline: Last absolute measurement before end of stream - extrapolating baseline")
            if num2date(absolutestream[-1].time).replace(tzinfo=None) + timedelta(days=extradays) < num2date(endtime).replace(tzinfo=None):
                usestepinbetween = True
                loggerstream.warning("Baseline: Well... thats an adventurous extrapolation, but as you wish...")
            
        endtime = num2date(endtime).replace(tzinfo=None)
        # 5) check whether an abolute measurement larger then 12-31 of the same year as enddate exists
        yearenddate = datetime.strftime(endtime,'%Y')
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
            basestarttime = num2date(absolutestream[0].time).replace(tzinfo=None) - timedelta(days=extradays)
        else:
            basestarttime = baseendtime-timedelta(days=(365+2*extradays))

        msg = 'Baseline: absolute data taken from %s to %s' % (basestarttime,baseendtime)
        loggerstream.debug(msg)

        bas = absolutestream.trim(starttime=basestarttime,endtime=baseendtime)
        if usestepinbetween:
            bas = bas.extrapolate(basestarttime,endtime)
        bas = bas.extrapolate(basestarttime,baseendtime)

        col = bas._get_column('dx')
        bas = bas._put_column(col,'x')
        col = bas._get_column('dy')
        bas = bas._put_column(col,'y')
        col = bas._get_column('dz')
        bas = bas._put_column(col,'z')

        func = bas.fit(['x','y','z'],fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep)

        if plotbaseline:
            if plotfilename:
                bas.plot(['x','y','z'],padding = 5, symbollist = ['o','o','o'],function=func,plottitle='Absolute data',outfile=plotfilename)
            else:
                bas.plot(['x','y','z'],padding = 5, symbollist = ['o','o','o'],function=func,plottitle='Absolute data')

        # subtract baseline
        #self = self.func_subtract(func, order=1)
        # add baseline
        self = self.func_add(func)

        loggerstream.info(' --- Finished baseline-correction at %s' % str(datetime.now()))

        if returnfunction:
            return self, func
        else:
            return self

    
    def date_offset(self, offset, **kwargs):
        """
        Corrects the time column of the selected stream by the offst
        offset is a timedelta object (e.g. timedelta(hours=1))
        """

        header = self.header
        newstream = DataStream()

        for elem in self:
            newtime = num2date(elem.time).replace(tzinfo=None) + offset
            elem.sectime = elem.time
            elem.time = date2num(newtime)
            newstream.add(elem)
        
        loggerstream.info('date_offset: Corrected time column by %s sec' % str(offset.seconds))

        return DataStream(newstream,header)


    def delta_f(self, **kwargs):
        """
        Calculates the difference of x+y+z to f
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
        for elem in self:
            elem.df = round(np.sqrt(elem.x**2+elem.y**2+elem.z**2),digits) - (elem.f + offset)

        self.header['unit-col-df'] = 'nT'
        
        loggerstream.info('--- Calculating delta f finished at %s ' % str(datetime.now()))

        return self


    def differentiate(self, **kwargs):
        """
        Method to differentiate all columns with respect to time.
        -- Using successive gradients

        optional:
        keys: (list - default ['x','y','z','f'] provide limited key-list
        put2key
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

        t = self._get_column('time')
        for i, key in enumerate(keys):
            val = self._get_column(key)
            dval = np.gradient(np.asarray(val))
            # gradient is shifted towards +x (compensate that by a not really good trick)
            dvaltmp = dval[1:]
            ndval = np.append(dvaltmp,float('NaN'))
            
            self._put_column(ndval, put2keys[i])

        loggerstream.info('--- derivative obtained at %s ' % str(datetime.now()))
        return self

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
        read stream and extract data of which key meets the criteria
        example:
        compare is string like ">, <, ==, !="
        st.extract('x',20000,'>')
        st.extract('str1','Berger')
        """

        if not compare:
            compare = '=='
        if not compare in [">=", "<=",">", "<", "==", "!="]:
            loggerstream.info('--- Extract: Please provide proper compare parameter ">=", "<=",">", "<", "==" or "!=" ')
            return self

        if not self._is_number(value):
            too = '"' + str(value) + '"'
        else:
            too = str(value)
        liste = [elem for elem in self if eval('elem.'+key+' '+ compare + ' ' + too)]

        return DataStream(liste,self.header)    


    def extrapolate(self, start, end):
        """
        read absolute stream and extrapolate the data
        currently:
        repeat the last and first input with baseline values at start and end
        """
        firstelem = self[0]
        lastelem = self[-1]
        # Find the last element with baseline values
        i = 1
        while isnan(lastelem.dx):
            lastelem = self[-i]
            i = i +1
        ltime = date2num(end + timedelta(days=1))
        ftime = date2num(start - timedelta(days=1))
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
        self = self.sorting()

        return self


    def filter(self, **kwargs):
        """
    DEFINITION:
        Code for simple application, filtering function.
	Returns stream with filtered data with sampling period of
	filter_width.

    PARAMETERS:
    Variables:
        - variable: 	(type) Description.
    Kwargs:
        - filter_type: 	(str) Options: gaussian, linear or special. Default = gaussian.
        - filter_width: (timedelta object) Default = timedelta(minutes=1)
        - filter_offset: 	(timedelta object) Default=0
        - gauss_win: 	(int) Default = 1.86506 (corresponds to +/-45 sec in case of min or 45 min in case of hour).
        - fmi_initial_data: 	(DataStream containing dH values (dx)) Default=[].

    RETURNS:
        - stream: 	(DataStream object) Stream containing filtered data.

    EXAMPLE:
        >>> stream_filtered = stream.filter(filter_width=timedelta(minutes=3))

    APPLICATION:

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
                    exec('col'+el+' = self._maskNAN(col'+el+')')
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
        self.header['DataSamplingRate'] = str(filter_width.seconds) + ' sec'
        self.header['DataSamplingFilter'] = filter_type + ' - ' + str(trange.seconds) + ' sec'
        #self.header['DataInterval'] = str(filter_width.seconds)+' sec'
        
        loggerstream.info('filter: Finished filtering.')

        return DataStream(resdata,self.header)  

        
    def fit(self, keys, **kwargs):
        """
        fitting data:
        NaN values are interpolated
        kwargs support the following keywords:
            - fitfunc   (string: 'poly', 'harmonic', 'spline') default='spline'
            - timerange (timedelta obsject) default=timedelta(hours=1)
            - fitdegree (float)  default=5
            - knotstep (float < 0.5) determines the amount of knots: amount = 1/knotstep ---> VERY smooth 0.1 | NOT VERY SMOOTH 0.001
            - flag 
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
        
        for key in keys:
            tmpst = self._drop_nans(key)
            t = tmpst._get_column('time')
            nt,sv,ev = self._normalize(t)
            sp = self.get_sampling_period()
            if sp == 0:  ## if no dominant sampling period can be identified then use minutes
                sp = 0.0177083333256
            if not key in KEYLIST[1:16]:
                raise ValueError, "Column key not valid"
            val = tmpst._get_column(key)
            # interplolate NaN values
            #nans, xxx= self._nan_helper(val)
            #val[nans]= np.interp(xxx(nans), xxx(~nans), val[~nans])
            #print np.min(nt), np.max(nt), sp, len(self)
            x = arange(np.min(nt),np.max(nt),sp)
            if len(val)>1 and fitfunc == 'spline':
                try:
                    knots = np.array(arange(np.min(nt)+knotstep,np.max(nt)-knotstep,knotstep))
                    if len(knots) > len(val):
                        knotstep = knotstep*4
                        knots = np.array(arange(np.min(nt)+knotstep,np.max(nt)-knotstep,knotstep))
                        loggerstream.warning('Too many knots in spline for available data. Please check amount of fitted data in time range. Trying to reduce resolution ...')
                    ti = interpolate.splrep(nt, val, k=3, s=0, t=knots)
                except:
                    loggerstream.error('Value error in fit function - likely reason: no valid numbers')
                    raise ValueError, "Value error in fit function"
                    return
                f_fit = interpolate.splev(x,ti)
            elif len(val)>1 and fitfunc == 'poly':
                loggerstream.debug('Selected polynomial fit - amount of data: %d, time steps: %d, degree of fit: %d' % (len(nt), len(val), fitdegree))
                ti = polyfit(nt, val, fitdegree)
                f_fit = polyval(ti,x)
            elif len(val)>1 and fitfunc == 'harmonic':
                loggerstream.debug('Selected harmonic fit - not yet implemented')
                ti = polyfit(nt, val, fitdegree)
                f_fit = polyval(ti,x)
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


    def flag_stream(self, key, flag, comment, startdate, enddate=None):
        """
        Add flags to specific times or time ranges if enddate is provided:
        Requires the following input:
        key: (char) the column (f, x,y,z)
        flag: (int) 0 ok, 1 remove, 2 force ok, 3 force remove, 4 merged from other instrument
        comment: (string) the reason
        startdate: the date of the (first) datapoint to remove
        optional:
        enddate: the enddate of a time range to be flagged in a identical way
        """
        
        if not key in KEYLIST:
            raise ValueError, "Wrong Key"
        if not flag in [0,1,2,3,4]:
            raise ValueError, "Wrong Flag"            

        startdate = self._testtime(startdate)

        if not enddate:
            enddate = startdate
        else:
            enddate = self._testtime(enddate)

        poslst = [i for i,el in enumerate(FLAGKEYLIST) if el == key]
        pos = poslst[0]

        for elem in self:
            if elem.time >= date2num(startdate) and elem.time <= date2num(enddate):
                fllist = list(elem.flag)
                fllist[pos] = str(flag)
                elem.flag=''.join(fllist)
                elem.comment = comment
        if flag == 1 or flag == 3:
            if enddate:
                loggerstream.info("Removed data from %s to %s ->  (%s)" % (startdate.isoformat(),enddate.isoformat(),comment))
            else:
                loggerstream.info("Removed data at %s -> (%s)" % (startdate.isoformat(),comment))
        return self
            
        
    def func_add(self,function,**kwargs):
        """
        Add a function to the selected values of the data stream -> e.g. get baseline
        Optional:
        keys (default = 'x','y','z')        
        """
        keys = kwargs.get('keys')
        if not keys:
            keys = ['x','y','z']

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

        if not order:
            order = 0
        
        if not keys:
            keys = ['x','y','z']

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

        return self


    def get_gaps(self, **kwargs):
        """
        Takes the dominant sample frequency and fills nan into non-existing time steps:
        This function provides the basis for discontinuous plots and gap analysis
        Supports to possible keywords:
        accuracy: float - (time relative to a day) - default 1 sec
        gapvariable: boolean  - writes 1 to var2 if time step missing, else 0 - default False        """
        accuracy = kwargs.get('accuracy')
        key = kwargs.get('key')

        if key in KEYLIST:
            gapvariable = True
            
        if not accuracy:
            accuracy = 1.0/(3600.0*24.0) # one second relative to day

        sp = self.get_sampling_period()
        
        loggerstream.info('--- Starting filling gaps with NANs at %s ' % (str(datetime.now())))

        header = self.header
        stream = DataStream()
        newline = LineStruct()
        prevtime = 0
        maxtime = self[-1].time
        for elem in self:
            if abs((prevtime+sp) - elem.time) > accuracy and prevtime+sp < elem.time and not prevtime == 0:
                currtime = prevtime+sp
                print currtime, abs((prevtime+sp) - elem.time)*24*3600, (elem.time-prevtime)*24*3600, sp*24*3600
                while currtime < elem.time:
                    if gapvariable:
                        newline.var3 = 1.0
                    newline.time = currtime
                    #print newline
                    stream.add(newline)
                    currtime += sp
            else:
                elem.var3 = 0.0
                if key in KEYLIST:
                    if isnan(eval('elem.'+key)):
                        elem.var3 = 1.0
                #print elem.time, elem.var2
                stream.add(elem)
            prevtime = elem.time

        loggerstream.info('--- Filling gaps finished at %s ' % (str(datetime.now())))
                
        return DataStream(stream,header)


    def get_sampling_period(self):
        """
        returns the dominant sampling frequency in unit ! days !
        
        for time savings, this function only tests the first 1000 elements
        """
        timedifflist = [[0,0]]
        domtd = [0,0]
        timecol= self._get_column('time')
        if len(timecol) <= 1000:
            testrange = len(timecol)
        else:
            testrange = 1000

        #print len(timecol)

        for idx, val in enumerate(timecol[:testrange]):
            if idx > 1 and not isnan(val):
                timediff = val - timeprev
                found = 0
                for tel in timedifflist:
                    if tel[1] == timediff:
                        tel[0] = tel[0]+1
                        found = 1
                if found == 0:
                    timedifflist.append([1,timediff])
            timeprev = val

        #print self

        # get the most often timediff
        dominate = 0
        for elem in timedifflist:
            if elem[0] > dominate:
                dominate = elem[0]
                domtd = elem

        return domtd[1]


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
            keys = ['x','y','z','f']

        t = self._get_column('time')
        for key in keys:
            val = self._get_column(key)
            dval = sp.integrate.cumtrapz(np.asarray(val),t)
            dval = np.insert(dval, 0, 0) # Prepend 0 to maintain original length
            self._put_column(dval, 'd'+key)

        loggerstream.info('--- integration finished at %s ' % str(datetime.now()))
        return self


    def interpol(self, keys, **kwargs):
        """
    DEFINITION:
        Uses Numpy interpolate.interp1d to interpolate streams.

    PARAMETERS:
    Variables:
        - keys: 	(list) List of keys to interpolate.
    Kwargs:
        - kind:		(str) type of interpolation. Options:
			linear = linear - Default
			slinear = spline (first order)
			quadratic = spline (second order)
			cubic = spline (third order)
			nearest = ?
			zero = ?
	(TODO: add these?)
        - timerange: 	(timedelta object) default=timedelta(hours=1).
        - fitdegree: 	(float) default=4.
        - knotstep: 	(float < 0.5) determines the amount of knots: 
			amount = 1/knotstep ---> VERY smooth 0.1 | NOT VERY SMOOTH 0.001

    RETURNS:
        - func: 	(list) Contains the following:
			list[0]:	(dict) {'f+key': interpolate function}
			list[1]:	(float) date2num value of minimum timestamp
			list[2]:	(float) date2num value of maximum timestamp

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

        t = self._get_column('time')
        nt,sv,ev = self._normalize(t)
        sp = self.get_sampling_period()
        functionkeylist = {}
        
        loggerstream.info("interpol: Interpolating stream with %s interpolation." % kind)

        for key in keys:
            if not key in KEYLIST[1:16]:
                loggerstream.error("interpol: Column key not valid!")
            val = self._get_column(key)
            # interplolate NaN values
            nans, xxx= self._nan_helper(val)
            try: # Try to interpolate nan values
                val[nans]= np.interp(xxx(nans), xxx(~nans), val[~nans])
            except:
                #val[nans]=int(nan)
                pass
            if len(val)>1:
                exec('f'+key+' = interpolate.interp1d(nt, val, kind)')
                exec('functionkeylist["f'+key+'"] = f'+key)
            else:
                pass

        loggerstream.info("interpol: Interpolation complete.")
            
        func = [functionkeylist, sv, ev]

        return func
        
    def k_fmi(self, **kwargs):
        """
        Calculating k values following the fmi approach:
        kwargs support the following keywords:
            - timerange (timedelta obsject) default=timedelta(hours=1)
            - fitdegree (float)  default=4
            - knotstep (float < 0.5) determines the amount of knots: amount = 1/knotstep ---> VERY smooth 0.1 | NOT VERY SMOOTH 0.001
            - flag 
        """
        fitfunc = kwargs.get('fitfunc')
        put2key = kwargs.get('put2key')
        fitdegree = kwargs.get('fitdegree')
        knotstep=kwargs.get('knotstep')
        m_fmi = kwargs.get('m_fmi')
        if not fitfunc:
            fitfunc = 'poly'
        if not fitdegree:
            fitdegree = 5
        if not m_fmi:
            m_fmi = 0
        if not put2key:
            put2key = 't2'
        
        stream = DataStream()
        # extract daily streams/24h slices from input
        iprev = 0
        iend = 0

        loggerstream.info('--- Starting k value calculation: %s ' % (str(datetime.now())))

        # Start with the full input stream
        # eventually convert xyz to hdz first
        for elem in self:
            gettyp = elem.typ
            break
        if gettyp == 'xyzf':
            fmistream = self._convertstream('xyz2hdz',keep_header=True)
        elif gettyp == 'idff':
            fmistream = self._convertstream('idf2xyz',keep_header=True)
            fmistream = self._convertstream('xyz2hdz',keep_header=True)
        elif gettyp == 'hdzf':
            fmistream = self
            pass
        else:
            loggerstream.error('Unkown typ (xyz?) in FMI function')
            return

        sr = fmistream.get_sampling_period()
        #print "Sampling rate of stream (sec) = %.1f" % (sr*24*3600)
        samprate = int(float("%.1f" % (sr*24*3600)))
        if not samprate in [1,60]:
            loggerstream.error('K-FMI: please check the sampling rate of the input file - should be second or minute data - current sampling rate is %d seconds' % samprate)
            return
        if samprate == 1:
            fmistream = fmistream.filtered(filter_type='gauss',filter_width=timedelta(seconds=45))
            fmi1stream = fmistream.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
        if samprate == 60:
            fmi1stream = fmistream.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
         
        fmi2stream = fmistream.filtered(filter_type='fmi',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30),fmi_initial_data=fmi1stream,m_fmi=m_fmi)

        loggerstream.info('--- -- k value: finished initial filtering at %s ' % (str(datetime.now())))

        t = fmi2stream._get_column('time')

        while iend < len(t)-1:
            istart = iprev
            ta, iend = self._find_nearest(np.asarray(t), date2num(num2date(t[istart]).replace(tzinfo=None) + timedelta(hours=24)))
            currsequence = fmi2stream[istart:iend+1]
            fmitmpstream = DataStream()
            for el in currsequence:
                fmitmpstream.add(el)
            func = fmitmpstream.fit(['x','y','z'],fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep)
            #fmitmpstream.plot(['x'],function=func)
            fmitmpstream = fmitmpstream.func_subtract(func)
            stream.extend(fmitmpstream,self.header)
            iprev = iend

        fmi3stream = stream.filtered(filter_type='linear',filter_width=timedelta(minutes=180),filter_offset=timedelta(minutes=90))
        fmi4stream = fmi3stream._get_k(put2key=put2key)

        self.header['col-'+put2key] = 'k'

        outstream = mergeStreams(self,fmi4stream,keys=[put2key])

        loggerstream.info('--- finished k value calculation: %s ' % (str(datetime.now())))
        
        return DataStream(outstream, self.header)

    def mean(self, key, **kwargs):
        """
    DEFINITION:
        Calculates mean values for the specified key, Nan's are regarded for.
        Means are only calculated if more then "amount" in percent are non-nan's
        Returns a float if successful or NaN.

    PARAMETERS:
    Variables:
        - key:   	(KEYLIST) element of Keylist like 'x' .
    Kwargs:
        - percentage:   (int) Define required percentage of non-nan values, if not 
                               met that nan will be returned. Default is 95 (%)
        - meanfunction: (string) accepts 'mean' and 'median'. Default is 'mean' 
        - std: 		(bool) if true, the standard deviation is returned as well 
        
    RETURNS:
        - mean/median(, std) (float) 
    EXAMPLE:
        >>> meanx = datastream.mean('x',meanfunction='median',percentage=90)

    APPLICATION:
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

        if not isinstance( percentage, (int,long)):
            loggerstream.error("mean: Percentage needs to be an integer!")
        if not key in KEYLIST[:16]:
            loggerstream.error("mean: Column key not valid!")

        ar = [eval('elem.'+key) for elem in self if not isnan(eval('elem.'+key))]
        div = float(len(ar))/float(len(self))*100.0

        if div >= percentage:
            if std:
                return eval('np.'+meanfunction+'(ar)'), np.std(ar)
            else:
                return eval('np.'+meanfunction+'(ar)')
        else:
            loggerstream.warning('mean: Too many nans in column, exceeding %d percent' % percentage)
            return float("NaN")


    def obspyspectrogram(self, data, samp_rate, per_lap=0.9, wlen=None, log=False, 
                    outfile=None, fmt=None, axes=None, dbscale=False, 
                    mult=8.0, cmap=None, zorder=None, title=None, show=True, 
                    sphinx=False, clip=[0.0, 1.0]): 

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
        nfft = int(self._nearestPow2(wlen * samp_rate))

        if nfft > npts: 
            nfft = int(self._nearestPow2(npts / 8.0)) 

        if mult != None: 
            mult = int(self._nearestPow2(mult)) 
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
        Offset treatment: offsets argument is a dictionary
        Apply constant offsets to elements of the datastream
        the Offsets arguments is a dictionary which refers to keys fo the datastream
        Important: Time offsets have to be timedelta objects
        : type offsets: dict
        : param offsets: looks like {'time': timedelta(hours=1), 'x': 4.2, 'f': -1.34242}
        # 1.) assert that offsets is a dictionary {'x': 4.2, 'y':... }
        # 2.) apply offsets
        kwargs:
        starttime, endtime 
        """

        for key in offsets:
            if key in KEYLIST:
                val = self._get_column(key)
                if key == 'time':
                    newval = [num2date(elem.time).replace(tzinfo=None) + offsets[key] for elem in val]
                    loggerstream.info('offset: Corrected time column by %s sec' % str(offset.seconds))
                else:
                    newval = [elem + offsets[key] for elem in val]
                    loggerstream.info('offset: Corrected column %s by %.3f' % (key, offsets[key]))
                self = self._put_column(newval, key)
            else:
                loggerstream.warning("offset: Key '%s' not in keylist." % key)
    
        return self
                            

    def plot(self, keys, debugmode=None, **kwargs):
        """
    DEFINITION:
        Code for simple application.
        Creates a simple graph of the current stream. 
        In order to run matplotlib from cron one needs to include (matplotlib.use('Agg')).

    PARAMETERS:
    Variables:
        - keys: 	(list) A list of the keys (str) to be plotted.
    Kwargs:
        - annote: 	(bool) Annotate data using comments
	- annoxy:	(dictionary) Define placement of annotation (in % of scale).
			Possible parameters:
			(called for annotated storm phases:) 
			sscx, sscy, mphx, mphy, recx, recy
	- annophases:	(bool) Annotate phase times with titles. Default False.
	- bgcolor:	(string) Define background color e.g. '0.5' greyscale, 'r' red, etc
        - colorlist: 	(list - default []) Provide a ordered color list of type ['b','g']
	- confinex:	(bool) Confines tags on x-axis to shorter values.
        - errorbar: 	(boolean - default False) plot dx,dy,dz,df values if True
        - function: 	(func) [0] is a dictionary containing keys (e.g. fx), [1] the startvalue, [2] the endvalue
			Plot the content of function within the plot.
        - fullday: 	(boolean) - default False. Rounds first and last day two 0:00 respectively 24:00 if True
        - fmt: 		(string?) format of outfile 
	- grid:		(bool) show grid or not, default = True 
	- gridcolor:	(string) Define grid color e.g. '0.5' greyscale, 'r' red, etc
	- labelcolor:	(string) Define grid color e.g. '0.5' greyscale, 'r' red, etc 
        - noshow: 	(bool) don't call show at the end, just returns figure handle
        - outfile: 	string to save the figure, if path is not existing it will be created
        - padding: 	(integer - default 0) Value to add to the max-min data for adjusting y-scales
        - savedpi: 	(integer) resolution
        - savefigure: 	(string - default None) if provided a copy of the plot is saved to savefilename.png
	- stormphases:	(list) Should be a list with four datetime objects:
			[0 = date of SSC/start of initial phase,
			1 = start of main phase,
			2 = start of recovery phase,
			3 = end of recovery phase]
	- plotphases:	(list) List of keys of plots to shade.
	- specialdict:	(dictionary) contains special information for specific plots.
			key corresponds to the column
			input is a list with the following parameters
			('None' if not used)
			ymin
			ymax
			ycolor
			bgcolor
			grid
			gridcolor
        - symbollist: 	(string - default '-') symbol for primary plot
        - symbol_func: 	(string - default '-') symbol of function plot 

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
        fmt = kwargs.get('fmt')
        function = kwargs.get('function')
        fullday = kwargs.get('fullday')
        grid = kwargs.get('grid')
        gridcolor = kwargs.get('gridcolor')
        labelcolor = kwargs.get('labelcolor')
        padding = kwargs.get('padding')
        plottitle = kwargs.get('plottitle')
        plottype = kwargs.get('plottype')
        noshow = kwargs.get('noshow')
        outfile = kwargs.get('outfile')
        savedpi = kwargs.get('savedpi')
        stormphases = kwargs.get('stormphases')
        plotphases = kwargs.get('plotphases')
        specialdict = kwargs.get('specialdict')
        symbol_func = kwargs.get('symbol_func')
        symbollist = kwargs.get('symbollist')
        figure = kwargs.get('figure')

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

        myyfmt = ScalarFormatter(useOffset=False)
        n_subplots = len(keys)

        if n_subplots < 1:
            loggerstream.error("plot: Number of keys not valid.")
        count = 0

        if not figure:
            fig = plt.figure()
        else:
            fig = figure

        #fig = matplotlib.figure.Figure()

        loggerstream.info("plot: Start plotting.")

        t = np.asarray([row[0] for row in self])
        for key in keys:
            if not key in KEYLIST[1:16]:
                loggerstream.error("plot: Column key not valid!")
            ind = KEYLIST.index(key)
            yplt = np.asarray([row[ind] for row in self])
            #yplt = self._get_column(key)

            # Switch between continuous and discontinuous plots
            if debugmode:
                print "column extracted at %s" % datetime.utcnow()
            if plottype == 'discontinuous':
                yplt = self._maskNAN(yplt)
            else: 
                nans, test = self._nan_helper(yplt)
                newt = [t[idx] for idx, el in enumerate(yplt) if not nans[idx]]
                t = newt
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
                    yerr = self._get_column('d'+key)
                    if len(yerr) > 0: 
                        ax.errorbar(t,yplt,yerr=varlist[ax+4],fmt=colorlist[count]+'o')
                    else:
                        loggerstream.warning('plot: Errorbars (d%s) not found for key %s' % (key, key))

		# -- Add grid:
                if grid:
                    ax.grid(True,color=gridcolor,linewidth=0.5)

                # -- Add annotations for flagged data:
                if annotate:
                    flag = self._get_column('flag')
                    comm = self._get_column('comment')
                    elemprev = "-"
                    try: # only do all that if column is in range of flagged elements (e.g. x,y,z,f)
                        poslst = [i for i,el in enumerate(FLAGKEYLIST) if el == key]
                        indexflag = int(poslst[0])
                        for idx, elem in enumerate(comm):
                            if not elem == elemprev:
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
                            tssc_anno, issc_anno = self._find_nearest(np.asarray(t), date2num(t_ssc))
                            yt_ssc = yplt[issc_anno]
		            if 'sscx' in annoxy:	# parameters for SSC annotation.
                                x_ssc = annoxy['sscx']
                            else:
                                x_ssc = t_ssc-timedelta(hours=2)
		            if 'sscy' in annoxy:
                                y_ssc = ymin + annoxy['sscy']*(ymax-ymin)
                            else:
                                y_ssc = y_anno
		            if 'mphx' in annoxy:	# parameters for main-phase annotation.
                                x_mph = annoxy['mphx']
                            else:
                                x_mph = t_mphase+timedelta(hours=1.5)
		            if 'mphy' in annoxy:
                                y_mph = ymin + annoxy['mphy']*(ymax-ymin)
                            else:
                                y_mph = y_anno
		            if 'recx' in annoxy:	# parameters for recovery-phase annotation.
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
                        ax.plot_date(self._denormalize(ttmp,function[1],function[2]),function[0][fkey](ttmp),'r-')
                # -- Add Y-axis ticks:
                if bool((count-1) & 1):
                    ax.yaxis.tick_right()
                    ax.yaxis.set_label_position("right")

                # -- Take Y-axis labels from header information
                try:
                    ylabel = self.header['col-'+key].upper()
                except:
                    ylabel = ''
                    pass
                try:
                    yunit = self.header['unit-col-'+key]
                except:
                    yunit = ''
                    pass
                if not yunit == '': 
                    yunit = re.sub('[#$%&~_^\{}]', '', yunit)
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


    def powerspectrum(self, key, debugmode=None, outfile=None, fmt=None, axes=None, title=None):
        """
    DEFINITION:
        Calculating the power spectrum
        following the numpy fft example

    PARAMETERS:
    Variables:
        - key:		(str) Key to analyse
    Kwargs:
	- axes:		(?) ?
        - debugmode: 	(bool) Variable to show steps
	- fmt:		(str) Format of outfile, e.g. "png"
	- outfile:	(str) Filename to save plot to
	- title:	(str) Title to display on plot

    RETURNS:
        - plot: 	(matplotlib plot) A plot of the powerspectrum

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
			title='Power
			outfile='ps.png')
        """
        if debugmode:
            print "Start powerspectrum at %s" % datetime.utcnow()

        dt = self.get_sampling_period()*24*3600

        t = np.asarray(self._get_column('time'))
        val = np.asarray(self._get_column(key))
        tnew, valnew = [],[]
        for idx, elem in enumerate(val):
            if not isnan(elem):
                tnew.append((t[idx]-np.min(t))*24*3600)
                valnew.append(elem)

        tnew = np.asarray(tnew)
        valnew = np.asarray(valnew)

        if debugmode:
            print "Extracted data for powerspectrum at %s" % datetime.utcnow()

        freq = np.fft.fftfreq(tnew.shape[-1],dt)
        freq = freq[range(len(tnew)/2)] # one side frequency range
        freq = freq[1:]

        s = np.fft.fft(valnew)
        s = s[range(len(valnew)/2)] # one side data range
        s = s[1:]
        ps = np.real(s*np.conjugate(s))

        if not axes: 
            fig = plt.figure() 
            ax = fig.add_subplot(111) 
        else: 
            ax = axes 

        ax.loglog(freq,ps,'r-')

        ax.set_xlabel('Frequency [Hz]') 
        ax.set_ylabel('PSD') 
        if title: 
            ax.set_title(title)

        if debugmode:
            print "Finished powerspectrum at %s" % datetime.utcnow()

        if outfile: 
            if fmt: 
                fig.savefig(outfile, format=fmt) 
            else: 
                fig.savefig(outfile) 
        elif show: 
            plt.show() 
        else: 
            return fig

        return freq, ps
    

    def remove_flagged(self, **kwargs):
        """
        remove flagged data from stream:
        kwargs support the following keywords:
            - flaglist  (list) default=[1,3]
            - keys (string list e.g. 'f') default=FLAGKEYLIST
        flag = '000' or '010' etc
        """
        
        # Defaults:
        flaglist = kwargs.get('flaglist')
        keys = kwargs.get('keys')

        if not flaglist:
            flaglist = [1,3]
        if not keys:
            keys = FLAGKEYLIST

        for key in keys:
            poslst = [i for i,el in enumerate(FLAGKEYLIST) if el == key]
            pos = poslst[0]
            liste = []
            emptyelem = LineStruct()
            for elem in self:
                fllst = list(elem.flag)
                try: # test whether useful flag is present: flaglst length changed during the program development
                    flag = int(fllst[pos])
                except:
                    flag = 0
                if not flag in flaglist:
                    liste.append(elem)
                else:
                    exec('elem.'+key+' = float("nan")')
                    liste.append(elem)

        #liste = [elem for elem in self if not elem.flag[pos] in flaglist]
        return DataStream(liste, self.header)      


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
	- keys:		(list) List of keys to evaluate. Default=['f']
        - threshold: 	(float) Determines threshold for outliers.
			1.5 = standard
			5 = keeps storm onsets in
			4 = Default as comprimise.
        - timerange: 	(timedelta Object) Time range. Default = timedelta(hours=1)

    RETURNS:
        - stream: 	(DataStream Object) Stream with flagged data.

    EXAMPLE:
        >>> stream.remove_outlier(keys=['x','y','z'], threshold=2)

    APPLICATION:
        """
        # Defaults:
        timerange = kwargs.get('timerange')
        threshold = kwargs.get('threshold')
        keys = kwargs.get('keys')
        if not timerange:
            timerange = timedelta(hours=1)
        if not keys:
            keys = ['f']
        if not threshold:
            threshold = 4.0
        # Position of flag in flagstring
        # f (intensity): pos 0
        # x,y,z (vector): pos 1
        # other (vector): pos 2
        
        loggerstream.info('remove_outlier: Starting outlier removal.')

        if len(self) < 1:
            loggerstream.warning('remove_outlier: No data - Stopping outlier removal.')
            return self
        
        # Start here with for key in keys:
        for key in keys:
            poslst = [i for i,el in enumerate(FLAGKEYLIST) if el == key]
            flagpos = poslst[0]

            st = self._get_min('time')
            et = self._get_max('time')
            at = date2num((num2date(st).replace(tzinfo=None)) + timerange)
            incrt = at-st
            

            arraytime = self._get_column('time')

            newst = DataStream()
            while st < et:
                tmpar, idxst = self._find_nearest(arraytime,st)
                tmpar, idxat = self._find_nearest(arraytime,at)
                if idxat == len(arraytime)-1:
                    idxat = len(arraytime)
                st = at
                at += incrt

                lstpart = self[idxst:idxat]
                selcol = [eval('row.'+key) for row in lstpart]
                
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

                for elem in lstpart:
                    row = LineStruct()
                    row = elem
                    if not md-whisker < eval('elem.'+key) < md+whisker:
                        fllist = list(row.flag)
                        fllist[flagpos] = '1'
                        row.flag=''.join(fllist)
                        row.comment = "%s removed by automatic outlier removal" % key
                        if not isnan(eval('elem.'+key)):
                            loggerstream.info("remove_outlier: removed %s (= %f)" % (key, eval('elem.'+key)))
                    else:
                        fllist = list(row.flag)
                        fllist[flagpos] = '0'
                        row.flag=''.join(fllist)
                    newst.add(row)

        loggerstream.info('remove_outlier: Outlier removal finished.')

        return DataStream(newst, self.header)        


    def resample(self, keys, **kwargs):
        """
    DEFINITION:
        Uses Numpy interpolate.interp1d to resample stream to requested period.

    PARAMETERS:
    Variables:
        - keys: 	(list) keys to be resampled.
    Kwargs:
        - period: 	(float) sampling period in seconds, e.g. 5s (0.2 Hz).

    RETURNS:
        - stream: 	(DataStream object) Stream containing resampled data.

    EXAMPLE:
        >>> resampled_stream = pos_data.resample(['f'],1)

    APPLICATION: 
        """

        period = kwargs.get('period')

        if not period:
            period = 60.

	sp = self.get_sampling_period()*24.*60.*60.

	loggerstream.info("resample: Resampling stream of sampling period %s to period %s." % (sp,period))

	t_min = self._get_min('time')
	t_max = self._get_max('time')

	t_list = []
        time = num2date(t_min)
        while time <= num2date(t_max):
           t_list.append(date2num(time))
           time = time + timedelta(seconds=period)

	res_stream = DataStream()
        for item in t_list:
            row = LineStruct()
            row.time = item
            res_stream.add(row)

        for key in keys:
            if key not in KEYLIST[1:16]:
                loggerstream.warning("resample: Key %s not supported!" % key)
            try:
                int_data = self.interpol([key],kind='linear')#'cubic')
            except:
                loggerstream.error("resample: Error interpolating stream. Stream too large?")

            int_func = int_data[0]['f'+key]
            int_min = int_data[1]
            int_max = int_data[2]

	    key_list = []
            for item in t_list:
                functime = (item - int_min)/(int_max - int_min)
                tempval = int_func(functime)
                key_list.append(tempval)

            res_stream._put_column(key_list,key)

        loggerstream.info("resample: Data resampling complete.")
	return DataStream(res_stream,self.headers)


    def rotation(self,**kwargs):
        """
        Rotation matrix for ratating x,y,z to new coordinate system xs,ys,zs using angles alpha and beta
        alpha is the horizontal rotation in degree,
        beta the vertical
        """
        
        
        unit = kwargs.get('unit')
        alpha = kwargs.get('alpha')
        beta = kwargs.get('beta')
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

        loggerstream.info('--- Applying rotation matrix: %s ' % (str(datetime.now())))

        for elem in self:
            ra = np.pi*alpha/(180.*ang_fac)
            rb = np.pi*beta/(180.*ang_fac)
            xs = elem.x*np.cos(rb)*np.cos(ra)-elem.y*np.sin(ra)+elem.z*np.sin(rb)*np.cos(ra)
            ys = elem.x*np.cos(rb)*np.sin(ra)+elem.y*np.cos(ra)+elem.z*np.sin(rb)*np.sin(ra)
            zs = elem.x*np.sin(rb)+elem.z*np.cos(rb)
            elem.x = xs
            elem.y = ys
            elem.z = zs

        loggerstream.info('--- finished reorientation: %s ' % (str(datetime.now())))

        return self


    def smooth(self, keys, **kwargs):
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
        - keys: 	(list) List of keys to smooth 
    Kwargs:
        - window_len: 	(int,odd) dimension of the smoothing window
        - window: 	(str) the type of window from 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'. A flat window will produce a moving average smoothing.
        (See also: 
        numpy.hanning, numpy.hamming, numpy.bartlett, numpy.blackman, numpy.convolve
        scipy.signal.lfilter)

    RETURNS:
        - self: 	(DataStream) The smoothed signal

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

        
        loggerstream.info('smooth: Start smoothing (%s window, width %d) at %s' % (window, window_len, str(datetime.now())))

        for key in keys:
            if not key in KEYLIST:
                loggerstream.error("Column key %s not valid." % key)

            x = self._get_column(key)

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

            self._put_column(y[(int(window_len/2)):(len(x)+int(window_len/2))],key)

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

        for key in keys:
            val = self._get_column(key)
            val = self._maskNAN(val)
            dt = self.get_sampling_period()*(samp_rate_multiplicator)
            Fs = float(1.0/dt)
            self.obspyspectrogram(val,Fs, per_lap=per_lap, wlen=wlen, log=log, 
                    outfile=outfile, fmt=fmt, axes=axes, dbscale=dbscale, 
                    mult=mult, cmap=cmap, zorder=zorder, title=title, show=show, 
                    sphinx=sphinx, clip=clip)


    def stereoplot(self, **kwargs):
        """
            DEFINITION:
                plots a dec and inc values in stereographic projection
                will abort if no idff typ is provided
                full circles denote positive inclinations, open negative

            PARAMETERS:
            variable:
                - stream		(DataStream) a magpy datastream object
            kwargs:
                - focus:	        (string) defines the plot area - can be either:
                                            all - -90 to 90 deg inc, 360 deg dec (default)
                                            q1 - first quadrant
                                            q2 - first quadrant
                                            q3 - first quadrant
                                            q4 - first quadrant
                                            data - focus on data (if angular spread is less then 10 deg
                - groups		(KEY) - key of keylist which defines color of points
                                             (e.g. ('str2') in absolutes to select
                                             different colors for different instruments 
                - legend		(bool) - draws legend only if groups is given - default True
                - legendposition    (string) - draws the legend at chosen position (e.g. "upper right", "lower center") - default is "lower left" 
                - labellimit        (integer)- maximum length of label in legend
                - noshow: 	        (bool) don't call show at the end, just returns figure handle
                - outfile: 	        (string) to save the figure, if path is not existing it will be created
                - gridcolor:	(string) Define grid color e.g. '0.5' greyscale, 'r' red, etc
                - savedpi: 	        (integer) resolution
                - figure: 	        (bool) True for GUI
                
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


    def trim(self, starttime=None, endtime=None):
        """
        Removing dates outside of range between start- and endtime
        """
        # include test - does not work yet
        #if date2num(self._testtime(starttime)) > date2num(self._testtime(endtime)):
        #    raise ValueError, "Starttime is larger then Endtime"
        # remove data prior to starttime input
        loggerstream.debug('Trim: Started from %s to %s' % (starttime,endtime))

        stream = DataStream()

        if starttime:
            # check starttime input
            starttime = self._testtime(starttime)
            stval = 0
            """
            if isinstance(starttime, float) or isinstance(starttime, int):
                try:
                    starttime = num2date(starttime)
                except:
                    raise TypeError
            elif isinstance(starttime, str):
                try:
                    starttime = datetime.strptime(starttime,"%Y-%m-%d")
                except:
                    raise TypeError
            elif not isinstance(starttime, datetime):
                raise TypeError
            """
            for idx, elem in enumerate(self):
                if not isnan(elem.time):
                    if num2date(elem.time).replace(tzinfo=None) > starttime:
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
            """
            if isinstance(endtime, float) or isinstance(endtime, int):
                try:
                    endtime = num2date(endtime).replace(tzinfo=None)
                except:
                    raise TypeError
            elif isinstance(endtime, str):
                try:
                    endtime = datetime.strptime(endtime,"%Y-%m-%d")
                except:
                    raise TypeError
            elif not isinstance(endtime, datetime):
                raise TypeError
            """
            edval = len(self)
            for idx, elem in enumerate(self):
                if not isnan(elem.time):
                    if num2date(elem.time).replace(tzinfo=None) > endtime:
                        edval = idx
                        #edval = idx-1
                        break
            self.container = self.container[:edval]

        return DataStream(self.container,self.header)


    def write(self, filepath, **kwargs):
        """
    DEFINITION:
        Code for simple application: write Stream to a file.

    PARAMETERS:
    Variables:
        - filepath: 	(str) Providing path/filename for saving.
    Kwargs:
        - coverage: 	(timedelta) day files or hour or month or year or all - default day.
        - dateformat: 	(str) outformat of date in filename (e.g. "%Y-%m-%d" -> "2011_11_22".
        - filenamebegins: 	(str) providing the begin of savename (e.g. "WIK_").
        - filenameends: 	(str) providing the end of savename (e.g. ".min").
        - format_type: 	(str) Which format - default pystr.
        - keys: 	(list) Keys to write to file.
			Current supported formats: PYSTR, PYCDF, IAGA, WDC, DIDD,
				PMAG1, PMAG2, DTU1,  GDASA1, RMRCS, AUTODIF_FREAD, 
				USBLOG, CR800, LATEX
        - mode: 	(str) Mode for handling existing files/data in files.
			Options: append, overwrite, replace, skip
        [- period: 	(str) Supports hour, day, month, year, all - default day.]
	[--> Where is this?]
        - wformat: 	(str) outputformat.

    RETURNS:
        - ...		(bool) True if successful.

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
        #period = kwargs.get('period')		# TODO
        #offsets = kwargs.get('offsets')	# retired? TODO
        keys = kwargs.get('keys')
        
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
        if not filenameends:
            # Extension for cdf files is automatically attached
            if format_type == 'PYCDF':
                filenameends = ''
            else:
                filenameends = '.txt'
        if not mode:
            mode= 'overwrite'

        if len(self) < 1:
            loggerstream.warning('write: Stream is empty!')
            return
            
        # divide stream in parts according to coverage and save them
        newst = DataStream()
        if coverage == 'month':
            starttime = datetime.strptime(datetime.strftime(num2date(self[0].time).replace(tzinfo=None),'%Y-%m-%d'),'%Y-%m-%d')
            cmonth = int(datetime.strftime(starttime,'%m')) + 1
            cyear = int(datetime.strftime(starttime,'%Y'))
            if cmonth == 13:
               cmonth = 1
               cyear = cyear + 1
            monthstr = str(cyear) + '-' + str(cmonth) + '-' + '1T00:00:00'
            endtime = datetime.strptime(monthstr,'%Y-%m-%dT%H:%M:%S')
            while starttime < num2date(self[-1].time).replace(tzinfo=None):
                lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                newst = DataStream(lst,self.header)
                filename = filenamebegins + datetime.strftime(starttime,dateformat) + filenameends
                if len(lst) > 0:
                    writeFormat(newst, os.path.join(filepath,filename),format_type,mode=mode,keys=keys)
                starttime = endtime
                # get next endtime
                cmonth = int(datetime.strftime(starttime,'%m')) + 1
                cyear = int(datetime.strftime(starttime,'%Y'))
                if cmonth == 13:
                   cmonth = 1
                   cyear = cyear + 1
                monthstr = str(cyear) + '-' + str(cmonth) + '-' + '1T00:00:00'
                endtime = datetime.strptime(monthstr,'%Y-%m-%dT%H:%M:%S')
        elif not coverage == 'all':
            starttime = datetime.strptime(datetime.strftime(num2date(self[0].time).replace(tzinfo=None),'%Y-%m-%d'),'%Y-%m-%d')
            endtime = starttime + coverage
            while starttime < num2date(self[-1].time).replace(tzinfo=None):
                lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                newst = DataStream(lst,self.header)
                filename = filenamebegins + datetime.strftime(starttime,dateformat) + filenameends
                if len(lst) > 0:
                    writeFormat(newst, os.path.join(filepath,filename),format_type,mode=mode,keys=keys)
                starttime = endtime
                endtime = endtime + coverage
        else:
            filename = filenamebegins + filenameends
            writeFormat(self, os.path.join(filepath,filename),format_type,mode=mode,keys=keys)

        return True


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
            smtpserver = 'smtp.web.de'
        if not sender:
           sender = 'roman_leonhardt@web.de'
        if not destination:
            destination = ['roman.leonhardt@zamg.ac.at']
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
        """
        - at the end of flag is important to be recognized as string
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
        return eval('self.'+key)
        #return self.__getitem__(index)

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
        return repr((self.time, self.x, self.y, self.z, self.f, self.dx, self.dy, self.dz, self.df, self.t1, self.t2, self.var1, self.var2, self.var3, self.var4, self.var5, self.str1, self.str2, self.str3, self.str4, self.flag, self.comment, self.typ))


# -------------------
#  Global functions of the stream file
# -------------------

def isNumber(s):
    """
    Test whether s is a number
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


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
    smtp.starttls()
    smtp.ehlo()
    smtp.login(user, pwd)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


# ##################
# read/write functions
# ##################

def read(path_or_url=None, dataformat=None, headonly=False, **kwargs):
    """
    DEFINITION:
        The read functions trys to open the selected dats

    PARAMETERS:
    Variables:
        - dataformat: 	(str) Auto-detection.
        - path_or_url:	(str) Path to data files in form:
			a) c:\my\data\*
			b) c:\my\data\thefile.txt
			c) /home/data/*
			d) /home/data/thefile.txt
			e) ftp://server/directory/
			f) ftp://server/directory/thefile.txt
			g) http://www.thepage.at/file.tab
    Kwargs:
        - starttime: 	(str/datetime object) Description.

    RETURNS:
        - variable: 	(type) Description.

    EXAMPLE:
        >>> alldata = mergeStreams(pos_stream, lemi_stream, keys=['x','y','z'])

    APPLICATION:

    optional arguments are starttime, endtime and dateformat of file given in kwargs
    """
    messagecont = ""

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debugmode = kwargs.get('debugmode')
    disableproxy = kwargs.get('disableproxy')
    keylist = kwargs.get('keylist') # for PYBIN

    if disableproxy:
        proxy_handler = urllib2.ProxyHandler( {} )           
        opener = urllib2.build_opener(proxy_handler)
        # install this opener
        urllib2.install_opener(opener)

    # 1. No path
    if not path_or_url:
        messagecont = "File not specified"
        return [],messagecont

    # 2. Create DataStream
    st = DataStream()

    # 3. Read data
    if not isinstance(path_or_url, basestring):
        # not a string - we assume a file-like object
        pass
    elif "://" in path_or_url:
        # some URL
        # extract extension if any
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
                    st.extend(stp.container,stp.header)
                    os.remove(fh.name)
        else:            
            # ToDo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
            stp = _read(file, dataformat, headonly, **kwargs)
            st.extend(stp.container,stp.header)
            #del stp
        if len(st) == 0:
            # try to give more specific information why the stream is empty
            if has_magic(pathname) and not glob(pathname):
                loggerstream.error("read: Check file/pathname - No file matching pattern: %s" % pathname)
                loggerstream.error("read: No file matching file pattern: %s" % pathname)
            elif not has_magic(pathname) and not os.path.isfile(pathname):
                loggerstream.error("read: No such file or directory: %s" % pathname)
            # Only raise error if no starttime/endtime has been set. This
            # will return an empty stream if the user chose a time window with
            # no data in it.
            # XXX: Might cause problems if the data is faulty and the user
            # set starttime/endtime. Not sure what to do in this case.
            elif not 'starttime' in kwargs and not 'endtime' in kwargs:
                loggerstream.error("read: Cannot open file/files: %s" % pathname)

    if headonly and (starttime or endtime):
        msg = "read: Keyword headonly cannot be combined with starttime or endtime."
        loggerstream.error(msg)
    # Sort the input data regarding time
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
    Reads a single file into a ObsPy Stream object.
    """
    stream = DataStream()
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

    # set _format identifier for each trace
    #for trace in stream:
    #    trace.stats._format = format_ep.name

    return DataStream(stream, stream.header)


def mergeStreams(stream_a, stream_b, **kwargs):
    """
    DEFINITION:
        Combine the contents of two data streams.
        Basically two methods are possible:
        1. replace data from specific columns of stream_a with data from stream_b.
        - requires keys
        2. fill gaps in stream_a data with stream_b data without replacing any data.
        - extend = True

    PARAMETERS:
    Variables:
        - stream_a	(DataStream object) main stream
	- stream_b	(DataStream object) this stream is merged into stream_a
    Kwargs:
        - addall: 	(bool) Add all elements from stream_b
        - comment: 	(str) Add comment to stream_b data in stream_a.
        - extend:	(bool) Time range of stream b is eventually added to stream a. 
			Default False.
			If extend = true => any existing date which is not present in stream_a 
			will be filled by stream_b
        - keys:		(list) List of keys to add from stream_b into stream_a.
        - offset: 	(float) Offset is added to stream b values. Default 0.
        - replace: 	(bool) Allows existing stream_a values to be replaced by stream_b ones.

    RETURNS:
        - Datastream(stream_a):	(DataStream) DataStream object.

    EXAMPLE:
        >>> # Joining two datasets together:
        >>> alldata = mergeStreams(pos_stream, lemi_stream, keys=['x','y','z'])

    APPLICATION:
    """

    addall = kwargs.get('addall') 
    comment = kwargs.get('comment')
    extend = kwargs.get('extend')
    keys = kwargs.get('keys')
    offset = kwargs.get('offset')
    replace = kwargs.get('replace')

    if not comment:
        comment = '-'
    if not keys:
        keys = KEYLIST[1:16]
    if not offset:
        offset = 0
    if not replace:
        replace = False
   
    loggerstream.info('mergeStreams: Start mergings at %s.' % str(datetime.now()))

    headera = stream_a.header
    headerb = stream_b.header

    # Test streams
    if len(stream_a) == 0:
        loggerstream.debug('mergeStreams: stream_a is empty - doing nothing')
        return stream_a
    if len(stream_b) == 0:
        loggerstream.debug('mergeStreams: stream_b is empty - returning unchanged stream_a')
        return stream_a
    # take stream_b data and find nearest element in time from stream_a
    timea = stream_a._get_column('time')
    timea = stream_a._maskNAN(timea)

    sta = list(stream_a)
    stb = list(stream_b)
    if addall:
        for elem in stream_b:
            sta.append(elem)
        sta.sort()
        return DataStream(sta, headera)
    elif extend:
        for elem in stream_b:
            if not elem.time in timea:
                sta.append(elem)
        sta.sort()
        return DataStream(sta, headera)
    else:
        # interpolate stream_b
        sb = stream_b.trim(starttime=np.min(timea), endtime=np.max(timea))
        timeb = sb._get_column('time')
        timeb = sb._maskNAN(timeb)

        function = sb.interpol(keys)

        taprev = 0
        for elem in sb:
            foundina = sb._find_nearest(timea,elem.time)
            pos = foundina[1]
            ta = foundina[0]
            if (ta > taprev) and (np.min(timeb) < ta < np.max(timeb)):
                taprev = ta
                functime = (ta-function[1])/(function[2]-function[1])
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        loggerstream.error('mergeStreams: Column key (%s) not valid.' % key)
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



def find_offset(stream1, stream2, **kwargs):
    '''
    DEFINITION:
        Uses least-squares method for a rough estimate of the offset in the time 
	axis of two different streams. Both streams must contain the same key, e.g. 'f'.
	GENTLE WARNING: This method is FAR FROM OPTIMISED.
			Interpolation brings in errors, *however* does allow for
			a more exact result.

    PARAMETERS:
    Variables:
        - stream1: 	(DataStream object) First stream to compare.
        - stream2: 	(DataStream object) Second stream to compare.
    Kwargs:
        - deltat_step:	(float) Time value to iterate over. Accuracy is higher with
			smaller values.
	- guess_low:	(float) Low guess for offset. Function will iterate from here.
	- guess_high:	(float) High guess for offset. Function will iterate till here.
	- log_chi:	(bool) If True, log chi values.
	- plot:		(bool) Filename of plot to save chi-sq values to, e.g. "chisq.png"

    RETURNS:
        - t_offset: 	(float) The offset (in seconds) calculated by least-squares method
			of stream_b.

    EXAMPLE:
        >>> offset = find_offset(gdas_data, pos_data, guess=-30.,deltat_min = 0.1)

    APPLICATION: 
        """
    '''

    # 1. Define starting parameters:
    deltat_step = kwargs.get('deltat_step')
    guess_low = kwargs.get('guess_low')
    guess_high = kwargs.get('guess_high')
    log_chi = kwargs.get('log_chi')

    if not deltat_step:
        deltat_step = 0.1
    if not guess_low:
        guess_low = -60.
    if not guess_high:
        guess_high = 60.
    N_iter = 0.

    # Interpolate the function with the smaller sample period.
    # Should hopefully lower error factors.

    sp1 = stream1.get_sampling_period()
    sp2 = stream2.get_sampling_period()

    if sp1 > sp2:
        stream_a = stream1
        stream_b = stream2
        main_a = True
    elif sp1 < sp2:
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

    stream_a = stream_a.trim(starttime=num2date(stime).replace(tzinfo=None)+timedelta(seconds=timespan*2), 
				endtime=num2date(etime).replace(tzinfo=None)+timedelta(seconds=-timespan*2))

    mean_a = stream_a.mean('f')
    mean_b = stream_b.mean('f')
    difference = mean_a - mean_b

    # Interpolate one stream:
    # Note: higher errors with lower degree of interpolation. Highest degree possible is desirable, linear terrible.
    try:
        int_data = stream_b.interpol(['f'],kind='cubic')
    except MemoryError:
        try:
            loggerstream.warning("find_offset: Not enough memory for cubic spline. Attempting quadratic...")
            int_data = stream_b.interpol(['f'],kind='quadratic')
        except MemoryError:
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
        #chisq_ = chisq_ + (item-difference)**2.		# Correction may be needed for reasonable values.
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


def subtractStreams(stream_a, stream_b, **kwargs):
    """
    combine the contents of two data stream:
    basically two methods are possible:
    1. replace all data from stream_a with differences (stream_a-stream_b)

    2. fill gaps in stream_a data with stream_b data without replacing
    """
    try:
        assert len(stream_a) > 0
    except:
        loggerstream.error('subtractStreams: stream_a empty - aborting merging function.')
        return stream_a
        
    keys = kwargs.get('keys')
    getmeans = kwargs.get('getmeans')
    if not keys:
        keys = KEYLIST[1:16]

    loggerstream.info('subtractStreams: Start subtracting streams.')

    headera = stream_a.header
    headerb = stream_b.header

    newst = DataStream()

    # take stream_b data and fine nearest element in time from stream_a
    timea = stream_a._get_column('time')
    timea = stream_a._maskNAN(timea)
    timeb = stream_b._get_column('time')
    timeb = stream_b._maskNAN(timeb)

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
            tb, itmp = stream_b._find_nearest(timeb,ta)
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
                            exec('elem.'+key+' = float(NaN)')
                    except:
                        loggerstream.warning("subtractStreams: Check why exception was thrown.")
                        exec('elem.'+key+' = float(NaN)')
            else:
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        loggerstream.error("subtractStreams: Column key %s not valid!" % key)
                    fkey = 'f'+key
                    if fkey in function[0]:
                        exec('elem.'+key+' = float(NaN)')
        else: # put NaNs in cloumn if no interpolated values in b exist
            for key in keys:
                if not key in KEYLIST[1:16]:
                    loggerstream.error("subtractStreams: Column key %s not valid!" % key)
                fkey = 'f'+key
                if fkey in function[0]:
                    exec('elem.'+key+' = float(NaN)')
                
    loggerstream.info('subtractStreams: Stream-subtraction finished.')

    return DataStream(stream_a, headera)      


def stackStreams(stream_a, stream_b, **kwargs): # TODO
    """
    stack the contents of two data stream:
    """
    pass

def compareStreams(stream_a, stream_b):
    '''
    DEFINITION:
        Default function will compare stream_a to stream_b. If data is missing in
        a or is different, it will be filled in with that from b.
        stream_b here is the reference stream.

    PARAMETERS:
    Variables:
        - stream_a: 	(DataStream) First stream
        - stream_b: 	(DataStream) Second stream, which is compared to stream_a for differences

    RETURNS:
        - stream_a: 	(DataStream) Description.

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
            else:	# insert row into stream_a
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
        date = datetime.strptime(daystring, '%b%d%y')
        dateform = '%b%d%y'
        # log ('Found Dateformat of type dateform
    except:
        # test for day month year
        tmpdaystring = re.findall(r'\d+',daystring)[0]
        testunder = daystring.replace('-','').split('_')
        for i in range(len(testunder)):
            try:
                numberstr = re.findall(r'\d+',testunder[i])[0]
            except:
                numberstr = '0'
            if len(numberstr) > 4:
                tmpdaystring = numberstr
        
        if len(tmpdaystring) > 8:
            tmpdaystring = tmpdaystring[:8]

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

    return date


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Now import the child classes with formats etc
# Otherwise DataStream etc will not be known
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
from lib.magpy_formats import *



if __name__ == '__main__':
    print "Starting a Test run of the MagPy program:"
    

    # Environmental Data
    # ------------------
    #rcs = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-RCS\RCS-T7-2012-02-01_00-00-00_c.txt'))
    #print "Plotting raw data:"
    #rcs.plot(['x','y','z','f'])
    #print "Applying Outlier removal:"
    #rcs = rcs.remove_outlier(keys=['x','y','z'])
    #rcs = rcs.remove_flagged(key='x')
    #rcs = rcs.remove_flagged(key='y')
    #rcs = rcs.remove_flagged(key='z')
    #rcs = rcs.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #rcs = rcs.smooth(['y'],window_len=21)
    #rcs.plot(['x','y','z','f'])
    #print "Header information RCS"
    #print rcs.header


    # Temperature measurements and corrections to time columns
    #usb = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\T-Logs\Schacht*'))
    #usb = usb.date_offset(-timedelta(hours=2)) # correcting times e.g. MET to UTC
    #usb = usb.filtered(filter_type='gauss',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30),respect_flags=True)
    #func = usb.interpol(['t1','t2','var1'])
    #usb.plot(['t1','t2','var1'],function=func)

    #rcs.clear_header()
    #print rcs.header

    # Variometer and Scalar Data
    # --------------------------

    # Storm analysis and fit functions
    #
    #st = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*'),starttime='2011-9-8',endtime='2011-9-14')
    #func = st.fit(['x','y','z'],fitfunc='spline',knotstep=0.1)
    #st = st.aic_calc('x',timerange=timedelta(hours=1))
    #st.plot(['x','y','z','var2'],function=func)
    #fmi = st.k_fmi(fitdegree=2)
    #fmi = st.k_fmi(fitfunc='spline',knotstep=0.4)
    #col = st._get_column('var2')
    #st = st._put_column(col,'y')
    #st = st.differentiate()
    #st.plot(['x','var2','dy','t2'],symbollist = ['-','-','-','z'])
    #
    # Seconds data and filtering
    #
    #st = read(path_or_url=os.path.normpath('e:\\leon\\Observatory\\Messdaten\\Data-Magnetism\\gdas\\rawdata\\*'),starttime='2011-7-8',endtime='2011-7-9')
    #st.plot(['x','y','z'])
    #st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #st.plot(['x','y','z'])
    #st = st.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
    #st.plot(['x','y','z'])
    #
    # Smoothing, differentiating, integrating, interpolating data
    #
    #st = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\Lemi\*'),starttime='2010-7-14',endtime=datetime(2010,7,15))
    #st = st.smooth(['x'],window_len=21)
    #st = st.differentiate()
    #col = st._get_column('dx')
    #st = st._put_column(col,'x')
    #st = st.integrate(keys=['x'])
    #func = st.interpol(['x','y','z'])
    #
    # Intensity values, Outlier removal and flagging
    #
    #st = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\Proton\CO091231.CAP'))
    #st = st.remove_outlier()
    #st = st.remove_flagged()
    #st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #st.plot(['f'])
    # --
    #st = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\proton\ZAGCPMAG-LOG_2010_07_18.txt'))
    #st = st.remove_outlier()
    #st = st.flag_stream('f',3,"Moaing",datetime(2010,7,18,12,0,0,0),datetime(2010,7,18,13,0,0,0))
    #st = st.remove_flagged()
    #func = st.fit(['f'],fitfunc='spline',knotstep=0.05)
    #st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #st.plot(['f'],function=func)
    #st = st.get_gaps(gapvariable=True)
    #st.plot(['f','var2'])
    # 
    # Merging data streams and filling of missing values
    #
    #st = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*')) #,starttime='2011-3-1')
    #newst = mergeStreams(st,usb,keys=['t1','var1'])
    #newst.plot(['x','y','z','t1','var1'],symbollist = ['-','-','-','-','-'],plottype='continuous')
    #st = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*')) #,starttime='2011-3-1')
    #print "Lenght before filling gaps"
    #print len(st)
    #st.plot(['x','y','z'])
    #st = st.get_gaps(gapvariable=True)
    #print "Lenght after filling gaps"
    #print len(st)
    #st.plot(['x','y','z','var2'])


    # Further functions -- incomplete
    # ---------------
    #st = st.func_subtract(func)

    #st.powerspectrum('x')

    #st = st.differentiate()
    #col = st._get_column('dx')
    #st = st._put_column(col,'x')
    #st = st.integrate(keys=['x'])
    #func = st.interpol(['x','y','z'])

    #st.plot(['x','y','z','var2'],function=func)
    #st.plot(['f'])

    # Absolute Values
    # ---------------
    #st = read(path_or_url=os.path.normpath(r'e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*'),starttime='2011-9-1',endtime='2011-9-02')
    #st = read(path_or_url=os.path.normpath(r'f:\Vario-Cobenzl\dIdD-System\*'),starttime='2011-8-20',endtime='2011-8-21')
    #bas = read(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_didd.txt'))
    #bas.plot(['x','y','z'])
    #func = bas.fit(['dx','dy','dz'],fitfunc='spline',knotstep=0.05)
    #bas.plot(['dx','dy','dz'],function=func)

    #st.plot(['x','y','z'])
    #st = st.baseline(bas,knotstep=0.05,plotbaseline=True)
    #st.plot(['x','y','z'])


    #stle = read(path_or_url=os.path.normpath(r'f:\Vario-Cobenzl\dIdD-System\LEMI\*'),starttime='2011-8-20',endtime='2011-8-21')
    #basle = read(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
    #stle = stle.baseline(basle,knotstep=0.05,plotbaseline=True)
    #stle.plot(['x','y','z'])


    # Baseline Correction and RotationMatrix
    # ---------------
    # alpha and beta describe the rotation matrix (alpha is the horizontal angle (D) and beta the vertical)
    #didd = read(path_or_url=os.path.normpath('g:\Vario-Cobenzl\dIdD-System\*'),starttime='2011-01-1',endtime='2011-12-31')
    #basdidd = read(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_didd.txt'))
    #lemi = read(path_or_url=os.path.normpath('g:\Vario-Cobenzl\dIdD-System\LEMI\*'),starttime='2011-01-1',endtime='2011-12-31')
    #baslemi = read(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
    #lemi = lemi.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
    #didd = didd.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
    #lemi = lemi.rotation(alpha=3.3,beta=0.0)
    #didd = didd.baseline(basdidd,knotstep=0.05,plotbaseline=True)
    #lemi = lemi.baseline(baslemi,knotstep=0.05,plotbaseline=True)
 
    #didd.plot(['x','y','z'])
    #lemi.plot(['x','y','z'])
    #newst = subtractStreams(didd,lemi,keys=['x','y','z'],getmeans=True)
    # for some reason first and last points are not subtracted
    #newst = newst.trim(starttime=datetime(2011,1,1,02,00),endtime=datetime(2011,12,30,22,00))
    #newst = newst.remove_outlier(keys=['x','y','z'])
    #print "Mean x: %f" % np.mean(newst._get_column('x'))
    #print "Mean y: %f" % np.mean(newst._get_column('y'))
    #print "Mean z: %f" % np.mean(newst._get_column('z'))

    #newst.plot(['x','y','z'])

    # DTU data
    # ---------------
    #dtust = read(path_or_url=os.path.normpath(r'g:\VirtualBox\Ny mappe\GDH4_20091215.cdf'))
    #st = read(path_or_url=os.path.normpath(r'g:\VirtualBox\Ny mappe\FHB*.sec'))
    #st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #st = st.aic_calc('x',timerange=timedelta(hours=1))
    #col = st._get_column('var2')
    #st = st._put_column(col,'y')
    #st = st.differentiate()
    #st.plot(['x','var2','dy'],symbollist = ['-','-','-'])
    #dtust = dtust.filtered(filter_type='linear',filter_width=timedelta(hours=1))
    #dtust.plot(['x','y','z'])

    # Comparison of baseline calculated with and without correct orientation of sensor
    # ---------------
    #baslemi1 = read(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_lemi_alpha3.3.txt'))
    #baslemi2 = read(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_didd.txt'))
    #newst = subtractStreams(baslemi1,baslemi2,keys=['x','y','z'])
    #newst = newst.trim(starttime=datetime(2010,7,10,00,02),endtime=datetime(2011,10,1,23,58))
    #newst.plot(['x','y','z'])

    #testarray = np.array(baslemi1)
    #print testarray[1][2]
    #print testarray.ndim
    # Testing new funcs
    #lemi = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\lemi\*'),starttime='2010-7-17',endtime='2010-7-18')
    #baslemi = read(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
    #lemi = lemi.rotation(alpha=3.30,beta=-4.5)
    #lemi = lemi.baseline(baslemi,knotstep=0.05,plotbaseline=True)
    #lemi.plot(['x','y','z'])

    
    #st = read(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*'),starttime='2011-9-1',endtime='2011-9-30')
    #st.plot(['x','y','z'])
    #newst = subtractStreams(bas,st,keys=['x','y','z'])
    #newst.plot(['x','y','z'])

    #print len(st)
    #print "Current header information:"
    #print st.header
    
