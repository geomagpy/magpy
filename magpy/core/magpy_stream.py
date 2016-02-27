#!/usr/bin/env python
"""
MagPy-General: Standard pymag package containing the following classes:
Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 23.02.2012)
"""

# ------------------------------------
# Part 1: Import routines for packages
# ------------------------------------
logpygen = '' # loggerstream variable
netcdf = True # Export routines for netcdf
spacecdf = True # Export routines for Nasa cdf
mailingfunc = True # E-mail notifications
nasacdfdir = "c:\CDF Distribution\cdf33_1-dist\lib"

try:
    #standard packages
    import csv
    import pickle
    import struct # binary package for reading/writing
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
except ImportError:
    print "Init MagPy: Critical Import failure: Python numpy-scipy required - please install to proceed"

try:
    # Matpoltlib
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
    from pylab import *  # do I need that?
    from datetime import datetime, timedelta
except ImportError:
    logpygen += "Init MagPy: Critical Import failure: Python matplotlib required - please install to proceed\n"

try:
    #numpy/scipy
    import numpy as np
    import scipy as sp
    from scipy import interpolate
    from scipy import stats
    import math
except ImportError:
    logpygen += "Init MagPy: Critical Import failure: Python numpy-scipy required - please install to proceed\n"

try:
    # NetCDF
    print "Loading Netcdf4 support ..."
    from netCDF4 import Dataset
    print "success"
except ImportError:
    netcdf = False
    print " -failed- "
    logpygen += "Init MagPy: Import failure: Netcdf not available\n"
    pass

try:
    # NasaCDF
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


try:
    import smtplib
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEBase import MIMEBase
    from email.MIMEText import MIMEText
    from email.Utils import COMMASPACE, formatdate
    from email import Encoders
    # Import mailing functions - for e-mail notifications
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

# ##################
# file format tests
# ##################

logging.basicConfig(filename='magpy.log',filemode='w',format='%(asctime)s %(levelname)s: %(message)s',level=logging.DEBUG)
#logging.basicConfig(filename='magpy.log',filemode='w',format='%(asctime)s %(levelname)s: %(message)s',level=logging.WARNING)
#logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',level=logging.WARNING)
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)

# Package loggers to identify info/problem source
loggerabs = logging.getLogger('core.magpy_abolutes')
loggertransfer = logging.getLogger('core.magpy_transfer')
loggerstream = logging.getLogger('core.magpy_stream')
loggerlib = logging.getLogger('lib')

# Special loggers for event notification
stormlogger = logging.getLogger('core.magpy_stream')


KEYLIST = ['time','x','y','z','f','t1','t2','var1','var2','var3','var4','var5','dx','dy','dz','df','str1','str2','str3','str4','flag','comment','typ','sectime']
KEYINITDICT = {'time':0,'x':float('nan'),'y':float('nan'),'z':float('nan'),'f':float('nan'),'t1':float('nan'),'t2':float('nan'),'var1':float('nan'),'var2':float('nan'),'var3':float('nan'),'var4':float('nan'),'var5':float('nan'),'dx':float('nan'),'dy':float('nan'),'dz':float('nan'),'df':float('nan'),'str1':'-','str2':'-','str3':'-','str4':'-','flag':'0000000000000000-','comment':'-','typ':'xyzf','sectime':float('nan')}
FLAGKEYLIST = KEYLIST[:16]
# KEYLIST[:8] # only primary values with time
# KEYLIST[1:8] # only primary values without time


PYMAG_SUPPORTED_FORMATS = ['IAGA', 'WDC', 'DIDD', 'GSM19', 'LEMIHF', 'LEMIBIN', 'LEMIBIN2', 'OPT', 'PMAG1', 'PMAG2', 'GDASA1',
                           'GDASB1', 'RMRCS', 'CR800','RADON', 'USBLOG', 'SERSIN', 'SERMUL', 'PYSTR', 'POS1', 'POS1TXT', 'ENV05',
                           'PYCDF', 'PYBIN', 'PYNC','DTU1','SFDMI','SFGSM','BDV1','GFZKP','NOAAACE','LATEX','CS','UNKOWN']

# -------------------
#  Main classes -- DataStream, LineStruct and PyMagLog (To be removed)
# -------------------

class DataStream(object):
    """
    Creates a list object from input files /url data
    data is organized in columns

    keys are column identifier:
    key in keys: see KEYLIST

    The following application methods are provided:
    - stream.aic_calc(key) -- returns stream (with !var2! filled with aic values)
    - stream.differentiate() -- returns stream (with !dx!,!dy!,!dz!,!df! filled by derivatives)
    - stream.fit(keys) -- returns function
    - stream.filter() -- returns stream (changes sampling_period; in case of fmi ...)
    - stream.integrate() -- returns stream (integrated vals at !dx!,!dy!,!dz!,!df!)
    - stream.interpol(keys) -- returns function
    - stream.routlier() -- returns stream (adds flags and comments)
    - stream.smooth(key) -- returns stream
    - stream.trim() -- returns stream within new time frame
    - stream.remove_flagged() -- returns stream (removes data from stream according to flags)
    - stream.flag_stream(key,flag,comment,startdate) -- returns stream (adds flags and comments)
    - stream.get_gaps()
    - stream.get_sampling_period() -- returns float (with period in days)

    - stream.pmplot(keys)
    - stream.pmspectrogram(keys)
    - stream.powerspectrum(key)

    Supporting internal methods are:
    - self._timetest(time) -- returns datetime object
    - self._get_column(key) -- returns list
    - self._put_column(key)
    - self._get_min(key) -- returns float
    - self._get_max(key) -- returns float
    - self._aic(signal, k) -- returns float -- determines Akaki Information Criterion for a specific index k
    - self._normalize(column) -- returns list,float,float -- normalizes selected column to range 0,1
    - self._denormalize -- returns list -- (column,startvalue,endvalue) denormalizes selected column from range 0,1 ro sv,ev

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

    # ----------------
    # Standard functions and overrides for list like objects
    # ----------------

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

    #def extend(self, datlst):
    #    self.container.extend(datlst)

    def extend(self,datlst,header):
        self.container.extend(datlst)
        self.header = header

    def sorting(self):
        """
        Sorting data according to time (maybe generalize that to some key)
        """
        liste = sorted(self.container, key=lambda tmp: tmp.time)
        return DataStream(liste, self.header)

    def clear_header(self):
        """
        Remove header information
        """
        self.header = {}

    # ----------------
    # internal methods
    # ----------------

    def _find_nearest(self, array, value):
        idx=(np.abs(array-value)).argmin()
        return array[idx], idx

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


    def _aic(self, signal, k, debugmode=None):
        try:
            aicval = k* np.log(np.var(signal[:k]))+(len(signal)-k-1)*np.log(np.var(signal[k:]))
        except:
            if debugmode:
                loggerstream.debug('_AIC: could not evaluate AIC at index position %i' % (k))
            pass
        return aicval

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

    def _hf(self, p, x):
        """
        Harmonic function
        """
        hf = p[0]*cos(2*pi/p[1]*x+p[2]) + p[3]*x + p[4] # Target function
        return hf

    def _residualFunc(self, func, y):
        """
        residual of the harmonic function
        """
        return y - func

    def _gf(self, t, tau):
        """
        Gauss function
        """
        return np.exp(-((t/tau)*(t/tau))/2)

    def _tau(self, period):
        """
        low pass filter with -3db point at period in sec (e.g. 120 sec)
        1. convert period from seconds to days as used in daytime
        2. return tau (in unit "day")
        """
        per = period/(3600*24)
        return 0.83255461*per/(2*np.pi)

    def _det_trange(self, period):
        """
        starting with coefficients above 1%
        is now returning a timedelta object
        """
        return np.sqrt(-np.log(0.01)*2)*self._tau(period)

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


    def _drop_nans(self, key):
        """Helper to drop lines with NaNs in any of the selected keys.

        """
        #print key
        #keylst = [eval('elem.'+key) for elem in self]
        #print keylst
        newst = [elem for elem in self if not isnan(eval('elem.'+key)) and not isinf(eval('elem.'+key))]
        return DataStream(newst,self.header)


    def _is_number(self, s):
        """
        Test whether s is a number
        """
        try:
            float(s)
            return True
        except ValueError:
            return False

    # ----------------
    # application methods - alphabetical order
    # ----------------


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

    def baseline( self, absolutestream, **kwargs):
        """
        calculates baseline correction for input stream (datastream)
        Uses available baseline values from the provided absolute file
        Special cases:
        1) Absolte data covers the full time range of the stream:
            -> Absolute data is extrapolated by duplicating the last and first entry at "extradays" offset
            -> desired function is calculated
        2) No Absolte data for the end of the stream:
            -> like 1: Absolute data is extrapolated by duplicating the last entry at "extradays" offset or end of stream
            -> and info message is created, if timedifference exceeds the "extraday" arg then a warning will be send
        2) No Absolte data for the beginning of the stream:
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
                bas.pmplot(['x','y','z'],padding = 5, symbollist = ['o','o','o'],function=func,plottitle='Absolute data',outfile=plotfilename)
            else:
                bas.pmplot(['x','y','z'],padding = 5, symbollist = ['o','o','o'],function=func,plottitle='Absolute data')

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


        loggerstream.info('Corrected time column by %s sec' % str(offset.seconds))

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

        loggerstream.info('--- Calculating derivative started at %s ' % str(datetime.now()))

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
            loggerstream.info('--- Extract: Please provide proper compare paramter ">=", "<=",">", "<", "==" or "!=" ')
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


    def filter(self, **kwargs):
        """
        Filtering function
        kwargs support the following keywords:
            - filter_type   (gaussian, linear or special) default=gaussian
            - filter_width (timedelta-object) default=timedelta(minutes=1)
            - filter_offset   (timedelta-object) default=0
            - gauss_win (int) default=1.86506 (corresponds to +/-45 sec in case of min or 45 min in case of hour
            - fmi_initial_data (DataStream containing dH values (dx)  default=[]
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
            loggerstream.warning('FilterFunc: No valid stream provided')
            return self

        # check whether requested filter_width >= sampling interval within 1 millisecond accuracy
        si = timedelta(seconds=self.get_sampling_period()*24*3600)
        if filter_width - si <= timedelta(microseconds=1000):
            loggerstream.warning('FilterFunc: Requested filter_width does not exceed sampling interval - aborting filtering')
            return self

        loggerstream.info('--- Start filtering at %s ' % str(datetime.now()))

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
                    loggerstream.warning("FilterFunc: Filter not recognized - aborting filering")
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
        #self.header['DigitalSamplingWidth'] = str(trange.seconds)+' sec'
        self.header['DataSamplingFilter'] = filter_type + str(filter_width.seconds)+' sec'
        #self.header['DataInterval'] = str(filter_width.seconds)+' sec'

        loggerstream.info(' --- Finished filtering at %s' % str(datetime.now()))

        return DataStream(resdata,self.header)


    def find_mean_var(self, key):
        '''
        A simple routine to find the probabilistic mean and variance from one column of data.
        '''

        if not key in KEYLIST:
            raise ValueError, "Wrong Key"

        column = self._get_column(key)

        mean_sum, var_sum = 0, 0
        for elem in column:
            mean_sum = mean_sum + elem
        mean = mean_sum/len(column)

        for elem in column:
            var_sum = var_sum + (elem - mean)**2.
        variance = var_sum/len(column)

        return mean, variance


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

        for time savings, this function is only testing the first 1000 elements
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
        interpolating streams:
        kwargs support the following keywords:
            - timerange (timedelta obsject) default=timedelta(hours=1)
            - fitdegree (float)  default=4
            - knotstep (float < 0.5) determines the amount of knots: amount = 1/knotstep ---> VERY smooth 0.1 | NOT VERY SMOOTH 0.001
            - flag
        """
        t = self._get_column('time')
        nt,sv,ev = self._normalize(t)
        sp = self.get_sampling_period()
        functionkeylist = {}

        for key in keys:
            if not key in KEYLIST[1:16]:
                raise ValueError, "Column key not valid"
            val = self._get_column(key)
            # interplolate NaN values
            nans, xxx= self._nan_helper(val)
            try: # Try to interpolate nan values
                val[nans]= np.interp(xxx(nans), xxx(~nans), val[~nans])
            except:
                #val[nans]=int(nan)
                pass
            if len(val)>1:
                exec('f'+key+' = interpolate.interp1d(nt, val)')
                exec('functionkeylist["f'+key+'"] = f'+key)
            else:
                pass

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
            #fmitmpstream.pmplot(['x'],function=func)
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
        Calculates mean values for the specified key, Nan's are regarded for.
        Means are only calculated if more then "amount" in percent are non-nan's
        Returns a float if successful or NaN.
        :type percentage: int
        :param percentage: Define required percentage of non-nan values, if not met that nan will be returned. Default is 95 (%)
        :type meanfunction: string
        :param meanfunction: accepts 'mean' and 'median'. Default is 'mean'

        Example:
        meanx = datastream.mean('x',meanfunction='median',percentage=90)
        """
        percentage = kwargs.get('percentage')
        meanfunction = kwargs.get('meanfunction')

        if not meanfunction:
            meanfunction = 'mean'
        if not percentage:
            percentage = 95
        if not isinstance( percentage, (int,long)):
            raise ValueError, "Mean: Percentage needs to be an integer"
        if not key in KEYLIST[:16]:
            raise ValueError, "Mean: Column key not valid"

        ar = [eval('elem.'+key) for elem in self if not isnan(eval('elem.'+key))]
        div = float(len(ar))/float(len(self))*100.0

        if div >= percentage:
            return eval('np.'+meanfunction+'(ar)')
        else:
            loggerstream.warning('mean: To many nans in column, exceeding %d percent' % percentage)
            return float("NaN")


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
        """
        #header = self.header

        for key in offsets:
            if key in KEYLIST:
                val = self._get_column(key)
                if key == 'time':
                    newval = [num2date(elem.time).replace(tzinfo=None) + offsets[key] for elem in val]
                    loggerstream.info('Offset function: Corrected time column by %s sec' % str(offset.seconds))
                else:
                    newval = [elem + offsets[key] for elem in val]
                    loggerstream.info('Offset function: Corrected column %s by %.3f' % (key, offsets[key]))
                self = self._put_column(newval, key)

        return self


    def plot(self, keys, debugmode=None, **kwargs):
        """
        Creates a simple graph of the current stream. In order to run matplotlib from cron one need to include (matplotlib.use('Agg'))
        Supports the following keywords:
        function: (func) [0] is a dictionary containing keys (e.g. fx), [1] the startvalue, [2] the endvalue  Plot the content of function within the plot
        fullday: (boolean - default False) rounds first and last day two 0:00 respectively 24:00 if True
        colorlist (list - default []): provide a ordered color list of type ['b','g'],....
        errorbar: (boolean - default False) plot dx,dy,dz,df values if True
        symbol: (string - default '-') symbol for primary plot
        symbol_func: (string - default '-') symbol of function plot
        savefigure: (string - default None) if provided a copy of the plot is saved to savefilename.png
        outfile: strign to save the figure, if path is not existing it will be created
        fmt: format of outfile
        savedpi: integer resolution
        noshow: bool- don't call show at the end, just returns figure handle
        annote: bool - annotate data using comments
        padding: (integer - default 0) Value to add to the max-min data for adjusting y-scales
                 maybe change that to a relative padding depending on data values
        :type bgcolor: string
        :param bgcolor: Define background color e.g. '0.5' greyscale, 'r' red, etc
        :type gridcolor: string
        :param gridcolor: Define grid color e.g. '0.5' greyscale, 'r' red, etc
        :type labelcolor: string
        :param labelcolor: Define grid color e.g. '0.5' greyscale, 'r' red, etc
        :type grid: bool
        :param grid: show grid or not, default = True
        :type specialdict: dictionary
        :param specialdict: contains special information for specific plots. key
                      key corresponds to the column
                      input is a list with the following parameters
                      ('None' if not used)
                      ymin
                      ymax
                      ycolor
                      bgcolor
                      grid
                      gridcolor


            from magpy_stream import *
            st = read()
            st.pmplot()

            keys define the columns to be plotted
        """
        function = kwargs.get('function')
        fullday = kwargs.get('fullday')
        plottitle = kwargs.get('plottitle')
        colorlist = kwargs.get('colorlist')
        errorbar = kwargs.get('errorbar')
        padding = kwargs.get('padding')
        labelcolor = kwargs.get('labelcolor')
        bgcolor = kwargs.get('bgcolor')
        gridcolor = kwargs.get('gridcolor')
        grid = kwargs.get('grid')
        bartrange = kwargs.get('bartrange') # in case of bars (z) use the following trange
        symbollist = kwargs.get('symbollist')
        plottype = kwargs.get('plottype')
        symbol_func = kwargs.get('symbol_func')
        endvalue = kwargs.get('endvalue')
        outfile = kwargs.get('outfile')
        fmt = kwargs.get('fmt')
        savedpi = kwargs.get('savedpi')
        noshow = kwargs.get('noshow')
        annotate = kwargs.get('annotate')
        confinex = kwargs.get('confinex')
        specialdict = kwargs.get('specialdict')

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
            raise  ValueError, "Provide valid key(s)"
        count = 0
        fig = plt.figure()

        if debugmode:
            print "Start plotting at %s" % datetime.utcnow()

        t = np.asarray([row[0] for row in self])
        for key in keys:
            if not key in KEYLIST[1:16]:
                raise ValueError, "Column key not valid"
            #print datetime.utcnow()
            ind = KEYLIST.index(key)
            yplt = np.asarray([row[ind] for row in self])
            #yplt = self._get_column(key)
            # switch between continuous and discontinuous plots
            if debugmode:
                print "column extracted at %s" % datetime.utcnow()
            if plottype == 'discontinuous':
                yplt = self._maskNAN(yplt)
            else:
                nans, test = self._nan_helper(yplt)
                newt = [t[idx] for idx, el in enumerate(yplt) if not nans[idx]]
                t = newt
                yplt = [el for idx, el in enumerate(yplt) if not nans[idx]]
            # start plotting if non-nan data is present
            len_val= len(yplt)
            if debugmode:
                print "Got row with %d elements" % len_val
            if len_val > 1:
                count += 1
                ax = count
                subplt = "%d%d%d" %(n_subplots,1,count)
                # Create primary plot and define x scale and ticks/labels of subplots
                if count == 1:
                    ax = fig.add_subplot(subplt, axisbg=bgcolor)
                    if plottitle:
                        ax.set_title(plottitle)
                    a = ax
                else:
                    ax = fig.add_subplot(subplt, sharex=a, axisbg=bgcolor)
                timeunit = ''
                if confinex:
                    trange = np.max(t) - np.min(t)
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
                        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%d.%b%y'))
                        setp(ax.get_xticklabels(),rotation='70')
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

                if count < len(keys):
                    setp(ax.get_xticklabels(), visible=False)
                else:
                    ax.set_xlabel("Time (UTC) %s" % timeunit, color=labelcolor)

                # Create plots
                ymin = np.min(yplt)-padding
                ymax = np.max(yplt)+padding
                if specialdict:
                    if key in specialdict:
                        paramlst = specialdict[key]
                        if not paramlst[0] == None:
                            ymin = paramlst[0]
                        if not paramlst[1] == None:
                            ymax = paramlst[1]
                # -- switch color and symbol
                if symbollist[count-1] == 'z': # symbol for plotting colored bars for k values
                    xy = range(9)
                    for num in range(len(t)):
                        if bartrange < t[num] < np.max(t)-bartrange:
                            ax.fill([t[num]-bartrange,t[num]+bartrange,t[num]+bartrange,t[num]-bartrange],[0,0,yplt[num]+0.1,yplt[num]+0.1],facecolor=cm.RdYlGn((9-yplt[num])/9.,1),alpha=1,edgecolor='k')
                    ax.plot_date(t,yplt,colorlist[count-1]+'|')
                else:
                    ax.plot_date(t,yplt,colorlist[count-1]+symbollist[count-1])
                if errorbar:
                    yerr = self._get_column('d'+key)
                    if len(yerr) > 0:
                        ax.errorbar(t,yplt,yerr=varlist[ax+4],fmt=colorlist[count]+'o')
                    else:
                        loggerstream.warning(' -- Errorbars (d%s) not found for key %s' % (key, key))
                #ax.plot_date(t2,yplt2,"r"+symbol[1],markersize=4)
                # Add annotations for flags
                if grid:
                    ax.grid(True,color=gridcolor,linewidth=0.5)
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
                            loggerstream.debug('PmPlot: shown column beyong flagging range: assuming flag of column 0 (= time)')

                if function:
                    fkey = 'f'+key
                    if fkey in function[0]:
                        ttmp = arange(0,1,0.0001)# Get the minimum and maximum relative times
                        ax.plot_date(self._denormalize(ttmp,function[1],function[2]),function[0][fkey](ttmp),'r-')
                # Y-axis ticks
                if bool((count-1) & 1):
                    ax.yaxis.tick_right()
                    ax.yaxis.set_label_position("right")
                # Y-axis labels from header information
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
                if debugmode:
                    print "Finished plot %d at %s" % (count, datetime.utcnow())
            else:
                loggerstream.warning("Plot: No data available for key %s" % key)

        fig.subplots_adjust(hspace=0)

        if outfile:
            path = os.path.split(outfile)[0]
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


    def write(self, filepath, **kwargs):
        """
        Writing Stream to a file
        filepath (string): provding path/filename for saving
        Keywords:
        format_type (string): in which format - default pystr
        period (string) : supports hour, day, month, year, all - default day
        filenamebegins (string): providing the begin of savename (e.g. "WIK_")
        filenameends (string): providing the end of savename (e.g. ".min")
        wformat (string): outputformat
        dateformat (string):  outformat of date in filename (e.g. "%Y-%m-%d" -> "2011_11_22"
        coverage: (timedelta): day files or hour or month or year or all - default day
        mode: (append, overwrite, replace, skip) mode for handling existing files/data in files
        --- > Example output: "WIK_2011-11-22.min"
        """
        format_type = kwargs.get('format_type')
        filenamebegins = kwargs.get('filenamebegins')
        filenameends = kwargs.get('filenameends')
        dateformat = kwargs.get('dateformat')
        coverage = kwargs.get('coverage')
        mode = kwargs.get('mode')
        offsets = kwargs.get('offsets')
        createlatex = kwargs.get('createlatex')
        keys = kwargs.get('keys')

        if not format_type:
            format_type = 'PYSTR'
        if not format_type in PYMAG_SUPPORTED_FORMATS:
            loggerstream.info('Write: Output format not supported')
            print "Format not supported"
            return
        if not dateformat:
            dateformat = '%Y-%m-%d' # or %Y-%m-%dT%H or %Y-%m or %Y or %Y
        if not coverage:
            coverage = timedelta(days=1)
        if not filenamebegins:
            filenamebegins = ''
        if not filenameends:
            # Extension for cfd files is automatically attached
            if format_type == 'PYCDF':
                filenameends = ''
            else:
                filenameends = '.txt'
        if not mode:
            mode= 'overwrite'

        if len(self) < 1:
            loggerstream.info('Write: zero length of stream ')
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


    def remove_flagged(self, **kwargs):
        """
        remove flagged data from stream:
        kwargs support the following keywords:
            - flaglist  (list) default=[1,3]
            - keys (string e.g. 'f') default=FLAGKEYLIST
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


    def remove_outlier(self, **kwargs):
        """
        uses quartiles: threshold should be 1.5
        treshold 5 keeps storm onsets in
        treshold 4 seems to be the best compromise
        Get start time and add (e.g.) one hour for upper limit
        kwargs support the following keywords:
            - timerange (timedelta obsject) default=timedelta(hours=1)
            - threshold (float)  default=4
            - flag
            - keys  (from KEYLIST) default 'f'

        Position of flag in flagstring
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

        loggerstream.info('--- Starting outlier removal at %s ' % (str(datetime.now())))

        if len(self) < 1:
            loggerstream.info('--- No data - Stopping outlier removal at %s ' % (str(datetime.now())))
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
                        loggerstream.warning("Eliminate outliers produced a problem: please check\n")
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
                            loggerstream.info("Outlier: removed %s (= %f) at time %s, " % (key, eval('elem.'+key), datetime.strftime(num2date(elem.time),"%Y-%m-%dT%H:%M:%S")))
                    else:
                        fllist = list(row.flag)
                        fllist[flagpos] = '0'
                        row.flag=''.join(fllist)
                    newst.add(row)

        loggerstream.info('--- Outlier removal finished at %s ' % str(datetime.now()))

        return DataStream(newst, self.header)


    def smooth(self, keys, **kwargs):
        """smooth the data using a window with requested size.
        (taken from Cookbook/Signal Smooth)
        This method is based on the convolution of a scaled window with the signal.
        The signal is prepared by introducing reflected copies of the signal
        (with the window size) in both ends so that transient parts are minimized
        in the begining and end part of the output signal.

        input:
            x: the input signal
            window_len: the dimension of the smoothing window; should be an odd integer
            window: the type of window from 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'
                flat window will produce a moving average smoothing.

        output:
            the smoothed signal

        example:

        t=linspace(-2,2,0.1)
        x=sin(t)+randn(len(t))*0.1
        y=smooth(x)

        see also:

        numpy.hanning, numpy.hamming, numpy.bartlett, numpy.blackman, numpy.convolve
        scipy.signal.lfilter

        TODO: the window parameter could be the window itself if an array instead of a string
        """
        # Defaults:
        window_len = kwargs.get('window_len')
        window = kwargs.get('window')
        if not window_len:
            window_len = 11
        if not window:
            window='hanning'


        loggerstream.info(' --- Start smoothing (%s window, width %d) at %s' % (window, window_len, str(datetime.now())))

        for key in keys:
            if not key in KEYLIST:
                raise ValueError, "Column key not valid"

            x = self._get_column(key)

            if x.ndim != 1:
                raise ValueError, "smooth only accepts 1 dimension arrays."
            if x.size < window_len:
                raise ValueError, "Input vector needs to be bigger than window size."
            if window_len<3:
                return x
            if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
                raise ValueError, "Window is none of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'"

            s=np.r_[x[window_len-1:0:-1],x,x[-1:-window_len:-1]]
            #print(len(s))
            if window == 'flat': #moving average
                w=np.ones(window_len,'d')
            else:
                w=eval('np.'+window+'(window_len)')

            y=np.convolve(w/w.sum(),s,mode='valid')

            self._put_column(y[(int(window_len/2)):(len(x)+int(window_len/2))],key)

        loggerstream.info(' --- Finished smoothing at %s' % (str(datetime.now())))

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


    def powerspectrum(self, key, debugmode=None, outfile=None, fmt=None, axes=None, title=None):
        """
        Calculating the power spectrum
        following the numpy fft example
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

def findMean(stream):
    '''
    I exist
    '''
    print "hello"

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
    The read functions trys to open the selected dats
    dataformat: none - autodetection
    supported formats - use extra packages: generalcdf, cdf, netcdf, MagStructTXT,
    IAGA02, DIDD, PMAG, TIMEVAL
    optional arguments are starttime, endtime and dateformat of file given in kwargs

    :type path_or_url: string
    :param path_or_url: pathname of the following kinds:
                        a) c:\my\data\*
                        b) c:\my\data\thefile.txt
                        c) /home/data/*
                        d) /home/data/thefile.txt
                        e) ftp://server/directory/
                        f) ftp://server/directory/thefile.txt
                        g) http://www.thepage.at/file.tab
    """
    messagecont = ""

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
                loggerstream.critical("Check file/pathname - No file matching pattern: %s" % pathname)
                raise Exception("No file matching file pattern: %s" % pathname)
            elif not has_magic(pathname) and not os.path.isfile(pathname):
                raise IOError(2, "No such file or directory", pathname)
            # Only raise error if no starttime/endtime has been set. This
            # will return an empty stream if the user chose a time window with
            # no data in it.
            # XXX: Might cause problems if the data is faulty and the user
            # set starttime/endtime. Not sure what to do in this case.
            elif not 'starttime' in kwargs and not 'endtime' in kwargs:
                raise Exception("Cannot open file/files: %s" % pathname)
    # Trim if times are given.
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    if headonly and (starttime or endtime):
        msg = "Keyword headonly cannot be combined with starttime or endtime."
        raise Exception(msg)
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
    # get format type
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
    # file format should be known by now
    #loggerstream.info('Appending data - dataformat: %s' % format_type)
    #print format_type
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
    # read
    #if not format_type == 'UNKNOWN':
    #print format_type
    stream = readFormat(filename, format_type, headonly=headonly, **kwargs)

    # set _format identifier for each trace
    #for trace in stream:
    #    trace.stats._format = format_ep.name
    return DataStream(stream, stream.header)


def mergeStreams(stream_a, stream_b, **kwargs):
    """
    combine the contents of two data stream:
    basically two methods are possible:
    1. replace data from specific columns of stream_a with data from stream_b
    - requires keys

    2. fill gaps in stream_a data with stream_b data without replacing
    - extend = true => any existing date which is not present in stream_a will be filled by stream_b

    keywords:
    keys
    extend: time range of stream b is eventually added to stream a
    offset: offset is added to stream b values
    comment: add comment to stream_b data in stream_a
    """
    keys = kwargs.get('keys')
    extend = kwargs.get('extend') # add all elements from stream_b for which no time exists in stream_a
    addall = kwargs.get('addall') # add all elements from stream_b
    offset = kwargs.get('offset')
    comment = kwargs.get('comment')
    if not keys:
        keys = KEYLIST[1:16]
    if not offset:
        offset = 0
    if not comment:
        comment = '-'

    loggerstream.info('--- Start mergings at %s ' % str(datetime.now()))

    headera = stream_a.header
    headerb = stream_b.header

    # Test streams
    if len(stream_a) == 0:
        loggerstream.info('mergeStreams: stream_a is empty - doing nothing')
        return stream_a
    if len(stream_b) == 0:
        loggerstream.info('mergeStreams: stream_b is empty - returning unchanged stream_a')
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
        # interploate stream_b
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
                        raise ValueError, "Column key not valid"
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

    loggerstream.info('--- Mergings finished at %s ' % str(datetime.now()))

    return DataStream(stream_a, headera)


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
        loggerstream.error('Stream a empty - aborting merging function')
        return stream_a

    keys = kwargs.get('keys')
    getmeans = kwargs.get('getmeans')
    if not keys:
        keys = KEYLIST[1:16]

    loggerstream.info('--- Start subtracting streams at %s ' % str(datetime.now()))

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
        loggerstream.error('Subtracting streams: stream are not overlapping')
        return stream_a

    # Take only the time range of the shorter stream
    # Important for baselines: extend the absfile to start and endtime of the stream to be corrected
    stream_a = stream_a.trim(starttime=num2date(stime).replace(tzinfo=None), endtime=num2date(etime).replace(tzinfo=None))
    stream_b = stream_b.trim(starttime=num2date(stimeb).replace(tzinfo=None), endtime=num2date(etimeb).replace(tzinfo=None))

    samplingrate_b = stream_b.get_sampling_period()

    loggerstream.info('Subtracting Streams: time range form %s to %s' % (num2date(stime).replace(tzinfo=None),num2date(etime).replace(tzinfo=None)))

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
                        raise ValueError, "Column key not valid"
                    fkey = 'f'+key
                    try:
                        if fkey in function[0] and not isnan(eval('stream_b[itmp].' + key)):
                            newval = function[0][fkey](functime)
                            exec('elem.'+key+' -= float(newval)')
                        else:
                            exec('elem.'+key+' = float(NaN)')
                    except:
                            print "Check why exception was thrown in subtractStreams function"
                            exec('elem.'+key+' = float(NaN)')
            else:
                for key in keys:
                    if not key in KEYLIST[1:16]:
                        raise ValueError, "Column key not valid"
                    fkey = 'f'+key
                    if fkey in function[0]:
                        exec('elem.'+key+' = float(NaN)')
        else: # put NaNs in cloumn if no interpolated values in b exist
            for key in keys:
                if not key in KEYLIST[1:16]:
                    raise ValueError, "Column key not valid"
                fkey = 'f'+key
                if fkey in function[0]:
                    exec('elem.'+key+' = float(NaN)')


    loggerstream.info('--- Stream-subtraction finished at %s ' % str(datetime.now()))

    return DataStream(stream_a, headera)


def stackStreams(stream_a, stream_b, **kwargs):
    """
    stack the contents of two data stream:
    """
    pass


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
    #rcs = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-RCS\RCS-T7-2012-02-01_00-00-00_c.txt'))
    #print "Plotting raw data:"
    #rcs.pmplot(['x','y','z','f'])
    #print "Applying Outlier removal:"
    #rcs = rcs.routlier(keys=['x','y','z'])
    #rcs = rcs.remove_flagged(key='x')
    #rcs = rcs.remove_flagged(key='y')
    #rcs = rcs.remove_flagged(key='z')
    #rcs = rcs.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #rcs = rcs.smooth(['y'],window_len=21)
    #rcs.pmplot(['x','y','z','f'])
    #print "Header information RCS"
    #print rcs.header


    # Temperature measurements and corrections to time columns
    #usb = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\T-Logs\Schacht*'))
    #usb = usb.date_offset(-timedelta(hours=2)) # correcting times e.g. MET to UTC
    #usb = usb.filtered(filter_type='gauss',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30),respect_flags=True)
    #func = usb.interpol(['t1','t2','var1'])
    #usb.pmplot(['t1','t2','var1'],function=func)

    #rcs.clear_header()
    #print rcs.header

    # Variometer and Scalar Data
    # --------------------------

    # Storm analysis and fit functions
    #
    #st = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*'),starttime='2011-9-8',endtime='2011-9-14')
    #func = st.fit(['x','y','z'],fitfunc='spline',knotstep=0.1)
    #st = st.aic_calc('x',timerange=timedelta(hours=1))
    #st.pmplot(['x','y','z','var2'],function=func)
    #fmi = st.k_fmi(fitdegree=2)
    #fmi = st.k_fmi(fitfunc='spline',knotstep=0.4)
    #col = st._get_column('var2')
    #st = st._put_column(col,'y')
    #st = st.differentiate()
    #st.pmplot(['x','var2','dy','t2'],symbollist = ['-','-','-','z'])
    #
    # Seconds data and filtering
    #
    #st = pmRead(path_or_url=os.path.normpath('e:\\leon\\Observatory\\Messdaten\\Data-Magnetism\\gdas\\rawdata\\*'),starttime='2011-7-8',endtime='2011-7-9')
    #st.pmplot(['x','y','z'])
    #st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #st.pmplot(['x','y','z'])
    #st = st.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
    #st.pmplot(['x','y','z'])
    #
    # Smoothing, differentiating, integrating, interpolating data
    #
    #st = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\Lemi\*'),starttime='2010-7-14',endtime=datetime(2010,7,15))
    #st = st.smooth(['x'],window_len=21)
    #st = st.differentiate()
    #col = st._get_column('dx')
    #st = st._put_column(col,'x')
    #st = st.integrate(keys=['x'])
    #func = st.interpol(['x','y','z'])
    #
    # Intensity values, Outlier removal and flagging
    #
    #st = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\Proton\CO091231.CAP'))
    #st = st.routlier()
    #st = st.remove_flagged()
    #st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #st.pmplot(['f'])
    # --
    #st = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\proton\ZAGCPMAG-LOG_2010_07_18.txt'))
    #st = st.routlier()
    #st = st.flag_stream('f',3,"Moaing",datetime(2010,7,18,12,0,0,0),datetime(2010,7,18,13,0,0,0))
    #st = st.remove_flagged()
    #func = st.fit(['f'],fitfunc='spline',knotstep=0.05)
    #st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #st.pmplot(['f'],function=func)
    #st = st.get_gaps(gapvariable=True)
    #st.pmplot(['f','var2'])
    #
    # Merging data streams and filling of missing values
    #
    #st = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*')) #,starttime='2011-3-1')
    #newst = mergeStreams(st,usb,keys=['t1','var1'])
    #newst.pmplot(['x','y','z','t1','var1'],symbollist = ['-','-','-','-','-'],plottype='continuous')
    #st = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*')) #,starttime='2011-3-1')
    #print "Lenght before filling gaps"
    #print len(st)
    #st.pmplot(['x','y','z'])
    #st = st.get_gaps(gapvariable=True)
    #print "Lenght after filling gaps"
    #print len(st)
    #st.pmplot(['x','y','z','var2'])


    # Further functions -- incomplete
    # ---------------
    #st = st.func_subtract(func)

    #st.powerspectrum('x')

    #st = st.differentiate()
    #col = st._get_column('dx')
    #st = st._put_column(col,'x')
    #st = st.integrate(keys=['x'])
    #func = st.interpol(['x','y','z'])

    #st.pmplot(['x','y','z','var2'],function=func)
    #st.pmplot(['f'])

    # Absolute Values
    # ---------------
    #st = pmRead(path_or_url=os.path.normpath(r'e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*'),starttime='2011-9-1',endtime='2011-9-02')
    #st = pmRead(path_or_url=os.path.normpath(r'f:\Vario-Cobenzl\dIdD-System\*'),starttime='2011-8-20',endtime='2011-8-21')
    #bas = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_didd.txt'))
    #bas.pmplot(['x','y','z'])
    #func = bas.fit(['dx','dy','dz'],fitfunc='spline',knotstep=0.05)
    #bas.pmplot(['dx','dy','dz'],function=func)

    #st.pmplot(['x','y','z'])
    #st = st.baseline(bas,knotstep=0.05,plotbaseline=True)
    #st.pmplot(['x','y','z'])


    #stle = pmRead(path_or_url=os.path.normpath(r'f:\Vario-Cobenzl\dIdD-System\LEMI\*'),starttime='2011-8-20',endtime='2011-8-21')
    #basle = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
    #stle = stle.baseline(basle,knotstep=0.05,plotbaseline=True)
    #stle.pmplot(['x','y','z'])


    # Baseline Correction and RotationMatrix
    # ---------------
    # alpha and beta describe the rotation matrix (alpha is the horizontal angle (D) and beta the vertical)
    #didd = pmRead(path_or_url=os.path.normpath('g:\Vario-Cobenzl\dIdD-System\*'),starttime='2011-01-1',endtime='2011-12-31')
    #basdidd = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_didd.txt'))
    #lemi = pmRead(path_or_url=os.path.normpath('g:\Vario-Cobenzl\dIdD-System\LEMI\*'),starttime='2011-01-1',endtime='2011-12-31')
    #baslemi = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
    #lemi = lemi.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
    #didd = didd.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
    #lemi = lemi.rotation(alpha=3.3,beta=0.0)
    #didd = didd.baseline(basdidd,knotstep=0.05,plotbaseline=True)
    #lemi = lemi.baseline(baslemi,knotstep=0.05,plotbaseline=True)

    #didd.pmplot(['x','y','z'])
    #lemi.pmplot(['x','y','z'])
    #newst = subtractStreams(didd,lemi,keys=['x','y','z'],getmeans=True)
    # for some reason first and last points are not subtracted
    #newst = newst.trim(starttime=datetime(2011,1,1,02,00),endtime=datetime(2011,12,30,22,00))
    #newst = newst.routlier(keys=['x','y','z'])
    #print "Mean x: %f" % np.mean(newst._get_column('x'))
    #print "Mean y: %f" % np.mean(newst._get_column('y'))
    #print "Mean z: %f" % np.mean(newst._get_column('z'))

    #newst.pmplot(['x','y','z'])

    # DTU data
    # ---------------
    #dtust = pmRead(path_or_url=os.path.normpath(r'g:\VirtualBox\Ny mappe\GDH4_20091215.cdf'))
    #st = pmRead(path_or_url=os.path.normpath(r'g:\VirtualBox\Ny mappe\FHB*.sec'))
    #st = st.filtered(filter_type='gauss',filter_width=timedelta(minutes=1))
    #st = st.aic_calc('x',timerange=timedelta(hours=1))
    #col = st._get_column('var2')
    #st = st._put_column(col,'y')
    #st = st.differentiate()
    #st.pmplot(['x','var2','dy'],symbollist = ['-','-','-'])
    #dtust = dtust.filtered(filter_type='linear',filter_width=timedelta(hours=1))
    #dtust.pmplot(['x','y','z'])

    # Comparison of baseline calculated with and without correct orientation of sensor
    # ---------------
    #baslemi1 = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_lemi_alpha3.3.txt'))
    #baslemi2 = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_didd.txt'))
    #newst = subtractStreams(baslemi1,baslemi2,keys=['x','y','z'])
    #newst = newst.trim(starttime=datetime(2010,7,10,00,02),endtime=datetime(2011,10,1,23,58))
    #newst.pmplot(['x','y','z'])

    #testarray = np.array(baslemi1)
    #print testarray[1][2]
    #print testarray.ndim
    # Testing new funcs
    #lemi = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\lemi\*'),starttime='2010-7-17',endtime='2010-7-18')
    #baslemi = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\AbsAnalysis\absolutes_lemi.txt'))
    #lemi = lemi.rotation(alpha=3.30,beta=-4.5)
    #lemi = lemi.baseline(baslemi,knotstep=0.05,plotbaseline=True)
    #lemi.pmplot(['x','y','z'])


    #st = pmRead(path_or_url=os.path.normpath('e:\leon\Observatory\Messdaten\Data-Magnetism\didd\*'),starttime='2011-9-1',endtime='2011-9-30')
    #st.pmplot(['x','y','z'])
    #newst = subtractStreams(bas,st,keys=['x','y','z'])
    #newst.pmplot(['x','y','z'])

    #print len(st)
    #print "Current header information:"
    #print st.header

