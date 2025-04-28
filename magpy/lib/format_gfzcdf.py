"""
GFZ CDF input filter
Rewritten by Roman Leonhardt November 2024
- contains test and read methods
- based on cdflib
- supports python >= 3.5

"""
# Specify what methods are really needed
#from magpy.stream import *
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2

from magpy.stream import DataStream
from magpy.core.methods import testtime, extract_date_from_string
from matplotlib.dates import num2date, get_epoch
from datetime import datetime
import numpy as np
import cdflib
import logging
logger = logging.getLogger(__name__)

KEYLIST = DataStream().KEYLIST
NUMKEYLIST = DataStream().NUMKEYLIST
referencetime = np.datetime64(datetime(1980, 1, 1))


def isGFZCDF(filename):
    """
    Checks whether a file is Nasa CDF format.
    """
    try:
        temp = cdflib.CDF(filename)
    except:
        return False
    try:
        cdfformat = temp.globalattsget().get('DataFormat')
        if cdfformat:
            return False
    except:
        pass
    try:
        try:
            variables = temp.cdf_info().get('zVariables')  # cdflib < 1.0.0
        except:
            variables = temp.cdf_info().zVariables  # cdflib >= 1.0.0
        if not 'Epoch' in variables:
            if not 'time' in variables:
                return False
    except:
        return False

    logger.debug("format_magpy: Found GFZCDF file %s" % filename)
    return True


def readGFZCDF(filename, headonly=False, **kwargs):
    """
    Reading CDF format data - DTU type.
    """
    stream = DataStream()

    array = [[] for key in KEYLIST]
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    oldtype = kwargs.get('oldtype')
    debug = kwargs.get('debug')
    getfile = True

    # Some identification parameters used by Juergs
    jind = ["H0", "D0", "Z0", "F0", "HNscv", "HEscv", "Zscv", "Basetrig", "time", \
"HNvar", "HEvar", "Zvar", "T1", "T2", "timeppm", "timegps", \
"timefge", "Fsc", "HNflag", "HEflag", "Zflag", "Fscflag", "FscQP", \
"T1flag", "T2flag", "Timeerr", "Timeerrtrig"]

    # Check whether header information is already present
    headskip = False
    if stream.header == None:
        stream.header.clear()
    else:
        headskip = True

    theday = extract_date_from_string(filename)
    try:
        if starttime:
            if not theday[-1] >= datetime.date(testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(testtime(endtime)):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True
    logbaddata = False

    # Get format type:
    # Juergens DTU type is using different date format (MATLAB specific)
    if getfile:
        cdfdat = cdflib.CDF(filename)
        cdfformat = cdfdat.globalattsget().get('DataFormat')
        if not cdfformat:
            cdfformat = "GFZCDF"
            version = 1.0
        if debug:
            print(" - read gfzcdf: version done")

        if headskip:
            for att in cdfdat.globalattsget():
                value = cdfdat.globalattsget().get(att)
                if debug:
                    print(" - read gfzccdf: value", value)
                try:
                    if isinstance(list(value), list):
                        if len(value) == 1:
                            value = value[0]
                except:
                    pass
                if not att in ['DataAbsFunctionObject','DataBaseValues', 'DataFlagList','DataFunctionObject']:
                    stream.header[att] = value

        logger.info('readPYCDF: %s Format: %s ' % (filename, cdfformat))

        if debug:
            print(" - read gfzcdf: header done")
            print (cdfdat.cdf_info())
        basetime = np.datetime64(get_epoch())
        daycorr = np.float64((referencetime-basetime)/1000000./3600./24.)
        # Testing for CDFLIB version using try except: version attributes are also changed in different cdflib versions, so this is equally effective
        try:
            variables = cdfdat.cdf_info().get('zVariables')  # cdflib < 1.0.0
            cdfvers = 0.9
        except:
            variables = cdfdat.cdf_info().zVariables  # cdflib >= 1.0.0
            cdfvers = 1.0

        for key in variables:
            if debug:
                print(" - read gfzcdf: reading type of key", key)
            if key == 'time': # or key == 'timegps':  # other time columns are eventually of different length
                # Time column identified
                if not key == 'timegps':
                    ind = KEYLIST.index('time')
                else:
                    ind = KEYLIST.index('sectime')
                try:
                    if cdfvers<1.0:
                        ttdesc = cdfdat.varinq(key).get('Data_Type_Description')
                    else:
                        ttdesc = cdfdat.varinq(key).Data_Type_Description
                    if not ttdesc == 'CDF_TIME_TT2000' and debug:
                        print ("WARNING: Time column is not CDF_TIME_TT2000 (found {})".format(ttdesc))
                    col = cdfdat.varget(key)
                    col = np.asarray(col) + daycorr
                    if len(col) > 0:
                        array[ind] = np.asarray(num2date(col))
                except:
                    array[ind] = np.asarray([])
            elif key.endswith('scv'):  # solely found in juergs files - now define magpy header info
                    try:
                        # Please note: using only the last value to identify scalevalue
                        # - a change of scale values should leed to a different cdf archive !!
                        stream.header['DataScaleX'] = cdfdat.varget('HNscv')[-1]
                        stream.header['DataScaleY'] = cdfdat.varget('HEscv')[-1]
                        stream.header['DataScaleZ'] = cdfdat.varget('Zscv')[-1]
                        stream.header['DataSensorOrientation'] = 'hdz'
                    except:
                        # print "error while interpreting header"
                        pass
            elif key.endswith('0') or key == "Basetrig":
                # add some way to store these values
                if key == 'H0':
                    stream.header['DataCompensationX'] = -np.mean(cdfdat.varget(key))/1000.
                if key == 'D0':
                    stream.header['DataCompensationY'] = -np.mean(cdfdat.varget(key))/1000.
                if key == 'Z0':
                    stream.header['DataCompensationZ'] = -np.mean(cdfdat.varget(key))/1000.
                if key == 'F0':
                    stream.header['DataOffset'] = {'f' : np.mean(cdfdat.varget(key))}
                if key == 'Basetrig':
                    ind = 11
            elif key.endswith('var') or key.endswith('sc') or key=="T1" or key=="T2":
                if key == 'HNvar':
                    ind = 1
                    stream.header['col-x'] = key
                    stream.header['unit-col-x'] = 'nT'
                if key == 'HEvar':
                    ind = 2
                    stream.header['col-y'] = key
                    stream.header['unit-col-y'] = 'nT'
                if key == 'Zvar':
                    ind = 3
                    stream.header['col-z'] = key
                    stream.header['unit-col-z'] = 'nT'
                if key == 'Fsc':
                    ind = 4
                    stream.header['col-f'] = key
                    stream.header['unit-col-f'] = 'nT'
                if key == 'T1':
                    ind = 5
                    stream.header['col-t1'] = key
                    stream.header['unit-col-t1'] = 'degC'
                if key == 'T2':
                    ind = 6
                    stream.header['col-t2'] = key
                    stream.header['unit-col-t2'] = 'degC'
                if cdfvers < 1.0:
                    ttdesc = cdfdat.varinq(key).get('Data_Type_Description')
                else:
                    ttdesc = cdfdat.varinq(key).Data_Type_Description
                col = cdfdat.varget(key)
                array[ind] = col
            if debug:
                print(" - reading done")

        if cdfvers < 1.0:
            cdfdat.close()
        del cdfdat

    stream.ndarray = np.asarray(array, dtype=object)
    return stream
