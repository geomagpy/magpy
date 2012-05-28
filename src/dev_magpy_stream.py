#!/usr/bin/env python
"""
MagPy-General: Standard pymag package containing the following classes:
Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 23.02.2012)
"""

# ------------------------------------
# Part 1: Import routines for packages
# ------------------------------------
logpygen = '' # logging variable
netcdf = True # Export routines for netcdf
spacecdf = True # Export routines for Nasa cdf
mailingfunc = True # E-mail notifications
nasacdfdir = "c:\CDF Distribution\cdf33_1-dist\lib"

try:
    # Matpoltlib
    import matplotlib
    import matplotlib.pyplot as plt
    from matplotlib.dates import date2num, num2date
    import matplotlib.cm as cm
except ImportError:
    logpygen += "pymag-general: Critical Import failure: Python matplotlib required - please install to proceed\n"

try:
    #numpy/scipy
    import numpy as np
    import scipy as sp
    from scipy import interpolate
    from scipy import stats
    import math
except ImportError:
    logpygen += "pymag-general: Critical Import failure: Python numpy-scipy required - please install to proceed\n"

try:
    #standard packages
    import csv
    import pickle 
    import logging
    import sys, re
    import thread, time, string, os, shutil
    import copy
    import fnmatch
    import urllib2
    import warnings
    from glob import glob, iglob, has_magic
    from pylab import *
    from datetime import datetime, timedelta
    from StringIO import StringIO
except ImportError:
    logpygen += "pymag-general: Critical Import failure: Python numpy-scipy required - please install to proceed\n"

try:
    # NetCDF
    print "Loading Netcdf4 support ..."
    from netCDF4 import Dataset
except ImportError:
    netcdf = False
    logpygen += "pymag-general: Import failure: Netcdf not available\n"
    pass

try:
    # NasaCDF
    print "Loading Spacepy package cdf support ..."
    os.putenv("CDF_LIB", nasacdfdir)
    import spacepy.pycdf as cdf
except:
    logpygen += "pymag-general: Import failure: Nasa cdf not available\n"
    pass

try:
    # Import mailing functions - for e-mail notifications
    import smtplib
    from email.mime.text import MIMEText
    #from smtplib import SMTP_SSL as SMTP       # this invokes the secure SMTP protocol (port 465, uses SSL)
    from smtplib import SMTP                  # use this for standard SMTP protocol   (port 25, no encryption)
    from email.MIMEText import MIMEText
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


KEYLIST = ['time','x','y','z','f','t1','t2','var1','var2','var3','var4','var5','dx','dy','dz','df','str1','str2','str3','str4','flag','comment','typ','sectime']
KEYINITDICT = {'time':0,'x':float('nan'),'y':float('nan'),'z':float('nan'),'f':float('nan'),'t1':float('nan'),'t2':float('nan'),'var1':float('nan'),'var2':float('nan'),'var3':float('nan'),'var4':float('nan'),'var5':float('nan'),'dx':float('nan'),'dy':float('nan'),'dz':float('nan'),'df':float('nan'),'str1':'-','str2':'-','str3':'-','str4':'-','flag':'000000000-','comment':'-','typ':'xyzf','sectime':float('nan')}
FLAGKEYLIST = KEYLIST[:8]
# KEYLIST[:8] # only primary values with time
# KEYLIST[1:8] # only primary values without time

PYMAG_SUPPORTED_FORMATS = ['IAGA', 'DIDD', 'PMAG1', 'PMAG2', 'GDASA1', 'RMRCS', 'USBLOG', 'SERSIN', 'SERMUL', 'PYSTR',
                            'PYCDF', 'PYNC','DTU1','UNKOWN']

def readFormat(filename, format_type, headonly=False, **kwargs):
    empty = DataStream()
    if (format_type == "IAGA"):
        return readIAGA(filename, headonly, **kwargs)
    elif (format_type == "DIDD"):
        return readDIDD(filename, headonly, **kwargs)
    elif (format_type == "PMAG1"):
        return readPMAG1(filename, headonly, **kwargs)
    elif (format_type == "PMAG2"):
        return readPMAG2(filename, headonly, **kwargs)
    elif (format_type == "GDASA1"):
        return readGDASA1(filename, headonly, **kwargs)
    elif (format_type == "DTU1"):
        return readDTU1(filename, headonly, **kwargs)
    elif (format_type == "RMRCS"):
        return readRMRCS(filename, headonly, **kwargs)
    elif (format_type == "PYSTR"):
        return readPYSTR(filename, headonly, **kwargs)
    elif (format_type == "PYCDF"):
        return readPYCDF(filename, headonly, **kwargs)
    elif (format_type == "USBLOG"):
        return readUSBLOG(filename, headonly, **kwargs)
    else:
        return DataStream(empty,empty.header)


def writeFormat(datastream, filename, format_type, **kwargs):
    """
    calls the format specific write functions
    if the selceted dir is not existing, it is created
    """
    directory = os.path.dirname(filename)
    if not os.path.exists(directory):
        os.makedirs(os.path.normpath(directory))
    if (format_type == "IAGA"):
        return writeIAGA(datastream, filename, **kwargs)
    elif (format_type == "DIDD"):
        return writeDIDD(datastream, filename, **kwargs)
    elif (format_type == "PMAG1"):
        return writePMAG1(datastream, filename, **kwargs)
    elif (format_type == "PMAG2"):
        return writePMAG2(datastream, filename, **kwargs)
    elif (format_type == "DTU1"):
        return writeDTU1(datastream, filename, **kwargs)
    elif (format_type == "GDASA1"):
        return writeGDASA1(datastream, filename, **kwargs)
    elif (format_type == "RMRCS"):
        return writeRMRCS(datastream, filename, **kwargs)
    elif (format_type == "PYSTR"):
        return writePYSTR(datastream, filename, **kwargs)
    elif (format_type == "PYCDF"):
        return writePYCDF(datastream, filename, **kwargs)
    elif (format_type == "USBLOG"):
        return writeUSBLOG(datastream, filename, **kwargs)
    else:
        return "Writing not succesful - format not recognized"


def isFormat(filename, format_type):
    if (format_type == "IAGA"):
        if (isIAGA(filename)):
            return True
    elif (format_type == "DIDD"):
        if (isDIDD(filename)):
            return True
    elif (format_type == "PMAG1"):
        if (isPMAG1(filename)):
            return True
    elif (format_type == "PMAG2"):
        if (isPMAG2(filename)):
            return True
    elif (format_type == "GDASA1"): # Data from the Conrad Observatory GDAS System
        if (isGDASA1(filename)):
            return True
    elif (format_type == "DTU1"): # ASCII Data from the DTU's FGE systems
        if (isDTU1(filename)):
            return True
    elif (format_type == "PYSTR"):
        if (isPYSTR(filename)):
            return True
    elif (format_type == "PYCDF"):
        if (isPYCDF(filename)):
            return True
    elif (format_type == "RMRCS"): # Data from the Conrad Observatory RCS System
        if (isRMRCS(filename)):
            return True
    elif (format_type == "USBLOG"):
        if (isUSBLOG(filename)):
            return True
    else:
        return False

# -------------------
#  Test file formats
# -------------------

def isIAGA(filename):
    """
    Checks whether a file is ASCII IAGA 2002 format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith(' Format'):
        return False
    if not 'IAGA-2002' in temp:
        return False
    return True

def isDIDD(filename):
    """
    Checks whether a file is ASCII DIDD (Tihany) format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith('hh mm'):
        return False
    if not 'F' in temp:
        return False
    return True


def isRMRCS(filename):
    """
    Checks whether a file is ASCII RCS format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith('# RCS'):
        return False
    return True


def isGDASA1(filename):
    """
    Checks whether a file is ASCII GDAS (type1) format used by Romans modification of Chris Turbits code.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith('  Time'):
        return False
    if not 'T' in temp:
        return False
    return True


def isDTU1(filename):
    """
    Checks whether a file is ASCII DTU (type1) format used within the DTU's FGE network
    Characteristic features are:    
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith('FILENAME:    '):
        elem = temp.split()
        if len(elem) == 6:
            try:
                testtime = datetime.strptime(elem[0],"%H:%M:%S")
            except:
                return False
        else:
            return False
    return True


def isPMAG1(filename):
    """
    Checks whether a file is ASCII PMAG format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    tmp = temp.split()
    if len(tmp) != 3:
        return False
    try:
        testdate = datetime.strptime(tmp[0],"%H:%M:%S")
    except:
        return False
    return True

def isPMAG2(filename):
    """
    Checks whether a file is ASCII PMAG2 format.
    Leading blank lines are likely
    """
    try:
        fh = open(filename, 'rt')
        temp = fh.readline()
        if temp == "":
            temp = fh.readline()
        if temp == "":
            temp = fh.readline()
    except:
        return False
    tmp = temp.split()
    if len(tmp) != 2:
        return False
    try:
        testdate = datetime.strptime(tmp[1],"%m%d%H%M%S")
    except:
        return False
    return True

def isPYCDF(filename):
    """
    Checks whether a file is Nasa CDF format.
    """
    try:
        temp = cdf.CDF(filename)
    except:
        return False
    try:
        lst =[key for key in temp if key == 'time' or key == 'Epoch']
        if lst == []:
            return False
    except:
        return False
    return True


def isPYSTR(filename):
    """
    Checks whether a file is ASCII PyStr format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith(' # MagPy - ASCII'):
        return False
    return True

def isUSBLOG(filename):
    """
    Checks whether a file is ASCII USB-Logger format.
    Supports temperture and humidity logger
    Extend that code for CO logger as well
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    sp = temp.split(',')
    if not len(sp) == 6:
        return False
    if not sp[1] == 'Time':
        return False
    return True

# -------------------
#  Read file formats
# -------------------


def readDIDD(filename, headonly=False, **kwargs):
    """
    Reading DIDD format data.
    Looks like:
    hh mm        X        Y        Z        F 
    00 00  20826.8   1206.1  43778.3  48494.8
    00 01  20833.3   1202.2  43779.3  48498.5
    00 02  20832.2   1198.7  43779.9  48498.4
    00 03  20832.6   1196.2  43779.6  48498.3
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None
    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    try:
        day = datetime.strftime(datetime.strptime(daystring[0], "%b%d%y"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring[0])
        return []
    # Select only files within eventually defined time range
    if starttime:
        if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False
    if endtime:
        if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line.startswith('hh mm'):
                # data header
                colsstr = line.lower().split()
                for it, elem in enumerate(colsstr):
                    if it>1: # dont take hh and mm
                        colname = "col-%s" % KEYLIST[it-1]
                        headers[colname] = elem            
                        headers['unit-' + colname] = 'nT'
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                elem = line.split()
                if (float(elem[5])) < 999990:
                    row.time=date2num(datetime.strptime(day+'T'+elem[0]+':'+elem[1],"%Y-%m-%dT%H:%M"))
                    xval = float(elem[2])
                    yval = float(elem[3])
                    zval = float(elem[4])
                    if (headers['col-x']=='x'):
                        row.x = xval
                        row.y = yval
                        row.z = zval
                    elif (headers['col-x']=='h'):
                        row.x, row.y, row.z = hdz2xyz(xval,yval,zval)
                    elif (headers['col-x']=='i'):
                        row.x, row.y, row.z = idf2xyz(xval,yval,zval)
                    else:
                        raise ValueError
                    row.f = float(elem[5])
                    stream.add(row)         
    else:
        headers = stream.header
        stream =[]

    fh.close()

    return DataStream(stream, headers)    


def readDTU1(filename, headonly=False, **kwargs):
    """
    Reading DTU1 format data.
    Looks like:
    FILENAME:    GDH4_20091215.sec
    INST. TYPE:  Primary magnetometer
    INSTRUMENT:  FGE S0120 E0192
    FILTER:      Electronic lowpass
    ADC:         ICP 7017 vers. B2.3
    SOFTWARE:    FG_ComData vers. 3.04
    CHANNELS:     6 Time,x,y,z,T1,T2
    TIME 1 hh:mm:ss PC clock, UT, timeserver
    x  400 nT/V variation horizontal magnetic north in nT
    y  400 nT/V variation horizontal magnetic east in nT
    z  400 nT/V variation vertical in nT
    T1 0 Kelvin/v no temp sensor on pendulum
    T2 320 Kelvin/V electronic temp in Kelvin, sensor: AD592
    DATA:
    00:00:01   124.04   134.08   -17.68   0.00   291.90   
    00:00:02   124.00   134.00   -17.68   0.00   291.90   
    00:00:03   124.08   134.00   -17.64   0.00   291.90   
    """
    fh = open(filename, 'rt')
    # read file and split text into channels
    data = []
    getfile = True
    key = None
    stream = DataStream()

    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    # get day from filename (platform independent)
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    daystring = daystring[0].split('_')
    print daystring[1]

    try:
        day = datetime.strftime(datetime.strptime(daystring[1] , "%Y%m%d"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring[0])
        return []
    # Select only files within eventually defined time range
    if starttime:
        if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False
    if endtime:
        if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False

    if getfile:
        for line in fh:
            elem = line.split()
            if line.isspace():
                # blank line
                pass
            elif line.startswith('FILENAME:'):
                pass
            elif line.startswith('INST. TYPE:'):
                tmp =  line.split(':')[1] 
                headers['InstrumentType'] = tmp.lstrip()
            elif line.startswith('INSTRUMENT:'):
                tmp =  line.split(':')[1] 
                headers['Instrument'] = tmp.lstrip()
            elif line.startswith('FILTER:'):
                tmp =  line.split(':')[1] 
                headers['Filter'] = tmp.lstrip()
            elif line.startswith('ADC:'):
                tmp =  line.split(':')[1] 
                headers['ADC'] = tmp.lstrip()
            elif line.startswith('SOFTWARE:'):
                tmp =  line.split(':')[1] 
                headers['Software'] = tmp.lstrip()
            elif line.startswith('CHANNELS:'):
                tmp =  line.split(':')[1] 
                headers['Channels'] = tmp.lstrip()
            elif line.startswith('TIME'):
                pass
            elif line.startswith('x'):
                pass
            elif line.startswith('y'):
                pass
            elif line.startswith('z'):
                pass
            elif line.startswith('T1'):
                pass
            elif line.startswith('T2'):
                pass
            elif line.startswith('DATA:'):
                pass
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                try:
                    row.time=date2num(datetime.strptime(day+'T'+elem[0],"%Y-%m-%dT%H:%M:%S"))
                    try:
                        row.x = float(elem[1])
                    except:
                        row.x = float('nan')
                    try:
                        row.y = float(elem[2])
                    except:
                        row.y = float('nan')
                    try:
                        row.z = float(elem[3])
                    except:
                        row.z = float('nan')
                    try:
                        row.t1 = float(elem[4])
                    except:
                        row.t1 = float('nan')
                    try:
                        row.t2 = float(elem[5])
                    except:
                        row.t2 = float('nan')
                except:
                    #raise ValueError, "Wrong date format in %s" % filename
                    pass
                stream.add(row)         

        fh.close()
    else:
        headers = stream.header
        stream =[]

    return DataStream(stream, headers)    


def readGDASA1(filename, headonly=False, **kwargs):
    """
    Reading GDASA1 format data.
    Looks like:
      Time              H       D       Z      T       F
    01-07-2011-00:00:00    -153     -40     183      67   99999
    01-07-2011-00:00:01    -155     -40     184      67   99999
    01-07-2011-00:00:02    -155     -41     185      67   99999
    01-07-2011-00:00:03    -156     -40     184      67   99999
    01-07-2011-00:00:04    -156     -40     184      67   99999
    01-07-2011-00:00:05    -156     -39     183      67   99999
    01-07-2011-00:00:06    -156     -40     184      67   99999
    01-07-2011-00:00:07    -155     -40     184      67   99999
    """
    fh = open(filename, 'rt')
    # read file and split text into channels
    data = []
    getfile = True
    key = None
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    # get day from filename (platform independent)
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    try:
        day = datetime.strftime(datetime.strptime(daystring[0].strip('gdas') , "%Y%m%d"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring[0])
        return []
    # Select only files within eventually defined time range
    if starttime:
        if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False
    if endtime:
        if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                pass
            elif line.startswith('  Time'):
                # data header
                colsstr = line.lower().split()
                for it, elem in enumerate(colsstr):
                    if elem == 'time':
                        headers['epoch'] = elem
                    else: # check for headers and replace hd with xy
                        if elem == 'h':
                            headers['InstrumentOrientation'] = 'hdz'
                            elem = 'x'
                        if elem == 'd':
                            elem = 'y'
                        colname = 'col-%s' % elem
                        headers[colname] = elem
                        if not elem == 't':
                            headers['unit-' + colname] = 'nT' # actually is 10*nT but that is corrected during data read
                        else:
                            headers['unit-' + colname] = 'C'                        
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                elem = line.split()
                try:
                    row.time=date2num(datetime.strptime(elem[0],"%d-%m-%Y-%H:%M:%S"))
                except:
                    try:
                        row.time = date2num(elem[0].isoformat())
                    except:
                        raise ValueError, "Wrong date format in %s" % filename
                row.x = float(elem[1])/10.0
                row.y = float(elem[2])/10.0
                row.z = float(elem[3])/10.0
                row.t1 = float(elem[4])/10.0
                if (float(elem[5]) != 99999):
                    row.f = float(elem[5])/10.0
                stream.add(row)         

        fh.close()
    else:
        headers = stream.header
        stream =[]

    return DataStream(stream, headers)    


def readRMRCS(filename, headonly=False, **kwargs):
    """
    Reading RMRCS format data. (Richard Mandl's RCS extraction)
    # RCS Fieldpoint T7
    # Conrad Observatorium, www.zamg.ac.at
    # 2012-02-01 00:00:00
    # 
    # 12="ZAGTFPT7	M6	I,cFP-AI-110	CH00	AP23	Niederschlagsmesser	--	Unwetter, S	AR0-20H0.1	mm	y=500x+0	AI"
    # 13="ZAGTFPT7	M6	I,cFP-AI-110	CH01	JC	Schneepegelsensor	OK	Mastverwehung, S	AR0-200H0	cm	y=31250x+0	AI"
    # 14="ZAGTFPT7	M6	I,cFP-AI-110	CH02	430A_T	Wetterhuette - Lufttemperatur	-	-, B	AR-35-45H0	C	y=4000x-35	AI"
    # 15="ZAGTFPT7	M6	I,cFP-AI-110	CH03	430A_F	Wetterhuette - Luftfeuchte	-	-, B	AR0-100H0	%	y=5000x+0	AI"
    # 
    1328054403.99	20120201 000004	49.276E-6	49.826E+0	-11.665E+0	78.356E+0
    1328054407.99	20120201 000008	79.480E-6	49.823E+0	-11.677E+0	78.364E+0
    1328054411.99	20120201 000012	68.555E-6	49.828E+0	-11.688E+0	78.389E+0
    """
    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    measurement = []
    unit = []
    i = 0
    key = None
    print "Reading data ..."
    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif line.startswith('#'):
            # data header
            colsstr = line.split(',')
            if (len(colsstr) == 3):
                # select the lines with three komma separaeted parts -> they describe the data
                meastype = colsstr[1].split()
                unittype = colsstr[2].split()
                measurement.append(meastype[2])
                unit.append(unittype[2])
                headers['col-'+KEYLIST[i+1]] = unicode(measurement[i],errors='ignore')
                headers['unit-col-'+KEYLIST[i+1]] = unicode(unit[i],errors='ignore')
                i=i+1
        elif headonly:
            # skip data for option headonly
            continue
        else:
            # data entry - may be written in multiple columns
            # row beinhaltet die Werte eine Zeile
            row=[]
            # Verwende das letzte Zeichen von "line" nicht, d.h. line[:-1],
            # da darin der Zeilenumbruch "\n" steht
            for val in string.split(line[:-1]):
                # nur nicht-leere Spalten hinzufuegen
                if string.strip(val)!="":
                    row.append(string.strip(val))
            # Baue zweidimensionales Array auf       
            data.append(row)

    fh.close()

    print " Got %d ASCII lines" % len(data)
    print "Extracting data..."

    for elem in data:
        # Time conv:
        row = LineStruct()
        try:
            row.time = date2num(elem[1].isoformat())
            add = 2
        except:
            try:
                row.time = date2num(datetime.strptime(elem[1]+'T'+elem[2],"%Y%m%dT%H%M%S"))
                add = 3
            except:
                raise ValueError, "Can't read date format in RCS file"
        for i in range(len(unit)):
            exec('row.'+KEYLIST[i+1]+' = float(elem['+str(i+add)+'])')
        stream.add(row)         

    print "Extraction finished. Starting processing ..."
    return DataStream(stream, headers)    

def readPMAG1(filename, headonly=False, **kwargs):
    """
    Reading PMAG1 format data.
    Looks like:
    00:00:07	48488.3	0713220022
    00:00:17	48487.7	0713220032
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    
    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None
    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    daystring = daystring[0].strip("ZAGCPMAG-LOG_")
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%Y_%m_%d"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring)
        return []
    print day
    # Select only files within eventually defined time range
    if starttime:
        if not datetime.strptime(day,'%Y-%m-%d') >= stream._testtime(starttime):
            getfile = False
    if endtime:
        if not datetime.strptime(day,'%Y-%m-%d') <= stream._testtime(endtime):
            getfile = False

    for line in fh:
        if line.isspace():
            # blank line
            continue
        elif len(line.split())!=3:
            # data header
            pass
        elif headonly:
            # skip data for option headonly
            continue
        else:
            # data entry - may be written in multiple columns
            # row beinhaltet die Werte eine Zeile
            row=[]
            # Verwende das letzte Zeichen von "line" nicht, d.h. line[:-1],
            # da darin der Zeilenumbruch "\n" steht
            for val in string.split(line[:-1]):
                # nur nicht-leere Spalten hinzufuegen
                if string.strip(val)!="":
                    row.append(string.strip(val))
            # Baue zweidimensionales Array auf       
            data.append(row)

    fh.close()

    # The final values for checking non-single day records
    data_len = len(data)
    finhour = datetime.strftime(datetime.strptime(data[-1][0],"%H:%M:%S"),"%H")
    if data_len > 0:
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    for idx, elem in enumerate(data):
        # Time conv:
        row = LineStruct()
        try:
            strtime = datetime.strptime(day+'T'+elem[0],"%Y-%m-%dT%H:%M:%S")
            hour = datetime.strftime(strtime,"%H")
            subday = 0
            if (int(finhour)-int(hour) == 0) and (data_len-idx > data_len/2):
                subday = -1
            row.time=date2num(strtime + timedelta(days=subday))
            try:
                strval = elem[1].replace(',','.')
            except:
                strval = elem[1]
                pass
            row.f = float(strval)
            row.sectime=date2num(datetime.strptime(day.split("-")[0]+elem[2],"%Y%m%d%H%M%S"))
            stream.add(row)
        except:
            logging.warning("Error in input data: %s - skipping bad value" % daystring)
            pass

    return DataStream(stream, headers)  


def readPMAG2(filename, headonly=False, **kwargs):
    """
    Reading PMAG2 format data.
    Looks like:
    48488.3	0713220022
    48487.7	0713220032
    """
    
    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None
    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    daystring = daystring[0].strip("CO")
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%y%m%d"),"%Y-%m-%d")
    except:
        #raise ValueError, "Dateformat in Filename missing"
        logging.warning("Wrong dateformat in Filename %s" % daystring)
        return []
    print day
    firsthit = True
    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif len(line.split())!=2:
            # data header
            pass
        elif headonly:
            # skip data for option headonly
            continue
        else:
            elem = line.split()
            row = LineStruct()
            if firsthit:
                startmonth = datetime.strftime(datetime.strptime(elem[1],"%m%d%H%M%S"),"%m")
                firsthit = False
            try:
                # ToDo: case if starting in 2009 and entering 2010....
                strtime = datetime.strptime(day.split("-")[0]+elem[1],"%Y%m%d%H%M%S")
                month = datetime.strftime(strtime,"%m")
                addyear = 0
                if int(month)-int(startmonth) < 0:
                    addyear = 1
                    strtime = datetime.strptime(str(int(day.split("-")[0])+addyear)+elem[1],"%Y%m%d%H%M%S")
                row.time=date2num(strtime)
                try:
                    strval = elem[0].replace(',','.')
                except:
                    strval = elem[0]
                    pass
                row.f = float(strval)/10
                stream.add(row)
            except:
                logging.warning("Error in input data: %s - skipping bad value" % daystring)
                pass

    if len(stream) > 0:
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    fh.close()

    return DataStream(stream, headers)    


def readPYSTR(filename, headonly=False, **kwargs):
    """
    Reading ASCII PyMagStructure format data.
    """
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header
    qFile= file( filename, "rb" )
    csvReader= csv.reader( qFile )
    for elem in csvReader:
        if elem[0]=='#':
            # blank line
            pass
        elif elem[0]==' #':
            # attributes
            pass
        elif elem[0]=='Epoch[]':
            for i in range(len(elem)):
                headval = elem[i].split('[')                
                colval = headval[0]
                unitval = headval[1].strip(']')
                exec('headers["col-'+KEYLIST[i]+'"] = colval')
                exec('headers["unit-col-'+KEYLIST[i]+'"] = unitval')
        elif headonly:
            # skip data for option headonly
            continue
        else:
            try:
                row = LineStruct()
                try:
                    row.time = date2num(datetime.strptime(elem[0],"%Y-%m-%d-%H:%M:%S.%f"))
                except:
                    try:
                        row.time = date2num(datetime.strptime(elem[0],"%Y-%m-%dT%H:%M:%S.%f"))
                    except:
                        raise ValueError, "Wrong date format in file %s" % filename
                for idx, key in enumerate(KEYLIST):
                    if not key == 'time':
                        try:
                            exec('row.'+key+' =  float(elem[idx])')
                        except:
                            exec('row.'+key+' =  elem[idx]')
                stream.add(row)
            except ValueError:
                pass
    qFile.close()

    return DataStream(stream, headers)    


def readPYCDF(filename, headonly=False, **kwargs):
    """
    Reading CDF format data - DTU type.
    """
    stream = DataStream()

    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header

    logging.info('--- Start reading CDF at %s ' % str(datetime.now()))

    cdf_file = cdf.CDF(filename)

    # Get format type:
    # DTU type is using different date format (MATLAB specific)
    # MagPy type is using datetime objects
    try:
        cdfformat = cdf_file.attrs['DataFormat']
    except:
        logging.info("No format specification in CDF - passing")
        cdfformat = 'Unknown'
        pass

    logging.info('--- File: %s Format: %s ' % (filename, cdfformat))

    for key in cdf_file:
        # first get time or epoch column
        lst = cdf_file[key]
        if key == 'time' or key == 'Epoch':
            ti = lst[...]
            for elem in ti:
                row = LineStruct()
                if str(cdfformat) == 'MagPyCDF':
                    row.time = date2num(elem)                  
                else:
                    row.time = elem+730485.0 # DTU MATLAB time
                stream.add(row)
        elif key == 'HNvar' or key == 'x':
            x = lst[...]
            stream._put_column(x,'x')
        elif key == 'HEvar' or key == 'y':
            y = lst[...]
            stream._put_column(y,'y')
        elif key == 'Zvar' or key == 'z':
            z = lst[...]
            stream._put_column(z,'z')
        elif key == 'Fsc' or key == 'f':
            f = lst[...]
            stream._put_column(f,'f')
        else:
            if key.lower() in KEYLIST:
                col = lst[...]
                stream._put_column(col,key.lower())

    cdf_file.close()

    logging.info('--- Finished reading CDF at %s ' % str(datetime.now()))

    return DataStream(stream, headers)    


def readUSBLOG(filename, headonly=False, **kwargs):
    """
    Reading ASCII USB DataLogger Structure format data.

    Vario,Time,Celsius(deg C),Humidity(%rh),dew point(deg C),Serial Number
    3,29/07/2010 12:58:03,21.0,88.5,19.0
    4,29/07/2010 13:28:03,21.0,88.5,19.0
    5,29/07/2010 13:58:03,21.0,88.5,19.0
    6,29/07/2010 14:28:03,21.0,88.5,19.0
    7,29/07/2010 14:58:03,21.0,89.0,19.1
    8,29/07/2010 15:28:03,21.0,89.0,19.1
    """
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header
    qFile= file( filename, "rb" )
    csvReader= csv.reader( qFile )
    for elem in csvReader:
        row = LineStruct()
        try:
            if elem[1] == 'Time':
                el2 = elem[2].split('(')
                test = el2[1]
                headers['unit-col-t1'] = unicode(el2[1].strip(')'),errors='ignore')
                headers['col-t1'] = el2[0]
                el3 = elem[3].split('(')
                headers['unit-col-var1'] = unicode(el3[1].strip(')'),errors='ignore')
                headers['col-var1'] = el3[0]
                el4 = elem[4].split('(')
                headers['unit-col-t2'] = unicode(el4[1].strip(')'),errors='ignore')
                headers['col-t2'] = el4[0]
            elif len(elem) == 6 and not elem[1] == 'Time':
                headers['InstrumentSerialNum'] = elem[5]
            else:
                row.time = date2num(datetime.strptime(elem[1],"%d/%m/%Y %H:%M:%S"))
                row.t1 = float(elem[2])
                row.var1 = float(elem[3])
                row.t2 = float(elem[4])
                stream.add(row)
        except ValueError:
            pass
    qFile.close()

    return DataStream(stream, headers)    


def readIAGA(filename, headonly=False, **kwargs):
    """
    Reading IAGA2002 format data.
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None

    

    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    tmpdaystring = splitpath[1].split('.')[0]
    daystring = re.findall(r'\d+',tmpdaystring)[0]
    if len(daystring) >  8:
        daystring = daystring[:8]
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%Y%m%d"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring)
        return []
    # Select only files within eventually defined time range
    if starttime:
        if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False
    if endtime:
        if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line.startswith(' '):
                # data info
                infoline = line.lstrip().strip('|')
                key = infoline[:23].rstrip()
                headers[key] = infoline[23:].rstrip()
            elif line.startswith('DATE'):
                # data header
                colsstr = line.lower().split()
                for it, elem in enumerate(colsstr):
                    if it > 2:
                        colname = "col-%s" % elem[-1]
                        headers[colname] = elem[-1]
                    else:
                        colname = "col-%s" % elem
                        headers[colname] = elem
            elif headonly:
                # skip data for option headonly
                continue
            elif line.startswith('%'):
                pass
            else:
                # data entry - may be written in multiple columns
                # row beinhaltet die Werte eine Zeile
                row=[]
                # Verwende das letzte Zeichen von "line" nicht, d.h. line[:-1],
                # da darin der Zeilenumbruch "\n" steht
                for val in string.split(line[:-1]):
                    # nur nicht-leere Spalten hinzufuegen
                    if string.strip(val)!="":
                        row.append(string.strip(val))
                # Baue zweidimensionales Array auf       
                data.append(row)

    fh.close()

    for elem in data:
        # Time conv:
        row = LineStruct()
        row.time=date2num(datetime.strptime(elem[0]+'-'+elem[1],"%Y-%m-%d-%H:%M:%S.%f"))
        xval = float(elem[3])
        yval = float(elem[4])
        zval = float(elem[5])
        if (headers['col-x']=='x'):
            row.x = xval
            row.y = yval
            row.z = zval
        elif (headers['col-h']=='h'):
            row.x, row.y, row.z = hdz2xyz(xval,yval,zval)
        elif (headers['col-i']=='i'):
            row.x, row.y, row.z = idf2xyz(xval,yval,zval)
        else:
            raise ValueError
        if not float(elem[6]) == 88888:
            if headers['col-f']=='f':
                row.f = float(elem[6])
            elif headers['col-g']=='g':
                row.f = np.sqrt(row.x**2+row.y**2+row.z**2) + float(elem[6])
            else:
                raise ValueError
        stream.add(row)

    """
    Speed optimization:
    Change the whole thing to column operations


    col = ColStruct(len(data))
    for idx, elem in enumerate(data):
        # Time conv:
        xxx = col.time
        col.time[idx] = (date2num(datetime.strptime(elem[0]+'-'+elem[1],"%Y-%m-%d-%H:%M:%S.%f")))
        xval = float(elem[3])
        yval = float(elem[4])
        zval = float(elem[5])
        if (headers['col-x']=='x'):
            col.x[idx] = xval
            col.y[idx] = yval
            col.z[idx] = zval
        elif (headers['col-h']=='h'):
            col.x[idx], col.y[idx], col.z[idx] = hdz2xyz(xval,yval,zval)
        elif (headers['col-i']=='i'):
            col.x[idx], col.y[idx], col.z[idx] = idf2xyz(xval,yval,zval)
        else:
            raise ValueError
        if not float(elem[6]) == 88888:
            if headers['col-f']=='f':
                col.f[idx] = float(elem[6])
            elif headers['col-g']=='g':
                col.f[idx] = np.sqrt(row.x**2+row.y**2+row.z**2) + float(elem[6])
            else:
                raise ValueError

    arraystream = np.asarray(col)
    try:
        print len(col.time)
        print "got it"
    except:
        pass
    stream = col
    """

    return DataStream(stream, headers)    


# -------------------
#  Write file formats
# -------------------

def writePYSTR(datastream, filename, **kwargs):
    """
    Function to write structural ASCII data 
    """

    mode = kwargs.get('mode')

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = pmRead(path_or_url=filename)
            datastream = mergeStreams(exst,datastream,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = pmRead(path_or_url=filename)
            datastream = mergeStreams(datastream,exst,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'append':
            myFile= open( filename, "ab" )
        else:
            myFile= open( filename, "wb" )
    else:
        myFile= open( filename, "wb" )
    wtr= csv.writer( myFile )
    headdict = datastream.header
    head, line = [],[]
    if not mode == 'append':
        wtr.writerow( [' # MagPy - ASCII'] )
        for key in headdict:
            if not key.find('col') >= 0:
                line = [' # ' + key +':  ' + headdict[key]]
                wtr.writerow( line )
        wtr.writerow( ['# head:'] )
        for key in KEYLIST:
            title = headdict.get('col-'+key,'-') + '[' + headdict.get('unit-col-'+key,'') + ']'
            head.append(title)
        wtr.writerow( head )
        wtr.writerow( ['# data:'] )
    for elem in datastream:
        row = []
        for key in KEYLIST:
            if key.find('time') >= 0:
                try:
                    row.append( datetime.strftime(num2date(eval('elem.'+key)).replace(tzinfo=None), "%Y-%m-%dT%H:%M:%S.%f") )
                except:
                    row.append( float('nan') )
                    pass
            else:
                row.append(eval('elem.'+key))
        wtr.writerow( row )
    myFile.close()


def writeDIDD(datastream, filename, **kwargs):
    """
    Looks like:
    hh mm        X        Y        Z        F 
    00 00  20826.8   1206.1  43778.3  48494.8
    00 01  20833.3   1202.2  43779.3  48498.5
    """
    if (datastream[-1].time - datastream[0].time) > 1:
        return "Writing DIDD format requires daily coverage - choose"

    headdict = datastream.header

    myFile= open( filename, 'wb' )
    wtr= csv.writer( myFile )
    headline = 'hh mm        '+headdict.get('col-x').upper()+'        '+headdict.get('col-y').upper()+'        '+headdict.get('col-z').upper()+'        '+headdict.get('col-f').upper()
    wtr.writerow( [headline] )
    for elem in datastream:
        time = datetime.strftime(num2date(elem.time).replace(tzinfo=None), "%H %M")
        line = '%s %7.1f %7.1f %7.1f %7.1f' % (time, elem.x, elem.x, elem.z, elem.f)
        wtr.writerow( [line] )
    myFile.close()


def writePYCDF(datastream, filename, **kwargs):
    # check for nan and - columns
    #for key in KEYLIST:
    #    title = headdict.get('col-'+key,'-') + '[' + headdict.get('unit col-'+key,'') + ']'
    #    head.append(title)

    mode = kwargs.get('mode')

    if os.path.isfile(filename+'.cdf'):
        if mode == 'skip': # skip existing inputs
            exst = pmRead(path_or_url=filename+'.cdf')
            datastream = mergeStreams(exst,datastream,extend=True)
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
        elif mode == 'replace': # replace existing inputs
            exst = pmRead(path_or_url=filename+'.cdf')
            datastream = mergeStreams(datastream,exst,extend=True)
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
        elif mode == 'append':
            mycdf = cdf.CDF(filename, filename) # append????
        else: # overwrite mode
            print " got here"
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
    else:
        mycdf = cdf.CDF(filename, '')

    headdict = datastream.header
    head, line = [],[]
    mycdf.attrs['DataFormat'] = 'MagPyCDF'
    if not mode == 'append':
        for key in headdict:
            if not key.find('col') >= 0:
                mycdf.attrs[key] = headdict[key]

    for key in KEYLIST:            
        col = datastream._get_column(key)
        if key == 'time':
            key = 'Epoch'
            mycdf[key] = np.asarray([num2date(elem).replace(tzinfo=None) for elem in col])
        elif len(col) > 0:
            mycdf[key] = col
        for keydic in headdict:
            if keydic.find('unit-col-'+key) > 0:
                mycdf[key].attrs['units'] = headdict.get('unit-col-'+key,'')
    mycdf.close()

 
# -------------------
#  Classes and functions of the stream file
# -------------------

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
        function to send logging lists by mail to the observer
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

      
class ColStruct(object):
    def __init__(self,length, time=float('nan'), x=float('nan'), y=float('nan'), z=float('nan'), f=float('nan'), dx=float('nan'), dy=float('nan'), dz=float('nan'), df=float('nan'), t1=float('nan'), t2=float('nan'), var1=float('nan'), var2=float('nan'), var3=float('nan'), var4=float('nan'), var5=float('nan'), str1='-', str2='-', str3='-', str4='-', flag='000000000-', comment='-', typ="xyzf", sectime=float('nan')):
        """
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

    
class LineStruct(object):
    def __init__(self, time=float('nan'), x=float('nan'), y=float('nan'), z=float('nan'), f=float('nan'), dx=float('nan'), dy=float('nan'), dz=float('nan'), df=float('nan'), t1=float('nan'), t2=float('nan'), var1=float('nan'), var2=float('nan'), var3=float('nan'), var4=float('nan'), var5=float('nan'), str1='-', str2='-', str3='-', str4='-', flag='000000000-', comment='-', typ="xyzf", sectime=float('nan')):
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



    #def __getitem__(self, key):
        #return dict.__getitem__(self,key)
    #    return self.key


# ##################
# read/write functions
# ##################
def pmRead(path_or_url=None, dataformat=None, headonly=False, **kwargs):
    """
    The read functions trys to open the selected dats
    dataformat: none - autodetection
    supported formats - use extra packages: generalcdf, cdf, netcdf, MagStructTXT,
    IAGA02, DIDD, PMAG, TIMEVAL
    optional arguments are starttime, endtime and dateformat of file given in kwargs
    """
    messagecont = ""

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
        suffix = os.path.basename(pathname_or_url).partition('.')[2] or '.tmp'
        #fh = NamedTemporaryFile(suffix=suffix)
        #fh.write(urllib2.urlopen(path_or_url).read())
        #fh.close()
        #st.extend(_pmRead(fh.name, format, headonly, **kwargs).traces)
        #os.remove(fh.name)
    else:
        # some file name
        pathname = path_or_url
        for file in iglob(pathname):
            stp = _pmRead(file, dataformat, headonly, **kwargs)
            st.extend(stp.container,stp.header)
        if len(st) == 0:
            # try to give more specific information why the stream is empty
            if has_magic(pathname) and not glob(pathname):
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
def _pmRead(filename, dataformat=None, headonly=False, **kwargs):
    """
    Reads a single file into a ObsPy Stream object.
    """    
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
    logging.info('Appending data - dataformat: %s' % format_type)
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
    """
    keys = kwargs.get('keys')
    extend = kwargs.get('extend')
    offset = kwargs.get('offset')
    if not keys:
        keys = KEYLIST[1:15]
    if not offset:
        offset = 0
   
    logging.info('--- Start mergings at %s ' % str(datetime.now()))

    headera = stream_a.header
    headerb = stream_b.header


    # take stream_b data and find nearest element in time from stream_a
    timea = stream_a._get_column('time')
    timea = stream_a._maskNAN(timea)

    if extend:
        for elem in stream_b:
            if not elem.time in timea:
                stream_a.add(elem)
        stream_a = stream_a.sorting()
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
                    if not key in KEYLIST[1:15]:
                        raise ValueError, "Column key not valid"
                    exec('keyval = stream_a[pos].'+key)
                    fkey = 'f'+key
                    if fkey in function[0] and (isnan(keyval) or not stream_a._is_number(keyval)):
                        newval = function[0][fkey](functime)
                        #print 'stream_a['+str(pos)+'].'+key+' = newval'
                        exec('stream_a['+str(pos)+'].'+key+' = float(newval) + offset')
                        #print "Added %f to column %s at time %s" % (newval, key, num2date(ta))

    logging.info('--- Mergings finished at %s ' % str(datetime.now()))

    return DataStream(stream_a, headera)      


def subtractStreams(stream_a, stream_b, **kwargs):
    """
    combine the contents of two data stream:
    basically two methods are possible:
    1. replace data from specific columns of stream_a with data from stream_b

    2. fill gaps in stream_a data with stream_b data without replacing
    """
    keys = kwargs.get('keys')
    getmeans = kwargs.get('getmeans')
    if not keys:
        keys = KEYLIST[1:15]

    
    logging.info('--- Start subtracting streams at %s ' % str(datetime.now()))

    headera = stream_a.header
    headerb = stream_b.header

    newst = DataStream()

    # take stream_b data and fine nearest element in time from stream_a
    timea = stream_a._get_column('time')
    #timeb = stream_b._get_column('time')
    # interploate stream_b
    timea = stream_a._maskNAN(timea)

    sb = stream_b.trim(starttime=np.min(timea), endtime=np.max(timea))
    timeb = sb._get_column('time')
    timeb = stream_b._maskNAN(timeb)

    function = sb.interpol(keys)

    taprev = 0
    for elem in sb:
        foundina = sb._find_nearest(timea,elem.time)
        pos = foundina[1]
        ta = foundina[0]
        if (ta > taprev) and (np.min(timeb) < ta < np.max(timeb)):
            functime = (ta-function[1])/(function[2]-function[1])
            taprev = ta
            for key in keys:
                if not key in KEYLIST[1:15]:
                    raise ValueError, "Column key not valid"
                exec('keyval = stream_a[pos].'+key)
                fkey = 'f'+key
                if fkey in function[0]:
                    newval = function[0][fkey](functime)
                    exec('stream_a['+str(pos)+'].'+key+' -= float(newval)')
 
    logging.info('--- Stream-subtraction finished at %s ' % str(datetime.now()))

    return DataStream(stream_a, headera)      

    
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
    - stream.filtered() -- returns stream (changes sampling_period; in case of fmi ...) 
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
        self.container.append(datlst)

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
        """
        if not key in KEYLIST:
            raise ValueError, "Column key not valid"

        # Get only columns which contain different data from initialization
        count = 0
        col = []
        for elem in self:
            if eval('elem.'+key) != KEYINITDICT[key] and not isnan(eval('elem.'+key)):
                count = count+1
            col.append(eval('elem.'+key))
        if count > 0:
            return np.asarray(col)
        else:
            return np.asarray([])

        #return np.asarray([eval('elem.'+key) for elem in self])


    def _put_column(self, column, key):
        """
        adds a column to a Stream
        """
        if not key in KEYLIST:
            raise ValueError, "Column key not valid"
        if not len(column) == len(self):
            raise ValueError, "Column length does not fit Datastream"

        for idx, elem in enumerate(self):
            exec('elem.'+key+' = column[idx]')
            
        return self


    def _get_max(self, key):
        if not key in KEYLIST[:15]:
            raise ValueError, "Column key not valid"
        elem = max(self, key=lambda tmp: eval('tmp.'+key))
        return eval('elem.'+key)


    def _get_min(self, key):
        if not key in KEYLIST[:15]:
            raise ValueError, "Column key not valid"
        elem = min(self, key=lambda tmp: eval('tmp.'+key))
        return eval('elem.'+key)


    def _get_k(self, **kwargs):
        """
        Calculates the k value according to the Bartels scale
        Requires alpha to be set correctly (default: alpha = 1 valid for Niemegk)
        Range nT 0 5 10 20 40 70 120 200 330 500
        KValue   0 1  2  3  4  5   6   7   8   9
        key: defines the column to write k values (default is t2)
        """

        key = kwargs.get('key')
        alpha = kwargs.get('alpha')
        if not alpha:
            alpha = 1
        if not key:
            key = 't2'

        k = 9
        outstream = DataStream()
        for elem in self:
            row = LineStruct()
            for key in KEYLIST:
                exec('row.'+key+' = elem.'+key)
            dH = elem.dx
            if dH < 500*alpha:
                k = 8
            if dH < 330*alpha:
                k = 7
            if dH < 200*alpha:
                k = 6
            if dH < 120*alpha:
                k = 5
            if dH < 70*alpha:
                k = 4
            if dH < 40*alpha:
                k = 3
            if dH < 20*alpha:
                k = 2
            if dH < 10*alpha:
                k = 1
            if dH < 5*alpha:
                k = 0
            row.t2 = k
            outstream.add(row)

        return outstream


    def _aic(self, signal, k):
        try:
            aicval = k* np.log(np.var(signal[:k]))+(len(signal)-k-1)*np.log(np.var(signal[k:]))
        except:
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
            for key in KEYLIST:
                exec('row.'+key+' = elem.'+key)
            row.type = ''.join((list(coordinate))[4:])
            exec('row.x,row.y,row.z = '+coordinate+'(elem.x,elem.y,elem.z)')
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
        Tests for NAN values in column and ually masks them
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
                    logging.warning("NAN warning: only nan in column")
                    return []
        except:
            numdat = False
            logging.warning("NAN warning: only nan in column")
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
        newst = [elem for elem in self if not isnan(eval('elem.'+key))]
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
        Keywords:
            timerange: defines the length of the time window examined by the aic iteration
                        default: timedelta(hours=6)
        """
        timerange = kwargs.get('timerange')
        if not timerange:
            timerange = timedelta(hours=1)

        t = self._get_column('time')
        signal = self._get_column(key)
        # corrcet approach
        iprev = 0
        iend = 0

        while iend < len(t)-1:
            istart = iprev
            ta, iend = self._find_nearest(np.asarray(t), date2num(num2date(t[istart]).replace(tzinfo=None) + timerange))
            currsequence = signal[istart:iend]
            for idx, el in enumerate(currsequence):
                if idx > 1 and idx < len(currsequence)-1:
                    aicval = self._aic(currsequence, idx)
                    self[idx+istart].var2 = aicval               
            iprev = iend

        self.header['col-var2'] = 'aic'
        # old approach
        #for idx, elem in enumerate(self):
        #    ta, ts = self._find_nearest(np.asarray(t), date2num(num2date(elem.time).replace(tzinfo=None) - timerange))
        #    ta, te = self._find_nearest(np.asarray(t), date2num(num2date(elem.time).replace(tzinfo=None) + timerange))
        #    signalsegment = signal[ts:te]
        #    elem.var2 = self._aic(signalsegment,int(len(signalsegment)/2))

        return self

    def baseline( self, absolutestream, **kwargs):
        """
        calculates baseline correction for input stream (datastream)
        keywords:
        starttime
        endtime
        fit parameters:
        fitfunc
        fitdegree
        knotstep

        extradays
        stabilitytest (bool)
        """
        fitfunc = kwargs.get('fitfunc')
        fitdegree = kwargs.get('fitdegree')
        knotstep = kwargs.get('knotstep')
        extradays = kwargs.get('extradays')
        plotbaseline = kwargs.get('plotbaseline')

        
        endtime = self[-1].time
        if not extradays:
            extradays = 30
        if not fitfunc:
            fitfunc = 'spline'
        if not fitdegree:
            fitdegree = 5
        if not knotstep:
            knotstep = 0.05


        logging.info(' --- Start baseline-correction at %s' % str(datetime.now()))

        # 1) test whether absolutes are in the selected absolute data stream
        if absolutestream[0].time == 0:
            raise ValueError ("Baseline: Input stream needs to contain absolute data ")

        # 3) check whether enddate is within abs time range or larger:
        if not absolutestream[0].time < endtime:
            raise ValueError ("Baseline: Endtime prior to beginning of absolute measurements selected ")
            
        # 4) check whether endtime is within abs time range or larger:
        if absolutestream[-1].time < endtime:
            logging.info("Baseline: Last absolute measurement before end of stream - extrapolating baseline")
        if num2date(absolutestream[-1].time).replace(tzinfo=None) + timedelta(days=extradays) < num2date(endtime).replace(tzinfo=None):
            logging.warning("Baseline: Well... thats an adventurous extrapolation, but as you wish...")

        endtime = num2date(endtime).replace(tzinfo=None)
        # 5) check whether an abolute measurement larger then 12-31 of the same year as enddate exists
        yearenddate = datetime.strftime(endtime,'%Y')
        lst = [elem for elem in absolutestream if datetime.strftime(num2date(elem.time),'%Y') > yearenddate]
        if len(lst) > 0:
            baseendtime = datetime.strptime(yearenddate+'-12-31', '%Y-%m-%d')
        else:
            baseendtime = endtime

        # now add the extradays to endtime
        baseendtime = baseendtime + timedelta(days=extradays)
        
        # endtime for baseline calc determined
        basestarttime = baseendtime-timedelta(days=(365+2*extradays))

        bas = absolutestream.trim(starttime=basestarttime,endtime=baseendtime)
        bas = bas.extrapolate(basestarttime,baseendtime)

        col = bas._get_column('dx')
        bas = bas._put_column(col,'x')
        col = bas._get_column('dy')
        bas = bas._put_column(col,'y')
        col = bas._get_column('dz')
        bas = bas._put_column(col,'z')

        func = bas.fit(['x','y','z'],fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep)

        if plotbaseline:
            bas.pmplot(['x','y','z'],symbollist = ['o','o','o'],function=func)

        # subtract baseline
        self = self.func_subtract(func)

        logging.info(' --- Finished baseline-correction at %s' % str(datetime.now()))

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

        
        logging.info('Corrected time column by %s sec' % str(offset.seconds))

        return DataStream(newstream,header)


    def differentiate(self, **kwargs):
        """
        Method to differentiate all columns with respect to time.
        -- Using successive gradients

        optional:
        keys: (list - default ['x','y','z','f'] provide limited key-list
        """
        

        logging.info('--- Calculating derivative started at %s ' % str(datetime.now()))

        keys = kwargs.get('keys')
        if not keys:
            keys = ['x','y','z','f']

        t = self._get_column('time')
        for key in keys:
            val = self._get_column(key)
            dval = np.gradient(np.asarray(val))
            self._put_column(dval, 'd'+key)

        logging.info('--- derivative obtained at %s ' % str(datetime.now()))
        return self
        

    def extrapolate(self, start, end):
        """
        read absolute stream and extrapolate the data
        currently:
        repeat the last and first input at start and end
        """

        firstelem = self[0]
        lastelem = self[-1]
        lastelem.time = date2num(end + timedelta(days=1))
        firstelem.time = date2num(start - timedelta(days=1))
        self.add(lastelem)
        self.add(firstelem)
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
            if not key in KEYLIST[1:15]:
                raise ValueError, "Column key not valid"
            val = tmpst._get_column(key)
            # interplolate NaN values
            #nans, xxx= self._nan_helper(val)
            #val[nans]= np.interp(xxx(nans), xxx(~nans), val[~nans])
            x = arange(np.min(nt),np.max(nt),sp)
            if len(val)>1 and fitfunc == 'spline':
                try:
                    knots = np.array(arange(np.min(nt)+knotstep,np.max(nt)-knotstep,knotstep))
                    ti = interpolate.splrep(nt, val, k=3, s=0, t=knots)
                except:
                    logging.warning('Value error in fit function - likely reason: no valid numbers')
                    raise ValueError, "Value error in fit function"
                    return
                f_fit = interpolate.splev(x,ti)
            elif len(val)>1 and fitfunc == 'poly':
                ti = polyfit(nt, val, fitdegree)
                f_fit = polyval(ti,x)
            elif len(val)>1 and fitfunc == 'harmonic':
                ti = polyfit(nt, val, fitdegree)
                f_fit = polyval(ti,x)
            elif len(val)<=1:
                logging.warning('Fit: No valid data')
                return
            else:
                logging.warning('Fit: function not valid')
                return
            exec('f'+key+' = interpolate.interp1d(x, f_fit, bounds_error=False)')
            exec('functionkeylist["f'+key+'"] = f'+key)

        func = [functionkeylist, sv, ev]

        return func


    def filtered(self, **kwargs):
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
            logging.warning('FilterFunc: No valid stream provided')
            return self

        # check whether requested filter_width >= sampling interval within 1 millisecond accuracy
        si = timedelta(seconds=self.get_sampling_period()*24*3600)
        if filter_width - si <= timedelta(microseconds=1000):
            logging.warning('FilterFunc: Requested filter_width does not exceed sampling interval - aborting')
            return self

        logging.info('--- Start filtering at %s ' % str(datetime.now()))

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
                n = np.power(get_k(elem.dx),3.3)
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
                for el in KEYLIST[:15]:
                    exec('col'+el+'=[]')
                if filter_type == "gauss":
                    normvec = []
                    # -- determine coefficients for gaussian weighting (needs time - is identical for evenly spaced data but only for that)
                    for k in range(lowlim,uplim):
                        normvec.append(self._gf(starray[k].time-abscurrtime,tau))
                    normcoeff = np.sum(normvec)
                    for k in range(lowlim,uplim):
                        nor = normvec[k-lowlim]/normcoeff
                        for el in KEYLIST[:15]:
                            exec('col'+el+'.append(starray[k].'+el+'*nor)')
                    resrow.time = abscurrtime
                    for el in KEYLIST[1:15]:
                        exec('resrow.'+el+' = np.sum(col'+el+')')
                elif filter_type == "linear" or filter_type == "fmi":
                    for k in range(lowlim,uplim):
                        for el in KEYLIST[:15]:
                            exec('col'+el+'.append(starray[k].'+el+')')
                    resrow.time = abscurrtime
                    for el in KEYLIST[1:15]:
                        exec('resrow.'+el+' = np.mean(col'+el+')')
                    # add maxmin diffs: important for fmi
                    if starray[k].typ != 'fonly':
                        resrow.dx = np.max(colx)-np.min(colx)
                        resrow.dy = np.max(coly)-np.min(coly)
                        resrow.dz = np.max(colz)-np.min(colz)
                        resrow.df = np.max(colf)-np.min(colf)
                else:
                    logging.warning("FilterFunc: Filter not recognized - aborting")
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
        self.header['DigitalSamplingWidth'] = str(trange.seconds)+' sec'
        self.header['DigitalSamplingFilter'] = filter_type
        self.header['DataInterval'] = str(filter_width.seconds)+' sec'
        
        logging.info(' --- Finished filtering at %s' % str(datetime.now()))

        return DataStream(resdata,self.header)      


    def flag_stream(self, key, flag, comment, startdate, enddate=None):
        """
        Add flags to specific times or time ranges if enddate is provided:
        Requires the following input:
        key: (char) the column (f, x,y,z)
        flag: (int) 0 ok, 1 remove, 2 force ok, 3 force remove
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
                logging.info("Removed data from %s to %s ->  (%s)" % (startdate.isoformat(),enddate.isoformat(),comment))
            else:
                logging.info("Removed data at %s -> (%s)" % (startdate.isoformat(),comment))
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
                    if not key in KEYLIST[1:15]:
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
        """
        keys = kwargs.get('keys')
        if not keys:
            keys = ['x','y','z']

        for elem in self:
            # check whether time step is in function range
            if function[1] <= elem.time <= function[2]:
                functime = (elem.time-function[1])/(function[2]-function[1])
                for key in keys:
                    if not key in KEYLIST[1:15]:
                        raise ValueError, "Column key not valid"
                    fkey = 'f'+key
                    exec('keyval = elem.'+key)
                    if fkey in function[0] and not isnan(keyval):
                        try:
                            newval = keyval - function[0][fkey](functime)
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
        gapvariable = kwargs.get('gapvariable')

        if not accuracy:
            accuracy = 1.0/(3600.0*24.0) # one second relative to day

        sp = self.get_sampling_period()

        
        logging.info('--- Starting filling gaps with NANs at %s ' % (str(datetime.now())))

        # remove any lines with NAN values
        #self = self._drop_empty_line()

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
                        newline.var2 = 1.0
                    newline.time = currtime
                    #print newline
                    stream.add(newline)
                    currtime += sp
            else:
                if gapvariable:
                    elem.var2 = 0.0
                #print elem.time, elem.var2
                stream.add(elem)
            prevtime = elem.time

        logging.info('--- Filling gaps finished at %s ' % (str(datetime.now())))
                
        return DataStream(stream,header)


    def get_sampling_period(self):
        """
        returns the dominant sampling frequency in unit ! days !
        
        for time savings, this function is only testing the first 1000 elements
        """
        timedifflist = [[0,0]]
        domtd = 0
        timecol= self._get_column('time')
        for idx, val in enumerate(timecol[:1000]):
            if idx > 1:
                timediff = val - timeprev
                found = 0
                for tel in timedifflist:
                    if tel[1] == timediff:
                        tel[0] = tel[0]+1
                        found = 1
                if found == 0:
                    timedifflist.append([1,timediff])
            timeprev = val
                
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
        

        logging.info('--- Integrating started at %s ' % str(datetime.now()))

        keys = kwargs.get('keys')
        if not keys:
            keys = ['x','y','z','f']

        t = self._get_column('time')
        for key in keys:
            val = self._get_column(key)
            dval = sp.integrate.cumtrapz(np.asarray(val),t)
            dval = np.insert(dval, 0, 0) # Prepend 0 to maintain original length
            self._put_column(dval, 'd'+key)

        logging.info('--- integration finished at %s ' % str(datetime.now()))
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
            if not key in KEYLIST[1:15]:
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
        key = kwargs.get('key')
        fitdegree = kwargs.get('fitdegree')
        knotstep=kwargs.get('knotstep')
        m_fmi = kwargs.get('m_fmi')
        if not fitfunc:
            fitfunc = 'poly'
        if not fitdegree:
            fitdegree = 5
        if not m_fmi:
            m_fmi = 0
        if not key:
            key = 't2'
        
        stream = DataStream()
        # extract daily streams/24h slices from input
        iprev = 0
        iend = 0

        logging.info('--- Starting k value calculation: %s ' % (str(datetime.now())))

        # Start with the full input stream
        # convert xyz to hdz first
        fmistream = self._convertstream('xyz2hdz',keep_header=True)
        fmi1stream = fmistream.filtered(filter_type='linear',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30))
        fmi2stream = fmistream.filtered(filter_type='fmi',filter_width=timedelta(minutes=60),filter_offset=timedelta(minutes=30),fmi_initial_data=fmi1stream,m_fmi=m_fmi)

        logging.info('--- -- k value: finished initial filtering at %s ' % (str(datetime.now())))

        t = fmi2stream._get_column('time')

        while iend < len(t)-1:
            istart = iprev
            ta, iend = self._find_nearest(np.asarray(t), date2num(num2date(t[istart]).replace(tzinfo=None) + timedelta(hours=24)))
            currsequence = fmi2stream[istart:iend+1]
            fmitmpstream = DataStream()
            for el in currsequence:
                fmitmpstream.add(el)
            func = fmitmpstream.fit(['x','y','z'],fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep)
            fmitmpstream = fmitmpstream.func_subtract(func)
            stream.extend(fmitmpstream,self.header)
            iprev = iend

        fmi3stream = stream.filtered(filter_type='linear',filter_width=timedelta(minutes=180),filter_offset=timedelta(minutes=90))
        fmi4stream = fmi3stream._get_k(key=key)

        self.header['col-'+key] = 'k'

        outstream = mergeStreams(self,fmi4stream,keys=[key])

        logging.info('--- finished k value calculation: %s ' % (str(datetime.now())))
        
        return DataStream(outstream, self.header)


    def pmplot(self, keys, **kwargs):
        """
        Creates a simple graph of the current stream.
        Supports the following keywords:
        function: (func) [0] is a dictionary containing keys (e.g. fx), [1] the startvalue, [2] the endvalue  Plot the content of function within the plot
        fullday: (boolean - default False) rounds first and last day two 0:00 respectively 24:00 if True
        colorlist (list - default []): provide a ordered color list of type ['b','g'],....
        errorbar: (boolean - default False) plot dx,dy,dz,df values if True
        symbol: (string - default '-') symbol for primary plot
        symbol_func: (string - default '-') symbol of function plot 
        savefigure: (string - default None) if provided a copy of the plot is saved to savefilename.png 

            from dev_magpy_stream import *
            st = read()
            st.pmplot()

            keys define the columns to be plotted
        """
        function = kwargs.get('function')
        fullday = kwargs.get('fullday')
        plottitle = kwargs.get('plottitle')
        colorlist = kwargs.get('colorlist')
        errorbar = kwargs.get('errorbar')
        padding = kwargs.get('padding') # needs to be incorporated
        symbollist = kwargs.get('symbollist')
        plottype = kwargs.get('plottype')
        symbol_func = kwargs.get('symbol_func')
        endvalue = kwargs.get('endvalue')
        savefigure = kwargs.get('savefigure')
        savedpi = kwargs.get('savedpi')

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

        myyfmt = ScalarFormatter(useOffset=False)
        n_subplots = len(keys)
        if n_subplots < 1:
            raise  ValueError, "Provide valid key(s)"
        count = 0
        fig = plt.figure()

        for key in keys:
            if not key in KEYLIST[1:15]:
                raise ValueError, "Column key not valid"
            t = self._get_column('time')
            yplt = self._get_column(key)
            # switch between continuous and discontinuous plots
            if plottype == 'discontinuous':
                yplt = self._maskNAN(yplt)
            else: 
                nans, test = self._nan_helper(yplt)
                newt = [t[idx] for idx, el in enumerate(yplt) if not nans[idx]]
                t = newt
                yplt = [el for idx, el in enumerate(yplt) if not nans[idx]]
            # start plotting if data is still present
            len_val= len(yplt)
            if len_val > 1:
                count += 1
                ax = count
                subplt = "%d%d%d" %(n_subplots,1,count)
                # Create primary plot and define x scale and ticks/labels of subplots
                if count == 1:
                    ax = fig.add_subplot(subplt)
                    if plottitle:
                        ax.set_title(plottitle)
                    a = ax
                else:
                    ax = fig.add_subplot(subplt, sharex=a)
                if count < len(keys):
                    setp(ax.get_xticklabels(), visible=False)
                else:
                    ax.set_xlabel("Time (UTC)")
                # Create plots
                # -- switch color and symbol
                if symbollist[count-1] == 'z': # secrect symbol for plotting colored bars for k values
                    tstep = 0.06
                    xy = range(9)
                    for num in range(len(t)):
                        if tstep < t[num] < np.max(t)-tstep:
                            ax.fill([t[num]-tstep,t[num]+tstep,t[num]+tstep,t[num]-tstep],[0,0,yplt[num]+0.1,yplt[num]+0.1],facecolor=cm.RdYlGn((9-yplt[num])/9.,1),alpha=1,edgecolor='k')
                    ax.plot_date(t,yplt,colorlist[count-1]+'|')
                else:
                    ax.plot_date(t,yplt,colorlist[count-1]+symbollist[count-1])
                if errorbar:
                    yerr = self._get_column('d'+key)
                    if len(yerr) > 0: 
                        ax.errorbar(t,yplt,yerr=varlist[ax+4],fmt=colorlist[count]+'o')
                    else:
                        logging.warning(' -- Errorbars (d%s) not found for key %s' % (key, key))
                #ax.plot_date(t2,yplt2,"r"+symbol[1],markersize=4)
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
                label = ylabel+' ['+yunit+']'
                ax.set_ylabel(label)
                ax.get_yaxis().set_major_formatter(myyfmt)
                if fullday:
                    ax.set_xlim(np.floor(np.min(t)),np.floor(np.max(t)+1))
            else:
                logging.warning("Plot: No data available for key %s" % key)

        fig.subplots_adjust(hspace=0)

        if not savefigure:
            plt.show()
        else:
            savefig(savefigure,dpi=savedpi)	# save as PNG/SVG


    def pmwrite(self, filepath, **kwargs):
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
        if not format_type:
            format_type = 'PYSTR'
        if not dateformat:
            dateformat = '%Y-%m-%d' # or %Y-%m-%dT%H or %Y-%m or %Y or %Y
        if not coverage:
            coverage = timedelta(days=1)
        if not filenamebegins:
            filenamebegins = 'Test-'
        if not filenameends:
            filenameends = '.txt'
        if not mode:
            mode= 'overwrite'

        # Extension for cfd files is automatically attached
        if format_type == 'PYCDF':
            filenameends = ''

        # divide stream in parts according to coverage and same them
        newst = DataStream()
        if not coverage == 'all':
            starttime = datetime.strptime(datetime.strftime(num2date(self[0].time).replace(tzinfo=None),'%Y-%m-%d'),'%Y-%m-%d')
            endtime = starttime + coverage
            while starttime < num2date(self[-1].time).replace(tzinfo=None):
                lst = [elem for elem in self if starttime <= num2date(elem.time).replace(tzinfo=None) < endtime]
                newst = DataStream(lst,self.header)
                filename = filenamebegins + datetime.strftime(starttime,dateformat) + filenameends
                if len(lst) > 0:
                    writeFormat(newst, os.path.join(filepath,filename),format_type,mode=mode)
                starttime = endtime
                endtime = endtime + coverage
        else:
            filename = filenamebegins + filenameends
            writeFormat(self, os.path.join(filepath,filename),format_type,mode=mode)
            

    def remove_flagged(self, **kwargs):
        """
        remove flagged data from stream:
        kwargs support the following keywords:
            - flaglist  (list) default=[1,3]
            - key (string e.g. 'f') default='f'
        flag = '000' or '010' etc
        """
        
        # Defaults:
        flaglist = kwargs.get('flaglist')
        key = kwargs.get('key')

        if not flaglist:
            flaglist = [1,3]
        if not key:
            key = 'f'

        poslst = [i for i,el in enumerate(FLAGKEYLIST) if el == key]
        pos = poslst[0]
        liste = []
        emptyelem = LineStruct()
        for elem in self:
            fllst = list(elem.flag)
            flag = int(fllst[pos])
            if not flag in flaglist:
                liste.append(elem)
            else:
                liste.append(emptyelem)

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

        logging.info('--- Applying rotation matrix: %s ' % (str(datetime.now())))

        for elem in self:
            ra = np.pi*alpha/(180.*ang_fac)
            rb = np.pi*beta/(180.*ang_fac)
            xs = elem.x*np.cos(rb)*np.cos(ra)-elem.y*np.sin(ra)+elem.z*np.sin(rb)*np.cos(ra)
            ys = elem.x*np.cos(rb)*np.sin(ra)+elem.y*np.cos(ra)+elem.z*np.sin(rb)*np.sin(ra)
            zs = elem.x*np.sin(rb)+elem.z*np.cos(rb)
            elem.x = xs
            elem.y = ys
            elem.z = zs

        logging.info('--- finished reorientation: %s ' % (str(datetime.now())))

        return self


    def routlier(self, **kwargs):
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
        threshold = kwargs.get('treshold')
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
        
        logging.info('--- Starting outlier removal at %s ' % (str(datetime.now())))

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
                        logging.warning("Eliminate outliers produced a problem: please check\n")
                        pass

                for elem in lstpart:
                    row = LineStruct()
                    row = elem
                    if not md-whisker < eval('elem.'+key) < md+whisker:
                        fllist = list(row.flag)
                        fllist[flagpos] = '1'
                        row.flag=''.join(fllist)
                        row.comment = "%s removed by automatic outlier removal" % key
                        logging.info("Outlier: removed %f at time %f, " % (eval('elem.'+key), elem.time))
                    else:
                        fllist = list(row.flag)
                        fllist[flagpos] = '0'
                        row.flag=''.join(fllist)
                    newst.add(row)

        logging.info('--- Outlier removal finished at %s ' % str(datetime.now()))

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

        
        logging.info(' --- Start smoothing (%s window, width %d) at %s' % (window, window_len, str(datetime.now())))

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

        logging.info(' --- Finished smoothing at %s' % (str(datetime.now())))
        
        return self


    def spectrogram(self, keys, **kwargs):
        """
        Creates a spectrogram plot of selected keys.

        """

        t = self._get_column('time')
        for key in keys:
            val = self._get_column(key)
            specgram(val,NFFT=512,noverlap=0)
            #dval = np.gradient(np.asarray(val))
            #self._put_column(dval, 'd'+key)

        show()
        #plot(t,val,'b-')
        

        dt = self.get_sampling_period()*(24*60)
        print "Sampling period: %f" % dt
        NFFT = 1024       # the length of the windowing segments
        Fs = int(1.0/dt)  # the sampling frequency

        #Pxx, freqs, bins, im = specgram(val,NFFT=NFFT,Fs=Fs)
        #show()

    def powerspectrum(self, key):
        """
        Calculating the power spectrum
        following the numpy fft example
        """
        
        t = np.asarray(self._get_column('time'))
        freq = np.fft.fftfreq(t.shape[-1])
        val = np.asarray(self._get_column(key))
        s = np.fft.fft(val)
        ps = np.real(s*np.conjugate(s))
        print ps

        plot(freq,ps,'r-')
        return freq, ps


    def trim(self, starttime=None, endtime=None):
        """
        Removing dates outside of range between start- and endtime
        """
        # include test - does not work yet
        #if date2num(self._testtime(starttime)) > date2num(self._testtime(endtime)):
        #    raise ValueError, "Starttime is larger then Endtime"
        # remove data prior to starttime input
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
                        stval = idx-1
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
                        edval = idx-1
                        break
            self.container = self.container[:edval]

        return DataStream(self.container,self.header)



         
if __name__ == '__main__':
    print "Starting the PyMag program:"
    

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
    baslemi1 = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_lemi_alpha3.3.txt'))
    baslemi2 = pmRead(path_or_url=os.path.normpath(r'e:\leon\Programme\Python\PyMag\ExperimentalFolder\absolutes_didd.txt'))
    newst = subtractStreams(baslemi1,baslemi2,keys=['x','y','z'])
    newst = newst.trim(starttime=datetime(2010,7,10,00,02),endtime=datetime(2011,10,1,23,58))
    newst.pmplot(['x','y','z'])

    testarray = np.array(baslemi1)
    print testarray[1][2]
    print testarray.ndim
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
    
