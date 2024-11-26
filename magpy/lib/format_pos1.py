"""
MagPy
Auxiliary input filter - POS-1 data
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from magpy.stream import DataStream
from datetime import datetime
import os
import numpy as np
from magpy.core.methods import testtime, extract_date_from_string
import logging
logger = logging.getLogger(__name__)
KEYLIST = DataStream().KEYLIST


def isPOS1(filename):
    """
    Checks whether a file is binary POS-1 file format.
    Header:
    # MagPyBin %s %s %s %s %s %s %d" % ('POS1', '[f,df,var1,sectime]', '[f,df,var1,GPStime]', '[nT,nT,none,none]', '[1000,1000,1,1]
    """
    try:
        with open(filename, "rb") as fi:
            temp = fi.readline()
    except:
        return False
    try:
        if not filename.find('POS') >= 0:
            return False
    except:
        return False
    try:
        temp = temp.decode()
        comp = temp.split()
        if len(comp) == 7 and float(comp[6]) > 18000:
            pass
        else:
            return False
    except:
        return False

    logger.info("format_pos1: Found POS-1 Binary file %s" % filename)
    return True

def isPOS1TXT(filename):
    """
    Checks whether a file is text POS-1 file format.
    """
    try:
        with open(filename, 'r', encoding='utf-8', newline='', errors='ignore') as fh:
            temp = fh.readline()
    except:
        return False
    try:
        #linebit = (temp.split())[2]
        comp = temp.split(",")
        if len(comp)==2:
            pass
    except:
        return False
    try:
        time = datetime.strptime(comp[0], "%Y-%m-%dT%H:%M:%S")
        val = float(comp[1])
    except:
        return False
    logger.info("format_pos1: Found POS-1 Text file %s" % filename)
    return True


def isPOSPMB(filename):
    """
    Checks whether a file is binary POS-1 file format.
    """
    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
    except:
        return False
    try:
        el = temp.split()
        if not len(el) == 5:
            return False
    except:
        return False
    try:
        date = datetime.strptime(el[3],"%m.%d.%y")
    except:
        return False

    logger.info("format_pos1: Found POS-1 pmb file {}".format(filename))
    return True


def readPOS1(filename, headonly=False, **kwargs):
    # Reading POS-1 Binary format data.

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True
    array = [[] for key in KEYLIST]
    headers= {}

    theday = extract_date_from_string(filename)
    try:
        day = datetime.strftime(theday,"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not theday >= datetime.date(testtime(starttime)):
                getfile = False
        if endtime:
            if not theday <= datetime.date(testtime(endtime)):
                getfile = False
    except:
        logger.warning("readPOS1BIN: Could not identify date in %s. Reading all ..." % filename)
        getfile = True

    if getfile:
        logger.info('readPOS1BIN: Reading %s' % (filename))
        headers['col-f'] = 'F'
        headers['unit-col-f'] = 'nT'
        with open(filename, "rb") as fi:
            for line in fi:
                line = line.decode()
                data = line.split()
                time = datetime(int(data[0]),int(data[1]),int(data[2]),int(data[3]),int(data[4]),int(data[5]))
                array[0].append(time)
                array[4].append(float(data[6])/1000.)
                try:
                    array[KEYLIST.index('df')].append(float(data[8])/1000.)
                    array[KEYLIST.index('var1')].append(int(data[9]))
                except:
                    pass

    array = [np.asarray(el) for el in array]
    return DataStream(header=headers,ndarray=np.asarray(array, dtype=object))

def readPOS1TXT(filename, headonly=False, **kwargs):
    # Reading POS-1 text format data.
    # NOTE: old format. Out of use in GMO since 4th June 2013.
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    array = [[] for key in KEYLIST]
    data = []
    key = None

    theday = extract_date_from_string(filename)
    try:
        day = datetime.strftime(theday,"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        logger.warning("readPOS1TXT: Could not identify date in {}. Reading all ...".format(filename))
        getfile = True

    if getfile:
        with open(filename, 'r', encoding='utf-8', newline='', errors='ignore') as fh:
            for line in fh:
                data = line.split(",")
                time = datetime.strptime(data[0], "%Y-%m-%dT%H:%M:%S")
                array[0].append(time)
                array[4].append(float(data[1])/1000.)
                try:
                    array[KEYLIST.index('df')].append(float(data[3])/1000.)
                except:
                    pass

    array = [np.asarray(el) for el in array]
    return DataStream(header=headers,ndarray=np.asarray(array, dtype=object))


def readPOSPMB(filename, headonly=False, **kwargs):
    # Reading POS-1 Binary format data.

    timestamp = os.path.getmtime(filename)
    creationdate = datetime.fromtimestamp(timestamp)
    daytmp = datetime.strftime(creationdate,"%Y-%m-%d")
    YeT = daytmp[:2]
    ctime = creationdate

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header information is already present
    headers = {}
    array = [[] for key in KEYLIST]

    data = []
    key = None
    logging.info(' Read: %s Format: POS pmb' % (filename))

    for line in fh:
        if line.isspace():
            # blank line
            pass
        else:
            data = line.split()
            #'48607466', '00011', '80', '06.28.18', '15:05:27,00'
            time = data[4].split(',')[0].split('.')[0]
            date = data[3]+'T'+time
            numtime = datetime.strptime(date,"%m.%d.%yT%H:%M:%S")
            if numtime:
                array[0].append(numtime)
                array[4].append(float(data[0])/1000.)

    if len(array[0]) > 0:
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    logging.info("Loaded POS pmb file")
    fh.close()

    headers['DataFormat'] = 'PMB'
    array = [np.asarray(el) for el in array]

    return DataStream(header=headers, ndarray=np.asarray(array,dtype=object))
