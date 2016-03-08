"""
MagPy
Auxiliary input filter - POS-1 data
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from magpy.stream import *

def isPOS1(filename):
    """
    Checks whether a file is binary POS-1 file format.
    Header:
    # MagPyBin %s %s %s %s %s %s %d" % ('POS1', '[f,df,var1,sectime]', '[f,df,var1,GPStime]', '[nT,nT,none,none]', '[1000,1000,1,1]
    """
    try:
        temp = open(filename, 'rb').readline()
    except:
        return False
    try:
        if not 'POS1' in temp:
            return False
    except:
        return False

    loggerlib.info("format_pos1: Found POS-1 Binary file %s" % filename)
    return True

def isPOS1TXT(filename):
    """
    Checks whether a file is text POS-1 file format.
    """
    try:
        temp = open(filename, 'rb').readline()
    except:
        return False
    try:
        linebit = (temp.split())[2]
    except:
        return False
    try:
        if not linebit == '+-':
            return False
    except:
        return False
    loggerlib.info("format_pos1: Found POS-1 Text file %s" % filename)
    return True

def readPOS1(filename, headonly=False, **kwargs):
    # Reading POS-1 Binary format data.

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rb')
    # read file and split text into channels
    stream = DataStream([],{})

    data = []
    key = None

    theday = extractDateFromString(filename)
    try:
        day = datetime.strftime(theday,"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not theday >= datetime.date(stream._testtime(starttime)):
                getfile = False
        if endtime:
            if not theday <= datetime.date(stream._testtime(endtime)):
                getfile = False
    except:
        logging.warning("readPOS1BIN: Could not identify date in %s. Reading all ..." % daystring)
        getfile = True

    if getfile:

        line = fh.readline()

        loggerlib.info('readPOS1BIN: Reading %s' % (filename))
        stream.header['col-f'] = 'F'
        stream.header['unit-col-f'] = 'nT'
        stream.header['col-df'] = 'dF'
        stream.header['unit-col-df'] = 'nT'
        stream.header['col-var1'] = 'ErrorCode'
        stream.header['unit-col-var1'] = ''

        line = fh.read(45)
        while line != "":
            data= struct.unpack("6hLLLh6hL",line.strip())

            row = LineStruct()

            time = datetime(data[0],data[1],data[2],data[3],data[4],data[5],data[6])
            row.time = date2num(time)
            row.f = float(data[7])/1000.
            row.df = float(data[8])/1000.
            row.var1 = int(data[9])

            stream.add(row)

            line = fh.read(45)

    fh.close()

    return stream

def readPOS1TXT(filename, headonly=False, **kwargs):
    # Reading POS-1 text format data.
    # NOTE: old format. Out of use in GMO since 4th June 2013.
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rb')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    data = []
    key = None

    theday = extractDateFromString(filename)
    try:
        day = datetime.strftime(theday,"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        loggerlib.warning("readPOS1TXT: Could not identify date in %s. Reading all ..." % daystring)
        getfile = True

    if getfile:

        line = fh.readline()
        loggerlib.info('readPOS1TXT: Reading %s' % (filename))

        while line != "":
            data = line.split()
            row = LineStruct()

            time = datetime.strptime(data[0], "%Y-%m-%dT%H:%M:%S.%f")
            row.time = date2num(time)
            row.f = float(data[1])/1000.
            row.df = float(data[3])/1000.
            stream.add(row)

            line = fh.readline()

        #print "Finished file reading of %s" % filename

    fh.close()


    return DataStream(stream, headers)
