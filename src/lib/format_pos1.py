"""
MagPy
Auxiliary input filter - POS-1 data
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *

def isPOS1(filename):
    """
    Checks whether a file is Binary POS-1 file format.
    Header:
    # MagPyBin %s %s %s %s %s %s %d" % ('POS1', '[f,df,var1,sectime]', '[f,df,var1,GPStime]', '[nT,nT,none,none]', '[1000,1000,1,1]
    """
    try:
        temp = open(filename, 'rb').readline()
    except:
        return False
    if not 'POS1' in temp:
        return False
    logging.debug("lib - format_pos1: Found POS-1 Binary file %s" % filename)
    return True

def isPOS1TXT(filename):
    """
    Checks whether a file is text POS-1 file format.
    """
    try:
        temp = open(filename, 'rb').readline()
    except:
        return False
    linebit = (temp.split())[2]
    if not linebit == '+-':
        return False
    logging.debug("lib - format_pos1: Found POS-1 Text file %s" % filename)
    return True

def readPOS1(filename, headonly=False, **kwargs):
    # Reading POS-1 Binary format data.
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rb')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None

    line = fh.readline()

    if getfile:

	line = fh.read(45)
	while line != "":
            data= struct.unpack("6hLLLh6hL",line.strip())

            row = LineStruct()
   
            time = datetime(data[0],data[1],data[2],data[3],data[4],data[5],data[6])
            row.time = date2num(time)
            row.f = data[7]/1000.
            row.df = data[8]/1000.
            stream.add(row)    

    	    line = fh.read(45)

    fh.close()

    print "Finished file reading of %s" % filename

    return DataStream(stream, headers)    

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
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None

    if getfile:

	line = fh.readline()
	while line != "":
            data = line.split()
            row = LineStruct()
   
            time = datetime.strptime(data[0], "%Y-%m-%dT%H:%M:%S.%f")
            row.time = date2num(time)
            row.f = float(data[1])/1000.
            row.df = float(data[3])/1000.
            stream.add(row)    

    	    line = fh.readline()

    fh.close()

    print "Finished file reading of %s" % filename

    return DataStream(stream, headers)

