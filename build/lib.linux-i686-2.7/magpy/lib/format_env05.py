"""
MagPy
Auxiliary input filter - Env05 data
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from __future__ import print_function

from magpy.stream import *

def isENV05(filename):
    """
    Checks whether a file is Binary Env05 file format.
    # MagPyBin %s %s %s %s %s %s %d" % (Env05, '[t1,t2,var1]', '[T,DewPoint,RH]', '[deg_C,deg_C,per rh]', '[1000,1000,1000]'
    """
    try:
        temp = open(filename, 'rb').readline()
    except:
        return False
    if not 'Env05' in temp:
        return False
    logging.debug("lib - format_env05: Found Env05 Binary file %s" % filename)
    return True

def readENV05(filename, headonly=False, **kwargs):
    #Reading Env05 Binary format data.
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

    if getfile:

        line = fh.readline()
        line = fh.read(29)
        while line != "":
            data= struct.unpack("6hLLLL",line.strip())

            row = LineStruct()

            time = datetime(data[0],data[1],data[2],data[3],data[4],data[5],data[6])
            row.time = date2num(time)
            row.t1 = data[7]/1000.      # Actual T (C)
            row.t2 = data[8]/1000.      # Dew point T (C)
            row.var1 = data[9]/1000.    # Humidity (%)

            stream.add(row)

            line = fh.read(29)

    fh.close()

    print("Finished file reading of %s" % filename)

    return DataStream(stream, headers)
