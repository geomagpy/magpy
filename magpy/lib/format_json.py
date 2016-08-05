"""
MagPy
IAGA02 input filter
Written by Roman Leonhardt June 2012
- contains test, read and write function
"""
from __future__ import print_function
import json

from magpy.stream import *


def isJSON(filename):
    """
    Checks whether a file is JSON format.
    """
    try:
        print("Attempting to read JSON file {}...".format(filename))
        jsonfile = open(filename, 'r')
        j = json.load(jsonfile)
    except:
        return False
    return True


def readJSON(filename, headonly=False, **kwargs):
    """
    Reading JSON format data.
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')

    array = [[] for key in KEYLIST]

    # read file and split text into channels
    stream = DataStream()

    # Check whether header infromation is already present
    headers = {}
    data = []
    key = None

    jsonfile = open(filename, 'r')
    fh = json.load(jsonfile)
    loggerlib.info('Read: %s Format: %s ' % (filename, "JSON"))

    # Insert formatting here
    print("Found JSON file!")

    #fh.close()
    #for idx, elem in enumerate(array):
        #array[idx] = np.asarray(array[idx])

    #stream = DataStream([LineStruct()],stream.header,np.asarray(array))
    #sr = stream.samplingrate()

    return stream
