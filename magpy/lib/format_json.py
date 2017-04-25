"""
MagPy
IAGA02 input filter
Written by Roman Leonhardt June 2012
- contains test, read and write function
"""
from __future__ import print_function
import json
from matplotlib.dates import date2num
import numpy as np

from magpy.stream import KEYLIST, DataStream, loggerlib, testTimeString


def isJSON(filename):
    """
    Checks whether a file is JSON format.
    """
    try:
        jsonfile = open(filename, 'r')
        j = json.load(jsonfile)
    except:
        return False
    return True


def readJSON(filename, headonly=False, **kwargs):
    """
    Reading JSON format data.
    """
    stream = DataStream()
    header = {}
    array = [[] for key in KEYLIST]

    with open(filename, 'r') as jsonfile:
        dataset = json.load(jsonfile)
        loggerlib.info('Read: %s, Format: %s ' % (filename, "JSON"))
        
        fillkeys = ['var1', 'var2', 'var3', 'var4', 'var5', 'x', 'y', 'z', 'f']
        datakeys = dataset[0]
        keydict = {}
        
        for i, key in enumerate(datakeys):
            if 'time' in key:
                keydict[i] = 'time'
            elif key == 'density':
                keydict[i] = 'var1'
                fillkeys.pop(fillkeys.index('var1'))
            elif key == 'speed':
                keydict[i] = 'var2'
                fillkeys.pop(fillkeys.index('var2'))
            elif key == 'temperature':
                keydict[i] = 'var3'
                fillkeys.pop(fillkeys.index('var3'))
            elif 'bx' in key.lower():
                keydict[i] = 'x'
                fillkeys.pop(fillkeys.index('x'))
            elif 'by' in key.lower():
                keydict[i] = 'y'
                fillkeys.pop(fillkeys.index('y'))
            elif 'bz' in key.lower():
                keydict[i] = 'z'
                fillkeys.pop(fillkeys.index('z'))
            elif 'bt' in key.lower():
                keydict[i] = 'f'
                fillkeys.pop(fillkeys.index('f'))
            else:
                try:
                    keydict[i] = fillkeys.pop(0)
                except IndexError:
                    loggerlib.warning("CAUTION! Out of available keys for data. {} will not be contained in stream.".format(key))
                    print("CAUTION! Out of available keys for data. {} will not be contained in stream.".format(key))
                    
            if 'time' in key:
                data = [date2num(testTimeString(str(x[i]))) for x in dataset[1:]]
            else:

                data = [np.nan if x[i] is None else float(x[i]) for x in dataset[1:]]
            array[KEYLIST.index(keydict[i])] = data
            header['col-'+keydict[i]] = key
            header['unit-col-'+keydict[i]] = ''
                
    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])

    stream = DataStream([],header,np.asarray(array))

    return stream
