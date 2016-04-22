"""
MagPy
RCS input filter for MagPy
Written by Richard Mandl
Short description...
"""
from __future__ import print_function

from magpy.stream import *

def GetINIData(relatedtofilename):
    # add here methods whcih you need to analyze RCS data    
    pass


def isRCS(filename):
    """
    Checks whether a file an RCS data set of form:

    """
    # if a unique pattern (e.g. in first line or filename or content) is recognized:
    #     return True
    # else:
    return False


def readRCS(filename, headonly=False, **kwargs):
    """
    DESCRIPTION:
        Reading RCS data
    PARAMETER:
        myparameter      (string) one of day,hour,minute,k   - default is minute

    """
    # typical keyword arguments - free to extend
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')

    getfile = True
    gethead = True

    # Initialize contents of DataStream object
    stream = DataStream()
    headers = {}
    ndlist = [np.asarray([]) for key in KEYLIST]

    print ("Hi Richard, this is the skeleton to insert your code")

    ndarray = np.asarray(ndlist)
    return DataStream([LineStruct()], headers, ndarray)


