"""
MagPy
RCS input filter for MagPy
Written by Richard Mandl
Short description...
"""
from magpy.stream import DataStream
from datetime import datetime
import numpy as np
from magpy.core.methods import testtime, extract_date_from_string
import logging
logger = logging.getLogger(__name__)

KEYLIST = DataStream().KEYLIST

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


def isRMRCS(filename):
    """
    Checks whether a file is ASCII RCS format.
    """
    try:
        with open(filename, 'r', encoding='utf-8', newline='', errors='ignore') as fh:
            temp = fh.readline()
    except:
        return False
    try:
        if not temp.startswith('# RCS'):
            return False
    except:
        return False
    return True


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
    return DataStream(header=headers, ndarray=ndarray)

def readRMRCS(filename, headonly=False, **kwargs):
    """
    Reading RMRCS format data. (Richard Mandl's RCS extraction)
    # RCS Fieldpoint T7
    # Conrad Observatorium, www.zamg.ac.at
    # 2012-02-01 00:00:00
    #
    # 12="ZAGTFPT7      M6      I,cFP-AI-110    CH00    AP23    Niederschlagsmesser     --      Unwetter, S     AR0-20H0.1      mm      y=500x+0        AI"
    # 13="ZAGTFPT7      M6      I,cFP-AI-110    CH01    JC      Schneepegelsensor       OK      Mastverwehung, S        AR0-200H0       cm      y=31250x+0      AI"
    # 14="ZAGTFPT7      M6      I,cFP-AI-110    CH02    430A_T  Wetterhuette - Lufttemperatur   -       -, B    AR-35-45H0      C       y=4000x-35      AI"
    # 15="ZAGTFPT7      M6      I,cFP-AI-110    CH03    430A_F  Wetterhuette - Luftfeuchte      -       -, B    AR0-100H0       %       y=5000x+0       AI"
    #
    1328054403.99       20120201 000004 49.276E-6       49.826E+0       -11.665E+0      78.356E+0
    1328054407.99       20120201 000008 79.480E-6       49.823E+0       -11.677E+0      78.364E+0
    1328054411.99       20120201 000012 68.555E-6       49.828E+0       -11.688E+0      78.389E+0
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    debug = kwargs.get('debug')
    if debug:
        print ("RCS: found data from Richards Perl script")

    # read file and split text into channels
    # --------------------------------------
    stream = DataStream()
    headers = {}
    array = [[] for key in KEYLIST]
    data = []
    measurement = []
    unit = []
    i = 0
    key = None

    # try to get day from filename (platform independent)
    # --------------------------------------
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

    if getfile:
        with open(filename, 'r', encoding='utf-8', newline='', errors='ignore') as fh:
            for line in fh:
                if line.isspace():
                    # blank line
                    pass
                elif line.startswith('# RCS Fieldpoint'):
                    # data header
                    fieldpoint = line.replace('# RCS Fieldpoint','').strip()
                elif line.startswith('#'):
                    # data header
                    colsstr = line.split(',')
                    if (len(colsstr) == 3):
                        # select the lines with three komma separeted parts -> they describe the data
                        meastype = colsstr[1].split()
                        unittype = colsstr[2].split()
                        measurement.append(meastype[2])
                        unit.append(unittype[2])
                        headers['col-'+KEYLIST[i+1]] = measurement[i]
                        headers['unit-col-'+KEYLIST[i+1]] = unit[i]
                        if headers['unit-col-'+KEYLIST[i+1]] == '--':
                            headers['unit-col-'+KEYLIST[i+1]] = ''
                        i=i+1
                elif headonly:
                    # skip data for option headonly
                    continue
                else:
                    # data entry - may be written in multiple columns
                    # row beinhaltet die Werte eine Zeile
                    elem = line[:-1].split()
                    gottime = False

                    try:
                        array[0].append(datetime.strptime(elem[1],"%Y-%m-%dT%H:%M:%S"))
                        add = 2
                        gottime = True
                    except:
                        try:
                            array[0].append(datetime.strptime(elem[1]+'T'+elem[2],"%Y%m%dT%H%M%S"))
                            add = 3
                            gottime = True
                        except:
                            raise ValueError("Can't read date format in RCS file")
                    if gottime:
                        for i in range(len(unit)):
                            try:
                                array[i+1].append(float(elem[i+add]))
                            except:
                                array[i+1].append(np.nan)
                                pass

            array = [np.asarray(el) for el in array]
            headers['SensorID'] = 'RCS{}_20160114_0001'.format(fieldpoint) # 20160114 corresponds to the date at which RCS was activated
            headers["SensorName"] = 'RCS{}'.format(fieldpoint)
            headers["SensorSerialNum"] = "20160114"
            headers["SensorRevision"] = "0001"
            headers["SensorModule"] = "RCS"
            headers["DataFormat"] = "RCS-Perl"
            headers["SensorGroup"] = "environment"
            headers["SensorDataLogger"] = "{}".format(fieldpoint)
    else:
        headers = stream.header
        stream =[]

    return DataStream(header=headers, ndarray=np.asarray(array,dtype=object))


