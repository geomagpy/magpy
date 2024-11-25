"""
MagPy
dIdD input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from magpy.stream import DataStream, read, subtract_streams, join_streams, magpyversion
from datetime import datetime, timedelta, timezone
import os
import numpy as np
from magpy.core.methods import testtime, extract_date_from_string
import logging
logger = logging.getLogger(__name__)

KEYLIST = DataStream().KEYLIST


def isDIDD(filename):
    """
    Checks whether a file is ASCII DIDD (Tihany) format.
    """
    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
    except:
        return False
    try:
        if not temp.startswith('hh mm'):
            if not  temp.startswith('%hh %mm'):
                return False
    except:
        return False

    return True


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

    array = [[] for key in KEYLIST]
    stream = DataStream()

    fi = os.path.split(filename)[1]

    if fi: # in flist:
        if starttime:
            startdate = datetime.strptime(datetime.strftime(testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d')
        if endtime:
            enddate = datetime.strptime(datetime.strftime(testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d')
        #for fi in flist:
        daystring = fi.split('.')
        try:
            day = datetime.strftime(datetime.strptime(daystring[0], "%b%d%y"),"%Y-%m-%d")
        except:
            logging.warning("format-DIDD: Unusual dateformat in Filename %s" % daystring[0])
            day = datetime.strftime(extract_date_from_string(filename)[0],"%Y-%m-%d")
            pass
            #return stream
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= startdate:
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= enddate:
                getfile = False
    else:
        print("read DIDD Format: no files found in choosen directory")
        return stream

    if getfile:
        fh = open(filename, 'rt')
        orient=[]
        headers = {}

        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line.startswith('hh mm') or line.startswith('%hh %mm'):
                # data header
                line = line.replace('%hh %mm','')
                line = line.replace('hh mm','')
                colsstr = line.lower().split()

                for idx, elem in enumerate(colsstr):
                    colname = "col-%s" % KEYLIST[idx+1]
                    colname = colname.lower()
                    headers[colname] = elem
                    unitstr =  'unit-%s' % colname
                    headers[unitstr] = 'nT'
                    orient.append(KEYLIST[idx+1].upper())
            elif headonly:
                # skip data for option headonly
                continue
            else:
                elem = line.split()
                if len(elem) < 6:
                    fval = np.nan
                else:
                    try:
                        fval = float(elem[5])
                        if np.isnan(fval) or fval > 88887:
                            fval = np.nan
                    except:
                        logging.warning("Fomat-DIDD: error while reading data line: %s from %s" % (line, filename))
                        fval = np.nan
                if not np.isnan(fval):
                    try:
                        array[0].append(datetime.strptime(day+'T'+elem[0]+':'+elem[1],"%Y-%m-%dT%H:%M"))
                        array[1].append(float(elem[2]))
                        array[2].append(float(elem[3]))
                        array[3].append(float(elem[4]))
                        array[4].append(fval)
                    except:
                        logging.warning("Fomat-DIDD: error while reading data line: %s from %s" % (line, filename))
        array[0] = np.asarray(array[0])
        array[1] = np.asarray(array[1])
        array[2] = np.asarray(array[2])
        array[3] = np.asarray(array[3])
        array[4] = np.asarray(array[4])

        headers['DataSensorOrientation'] = "".join(orient)
        stream.header['SensorElements'] = ','.join(colsstr)
        stream.header['SensorKeys'] = ','.join(colsstr)
        headers['unit-col-f'] = 'nT'
        fh.close()
    else:
        headers = stream.header

    return DataStream(header=headers, ndarray=np.asarray(array,dtype=object))



def writeDIDD(datastream, filename, **kwargs):
    """
    Looks like:
    hh mm        X        Y        Z        F
    00 00  20826.8   1206.1  43778.3  48494.8
    00 01  20833.3   1202.2  43779.3  48498.5
    """
    ndtype = False
    if len(datastream.ndarray[0]) > 0:
        ndtype = True
        # don't argue about time range
        pass
    else:
        if (datastream[-1].time - datastream[0].time) > 1:
            print("Writing DIDD format requires daily coverage - choose")
            return False

    sr = datastream.samplingrate()
    if not sr < 62 or not sr > 58:
         print("writeDIDD: currently only minute data is supported")
         return False

    headdict = datastream.header

    try:
        xhead = headdict.get('col-x').upper()
        yhead = headdict.get('col-y').upper()
        zhead = headdict.get('col-z').upper()
        fhead = headdict.get('col-f').upper()
    except:
        xhead = 'X'
        yhead = 'Y'
        zhead = 'Z'
        fhead = 'F'

    if sys.version_info >= (3,0,0):
        myFile = open(filename, 'w', newline='')
    else:
        myFile= open( filename, 'wb' )
    wtr= csv.writer( myFile )
    headline = 'hh mm        '+xhead+'        '+yhead+'        '+zhead+'        '+fhead
    wtr.writerow( [headline] )
    if ndtype:
        for idx,elem in enumerate(datastream.ndarray[0]):
            time = datetime.strftime(num2date(elem).replace(tzinfo=None), "%H %M")
            line = '%s %8.1f %8.1f %8.1f %8.1f' % (time, datastream.ndarray[1][idx], datastream.ndarray[2][idx], datastream.ndarray[3][idx], datastream.ndarray[4][idx])
            wtr.writerow( [line] )
    else:
        for elem in datastream:
            time = datetime.strftime(num2date(elem.time).replace(tzinfo=None), "%H %M")
            line = '%s %8.1f %8.1f %8.1f %8.1f' % (time, elem.x, elem.y, elem.z, elem.f)
            wtr.writerow( [line] )
    myFile.close()
    return True
