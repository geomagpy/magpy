"""
MagPy
dIdD input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from magpy.stream import *
from io import open


def isDIDD(filename):
    """
    Checks whether a file is ASCII DIDD (Tihany) format.
    """
    try:
        temp = open(filename, 'rt').readline()
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
    stream = DataStream([],{},np.asarray(array))

    ### Speed up test - not faster as traditional method (at least not for small DIDD files)
    # 1. get a filelist
    #    and limit the filelist to matching date ranges
    #dirname = os.path.dirname(filename)
    #flist = []
    #for (dirpath, dirnames, filenames) in os.walk(dirname):
    #    flist.extend(filenames)
    #    break

    fi = os.path.split(filename)[1]

    if fi: # in flist:
        if starttime:
            startdate = datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d')
        if endtime:
            enddate = datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d')
        #for fi in flist:
        daystring = fi.split('.')
        try:
            day = datetime.strftime(datetime.strptime(daystring[0], "%b%d%y"),"%Y-%m-%d")
        except:
            logging.warning("format-DIDD: Unusual dateformat in Filename %s" % daystring[0])
            day = datetime.strftime(extractDateFromString(filename)[0],"%Y-%m-%d")
            pass
            #return stream
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= startdate:
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= enddate:
                getfile = False
        #print daystring, day, startdate, enddate, getfile
    else:
        print("read DIDD Format: no files found in choosen directory")
        return stream

    if getfile:
        fh = open(filename, 'rt')
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
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                elem = line.split()
                if len(elem) < 6:
                    #fval = 9999  # why 9999 ??
                    fval = float('nan')
                else:
                    try:
                        fval = float(elem[5])
                        if np.isnan(fval):
                            fval = 88888.0
                    except:
                        logging.warning("Fomat-DIDD: error while reading data line: %s from %s" % (line, filename))
                        fval = float('nan')
                if not np.isnan(fval):
                    try:
                        array[0].append(date2num(datetime.strptime(day+'T'+elem[0]+':'+elem[1],"%Y-%m-%dT%H:%M")))
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

        headers['DataSensorOrientation'] = 'xyz'
        stream.header['SensorElements'] = ','.join(colsstr)
        stream.header['SensorKeys'] = ','.join(colsstr)
        headers['unit-col-f'] = 'nT'
        fh.close()
    else:
        headers = stream.header
        stream = []

    stream = [LineStruct()]

    return DataStream(stream, headers, np.asarray(array))



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
