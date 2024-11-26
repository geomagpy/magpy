"""
MagPy
dIdD input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import DataStream, read, subtract_streams, join_streams, magpyversion
from datetime import datetime, timedelta, timezone
import os
import csv
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
    debug = kwargs.get('debug')
    getfile = True

    array = [[] for key in KEYLIST]
    stream = DataStream()

    day = extract_date_from_string(filename)
    theday = day[0].strftime("%Y-%m-%d")
    try:
        if starttime:
            if not day[-1] >= datetime.date(testtime(starttime)):
                getfile = False
        if endtime:
            if not day[0] <= datetime.date(testtime(endtime)):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True

    if debug:
        print ("Reading DIDD file for", day, getfile)
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
                        array[0].append(datetime.strptime(theday+'T'+elem[0]+':'+elem[1],"%Y-%m-%dT%H:%M"))
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
    if not len(datastream.ndarray[0]) > 0:
        print ("DIDD format: No data to be written")
        return False
    sr = datastream.samplingrate()
    if not sr < 61 or not sr > 59:
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

    myFile = open(filename, 'w', newline='')
    wtr= csv.writer( myFile )
    headline = 'hh mm        '+xhead+'        '+yhead+'        '+zhead+'        '+fhead
    wtr.writerow( [headline] )
    for idx,elem in enumerate(datastream.ndarray[0]):
            time = datetime.strftime(elem.replace(tzinfo=None), "%H %M")
            line = '%s %8.1f %8.1f %8.1f %8.1f' % (time, datastream.ndarray[1][idx], datastream.ndarray[2][idx], datastream.ndarray[3][idx], datastream.ndarray[4][idx])
            wtr.writerow( [line] )
    myFile.close()
    return True


if __name__ == '__main__':

    from scipy import signal
    import subprocess
    print()
    print("----------------------------------------------------------")
    print("TESTING: DIDD FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE DIDD LIBRARY.")
    print("All main methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()
    # 1. Creating a test data set of minute resolution and 1 month length
    #    This testdata set will then be transformed into appropriate output formats
    #    and written to a temporary folder by the respective methods. Afterwards it is
    #    reloaded and compared to the original data set
    def create_minteststream(startdate=datetime(2022, 11, 1), addnan=True):
        c = 1000  # 4000 nan values are filled at random places to get some significant data gaps
        l = 32 * 1440
        #import scipy
        teststream = DataStream()
        array = [[] for el in DataStream().KEYLIST]
        win = signal.windows.hann(60)
        a = np.random.uniform(20950, 21000, size=int(l / 2))
        b = np.random.uniform(20950, 21050, size=int(l / 2))
        x = signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        if addnan:
            x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
        array[1] = x[1440:-1440]
        a = np.random.uniform(1950, 2000, size=int(l / 2))
        b = np.random.uniform(1900, 2050, size=int(l / 2))
        y = signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        if addnan:
            y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
        array[2] = y[1440:-1440]
        a = np.random.uniform(44300, 44400, size=l)
        z = signal.convolve(a, win, mode='same') / sum(win)
        array[3] = z[1440:-1440]
        array[4] = np.sqrt((x * x) + (y * y) + (z * z))[1440:-1440]
        array[0] = np.asarray([startdate + timedelta(minutes=i) for i in range(0, len(array[1]))])
        teststream = DataStream(header={'SensorID': 'Test_0001_0001'}, ndarray=np.asarray(array, dtype=object))
        minstream = teststream.filter()
        teststream.header['col-x'] = 'X'
        teststream.header['col-y'] = 'Y'
        teststream.header['col-z'] = 'Z'
        teststream.header['col-f'] = 'F'
        teststream.header['unit-col-x'] = 'nT'
        teststream.header['unit-col-y'] = 'nT'
        teststream.header['unit-col-z'] = 'nT'
        teststream.header['unit-col-f'] = 'nT'
        return teststream

    teststream = create_minteststream(addnan=False)
    teststream = teststream.trim('2022-11-22','2022-11-23')

    errors = {}
    successes = {}
    testrun = 'MAGPYTESTFILE'
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)

    while True:
        testset = 'DIDD'
        try:
            filename = os.path.join('/tmp','{}_{}.dat'.format(testrun, datetime.strftime(teststream.start(),'%b%d%y')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            succ1 = writeDIDD(teststream, filename)
            succ2 = isDIDD(filename)
            dat = readDIDD(filename)
            if not len(dat) > 0:
                raise Exception("Error - no data could be read")
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            diff = subtract_streams(teststream, dat, debug=True)
            xm = diff.mean('x')
            ym = diff.mean('y')
            zm = diff.mean('z')
            fm = diff.mean('f')
            # agreement should be better than 0.01 nT as resolution is 0.1 nT in file
            if np.abs(xm) > 0.01 or np.abs(ym) > 0.01 or np.abs(zm) > 0.01 or np.abs(fm) > 0.01:
                 raise Exception("ERROR within data validity test")
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))

        break

    t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
    time_taken = t_end_test - t_start_test
    print(datetime.now(timezone.utc).replace(tzinfo=None), "- Stream testing completed in {} s. Results below.".format(time_taken.total_seconds()))

    print()
    print("----------------------------------------------------------")
    del_test_files = 'rm {}*'.format(os.path.join('/tmp',testrun))
    subprocess.call(del_test_files,shell=True)
    for item in successes:
        print ("{} :     {}".format(item, successes.get(item)))
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(" {}".format(errors.keys()))
        print()
        for item in errors:
                print(item + " error string:")
                print("    " + errors.get(item))
    print()
    print("Good-bye!")
    print("----------------------------------------------------------")
