"""
MagPy
MagPy input/output filters
Written by Roman Leonhardt June 2012

CSV import filter:

please make sure to adapt the header of the CSV file

"""
from io import open

# Specify what methods are really needed
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import DataStream, read, join_streams, subtract_streams, magpyversion
from magpy.core.methods import testtime, extract_date_from_string
import csv
import numpy as np
import os
import string
import logging
from datetime import datetime, timedelta, timezone
import dateutil.parser

KEYLIST = DataStream().KEYLIST
NUMKEYLIST = DataStream().NUMKEYLIST
logger = logging.getLogger(__name__)


def check_date(date_string):
        dt = False
        try:
            dt = dateutil.parser.parse(date_string)
        except ValueError:
            #print("Invalid isoformat string: {}".format(date_string))
            dt = False
        return dt


def isCSV(filename):
    """
    Checks whether a file is BASIC csv file
    """
    #print ("Checking CSV")
    lines_to_check = 40
    try:
        with open(filename, newline='') as csvfile:
            start = csvfile.read(4096)
            # isprintable does not allow newlines, printable does not allow umlauts...
            if not all([c in string.printable or c.isprintable() for c in start]):
                return False
            dialect = csv.Sniffer().sniff(start)            
    except csv.Error:
        # Could not get a csv dialect -> probably not a csv.
        return False   
    except:
        return False   

    try:
        # Testing comma separation
        N = start.count(',')
        if N < 5:
            return False
    except:
        return False

    try:
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            datelst = []
            for idx,row in enumerate(csv_reader):
                if len(row) > 0 and idx < lines_to_check:
                    dt = check_date(row[0])
                    datelst.append(dt)
        if not any(datelst):
            return False
    except:
        return False

    logger.debug("format_magpy: Found BASIC CSV file {}".format(filename))
    return True


def readCSV(filename, headonly=False, starttime=None, endtime=None, debug=False, **kwargs):
    """
    #DT_datatime,N_latency[ms],N_download[Mbyte/s],N_upload[Mbyte/s],N_serverdistance[km],S_sever,S_location
    #2021-09-20T00:05:02.628946Z,12.585,71.92494097727986,36.592751994082455,52.214518722142806,JStorfingerDE,Munich
    #2021-09-20T00:10:02.852509Z,12.867,73.2147567273973,37.220394235557094,52.214518722142806,InterNetX GmbH,Munich
    """

    if debug:
        print(
            "CSV import: Header needs to look like DT_datetime,N_ping[ms],S_comment")

    getfile = True
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
      with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        fulldata = []
        header = []
        comments = {}
        for row in csv_reader:
            if len(row) > 0:
                dt = check_date(row[0])
                if row[0].startswith('#'):
                    # split, remove spaces
                    if debug:
                        print ("CSV import: Found comment")
                    line = ",".join(row)
                    lin = line.replace('#','').replace(' ','').replace('\t','').strip()
                    li = lin.split(':')
                    try:
                        comments[li[0]] = li[1]
                    except:
                        pass
                elif dt:
                    data = [dt.replace(tzinfo=None)]
                    dat = [el for idx,el in enumerate(row) if idx > 0]
                    data.extend(dat)
                    fulldata.append(data)
                else:
                    if debug:
                        print ("CSV import: Found header")
                    header.append(row)

        # Convert header
        #  use only first line
        #  assign each index to a specific key of DATASTREAM
        numkeys = NUMKEYLIST
        strkeys = ['str1','str2','str3']
        head = header[0]
        assign = {}
        numN = 0
        strN = 0
        for idx,el in enumerate(head):
            headel = el.split('_')
            typus = headel[0]
            if len(headel) > 1:
                elementunit = headel[1].replace("]",'').split("[")
            else:
                if debug:
                    print ("CSV import: Make sure that your header follows the CSV header convention of MagPy")
            if typus.upper() in ['TIME','EPOCH','DT','DATETIME']:
                typus = 'time'
                assign[idx] = 'time'
            else:
                #print (typus, elementunit)
                if typus == 'N':
                    assign[idx] = numkeys[numN]
                    try:
                        comments['col-{}'.format(numkeys[numN])] = elementunit[0]
                    except:
                        comments['col-{}'.format(numkeys[numN])] = numkeys[numN]
                    try:
                        comments['unit-col-{}'.format(numkeys[numN])] = elementunit[1]
                    except:
                        pass
                    numN += 1
                elif typus == 'S':
                    assign[idx] = strkeys[strN]
                    comments['col-{}'.format(strkeys[strN])] = elementunit[0]
                    strN += 1
                else:
                    assign[idx] = numkeys[numN]
                    numN += 1

        # Convert data
        numpy_array = np.array(fulldata)
        transp = numpy_array.T
        transpose_list = transp.tolist()
        array = [[] for el in KEYLIST]
        for idx,el in enumerate(transpose_list):
            pos = KEYLIST.index(assign[idx])
            try:
                array[pos] = np.asarray(el, dtype=float)
            except:
                array[pos] = np.asarray(el, dtype=object)
        array = np.asarray([np.asarray(el).astype(object) for el in array], dtype=object)

        return DataStream(header=comments,ndarray=array)

def writeCSV(datastream, filename, kind='simple',returnstring=False,mode=None, **kwargs):
    """
    Function to write basic CSV data
    """
    tst = ''

    #logger.info("writeBASICCSV: Writing file to %s" % filename)

    if not len(datastream.ndarray[0]) > 0:
        return False

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(exst,datastream,extend=True)
            myFile = open(filename, 'w', newline='')
        elif mode == 'replace': # replace existing inputs
            logger.debug("write ascii filename", filename)
            exst = read(path_or_url=filename)
            datastream = join_streams(datastream,exst,extend=True)
            myFile = open(filename, 'w', newline='')
        elif mode == 'append':
            myFile = open(filename, 'a', newline='')
        else:
            myFile = open(filename, 'w', newline='')
    elif filename.find('StringIO') > -1 and not os.path.isfile(filename):
        import io
        myFile = io.StringIO()
        returnstring = True
    else:
        myFile = open(filename, 'w', newline='')

    wtr= csv.writer( myFile )

    headdict = datastream.header
    head, line = [],[]
    keylst = datastream._get_key_headers()
    keylst[:0] = ['time']

    if not mode == 'append':
        for key in keylst:
            typus = 'S'
            unit = ''
            if key in NUMKEYLIST:
                typus = 'N'
                u = headdict.get('unit-col-'+key,'')
                if u:
                    unit = '[{}]'.format(u)
            if kind == 'simple':
                title = "{}{}".format(headdict.get('col-'+key,'-'), unit)
                tst = ''
            else:
                title = "{}_{}{}".format(typus, headdict.get('col-'+key,'-'), unit)
                tst = 'DT_'
            head.append(title)
        head[0] = '{}Time'.format(tst)
        wtr.writerow( head )
    if len(datastream.ndarray[0]) > 0:
        for i in range(len(datastream.ndarray[0])):
            row = []
            for idx,el in enumerate(datastream.ndarray):
                if len(datastream.ndarray[idx]) > 0:
                    if KEYLIST[idx].find('time') >= 0:
                        row.append((el[i]).replace(tzinfo=None).isoformat()+'Z')
                    else:
                        if not KEYLIST[idx] in NUMKEYLIST: # Get String and replace all non-standard ascii characters
                            try:
                                valid_chars='-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
                                el[i] = ''.join([e for e in list(el[i]) if e in list(valid_chars)])
                            except:
                                pass
                        row.append(el[i])
            wtr.writerow(row)

    if returnstring:
        return myFile
    myFile.close()
    return filename

if __name__ == '__main__':

    import scipy
    import subprocess
    print()
    print("----------------------------------------------------------")
    print("TESTING: CSV FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE BASICCSV LIBRARY.")
    print("All main methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()
    # 1. Creating a test data set of minute resolution and 1 month length
    #    This testdata set will then be transformed into appropriate output formats
    #    and written to a temporary folder by the respective methods. Afterwards it is
    #    reloaded and compared to the original data set
    c = 1000  # 4000 nan values are filled at random places to get some significant data gaps
    l = 88400
    array = [[] for el in DataStream().KEYLIST]
    win = scipy.signal.windows.hann(60)
    a = np.random.uniform(20950, 21000, size=int(l/2))
    b = np.random.uniform(20950, 21050, size=int(l/2))
    x = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
    x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
    array[1] = x[1000:-1000]
    a = np.random.uniform(1950, 2000, size=int(l/2))
    b = np.random.uniform(1900, 2050, size=int(l/2))
    y = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
    y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
    array[2] = y[1000:-1000]
    a = np.random.uniform(44300, 44400, size=l)
    z = scipy.signal.convolve(a, win, mode='same') / sum(win)
    array[3] = z[1000:-1000]
    a = np.random.uniform(49000, 49200, size=l)
    f = scipy.signal.convolve(a, win, mode='same') / sum(win)
    array[4] = f[1000:-1000]
    array[0] = np.asarray([datetime(2022, 11, 1) + timedelta(seconds=i) for i in range(0, len(array[1]))])
    # 2. Creating artificial header information
    header = {}
    header['DataSamplingRate'] = 1
    header['SensorID'] = 'Test_0001_0002'
    header['StationIAGAcode'] = 'XXX'
    header['DataAcquisitionLatitude'] = 48.123
    header['DataAcquisitionLongitude'] = 15.999
    header['DataElevation'] = 1090
    header['DataComponents'] = 'XYZS'
    header['StationInstitution'] = 'TheWatsonObservatory'
    header['DataDigitalSampling'] = '1 Hz'
    header['DataSensorOrientation'] = 'HEZ'
    header['StationName'] = 'Holmes'

    teststream = DataStream(header=header, ndarray=np.asarray(array, dtype=object))


    errors = {}
    successes = {}
    testrun = 'STREAMTESTFILE'
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)

    while True:
        testset = 'CSV'
        try:
            filename = os.path.join('/tmp','{}_{}_{}'.format(testrun, testset, datetime.strftime(t_start_test,'%Y%m%d-%H%M')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            succ1 = writeCSV(teststream, filename)
            succ2 = isCSV(filename)
            dat = readCSV(filename)
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            diff = subtract_streams(teststream, dat, debug=True)
            xm = diff.mean('x')
            ym = diff.mean('y')
            zm = diff.mean('z')
            fm = diff.mean('f')
            if np.abs(xm) > 0.00001 or np.abs(ym) > 0.00001 or np.abs(zm) > 0.00001 or np.abs(fm) > 0.00001:
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
