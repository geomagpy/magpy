"""
MagPy
MagPy input/output filters
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

# Specify what methods are really needed
from magpy.stream import *

import logging
logger = logging.getLogger(__name__)

import gc

def isCOMMATXT(filename):
    """
    Checks whether a file is comma separeted ASCII format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.find('# MagPy TXT') > -1:
        return False

    logger.debug("format_magpy: Found simple ASCII file %s" % filename)
    return True


def readCOMMATXT(filename, headonly=False, **kwargs):
    """
    Reading a simple comma separated text file .
    Should contain the following:
     - first line
        # MagPy TXT
     - Optional meta information
        # SensorID MySensor_887689_0001
        # StationID WIC
        # StationLongitude 11.234
        # StationLatitude 48.234
        # Columns Temp,Voltage
        # Units deg,V
     - Data block
        2013-03-01T01:00:00.000000Z,14.940719529965,11.869002442573095

    """
    stream = DataStream([],{})

    array = [[] for key in KEYLIST]

    headers = {}

    logger.info('readCOMMATXT: Reading %s' % (filename))

    qFile= open( filename, "r", newline='' )

    csvReader= csv.reader( qFile )
    keylst = []
    timeconv = False
    timecol = -1

    for elem in csvReader:
        if elem==[]:
            # blank line
            pass
        elif elem[0].startswith('#') and not elem[0].startswith('# MagPy TXT'):
            # blank header
            headlst = elem.split(' ')
            if len(headlst) >= 3:
                key = headlst[1]
                if not key in ['Columns','Units']:
                    value = " ".join(headlst[2:])
                else:
                    vallst = headlst[2].split(',')
                    key = 'SensorElements'
        elif elem[0].startswith('# MagPy TXT'):
            pass
        elif headonly:
            # skip data for option headonly
            continue
        else:
            print (elem)
            try:
                if timeconv:
                    ti = date2num(stream._testtime(elem[timecol]))
                else:
                    ti = elem[timecol]
                array[0].append(ti)
                for idx,i in enumerate(keylst):
                    array[idx+1].append(float(elem[i]))
                    #print NUMKEYLIST[idx]
            except ValueError:
                pass
    qFile.close()

    # Clean up the file contents
    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    for idx,ar in enumerate(array):
        if len(ar) > 0:
            if KEYLIST[idx] in NUMKEYLIST:
                tester = float('nan')
            else:
                tester = '-'
            array[idx] = np.asarray(array[idx])
            if not False in checkEqual3(array[idx]) and ar[0] == tester:
                array[idx] = np.asarray([])

    headers['DataFormat'] = 'MagPy-TXT-v1.0'
    if headers.get('SensorID','') == '':
        headers['SensorID'] = 'unkown'

    return DataStream([LineStruct()], headers, np.asarray(array).astype(object))


def writeCOMMATXT(datastream, filename, **kwargs):
    """
    Function to write basic ASCII data
    """

    mode = kwargs.get('mode')
    logger.info("writePYASCII: Writing file to %s" % filename)
    returnstring = False

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream,extend=True)
            if sys.version_info >= (3,0,0):
                myFile = open(filename, 'w', newline='')
            else:
                myFile = open(filename, 'wb')
        elif mode == 'replace': # replace existing inputs
            logger.debug("write ascii filename", filename)
            exst = read(path_or_url=filename)
            datastream = joinStreams(datastream,exst,extend=True)
            if sys.version_info >= (3,0,0):
                myFile = open(filename, 'w', newline='')
            else:
                myFile = open(filename, 'wb')
        elif mode == 'append':
            if sys.version_info >= (3,0,0):
                myFile = open(filename, 'a', newline='')
            else:
                myFile = open(filename, 'ab')
        else:
            if sys.version_info >= (3,0,0):
                myFile = open(filename, 'w', newline='')
            else:
                myFile = open(filename, 'wb')
    elif filename.find('StringIO') > -1 and not os.path.isfile(filename):
        if sys.version_info >= (3,0,0):
            import io
            myFile = io.StringIO()
            returnstring = True
        else:
            import StringIO
            myFile = StringIO.StringIO()
            returnstring = True
    else:
        if sys.version_info >= (3,0,0):
            myFile = open(filename, 'w', newline='')
        else:
            myFile = open(filename, 'wb')

    wtr= csv.writer( myFile )
    headdict = datastream.header
    head, line = [],[]

    keylst = datastream._get_key_headers()
    keylst[:0] = ['time']

    if not mode == 'append':
        for key in keylst:
            title = headdict.get('col-'+key,'-') + '[' + headdict.get('unit-col-'+key,'') + ']'
            head.append(title)
        head[0] = 'Time'
        headnew = ['Time-days']
        headnew.extend(head)
        head = headnew
        wtr.writerow( [' # MagPy ASCII'] )
        wtr.writerow( head )
    if len(datastream.ndarray[0]) > 0:
        for i in range(len(datastream.ndarray[0])):
            row = []
            for idx,el in enumerate(datastream.ndarray):
                if len(datastream.ndarray[idx]) > 0:
                    if KEYLIST[idx].find('time') >= 0:
                        #print el[i]
                        row.append(float(el[i]))
                        row.append(datetime.strftime(num2date(float(el[i])).replace(tzinfo=None), "%Y-%m-%dT%H:%M:%S.%f") )
                    else:
                        if not KEYLIST[idx] in NUMKEYLIST: # Get String and replace all non-standard ascii characters
                            try:
                                valid_chars='-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
                                el[i] = ''.join([e for e in list(el[i]) if e in list(valid_chars)])
                            except:
                                pass
                        row.append(el[i])
            wtr.writerow(row)
    else:
        for elem in datastream:
            row = []
            for key in keylst:
                if key.find('time') >= 0:
                    row.append(elem.time)
                    try:
                        row.append( datetime.strftime(num2date(eval('elem.'+key)).replace(tzinfo=None), "%Y-%m-%dT%H:%M:%S.%f") )
                    except:
                        row.append( float('nan') )
                        pass
                else:
                    row.append(eval('elem.'+key))
            wtr.writerow( row )

    if returnstring:
        return myFile

    myFile.close()
    return filename
