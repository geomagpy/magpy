"""
MagPy
MagPy input/output filters
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from io import open

# Specify what methods are really needed
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import DataStream, read, join_streams, subtract_streams, magpyversion
from magpy.core.methods import testtime, extract_date_from_string
from magpy.core import flagging
import datetime
from datetime import datetime, timedelta, timezone
import numpy as np
import os
import json
import struct
import csv
import logging
logger = logging.getLogger(__name__)

KEYLIST = DataStream().KEYLIST
NUMKEYLIST = DataStream().NUMKEYLIST
#import gc


# K0 (Browsing - not for Serious Science) ACE-EPAM data from the OMNI database:
k0_epm_KEYDICT = {#'H_lo',                      # H (0.48-0.97 MeV)     (UNUSED)
                'Ion_very_lo': 'var1',          # Ion (47-65 keV) 1/(cm2 s ster MeV)
                'Ion_lo': 'var2',               # Ion (310-580 keV) 1/(cm2 s ster MeV)
                'Ion_mid': 'var3',              # Ion (310-580 keV) 1/(cm2 s ster MeV)
                'Ion_hi': 'var5',               # Ion (1060-1910 keV) 1/(cm2 s ster MeV)
                'Electron_lo': 'z',             # Electron (38-53 keV) 1/(cm2 s ster MeV)
                'Electron_hi': 'f'              # Electron (175-315 keV) 1/(cm2 s ster MeV)
                   }
# H1 (Level 2 final 5min data) ACE-EPAM data from the OMNI database:
h1_epm_KEYDICT = {
                'P1': 'var1',                   # Ion (47-65 keV) 1/(cm2 s ster MeV)
                'P3': 'var2',                   # Ion (115-195 keV) 1/(cm2 s ster MeV)
                'P5': 'var3',                   # Ion (310-580 keV) 1/(cm2 s ster MeV)
                'P7': 'var5',                   # Ion (1060-1910 keV) 1/(cm2 s ster MeV)
                'DE1': 'z',                     # Electron (38-53 keV) 1/(cm2 s ster MeV)
                'DE4': 'f'                      # Electron (175-315 keV) 1/(cm2 s ster MeV)
                # (... Many, MANY other unused keys.)
                   }
# H0 (Level 2 final 64s data) ACE-SWEPAM data from the OMNI database:
h0_swe_KEYDICT = {
                'Np': 'var1',                   # H_Density #/cc
                'Vp': 'var2',                   # SW_H_Speed km/s
                'Tpr': 'var3',                  # H_Temp_radial Kelvin
                # (... Many other keys unused.)
                   }
# H0 (Level 2 final 16s data) ACE-MAG data from the OMNI database:
h0_mfi_KEYDICT = {
                'Magnitude': 'f',               # B-field total magnitude (Bt)
                'BGSM': ['x','y','z'],          # B-field in GSM coordinates (Bx, By, Bz)
                #'BGSEc': ['x','y','z'],        # B-field in GSE coordinates
                # (... Many other keys unused.)
                   }

def isPYSTR(filename):
    """
    Checks whether a file is ASCII PyStr format.
    """
    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
    except:
        return False
    if not temp.startswith(' # MagPy - ASCII'):
        return False

    logger.debug("format_magpy: Found PYSTR file %s" % filename)
    return True


def isPYASCII(filename):
    """
    Checks whether a file is ASCII PyStr format.
    """
    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
    except:
        return False
    if not temp.find('# MagPy ASCII') > -1:
        return False

    logger.debug("format_magpy: Found PYASCII file %s" % filename)
    return True


def isPYBIN(filename):
    """
    Checks whether a file is binary PyStr format.
    """
    try:
        with open(filename, 'r', encoding='utf-8', newline='', errors='ignore') as fi:
            temp = fi.readline()
        #temp = open(filename, 'r', encoding='utf-8', newline='', errors='ignore').readline()
    except:
        return False
    if not temp.startswith('# MagPyBin'):
        return False

    logger.debug("format_magpy: Found PYBIN file %s" % filename)
    return True


def readPYASCII(filename, headonly=False, **kwargs):
    """
    Reading basic ASCII format data.
    Should look like:
         # MagPy ASCII
        Time-days,Time,Temp[deg],Voltage[V]
        734928.0416666666,2013-03-01T01:00:00.000000,14061.940719529965,6.8539941994665305,11.869002442573095

    """
    stream = DataStream()

    array = [[] for key in KEYLIST]

    headers = {}

    logger.info('readPYASCII: Reading %s' % (filename))

    with open(filename, "r", newline='' ) as csv_file:
        csv_reader = csv.reader(csv_file)

        keylst = []
        timeconv = False
        timecol = -1
        minimumlinelength = 2

        for elem in csv_reader:
            #print (elem)
            if elem==[]:
                # blank line
                pass
            elif elem[0].startswith('#'):
                # blank header
                pass
            elif elem[0].startswith(' #') and not elem[0].startswith(' # MagPy ASCII'):
                # attributes - assign header values
                headlst = elem[0].strip(' # ').split(':')
                headkey = headlst[0]
                headval = headlst[1]
                if not headkey.startswith('Column'):
                    headers[headkey] = headval.strip()
            elif elem[0].startswith(' # MagPy ASCII'):
                # blank header
                pass
            elif elem[0].startswith('Time'): # extract column info and keys
                for i in range(len(elem)):
                    #print elem[i]
                    if not elem[i].startswith('Time'):
                        try:  # neglecte columns without units (e.g. text)
                             headval = elem[i].split('[')
                             colval = headval[0]
                             unitval = headval[1].strip(']')
                             exec('headers["col-'+NUMKEYLIST[len(keylst)]+'"] = colval')
                             exec('headers["unit-col-'+NUMKEYLIST[len(keylst)]+'"] = unitval')
                             keylst.append(i)
                        except:
                             pass
                    elif elem[i] == 'Time' and not timecol > 0:
                        timecol = i
                        timeconv = True
                    #elif elem[i] == 'Time-days':  # ignore this column from 2.0 onwards
                    #    timecol = i
                    #    timeconv = False
                if len(keylst) > len(NUMKEYLIST):
                    keylst = keylst[:len(NUMKEYLIST)]
            elif headonly:
                # skip data for option headonly
                continue
            elif len(elem) < minimumlinelength:
                pass
            else:
                try:
                    if timeconv:
                        ti = testtime(elem[timecol])
                    else:
                        ti = elem[timecol]
                    array[0].append(ti)
                    for idx,i in enumerate(keylst):
                        array[idx+1].append(float(elem[i]))
                        #print NUMKEYLIST[idx]
                except ValueError:
                    pass

    # Clean up the file contents
    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    for idx,ar in enumerate(array):
        if len(ar) > 0:
            if KEYLIST[idx] in NUMKEYLIST:
                tester = np.nan
            else:
                tester = '-'
            array[idx] = np.asarray(array[idx])
            if not False in checkEqual3(array[idx]) and ar[0] == tester:
                array[idx] = np.asarray([])

    headers['DataFormat'] = 'MagPy-ASCII-v1.0'
    if headers.get('SensorID','') == '':
        headers['SensorID'] = 'unknown_12345_0001'

    return DataStream(header=headers, ndarray=np.asarray(array,dtype=object))



def readPYSTR(filename, headonly=False, **kwargs):
    """
    Reading ASCII PyMagStructure format data.
    """
    debug = kwargs.get('debug')
    stream = DataStream([],{})

    array = [[] for key in KEYLIST]

    # Check whether header infromation is already present
    headers={}

    logger.info('readPYSTR: Reading %s' % (filename))
    #qFile= file( filename, "rb" )
    qFile= open( filename, "rt", newline='' )
    csvReader= csv.reader( qFile )

    for elem in csvReader:
        if elem==[]:
            # blank line
            pass
        elif elem[0].startswith('#'):
            # blank header
            pass
        elif elem[0].startswith(' #') and not elem[0].startswith(' # MagPy - ASCII'):
            # attributes - assign header values
            headlst = elem[0].strip(' # ').split(':')
            headkey = headlst[0]
            headval = headlst[1]
            if not headkey.startswith('Column') and not headkey in ['DataAbsFunctionObject','DataBaseValues', 'DataFlags','DataFunctionObject']:
                headers[headkey] = headval.strip()
            elif headkey in ['DataFlags']:
                flstr = elem[0].strip(' # DataFlags: ')
                fl = json.loads(flstr, object_hook=_dateparser)
                headers[headkey] = flagging.Flags(fl)
        elif elem[0].startswith(' # MagPy - ASCII'):
            # blank header
            pass
        elif elem[0]=='Epoch[]' or elem[0]=='-[]' or elem[0]=='time[]':
            for i in range(len(elem)):
                headval = elem[i].split('[')
                colval = headval[0]
                unitval = headval[1].strip(']')
                exec('headers["col-'+KEYLIST[i]+'"] = colval')
                exec('headers["unit-col-'+KEYLIST[i]+'"] = unitval')
        elif headonly:
            # skip data for option headonly
            continue
        else:
            try:
                if not len(elem) == len(KEYLIST):
                    print("readPYSTR: Warning file contents do not fit to KEYLIST - content {a}, KEYLIST {b}".format(a=len(elem), b=len(KEYLIST)))
                for idx, key in enumerate(KEYLIST):
                    if key.find('time') >= 0:
                        try:
                            ti = datetime.strptime(elem[idx],"%Y-%m-%d-%H:%M:%S.%f")
                        except:
                            try:
                                ti = datetime.strptime(elem[idx],"%Y-%m-%dT%H:%M:%S.%f")
                            except:
                                ti = elem[idx]
                                pass
                                #raise ValueError, "Wrong date format in file %s" % filename
                        array[idx].append(ti)
                    else:
                        if key in NUMKEYLIST:
                            if elem[idx] in ['-','']:
                                elem[idx]=np.nan
                            array[idx].append(float(elem[idx]))
                        else:
                            array[idx].append(elem[idx])
            except ValueError:
                print("readPYSTR: Found value error when reading data")
                pass
    qFile.close()

    # Clean up the file contents
    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    if len(array[0]) > 0:
        for idx,ar in enumerate(array):
            if KEYLIST[idx] in NUMKEYLIST or KEYLIST[idx] == 'time':
                tester = np.nan
            else:
                tester = '-'
            array[idx] = np.asarray(array[idx],dtype=object)
            if not False in checkEqual3(array[idx]) and ar[0] == tester:
                array[idx] = np.asarray([])

    result = DataStream(header=headers,ndarray=np.asarray(array,dtype=object))
    if len(result._get_column('flag')) > 1 and not result.header.get('DataFlags'):
        result.header['DataFlags'] = flagging.extract_flags(result, debug=debug)

    return result


def readPYBIN(filename, headonly=False, **kwargs):
    """
    Read binary format of the MagPy package
    Binary formatted data consists of an ascii header line and a binary body containing data
    The header line contains the following space separated inputs:
        1. Format specification: preset to 'MagPyBin' -> used to identify file type in MagPy
        2. SensorID: required
        3. MagPy-Keys: list defining the related magpy keys under which data is stored e.g. ['x','y','z','t1','var5'] - check KEYLIST for available keys
        4. ListSpecification: list defining the variables stored under the keys e.g. ['H','D','Z','DewPoint','Kp']
        5. UnitSpecification: list defining units e.g. ['nT','deg','nT','deg C','']
        6. Multiplier: list defining multipiers used before packing of columns - value divided by multipl. returns correct units in 5 e.g. [100,100,100,1000,1]
        7. Packingcode: defined by pythons struct.pack always starts with 6hL e.g. 6hLLLLLL
        8. Packingsize: size of packing code
        9. Special format specification - optional
            in this case ... (important for high frequency records)
            to be written
        Important: lists 3,4,5,6 must be of identical length
    The data section is packed accoring to the packing code using struct.pack
    """
    keylist = kwargs.get('keylist') # required for very old format, does not affect other formats
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debug = kwargs.get('debug')

    getfile = True
    elemlist = []
    unitlist = []
    multilist = []
    array = [[] for key in KEYLIST]


    stream = DataStream([],{},[[] for key in KEYLIST])

    headskip = False
    if stream.header == None:
        stream.header.clear()
    else:
        headskip = True

    if debug:
        print ("PYBIN: reading data ...")

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
    logbaddata = False

    #t1 = datetime.now(timezone.utc).replace(tzinfo=None)
    if getfile:
        logger.info("readPYBIN: %s Format: PYBIN" % filename)
        if debug:
            print ("readPYBIN: {} Format: PYBIN".format(filename))

        fh = open(filename, 'rb')
        #fh = open(filename, 'r', encoding='utf-8', newline='', errors='ignore')
        #infile = open(filename, 'r', encoding='utf-8', newline='')
        # read header line and extract packing format
        header = fh.readline()
        header = header.decode('utf-8')
        # some cleaning actions for false header inputs
        header = header.replace(', ',',')
        header = header.replace('deg C','deg')
        h_elem = header.strip().split()
        logger.debug("PYBIN: Header {}".format(header))
        if debug:
            print ("PYBIN: Header {}".format(header))

        logger.debug('readPYBIN- debug header type (len should be 9): {}, {}'.format(h_elem, len(h_elem)))
        if debug:
            print ('readPYBIN: debug header type (len should be 9): {}, {}'.format(h_elem, len(h_elem)))

        if not h_elem[1] == 'MagPyBin':
            logger.error('readPYBIN: No MagPyBin format - aborting')
            return
        #print "Length ", len(h_elem), h_elem[2]

        #Test whether element 3,4,5 (and 6) are lists of equal length
        if len(h_elem) == 8:
            stream.header['DataFormat'] = 'MagPy-BIN-v1.0'
            nospecial = True
            try:
                if not keylist:
                    logger.error('readPYBIN: keylist of length(elemlist) needs to be specified')
                    return stream
                elemlist = h_elem[3].strip('[').strip(']').split(',')
                unitlist = h_elem[4].strip('[').strip(']').split(',')
                multilist = list(map(float,h_elem[5].strip('[').strip(']').split(',')))
            except:
                logger.error("readPYBIN: Could not extract lists from header - check format - aborting...")
                return stream
            if not len(keylist) == len(elemlist) or not len(keylist) == len(unitlist) or not  len(keylist) == len(multilist):
                logger.error("readPYBIN: Provided lists from header of differenet lengths - check format - aborting...")
                return stream
        elif len(h_elem) == 9:
            stream.header['DataFormat'] = 'MagPy-BIN-v1.1'
            nospecial = True
            try:
                keylist = h_elem[3].strip('[').strip(']').split(',')
                elemlist = h_elem[4].strip('[').strip(']').split(',')
                unitlist = h_elem[5].strip('[').strip(']').split(',')
                multilist = list(map(float,h_elem[6].strip('[').strip(']').split(',')))
            except:
                logger.error("readPYBIN: Could not extract lists from header - check format - aborting...")
                return stream
            if not len(keylist) == len(elemlist) or not len(keylist) == len(unitlist) or not  len(keylist) == len(multilist):
                if debug:
                    print('readPYBIN- header list error:', len(keylist), len(elemlist), len(unitlist), len(multilist))
                logger.error("readPYBIN: Provided lists from header of differenet lengths - check format - aborting...")
                return stream
        elif len(h_elem) == 10:
            stream.header['DataFormat'] = 'MagPy-BIN-v1.S'
            logger.info("readPYBIN: Special format detected. May not be able to read file.")
            nospecial = False
            if h_elem[2][:5] == 'ENV05' or h_elem[2] == 'Env05':
                keylist = h_elem[3].strip('[').strip(']').split(',')
                elemlist = h_elem[4].strip('[').strip(']').split(',')
                unitlist = h_elem[5].strip('[').strip(']').split(',')
                multilist = list(map(float,h_elem[7].strip('[').strip(']').split(',')))
                nospecial = True
        else:
            logger.error('readPYBIN: No valid MagPyBin format, inadequate header length - aborting')
            if debug:
                print ('readPYBIN: No valid MagPyBin format, inadequate header length - aborting')
            return stream

        logger.debug('readPYBIN: checking code')
        if debug:
            print ("readPYBIN: checking code: {}".format(len(h_elem)))

        packstr = '<'+h_elem[-2]+'B'
        #packstr = packstr.encode('ascii','ignore')
        lengthcode = struct.calcsize(packstr)
        lengthgiven = int(h_elem[-1])+1
        length = lengthgiven
        if not lengthcode == lengthgiven:
            logger.warning("readPYBIN: Giving bit length of packing code ({}) and actual length ({}) differ - Check your packing code!".format(lengthcode,length))
            if lengthcode < lengthgiven:
                missings = lengthgiven-lengthcode
                for i in range(missings):
                    packstr += 'B'
                    length = lengthgiven
            else:
                length = lengthcode

        packstr = packstr.encode('ascii','ignore')

        logger.debug('readPYBIN: unpack info: {}, {}, {}'.format(packstr, lengthcode, lengthgiven))
        if debug:
            print ('readPYBIN: unpack info: {}, {}, {}'.format(packstr, lengthcode, lengthgiven))

        #fh = open(filename, 'rb')
        line = fh.read(length)
        stream.header['SensorID'] = h_elem[2]
        stream.header['SensorElements'] = ','.join(elemlist)
        stream.header['SensorKeys'] = ','.join(keylist)
        lenel = len([el for el in elemlist if el in KEYLIST]) # If elemlist and Keylist are disorderd
        lenke = len([el for el in keylist if el in KEYLIST])
        if lenel > lenke:
            keylist = elemlist

        array = [[] for key in KEYLIST]
        if nospecial:
            logger.debug('readPYBIN- debug found line')

            for idx, elem in enumerate(keylist):
                stream.header['col-'+elem] = elemlist[idx]
                stream.header['unit-col-'+elem] = unitlist[idx]
                # Header info
                pass
            while not len(line) == 0:
                lastdata = 'None'
                data = 'None'
                try:
                    data= struct.unpack(packstr, line)
                except:
                    logger.error("readPYBIN: struct error {} {}".format(filename, len(line)))
                    if debug:
                        print ("readPYBIN: struct error {} {}".format(filename, len(line)))
                try:
                    time = datetime(data[0],data[1],data[2],data[3],data[4],data[5],data[6])
                    array[0].append(testtime(time))
                    # check elemlist and keylist
                    for idx, elem in enumerate(keylist):
                        try:
                            index = KEYLIST.index(elem)
                            if not elem.endswith('time'):
                                if elem in NUMKEYLIST:
                                    array[index].append(data[idx+7]/float(multilist[idx]))
                                else:
                                    array[index].append(data[idx+7])
                            else:
                                try:
                                    sectime = datetime(data[idx+7],data[idx+8],data[idx+9],data[idx+10],data[idx+11],data[idx+12],data[idx+13])
                                    array[index].append(testtime(sectime))
                                except:
                                    pass
                        except:
                            if elem.endswith('time'):
                                try:
                                    sectime = datetime(data[idx+7],data[idx+8],data[idx+9],data[idx+10],data[idx+11],data[idx+12],data[idx+13])
                                    index = KEYLIST.index('sectime')
                                    array[index].append(testtime(sectime))
                                except:
                                    pass
                    if logbaddata == True:
                        logger.error("readPYBIN: Good data resumes with: %s" % str(data))
                        logbaddata = False
                except:
                    logger.error("readPYBIN: Error in line while reading data file. Last line at: %s" % str(lastdata))
                    logbaddata = True
                lastdata = data
                line = fh.read(length)
        else:
            print("Not implemented")
            pass

        if debug:
            print ('readPYBIN: array: {}'.format(len(array)))

        array = [np.asarray(el,dtype=object) for el in array]

    stream.header["DataFormat"] = "PYBIN"

    return DataStream(header=stream.header, ndarray=np.asarray(array,dtype=object))


def _dateparser(dct):
    # Convert dates in dictionary to datetime objects
    for (key, value) in dct.items():
        if str(value).count('-') + str(value).count(':') == 4:
            try:
                try:
                    value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
                except:
                    value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except:
                pass
        dct[key] = value
    return dct


def _dateconv(d):
    # Converter to serialize datetime objects in json
    if isinstance(d, datetime):
        return d.__str__()

def writePYSTR(datastream, filename, **kwargs):
    """
    Function to write structural ASCII data
    """

    mode = kwargs.get('mode')
    logger.info("writePYSTR: Writing file to %s" % filename)

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            try:
                exst = read(path_or_url=filename)
                datastream = join_streams(exst,datastream)
            except:
                logger.info("writePYSTR: Could not interprete existing file - replacing %s" % filename)
            if sys.version_info >= (3,0,0):
                myFile= open( filename, "w", newline='' )
            else:
                myFile= open( filename, "wb")
        elif mode == 'replace': # replace existing inputs
            try:
                exst = read(path_or_url=filename)
                datastream = join_streams(datastream,exst)
            except:
                logger.info("writePYSTR: Could not interprete existing file - replacing %s" % filename)
            if sys.version_info >= (3,0,0):
                myFile= open( filename, "w", newline='' )
            else:
                myFile= open( filename, "wb")
        elif mode == 'append':
            if sys.version_info >= (3,0,0):
                myFile= open( filename, "a", newline='' )
            else:
                myFile= open( filename, "ab")
        else:
            if sys.version_info >= (3,0,0):
                myFile= open( filename, "w", newline='' )
            else:
                myFile= open( filename, "wb")
    else:
        if sys.version_info >= (3,0,0):
            myFile= open( filename, "w", newline='' )
        else:
            myFile= open( filename, "wb")
    wtr= csv.writer( myFile )
    headdict = datastream.header
    head, line = [],[]
    if not mode == 'append':
        wtr.writerow( [' # MagPy - ASCII'] )
        for key in headdict:
            if not key.find('col') >= 0 and not key in ['DataAbsFunctionObject','DataBaseValues', 'DataFlags','DataFunctionObject']:
                line = [' # ' + key +':  ' + str(headdict[key]).strip()]
                wtr.writerow( line )
            elif key in 'DataFlags':
                mydic = headdict[key]
                flagcont = json.dumps(mydic.flagdict, default=_dateconv)
                line = [' # ' + key + ':  ' + flagcont]
                wtr.writerow(line)
        wtr.writerow( ['# head:'] )
        for key in KEYLIST:
            title = headdict.get('col-'+key,'-') + '[' + headdict.get('unit-col-'+key,'') + ']'
            head.append(title)
        wtr.writerow( head )
        wtr.writerow( ['# data:'] )

    if len(datastream.ndarray[0]) > 0:
        for i in range(len(datastream.ndarray[0])):
            row = []
            for idx,el in enumerate(datastream.ndarray):
                if len(datastream.ndarray[idx]) > 0:
                    if KEYLIST[idx].find('time') >= 0:
                        # check whether floats are present - secondary time column 
                        # might be filled with string '-' placeholder
                        if isinstance(el[0],datetime):
                            row.append(
                                datetime.strftime(el[i].replace(tzinfo=None), "%Y-%m-%dT%H:%M:%S.%f"))
                        elif isinstance(el[0],np.datetime64):
                            row.append(np.datetime_as_string(el[i], unit='us'))
                        else:
                            row.append(np.nan)
                    else:
                        if not KEYLIST[idx] in NUMKEYLIST: # Get String and replace all non-standard ascii characters
                            try:
                                valid_chars='-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
                                el[i] = ''.join([e for e in list(el[i]) if e in list(valid_chars)])
                            except:
                                pass
                        row.append(el[i])
                else:
                    if KEYLIST[idx] in NUMKEYLIST:
                        row.append(np.nan)
                    else:
                        row.append('-')
            wtr.writerow(row)
    myFile.close()
    return filename


def writePYASCII(datastream, filename, **kwargs):
    """
    Function to write basic ASCII data
    """

    mode = kwargs.get('mode')
    logger.info("writePYASCII: Writing file to %s" % filename)
    returnstring = False

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
                        row.append(np.datetime64(el[i]).astype(np.float64))
                        row.append(datetime.strftime(el[i].replace(tzinfo=None), "%Y-%m-%dT%H:%M:%S.%f") )
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
    print("TESTING: MAGPY SPECIFIC FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE MAGPY LIBRARY.")
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
        testset = 'PYSTR'
        try:
            filename = os.path.join('/tmp','{}_{}_{}'.format(testrun, testset, datetime.strftime(t_start_test,'%Y%m%d-%H%M')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            succ1 = writePYSTR(teststream, filename)
            succ2 = isPYSTR(filename)
            dat = readPYSTR(filename)
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

        testset = 'PYASCII'
        try:
            filename = os.path.join('/tmp', '{}_{}_{}'.format(testrun, testset,
                                                              datetime.strftime(t_start_test, '%Y%m%d-%H%M')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            succ1 = writePYASCII(teststream, filename)
            succ2 = isPYASCII(filename)
            dat = readPYASCII(filename)
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
