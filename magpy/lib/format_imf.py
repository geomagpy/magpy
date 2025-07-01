"""
MagPy
Intermagnet input filter
Written by Roman Leonhardt December 2012
- contains test, read and write functions for
        IMF 1.22,1.23
        IAF
        ImagCDF  -> moved to separate library
        IYFV     (yearly means)
        DKA      (k values)
        IBFV2.0  (baseline data)
        IBFV1.2  (baseline data)
"""
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
#from magpy.stream import *
from magpy.stream import DataStream, read, subtract_streams, join_streams, magpyversion
from magpy.stream import LineStruct
from magpy.core.activity import K_fmi
from magpy.core.methods import testtime, convert_geo_coordinate, is_number, extract_date_from_string
import os
from datetime import datetime, timedelta, timezone
import re
import numpy as np
import sys
import logging
import struct
import dateutil.parser as dparser
from matplotlib.dates import date2num, num2date
logger = logging.getLogger(__name__)

KEYLIST = DataStream().KEYLIST

def coordinatetransform(u,v,w,kind):
    """
    DESCRIPTION:
        Transforms given values and returns [d,i,h,x,y,z,f] if successful, False if not.
        Parameter "kind" defines the type of provided values
    APPLICATION:
        list = coordinatetransform(meanx,meany,meanz,'xyz')
    """

    if not kind in ['xyz','hdz','dhz','idf']:
        return [0]*7
    if kind == 'xyz':
        h = np.sqrt(u**2 + v**2)
        i = (180.)/np.pi * np.arctan2(w, h)
        d = (180.)/np.pi * np.arctan2(v, u)
        f = np.sqrt(u**2+v**2+w**2)
        return [d,i,h,u,v,w,f]
    elif kind == 'hdz':
        dc = v*np.pi/(180.)
        xtmp = u /np.sqrt((np.tan(dc))**2 + 1)
        y = np.sqrt(u**2 - xtmp**2)
        x = xtmp
        f = np.sqrt(x**2+y**2+w**2)
        i = (180.)/np.pi * np.arctan2(w, u)
        return [v,i,u,x,y,w,f]
    elif kind == 'dhz':
        dc = u*np.pi/(180.)
        xtmp = v /np.sqrt((np.tan(dc))**2 + 1)
        y = np.sqrt(v**2 - xtmp**2)
        x = xtmp
        f = np.sqrt(v**2+w**2)
        i = (180.)/np.pi * np.arctan2(w, v)
        return [u,i,v,x,y,w,f]
    return [0]*7

def dms2d(dms):
        """
        DESCRIPTION:
            converts a string with degree:minutes:seconds to degree.decimals
        VARIBALES:
            dms (string) like -0:37:23 or 23:23
        """
        # 1. get sign
        sign = dms[0]
        multi = 1
        if sign == '-':
            multi = -1
            dms = dms[1:]

        dmsar = dms.split(':')
        if len(dmsar) > 3:
            print("Could not interpret dms")
            return 0.0
        val = []
        for i in range(0, 3):
            try:
                val.append(float(dmsar[i]))
            except:
                val.append(0.0)
        d = multi * (val[0] + val[1] / 60. + val[2] / 3600.)
        return d


def isIMF(filename):
    """
    Checks whether a file is ASCII IMF 1.22,1.23 minute format.
    """
    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
    except:
        return False
    try:
        if not 63 <= len(temp) <= 65:  # Range which regards any variety of return
            return False
        if temp[3] == ' ' and temp[11] == ' ' and temp[29] == ' ' and temp[45] == ' ' and temp[46] == 'R':
            pass
        else:
            return False
    except:
        return False

    logger.debug("isIMF: Found IMF data")
    return True


def isIAF(filename):
    """
    Checks whether a file is BIN IAF INTERMAGNET Archive format.
    """

    try:
        with open(filename, 'rb') as fi:
            temp = fi.read(64)
        data= struct.unpack('<4s4l4s4sl4s4sll4s4sll', temp)
    except:
        return False
    try:
        date = str(data[1])
        if not len(date) == 7:
            return False
    except:
        return False
    try:
        datetime.strptime(date,"%Y%j")
    except:
        return False

    return True


def isBLV1_2(filename):
    """
    Checks whether a file is ASCII IBFV 1.2 Baseline format.
    This is rather complicated as apparently many data providers did not
    care about the content and format description.
    """
    try:
        #import ntpath
        #head, tail = ntpath.split(filename)
        head, tail = os.path.split(filename)
        tail = tail.lower()
        if tail.endswith(".blv"):
            name = tail.replace(".blv",'')
            if not len(name) == 5:
                return False
        else:
            return False
    except:
        return False
    try:
        fi = open(filename, 'rt')
        temp1 = fi.readline()
        temp2 = fi.readline()
        fi.close()
    except:
        return False
    if temp1.startswith('XYZ') or temp1.startswith('DIF') or temp1.startswith('HDZ') or temp1.startswith('UVZ') or temp1.startswith('DHZ'):
        pass
    else:
        return False
    try:
        tl1 = temp1.split()
        tl2 = temp2.split()
    except:
        return False
    if not len(tl1) in [2,3,4] or not len(tl2) in [4,5]:
        return False
    if not 5 <= len(temp1) <= 30:
        return False
    if not len(tl2[0]) == 3: # check DOY length
        try:
            if int(tl2[0]) < 365:
                pass
            else:
                return False
        except:
            return False

    logger.debug("isBLV1_2: Found IBFV1.2 data")
    return True

def isBLV(filename):
    """
    Checks whether a file is ASCII IBFV 2.0 format.
    """
    try:
        with open(filename, "rt") as fi:
            temp1 = fi.readline()
            temp2 = fi.readline()
    except:
        return False
    if temp1.startswith('XYZ') or temp1.startswith('DIF') or temp1.startswith('HDZ') or temp1.startswith('UVZ') or temp1.startswith('DHZ'):
        pass
    else:
        return False
    #if temp2.startswith('0'):
    #    pass
    #else:
    #    return False
    if not 15 <= len(temp1) <= 30:
        return False
    if not 43 <= len(temp2) <= 45:
        return False

    logger.debug("isBLV: Found BLV data")
    return True


def isIYFV(filename):
    """
    Checks whether a file is ASCII IYFV 1.01 yearly mean format.

    _YYYY.yyy_DDD_dd.d_III_ii.i_HHHHHH_XXXXXX_YYYYYY_ZZZZZZ_FFFFFF_A_EEEE_NNNCrLf
    """
    # Search for identifier in the first three line
    code = 'rt'
    try:
        fi = open(filename, code, errors='replace')
    except:
        return False

    for ln in range(0,2):
        try:
            temp = fi.readline()
            if not temp:
                temp = fi.readline()
        except UnicodeDecodeError as e:
            print ("Found an unicode error whene reading:",e)
            fi.close()
            return False
        except:
            fi.close()
            return False
        try:
            searchstr = ['ANNUAL MEAN VALUES', 'Annual Mean Values', 'annual mean values', 'ANNUAL MEANS VALUES']
            for elem in searchstr:
                if temp.find(elem) > 0:
                    logger.debug("isIYFV: Found IYFV data")
                    fi.close()
                    return True
        except:
            fi.close()
            return False
    fi.close()
    return False


def isDKA(filename):
    """
    Checks whether a file is ASCII DKA k value format.

                               AAA
                  Geographical latitude:    43.250 N
                  Geographical longitude:   76.920 E

            K-index values for 2010     (K9-limit =  300 nT)
    """
    ok = False
    try:
        with open(filename, "rt") as fh:
            temp1 = fh.readline()
            temp2 = fh.readline()
            temp3 = fh.readline()
            temp4 = fh.readline()
            temp5 = fh.readline()
            temp6 = fh.readline()
    except:
        return False
    try:
        searchstr = ['latitude', 'LATITUDE']
        for elem in searchstr:
            if temp2.find(elem) > 0:
                ok = True
        if not ok:
            return False
        searchstr = ['K-index values', 'K-INDEX VALUES']
        for elem in searchstr:
            if temp5.find(elem) > 0:
                logger.debug("isDKA: Found DKA data")
                return True
    except:
        return False
    return False


def readIAF(filename, headonly=False, **kwargs):
    """
    DESCRIPTION:
        Reading Intermagnet archive data format
    PARAMETER:
        resolution      (string) one of day,hour,minute,k   - default is minute

    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    resolution = kwargs.get('resolution')
    timestamp = kwargs.get('timestamp')
    debug = kwargs.get('debug')

    getfile = True
    gethead = True
    dttest = False

    if debug:
        logger.info("Found IAF file ...")

    if not resolution:
        resolution = u'minutes'
    if timestamp == 'datetime':
        dttest = True
    stream = DataStream()
    # Check whether header infromation is already present

    headers = {}
    basestring = (str, bytes)
    data = []
    key = None
    keystr = ''

    x,y,z,f,xho,yho,zho,fho,xd,yd,zd,fd,k,ir = [],[],[],[],[],[],[],[],[],[],[],[],[],[]
    datelist = []

    if debug:
        print ("readIAF: opening file...")

    fh = open(filename, 'rb')
    while True:
        #try:
        getline = True
        start = fh.read(64)
        if not len(start) == 64:
            break
        else:
            head = struct.unpack('<4s4l4s4sl4s4sll4s4sll', start)
            newhead = []
            for el in head:
                if not isinstance(el,(int,basestring)):
                    try:
                        el = el.decode('utf-8')
                    except: # might fail e.g. for empty publication date
                        el = None
                newhead.append(el)
            head = newhead
            date = datetime.strptime(str(head[1]),"%Y%j")
            datelist.append(date)
            if getline:
                # unpack header
                if gethead:
                    if sys.version_info.major>=3:
                        try:
                            head[0] = head[0].decode('ascii')
                        except:
                            pass
                        try:
                            head[5] = head[5].decode('ascii')
                        except:
                            pass
                        try:
                            head[6] = head[6].decode('ascii')
                        except:
                            pass
                        try:
                            head[8] = head[8].decode('ascii')
                        except:
                            pass
                        try:
                            head[9] = head[9].decode('ascii')
                        except:
                            pass
                        try:
                            head[12] = head[12].decode('ascii')
                        except:
                            pass
                        try:
                            head[13] = head[13].decode('ascii')
                        except:
                            head[13] = None
                    headers['StationIAGAcode'] = head[0].strip()
                    headers['StationID'] = head[0].strip()
                    #
                    headers['DataAcquisitionLatitude'] = 90-(float(head[2])/1000.)
                    headers['DataAcquisitionLongitude'] = float(head[3])/1000.
                    headers['DataElevation'] = head[4]
                    headers['DataComponents'] = head[5].lower()
                    for c in head[5].lower():
                        if c == 'g':
                            headers['col-df'] = 'dF'
                            headers['unit-col-df'] = 'nT'
                        else:
                            headers['col-'+str(c)] = c
                            headers['unit-col-'+str(c)] = 'nT'
                    keystr = ','.join([str(c) for c in head[5].lower().replace(' ','')]).replace(' ','')
                    keystr = keystr.strip()
                    if len(keystr) < 6 and not keystr.endswith('f'):
                        keystr = keystr + ',f'
                    elif len(keystr) < 7 and keystr.endswith(','):
                        keystr = keystr + 'f'
                    keystr = keystr.replace('s','f')
                    keystr = keystr.replace('d','y')
                    keystr = keystr.replace('g','df')
                    keystr = keystr.replace('h','x')
                    headers['StationInstitution'] = head[6]
                    headers['DataConversion'] = head[7]
                    headers['DataQuality'] = head[8]
                    headers['SensorType'] = head[9]
                    headers['StationK9'] = head[10]
                    #headers['DataDigitalSampling'] = float(head[11])/1000.
                    headers['DataDigitalSampling'] = str(float(head[11])/1000.) + ' sec'
                    headers['DataSensorOrientation'] = head[12].lower()
                    headers['DataPublicationLevel'] = '4'
                    # New in 0.3.99 - provide a SensorID consisting of IAGA code, min
                    # and numerical publevel
                    #  IAGA code
                    headers['SensorID'] = head[0].strip().upper()+'min_4_0001'
                    try:
                        pubdate = datetime.strptime(str(head[13]),"%y%m")
                    except:
                        # Publications date not provided within the header - use current
                        pubdate = datetime.strptime(datetime.strftime(datetime.now(timezone.utc).replace(tzinfo=None), "%y%m"), "%y%m")

                    headers['DataPublicationDate'] = pubdate
                    gethead = False

                # get minute data
                xb = fh.read(5760)
                x.extend(struct.unpack('<1440l', xb))
                #x = np.asarray(struct.unpack('<1440l', xb))/10. # needs an extend
                yb = fh.read(5760)
                y.extend(struct.unpack('<1440l', yb))
                zb = fh.read(5760)
                z.extend(struct.unpack('<1440l', zb))
                fb = fh.read(5760)
                f.extend(struct.unpack('<1440l', fb))
                # get hourly means
                xhb = fh.read(96)
                xho.extend(struct.unpack('<24l', xhb))
                #xho = np.asarray(struct.unpack('<24l', xhb))/10.
                yhb = fh.read(96)
                yho.extend(struct.unpack('<24l', yhb))
                zhb = fh.read(96)
                zho.extend(struct.unpack('<24l', zhb))
                fhb = fh.read(96)
                fho.extend(struct.unpack('<24l', fhb))
                # get daily means
                xdb = fh.read(4)
                xd.extend(struct.unpack('<l', xdb))
                ydb = fh.read(4)
                yd.extend(struct.unpack('<l', ydb))
                zdb = fh.read(4)
                zd.extend(struct.unpack('<l', zdb))
                fdb = fh.read(4)
                fd.extend(struct.unpack('<l', fdb))
                kb = fh.read(32)
                k.extend(struct.unpack('<8l', kb))
                ilb = fh.read(16)
                ir.extend(struct.unpack('<4l', ilb))
        #except:
        #break
    fh.close()

    if debug:
        print ("readIAF: extracted data from binary file")
    #x = np.asarray([val for val in x if not val > 888880])/10.   # use a pythonic way here
    x = np.asarray(x)/10.
    x[x > 88880] = float(np.nan)
    y = np.asarray(y)/10.
    y[y > 88880] = float(np.nan)
    z = np.asarray(z)/10.
    z[z > 88880] = float(np.nan)
    f = np.asarray(f)/10.
    f[f > 88880] = float(np.nan)
    with np.errstate(invalid='ignore'):
        f[f < -44440] = float(np.nan)
    xho = np.asarray(xho)/10.
    xho[xho > 88880] = float(np.nan)
    yho = np.asarray(yho)/10.
    yho[yho > 88880] = float(np.nan)
    zho = np.asarray(zho)/10.
    zho[zho > 88880] = float(np.nan)
    fho = np.asarray(fho)/10.
    fho[fho > 88880] = float(np.nan)
    with np.errstate(invalid='ignore'):
        fho[fho < -44440] = float(np.nan)
    xd = np.asarray(xd)/10.
    xd[xd > 88880] = float(np.nan)
    yd = np.asarray(yd)/10.
    yd[yd > 88880] = float(np.nan)
    zd = np.asarray(zd)/10.
    zd[zd > 88880] = float(np.nan)
    fd = np.asarray(fd)/10.
    fd[fd > 88880] = float(np.nan)
    with np.errstate(invalid='ignore'):
        fd[fd < -44440] = float(np.nan)
    k = np.asarray(k).astype(float)/10.
    k[k > 88] = float(np.nan)
    ir = np.asarray(ir)

    if debug:
        print ("readIAF: asigned arrays")

    # ndarray
    def data2array(arlist, keylist, starttime, sr):
        array = [[] for key in KEYLIST]
        ta = []
        val = starttime
        for ind, elem in enumerate(arlist[0]):
            ta.append(val)
            val = val+timedelta(seconds=sr)
        array[0] = np.asarray(ta)
        for idx,ar in enumerate(arlist):
            pos = KEYLIST.index(keylist[idx])
            if not np.isnan(np.asarray(ar)).all():
                array[pos] = np.asarray(ar)
        return np.asarray(array,dtype=object)

    headers['DataFormat'] = 'IAF'

    if resolution in ['day','days','Day','Days','DAY','DAYS']:
        ndarray = data2array([xd,yd,zd,fd],keystr.split(','),min(datelist),sr=86400)
        headers['DataSamplingRate'] = '86400 sec'
    elif resolution in ['hour','hours','Hour','Hours','HOUR','HOURS']:
        ndarray = data2array([xho,yho,zho,fho],keystr.split(','),min(datelist)+timedelta(minutes=30),sr=3600)
        headers['DataSamplingRate'] = '3600 sec'
    elif resolution in ['k','K']:
        #ndarray = data2array([k,ir],['var1','var2'],min(datelist)+timedelta(minutes=90),sr=10800)
        ndarray = data2array([k],['var1'],min(datelist)+timedelta(minutes=90),sr=10800)
        headers['DataSamplingRate'] = '10800 sec'
        headers['DataFormat'] = 'MagPyK'
        headers['col-var1'] = "K"
    else:
        logger.debug("Key and minimum: {} {}".format(keystr, min(datelist)))
        ndarray = data2array([x,y,z,f],keystr.split(','),min(datelist),sr=60)
        headers['DataSamplingRate'] = '60 sec'

    stream = DataStream(header=headers, ndarray=ndarray)
    #if 'df' in keystr:
    #    stream = stream.f_from_df()

    return stream


def writeIAF(datastr, filename, **kwargs):
    """
    Writing Intermagnet archive format (2.1)
    """

    kvals = kwargs.get('kvals')
    mode = kwargs.get('mode')
    debug = kwargs.get('debug')

    datastream = datastr.copy()

    df=False
    # Check whether data is present at all
    if not len(datastream.ndarray[0]) > 0:
        logger.error("writeIAF: No data found - check ndarray")
        return False
    # Check whether minute file
    sr = datastream.samplingrate()
    if not int(sr) == 60:
        logger.error("writeIAF: Minute data needs to be provided")
        return False
    # check whether data covers one month
    tdiff = int(np.round((datastream.ndarray[0][-1]-datastream.ndarray[0][0]).total_seconds()/86400.))
    if not tdiff >= 28:
        logger.error("writeIAF: Data needs to cover one month")
        return False

    try:
        datastream.header['DataComponents'] = datastream.header.get('DataComponents','').upper()
        # Convert data to XYZ if HDZ
        if not datastream.header.get('DataComponents','').startswith('XYZ'):
            logger.info("Data contains: {}".format(datastream.header.get('DataComponents','')))
        if datastream.header.get('DataComponents').startswith('HDZ'):
            datastream = datastream.hdz2xyz()
    except:
        logger.error("writeIAF: HeaderInfo on DataComponents seems to be missing")
        return False

    dsf = datastream.header.get('DataSamplingFilter','')

    #print ("WRITING IAF DATA 2a")

    # Check whether f is contained (or delta f)
    # if f calc delta f
    #   a) replaceing s by f
    if datastream.header.get('DataComponents','') in ['HDZS','hdzs']:
        datastream.header['DataComponents'] = 'HDZF'
    if datastream.header.get('DataComponents','') in ['XYZS','xyzs']:
        datastream.header['DataComponents'] = 'XYZF'
    if datastream.header.get('DataSensorOrientation','') == '':
        datastream.header['DataSensorOrientation'] = datastream.header.get('DataComponents','')
    dfpos = KEYLIST.index('df')
    fpos = KEYLIST.index('f')
    dflen = len(datastream.ndarray[dfpos])
    flen = len(datastream.ndarray[fpos])

    if not dflen == len(datastream.ndarray[0]):
        #check for F and calc
        if not flen == len(datastream.ndarray[0]):
            df=False
        else:
            datastream = datastream.delta_f()
            df=True
            if len(datastream.header.get('DataComponents','')) == 4:
                datastream.header['DataComponents'] = datastream.header.get('DataComponents','')[:3]
            if datastream.header.get('DataComponents','') in ['HDZ','XYZ']:
                datastream.header['DataComponents'] += 'G'
            if datastream.header.get('DataSensorOrientation','') in ['HDZ','XYZ','hdz','xyz']:
                datastream.header['DataSensorOrientation'] += datastream.header.get('DataSensorOrientation','').upper() + 'F'
    else:
        df=True
        if len(datastream.header.get('DataComponents','')) == 4:
            datastream.header['DataComponents'] = datastream.header.get('DataComponents','')[:3]
        if datastream.header.get('DataComponents','') in ['HDZ','XYZ']:
            datastream.header['DataComponents'] += 'G'
        if datastream.header.get('DataSensorOrientation','') in ['HDZ','XYZ','hdz','xyz']:
            datastream.header['DataSensorOrientation'] = datastream.header.get('DataSensorOrientation','').upper() + 'F'

    # Eventually converting Locations data
    proj = datastream.header.get('DataLocationReference','')
    longi = datastream.header.get('DataAcquisitionLongitude',' ')
    lati = datastream.header.get('DataAcquisitionLatitude',' ')
    if not longi=='' or lati=='':
        if proj == '':
            pass
        else:
            if proj.find('EPSG:') > 0:
                epsg = int(proj.split('EPSG:')[1].strip())
                if not epsg==4326:
                    longi,lati = convert_geo_coordinate(float(longi),float(lati),'epsg:'+str(epsg),'epsg:4326')
                    datastream.header['DataAcquisitionLongitude'] = longi
                    datastream.header['DataAcquisitionLatitude'] = lati
                    datastream.header['DataLocationReference'] = 'WGS84, EPSG:4326'

        if is_number(datastream.header.get('DataAcquisitionLatitude', 0)) and float(datastream.header.get('DataAcquisitionLatitude', 0)) >= 90 or float(datastream.header.get('DataAcquisitionLatitude', 0)) <= -90:
            logger.info("Latitude and Longitude apparently not correctly provided - setting to zero")
            print("Latitude and Longitude need to be provided in degrees")
            datastream.header['DataAcquisitionLongitude'] = 0.0
            datastream.header['DataAcquisitionLatitude'] = 0.0

    # Check whether all essential header info is present
    requiredinfo = ['StationIAGAcode','StartDate','DataAcquisitionLatitude', 'DataAcquisitionLongitude', 'DataElevation', 'DataComponents', 'StationInstitution', 'DataConversion', 'DataQuality', 'SensorType', 'StationK9', 'DataDigitalSampling', 'DataSensorOrientation', 'DataPublicationDate','FormatVersion','Reserved']

    # cycle through data - day by day
    t0 = int(date2num(datastream.ndarray[0][1]))
    output = b''
    kstr=[]

    tmpstream = datastream.copy()
    hourvals = tmpstream.filter(filter_width=timedelta(minutes=60), resampleoffset=timedelta(minutes=30), filter_type='flat',missingdata='mean')
    hourvals = hourvals.get_gaps()
    printhead = True

    calck = True
    if not kvals and calck:
        # calculate kvalues
        print(" K values not provided - calculating them")
        print(
            " IMPORTANT: first and last day will be missing every month - provide kvals separately in order to overcome this limitation")
        if not datastream.header.get('StationK9', None):
            print(" -> no K9 value provided for Station: using 500nT as default")
        if not datastream.header.get('DataAcquisitionLongitude', None):
            print(" -> no Longitude provided for Station: using 15E as default")
        kvals = K_fmi(datastream, K9_limit=float(datastream.header.get('StationK9', 500)),
                          longitude=float(datastream.header.get('DataAcquisitionLongitude', 15)))

    #print ("HERE", datastream.header)

    for i in range(tdiff):
        dayar = datastream._select_timerange(starttime=t0+i,endtime=t0+i+1)
        if len(dayar[0]) > 1440:
            logger.info("writeIAF: found {} datapoints (expected are 1440) - assuming last value(s) to represent next month".format(len(dayar[0])))
            dayar = np.asarray([elem[:1440] for elem in dayar])
        elif len(dayar[0]) < 1440:
            logger.info("writeIAF: found {} datapoints (expected are 1440) - check gaps".format(len(dayar[0])))
            print ("writeIAF:  Use get_gap function to identify and mark gaps in the timeseries")
        # get all indices
        #minutest = DataStream([LineStruct],datastream.header,dayar)
        #temp = minutest.copy() ### Necessary so that dayar is not modified by the filtering process
        #temp = temp.filter(filter_width=timedelta(minutes=60), resampleoffset=timedelta(minutes=30), filter_type='flat')
        tempvals = hourvals._select_timerange(starttime=t0+i,endtime=t0+i+1)
        temp = DataStream(header=datastream.header,ndarray=tempvals)

        head = []
        reqinfotmp = requiredinfo
        misslist = []
        for elem in requiredinfo:
            try:
                if elem == 'StationIAGAcode':
                    value = " "+datastream.header.get('StationIAGAcode', '')
                    if value == '':
                        misslist.append(elem)
                elif elem == 'StartDate':
                    value = int(datetime.strftime(dayar[0][0], '%Y%j'))
                elif elem == 'DataAcquisitionLatitude':
                    if not float(datastream.header.get('DataAcquisitionLatitude',0)) < 90 and float(datastream.header.get('DataAcquisitionLatitude','')) > -90:
                        logger.info("Latitude and Longitude apparently not correctly provided - setting to zero")
                        return False
                    value = int(np.round((90-float(datastream.header.get('DataAcquisitionLatitude',0)))*1000))
                    if value == 0:
                        misslist.append(elem)
                elif elem == 'DataAcquisitionLongitude':
                    value = int(np.round(float(datastream.header.get('DataAcquisitionLongitude',0))*1000))
                    if value == 0:
                        misslist.append(elem)
                elif elem == 'DataElevation':
                    value = int(np.round(float(datastream.header.get('DataElevation',0))))
                    datastream.header['DataElevation'] = value
                    if value == 0:
                        misslist.append(elem)
                elif elem == 'DataConversion':
                    if datastream.header.get('DataComponents','').startswith('XYZ'):
                        value = int(10000)
                    else:
                        value = int(np.round(float(datastream.header.get('DataConversion',0))))
                    if value == 0:
                        misslist.append(elem)
                elif elem == 'DataPublicationDate':
                    da = datastream.header.get('DataPublicationDate','')
                    try:
                        value = datetime.strftime(testtime(da),'%y%m')
                    except:
                        #print("writeIAF: DataPublicationDate --  appending current date")
                        value = datetime.strftime(datetime.now(timezone.utc).replace(tzinfo=None),'%y%m')
                elif elem == 'FormatVersion':
                    value = 3
                elif elem == 'StationK9':
                    value = int(np.round(float(datastream.header.get('StationK9',0))))
                    if value == 0:
                        misslist.append(elem)
                        # TODO replace by correct K9LL using Audes formula
                        value = 500
                elif elem == 'DataDigitalSampling':
                    try:
                        value = int(datastream.header.get('DataDigitalSampling',0)*1000)
                        if value == 0:
                            misslist.append(elem)
                            value = 999
                    except:
                        value = datastream.header.get('DataDigitalSampling','')
                        #print ("writeIAF: ", value)
                        #print ("writeIAF: DataDigitialSampling info needs to be an integer")
                        #print ("          - extracting integers from provided string")
                        valtmp = re.findall(r'\d+', value)
                        #print ("writeIAF: ", valtmp)
                        try:
                            val = float(".".join(valtmp))
                        except:
                            try:
                                val = int(valtmp[0])
                            except:
                                val = 0
                                #val = int(valtmp[-1])  ## OLD version, does not make sense to me now (leon)
                        if 'hz' in value or 'Hz' in value or 'Hertz' in value and not val == 0:
                            value = int(1./val*1000.)
                        else:
                            value = int(val*1000.)
                        #print ("          extracted: {}".format(value))
                elif elem == 'Reserved':
                    value = 0
                else:
                    value = datastream.header.get(elem,'')
                if isinstance(value, (list,tuple)):
                    if len(value)>0:
                        value = value[0]
                    else:
                        value = ''
                if not is_number(value):
                    if len(value) < 4:
                        value = value.ljust(4)
                    elif len(value) > 4:
                        value = value[:4]
                head.append(value)
                reqinfotmp = [el for el in reqinfotmp if not el==elem]
            except:
                print("Check header content: could not interpret header information")
                print("  --  critical information error in data header: {}  --".format(elem))
                print("  ---------------------------------------------------")
                print(" Please provide: StationIAGAcode, DataAcquisitionLatitude, ")
                print(" DataAcquisitionLongitude, DataElevation, DataConversion, ")
                print(" DataComponents, StationInstitution, DataQuality, SensorType, ")
                print(" StationK9, DataDigitalSampling, DataSensorOrientation")
                print(" e.g. data.header['StationK9'] = 750")
                return False

        if len(misslist) > 0 and printhead:
            print ("The following meta information is missing. Please provide!")
            for he in misslist:
                print (he)
            printhead = False

        # Constructing header Info
        packcode = '<4s4l4s4sl4s4sll4s4sll' # fh.read(64)
        unicode = str
        head = [el.encode('ascii','ignore') if isinstance(el, unicode) else el for el in head]
        if debug:
            print ("Header looks like:", head)
        head_bin = struct.pack(packcode,*head)
        # add minute values
        packcode += '1440l' # fh.read(64)
        xvals = np.asarray([np.round(elem,1) if not np.isnan(elem) else 99999.9 for elem in dayar[1]])
        xvals = np.asarray(xvals*10).astype(int)
        head.extend(xvals)
        if not len(xvals) == 1440:
            logger.error("writeIAF: Found inconsistency in minute data set")
            logger.error("writeIAF: for {}".format(datetime.strftime(dayar[0][0]),'%Y%j'))
            logger.error("writeIAF: expected 1440 records, found {} records".format(len(xvals)))
        packcode += '1440l' # fh.read(64)
        yvals = np.asarray([np.round(elem,1) if not np.isnan(elem) else 99999.9 for elem in dayar[2]])
        yvals = np.asarray(yvals*10).astype(int)
        head.extend(yvals)
        packcode += '1440l' # fh.read(64)
        zvals = np.asarray([np.round(elem,1) if not np.isnan(elem) else 99999.9 for elem in dayar[3]])
        zvals = np.asarray(zvals*10).astype(int)
        head.extend(zvals)
        packcode += '1440l' # fh.read(64)
        if df:
            #print ([elem for elem in dayar[dfpos]])
            dfvals = np.asarray([np.round(elem*10.,0) if not np.isnan(elem) else 999999 for elem in dayar[dfpos]])
            #print ("dfmin",dfvals)
            #dfvals = np.asarray(dfvals*10.).astype(int)
            dfvals = dfvals.astype(int)
        else:
            dfvals = np.asarray([888888]*len(dayar[0])).astype(int)
        head.extend(dfvals)

        # add hourly means
        packcode += '24l'
        xhou = np.asarray([np.round(elem,1) if not np.isnan(elem) else 99999.9 for elem in temp.ndarray[1]])
        xhou = np.asarray(xhou*10).astype(int)
        head.extend(xhou)
        if not len(xhou) == 24:
            logger.error("writeIAF: Found inconsistency in hourly data set: expected 24, found {} records".format(len(xhou)))
            logger.error("writeIAF: Error in day {}".format(datetime.strftime(num2date(dayar[0][0]),'%Y%j')))
        packcode += '24l'
        yhou = np.asarray([np.round(elem,1) if not np.isnan(elem) else 99999.9 for elem in temp.ndarray[2]])
        yhou = np.asarray(yhou*10).astype(int)
        head.extend(yhou)
        packcode += '24l'
        zhou = np.asarray([np.round(elem,1) if not np.isnan(elem) else 99999.9 for elem in temp.ndarray[3]])
        zhou = np.asarray(zhou*10).astype(int)
        head.extend(zhou)
        packcode += '24l'
        if df:
            dfhou = np.asarray([np.round(elem,1) if not np.isnan(elem) else 99999.9 for elem in temp.ndarray[dfpos]])
            dfhou = np.asarray(dfhou*10).astype(int)
        else:
            dfhou = np.asarray([888888]*24).astype(int)
        head.extend(dfhou)

        #print ("2:", len(head))

        # add daily means
        packcode += '4l'
        # -- drop all values above 88888
        xvalid = np.asarray([elem for elem in xvals if elem < 888880])
        yvalid = np.asarray([elem for elem in yvals if elem < 888880])
        zvalid = np.asarray([elem for elem in zvals if elem < 888880])
        if len(xvalid)>0.9*len(xvals):
            head.append(int(np.round(np.mean(xvalid),0)))
        else:
            head.append(999999)
        if len(xvalid)>0.9*len(xvals):
            head.append(int(np.round(np.mean(yvalid),0)))
        else:
            head.append(999999)
        if len(xvalid)>0.9*len(xvals):
            head.append(int(np.round(np.mean(zvalid),0)))
        else:
            head.append(999999)
        if df:
            dfvalid = np.asarray([elem for elem in dfvals if elem < 888880])
            if len(dfvalid)>0.9*len(dfvals):
                head.append(int(np.round(np.mean(dfvalid),0)))
            else:
                head.append(999999)
        else:
            head.append(888888)

        # add k values
        if kvals:
            dayk = kvals._select_timerange(starttime=t0+i,endtime=t0+i+1)
            kdat = dayk[KEYLIST.index('var1')]
            # replace nans and or negative (-1) by 999
            kdat = [int(el*10.) if not np.isnan(el) and el >= 0 else 999 for el in kdat]
            packcode += '8l'
            if not len(kdat) == 8:
                ks = [999]*8
            else:
                ks = kdat
            sumk = int(sum(ks))/10.
            if sumk > 99:
                sumk = 999
            linestr = "  {0}   {1}".format(datetime.strftime(num2date(t0+i),'%d-%b-%y'), datetime.strftime(num2date(t0+i),'%j'))
            tup = tuple([str(int(elem/10.)) if not elem==999 else 999 for elem in ks])
            linestr += "{0:>6}{1:>5}{2:>5}{3:>5}{4:>7}{5:>5}{6:>5}{7:>5}".format(*tup)
            linestr += "{0:>9}".format(str(sumk))
            kstr.append(linestr)
            head.extend(ks)
        else:
            packcode += '8l'
            ks = [999]*8
            head.extend(ks)
        # add reserved
        packcode += '4l'
        reserved = [0,0,0,0]
        head.extend(reserved)

        line = struct.pack(packcode,*head)
        output = output + line


    path = os.path.split(filename)
    filename = os.path.join(path[0],path[1].upper())

    logger.info("Writing monthly IAF data format to {}".format(path[1].upper()))
    if os.path.isfile(filename):
        if mode == 'append':
            with open(filename, "a") as myfile:
                myfile.write(output)
        else: # overwrite mode
            os.remove(filename)
            myfile = open(filename, "wb")
            myfile.write(output)
            myfile.close()
    else:
        myfile = open(filename, "wb")
        myfile.write(output)
        myfile.close()

    readme = True
    if readme:
        try:
            success = writeIAFREADME(datastream,path[0],debug=debug)
        except:
            success = False
        if success:
            print (" README file successfully written")

    return True

def writeIAFDKA(datastream,kstr,path, debug=False):
        if debug:
            print (" debugDKA: Creating DKA file from k values info in {}".format(path))
        station=datastream.header.get('StationIAGAcode',"XXX")
        k9=int(datastream.header.get('StationK9',500))
        lat=np.round(float(datastream.header.get('DataAcquisitionLatitude')),3)
        lon=np.round(float(datastream.header.get('DataAcquisitionLongitude')),3)
        year=str(int(datetime.strftime(num2date(datastream.ndarray[0][1]),'%y')))
        ye=str(int(datetime.strftime(num2date(datastream.ndarray[0][1]),'%Y')))
        kfile = os.path.join(path,station.upper()+year+'K.DKA')
        if debug:
            print (" debugDKA: Writing k summary file: {}".format(kfile))
        head = []
        if not os.path.isfile(kfile):
            head.append("{0:^66}".format(station.upper()))
            head2 = '                  Geographical latitude: {:>10.3f} N'.format(lat)
            head3 = '                  Geographical longitude:{:>10.3f} E'.format(lon)
            head4 = '            K-index values for {0}     (K9-limit = {1:>4} nT)'.format(ye, k9)
            head5 = '  DA-MON-YR  DAY #    1    2    3    4      5    6    7    8       SK'
            emptyline = ''
            head.append("{0:<50}".format(head2))
            head.append("{0:<50}".format(head3))
            head.append("{0:<50}".format(emptyline))
            head.append("{0:<50}".format(head4))
            head.append("{0:<50}".format(emptyline))
            head.append("{0:<50}".format(head5))
            head.append("{0:<50}".format(emptyline))

            with open(kfile, "w", newline='') as myfile:
                for elem in head:
                    myfile.write(elem+'\r\n')
        with open(kfile, "a", newline='') as myfile:
            for elem in kstr:
                myfile.write(elem+'\r\n')
        return True

def writeIAFREADME(datastream,path,debug=False,**kwargs):
        if debug:
            print(" debugREADME: Creating README from header info in {}".format(path))
        requiredhead = ['StationName','StationInstitution', 'StationStreet','StationCity','StationPostalCode','StationCountry','StationWebInfo', 'StationEmail','StationK9']
        acklist = ['StationName','StationInstitution', 'StationStreet','StationCity','StationPostalCode','StationCountry','StationWebInfo' ]
        conlist = ['StationName','StationInstitution', 'StationStreet','StationCity','StationPostalCode','StationCountry', 'StationEmail']

        for h in requiredhead:
            test = datastream.header.get(h,None)
            if not test:
                print (" README generation: Info on {0} is missing".format(h))
        ack = []
        contact = []
        for a in acklist:
            try:
                ack.append("               {0}".format(datastream.header.get(a)))
            except:
                pass
        for c in conlist:
            try:
                contact.append("               {0}".format(datastream.header.get(c)))
            except:
                pass

        # 1. Check completeness of essential header information
        station=datastream.header.get('StationIAGAcode','XXX')
        stationname = datastream.header.get('StationName','to be provided')
        try:
            k9=int(datastream.header.get('StationK9',500))
        except:
            k9 = 500
        dsf = datastream.header.get('DataSamplingFilter','')
        lat=np.round(float(datastream.header.get('DataAcquisitionLatitude')),3)
        lon=np.round(float(datastream.header.get('DataAcquisitionLongitude')),3)
        ye=str(int(datetime.strftime(datastream.ndarray[0][1],'%Y')))
        rfile = os.path.join(path,"README."+station.upper())
        if os.path.isfile(rfile):
            print (" README file already existing - skipping writeREADME")
            return False
        head = []
        if debug:
            print("Writing README file: {}".format(rfile))

        dummy = "please insert manually"
        if not os.path.isfile(rfile):
            emptyline = ''
            head.append("{0:^66}".format(station.upper()))
            head.append("{0:<50}".format(emptyline))
            head.append("{0:>23} OBSERVATORY INFOMATION {1:>5}".format(stationname.upper(), ye))
            head.append("{0:<50}".format(emptyline))
            head.append("ACKNOWLEDGEMT: Users of {0:}-data should acknowledge:".format(station.upper()))
            for elem in ack:
                head.append(elem)
            head.append("{0:<50}".format(emptyline))
            head.append("STATION ID   : {0}".format(station.upper()))
            head.append("LOCATION     : {0}, {1}".format(datastream.header.get('StationCity','city'),datastream.header.get('StationCountry','country')))
            head.append("ORGANIZATION : {0:<50}".format(datastream.header.get('StationInstitution','institution')))
            head.append("CO-LATITUDE  : {:.3f} Deg.".format(90.-float(lat)))
            head.append("LONGITUDE    : {:.3f} Deg. E".format(float(lon)))
            head.append("ELEVATION    : {0} meters".format(int(datastream.header.get('DataElevation','elevation'))))
            head.append("{0:<50}".format(emptyline))
            head.append("ABSOLUTE")
            head.append("INSTRUMENTS  : please insert manually")
            head.append("RECORDING")
            head.append("VARIOMETER   : {}".format(datastream.header.get('SensorName',dummy)))
            head.append("ORIENTATION  : {}".format(datastream.header.get('DataSensorOrientation','orientation')))
            head.append("{0:<50}".format(emptyline))
            head.append("DYNAMIC RANGE: {}".format(datastream.header.get('SensorDynamicRange',dummy)))
            head.append("RESOLUTION   : {}".format(datastream.header.get('SensorResolution',dummy)))
            head.append("SAMPLING RATE: {}".format(datastream.header.get('DataDigitalSampling',dummy)))
            head.append("FILTER       : {0}".format(dsf))
            # Provide method with head of kvals
            head.append("K-NUMBERS    : Computer derived (FMI method, MagPy)")
            head.append("K9-LIMIT     : {0:>4} nT".format(k9))
            head.append("{0:<50}".format(emptyline))
            head.append("GINS         : please insert manually")
            head.append("SATELLITE    : please insert manually")
            head.append("OBSERVER(S)  : please insert manually")
            head.append("ENGINEER(S)  : please insert manually")
            head.append("CONTACT      : ")
            for elem in contact:
                head.append(elem)
            with open(rfile, "w", newline='') as myfile:
                for elem in head:
                    myfile.write(elem+'\r\n')
            myfile.close()
        return True


def comp_decode(type, value)-> float:
    """
    type : variable is deg or nT
    value the value to decode
    """

    if type == 'deg':
        return value/6000
    else:
        return value/10

def comp_encode(type, value)-> float:
    """
    type : variable is deg or nT
    value the value to encorde
    """
    if type == 'deg':
        return value*6000
    else:
        return value*10

def readIMF(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet data format (IMF1.23)
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    headers={}
    array = [[] for elem in KEYLIST]

    def add_col_info_headers(header, components):
        #components = XYZF or HDZF etc
        header["col-x"] = components[0].upper()
        header["col-y"] = components[1].upper()
        header["col-z"] = components[2].upper()
        header["unit-col-x"] = 'nT'
        header["unit-col-y"] = 'nT'
        header["unit-col-z"] = 'nT'
        header["unit-col-f"] = 'nT'
        if components.endswith('g'):
            header["unit-col-df"] = 'nT'
            header["col-df"] = 'G'
            header["col-f"] = 'F'
        else:
            header["col-f"] = 'F'
        # print ("VAR", varstr)
        if components in ['dhzf', 'dhzg']:
            header["unit-col-y"] = 'deg'
            header['DataComponents'] = 'HDZF'
        elif components in ['ehzf', 'ehzg']:
            # consider the different order in the file
            header["col-x"] = 'H'
            header["col-y"] = 'E'
            header["col-z"] = 'Z'
            header['DataComponents'] = 'HEZF'
        elif components in ['dhif', 'dhig']:
            header["col-x"] = 'I'
            header["col-y"] = 'D'
            header["col-z"] = 'F'
            header["unit-col-x"] = 'deg'
            header["unit-col-y"] = 'deg'
            header['DataComponents'] = 'IDFF'
        elif components in ['hdzf', 'hdzg']:
            header["unit-col-y"] = 'deg'
            header['DataComponents'] = 'HDZF'
        else:
            header['DataComponents'] = 'XYZF'

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    datehh = ''
    minute = 0
    headers = {}
    data = []
    key = None
    find = KEYLIST.index('f')
    t1ind = KEYLIST.index('t1')
    var1ind = KEYLIST.index('var1')
    t2ind = KEYLIST.index('t2')

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
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line[29] == ' ':
                # data info
                block = line.split()
                #print block
                headers['StationID'] = block[0]
                headers['DataAcquisitionLatitude'] = float(block[7][:4])/10
                headers['DataAcquisitionLongitude'] = float(block[7][4:])/10
                comps = block[4].lower()
                add_col_info_headers(headers, comps)
                headers['DataSensorAzimuth'] = float(block[8])/10/60
                headers['DataSamplingRate'] = '60 sec'
                headers['DataType'] = block[5]
                try:
                    headers['SensorID'] = "{}{}_{}_{}".format(block[0].upper(),'min','4','0001')
                except:
                    pass
                datehh = block[1] + '_' + block[3]
                #print float(block[7][:4])/10, float(block[7][4:])/10, float(block[8])/10/60
                minute = 0
            elif headonly:
                # skip data for option headonly
                return
            else:
                # data entry - may be written in multiple columns
                # row beinhaltet die Werte eine Zeile
                data = line.split()
                for i in range(2):
                    try:
                        #row = LineStruct()
                        time = datehh+':'+str(minute+i)
                        #row.time=date2num(datetime.strptime(time,"%b%d%y_%H:%M"))
                        array[0].append(datetime.strptime(time,"%b%d%y_%H:%M"))

                        index = int(4*i)
                        if not int(data[0+index]) > 999990:
                              array[1].append(comp_decode(headers["unit-col-x"], float(data[0 + index])))
                        else:
                              array[1].append(np.nan)
                        if not int(data[1+index]) > 999990:
                              array[2].append(comp_decode(headers["unit-col-y"], float(data[1 + index])))
                        else:
                              array[2].append(np.nan)
                        if not int(data[2+index]) > 999990:
                              array[3].append(comp_decode(headers["unit-col-z"], float(data[2 + index])))
                        else:
                              array[3].append(np.nan)
                        if not int(data[3+index]) > 999990:
                              array[4].append(comp_decode(headers["unit-col-f"],float(data[3+index])))
                        #stream.add(row)
                    except:
                        logging.error('format_imf: problem with dataformat - check block header')
                        return DataStream(header=headers, ndarray=np.asarray([np.asarray(el) for el in array],dtype=object))
                minute = minute + 2

    fh.close()

    headers['DataFormat'] = 'IMF'
    array = np.asarray([np.asarray(el) for el in array],dtype=object)
    return DataStream(header=headers, ndarray=array)


def writeIMF(datastream, filename, **kwargs):
    """
    Writing Intermagnet format data.
    """

    mode = kwargs.get('mode')
    version = kwargs.get('version')
    gin = kwargs.get('gin')
    datatype = kwargs.get('datatype')

    minute = 0
    success = False
    # 1. check whether datastream corresponds to minute file
    if not 60*0.9 < datastream.samplingrate() < 60*1.1:
        logger.error("writeIMF: Data needs to be minute data for Intermagnet - filter it accordingly")
        return False

    # 2. check whether file exists and according to mode either create, append, replace
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(exst,datastream)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(datastream,exst)
            myFile= open( filename, "wb" )
        elif mode == 'append':
            myFile= open( filename, "ab" )
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= open( filename, "wb" )
    else:
        myFile= open( filename, "wb" )

    # 3. Get essential header info
    header = datastream.header
    if not gin:
        gin = 'EDI'
    if not datatype:
        if header.get('DataPublicationLevel') in [4,'4','D','definitive','Definitive']:
            datatype = 'D' # reported; can also be 'A', 'Q', 'D'
        elif header.get('DataPublicationLevel') in [3,'3','Q','quasi-definitive','Quasi-definitive']:
            datatype = 'Q'
        elif header.get('DataPublicationLevel') in [2,'2','P','provisional','Provisional']:
            datatype = 'A'
        else:
            datatype = 'R'
    elif not datatype in ['A','Q','D']:
        datatype = 'R' # reported; can also be 'A', 'Q', 'D'
    try:
        idc = header['StationID']
    except:
        logger.warning("writeIMF: No station code specified. Setting to XYZ ...")
        idc = 'XYZ'
        #return False
    try:
        colat = 90 - float(header['DataAcquisitionLatitude'])
        longi = float(header['DataAcquisitionLongitude'])
    except:
        logger.warning("writeIMF: No location specified. Setting 99,999 ...")
        colat = 99.9
        longi = 999.9
        #return False
    try:
        decbas = float(header['DataSensorAzimuth'])
    except:
        logger.warning("writeIMF: No orientation angle specified. Setting 999.9 ...")
        decbas = 999.9
        #return False

    # 4. Data
    dataline,blockline = '',''
    minuteprev = 0

    elemtype = datastream.header.get('DataComponents','XYZF').upper().replace("G","F")

    fulllength = datastream.length()[0]
    ndtype = False
    if len(datastream.ndarray[0]) > 0:
        ndtype = True

    # Check data contents
    xlen = len(datastream.ndarray[KEYLIST.index('x')])
    ylen = len(datastream.ndarray[KEYLIST.index('y')])
    zlen = len(datastream.ndarray[KEYLIST.index('z')])
    flen = len(datastream.ndarray[KEYLIST.index('f')])
    dflen = len(datastream.ndarray[KEYLIST.index('df')])

    if not xlen > 0 or not ylen > 0 or not zlen > 0:
        logger.error("writeIMF: vector data seems to be missing or incomplete - aborting")
        return False

    if not flen > 0 and not dflen > 0:
        print ("writeIMF: required information on f is missing - aborting")
        logger.error("writeIMF: required information on f is missing - aborting")
        return False

    if not flen > 0 and dflen > 0:
        logger.warning("writeIMF: delta F provided, but no F values")
        logger.warning("writeIMF: calcualting F ...")
        datastream = datastream.calc_f()

    flen = len(datastream.ndarray[KEYLIST.index('f')])

    xind = KEYLIST.index('x')
    yind = KEYLIST.index('y')
    zind = KEYLIST.index('z')
    find = KEYLIST.index('f')
    for i in range(fulllength):
        elemx = datastream.ndarray[xind][i]
        elemy = datastream.ndarray[yind][i]
        elemz = datastream.ndarray[zind][i]
        elemf = datastream.ndarray[find][i]
        timeval = datastream.ndarray[0][i]

        date = timeval
        doy = datetime.strftime(date, "%j")
        day = datetime.strftime(date, "%b%d%y")
        hh = datetime.strftime(date, "%H")
        minute = int(datetime.strftime(date, "%M"))
        strcola = '%3.f' % (colat*10)
        strlong = '%3.f' % (longi*10)
        decbasis = str(int(np.round(decbas*60*10)))
        blockline = "{} {} {} {} {} {} {} {}{} {} {}\r\n".format(idc.upper(),day.upper(),doy, hh, elemtype, datatype, gin, strcola.zfill(4), strlong.zfill(4), decbasis.zfill(6),'RRRRRRRRRRRRRRRR')
        if minute == 0 and not i == 0:
            #print blockline
            #myFile.writelines( blockline.encode('utf-8') )
            myFile.write( blockline.encode('utf-8') )
            pass
        if i == 0:
            #print blockline
            #myFile.writelines( blockline.encode('utf-8') )
            myFile.write( blockline.encode('utf-8') )
            if not minute == 0:
                j = 0
                while j < minute:
                    if j % 2: # uneven
                         #AAAAAAA_BBBBBBB_CCCCCCC_FFFFFF__AAAAAAA_BBBBBBB_CCCCCCC_FFFFFFCrLf
                        dataline += '  9999999 9999999 9999999 999999'
                    else: # even
                        dataline = '9999999 9999999 9999999 999999'
                    j = j+1
        if not np.isnan(elemx):
            x = comp_encode(header.get('unit-col-x','nT'),elemx)
        else:
            x = 999999
        if not np.isnan(elemy):
            y =comp_encode(header.get('unit-col-y','nT'),elemy)
        else:
            y = 999999
        if not np.isnan(elemz):
            z = comp_encode(header.get('unit-col-z','nT'),elemz)
        else:
            z = 999999
        if not np.isnan(elemf):
            f = comp_encode(header.get('unit-col-f','nT'),elemf)
        else:
            f = 999999
        if minute > minuteprev + 1:
            while minuteprev+1 < minute:
                if minuteprev+1 % 2: # uneven
                    dataline += '  9999999 9999999 9999999 999999\r\n'
                    #myFile.writelines( dataline.encode('utf-8') )
                    myFile.write( dataline.encode('utf-8') )
                    #print minuteprev+1, dataline
                else: # even
                    dataline = '9999999 9999999 9999999 999999'
                minuteprev = minuteprev + 1
        minuteprev = minute
        if minute % 2: # uneven
            if len(dataline) < 10: # if record starts with uneven minute then
                dataline = '9999999 9999999 9999999 999999'
            dataline += '  %7.0f%8.0f%8.0f%7.0f\r\n' % (x, y, z, f)
            #myFile.writelines( dataline.encode('utf-8') )
            myFile.write( dataline.encode('utf-8') )
            #print minute, dataline
        else: # even
            dataline = '%7.0f%8.0f%8.0f%7.0f' % (x, y, z, f)

    minute = minute + 1
    if not minute == 59:
        while minute < 60:
            if minute % 2: # uneven
                dataline += '  9999999 9999999 9999999 999999\r\n'
                myFile.writelines( dataline.encode('utf-8') )
            else: # even
                dataline = '9999999 9999999 9999999 999999'
            minute = minute + 1

    myFile.close()

    return True

def readBLV1_2(filename, headonly=False, **kwargs):
    """
    IBFV1.20 INTERMAGNET BASELINE FORMAT (2008 and BEFORE)
    This format is to be used to provide baselines for use in examining equipment performance and for
    inclusion on the INTERMAGNET DVD. The first section contains the observed baseline values on those
    days on which they were measured. Consequently, the number of entries will depend upon the schedule for
    absolute measurements at that observatory. The second section contains adopted baseline values
    representing each day of the year. A comment section is also provided.
    COMP_HHHHH_IDC_YEARCrLf
    DDD_AAAAAAA_BBBBBBB_ZZZZZZZ CrLf.
    . . . . .
    . . . . .
    . . . . .
    DDD_AAAAAAA_BBBBBBB_ZZZZZZZ CrLf.
    *
    001_AAAAAAA_BBBBBBB_ZZZZZZZ_FFFFF CrLf.
    002_AAAAAAA_BBBBBBB_ZZZZZZZ_FFFFF CrLf.
    003_AAAAAAA_BBBBBBB_ZZZZZZZ_FFFFF CrLf.
    ...
    366_AAAAAAA_BBBBBBB_ZZZZZZZ_FFFFF CrLf.
    *
    Comments:
    Component values are coded as signed integers, right-justified with a field width of 7. Total field (Delta
    F) values are coded as signed integers, right-justified with a field width of 5. The field widths must be
    maintained, either through zero-filling or space-filling. The '+' sign for positive values is optional.
    Field Description
    COMP Order of components HDZF, XYZF, DIF, UVZF
    HHHHH Annual mean value of H component in nT.
    IDC IAGA three-letter observatory ID code eg: BOU for Boulder, OTT for Ottawa, LER for
    Lerwick, etc.
    YEAR 4-digit Year: for example, 1991.
    DDD Day of the year.
    AAAAAAA Signed value of H, D, U or X in 0.1 nT
    BBBBBBB Signed value of D, I, V or Y in 0.1 nT or 0.1 min of arc for D
    ZZZZZZZ Signed value of Z or F in 0.1 nT
    FFFFF Signed value of Delta F, the difference between calculated and observed value of F (by a
    proton magnetometer) in 0.1 nT
    * Section separator.
    _ Space character.
    CrLf Indicates Carriage Return and Line Feed.
    Missing values must be replaced by 999999 for D, H, X, Y, Z and by 9999 for F.
    File name convention is IAGYR.BLV where:
    IAG = 3-letter observatory IAGA code
    YR = 2-digit year
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    mode = kwargs.get('mode')
    debug = kwargs.get('debug')
    getfile = True
    obscode = ''
    year = 1900

    head, tail = os.path.split(filename)
    tail = tail.lower()
    fname = tail.replace(".blv","")
    obscode = fname[:3].upper()
    ye = int(fname[-2:])
    if ye < 50:
        year = str(2000+ye)
    else:
        year = str(1900+ye)

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    data = []
    key = None

    # get day from filename (platform independent)
    theday = extract_date_from_string(filename)
    try:
        year = str(theday[0].year)
    except:
        pass
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

    array = [[] for key in KEYLIST]
    farray = [[] for key in KEYLIST]
    funclist = []
    xpos = KEYLIST.index('dx')
    ypos = KEYLIST.index('dy')
    zpos = KEYLIST.index('dz')
    fpos = KEYLIST.index('df')
    scalarid = 'Scalar'
    varioid = 'Variometer'
    pierid = 'Pier'
    comments = ''

    starfound = []
    if getfile:
        for line in fh:
            block = line.split()
            if line.isspace():
                # blank line
                continue
            elif line.startswith('XYZ') or line.startswith('DIF') or line.startswith('HDZ') or line.startswith('UVZ') or line.startswith('DHZ') and not len(starfound) > 0:
                # data info
                if len(block) == 4:
                    year = block[-1]
                headers['DataComponents'] = block[0]
                headers['col-{}'.format(KEYLIST[fpos])] = 'f base'
                headers['unit-col-{}'.format(KEYLIST[fpos])] = 'nT'
                headers['col-{}'.format(KEYLIST[zpos])] = 'z base'
                headers['unit-col-{}'.format(KEYLIST[zpos])] = 'nT'
                headers['unit-col-{}'.format(KEYLIST[xpos])] = 'nT'
                if headers['DataComponents'].startswith('HDZ') or headers['DataComponents'].startswith('hdz'):
                     headers['col-{}'.format(KEYLIST[xpos])] = 'h base'
                     headers['col-{}'.format(KEYLIST[ypos])] = 'd base'
                     headers['unit-col-{}'.format(KEYLIST[ypos])] = 'deg'
                if headers['DataComponents'].startswith('XYZ') or headers['DataComponents'].startswith('xyz'):
                     headers['col-{}'.format(KEYLIST[xpos])] = 'x base'
                     headers['col-{}'.format(KEYLIST[ypos])] = 'y base'
                     headers['unit-col-{}'.format(KEYLIST[ypos])] = 'nT'
                if headers['DataComponents'].startswith('DIF') or headers['DataComponents'].startswith('dif'):
                     headers['col-{}'.format(KEYLIST[xpos])] = 'd base'
                     headers['unit-col-{}'.format(KEYLIST[xpos])] = 'deg'
                     headers['col-{}'.format(KEYLIST[ypos])] = 'i base'
                     headers['unit-col-{}'.format(KEYLIST[ypos])] = 'deg'
                     headers['col-{}'.format(KEYLIST[zpos])] = 'f base'
                if headers['DataComponents'].startswith('UVZ') or headers['DataComponents'].startswith('uvz'):
                     headers['col-{}'.format(KEYLIST[xpos])] = 'u base'
                     headers['col-{}'.format(KEYLIST[ypos])] = 'v base'
                     headers['unit-col-{}'.format(KEYLIST[ypos])] = 'nT'
                headers['DataScaleX'] = float(block[1])
                if len(block) == 4:
                    obscode = block[-2]
                headers['StationID'] = obscode
                headers['StationIAGAcode'] = obscode
            elif headonly:
                # skip data for option headonly
                return
            elif len(block) in [4,5] and not len(starfound) > 0 and int(block[0]) < 367:  # block 1 - basevalues
                # data basevalues
                if not mode == 'adopted':
                    block = line.split()
                    block = [el if not float(el) > 999998.00 else np.nan for el in block]
                    doy = str(int(block[0])).zfill(3)
                    dttime = datetime.strptime(year+'-'+doy, "%Y-%j")+timedelta(hours=12)
                    if dttime in array[0]:
                        dttime = dttime+timedelta(seconds=1)
                    array[0].append(dttime)
                    comps = headers.get('DataComponents', '').upper()
                    if comps.startswith('DIF'):
                        array[xpos].append(float(block[1])/10./60.0)
                    else:
                        array[xpos].append(float(block[1])/10.)
                    if comps.startswith('HDZ') or comps.startswith('DIF'):
                        array[ypos].append(float(block[2])/10./60.0)
                    else:
                        array[ypos].append(float(block[2])/10.)
                    array[zpos].append(float(block[3])/10.)
                    if len(block) == 5:
                        array[fpos].append(float(block[4])/10.)
            elif len(block) in [4,5] and len(starfound) == 1 and int(block[0]) < 367:  # block 2 - adopted basevalues
                # adopted basevalues
                if float(block[1])>888887.0:
                    block[1] = np.nan
                if float(block[2])>888887.0:
                    block[2] = np.nan
                if float(block[3])>888887.0:
                    block[3] = np.nan
                if len(block) == 5:
                    if float(block[4])>8887.0:
                        block[4] = np.nan
                doy = str(int(block[0])).zfill(3)
                dt = datetime.strptime(year+'-'+doy, "%Y-%j")+timedelta(hours=12)
                comps = headers.get('DataComponents', '').upper()
                if comps.startswith('DIF'):
                    xval = float(block[1])/10./60.0
                else:
                    xval = float(block[1])/10.
                if comps.startswith('HDZ') or comps.startswith('DIF'):
                    yval = float(block[2])/10./60.0
                else:
                    yval = float(block[2])/10.
                zval = float(block[3])/10.
                if len(block) == 5:
                    dfval = float(block[4])/10.
                else:
                    dfval = 9999
                if mode == 'adopted':
                    array[0].append(dt)
                    array[xpos].append(xval)
                    array[ypos].append(yval)
                    array[zpos].append(zval)
                    array[fpos].append(dfval)
                else:
                    try:
                        farray[0].append(dt)
                        farray[xpos].append(xval)
                        farray[ypos].append(yval)
                        farray[zpos].append(zval)
                        farray[fpos].append(dfval)
                    except:
                        pass
            elif line.startswith('*'):
                # data info
                starfound.append('*')
                if len(starfound) > 1 and not mode == 'adopted': # Comment section starts here
                    if debug:
                        print("Fitting from {} to {}".format(farray[0][0], farray[0][-1]))
                    tempstream = DataStream(header={}, ndarray=np.asarray([np.asarray(el) for el in farray],dtype=object))
                    tempstream = tempstream._remove_nancolumns()
                    func1 = tempstream.fit([KEYLIST[xpos],KEYLIST[ypos], KEYLIST[zpos]],fitfunc='spline')
                    funclist.append(func1)
                    if len(tempstream.ndarray[fpos]) > 0:
                        func2 = tempstream.fit([KEYLIST[fpos]],fitfunc='spline')
                        funclist.append(func2)

            elif len(starfound) > 1: # Comment section starts here
                logger.debug("Found comment section", starfound)
                if block[0].startswith('Scalar') and len(block) > 1:
                    scalarid = block[1]
                elif block[0].startswith('Vario') and len(block) > 1:
                    varioid = block[1]
                elif block[0].startswith('Pier') and len(block) > 1:
                    pierid = block[1]
                else:
                    comments += "{} ".format(line.strip())
            else:
                pass
    fh.close()

    array = [np.asarray(el) for el in array]
    if len(funclist) > 0:
        headers['DataFunctionObject'] = funclist
    if comments:
        headers['DataComments'] = comments
    headers['DataFormat'] = 'MagPyDI'
    headers['DataType'] = 'MagPyDI0.1'
    if varioid == 'Variometer':
        varioid += obscode.upper()
    if scalarid == 'Scalar':
        scalarid += obscode.upper()
    if pierid == 'Pier':
        pierid += obscode.upper()
    headers['SensorID'] = 'BLV_{}_{}_{}'.format(varioid,scalarid,pierid)

    return DataStream(header=headers, ndarray=np.asarray(array,dtype=object))


def readBLV(filename, headonly=False, **kwargs):
    """
    Reading INTERMAGNET BLV data format (IBFV2.00)
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    mode = kwargs.get('mode')
    debug = kwargs.get('debug')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    headers = {}

    data = []
    key = None
    year = 1900

    # get day from filename (platform independent)
    theday = extract_date_from_string(filename)
    try:
        year = str(theday[0].year)
    except:
        pass
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

    array = [[] for key in KEYLIST]
    farray = [[] for key in KEYLIST]
    funclist = []
    xpos = KEYLIST.index('dx')
    ypos = KEYLIST.index('dy')
    zpos = KEYLIST.index('dz')
    fpos = KEYLIST.index('df')
    dfpos = KEYLIST.index('f')
    strpos = KEYLIST.index('str1')
    scalarid = 'Scalar'
    varioid = 'Variometer'
    pierid = 'Pier'
    obscode = ''
    comments = ''

    starfound = []
    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif len(line) in [26,27] and not len(starfound) > 0:
                # data info
                block = line.split()
                year = block[-1]
                headers['DataComponents'] = block[0]
                headers['col-{}'.format(KEYLIST[fpos])] = 'f base'
                headers['unit-col-{}'.format(KEYLIST[fpos])] = 'nT'
                headers['col-{}'.format(KEYLIST[zpos])] = 'z base'
                headers['unit-col-{}'.format(KEYLIST[zpos])] = 'nT'
                headers['unit-col-{}'.format(KEYLIST[xpos])] = 'nT'
                if headers['DataComponents'].startswith('HDZ') or headers['DataComponents'].startswith('hdz'):
                     headers['col-{}'.format(KEYLIST[xpos])] = 'h base'
                     headers['col-{}'.format(KEYLIST[ypos])] = 'd base'
                     headers['unit-col-{}'.format(KEYLIST[ypos])] = 'deg'
                if headers['DataComponents'].startswith('XYZ') or headers['DataComponents'].startswith('xyz'):
                     headers['col-{}'.format(KEYLIST[xpos])] = 'x base'
                     headers['col-{}'.format(KEYLIST[ypos])] = 'y base'
                     headers['unit-col-{}'.format(KEYLIST[ypos])] = 'nT'
                if headers['DataComponents'].startswith('DIF') or headers['DataComponents'].startswith('dif'):
                     headers['col-{}'.format(KEYLIST[xpos])] = 'd base'
                     headers['unit-col-{}'.format(KEYLIST[xpos])] = 'deg'
                     headers['col-{}'.format(KEYLIST[ypos])] = 'i base'
                     headers['unit-col-{}'.format(KEYLIST[ypos])] = 'deg'
                     headers['col-{}'.format(KEYLIST[zpos])] = 'f base'
                if headers['DataComponents'].startswith('UVZ') or headers['DataComponents'].startswith('uvz'):
                     headers['col-{}'.format(KEYLIST[xpos])] = 'u base'
                     headers['col-{}'.format(KEYLIST[ypos])] = 'v base'
                     headers['unit-col-{}'.format(KEYLIST[ypos])] = 'nT'
                headers['DataScaleX'] = float(block[1])
                headers['DataScaleZ'] = float(block[2])
                headers['StationID'] = block[3]
                obscode = block[3]
                headers['StationIAGAcode'] = block[3]
            elif headonly:
                # skip data for option headonly
                return
            elif len(line) in [44,45] and not len(starfound) > 1:  # block 1 - basevalues
                # data info
                if not mode == 'adopted':
                    block = line.split()
                    block = [el if not float(el) == 99999.00 and not float(el) == 88888.00 else np.nan for el in block]
                    dttime = datetime.strptime(year+'-'+block[0], "%Y-%j")+timedelta(hours=12)
                    if dttime in array[0]:
                        dttime = dttime+timedelta(seconds=1)
                    array[0].append(dttime)
                    comps = headers.get('DataComponents', '').upper()
                    if comps.startswith('DIF'):
                        array[xpos].append(float(block[1])/60.0)
                    else:
                        array[xpos].append(float(block[1]))
                    if comps.startswith('HDZ') or comps.startswith('DIF'):
                        array[ypos].append(float(block[2])/60.0)
                    else:
                        array[ypos].append(float(block[2]))
                    array[zpos].append(float(block[3]))
                    if not block[4] == 999.0 and not block[4] == 888.0:
                        array[fpos].append(float(block[4]))
                    else:
                        array[fpos].append(np.nan)
            elif len(line) in [54,55] and not len(starfound) > 1:  # block 2 - adopted basevalues
                # data info
                block = line.split()
                if float(block[5])==999.0 or float(block[5])==888.0:
                    block[5] = np.nan
                if float(block[1])>88887.0:
                    block[1] = np.nan
                if float(block[2])>88887.0:
                    block[2] = np.nan
                if float(block[3])>88887.0:
                    block[3] = np.nan
                if float(block[4])>88887.0:
                    block[4] = np.nan
                dt = datetime.strptime(year+'-'+block[0], "%Y-%j")+timedelta(hours=12)
                comps = headers.get('DataComponents', '').upper()
                if comps.startswith('DIF'):
                    xval = float(block[1]) / 60.0
                else:
                    xval = float(block[1])
                if comps.startswith('HDZ') or comps.startswith('DIF'):
                    yval = float(block[2])/60.0
                else:
                    yval = float(block[2])
                zval = float(block[3])
                fval = float(block[4])
                dfval = float(block[5])
                strval = block[6]
                if mode == 'adopted':
                    if strval in ['d','D']:
                        if debug:
                            print ("Found break at {}".format(block[0]))
                            print ("Adding nan column for jumps in plot")
                        array[0].append(dt-timedelta(days=0.5))
                        array[xpos].append(np.nan)
                        array[ypos].append(np.nan)
                        array[zpos].append(np.nan)
                        array[fpos].append(np.nan)
                        array[dfpos].append(np.nan)
                        array[strpos].append('a')
                    array[0].append(dt)
                    array[xpos].append(xval)
                    array[ypos].append(yval)
                    array[zpos].append(zval)
                    array[fpos].append(fval)
                    array[dfpos].append(dfval)
                    array[strpos].append(strval)
                else:
                    try:
                        if strval in ['d','D']:
                            # use the current time step as last one for fit and then add this step as first
                            # element for the next fit
                            if debug:
                                print ("Fitting from {} to {}".format(farray[0][0],farray[0][-1]))
                            tempstream = DataStream(header={}, ndarray=np.asarray([np.asarray(el) for el in farray],dtype=object))
                            tempstream = tempstream._remove_nancolumns()
                            if 1. / len(tempstream) < 0.01:
                                knotstep = 0.01
                            else:
                                knotstep = 1. / len(tempstream) + 0.01
                            func1 = tempstream.fit([KEYLIST[xpos], KEYLIST[ypos], KEYLIST[zpos]],fitfunc='spline',knotstep=knotstep)
                            func2 = tempstream.fit([KEYLIST[fpos]],fitfunc='spline',knotstep=knotstep)
                            funclist.append(func1)
                            funclist.append(func2)
                            farray = [[] for key in KEYLIST]
                        farray[0].append(dt)
                        farray[xpos].append(xval)
                        farray[ypos].append(yval)
                        farray[zpos].append(zval)
                        farray[fpos].append(fval)
                        farray[dfpos].append(dfval)
                        farray[strpos].append(strval)
                    except:
                        pass
            elif line.startswith('*'):
                # data info
                starfound.append('*')
                if len(starfound) > 1 and not mode == 'adopted': # adopted values
                    if debug:
                        print("Fitting from {} to {}".format(farray[0][0], farray[0][-1]))
                    tempstream = DataStream(header={}, ndarray=np.asarray([np.asarray(el) for el in farray],dtype=object))
                    tempstream = tempstream._remove_nancolumns()
                    if 1./len(tempstream) < 0.01:
                        knotstep = 0.01
                    else:
                        knotstep = 1./len(tempstream) + 0.01
                    func1 = tempstream.fit([KEYLIST[xpos],KEYLIST[ypos], KEYLIST[zpos]],fitfunc='spline',knotstep=knotstep)
                    funclist.append(func1)
                    if len(tempstream.ndarray[fpos]) > 0:
                        func2 = tempstream.fit([KEYLIST[fpos]],fitfunc='spline',knotstep=knotstep)
                        funclist.append(func2)
                    if debug:
                        print("Done")
            elif len(starfound) > 1: # Comment section starts here
                if debug:
                    print ("Found comment section")
                logger.debug("Found comment section", starfound)
                block = line.split()
                if block[0].startswith('Scalar') and len(block) > 1:
                    scalarid = block[1]
                elif block[0].startswith('Vario') and len(block) > 1:
                    varioid = block[1]
                elif block[0].startswith('Pier') and len(block) > 1:
                    pierid = block[1]
                else:
                    comments += "{} ".format(line.strip())
            else:
                pass
    fh.close()

    array = [np.asarray(el) for el in array]
    if len(funclist) > 0:
        headers['DataFunctionObject'] = funclist
    if comments:
        headers['DataComments'] = comments
    if varioid == 'Variometer':
        varioid += obscode.upper()
    if scalarid == 'Scalar':
        scalarid += obscode.upper()
    if pierid == 'Pier':
        pierid += obscode.upper()
    headers['DataFormat'] = 'MagPyDI'
    headers['DataType'] = 'MagPyDI0.1'
    headers['SensorID'] = 'BLV_{}_{}_{}'.format(varioid,scalarid,pierid)

    return DataStream(header=headers, ndarray=np.asarray(array,dtype=object))


def writeBLV(datastream, filename, **kwargs):
    """
    DESCRIPTION:
        Writing Intermagnet - baseline data.
        uses baseline function
    PARAMETERS:
        datastream      : (DataStream) basevalue data stream
        filename        : (string) path

      Optional:
        deltaF          : (float) average field difference in nT between DI pier and F
                                   measurement position.
                          OR
                          (string) which is either 'mean', 'median', or an absinfo string
                                   providing fitting methods.
                          Mean or median require deltaF values in basevalue file.
                          DeltaF represents the adopted value for all days
        diff            : (ndarray) array containing daily averages of delta F values between
                          variometer and F measurement
    """

    absinfo = kwargs.get('absinfo')   # new in v0.3.95
    fitfunc = kwargs.get('fitfunc')   # replaced by absinfo
    fitdegree = kwargs.get('fitdegree')   # replaced by absinfo
    knotstep = kwargs.get('knotstep')   # replaced by absinfo
    extradays = kwargs.get('extradays')   # replaced by absinfo
    mode = kwargs.get('mode')
    year = kwargs.get('year')
    meanh = kwargs.get('meanh')
    meanf = kwargs.get('meanf')
    keys = kwargs.get('keys')   # replaced by absinfo
    deltaF = kwargs.get('deltaF')
    diff = kwargs.get('diff')

    parameterlist = False

    def getAbsInfo(absinfostring):
        """
        # extract parameter from DataAbsInfo list
        """
        # 1. remove komma from elements
        absinfostring = absinfostring.replace(', EPSG',' EPSG')
        absinfostring = absinfostring.replace(',EPSG',' EPSG')
        absinfostring = absinfostring.replace(', epsg',' EPSG')
        absinfostring = absinfostring.replace(',epsg',' EPSG')
        # 2. split absinfo list
        absinfolist = absinfostring.split(',')
        # check whether format looks ok
        if not (absinfolist[0].startswith('7') or absinfolist[0].startswith('1')) and not len(absinfolist) > 5:
            return None
        funclist = []
        for absi in absinfolist:
            parameter = absi.split('_')
            startabs=float(parameter[0])
            endabs=float(parameter[1])
            extradays=int(parameter[2])
            fitfunc=parameter[3]
            try:
                fitdegree=int(parameter[4])
            except:
                fitdegree=5
            try:
                knotstep=float(parameter[5])
            except:
                knotstep=0.1
            keys = parameter[6:9]
            print (startabs,endabs,extradays,fitfunc,fitdegree,knotstep,keys)
            line = [startabs,endabs,extradays,fitfunc,fitdegree,knotstep,keys]
            if len(parameter) >= 14:
                    #extract pier information
                    pierdata = True
                    pierlon = float(parameter[9])
                    pierlat = float(parameter[10])
                    pierlocref = parameter[11]
                    pierel = float(parameter[12])
                    pierelref =  parameter[13]
                    line.extend([pierlon,pierlat,pierlocref,pierel,pierelref])
            funclist.append(line)
        return funclist

    if not year:
        st, et = datastream.timerange()
        average_delta = (et - st) / 2
        average_ts = st + average_delta
        year = datetime.strftime(average_ts,'%Y')
    t1 = datetime.strptime(str(int(year))+'-01-01','%Y-%m-%d')
    t2 = datetime.strptime(str(int(year)+1)+'-01-01','%Y-%m-%d')

    absinfodiff = ''
    if diff:
        if diff.length()[0] > 1:
            if not absinfo:
                absinfodiff = diff.header.get('DataAbsInfo','')
            if not absinfodiff == '':
                logger.info("writeBLV: Getting Absolute info from header of provided dailymean file")
                absinfo = absinfodiff

    # getting functional parameters for adopted baseline
    if absinfo:
        parameterlist = getAbsInfo(absinfo)
    if not absinfo or not parameterlist:
        if not extradays:
            extradays = 15
        if not fitfunc:
            fitfunc = 'spline'
        if not fitdegree:
            fitdegree = 5
        if not knotstep:
            knotstep = 0.1
        if not keys:
            keys = ['dx','dy','dz']#,'df']
        parameterlist = [[t1,t2,extradays,fitfunc,fitdegree,knotstep,keys]]

    # Get functionlist
    # primarily use the baseline adoption function provided with the stream
    # override using optionally provided fit parameters
    basefunctionlist = datastream.header.get('DataFunctionObject')
    if not basefunctionlist:
        print ("writeBLV: baseline adoption function not part of the data header")
        basefunctionlist =[]
    if fitfunc and isinstance(fitfunc, list) and len(fitfunc) > 0:
        print ("writeBLV: baseline adoption function directly provided along with write call")
        basefunctionlist = fitfunc
    if len(basefunctionlist) < 1 and not absinfo:
        print ("writeBLV: no baseline adoption function(s) specified - using default values (spline with 15 days extrapolation)")

    logger.info("writeBLV: Extracted baseline parameter: {}".format(parameterlist))

    # 2. check whether file exists and according to mode either create, append, replace
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(exst,datastream)
            myFile= open( filename, "wt", newline='' )
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(datastream,exst)
            myFile= open( filename, "wt", newline='' )
        elif mode == 'append':
            myFile= open( filename, "at", newline='' )
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= open( filename, "wt", newline='' )
    else:
        myFile= open( filename, "wt", newline='' )

    #print ("filename", filename)
    logger.info("writeBLV: file: {}".format(filename))

    # 3. check whether datastream corresponds to an absolute file and remove unreasonable inputs
    #     - check whether F measurements were performed at the main pier - delta F's are available

    if not datastream.header.get('DataFormat','') == 'MagPyDI':
        if not  datastream.header.get('DataType','').startswith('MagPyDI'):
            logger.error("writeBLV: Unsupported format - convert to MagPyDI first")
            logger.error("  -- export BLV data -- too be done")
            logger.error("  -- eventually also not yet assigned when accessing database contents")
            return False

    # 4. create dummy stream with time range
    dummystream = DataStream()
    array = [[] for key in KEYLIST]
    array[0].append(t1)
    array[0].append(t2)
    indx = KEYLIST.index('dx')
    indy = KEYLIST.index('dy')
    indz = KEYLIST.index('dz')
    indf = KEYLIST.index('df')
    indFtype = KEYLIST.index('str4')
    for i in range(0,2):
        array[indx].append(0.0)
        array[indy].append(0.0)
        array[indz].append(0.0)
        array[indf].append(0.0)
    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])
    dummystream.ndarray = np.asarray(array, dtype=object)

    # 5. Extract the data for one year and calculate means
    backupabsstream = datastream.copy()

    datastream = datastream.trim(starttime=t1, endtime=t2)

    indf = KEYLIST.index('df')
    fbase = False
    if len([elem for elem in datastream.ndarray[indf] if not np.isnan(float(elem))]) > 0:
        keys = ['dx','dy','dz','df']
    else:
        if not deltaF:
            array = np.asarray([88888.00]*len(datastream.ndarray[0]))
            datastream = datastream._put_column(array, 'df')
        else:
            try:
                # TODO provided values can only be used as adopted scalar baseline
                # Should work
                delf = float(deltaF)
                array = np.asarray([delf]*len(datastream.ndarray[0]))
                datastream = datastream._put_column(array, 'df')
            except:
                fbase = getAbsInfo(deltaF)
                if fbase:
                    # doesnt make sense - is set false in a few lines
                    fbasefunc = True

    if keys == ['dx','dy','dz','df'] and is_number(deltaF):
        logger.info("writeBLV: found deltaF values, but using provided deltaF {} for adopted scalar baseline ".format(deltaF))

    fbasefunc = False
    if keys == ['dx','dy','dz','df'] and not is_number(deltaF) and not deltaF == None:
        if deltaF in ['mean','MEAN','Mean']:
            logger.info("writeBLV: MEAN deltaF: {}".format(datastream.mean('df',percentage=1)))
            deltaF = datastream.mean('df',percentage=1)
        elif deltaF in ['median','MEDIAN','Median']:
            logger.info("writeBLV: MEDIAN deltaF: {}".format(datastream.mean('df',percentage=1, meanfunction='median')))
            deltaF = datastream.mean('df',percentage=1, meanfunction='median')
        else:
            fbase = getAbsInfo(deltaF)
            if fbase:
                fbasefunc = True

    try:
        comps = datastream.header['DataComponents']
        if comps in ['IDFF','idff','idf','IDF']:
            datastream = datastream.idf2xyz()
            datastream = datastream.xyz2hdz()
            comps = 'HDZF'
        #elif comps in ['XYZF','xyzf','xyz','XYZ']:
        #    datastream = datastream.xyz2hdz()
        #comps = 'HDZF'
    except:
        # assume idf orientation
        datastream = datastream.idf2xyz()
        datastream = datastream.xyz2hdz()
        comps = 'HDZF'

    meanstream = datastream.extract('f', 0.0, '>')
    # new 2.0 : do not flag anything internally, the user should do that beforehand
    #if meanstream and len(meanstream) > 0:
    #    meanstream = meanstream.flag_outlier()
    #    meanstream = meanstream.remove_flagged()

    if not meanf:
        meanf = meanstream.mean('f')
    if not meanh:
        meanh = meanstream.mean('x')

    # 6. calculate baseline function
    t1num = date2num(t1)
    t2num = date2num(t2)
    if not len(basefunctionlist) > 0:
        basefunctionlist =[]
        keys = []
        print ("writeBLV: baseline functions will be obtained from parameterlist")
        for parameter in parameterlist:
            # check whether timerange is fitting
            if (parameter[0] >= t1num and parameter[0] <= t2num) or (parameter[1] >= t1num and parameter[1] <= t2num) or (parameter[0] < t1num and parameter[1] > t2num):
                keys = parameter[6]
                print ("writeBLV: calculating baseline .... using line", parameter, backupabsstream.length())
                basefunctionlist.append(dummystream.baseline(backupabsstream, startabs=parameter[0],endabs=parameter[1],keys=parameter[6], fitfunc=parameter[3],fitdegree=parameter[4],knotstep=parameter[5],extradays=parameter[2]))

    yar = [[] for key in KEYLIST]
    datelist = num2date([day+0.5 for day in range(int(t1num),int(t2num))])
    for idx, elem in enumerate(yar):
        if idx == 0:
            yar[idx] = np.asarray(datelist)
        elif idx in [indx,indy,indz,indf]:
            yar[idx] = np.asarray([0]*len(datelist))
        else:
            yar[idx] = np.asarray(yar[idx])

    yearstream = DataStream(header=datastream.header,ndarray=np.asarray(yar, dtype=object))
    yearstream = yearstream.func2stream(basefunctionlist,mode='addbaseline',keys=keys)

    if fbasefunc:
        logger.info("Adding adopted scalar from function {}".format(fbase))
        fbasefunclist = []
        for parameter in fbase:
            # check whether timerange is fitting
            if (parameter[0] >= t1num and parameter[0] <= t2num) or (parameter[1] >= t1num and parameter[1] <= t2num) or (parameter[0] < t1num and parameter[1] > t2num):
                fbasefunclist.append(dummystream.baseline(backupabsstream,startabs=parameter[0],endabs=parameter[1],keys=parameter[6], fitfunc=parameter[3],fitdegree=parameter[4],knotstep=parameter[5],extradays=parameter[2]))
        yearstream = yearstream.func2stream(fbasefunclist,mode='values',keys=['df'])


    # 7. Get essential header info
    header = datastream.header
    try:
        idc = header['StationID']
    except:
        logging.error("formatBLV: No station code specified. Aborting ...")
        return False
    headerline = '%s %5.f %5.f %s %s\r\n' % (comps.upper(),meanh,meanf,idc,year)
    myFile.writelines( headerline ) #.decode('ascii').encode('utf-8') )

    # 8. Basevalues
    if len(datastream.ndarray[0]) > 0:
        logger.debug("writeBLV: {}".format(datastream.ndarray[indFtype]))
        logger.debug("writeBLV: {}".format(datastream.ndarray))
        logger.debug("writeBLV: {}".format(datastream.length()))
        for idx, elem in enumerate(datastream.ndarray[0]):
            if t2 >= elem >= t1:
                day = datetime.strftime(elem,'%j')
                x = float(datastream.ndarray[indx][idx])
                if comps.lower() in ['xyzf','xyz']:
                    y = float(datastream.ndarray[indy][idx])
                else:
                    y = float(datastream.ndarray[indy][idx])*60.
                #y = float(datastream.ndarray[indy][idx])*60.
                z = float(datastream.ndarray[indz][idx])
                df = float(datastream.ndarray[indf][idx])
                try:
                    ftype = datastream.ndarray[indFtype][idx]
                except:
                    ftype = '' ## exception for only-numerical columns -> xmagpy
                if np.isnan(x):
                    x = 99999.00
                if np.isnan(y):
                    y = 99999.00
                if np.isnan(z):
                    z = 99999.00
                if np.isnan(df) or ftype.startswith('Fext'):
                    df = 99999.00
                elif deltaF and is_number(deltaF):
                    # TODO check the sign
                    df = df + deltaF
                line = '%s %9.2f %9.2f %9.2f %9.2f\r\n' % (day,x,y,z,df)
                myFile.writelines( line ) #.encode('utf-8') )
    else:
        datastream = datastream.trim(starttime=t1, endtime=t2)
        for elem in datastream:
            #DDD_aaaaaa.aa_bbbbbb.bb_zzzzzz.zz_ssssss.ssCrLf
            day = datetime.strftime(num2date(elem.time),'%j')
            if np.isnan(elem.x):
                x = 99999.00
            else:
                if not elem.typ == 'idff':
                    x = elem.x
                else:
                    x = elem.x*60
            if np.isnan(elem.y):
                y = 99999.00
            else:
                if elem.typ == 'xyzf':
                    y = elem.y
                else:
                    y = elem.y*60
            if np.isnan(elem.z):
                z = 99999.00
            else:
                z = elem.z
            if np.isnan(elem.df):
                f = 99999.00
            elif deltaF and is_number(deltaF):
                f = elem.df + deltaF
            else:
                f = elem.df
            line = '%s %9.2f %9.2f %9.2f %9.2f\r\n' % (day,x,y,z,f)
            myFile.writelines( line ) #.encode('utf-8') )

    # 9. adopted basevalues
    myFile.writelines( '*\r\n' )
    posstr = KEYLIST.index('str1')

    if not len(yearstream.ndarray[posstr]) > 0:
        parameterlst = ['c']*len(yearstream.ndarray[0])
    else:
        parameterlst = yearstream.ndarray[posstr]

    for idx, t in enumerate(yearstream.ndarray[0]):
        #001_AAAAAA.AA_BBBBBB.BB_ZZZZZZ.ZZ_SSSSSS.SS_DDDD.DD_mCrLf
        day = datetime.strftime(t,'%j')
        parameter = parameterlst[idx]
        if not len(yearstream.ndarray[indx])>0:
            x = 99999.00
        elif np.isnan(yearstream.ndarray[indx][idx]):
            x = 99999.00
        else:
            if not comps.lower() == 'idff':
                x = yearstream.ndarray[indx][idx]
            else:
                x = yearstream.ndarray[indx][idx]*60.
        if not len(yearstream.ndarray[indy])>0:
            y = 99999.00
        elif np.isnan(yearstream.ndarray[indy][idx]):
            y = 99999.00
        else:
            if comps.lower() == 'xyzf':
                y = yearstream.ndarray[indy][idx]
            else:
                y = yearstream.ndarray[indy][idx]*60.
        if not len(yearstream.ndarray[indz])>0:
            z = 99999.00
        elif np.isnan(yearstream.ndarray[indz][idx]):
            z = 99999.00
        else:
            z = yearstream.ndarray[indz][idx]
        if deltaF and is_number(deltaF):
            f = deltaF
        elif deltaF: # and is_number(deltaF):
            f = yearstream.ndarray[indf][idx]
        elif not len(yearstream.ndarray[indf])>0:
            f = 99999.00
        elif np.isnan(yearstream.ndarray[indf][idx]):
            f = 99999.00
        else:
            f = yearstream.ndarray[indf][idx]
        if diff:
            #print ("writeBLV: Here", t, diff.length()[0])
            posdf = KEYLIST.index('df')
            difft = [datetime.strftime(el, '%j') for el in diff.ndarray[0]]
            if day in difft:
                ind = difft.index(day)
                df = diff.ndarray[posdf][ind]
                if np.isnan(df):
                    df = 999.00
            else:
                df = 999.00
        else:
            df = 888.00
        line = '%s %9.2f %9.2f %9.2f %9.2f %7.2f %s\r\n' % (day,x,y,z,f,df,parameter)
        myFile.writelines( line ) #.encode('utf-8') )

    # 9. comments
    myFile.writelines( '*\r\n' ) #.encode('utf-8') )
    myFile.writelines( 'Comments:\r\n' ) #.encode('utf-8') )
    myFile.writelines( '-'*40 + '\r\n' ) #.encode('utf-8') )
    commentstring = datastream.header.get('DataComments','')
    # split comments to individual string of length x
    absinfostring = dummystream.header.get('DataAbsInfo','')
    parameterlist = getAbsInfo(absinfostring)
    #print ("writeBLV", absinfostring, parameterlist)
    if not absinfostring == '':
        for parameter in parameterlist:
            funcline1 = '+++++++\r\n'
            funcline2 = 'Adopted baseline between {} and {}\r\n'.format(str(num2date(float(parameter[0])).replace(tzinfo=None)),str(num2date(float(parameter[1])).replace(tzinfo=None)))
            if parameter[3].startswith('poly'):
                funcline3 = 'Baselinefunction: {}, Degree: {}, keys: {}\r\n'.format(parameter[3],parameter[4],','.join(parameter[6]))
            else:
                funcline3 = 'Baselinefunction: {}, relative knot distance: {}, keys: {}\r\n'.format(parameter[3],parameter[5],','.join(parameter[6]))
            funcline4 = 'Comment: please add\r\n'
            myFile.writelines( funcline1 ) #.encode('utf-8') )
            myFile.writelines( funcline2 ) #.encode('utf-8') )
            myFile.writelines( funcline3 ) #.encode('utf-8') )
            myFile.writelines( funcline4 ) #.encode('utf-8') )

    # get some data:
    infolist = [] # contains all provided information for comment section
    posst2 = KEYLIST.index('str2')
    posst3 = KEYLIST.index('str3')
    posst4 = KEYLIST.index('str4')
    if len(datastream.ndarray[posst2]) > 0:
        infolist.append(datastream.ndarray[posst2][-1])
    if len(datastream.ndarray[posst3]) > 0:
        infolist.append(datastream.ndarray[posst3][-1])
    if len(datastream.ndarray[posst4]) > 0:
        infolist.append(datastream.ndarray[posst4][-1])


    myFile.writelines( '-'*40 + '\r\n' ) #.encode('utf-8') )
    funcline5 = 'Measurements conducted primarily with:\r\n'
    myFile.writelines( funcline5 ) #.encode('utf-8') )
    if len(infolist) > 0:
        funcline6 = 'DI: %s\r\n' % infolist[0]
        myFile.writelines( funcline6 ) #.encode('utf-8') )
    if len(infolist) > 1:
        funcline7 = 'Scalar: %s\r\n' % infolist[1]
        myFile.writelines(funcline7)  # .encode('utf-8') )
    if len(infolist) > 2:
        funcline8 = 'Variometer: %s\r\n' % infolist[2]
        myFile.writelines( funcline8 ) #.encode('utf-8') )
    funcline9 = 'Pier: %s\r\n' % datastream.header.get('DataPier','-')
    # additional text with pier, instrument, how f difference is defined, which are the instruments etc
    summaryline = '-- analysis supported by MagPy\r\n'
    myFile.writelines( funcline9 ) #.encode('utf-8') )
    myFile.writelines( '-'*40 + '\r\n' ) #.encode('utf-8') )
    myFile.writelines( summaryline ) #.encode('utf-8') )  changed open to 'wt' -> no encoding to binary necessary

    myFile.close()
    print ("writeBLV: done")
    return True


def readIYFV(filename, headonly=False, **kwargs):
    """
    DESCRIPTION:
        Reads annual mean values. Elements given in column ELE are imported.
        Other components are calculated and checked against file content.
    PARAMETER:

                      ANNUAL MEAN VALUES

                      ALMA-ATA, AAA, KAZAKHSTAN

  COLATITUDE: 46.75   LONGITUDE: 76.92 E   ELEVATION: 1300 m

  YEAR        D        I       H      X      Y      Z      F   * ELE Note
           deg min  deg min    nT     nT     nT     nT     nT

 2005.500   4 46.6  62 40.9  25057  24970   2087  48507  54597 A XYZF   1
 2006.500   4 47.5  62 42.9  25044  24957   2092  48552  54631 A XYZF   1
 2007.500   4 47.8  62 45.8  25017  24930   2092  48603  54664 A XYZF   1

    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    kind = kwargs.get('kind')
    debug = kwargs.get('debug')

    if debug:
        print ("readIYVF: Reading yearmean file {}".format(filename))

    if not starttime:
        starttime = datetime(1516,1,1)
    else:
        starttime = testtime(starttime)
    if not endtime:
        endtime = datetime.now()
    else:
        endtime = testtime(endtime)
    stream = DataStream()
    headers = {}
    dataarray = []

    array = [[] for key in KEYLIST]

    cnt = 0
    ln = 0 # line number
    comm = '' # comment
    paracnt = 999998
    roworder=['d','i','h','x','y','z','f']
    jumpx,jumpy,jumpz,jumpf=0,0,0,0
    if not kind or not kind in ['I','Q','D']:
        tsel = ['A'] # Use only all days rows
    else:
        tsel = [kind]
    if kind == 'I':
        tsel = ['A','I'] # Use only all days rows
    tprev = tsel[0] # For jump treatment
    lc = KEYLIST.index('var5')  ## store the line number of each loaded line here
                                ## this is used by writeIYFV to add at the correct position
    mainele = KEYLIST.index('str1')  ## store the recording elements
    stkind = KEYLIST.index('str2')  ## store the kind
    newarray = []
    units = []
    para = []
    goodval = False
    ele = 'XYZ'

    headfound = False
    latitudefound = False

    def dropnonascii(text):
        return ''.join([i if ord(i) < 128 else ' ' for i in text])

    if debug:
        print ("readIYVF: Reading data (only lines with {}) ...".format(tsel))

    code = 'rt'
    fh = open(filename, code, errors='ignore')  # python3

    for line in fh:
        ln = ln+1
        line = dropnonascii(line)
        line = line.rstrip()
        try:
            tyear = int(line[:5])
        except:
            tyear = 99999
        cnt = cnt+1
        #if debug:
        #    print(line)
        if line.isspace():
            # blank line
            pass
        elif line.find('ANNUAL') > 0 or line.find('annual') > 0:
            headfound = True
            pass
        elif headfound and not latitudefound and cnt >= 3 and cnt < 6 and not line.find('COLATITUDE') > 0 and len(line) > 0:
            # station info
            block = line.split(',')
            try:
                headers['StationName'] = block[0].strip()
            except:
                pass
            try:
                headers['StationID'] = block[1].strip()
                headers['StationIAGAcode'] = block[1].strip()
            except:
                pass
            try:
                headers['StationCountry'] = block[2].strip()
            except:
                pass
        elif line.find('COLATITUDE') > 0 and not latitudefound:
            latitudefound = True
            loc = line.split()
            headers['DataAcquisitionLatitude'] = 90.0-float(loc[1])
            headers['DataAcquisitionLongitude'] = float(loc[3])
            ele = line.split('ELEVATION:')[1]
            headers['DataElevation'] = float(ele.split()[0])
        elif line.find(' YEAR ') > 0:
            paracnt = cnt
            para = line.split()
            para = [elem.lower() for elem in para[1:8]]
        elif cnt == paracnt+1:
            units = line.split()
            tmp = ['deg','deg']
            tmp.extend(units[4:10])
            units = tmp
        elif tyear < 3000 and tyear > 1516: # Upcoming year 3k problem ;)
            try:
                if not headonly:
                    if line.find('J') > 0: # JUMP
                        line = line.replace('     0.0','  0  0.0')
                    data = line.split()
                    ye = data[0].split('.')
                    dat = ye[0] + '-06-01'
                    ti = datetime.strptime(dat, "%Y-%m-%d")
                    t = data[10] # only successful if long enough
                    if t in tsel or t == 'J':
                        if ti >= starttime and ti <= endtime: # and not len(data) >= 12:
                            if t == 'J' and tprev in tsel: # otherwise it will also add jumps in Q and D blocks if A selected
                                dataarray.append(data)
                            elif not t == 'J':
                                dataarray.append(data)
                    tprev = t
            except:
                pass
        else:
            if ln>10:
                comm += "{}\n".format(line)
            pass
    fh.close()
    dataarray = sorted(dataarray, reverse=True)
    # now apply the jump values and create the data stream
    headers['col-x'] = 'x'
    headers['unit-col-x'] = 'nT'
    headers['col-y'] = 'y'
    headers['unit-col-y'] = 'nT'
    headers['col-z'] = 'z'
    headers['unit-col-z'] = 'nT'
    headers['col-f'] = 'f'
    headers['unit-col-f'] = 'nT'
    headers['DataComponents'] = 'XYZF'
    for data in dataarray:
        if not data[10] == 'J':
            ye = data[0].split('.')
            dat = ye[0] + '-06-01'
            ti = datetime.strptime(dat, "%Y-%m-%d")
            array[0].append(ti)
            array[lc].append(cnt)
            if len(data) > 8 and is_number(data[6]) and is_number(data[7]) and is_number(data[8]) and is_number(data[9]) and float(data[6]) < 999999 and float(data[7]) < 999999 and float(data[8]) < 999999 and float(data[9]) < 999999:
                array[1].append(float(data[6]) - jumpx)
                array[2].append(float(data[7]) - jumpy)
                array[3].append(float(data[8]) - jumpz)
                array[4].append(float(data[9]) - jumpf)
            else:
                array[1].append(np.nan)
                array[2].append(np.nan)
                array[3].append(np.nan)
                array[4].append(np.nan)
            t = data[10]
            ele = data[11]
            array[mainele].append(ele)
            array[stkind].append(t)
        else:
            ye = data[0].split('.')
            dat = ye[0] + '-01-01'
            ti = datetime.strptime(dat, "%Y-%m-%d")
            comment = "jump line: {}\n".format(data)
            comm += comment
            if debug:
                print("Found jump value at {}".format(ti))
            jumpx += float(data[6])
            jumpy += float(data[7])
            jumpz += float(data[8])
            jumpf += float(data[9])

    array = [np.asarray(ar,dtype=object) for ar in array]
    stream = DataStream(header=headers, ndarray=np.asarray(array,dtype=object))

    #if not ele.lower().startswith('xyz') and ele.lower()[:3] in ['xyz','hdz','dhz','hez','idf']:
    #    if ele.lower()[:3] in ['hdz','dhz']: # exception for usgs
    #        ele = 'hdz'
    #    stream = stream._convertstream('xyz2'+ele.lower()[:3])

    stream.header['SensorID'] = "{}_{}".format(headers.get('StationID','XXX').upper(),'IYFV')
    stream.header['DataComments'] = comm
    if debug:
        print ("readIYVF: Got data ...")

    return stream


def writeIYFV(datastream,filename, **kwargs):
    """
    DESCRIPTION:
        IYFV requires a datastream containing one year of data (if not kind='Q' or 'D' are given).
        Method calculates mean values and adds them to an eventually existing yearly mean file.
        Please note: jumps (J) need to be defined manually within the ASCII mean file.
    PARAMETERS:
        datastream:     (DataStream) containing the header info and one year of data.
                                     DataComponents should be provided in header.
                                     If data

        kind:           (string) One of Q,D,A -> default is A

    """

    kind = kwargs.get('kind')
    note = 0

    if not kind in ['A','Q','D','q','d']:
        kind = 'A'
    else:
        kind = kind.upper()

    # check datastream
    if not datastream.length()[0] > 1:
        logger.error(" writeIYFV: Datastream does not contain data")
        return False
    if not len(datastream.ndarray[1]) > 1:
        logger.error(" writeIYFV: Datastream does not contain data")
        return False
    # check time range
    tmin, tmax = datastream._find_t_limits()
    tmin = date2num(tmin)
    tmax = date2num(tmax)
    meant = np.mean([tmin,tmax])
    if tmax-tmin < 365*0.9: # 90% of one year
        logger.error(" writeIYFV: Datastream does not cover at least 90% of one year")
        if not kind in ['Q', 'D', 'q', 'd']:
            kind = 'I'
        #return False
    # if timerange covers more than one year ??????
    # should be automatically called with coverage='year' and filenamebegins='yearmean',
    # filenameends=Obscode

    header = datastream.header
    comp = header.get('DataComponents','')
    comp = comp.lower()
    logger.info(("writeIYFV: components found: {}".format(comp)))
    if not comp in ['hdz','xyz','idf','hez', 'hdzf','xyzf','idff','hezf', 'hdzg','xyzg','idfg','hezg']:
        logger.warning(" writeIYFV: valid DataComponents could not be read from header - assuming xyz data")
        comp = 'xyz'
    elif comp.startswith('hdz'):
        datastream = datastream.hdz2xyz()
    elif comp.startswith('idf'):
        datastream = datastream.idf2xyz()
    elif comp.startswith('hez'):
        alpha = header.get('DataSensorAzimuth','')
        if not is_number(alpha):
            logger.error(" writeIYFV: hez provided but no DataSensorAzimuth (usually the declination while sensor installation - aborting")
            return False
        datastream = datastream.rotation(alpha=alpha)

    # Obtain means   ( drop nans ):
    meanx = datastream.mean('x',percentage=90)
    meany = datastream.mean('y',percentage=90)
    meanz = datastream.mean('z',percentage=90)
    if np.isnan(meanx) or np.isnan(meany) or np.isnan(meanz):
        logger.warning(" writeIYFV: found more then 10% of NaN values - setting minimum requirement to 10% data recovery and change kind to I (incomplete)")
        meanx = datastream.mean('x',percentage=10)
        meany = datastream.mean('y',percentage=10)
        meanz = datastream.mean('z',percentage=10)
        if not kind in ['Q', 'D', 'q', 'd']:
            kind = 'I'
        if np.isnan(meanx) or np.isnan(meany) or np.isnan(meanz):
            logger.error(" writeIYFV: less then 10% of data - aborting")
            return False
    meanyear = int(datetime.strftime(num2date(meant),"%Y"))
    # create datalist
    datalist = [meanyear]
    reslist = coordinatetransform(meanx,meany,meanz,'xyz')
    datalist.extend(reslist)

    logger.info( "writeIYFV means: {}, {}, {}".format(meanx, meany, meanz ))
    #print ( "writeIYFV: kind", kind )
    #print ( "writeIYFV: comment", comment )
    #kind = 'Q'
    #meanyear = '2011'

    #_YYYY.yyy_DDD_dd.d_III_ii.i_HHHHHH_XXXXXX_YYYYYY_ZZZZZZ_FFFFFF_A_EEEE_NNNCrLf
    decsep= str(datalist[1]).split('.')
    incsep= str(datalist[2]).split('.')
    if int(note) > 0:
        newline = " {0}.500 {1:>3} {2:4.1f} {3:>3} {4:4.1f} {5:>6} {6:>6} {7:>6} {8:>6} {9:>6} {10:>1} {11:>4} {12:>3}\r\n".format(meanyear,decsep[0],float('0.'+str(decsep[1]))*60.,incsep[0],float('0.'+str(incsep[1]))*60.,int(np.round(datalist[3],0)),int(np.round(datalist[4],0)),int(np.round(datalist[5],0)),int(np.round(datalist[6],0)),int(np.round(datalist[7],0)), kind, comp.upper(), int(note))
    else:
        newline = " {0}.500 {1:>3} {2:4.1f} {3:>3} {4:4.1f} {5:>6} {6:>6} {7:>6} {8:>6} {9:>6} {10:>1} {11:>4}{12:>3}\r\n".format(meanyear,decsep[0],float('0.'+str(decsep[1]))*60.,incsep[0],float('0.'+str(incsep[1]))*60.,int(np.round(datalist[3],0)),int(np.round(datalist[4],0)),int(np.round(datalist[5],0)),int(np.round(datalist[6],0)),int(np.round(datalist[7],0)), kind, comp.upper(), ' ')

    # create dummy header (check for existing values) and add data
    # inform observer to modify/check head
    def createhead(filename, locationname,coordlist,newline):
        """
        internal method to create header info for yearmean file
        """
        if not len(coordlist) == 3:
            logger.warning("writeIYFV: Coordinates missing")
            if len(coordlist) == 2:
                coordlist.append(np.nan)
            else:
                return False

        empty = "\r\n"
        content = []
        content.append("{:^70}\r\n".format("ANNUAL MEAN VALUES"))
        content.append(empty)
        content.append("{:^70}\r\n".format(locationname))
        content.append(empty)
        content.append("  COLATITUDE: {a:.3f}   LONGITUDE: {b:.3f} E   ELEVATION: {c:.0f} m\r\n".format(a=90.0-coordlist[0],b=coordlist[1],c=coordlist[2]))
        content.append(empty)
        content.append("  YEAR        D        I       H      X      Y      Z      F   * ELE Note\r\n")
        content.append("           deg min  deg min    nT     nT     nT     nT     nT\r\n")
        content.append(empty)
        content.append(newline)
        content.append(empty)
        content.append("* A = All days\r\n")
        content.append("* Q = Quiet days\r\n")
        content.append("* D = Disturbed days\r\n")
        content.append("* I = Incomplete\r\n")
        content.append("* J = Jump:         jump value = old site value - new site value\r\n")

        f = open(filename, "w")
        contents = "".join(content)
        f.write(contents)
        f.close()
        """
                      ANNUAL MEAN VALUES

                      ALMA-ATA, AAA, KAZAKHSTAN

  COLATITUDE: 46.75   LONGITUDE: 76.92 E   ELEVATION: 1300 m

  YEAR        D        I       H      X      Y      Z      F   * ELE Note
           deg min  deg min    nT     nT     nT     nT     nT

* A = All days
* Q = Quiet days
* D = Disturbed days
* I = Incomplete
* J = Jump:         jump value = old site value - new site value
        """

    def addline(filename, newline, kind, year):
        """
        internal method to insert new yearly means in a file
        """
        content = []
        fh = open(filename, 'rt', newline='')
        for line in fh:
            content.append(line)
        fh.close()

        yearlst = []
        foundcomm = False
        idx = 0
        commidx = 0

        for idx,elem in enumerate(content):
            ellst = elem.split()
            if len(ellst)>11:
                if ellst[10] == kind:
                    # get years
                    yearlst.append([idx, int(ellst[0].split('.')[0])])
            if elem.startswith('*') and not foundcomm: # begin of comment section
                foundcomm = True
                commidx = idx

        if not foundcomm: # No comment section - append at the end of file
            commidx = idx

        if not len(yearlst) > 0: # e.g. kind not yet existing
            # add line just above footer
            content.insert(commidx, '')
            content.insert(commidx, newline)
        else:
            years = [el[1] for el in yearlst]
            indicies = [el[0] for el in yearlst]
            if year in years:
                idx= indicies[years.index(year)]
                content[idx] = newline
            elif int(year) > np.max(years):
                idx= indicies[years.index(max(years))]
                content.insert(idx+1, newline)
            elif int(year) < np.min(years):
                idx= indicies[years.index(min(years))]
                content.insert(idx, newline)
            elif int(year) > np.min(years) and int(year) < np.max(years):
                i = 0
                for i,y in enumerate(years):
                    if int(y) > int(year):
                        break
                idx = indicies[i]
                content.insert(idx, newline)

        f = open(filename, "w")
        contents = "".join(content)
        f.write(contents)
        f.close()

    if os.path.isfile(filename):
        addline(filename, newline, kind, meanyear)
    else:
        name = header.get('StationName',' ')
        co = header.get('StationIAGAcode',' ')
        coun = header.get('StationCountry',' ')
        locationname = "{a:>34}, {b}, {c:<23}".format(a=name[:35],b=co,c=coun[:25])
        lat = header.get('DataAcquisitionLatitude',np.nan)
        lon = header.get('DataAcquisitionLongitude',np.nan)
        elev = header.get('DataElevation',np.nan)
        coordlist = [float(lat), float(lon), float(elev)]
        createhead(filename, locationname, coordlist, newline)

    return True

def readDKA(filename, headonly=False, **kwargs):
    """
                               AAA
                  Geographical latitude:    43.250 N
                  Geographical longitude:   76.920 E

            K-index values for 2010     (K9-limit =  300 nT)

  DA-MON-YR  DAY #    1    2    3    4      5    6    7    8       SK

  01-JAN-10   001     0    1    0    0      0    1    1    2        5
  02-JAN-10   002     0    1    2    1      1    1    0    0        6
  03-JAN-10   003     0    0    1    2      2    2    0    1        8
  04-JAN-10   004     1    1    1    1      1    0    1    1        7
    """

    getfile = True

    stream = DataStream()
    headers = {}
    data = []
    key = None

    array = [[] for key in KEYLIST]

    fh = open(filename, 'rt')
    ok = True
    cnt = 0
    datacoming = 0
    kcol = KEYLIST.index('var1')

    if ok:
        for line in fh:
            cnt = cnt+1
            block = line.split()
            if line.isspace():
                # blank line
                pass
            elif cnt == 1:
                # station info
                headers['StationID'] = block[0]
                headers['StationIAGAcode'] = block[0]
            elif line.find('latitude') > 0 or line.find('LATITUDE') > 0:
                headers['DataAcquisitionLatitude'] = float(block[-2])
            elif line.find('longitude') > 0 or line.find('LONGITUDE') > 0:
                headers['DataAcquisitionLongitude'] = float(block[-2])
            elif line.find('K9-limit') > 0:
                headers['StationK9'] = float(block[-2])
            elif line.find('DA-MON-YR') > 0:
                datacoming = cnt
            elif cnt > datacoming:
                if len(block) > 9:
                    for i in range(8):
                        # TODO Locale language settings is important to correctly interpret date
                        # e.g. "01-Mar-14" fails for de_DE
                        # solved by tring to switch to english-US
                        # - might not work if language not installed and on windows
                        # switched to dparser for MagPy 1.0.5 as independent of locale
                        ti = dparser.parse(block[0]) + timedelta(minutes=90) + timedelta(minutes=180*i)
                        #ti = datetime.strptime(block[0],"%d-%b-%y") + timedelta(minutes=90) + timedelta(minutes=180*i)
                        val = float(block[2+i])
                        array[0].append(ti)
                        if val < 990:
                            array[kcol].append(val)
                        else:
                            array[kcol].append(np.nan)
        #locale.setlocale(locale.LC_TIME, old_loc)

    fh.close()
    headers['col-var1'] = 'K'
    headers['unit-col-var1'] = ''
    headers['DataFormat'] = 'MagPyK'

    array = [np.asarray(ar) for ar in array]
    stream = DataStream(header=headers, ndarray=np.asarray(array,dtype=object))

    # Eventually add trim
    return stream


def writeDKA(datastream, filename, mode='overwrite',**kwargs):
    """
    DESCRIPTION
       DKA files are created by INTERMAGNET
       The here presented version is not yet fully tested against the IM version
    :param datastream:
    :param filename:
    :param mode:
    :param kwargs:
    :return:
    """

    # filling gaps with mens for daily sums or do not calculate sums - no description found
    fillmeans = True
    # extract k string from datastream
    # check kvals
    kstr = []
    # Get days out of ndarray:
    times = sorted(list(set([elem.date() for elem in datastream.ndarray[0]])))
    # Get all k values for each timestep
    for d in times:
        klist = [kval for idx,kval in enumerate(datastream.ndarray[KEYLIST.index('var1')]) if datastream.ndarray[0][idx].date() == d]
        if len(klist) == 8:
            validk = [el for el in klist if not np.isnan(el) and el >= 0]
            if len(validk) == 8:
                sumk = "{:.1f}".format(np.sum(validk))
            else:
                if fillmeans:
                    # fill list with means for sum
                    mk = np.mean(validk)
                    validk = validk + [mk] * (8 - len(validk))
                    sumk = "{:.1f}".format(np.sum(validk))
                else:
                    sumk = ''

        else:
            sumk=''
        kstring = "  {0:12}{1:7}{2:42}{3:>4}".format(d.strftime("%d-%b-%y"), d.strftime("%j"), "   ".join(["{:>2}".format(str(int(k))) for k in klist]), sumk)
        if len(kstring) > 39:
            kstring = kstring[:39] + '  ' + kstring[39:]

        kstr.append(kstring)

    if len(kstr) > 0:
        station=datastream.header.get('StationIAGAcode')
        k9=datastream.header.get('StationK9')
        latnum = datastream.header.get('DataAcquisitionLatitude',0)
        if not latnum:
            latnum = datastream.header.get('StationLatitude',0)
        lat=np.round(float(latnum),3)
        lonnum = datastream.header.get('DataAcquisitionLongitude',0)
        if not lonnum:
            lonnum = datastream.header.get('StationLongitude',0)
        lon=np.round(float(lonnum),3)
        year=str(int(datetime.strftime(datastream.ndarray[0][1],'%y')))
        ye=str(int(datetime.strftime(datastream.ndarray[0][1],'%Y')))

        head = []
        if not os.path.isfile(filename):
            head.append("{0:^66}".format(station.upper()))
            head2 = '                  Geographical latitude: {:>10.3f} N'.format(lat)
            head3 = '                  Geographical longitude:{:>10.3f} E'.format(lon)
            head4 = '            K-index values for {0}     (K9-limit = {1:>4} nT)'.format(ye, k9)
            head5 = '  DA-MON-YR  DAY #    1    2    3    4      5    6    7    8       SK'
            emptyline = ''
            head.append("{0:<50}".format(head2))
            head.append("{0:<50}".format(head3))
            head.append("{0:<50}".format(emptyline))
            head.append("{0:<50}".format(head4))
            head.append("{0:<50}".format(emptyline))
            head.append("{0:<50}".format(head5))
            head.append("{0:<50}".format(emptyline))

            with open(filename, "w", newline='') as myfile:
                for elem in head:
                    myfile.write(elem + '\r\n')
        with open(filename, "a", newline='') as myfile:
            for elem in kstr:
                myfile.write(elem + '\r\n')

        return True

if __name__ == '__main__':

    import scipy
    import subprocess
    print()
    print("----------------------------------------------------------")
    print("TESTING: IMF FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE IMF LIBRARY.")
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
    l = 32*1440
    array = [[] for el in DataStream().KEYLIST]
    win = scipy.signal.windows.hann(60)
    a = np.random.uniform(20950, 21000, size=int(l/2))
    b = np.random.uniform(20950, 21050, size=int(l/2))
    x = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
    x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
    array[1] = x[1440:-1440]
    a = np.random.uniform(1950, 2000, size=int(l/2))
    b = np.random.uniform(1900, 2050, size=int(l/2))
    y = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
    y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
    array[2] = y[1440:-1440]
    a = np.random.uniform(44300, 44400, size=l)
    z = scipy.signal.convolve(a, win, mode='same') / sum(win)
    array[3] = z[1440:-1440]
    a = np.random.uniform(48900, 49100, size=l)
    f = scipy.signal.convolve(a, win, mode='same') / sum(win)
    array[4] = f[1440:-1440]
    array[0] = np.asarray([datetime(2022, 11, 1) + timedelta(minutes=i) for i in range(0, len(array[1]))])
    # 2. Creating artificial header information
    header = {}
    header['DataSamplingRate'] = 60
    header['SensorID'] = 'Test_0001_0002'
    header['StationK9'] = 450
    header['StationIAGAcode'] = 'XXX'
    header['DataAcquisitionLatitude'] = 48.123
    header['DataAcquisitionLongitude'] = 15.999
    header['DataElevation'] = 1090
    header['DataComponents'] = 'XYZS'
    header['StationInstitution'] = 'TheWatsonObservatory'
    header['DataQuality'] = ''
    header['SensorType'] = 'SherlocksSystem'
    header['DataDigitalSampling'] = '1 Hz'
    header['DataSensorOrientation'] = 'HEZ'
    header['StationName'] = 'Holmes'
    header['StationStreet'] = '221B Baker Street'
    header['StationCity'] = 'London'
    header['StationPostalCode'] = 'W1U 6SG'
    header['StationCountry'] = 'England'
    header['StationWebInfo'] = 'Not yet existing'
    header['StationEmail'] = 'arthur@conan.doyle'

    #header['DataPublicationDate'] = ''
    #requiredinfo = ['FormatVersion','Reserved']

    teststream = DataStream(header=header, ndarray=np.asarray(array, dtype=object))
    hourstream = teststream.filter()


    errors = {}
    successes = {}
    testrun = 'STREAMTESTFILE'
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)
    # Testing the following methods
    #writeIYFV()

    while True:
        testset = 'IAF'
        try:
            # Testing IAF
            filename = os.path.join('/tmp','{}_{}_{}'.format(testrun, testset, datetime.strftime(t_start_test,'%Y%m%d-%H%M')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            # IAF write
            succ1 = writeIAF(teststream, filename)
            # IAF test
            succ2 = isIAF(filename)
            # IAF read
            dat = readIAF(filename)
            t_a = len(dat)
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            dat = dat.calc_f()
            diff = subtract_streams(teststream,dat)
            xm = diff.mean('x')
            ym = diff.mean('y')
            zm = diff.mean('z')
            fm = diff.mean('f')
            if np.abs(xm) > 0.001 or np.abs(ym) > 0.001 or np.abs(zm) > 0.001 or np.abs(fm) > 0.001:
                 raise Exception("ERROR within IAF data validity test")
            dat = readIAF(filename, resolution='hour')
            t_b = len(dat)
            dat = readIAF(filename, resolution='k')
            t_c = len(dat)
            if not t_a == 43200 or not t_b == 720 or not t_c == 240:
                 raise Exception("ERROR within IAF resolution test")
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))
        testset = 'IMF'
        try:
            filename = os.path.join('/tmp','{}_{}_{}'.format(testrun, testset, datetime.strftime(t_start_test,'%Y%m%d-%H%M')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            # write
            succ1 = writeIMF(teststream,filename)
            # test
            succ2 = isIMF(filename)
            dat = readIMF(filename)
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            diff = subtract_streams(teststream,dat)
            xm = diff.mean('x')
            ym = diff.mean('y')
            zm = diff.mean('z')
            fm = diff.mean('f')
            if np.abs(xm) > 0.001 or np.abs(ym) > 0.001 or np.abs(zm) > 0.001 or np.abs(fm) > 0.001:
                 raise Exception("ERROR within {} validity test".format(testset))
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))
        testset = 'DKA'
        try:
            filename = os.path.join('/tmp','{}_{}_{}'.format(testrun, testset, datetime.strftime(t_start_test,'%Y%m%d-%H%M')))
            kstream = K_fmi(teststream)
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            # write
            print ("Writing")
            succ1 = writeDKA(kstream,filename)
            # test
            print ("Testing")
            succ2 = isDKA(filename)
            print ("Reading")
            dat = readDKA(filename)
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            diff = subtract_streams(kstream,dat)
            km = diff.mean('var1')
            print (km)
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))
        testset = 'BLV'
        try:
            print ("Running BLV test")
            from magpy.stream import example3
            filename = os.path.join('/tmp',
                                    '{}_{}_{}'.format(testrun, testset, datetime.strftime(t_start_test, '%Y%m%d-%H%M')))
            # read basevalue data
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            base = read(example3)
            # fit adopted baseline
            func1 = base.fit(['dx', 'dy', 'dz'], fitfunc='spline', knotstep=0.33, endtime='2023-07-15')
            func2 = base.fit(['dx', 'dy', 'dz'], fitfunc='spline', knotstep=0.05, starttime='2023-07-15')
            func = [func1, func2]
            # no extrapolation to a endtime
            base.header['DataFunctionObject'] = [func1, func2]
            # mp.tsplot([base], keys=[['dx','dy','dz']], symbols=[['o','o','o']], padding=[[2,0.02,2]], functions=[[func,func,func]])
            print("Writing to ", filename)
            succ1 = writeBLV(base, filename, coverage='all')
            # raw data will have exact time steps of measurements - BLV data will have only daily accuracy
            succ2 = isBLV(filename)
            print("Reading")
            dat = readBLV(filename)
            func2 = dat.header.get('DataFunctionObject')
            # Check func2 display - seems not to work in all cases in BLV tests - part of the fit verification
            # validity tests - run comparison of basevalue
            base = base.trim(starttime='2018-01-01', endtime='2019-01-01')
            print(np.round(base.mean('dx'), 2), np.round(dat.mean('dx'), 2))
            print(np.round(base.mean('dy') * 60., 2), np.round(dat.mean('dy') * 60., 2))
            print(np.round(base.mean('dz'), 2), np.round(dat.mean('dz'), 2))
            ado = readBLV(filename, mode="adopted")
            # mp.tsplot([base,dat,ado], keys=[['dx','dy','dz']], symbols=[['o','o','o'],['o','o','o'],['-','-','-']], padding=[[2,0.02,2]], functions=[[func,func,func],[],[]])
            # ado and func perfectly match in graph find another test
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))
        testset = 'IYFV'
        try:
            # read one year of magnetic data data
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            data = read('ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc')
            print("Writing to Yearmean")
            data.write("/tmp/", format_type='IYFV')
            print("Reading")
            dat = readIYFV("/tmp/YEARMEAN.FUR")
            # validity tests - check value
            f = dat.ndarray[4][0]
            print("F from constructed values should be 48145 nT: {} nT".format(f))
            te = datetime.now(timezone.utc).replace(tzinfo=None)
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
