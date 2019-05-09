"""
MagPy
Intermagnet input filter
Written by Roman Leonhardt December 2012
- contains test, read and write functions for
        IMF 1.22,1.23
        IAF
        ImagCDF
        IYFV    (yearly means)
        DKA     (k values)d
        BLV     (baseline data)
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

from magpy.stream import *

import sys
import logging
logger = logging.getLogger(__name__)

def isIMF(filename):
    """
    Checks whether a file is ASCII IMF 1.22,1.23 minute format.
    """
    try:
        temp = open(filename, 'rt').readline()
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


def isIMAGCDF(filename):
    """
    Checks whether a file is ImagCDF format.
    """
    try:
        temp = cdf.CDF(filename)
    except:
        return False
    try:
        form = temp.attrs['FormatDescription']
        if not form[0].startswith('INTERMAGNET'):
            return False
    except:
        return False
    
    logger.debug("isIMAGCDF: Found INTERMAGNET CDF data")
    return True


def isIAF(filename):
    """
    Checks whether a file is BIN IAF INTERMAGNET Archive format.
    """

    try:
        temp = open(filename, 'rb').read(64)
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


def isBLV(filename):
    """
    Checks whether a file is ASCII IMF 1.22,1.23 minute format.
    """
    try:
        fi = open(filename, 'rt')
        temp1 = fi.readline()
        temp2 = fi.readline()
    except:
        return False
    if temp1.startswith('XYZ') or temp1.startswith('DIF') or temp1.startswith('HDZ') or temp1.startswith('UVZ') or temp1.startswith('DHZ'):
        pass
    else:
        return False
    if not 24 <= len(temp1) <= 30:
        return False
    if not 43 <= len(temp2) <= 45:
        return False
    #try:
    #    if 
    #    print ("Reading BLV1", len(temp1), temp1, len(temp2), temp2)
    #    if not 63 <= len(temp1) <= 65:  # Range which regards any variety of return
    #        return False
    #    if temp1[3] == ' ' and temp1[11] == ' ' and temp1[29] == ' ' and temp1[45] == ' ' and temp1[46] == 'R':
    #        pass
    #    else:
    #        return False
    #except:
    #    return False
    
    logger.debug("isBLV: Found BLV data")
    return True


def isIYFV(filename):
    """
    Checks whether a file is ASCII IYFV 1.01 yearly mean format.

    _YYYY.yyy_DDD_dd.d_III_ii.i_HHHHHH_XXXXXX_YYYYYY_ZZZZZZ_FFFFFF_A_EEEE_NNNCrLf
    """
    # Search for identifier in the first three line
    if sys.version_info >= (3, 0):
        code = 'rt' 
        try:
            fi = open(filename, code, errors='replace')
        except:
            return False
    else:
        code = 'rb'
        try:
            fi = open(filename, code)
        except:
            return False

    for ln in range(0,2):
        try:
            temp = fi.readline()
        except UnicodeDecodeError as e:
            print ("Found an unicode error whene reading:",e)
            return False
        except:
            return False
        try:
            searchstr = ['ANNUAL MEAN VALUES', 'Annual Mean Values', 'annual mean values']
            for elem in searchstr:
                if temp.find(elem) > 0:
                    logger.debug("isIYFV: Found IYFV data")
                    return True
        except:
            return False
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
        fh = open(filename, 'rt')
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
    debug = kwargs.get('debug')

    getfile = True
    gethead = True

    if debug:
        logger.info("Found IAF file ...")

    if not resolution:
        resolution = u'minutes'
    stream = DataStream()
    # Check whether header infromation is already present

    headers = {}

    data = []
    key = None

    if starttime:
        begin = stream._testtime(starttime)
    if endtime:
        end = stream._testtime(endtime)


    x,y,z,f,xho,yho,zho,fho,xd,yd,zd,fd,k,ir = [],[],[],[],[],[],[],[],[],[],[],[],[],[]
    datelist = []

    fh = open(filename, 'rb')
    while True:
        #try:
        getline = True
        start = fh.read(64)
        #print (len(start))
        if not len(start) == 64:
            break
        else:
            head = struct.unpack('<4s4l4s4sl4s4sll4s4sll', start)
            head = [el.decode('utf-8') if not isinstance(el,(int,basestring)) else el for el in head]
            date = datetime.strptime(str(head[1]),"%Y%j")
            datelist.append(date)
            #if starttime:  ## This does not work
            #    if date < begin:
            #        getline = False
            #if endtime:
            #    if date > end:
            #        getline = False
            if getline:
                # unpack header
                if gethead:
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
                    keystr = ','.join([str(c) for c in head[5].lower()]).replace(' ','')
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
                        pubdate = datetime.strptime(datetime.strftime(datetime.utcnow(),"%y%m"),"%y%m")
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

    #x = np.asarray([val for val in x if not val > 888880])/10.   # use a pythonic way here
    x = np.asarray(x)/10.
    x[x > 88880] = float(nan)
    y = np.asarray(y)/10.
    y[y > 88880] = float(nan)
    z = np.asarray(z)/10.
    z[z > 88880] = float(nan)
    f = np.asarray(f)/10.
    f[f > 88880] = float(nan)
    with np.errstate(invalid='ignore'):
        f[f < -44440] = float(nan)
    xho = np.asarray(xho)/10.
    xho[xho > 88880] = float(nan)
    yho = np.asarray(yho)/10.
    yho[yho > 88880] = float(nan)
    zho = np.asarray(zho)/10.
    zho[zho > 88880] = float(nan)
    fho = np.asarray(fho)/10.
    fho[fho > 88880] = float(nan)
    with np.errstate(invalid='ignore'):
        fho[fho < -44440] = float(nan)
    xd = np.asarray(xd)/10.
    xd[xd > 88880] = float(nan)
    yd = np.asarray(yd)/10.
    yd[yd > 88880] = float(nan)
    zd = np.asarray(zd)/10.
    zd[zd > 88880] = float(nan)
    fd = np.asarray(fd)/10.
    fd[fd > 88880] = float(nan)
    with np.errstate(invalid='ignore'):
        fd[fd < -44440] = float(nan)
    k = np.asarray(k).astype(float)/10.
    k[k > 88] = float(nan)
    ir = np.asarray(ir)

    # ndarray
    def data2array(arlist,keylist,starttime,sr):
        array = [[] for key in KEYLIST]
        ta = []
        val = starttime
        for ind, elem in enumerate(arlist[0]):
            ta.append(date2num(val))
            val = val+timedelta(seconds=sr)
        array[0] = np.asarray(ta)
        for idx,ar in enumerate(arlist):
            pos = KEYLIST.index(keylist[idx])
            array[pos] = np.asarray(ar)

        return np.asarray(array)

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
    else:
        logger.debug("Key and minimum: {} {}".format(keystr, min(datelist)))
        ndarray = data2array([x,y,z,f],keystr.split(','),min(datelist),sr=60)
        headers['DataSamplingRate'] = '60 sec'

    stream = DataStream([LineStruct()], headers, ndarray)
    #if 'df' in keystr:
    #    stream = stream.f_from_df()
    stream.header['DataFormat'] = 'IAF'

    return stream


def writeIAF(datastream, filename, **kwargs):
    """
    Writing Intermagnet archive format (2.1)
    """

    kvals = kwargs.get('kvals')
    mode = kwargs.get('mode')
    debug = kwargs.get('debug')

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
    tdiff = int(np.round(datastream.ndarray[0][-1]-datastream.ndarray[0][0]))
    if not tdiff >= 28:
        logger.error("writeIAF: Data needs to cover one month")
        return False

    try:
        # Convert data to XYZ if HDZ
        if not datastream.header.get('DataComponents','').startswith('XYZ'):
            logger.indo("Data contains: {}".format(datastream.header.get('DataComponents','')))
        if datastream.header['DataComponents'].startswith('HDZ'):
            datastream = datastream.hdz2xyz()
    except:
        logger.error("writeIAF: HeaderInfo on DataComponents seems to be missing")
        return False

    dsf = datastream.header.get('DataSamplingFilter','')

    # Check whether f is contained (or delta f)
    # if f calc delta f
    #   a) replaceing s by f     
    if datastream.header.get('DataComponents','') in ['HDZS','hdzs']:
        datastream.header['DataComponents'] = 'HDZF'
    if datastream.header.get('DataComponents','') in ['XYZS','xyzs']:
        datastream.header['DataComponents'] = 'XYZF'
    if datastream.header.get('DataSensorOrientation','') == '':
        datastream.header.get['DataSensorOrientation'] = datastream.header.get('DataComponents','')
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
                    longi,lati = convertGeoCoordinate(float(longi),float(lati),'epsg:'+str(epsg),'epsg:4326')
    datastream.header['DataAcquisitionLongitude'] = longi
    datastream.header['DataAcquisitionLatitude'] = lati
    datastream.header['DataLocationReference'] = 'WGS84, EPSG:4326'

    # Check whether all essential header info is present
    requiredinfo = ['StationIAGAcode','StartDate','DataAcquisitionLatitude', 'DataAcquisitionLongitude', 'DataElevation', 'DataComponents', 'StationInstitution', 'DataConversion', 'DataQuality', 'SensorType', 'StationK9', 'DataDigitalSampling', 'DataSensorOrientation', 'DataPublicationDate','FormatVersion','Reserved']

    # cycle through data - day by day
    t0 = int(datastream.ndarray[0][1])
    output = b''
    kstr=[]

    tmpstream = datastream.copy()
    hourvals = tmpstream.filter(filter_width=timedelta(minutes=60), resampleoffset=timedelta(minutes=30), filter_type='flat',missingdata='mean')
    hourvals = hourvals.get_gaps()
    printhead = True

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
        temp = DataStream([LineStruct],datastream.header,tempvals)

        head = []
        reqinfotmp = requiredinfo
        misslist = []
        for elem in requiredinfo:
            try:
                if elem == 'StationIAGAcode':
                    value = " "+datastream.header.get('StationIAGAcode','')
                    if value == '':
                        misslist.append(elem)
                elif elem == 'StartDate':
                    value = int(datetime.strftime(num2date(dayar[0][0]),'%Y%j'))
                elif elem == 'DataAcquisitionLatitude':
                    if not float(datastream.header.get('DataAcquisitionLatitude',0)) < 90 and float(datastream.header.get('DataAcquisitionLatitude','')) > -90:
                        print("Latitude and Longitude need to be provided in degrees")
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
                        value = datetime.strftime(datastream._testtime(da),'%y%m')
                    except:
                        #print("writeIAF: DataPublicationDate --  appending current date")
                        value = datetime.strftime(datetime.utcnow(),'%y%m')
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
                
                if not datastream._is_number(value):
                    if len(value) < 4:
                        value = value.ljust(4)
                    elif len(value) > 4:
                        value = value[:4]
                head.append(value)
                reqinfotmp = [el for el in reqinfotmp if not el==elem]
            except:
                print("Check header content: could not interprete header information")
                print("  --  critical information error in data header: {}  --".format(elem))
                print("  ---------------------------------------------------")
                print(" Please provide: StationIAGAcode, DataAcquisitionLatitude, ")
                print(" DataAcquisitionLongitude, DataElevation, DataConversion, ")
                print(" DataComponents, StationInstitution, DataQuality, SensorType, ")
                print(" StationK9, DataDigitalSampling, DataSensorOrientation")
                print(" e.g. data.header['StationK9'] = 750")
                return False

        """
            if len(misslist) == 0:
                    if not datastream._is_number(value):
                        if len(value) < 4:
                            value = value.ljust(4)
                        elif len(value) > 4:
                            value = value[:4]
                    head.append(value)
                    reqinfotmp = [el for el in reqinfotmp if not el==elem]
            else:
                    print("Check header: below mentioned content appears to be missing in header")
                    print("  --  critical information missing in data header  --")
                    print("  ---------------------------------------------------")
                    print(" Please provide: {} ".format(misslist))
                    print(" e.g. data.header['StationK9'] = 750")
                    return False
        """
        if len(misslist) > 0 and printhead:
            print ("The following meta information is missing. Please provide!")
            for he in misslist:
                print (he)
            printhead = False

        # Constructing header Info
        packcode = '<4s4l4s4sl4s4sll4s4sll' # fh.read(64)
        head = [el.encode('ascii','ignore') if isinstance(el, unicode) else el for el in head]
        if debug:
            print ("Header looks like:", head)
        head_bin = struct.pack(packcode,*head)
        # add minute values
        packcode += '1440l' # fh.read(64)
        xvals = np.asarray([np.round(elem,1) if not isnan(elem) else 99999.9 for elem in dayar[1]])
        xvals = np.asarray(xvals*10).astype(int)
        head.extend(xvals)
        if not len(xvals) == 1440:
            logger.error("writeIAF: Found inconsistency in minute data set")
            logger.error("writeIAF: for {}".format(datetime.strftime(num2date(dayar[0][0]),'%Y%j')))
            logger.error("writeIAF: expected 1440 records, found {} records".format(len(xvals)))
        packcode += '1440l' # fh.read(64)
        yvals = np.asarray([np.round(elem,1) if not isnan(elem) else 99999.9 for elem in dayar[2]])
        yvals = np.asarray(yvals*10).astype(int)
        head.extend(yvals)
        packcode += '1440l' # fh.read(64)
        zvals = np.asarray([np.round(elem,1) if not isnan(elem) else 99999.9 for elem in dayar[3]])
        zvals = np.asarray(zvals*10).astype(int)
        head.extend(zvals)
        packcode += '1440l' # fh.read(64)
        if df:
            #print ([elem for elem in dayar[dfpos]])
            dfvals = np.asarray([np.round(elem*10.,0) if not isnan(elem) else 999999 for elem in dayar[dfpos]])
            #print ("dfmin",dfvals)
            #dfvals = np.asarray(dfvals*10.).astype(int)
            dfvals = dfvals.astype(int)
        else:
            dfvals = np.asarray([888888]*len(dayar[0])).astype(int)
        head.extend(dfvals)

        # add hourly means
        packcode += '24l'
        xhou = np.asarray([np.round(elem,1) if not isnan(elem) else 99999.9 for elem in temp.ndarray[1]])
        xhou = np.asarray(xhou*10).astype(int)
        head.extend(xhou)
        if not len(xhou) == 24:
            logger.error("writeIAF: Found inconsistency in hourly data set: expected 24, found {} records".format(len(xhou)))
            logger.error("writeIAF: Error in day {}".format(datetime.strftime(num2date(dayar[0][0]),'%Y%j')))
        packcode += '24l'
        yhou = np.asarray([np.round(elem,1) if not isnan(elem) else 99999.9 for elem in temp.ndarray[2]])
        yhou = np.asarray(yhou*10).astype(int)
        head.extend(yhou)
        packcode += '24l'
        zhou = np.asarray([np.round(elem,1) if not isnan(elem) else 99999.9 for elem in temp.ndarray[3]])
        zhou = np.asarray(zhou*10).astype(int)
        head.extend(zhou)
        packcode += '24l'
        if df:
            dfhou = np.asarray([np.round(elem,1) if not isnan(elem) else 99999.9 for elem in temp.ndarray[dfpos]])
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

        #print("3:", len(head))

        # add k values
        if kvals:
            dayk = kvals._select_timerange(starttime=t0+i,endtime=t0+i+1)
            kdat = dayk[KEYLIST.index('var1')]
            kdat = [el*10. if not np.isnan(el) else 999 for el in kdat]
            #print("kvals", len(kdat), t0+i,t0+i+1,kdat,dayk[0],dayk[-1])
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

        #print("HERE", len(ks), len(head), head[-18:])
        #print [num2date(elem) for elem in temp.ndarray[0]]
        line = struct.pack(packcode,*head)
        output = output + line

    path = os.path.split(filename)
    filename = os.path.join(path[0],path[1].upper())

    if len(kstr) > 0:
        station=datastream.header['StationIAGAcode']
        k9=datastream.header['StationK9']
        lat=np.round(float(datastream.header.get('DataAcquisitionLatitude')),3)
        lon=np.round(float(datastream.header.get('DataAcquisitionLongitude')),3)
        year=str(int(datetime.strftime(num2date(datastream.ndarray[0][1]),'%y')))
        ye=str(int(datetime.strftime(num2date(datastream.ndarray[0][1]),'%Y')))
        kfile = os.path.join(path[0],station.upper()+year+'K.DKA')
        logger.info("Writing k summary file: {}".format(kfile))
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
            with open(kfile, "wb") as myfile:
                for elem in head:
                    myfile.write(elem+'\r\n')
                #print elem
        # write data
        with open(kfile, "a") as myfile:
            for elem in kstr:
                myfile.write(elem+'\r\n')
                #print elem

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

    logger.info("Creating README from header info in {}".format(path[1].upper()))
    readme = True
    if readme:
        requiredhead = ['StationName','StationInstitution', 'StationStreet','StationCity','StationPostalCode','StationCountry','StationWebInfo', 'StationEmail','StationK9']
        acklist = ['StationName','StationInstitution', 'StationStreet','StationCity','StationPostalCode','StationCountry','StationWebInfo' ]
        conlist = ['StationName','StationInstitution', 'StationStreet','StationCity','StationPostalCode','StationCountry', 'StationEmail']

        for h in requiredhead:
            try:
                test = datastream.header[h]
            except:
                logger.error("README file could not be generated. Info on {0} is missing".format(h))
                return True
        ack = []
        contact = []
        for a in acklist:
            try:
                ack.append("               {0}".format(datastream.header[a]))
            except:
                pass
        for c in conlist:
            try:
                contact.append("               {0}".format(datastream.header[c]))
            except:
                pass

        # 1. Check completeness of essential header information
        station=datastream.header['StationIAGAcode']
        stationname = datastream.header['StationName']
        k9=datastream.header['StationK9']
        lat=np.round(float(datastream.header.get('DataAcquisitionLatitude')),3)
        lon=np.round(float(datastream.header.get('DataAcquisitionLongitude')),3)
        ye=str(int(datetime.strftime(num2date(datastream.ndarray[0][1]),'%Y')))
        rfile = os.path.join(path[0],"README."+station.upper())
        head = []
        logger.info("Writing README file: {}".format(rfile))

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
            head.append("LOCATION     : {0}, {1}".format(datastream.header['StationCity'],datastream.header['StationCountry']))
            head.append("ORGANIZATION : {0:<50}".format(datastream.header['StationInstitution']))
            head.append("CO-LATITUDE  : {:.3f} Deg.".format(90.-float(lat)))
            head.append("LONGITUDE    : {:.3f} Deg. E".format(float(lon)))
            head.append("ELEVATION    : {0} meters".format(int(datastream.header['DataElevation'])))
            head.append("{0:<50}".format(emptyline))
            head.append("ABSOLUTE")
            head.append("INSTRUMENTS  : please insert manually")
            head.append("RECORDING")
            head.append("VARIOMETER   : {}".format(datastream.header.get('SensorName',dummy)))
            head.append("ORIENTATION  : {}".format(datastream.header['DataSensorOrientation']))
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
            with open(rfile, "wb") as myfile:
                for elem in head:
                    myfile.write(elem+'\r\n'.encode('utf-8'))
            myfile.close()

    return True


def readIMAGCDF(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet CDF format (1.0,1.1,1.2)
    """

    debug = kwargs.get('debug')

    if debug:
        logger.info("readIMAGCDF: FOUND IMAGCDF file")

    cdfdat = cdf.CDF(filename)
    if debug:
        for line in cdfdat:
            logger.info("{}".format(line))

    # get Attribute list
    attrslist = [att for att in cdfdat.attrs]
    # get Data list
    datalist = [att for att in cdfdat]
    headers={}

    arraylist = []
    array = [[] for elem in KEYLIST]
    startdate = cdfdat[datalist[-1]][0]
    flagruleversion  = ''
    flagruletype = ''
    flaglist = []

    #  #################################
    # Get header info:
    #  #################################
    if 'FormatDescription' in attrslist:
        form = cdfdat.attrs['FormatDescription']
        headers['DataFormat'] = str(cdfdat.attrs['FormatDescription'])
    if 'FormatVersion' in attrslist:
        vers = cdfdat.attrs['FormatVersion']
        headers['DataFormat'] = str(form) + '; ' + str(vers)
    if 'Title' in attrslist:
        pass
    if 'IagaCode' in attrslist:
        headers['StationIAGAcode'] = str(cdfdat.attrs['IagaCode'])
        headers['StationID'] = str(cdfdat.attrs['IagaCode'])
    if 'ElementsRecorded' in attrslist:
        headers['DataComponents'] = str(cdfdat.attrs['ElementsRecorded'])
    if 'PublicationLevel' in attrslist:
        headers['DataPublicationLevel'] = str(cdfdat.attrs['PublicationLevel'])
    if 'PublicationDate' in attrslist:
        headers['DataPublicationDate'] = str(cdfdat.attrs['PublicationDate'])
    if 'ObservatoryName' in attrslist:
        headers['StationName'] = str(cdfdat.attrs['ObservatoryName'])
    if 'Latitude' in attrslist:
        headers['DataAcquisitionLatitude'] = str(cdfdat.attrs['Latitude'])
    if 'Longitude' in attrslist:
        headers['DataAcquisitionLongitude'] = str(cdfdat.attrs['Longitude'])
    if 'Elevation' in attrslist:
        headers['DataElevation'] = str(cdfdat.attrs['Elevation'])
    if 'Institution' in attrslist:
        headers['StationInstitution'] = str(cdfdat.attrs['Institution'])
    if 'VectorSensOrient' in attrslist:
        headers['DataSensorOrientation'] = str(cdfdat.attrs['VectorSensOrient'])
    if 'StandardLevel' in attrslist:
        headers['DataStandardLevel'] = str(cdfdat.attrs['StandardLevel'])
    if 'StandardName' in attrslist:
        headers['DataStandardName'] = str(cdfdat.attrs['StandardName'])
    if 'StandardVersion' in attrslist:
        headers['DataStandardVersion'] = str(cdfdat.attrs['StandardVersion'])
    if 'PartialStandDesc' in attrslist:
        headers['DataPartialStandDesc'] = str(cdfdat.attrs['PartialStandDesc'])
    if 'Source' in attrslist:
        headers['DataSource'] = str(cdfdat.attrs['Source'])
    if 'TermsOfUse' in attrslist:
        headers['DataTerms'] = str(cdfdat.attrs['TermsOfUse'])
    if 'References' in attrslist:
        headers['DataReferences'] = str(cdfdat.attrs['References'])
    if 'UniqueIdentifier' in attrslist:
        headers['DataID'] = str(cdfdat.attrs['UniqueIdentifier'])
    if 'ParentIdentifiers' in attrslist:
        headers['SensorID'] = str(cdfdat.attrs['ParentIdentifier'])
    if 'ReferenceLinks' in attrslist:
        headers['StationWebInfo'] = str(cdfdat.attrs['ReferenceLinks'])
    if 'FlagRulesetType' in attrslist:
        flagruletype = str(cdfdat.attrs['FlagRulesetType'])
    if 'FlagRulesetVersion' in attrslist:
        flagruleversion = str(cdfdat.attrs['FlagRulesetVersion'])

    # New in 0.3.99 - provide a SensorID as well consisting of IAGA code, min/sec 
    # and numerical publevel
    #  IAGA code
    if headers.get('SensorID','') == '':
        try:
            #TODO determine resolution
            headers['SensorID'] = "{}_{}_{}".format(headers.get('StationIAGAcode','xxx').upper()+'sec',headers.get('DataPublicationLevel','0'),'0001')
        except:
            pass


    #  #################################
    # Get data:
    #  #################################

    # Reorder datalist and Drop time column
    # #########################################################
    # 1. Get the amount of Times columns and associated lengths
    # #########################################################
    #print "Analyzing file structure and returning values"
    #print datalist
    zpos = KEYLIST.index('z') # used for idf records
    mutipletimerange = False
    newdatalist = []
    tllist = []
    indexarray = np.asarray([])
    for elem in datalist:
        if elem.endswith('Times') and not elem.startswith('Flag'):
            #print "Found Time Column"
            # Get length
            tl = int(str(cdfdat[elem]).split()[1].strip('[').strip(']'))
            #print "Length", tl
            tllist.append([tl,elem])
    if len(tllist) < 1:
        #print "No time column identified"
        # Check for starttime and sampling rate in header
        if 'StartTime' in attrslist and 'SamplingPeriod' in attrslist:
            # TODO Write that function
            st = str(cdfdat.attrs['StartTime'])
            sr = str(cdfdat.attrs['SamplingPeriod'])
        else:
            logger.error("readIMAGCDF: No Time information available - aborting")
            return
    elif len(tllist) > 1:
        tl = [el[0] for el in tllist]
        if not max(tl) == min(tl):
            logger.warning("readIMAGCDF: Time columns of different length. Choosing longest as basis")
            newdatalist.append(['time',max(tllist)[1]])
            try:
                indexarray = np.nonzero(np.in1d(date2num(cdfdat[max(tllist)[1]][...]),date2num(cdfdat[min(tllist)[1]][...])))[0]
            except:
                indexarray = np.asarray([])
            mutipletimerange = True
        else:
            logger.info("readIMAGCDF: Equal length time axes found - assuming identical time")
            if 'GeomagneticVectorTimes' in datalist:
                newdatalist.append(['time','GeomagneticVectorTimes'])
            else:
                newdatalist.append(['time',tllist[0][1]]) # Take the first one
    else:
        #print "Single time axis found in file"
        newdatalist.append(['time',tllist[0][1]])

    def Ruleset2Flaglist(flagginglist,rulesettype,rulesetversion):
        if flagruletype in ['Conrad', 'conrad', 'MagPy','magpy']:
            if flagruleversion in ['1.0','1',1]:
                flagcolsconrad = [flagginglist[0],flagginglist[1],flagginglist[3],flagginglist[4],flagginglist[5],flagginglist[6],flagginglist[2]]
                flaglisttmp = []
                for elem in flagcolsconrad:
                    flaglisttmp.append(cdfdat[elem][...])
                flaglist = np.transpose(flaglisttmp)
                flaglist = [list(elem) for elem in flaglist]
                return list(flaglist)
        else:
            logger.warning("readIMAGCDF: Could  not interprete Ruleset")

    if not flagruletype == '':
        logger.info("readIMAGCDF: Found flagging ruleset {} vers.{} - extracting flagging information".format(flagruletype,flagruleversion))
        flagginglist = [elem for elem in datalist if elem.startswith('Flag')]
        flaglist = Ruleset2Flaglist(flagginglist,flagruletype,flagruleversion)

    datalist = [elem for elem in datalist if not elem.endswith('Times') and not elem.startswith('Flag')]

    # #########################################################
    # 2. Sort the datalist according to KEYLIST
    # #########################################################
    for key in KEYLIST:
        possvals = [key]
        if key == 'x':
            possvals.extend(['h','i'])
        if key == 'y':
            possvals.extend(['d','e'])
        if key == 'df':
            possvals.append('g')
        if key == 'f':
            possvals.append('s')
        for elem in datalist:
            try:
                label = cdfdat[elem].attrs['LABLAXIS'].lower()
                if label in possvals:
                    newdatalist.append([key,elem])
            except:
                pass # for lines which have no Label

    if not len(datalist) == len(newdatalist)-1:
        logger.warning("readIMAGCDF: error encountered in key assignment - please check")

    # 3. Create equal length array reducing all data to primary Times and filling nans for non-exist
    # (4. eventually completely drop time cols and just store start date and sampling period in header)
    # Deal with scalar data (independent or whatever

    for elem in newdatalist:
        #print ("Here", elem)
        if elem[0] == 'time':
            try:
                ar = date2num(cdfdat[elem[1]][...])
            except:
                ar = date2num(np.asarray([cdf.lib.tt2000_to_datetime(el) for el in cdfdat[elem[1]][...]]))
            arlen= len(ar)
            arraylist.append(ar)
            ind = KEYLIST.index('time')
            array[ind] = ar
        else:
            ar = cdfdat[elem[1]][...]
            if elem[0] in NUMKEYLIST:
                with np.errstate(invalid='ignore'):
                    ar[ar > 88880] = float(nan)
                ind = KEYLIST.index(elem[0])
                headers['col-'+elem[0]] = cdfdat[elem[1]].attrs['LABLAXIS'].lower()
                headers['unit-col-'+elem[0]] = cdfdat[elem[1]].attrs['UNITS']
                if len(indexarray) > 0 and elem[0] in ['f','df']:  ## this is no good - point to depend_0
                    newar = np.asarray([np.nan]*arlen)
                    #print (len(newar),len(ar),len(indexarray))
                    newar[indexarray] = ar
                    #print (len(newar))
                    array[ind] = newar
                    arraylist.append(newar)
                else:
                    array[ind] = ar
                    arraylist.append(ar)
                # if idf -> add f column also to z
                if elem[0] in ['f','F'] and headers.get('DataComponents','') in ['DIF','dif','idf','IDF'] and not len(array[zpos]) > 0:
                    array[zpos] = ar
                    arraylist.append(ar)
                    headers['col-z'] = cdfdat[elem[1]].attrs['LABLAXIS'].lower()
                    headers['unit-col-z'] = cdfdat[elem[1]].attrs['UNITS']

    ndarray = np.array(array)


    stream = DataStream()
    stream = [LineStruct()]

    result = DataStream(stream,headers,ndarray)

    if not flagruletype == '' and len(flaglist) > 0:
        result = result.flag(flaglist)
    #t2 = datetime.utcnow()
    #print "Duration for conventional stream assignment:", t2-t1

    return result


def writeIMAGCDF(datastream, filename, **kwargs):
    """
    Writing Intermagnet CDF format (currently: vers1.2) + optional flagging info
    
    """

    logger.info("Writing IMAGCDF Format {}".format(filename))
    mode = kwargs.get('mode')
    addflags = kwargs.get('addflags')
    skipcompression = kwargs.get('skipcompression')

    cdf.lib.set_backward(False) ## necessary for time_tt2000 support

    testname = str(filename+'.cdf')

    if os.path.isfile(testname):
        filename = testname
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream)
            os.remove(filename)
            mycdf = cdf.CDF(filename, '')
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(datastream,exst)
            os.remove(filename)
            mycdf = cdf.CDF(filename, '')
        elif mode == 'append':
            mycdf = cdf.CDF(filename, filename) # append????
        else: # overwrite mode
            #print filename
            os.remove(filename)
            mycdf = cdf.CDF(filename, '')
    else:
        mycdf = cdf.CDF(filename, '')

    keylst = datastream._get_key_headers()
    tmpkeylst = ['time']
    tmpkeylst.extend(keylst)
    keylst = tmpkeylst

    headers = datastream.header
    head, line = [],[]
    success = False

    # For test purposes: flagging
    flaglist = []

    # check DataComponents for correctness
    dcomps = headers.get('DataComponents','')
    dkeys = datastream._get_key_headers()
    if 'f' in dkeys and len(dcomps) == 3:
        dcomps = dcomps+'S'
    if 'df' in dkeys and len(dcomps) == 3:
        dcomps = dcomps+'G'
    headers['DataComponents'] = dcomps

    ### #########################################
    ###            Check Header 
    ### #########################################

    ## 1. Fixed Part -- current version is 1.2
    ## Transfer MagPy Header to INTERMAGNET CDF attributes
    mycdf.attrs['FormatDescription'] = 'INTERMAGNET CDF format'
    mycdf.attrs['FormatVersion'] = '1.2'
    if addflags:
        mycdf.attrs['FormatVersion'] = '1.x'
    mycdf.attrs['Title'] = 'Geomagnetic time series data'

    ## 2. Check for required info
    for key in headers:
        if key == 'StationIAGAcode' or key == 'IagaCode':
            mycdf.attrs['IagaCode'] = headers[key]
        if key == 'DataComponents' or key == 'ElementsRecorded':
            mycdf.attrs['ElementsRecorded'] = headers[key].upper()
        if key == 'DataPublicationLevel' or key == 'PublicationLevel':
            mycdf.attrs['PublicationLevel'] = headers[key]
        if key == 'StationName' or key == 'ObservatoryName':
            mycdf.attrs['ObservatoryName'] = headers[key]
        if key == 'DataElevation' or key == 'Elevation':
            patt = mycdf.attrs
            patt.new('Elevation',float(headers[key]),type=cdf.const.CDF_DOUBLE)
            #mycdf.attrs['Elevation'] = headers[key]
        if key == 'StationInstitution' or key == 'Institution':
            mycdf.attrs['Institution'] = headers[key]
        if key == 'DataSensorOrientation' or key == 'VectorSensOrient':
            mycdf.attrs['VectorSensOrient'] = headers[key].upper()
        if key == 'DataStandardVersion' or key == 'StandardVersion':
            mycdf.attrs['StandardVersion'] = headers[key]
        if key == 'DataPartialStandDesc' or key == 'PartialStandDesc':
            if headers['DataStandardLevel'] in ['partial','Partial']:
                pass
            mycdf.attrs['PartialStandDesc'] = headers[key]
        if key == 'DataTerms' or key == 'TermsOfUse':
            mycdf.attrs['TermsOfUse'] = headers[key]
        if key == 'DataReferences' or key == 'References':
            mycdf.attrs['References'] = headers[key]
        if key == 'DataID' or key == 'UniqueIdentifier':
            mycdf.attrs['UniqueIdentifier'] = headers[key]
        if key == 'SensorID'or key == 'ParentIdentifier':
            mycdf.attrs['ParentIdentifier'] = headers[key]
        if key == 'StationWebInfo' or key == 'ReferenceLinks':
            mycdf.attrs['ReferenceLinks'] = headers[key]

    ## 3. Optional flagging information
    ##    identify flags within the data set and if they are present then add an attribute to the header
    if addflags:
        flaglist = datastream.extractflags()
        if len(flaglist) > 0:
            mycdf.attrs['FlagRulesetVersion'] = '1.0'
            mycdf.attrs['FlagRulesetType'] = 'Conrad'
    

    #pubdate = cdf.lib.datetime_to_tt2000(datastream._testtime(headers.get('DataPublicationDate','')))
    if not headers.get('DataPublicationDate','') == '':
        try:
            pubdate = datastream._testtime(headers.get('DataPublicationDate',''))
            pubdate = cdf.lib.datetime_to_tt2000(pubdate)
            patt = mycdf.attrs
            patt.new('PublicationDate',pubdate,type=cdf.const.CDF_TIME_TT2000)
        except:
            try:
                pubdate = datastream._testtime(headers.get('DataPublicationDate','')) ## Epoch
                mycdf.attrs['PublicationDate'] = pubdate
            except:
                pubdate = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%S.%f")
                mycdf.attrs['PublicationDate'] = pubdate
    else:
        try:
            pubdate = cdf.datetime_to_tt2000(datetime.utcnow())
        except:
            pubdate = datetime.utcnow()
        mycdf.attrs['PublicationDate'] = pubdate

    if not headers.get('DataSource','')  == '':
        if headers.get('DataSource','') in ['INTERMAGNET', 'WDC']:
            mycdf.attrs['Source'] = headers.get('DataSource','')
        else:
            mycdf.attrs['Source'] = headers.get('DataSource','')
    else:
        mycdf.attrs['Source'] = headers.get('StationInstitution','')

    if not headers.get('DataStandardLevel','') == '':
        if headers.get('DataStandardLevel','') in ['None','none','Partial','partial','Full','full']:
            mycdf.attrs['StandardLevel'] = headers.get('DataStandardLevel','')
        else:
            print("writeIMAGCDF: StandardLevel not defined - please specify by yourdata.header['DataStandardLevel'] = ['None','Partial','Full']")
            mycdf.attrs['StandardLevel'] = 'None'
        if headers.get('DataStandardLevel','') in ['partial','Partial']:
            # one could add a validity check whether provided list is aggreement with standards
            if headers.get('DataPartialStandDesc','') == '':
                print("writeIMAGCDF: PartialStandDesc is missing. Add items like IMOM-11,IMOM-12,IMOM-13 ...")
    else:
        print("writeIMAGCDF: StandardLevel not defined - please specify by yourdata.header['DataStandardLevel'] = ['None','Partial','Full']")
        mycdf.attrs['StandardLevel'] = 'None'

    if not headers.get('DataStandardName','') == '':
        mycdf.attrs['StandardName'] = headers.get('DataStandardName','')
    else:
        try:
            #print ("writeIMAGCDF: Asigning StandardName")
            samprate = float(str(headers.get('DataSamplingRate',0)).replace('sec','').strip())
            if int(samprate) == 1:
                stdadd = 'INTERMAGNET_1-Second'
            elif int(samprate) == 60:
                stdadd = 'INTERMAGNET_1-Minute'
            if headers.get('DataPublicationLevel',0) in [3,'3','Q','quasi-definitive','Quasi-definitive']:
                stdadd += '_QD'
                mycdf.attrs['StandardName'] = stdadd
            elif headers.get('DataPublicationLevel',0) in [4,'4','D','definitive','Definitive']:
                mycdf.attrs['StandardName'] = stdadd
            else:
                print ("writeIMAGCDF: current Publication level {} does not allow to set StandardName".format(headers.get('DataPublicationLevel',0)))
                mycdf.attrs['StandardLevel'] = 'None'
        except:
            print ("writeIMAGCDF: Asigning StandardName Failed")

    proj = headers.get('DataLocationReference','')
    longi = headers.get('DataAcquisitionLongitude','')
    lati = headers.get('DataAcquisitionLatitude','')
    try:
        longi = "{:.3f}".format(float(longi))
        lati = "{:.3f}".format(float(lati))
    except:
        print("writeIMAGCDF: could not convert lat long to floats")
    if not longi=='' or lati=='':
        if proj == '':
            patt = mycdf.attrs
            try:
                patt.new('Latitude',float(lati),type=cdf.const.CDF_DOUBLE)
                patt.new('Longitude',float(longi),type=cdf.const.CDF_DOUBLE)
            except:
                patt.new('Latitude',lati,type=cdf.const.CDF_DOUBLE)
                patt.new('Longitude',longi,type=cdf.const.CDF_DOUBLE)
            #mycdf.attrs['Latitude'] = lati
            #mycdf.attrs['Longitude'] = longi
        else:
            if proj.find('EPSG:') > 0:
                epsg = int(proj.split('EPSG:')[1].strip())
                if not epsg==4326:
                    print ("writeIMAGCDF: converting coordinates to epsg 4326")
                    longi,lati = convertGeoCoordinate(float(longi),float(lati),'epsg:'+str(epsg),'epsg:4326')
                    longi = "{:.3f}".format(float(longi))
                    lati = "{:.3f}".format(float(lati))
            patt = mycdf.attrs
            patt.new('Latitude',float(lati),type=cdf.const.CDF_DOUBLE)
            patt.new('Longitude',float(longi),type=cdf.const.CDF_DOUBLE)
            #mycdf.attrs['Latitude'] = lati
            #mycdf.attrs['Longitude'] = longi

    if not 'StationIagaCode' in headers and 'StationID' in headers:
        mycdf.attrs['IagaCode'] = headers.get('StationID','')

    ### #########################################
    ###               Data 
    ### #########################################

    def checkEqualIvo(lst):
        # http://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
        return not lst or lst.count(lst[0]) == len(lst)

    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    ndarray = False
    if len(datastream.ndarray[0]>0):
        ndarray = True

    # Check F/S/G select either S or G, send out warning if presumably F (mean zero, stddeviation < resolution)
    naninds = np.asarray([])
    ## Analyze F and dF columns:
    fcolname = 'S'
    if 'f' in keylst or 'df' in keylst:
        if 'f' in keylst:
            if not 'df' in keylst:
                 print ("writeIMAGCDF: Found F column") # check whether F or S
                 comps = datastream.header.get('DataComponents')
                 if not comps.endswith('S'):
                     print ("writeIMAGCDF: given components are {}. Checking F column...".format(datastream.header.get('DataComponents')))
                     #calculate delta F and determine average diff
                     datastream = datastream.delta_f()
                     dfmean, dfstd = datastream.mean('df',std=True, percentage=50)
                     if dfmean < 0.0000000001 and dfstd < 0.0000000001:
                         fcolname = 'F'
                         print ("writeIMAGCDF: analyzed F column - values are apparently calculated from vector components - using column name 'F'")
                     else:
                         print ("writeIMAGCDF: analyzed F column - values are apparently independend from vector components - using column name 'S'")
            pos = KEYLIST.index('f')
            col = datastream.ndarray[pos]
        if 'df' in keylst:
            #print ("writeIMAGCDF: Found dF column")
            pos = KEYLIST.index('df')
            col = datastream.ndarray[pos]
        col = col.astype(float)
        
        nonancol = col[~np.isnan(col)]
            
        #print ("IMAG", len(nonancol),datastream.length()[0])
        if len(nonancol) < datastream.length()[0]/2.:
            #shorten col
            print ("writeIMF - reducing f column resolution:", len(nonancol), len(col))
            naninds = np.where(np.isnan(col))[0]
            #print (naninds, len(naninds))
            useScalarTimes=True
            #[inds]=np.take(col_mean,inds[1])
        else:
            #keep column and (later) leave time       
            useScalarTimes=True  # change to False in order to use a single col

    ## get sampling rate of vec, get sampling rate of scalar, if different extract scalar and time use separate, else ..

    for key in keylst:
        if key in ['time','sectime','x','y','z','f','dx','dy','dz','df','t1','t2']:
            ind = KEYLIST.index(key)
            if ndarray and len(datastream.ndarray[ind])>0:
                col = datastream.ndarray[ind]
            else:
                col = datastream._get_column(key)
            col = col.astype(float)

            if not False in checkEqual3(col):
                logger.warning("Found identical values only for {}".format(key))
                col = col[:1]
            if key == 'time':
                key = 'GeomagneticVectorTimes'
                try: ## requires spacepy >= 1.5
                    mycdf.new(key, type=cdf.const.CDF_TIME_TT2000)
                    mycdf[key] = cdf.lib.v_datetime_to_tt2000(np.asarray([num2date(elem).replace(tzinfo=None) for elem in col]))
                    #print("writeIMAGCDF: Datetimes successfully converted to TT2000")
                    if useScalarTimes:
                        key = 'GeomagneticScalarTimes'
                        mycdf.new(key, type=cdf.const.CDF_TIME_TT2000)
                        if len(naninds) > 0:
                            logger.info("writeIMAGCDF: ({}) removing nan values from scalar times".format(datetime.utcnow()))
                            mycdf[key] = np.delete(mycdf['GeomagneticVectorTimes'], naninds)
                            logger.info("writeIMAGCDF: ({}) finished".format(datetime.utcnow()))
                        else:
                            mycdf[key] = mycdf['GeomagneticVectorTimes']
                except:
                    mycdf[key] = np.asarray([num2date(elem).replace(tzinfo=None) for elem in col])
            elif len(col) > 0:
                #if len(col) > 1000000:
                #    print ("Starting with {}".format(key))
                comps = datastream.header.get('DataComponents','')
                keyup = key.upper()
                if key in ['t1','t2']:
                    cdfkey = key.upper().replace('T','Temperature')
                elif not comps == '':
                    try:
                        if key == 'x':
                            compsupper = comps[0].upper()
                        elif key == 'y':
                            compsupper = comps[1].upper()
                        elif key == 'z':
                            compsupper = comps[2].upper()
                        elif key == 'f':
                            compsupper = fcolname ## MagPy requires independend F value
                        elif key == 'df':
                            compsupper = 'G'
                        else:
                            compsupper = key.upper()
                        cdfkey = 'GeomagneticField'+compsupper
                        keyup = compsupper
                    except:
                        cdfkey = 'GeomagneticField'+key.upper()
                        keyup = key.upper()
                else:
                    cdfkey = 'GeomagneticField'+key.upper()
                #print(len(col), keyup, key)
                #print("1", datetime.utcnow())
                nonetest = [elem for elem in col if not elem == None]
                #nonetest = col[col != np.array(None)]
                #print("2", datetime.utcnow())
                if len(nonetest) > 0:
                    mycdf[cdfkey] = col
                    mycdf[cdfkey].attrs['DEPEND_0'] = "GeomagneticVectorTimes"
                    mycdf[cdfkey].attrs['DISPLAY_TYPE'] = "time_series"
                    mycdf[cdfkey].attrs['LABLAXIS'] = keyup
                    mycdf[cdfkey].attrs['FILLVAL'] = np.nan
                    if key in ['x','y','z','h','e','g','t1','t2']:
                        mycdf[cdfkey].attrs['VALIDMIN'] = -88880.0
                        mycdf[cdfkey].attrs['VALIDMAX'] = 88880.0
                    elif key == 'i':
                        mycdf[cdfkey].attrs['VALIDMIN'] = -90.0
                        mycdf[cdfkey].attrs['VALIDMAX'] = 90.0
                    elif key == 'd':
                        mycdf[cdfkey].attrs['VALIDMIN'] = -360.0
                        mycdf[cdfkey].attrs['VALIDMAX'] = 360.0
                    elif key in ['f','s']:
                        if useScalarTimes:
                            if len(naninds) > 0:
                                mycdf[cdfkey] = col[~np.isnan(col)]
                            mycdf[cdfkey].attrs['DEPEND_0'] = "GeomagneticScalarTimes"
                        #else:
                        #    mycdf[cdfkey] = col
                        mycdf[cdfkey].attrs['VALIDMIN'] = 0.0
                        mycdf[cdfkey].attrs['VALIDMAX'] = 88880.0
                #if len(col) > 1000000:
                #    print ("Finished column {}".format(key))
                #print("3", datetime.utcnow())

                for keydic in headers:
                    if keydic == ('col-'+key):
                        if key in ['x','y','z','f','dx','dy','dz','df']:
                            try:
                                mycdf[cdfkey].attrs['FIELDNAM'] = "Geomagnetic Field Element "+key.upper()
                            except:
                                pass
                        if key in ['t1','t2']:
                            try:
                                mycdf[cdfkey].attrs['FIELDNAM'] = "Temperature"+key.replace('t','')
                            except:
                                pass
                    if keydic == ('unit-col-'+key):
                        if key in ['x','y','z','f','dx','dy','dz','df','t1','t2']:
                            try:
                                unit = 'unspecified'
                                if 'unit-col-'+key == 'deg C':
                                    #mycdf[cdfkey].attrs['FIELDNAM'] = "Temperature "+key.upper()
                                    unit = 'Celsius'
                                elif 'unit-col-'+key == 'deg':
                                    unit = 'Degrees of arc'
                                else:
                                    unit = headers.get('unit-col-'+key,'')
                                mycdf[cdfkey].attrs['UNITS'] = unit
                            except:
                                pass
            success = True

    if len(flaglist) > 0 and addflags == True:
        flagstart = 'FlagBeginTimes'
        flagend = 'FlagEndTimes'
        flagcomponents = 'FlagComponents'
        flagcode = 'FlagCode'
        flagcomment = 'FlagDescription'
        flagmodification = 'FlagModificationTimes'
        flagsystemreference = 'FlagSystemReference'
        flagobserver = 'FlagObserver'

        trfl = np.transpose(flaglist)
        #print ("Transposed flaglist", trfl)
        ok =True
        if ok:
        #try:
            mycdf.new(flagstart, type=cdf.const.CDF_TIME_TT2000)
            mycdf[flagstart] = cdf.lib.v_datetime_to_tt2000(trfl[0])
            mycdf.new(flagend, type=cdf.const.CDF_TIME_TT2000)
            mycdf[flagend] = cdf.lib.v_datetime_to_tt2000(trfl[1])
            mycdf.new(flagmodification, type=cdf.const.CDF_TIME_TT2000)
            mycdf[flagmodification] = cdf.lib.v_datetime_to_tt2000(trfl[-1])

            # Here we can select between different content
            if len(flaglist[0]) == 7:
                #[st,et,key,flagnumber,commentarray[idx],sensorid,now]
                # eventually change flagcomponent in the future
                fllist = [flagcomponents,flagcode,flagcomment, flagsystemreference] # , flagobserver]
            elif len(flaglist[0]) == 8:  
                # Future version ??
                fllist = [flagcomponents,flagcode,flagcomment, flagsystemreference, flagobserver]
            for idx, cdfkey in enumerate(fllist):
                if not cdfkey == flagcode:
                    ll = [el.encode('UTF8') for el in trfl[idx+2]]
                else:
                    ll = trfl[idx+2]
                mycdf[cdfkey] = ll
                mycdf[cdfkey].attrs['DEPEND_0'] = "FlagBeginTimes"
                mycdf[cdfkey].attrs['DISPLAY_TYPE'] = "time_series"
                mycdf[cdfkey].attrs['LABLAXIS'] = cdfkey.strip('Flag')
                mycdf[cdfkey].attrs['FILLVAL'] = np.nan
                mycdf[cdfkey].attrs['FIELDNAM'] = cdfkey
                if cdfkey in ['flagcode']:
                    mycdf[cdfkey].attrs['VALIDMIN'] = 0
                    mycdf[cdfkey].attrs['VALIDMAX'] = 9
        #except:
        #    print ("writeIMAGCDF: error when adding flags. skipping this part")
        logger.info("writeIMAGCDF: Flagging information added to file")

    if not skipcompression:
        try:
            mycdf.compress(cdf.const.GZIP_COMPRESSION, 5)
        except:
            logger.warning("writeIMAGCDF: Compression failed for unknown reason - storing uncompresed data")
            pass
    mycdf.close()
    return success


def readIMF(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet data format (IMF1.23)
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    headers={}
    array = [[] for elem in KEYLIST]

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    #if stream.header is None:
    #    headers = {}
    #else:
    #    headers = stream.header
    headers = {}
    data = []
    key = None
    find = KEYLIST.index('f')
    t1ind = KEYLIST.index('t1')
    var1ind = KEYLIST.index('var1')
    t2ind = KEYLIST.index('t2')

    theday = extractDateFromString(filename)
    try:
        if starttime:
            if not theday[-1] >= datetime.date(stream._testtime(starttime)):
                getfile = False
            #if not theday >= datetime.date(stream._testtime(starttime)):
            #    getfile = False
        if endtime:
            #if not theday <= datetime.date(stream._testtime(endtime)):
            #    getfile = False
            if not theday[0] <= datetime.date(stream._testtime(endtime)):
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
                headers['DataComponents'] = block[4]
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
                        array[0].append(date2num(datetime.strptime(time,"%b%d%y_%H:%M")))

                        index = int(4*i)
                        if not int(data[0+index]) > 999990:
                            #row.x = float(data[0+index])/10
                            array[1].append(float(data[0+index])/10)
                        else:
                            #row.x = float(nan)
                            array[1].append(np.nan)
                        if not int(data[1+index]) > 999990:
                            #row.y = float(data[1+index])/10
                            array[2].append(float(data[1+index])/10)
                        else:
                            #row.y = float(nan)
                            array[2].append(np.nan)
                        if not int(data[2+index]) > 999990:
                            #row.z = float(data[2+index])/10
                            array[3].append(float(data[2+index])/10)
                        else:
                            #row.z = float(nan)
                            array[3].append(np.nan)
                        if not int(data[3+index]) > 999990:
                            #row.f = float(data[3+index])/10
                            array[4].append(float(data[3+index])/10)
                        else:
                            #row.f = float(nan)
                            array[4].append(np.nan)
                        #typus = block[4].lower()
                        #stream.add(row)
                    except:
                        logging.error('format_imf: problem with dataformat - check block header')
                        return DataStream([LineStruct()], headers, np.asarray([np.asarray(el) for el in array]))
                minute = minute + 2

    fh.close()

    array = np.asarray([np.asarray(el) for el in array])
    stream = [LineStruct()]
    return DataStream(stream, headers, array)


def writeIMF(datastream, filename, **kwargs):
    """
    Writing Intermagnet format data.
    """

    mode = kwargs.get('mode')
    version = kwargs.get('version')
    gin = kwargs.get('gin')
    datatype = kwargs.get('datatype')

    success = False
    # 1. check whether datastream corresponds to minute file
    if not 0.9 < datastream.get_sampling_period()*60*24 < 1.1:
        logger.error("writeIMF: Data needs to be minute data for Intermagnet - filter it accordingly")
        return False

    # 2. check whether file exists and according to mode either create, append, replace
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(datastream,exst)
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

    elemtype = 'XYZF'
    try:
        elemtpye = datastream.header['']
    except:
        pass

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
        if not ndtype:
            elem = datastream[i]
            elemx = elem.x
            elemy = elem.y
            elemz = elem.z
            elemf = elem.f
            timeval = elem.time
        else:
            elemx = datastream.ndarray[xind][i]
            elemy = datastream.ndarray[yind][i]
            elemz = datastream.ndarray[zind][i]
            elemf = datastream.ndarray[find][i]
            timeval = datastream.ndarray[0][i]

        date = num2date(timeval).replace(tzinfo=None)
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
        if not isnan(elemx):
            x = elemx*10
        else:
            x = 999999
        if not isnan(elemy):
            y = elemy*10
        else:
            y = 999999
        if not isnan(elemz):
            z = elemz*10
        else:
            z = 999999
        if not isnan(elemf):
            f = elemf*10
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
                #print minute, dataline
            else: # even
                dataline = '9999999 9999999 9999999 999999'
            minute = minute + 1

    myFile.close()

    return True



def readBLV(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet BLV data format (IBFV2.00)
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
    theday = extractDateFromString(filename)
    try:
        year = str(theday[0].year)
    except:
        pass
    try:
        if starttime:
            if not theday[-1] >= datetime.date(stream._testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(stream._testtime(endtime)):
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
    scalarid = 'None'
    varioid = 'None'
    pierid = 'None'

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
                headers['DataScaleX'] = float(block[1])
                headers['DataScaleZ'] = float(block[2])
                headers['StationID'] = block[3]
                headers['StationIAGAcode'] = block[3]
            elif headonly:
                # skip data for option headonly
                return
            elif len(line) in [44,45] and not len(starfound) > 1:  # block 1 - basevalues
                # data info
                if not mode == 'adopted':
                    block = line.split()
                    block = [el if not float(el) > 99998.00 else np.nan for el in block]
                    dttime = datetime.strptime(year+'-'+block[0], "%Y-%j")+timedelta(hours=12)
                    if date2num(dttime) in array[0]:
                        dttime = dttime+timedelta(seconds=1)
                    array[0].append(date2num(dttime))
                    array[xpos].append(float(block[1]))
                    if headers.get('DataComponents').startswith('HDZ') or headers.get('DataComponents').startswith('hdz'):
                        array[ypos].append(float(block[2])/60.0)
                    else:
                        array[ypos].append(float(block[2]))
                    array[zpos].append(float(block[3]))
                    array[fpos].append(float(block[4]))
                #print block
            elif len(line) in [54,55] and not len(starfound) > 1:  # block 2 - adopted basevalues
                # data info
                block = line.split()
                if float(block[5])>998.0:
                    block[5] = np.nan
                dt = date2num(datetime.strptime(year+'-'+block[0], "%Y-%j")+timedelta(hours=12))
                xval = float(block[1])
                if headers['DataComponents'][:3] == 'HDZ':
                    yval = float(block[2])/60.0
                else:
                    yval = float(block[2])
                zval = float(block[3])
                fval = float(block[4])
                dfval = float(block[5])
                strval = block[6]
                if mode == 'adopted':
                    if strval in ['d','D']:
                        print ("Found break at {}".format(block[0]))
                        print ("Adding nan column for jumps in plot")
                        array[0].append(dt-0.5)
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
                    if strval in ['d','D']:
                        tempstream = DataStream([LineStruct()], {}, np.asarray([np.asarray(el) for el in farray]))
                        funclist.append(tempstream.fit([KEYLIST[xpos],KEYLIST[ypos],KEYLIST[zpos],KEYLIST[fpos]],fitfunc='spline'))
                        farray = [[] for key in KEYLIST]
                    farray[0].append(dt)
                    farray[xpos].append(xval)
                    farray[ypos].append(yval)
                    farray[zpos].append(zval)
                    farray[fpos].append(fval)
                    farray[dfpos].append(dfval)
                    farray[strpos].append(strval)
            elif line.startswith('*'):
                # data info
                starfound.append('*')
                if len(starfound) > 1: # Comment section starts here
                    tempstream = DataStream([LineStruct()], {}, np.asarray([np.asarray(el) for el in farray]))
                    funclist.append(tempstream.fit([KEYLIST[xpos],KEYLIST[ypos],KEYLIST[zpos],KEYLIST[fpos]],fitfunc='spline'))
            elif len(starfound) > 1: # Comment section starts here
                logger.debug("Found comment section", starfound)
                block = line.split()
                if block[0].startswith('Scalar') and len(block) > 1:
                    scalarid = block[1]
                if block[0].startswith('Vario') and len(block) > 1:
                    varioid = block[1]
                if block[0].startswith('Pier') and len(block) > 1:
                    pierid = block[1]
            else:
                pass
    fh.close()

    array = [np.asarray(el) for el in array]
    if len(funclist) > 0:
        headers['DataFunction'] = funclist
    headers['DataFormat'] = 'MagPyDI'
    headers['SensorID'] = 'BLV_{}_{}_{}'.format(varioid,scalarid,pierid)

    return DataStream([LineStruct()], headers, np.asarray(array))


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
        if not absinfolist[0].startswith('7') and not len(absinfolist) > 5:
            return None
        funclist = []
        for absinfo in absinfolist:
            parameter = absinfo.split('_')
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
            line = [startabs,endabs,extradays,fitfunc,fitdegree,knotstep,keys]
            if len(parameter) >= 14:
                    #extract pier information
                    pierdata = True
                    pierlon = float(parameter[9]) 
                    pierlat = float(parameter[10])
                    pierlocref = parameter[11]
                    pierel = float(parameter[12])
                    pierelref =  parameter[13]
                    line.extend([pierlon,pierlat,pierlocref,pierel,pieelref])
            funclist.append(line)
        return funclist

    if not year:
        year = datetime.strftime(datetime.utcnow(),'%Y')
        t1 = date2num(datetime.strptime(str(int(year))+'-01-01','%Y-%m-%d'))
        t2 = date2num(datetime.utcnow())
    else:
        t1 = date2num(datetime.strptime(str(int(year))+'-01-01','%Y-%m-%d'))
        t2 = date2num(datetime.strptime(str(int(year)+1)+'-01-01','%Y-%m-%d'))

    #absinfoline = []
    absinfodiff = ''
    if diff:
        if diff.length()[0] > 1:
            if not absinfo:
                absinfodiff = diff.header.get('DataAbsInfo','')
            if not absinfodiff == '':
                logger.info("writeBLV: Getting Absolute info from header of provided dailymean file")
                absinfo = absinfodiff
                #absinfoline = absinfo.split('_')
                #extradays= int(absinfoline[2])
                #fitfunc = absinfoline[3]
                #fitdegree = int(absinfoline[4])
                #knotstep = float(absinfoline[5])
                #keys = ['dx','dy','dz']#,'df'] # absinfoline[6]

    if not absinfo:
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
    else:
        parameterlist = getAbsInfo(absinfo)

    logger.info("writeBLV: Extracted baseline parameter: {}".format(parameterlist))

    # 2. check whether file exists and according to mode either create, append, replace
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream)
            myFile= open( filename, "wt" )
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(datastream,exst)
            myFile= open( filename, "wt" )
        elif mode == 'append':
            myFile= open( filename, "at" )
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= open( filename, "wt" )
    else:
        myFile= open( filename, "wt" )

    #print ("filename", filename)
    logger.info("writeBLV: file: {}".format(filename))

    # 3. check whether datastream corresponds to an absolute file and remove unreasonable inputs
    #     - check whether F measurements were performed at the main pier - delta F's are available

    if not datastream.header.get('DataFormat','') == 'MagPyDI':
        logger.error("writeBLV: Unsupported format - convert to MagPyDI first")
        logger.error("  -- export BLV data -- too be done")
        logger.error("  -- eventually also not yet assigned when accessing database contents")
        return False

    #try:
    #    if not datastream.header['DataFormat'] == 'MagPyDI':
    #        logger.error("writeBLV: Format not recognized - needs to be MagPyDI")
    #        return False
    #except:
    #    logger.error("writeBLV: Format not recognized - should be MagPyDI")
    #    logger.error("writeBLV: is not yet assigned during database access")
    #    #return False


    # 4. create dummy stream with time range
    dummystream = DataStream()
    array = [[] for key in KEYLIST]
    row1, row2 = LineStruct(), LineStruct()
    row1.time = t1
    row2.time = t2
    array[0].append(row1.time)
    array[0].append(row2.time)
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
    dummystream.add(row1)
    dummystream.add(row2)
    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])
    dummystream.ndarray = np.asarray(array)

    #print("1", row1.time, row2.time)

    # 5. Extract the data for one year and calculate means
    backupabsstream = datastream.copy()
    if not len(datastream.ndarray[0]) > 0:
        backupabsstream = backupabsstream.linestruct2ndarray()

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
                    fbasefunc = True

    if keys == ['dx','dy','dz','df'] and datastream._is_number(deltaF):
        logger.info("writeBLV: found deltaF values, but using provided deltaF {} for adopted scalar baseline ".format(deltaF))

    fbasefunc = False
    if keys == ['dx','dy','dz','df'] and not datastream._is_number(deltaF) and not deltaF == None:
        #print ("writeBLV: found string in deltaF")
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
        elif comps in ['XYZF','xyzf','xyz','XYZ']:
            datastream = datastream.xyz2hdz()
        comps = 'HDZF'
    except:
        # assume idf orientation
        datastream = datastream.idf2xyz()
        datastream = datastream.xyz2hdz()
        comps = 'HDZF'

    meanstream = datastream.extract('f', 0.0, '>')
    meanstream = meanstream.flag_outlier()
    meanstream = meanstream.remove_flagged()

    if not meanf:
        meanf = meanstream.mean('f')
    if not meanh:
        meanh = meanstream.mean('x')

    # 6. calculate baseline function
    basefunctionlist =[]
    keys = []
    for parameter in parameterlist:
        # check whether timerange is fitting
        if (parameter[0] >= t1 and parameter[0] <= t2) or (parameter[1] >= t1 and parameter[1] <= t2) or (parameter[0] < t1 and parameter[1] > t2):
            keys = parameter[6]
            print ("writeBLV: Using line", parameter, backupabsstream.length())
            basefunctionlist.append(dummystream.baseline(backupabsstream,startabs=parameter[0],endabs=parameter[1],keys=parameter[6], fitfunc=parameter[3],fitdegree=parameter[4],knotstep=parameter[5],extradays=parameter[2]))

    #print ("writeBLV: Extracted parameter", basefunctionlist)
    #basefunction = dummystream.baseline(backupabsstream,keys=keys, fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep,extradays=extradays)

    yar = [[] for key in KEYLIST]
    datelist = [day+0.5 for day in range(int(t1),int(t2))]
    for idx, elem in enumerate(yar):
        if idx == 0:
            yar[idx] = np.asarray(datelist)
        elif idx in [indx,indy,indz,indf]:
            yar[idx] = np.asarray([0]*len(datelist))
        else:
            yar[idx] = np.asarray(yar[idx])


    yearstream = DataStream([LineStruct()],datastream.header,np.asarray(yar))
    yearstream = yearstream.func2stream(basefunctionlist,mode='addbaseline',keys=keys)

    #print("writeBLV:", yearstream.length())

    if fbasefunc:
        logger.info("Adding adopted scalar from function {}".format(fbase))
        fbasefunclist = []
        for parameter in fbase:
            # check whether timerange is fitting
            if (parameter[0] >= t1 and parameter[0] <= t2) or (parameter[1] >= t1 and parameter[1] <= t2) or (parameter[0] < t1 and parameter[1] > t2):
                fbasefunclist.append(dummystream.baseline(backupabsstream,startabs=parameter[0],endabs=parameter[1],keys=parameter[6], fitfunc=parameter[3],fitdegree=parameter[4],knotstep=parameter[5],extradays=parameter[2]))
        yearstream = yearstream.func2stream(fbasefunclist,mode='values',keys=['df'])
        #print("writeBLV:", yearstream.length())


    #print "writeBLV: Testing deltaF (between Pier and F):"
    #print "adopted diff is yearly average"
    #print "adopted average daily delta F comes from diff of vario and scalar"
    #pos = KEYLIST.index('df')
    #dfl = [val for val in datastream.ndarray[pos] if not isnan(val)]
    #meandf = datastream.mean('df')
    #print "Mean df", meandf, mean(dfl)

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
                day = datetime.strftime(num2date(elem),'%j')
                x = float(datastream.ndarray[indx][idx])
                y = float(datastream.ndarray[indy][idx])*60.
                z = float(datastream.ndarray[indz][idx])
                df = float(datastream.ndarray[indf][idx])
                ftype = datastream.ndarray[indFtype][idx]
                if np.isnan(x):
                    x = 99999.00
                if np.isnan(y):
                    y = 99999.00
                if np.isnan(z):
                    z = 99999.00
                if np.isnan(df) or ftype.startswith('Fext'):
                    df = 99999.00
                elif deltaF and dummystream._is_number(deltaF):
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
            elif deltaF and dummystream._is_number(deltaF):
                f = elem.df + deltaF
            else:
                f = elem.df
            line = '%s %9.2f %9.2f %9.2f %9.2f\r\n' % (day,x,y,z,f)
            myFile.writelines( line ) #.encode('utf-8') )

    # 9. adopted basevalues
    myFile.writelines( '*\r\n' )
    posstr = KEYLIST.index('str1')
    #print ("LENGTH", len(yearstream.ndarray[posstr]))
    if not len(yearstream.ndarray[posstr]) > 0:
        parameterlst = ['c']*len(yearstream.ndarray[0])
    else:
        parameterlst = yearstream.ndarray[posstr]

    for idx, t in enumerate(yearstream.ndarray[0]):
        #001_AAAAAA.AA_BBBBBB.BB_ZZZZZZ.ZZ_SSSSSS.SS_DDDD.DD_mCrLf
        day = datetime.strftime(num2date(t),'%j')
        parameter = parameterlst[idx]
        if np.isnan(yearstream.ndarray[indx][idx]):
            x = 99999.00
        else:
            if not comps.lower() == 'idff':
                x = yearstream.ndarray[indx][idx]
            else:
                x = yearstream.ndarray[indx][idx]*60.
        if np.isnan(yearstream.ndarray[indy][idx]):
            y = 99999.00
        else:
            if comps.lower() == 'xyzf':
                y = yearstream.ndarray[indy][idx]
            else:
                y = yearstream.ndarray[indy][idx]*60.
        if np.isnan(yearstream.ndarray[indz][idx]):
            z = 99999.00
        else:
            z = yearstream.ndarray[indz][idx]
        if deltaF and dummystream._is_number(deltaF):
            f = deltaF
        elif deltaF: # and dummystream._is_number(deltaF):
            f = yearstream.ndarray[indf][idx]
        elif np.isnan(yearstream.ndarray[indf][idx]):
            f = 99999.00
        else:
            f = yearstream.ndarray[indf][idx]
        if diff:
            #print ("writeBLV: Here", t, diff.length()[0])
            posdf = KEYLIST.index('df')
            indext = [np.abs(np.asarray(diff.ndarray[0])-t).argmin()]
            #indext = [i for i,tpos in enumerate(diff.ndarray[0]) if num2date(tpos).date() == num2date(t).date()]
            #print("Hello", posdf, diff.ndarray[0], diff.ndarray[posdf], len(diff.ndarray[0]),indext, t)
            #                                                     []       365           [0] 735599.5
            if len(indext) > 0:
                df = diff.ndarray[posdf][indext[0]]
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
    db = False
    if not db:
        posst1 = KEYLIST.index('str2')
        infolist.append(datastream[-1].str2)
        infolist.append(datastream[-1].str3)
        infolist.append(datastream[-1].str4)
        #

    funcline5 = 'Measurements conducted primarily with:\r\n'
    funcline6 = 'DI: %s\r\n' % infolist[0]
    funcline7 = 'Scalar: %s\r\n' % infolist[1]
    funcline8 = 'Variometer: %s\r\n' % infolist[2]
    funcline9 = 'Pier: %s\r\n' % datastream.header.get('DataPier','-')
    # additional text with pier, instrument, how f difference is defined, which are the instruments etc
    summaryline = '-- analysis supported by MagPy\r\n'
    myFile.writelines( '-'*40 + '\r\n' ) #.encode('utf-8') )
    myFile.writelines( funcline5 ) #.encode('utf-8') )
    myFile.writelines( funcline6 ) #.encode('utf-8') )
    myFile.writelines( funcline7 ) #.encode('utf-8') )
    myFile.writelines( funcline8 ) #.encode('utf-8') )
    myFile.writelines( funcline9 ) #.encode('utf-8') )
    myFile.writelines( '-'*40 + '\r\n' ) #.encode('utf-8') )
    myFile.writelines( summaryline ) #.encode('utf-8') )  changed open to 'wt' -> no encoding to binary necessary

    myFile.close()
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
    debug = kwargs.get('debug')

    if debug:
        print ("readIYVF: Reading data")

    getfile = True

    stream = DataStream()
    headers = {}
    data = []
    key = None

    array = [[] for key in KEYLIST]

    cnt = 0
    paracnt = 999998
    roworder=['d','i','h','x','y','z','f']
    jumpx,jumpy,jumpz,jumpf=0,0,0,0
    tsel = 'A' # Use only all days rows
    tprev = tsel # For jump treatment
    lc = KEYLIST.index('var5')  ## store the line number of each loaded line here
                                ## this is used by writeIYFV to add at the correct position
    newarray = []

    headfound = False
    latitudefound = False

    def dropnonascii(text):
        return ''.join([i if ord(i) < 128 else ' ' for i in text])

    if debug:
        print ("readIYVF: Reading data ...")


    if sys.version_info >= (3, 0):
        code = 'rt' 
        fh = open(filename, code, errors='ignore')  # python3
    else:
        code = 'rb'
        fh = open(filename, code)  #
    
    for line in fh:
            line = dropnonascii(line)
            line = line.rstrip()
            cnt = cnt+1
            #print ("Line", line)
            #line = line.lstrip()  # delete leading spaces
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
                #elif line.find('COLATITUDE') > 0:
                #    loc = line.split()
                #    headers['DataAcquisitionLatitude'] = 90.0-float(loc[1])
                #    headers['DataAcquisitionLongitude'] = float(loc[3])
                #    headers['DataElevation'] = float(loc[3])

            elif line.find(' YEAR ') > 0:
                paracnt = cnt
                para = line.split()
                para = [elem.lower() for elem in para[1:8]]
            elif cnt == paracnt+1:
                units = line.split()
                tmp = ['deg','deg']
                tmp.extend(units[4:10])
                units = tmp
            elif line.startswith(' 1') or line.startswith(' 2'): # Upcoming year 3k problem ;)
                if not headonly:
                    #if debug:
                    # get data
                    data = line.split()
                    getdata = True
                    if line.find('J') > 0:
                        line = line.replace('     0.0','  0  0.0')
                        data = line.split()
                        num = data.index('J')
                        if len(data) >= 12 and num == 10:
                            getdata = True
                        else:
                            logger.warning("readIYFV: could not interprete jump line")
                            getdata = False
                    if not len(data) >= 12:
                        getdata = False
                        logger.warning("readIYFV: apparent inconsistency in file format - {}".format(len(data)))
                    if getdata:
                        #try:
                        ye = data[0].split('.')
                        dat = ye[0]+'-06-01'
                        row = []
                        ti = date2num(datetime.strptime(dat,"%Y-%m-%d"))
                        row.append(dms2d(data[1]+':'+data[2]))
                        row.append(dms2d(data[3]+':'+data[4]))
                        row.append(float(data[5]))
                        row.append(float(data[6]))
                        row.append(float(data[7]))
                        row.append(float(data[8]))
                        row.append(float(data[9]))
                        t =  data[10]
                        ele =  data[11]
                        headers['DataComponents'] = ele
                        # transfer
                        #print ("Check", t, tsel)
                        if len(data) == 13:
                            note =  data[12]
                        if t == tsel:
                            array[0].append(ti)
                            array[lc].append(cnt)
                            """
                            for comp in ele.lower():
                                if comp in ['x','h','i']:
                                    headers['col-x'] = comp
                                    headers['unit-col-x'] = units[para.index(comp)]
                                    array[1].append(row[para.index(comp)]-jumpx)
                                elif comp in ['y','d']:
                                    headers['col-y'] = comp
                                    headers['unit-col-y'] = units[para.index(comp)]
                                    array[2].append(row[para.index(comp)]-jumpy)
                                elif comp in ['i','z']:
                                    headers['col-z'] = comp
                                    headers['unit-col-z'] = units[para.index(comp)]
                                    array[3].append(row[para.index(comp)]-jumpz)
                                elif comp in ['f']:
                                    headers['col-f'] = comp
                                    headers['unit-col-f'] = units[para.index(comp)]
                                    array[4].append(row[para.index(comp)]-jumpf)
                            """
                            headers['col-x'] = 'x'
                            headers['unit-col-x'] = units[para.index('x')]
                            headers['col-y'] = 'y'
                            headers['unit-col-y'] = units[para.index('y')]
                            headers['col-z'] = 'z'
                            headers['unit-col-z'] = units[para.index('z')]
                            headers['col-f'] = 'f'
                            headers['unit-col-f'] = units[para.index('f')]
                            array[1].append(row[para.index('x')]-jumpx)
                            array[2].append(row[para.index('y')]-jumpy)
                            array[3].append(row[para.index('z')]-jumpz)
                            array[4].append(row[para.index('f')]-jumpf)
                            #print ("here", array)
                            checklist = coordinatetransform(array[1][-1],array[2][-1],array[3][-1],'xyz')
                            #print ("checks")
                            diffs = (np.array(row) - np.array(checklist))
                            #print ("diffs")
                            for idx,el in enumerate(diffs):
                                goodval = True
                                if idx in [0,1]: ## Angular values
                                    if el > 0.008:
                                        goodval = False
                                else:
                                    if el > 0.8:
                                        goodval = False
                            if not goodval:
                                logger.warning("readIYFV: verify conversions between components !")
                                logger.warning("readIYFV: found: {}".format(np.array(row)))
                                logger.warning("readIYFV: expected: {}".format(np.array(checklist)))
                        elif t == 'J' and tprev == tsel:
                            jumpx = jumpx + row[para.index('x')]
                            jumpy = jumpy + row[para.index('y')]
                            jumpz = jumpz + row[para.index('z')]
                            jumpf = jumpf + row[para.index('f')]
                        tprev = tsel
            else:
                pass

    fh.close()

    if debug:
        print ("readIYVF: Got data ...")

    #fh.close()
    array = [np.asarray(ar) for ar in array]
    stream = DataStream([LineStruct()], headers, np.asarray(array))

    if not ele.lower().startswith('xyz'):
        if ele.lower()[:3] in ['hdz','dhz']: # exception for usgs
            ele = 'hdz'
        stream = stream._convertstream('xyz2'+ele.lower()[:3])

    stream.header['DataFormat'] = 'IYFV'
    stream.header['SensorID'] = "{}_{}".format(headers.get('StationID','xxx').upper(),'IYFV')

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
        comment:        (string) a comment related to the datastream

    """

    kind = kwargs.get('kind')
    comment = kwargs.get('comment')

    if not kind in ['A','Q','D','q','d']:
        kind = 'A'
    else:
        kind = kind.upper()
    if comment:
        logger.info("writeIYFV: Comments not yet supported")
        #identify next note and add comment at the send of the file
        pass
    else:
        note = 0

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
    meant = mean([tmin,tmax])
    if tmax-tmin < 365*0.9: # 90% of one year
        logger.error(" writeIYFV: Datastream does not cover at least 90% of one year")
        return False
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
    if isnan(meanx) or isnan(meany) or isnan(meanz):
        logger.warning(" writeIYFV: found more then 10% of NaN values - setting minimum requirement to 40% data recovery and change kind to I (incomplete)")
        meanx = datastream.mean('x',percentage=40)
        meany = datastream.mean('y',percentage=40)
        meanz = datastream.mean('z',percentage=40)
        kind = 'I'
        if isnan(meanx) or isnan(meany) or isnan(meanz):
            logger.error(" writeIYFV: less then 40% of data - aborting")
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
    newline = " {0}.500 {1:>3} {2:4.1f} {3:>3} {4:4.1f} {5:>6} {6:>6} {7:>6} {8:>6} {9:>6} {10:>1} {11:>4} {12:>3}\r\n".format(meanyear,decsep[0],float('0.'+str(decsep[1]))*60.,incsep[0],float('0.'+str(incsep[1]))*60.,int(np.round(datalist[3],0)),int(np.round(datalist[4],0)),int(np.round(datalist[5],0)),int(np.round(datalist[6],0)),int(np.round(datalist[7],0)), kind, comp.upper(), int(note))

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
        content.append("  COLATITUDE: {a:.2f}   LONGITUDE: {b:.2f} E   ELEVATION: {c:.0f} m\r\n".format(a=90.0-coordlist[0],b=coordlist[1],c=coordlist[2]))
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
        fh = open(filename, 'rU', newline='')
        for line in fh:
            content.append(line)

        fh.close()

        yearlst = []
        foundcomm = False

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
        import locale  # to get english month descriptions
        old_loc = locale.getlocale(locale.LC_TIME)
        try:
            locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
        except:
            pass
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
                        ti = datetime.strptime(block[0],"%d-%b-%y") + timedelta(minutes=90) + timedelta(minutes=180*i)
                        val = float(block[2+i])
                        array[0].append(date2num(ti))
                        if val < 990:
                            array[kcol].append(val)
                        else:
                            array[kcol].append(np.nan)
        locale.setlocale(locale.LC_TIME, old_loc)

    fh.close()
    headers['col-var1'] = 'K'
    headers['unit-col-var1'] = ''
    headers['DataFormat'] = 'MagPyK'

    array = [np.asarray(ar) for ar in array]
    stream = DataStream([LineStruct()], headers, np.asarray(array))

    # Eventually add trim
    return stream


def writeDKA(datastream, filename, **kwargs):
    pass
