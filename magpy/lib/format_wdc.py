"""
MagPy
WDC (BGS version) input filter
Written by Roman Leonhardt October 2012
- contains test, read and write function for hour data
"""
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import DataStream, read, merge_streams, subtract_streams, magpyversion
from datetime import datetime, timedelta, timezone
import os
import numpy as np
from magpy.core.methods import testtime, extract_date_from_string
import logging
logger = logging.getLogger(__name__)
KEYLIST = DataStream().KEYLIST

def isWDC(filename):
    """
    Checks whether a file is ASCII PMAG format.
    """
    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
    except:
        return False
    try:
        if not temp[10:12] in ["  ","RR"] : # Hour format
            if not temp[27:34] == '       ': # Minute format
                return False
        if not len(temp.strip()) == 120: # Hour format
            if not len(temp) in [401,402]: # Minute format, strip is important to remove eventual \r\n sequences or \n
                return False
    except:
        return False
    return True


def readWDC(filename, headonly=False, **kwargs):
    """
    Reading WDC format data.
    Hourly looks like:
    WIK1209F01  20 4355059506150605058505950585053504750395034503850385043504850515049505150545056505750575057506050575052
    WIK1209F02  20 4355056505950565055505750515049504150395036503750405041504550565059506250605058506750565053505250495052
    WIK1209F03  20 4355047505050535054505550525045503950315021502050315053505250605064506750685071506550615067505950625052
    Minute looks like:
     42070 15865151125Z21WIC 0P       243.29243.29 243.3243.26243.24243.24243.29243.27243.27 243.3243.33243.34243.22243.18243.17243.18243.11243.15243.11243.04243.11243.12243.12243.17243.17243.21243.15243.14243.05243.12243.18243.15243.11243.13243.13243.16243.08 243.1243.09243.08243.09243.11243.09243.15243.13243.17243.09243.12243.21243.14243.15243.19243.15243.11243.06243.06243.07243.07243.04242.97243.16
     42070 15865151125Z22WIC 0P       243.04243.01243.06242.99242.94 243.0243.01243.01242.93 243.0242.97243.03242.96242.92242.95243.03243.08242.98242.93242.84242.89242.87242.84242.78242.81242.84242.79242.78242.77242.78242.76242.73242.76242.82242.81242.89242.93242.94242.86242.96242.94242.98242.96242.95242.94242.98242.94242.89242.84 242.9 243.0243.02 243.0243.01242.98242.99242.93242.86242.88242.85242.92
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True
    KEYLIST = DataStream().KEYLIST

    fh = open(filename, 'rt')

    stream = DataStream()
    # Check whether header information is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    # read file and split text into channels
    li,ld,lh,lx,ly,lz,lf,lt = [],[],[],[],[],[],[],[]
    array = [[] for key in KEYLIST]
    tind,xind,yind,zind,find = KEYLIST.index('time'), KEYLIST.index('x'), KEYLIST.index('y'), KEYLIST.index('z'), KEYLIST.index('f')
    str1ind = KEYLIST.index('str1')
    code = ''
    itest = 0
    minute = False
    complist = ['','','','']
    nanval = np.nan
    oldformat = False
    kind = '' # To store Q, D in all data format (Quiet, Disturbed)
    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif headonly:
            # skip data for option headonly
            continue
        elif len(line.strip()) == 120: # hour file
            # skip data for option headonly
            code = line[:3]
            ar = line[3:5]
            mo = line[5:7]
            co = line[7:8].lower()
            if itest == 0:
                firstco = co
            day = line[8:10]
            try:
                cent = int(line[14:16])
                if not cent > 10:
                    x=1/0
            except:
                oldformat = True
                kind = line[14:16] # use test for old format with quiet and disturbed days
                cent = '19'
                # eventually use kind to load only quiet days/disturbed days from old WDC data
            year = str(cent)+ar # use test for old format with quiet and disturbed days
            day = str(int(day)).zfill(2)
            #print (code, year, mo, day, co)
            cf= lambda s,p: [ s[i:i+p] for i in range(0,len(s),p) ]
            dailymean = line[116:120]
            base = line[16:20]
            lst = cf(line[20:116],4)
            for i, elem in enumerate(lst):
                try:
                    hour = "%i" % i
                    date = year + '-' + mo + '-' + day + 'T' + hour + ':30:00'
                    #print date
                    if co == firstco:
                        time=datetime.strptime(date,"%Y-%m-%dT%H:%M:%S")
                        array[tind].append(time)
                        if not kind == '':
                            array[str1ind].append(kind)
                    if co=='i':
                        if not elem == "9999":
                            x = float(base) + float(elem)/600
                        else:
                            x = np.nan
                        complist[0] = co
                        array[xind].append(x)
                        headers['col-x'] = 'i'
                        headers['unit-col-x'] = 'deg'
                    if co=='d':
                        if not elem == "9999":
                            y = float(base) + float(elem)/600
                        else:
                            y = np.nan
                        complist[1] = co
                        array[yind].append(y)
                        headers['col-y'] = 'd'
                        headers['unit-col-y'] = 'deg'
                    if co in ['x','h']:
                        if not elem == "9999":
                            x = float(base)*100 + float(elem)
                        else:
                            x = np.nan
                        complist[0] = co
                        array[xind].append(x)
                        headers['col-x'] = co
                        headers['unit-col-x'] = 'nT'
                    if co=='y':
                        if not elem == "9999":
                            y = float(base)*100 + float(elem)
                        else:
                            y = np.nan
                        complist[1] = co
                        array[yind].append(y)
                        headers['col-y'] = 'y'
                        headers['unit-col-y'] = 'nT'
                    if co=='z':
                        if not elem == "9999":
                            z = float(base)*100 + float(elem)
                        else:
                            z = np.nan
                        complist[2] = co
                        array[zind].append(z)
                        headers['col-z'] = 'z'
                        headers['unit-col-z'] = 'nT'
                    if co=='f':
                        if not elem == "9999":
                            f = float(base)*100 + float(elem)
                        else:
                            f = np.nan
                        complist[3] = co
                        array[find].append(f)
                        headers['col-f'] = 'f'
                        headers['unit-col-f'] = 'nT'
                    if co == '*':
                        if not elem == "9999":
                            dst = float(elem)
                        else:
                            dst = np.nan
                        complist[3] = co
                        array[KEYLIST.index('var1')].append(dst)
                        headers['col-var1'] = "DST"
                        headers['unit-col-var1'] = 'nT'
                except:
                    pass
        elif len(line) in [401,402]: # minute file
            # skip data for option headonly
            stream = DataStream([],{},[[] for key in KEYLIST])
            minute = True
            headers['DataAcquisitionLatitude'] = float(line[:6])/1000.
            headers['DataAcquisitionLongitude'] = float(line[6:12])/1000.
            ar = line[12:14]
            mo = line[14:16]
            day = line[16:18]
            var = line[18:19].lower()
            if itest == 0:
                firstvar = var
            hr = line[19:21]
            code = line[21:24]
            #headers['StationIAGAcode'] = code
            yestr = line[25:26]
            if int(yestr) in [5,6,7,8,9]:
                ye = '1'+yestr
            elif int(yestr) in [0,1,2,3]:
                ye = '2'+yestr
            year = ye+ar
            datestr = year+'-'+mo+'-'+day

            for i in range(0, 60*6, 6):
                if var == firstvar:
                    timestr = hr+':%02d:00' % (i/6)
                    array[tind].append(datetime.strptime(datestr+'T'+timestr, "%Y-%m-%dT%H:%M:%S"))
                val = float(line[34+i:40+i])
                if var in ['x','i','h']:
                    complist[0] = var
                    headers['col-x'] = var
                    if val >= 999999.:
                        array[xind].append(nanval)
                    else:
                        # if val == i !!! check float(base) + float(elem)/600
                        if var == 'i':
                            headers['unit-col-x'] = 'deg'
                        else:
                            headers['unit-col-x'] = 'nT'
                        array[xind].append(val)
                if var in ['y','d','e']:
                    complist[1] = var
                    if val >= 999999.:
                        array[yind].append(nanval)
                    else:
                        # if val == d !!! check float(base) + float(elem)/600
                        headers['col-y'] = var
                        if var == 'd':
                            headers['unit-col-y'] = 'deg'
                        else:
                            headers['unit-col-y'] = 'nT'
                        array[yind].append(val)
                if var == 'z':
                    complist[2] = var
                    if val >= 999999.:
                        array[zind].append(nanval)
                    else:
                        headers['col-z'] = 'z'
                        headers['unit-col-z'] = 'nT'
                        array[zind].append(val)
                if var == 'f':
                    complist[3] = var
                    if val >= 999999.:
                        array[find].append(nanval)
                    else:
                        headers['col-f'] = 'f'
                        headers['unit-col-f'] = 'nT'
                        array[find].append(val)
                #if var == 'h':
                    #if val >= 999999.:
                    #    array[xind].append(nanval) # ??
                    #else:
                    #    array[xind].append(val)
            lasttimestr = timestr

        else:
            print(" WDC read - skipping line {}".format(line))
            pass
        itest += 1
    fh.close()

    #    if minute:
    headers['DataComponents'] = "".join(complist).upper()
    headers['DataFormat'] = "WDC"
    headers['StationIAGAcode'] = code
    headers['StationID'] = code
    headers['SensorID'] = code.upper()+'hou_4_0001'
    array = [np.asarray(el, dtype=object) for el in array]

    if oldformat:
        print ("readWDC: found old WDC format - assuming 20th century")

    stream = DataStream(header=headers, ndarray=np.asarray(array,dtype=object))

    return stream


def writeWDC(datastream, filename, **kwargs):
    """
    Writing WDC format data.
    """

    mode = kwargs.get('mode')
    #createlatex = kwargs.get('createlatex')
    KEYLIST = DataStream().KEYLIST

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


    def OpenFile(filename, mode='w'):
        f = open(filename, mode, newline='')
        return f

    keylst = datastream._get_key_headers()
    if not 'x' in keylst or not 'y' in keylst or not 'z' in keylst or not 'f' in keylst:
        print("formatWDC: writing WDC data requires x,y,z,f components")
        return False

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = merge_streams(exst,datastream,extend=True)
            myFile= OpenFile(filename)
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = merge_streams(datastream,exst,extend=True)
            myFile= OpenFile(filename)
        elif mode == 'append':
            myFile= OpenFile(filename,mode='a')
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= OpenFile(filename)
    else:
        myFile= OpenFile(filename)

    success = False
    # 1.) Test whether min or hourly data are used
    hourly, minute = False, False
    samplinginterval = datastream.get_sampling_period()
    if 0.98 < samplinginterval/3600. < 1.02:
        hourly = True
    elif 0.98 < samplinginterval/60. < 1.02:
        minute = True
    else:
        print("Wrong sampling interval - please filter the data to minutes or hours")
        return success


    # 2.) Get Iaga code
    header = datastream.header
    iagacode = header.get('StationIAGAcode','XXX').upper()

    # 3.) Create component objects:

    class x:
        dailymean = int(9999)
        base = int(9999)
        mean = int(9999)
        el = []
        hourel = []
        ind = 0
        elem = int(9999)
        name = ''
        row = ''
    class y:
        dailymean = int(9999)
        base = int(9999)
        mean = int(9999)
        el = []
        hourel = []
        ind = 0
        elem = int(9999)
        name = ''
        row = ''
    class z:
        dailymean = int(9999)
        base = int(9999)
        mean = int(9999)
        el = []
        hourel = []
        ind = 0
        elem = int(9999)
        name = ''
        row = ''
    class f:
        dailymean = int(9999)
        base = int(9999)
        mean = int(9999)
        el = []
        hourel = []
        ind = 0
        elem = int(9999)
        name = ''
        row = ''
    

    # 4.)
    if hourly:
        #try:
        line, textable = [],[]
        x.row, y.row, z.row, f.row = '','','',''
        latexrowx = ''

        ndtype = False
        if len(datastream.ndarray[0]) > 0:
            ndtype = True

        fulllength = datastream.length()[0]

        xind = KEYLIST.index('x')
        yind = KEYLIST.index('y')
        zind = KEYLIST.index('z')
        find = KEYLIST.index('f')
        for i in range(fulllength):
            if not ndtype:
                elem = datastream[i]
                x.elem = elem.x
                y.elem = elem.y
                z.elem = elem.z
                f.elem = elem.f
                timeval = elem.time
            else:
                x.elem = datastream.ndarray[xind][i]
                y.elem = datastream.ndarray[yind][i]
                z.elem = datastream.ndarray[zind][i]
                f.elem = datastream.ndarray[find][i]
                timeval = datastream.ndarray[0][i]
            arb = '  '
            for key in KEYLIST:
                if key == 'time':
                    try:
                        year = datetime.strftime(timeval.replace(tzinfo=None), "%Y")
                        month = datetime.strftime(timeval.replace(tzinfo=None), "%m")
                        day = datetime.strftime(timeval.replace(tzinfo=None), "%d")
                        hour = datetime.strftime(timeval.replace(tzinfo=None), "%H")
                        ye = year[2:]
                        ar = year[:-2]
                    except:
                        x.row, y.row, z.row, f.row = '','','',''
                        pass
                elif key in ['x','y','z','f']:
                    #print ("Dealing with key {}".format(key))
                    if key == 'x':
                        cl = x
                    elif key == 'y':
                        cl = y
                    elif key == 'z':
                        cl = z
                    elif key == 'f':
                        cl = f
                    cl.name = "{}{}{}{}{}  {}{}".format(iagacode,ye,month,header.get('col-{}'.format(key),key).upper()[:1],day,arb,ar)
                    if cl.row[:16] == cl.name:
                        if not np.isnan(cl.elem):
                            cl.el.append(cl.elem)
                            cl.hourel.append(int(hour))
                    elif cl.row == '':
                        cl.row = cl.name
                        if not np.isnan(cl.elem):
                            cl.el = [cl.elem]
                            cl.hourel = [int(hour)]
                        else:
                            cl.el = []
                            cl.hourel = []
                    else:
                        if len(cl.el)<1:
                            cl.dailymean = int(9999)
                            cl.base = int(9999)
                        else:
                            cl.mean = np.round(np.mean(cl.el),0)
                            cl.base = cl.mean - 5000.0
                            cl.base = int(cl.base/100)
                            cl.dailymean = int(cl.mean - cl.base*100)
                        cl.row += "%4i" % cl.base
                        count = 0
                        for i in range(24):
                            if len(cl.hourel) > 0 and count < len(cl.hourel) and cl.hourel[count] == i:
                                cl.val = int(np.round(cl.el[count],0) - cl.base*100)
                                count = count+1
                            else:
                                cl.val = int(9999)
                                cl.dailymean = int(9999)
                            cl.row+='%4i' % cl.val
                        eol = '\n'
                        cl.row+='%4i%s' % (cl.dailymean,eol)
                        line.append(cl.row)
                        cl.row = cl.name
                        cl.el, cl.hourel = [], []
                        if not np.isnan(cl.elem):
                            cl.el.append(cl.elem)
                            cl.hourel.append(int(hour))

        # Finally save data of the last day, which dropped out by above procedure
        # TODO Replace all eval methods with better attribute definitions 
        for comp in [x,y,z,f]:
            if len(comp.el)<1:
                comp.dailymean = int(9999)
                comp.base = int(9999)
            else:
                comp.mean=np.round(np.mean(comp.el),0)
                comp.base = comp.mean - 5000.0
                comp.base = int(comp.base/100)
                comp.dailymean = int(comp.mean - comp.base*100)
            comp.row += "%4i" % comp.base
            count = 0
            for i in range(24):
                if len(comp.hourel) > 0 and count < len(comp.hourel) and comp.hourel[count] == i:
                    comp.val = int(comp.el[count] - comp.base*100)
                    count = count+1
                else:
                    comp.val = int(9999)
                    comp.dailymean = int(9999)
                comp.row +="%4i" % comp.val
            eol = '\n'
            comp.row +="%4i%s" % (comp.dailymean,eol)
            line.append(comp.row)
        line.sort()
        try:
            myFile.writelines( line )
            pass
        finally:
           myFile.close()
        success = True

    # 3.)
    elif minute:
        '''
        COLUMNS   FORMAT   DESCRIPTION
        
        1-6       I6       Observatory's North Polar distance (header['DataAcquisitionLatitude']).
                           the north geographic pole in thousandths
                           of a degree. Decimal point is implied between positions 3
                           and 4.
        7-12      I6       Observatory's Geographic longitude (header['DataAcquisitionLongitude']).
                           of a degree. Decimal point is implied between positions 9
                           and 10.
        13-14     I2       Year. Last 2 digits, 1996 = 96. See also column 26.
        15-16     I2       Month (01-12)
        17-18     I2       Day of month (01-31)
        19        Al       Element (D,I,H,X,Y,Z, or F)
        20-21     I2       Hour of day (00-23)
        22-24     A3       Observatory 3-letter code (header['StationIAGAcode']).
        25        A1       Arbitrary.
        26        I1       Century digit.
                           Year = 2014, Century digit = 0.
                           Year = 1889, Century digit = 8.
                           Year = 1996, Century digit = 9 or 'SPACE' for backwards
                           compatibility.
        27        A1       Preliminary or Definitive data (given by stream header['DataRating']).
                           Preliminary = P , Definitive = D
        28-34     A7       Blanks
        35-394    60I6     60 6-digit 1-minute values for the given element for that
                           data hour.
                           The values are in tenth-minutes for D and I, and in
                           nanoTeslas for the intensity elements.
        395-400   I6       Hourly Mean.
                           The average of the preceeding 60 1-minute values.
        401-402            Record end marker.
                           Two chars 'cr'= 13 and 'nl'= 10.
        '''

        # http://www.wdc.bgs.ac.uk/catalog/format.html
        min_dict, hour_dict, day_dict = {}, {}, {}
        write_KEYLIST = ['x','y','z','f']
        for key in write_KEYLIST:
            min_dict[key] = ''
        day, hour = '0', '0'
        year = '2024'

        for day in range(1,32):         # TODO: make this exact for given month
            day_dict[str(day).zfill(2)] = ''

        # Read data into dictionaries for ease of writing:
        ndtype = False
        if len(datastream.ndarray[0]) > 0:
            ndtype = True

        fulllength = datastream.length()[0]

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
            timestamp = timeval.replace(tzinfo=None)
            minute = datetime.strftime(timestamp, "%M")
            if minute == '00':
                if len(min_dict['x']) != 360:
                    logger.error('format_wdc: Error in formatting data for %s.' % datetime.strftime(timestamp,'%Y-%m-%d %H:%M'))
                minutedata = dict(min_dict)
                hour_dict[hour] = minutedata
                for key in write_KEYLIST:
                    min_dict[key] = ''
            hour = datetime.strftime(timestamp, "%H")
            if hour == '00' and minute == '00':
                hourdata = dict(hour_dict)
                day_dict[day] = hourdata
                for hour in range(0,24):
                    hour_dict[str(hour).zfill(2)] = ''
            year = datetime.strftime(timestamp, "%Y")
            day = datetime.strftime(timestamp, "%d")
            for key in write_KEYLIST:
                #exec('value = elem'+key)
                value = eval('elem'+key)
                if not np.isnan(value):
                    if len(str(value)) > 6:
                        if value >= 10000:
                            value = int(round(value))
                        elif value >= 1000:
                            value = round(value,1)
                        else:
                            value = round(value,2)
                    val_f = str(value).rjust(6)
                else:
                    val_f = '999999'
                min_dict[key] = min_dict[key] + val_f

        # Save last day of data, which is left out in loop:
        minutedata = dict(min_dict)
        hour_dict[hour] = minutedata
        hourdata = dict(hour_dict)
        day_dict[day] = hourdata

        # Find data for preamble tags at line beginning:
        ar = year[2:]
        ye = year[:-2]
        century = ye[1:]
        month = datetime.strftime(timestamp, "%m")

        header = datastream.header
        try:
            predef = header.get('DataPublicationLevel'," ")
            if str(predef) == '2' or predef[0].upper == 'P': # Preliminary data
                data_predef = 'P'
            elif str(predef) in ['1','3']: # Raw (1) or quasi-definitive (3) data
                logger.warning("format_WDC: DataPublicationLevel as 1 or 3 are not supported by WDC. Assuming P (preliminary).")
                data_predef = 'P'
            elif str(predef) == '4' or predef[0].upper == 'D': # Definitive data
                data_predef = 'D'
            elif predef == ' ':
                logger.warning("format_WDC: No DataPublicationLevel defined in header! Assuming P (preliminary).")
                data_predef = 'P'
        except:
            data_predef = 'P'
        try:
            iagacode = header.get('StationIAGAcode'," ").upper()
            if iagacode == ' ':
                logger.warning("format_WDC: No StationIAGAcode defined in header!")
                iagacode = 'XXX'
        except:
            iagacode = 'WIC'
        try:
            station_lat = header.get('DataAcquisitionLatitude'," ")
            if station_lat == ' ':
                logger.warning("format_WDC: No DataAcquisitionLatitude defined in header!")
                station_lat = 00.00
        except:
            station_lat = 00.00 # 47.93
        try:
            station_long = header.get('DataAcquisitionLongitude'," ")
            if station_long == ' ':
                logger.warning("format_WDC: No DataAcquisitionLongitude defined in header!")
                station_long = 00.00
        except:
            station_long = 00.00 # 15.865
        northpolardistance = int(round((90-float(station_lat))*1000))
        longitude = int(round(float(station_long) * 1000))

        pre_geopos = str(northpolardistance).rjust(6) + str(longitude).rjust(6)
        day = datetime(int(year),int(month),1)

        # TODO write routine to check for missing data and fill in spaces?

        # Find time range:
        if int(month) < 12:
            nextmonth = datetime(int(year),int(month)+1,1)
        else:
            nextmonth = datetime(int(year)+1,1,1)

        # Write data in any format:
        while day < nextmonth:
            for key in write_KEYLIST:
                for hour_ in range(0,24):
                    hour = str(hour_).zfill(2)
                    dom = datetime.strftime(day,"%d")
                    pre_date = ar + month + datetime.strftime(day,'%d')
                    pre_rest = key.upper() + hour + iagacode + ' ' + century + data_predef + 7*' '
                    preamble = pre_geopos + pre_date + pre_rest

                    # Get data + calculate mean:
                    try:
                        data = day_dict[dom][hour][key]
                        total = 0.
                        try:
                            for i in range(0,60):
                                total = total + float(data[i*6:(i+1)*6])
                            hourly_mean = total/60.
                        except:
                            hourly_mean = 999999
                        if len(str(hourly_mean)) > 6:
                            if hourly_mean >= 10000:
                                hourly_mean = int(round(hourly_mean))
                            elif hourly_mean >= 1000:
                                hourly_mean = round(hourly_mean,1)
                            else:
                                hourly_mean = round(hourly_mean,2)
                        hourly_mean = str(hourly_mean).rjust(6)
                    except KeyError:
                        logger.warning('format_wdc: key It appears there is missing data for date %s. Replacing with 999999.'
                                % (datetime.strftime(day,'%Y-%m-%d ')+hour+':00'))
                        data = '999999'*60
                        hourly_mean = '999999'
                    except TypeError:
                        logger.warning('format_wdc: type It appears there is missing data for date %s. Replacing with 999999.'
                                % (datetime.strftime(day,'%Y-%m-%d ')+hour+':00'))
                        data = '999999'*60
                        hourly_mean = '999999'
                    if len(data) != 360:
                        logger.warning('format_wdc: It appears there is missing data for date %s. Replacing with 999999.'
                                % (datetime.strftime(day,'%Y-%m-%d ')+hour+':00'))
                        data = '999999'*60
                        hourly_mean = '999999'
                    line = preamble + data + hourly_mean + '\r\n'
                    myFile.write(line)
            day = day + timedelta(days=1)

        success = True

    else:
        logging.warning("Could not save WDC data. Please provide hour or minute data")

    return success

if __name__ == '__main__':

    from scipy import signal
    import subprocess
    print()
    print("----------------------------------------------------------")
    print("TESTING: WDC FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE WDC LIBRARY.")
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
        teststream.header['StationID'] = 'XXX'
        teststream.header['StationIAGAcode'] = 'XXX'
        return teststream

    teststream = create_minteststream(addnan=False)
    #teststream = teststream.trim('2022-11-22','2022-11-23')
    #print (len(teststream))

    errors = {}
    successes = {}
    testrun = 'MAGPYTESTFILE'
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)

    while True:
        testset = 'WDC-minute'
        try:
            filename = os.path.join('/tmp','{}_{}.dat'.format(testrun, datetime.strftime(teststream.start(),'%b%d%y')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            succ1 = writeWDC(teststream, filename)
            succ2 = isWDC(filename)
            dat = readWDC(filename)
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
        testset = 'WDC-hour'
        try:
            teststream = teststream.filter(filter_type='flat',filter_width=timedelta(hours=1),resampleoffset=timedelta(minutes=30))
            filename = os.path.join('/tmp','{}_{}.dat'.format(testrun, datetime.strftime(teststream.start(),'%b%d%y')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            succ1 = writeWDC(teststream, filename)
            succ2 = isWDC(filename)
            dat = readWDC(filename)
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
            if np.abs(xm) > 0.05 or np.abs(ym) > 0.05 or np.abs(zm) > 0.05 or np.abs(fm) > 0.05:
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
