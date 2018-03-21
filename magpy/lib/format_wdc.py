"""
MagPy
WDC (BGS version) input filter
Written by Roman Leonhardt October 2012
- contains test, read and write function for hour data
ToDo: Filter for minute data
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import division
from io import open

from magpy.stream import *
from datetime import timedelta

def isWDC(filename):
    """
    Checks whether a file is ASCII PMAG format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        if not temp[10:12] == "  " : # Minute format
            if not temp[27:34] == '       ': # Hour format
                return False
        if not len(temp.strip()) == 120: # Minute format
            if not len(temp) in [401,402]: # Hour format, strip is important to remove eventual \r\n sequences or \n
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
    nanval = float(NaN)
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
            #year = line[14:16]+ar # use test for old format with quiet and disturbed days
            #if int(year) < 1000: #old format
            #    year= '19'+ar
            #print co
            day = str(int(day)).zfill(2)
            #print code, year, mo, day, co
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
                        time=date2num(datetime.strptime(date,"%Y-%m-%dT%H:%M:%S"))
                        array[tind].append(time)
                        if not kind == '':
                            array[str1ind].append(kind)
                    if co=='i':
                        if not elem == "9999":
                            x = float(base) + float(elem)/600
                        else:
                            x = float(NaN)
                        complist[0] = co
                        array[xind].append(x)
                        headers['col-x'] = 'i'
                        headers['unit-col-x'] = 'deg'
                    if co=='d':
                        if not elem == "9999":
                            y = float(base) + float(elem)/600
                        else:
                            y = float(NaN)
                        complist[1] = co
                        array[yind].append(y)
                        headers['col-y'] = 'd'
                        headers['unit-col-y'] = 'deg'
                    if co in ['x','h']:
                        if not elem == "9999":
                            x = float(base)*100 + float(elem)
                        else:
                            x = float(NaN)
                        complist[0] = co
                        array[xind].append(x)
                        headers['col-x'] = co
                        headers['unit-col-x'] = 'nT'
                    if co=='y':
                        if not elem == "9999":
                            y = float(base)*100 + float(elem)
                        else:
                            y = float(NaN)
                        complist[1] = co
                        array[yind].append(y)
                        headers['col-y'] = 'y'
                        headers['unit-col-y'] = 'nT'
                    if co=='z':
                        if not elem == "9999":
                            z = float(base)*100 + float(elem)
                        else:
                            z = float(NaN)
                        complist[2] = co
                        array[zind].append(z)
                        headers['col-z'] = 'z'
                        headers['unit-col-z'] = 'nT'
                    if co=='f':
                        if not elem == "9999":
                            f = float(base)*100 + float(elem)
                        else:
                            f = float(NaN)
                        complist[3] = co
                        array[find].append(f)
                        headers['col-f'] = 'f'
                        headers['unit-col-f'] = 'nT'
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
                    array[tind].append(date2num(datetime.strptime(datestr+'T'+timestr, "%Y-%m-%dT%H:%M:%S")))
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
            print("Can not open WDC format")
            pass
        itest += 1
    fh.close()

    #    if minute:
    headers['DataComponents'] = "".join(complist).upper()
    headers['DataFormat'] = "WDC"
    headers['StationIAGAcode'] = code
    headers['StationID'] = code
    headers['SensorID'] = code.upper()+'hou_4_0001'
    array = np.asarray([np.asarray(el) for el in array])
    if oldformat:
        print ("readWDC: found old WDC format - assuming 20th century")

    stream = DataStream([LineStruct()], headers, array)

    return stream


def writeWDC(datastream, filename, **kwargs):
    """
    Writing WDC format data.
    """

    mode = kwargs.get('mode')
    #createlatex = kwargs.get('createlatex')

    keylst = datastream._get_key_headers()
    if not 'x' in keylst or not 'y' in keylst or not 'z' in keylst or not 'f' in keylst:
        print("formatWDC: writing WDC data requires x,y,z,f components")
        return False

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = pmRead(path_or_url=filename)
            datastream = mergeStreams(exst,datastream,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = pmRead(path_or_url=filename)
            datastream = mergeStreams(datastream,exst,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'append':
            myFile= open( filename, "ab" )
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= open( filename, "wb" )
    else:
        myFile= open( filename, "wb" )

    success = False
    # 1.) Test whether min or hourly data are used
    hourly, minute = False, False
    samplinginterval = datastream.get_sampling_period()
    if 0.98 < samplinginterval*24 < 1.02:
        hourly = True
    elif 0.98 < samplinginterval*24*60 < 1.02:
        minute = True
    else:
        print("Wrong sampling interval - please filter the data to minutes or hours")
        return success


    # 2.) Get Iaga code
    header = datastream.header
    iagacode = header.get('StationIAGAcode'," ").upper()

    # 3.)
    if hourly:
        #try:
        line, textable = [],[]
        rowx, rowy, rowz, rowf = '','','',''
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
            arb = '  '
            for key in KEYLIST:
                if key == 'time':
                    try:
                        year = datetime.strftime(num2date(timeval).replace(tzinfo=None), "%Y")
                        month = datetime.strftime(num2date(timeval).replace(tzinfo=None), "%m")
                        day = datetime.strftime(num2date(timeval).replace(tzinfo=None), "%d")
                        hour = datetime.strftime(num2date(timeval).replace(tzinfo=None), "%H")
                        ye = year[2:]
                        ar = year[:-2]
                    except:
                        rowx, rowy, rowz, rowf = '','','',''
                        pass
                elif key == 'x':
                    xname = iagacode + ye + month + header['col-x'].upper() + day + '  ' + arb + ar
                    if rowx[:16] == xname:
                        if not isnan(elemx):
                            xel.append(elemx)
                            xhourel.append(int(hour))
                    elif rowx == '':
                        rowx = xname
                        if not isnan(elemx):
                            xel = [elemx]
                            xhourel = [int(hour)]
                        else:
                            xel = []
                            xhourel = []
                    else:
                        if len(xel)<1:
                            xdailymean = int(9999)
                            xbase = int(9999)
                        else:
                            xmean = round(np.mean(xel),0)
                            xbase = xmean - 5000.0
                            xbase = int(xbase/100)
                            xdailymean = int(xmean - xbase*100)
                        rowx += "%4i" % xbase
                        count = 0
                        for i in range(24):
                            if len(xhourel) > 0 and count < len(xhourel) and xhourel[count] == i:
                                xval = int(xel[count] - xbase*100)
                                count = count+1
                            else:
                                xval = int(9999)
                                xdailymean = int(9999)
                            rowx+='%4i' % xval
                        eol = '\n'
                        rowx+='%4i%s' % (xdailymean,eol)
                        line.append(rowx)
                        rowx = xname
                        xel, xhourel = [], []
                        if not isnan(elemx):
                            xel.append(elemx)
                            xhourel.append(int(hour))
                elif key == 'y':
                    yname = iagacode + ye + month + header['col-y'].upper() + day + '  ' + arb  + ar
                    if rowy[:16] == yname:
                        if not isnan(elemy):
                            yel.append(elemy)
                            yhourel.append(int(hour))
                    elif rowy == '':
                        rowy = yname
                        if not isnan(elemy):
                            yel = [elemy]
                            yhourel = [int(hour)]
                        else:
                            yel = []
                            yhourel = []
                    else:
                        if len(yel)<1:
                            ydailymean = int(9999)
                            ybase = int(9999)
                        else:
                            ymean = round(np.mean(yel),0)
                            ybase = ymean - 5000.0
                            ybase = int(ybase/100)
                            ydailymean = int(ymean - ybase*100)
                        rowy += "%4i" % ybase
                        count = 0
                        for i in range(24):
                            if len(yhourel) > 0 and count < len(yhourel) and yhourel[count] == i:
                                yval = int(yel[count] - ybase*100)
                                count = count+1
                            else:
                                yval = int(9999)
                                ydailymean = int(9999)
                            rowy+='%4i' % yval
                        rowy+='%4i\n' % ydailymean
                        line.append(rowy)
                        rowy = yname
                        yel, yhourel = [], []
                        if not isnan(elemy):
                            yel.append(elemy)
                            yhourel.append(int(hour))
                elif key == 'z':
                    zname = iagacode + ye + month + header['col-z'].upper() + day + '  ' + arb  + ar
                    if rowz[:16] == zname:
                        if not isnan(elemz):
                            zel.append(elemz)
                            zhourel.append(int(hour))
                    elif rowz == '':
                        rowz = zname
                        if not isnan(elemz):
                            zel = [elemz]
                            zhourel = [int(hour)]
                        else:
                            zel = []
                            zhourel = []
                    else:
                        if len(zel)<1:
                            zdailymean = int(9999)
                            zbase = int(9999)
                        else:
                            zmean = round(np.mean(zel),0)
                            zbase = zmean - 5000.0
                            zbase = int(zbase/100)
                            zdailymean = int(zmean - zbase*100)
                        rowz += "%4i" % zbase
                        count = 0
                        for i in range(24):
                            if len(zhourel) > 0 and count < len(zhourel) and zhourel[count] == i:
                                zval = int(zel[count] - zbase*100)
                                count = count+1
                            else:
                                zval = int(9999)
                                zdailymean = int(9999)
                            rowz+='%4i' % zval
                        rowz+='%4i\n' % zdailymean
                        line.append(rowz)
                        rowz = zname
                        zel, zhourel = [], []
                        if not isnan(elemz):
                            zel.append(elemz)
                            zhourel.append(int(hour))
                elif key == 'f':
                    fname = iagacode + ye + month + header['col-f'].upper() + day + '  ' + arb  + ar
                    if rowf[:16] == fname:
                        if not isnan(elemf):
                            fel.append(elemf)
                            fhourel.append(int(hour))
                    elif rowf == '':
                        rowf = fname
                        if not isnan(elemf):
                            fel = [elemf]
                            fhourel = [int(hour)]
                        else:
                            fel = []
                            fhourel = []
                    else:
                        if len(fel)<1:
                            fdailymean = int(9999)
                            fbase = int(9999)
                        else:
                            fmean = round(np.mean(fel),0)
                            fbase = fmean - 5000.0
                            fbase = int(fbase/100)
                            fdailymean = int(fmean - fbase*100)
                        rowf += "%4i" % fbase
                        count = 0
                        for i in range(24):
                            if len(fhourel) > 0 and count < len(fhourel) and fhourel[count] == i:
                                fval = int(fel[count] - fbase*100)
                                count = count+1
                            else:
                                fval = int(9999)
                                fdailymean = int(9999)
                            rowf+='%4i' % fval
                        rowf+='%4i\n' % fdailymean
                        line.append(rowf)
                        rowf = fname
                        fel, fhourel = [], []
                        if not isnan(elemf):
                            fel.append(elemf)
                            fhourel.append(int(hour))
        # Finally save data of the last day, which dropped out by above procedure
        for comp in ['x','y','z','f']:
            if len(eval(comp+'el'))<1:
                exec(comp+'dailymean = int(9999)')
                exec(comp+'base = int(9999)')
            else:
                exec(comp+'mean = round(np.mean(' + comp +'el),0)')
                exec(comp+'base = ' + comp +'mean - 5000.0')
                exec(comp+'base = int(' + comp +'base/100)')
                exec(comp+'dailymean = int(' + comp +'mean - ' + comp +'base*100)')
            exec('row'+comp+'+= "%4i" % '+comp+'base')
            count = 0
            for i in range(24):
                if len(eval(comp+'hourel')) > 0 and count < len(eval(comp+'hourel')) and eval(comp+'hourel[count]') == i:
                    exec(comp+'val = int(' + comp +'el[count] - ' + comp + 'base*100)')
                    count = count+1
                else:
                    exec(comp+'val = int(9999)')
                    exec(comp+'dailymean = int(9999)')
                exec('row' + comp + '+="%4i" % ' + comp + 'val')
            eol = '\n'
            exec('row' + comp + '+="%4i%s" % (' + comp + 'dailymean,eol)')
            line.append(eval('row'+comp))
        line.sort()
        try:
            myFile.writelines( line )
            pass
        finally:
           myFile.close()
        #except IOError:
        #    pass
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
            timestamp = num2date(timeval).replace(tzinfo=None)
            minute = datetime.strftime(timestamp, "%M")
            if minute == '00':
                if len(min_dict['x']) != 360:
                    loggerlib.error('format_wdc: Error in formatting data for %s.' % datetime.strftime(timestamp,'%Y-%m-%d %H:%M'))
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
                if not isnan(value):
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
                loggerlib.warning("format_WDC: DataPublicationLevel as 1 or 3 are not supported by WDC. Assuming P (preliminary).")
                data_predef = 'P'
            elif str(predef) == '4' or predef[0].upper == 'D': # Definitive data
                data_predef = 'D'
            elif predef == ' ':
                loggerlib.warning("format_WDC: No DataPublicationLevel defined in header! Assuming P (preliminary).")
                data_predef = 'P'
        except:
            data_predef = 'P'
        try:
            iagacode = header.get('StationIAGAcode'," ").upper()
            if iagacode == ' ':
                loggerlib.warning("format_WDC: No StationIAGAcode defined in header!")
                iagacode = 'XXX'
        except:
            iagacode = 'WIC'
        try:
            station_lat = header.get('DataAcquisitionLatitude'," ")
            if station_lat == ' ':
                loggerlib.warning("format_WDC: No DataAcquisitionLatitude defined in header!")
                station_lat = 00.00
        except:
            station_lat = 00.00 # 47.93
        try:
            station_long = header.get('DataAcquisitionLongitude'," ")
            if station_long == ' ':
                loggerlib.warning("format_WDC: No DataAcquisitionLongitude defined in header!")
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

        # Write data in beliebiges Format:
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
                        loggerlib.warning('format_wdc: key It appears there is missing data for date %s. Replacing with 999999.'
                                % (datetime.strftime(day,'%Y-%m-%d ')+hour+':00'))
                        data = '999999'*60
                        hourly_mean = '999999'
                    except TypeError:
                        loggerlib.warning('format_wdc: type It appears there is missing data for date %s. Replacing with 999999.'
                                % (datetime.strftime(day,'%Y-%m-%d ')+hour+':00'))
                        data = '999999'*60
                        hourly_mean = '999999'
                    if len(data) != 360:
                        loggerlib.warning('format_wdc: It appears there is missing data for date %s. Replacing with 999999.'
                                % (datetime.strftime(day,'%Y-%m-%d ')+hour+':00'))
                        data = '999999'*60
                        hourly_mean = '999999'
                    line = preamble + data + hourly_mean + '\r\n'
                    myFile.write(line.encode('utf-8'))
            day = day + timedelta(days=1)

        success = True

    else:
        logging.warning("Could not save WDC data. Please provide hour or minute data")

    return success
