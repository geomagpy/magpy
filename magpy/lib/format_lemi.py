'''
Path:                   magpy.lib.format_lemi
Part of package:        stream (read/write)
Type:                   Input filter, part of read library

PURPOSE:
        Auxiliary input filter for Lemi data.

CONTAINS:
        isLEMIBIN:      (Func) Checks if file is LEMI format binary file.
        readLEMIBIN:    (Func) Reads current LEMI data format binary files.
        isLEMIBIN1:     (Func) Checks if file is LEMI format data file.
        readLEMIBIN1:   (Func) Reads outdated LEMI data format binary files.
        isLEMIHF:       (Func) Checks if file is LEMI format data file.
        readLEMIHF:     (Func) Reads outdated LEMI data format text files.

DEPENDENCIES:
        None.

CALLED BY:
        magpy.lib.magpy_formats
'''
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

from magpy.stream import *

def h2d(x):
    '''
    Hexadecimal to decimal (for format LEMIBIN2)
    Because the binary for dates is in binary-decimal, not just binary.
    '''
    y = int(x/16)*10 + x%16
    return y

def isLEMIHF(filename):
    '''
    Checks whether a file is ASCII Lemi txt file format.
    '''
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        elem = temp.split()
        if len(elem) == 13:
            try:
                testtime = datetime.strptime(elem[0]+'-'+elem[1]+'-'+elem[2],'%Y-%m-%d')
            except:
                return False
        else:
            return False
    except:
        return False

    #loggerlib.info("format_lemi: Found Lemi 10Hz ascii file %s." % filename)
    return True


def isLEMIBIN1(filename):
    '''
    Checks whether a file is Binary Lemi file format.
    '''
    try:
        temp = open(filename, 'rb').read(32)
        data= struct.unpack('<4cb6B11Bcbbhhhb', temp)
    except:
        return False

    try:
        if not data[0].decode('ascii') == 'L':
            return False
        if not data[22].decode('ascii') in (['A','P']):
            return false
    except:
        return False

    #loggerlib.info("format_lemi: Found Lemi 10Hz binary file %s." % filename)
    return True


def isLEMIBIN(filename):
    '''
    Checks whether a file is Binary Lemi025 file format. (2nd format. Used at Conrad Observatory.)
    '''
    try:
        temp = open(filename, 'rb').read(169)
        if temp[:20].decode('ascii').startswith("LemiBin"):
            return True
        else:
            data= struct.unpack('<4cb6B8hb30f3BcB6hL', temp)
    except:
        return False

    try:
        if not data[0].decode('ascii') == 'L':
            return False
        if not data[53].decode('ascii') in (['A','P']):
            return false
    except:
        return False

    print ("Reading a Lemi Binary format")
    #loggerlib.info("format_lemi: Found Lemi 10Hz binary file %s." % filename)
    return True


def readLEMIHF(filename, headonly=False, **kwargs):
    '''
    Reading IAGA2002 LEMI format data.
    '''

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    array = [[] for key in KEYLIST]
    # Check whether header information is already present
    headers = {}
    data = []
    key = None

    xpos = KEYLIST.index('x')
    ypos = KEYLIST.index('y')
    zpos = KEYLIST.index('z')
    t1pos = KEYLIST.index('t1')
    t2pos = KEYLIST.index('t2')
    var2pos = KEYLIST.index('var2')
    var3pos = KEYLIST.index('var3')

    # get day from filename (platform independent)
    # --------------------------------------
    splitpath = os.path.split(filename)
    tmpdaystring = splitpath[1].split('.')[0]
    daystring = re.findall(r'\d+',tmpdaystring)[0]
    if len(daystring) >  8:
        daystring = daystring[:8]
    try:
        day = datetime.strftime(datetime.strptime(daystring, '%Y%m%d'),'%Y-%m-%d')
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        loggerlib.warning("readLEMIHF: Wrong dateformat in Filename %s." % filename)
        pass

    if getfile:
        loggerlib.info('readLEMIHF: Reading %s...' % (filename))
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif headonly:
                # skip data for option headonly
                continue
            else:
                #row = LineStruct()
                elem = line.split()
                tim = date2num(datetime.strptime(elem[0]+'-'+elem[1]+'-'+elem[2]+'T'+elem[3]+':'+elem[4]+':'+elem[5],'%Y-%m-%dT%H:%M:%S.%f'))
                #row.time = tim
                array[0].append(tim)
                array[xpos].append(float(elem[6]))
                array[ypos].append(float(elem[7]))
                array[zpos].append(float(elem[8]))
                if len(elem) > 8:
                    try:
                        array[t1pos].append(float(elem[9]))
                        array[t2pos].append(float(elem[10]))
                        array[var2pos].append(float(elem[11]))
                        array[var3pos].append(float(elem[12]))
                    except:
                        pass
        headers['col-x'] = 'x'
        headers['unit-col-x'] = 'nT'
        headers['col-y'] = 'y'
        headers['unit-col-y'] = 'nT'
        headers['col-z'] = 'z'
        headers['unit-col-z'] = 'nT'
        if len(elem) > 8:
            headers['col-t1'] = 'Tsens'
            headers['unit-col-t1'] = 'C'
            headers['col-t2'] = 'Tel'
            headers['unit-col-t2'] = 'C'
            headers['col-var2'] = 'VCC'
            headers['unit-col-var2'] = 'V'
            headers['col-var3'] = 'Index'
            headers['unit-col-var3'] = ''
    else:
        headers = stream.header
        stream =[]

    fh.close()

    for idx,ar in enumerate(array):
        if len(ar) > 0:
            array[idx] = np.asarray(array[idx])

    headers['DataFormat'] = 'Lviv-LEMI-Buffer'

    return DataStream([LineStruct()], headers, np.asarray(array).astype(object))


def readLEMIBIN(filename, headonly=False, **kwargs):
    '''
    Function for reading current data format of LEMI data.

    KWARGS:
        tenHz:          (bool) to use 10Hz data
        timeshift:      (float) providing a time shift, which is added to PC time column (usually NTP)

    COMPLETE DATA STRUCTURE:'<4cb6B8hb30f3BcBcc5hL'
     --TAG:            data[0:4]                # L025
     --TIME (LEMI):    2000+h2d(data[5]),h2d(data[6]),h2d(data[7]),h2d(data[8]),h2d(data[9]),h2d(data[10])
     --T (sensor):     data[11]/100.
     --T (electr.):    data[12]/100.
     --BIAS:           data[13],data[14],data[15]
     --BIAS FIELD:     data[16]/400.,data[17]/400.,data[18]/400.
     --(EMPTY)         data[19]
     --DATA1:          data[20]*1000.,data[21]*1000.,data[22]*1000.
     --DATA2:          data[23]*1000.,data[24]*1000.,data[25]*1000.
     --DATA3:          data[26]*1000.,data[27]*1000.,data[28]*1000.
     --DATA4:          data[29]*1000.,data[30]*1000.,data[31]*1000.
     --DATA5:          data[32]*1000.,data[33]*1000.,data[34]*1000.
     --DATA6:          data[35]*1000.,data[36]*1000.,data[37]*1000.
     --DATA7:          data[38]*1000.,data[39]*1000.,data[40]*1000.
     --DATA8:          data[41]*1000.,data[42]*1000.,data[43]*1000.
     --DATA9:          data[44]*1000.,data[45]*1000.,data[46]*1000.
     --DATA10:         data[47]*1000.,data[48]*1000.,data[49]*1000.
     --MODE:           data[50]         # Mode: 1, 2 or 3
     --FLASH % FREE:   data[51]
     --BATTERY (V):    data[52]
     --GPS STATUS:     data[53]         # A (active) or P (passive)
     --(EMPTY)         data[54]
     --TIME (PC):      2000+data[55],data[56],data[57],data[58],data[59],data[60],data[61]
    '''

    # Reading Lemi025 Binary format data.
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debug = kwargs.get('debug')
    getfile = True

    timeshift = kwargs.get('timeshift')
    gpstime = kwargs.get('gpstime')

    #print "Reading LEMIBIN -- careful --- check time shifts and used time column (used during acquisition and read????)"
    timediff = []

    ## Moved the following into acquisition
    if not timeshift:
        timeshift = 0.0 # milliseconds and time delay (PC-GPS) are already considered in acquisition

    if not gpstime:
        gpstime = False # if true then PC time will be saved to the sectime column and gps time will occupy the time column

    # Check whether its the new (with ntp time) or old (without ntp) format
    temp = open(filename, 'rb').read(169)

    if temp[:60].decode('ascii').startswith("LemiBin"):
        # current format
        sensorid = temp[:60].split()[1]
        dataheader = True
        lemiformat = "current"
        packcode = '<4cb6B8hb30f3BcB6hL'
        linelength = 169
        stime = True
        if debug:
            print ("SensorID", sensorid)
    else:
        # old format
        data = struct.unpack('<4cb6B8hb30f3BcBcc5hL', temp)
        if data[55] == 'L':
            dataheader = False
            lemiformat = "out-dated"
            packcode = '<4cb6B8hb30f3BcB'
            linelength = 153
            stime = False
        elif data[0] == 'L' and data[55] != 'L':
            dataheader = False
            lemiformat = "current (without header)"
            packcode = '<4cb6B8hb30f3BcB6hL'
            linelength = 169
            stime = True
        else:
            loggerlib.error("readLEMIBIN: Something, somewhere, went very wrong.")

    fh = open(filename, 'rb')

    stream = DataStream([],{})
    array = [[] for key in KEYLIST]

    data = []
    key = None

    theday = extractDateFromString(filename)
    try:
        if starttime:
            if not theday[-1] >= datetime.date(stream._testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(stream._testtime(endtime)):
                getfile = False
    except:
        getfile = True

    if getfile:
        loggerlib.info("read: %s Format: Binary LEMI format (%s)." % (filename,lemiformat))

        if dataheader == True:
            junkheader = fh.readline()
            stream.header['SensorID'] = sensorid

        loggerlib.info('readLEMIBIN: Reading %s...' % (filename))
        stream.header['col-x'] = 'x'
        stream.header['unit-col-x'] = 'nT'
        stream.header['col-y'] = 'y'
        stream.header['unit-col-y'] = 'nT'
        stream.header['col-z'] = 'z'
        stream.header['unit-col-z'] = 'nT'
        stream.header['col-t1'] = 'Ts'
        stream.header['unit-col-t1'] = 'deg'
        stream.header['col-t2'] = 'Te'
        stream.header['unit-col-t2'] = 'deg'
        stream.header['col-var2'] = 'Voltage'
        stream.header['unit-col-var2'] = 'V'
        stream.header['col-str1'] = 'GPS-Status'

        timediff = []

        line = fh.read(linelength)

        while len(line) > 0:
            try:
                data= struct.unpack(str(packcode),line)
            except Exception as e:
                loggerlib.warning('readLEMIBIN: Error reading data. There is probably a broken line.')
                loggerlib.warning('readLEMIBIN: Error string: "%s"' % e)
                loggerlib.warning('readLEMIBIN: Aborting data read.')
                line = ''
            bfx = data[16]/400.
            bfy = data[17]/400.
            bfz = data[18]/400.

            stream.header['DataCompensationX'] = bfx
            stream.header['DataCompensationY'] = bfy
            stream.header['DataCompensationZ'] = bfz

            # get GPSstate
            gpsstate = data[53]

            if gpsstate == 'A':
                time = datetime(2000+h2d(data[5]),h2d(data[6]),h2d(data[7]),h2d(data[8]),h2d(data[9]),h2d(data[10]))  # Lemi GPS time
                sectime = datetime(2000+data[55],data[56],data[57],data[58],data[59],data[60],data[61])+timedelta(microseconds=timeshift*1000.)                 # PC time
                timediff.append((date2num(time)-date2num(sectime))*24.*3600.) # in seconds
            else:
                try:
                    time = datetime(2000+data[55],data[56],data[57],data[58],data[59],data[60],data[61])+timedelta(microseconds=timeshift*1000.)                        # PC time
                except:
                    loggerlib.error("readLEMIBIN: Error reading line. Aborting read. (See docs.)")
                try:
                    sectime = datetime(2000+h2d(data[5]),h2d(data[6]),h2d(data[7]),h2d(data[8]),h2d(data[9]),h2d(data[10]))  # Lemi GPS time
                    timediff.append((date2num(time)-date2num(sectime))*24.*3600.) # in seconds
                except:
                    loggerlib.warning("readLEMIBIN: Could not read secondary time column.")
#--------------------TODO--------------------------------------------
# This is usually an error that comes about during an interruption of data writing
# that leads to only a partial line being written. Normal data usually follows if the
# data logger starts up again within the same day.
# ---> It can be remedied using an iterative search for the next appearing "L025" tag
# in the binary data. See magpy/acquisition/lemiprotocol.py for an example of this
# iterative search.
#--------------------------------------------------------------------

            xpos = KEYLIST.index('x')
            ypos = KEYLIST.index('y')
            zpos = KEYLIST.index('z')
            t1pos = KEYLIST.index('t1')
            t2pos = KEYLIST.index('t2')
            var2pos = KEYLIST.index('var2')
            str1pos = KEYLIST.index('str1')
            secpos = KEYLIST.index('sectime')
            for i in range(10):
                tim = date2num(time+timedelta(microseconds=(100000.*i)))
                array[0].append(tim)
                array[xpos].append((data[20+i*3])*1000.)
                array[ypos].append((data[21+i*3])*1000.)
                array[zpos].append((data[22+i*3])*1000.)
                array[t1pos].append(data[11]/100.)
                array[t2pos].append(data[12]/100.)
                array[var2pos].append(data[52]/10.)
                array[str1pos].append(data[53])
                sectim = date2num(sectime+timedelta(microseconds=(100000.*i)))
                array[secpos].append(sectim)

            line = fh.read(linelength)

    fh.close()
    gpstime = True
    if gpstime and len(timediff) > 0:
        loggerlib.info("readLEMIBIN2: Time difference (in sec) between GPS and PC (GPS-PC): %f sec +- %f" % (np.mean(timediff), np.std(timediff)))
        print("Time difference between GPS and PC (GPS-PC):", np.mean(timediff), np.std(timediff))

    for idx,ar in enumerate(array):
        if len(ar) > 0:
            array[idx] = np.asarray(array[idx]).astype(object)

    stream.header['DataFormat'] = 'Lviv-LEMI'

    return DataStream([LineStruct()], stream.header, np.asarray(array))



def readLEMIBIN1(filename, headonly=False, **kwargs):
    '''
    Function for reading LEMI format data.
    NOTE: This function reads an outdated data format.
    Timeshift of ~0.3 seconds must be accounted for.
    (This timeshift is corrected for in current acquisition.lemiprotocol.)
    '''

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debug = kwargs.get('debug')
    getfile = True

    fh = open(filename, 'rb')
    # read file and split text into channels
    stream = DataStream()
    array = [[] for key in KEYLIST]
    # Check whether header infromation is already present
    headers = {}
    data = []
    key = None

    theday = extractDateFromString(filename)
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

    if getfile:
        loggerlib.info('readLEMIBIN1: Reading %s' % (filename))
        headers['col-x'] = 'x'
        headers['unit-col-x'] = 'nT'
        headers['col-y'] = 'y'
        headers['unit-col-y'] = 'nT'
        headers['col-z'] = 'z'
        headers['unit-col-z'] = 'nT'


        xpos = KEYLIST.index('x')
        ypos = KEYLIST.index('y')
        zpos = KEYLIST.index('z')
        t1pos = KEYLIST.index('t1')
        t2pos = KEYLIST.index('t2')

        line = fh.read(32)
        #print (line, len(line))
        while len(line) > 0:
            data= struct.unpack("<4cb6B11Bcbbhhhb", line)
            data = [el.decode('ascii') if isinstance(el, basestring) else el for el in data]
            bfx = data[-4]/400.
            bfy = data[-3]/400.
            bfz = data[-2]/400.
            headers['DataCompensationX'] = bfx
            headers['DataCompensationY'] = bfy
            headers['DataCompensationZ'] = bfz
            headers['SensorID'] = line[0:4].decode('ascii')
            newtime = []
            for i in range (5,11):
                newtime.append(h2d(data[i]))
            currsec = newtime[-1]
            newtime.append(0.0)
            for i in range (0,30):
                row = LineStruct()
                line = fh.read(16)
                data= struct.unpack('<3f2h', line)
                microsec = i/10.
                if microsec >= 2:
                    secadd = 2.
                elif microsec >= 1:
                    secadd = 1.
                else:
                    secadd = 0.
                newtime[-1] = microsec-secadd
                newtime[-2] = currsec+secadd
                time = datetime(2000+newtime[0],newtime[1],newtime[2],newtime[3],newtime[4],int(newtime[5]),int(newtime[6]*1000000))

                array[0].append(date2num(time))
                array[xpos].append((data[0])*1000.)
                array[ypos].append((data[1])*1000.)
                array[zpos].append((data[2])*1000.)
                array[t1pos].append(data[3]/100.)
                array[t2pos].append(data[4]/100.)

            line = fh.read(32)

    fh.close()

    #print "Finished file reading of %s" % filename
    for idx,ar in enumerate(array):
        if len(ar) > 0:
            array[idx] = np.asarray(array[idx]).astype(object)

    headers['DataFormat'] = 'Lviv-LEMI-old'

    return DataStream([LineStruct()], headers, np.asarray(array))

