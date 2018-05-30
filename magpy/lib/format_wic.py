"""
MagPy
Auxiliary input filter - WIC/WIK
Supports USB temperature loggers, RCS files, old Caesium data and SG data
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import division

from io import open

from magpy.stream import *


def OpenFile(filename, mode='w'):
    if sys.version_info >= (3,0,0):
        f = open(filename, mode, newline='')
    else:
        f = open(filename, mode+'b')
    return f

def isUSBLOG(filename):
    """
    Checks whether a file is ASCII USB-Logger format.
    Supports temperture and humidity logger
    Extend that code for CO logger as well
    """
    try:
        temp = open( filename, "r", newline='', encoding='utf-8', errors='ignore' ).readline()
    except:
        return False
    try:
        sp = temp.split(',')
        if not len(sp) == 6:
            return False
        if not sp[1] == 'Time':
            return False
    except:
        return False
    return True


def isRMRCS(filename):
    """
    Checks whether a file is ASCII RCS format.
    """
    try:
        fh = open(filename, 'r', encoding='utf-8', newline='', errors='ignore')
        temp = fh.readline()
    except:
        return False
    try:
        if not temp.startswith('# RCS'):
            return False
    except:
        return False
    return True

def isIWT(filename):
    """
    Checks whether a file is ASCII Tiltmeter format.
    """

    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        comp = temp.split()
        if not len(comp) == 4:
            return False
    except:
        return False
    try:
        test = datetime.strptime(comp[0],"%Y%m%dT%H%M%S.%f")
    except:
        return False
    return True

def isMETEO(filename):
    """
    Checks whether a file is ASCII METEO format provided by the Cobs RCS system.
    """

    try:
        fh = open(filename, 'rb')
        temp = fh.readline()
    except:
        return False
    try:
        comp = temp.split()
    except:
        return False

    try:
        if not comp[0].decode('utf-8') == 'Date':
            return False
        if not comp[3].decode('utf-8').startswith('AP23'):
            return False

        temp = fh.readline()
        comp = temp.split()
        date = comp[0].decode('utf-8') + '-' + comp[1].decode('utf-8')
        test = datetime.strptime(date,"%Y%m%d-%H%M%S")
    except:
        return False

    return True


def isLNM(filename):
    """
    Checks whether a file is ASCII Laser-Niederschlags-Monitor file (Thies).
    """

    try:
        fh = open(filename, 'rt')
        temp = fh.readline()
    except:
        return False
    try:
        if not temp.startswith('# LNM '):
            return False
    except:
        return False
    return True


def isLIPPGRAV(filename):
    """
    Checks whether a file is an ASCII Lippmann tiltmeter file.
    """

    try:
        fh = open(filename, 'rt')
        temp = fh.readline()
    except:
        return False
    try:
        comp = temp.split()
        if not len(comp) == 6:
            return False
    except:
        return False
    try:
        test = datetime.strptime(comp[0],"%Y%m%d%H%M%S")
    except:
        return False
    return True


def isGRAVSG(filename):
    """
    Checks whether a file is ASCII SG file format.
    """

    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        if not temp.startswith('[TSF-file]'):
            return False
    except:
        return False
    return True


def isCS(filename):
    """
    Checks whether a file is ASCII CS Mag and initial ws format.
    should be called as one of the last options
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        tmp = temp.split()
        if not len(tmp) in [2,4]:
            return False
    except:
        return False
    try:
        testdate = datetime.strptime(tmp[0].strip(','),"%H:%M:%S.%f")
    except:
        try:
            testdate = datetime.strptime(tmp[0],"%Y-%m-%dT%H:%M:%S.%f")
        except:
            return False
    return True

def readRMRCS(filename, headonly=False, **kwargs):
    """
    Reading RMRCS format data. (Richard Mandl's RCS extraction)
    # RCS Fieldpoint T7
    # Conrad Observatorium, www.zamg.ac.at
    # 2012-02-01 00:00:00
    #
    # 12="ZAGTFPT7      M6      I,cFP-AI-110    CH00    AP23    Niederschlagsmesser     --      Unwetter, S     AR0-20H0.1      mm      y=500x+0        AI"
    # 13="ZAGTFPT7      M6      I,cFP-AI-110    CH01    JC      Schneepegelsensor       OK      Mastverwehung, S        AR0-200H0       cm      y=31250x+0      AI"
    # 14="ZAGTFPT7      M6      I,cFP-AI-110    CH02    430A_T  Wetterhuette - Lufttemperatur   -       -, B    AR-35-45H0      C       y=4000x-35      AI"
    # 15="ZAGTFPT7      M6      I,cFP-AI-110    CH03    430A_F  Wetterhuette - Luftfeuchte      -       -, B    AR0-100H0       %       y=5000x+0       AI"
    #
    1328054403.99       20120201 000004 49.276E-6       49.826E+0       -11.665E+0      78.356E+0
    1328054407.99       20120201 000008 79.480E-6       49.823E+0       -11.677E+0      78.364E+0
    1328054411.99       20120201 000012 68.555E-6       49.828E+0       -11.688E+0      78.389E+0
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    debug = kwargs.get('debug')
    debug = True
    if debug:
        print ("RCS: found data from Richards Perl script")

    #fh = open(filename, 'r', encoding='utf-8', newline='', errors='ignore')
    #fh = open(filename, 'r', newline='')
    fh = open(filename, 'rb')
    # read file and split text into channels
    # --------------------------------------
    stream = DataStream()
    headers = {}
    array = [[] for key in KEYLIST]
    data = []
    measurement = []
    unit = []
    i = 0
    key = None

    # try to get day from filename (platform independent)
    # --------------------------------------
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
        for line in fh:
            line = line.decode('utf-8','ignore')
            if line.isspace():
                # blank line
                pass
            elif line.startswith('# RCS Fieldpoint'):
                # data header
                fieldpoint = line.replace('# RCS Fieldpoint','').strip()
            elif line.startswith('#'):
                # data header
                colsstr = line.split(',')
                if (len(colsstr) == 3):
                    # select the lines with three komma separeted parts -> they describe the data
                    meastype = colsstr[1].split()
                    unittype = colsstr[2].split()
                    measurement.append(meastype[2])
                    unit.append(unittype[2])
                    headers['col-'+KEYLIST[i+1]] = measurement[i]
                    headers['unit-col-'+KEYLIST[i+1]] = unit[i]
                    if headers['unit-col-'+KEYLIST[i+1]] == '--':
                        headers['unit-col-'+KEYLIST[i+1]] = ''
                    i=i+1
            elif headonly:
                # skip data for option headonly
                continue
            else:
                # data entry - may be written in multiple columns
                # row beinhaltet die Werte eine Zeile
                elem = line[:-1].split()
                gottime = False

                try:
                    array[0].append(date2num(datetime.strptime(elem[1],"%Y-%m-%dT%H:%M:%S")))
                    add = 2
                    gottime = True
                except:
                    try:
                        array[0].append(date2num(datetime.strptime(elem[1]+'T'+elem[2],"%Y%m%dT%H%M%S")))
                        add = 3
                        gottime = True
                    except:
                        raise ValueError("Can't read date format in RCS file")
                if gottime:
                    for i in range(len(unit)):
                        try:
                            array[i+1].append(float(elem[i+add]))
                        except:
                            array[i+1].append(float(nan))
                            pass

        array = [np.asarray(el) for el in array]
        headers['SensorID'] = 'RCS{}_20160114_0001'.format(fieldpoint) # 20160114 corresponds to the date at which RCS was activated
        headers["SensorName"] = 'RCS{}'.format(fieldpoint)
        headers["SensorSerialNum"] = "20160114"
        headers["SensorRevision"] = "0001"
        headers["SensorModule"] = "RCS"
        headers["SensorGroup"] = "environment"
        headers["SensorDataLogger"] = "{}".format(fieldpoint)
    else:
        headers = stream.header
        stream =[]

    fh.close()


    return DataStream([LineStruct()], headers, np.asarray(array))


def readLNM(filename, headonly=False, **kwargs):
    """
    Reading ASCII LNM data files.

    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debug = kwargs.get('debug')
    getfile = True

    array = [[] for key in KEYLIST]
    stream = DataStream([],{},np.asarray(array))

    if debug:
        print("Found LNM file")
    synopdict = {"-1":"Sensorfehler",
                 "41":"Leichter bis maessiger Niederschlag (nicht identifiziert, unbekannt)",
                 "42":"Starker Niederschlag (nicht identifiziert, unbekannt)",
                 "00":"Kein Niederschlag",
                 "51":"Leichter Niesel",
                 "52":"Maessiger Niesel",
                 "53":"Starker Niesel",
                 "57":"Leichter Niesel mit Regen",
                 "58":"Maessiger bis starker Niesel mit Regen",
                 "61":"Leichter Regen",
                 "62":"Maessiger Regen",
                 "63":"Starker Regen",
                 "67":"Leichter Regen",
                 "68":"Maessiger bis starker Regen",
                 "77":"Schneegriesel",
                 "71":"Leichter Schneefall",
                 "72":"Maessiger Schneefall",
                 "73":"Starker Schneefall",
                 "74":"Leichte Graupel",
                 "75":"Maessige Graupel",
                 "76":"Starke Graupel",
                 "89":"Hagel"}

    # get day from filename (platform independent)
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
    # Check whether header infromation is already present

    if getfile:
        headers = {}
        # Get the indicies to be used for the array
        indx = KEYLIST.index('x')
        indy = KEYLIST.index('y')
        indz = KEYLIST.index('z')
        indf = KEYLIST.index('f')
        inddx = KEYLIST.index('dx')
        inddy = KEYLIST.index('dy')
        inddz = KEYLIST.index('dz')
        indt1 = KEYLIST.index('t1')
        indt2 = KEYLIST.index('t2')
        indvar1 = KEYLIST.index('var1')
        indvar2 = KEYLIST.index('var2')
        indvar3 = KEYLIST.index('var3')
        indvar4 = KEYLIST.index('var4')
        indvar5 = KEYLIST.index('var5')
        indstr1 = KEYLIST.index('str1')
        indstr2 = KEYLIST.index('str2')

        cnt = 0
        #qFile= file( filename, "rb" )
        qFile= open( filename, encoding='utf-8' )
        csvReader= csv.reader( qFile, delimiter=str(';'))
        for idx, elem in enumerate(csvReader):
            try:
                if elem[0].startswith('# LNM'):
                    headers['col-x'] = 'rainfall'
                    headers['unit-col-x'] = 'mm'
                    headers['col-y'] = 'visibility'
                    headers['unit-col-y'] = 'm'
                    headers['col-z'] = 'reflectivity'
                    headers['unit-col-z'] = 'dBZ'
                    headers['col-f'] = 'P_tot'
                    headers['col-t1'] = 'T'
                    headers['unit-col-t1'] = 'degC'
                    headers['col-t2'] = 'T_el'
                    headers['unit-col-t2'] = 'degC'
                    headers['col-var1'] = 'I_tot'
                    headers['col-var2'] = 'I_fluid'
                    headers['col-var3'] = 'I_solid'
                    headers['col-var4'] = 'd(hail)'
                    headers['unit-col-var4'] = 'mm'
                    headers['col-var5'] = 'qualtiy'
                    headers['unit-col-var5'] = 'percent'
                    headers['col-dx'] = 'P_slow'
                    headers['col-dy'] = 'P_fast'
                    headers['col-dz'] = 'P_small'
                    headers['col-str1'] = 'SYNOP-4680-code'
                    headers['col-str2'] = 'SYNOP-4680-description'
                elif len(elem) == 527:
                    cnt += 1
                    #print datetime.strptime(elem[0]+'T'+elem[1],"%Y-%m-%dT%H:%M:%S.%f")
                    array[0].append(date2num(datetime.strptime(elem[0]+'T'+elem[1],"%Y-%m-%dT%H:%M:%S.%f")))
                    array[indx].append(elem[17])
                    array[indy].append(elem[18])
                    array[indz].append(elem[19])
                    array[indf].append(elem[51])
                    array[indt1].append(elem[46])
                    array[indt2].append(elem[38])
                    array[indvar1].append(elem[14])
                    array[indvar2].append(elem[15])
                    array[indvar3].append(elem[16])
                    array[indvar4].append(elem[21])
                    array[indvar5].append(elem[20])
                    array[inddx].append(elem[53])
                    array[inddy].append(elem[55])
                    array[inddz].append(elem[57])
                    array[indstr1].append(elem[8])
                    array[indstr2].append(synopdict.get(elem[8],'undefined'))
                    if cnt == 1:
                        headers['SensorDate'] = datetime.strftime(datetime.strptime(elem[5],'%d.%m.%y'),'%Y-%m-%d')
                        headers['SensorSerialNum'] = elem[3]
                else:
                    pass
            except:
                pass
    qFile.close()

    for idx,elem in enumerate(array):
        if KEYLIST[idx] in NUMKEYLIST:
            array[idx] = np.asarray(array[idx]).astype(float)
        else:
            array[idx] = np.asarray(array[idx]).astype(object)
    # Add some Sensor specific header information
    headers['SensorDescription'] = 'Thies Laser Niederschlags Monitor: Percipitation analysis'
    headers['SensorName'] = 'LNM'
    headers['SensorGroup'] = 'environment'
    headers['SensorRevision'] = '0001'
    headers['SensorType'] = 'meteorology'
    headers['SensorKeys'] = 'x,y,z,f,t1,t2,var1,var2,var3,var4,var5,dx,dy,dz,df,str1,str2'
    headers['SensorElements'] = 'rainfall,visibility,reflectivity,P_tot,T,T_el,I_tot,I_fluid,I_solid,d(hail),qualtiy,P_slow,P_fast,P_small,SYNOP-4680-code,SYNOP-4680-description'

    try:
        headers['SensorID'] = "{}_{}_{} ".format(headers.get('SensorName'),headers.get('SensorSerialNum'),headers.get('SensorRevision'))
    except:
        pass

    headers['DataFormat'] = 'Theiss-LaserNiederschlagsMonitor'

    return DataStream([LineStruct()], headers, np.asarray(array))


def readUSBLOG(filename, headonly=False, **kwargs):
    """
    Reading ASCII USB DataLogger Structure format data.

    Vario,Time,Celsius(deg C),Humidity(%rh),dew point(deg C),Serial Number
    3,29/07/2010 12:58:03,21.0,88.5,19.0
    4,29/07/2010 13:28:03,21.0,88.5,19.0
    5,29/07/2010 13:58:03,21.0,88.5,19.0
    6,29/07/2010 14:28:03,21.0,88.5,19.0
    7,29/07/2010 14:58:03,21.0,89.0,19.1
    8,29/07/2010 15:28:03,21.0,89.0,19.1
    """

    stream = DataStream()

    array = [[] for elem in KEYLIST]
    t1ind = KEYLIST.index('t1')
    t2ind = KEYLIST.index('t2')
    var1ind = KEYLIST.index('var1')

    # Check whether header infromation is already present
    headers = {}
    qFile= open( filename, "r", newline='', encoding='utf-8', errors='ignore' )
    csvReader= csv.reader( qFile )
    for elem in csvReader:
        #row = LineStruct()
        try:
            if elem[1] == 'Time':
                el2 = elem[2].split('(')
                test = el2[1]
                headers['unit-col-t1'] = "\circ C" #unicode(el2[1].strip(')'),errors='ignore')
                headers['col-t1'] = 'T'
                el3 = elem[3].split('(')
                headers['unit-col-var1'] = "percent" #unicode(el3[1].strip(')'),errors='ignore')
                headers['col-var1'] = 'RH'
                el4 = elem[4].split('(')
                headers['unit-col-t2'] = "\circ C" #unicode(el4[1].strip(')'),errors='ignore')
                headers['col-t2'] = 'T(dew)'
            elif len(elem) == 6 and not elem[1] == 'Time':
                headers['SensorSerialNum'] = '%s' % elem[5]
            else:
                array[0].append(date2num(datetime.strptime(elem[1],"%d/%m/%Y %H:%M:%S")))
                array[t1ind].append(float(elem[2]))
                array[t2ind].append(float(elem[4]))
                array[var1ind].append(float(elem[3]))
                #row.time = date2num(datetime.strptime(elem[1],"%d/%m/%Y %H:%M:%S"))
                #row.t1 = float(elem[2])
                #row.var1 = float(elem[3])
                #row.t2 = float(elem[4])
                #stream.add(row)
        except:
            pass
    qFile.close()
    # Add some Sensor specific header information
    headers['SensorDescription'] = 'Model HMHT-LG01: This Humidity and Temperature USB data logger measures and stores relative humidity temperature readings over 0 to 100 per RH and -35 to +80 deg C measurement ranges. Humidity: Repeatability (short term) 0.1 per RH, Accuracy (overall error) 3.0* 6.0 per RH, Internal resolution 0.5 per RH, Long term stability 0.5 per RH/Yr; Temperature: Repeatability 0.1 deg C, Accuracy (overall error) 0.5 and 2  deg C, Internal resolution 0.5 deg C'
    headers['SensorName'] = 'HMHT-LG01'
    headers['SensorType'] = 'Temperature/Humidity'
    headers['SensorGroup'] = 'environment'


    array = np.asarray([np.asarray(el) for el in array])
    stream = [LineStruct()]
    return DataStream(stream, headers, array)


def readMETEO(filename, headonly=False, **kwargs):
    """
    Reading RCS Meteo data files.

    Format looks like:
Date    Time    SK      AP23    JC      430A_T  430A_F  430A_UEV        HePKS   HePKR   HePCS   HePCR   HeTKS   HeTKR   HeFlowK WV      WR      WT      LNM     Barometer
20101007        000000  0       0.000E+0        12.507E+0       6.883E+0        99.916E+0       0       17.576E+0       5.896E+0        17.282E+0       5.974E+0        28.815E+0       28.565E+0       107.942E+0      600.000E-3      200.000E+0      8.200E+0        0.000E+0        900.850E+0
20101007        000100  0       320.439E-6      12.517E+0       6.875E+0        99.924E+0       0       17.538E+0       5.974E+0        17.476E+0       5.993E+0        28.812E+0       28.555E+0       107.942E+0      200.000E-3      325.000E+0      8.400E+0        0.000E+0        900.840E+0


    By default, Helium columns are neglected
    """

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    takehelium = kwargs.get('takehelium')
    debug = kwargs.get('debug')
    getfile = True

    heliumcols = []

    stream = DataStream()

    if debug:
        print ("METEO: found RCS meteo data")

    # Check whether header infromation is already present
    headers = {}

    theday = extractDateFromString(filename)

    try:
        if starttime:
            if not theday[-1] >= datetime.date(stream._testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(stream._testtime(endtime)):
                getfile = False
    except:
        print("Did not recognize the date format")
        # Date format not recognized. Need to read all files
        getfile = True

    fh = open(filename, 'rb')

    array = [[] for key in KEYLIST]
    fkeys = []
    felements = []

    if getfile:
        for line in fh:
            line = line.decode('utf-8',errors='ignore')
            if line.isspace():
                # blank line
                continue
            elif line.startswith(' '):
                continue
            elif line.startswith('Date'):
                # Read the header information
                #1) first get number of columns
                cols = line.split()
                if not takehelium:
                    try:
                        columns = [elem for elem in cols if not elem.startswith('He')]
                    except:
                        print("Found error in header", filename)
                        columns = []
                else:
                    columns = cols
                for i, elem in enumerate(columns):
                    if i > 1:
                        key = KEYLIST[i-1]
                        fkeys.append(key)
                        headers['col-'+key] = elem.replace('_','')
                        headers['unit-col-'+key] = '-'

            else:
                colsstr = line.split()
                if not takehelium:
                    try:
                        colsstr = [elem for i, elem in enumerate(colsstr) if not cols[i].startswith('He')]
                    except:
                        print("Found error in data sequence", filename)
                        #print colsstr
                        break
                row = LineStruct()
                try:
                    date = colsstr[0]+'-'+colsstr[1]
                    array[0].append(date2num(datetime.strptime(date,"%Y%m%d-%H%M%S")))
                    #row.time = date2num(datetime.strptime(date,"%Y%m%d-%H%M%S"))
                    for i in range(2,len(colsstr)):
                        key = KEYLIST[i-1]
                        if not key.startswith('str') and not key in ['flag','comment','typ']:
                            array[i-1].append(float(colsstr[i]))
                            #exec('row.'+key+' = float(colsstr[i])')
                        elif not key in ['flag','comment','typ']:
                            array[i-1].append(str(float(colsstr[i])))
                            #exec('row.'+key+' = str(float(colsstr[i]))')
                        #row.typ = 'other'
                    #stream.add(row)
                except:
                    pass

        for idx,el in enumerate(array):
            array[idx] = np.asarray(el)

        headers['SensorDescription'] = 'RCS: filtered Meteorlogical data - Andreas Winkelbauer'
        headers['SensorName'] = 'Various Meteorology sensors'
        headers['SensorID'] = 'METEO_RCS2015_0001'
        headers['SensorType'] = 'Various'
        headers['SensorModule'] = 'RCS'
        headers['SensorDataLogger'] = 'F77'
        headers['SensorGroup'] = 'environment'
        headers['DataFormat'] = 'RCSMETEO v3.0'
        headers['col-t2'] = '430UEV' # Necessary because of none UTF8 coding in header
        headers['col-f'] = 'T'
        headers['unit-col-f'] = 'deg C'
        headers['col-z'] = 'Schneehoehe'
        headers['unit-col-z'] = 'cm'
        if not takehelium:
            headers['col-t1'] = 'rh'
            headers['unit-col-t1'] = 'percent'
            headers['col-var5'] = 'P'
            headers['unit-col-var5'] = 'hPa'
            headers['col-var1'] = 'Wind'
            headers['unit-col-var1'] = 'm/s'

        headers['SensorKeys'] = ','.join(fkeys)
        headers['SensorElements'] = ','.join([headers['col-'+key] for key in KEYLIST if key in fkeys])

    if debug:
        print ("METEO: Successfully loaded METEO data")
    return DataStream([LineStruct()], headers, np.asarray(array))


def readLIPPGRAV(filename, headonly=False, **kwargs):
    """
    Reading Lippmann tiltmeter data files.

    Format looks like:
20141015000000   -5.2565   -7.5508    8.68   74.64  909.52
20141015000001   -5.2601   -7.5455    8.69   74.66  909.52
20141015000002   -5.2606   -7.5504    8.69   74.66  909.51
20141015000003   -5.2555   -7.5565    8.68   74.67  909.52
20141015000004   -5.2490   -7.5477    8.68   74.66  909.51
20141015000005   -5.2515   -7.5463    8.68   74.65  909.51
20141015000005   -5.2624   -7.5549    8.69   74.69  909.50
20141015000006   -5.2546   -7.5573    8.69   74.66  909.51
20141015000007   -5.2607   -7.5518    8.69   74.66  909.51
20141015000008   -5.2569   -7.5531    8.69   74.66  909.50
20141015000009   -5.2567   -7.5570    8.69   74.65  909.49
20141015000010   -5.2549   -7.5486    8.68   74.65  909.51

    """

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    stream = DataStream()

    # Check whether header infromation is already present
    headers = {}

    theday = extractDateFromString(filename)

    try:
        if starttime:
            if not theday[-1] >= datetime.date(stream._testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(stream._testtime(endtime)):
                getfile = False
    except:
        print("Did not recognize the date format")
        # Date format not recognized. Need to read all files
        getfile = True

    array = [[] for key in KEYLIST]
    posx = KEYLIST.index('x')
    posy = KEYLIST.index('y')
    post1 = KEYLIST.index('t1')
    posvar1 = KEYLIST.index('var1')
    posvar2 = KEYLIST.index('var2')

    fh = open(filename, 'rt')

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line.startswith(' '):
                continue
            else:
                colsstr = line.split()
                row = LineStruct()
                try:
                    date = colsstr[0]+'-'+colsstr[1]
                    array[0].append(date2num(datetime.strptime(colsstr[0],"%Y%m%d%H%M%S")))
                    array[posx].append(float(colsstr[1]))
                    array[posy].append(float(colsstr[2]))
                    array[post1].append(float(colsstr[3]))
                    array[posvar1].append(float(colsstr[4]))
                    array[posvar2].append(float(colsstr[5]))
                except:
                    pass

        headers['unit-col-x'] = 'lambda'
        headers['col-x'] = 'tilt'
        headers['unit-col-y'] = 'lambda'
        headers['col-y'] = 'tilt'
        headers['unit-col-t1'] = 'deg C'
        headers['col-t1'] = 'T'
        headers['unit-col-var1'] = 'percent'
        headers['col-var1'] = 'rh'
        headers['unit-col-var2'] = 'hPa'
        headers['col-var2'] = 'p'
        headers['SensorDescription'] = 'Lippmann: Tiltmeter system'
        headers['SensorName'] = 'Lippmann Tiltmeter'
        headers['SensorType'] = 'Tiltmeter'
        headers['SensorID'] = 'Lippmann_Tilt'

        for idx,el in enumerate(array):
            array[idx] = np.asarray(el)

    return DataStream([LineStruct()], headers, np.asarray(array))

def readIWT(filename, headonly=False, **kwargs):
    """
    Reading Tiltmete data files.

    Format looks like:
20140831T000000.041770      -28.376309       0.003279        2.224
20140831T000000.175237      -28.373077       0.003500        2.232
20140831T000000.308580      -28.377111       0.003470        2.230
20140831T000000.441923      -28.381986       0.003322        2.222
20140831T000000.575266      -28.373106       0.003399        2.231
20140831T000000.708608      -28.376691       0.003464        2.229

    """

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    sensorid = kwargs.get('sensorid')
    debug = kwargs.get('debug')
    getfile = True

    stream = DataStream()

    # Check whether header infromation is already present
    headers = {}

    theday = extractDateFromString(filename)

    try:
        if starttime:
            if not theday[-1] >= datetime.date(stream._testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(stream._testtime(endtime)):
                getfile = False
    except:
        if debug:
            print("IWT: Did not recognize the date format")
        # Date format not recognized. Need to read all files
        getfile = True

    fh = open(filename, 'rt')

    if getfile:
        ta,xa,ya,za = [],[],[],[]
        cnt = 0
        for line in fh:
            skipline = False
            if line.isspace():
                # blank line
                continue
            elif line.startswith(' '):
                continue
            else:
                colsstr = line.split()
                try:
                    try:
                        t = date2num(datetime.strptime(colsstr[0],"%Y%m%dT%H%M%S.%f"))
                    except:
                        try:
                            t = date2num(datetime.strptime(colsstr[0],"%Y%m%dT%H%M%S"))
                        except:
                            if debug:
                                print("IWT: Could not interprete time in line {}".format(cnt))
                            skipline = True
                    if not skipline:
                        x = float(colsstr[1])
                        y = float(colsstr[2])
                        z = float(colsstr[3])
                        ta.append(t)
                        xa.append(x)
                        ya.append(y)
                        za.append(z)
                except:
                    if debug:
                        print("IWT: Could not interprete values in line {}: Found {}".format(cnt,line))
                    pass
                cnt += 1
        array = [np.asarray(ta),np.asarray(xa),np.asarray(ya),np.asarray(za)]


        ndarray = np.array(array)

        stream = DataStream()
        stream = [LineStruct()]


        headers['unit-col-x'] = 'nrad'
        headers['col-x'] = 'tilt'
        headers['unit-col-y'] = 'lambda'
        headers['col-y'] = 'phase'
        headers['unit-col-z'] = 'arb'
        headers['col-z'] = 'val3'
        headers['SensorDescription'] = 'iWT: Tiltmeter system'
        headers['SensorName'] = 'Tiltmeter'
        headers['SensorType'] = 'Tiltmeter'
        if sensorid:
            headers['SensorID'] = sensorid

    return DataStream(stream,headers,ndarray)


def readCS(filename, headonly=False, **kwargs):
    """
    Reading ASCII PyMagStructure format data.
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')

    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    array = [[] for elem in KEYLIST]
    qFile= open( filename, "rt", newline='' )
    csvReader= csv.reader( qFile )

    # get day from filename (platform independent)
    getfile = True

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

    # Select only files within eventually defined time range
    if getfile:
        print ("REading", theday[0])
        logging.info(' Read: %s Format: CS (txt) ' % (filename))
        for elem in csvReader:
            #print (elem)
            if len(elem) == 1:
                elem = elem[0].split()
            if elem[0]=='#':
                # blank line
                pass
            elif headonly:
                # skip data for option headonly
                continue
            else:
                try:
                    #row = LineStruct()
                    try:
                        #row.time = date2num(datetime.strptime(day+'T'+elem[0],"%Y-%m-%dT%H:%M:%S.%f"))
                        ti = date2num(datetime.strptime(day+'T'+elem[0],"%Y-%m-%dT%H:%M:%S.%f"))
                    except:
                        #row.time = date2num(datetime.strptime(elem[0],"%Y-%m-%dT%H:%M:%S.%f"))
                        ti = date2num(datetime.strptime(elem[0],"%Y-%m-%dT%H:%M:%S.%f"))
                    array[find].append(ti)
                    if len(elem) == 2:
                        #row.f = float(elem[1])
                        array[find].append(float(elem[1]))
                    elif len(elem) == 4:
                        array[t1ind].append(float(elem[1]))
                        array[var1ind].append(float(elem[2]))
                        array[t2ind].append(float(elem[3]))
                        #row.t1 = float(elem[1])
                        #row.var1 = float(elem[2])
                        #row.t2 = float(elem[3])

                    #stream.add(row)
                except ValueError:
                    pass
        qFile.close()

        if len(elem) == 2:
            headers['unit-col-f'] = 'nT'
            headers['col-f'] = 'F'
        elif len(elem) == 4:
            headers['unit-col-t1'] = 'deg C'
            headers['unit-col-t2'] = 'deg C'
            headers['unit-col-var1'] = 'percent'
            headers['col-t1'] = 'T'
            headers['col-t2'] = 'Dewpoint'
            headers['col-var1'] = 'RH'

    array = np.asarray([np.asarray(el) for el in array])
    stream = [LineStruct()]
    return DataStream(stream, headers, array)


def readGRAVSG(filename, headonly=False, **kwargs):
    """
    Reading SG-Gravity data files.
    """

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    stream = DataStream()

    array = [[] for key in KEYLIST]

    # Check whether header infromation is already present
    headers = {}

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

    fh = open(filename, 'rt')

    ncol = 0
    ucol = 0
    getchannel = False
    getunit = False
    if getfile:
        datablogstarts = False
        for line in fh:
            if line.isspace():
                # blank line
                continue
            #elif line.startswith(' '):
            #    continue
            elif line.startswith('[TSF-file]'):
                contline = line.split()
                stream.header['DataFormat'] = contline[1]
            elif line.startswith('[TIMEFORMAT]'):
                contline = line.split()
                val = contline[1]
            elif line.startswith('[INCREMENT]'):
                contline = line.split()
                stream.header['DataSamplingRate'] = contline[1]
            elif line.startswith('[CHANNELS]'):
                getchannel = True
                #line = fh.readline()
                #while not line.startswith('['):
                #    #except:
                #    #    pass
                #    # eventually do ot like that
                #
                #CO:SG025:Grav-1
                #CO:SG025:Grav-2
                #CO:SG025:Baro-1
                #CO:SG025:Baro-2
                pass
            elif line.startswith('   ') and getchannel:
                ncol += 1
                #line = fh.readline()
                #try:
                if ncol <= 15:
                    colnames = line.split(':')[2]
                    key = KEYLIST[ncol]
                    stream.header['col-'+key] = colnames.strip()
                else:
                    ncol = 15
            elif line.startswith('[UNITS]'):
                getchannel = False
                getunit = True
            elif line.startswith('   ') and getunit:
                ucol += 1
                if ucol <= 15:
                    unitnames = line.strip()
                    key = KEYLIST[ucol]
                    stream.header['unit-col-'+key] = unitnames
                else:
                    ucol = 15
                #VOLT
                #VOLT
                #mbar
                #mbar
            elif line.startswith('[UNDETVAL]'):
                getunit = False
                pass
            elif line.startswith('[PHASE_LAG_1_DEG_CPD]'):
                #0.0390
                pass
            elif line.startswith('[PHASE_LAG_1_DEG_CPD_ERROR]'):
                #0.0001
                pass
            elif line.startswith('[N_LATITUDE_DEG]'):
                #47.9288
                contline = line.split()
                stream.header['DataAcquisitionLatitude'] = contline[1]
            elif line.startswith('[N_LATITUDE_DEG_ERROR]'):
                #0.0005
                pass
            elif line.startswith('[E_LONGITUDE_DEG]') :
                #015.8609
                contline = line.split()
                stream.header['DataAcquisitionLongitude'] = contline[1]
            elif line.startswith('[E_LONGITUDE_DEG_ERROR]'):
                #0.0005
                pass
            elif line.startswith('[HEIGHT_M_1]'):
                #1045.00
                contline = line.split()
                stream.header['DataElevation'] = contline[1]
            elif line.startswith('[HEIGHT_M_1_ERROR]'):
                #0.10
                pass
            elif line.startswith('[GRAVITY_CAL_1_UGAL_V]'):
                #-77.8279
                contline = line.split()
                stream.header['DataScaleX'] = contline[1]
            elif line.startswith('[GRAVITY_CAL_1_UGAL_V_ERROR]'):
                #0.5000
                pass
            elif line.startswith('[PRESSURE_CAL_MBAR_V]'):
                #1.0000
                contline = line.split()
                stream.header['DataScaleY'] = contline[1]
            elif line.startswith('[PRESSURE_CAL_MBAR_V_ERROR]'):
                #0.0001
                pass
            elif line.startswith('[AUTHOR]'):
                #(bruno.meurers@univie.ac.at)
                contline = line.split()
                stream.header['SensorDecription'] = contline[1]
            elif line.startswith('[PHASE_LAG_2_DEG_CPD]'):
                #0.0000
                pass
            elif line.startswith('[PHASE_LAG_2_DEG_CPD_ERROR]'):
                #0.0000
                pass
            elif line.startswith('[HEIGHT_M_2]'):
                #00.00
                pass
            elif line.startswith('[HEIGHT_M_2_ERROR]'):
                #0.00
                pass
            elif line.startswith('[GRAVITY_CAL_2_UGAL_V]'):
                #-77.8279
                contline = line.split()
                stream.header['DataScaleZ'] = contline[1]
            elif line.startswith('[GRAVITY_CAL_2_UGAL_V_ERROR]'):
                #0.5000
                pass
            elif line.startswith('[PRESSURE_ADMIT_HPA_NMS2]'):
                #03.5300
                pass
            elif line.startswith('[PRESSURE_MEAN_HPA]'):
                #1000.0
                pass
            elif line.startswith('[COMMENT]'):
                pass
                #SG CT-025 Moved from Vienna to Conrad Observatory 2007/11/07
                #Institute of Meteorology and Geophysics Vienna, Austria
                #Instrument owner Central Institute for Meteorology and Geodynamics
                #Geology Limestone
                #Calibration method LSQ fit to absolute gravity measurements
                #Installation by Eric Brinton (GWR) November 7, 2007
                #Installation Team N.Blaumoser, S.Haden, P.Melichar, B.Meurers, R.Steiner
                #Maintenance by N.Blaumoser, M.Goeschke, S.Haden, B.Meurers
                #date           time       Grav_1     Grav_2    Baro_1    Baro_2
            elif line.startswith('[DATA]'):
                datablogstarts = True
                if headonly:
                    # skip data for option headonly
                    return stream
            else:
                if datablogstarts:
                    # Read data - select according to channels
                    colsstr = line.split()
                    row = LineStruct()
                    datatime = colsstr[0]+'-'+colsstr[1]+'-'+colsstr[2]+'T'+colsstr[3]+':'+colsstr[4]+':'+colsstr[5]
                    array[0].append(date2num(datetime.strptime(datatime,"%Y-%m-%dT%H:%M:%S")))
                    for n in range(ncol):
                        array[n+1].append(float(colsstr[n+6]))
                else:
                    # some header lines not noted above found
                    pass

    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])

    stream = DataStream([LineStruct()],stream.header,np.asarray(array))

    fh.close()
    return stream
