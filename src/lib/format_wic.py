"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *

def isUSBLOG(filename):
    """
    Checks whether a file is ASCII USB-Logger format.
    Supports temperture and humidity logger
    Extend that code for CO logger as well
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    sp = temp.split(',')
    if not len(sp) == 6:
        return False
    if not sp[1] == 'Time':
        return False
    return True


def isRMRCS(filename):
    """
    Checks whether a file is ASCII RCS format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith('# RCS'):
        return False
    return True


def isCS(filename):
    """
    Checks whether a file is ASCII PMAG format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    tmp = temp.split()
    if not len(tmp) == 2:
        return False
    try:
        testdate = datetime.strptime(tmp[0].strip(','),"%H:%M:%S.%f")
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
    # 12="ZAGTFPT7	M6	I,cFP-AI-110	CH00	AP23	Niederschlagsmesser	--	Unwetter, S	AR0-20H0.1	mm	y=500x+0	AI"
    # 13="ZAGTFPT7	M6	I,cFP-AI-110	CH01	JC	Schneepegelsensor	OK	Mastverwehung, S	AR0-200H0	cm	y=31250x+0	AI"
    # 14="ZAGTFPT7	M6	I,cFP-AI-110	CH02	430A_T	Wetterhuette - Lufttemperatur	-	-, B	AR-35-45H0	C	y=4000x-35	AI"
    # 15="ZAGTFPT7	M6	I,cFP-AI-110	CH03	430A_F	Wetterhuette - Luftfeuchte	-	-, B	AR0-100H0	%	y=5000x+0	AI"
    # 
    1328054403.99	20120201 000004	49.276E-6	49.826E+0	-11.665E+0	78.356E+0
    1328054407.99	20120201 000008	79.480E-6	49.823E+0	-11.677E+0	78.364E+0
    1328054411.99	20120201 000012	68.555E-6	49.828E+0	-11.688E+0	78.389E+0
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    # --------------------------------------
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    measurement = []
    unit = []
    i = 0
    key = None

    # try to get day from filename (platform independent)
    # --------------------------------------
    splitpath = os.path.split(filename)
    tmpdaystring = splitpath[1].split('.')[0].split('_')
    tmpdaystring = tmpdaystring[0].replace('-','')
    daystring = re.findall(r'\d+',tmpdaystring)[0]
    if len(daystring) >  8:
        daystring = daystring[-8:]
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%Y%m%d"),"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        logging.warning("Could not identify date in filename %s - reading all" % filename)
        getfile = True
        pass

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                pass
            elif line.startswith('#'):
                # data header
                colsstr = line.split(',')
                if (len(colsstr) == 3):
                    # select the lines with three komma separaeted parts -> they describe the data
                    meastype = colsstr[1].split()
                    unittype = colsstr[2].split()
                    measurement.append(meastype[2])
                    unit.append(unittype[2])
                    headers['col-'+KEYLIST[i+1]] = unicode(measurement[i],errors='ignore')
                    headers['unit-col-'+KEYLIST[i+1]] = unicode(unit[i],errors='ignore')
                    i=i+1
            elif headonly:
                # skip data for option headonly
                continue
            else:
                # data entry - may be written in multiple columns
                # row beinhaltet die Werte eine Zeile
                elem = string.split(line[:-1])
                row = LineStruct()
                try:
                    row.time = date2num(datetime.strptime(elem[1],"%Y-%m-%dT%H:%M:%S"))
                    add = 2
                except:
                    try:
                        row.time = date2num(datetime.strptime(elem[1]+'T'+elem[2],"%Y%m%dT%H%M%S"))
                        add = 3
                    except:
                        raise ValueError, "Can't read date format in RCS file"
                for i in range(len(unit)):
                    try:
                        #print eval('elem['+str(i+add)+']')
                        exec('row.'+KEYLIST[i+1]+' = float(elem['+str(i+add)+'])')
                    except:
                        pass
                stream.add(row)         
    else:
        headers = stream.header
        stream =[]

    fh.close()

    return DataStream(stream, headers)    



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
    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header
    qFile= file( filename, "rb" )
    csvReader= csv.reader( qFile )
    for elem in csvReader:
        row = LineStruct()
        try:
            if elem[1] == 'Time':
                el2 = elem[2].split('(')
                test = el2[1]
                headers['unit-col-t1'] = unicode(el2[1].strip(')'),errors='ignore')
                headers['col-t1'] = el2[0]
                el3 = elem[3].split('(')
                headers['unit-col-var1'] = unicode(el3[1].strip(')'),errors='ignore')
                headers['col-var1'] = el3[0]
                el4 = elem[4].split('(')
                headers['unit-col-t2'] = unicode(el4[1].strip(')'),errors='ignore')
                headers['col-t2'] = el4[0]
            elif len(elem) == 6 and not elem[1] == 'Time':
                headers['InstrumentSerialNum'] = 'T-sensor:%s' % elem[5]
            else:
                row.time = date2num(datetime.strptime(elem[1],"%d/%m/%Y %H:%M:%S"))
                row.t1 = float(elem[2])
                row.var1 = float(elem[3])
                row.t2 = float(elem[4])
                stream.add(row)
        except ValueError:
            pass
    qFile.close()

    return DataStream(stream, headers)    


def readCS(filename, headonly=False, **kwargs):
    """
    Reading ASCII PyMagStructure format data.
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')

    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header
    qFile= file( filename, "rb" )
    csvReader= csv.reader( qFile )

    # get day from filename (platform independent)
    getfile = True
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    daystring = daystring[0][-6:]
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%d%m%y"),"%Y-%m-%d")
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= stream._testtime(starttime):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= stream._testtime(endtime):
                getfile = False
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring)
        getfile = True

    # Select only files within eventually defined time range
    #if getfile:
    for elem in csvReader:
        if elem[0]=='#':
            # blank line
            pass
        elif headonly:
            # skip data for option headonly
            continue
        else:
            try:
                row = LineStruct()
                row.time = date2num(datetime.strptime(day+'T'+elem[0],"%Y-%m-%dT%H:%M:%S.%f"))
                row.f = float(elem[1])
                stream.add(row)
            except ValueError:
                pass
    qFile.close()

    return DataStream(stream, headers)    

