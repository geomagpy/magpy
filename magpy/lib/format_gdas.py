"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from magpy.stream import *


def isGDASA1(filename):
    """
    Checks whether a file is ASCII GDAS (type1) format used by Romans modification of Chris Turbits code.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        if not temp.startswith('# Cobs GDAS'):
            if not temp.startswith('  Time'):
                return False
    except:
        return False
    return True


def isGDASB1(filename):
    """
    Checks whether a file is Binary GDAS (type1) format "flare type Chris Turbit".
    """
    try:
        temp = open(filename, 'rb').read(25)
    except:
        return False
    # This format falsely identifies some ace data.
    # Temporary fix until a better filter can be created:
    try:
        if 'ace' in temp:
            return False
    except:
        return False
    try:
        data= struct.unpack("<BBBBLLLLLc", temp)
        #print ("GDAS", data)
    except:
        return False
    try:
        if not data[9]=='x':
            return False
        if data[0].decode('ascii') == 'L':
            return False
    except:
        return False
    return True



# -------------------
#  Read file formats
# -------------------


def readGDASA1(filename, headonly=False, **kwargs):
    """
    Reading GDASA1 format data.
    Looks like:
      Time              H       D       Z      T       F
    01-07-2011-00:00:00    -153     -40     183      67   99999
    01-07-2011-00:00:01    -155     -40     184      67   99999
    01-07-2011-00:00:02    -155     -41     185      67   99999
    01-07-2011-00:00:03    -156     -40     184      67   99999
    01-07-2011-00:00:04    -156     -40     184      67   99999
    01-07-2011-00:00:05    -156     -39     183      67   99999
    01-07-2011-00:00:06    -156     -40     184      67   99999
    01-07-2011-00:00:07    -155     -40     184      67   99999
    """
    # read file and split text into channels
    data = []
    getfile = True
    key = None

    array = [[] for key in KEYLIST]
    stream = DataStream([],{},np.asarray(array))
    linestruct = False

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    sensorid = kwargs.get('sensorid')
    linestruct = kwargs.get('linestruct')   # for testing purposes of old data struct


    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    try:
        tmpdate = daystring[0][-8:]
        day = datetime.strftime(datetime.strptime(tmpdate, "%Y%m%d"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring[0])
        return []
    # Select only files within eventually defined time range
    if starttime:
        if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False
    if endtime:
        if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False

    if getfile:
        fh = open(filename, 'rt')
        logging.info(' Read: %s Format: GDAS ' % (filename))
        cnt = 0
        tpos = KEYLIST.index('t1')
        for line in fh:
            if line.isspace():
                # blank line
                pass
            elif '  Time' in line: # line.startswith('  Time'):
                # data header
                colsstr = line.lower().split()
                ind = colsstr.index('time')
                colsstr = colsstr[ind:]

                for it, elem in enumerate(colsstr):
                    if elem == 'time':
                        stream.header['epoch'] = elem
                    else: # check for headers and replace hd with xy
                        if elem == 'h':
                            stream.header['DataSensorOrientation'] = 'hdz'
                            elem = 'x'
                        if elem == 'd':
                            elem = 'y'
                        if elem == 't':
                            elem = 't1'
                        if not elem == 'f':
                            colname = 'col-%s' % elem
                            stream.header[colname] = elem
                            if not elem == 't1':
                                stream.header['unit-' + colname] = 'nT' # actually is 10*nT but that is corrected during data read
                            else:
                                stream.header['unit-' + colname] = 'degC'
                    if sensorid:
                        stream.header['SensorID'] = sensorid
            elif line.startswith('# Cobs'):
                # data header
                pass
            elif headonly:
                # skip data for option headonly
                continue
            else:
                elem = line.split()
                if linestruct:
                    row = LineStruct()
                    try:
                        row.time=date2num(datetime.strptime(elem[0],"%d-%m-%Y-%H:%M:%S"))
                    except:
                        try:
                            row.time = date2num(datetime.strptime(elem[0],"%Y-%m-%dT%H:%M:%S"))
                        except:
                            raise ValueError("Wrong date format in %s" % filename)
                    if float(elem[1]) < 99999.:
                        row.x = float(elem[1])/10.0
                    if float(elem[1]) < 99999.:
                        row.y = float(elem[2])/10.0
                    if float(elem[1]) < 99999.:
                        row.z = float(elem[3])/10.0
                    if float(elem[1]) < 99999.:
                        row.t1 = float(elem[4])/10.0
                    try:
                        if (float(elem[5]) != 99999):
                            row.f = float(elem[5])/10.0
                            if cnt == 1:
                                stream.header['col-f'] = 'f'
                                stream.header['unit-col-f'] = 'f'
                            cnt = cnt +1
                    except:
                        pass
                    stream.add(row)
                else:
                    try:
                        array[0].append(date2num(datetime.strptime(elem[0],"%d-%m-%Y-%H:%M:%S")))
                    except:
                        try:
                            array[0].append(date2num(datetime.strptime(elem[0],"%Y-%m-%dT%H:%M:%S")))
                        except:
                            raise ValueError("Wrong date format in %s" % filename)
                    if float(elem[1]) < 99999.:
                        array[1].append(float(elem[1])/10.0)
                    else:
                        array[1].append(np.nan)
                    if float(elem[2]) < 99999.:
                        array[2].append(float(elem[2])/10.0)
                    else:
                        array[2].append(np.nan)
                    if float(elem[3]) < 99999.:
                        array[3].append(float(elem[3])/10.0)
                    else:
                        array[3].append(np.nan)
                    if float(elem[4]) < 99999.:
                        array[tpos].append(float(elem[4])/10.0)
                    else:
                        array[tpos].append(np.nan)
                    try:
                        if (float(elem[5]) != 99999):
                            array[4].append(float(elem[5])/10.0)
                        else:
                            array[4].append(np.nan)
                    except:
                        pass

        fh.close()

        array[0] = np.asarray(array[0])
        array[1] = np.asarray(array[1])
        array[2] = np.asarray(array[2])
        array[3] = np.asarray(array[3])
        array[tpos] = np.asarray(array[tpos])
        array[4] = np.asarray(array[4])

    if linestruct:
        return stream
    else:
        return DataStream([LineStruct()], stream.header, np.asarray(array))


def readGDASB1(filename, headonly=False, **kwargs):
    """
    Reading GDAS Binary format data. (flare format)
    25 bit line lenght:
    """
    fh = open(filename, 'rb')
    # read file and split text into channels
    data = []
    getfile = True
    key = None
    stream = DataStream()
    array = [[] for key in KEYLIST]
    # Check whether header infromation is already present
    headers = {}
    # get day from filename (platform independent) -- does not work for temporary files
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')

    getfile = True
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
        line = fh.read(25)
        while line != "":
            row = LineStruct()
            data= struct.unpack("<BBBBlllllc", line)
            #print data
            date = year + '-' + str(data[0]) + '-' + str(data[1]) + 'T' + str(data[2]) + ':' + str(data[3]) + ':00'
            row.time=date2num(datetime.strptime(date,"%Y-%m-%dT%H:%M:%S"))
            if not data[4] == 99999:
                row.x = float(data[4])/10.0
            if not data[5] == 99999:
                row.y = float(data[5])/10.0
            if not data[6] == 99999:
                row.z = float(data[6])/10.0
            if not data[7] == 99999:
                row.t1 = float(data[7])/10.0
            if not data[8] == 99999:
                row.f = float(data[8])/10.0
            if not data[8] == 99999 or not data[4] == 99999:
                stream.add(row)
            line = fh.read(25)

        headers['col-x'] = 'x'
        headers['col-y'] = 'y'
        headers['col-z'] = 'z'
        headers['col-t1'] = 'T'
        headers['col-f'] = 'F'
        headers['unit-col-x'] = 'nT'
        headers['unit-col-y'] = 'nT'
        headers['unit-col-z'] = 'nT'
        headers['unit-col-f'] = 'nT'
        headers['unit-col-t1'] = 'C'

        fh.close()
    else:
        headers = stream.header
        stream =[]

    return DataStream(stream, headers,np.asarray(array))
