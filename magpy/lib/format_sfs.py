"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from magpy.stream import *


def isSFDMI(filename):
    """
    Checks whether a file is spanish DMI format.
    Time is in seconds relative to one day
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if len(temp) >= 9:
        if temp[9] in ['o','+','-']: # Prevent errors with GFZ kp
            return False
    sp = temp.split()
    if not len(sp) == 6:
        return False
    if not isNumber(sp[0]):
        return False
    #logging.info(" Found SFS file")
    return True


def isSFGSM(filename):
    """
    Checks whether a file is spanish GSM format.
    Time is in seconds relative to one day
    """
    try:
        fh = open(filename, 'rt')
        temp = fh.readline()
    except:
        return False

    sp = temp.split()
    if len(sp) != 2:
        return False
    if not isNumber(sp[0]):
        return False
    try:
        if not 20000 < float(sp[1]) < 80000:
            return False
    except:
        return False
    return True


def readSFDMI(filename, headonly=False, **kwargs):
    """
    Reading SF DMI format data.
    Looks like:
    0.03          99.11         -29.76        26.14         22.05         30.31
    5.04          98.76         -29.78        26.20         22.04         30.31
    10.01         98.85         -29.76        26.04         22.04         30.31
    15.15         98.63         -29.79        26.20         22.04         30.31
    20.12         98.85         -29.78        26.11         22.04         30.31
    first column are seconds of day
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')

    # read file and split text into channels
    stream = DataStream()
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    data = []
    key = None
    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    try:
        day = datetime.strftime(datetime.strptime(daystring[0], "%d%m%Y"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring[0])
        fh.close()
        return DataStream([], headers)


    # Select only files within eventually defined time range
    if starttime:
        if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False
    if endtime:
        if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            else:
                row = LineStruct()
                elem = line.split()
                if (len(elem) == 6):
                    row.time=date2num(datetime.strptime(day,"%Y-%m-%d"))+ float(elem[0])/86400
                    xval = float(elem[1])
                    yval = float(elem[2])
                    zval = float(elem[3])
                    row.x = xval
                    row.y = yval
                    row.z = zval
                    row.t1 = float(elem[4])
                    row.t2 = float(elem[5])
                    stream.add(row)
        stream.header['col-x'] = 'x'
        stream.header['col-y'] = 'y'
        stream.header['col-z'] = 'z'
        stream.header['col-t1'] = 'T1'
        stream.header['col-t2'] = 'T2'
        stream.header['unit-col-x'] = 'nT'
        stream.header['unit-col-y'] = 'nT'
        stream.header['unit-col-z'] = 'nT'
        stream.header['unit-col-t1'] = 'deg C'
        stream.header['unit-col-t2'] = 'deg C'
    else:
        headers = stream.header
        stream =[]

    fh.close()

    return DataStream(stream, headers)


def readSFGSM(filename, headonly=False, **kwargs):
    """
    Reading SF GSM format data.
    Looks like:
     22            42982.35
     52            42982.43
     82            42982.47
    first column are seconds of day
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None
    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    try:
        day = datetime.strftime(datetime.strptime(daystring[0], "%d%m%Y"),"%Y-%m-%d")
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
        for line in fh:
            if line.isspace():
                # blank line
                continue
            else:
                row = LineStruct()
                elem = line.split()
                if (len(elem) == 2):
                    row.time=date2num(datetime.strptime(day,"%Y-%m-%d"))+ float(elem[0])/86400
                    row.f = float(elem[1])
                    stream.add(row)
        stream.header['col-f'] = 'f'
        stream.header['unit-col-f'] = 'nT'
    else:
        headers = stream.header
        stream =[]

    fh.close()

    return DataStream(stream, headers)
