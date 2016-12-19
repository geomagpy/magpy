"""
MagPy
PMAG input filter (specific for WIK - Elsec)
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from __future__ import print_function

from magpy.stream import *

def isOPT(filename):
    """
    Checks whether a file is ASCII PMAG format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        if not temp.startswith('Tag'):
            return False
    except:
        return False
    return True


def isPMAG1(filename):
    """
    Checks whether a file is ASCII PMAG format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False

    try:
        tmp = temp.split()
        if len(tmp) != 3:
            return False
    except:
        return False
    try:
        testdate = datetime.strptime(tmp[0],"%H:%M:%S")
    except:
        return False
    return True

def isPMAG2(filename):
    """
    Checks whether a file is ASCII PMAG2 format.
    Leading blank lines are likely
    """
    try:
        fh = open(filename, 'rt')
        temp = fh.readline()
        if temp == "":
            temp = fh.readline()
        if temp == "":
            temp = fh.readline()
    except:
        return False
    try:
        tmp = temp.split()
        if len(tmp) != 2:
            return False
    except:
        return False
    try:
        testdate = datetime.strptime(tmp[1],"%m%d%H%M%S")
    except:
        return False
    return True

def readOPT(filename, headonly=False, **kwargs):
    """
    Reading PMAG1 format data.
    Looks like:
    Tag 1       2       3       4       5       6       7       8       9       10      11      12      13      14      15      16      17      18      19      20      21      22      23      24      mittel
    1.  66      66      66      65      64      65      66      66      67      66      65      66      64      64      65      66      67      66      66      67      67      67      68      67      65,97
    2.  67      66      66      66      66      66      66      67      67      67      66      66      65      65      66      65      65      66      66      68      68      67      66      68      66,29
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    key = None
    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    namestring = splitpath[1].split('.')
    elemstring = namestring[0].split('_')
    year = elemstring[1]
    month = elemstring[2][:2]
    comp = elemstring[0].lower()
    if comp=='d':
        offset = float(elemstring[3].strip('grad').strip('Basis'))
        headers['col-y'] = 'd'
        headers['unit-col-y'] = 'deg'
    elif comp=='h':
        offset = float(elemstring[3].strip('Basis'))
        headers['col-x'] = 'h'
        headers['unit-col-x'] = 'nT'
    elif comp=='z':
        offset = float(elemstring[3].strip('Basis'))
        headers['col-z'] = 'z'
        headers['unit-col-z'] = 'nT'
    elif comp=='f':
        offset = float(elemstring[3].strip('Basis'))
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    print("Basis for component %s: %s" % (comp, offset))

    for line in fh:
        if line.isspace():
            # blank line
            continue
        elif line.startswith('Tag'):
            # data header
            pass
        elif headonly:
            # skip data for option headonly
            continue
        else:
            elem = line.split()
            day = elem[0]
            i=0
            for i in range(24):
                try:
                    row = LineStruct()
                    date = year + '-' + month + '-' + str(day).strip('.') + 'T' + str(i) + ':30:00'
                    row.time=date2num(datetime.strptime(date,"%Y-%m-%dT%H:%M:%S"))
                    if comp=='d':
                        # minutes to seconds
                        row.y = offset + float(elem[i+1])/60.
                    if comp=='z':
                        # minutes to seconds
                        row.z = offset + float(elem[i+1])
                    if comp=='h':
                        # minutes to seconds
                        row.x = offset + float(elem[i+1])
                    if comp=='f':
                        # minutes to seconds
                        if (offset + float(elem[i+1])) < 100000: # empty values get 100000 as input in data file
                            row.f = offset + float(elem[i+1])
                    row.typ = 'hdzf'
                    stream.add(row)
                except:
                    pass

    fh.close()

    return DataStream(stream, headers)



def readPMAG1(filename, headonly=False, **kwargs):
    """
    Reading PMAG1 format data.
    Looks like:
    00:00:07    48488.3 0713220022
    00:00:17    48487.7 0713220032
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    array = [[] for elem in KEYLIST]
    find = KEYLIST.index('f')
    secind = KEYLIST.index('sectime')

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    data = []
    day = ''
    key = None
    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    daystring = daystring[0][-10:]
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%Y_%m_%d"),"%Y-%m-%d")
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
    if getfile:
        regularfound = False
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif len(line.split())!=3:
                # data header
                pass
            elif headonly:
                # skip data for option headonly
                continue
            else:
                # data entry - may be written in multiple columns
                # row beinhaltet die Werte eine Zeile
                row=LineStruct()
                # Verwende das letzte Zeichen von "line" nicht, d.h. line[:-1],
                elem = line.split()
                try:
                    strtime = datetime.strptime(day+'T'+elem[0],"%Y-%m-%dT%H:%M:%S")
                    hour = datetime.strftime(strtime,"%H")
                    subday = 0
                    if (23-int(hour) == 0) and not regularfound:
                        subday = -1
                    elif int(hour) == 0:
                        regularfound = True
                    #row.time=date2num(strtime + timedelta(days=subday))
                    array[0].append(date2num(strtime + timedelta(days=subday)))
                    try:
                        strval = elem[1].replace(',','.')
                    except:
                        strval = elem[1]
                        pass
                    #row.f = float(strval)
                    array[find].append(float(strval))
                    array[secind].append(date2num(datetime.strptime(day.split("-")[0]+elem[2],"%Y%m%d%H%M%S")))
                    #row.sectime=date2num(datetime.strptime(day.split("-")[0]+elem[2],"%Y%m%d%H%M%S"))
                    #stream.add(row)
                except:
                    logging.warning("Error in input data: %s - skipping bad value" % daystring)
                    pass
        fh.close()

    headers['col-f'] = 'f'
    headers['unit-col-f'] = 'nT'

    array = np.asarray([np.asarray(el) for el in array])
    stream = [LineStruct()]
    return DataStream(stream, headers, array)



def readPMAG2(filename, headonly=False, **kwargs):
    """
    Reading PMAG2 format data.
    Looks like:
    48488.3     0713220022
    48487.7     0713220032
    """

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    data = []
    day = ''
    key = None

    array = [[] for elem in KEYLIST]
    find = KEYLIST.index('f')

    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    daystring = daystring[0].upper().strip("CO")
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%y%m%d"),"%Y-%m-%d")
    except:
        #raise ValueError, "Dateformat in Filename missing"
        logging.warning("Wrong dateformat in Filename %s" % daystring)
        pass

    firsthit = True
    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif len(line.split())!=2:
            # data header
            pass
        elif headonly:
            # skip data for option headonly
            continue
        else:
            elem = line.split()
            if firsthit:
                startmonth = datetime.strftime(datetime.strptime(elem[1],"%m%d%H%M%S"),"%m")
                firsthit = False
            try:
                # ToDo: case if starting in 2009 and entering 2010....
                strtime = datetime.strptime(day.split("-")[0]+elem[1],"%Y%m%d%H%M%S")
                month = datetime.strftime(strtime,"%m")
                addyear = 0
                #row = LineStruct()
                if int(month)-int(startmonth) < 0:
                    addyear = 1
                    strtime = datetime.strptime(str(int(day.split("-")[0])+addyear)+elem[1],"%Y%m%d%H%M%S")
                #row.time=date2num(strtime)
                array[0].append(date2num(strtime))
                try:
                    strval = elem[0].replace(',','.')
                except:
                    strval = elem[0]
                    pass
                array[find].append(float(strval)/10)
                #row.f = float(strval)/10
                #stream.add(row)
            except:
                logging.warning("Error in input data: %s - skipping bad value" % daystring)
                pass

    if len(array[0]) > 0:
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    fh.close()

    array = np.asarray([np.asarray(el) for el in array])
    stream = [LineStruct()]
    return DataStream(stream, headers, array)

