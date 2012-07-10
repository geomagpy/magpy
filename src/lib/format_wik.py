"""
MagPy
PMAG input filter (specific for WIK - Elsec)
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *


def isPMAG1(filename):
    """
    Checks whether a file is ASCII PMAG format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    tmp = temp.split()
    if len(tmp) != 3:
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
    tmp = temp.split()
    if len(tmp) != 2:
        return False
    try:
        testdate = datetime.strptime(tmp[1],"%m%d%H%M%S")
    except:
        return False
    return True


def readPMAG1(filename, headonly=False, **kwargs):
    """
    Reading PMAG1 format data.
    Looks like:
    00:00:07	48488.3	0713220022
    00:00:17	48487.7	0713220032
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    
    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None
    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    daystring = daystring[0].strip("ZAGCPMAG-LOG_")
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%Y_%m_%d"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring)
        return []
    print day
    # Select only files within eventually defined time range
    if starttime:
        if not datetime.strptime(day,'%Y-%m-%d') >= stream._testtime(starttime):
            getfile = False
    if endtime:
        if not datetime.strptime(day,'%Y-%m-%d') <= stream._testtime(endtime):
            getfile = False

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
            row=[]
            # Verwende das letzte Zeichen von "line" nicht, d.h. line[:-1],
            # da darin der Zeilenumbruch "\n" steht
            for val in string.split(line[:-1]):
                # nur nicht-leere Spalten hinzufuegen
                if string.strip(val)!="":
                    row.append(string.strip(val))
            # Baue zweidimensionales Array auf       
            data.append(row)

    fh.close()

    # The final values for checking non-single day records
    data_len = len(data)
    finhour = datetime.strftime(datetime.strptime(data[-1][0],"%H:%M:%S"),"%H")
    if data_len > 0:
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    for idx, elem in enumerate(data):
        # Time conv:
        row = LineStruct()
        try:
            strtime = datetime.strptime(day+'T'+elem[0],"%Y-%m-%dT%H:%M:%S")
            hour = datetime.strftime(strtime,"%H")
            subday = 0
            if (int(finhour)-int(hour) == 0) and (data_len-idx > data_len/2):
                subday = -1
            row.time=date2num(strtime + timedelta(days=subday))
            try:
                strval = elem[1].replace(',','.')
            except:
                strval = elem[1]
                pass
            row.f = float(strval)
            row.sectime=date2num(datetime.strptime(day.split("-")[0]+elem[2],"%Y%m%d%H%M%S"))
            stream.add(row)
        except:
            logging.warning("Error in input data: %s - skipping bad value" % daystring)
            pass

    return DataStream(stream, headers)  


def readPMAG2(filename, headonly=False, **kwargs):
    """
    Reading PMAG2 format data.
    Looks like:
    48488.3	0713220022
    48487.7	0713220032
    """
    
    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None
    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    daystring = daystring[0].strip("CO")
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%y%m%d"),"%Y-%m-%d")
    except:
        #raise ValueError, "Dateformat in Filename missing"
        logging.warning("Wrong dateformat in Filename %s" % daystring)
        return []
    print day
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
            row = LineStruct()
            if firsthit:
                startmonth = datetime.strftime(datetime.strptime(elem[1],"%m%d%H%M%S"),"%m")
                firsthit = False
            try:
                # ToDo: case if starting in 2009 and entering 2010....
                strtime = datetime.strptime(day.split("-")[0]+elem[1],"%Y%m%d%H%M%S")
                month = datetime.strftime(strtime,"%m")
                addyear = 0
                if int(month)-int(startmonth) < 0:
                    addyear = 1
                    strtime = datetime.strptime(str(int(day.split("-")[0])+addyear)+elem[1],"%Y%m%d%H%M%S")
                row.time=date2num(strtime)
                try:
                    strval = elem[0].replace(',','.')
                except:
                    strval = elem[0]
                    pass
                row.f = float(strval)/10
                stream.add(row)
            except:
                logging.warning("Error in input data: %s - skipping bad value" % daystring)
                pass

    if len(stream) > 0:
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    fh.close()

    return DataStream(stream, headers)    

