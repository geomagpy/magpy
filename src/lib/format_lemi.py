"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *


def isLEMIHF(filename):
    """
    Checks whether a file is ASCII Lemi txt file format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    elem = temp.split()
    if len(elem) == 13:
        try:
            testtime = datetime.strptime(elem[0]+'-'+elem[1]+'-'+elem[2],"%Y-%m-%d")
        except:
            return False
    else:
        return False
    return True


def readLEMIHF(filename, headonly=False, **kwargs):
    """
    Reading IAGA2002 format data.
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
    tmpdaystring = splitpath[1].split('.')[0]
    daystring = re.findall(r'\d+',tmpdaystring)[0]
    if len(daystring) >  8:
        daystring = daystring[:8]
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%Y%m%d"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring)
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
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                elem = line.split()
                row.time = date2num(datetime.strptime(elem[0]+'-'+elem[1]+'-'+elem[2]+'T'+elem[3]+':'+elem[4]+':'+elem[5],"%Y-%m-%dT%H:%M:%S.%f"))
                row.x = float(elem[6])
                row.y = float(elem[7])
                row.z = float(elem[8])
                stream.add(row)         
    else:
        headers = stream.header
        stream =[]

    fh.close()

    print "Finished file reading of %s" % filename

    return DataStream(stream, headers)    
