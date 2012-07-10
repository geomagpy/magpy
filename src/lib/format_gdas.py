"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *


def isGDASA1(filename):
    """
    Checks whether a file is ASCII GDAS (type1) format used by Romans modification of Chris Turbits code.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith('  Time'):
        return False
    if not 'T' in temp:
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
    fh = open(filename, 'rt')
    # read file and split text into channels
    data = []
    getfile = True
    key = None
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    # get day from filename (platform independent)
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    try:
        day = datetime.strftime(datetime.strptime(daystring[0].strip('gdas') , "%Y%m%d"),"%Y-%m-%d")
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
                pass
            elif line.startswith('  Time'):
                # data header
                colsstr = line.lower().split()
                for it, elem in enumerate(colsstr):
                    if elem == 'time':
                        headers['epoch'] = elem
                    else: # check for headers and replace hd with xy
                        if elem == 'h':
                            headers['InstrumentOrientation'] = 'hdz'
                            elem = 'x'
                        if elem == 'd':
                            elem = 'y'
                        colname = 'col-%s' % elem
                        headers[colname] = elem
                        if not elem == 't':
                            headers['unit-' + colname] = 'nT' # actually is 10*nT but that is corrected during data read
                        else:
                            headers['unit-' + colname] = 'C'                        
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                elem = line.split()
                try:
                    row.time=date2num(datetime.strptime(elem[0],"%d-%m-%Y-%H:%M:%S"))
                except:
                    try:
                        row.time = date2num(elem[0].isoformat())
                    except:
                        raise ValueError, "Wrong date format in %s" % filename
                row.x = float(elem[1])/10.0
                row.y = float(elem[2])/10.0
                row.z = float(elem[3])/10.0
                row.t1 = float(elem[4])/10.0
                if (float(elem[5]) != 99999):
                    row.f = float(elem[5])/10.0
                stream.add(row)         

        fh.close()
    else:
        headers = stream.header
        stream =[]

    return DataStream(stream, headers)    


