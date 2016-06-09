"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from magpy.stream import *


def isBDV1(filename):
    """
    Checks whether a file is Budkov gdas format.
    Time is in seconds relative to one day
    """
    try:
        fh = open(filename, 'rt')
        temp = fh.readline()
    except:
        return False
    if not temp.startswith('BDV G-das'):
        return False
    try:
        temp = fh.readline()
        tmp = temp.split()
    except:
        return False
    try:
        testdate = datetime.strptime(tmp[0],"%H:%M:%S")
    except:
        return False
    return True


def readBDV1(filename, headonly=False, **kwargs):
    """
    Reading BDV GDAS format data.
    Looks like:
    BDV G-das instrument datas Time H D Z T F 0.1 nT 0.1 deg.C date:22:04:2012
    00:00:00  3  1222  187  200  485562
    00:00:01  3  1220  189  200
    00:00:02  3  1222  188  200
    00:00:03  3  1221  188  200
    00:00:04  3  1221  189  200
    00:00:05  3  1221  188  200
    00:00:06  3  1222  187  200
    00:00:07  4  1222  189  200
    00:00:08  3  1221  187  200
    00:00:09  3  1222  189  200
    00:00:10  3  1221  190  200  485562
    00:00:11  3  1220  188  200
    00:00:12  3  1220  189  200
    """
    fh = open(filename, 'rt')
    # read file and split text into channels
    data = []
    getfile = True
    key = None
    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    # get day from filename (platform independent)
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    splitpath = os.path.split(filename)
    daystring = splitpath[1].split('.')
    try:
        day = datetime.strftime(datetime.strptime(daystring[0][:8] , "%Y%m%d"),"%Y-%m-%d")
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
            elif line.startswith('BDV G-das instrument datas'):
                # data header
                line = line.replace('BDV G-das instrument datas','')
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
                        if elem == 't':
                            colname = 'col-%s1' % elem
                        else:
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
                    row.time=date2num(datetime.strptime(day+'-'+elem[0],"%Y-%m-%d-%H:%M:%S"))
                except:
                    try:
                        row.time = date2num(elem[0].isoformat())
                    except:
                        raise ValueError("Wrong date format in %s" % filename)
                row.x = float(elem[1])/10.0
                row.y = float(elem[2])/10.0
                row.z = float(elem[3])/10.0
                row.t1 = float(elem[4])/10.0
                try:
                    if (float(elem[5]) != 99999):
                        row.f = float(elem[5])/10.0
                except:
                    pass
                stream.add(row)

        fh.close()
    else:
        headers = stream.header
        stream =[]

    return DataStream(stream, headers)
