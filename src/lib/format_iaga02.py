"""
MagPy
IAGA02 input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *


def isIAGA(filename):
    """
    Checks whether a file is ASCII IAGA 2002 format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith(' Format'):
        return False
    if not 'IAGA-2002' in temp:
        return False
    return True



def readIAGA(filename, headonly=False, **kwargs):
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
            elif line.startswith(' '):
                # data info
                infoline = line.lstrip().strip('|')
                key = infoline[:23].rstrip()
                headers[key] = infoline[23:].rstrip()
            elif line.startswith('DATE'):
                # data header
                colsstr = line.lower().split()
                for it, elem in enumerate(colsstr):
                    if it > 2:
                        colname = "col-%s" % elem[-1]
                        headers[colname] = elem[-1]
                    else:
                        colname = "col-%s" % elem
                        headers[colname] = elem
            elif headonly:
                # skip data for option headonly
                continue
            elif line.startswith('%'):
                pass
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

    for elem in data:
        # Time conv:
        row = LineStruct()
        row.time=date2num(datetime.strptime(elem[0]+'-'+elem[1],"%Y-%m-%d-%H:%M:%S.%f"))
        xval = float(elem[3])
        yval = float(elem[4])
        zval = float(elem[5])
        if (headers['col-x']=='x'):
            row.x = xval
            row.y = yval
            row.z = zval
        elif (headers['col-h']=='h'):
            row.x, row.y, row.z = hdz2xyz(xval,yval,zval)
        elif (headers['col-i']=='i'):
            row.x, row.y, row.z = idf2xyz(xval,yval,zval)
        else:
            raise ValueError
        if not float(elem[6]) == 88888:
            if headers['col-f']=='f':
                row.f = float(elem[6])
            elif headers['col-g']=='g':
                row.f = np.sqrt(row.x**2+row.y**2+row.z**2) + float(elem[6])
            else:
                raise ValueError
        stream.add(row)

    """
    Speed optimization:
    Change the whole thing to column operations


    col = ColStruct(len(data))
    for idx, elem in enumerate(data):
        # Time conv:
        xxx = col.time
        col.time[idx] = (date2num(datetime.strptime(elem[0]+'-'+elem[1],"%Y-%m-%d-%H:%M:%S.%f")))
        xval = float(elem[3])
        yval = float(elem[4])
        zval = float(elem[5])
        if (headers['col-x']=='x'):
            col.x[idx] = xval
            col.y[idx] = yval
            col.z[idx] = zval
        elif (headers['col-h']=='h'):
            col.x[idx], col.y[idx], col.z[idx] = hdz2xyz(xval,yval,zval)
        elif (headers['col-i']=='i'):
            col.x[idx], col.y[idx], col.z[idx] = idf2xyz(xval,yval,zval)
        else:
            raise ValueError
        if not float(elem[6]) == 88888:
            if headers['col-f']=='f':
                col.f[idx] = float(elem[6])
            elif headers['col-g']=='g':
                col.f[idx] = np.sqrt(row.x**2+row.y**2+row.z**2) + float(elem[6])
            else:
                raise ValueError

    arraystream = np.asarray(col)
    try:
        print len(col.time)
        print "got it"
    except:
        pass
    stream = col
    """

    return DataStream(stream, headers)    


