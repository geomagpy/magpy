"""
MagPy
dIdD input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *


def isDIDD(filename):
    """
    Checks whether a file is ASCII DIDD (Tihany) format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith('hh mm'):
        return False
    if not 'F' in temp:
        return False
    return True



def readDIDD(filename, headonly=False, **kwargs):
    """
    Reading DIDD format data.
    Looks like:
    hh mm        X        Y        Z        F 
    00 00  20826.8   1206.1  43778.3  48494.8
    00 01  20833.3   1202.2  43779.3  48498.5
    00 02  20832.2   1198.7  43779.9  48498.4
    00 03  20832.6   1196.2  43779.6  48498.3
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
        day = datetime.strftime(datetime.strptime(daystring[0], "%b%d%y"),"%Y-%m-%d")
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
            elif line.startswith('hh mm'):
                # data header
                colsstr = line.lower().split()
                for it, elem in enumerate(colsstr):
                    if it>1: # dont take hh and mm
                        colname = "col-%s" % KEYLIST[it-1]
                        colname = colname.lower()
                        headers[colname] = elem
                        unitstr =  'unit-%s' % colname        
                        headers[unitstr] = 'nT'
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                elem = line.split()
                if (float(elem[5])) < 999990:
                    row.time=date2num(datetime.strptime(day+'T'+elem[0]+':'+elem[1],"%Y-%m-%dT%H:%M"))
                    xval = float(elem[2])
                    yval = float(elem[3])
                    zval = float(elem[4])
                    if (headers['col-x']=='x'):
                        row.x = xval
                        row.y = yval
                        row.z = zval
                    elif (headers['col-x']=='h'):
                        row.x, row.y, row.z = hdz2xyz(xval,yval,zval)
                    elif (headers['col-x']=='i'):
                        row.x, row.y, row.z = idf2xyz(xval,yval,zval)
                    else:
                        raise ValueError
                    row.f = float(elem[5])
                    stream.add(row)         
    else:
        headers = stream.header
        stream =[]

    fh.close()

    return DataStream(stream, headers)    



def writeDIDD(datastream, filename, **kwargs):
    """
    Looks like:
    hh mm        X        Y        Z        F 
    00 00  20826.8   1206.1  43778.3  48494.8
    00 01  20833.3   1202.2  43779.3  48498.5
    """
    if (datastream[-1].time - datastream[0].time) > 1:
        return "Writing DIDD format requires daily coverage - choose"

    headdict = datastream.header

    myFile= open( filename, 'wb' )
    wtr= csv.writer( myFile )
    headline = 'hh mm        '+headdict.get('col-x').upper()+'        '+headdict.get('col-y').upper()+'        '+headdict.get('col-z').upper()+'        '+headdict.get('col-f').upper()
    wtr.writerow( [headline] )
    for elem in datastream:
        time = datetime.strftime(num2date(elem.time).replace(tzinfo=None), "%H %M")
        line = '%s %8.1f %8.1f %8.1f %8.1f' % (time, elem.x, elem.y, elem.z, elem.f)
        wtr.writerow( [line] )
    myFile.close()

