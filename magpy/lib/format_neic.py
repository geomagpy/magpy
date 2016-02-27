"""
MagPy
NEIC input filter
Written by Roman Leonhardt February 2015
- contains read function to import neic data (usgs wget seismic data)
"""

from magpy.stream import *
import csv

def isNEIC(filename):
    """
    Checks whether a file is ASCII NEIC format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith('time,latitude,longitude,depth,mag,magType,nst,'):
        return False
    return True


def readNEIC(filename, headonly=False, **kwargs):
    """
    Reading NEIC format data.
    Looks like:
time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,horizontalError,depthError,magError,magNst,status,locationSource,magSource
2016-02-09T12:28:44.800Z,-57.874,-26.0044,69.61,5.2,mb,,82,6.881,0.67,us,us20004z0a,2016-02-09T12:49:19.348Z,"132km NNE of Bristol Island, South Sandwich Islands",earthquake,8.6,6.6,0.061,88,reviewed,us,us
2016-02-09T12:12:59.250Z,11.1366,-86.7727,38.94,5,mb,,158,1.173,1.06,us,us20004z02,2016-02-09T12:31:35.276Z,"77km SSW of Masachapa, Nicaragua",earthquake,6.7,9.8,0.062,84,reviewed,us,us

    """
    getfile = True

    array = [[] for key in KEYLIST]
    stream = DataStream([],{},np.asarray(array))
    headers = {}

    print "reading NEIC"
    datalist = []
    pos = KEYLIST.index('str1')
    if getfile:
        with open(filename, 'rb') as csvfile:
            neicreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in neicreader:
                if row[0] == 'time':
                    print "Got Header"
                    print len(row)
                    for i,key in enumerate(KEYLIST):
                        if i < 5:
                            headers['col-'+key] = row[i]
                        if i >= pos and i < pos+4:
                            headers['col-'+key] = row[i-pos+11]
                else:
                    datalist.append(row)

    neicarray = np.asarray(datalist)
    neicar = neicarray.transpose()
    timecol = np.asarray([date2num(stream._testtime(elem.replace('Z',''))) for elem in neicar[0]])
    
    array[0] = timecol
    for i in range(1,5):
        array[i] = neicar[i].astype(float)
    for i in range(7,10):
        print neicar[i]
    for i in range(11,15):
        array[i-11+pos] = neicar[i]

    ### TODO proper applictaion requires a stream2flaglist method of which stream elements (or its range) can be assigned to flaglist elements (e.g. time -> start and endtime, flagnumber as kwarg, comment either as kwarg or from selected columns (ideal: "Mytext %f, Mysecondtext %str1", where %f, %st1 is replaced by the specific column content)
    ### Use in combination with extract method

    #    array[i] = np.asarray([float(elem) for elem in neicar[i] if not elem == '' else float(nan)])
    #array[] = neicar[i].astype(float)
    print array
    print headers
    return DataStream([LineStruct()], headers, np.asarray(array))

