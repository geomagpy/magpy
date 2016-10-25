"""
MagPy
NEIC input filter
Written by Roman Leonhardt February 2015
- contains read function to import neic data (usgs wget seismic data)
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

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
    try:
        if not temp.startswith('time,latitude,longitude,depth,mag,magType,nst,'):
            return False
    except:
        return False

    print ("Found NEIC data")
    return True


def readNEIC(filename, headonly=False, **kwargs):
    """
    Reading NEIC format data.
    obtained by 
    curl http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.csv
    Looks like:
time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,horizontalError,depthError,magError,magNst,status,locationSource,magSource
2016-02-09T12:28:44.800Z,-57.874,-26.0044,69.61,5.2,mb,,82,6.881,0.67,us,us20004z0a,2016-02-09T12:49:19.348Z,"132km NNE of Bristol Island, South Sandwich Islands",earthquake,8.6,6.6,0.061,88,reviewed,us,us
2016-02-09T12:12:59.250Z,11.1366,-86.7727,38.94,5,mb,,158,1.173,1.06,us,us20004z02,2016-02-09T12:31:35.276Z,"77km SSW of Masachapa, Nicaragua",earthquake,6.7,9.8,0.062,84,reviewed,us,us

    """
    getfile = True

    array = [[] for key in KEYLIST]
    stream = DataStream([],{},np.asarray(array))
    headers = {}

    datalist = []
    pos = KEYLIST.index('str1')
    if getfile:
        with open(filename, 'r', encoding='utf-8', newline='', errors='ignore') as csvfile:
            neicreader = csv.reader(csvfile, delimiter=str(','), quotechar=str('"'))
            #print (neicreader)
            for row in neicreader:
                #row = [el.encode('ascii','ignore') for el in row]
                if len(row) > 0:
                  if row[0] == 'time':
                    #print("Got Header")
                    #print(len(row))
                    dxp = KEYLIST.index('dy')
                    secp = KEYLIST.index('sectime')
                    for i,key in enumerate(KEYLIST):
                        if i < 5:
                            headers['col-'+key] = row[i]
                        if i in range(5,8):
                            headers['col-'+key] = row[i+2]
                        if i == pos:
                            headers['col-'+key] = row[i-pos+11]
                        if i in range(pos+2,pos+4):
                            headers['col-'+key] = row[i-pos+11]
                        if i in range(dxp,dxp+3):
                            headers['col-'+key] = row[i-dxp+15]
                        if i == pos+1:
                            headers['col-'+key] = row[17]
                        if i == secp:
                            headers['col-'+key] = row[12]
                  else:
                    datalist.append(row)

    neicarray = np.asarray(datalist)
    neicar = neicarray.transpose()
    if sys.version_info >= (3,0,0):
        timecol = np.asarray([date2num(stream._testtime(elem.replace('Z',''))) for elem in neicar[0]])
    else:
        timecol = np.asarray([date2num(stream._testtime(elem.replace('Z','').encode('ascii','ignore'))) for elem in neicar[0]])
    array[0] = timecol
    for i in range(1,5):
        array[i] = neicar[i].astype(float)
    for i in range(7,10):
        array[i-2] = neicar[i]
    for i in range(pos+2,pos+4):
        array[i] = neicar[i-pos+11]
    for i in range(dxp,dxp+3):
        array[i] = neicar[i-dxp+15]
    array[pos] = neicar[11]
    # sec time
    if sys.version_info >= (3,0,0):
        array[secp] = np.asarray([date2num(stream._testtime(elem.replace('Z',''))) for elem in neicar[12]])
    else:
        array[secp] = np.asarray([date2num(stream._testtime(elem.replace('Z','').encode('ascii','ignore'))) for elem in neicar[12]])
    # status
    array[pos+1] = neicar[i-dxp+17]

    ## General Header data
    headers['DataFormat'] = 'NEICCSV'
    headers['DataSource'] = 'Earthquake Hazards Program of the USGS'
    #headers['DataTerms'] = ''
    headers['DataReferences'] = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.csv'

    return DataStream([LineStruct()], headers, np.asarray(array).astype(object))

