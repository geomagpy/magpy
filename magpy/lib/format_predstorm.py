"""
MagPy
PREDSTORM input filter
Written by Roman Leonhardt April 2022
- contains test and read function
- testet for py3 only
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

from magpy.stream import *


def isPREDSTORM(filename, debug=False):
    """
    Checks whether a file is ASCII IAGA 2002 format.
    """
    try:
        temp = open(filename, 'rt').readline()
        if debug:
            print ("First line:", temp)
    except:
        return False
    try:
        # Check header for typical/unique contents of a predstorm file
        if not temp.startswith('#') and not temp.find('matplotlib_time') > 0  and not temp.find('Bx') > 0 and not temp.find('Dst') > 0:
            return False
    except:
        return False
    if debug:
        print (" -> PREDSTORM format successfully identified")
    return True



def readPREDSTORM(filename, headonly=False, **kwargs):
    """
    Reading PREDSTORM format data.
    
    Looks like:
    #  Y  m  d  H  M  S matplotlib_time  B[nT]     Bx     By     Bz N[ccm-3]  V[km/s] Dst[nT]     Kp  AP[GW]Ec/4421[(km/s)**(4/3)nT**(2/3)]
    2022  4  7 16 30  0   738252.687500   6.58  -4.01  -4.88  -1.83        6      407      21   3.40    24.8         2.1
    2022  4  7 16 31  0   738252.688194   6.51  -3.86  -4.87  -1.95        6      406      21   3.40    24.8         2.1

    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debug = kwargs.get('debug')
    getfile = True

    array = [[] for key in KEYLIST]
    header = {}


    with open(filename, 'rt') as fh:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line.startswith('#'):
                # data info
                line = line.replace('#','').replace(']','] ')
                elements = line.split()
                relelements = elements[6:]
                for idx,key in enumerate(KEYLIST):
                    if idx > 0 and idx < len(relelements):
                        name = relelements[idx].split('[')
                        header['col-{}'.format(key)] = name[0]
                        if len(name) > 1:
                            header['unit-col-{}'.format(key)] = name[1].replace(']','')
                        elif name[0].startswith('B'):
                            header['unit-col-{}'.format(key)] = 'nT'
            else:
                datalist = line.split()
                datestring = "-".join(datalist[:6])
                d = datetime.strptime(datestring,"%Y-%m-%d-%H-%M-%S")
                array[0].append(date2num(d))
                valuelist = datalist[6:]
                for idx,key in enumerate(KEYLIST):
                    if idx > 0 and idx < len(valuelist):
                        array[idx].append(float(valuelist[idx]))
    
    nparray = np.array([np.array(ar).astype(object) for ar in array])
    code = os.path.basename(filename).replace('predstorm','').replace('.txt','').replace('_','')
    header['SensorName'] = "PREDSTORM"
    header['SensorSerialNum'] = "HELIO{}".format(code.upper())
    header['SensorID'] = "{}_{}_0001".format(header.get('SensorName'),header.get('SensorSerialNum'))
    if debug:
        print (header)
        print (nparray)
    stream = DataStream([LineStruct()],header,nparray)
    if starttime:
        stream = stream.trim(starttime=starttime)
    if endtime:
        stream = stream.trim(endtime=endtime)
        
    return stream

