"""
MagPy
Input filter for IONOMETER data
Written by Roman Leonhardt December 2015
- contains test and read function, no write function
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open


from magpy.stream import *

def isIONO(filename):
    """
    Checks whether a file is IM806 format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        if not temp.startswith('Messdaten IM806'):
            return False
    except:
        return False
    return True


def readIONO(filename, headonly, **kwargs):
    """
    Reading IONOMETER data to ndarray
    """
    debug = kwargs.get('debug')
    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    array = [[] for key in KEYLIST]
    #qFile= file( filename, "rb" )
    qFile= open( filename, "rt", newline='' )
    csvReader= csv.reader( qFile )
    for line in csvReader:
        elem = line[0].split(';')
        try:
            if elem[0].startswith('Messdaten'):
                el = elem[0].split()
                headers['SensorName'] = el[1].strip()
                headers['DataStandardVersion'] = el[-1].strip()
            elif elem[0].strip().startswith('IM806'):
                el = elem[0].split()
                headers['SensorSerialNum'] = el[-1].strip()
            elif elem[2] == 'Time':
                for idx,el in enumerate(elem):
                    if idx > 2:
                        key = KEYLIST[idx-2]
                        headers['unit-col-'+key] = "N"
                        headers['col-'+key] = el.strip()
            elif not headonly:
                array[0].append(date2num(datetime.strptime(elem[1]+'T'+elem[2],'%d.%m.%YT%H:%M:%S')))
                for idx,el in enumerate(elem):
                    if idx > 2:
                        ind = idx-2
                        array[ind].append(float(el))
        except:
            print ("Importing of IM806 data failed")
    qFile.close()

    # Add some Sensor specific header information
    headers['SensorDescription'] = 'Ionometer IM806'
    array = [np.asarray(el) for el in array]

    return DataStream([LineStruct()], headers, np.asarray(array))    
 
