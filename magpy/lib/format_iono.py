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
            if not temp.startswith('Date;Time;NegMin;'):
                return False
    except:
        return False
    return True


def readIONO(filename, headonly, **kwargs):
    """
    Reading IONOMETER data to ndarray
    Two different formats are supported:
    1. Text export
    2. FTP export
    """
    debug = kwargs.get('debug')
    stream = DataStream()
    # Check whether header infromation is already present
    headers = {}
    array = [[] for key in KEYLIST]
    #qFile= file( filename, "rb" )
    qFile= open( filename, "rt", newline='' )
    csvReader= csv.reader( qFile )
    fileformat = 'ftpexp' # 'fileexp'
    headers['SensorName'] = 'IM806'
    headers['SensorSerialNum'] = '12IM0183'
    lensum, lencnt = 0,0
    for line in csvReader:
        elem = line[0].split(';')
        try:
            if elem[0].startswith('Messdaten'):
                el = elem[0].split()
                headers['SensorName'] = el[1].strip()
                headers['DataStandardVersion'] = el[-1].strip()
                fileformat = 'fileexp'
            elif elem[0].strip().startswith('IM806'):
                el = elem[0].split()
                headers['SensorSerialNum'] = el[-1].strip()
            elif fileformat == 'fileexp' and elem[2] == 'Time':
                for idx,el in enumerate(elem):
                    if idx > 2:
                        key = KEYLIST[idx-2]
                        headers['unit-col-'+key] = "N"
                        headers['col-'+key] = el.strip()
            elif fileformat == 'ftpexp' and elem[1] == 'Time' and elem[2] == 'NegMin':
                for idx,el in enumerate(elem):
                    if idx > 1:
                        key = KEYLIST[idx-1]
                        headers['unit-col-'+key] = "N"
                        headers['col-'+key] = el.strip()
            elif fileformat == 'fileexp' and not headonly:
                array[0].append(date2num(datetime.strptime(elem[1]+'T'+elem[2],'%d.%m.%YT%H:%M:%S')))
                for idx,el in enumerate(elem):
                    if idx > 2:
                        ind = idx-2
                        array[ind].append(float(el))
            elif fileformat == 'ftpexp' and not headonly:
                array[0].append(date2num(datetime.strptime(elem[0]+'T'+elem[1],'%d.%m.%YT%H:%M:%S')))
                # Typical problem of last line -> missing elements -> pad with nan values
                lensum += len(elem)
                lencnt += 1
                avlen = int(np.round(lensum/lencnt))
                if not len(elem) == avlen:
                    elem = (elem + ['nan'] * avlen)[:avlen]
                for idx,el in enumerate(elem):
                    if idx > 1:
                        ind = idx-1
                        if el.strip() in [u'nan','']:
                            array[ind].append(np.nan)
                        elif ind > 7:
                            array[ind].append(float(el)/10.)
                        else:
                            array[ind].append(float(el))
        except:
            print ("Importing of IM806 data failed")
    qFile.close()

    # Add some Sensor specific header information
    headers['SensorDescription'] = 'Ionometer IM806'
    headers['SensorID'] = '{}_{}_0001'.format(headers.get('SensorName','None'),headers.get('SensorSerialNum','12345'))
    array = [np.asarray(el) for el in array]

    return DataStream([LineStruct()], headers, np.asarray(array))    
 
