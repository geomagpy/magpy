"""
MagPy
GFZ input filter

Written by Roman Leonhardt June 2019

Supported format looks like:

 Momentanwerte der FGE-Registrierung


   Datum      Zeit         X/nT      Y/nT      Z/nT

 5. 6.2019 00:00:00    23709.53   2036.22  41051.89
 5. 6.2019 00:00:01    23709.45   2036.18  41051.98
 5. 6.2019 00:00:02    23709.43   2036.23  41051.99

"""
from __future__ import print_function

from magpy.stream import *


def isGFZTMP(filename):
    """
    Checks whether a file is ASCII Data format
    containing the GFZ Kp values
    """
    try:
        # Read first 10 lines
        datesuccess = False
        headsuccess = False
        with open(filename, 'rt' ) as file:   
            for idx in range(0,10):
                temp = file.readline()
                try:
                    testdate = datetime.strptime(temp[:19].replace(' ',''),"%d.%m.%Y%H:%M:%S")
                    datesuccess = True
                except:
                    pass
                try:
                    testdate = temp[:19].find('Datum')
                    testzeit = temp[:19].find('Zeit')
                    if testdate >= 0 and testzeit >= 0:
                        headsuccess = True
                except:
                   pass
            if datesuccess and headsuccess:
                pass
            else:
                return False
    except:
        return False

    #print('Found GFZ TMP format')
    return True


def readGFZTMP(filename, headonly=False, **kwargs):
    """
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True
    headfound = False
    datefound = False

    logging.info(' Read: %s Format: GFZ ASCII tmp' % (filename))

    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    array = [[] for key in KEYLIST]
    timear,xar,yar,zar = [],[],[],[]

    with open(filename, 'rt' ) as file:   
        for line in file:
            if not headfound and len(line)>=19:
                testdate = line[:19].find('Datum')
                testzeit = line[:19].find('Zeit')
                if testdate >= 0 and testzeit >= 0:
                    headfound = True
            elif headonly:
                continue
            if not datefound and len(line)>=19:
                try:
                    testdate = datetime.strptime(line[:19].replace(' ',''),"%d.%m.%Y%H:%M:%S")
                    datefound = True
                except:
                    pass

            if line.isspace():
                # blank line
                pass
            elif headfound and not datefound:
                #   Datum      Zeit         X/nT      Y/nT      Z/nT
                elements = line.split()
                var = ['','','x','y','z']
                for idx, el in enumerate(elements):
                    if idx > 1:
                        comp = el.split('/')
                        headers['col-{}'.format(var[idx])] = comp[0]
                        headers['unit-col-{}'.format(var[idx])] = comp[1]
            elif datefound:
                dt = datetime.strptime(line[:19].replace(' ',''),"%d.%m.%Y%H:%M:%S")
                timear.append(date2num(dt))
                vals = line[19:].split()
                xar.append(vals[0])
                yar.append(vals[1])
                zar.append(vals[2])
            else:
                pass

    array[0]=np.asarray(timear)
    array[1]=np.asarray(xar)
    array[2]=np.asarray(yar)
    array[3]=np.asarray(zar)

    # header info
    headers['SensorID'] = '{}_{}_0001_0001'.format('FGE','')
    headers['DataFormat'] = 'GPZ ASCII tmp'

    return DataStream([LineStruct()], headers, np.asarray(array))

