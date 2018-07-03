"""
MagPy
QSPIN input filter
Written by Roman Leonhardt, Tim White June 2018
- contains test and read function, toDo: write function
"""
from __future__ import print_function

from magpy.stream import *

def isQSPIN(filename):
    """
    Checks whether a file is GSM19 format.
    """
    try:
        temp = open(filename, 'rt') #, encoding='utf-8', errors='ignore'
    except:
        return False
    try:
        li = temp.readline()
    except:
        return False
    if not li.startswith('*Start Header*'):
        return False
    return True


def readQSPIN(filename, headonly=False, **kwargs):
    """
    Reading QSPIN format.
    Basis looks like:

*Start Header*
SN: QTFA-00X
DT: 0.0049152
TS: Saturday June 23, 2018 12:56:04.68347
Line Format: <Data in nT>,<valid>,<counter>,<strength>
*End Header*
48613.0368106664,True,,99
48613.0103518634,True,,99
48613.0399724101,True,,99
48613.0531186079,True,,99
48613.0516209398,True,,99
48613.0158433131,True,,99


    """

    #print ("Found QSPIN format")
    #print ("-------------------------------------")

    timestamp = os.path.getmtime(filename)
    creationdate = datetime.fromtimestamp(timestamp)
    daytmp = datetime.strftime(creationdate,"%Y-%m-%d")
    YeT = daytmp[:2]
    ctime = creationdate

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header information is already present
    headers = {}
    array = [[] for key in KEYLIST]

    data = []
    key = None
    logging.info(' Read: %s Format: QSPIN' % (filename))
    headercoming = False
    delta = 0

    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif line.startswith('*Start Header*'):
            headercoming = True
        elif line.startswith('*End Header*'):
            headercoming = False
        elif headercoming:
            head = line.split(':')
            if head[0] == 'SN':
                #SN: QTFA-00X
                headers['SensorSerialNum'] =  head[1].strip()
                headers['SensorID'] =  "QSPIN_{}_{}".format(head[1].strip(),'0001')
            if head[0] == 'DT':
                #DT: 0.0049152
                delta = float(head[1])
            if head[0] == 'TS':
                #TS: Saturday June 23, 2018 12:56:04.68347
                tstring = "{}:{}:{}".format(head[1].strip(),head[2],head[3])
                ctime = datetime.strptime(tstring.strip(), "%A %B %d, %Y %H:%M:%S.%f")
            if head[0] == 'Line Format':
                columns = head[1].split(',')
                #Line Format: <Data in nT>,<valid>,<counter>,<strength>
                lineformat = True
        elif not headercoming and delta and lineformat:
            data = line.split(',')
            #48613.0368106664,True,,99
            ctime = ctime+timedelta(seconds=delta)
            numtime = date2num(ctime)
            if numtime > 0:
                array[0].append(numtime)
            for idx,el in enumerate(data):
                pos = 4+idx # start with f column
                try:
                    array[pos].append(float(el))
                except:
                    pass

    if len(array[0]) > 0:
        array[0] = array[0][:len(array[4])]
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    logging.info("Loaded QSPIN file")
    fh.close()

    headers['DataSamplingRate'] = delta
    headers['DataFormat'] = 'QSpin'
    array = [np.asarray(el) for el in array]

    return DataStream([LineStruct()], headers, np.asarray(array))
