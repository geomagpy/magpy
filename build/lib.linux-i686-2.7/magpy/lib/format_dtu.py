"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from __future__ import print_function

from magpy.stream import *

def isDTU1(filename):
    """
    Checks whether a file is ASCII DTU (type1) format used within the DTU's FGE network
    Characteristic features are:
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        if not temp.startswith('FILENAME:    '):
            elem = temp.split()
            if len(elem) == 6:
                try:
                    testtime = datetime.strptime(elem[0],"%H:%M:%S")
                except:
                    return False
            else:
                return False
    except:
        return False
    return True



def readDTU1(filename, headonly=False, **kwargs):
    """
    Reading DTU1 format data.
    Looks like:
    FILENAME:    GDH4_20091215.sec
    INST. TYPE:  Primary magnetometer
    INSTRUMENT:  FGE S0120 E0192
    FILTER:      Electronic lowpass
    ADC:         ICP 7017 vers. B2.3
    SOFTWARE:    FG_ComData vers. 3.04
    CHANNELS:     6 Time,x,y,z,T1,T2
    TIME 1 hh:mm:ss PC clock, UT, timeserver
    x  400 nT/V variation horizontal magnetic north in nT
    y  400 nT/V variation horizontal magnetic east in nT
    z  400 nT/V variation vertical in nT
    T1 0 Kelvin/v no temp sensor on pendulum
    T2 320 Kelvin/V electronic temp in Kelvin, sensor: AD592
    DATA:
    00:00:01   124.04   134.08   -17.68   0.00   291.90
    00:00:02   124.00   134.00   -17.68   0.00   291.90
    00:00:03   124.08   134.00   -17.64   0.00   291.90
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
    daystring = daystring[0].split('_')
    print(daystring[1])

    try:
        day = datetime.strftime(datetime.strptime(daystring[1] , "%Y%m%d"),"%Y-%m-%d")
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
            elem = line.split()
            if line.isspace():
                # blank line
                pass
            elif line.startswith('FILENAME:'):
                pass
            elif line.startswith('INST. TYPE:'):
                tmp =  line.split(':')[1]
                headers['InstrumentType'] = tmp.lstrip()
            elif line.startswith('INSTRUMENT:'):
                tmp =  line.split(':')[1]
                headers['Instrument'] = tmp.lstrip()
            elif line.startswith('FILTER:'):
                tmp =  line.split(':')[1]
                headers['Filter'] = tmp.lstrip()
            elif line.startswith('ADC:'):
                tmp =  line.split(':')[1]
                headers['ADC'] = tmp.lstrip()
            elif line.startswith('SOFTWARE:'):
                tmp =  line.split(':')[1]
                headers['Software'] = tmp.lstrip()
            elif line.startswith('CHANNELS:'):
                tmp =  line.split(':')[1]
                headers['Channels'] = tmp.lstrip()
            elif line.startswith('TIME'):
                pass
            elif line.startswith('x'):
                pass
            elif line.startswith('y'):
                pass
            elif line.startswith('z'):
                pass
            elif line.startswith('T1'):
                pass
            elif line.startswith('T2'):
                pass
            elif line.startswith('DATA:'):
                pass
            elif headonly:
                # skip data for option headonly
                continue
            else:
                row = LineStruct()
                try:
                    row.time=date2num(datetime.strptime(day+'T'+elem[0],"%Y-%m-%dT%H:%M:%S"))
                    try:
                        row.x = float(elem[1])
                    except:
                        row.x = float('nan')
                    try:
                        row.y = float(elem[2])
                    except:
                        row.y = float('nan')
                    try:
                        row.z = float(elem[3])
                    except:
                        row.z = float('nan')
                    try:
                        row.t1 = float(elem[4])
                    except:
                        row.t1 = float('nan')
                    try:
                        row.t2 = float(elem[5])
                    except:
                        row.t2 = float('nan')
                except:
                    #raise ValueError, "Wrong date format in %s" % filename
                    pass
                stream.add(row)

        fh.close()
    else:
        headers = stream.header
        stream =[]

    return DataStream(stream, headers)
