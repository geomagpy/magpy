"""
MagPy
GSM 19 input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *

def isGSM19(filename):
    """
    Checks whether a file is GSM19 format.
    """
    try:
        temp = open(filename, 'rt')
    except:
        return False
    li = temp.readline()
    while li == '':
        li = temp.readline()
    li = temp.readline()
    if not li.startswith('Gem Systems GSM-19'):
        return False
    return True


def readGSM19(filename, headonly=False, **kwargs):
    """
    Reading GSM19 format.
    Looks like:

    Gem Systems GSM-19W 3101329 v6.0 24 X 2003 
    ID 1 file 23c-ost .b   09 VII10
    datum  48000.00


    115014.0  48484.12 99
    115015.0  48487.04 99
    115016.0  48489.92 99
    115017.0  48488.16 99
    115018.0  48486.06 99
    115019.0  48487.17 99
    115020.0  48487.44 99
    """
    timestamp = os.path.getmtime(filename)
    creationdate = datetime.fromtimestamp(timestamp)
    day = datetime.strftime(creationdate,"%Y-%m-%d")
    
    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header
    data = []
    key = None

    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif line.startswith('Gem Systems GSM-19'):
            head = line.split()
            headers['SerialNumber'] =  head[3]
            headers['Instrument'] = head[2]+head[4]
            # data header
            pass
        elif line.startswith('ID'):
            tester = line.split('.')
            typus = tester[1].split()
            # data header
            pass
        elif line.startswith('datum'):
            # data header
            pass
        elif headonly:
            # skip data for option headonly
            continue
        else:
            elem = line.split()
            if len(elem) == 3 and typus[0]=='b': # a Baseline file
                try:
                    row = LineStruct()
                    hour = elem[0][:2]
                    minute = elem[0][2:4]
                    second = elem[0][4:]
                    # add day
                    strtime = datetime.strptime(day+"T"+str(hour)+":"+str(minute)+":"+str(second),"%Y-%m-%dT%H:%M:%S.%f")
                    row.time=date2num(strtime)
                    row.f = float(elem[1])
                    row.var5 = float(elem[2])
                    stream.add(row)
                except:
                    logging.warning("Error in input data: %s - skipping bad value" % filename)
                    pass

    if len(stream) > 0:
        headers['col-f'] = 'f'
        headers['unit-col-f'] = 'nT'

    logging.info("Loaded GSM19 file of type %s, using creationdate of file (%s), %d values" % (typus[0],day,len(stream)))
    fh.close()

    return DataStream(stream, headers)    
