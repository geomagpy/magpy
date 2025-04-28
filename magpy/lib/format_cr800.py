"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from io import open
from magpy.stream import *
from magpy.core.methods import testtime, extract_date_from_string
import csv


def isCR800(filename):
    """
    Checks whether a file is ASCII CR800 txt file format.
    """
    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
    except:
        return False
    try:
        elem = temp.split()
    except:
        return False
    try:
        if not elem[2] == "CR800":
            return False
    except:
        return False
    return True

def isRADON(filename):
    """
    Checks whether a file is ASCII Komma separated txt file with radon data.
    """
    debug = True
    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
    except:
        return False
    try:
        elem = temp.split(',')
    except:
        return False
    if not len(elem) == 4:
        return False
    # Date test
    try:
        testdate = datetime.strptime(elem[0],"%Y-%m-%d %H:%M:%S")
    except:
        return False
    # Voltage test
    if not 6 < float(elem[1]) < 15:
        return False
    return True

def readRADON(filename, headonly=False, **kwargs):
    """
    Reading CR800 format data.
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debug = kwargs.get('debug')
    getfile = True
    KEYLIST = DataStream().KEYLIST

    if debug:
        print ("RADON: Reading Radon data")
    theday = extract_date_from_string(filename)
    try:
        if starttime:
            if not theday[-1] >= datetime.date(testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(testtime(endtime)):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True

    # read file and split text into channels
    array = [[] for key in KEYLIST]
    stream = DataStream([],{},np.asarray(array))
    tpos = KEYLIST.index('t1')
    varpos = KEYLIST.index('var1')
    # Check whether header information is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    if getfile:
        try:
            with open(filename) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for line in csv_reader:
                    if line[0].isspace():
                        # blank line
                        pass
                    elif headonly:
                        # skip data for option headonly
                        continue
                    else:
                        time = datetime.strptime(line[0],"%Y-%m-%d %H:%M:%S")
                        array[0].append(time)
                        array[1].append(float(line[3]))
                        array[tpos].append(float(line[2]))
                        array[varpos].append(float(line[1]))
                stream.header['col-x'] = 'Counts'
                stream.header['col-t1'] = 'Temp'
                stream.header['unit-col-t1'] = 'deg'
                stream.header['col-var1'] = 'Voltage'
                stream.header['unit-col-var1'] = 'V'
                if debug:
                    print ("RADON: Successfully loaded radon data")
        except:
            headers = stream.header
            stream =[]
            if debug:
                print ("RADON: Error when reading data")

        array[0] = np.asarray(array[0]).astype(object)
        array[1] = np.asarray(array[1]).astype(object)
        array[tpos] = np.asarray(array[tpos]).astype(object)
        array[varpos] = np.asarray(array[varpos]).astype(object)

    headers['DataFormat'] = 'CR800RADON'
    headers['DataSource'] = 'Radon data from the Conrad Observatory'

    return DataStream([], headers, np.asarray(array, dtype=object))


def readCR800(filename, headonly=False, **kwargs):
    """
    Reading CR800 format data.
    """
    #starttime = kwargs.get('starttime')
    #endtime = kwargs.get('endtime')
    getfile = True

    # read file and split text into channels
    stream = DataStream()

    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    try:
        infile = open(filename, 'r', encoding='utf-8', newline='')
        CSVReader = csv.reader(infile, delimiter=' ', quotechar='|')
        for line in CSVReader:
            elem = line.split
            print(elem, len(elem))
            if line.isspace():
                # blank line
                continue
            elif headonly:
                # skip data for option headonly
                continue
            elif headonly:
                # skip data for option headonly
                continue
            else:
                #row = LineStruct()
                #row.time = date2num(datetime.strptime(elem[0]+'-'+elem[1]+'-'+elem[2]+'T'+elem[3]+':'+elem[4]+':'+elem[5],"%Y-%m-%dT%H:%M:%S.%f"))
                #row.x = float(elem[6])
                #row.y = float(elem[7])
                #row.z = float(elem[8])
                #stream.add(row)
                pass
    except:
        headers = stream.header
        stream =[]

    return DataStream(stream, headers)



def writeCR800(filename, headonly=False, **kwargs):
    """
    Writing CR800 format data.
    """
    return False
