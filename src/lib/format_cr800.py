"""
MagPy
Auxiliary input filter - WIC/WIK
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *


def isCR800(filename):
    """
    Checks whether a file is ASCII Lemi txt file format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    elem = temp.split()
    if not elem[2] == "CR800":
        return False
    return True


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
        CSVReader = csv.reader(open(filename, 'rb'), delimiter=' ', quotechar='|')
        for line in CSVReader:
            elem = line.split
            print elem, len(elem)
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
    pass
   
