"""
MagPy
GFZ input filter
supports Kp values from the qlyymm.tab

Written by Roman Leonhardt October 2012
- contains test, read and write function for hour data
ToDo: Filter for minute data
"""

from core.magpy_stream import *


def isGFZKP(filename):
    """
    Checks whether a file is ASCII Data format
    containing the GFZ Kp values
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        testdate = datetime.strptime(temp[:6],"%y%m%d")
    except:
        return False
    if not temp[6:8] == "  ": # strip is important to remove eventual \r\n sequences or \n
        return False
    if not temp[9] in ['o','+','-']: # strip is important to remove eventual \r\n sequences or \n
        return False    
    print 'Found GFZ Kp format'
    return True


def readGFZKP(filename, headonly=False, **kwargs):
    """
    Reading GFZ format data.
    contains 3 hours Kp values with sign, cumulative Kp
    Looks like:
    121001  7o 6o 4o 2+  2+ 1+ 1o 2-   26-     34 1.3
    121002  2+ 1- 1- 3o  2+ 2- 1+ 2+   14+      7 0.4
    121003  3- 2+ 2- 1o  1- 0+ 1- 1+   11-      6 0.2
    121004  1- 1- 0+ 1-  0o 0o 0o 0+    3-      2 0.0
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True
    
    fh = open(filename, 'rt')

    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    # read file and split text into channels
    li,ld,lh,lx,ly,lz,lf = [],[],[],[],[],[],[]
    code = ''
    for line in fh:
        if line.isspace():
            # blank line
            pass
        elif headonly:
            # skip data for option headonly
            continue
        elif len(line) > 6: # hour file  
            # skip data for option headonly
            elements = line.split()
            day = datetime.strptime(elements[0],"%y%m%d")
            if len(elements) == 12:
                cum = float(elements[9].strip('o').strip('-').strip('+'))
                num = int(elements[10])
                fum = float(elements[11])
            else:
                cum = float(NaN)
                num = float(NaN)
                fum = float(NaN)
            if len(elements)>9:
                endcount = 9
            else:
                endcount = len(elements)
            for i in range (1,endcount):
                row = LineStruct()
                signval = elements[i][1:]
                if signval == 'o':
                    adderval = 0.0
                elif signval == '-':
                    adderval = -0.33333333
                elif signval == '+':
                    adderval = +0.33333333
                row.var1 = float(elements[i][:1])+adderval
                dt = i*3-1.5
                row.time = date2num(day + timedelta(hours=dt))
                row.var2 = cum
                row.var3 = num
                row.var4 = fum
                stream.add(row)
        else:
            print "Error while reading GFZ Kp format"
            pass
    fh.close()


    # header info
    headers['col-var1'] = 'Kp'
    headers['col-var2'] = 'Cumulative Kp'
    headers['col-var3'] = '?'
    headers['col-var3'] = '?'
    
    return DataStream(stream, headers)  

