"""
MagPy
GFZ input filter
supports Kp values from the qlyymm.tab

Written by Roman Leonhardt October 2012
- contains test, read and write function for hour data
ToDo: Filter for minute data
"""
from __future__ import print_function

from magpy.stream import *


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
    try:
        if not temp[6:8] == "  ": # strip is important to remove eventual \r\n sequences or \n
            return False
        if not temp[9] in ['o','+','-']: # strip is important to remove eventual \r\n sequences or \n
            return False
    except:
        return False
    print('Found GFZ Kp format')
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

    logging.info(' Read: %s Format: GFZ Kp' % (filename))

    # read file and split text into channels
    li,ld,lh,lx,ly,lz,lf = [],[],[],[],[],[],[]
    code = ''
    array = [[] for key in KEYLIST]
    indvar1 = KEYLIST.index('var1')
    indvar2 = KEYLIST.index('var2')
    indvar3 = KEYLIST.index('var3')
    indvar4 = KEYLIST.index('var4')

    for line in fh:
        elements = line.split()
        getdat = True
        try:
            if len(elements[0])>4:
                day = datetime.strptime(elements[0],"%y%m%d")
                getdat = True
            else:
                getdat = False
        except:
            getdat = False
        if line.isspace():
            # blank line
            pass
        elif headonly:
            # skip data for option headonly
            continue
        elif len(line) > 6 and getdat: # hour file
            # skip data for option headonly
            elements = line.split()
            #try:
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
                #row = LineStruct()
                signval = elements[i][1:]
                if signval == 'o':
                    adderval = 0.0
                elif signval == '-':
                    adderval = -0.33333333
                elif signval == '+':
                    adderval = +0.33333333
                array[indvar1].append(float(elements[i][:1])+adderval)
                dt = i*3-1.5
                array[0].append(date2num(day + timedelta(hours=dt)))
                array[indvar2].append(cum)
                array[indvar3].append(num)
                array[indvar4].append(fum)
        elif len(line) > 6 and not getdat: # monthly mean
            if line.split()[1] == 'Mean':
                means = line.split()
                # Not used so far
                monthlymeanap = means[2]
                monthlymeancp = means[3]
            pass
        else:
            print("Error while reading GFZ Kp format")
            pass
    fh.close()


    array[0]=np.asarray(array[0])
    array[indvar1]=np.asarray(array[indvar1])
    array[indvar2]=np.asarray(array[indvar2])
    array[indvar3]=np.asarray(array[indvar3])
    array[indvar4]=np.asarray(array[indvar4])

    # header info
    headers['col-var1'] = 'Kp'
    headers['col-var2'] = 'Sum Kp'
    headers['col-var3'] = 'Ap'
    headers['col-var4'] = 'Cp'
    headers['DataSource'] = 'GFZ Potsdam'
    headers['DataFormat'] = 'MagPyK'
    headers['DataReferences'] = 'http://www-app3.gfz-potsdam.de/kp_index/'

    return DataStream([LineStruct()], headers, np.asarray(array))
    #return DataStream(stream, headers, np.asarray(array))
