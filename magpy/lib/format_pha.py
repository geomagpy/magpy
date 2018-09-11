"""
MagPy
PHA input filter
supports Kp values from the qlyymm.tab

Written by Roman Leonhardt October 2018
- contains test, read and write function for hour data
"""
from __future__ import print_function

from magpy.stream import *
import json

def isPHA(filename):
    """
    Checks whether a file is ASCII Data format
    containing the GFZ Kp values
    """
    try:
        if filename.endswith('gz'):
            import gzip
            with gzip.open(filename) as f:
                data = json.load(f)
        else:
            with open(filename) as f:
                data = json.load(f)
    except:
        return False

    try:
        if not u'Synodic_period' in data[0]:
            return False

        if not u'Aphelion_dist' in data[0]:
            return False
    except:
        return False

    print ("FOUND IT")
    return True


def readPHA(filename, headonly=False, **kwargs):
    """
    Reading PHA (Potentially Hazardous Objects) JSON format data.
    Looks like:
    p8678   17.4   0.15 K183N 313.27119  159.71202  248.25304   30.26578  0.5725990  0.30428239   2.1891969  0 MPO447062   137   4 2008-2018 0.37 M-v 3Eh MPCLINUX   9803 (518678) 2008 UZ94          20180517
    p8810   21.7   0.15 K183N 127.61617  265.25313  154.74521    7.17673  0.2699400  0.75637374   1.1930128  0 MPO447097   183   4 2010-2018 1.30 M-v 3Eh MPCALB     8803 (518810) 2010 CF19          20180523
    p8847   19.0   0.15 K183N 300.67915  220.98518  140.79248    5.00293  0.8397187  0.32826554   2.0812257  0 MPO447107    76   7 2000-2018 0.35 M-v 3Eh MPCLINUX   8803 (518847) 2010 DM            20180516

    """
    if filename.endswith('gz'):
        import gzip
        with gzip.open(filename) as f:
            data = json.load(f)
    else:
        with open(filename) as f:
            data = json.load(f)
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    logging.info(' Read: %s Format: PHA' % (filename))

    print ("Found {} objects".format(len(data)))
    #for pha in data:
    #    print (pha.get(u'Principal_desig'))        

    """
    count = 0
    found24 = False
    found25 = False
    found26 = False
    for line in fh:
        count += 1
        v01 = line[:8].strip()
        v02 = line[8:14].strip()
        v03 = line[14:20].strip()
        v04 = line[20:26].strip()
        v05 = line[26:36].strip()
        v06 = line[36:47].strip()
        v07 = line[47:58].strip()
        v08 = line[58:69].strip()
        v09 = line[69:80].strip()
        v10 = line[80:92].strip()
        v11 = line[92:104].strip()
        v12 = line[104:107].strip()
        v13 = line[107:117].strip()
        v14 = line[117:124].strip()
        v15 = line[124:127].strip()
        v16 = line[127:137].strip()
        v17 = line[137:142].strip()
        v18 = line[142:146].strip()
        v19 = line[146:150].strip()
        v20 = line[150:159].strip()
        v21 = line[159:166].strip()
        v22 = line[166:175].strip()
        v23 = line[175:194].strip()  # ObjectID: 2016 JP
        v24 = line[194:].strip()     # Date (discovery?)

        #q2684   21.1   0.15 K183N  38.62114  255.67115  202.66518   11.36631  0.3831636  0.99364975   0.9945970  3 MPO448046   340   3 2016-2018 0.36 M-v 3Eh MPCLINUX   C802 (522684) 2016 JP            20180524

        #for i in range(1,24):
        #    print (eval("v{num:02d}".format(num=i)))
        if v23.startswith("2018"):
            print (v23)
        elements = line.split()
        l = len(elements)
        if l in [24,25,26]:  # valid data
            #print (elements)
            if l == 24 and not found24:
                print ("{}: {}".format(len(elements), elements))
                found24 = True
            if l == 25 and not found25:
                print ("{}: {}".format(len(elements), elements))
                found25 = True
            if l == 26 and not found26:
                print ("{}: {}".format(len(elements), elements))
                found26 = True
            #if elements[l-2] == 'FP118':
            #    print ("RC: {}: {}".format(len(elements), elements))

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
    """
    """
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
    """

    #return DataStream([LineStruct()], headers, np.asarray(array))
    return DataStream()

