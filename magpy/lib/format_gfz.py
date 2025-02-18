"""
MagPy
GFZ input filter
supports Kp values from the qlyymm.tab
"""

import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import DataStream, read, magpyversion
from datetime import datetime, timedelta, timezone
import numpy as np
from magpy.core.methods import test_timestring
import json
import logging
logger = logging.getLogger(__name__)
KEYLIST = DataStream().KEYLIST

KEYLIST = DataStream().KEYLIST

def isGFZINDEXJSON(filename):
    """
    Checks whether a file is JSON format.
    """
    try:
        with open(filename, 'r') as jsonfile:
            j = json.load(jsonfile)
    except:
        return False
    try:
        if not j.get("meta").get("source") == 'GFZ Potsdam':
            # Found other json - use separate filter
            return False
    except:
        return False
    return True

def readGFZINDEXJSON(filename, headonly=False, **kwargs):
    """
    Reading JSON format data.
    Example:
    data=read('https://kp.gfz-potsdam.de/app/json/?start=2024-11-01T00:00:00Z&end=2024-11-02T23:59:59Z&index=Kp&status=def')
    Options: start=2024-11-01T00:00:00Z
             end=2024-11-02T23:59:59Z
             index=Kp  # one of 'Kp', 'ap', 'Ap', 'Cp', 'C9', 'Hp30', 'Hp60', 'ap30', 'ap60', 'SN', 'Fobs', 'Fadj'
             status=def # for definitive or leave
    """
    stream = DataStream()
    header = {}
    array = [[] for key in KEYLIST]
    posskeys = ['var1','var2','var3','var4','var5']
    ind = 0

    with open(filename, 'r') as jsonfile:
        dataset = json.load(jsonfile)
        logger.info('Read: %s, Format: %s ' % (filename, "GFZINDEXJSON"))
        metadata = dataset.get('metadata',{})
        if not metadata:
            metadata = dataset.get('meta')
        datetime = dataset.get('datetime')
        status = dataset.get('status',[])
        for key in dataset:
            if not key in ["datetime","metadata","meta","status"]:
                datacol = dataset.get(key)
                data = [np.nan if x is None else float(x) for x in datacol]
                if ind < 5:
                    array[KEYLIST.index(posskeys[ind])] = data
                    header['col-'+posskeys[ind]] = key
                    header['unit-col-'+posskeys[ind]] = ''
                ind += 1
        timecol = [test_timestring(str(x)) for x in datetime]
        array[0] = timecol
        if len(status) == len(timecol):
            array[KEYLIST.index('str1')] = status

    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx],dtype=object)

    header['SensorID'] = "GFZ_{}".format(header.get("col-var1"))
    header['DataSource'] = metadata.get("source")
    if header.get("col-var1") == "Kp":
        header['DataFormat'] = 'MagPyK'
    header["DataTerms"] = metadata.get("license")
    header['DataReferences'] = 'https://kp.gfz-potsdam.de/'

    stream = DataStream(header=header,ndarray=np.asarray(array,dtype=object))

    return stream


def isGFZKP(filename):
    """
    Checks whether a file is ASCII Data format
    containing the GFZ Kp values
    """
    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
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
                cum = np.nan
                num = np.nan
                fum = np.nan
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
                array[0].append(day + timedelta(hours=dt))
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

    return DataStream(header=headers, ndarray=np.asarray(array, dtype=object))
    #return DataStream(stream, headers, np.asarray(array))


if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: GFZ FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE GFZ Indices LIBRARY.")
    print("All main methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()

    errors = {}
    successes = {}
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)

    while True:
        testset = "GFZINDEXJSON"
        try:
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            data = read('https://kp.gfz-potsdam.de/app/json/?start=2024-11-01T00:00:00Z&end=2024-11-02T23:59:59Z&index=Kp&status=def')
            m = np.round(data.mean("var1"),4)
            # agreement should be better than 0.01 nT as resolution is 0.1 nT in file
            if not m == 1.8124:
                 raise Exception("ERROR within data validity test")
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))

        testset = "GFZKP"
        try:
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            data = read(r'http://www-app3.gfz-potsdam.de/kp_index/qlyymm.tab')
            kp = data._get_column("var1")
            # agreement should be better than 0.01 nT as resolution is 0.1 nT in file
            if not len(kp) > 0:
                 raise Exception("ERROR within data validity test")
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))

        break

    t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
    time_taken = t_end_test - t_start_test
    print(datetime.now(timezone.utc).replace(tzinfo=None), "- Stream testing completed in {} s. Results below.".format(time_taken.total_seconds()))

    print()
    print("----------------------------------------------------------")
    for item in successes:
        print ("{} :     {}".format(item, successes.get(item)))
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(" {}".format(errors.keys()))
        print()
        for item in errors:
                print(item + " error string:")
                print("    " + errors.get(item))
    print()
    print("Good-bye!")
    print("----------------------------------------------------------")
