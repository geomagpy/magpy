"""
MagPy
MagPy input/output filters
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

# Specify what methods are really needed
from magpy.stream import *

import datetime
import dateutil.parser


def check_date(date_string):
        dt = False
        try:
            dt = dateutil.parser.parse(date_string)
        except ValueError:
            #print("Invalid isoformat string: {}".format(date_string))
            dt = False
        return dt


def isCSV(filename):
    """
    Checks whether a file is BASIC csv file
    """
    #print ("Checking CSV")
    lines_to_check = 40
    try:
        with open(filename, newline='') as csvfile:
            start = csvfile.read(4096)
            # isprintable does not allow newlines, printable does not allow umlauts...
            if not all([c in string.printable or c.isprintable() for c in start]):
                return False
            dialect = csv.Sniffer().sniff(start)            
    except csv.Error:
        # Could not get a csv dialect -> probably not a csv.
        return False   
    except:
        return False   

    try:
        # Testing comma separation
        N = start.count(',')
        if N < 5:
            return False
    except:
        return False

    try:
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            datelst = []
            for idx,row in enumerate(csv_reader):
                if len(row) > 0 and idx < lines_to_check:
                    dt = check_date(row[0])
                    datelst.append(dt)
        if not any(datelst):
            return False
    except:
        return False

    logger.debug("format_magpy: Found BASIC CSV file {}".format(filename))
    return True


def readCSV(filename, headonly=False, **kwargs):
    """
    #DT_datatime,N_latency[ms],N_download[Mbyte/s],N_upload[Mbyte/s],N_serverdistance[km],S_sever,S_location
    #2021-09-20T00:05:02.628946Z,12.585,71.92494097727986,36.592751994082455,52.214518722142806,JStorfingerDE,Munich
    #2021-09-20T00:10:02.852509Z,12.867,73.2147567273973,37.220394235557094,52.214518722142806,InterNetX GmbH,Munich
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debug = kwargs.get('debug')

    getfile = True
    
    theday = extractDateFromString(filename)
    try:
        if starttime:
            if not theday[-1] >= datetime.date(DataStream()._testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(DataStream()._testtime(endtime)):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True

    if getfile:
      with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        fulldata = []
        header = []
        comments = {}
        for row in csv_reader:
            if len(row) > 0:
                dt = check_date(row[0])
                if row[0].startswith('#'):
                    # split, remove spaces
                    print ("Found comment")
                    line = ",".join(row)
                    lin = line.replace('#','').replace(' ','').replace('\t','').strip()
                    li = lin.split(':')
                    try:
                        comments[li[0]] = li[1]
                    except:
                        pass
                elif dt:
                    #print ("Found data line")
                    data = [date2num(dt.replace(tzinfo=None))]
                    dat = [el for idx,el in enumerate(row) if idx > 0]
                    data.extend(dat)
                    fulldata.append(data)
                else:
                    #print ("Found header")
                    header.append(row)
        # Convert header
        #  use only first line
        #  assign each index to a specific key of DATASTREAM
        numkeys = NUMKEYLIST
        strkeys = ['str1','str2','str3']
        head = header[0]
        assign = {}
        numN = 0
        strN = 0
        for idx,el in enumerate(head):
            headel = el.split('_')
            typus = headel[0]
            elementunit = headel[1].replace("]",'').split("[")
            if typus in ['time','Epoch','DT']:
                typus = 'time'
                assign[idx] = 'time'
            else:
                #print (typus, elementunit)
                if typus == 'N':
                    assign[idx] = numkeys[numN]
                    comments['col-{}'.format(numkeys[numN])] = elementunit[0]
                    comments['unit-col-{}'.format(numkeys[numN])] = elementunit[1]
                    numN += 1
                if typus == 'S':
                    assign[idx] = strkeys[strN]
                    comments['col-{}'.format(strkeys[strN])] = elementunit[0]
                    strN += 1
        
        # Convert data
        #print (fulldata)
        numpy_array = np.array(fulldata)
        transp = numpy_array.T
        transpose_list = transp.tolist()
        array = [[] for el in KEYLIST]
        for idx,el in enumerate(transpose_list):
            pos = KEYLIST.index(assign[idx])
            try:
                array[pos] = np.asarray(el, dtype=float)
            except:
                array[pos] = np.asarray(el, dtype=object)
        array = np.asarray([np.asarray(el).astype(object) for el in array], dtype=object)

        return DataStream(header=comments,ndarray=array)



def writeCSV(datastream, filename, kind='simple',returnstring = False,**kwargs):
    """
    Function to write basic CSV data
    """

    mode = kwargs.get('mode')  # simple (no meta), full (with meta)
    # kind  = 'simple' # simple (simple header), normal, full (with meta)

    #logger.info("writeBASICCSV: Writing file to %s" % filename)

    if not len(datastream.ndarray[0]) > 0:
        return False


    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream,extend=True)
            if sys.version_info >= (3,0,0):
                myFile = open(filename, 'w', newline='')
            else:
                myFile = open(filename, 'wb')
        elif mode == 'replace': # replace existing inputs
            logger.debug("write ascii filename", filename)
            exst = read(path_or_url=filename)
            datastream = joinStreams(datastream,exst,extend=True)
            if sys.version_info >= (3,0,0):
                myFile = open(filename, 'w', newline='')
            else:
                myFile = open(filename, 'wb')
        elif mode == 'append':
            if sys.version_info >= (3,0,0):
                myFile = open(filename, 'a', newline='')
            else:
                myFile = open(filename, 'ab')
        else:
            if sys.version_info >= (3,0,0):
                myFile = open(filename, 'w', newline='')
            else:
                myFile = open(filename, 'wb')
    elif filename.find('StringIO') > -1 and not os.path.isfile(filename):
        if sys.version_info >= (3,0,0):
            import io
            myFile = io.StringIO()
            returnstring = True
        else:
            import StringIO
            myFile = StringIO.StringIO()
            returnstring = True
    else:
        if sys.version_info >= (3,0,0):
            myFile = open(filename, 'w', newline='')
        else:
            myFile = open(filename, 'wb')

    wtr= csv.writer( myFile )

    headdict = datastream.header
    head, line = [],[]
    keylst = datastream._get_key_headers()
    keylst[:0] = ['time']

    if not mode == 'append':
        for key in keylst:
            typus = 'S'
            unit = ''
            if key in NUMKEYLIST:
                typus = 'N'
                u = headdict.get('unit-col-'+key,'')
                if u:
                    unit = '[{}]'.format(u)
            if kind == 'simple':
                title = "{}{}".format(headdict.get('col-'+key,'-'), unit)
                tst = ''
            else:
                title = "{}_{}{}".format(typus, headdict.get('col-'+key,'-'), unit)
                tst = 'DT_'
            head.append(title)
        head[0] = '{}Time'.format(tst)
        wtr.writerow( head )
    if len(datastream.ndarray[0]) > 0:
        for i in range(len(datastream.ndarray[0])):
            row = []
            for idx,el in enumerate(datastream.ndarray):
                if len(datastream.ndarray[idx]) > 0:
                    if KEYLIST[idx].find('time') >= 0:
                        row.append((num2date(float(el[i]))).replace(tzinfo=None).isoformat()+'Z')
                        #row.append(datetime.strftime(num2date(float(el[i])).replace(tzinfo=None), "%Y-%m-%dT%H:%M:%S.%f") )
                    else:
                        if not KEYLIST[idx] in NUMKEYLIST: # Get String and replace all non-standard ascii characters
                            try:
                                valid_chars='-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
                                el[i] = ''.join([e for e in list(el[i]) if e in list(valid_chars)])
                            except:
                                pass
                        row.append(el[i])
            wtr.writerow(row)

    if returnstring:
        return myFile
    myFile.close()
    return filename

