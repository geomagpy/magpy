"""
MagPy
Intermagnet input filter
Written by Roman Leonhardt December 2012
- contains test, read and write functions for IMF 1.22,1.23
"""

from stream import *


def isIMF(filename):
    """
    Checks whether a file is ASCII IMF 1.22,1.23 minute format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not 63 <= len(temp) <= 65:  # Range which regards any variety of return 
        return False
    if temp[3] == ' ' and temp[11] == ' ' and temp[29] == ' ' and temp[45] == ' ' and temp[46] == 'R':
        pass
    else:
        return False

    print "Found IMF data"
    return True


def isIMAGCDF(filename):
    """
    Checks whether a file is ImagCDF format.
    """
    try:
        temp = cdf.CDF(filename)
    except:
        return False
    try:
        form = temp.attrs['FormatDescription']
        if not form[0].startswith('INTERMAGNET'):
            return False
    except:
        return False
    return True


def isIAF(filename):
    """
    Checks whether a file is BIN IAF INTERMAGNET Archive format.
    """

    try:
        temp = open(filename, 'rb').read(64)
        data= struct.unpack('<4s4l4s4sl4s4sll4s4sll', temp)
    except:
        return False
    try:
        date = str(data[1])
        if not len(date) == 7:
            return False         
    except:
        return False
    try:
        datetime.strptime(date,"%Y%j")
    except:
        return False

    return True


def isBLV(filename):
    """
    Checks whether a file is ASCII IMF 1.22,1.23 minute format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not 63 <= len(temp) <= 65:  # Range which regards any variety of return 
        return False
    if temp[3] == ' ' and temp[11] == ' ' and temp[29] == ' ' and temp[45] == ' ' and temp[46] == 'R':
        pass
    else:
        return False
    return True

def readIAF(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet archive data format


    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    resolution = kwargs.get('resolution')

    getfile = True
    gethead = True

    if not resolution:
        resolution = 'minutes'
    stream = DataStream()
    # Check whether header infromation is already present

    headers = {}

    data = []
    key = None

    if starttime:
        begin = stream._testtime(starttime)
    if endtime:
        end = stream._testtime(endtime)

    x,y,z,f,xho,yho,zho,fho,xd,yd,zd,fd,k,ir = [],[],[],[],[],[],[],[],[],[],[],[],[],[]
    datelist = []

    fh = open(filename, 'rb')
    while True:
      try:
        getline = True
        start = fh.read(64)
        head = struct.unpack('<4s4l4s4sl4s4sll4s4sll', start)
        date = datetime.strptime(str(head[1]),"%Y%j")
        datelist.append(date)
        if starttime:
            if date < begin:
                getline = False
        if endtime:
            if date > end:
                getline = False
        if getline:
            # unpack header
            if gethead:
                stream.header['StationIAGAcode'] = head[0].strip()
                headers['StationID'] = head[0].strip()
                #
                headers['DataAcquisitionLatitude'] = (90-float(head[2]))/1000.
                headers['DataAcquisitionLongitude'] = float(head[3])/1000.
                headers['DataElevation'] = head[4]
                headers['DataComponents'] = head[5].lower()
                for c in head[5].lower():
                    if c == 'g':
                        headers['col-df'] = 'dF'                   
                        headers['unit-col-df'] = 'nT'
                    else:
                        headers['col-'+c] = c                   
                        headers['unit-col-'+c] = 'nT'
                keystr = ','.join([c for c in head[5].lower()])
                if len(keystr) < 6:
                    keystr = keystr + ',f'
                keystr = keystr.replace('d','y')
                keystr = keystr.replace('g','df')
                keystr = keystr.replace('h','x')
                headers['StationInstitution'] = head[6]
                headers['DataConversion'] = head[7]
                headers['DataQuality'] = head[8]
                headers['SensorType'] = head[9]
                headers['StationK9'] = head[10]
                headers['DataDigitalSampling'] = head[11]
                headers['DataSensorOrientation'] = head[12].lower()
                pubdate = datetime.strptime(str(head[13]),"%y%m")
                headers['DataPublicationDate'] = pubdate
                gethead = False
            # get minute data
            xb = fh.read(5760)
            x.extend(struct.unpack('<1440l', xb))
            #x = np.asarray(struct.unpack('<1440l', xb))/10. # needs an extend
            yb = fh.read(5760)
            y.extend(struct.unpack('<1440l', yb))
            zb = fh.read(5760)
            z.extend(struct.unpack('<1440l', zb))
            fb = fh.read(5760)
            f.extend(struct.unpack('<1440l', fb))
            # get hourly means
            xhb = fh.read(96)
            xho.extend(struct.unpack('<24l', xhb))
            #xho = np.asarray(struct.unpack('<24l', xhb))/10.
            yhb = fh.read(96)
            yho.extend(struct.unpack('<24l', yhb))
            zhb = fh.read(96)
            zho.extend(struct.unpack('<24l', zhb))
            fhb = fh.read(96)
            fho.extend(struct.unpack('<24l', fhb))
            # get daily means
            xdb = fh.read(4)
            xd.extend(struct.unpack('<l', xdb))
            ydb = fh.read(4)
            yd.extend(struct.unpack('<l', ydb))
            zdb = fh.read(4)
            zd.extend(struct.unpack('<l', zdb))
            fdb = fh.read(4)
            fd.extend(struct.unpack('<l', fdb))
            kb = fh.read(32)
            k.extend(struct.unpack('<8l', kb))
            ilb = fh.read(16)
            ir.extend(struct.unpack('<4l', ilb))
      except:
        break
    fh.close()

    #x = np.asarray([val for val in x if not val > 888880])/10.   # use a pythonic way here
    x = np.asarray(x)/10.
    x[x > 88880] = float(nan)
    y = np.asarray(y)/10.
    y[y > 88880] = float(nan)
    z = np.asarray(z)/10.
    z[z > 88880] = float(nan)
    f = np.asarray(f)/10.
    f[f > 88880] = float(nan)
    f[f < -44440] = float(nan)
    xho = np.asarray(xho)/10.
    xho[xho > 88880] = float(nan)
    yho = np.asarray(yho)/10.
    yho[yho > 88880] = float(nan)
    zho = np.asarray(zho)/10.
    zho[zho > 88880] = float(nan)
    fho = np.asarray(fho)/10.
    fho[fho > 88880] = float(nan)
    fho[fho < -44440] = float(nan)
    xd = np.asarray(xd)/10.
    xd[xd > 88880] = float(nan)
    yd = np.asarray(yd)/10.
    yd[yd > 88880] = float(nan)
    zd = np.asarray(zd)/10.
    zd[zd > 88880] = float(nan)
    fd = np.asarray(fd)/10.
    fd[fd > 88880] = float(nan)
    fd[fd < -44440] = float(nan)
    k = np.asarray(k).astype(float)
    k[k > 880] = float(nan)
    ir = np.asarray(ir)

    # ndarray
    def data2array(arlist,keylist,starttime,sr):
        array = [[] for key in KEYLIST]
        ta = []
        val = starttime
        for ind, elem in enumerate(arlist[0]):
            ta.append(date2num(val))
            val = val+timedelta(seconds=sr)
        array[0] = np.asarray(ta)
        for idx,ar in enumerate(arlist):
            pos = KEYLIST.index(keylist[idx])
            array[pos] = np.asarray(ar)

        return np.asarray(array)

    if resolution in ['day','days','Day','Days','DAY','DAYS']:
        ndarray = data2array([xd,yd,zd,fd],keystr.split(','),min(datelist),sr=86400)
        headers['DataSamplingRate'] = '86400 sec'
    elif resolution in ['hour','hours','Hour','Hours','HOUR','HOURS']:
        ndarray = data2array([xho,yho,zho,fho],keystr.split(','),min(datelist)+timedelta(minutes=30),sr=3600)
        headers['DataSamplingRate'] = '3600 sec'
    elif resolution in ['k','K']:
        ndarray = data2array([k,ir],['var1','var2'],min(datelist)+timedelta(minutes=90),sr=10800)
        headers['DataSamplingRate'] = '10800 sec'
    else:
        ndarray = data2array([x,y,z,f],keystr.split(','),min(datelist),sr=60)
        headers['DataSamplingRate'] = '60 sec'

    return DataStream([LineStruct()], headers, ndarray)    


def writeIAF(datastream, filename, **kwargs):
    """
    Writing Intermagnet archive format (2.1)
    """
    
    kvals = kwargs.get('kvals')
    mode = kwargs.get('mode')

    df=False
    # Check whether data is present at all
    if not len(datastream.ndarray[0]) > 0:
        print "writeIAF: No data found - check ndarray"
        return False
    # Check whether minute file
    sr = datastream.samplingrate()
    if not int(sr) == 60:
        print "writeIAF: Minute data needs to be provided"
        return False
    # check whether data covers one month
    tdiff = int(np.round(datastream.ndarray[0][-1]-datastream.ndarray[0][0]))
    if not tdiff >= 28:
        print "writeIAF: Data needs to cover one month"
        return False

    try:
        # Convert data to XYZ if HDZ
        if datastream.header['DataComponents'].startswith('HDZ'):
            datastream = datastream.hdz2xyz()
    except:
        print "writeIAF: HeaderInfo on DataComponents seems to be missing"
        return False

    try:
        # Preserve sampling filter of original data
        dsf = datastream.header['DataSamplingFilter']
    except:
        dsf = ''

    # Check whether f is contained (or delta f)
    # if f calc delta f
    dfpos = KEYLIST.index('df')
    fpos = KEYLIST.index('f')
    dflen = len(datastream.ndarray[dfpos])
    flen = len(datastream.ndarray[fpos])
    if not dflen == len(datastream.ndarray[0]):
        #check for F and calc
        if not flen == len(datastream.ndarray[0]):
            df=False
        else:
            datastream = datastream.delta_f()
            df=True
            if datastream.header['DataComponents'] in ['HDZ','XYZ']:
                datastream.header['DataComponents'] += 'G'
            if datastream.header['DataSensorOrientation'] in ['HDZ','XYZ']:
                datastream.header['DataSensorOrientation'] += 'F'
    else:
        df=True
        if datastream.header['DataComponents'] in ['HDZ','XYZ']:
            datastream.header['DataComponents'] += 'G'
        if datastream.header['DataSensorOrientation'] in ['HDZ','XYZ']:
            datastream.header['DataSensorOrientation'] += 'F'

    # Check whether all essential header info is present
    requiredinfo = ['StationIAGAcode','StartDate','DataAcquisitionLatitude', 'DataAcquisitionLongitude', 'DataElevation', 'DataComponents', 'StationInstitution', 'DataConversion', 'DataQuality', 'SensorType', 'StationK9', 'DataDigitalSampling', 'DataSensorOrientation', 'DataPublicationDate','FormatVersion','Reserved']

    # cycle through data - day by day
    t0 = int(datastream.ndarray[0][1])
    output = ''
    kstr=[]
    for i in range(tdiff):  
        dayar = datastream._select_timerange(starttime=t0+i,endtime=t0+i+1)
        # get all indicies
        temp = DataStream([LineStruct],datastream.header,dayar)
        temp = temp.filter(filter_width=timedelta(minutes=60), resampleoffset=timedelta(minutes=30), filter_type='flat')

        head = []
        for elem in requiredinfo:
            #print "Checking", elem
            try:
                if elem == 'StationIAGAcode':
                    value = datastream.header['StationIAGAcode']
                    value = value[:3]
                    #print value
                elif elem == 'StartDate':
                    value = int(datetime.strftime(num2date(datastream.ndarray[0][1]),'%Y%j'))
                elif elem == 'DataAcquisitionLatitude':
                    if not float(datastream.header['DataAcquisitionLatitude']) < 90 and float(datastream.header['DataAcquisitionLatitude']) > -90:
                        print "Latitude and Longitude need to be provided in Degree"
                        x=1/0
                    value = int(np.round((90-float(datastream.header['DataAcquisitionLatitude']))*1000))
                elif elem == 'DataAcquisitionLongitude':
                    value = int(np.round(float(datastream.header['DataAcquisitionLongitude'])*1000))
                elif elem == 'DataElevation':
                    value = int(datastream.header['DataElevation'])
                elif elem == 'DataConversion':
                    value = int(datastream.header['DataConversion'])
                elif elem == 'DataPublicationDate':
                    value = datetime.strftime(datastream.header['DataPublicationDate'],'%y%m')
                elif elem == 'FormatVersion':
                    value = 3
                elif elem == 'Reserved':
                    value = 0
                else:
                    value = datastream.header[elem]
                head.append(value)
            except:
                print "%s missing in datastream header"
                if elem == 'DataPublicationDate':
                    print "  --  appending current date"
                    value = datetime.strftime(datetime.utcnow(),'%y%m')
                    head.append(value)
                else:
                    print "  --  critical information missing in data header  --"
                    print "  ---------------------------------------------------"
                    print " Please provide: StationIAGAcode, DataAcquisitionLatitude, "
                    print " DataAcquisitionLongitude, DataElevation, DataConversion, "
                    print " DataComponents, StationInstitution, DataQuality, SensorType, "
                    print " StationK9, DataDigitalSampling, DataSensorOrientation"
                    print " e.g. data.header['StationK9'] = 750"
                    return False

        # Constructing header Info
        #print head, len(head)
        packcode = '4s4l4s4sl4s4sll4s4sll' # fh.read(64)
        head_bin = struct.pack(packcode,*head)

        # add minute values
        packcode += '1440l' # fh.read(64)
        xvals = np.asarray(dayar[1]*10).astype(int)
        xvals = np.asarray([elem if not isnan(elem) else 999999 for elem in xvals])
        head.extend(xvals)
        packcode += '1440l' # fh.read(64)
        yvals = np.asarray(dayar[2]*10).astype(int)
        yvals = np.asarray([elem if not isnan(elem) else 999999 for elem in yvals])
        head.extend(yvals)
        packcode += '1440l' # fh.read(64)
        zvals = np.asarray(dayar[3]*10).astype(int)
        zvals = np.asarray([elem if not isnan(elem) else 999999 for elem in zvals])
        head.extend(zvals)
        packcode += '1440l' # fh.read(64)
        if df:
            dfvals = np.asarray(dayar[dfpos]*10).astype(int)
            dfvals = np.asarray([elem if not isnan(elem) else 999999 for elem in dfvals])
        else:
            dfvals = np.asarray([888888]*len(dayar[0])).astype(int)
        head.extend(dfvals)

        # add hourly means
        packcode += '24l'
        xhou = np.asarray(temp.ndarray[1]*10).astype(int)
        xhou = np.asarray([elem if not isnan(elem) else 999999 for elem in xhou])
        head.extend(xhou)
        packcode += '24l'
        yhou = np.asarray(temp.ndarray[2]*10).astype(int)
        yhou = np.asarray([elem if not isnan(elem) else 999999 for elem in yhou])
        head.extend(yhou)
        packcode += '24l'
        zhou = np.asarray(temp.ndarray[3]*10).astype(int)
        zhou = np.asarray([elem if not isnan(elem) else 999999 for elem in zhou])
        head.extend(zhou)
        packcode += '24l'
        if df:
            dfhou = np.asarray(temp.ndarray[dfpos]*10).astype(int)
            dfhou = np.asarray([elem if not isnan(elem) else 999999 for elem in dfhou])
        else:
            dfhou = np.asarray([888888]*24).astype(int)
        head.extend(dfhou)
        
        # add daily means
        packcode += '4l'
        # -- drop all values above 88888
        xvalid = np.asarray([elem for elem in xvals if elem < 888880])
        yvalid = np.asarray([elem for elem in yvals if elem < 888880])
        zvalid = np.asarray([elem for elem in zvals if elem < 888880])
        if len(xvalid)>0.9*len(xvals):
            head.append(int(np.mean(xvalid)))
        else:
            head.append(999999)
        if len(xvalid)>0.9*len(xvals):
            head.append(int(np.mean(yvalid)))
        else:
            head.append(999999)
        if len(xvalid)>0.9*len(xvals):
            head.append(int(np.mean(zvalid)))
        else:
            head.append(999999)
        if df:
            dfvalid = np.asarray([elem for elem in dfvals if elem < 88888])
            if len(dfvalid)>0.9*len(dfvals):
                head.append(int(np.mean(dfvalid)))
            else:
                head.append(999999)
        else:
            head.append(888888)

        # add k values
        if kvals:
            dayk = kvals._select_timerange(starttime=t0+i,endtime=t0+i+1)
            dayk = dayk[KEYLIST.index('var1')]
            packcode += '8l'
            if not len(dayk) == 8:
                ks = [999]*8
            else:
                ks = dayk
            sumk = int(sum(ks))
            if sumk > 999:
                sumk = 999
            linestr = "  {0}   {1}".format(datetime.strftime(num2date(t0+i),'%d-%b-%y'), datetime.strftime(num2date(t0+i),'%j'))
            tup = tuple([str(int(elem)) for elem in ks])
            linestr += "{0:>6}{1:>5}{2:>5}{3:>5}{4:>7}{5:>5}{6:>5}{7:>5}".format(*tup)
            linestr += "{0:>9}".format(str(sumk))
            kstr.append(linestr)
            head.extend(ks)
        else:
            packcode += '8l'
            ks = [999]*8
            head.extend(ks)
        # add reserved
        packcode += '4l'
        reserved = [0,0,0,0]
        head.extend(reserved)

        #print head      
        #print [num2date(elem) for elem in temp.ndarray[0]]
        line = struct.pack(packcode,*head)
        output = output + line

    path = os.path.split(filename)
    filename = os.path.join(path[0],path[1].upper())    

    if len(kstr) > 0:
        station=datastream.header['StationIAGAcode']
        k9=datastream.header['StationK9']
        lat=datastream.header['DataAcquisitionLatitude']
        lon=datastream.header['DataAcquisitionLongitude']
        year=str(int(datetime.strftime(num2date(datastream.ndarray[0][1]),'%y')))
        ye=str(int(datetime.strftime(num2date(datastream.ndarray[0][1]),'%Y')))
        kfile = os.path.join(path[0],station.upper()+year+'K.DKA')
        print "Writing k summary file:", kfile
        head = []
        if not os.path.isfile(kfile):
            head.append("{0:^66}".format(station.upper()))
            head2 = '                  Geographical latitude: {:>10} N'.format(lat)
            head3 = '                  Geographical longitude:{:>10} E'.format(lon)
            head4 = '            K-index values for {0}     (K9-limit = {1:>4} nT)'.format(ye, k9)
            head5 = '  DA-MON-YR  DAY #    1    2    3    4      5    6    7    8       SK'
            emptyline = ''
            head.append("{0:<50}".format(head2))
            head.append("{0:<50}".format(head3))
            head.append("{0:<50}".format(emptyline))
            head.append("{0:<50}".format(head4))
            head.append("{0:<50}".format(emptyline))
            head.append("{0:<50}".format(head5))
            head.append("{0:<50}".format(emptyline))
            with open(kfile, "wb") as myfile:
                for elem in head:
                    myfile.write(elem+'\r\n')
                #print elem
        # write data            
        with open(kfile, "a") as myfile:
            for elem in kstr:
                myfile.write(elem+'\r\n')
                #print elem

    print "Writing monthly IAF data format:", path[1].upper()
    if os.path.isfile(filename):
        if mode == 'append':
            with open(filename, "a") as myfile:
                myfile.write(output)
        else: # overwrite mode
            os.remove(filename)
            myfile = open(filename, "wb")
            myfile.write(output)
            myfile.close()
    else:
        myfile = open(filename, "wb")
        myfile.write(output)
        myfile.close()

    print "Creating README from header info:", path[1].upper()
    readme = True
    if readme:
        requiredhead = ['StationName','StationInstitution', 'StationStreet','StationCity','StationPostalCode','StationCountry','StationWebInfo', 'StationEmail','StationK9']
        acklist = ['StationName','StationInstitution', 'StationStreet','StationCity','StationPostalCode','StationCountry','StationWebInfo' ]
        conlist = ['StationName','StationInstitution', 'StationStreet','StationCity','StationPostalCode','StationCountry', 'StationEmail']

        for h in requiredhead:
            try:
                test = datastream.header[h]
            except:
                print ("README file could not be generated")
                print ("Info on {0} is missing".format(h))
                return True
        ack = []
        contact = []
        for a in acklist:
            try:
                ack.append("               {0}".format(datastream.header[a]))
            except:
                pass
        for c in conlist:
            try:
                contact.append("               {0}".format(datastream.header[c]))
            except:
                pass
        
        # 1. Check completeness of essential header information
        station=datastream.header['StationIAGAcode']
        stationname = datastream.header['StationName']
        k9=datastream.header['StationK9']
        lat=datastream.header['DataAcquisitionLatitude']
        lon=datastream.header['DataAcquisitionLongitude']
        ye=str(int(datetime.strftime(num2date(datastream.ndarray[0][1]),'%Y')))
        rfile = os.path.join(path[0],"README."+station.upper())
        head = []
        print "Writing README file:", rfile
        
        if not os.path.isfile(rfile):
            emptyline = ''
            head.append("{0:^66}".format(station.upper()))
            head.append("{0:<50}".format(emptyline))
            head.append("{0:>23} OBSERVATORY INFOMATION {1:>5}".format(stationname.upper(), ye))
            head.append("{0:<50}".format(emptyline))
            head.append("ACKNOWLEDGEMT: Users of {0:}-data should acknowledge:".format(station.upper()))
            for elem in ack:
                head.append(elem)
            head.append("{0:<50}".format(emptyline))
            head.append("STATION ID   : {0}".format(station.upper()))
            head.append("LOCATION     : {0}, {1}".format(datastream.header['StationCity'],datastream.header['StationCountry']))
            head.append("ORGANIZATION : {0:<50}".format(datastream.header['StationInstitution']))
            head.append("CO-LATITUDE  : {:.2} Deg.".format(90-float(lat)))
            head.append("LONGITUDE    : {:.2} Deg. E".format(float(lon)))
            head.append("ELEVATION    : {0} meters".format(int(datastream.header['DataElevation'])))
            head.append("{0:<50}".format(emptyline))
            head.append("ABSOLUTE")
            head.append("INSTRUMENTS  : please insert manually")
            head.append("RECORDING")
            head.append("VARIOMETER   : please insert manually")
            head.append("ORIENTATION  : {0}".format(datastream.header['DataSensorOrientation']))
            head.append("{0:<50}".format(emptyline))
            head.append("DYNAMIC RANGE: please insert manually")
            head.append("RESOLUTION   : please insert manually")
            head.append("SAMPLING RATE: please insert manually")
            head.append("FILTER       : {0}".format(dsf))
            # Provide method with head of kvals
            head.append("K-NUMBERS    : Computer derived (FMI method, MagPy)")
            head.append("K9-LIMIT     : {0:>4} nT".format(k9))
            head.append("{0:<50}".format(emptyline))
            head.append("GINS         : please insert manually")
            head.append("SATELLITE    : please insert manually")
            head.append("OBSERVER(S)  : please insert manually")
            head.append("ENGINEER(S)  : please insert manually")
            head.append("CONTACT      : ")
            for elem in contact:
                head.append(elem)
            with open(rfile, "wb") as myfile:
                for elem in head:
                    myfile.write(elem+'\r\n')
            myfile.close()

    return True
    

def readIMAGCDF(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet CDF format (1.1)
    """

    print "FOUND IMAGCDF"

    cdfdat = cdf.CDF(filename)
    # get Attribute list
    attrslist = [att for att in cdfdat.attrs]
    # get Data list
    datalist = [att for att in cdfdat]
    headers={}

    arraylist = []
    array = [[] for elem in KEYLIST]
    startdate = cdfdat[datalist[-1]][0]

    #  #################################
    # Get header info:
    #  #################################
    if 'FormatDescription' in attrslist:
        form = cdfdat.attrs['FormatDescription']
        headers['DataFormat'] = str(cdfdat.attrs['FormatDescription'])     
    if 'FormatVersion' in attrslist:
        vers = cdfdat.attrs['FormatVersion']
        headers['DataFormat'] = str(form) + '; ' + str(vers)       
    if 'Title' in attrslist:
        pass
    if 'IagaCode' in attrslist:
        headers['StationIAGAcode'] = str(cdfdat.attrs['IagaCode'])
        headers['StationID'] = str(cdfdat.attrs['IagaCode'])
    if 'ElementsRecorded' in attrslist:
        headers['DataComponents'] = str(cdfdat.attrs['ElementsRecorded'])
    if 'PublicationLevel' in attrslist:
        headers['DataPublicationLevel'] = str(cdfdat.attrs['PublicationLevel'])
    if 'PublicationDate' in attrslist:
        headers['DataPublicationDate'] = str(cdfdat.attrs['PublicationDate'])
    if 'ObservatoryName' in attrslist:
        headers['StationName'] = str(cdfdat.attrs['ObservatoryName'])
    if 'Latitude' in attrslist:
        headers['DataAcquisitionLatitude'] = str(cdfdat.attrs['Latitude'])
    if 'Longitude' in attrslist:
        headers['DataAcquisitionLongitude'] = str(cdfdat.attrs['Longitude'])
    if 'Elevation' in attrslist:
        headers['DataElevation'] = str(cdfdat.attrs['Elevation'])
    if 'Institution' in attrslist:
        headers['StationInstitution'] = str(cdfdat.attrs['Institution'])
    if 'VectorSensOrient' in attrslist:
        headers['DataSensorOrientation'] = str(cdfdat.attrs['VectorSensOrient'])
    if 'StandardLevel' in attrslist:
        headers['DataStandardLevel'] = str(cdfdat.attrs['StandardLevel'])
    if 'StandardName' in attrslist:
        headers['DataStandardName'] = str(cdfdat.attrs['StandardName'])
    if 'StandardVersion' in attrslist:
        headers['DataStandardVersion'] = str(cdfdat.attrs['StandardVersion'])
    if 'PartialStandDesc' in attrslist:
        headers['DataPartialStandDesc'] = str(cdfdat.attrs['PartialStandDesc'])
    if 'Source' in attrslist:
        headers['DataSource'] = str(cdfdat.attrs['Source'])
    if 'TermsOfUse' in attrslist:
        headers['DataTerms'] = str(cdfdat.attrs['TermsOfUse'])
    if 'References' in attrslist:
        headers['DataReferences'] = str(cdfdat.attrs['References'])
    if 'UniqueIdentifier' in attrslist:
        headers['DataID'] = str(cdfdat.attrs['UniqueIdentifier'])
    if 'ParentIdentifiers' in attrslist:
        headers['SensorID'] = str(cdfdat.attrs['ParentIdentifier'])
    if 'ReferenceLinks' in attrslist:
        headers['StationWebInfo'] = str(cdfdat.attrs['ReferenceLinks'])

    #  #################################
    # Get data:
    #  #################################

    # Reorder datalist and Drop time column
    # #########################################################
    # 1. Get the amount of Times columns and associated lengths
    # #########################################################
    #print "Analyzing file structure and returning values"
    #print datalist
    mutipletimerange = False
    newdatalist = []
    tllist = []
    for elem in datalist:
        if elem.endswith('Times'):
            #print "Found Time Column"
            # Get length
            tl = int(str(cdfdat[elem]).split()[1].strip('[').strip(']'))
            #print "Length", tl
            tllist.append([tl,elem])
    if len(tllist) < 1:
        #print "No time column identified"
        # Check for starttime and sampling rate in header
        if 'StartTime' in attrslist and 'SamplingPeriod' in attrslist:
            # TODO Write that function
            st = str(cdfdat.attrs['StartTime'])
            sr = str(cdfdat.attrs['SamplingPeriod'])
        else:
            print "No Time information available - aborting"
            return
    elif len(tllist) > 1:
        tl = [el[0] for el in tllist]
        if not max(tl) == min(tl):
            print "Time columns of different length. Choosing longest as basis"
            newdatalist.append(max(tllist)[1])
            mutipletimerange = True
        else:
            print "Equal length time axes found - assuming identical time"
            if 'GeomagneticVectorTimes' in datalist:
                newdatalist.append(['time','GeomagneticVectorTimes'])
            else:
                newdatalist.append(['time',tllist[0][1]]) # Take the first one
    else:
        #print "Single time axis found in file"
        newdatalist.append(['time',tllist[0][1]])

    datalist = [elem for elem in datalist if not elem.endswith('Times')]

    # #########################################################
    # 2. Sort the datalist according to KEYLIST
    # #########################################################
    for key in KEYLIST:
        possvals = [key]
        if key == 'x':
            possvals.extend(['h','i'])
        if key == 'y':
            possvals.append('d')
        if key == 'df':
            possvals.append('g')
        for elem in datalist:
            try:
                label = cdfdat[elem].attrs['LABLAXIS'].lower()
                if label in possvals:
                    newdatalist.append([key,elem])
            except:
                pass # for lines which have no Label

    if not len(datalist) == len(newdatalist)-1:
        print "error encountered in key assignment - please check"

    # 3. Create equal length array reducing all data to primary Times and filling nans for non-exist
    # (4. eventually completely drop time cols and just store start date and sampling period in header)
    # Deal with scalar data (independent or whatever
    
    for elem in newdatalist:
        if elem[0] == 'time':
            ar = date2num(cdfdat[elem[1]][...])
            arraylist.append(ar)
            ind = KEYLIST.index('time')
            array[ind] = ar
        else:
            ar = cdfdat[elem[1]][...]
            if elem[0] in NUMKEYLIST:
                ar[ar > 88880] = float(nan)
                ind = KEYLIST.index(elem[0])
                headers['col-'+elem[0]] = cdfdat[elem[1]].attrs['LABLAXIS'].lower()                
                headers['unit-col-'+elem[0]] = cdfdat[elem[1]].attrs['UNITS']
                array[ind] = ar
                arraylist.append(ar)  

    ndarray = np.array(array)

    stream = DataStream()
    stream = [LineStruct()]
    #stream = array2stream(arraylist,'time,x,y,z')

    #t2 = datetime.utcnow()
    #print "Duration for conventional stream assignment:", t2-t1

    return DataStream(stream,headers,ndarray)   


def writeIMAGCDF(datastream, filename, **kwargs):
    """
    Writing Intermagnet CDF format (1.1)
    """

    print "Writing IMAGCDF Format"
    #print "filename"
    mode = kwargs.get('mode')

    if os.path.isfile(filename+'.cdf'):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename+'.cdf')
            datastream = joinStreams(exst,datastream)
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename+'.cdf')
            datastream = joinStreams(datastream,exst)
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
        elif mode == 'append':
            mycdf = cdf.CDF(filename, filename) # append????
        else: # overwrite mode
            #print filename
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
    else:
        mycdf = cdf.CDF(filename, '')

    keylst = datastream._get_key_headers()
    tmpkeylst = ['time']
    tmpkeylst.extend(keylst)
    keylst = tmpkeylst 

    headers = datastream.header
    head, line = [],[]
    success = False

    print "Getting Header"

    ## Transfer MagPy Header to INTERMAGNET CDF attributes
    mycdf.attrs['FormatDescription'] = 'INTERMAGNET CDF format'
    mycdf.attrs['FormatVersion'] = '1.1'
    mycdf.attrs['Title'] = 'Geomagnetic time series data'
    for key in headers:
        if key == 'StationIAGAcode' or key == 'IagaCode':
            mycdf.attrs['IagaCode'] = headers[key]
        if key == 'DataComponents' or key == 'ElementsRecorded':
            mycdf.attrs['ElementsRecorded'] = headers[key]
        if key == 'DataPublicationLevel' or key == 'PublicationLevel':
            mycdf.attrs['PublicationLevel'] = headers[key]
        if key == 'DataPublicationDate' or key == 'PublicationDate':
            pubdate = datetime.strftime(datastream._testtime(headers[key]),'%Y-%m-%d %H:%M:%S')
            mycdf.attrs['PublicationDate'] = pubdate
        if key == 'StationName' or key == 'ObservatoryName':
            mycdf.attrs['ObservatoryName'] = headers[key]
        if key == 'DataAcquisitionLatitude' or key == 'Latitude':
            mycdf.attrs['Latitude'] = headers[key]
        if key == 'DataAcquisitionLongitude' or key == 'Longitude':
            mycdf.attrs['Longitude'] = headers[key]
        if key == 'DataElevation' or key == 'Elevation':
            mycdf.attrs['Elevation'] = headers[key]
        if key == 'StationInstitution' or key == 'Institution':
            mycdf.attrs['Institution'] = headers[key]
        if key == 'DataSensorOrientation' or key == 'VectorSensOrient':
            mycdf.attrs['VectorSensOrient'] = headers[key]
        if key == 'DataStandardLevel' or key == 'StandardLevel':
            mycdf.attrs['StandardLevel'] = headers[key]
        if key == 'DataStandardName' or key == 'StandardName':
            mycdf.attrs['StandardName'] = headers[key]
        if key == 'DataStandardVersion' or key == 'StandardVersion':
            mycdf.attrs['StandardVersion'] = headers[key]
        if key == 'DataPartialStandDesc' or key == 'PartialStandDesc':
            mycdf.attrs['PartialStandDesc'] = headers[key]
        if key == 'DataSource' or key == 'Source':
            mycdf.attrs['Source'] = headers[key]
        if key == 'DataTerms' or key == 'TermsOfUse':
            mycdf.attrs['TermsOfUse'] = headers[key]
        if key == 'DataReferences' or key == 'References':
            mycdf.attrs['References'] = headers[key]
        if key == 'DataID' or key == 'UniqueIdentifier':
            mycdf.attrs['UniqueIdentifier'] = headers[key]
        if key == 'SensorID'or key == 'ParentIdentifier':
            mycdf.attrs['ParentIdentifier'] = headers[key]
        if key == 'StationWebInfo' or key == 'ReferenceLinks':
            mycdf.attrs['ReferenceLinks'] = headers[key]
    if not 'StationIagaCode' in headers and 'StationID' in headers:
            mycdf.attrs['IagaCode'] = headers['StationID']
    
    def checkEqualIvo(lst):
        # http://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
        return not lst or lst.count(lst[0]) == len(lst)

    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    ndarray = False
    if len(datastream.ndarray[0]>0):
        ndarray = True

    for key in keylst:
        ind = KEYLIST.index(key)
        if ndarray and len(datastream.ndarray[ind]>0):
            col = datastream.ndarray[ind]
        else:
            col = datastream._get_column(key)
        
        if not False in checkEqual3(col):
            print "Found identical values only:", key
            col = col[:1]
        if key == 'time':
            key = 'GeomagneticVectorTimes'
            try: ## requires spacepy >= 1.5 
                mycdf[key] = np.asarray([cdf.datetime_to_tt2000(num2date(elem).replace(tzinfo=None)) for elem in col])
            except:
                mycdf[key] = np.asarray([num2date(elem).replace(tzinfo=None) for elem in col])
        elif len(col) > 0:
            cdfkey = 'GeomagneticField'+key.upper() 
            nonetest = [elem for elem in col if not elem == None]
            if len(nonetest) > 0:
                mycdf[cdfkey] = col
                
                mycdf[cdfkey].attrs['DEPEND_0'] = "GeomagneticVectorTimes"
                mycdf[cdfkey].attrs['DISPLAY_TYPE'] = "time series"
                mycdf[cdfkey].attrs['LABLAXIS'] = key.upper()
                if key in ['x','y','z','h']:
                    mycdf[cdfkey].attrs['VALIDMIN'] = -88880.0
                    mycdf[cdfkey].attrs['VALIDMAX'] = 88880.0
                elif key == 'i':
                    mycdf[cdfkey].attrs['VALIDMIN'] = -90.0
                    mycdf[cdfkey].attrs['VALIDMAX'] = 90.0
                elif key == 'd':
                    mycdf[cdfkey].attrs['VALIDMIN'] = -360.0
                    mycdf[cdfkey].attrs['VALIDMAX'] = 360.0
                elif key == 'f':
                    mycdf[cdfkey].attrs['VALIDMIN'] = 0.0
                    mycdf[cdfkey].attrs['VALIDMAX'] = 88880.0


            for keydic in headers:
                if keydic == ('col-'+key):
                    try:
                        mycdf[cdfkey].attrs['FIELDNAM'] = "Geomagnetic Field Element "+key.upper()
                    except:
                        pass
                if keydic == ('unit-col-'+key):
                    try:
                        if 'unit-col-'+key == 'deg C':
                            unit = 'Celsius'
                        elif 'unit-col-'+key == 'deg':
                            unit = 'Degrees of arc'
                        else:
                            unit = headers.get('unit-col-'+key,'')
                        mycdf[cdfkey].attrs['UNITS'] = unit
                    except:
                        pass
        success = True            
    mycdf.close()
    return success


def readIMF(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet data format (IMF1.23)
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    #if stream.header is None:
    #    headers = {}
    #else:
    #    headers = stream.header
    headers = {}
    data = []
    key = None

    # get day from filename (platform independent)
    theday = extractDateFromString(filename)
    try:
        day = datetime.strftime(theday,"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        logging.warning("Could not identify typical IAGA date in %s. Reading all ..." % daystring)
        getfile = True

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line[29] == ' ':
                # data info
                block = line.split()
                #print block
                headers['StationID'] = block[0]
                headers['DataAcquisitionLatitude'] = float(block[7][:4])/10
                headers['DataAcquisitionLongitude'] = float(block[7][4:])/10
                headers['DataComponents'] = block[4]
                headers['DataSensorAzimuth'] = float(block[8])/10/60
                headers['DataSamplingRate'] = '60 sec'
                headers['DataType'] = block[5]
                datehh = block[1] + '_' + block[3]
                #print float(block[7][:4])/10, float(block[7][4:])/10, float(block[8])/10/60
                minute = 0
            elif headonly:
                # skip data for option headonly
                return
            else:
                # data entry - may be written in multiple columns
                # row beinhaltet die Werte eine Zeile
                data = line.split()
                for i in range(2):
                    try:
                        row = LineStruct()
                        time = datehh+':'+str(minute+i)
                        row.time=date2num(datetime.strptime(time,"%b%d%y_%H:%M"))
                        index = int(4*i)
                        if not int(data[0+index]) > 999990:
                            row.x = float(data[0+index])/10
                        else:
                            row.x = float(nan)
                        if not int(data[1+index]) > 999990:
                            row.y = float(data[1+index])/10
                        else:
                            row.y = float(nan)
                        if not int(data[2+index]) > 999990:
                            row.z = float(data[2+index])/10
                        else:
                            row.z = float(nan)
                        if not int(data[3+index]) > 999990:
                            row.f = float(data[3+index])/10
                        else:
                            row.f = float(nan)
                        row.typ = block[4].lower()
                        stream.add(row)
                    except:
                        logging.error('format_imf: problem with dataformat - check block header')
                        return DataStream(stream, headers)
                minute = minute + 2

    fh.close()

    return DataStream(stream, headers)    


def writeIMF(datastream, filename, **kwargs):
    """
    Writing Intermagnet format data.
    """
    
    mode = kwargs.get('mode')
    version = kwargs.get('version')
    gin = kwargs.get('gin')
    datatype = kwargs.get('datatype')

    success = False
    # 1. check whether datastream corresponds to minute file
    if not 0.9 < datastream.get_sampling_period()*60*24 < 1.1:
        print ("format-imf: Data needs to be minute data for Intermagent - filter it accordingly")
        return False

    # 2. check whether file exists and according to mode either create, append, replace
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(datastream,exst)
            myFile= open( filename, "wb" )
        elif mode == 'append':
            myFile= open( filename, "ab" )
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= open( filename, "wb" )
    else:
        myFile= open( filename, "wb" )

    # 3. Get essential header info
    header = datastream.header
    if not gin:
        gin = 'EDI'
    if not datatype:
        datatype = 'R' # reported; can also be 'A', 'Q', 'D'
    try:
        idc = header['StationID']
    except:
        print ("format-imf: No station code specified. Setting to XYZ ...")
        idc = 'XYZ'
        #return False
    try:
        colat = 90 - float(header['DataAcquisitionLatitude'])
        longi = float(header['DataAcquisitionLongitude'])
    except:
        print ("format-imf: No location specified. Setting 99,999 ...")        
        colat = 99.9
        longi = 999.9
        #return False
    try:
        decbas = float(header['DataSensorAzimuth'])
    except:
        print ("format-imf: No orientation angle specified. Setting 999.9 ...")
        decbas = 999.9
        #return False

    # 4. Data
    dataline,blockline = '',''
    minuteprev = 0

    elemtype = 'XYZF'
    try:
        elemtpye = datastream.header['']
    except:
        pass

    fulllength = datastream.length()[0]
    ndtype = False
    if len(datastream.ndarray[0]) > 0:
        ndtype = True

    xind = KEYLIST.index('x')
    yind = KEYLIST.index('y')
    zind = KEYLIST.index('z')
    find = KEYLIST.index('f')
    for i in range(fulllength):
        if not ndtype:
            elem = datastream[i]
            elemx = elem.x
            elemy = elem.y
            elemz = elem.z
            elemf = elem.f
            timeval = elem.time
        else:
            elemx = datastream.ndarray[xind][i]
            elemy = datastream.ndarray[yind][i]
            elemz = datastream.ndarray[zind][i]
            elemf = datastream.ndarray[find][i]
            timeval = datastream.ndarray[0][i]

        date = num2date(timeval).replace(tzinfo=None)
        doy = datetime.strftime(date, "%j")
        day = datetime.strftime(date, "%b%d%y")
        hh = datetime.strftime(date, "%H")
        minute = int(datetime.strftime(date, "%M"))
        strcola = '%3.f' % (colat*10)
        strlong = '%3.f' % (longi*10)
        decbasis = str(int(np.round(decbas*60*10)))
        blockline = "%s %s %s %s %s %s %s %s%s %s %s\r\n" % (idc.upper(),day.upper(),doy, hh, elemtype, datatype, gin, strcola.zfill(4), strlong.zfill(4), decbasis.zfill(6),'RRRRRRRRRRRRRRRR')
        if minute == 0 and not i == 0:
            #print blockline
            myFile.writelines( blockline )
            pass
        if i == 0:
            #print blockline
            myFile.writelines( blockline )           
            if not minute == 0:
                j = 0
                while j < minute:
                    if j % 2: # uneven
                         #AAAAAAA_BBBBBBB_CCCCCCC_FFFFFF__AAAAAAA_BBBBBBB_CCCCCCC_FFFFFFCrLf
                        dataline += '  9999999 9999999 9999999 999999'
                    else: # even
                        dataline = '9999999 9999999 9999999 999999'
                    j = j+1
        if not isnan(elemx):
            x = elemx*10
        else:
            x = 999999 
        if not isnan(elemy):
            y = elemy*10
        else:
            y = 999999 
        if not isnan(elemz):
            z = elemz*10
        else:
            z = 999999 
        if not isnan(elemf):
            f = elemf*10
        else:
            f = 999999
        if minute > minuteprev + 1:
            while minuteprev+1 < minute:
                if minuteprev+1 % 2: # uneven
                    dataline += '  9999999 9999999 9999999 999999\r\n'
                    myFile.writelines( dataline )
                    #print minuteprev+1, dataline
                else: # even
                    dataline = '9999999 9999999 9999999 999999'
                minuteprev = minuteprev + 1 
        minuteprev = minute
        if minute % 2: # uneven
            if len(dataline) < 10: # if record starts with uneven minute then
                dataline = '9999999 9999999 9999999 999999'
            dataline += '  %7.0f%8.0f%8.0f%7.0f\r\n' % (x, y, z, f)
            myFile.writelines( dataline )
            #print minute, dataline
        else: # even
            dataline = '%7.0f%8.0f%8.0f%7.0f' % (x, y, z, f)

    minute = minute + 1
    if not minute == 59:
        while minute < 60:
            if minute % 2: # uneven
                dataline += '  9999999 9999999 9999999 999999\r\n'
                myFile.writelines( dataline )
                #print minute, dataline
            else: # even
                dataline = '9999999 9999999 9999999 999999'
            minute = minute + 1
 
    myFile.close()

    return True



def readBLV(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet data format (IMF1.23)
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
    headers = {}

    data = []
    key = None

    # get day from filename (platform independent)
    theday = extractDateFromString(filename)
    try:
        day = datetime.strftime(theday,"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        logging.warning("Could not identify typical IAGA date in %s. Reading all ..." % daystring)
        getfile = True

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line[29] == ' ':
                # data info
                block = line.split()
                #print block
                headers['StationID'] = block[0]
                headers['DataAcquisitionLatitude'] = float(block[7][:4])/10
                headers['DataAcquisitionLongitude'] = float(block[7][4:])/10
                headers['DataComponents'] = block[4]
                headers['DataSensorAzimuth'] = float(block[8])/10/60
                headers['DataSamplingRate'] = '60 sec'
                headers['DataType'] = block[5]
                datehh = block[1] + '_' + block[3]
                #print float(block[7][:4])/10, float(block[7][4:])/10, float(block[8])/10/60
                minute = 0
            elif headonly:
                # skip data for option headonly
                return
            else:
                # data entry - may be written in multiple columns
                # row beinhaltet die Werte eine Zeile
                data = line.split()
                for i in range(2):
                    try:
                        row = LineStruct()
                        time = datehh+':'+str(minute+i)
                        row.time=date2num(datetime.strptime(time,"%b%d%y_%H:%M"))
                        index = int(4*i)
                        if not int(data[0+index]) > 999990:
                            row.x = float(data[0+index])/10
                        else:
                            row.x = float(nan)
                        if not int(data[1+index]) > 999990:
                            row.y = float(data[1+index])/10
                        else:
                            row.y = float(nan)
                        if not int(data[2+index]) > 999990:
                            row.z = float(data[2+index])/10
                        else:
                            row.z = float(nan)
                        if not int(data[3+index]) > 999990:
                            row.f = float(data[3+index])/10
                        else:
                            row.f = float(nan)
                        row.typ = block[4].lower()
                        stream.add(row)
                    except:
                        logging.error('format_imf - blv: problem with dataformat - check block header')
                        return DataStream(stream, headers)
                stream.add(row)
                minute = minute + 2

    fh.close()

    return DataStream(stream, headers)    


def writeBLV(datastream, filename, **kwargs):
    """
    DESCRIPTION:
        Writing Intermagnet - baseline data.
        uses baseline function
    PARAMETERS:
        datastream	: (DataStream) basevalue data stream
        filename	: (string) path

      Optional:
        deltaF		: (float) average field difference in nT between DI pier and F 
                          measurement position. If provided, this value is assumed to
                          represent the adopted value for all days: If not, then the baseline
                          function is assumed to be used.
        diff		: (ndarray) array containing dayly averages of delta F values between
                          variometer and F measurement
    """

    baselinefunction = kwargs.get('baselinefunction')   
    fitfunc = kwargs.get('fitfunc')
    fitdegree = kwargs.get('fitdegree')
    knotstep = kwargs.get('knotstep')
    extradays = kwargs.get('extradays')
    mode = kwargs.get('mode')
    year = kwargs.get('year')
    meanh = kwargs.get('meanh')
    meanf = kwargs.get('meanf')
    keys = kwargs.get('keys')
    deltaF = kwargs.get('deltaF')
    diff = kwargs.get('diff')
    # add list for baseline data/jumps -> extract from db
    #  list contains time ranges and parameters for baselinecalc
    baseparam =  kwargs.get('baseparam')

    if not year:
        year = datetime.strftime(datetime.utcnow(),'%Y')
        t1 = date2num(datetime.strptime(str(int(year))+'-01-01','%Y-%m-%d'))
        t2 = date2num(datetime.utcnow())
    else:
        t1 = date2num(datetime.strptime(str(int(year))+'-01-01','%Y-%m-%d'))
        t2 = date2num(datetime.strptime(str(int(year)+1)+'-01-01','%Y-%m-%d'))

    if not extradays:
        extradays = 15
    if not fitfunc:
        fitfunc = 'spline'
    if not fitdegree:
        fitdegree = 5
    if not knotstep:
        knotstep = 0.1
    if not keys:
        keys = ['dx','dy','dz']#,'df']

    # 2. check whether file exists and according to mode either create, append, replace
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(datastream,exst)
            myFile= open( filename, "wb" )
        elif mode == 'append':
            myFile= open( filename, "ab" )
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= open( filename, "wb" )
    else:
        myFile= open( filename, "wb" )

    print "writeBLV: file:", filename

    # 3. check whether datastream corresponds to an absolute file and remove unreasonable inputs
    #     - check whether F measurements were performed at the main pier - delta F's are available

    try:
        if not datastream.header['DataFormat'] == 'MagPyDI':
            print "writeBLV: Format not recognized - needs to be MagPyDI"
            return False
    except:
        print "writeBLV: Format not recognized - should be MagPyDI"
        print "writeBLV: is not yet assigned during database access"
        #return False
     
    indf = KEYLIST.index('df')
    if len([elem for elem in datastream.ndarray[indf] if not np.isnan(float(elem))]) > 0:
        keys = ['dx','dy','dz','df']
    else:
        if not deltaF:
            array = np.asarray([88888.00]*len(datastream.ndarray[0]))
            datastream = datastream._put_column(array, 'df')
        else:
            array = np.asarray([deltaF]*len(datastream.ndarray[0]))
            datastream = datastream._put_column(array, 'df')

    # 4. create dummy stream with time range
    dummystream = DataStream()
    array = [[] for key in KEYLIST]
    row1, row2 = LineStruct(), LineStruct()
    row1.time = t1
    row2.time = t2
    array[0].append(row1.time)
    array[0].append(row2.time)
    indx = KEYLIST.index('dx')
    indy = KEYLIST.index('dy')
    indz = KEYLIST.index('dz')
    indf = KEYLIST.index('df')
    indFtype = KEYLIST.index('str4')
    for i in range(0,2):
        array[indx].append(0.0)
        array[indy].append(0.0)
        array[indz].append(0.0)
        array[indf].append(0.0)
    dummystream.add(row1)
    dummystream.add(row2)
    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])
    dummystream.ndarray = np.asarray(array)

    #print "1", row1.time, row2.time

    # 5. Extract the data for one year and calculate means 
    backupabsstream = datastream.copy()
    if not len(datastream.ndarray[0]) > 0:
        backupabsstream = backupabsstream.linestruct2ndarray()

    datastream = datastream.trim(starttime=t1, endtime=t2)
    try:
        comps = datastream.header['DataComponents']
        if comps in ['IDFF','idff','idf','IDF']:
            datastream = datastream.idf2xyz()
            datastream = datastream.xyz2hdz()
        elif comps in ['XYZF','xyzf','xyz','XYZ']:
            datastream = datastream.xyz2hdz()
        comps = 'HDZF'
    except:
        # assume idf orientation
        datastream = datastream.idf2xyz()
        datastream = datastream.xyz2hdz()
        comps = 'HDZF'

    if not meanf:
        meanf = datastream.mean('f')
    if not meanh:
        meanh = datastream.mean('x')

    ##### ###########################################################################
    print "TODO: cycle through parameter baseparam here"
    print " baseparam contains time ranges their valid baseline function parameters"
    print " -> necessary for discontiuous fits"
    print " join the independent year stream, and create datelist for marking jumps with d" 
    ##### ###########################################################################

    # 6. calculate baseline function
    basefunction = dummystream.baseline(backupabsstream,keys=keys, fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep,extradays=extradays)

    yar = [[] for key in KEYLIST]
    datelist = [day+0.5 for day in range(int(t1),int(t2))]
    for idx, elem in enumerate(yar):
        if idx == 0:
            yar[idx] = np.asarray(datelist)
        elif idx in [indx,indy,indz,indf]:
            yar[idx] = np.asarray([0]*len(datelist))
        else:
            yar[idx] = np.asarray(yar[idx])
   

    yearstream = DataStream([LineStruct()],datastream.header,np.asarray(yar))
    yearstream = yearstream.func2stream(basefunction,mode='addbaseline',keys=keys)

    #print "writeBLV:", yearstream.length()

    #print "writeBLV: Testing deltaF (between Pier and F):"
    #print "adopted diff is yearly average"
    #print "adopted average daily delta F comes from diff of vario and scalar"
    #pos = KEYLIST.index('df')
    #dfl = [val for val in datastream.ndarray[pos] if not isnan(val)]
    #meandf = datastream.mean('df')
    #print "Mean df", meandf, mean(dfl)

    # 7. Get essential header info
    header = datastream.header
    try:
        idc = header['StationID']
    except:
        print "formatBLV: No station code specified. Aborting ..."
        logging.error("formatBLV: No station code specified. Aborting ...")        
        return False
    headerline = '%s %5.f %5.f %s %s' % (comps.upper(),meanh,meanf,idc,year)
    myFile.writelines( headerline+'\r\n' )

    #print "writeBLV:", headerline

    # 8. Basevalues
    if len(datastream.ndarray[0]) > 0:
        for idx, elem in enumerate(datastream.ndarray[0]):
            if t2 >= elem >= t1:
                day = datetime.strftime(num2date(elem),'%j')
                x = float(datastream.ndarray[indx][idx])
                y = float(datastream.ndarray[indy][idx])*60.
                z = float(datastream.ndarray[indz][idx])
                df = float(datastream.ndarray[indf][idx])
                ftype = datastream.ndarray[indFtype][idx]
                if isnan(x):
                    x = 99999.00
                if isnan(y):
                    y = 99999.00
                if isnan(z):
                    z = 99999.00
                if isnan(df) or ftype.startswith('Fext'): 
                    df = 99999.00
                line = '%s %9.2f %9.2f %9.2f %9.2f\r\n' % (day,x,y,z,df)
                myFile.writelines( line )
    else:
        datastream = datastream.trim(starttime=t1, endtime=t2)
        for elem in datastream:
            #DDD_aaaaaa.aa_bbbbbb.bb_zzzzzz.zz_ssssss.ssCrLf
            day = datetime.strftime(num2date(elem.time),'%j')
            if isnan(elem.x):
                x = 99999.00
            else:
                if not elem.typ == 'idff':
                    x = elem.x
                else:
                    x = elem.x*60
            if isnan(elem.y):
                y = 99999.00
            else:
                if elem.typ == 'xyzf':
                    y = elem.y
                else:
                    y = elem.y*60
            if isnan(elem.z):
                z = 99999.00
            else:
                z = elem.z
            if isnan(elem.df):
                f = 99999.00
            else:
                f = elem.df
            line = '%s %9.2f %9.2f %9.2f %9.2f\r\n' % (day,x,y,z,f)
            myFile.writelines( line )

    # 9. adopted basevalues
    myFile.writelines( '*\r\n' )
    #TODO: deltaf and continuity parameter from db
    parameter = 'c' # corresponds to m
    for idx, t in enumerate(yearstream.ndarray[0]):
        #001_AAAAAA.AA_BBBBBB.BB_ZZZZZZ.ZZ_SSSSSS.SS_DDDD.DD_mCrLf
        day = datetime.strftime(num2date(t),'%j')
        if np.isnan(yearstream.ndarray[indx][idx]):
            x = 99999.00
        else:
            if not comps.lower() == 'idff':
                x = yearstream.ndarray[indx][idx]
            else:
                x = yearstream.ndarray[indx][idx]*60.
        if np.isnan(yearstream.ndarray[indy][idx]):
            y = 99999.00
        else:
            if comps.lower() == 'xyzf':
                y = yearstream.ndarray[indy][idx]
            else:
                y = yearstream.ndarray[indy][idx]*60.
        if np.isnan(yearstream.ndarray[indz][idx]):
            z = 99999.00
        else:
            z = yearstream.ndarray[indz][idx]
        if deltaF:
            f = deltaF
        elif np.isnan(yearstream.ndarray[indf][idx]):
            f = 99999.00
        else:
            f = yearstream.ndarray[indf][idx]
        if diff:
            posdf = KEYLIST.index('df')
            indext = [i for i,tpos in enumerate(diff.ndarray[0]) if num2date(tpos).date() == num2date(t).date()]
            if len(indext) > 0:
                df = diff.ndarray[posdf][indext[0]]
            else:
                df = 999.00
        else:
            df = 888.00
        line = '%s %9.2f %9.2f %9.2f %9.2f %7.2f %s\r\n' % (day,x,y,z,f,df,parameter)
        myFile.writelines( line )

    # 9. comments
    myFile.writelines( '*\r\n' )
    myFile.writelines( 'Comments:\r\n' )
    funcline1 = 'Baselinefunction: %s\r\n' % dummystream.header['DataAbsFunc']
    funcline2 = 'Degree: %s, Knots: %s\r\n' % (dummystream.header['DataAbsDegree'],dummystream.header['DataAbsKnots'])
    funcline3 = 'For adopted values the fit has been applied between\r\n'
    funcline4 = '%s and %s\r\n' % (str(num2date(dummystream.header['DataAbsMinTime']).replace(tzinfo=None)),str(num2date(dummystream.header['DataAbsMaxTime']).replace(tzinfo=None)))
    # get some data:
    infolist = [] # contains all provided information for comment section
    db = False 
    if not db:
        infolist.append(datastream[-1].str2)
        infolist.append(datastream[-1].str3)
        infolist.append(datastream[-1].str4)
        # 
    funcline5 = 'Measurements conducted primarily with:\r\n'
    funcline6 = 'DI: %s\r\n' % infolist[0]
    funcline7 = 'Scalar: %s\r\n' % infolist[1]
    funcline8 = 'Variometer: %s\r\n' % infolist[2]
    # additional text with pier, instrument, how f difference is defined, which are the instruments etc
    summaryline = '-- analysis supported by MagPy\r\n'
    myFile.writelines( '-'*40 + '\r\n' )
    myFile.writelines( funcline1 )
    myFile.writelines( funcline2 )
    myFile.writelines( funcline3 )
    myFile.writelines( funcline4 )
    myFile.writelines( funcline5 )
    myFile.writelines( funcline6 )
    myFile.writelines( funcline7 )
    myFile.writelines( '-'*40 + '\r\n' )
    myFile.writelines( summaryline )

    myFile.close()
    return True
