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
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header
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
                stream.add(row)
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

    # 1. check whether datastream corresponds to minute file
    if not 0.9 < datastream.get_sampling_period()*60*24 < 1.1:
        logging.error("format-imf: Data needs to be minute data for Intermagent - filter it accordingly")
        return

    # 2. check whether file exists and according to mode either create, append, replace
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = mergeStreams(exst,datastream,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = mergeStreams(datastream,exst,extend=True)
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
        logging.error("format-imf: No station code specified. Aborting ...")        
        return
    try:
        colat = 90 - float(header['DataAcquisitionLatitude'])
        longi = float(header['DataAcquisitionLongitude'])
    except:
        logging.error("format-imf: No location specified. Aborting ...")        
        return
    try:
        decbas = float(header['DataSensorAzimuth'])
    except:
        logging.error("format-imf: No orientation angle specified. Aborting ...")        
        return

    # 4. Data
    dataline,blockline = '',''
    minuteprev = 0

    for i, elem in enumerate(datastream):
        date = num2date(elem.time).replace(tzinfo=None)
        doy = datetime.strftime(date, "%j")
        day = datetime.strftime(date, "%b%d%y")
        hh = datetime.strftime(date, "%H")
        minute = int(datetime.strftime(date, "%M"))
        strcola = '%3.f' % (colat*10)
        strlong = '%3.f' % (longi*10)
        decbasis = str(np.round(decbas*60*10))
        blockline = "%s %s %s %s %s %s %s %s%s %s %s\r\n" % (idc.upper(),day.upper(),doy, hh, elem.typ.upper(), datatype, gin, strcola.zfill(4), strlong.zfill(4), decbasis.zfill(6),'RRRRRRRRRRRRRRRR')
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
        if not isnan(elem.x):
            x = elem.x*10
        else:
            x = 999999 
        if not isnan(elem.y):
            y = elem.y*10
        else:
            y = 999999 
        if not isnan(elem.z):
            z = elem.z*10
        else:
            z = 999999 
        if not isnan(elem.f):
            f = elem.f*10
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

    #try:
    #        myFile.writelines( dataline )
    #finally:
    #    myFile.close()
    #except IOError:
    #    pass



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
    Writing Intermagnet - baseline data.
    uses baseline function
    """
  
    fitfunc = kwargs.get('fitfunc')
    fitdegree = kwargs.get('fitdegree')
    knotstep = kwargs.get('knotstep')
    extradays = kwargs.get('extradays')
    mode = kwargs.get('mode')
    year = kwargs.get('year')
    meanh = kwargs.get('meanh')
    meanf = kwargs.get('meanf')

    if not year:
        year = datetime.strftime(datetime.utcnow(),'%Y')

    # 2. check whether file exists and according to mode either create, append, replace
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = mergeStreams(exst,datastream,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = mergeStreams(datastream,exst,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'append':
            myFile= open( filename, "ab" )
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= open( filename, "wb" )
    else:
        myFile= open( filename, "wb" )

    # 3. check whether datastream corresponds to a baseline file and remove unreasonable inputs
    datastream = datastream.extract('x',compare='<', value=700000)    

    # 4. create dummy stream with time range
    dummystream = DataStream()
    row1, row2 = LineStruct(), LineStruct()
    row1.time = date2num(datetime.strptime(str(int(year))+'-01-01','%Y-%m-%d'))
    row2.time = date2num(datetime.strptime(str(int(year)+1)+'-01-01','%Y-%m-%d'))
    dummystream.add(row1)
    dummystream.add(row2)

    # 5. Extract the data for one year and calculate means 
    datastream = datastream.trim(starttime=row1.time, endtime=row2.time)
    datatyp = datastream[0].typ
    if not meanf:
        meanf = datastream.mean('f')
    if not meanh:
        if datatyp == 'hdzf':
            meanh = datastream.mean('x')
        elif datatyp == 'xyzf':
            meanh = np.sqrt(datastream.mean('x')*datastream.mean('x') + datastream.mean('y')*datastream.mean('y'))
        elif datatyp == 'xyzf':
            pass

    backupabsstream = DataStream()
    for elem in datastream:
        backupabsstream.add(elem)

    # 6. calculate baseline function
    emptystream,basefunction = dummystream.baseline(backupabsstream,keys=['x','y','z','df'],returnfunction=True, fitfunc=fitfunc,fitdegree=fitdegree,knotstep=knotstep,extradays=extradays)

    yearstream = DataStream()
    for day in range(int(row1.time),int(row2.time)):
        date = date2num(datetime.strptime(datetime.strftime(num2date(day),'%Y-%m-%d')+'_12:00:00','%Y-%m-%d_%H:%M:%S'))
        row = LineStruct()
        row.time = date
        row.x = 0.0
        row.y = 0.0
        row.z = 0.0
        row.df = 0.0
        yearstream.add(row) 
    yearstream = yearstream.func_add(basefunction,keys=['x','y','z','df'])

    print "Year: ", yearstream[5]

    # 7. Get essential header info
    header = datastream.header
    try:
        idc = header['StationID']
    except:
        logging.error("format-imf: No station code specified. Aborting ...")        
        return
    headerline = '%s %5.f %5.f %s %s' % (datatyp.upper(),meanh,meanf,idc,year)
    myFile.writelines( headerline+'\r\n' )

    # 8. Basevalues
    for elem in datastream:
        #DDD_aaaaaa.aa_bbbbbb.bb_zzzzzz.zz_ssssss.ssCrLf
        day = datetime.strftime(num2date(elem.time),'%j')
        if isnan(elem.x):
            x = 999999.00
        else:
            if not elem.typ == 'idff':
                x = elem.x
            else:
                x = elem.x*60
        if isnan(elem.y):
            y = 999999.00
        else:
            if elem.typ == 'xyzf':
                y = elem.y
            else:
                y = elem.y*60
        if isnan(elem.z):
            z = 999999.00
        else:
            z = elem.z
        if isnan(elem.df):
            f = 999999.00
        else:
            f = elem.df
        line = '%s %9.2f %9.2f %9.2f %9.2f\r\n' % (day,x,y,z,f)
        myFile.writelines( line )

    # 9. adopted basevalues
    myFile.writelines( '*\r\n' )
    #TODO: deltaf and continuity parameter from db
    parameter = 'c' # corresponde to m
    for elem in yearstream:
        #001_AAAAAA.AA_BBBBBB.BB_ZZZZZZ.ZZ_SSSSSS.SS_DDDD.DD_mCrLf
        day = datetime.strftime(num2date(elem.time),'%j')
        if isnan(elem.x):
            x = 999999.00
        else:
            if not elem.typ == 'idff':
                x = elem.x
            else:
                x = elem.x*60
        if isnan(elem.y):
            y = 999999.00
        else:
            if elem.typ == 'xyzf':
                y = elem.y
            else:
                y = elem.y*60
        if isnan(elem.z):
            z = 999999.00
        else:
            z = elem.z
        if isnan(elem.df):
            f = 999999.00
        else:
            f = elem.df
        df = 9999.00
        line = '%s %9.2f %9.2f %9.2f %9.2f %7.2f %s\r\n' % (day,x,y,z,f,df,parameter)
        myFile.writelines( line )

    # 9. comments
    myFile.writelines( '*\r\n' )
    myFile.writelines( 'Comments:\r\n' )
    funcline1 = 'Baselinefunction: %s\r\n' % emptystream.header['DataAbsFunc']
    funcline2 = 'Degree: %s, Knots: %s\r\n' % (emptystream.header['DataAbsDegree'],emptystream.header['DataAbsKnots'])
    funcline3 = 'For adopted values the fit has been applied between\r\n'
    funcline4 = '%s and %s\r\n' % (emptystream.header['DataAbsMinTime'],emptystream.header['DataAbsMaxTime'])
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

