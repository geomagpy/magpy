"""
MagPy
IAGA02 input filter
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *


def isIAGA(filename):
    """
    Checks whether a file is ASCII IAGA 2002 format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith(' Format'):
        return False
    if not 'IAGA-2002' in temp:
        return False
    return True



def readIAGA(filename, headonly=False, **kwargs):
    """
    Reading IAGA2002 format data.
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
    splitpath = os.path.split(filename)
    tmpdaystring = splitpath[1].split('.')[0]
    daystring = re.findall(r'\d+',tmpdaystring)[0]
    if len(daystring) >  8:
        daystring = daystring[:8]
    try:
        day = datetime.strftime(datetime.strptime(daystring, "%Y%m%d"),"%Y-%m-%d")
    except:
        logging.warning("Wrong dateformat in Filename %s" % daystring)
        return []
    # Select only files within eventually defined time range
    if starttime:
        if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False
    if endtime:
        if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
            getfile = False

    if getfile:
        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line.startswith(' '):
                # data info
                infoline = line[:-4]
                key = infoline[:23].strip()
                val = infoline[23:].strip()
                if key.find('Source') > -1:
                    if not val == '': 
                        headers['Institution'] = val
                if key.find('Station') > -1:
                    if not val == '': 
                        headers['Station'] = val
                if key.find('IAGA') > -1:
                    if not val == '': 
                        headers['IAGAcode'] = val
                if key.find('Latitude') > -1:
                    if not val == '': 
                        headers['Latitude'] = val
                if key.find('Longitude') > -1:
                    if not val == '': 
                        headers['Longitude'] = val
                if key.find('Elevation') > -1:
                    if not val == '': 
                        headers['Elevation'] = val
                if key.find('Format') > -1:
                    if not val == '': 
                        headers['DataFormat'] = val
                if key.find('Reported') > -1:
                    if not val == '': 
                        headers['Reported'] = val
                if key.find('Orientation') > -1:
                    if not val == '': 
                        headers['Orientation'] = val
                if key.find('Digital') > -1:
                    if not val == '': 
                        headers['DigitalSamplingInterval'] = val
                if key.find('Interval') > -1:
                    if not val == '': 
                        headers['DigitalFilter'] = val
                if key.find('Data Type') > -1:
                    if not val == '': 
                        headers['ProvidedType'] = val
            elif line.startswith('DATE'):
                # data header
                colsstr = line.lower().split()
                for it, elem in enumerate(colsstr):
                    if it > 2:
                        colname = "col-%s" % elem[-1]
                        colname = colname.lower()
                        headers[colname] = elem[-1].lower()
                        if elem[-1].lower() in ['x','y','z','f']:
                            headers['unit-'+colname] = 'nT' 
                    else:
                        colname = "col-%s" % elem
                        colname = colname.lower()
                        headers[colname] = elem.lower()
                        if elem.lower() in ['x','y','z','f']:
                            headers['unit-'+colname] = 'nT'
            elif headonly:
                # skip data for option headonly
                continue
            elif line.startswith('%'):
                pass
            else:
                # data entry - may be written in multiple columns
                # row beinhaltet die Werte eine Zeile
                row=[]
                # Verwende das letzte Zeichen von "line" nicht, d.h. line[:-1],
                # da darin der Zeilenumbruch "\n" steht
                for val in string.split(line[:-1]):
                    # nur nicht-leere Spalten hinzufuegen
                    if string.strip(val)!="":
                        row.append(string.strip(val))
                # Baue zweidimensionales Array auf       
                data.append(row)

    fh.close()

    for elem in data:
        # Time conv:
        row = LineStruct()
        row.time=date2num(datetime.strptime(elem[0]+'-'+elem[1],"%Y-%m-%d-%H:%M:%S.%f"))
        xval = float(elem[3])
        yval = float(elem[4])
        zval = float(elem[5])
        if (headers['col-x']=='x'):
            row.x = xval
            row.y = yval
            row.z = zval
        elif (headers['col-h']=='h'):
            row.x, row.y, row.z = hdz2xyz(xval,yval,zval)
        elif (headers['col-i']=='i'):
            row.x, row.y, row.z = idf2xyz(xval,yval,zval)
        else:
            raise ValueError
        if not float(elem[6]) == 88888:
            if headers['col-f']=='f':
                row.f = float(elem[6])
            elif headers['col-g']=='g':
                row.f = np.sqrt(row.x**2+row.y**2+row.z**2) + float(elem[6])
            else:
                raise ValueError
        stream.add(row)

    """
    Speed optimization:
    Change the whole thing to column operations


    col = ColStruct(len(data))
    for idx, elem in enumerate(data):
        # Time conv:
        xxx = col.time
        col.time[idx] = (date2num(datetime.strptime(elem[0]+'-'+elem[1],"%Y-%m-%d-%H:%M:%S.%f")))
        xval = float(elem[3])
        yval = float(elem[4])
        zval = float(elem[5])
        if (headers['col-x']=='x'):
            col.x[idx] = xval
            col.y[idx] = yval
            col.z[idx] = zval
        elif (headers['col-h']=='h'):
            col.x[idx], col.y[idx], col.z[idx] = hdz2xyz(xval,yval,zval)
        elif (headers['col-i']=='i'):
            col.x[idx], col.y[idx], col.z[idx] = idf2xyz(xval,yval,zval)
        else:
            raise ValueError
        if not float(elem[6]) == 88888:
            if headers['col-f']=='f':
                col.f[idx] = float(elem[6])
            elif headers['col-g']=='g':
                col.f[idx] = np.sqrt(row.x**2+row.y**2+row.z**2) + float(elem[6])
            else:
                raise ValueError

    arraystream = np.asarray(col)
    try:
        print len(col.time)
        print "got it"
    except:
        pass
    stream = col
    """

    return DataStream(stream, headers)    


def writeIAGA(filename, headonly=False, **kwargs):
    """
    Writing IAGA2002 format data.
    """
    mode = kwargs.get('mode')

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = pmRead(path_or_url=filename)
            datastream = mergeStreams(exst,datastream,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'replace': # replace existing inputs
            exst = pmRead(path_or_url=filename)
            datastream = mergeStreams(datastream,exst,extend=True)
            myFile= open( filename, "wb" )
        elif mode == 'append':
            myFile= open( filename, "ab" )
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= open( filename, "wb" )
    else:
        myFile= open( filename, "wb" )

    wtr= csv.writer( myFile )
    headdict = datastream.header
    head, line = [],[]
    if not mode == 'append':
        try:
            val = '%-48s' % header['DataFormat']
        except:
            val = ''
        line = ' Format                 ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['Institution']
        except:
            val = ''
        line = ' Source of Data         ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['Station']
        except:
            val = ''
        line = ' Station Name           ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['IAGAcode']
        except:
            val = ''
        line = ' IAGA Code              ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['Latitude']
        except:
            val = ''
        line = ' Geodetic Latitude      ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['Longitude']
        except:
            val = ''
        line = ' Geodetic Longitude     ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['Elevation']
        except:
            val = ''
        line = ' Elevation              ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['Reported']
        except:
            val = ''
        line = ' Reported               ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['Orinetation']
        except:
            val = ''
        line = ' Sensor Orientation     ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['DigitalSampling']
        except:
            val = ''
        line = ' Digital Sampling       ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['DataFomat']
        except:
            val = ''
        line = ' Data Interval Type     ' + val[:48] + '|'
        wtr.writerow( line )
        try:
            val = '%-48s' % header['ProvidedType']
        except:
            val = ''
        line = ' Data Type              ' + val[:48] + '|'
        wtr.writerow( line )
        for key in KEYLIST:
            title = headdict.get('col-'+key,'-') + '[' + headdict.get('unit-col-'+key,'') + ']'
            head.append(title)
        wtr.writerow( head )
        wtr.writerow( ['# data:'] )
    for elem in datastream:
        row = []
        for key in KEYLIST:
            if key.find('time') >= 0:
                try:
                    row.append( datetime.strftime(num2date(eval('elem.'+key)).replace(tzinfo=None), "%Y-%m-%dT%H:%M:%S.%f") )
                except:
                    row.append( float('nan') )
                    pass
            else:
                row.append(eval('elem.'+key))
        wtr.writerow( row )
    myFile.close()



