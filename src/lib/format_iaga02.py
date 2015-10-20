"""
MagPy
IAGA02 input filter
Written by Roman Leonhardt June 2012
- contains test, read and write function
"""

try:
    from stream import *
except:
    from magpy.stream import *

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

    array = [[] for key in KEYLIST]

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()

    # Check whether header infromation is already present
    headers = {}
    data = []
    key = None

    # get day from filename (platform independent)
    theday = extractDateFromString(filename)
    #print theday
    #splitpath = os.path.split(filename)
    #tmpdaystring = splitpath[1].split('.')[0]
    #daystring = re.findall(r'\d+',tmpdaystring)[0]
    #if len(daystring) >  8:
    #    daystring = daystring[:8]
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
        logging.warning("Could not identify typical IAGA date for %s. Reading all ..." % day)
        getfile = True

    if getfile:
        loggerlib.info('Read: %s Format: %s ' % (filename, "IAGA2002"))
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
                        stream.header['StationInstitution'] = val
                if key.find('Station') > -1:
                    if not val == '': 
                        stream.header['StationName'] = val
                if key.find('IAGA') > -1:
                    if not val == '': 
                        stream.header['StationIAGAcode'] = val
                if key.find('Latitude') > -1:
                    if not val == '': 
                        stream.header['DataAcquisitionLatitude'] = val
                if key.find('Longitude') > -1:
                    if not val == '': 
                        stream.header['DataAcquisitionLongitude'] = val
                if key.find('Elevation') > -1:
                    if not val == '': 
                        stream.header['DataElevation'] = val
                if key.find('Format') > -1:
                    if not val == '': 
                        stream.header['DataFormat'] = val
                if key.find('Reported') > -1:
                    if not val == '': 
                        stream.header['DataComponents'] = val
                if key.find('Orientation') > -1:
                    if not val == '': 
                        stream.header['DataSensorOrientation'] = val
                if key.find('Digital') > -1:
                    if not val == '': 
                        stream.header['DataDigitalSampling'] = val
                if key.find('Interval') > -1:
                    if not val == '': 
                        stream.header['DataSamplingFilter'] = val
                if key.find('Data Type') > -1:
                    if not val == '': 
                        stream.header['DataType'] = val
                if key.find('Publication Date') > -1:
                    if not val == '': 
                        stream.header['DataPublicationDate'] = val
            elif line.startswith('DATE'):
                # data header
                colsstr = line.lower().split()
                varstr = ''
                for it, elem in enumerate(colsstr):
                    if it > 2:
                        varstr += elem[-1]
                        if elem[-1] == 'H':
                            stream.header["col-x"] = 'H'
                            stream.header["col-y"] = 'D'
                            stream.header["col-z"] = 'Z'
                            stream.header['DataType'] = 'HDZ'    
                        elif elem[-1] == 'X':
                            stream.header["col-x"] = 'X'
                            stream.header["col-y"] = 'Y'
                            stream.header["col-z"] = 'Z'
                            stream.header['DataType'] = 'HDZ'       
                        elif elem[-1] == 'I':
                            stream.header["col-x"] = 'I'
                            stream.header["col-y"] = 'D'
                            #stream.header["col-z"] = 'Z'
                            stream.header['DataType'] = 'IDF'    
                        #colname = "col-%s" % elem[-1]
                        #colname = colname.lower()
                        #stream.header[colname] = elem[-1].lower()
                        if elem[-1].lower() in ['x','y','z','f']:
                            stream.header['unit-'+colname] = 'nT' 
                    else:
                        colname = "col-%s" % elem
                        colname = colname.lower()
                        stream.header[colname] = elem.lower()
                        if elem.lower() in ['x','y','z','f']:
                            stream.header['unit-'+colname] = 'nT'
                #if (stream.header['col-x']=='x'):
                #    stream.header['DataType'] = 'XYZ'    
                #elif (stream.header['col-h']=='h'):
                #    stream.header['DataType'] = 'HDZ'    
                #elif (stream.header['col-i']=='i'):
                #    stream.header['DataType'] = 'IDF'    
                #else:
                #    raise ValueError
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
                array[0].append( date2num(datetime.strptime(row[0]+'-'+row[1],"%Y-%m-%d-%H:%M:%S.%f")) ) 
                if varstr[:4] == 'dhzf':  
                    array[1].append( float(row[4]) )      
                    array[2].append( float(row[3]) )      
                    array[3].append( float(row[5]) )
                else:
                    array[1].append( float(row[3]) )      
                    array[2].append( float(row[4]) )      
                    array[3].append( float(row[5]) )      
                try:
                    if not float(row[6]) == 88888:
                        if stream.header['col-f']=='f':
                            array[4].append(float(elem[6]))
                        elif stream.header['col-g']=='g':
                            array[4].append(np.sqrt(row[3]**2+row[4]**2+row[5]**2) + float(row[6]))
                        else:
                            raise ValueError
                except:
                    if not float(row[6]) == 88888:
                        array[4].append(float(row[6]))
                data.append(row)

    fh.close()
    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])
    #print np.asarray(array)

    for elem in data:
        # Time conv:
        row = LineStruct()
        row.time=date2num(datetime.strptime(elem[0]+'-'+elem[1],"%Y-%m-%d-%H:%M:%S.%f"))
        xval = float(elem[3])
        yval = float(elem[4])
        zval = float(elem[5])
        try:
            if (stream.header['col-x']=='x'):
                row.x = xval
                row.y = yval
                row.z = zval
            elif (stream.header['col-h']=='h'):
                row.x, row.y, row.z = hdz2xyz(xval,yval,zval)
            elif (stream.header['col-i']=='i'):
                row.x, row.y, row.z = idf2xyz(xval,yval,zval)
            else:
                raise ValueError
            if not float(elem[6]) == 88888:
                if stream.header['col-f']=='f':
                    row.f = float(elem[6])
                elif stream.header['col-g']=='g':
                    row.f = np.sqrt(row.x**2+row.y**2+row.z**2) + float(elem[6])
                else:
                    raise ValueError
        except:
            row.x = xval
            row.y = yval
            row.z = zval
            if not float(elem[6]) == 88888:
                row.f = float(elem[6])
        stream.add(row)


    #print "Finished file reading of %s" % filename

    return DataStream(stream,stream.header,np.asarray(array))    


def writeIAGA(datastream, filename, **kwargs):
    """
    Writing IAGA2002 format data.
    """
    
    mode = kwargs.get('mode')

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

    header = datastream.header
    line = []
    if not mode == 'append':
        if header.get('Elevation') > 0:
            print header
        line.append(' Format %-15s IAGA-2002 %-34s |\n' % (' ',' '))
        line.append(' Source of Data %-7s %-44s |\n' % (' ',header.get('StationInstitution'," ")[:44]))
        line.append(' Station Name %-9s %-44s |\n' % (' ', header.get('StationName'," ")[:44]))
        line.append(' IAGA Code %-12s %-44s |\n' % (' ',header.get('StationIAGAcode'," ")[:44]))
        line.append(' Geodetic Latitude %-4s %-44s |\n' % (' ',str(header.get('DataAcquisitionLatitude'," "))[:44]))
        line.append(' Geodetic Longitude %-3s %-44s |\n' % (' ',str(header.get('DataAcquisitionLongitude'," "))[:44]))
        line.append(' Elevation %-12s %-44s |\n' % (' ',str(header.get('DataElevation'," "))[:44]))
        line.append(' Reported %-13s %-44s |\n' % (' ',header.get('DataComponents'," ")))
        line.append(' Sensor Orientation %-3s %-44s |\n' % (' ',header.get('DataSensorOrientation'," ")[:44]))
        line.append(' Digital Sampling %-5s %-44s |\n' % (' ',header.get('DataDigitalSampling'," ")[:44]))
        line.append(' Data Interval Type %-3s %-44s |\n' % (' ',(str(header.get('DataSamplingRate'," "))+' ('+header.get('DataSamplingFilter'," ")+')')[:44]))
        line.append(' Data Type %-12s %-44s |\n' % (' ',header.get('DataType'," ")[:44]))
        line.append('DATE       TIME         DOY %8s %9s %9s %9s   |\n' % (header.get('col-x',"x").upper(),header.get('col-y',"y").upper(),header.get('col-z',"z").upper(),header.get('col-f',"f").upper()))
    try:
        myFile.writelines(line) # Write header sequence of strings to a file
    except IOError:
        pass

    try:
        line = []
        #if len(datastream.ndarray[0]) > 0:


        for elem in datastream:
            row = ''
            for key in KEYLIST:
                if key == 'time':
                    try:
                        row = datetime.strftime(num2date(eval('elem.'+key)).replace(tzinfo=None), "%Y-%m-%d %H:%M:%S.%f")
                        row = row[:-3]
                        doi = datetime.strftime(num2date(eval('elem.'+key)).replace(tzinfo=None), "%j")
                        row += ' %s' % str(doi)
                    except:
                        row = ''
                        pass
                elif key == 'x':
                    if isnan(elem.x):
                        row += '%13.2f' % 88888.0
                    else:
                        row += '%13.2f' % elem.x
                elif key == 'y':
                    if isnan(elem.y):
                        row += '%10.2f' % 88888.0
                    else:
                        row += '%10.2f' % elem.y
                elif key == 'z':
                    if isnan(elem.z):
                        row += '%10.2f' % 88888.0
                    else:
                        row += '%10.2f' % elem.z
                elif key == 'f':
                    if isnan(elem.f):
                        row += '  %.2f' % 88888.0
                    else:
                        row += '  %.2f' % elem.f
            line.append(row + '\n')
        try:
            myFile.writelines( line )
            pass
        finally:
            myFile.close()
    except IOError:
        pass



