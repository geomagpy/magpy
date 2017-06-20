"""
MagPy
IAGA02 input filter
Written by Roman Leonhardt June 2012
- contains test, read and write function
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

from magpy.stream import *

def isIAGA(filename):
    """
    Checks whether a file is ASCII IAGA 2002 format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    try:
        if not temp.startswith(' Format'):
            return False
        if not 'IAGA-2002' in temp:
            return False
    except:
        return False
    return True



def readIAGA(filename, headonly=False, **kwargs):
    """
    Reading IAGA2002 format data.
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    debug = kwargs.get('debug')
    getfile = True

    array = [[] for key in KEYLIST]

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()

    # Check whether header infromation is already present
    headers = {}
    data = []
    key = None

    try:
        # get day from filename (platform independent)
        theday = extractDateFromString(filename)[0]
        day = datetime.strftime(theday,"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        logging.warning("Could not identify typical IAGA date for %s. Reading all ...".format(filename))
        getfile = True

    if getfile:
        loggerlib.info('Read: %s Format: %s ' % (filename, "IAGA2002"))
        dfpos = KEYLIST.index('df')

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
                        stream.header['StationID'] = val
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
                if key.startswith(' #'):
                    if key.find('# V-Instrument') > -1:
                        if not val == '':
                            stream.header['SensorID'] = val
                    elif key.find('# PublicationDate') > -1:
                        if not val == '':
                            stream.header['DataPublicationDate'] = val
                    else:
                        print ("formatIAGA: did not import optional header info {a}".format(a=key))
                if key.find('Data Type') > -1:
                    if not val == '':
                        if val[0] in ['d','D']:
                            stream.header['DataPublicationLevel'] = '4'
                        elif val[0] in ['q','Q']:
                            stream.header['DataPublicationLevel'] = '3'
                        elif val[0] in ['p','P']:
                            stream.header['DataPublicationLevel'] = '2'
                        else:
                            stream.header['DataPublicationLevel'] = '1'
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
                varstr = varstr[:4]
                stream.header["col-x"] = varstr[0].upper()
                stream.header["col-y"] = varstr[1].upper()
                stream.header["col-z"] = varstr[2].upper()
                stream.header["unit-col-x"] = 'nT'
                stream.header["unit-col-y"] = 'nT'
                stream.header["unit-col-z"] = 'nT'
                stream.header["unit-col-f"] = 'nT'
                if varstr.endswith('g'):
                    stream.header["unit-col-df"] = 'nT'
                    stream.header["col-df"] = 'G'
                    stream.header["col-f"] = 'F'
                else:
                    stream.header["col-f"] = 'F'
                if varstr in ['dhzf','dhzg']:
                    #stream.header["col-x"] = 'H'
                    #stream.header["col-y"] = 'D'
                    #stream.header["col-z"] = 'Z'
                    stream.header["unit-col-y"] = 'deg'
                    stream.header['DataComponents'] = 'HDZF'
                elif varstr in ['ehzf','ehzg']:
                    #stream.header["col-x"] = 'H'
                    #stream.header["col-y"] = 'E'
                    #stream.header["col-z"] = 'Z'
                    stream.header['DataComponents'] = 'HEZF'
                elif varstr in ['dhif','dhig']:
                    stream.header["col-x"] = 'I'
                    stream.header["col-y"] = 'D'
                    stream.header["col-z"] = 'F'
                    stream.header["unit-col-x"] = 'deg'
                    stream.header["unit-col-y"] = 'deg'
                    stream.header['DataComponents'] = 'IDFF'
                elif varstr in ['hdzf','hdzg']:
                    #stream.header["col-x"] = 'H'
                    #stream.header["col-y"] = 'D'
                    stream.header["unit-col-y"] = 'deg'
                    #stream.header["col-z"] = 'Z'
                    stream.header['DataComponents'] = 'HDZF'
                else:
                    #stream.header["col-x"] = 'X'
                    #stream.header["col-y"] = 'Y'
                    #stream.header["col-z"] = 'Z'
                    stream.header['DataComponents'] = 'XYZF'

            elif headonly:
                # skip data for option headonly
                continue
            elif line.startswith('%'):
                pass
            else:
                # data entry - may be written in multiple columns
                # row beinhaltet die Werte eine Zeile
                # transl. row values contains a line
                row=[]
                # Verwende das letzte Zeichen von "line" nicht, d.h. line[:-1],
                # da darin der Zeilenumbruch "\n" steht
                # transl. Do not use the last character of "line", d.h. line [:-1],
                # 				since this is the line break "\n"
                for val in line[:-1].split():
                    # nur nicht-leere Spalten hinzufuegen
                    # transl. Just add non-empty columns
                    if val.strip()!="":
                        row.append(val.strip())

                # Baue zweidimensionales Array auf
                # transl. Build two-dimensional array
                array[0].append( date2num(datetime.strptime(row[0]+'-'+row[1],"%Y-%m-%d-%H:%M:%S.%f")) )
                if float(row[3]) >= 88888.0:
                    row[3] = np.nan
                if float(row[4]) >= 88888.0:
                    row[4] = np.nan
                if float(row[5]) >= 88888.0:
                    row[5] = np.nan
                if varstr in ['dhzf','dhzg']:
                    array[1].append( float(row[4]) )
                    array[2].append( float(row[3])/60.0 )
                    array[3].append( float(row[5]) )
                elif varstr in ['ehzf','ehzg']:
                    array[1].append( float(row[4]) )
                    array[2].append( float(row[3]) )
                    array[3].append( float(row[5]) )
                elif varstr in ['dhif','dhig']:
                    array[1].append( float(row[5])/60.0 )
                    array[2].append( float(row[3])/60.0 )
                    array[3].append( float(row[6]) )
                elif varstr in ['hdzf','hdzg']:
                    array[1].append( float(row[3]) )
                    array[2].append( float(row[4])/60.0 )
                    array[3].append( float(row[5]) )
                else:
                    array[1].append( float(row[3]) )
                    array[2].append( float(row[4]) )
                    array[3].append( float(row[5]) )
                try:
                    if float(row[6]) < 88888:
                        if varstr[-1]=='f':
                            array[4].append(float(elem[6]))
                        elif varstr[-1]=='g' and varstr=='xyzg':
                            array[4].append(np.sqrt(float(row[3])**2+float(row[4])**2+float(row[5])**2) - float(row[6]))
                            array[dfpos].append(float(row[6]))
                        elif varstr[-1]=='g' and varstr in ['hdzg','dhzg','ehzg']:
                            array[4].append(np.sqrt(float(row[3])**2+float(row[5])**2) - float(row[6]))
                            array[dfpos].append(float(row[6]))
                        elif varstr[-1]=='g' and varstr in ['dhig']:
                            array[4].append(float(row[6]))
                            array[dfpos].append(float(row[6]))
                        else:
                            raise ValueError
                    else:
                        array[4].append(float('nan'))
              
                except:
                    if not float(row[6]) >= 88888:
                        array[4].append(float(row[6]))
                    else:
                        array[4].append(float('nan'))
                #data.append(row)

    fh.close()
    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])

    stream = DataStream([LineStruct()],stream.header,np.asarray(array))
    sr = stream.samplingrate()

    return stream


def writeIAGA(datastream, filename, **kwargs):
    """
    Writing IAGA2002 format data.
    """

    mode = kwargs.get('mode')
    useg = kwargs.get('useg')

    def OpenFile(filename, mode='w'):
        if sys.version_info >= (3,0,0):
            f = open(filename, mode, newline='')
        else:
            f = open(filename, mode+'b')
        return f

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = mergeStreams(exst,datastream,extend=True)
            myFile= OpenFile(filename)
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = mergeStreams(datastream,exst,extend=True)
            myFile= OpenFile(filename)
        elif mode == 'append':
            myFile= OpenFile(filename,mode='a')
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= OpenFile(filename)
    else:
        myFile= OpenFile(filename)

    header = datastream.header

    datacomp = header.get('DataComponents'," ")
    if datacomp in ['hez','HEZ','hezf','HEZF','hezg','HEZG']:
        order = [1,0,2]
        datacomp = 'EHZ'
    elif datacomp in ['hdz','HDZ','hdzf','HDZF','hdzg','HDZG']:
        order = [1,0,2]
        datacomp = 'DHZ'
    elif datacomp in ['idf','IDF','idff','IDFF','idfg','IDFG']:
        order = [1,3,0]
        datacomp = 'DHI'
    elif datacomp in ['xyz','XYZ','xyzf','XYZF','xyzg','XYZG']:
        order = [0,1,2]
        datacomp = 'XYZ'
    elif datacomp in ['ehz','EHZ','ehzf','EHZF','ehzg','EHZG']:
        order = [0,1,2]
        datacomp = 'EHZ'
    elif datacomp in ['dhz','DHZ','dhzf','DHZF','dhzg','DHZG']:
        order = [0,1,2]
        datacomp = 'DHZ'
    elif datacomp in ['dhi','DHI','dhif','DHIF','dhig','DHIG']:
        order = [0,1,2]
        datacomp = 'DHI'
    else:
        order = [0,1,2]
        datacomp = 'XYZ'

    find = KEYLIST.index('f')
    findg = KEYLIST.index('df')
    if len(datastream.ndarray[findg]) > 0:
        useg = True

    if len(datastream.ndarray[find]) > 0:
        if not useg:
            datacomp = datacomp+'F'
        else:
            datacomp = datacomp+'G'
    else:
        datacomp = datacomp+'F'

    publevel = str(header.get('DataPublicationLevel'," "))
    if publevel == '2':
        publ = 'Provisional'
    elif publevel == '3':
        publ = 'Quasi-definitive'
    elif publevel == '4':
        publ = 'Definitive'
    else:
        publ = 'Variation'

    proj = header.get('DataLocationReference','')
    longi = header.get('DataAcquisitionLongitude',' ')
    lati = header.get('DataAcquisitionLatitude',' ')
    if not longi=='' or lati=='':
        if proj == '':
            pass
        else:
            if proj.find('EPSG:') > 0:
                epsg = int(proj.split('EPSG:')[1].strip())
                if not epsg==4326:
                    longi,lati = convertGeoCoordinate(float(longi),float(lati),'epsg:'+str(epsg),'epsg:4326')

    line = []
    if not mode == 'append':
        #if header.get('Elevation') > 0:
        #    print(header)
        line.append(' Format %-15s IAGA-2002 %-34s |\n' % (' ',' '))
        line.append(' Source of Data %-7s %-44s |\n' % (' ',header.get('StationInstitution'," ")[:44]))
        line.append(' Station Name %-9s %-44s |\n' % (' ', header.get('StationName'," ")[:44]))
        line.append(' IAGA Code %-12s %-44s |\n' % (' ',header.get('StationIAGAcode'," ")[:44]))
        line.append(' Geodetic Latitude %-4s %-44s |\n' % (' ',str(lati)[:44]))
        line.append(' Geodetic Longitude %-3s %-44s |\n' % (' ',str(longi)[:44]))
        line.append(' Elevation %-12s %-44s |\n' % (' ',str(header.get('DataElevation'," "))[:44]))
        line.append(' Reported %-13s %-44s |\n' % (' ',datacomp))
        line.append(' Sensor Orientation %-3s %-44s |\n' % (' ',header.get('DataSensorOrientation'," ").upper()[:44]))
        line.append(' Digital Sampling %-5s %-44s |\n' % (' ',str(header.get('DataDigitalSampling'," "))[:44]))
        line.append(' Data Interval Type %-3s %-44s |\n' % (' ',(str(header.get('DataSamplingRate'," "))+' ('+header.get('DataSamplingFilter'," ")+')')[:44]))
        line.append(' Data Type %-12s %-44s |\n' % (' ',publ[:44]))
        if not header.get('DataPublicationDate','') == '':
            line.append(' {a:<20}   {b:<45s}|\n'.format(a='Publication date',b=str(header.get('DataPublicationDate'))[:10]))
        # Optional header part:
        skipopt = False
        if not skipopt:
            if not header.get('SensorID','') == '':
                line.append(' #{a:<20}  {b:<45s}|\n'.format(a='V-Instrument',b=header.get('SensorID')[:44]))
            if not header.get('SecondarySensorID','') == '':
                line.append(' #{a:<20}  {b:<45s}|\n'.format(a='F-Instrument',b=header.get('SecondarySensorID')[:44]))
            if not header.get('StationMeans','') == '':
                try:
                    meanlist = header.get('StationMeans') # Assume something like H:xxxx,D:xxx,Z:xxxx
                    meanlist = meanlist.split(',')
                    for me in meanlist:
                        if me.startswith('H'):
                            hval = me.split(':')
                            line.append(' #{a:<20}  {b:<45s}|\n'.format(a='Approx H',b=hval[1]))
                except:
                    pass
        line.append(' #{a:<20}  {b:<45s}|\n'.format(a='File created by',b='MagPy '+magpyversion))
        iagacode = header.get('StationIAGAcode',"")
        line.append('DATE       TIME         DOY %8s %9s %9s %9s   |\n' % (iagacode+datacomp[0],iagacode+datacomp[1],iagacode+datacomp[2],iagacode+datacomp[3]))


    try:
        myFile.writelines(line) # Write header sequence of strings to a file
    except IOError:
        pass

    try:
        line = []
        ndtype = False
        if len(datastream.ndarray[0]) > 0:
            ndtype = True

        fulllength = datastream.length()[0]

        # Possible types: DHIF, DHZF, XYZF, or DHIG, DHZG, XYZG
        #datacomp = 'EHZ'
        #datacomp = 'DHZ'
        #datacomp = 'DHI'
        #datacomp = 'XYZ'
        xmult = 1.0
        ymult = 1.0
        zmult = 1.0

        xind = order[0]+1
        yind = order[1]+1
        zind = order[2]+1
        if len(datastream.ndarray[xind]) == 0 or len(datastream.ndarray[yind]) == 0 or len(datastream.ndarray[zind]) == 0:
            print("writeIAGA02: WARNING! Data missing in X, Y or Z component! Writing anyway...")
        find = KEYLIST.index('f')
        if datacomp.startswith('DHZ'):
            xmult = 60.0
        elif datacomp.startswith('DHI'):
            xmult = 60.0
            zmult = 60.0
        for i in range(fulllength):
            if not ndtype:
                elem = datastream[i]
                xval = elem.x
                yval = elem.y
                zval = elem.z
                fval = elem.f
                timeval = elem.time
            else:
                if len(datastream.ndarray[xind]) > 0:
                    xval = datastream.ndarray[xind][i]*xmult
                else:
                    xval = 88888.0
                if len(datastream.ndarray[yind]) > 0:
                    yval = datastream.ndarray[yind][i]
                    if order[1] == '3':
                        yval = datastream.ndarray[yind][i]*np.cos(datastream.ndarray[zind][i]*np.pi/180.)
                else:
                    yval = 88888.0
                if len(datastream.ndarray[zind]) > 0:
                    zval = datastream.ndarray[zind][i]*zmult
                else:
                    zval = 88888.0
                if len(datastream.ndarray[find]) > 0:
                    if not useg:
                        fval = datastream.ndarray[find][i]
                    else:
                        fval = np.sqrt(xval**2+yval**2+zval**2)-datastream.ndarray[find][i]
                else:
                    fval = 88888.0
                timeval = datastream.ndarray[0][i]
            row = ''
            try:
                row = datetime.strftime(num2date(timeval).replace(tzinfo=None),"%Y-%m-%d %H:%M:%S.%f")
                row = row[:-3]
                doi = datetime.strftime(num2date(timeval).replace(tzinfo=None), "%j")
                row += ' %s' % str(doi)
            except:
                row = ''
                pass
            if isnan(xval):
                row += '%13.2f' % 99999.0
            else:
                row += '%13.2f' % xval
            if isnan(yval):
                row += '%10.2f' % 99999.0
            else:
                row += '%10.2f' % yval
            if isnan(zval):
                row += '%10.2f' % 99999.0
            else:
                row += '%10.2f' % zval
            if isnan(fval):
                row += '%10.2f' % 99999.0
            else:
                row += '%10.2f' % fval
            line.append(row + '\n')
        try:
            myFile.writelines( line )
            pass
        finally:
            myFile.close()
    except IOError:
        return False
        pass

    return True
