"""
MagPy
IAGA02 input filter
Written by Roman Leonhardt June 2012
- contains test, read and write function
"""
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
#from magpy.stream import *
from magpy.stream import DataStream, read, magpyversion, join_streams, subtract_streams
from magpy.core.methods import testtime, convert_geo_coordinate, extract_date_from_string
import os
from datetime import datetime, timedelta, timezone
import time
import numpy as np
import logging

#global variables
logger = logging.getLogger(__name__)
KEYLIST = DataStream().KEYLIST
MISSING_DATA = 99999
NOT_REPORTED = 88888


def LeapTime(t):
    """
    converts strings to datetime, considering leap seconds
    """
    nofrag, frag = t.split('.')
    if len(frag) < 6:  # IAGA string has only millisecond resolution:
        frag = frag.ljust(6, '0')
    nofrag_dt = time.strptime(nofrag, "%Y-%m-%dT%H:%M:%S")
    ts = datetime(*nofrag_dt[:5]+(min([nofrag_dt[5], 59]),))
    #ts = datetime.fromtimestamp(time.mktime(nofrag_dt))
    dt = ts.replace(microsecond=int(frag))
    return dt


def isIAGA(filename):
    """
    Checks whether a file is ASCII IAGA 2002 format.
    """
    try:
        with open(filename, 'rt') as myfi:
            temp = myfi.readline()
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


def splittexttolines(text, linelength):
    """
    DESCRIPTION:
        Internal method which splits provided comments into IAGA 2002 conform lines.
        Returns a list of individual lines which do not exceed the given linelength.
    EXAMPLE:
        comment = "De Bello Gallico\nJulius Caesar\nGallia est omnis divisa in partes tres, quarum unam incolunt Belgae, aliam Aquitani, tertiam qui ipsorum lingua Celtae, nostra Galli appellantur. Hi omnes lingua, institutis, legibus inter se differunt. Gallos ab Aquitanis Garumna flumen, a Belgis Matrona et Sequana dividit. Horum omnium fortissimi sunt Belgae, propterea quod a cultu atque humanitate provinciae longissime absunt, minimeque ad eos mercatores saepe commeant atque ea quae ad effeminandos animos pertinent important, proximique sunt Germanis, qui trans Rhenum incolunt, quibuscum continenter bellum gerunt. Qua de causa Helvetii quoque reliquos Gallos virtute praecedunt, quod fere cotidianis proeliis cum Germanis contendunt, cum aut suis finibus eos prohibent aut ipsi in eorum finibus bellum gerunt. Eorum una, pars, quam Gallos obtinere dictum est, initium capit a flumine Rhodano, continetur Garumna flumine, Oceano, finibus Belgarum, attingit etiam ab Sequanis et Helvetiis flumen Rhenum, vergit ad septentriones."
        output = splittexttolines(comment, 64)
    """
    newline = ''
    linelst = text.split('\n')
    output = []
    for line in linelst:
        wordlst = line.split()
        for word in wordlst:
            if len(newline+word)+1 < linelength:
                if newline == '':
                    newline = word
                else:
                    newline = newline+" "+word
            else:
                output.append(newline)
                newline = word
                if not len(newline) < linelength:
                    newline = word[:linelength-1]
        output.append(newline)
        newline = ''
    return output


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

    # Check whether header information is already present
    stream.header = {}
    data = []
    key = None
    varstr = ''
    elem = 0
    comment = ''

    try:
        # get day from filename (platform independent)
        theday = extract_date_from_string(filename)[0]
        day = datetime.strftime(theday,"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        logging.warning("Could not identify typical IAGA date for %s. Reading all ...".format(filename))
        getfile = True

    if getfile:
        logger.info('Read: %s Format: %s ' % (filename, "IAGA2002"))
        dfpos = KEYLIST.index('df')
        comment = ''

        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line.startswith(' '):
                # data info
                infoline = line[:-4]
                key = infoline[:23].strip()
                val = infoline[23:].strip()
                if not line.startswith(' #'):
                    # main part
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
                    if key.find('Data Type') > -1:
                        if not val == '':
                            if val[0] in ['d','D','4']:
                                stream.header['DataPublicationLevel'] = '4'
                            elif val[0] in ['q','Q','3']:
                                stream.header['DataPublicationLevel'] = '3'
                            elif val[0] in ['p','P','2','a','A']:
                                stream.header['DataPublicationLevel'] = '2'
                            else:
                                stream.header['DataPublicationLevel'] = '1'
                    if key.find('Publication Date') > -1:
                        if not val == '':
                            stream.header['DataPublicationDate'] = val
                else:
                    # optional comment part
                    #if key.find('# V-Instrument') > -1:
                    #    if not val == '':
                    #        stream.header['SensorID'] = val
                    if key.find('# PublicationDate') > -1:
                        if not val == '':
                            stream.header['DataPublicationDate'] = val
                    elif key.find('# K9-limit') > -1 or key.find('# StationK9') > -1 :
                        if not val == '':
                            try:
                                stream.header['StationK9'] = int(val)
                            except:
                                pass
                    elif key.find('# D-conversion factor') > -1:
                        if not val == '':
                            try:
                                stream.header['DataConversion'] = int(val)
                            except:
                                pass
                    elif key.find('# F-Instrument') > -1:
                        pass
                    elif key.find('# File created') > -1:
                        pass
                    else:
                        comment += line.strip(' #').replace('|','').strip()+' '
                        #print ("formatIAGA: did not import optional header info {a}".format(a=key))
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
                #print ("VAR", varstr)
                if varstr in ['dhzf','dhzg']:
                    # Please note: positions of D and H are exchanged while reading from data set below
                    stream.header["col-x"] = 'H'
                    stream.header["col-y"] = 'D'
                    stream.header["col-z"] = 'Z'
                    stream.header["unit-col-y"] = 'deg'
                    stream.header['DataComponents'] = 'HDZF'
                elif varstr in ['ehzf','ehzg']:
                    # consider the different order in the file
                    stream.header["col-x"] = 'H'
                    stream.header["col-y"] = 'E'
                    stream.header["col-z"] = 'Z'
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
                # transl. row values contains a line
                row=[]
                # transl. Do not use the last character of "line", d.h. line [:-1],
                # 				since this is the line break "\n"
                for val in line[:-1].split():
                    # transl. Just add non-empty columns
                    if val.strip()!="":
                        row.append(val.strip())

                # Build two-dimensional array
                timestring = row[0]+'T'+row[1]
                #t = '2012-06-30T23:59:60.209215'
                array[0].append( LeapTime(timestring) )

                if float(row[3]) >= NOT_REPORTED:
                    row[3] = np.nan
                if float(row[4]) >= NOT_REPORTED:
                    row[4] = np.nan
                if float(row[5]) >= NOT_REPORTED:
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
                    if float(row[6]) < NOT_REPORTED:
                        # dF = F(vector) - F(scalar) according to IM technical mamual
                        if varstr[-1]=='f':
                            array[4].append(float(elem[6]))
                        elif varstr[-1]=='g' and varstr=='xyzg':
                            array[4].append(np.sqrt(float(row[3])**2+float(row[4])**2+float(row[5])**2) - float(row[6]))
                            array[dfpos].append(float(row[6]))
                        elif varstr[-1]=='g' and varstr in ['hdzg']:
                            array[4].append(np.sqrt(float(row[3])**2+float(row[5])**2) - float(row[6]))
                            array[dfpos].append(float(row[6]))
                        elif varstr[-1] == 'g' and varstr in ['dhzg', 'ehzg']:
                            array[4].append(np.sqrt(float(row[4]) ** 2 + float(row[5]) ** 2) - float(row[6]))
                            array[dfpos].append(float(row[6]))
                        elif varstr[-1]=='g' and varstr in ['dhig']:
                            array[4].append(float(row[6]))
                            array[dfpos].append(float(row[6]))
                        else:
                            raise ValueError
                    else:
                        array[4].append(np.nan)
                        if varstr[-1] in ['g']:
                            array[dfpos].append(np.nan)
                except:
                    if not float(row[6]) >= NOT_REPORTED:
                        array[4].append(float(row[6]))
                    else:
                        array[4].append(np.nan)
                #data.append(row)

    fh.close()

    # New in 0.3.99 - provide a SensorID as well consisting of IAGA code, min
    # and numerical publevel
    #  IAGA code
    try:
        # if path is a url the following will not be optimal
        fileext = ""
        if filename.find("sampling_period") > 0:
            fsp = filename.split("sampling_period=")
            if fsp[1].startswith("60"):
                fileext = "min"
            if fsp[1].startswith("1"):
                fileext = "sec"
            if fsp[1].startswith("3600"):
                fileext = "hou"
        elif filename.count(".") == 1 and len(filename) < 30 and not filename.find("&")>-1:
            tmp, fileext = os.path.splitext(filename)
        stream.header['SensorID'] = stream.header.get('StationIAGAcode','NoCode').upper()+fileext.replace('.','')+'_'+stream.header.get('DataPublicationLevel','0')+'_0001'
        stream.header['DataID'] = "{}_0001".format(stream.header['SensorID'])
    except:
        pass

    if not comment == '':
        stream.header['DataComments'] = comment
    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])

    stream = DataStream(header=stream.header,ndarray=np.asarray(array,dtype=object))

    return stream


def writeIAGA(datastream, filename, **kwargs):
    """
    Writing IAGA2002 format data.
    """
    mode = kwargs.get('mode')
    useg = kwargs.get('useg')
    window = ''
    returnstring = False
    def OpenFile(filename, mode='w'):
        if sys.version_info >= (3,0,0):
            f = open(filename, mode, newline='')
        else:
            f = open(filename, mode+'b')
        return f

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(exst,datastream,extend=True)
            myFile= OpenFile(filename)
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(datastream,exst,extend=True)
            myFile= OpenFile(filename)
        elif mode == 'append':
            myFile= OpenFile(filename,mode='a')
        else: # overwrite mode
            #os.remove(filename)  ?? necessary ??
            myFile= OpenFile(filename)
    elif filename.find('StringIO') > -1 and not os.path.exists(filename):
        if sys.version_info >= (3,0,0):
            import io
            myFile = io.StringIO()
            returnstring = True
        else:
            from io import StringIO
            myFile = StringIO()
            returnstring = True
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

    gavail, favail = False, False
    if len(datastream.ndarray[findg]) > 0:
        gavail = True
    if len(datastream.ndarray[find]) > 0:
        favail = True

    if gavail:
        datacomp = datacomp+'G'
    elif favail and useg:
        datacomp = datacomp+'G'
    else:
        datacomp = datacomp+'F'

    publevel = str(header.get('DataPublicationLevel',""))
    if publevel in ['2','P','Provisional','provisional']:
        publ = 'provisional'
    elif publevel in ['3','Q','Quasi-definitive','quasi-definitive','Quasidefinitive','quasidefinitive']:
        publ = 'quasi-definitive'
    elif publevel in ['4','D','Definitive','definitive']:
        publ = 'definitive'
    elif publevel in ['0','','1','V','variation','Variation',None]:
        publ = 'variation'
    else:
        publ = publevel

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
                    longi,lati = convert_geo_coordinate(float(longi),float(lati),'epsg:'+str(epsg),'epsg:4326')

    if not header.get('StationIAGAcode','') == '':
        iagacode = header.get('StationIAGAcode','')
    else:
        iagacode = header.get('StationID','')

    filtercomment = ''
    if not header.get('DataIntervalType','') == '':
        intervaltype = header.get('DataIntervalType',' ')
    else:
        sr = datastream.samplingrate()
        filteradd = ''
        if sr in [1,'1','1.0']:
            interval = '1-second'
            window = '501-1500'
            filteradd = ' centered on the second'
        elif sr in [60,'60','60.0']:
            interval = '1-minute'
            window = '0.30-1.29'
            filteradd = ' centered on the minute'
        elif sr in [3600,'3600','3600.0']:
            interval = '1-hour'
            window = '00-59'
            filteradd = ' centered on half hour'
        elif sr in [86400,'86400','86400.0',86401,'86401','86401.0']:
            interval = '1-day'
            window = '00-23'
            filteradd = ' centered on noon'
        elif sr > 86400:
            interval = '1-month'
            window = '00-31'
        elif sr in ['']:
            interval = ' '
        else:
            interval = str(sr) +'-seconds'

        intervaltype = interval+' ('+window+')'

        sf = header.get('DataSamplingFilter','')
        sflist = sf.split()

        if len(sflist) > 3:
            #try:
            filtercomment = '{} filter with {} {} passband{}'.format(sflist[0],sflist[-2],sflist[-1],filteradd)
            #except:
            #    filtercomment = ''
        else:
            filtercomment = ''

    line = []
    if not mode == 'append':
        #if header.get('Elevation') > 0:
        #    print(header)
        line.append(' Format %-15s IAGA-2002 %-34s |\r\n' % (' ',' '))
        line.append(' Source of Data %-7s %-44s |\r\n' % (' ',header.get('StationInstitution'," ")[:44]))
        line.append(' Station Name %-9s %-44s |\r\n' % (' ', header.get('StationName'," ")[:44]))
        line.append(' IAGA Code %-12s %-44s |\r\n' % (' ',iagacode[:44]))
        line.append(' Geodetic Latitude %-4s %-44s |\r\n' % (' ',str(lati)[:44]))
        line.append(' Geodetic Longitude %-3s %-44s |\r\n' % (' ',str(longi)[:44]))
        line.append(' Elevation %-12s %-44s |\r\n' % (' ',str(header.get('DataElevation'," "))[:44]))
        line.append(' Reported %-13s %-44s |\r\n' % (' ',datacomp))
        line.append(' Sensor Orientation %-3s %-44s |\r\n' % (' ',header.get('DataSensorOrientation'," ").upper()[:44]))
        line.append(' Digital Sampling %-5s %-44s |\r\n' % (' ',str(header.get('DataDigitalSampling'," "))[:44]))
        line.append(' Data Interval Type %-3s %-44s |\r\n' % (' ',intervaltype[:44]))
        line.append(' Data Type %-12s %-44s |\r\n' % (' ',publ[:44]))
        if not header.get('DataPublicationDate','') == '':
            line.append(' {a:<20}   {b:<45s}|\r\n'.format(a='Publication date',b=str(header.get('DataPublicationDate'))[:10]))
        # Optional header part:
        skipopt = False
        if not skipopt:
            if not filtercomment == '':
                output = splittexttolines(filtercomment, 64)
                for el in output:
                    line.append(' # {a:<66s}|\r\n'.format(a=el))
            if not header.get('DataConversion','') == '':
                line.append(' # {a:<19}  {b:<45s}|\r\n'.format(a='D-conversion factor',b=str(header.get('DataConversion'))[:44]))
            if not header.get('StationK9','') == '':
                line.append(' # {a:<19}  {b:<45s}|\r\n'.format(a='K9-limit',b=str(header.get('StationK9'))[:44]))
            if not header.get('DataComments','') == '':
                output = splittexttolines(header.get('DataComments',''), 64)
                for el in output:
                    line.append(' # {a:<66s}|\r\n'.format(a=el))
            if not header.get('SensorID','') == '':
                line.append(' # {a:<19}  {b:<45s}|\r\n'.format(a='V-Instrument',b=header.get('SensorID')[:44]))
            if not header.get('SecondarySensorID','') == '':
                line.append(' # {a:<19}  {b:<45s}|\r\n'.format(a='F-Instrument',b=header.get('SecondarySensorID')[:44]))
            if not header.get('StationMeans','') == '':
                try:
                    meanlist = header.get('StationMeans') # Assume something like H:xxxx,D:xxx,Z:xxxx
                    meanlist = meanlist.split(',')
                    for me in meanlist:
                        if me.startswith('H'):
                            hval = me.split(':')
                            line.append(' # {a:<19}  {b:<45s}|\r\n'.format(a='Approx H',b=hval[1]))
                except:
                    pass
        line.append(' # {a:<19}  {b:<45s}|\r\n'.format(a='File created by',b='MagPy '+magpyversion))
        #iagacode = header.get('StationIAGAcode',"")
        line.append('DATE       TIME         DOY %8s %9s %9s %9s   |\r\n' % (iagacode+datacomp[0],iagacode+datacomp[1],iagacode+datacomp[2],iagacode+datacomp[3]))


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
                    xval = NOT_REPORTED
                if len(datastream.ndarray[yind]) > 0:
                    yval = datastream.ndarray[yind][i]
                    if order[1] == '3':
                        yval = datastream.ndarray[yind][i]*np.cos(datastream.ndarray[zind][i]*np.pi/180.)
                else:
                    yval = NOT_REPORTED
                if len(datastream.ndarray[zind]) > 0:
                    zval = datastream.ndarray[zind][i]*zmult
                else:
                    zval = NOT_REPORTED
                if gavail:
                    fval = datastream.ndarray[findg][i]
                elif favail and useg:
                    fval = np.sqrt(xval**2+yval**2+zval**2)-datastream.ndarray[find][i]
                elif favail:
                    fval = datastream.ndarray[find][i]
                else:
                    fval = NOT_REPORTED
                """
                if len(datastream.ndarray[find]) > 0:
                    if not useg:
                        fval = datastream.ndarray[find][i]
                    else:
                        fval = np.sqrt(xval**2+yval**2+zval**2)-datastream.ndarray[find][i]
                else:
                    fval = NOT_REPORTED
                """
                timeval = datastream.ndarray[0][i]
            row = ''
            try:
                row = datetime.strftime(timeval.replace(tzinfo=None),"%Y-%m-%d %H:%M:%S.%f")
                row = row[:-3]
                doi = datetime.strftime(timeval.replace(tzinfo=None), "%j")
                row += ' %s' % str(doi)
            except:
                row = ''
                pass
            if np.isnan(xval):
                row += '%13.2f' % MISSING_DATA
            else:
                row += '%13.2f' % xval
            if np.isnan(yval):
                row += '%10.2f' % MISSING_DATA
            else:
                row += '%10.2f' % yval
            if np.isnan(zval):
                row += '%10.2f' % MISSING_DATA
            else:
                row += '%10.2f' % zval
            if np.isnan(fval):
                row += '%10.2f' % MISSING_DATA
            else:
                row += '%10.2f' % fval
            line.append(row + '\r\n')
        try:
            myFile.writelines( line )
            pass
        finally:
            if returnstring:
                return myFile
            myFile.close()
    except IOError:
        return False
        pass


    return filename

if __name__ == '__main__':

    import scipy
    import subprocess
    print()
    print("----------------------------------------------------------")
    print("TESTING: IMF FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE IMF LIBRARY.")
    print("All main methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()
    # 1. Creating a test data set of minute resolution and 1 month length
    #    This testdata set will then be transformed into appropriate output formats
    #    and written to a temporary folder by the respective methods. Afterwards it is
    #    reloaded and compared to the original data set
    c = 1000  # 4000 nan values are filled at random places to get some significant data gaps
    l = 88400
    array = [[] for el in DataStream().KEYLIST]
    win = scipy.signal.windows.hann(60)
    a = np.random.uniform(20950, 21000, size=int(l/2))
    b = np.random.uniform(20950, 21050, size=int(l/2))
    x = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
    x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
    array[1] = x[1000:-1000]
    a = np.random.uniform(1950, 2000, size=int(l/2))
    b = np.random.uniform(1900, 2050, size=int(l/2))
    y = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
    y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
    array[2] = y[1000:-1000]
    a = np.random.uniform(44300, 44400, size=l)
    z = scipy.signal.convolve(a, win, mode='same') / sum(win)
    array[3] = z[1000:-1000]
    a = np.random.uniform(49000, 49200, size=l)
    f = scipy.signal.convolve(a, win, mode='same') / sum(win)
    array[4] = f[1000:-1000]
    array[0] = np.asarray([datetime(2022, 11, 1) + timedelta(seconds=i) for i in range(0, len(array[1]))])
    # 2. Creating artificial header information
    header = {}
    header['DataSamplingRate'] = 1
    header['SensorID'] = 'Test_0001_0002'
    header['StationIAGAcode'] = 'XXX'
    header['DataAcquisitionLatitude'] = 48.123
    header['DataAcquisitionLongitude'] = 15.999
    header['DataElevation'] = 1090
    header['DataComponents'] = 'XYZS'
    header['StationInstitution'] = 'TheWatsonObservatory'
    header['DataDigitalSampling'] = '1 Hz'
    header['DataSensorOrientation'] = 'HEZ'
    header['StationName'] = 'Holmes'

    teststream = DataStream(header=header, ndarray=np.asarray(array, dtype=object))


    errors = {}
    successes = {}
    testrun = 'STREAMTESTFILE'
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)

    while True:
        testset = 'IAGA'
        try:
            # Testing IAF
            filename = os.path.join('/tmp','{}_{}_{}'.format(testrun, testset, datetime.strftime(t_start_test,'%Y%m%d-%H%M')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            # IAF write
            succ1 = writeIAGA(teststream, filename)
            # IAF test
            succ2 = isIAGA(filename)
            # IAF read
            dat = readIAGA(filename)
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            diff = subtract_streams(teststream,dat)
            xm = diff.mean('x')
            ym = diff.mean('y')
            zm = diff.mean('z')
            fm = diff.mean('f')
            if np.abs(xm) > 0.001 or np.abs(ym) > 0.001 or np.abs(zm) > 0.001 or np.abs(fm) > 0.001:
                 raise Exception("ERROR within data validity test")
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
    del_test_files = 'rm {}*'.format(os.path.join('/tmp',testrun))
    subprocess.call(del_test_files,shell=True)
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
