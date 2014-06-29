"""
MagPy
MagPy input/output filters 
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from stream import *

import gc

def isPYCDF(filename):
    """
    Checks whether a file is Nasa CDF format.
    """
    try:
        temp = cdf.CDF(filename)
    except:
        return False
    try:
        if not 'Epoch' in temp:
            if not 'time' in temp:
                return False
    except:
        return False

    loggerlib.debug("format_magpy: Found PYCDF file %s" % filename)
    return True


def isPYSTR(filename):
    """
    Checks whether a file is ASCII PyStr format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith(' # MagPy - ASCII'):
        return False

    loggerlib.debug("format_magpy: Found PYSTR file %s" % filename)
    return True


def isPYBIN(filename):
    """
    Checks whether a file is binary PyStr format.
    """
    try:
        temp = open(filename, 'rt').readline()
    except:
        return False
    if not temp.startswith('# MagPyBin'):
        return False

    loggerlib.debug("format_magpy: Found PYBIN file %s" % filename)
    return True


def readPYSTR(filename, headonly=False, **kwargs):
    """
    Reading ASCII PyMagStructure format data.
    """
    stream = DataStream([],{})

    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header

    loggerlib.info('readPYSTR: Reading %s' % (filename))
    qFile= file( filename, "rb" )
    csvReader= csv.reader( qFile )

    for elem in csvReader:
        if elem==[]:
            # blank line
            pass
        elif elem[0]=='#':
            # blank header
            pass
        elif elem[0].startswith(' #') and not elem[0].startswith(' # MagPy - ASCII'):
            # attributes - assign header values
            headlst = elem[0].strip(' # ').split(':')
            headkey = headlst[0]
            headval = headlst[1]
            if not headkey.startswith('Column'):
                headers[headkey] = headval
        elif elem[0]=='Epoch[]':
            for i in range(len(elem)):
                headval = elem[i].split('[')                
                colval = headval[0]
                unitval = headval[1].strip(']')
                exec('headers["col-'+KEYLIST[i]+'"] = colval')
                exec('headers["unit-col-'+KEYLIST[i]+'"] = unitval')
        elif headonly:
            # skip data for option headonly
            continue
        else:
            try:
                row = LineStruct()
                try:
                    row.time = date2num(datetime.strptime(elem[0],"%Y-%m-%d-%H:%M:%S.%f"))
                except:
                    try:
                        row.time = date2num(datetime.strptime(elem[0],"%Y-%m-%dT%H:%M:%S.%f"))
                    except:
                        raise ValueError, "Wrong date format in file %s" % filename
                for idx, key in enumerate(KEYLIST):
                    if not key == 'time':
                        try:
                            exec('row.'+key+' =  float(elem[idx])')
                        except:
                            exec('row.'+key+' =  elem[idx]')
                stream.add(row)
            except ValueError:
                pass
    qFile.close()

    return DataStream(stream, headers)    


def readPYCDF(filename, headonly=False, **kwargs):
    """
    Reading CDF format data - DTU type.
    """
    stream = DataStream([],{})

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    # Check whether header infromation is already present
    headskip = False
    if stream.header == None:
        stream.header.clear()
    else:
        headskip = True
    
    cdf_file = cdf.CDF(filename)

    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    tmpdaystring = splitpath[1].split('.')[0]
    daystring = tmpdaystring[-10:]
    # test for day month year
    try:
        bounddate = datetime.strptime(daystring,'%Y-%m-%d')
        testdateform = '%Y-%m-%d'
    except:
        try:
            bounddate = datetime.strptime(daystring[-7:],'%Y-%m')
            testdateform = '%Y-%m'
        except:
            try:
                bounddate = datetime.strptime(daystring[-4:],'%Y')
                testdateform = '%Y'
            except:
                pass
         
    try:
        if starttime:
            if not bounddate >= datetime.strptime(datetime.strftime(stream._testtime(starttime),testdateform),testdateform):
                getfile = False
        if endtime:
            if not bounddate <= datetime.strptime(datetime.strftime(stream._testtime(endtime),testdateform),testdateform):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True 

    # Get format type:
    # DTU type is using different date format (MATLAB specific)
    # MagPy type is using datetime objects
    if getfile:
        try:
            cdfformat = cdf_file.attrs['DataFormat']
        except:
            logging.info("No format specification in CDF - passing")
            cdfformat = 'Unknown'
            pass
        
        if headskip:
            for key in cdf_file.attrs:
                stream.header[key] = str(cdf_file.attrs[key])

        #if headonly:
        #    cdf_file.close()
        #    return DataStream(stream, stream.header)    

        logging.info(' Read: %s Format: %s ' % (filename, cdfformat))

        for key in cdf_file:
            # first get time or epoch column
            #lst = cdf_file[key]
            if key == 'time' or key == 'Epoch':
                #ti = cdf_file[key][...]
                #row = LineStruct()
                if str(cdfformat) == 'MagPyCDF':
                    #ti = [date2num(elem) for elem in ti]
                    #stream._put_column(ti,'time')
                    for elem in cdf_file[key][...]:
                        row = LineStruct()
                        row.time = date2num(elem)
                        stream.add(row)
                        del row
                else:
                    for elem in cdf_file[key][...]:
                        row = LineStruct()
                        # correcting matlab day (relative to 1.1.2000) to python day (1.1.1)
                        row.time = 730120. + elem
                        stream.add(row)
                        del row
                #del ti
            elif key == 'HNvar' or key == 'x':
                #x = cdf_file[key][...]
                stream._put_column(cdf_file[key][...],'x')
                #del x
                #if not headskip:
                stream.header['col-x'] = 'x'
                try:
                    stream.header['col-x'] = cdf_file['x'].attrs['name']
                except:
                    pass
                try:
                    stream.header['unit-col-x'] = cdf_file['x'].attrs['units']
                except:
                    pass
            elif key == 'HEvar' or key == 'y':
                #y = cdf_file[key][...]
                stream._put_column(cdf_file[key][...],'y')
                #del y
                try:
                    stream.header['col-y'] = cdf_file['y'].attrs['name']
                except:
                    stream.header['col-y'] = 'y'
                try:
                    stream.header['unit-col-y'] = cdf_file['y'].attrs['units']
                except:
                    pass
            elif key == 'Zvar' or key == 'z':
                #z = cdf_file[key][...]
                stream._put_column(cdf_file[key][...],'z')
                #del z
                try:
                    stream.header['col-z'] = cdf_file['z'].attrs['name']
                except:
                    stream.header['col-z'] = 'z'
                try:
                    stream.header['unit-col-z'] = cdf_file['z'].attrs['units']
                except:
                    pass
            elif key == 'Fsc' or key == 'f':
                #f = cdf_file[key][...]
                stream._put_column(cdf_file[key][...],'f')
                #del f
                try:
                    stream.header['col-f'] = cdf_file['f'].attrs['name']
                except:
                    stream.header['col-f'] = 'f'
                try:
                    stream.header['unit-col-f'] = cdf_file['f'].attrs['units']
                except:
                    pass
            else:
                if key.lower() in KEYLIST:
                    #col = cdf_file[key][...]
                    stream._put_column(cdf_file[key][...],key.lower())
                    #del col
                    stream.header['col-'+key.lower()] = key.lower()
                    try:
                        stream.header['unit-col-'+key.lower()] = cdf_file[key.lower()].attrs['units']
                    except:
                        pass
                    try:
                        stream.header['col-'+key.lower()] = cdf_file[key.lower()].attrs['name']
                    except:
                        pass
            
    cdf_file.close()
    del cdf_file
    #gc.collect()

    return DataStream(stream, stream.header)   

def readPYBIN(filename, headonly=False, **kwargs):
    """
    Read binary format of the MagPy package
    Binary formatted data consists of an ascii header line and a binary body containing data
    The header line contains the following space separated inputs:
        1. Format specification: preset to 'MagPyBin' -> used to identify file type in MagPy
        2. SensorID: required 
        3. MagPy-Keys: list defining the related magpy keys under which data is stored e.g. ['x','y','z','t1','var5'] - check KEYLIST for available keys  
        4. ListSpecification: list defining the variables stored under the keys e.g. ['H','D','Z','DewPoint','Kp']  
        5. UnitSpecification: list defining units e.g. ['nT','deg','nT','deg C','']
        6. Multiplier: list defining multipiers used before packing of columns - value divided by multipl. returns correct units in 5 e.g. [100,100,100,1000,1]
        7. Packingcode: defined by pythons struct.pack always starts with 6hL e.g. 6hLLLLLL
        8. Packingsize: size of packing code
        9. Special format specification - optional
            in this case ... (important for high frequency records)
            to be written
        Important: lists 3,4,5,6 must be of identical length
    The data section is packed accoring to the packing code using struct.pack
    """
    keylist = kwargs.get('keylist') # required for very old format, does not affect other formats
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    loggerlib.info('readPYBIN: Reading Magpy binary data.')

    stream = DataStream([],{})

    theday = extractDateFromString(filename)
    try:
        if starttime:
            if not theday >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not theday <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True 

    if getfile:
        fh = open(filename, 'rb')
        # read header line and extract packing format
        header = fh.readline()
        h_elem = header.strip().split()
        if not h_elem[1] == 'MagPyBin':
            print 'No MagPyBin format - aborting'
            return
        #print "Length ", len(h_elem), h_elem[2]

        #Test whether element 3,4,5 (and 6) are lists of equal length 
        if len(h_elem) == 8:
            #print "Very old import format"
            nospecial = True
            try:
                if not keylist:
                    loggerlib.error('keylist of length elemlist must be specified')
                    return
                elemlist = h_elem[3].strip('[').strip(']').split(',')
                unitlist = h_elem[4].strip('[').strip(']').split(',')
                multilist = map(float,h_elem[5].strip('[').strip(']').split(','))
            except:
                print "readPYBIN: Could not extract lists from header - check format - aborting..."
                return stream
            if not len(keylist) == len(elemlist) or not len(keylist) == len(unitlist) or not  len(keylist) == len(multilist):
                loggerlib.error("readPYBIN: Provided lists from header of differenet lengths - check format - aborting...")
                return stream
        elif len(h_elem) == 9:
            #print "The current format"
            nospecial = True
            try:
                keylist = h_elem[3].strip('[').strip(']').split(',')
                elemlist = h_elem[4].strip('[').strip(']').split(',')
                unitlist = h_elem[5].strip('[').strip(']').split(',')
                multilist = map(float,h_elem[6].strip('[').strip(']').split(','))
            except:
                loggerlib.error("readPYBIN: Could not extract lists from header - check format - aborting...")
                return stream
            if not len(keylist) == len(elemlist) or not len(keylist) == len(unitlist) or not  len(keylist) == len(multilist):
                loggerlib.error("readPYBIN: Provided lists from header of differenet lengths - check format - aborting...")
                return stream
        elif len(h_elem) == 10:
            #print "Special format"
	    loggerlib.debug("readPYBIN: Special format detected. May not be able to read file.")
            nospecial = False
	    if h_elem[2][:5] == 'ENV05' or h_elem[2] == 'Env05':
                keylist = h_elem[3].strip('[').strip(']').split(',')
                elemlist = h_elem[4].strip('[').strip(']').split(',')
                unitlist = h_elem[5].strip('[').strip(']').split(',')
                multilist = map(float,h_elem[7].strip('[').strip(']').split(','))
                nospecial = True
        else:
            loggerlib.error('readPYBIN: No valid MagPyBin format, inadequate header length - aborting')
            return stream
            
        packstr = '<'+h_elem[-2]+'B'
        lengthcode = struct.calcsize(packstr)
        lengthgiven = int(h_elem[-1])+1
        length = lengthgiven
        if not lengthcode == lengthgiven:
            loggerlib.debug("readPYBIN: Check your packing code!")
            if lengthcode < lengthgiven:
                missings = lengthgiven-lengthcode
                for i in range(missings):
                    packstr += 'B'
                    length = lengthgiven
            else:
                length = lengthcode


        line = fh.read(length)
        stream.header['SensorID'] = h_elem[2]
        stream.header['SensorElements'] = ','.join(elemlist)
        stream.header['SensorKeys'] = ','.join(keylist)
        if nospecial:
            for idx, elem in enumerate(keylist):
                stream.header['col-'+elem] = elemlist[idx]
                stream.header['unit-col-'+elem] = unitlist[idx]
                # Header info
                pass
            while not line == "":
                try:
                    data= struct.unpack(packstr, line)
                except:
                    print "readPYBIN: struct error", filename, packstr, struct.calcsize(packstr)
                time = datetime(data[0],data[1],data[2],data[3],data[4],data[5],data[6])
                row = LineStruct()
                row.time = date2num(stream._testtime(time))
                for idx, elem in enumerate(elemlist):
                    exec('row.'+keylist[idx]+' = data[idx+7]/float(multilist[idx])')
                stream.add(row)
                line = fh.read(length)
        else:
            print "To be done ..."
            pass

    #print stream.header        
    return stream 


def writePYSTR(datastream, filename, **kwargs):
    """
    Function to write structural ASCII data 
    """

    mode = kwargs.get('mode')
    loggerlib.info("writePYSTR: Writing file to %s" % filename)

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
        else:
            myFile= open( filename, "wb" )
    else:
        myFile= open( filename, "wb" )
    wtr= csv.writer( myFile )
    headdict = datastream.header
    head, line = [],[]
    if not mode == 'append':
        wtr.writerow( [' # MagPy - ASCII'] )
        for key in headdict:
            if not key.find('col') >= 0:
                line = [' # ' + key +':  ' + str(headdict[key])]
                wtr.writerow( line )
        wtr.writerow( ['# head:'] )
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


def writePYCDF(datastream, filename, **kwargs):
    # check for nan and - columns
    #for key in KEYLIST:
    #    title = headdict.get('col-'+key,'-') + '[' + headdict.get('unit col-'+key,'') + ']'
    #    head.append(title)

    mode = kwargs.get('mode')

    if os.path.isfile(filename+'.cdf'):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename+'.cdf')
            datastream = mergeStreams(exst,datastream,extend=True)
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename+'.cdf')
            datastream = mergeStreams(datastream,exst,extend=True)
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

    headdict = datastream.header
    head, line = [],[]

    if not mode == 'append':
        for key in headdict:
            if not key.find('col') >= 0:
                mycdf.attrs[key] = headdict[key]
    mycdf.attrs['DataFormat'] = 'MagPyCDF'

    for key in KEYLIST:
        col = datastream._get_column(key)
        if key == 'time':
            key = 'Epoch'
            mycdf[key] = np.asarray([num2date(elem).replace(tzinfo=None) for elem in col])
        elif len(col) > 0:
            nonetest = [elem for elem in col if not elem == None]
            if len(nonetest) > 0:
                mycdf[key] = col
        for keydic in headdict:
            if keydic == ('col-'+key):
                try:
                    mycdf[key].attrs['name'] = headdict.get('col-'+key,'')
                except:
                    pass
            if keydic == ('unit-col-'+key):
                try:
                    mycdf[key].attrs['units'] = headdict.get('unit-col-'+key,'')
                except:
                    pass
                    
    mycdf.close()
