"""
MagPy
MagPy input/output filters 
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""

from core.magpy_stream import *

def isPYCDF(filename):
    """
    Checks whether a file is Nasa CDF format.
    """
    try:
        temp = cdf.CDF(filename)
    except:
        return False
    try:
        lst =[key for key in temp if key == 'time' or key == 'Epoch']
        if lst == []:
            return False
    except:
        return False
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
    return True



def readPYSTR(filename, headonly=False, **kwargs):
    """
    Reading ASCII PyMagStructure format data.
    """
    stream = DataStream()
    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header
    qFile= file( filename, "rb" )
    csvReader= csv.reader( qFile )
    for elem in csvReader:
        if elem[0]=='#':
            # blank line
            pass
        elif elem[0]==' #':
            # attributes
            pass
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
    stream = DataStream()

    # Check whether header infromation is already present
    if stream.header == None:
        headers = {}
    else:
        headers = stream.header

    logging.info('--- Start reading CDF at %s ' % str(datetime.now()))

    cdf_file = cdf.CDF(filename)

    # Get format type:
    # DTU type is using different date format (MATLAB specific)
    # MagPy type is using datetime objects
    try:
        cdfformat = cdf_file.attrs['DataFormat']
    except:
        logging.info("No format specification in CDF - passing")
        cdfformat = 'Unknown'
        pass

    logging.info('--- File: %s Format: %s ' % (filename, cdfformat))

    for key in cdf_file:
        # first get time or epoch column
        lst = cdf_file[key]
        if key == 'time' or key == 'Epoch':
            ti = lst[...]
            for elem in ti:
                row = LineStruct()
                if str(cdfformat) == 'MagPyCDF':
                    row.time = date2num(elem)                  
                else:
                    row.time = elem+730485.0 # DTU MATLAB time
                stream.add(row)
        elif key == 'HNvar' or key == 'x':
            x = lst[...]
            stream._put_column(x,'x')
        elif key == 'HEvar' or key == 'y':
            y = lst[...]
            stream._put_column(y,'y')
        elif key == 'Zvar' or key == 'z':
            z = lst[...]
            stream._put_column(z,'z')
        elif key == 'Fsc' or key == 'f':
            f = lst[...]
            stream._put_column(f,'f')
        else:
            if key.lower() in KEYLIST:
                col = lst[...]
                stream._put_column(col,key.lower())

    cdf_file.close()

    logging.info('--- Finished reading CDF at %s ' % str(datetime.now()))

    return DataStream(stream, headers)    


def writePYSTR(datastream, filename, **kwargs):
    """
    Function to write structural ASCII data 
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
                line = [' # ' + key +':  ' + headdict[key]]
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
            exst = pmRead(path_or_url=filename+'.cdf')
            datastream = mergeStreams(exst,datastream,extend=True)
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
        elif mode == 'replace': # replace existing inputs
            exst = pmRead(path_or_url=filename+'.cdf')
            datastream = mergeStreams(datastream,exst,extend=True)
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
        elif mode == 'append':
            mycdf = cdf.CDF(filename, filename) # append????
        else: # overwrite mode
            os.remove(filename+'.cdf')
            mycdf = cdf.CDF(filename, '')
    else:
        mycdf = cdf.CDF(filename, '')

    headdict = datastream.header
    head, line = [],[]
    mycdf.attrs['DataFormat'] = 'MagPyCDF'
    if not mode == 'append':
        for key in headdict:
            if not key.find('col') >= 0:
                mycdf.attrs[key] = headdict[key]

    for key in KEYLIST:
        col = datastream._get_column(key)
        if key == 'time':
            key = 'Epoch'
            mycdf[key] = np.asarray([num2date(elem).replace(tzinfo=None) for elem in col])
        elif len(col) > 0:
            mycdf[key] = col
        for keydic in headdict:
            if keydic.find('unit-col-'+key) > 0:
                mycdf[key].attrs['units'] = headdict.get('unit-col-'+key,'')
    mycdf.close()

 

