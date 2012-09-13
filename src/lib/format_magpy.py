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
        if not 'Epoch' in temp:
            if not 'time' in temp:
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
    try:
        if starttime:
            if not datetime.strptime(daystring,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(stream._testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(daystring,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(stream._testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
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
        
        if not headskip:
            for key in cdf_file.attrs:
                stream.header[key] = str(cdf_file.attrs[key])

        logging.info('--- File: %s Format: %s ' % (filename, cdfformat))

        for key in cdf_file:
            # first get time or epoch column
            lst = cdf_file[key]
            if key == 'time' or key == 'Epoch':
                ti = lst[...]
                #row = LineStruct()
                if str(cdfformat) == 'MagPyCDF':
                    #ti = [date2num(elem) for elem in ti]
                    #stream._put_column(ti,'time')
                    for elem in ti:
                        row = LineStruct()
                        row.time = date2num(elem)
                        stream.add(row)
                        del row
                else:
                    for elem in ti:
                        row = LineStruct()
                        row.time = date2num(elem)
                        stream.add(row)
                        del row
                del ti
            elif key == 'HNvar' or key == 'x':
                x = lst[...]
                stream._put_column(x,'x')
                del x
                #if not headskip:
                stream.header['col-x'] = 'x'
                try:
                    stream.header['unit-col-x'] = cdf_file['x'].attrs['units']
                except:
                    pass
            elif key == 'HEvar' or key == 'y':
                y = lst[...]
                stream._put_column(y,'y')
                del y
                stream.header['col-y'] = 'y'
                try:
                    stream.header['unit-col-y'] = cdf_file['y'].attrs['units']
                except:
                    pass
            elif key == 'Zvar' or key == 'z':
                z = lst[...]
                stream._put_column(z,'z')
                del z
                stream.header['col-z'] = 'z'
                try:
                    stream.header['unit-col-z'] = cdf_file['z'].attrs['units']
                except:
                    pass
            elif key == 'Fsc' or key == 'f':
                f = lst[...]
                stream._put_column(f,'f')
                del f
                stream.header['col-f'] = 'f'
                try:
                    stream.header['unit-col-f'] = cdf_file['f'].attrs['units']
                except:
                    pass
            else:
                if key.lower() in KEYLIST:
                    col = lst[...]
                    stream._put_column(col,key.lower())
                    del col
                    stream.header['col-'+key.lower()] = key.lower()
                    try:
                        stream.header['unit-col'+key.lower()] = cdf_file[key.lower()].attrs['units']
                    except:
                        pass

    cdf_file.close()

    del cdf_file

    return DataStream(stream, stream.header)    


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
            mycdf[key] = col
        for keydic in headdict:
            if keydic == ('unit-col-'+key):
                try:
                    mycdf[key].attrs['units'] = headdict.get('unit-col-'+key,'')
                except:
                    pass
    mycdf.close()

 

