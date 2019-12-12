"""
MagPy
MagPy's CDF input/output filters
Rewritten by Roman Leonhardt October 2019
- contains test, read and write methods
        MagPyCDF
- based on cdflib
- supports python >= 3.5

"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

# Specify what methods are really needed
from magpy.stream import *

import logging
logger = logging.getLogger(__name__)

import gc
import cdflib

def isPYCDF(filename):
    """
    Checks whether a file is Nasa CDF format.
    """
    try:
        temp = cdflib.CDF(filename)
    except:
        return False
    try:
        cdfformat = temp.globalattsget().get('DataFormat')
        if not cdfformat.startswith('MagPyCDF'):
            return False
    except:
        pass
    try:
        variables = temp.cdf_info().get('zVariables')
        if not 'Epoch' in variables:
            if not 'time' in variables:
                return False
    except:
        return False

    logger.debug("format_magpy: Found PYCDF file %s" % filename)
    return True


def readPYCDF(filename, headonly=False, **kwargs):
    """
    Reading CDF format data.
    """
    stream = DataStream([],{},np.asarray([[] for key in KEYLIST]))

    array = [[] for key in KEYLIST]
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    oldtype = kwargs.get('oldtype')
    getfile = True

    print ("Reading PYCDF with CDFLIB")
    # Check whether header information is already present
    headskip = False
    if stream.header == None:
        stream.header.clear()
    else:
        headskip = True

    theday = extractDateFromString(filename)
    try:
        if starttime:
            if not theday[-1] >= datetime.date(stream._testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(stream._testtime(endtime)):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True
    logbaddata = False

    # Get format type:
    # MagPy type is using datetime objects
    if getfile:
        cdfdat = cdflib.CDF(filename)
        cdfformat = cdfdat.globalattsget().get('DataFormat')
        try:
            version = float(cdfformat.replace('MagPyCDF',''))
        except:
            version = 1.0

        if headskip:  # TODO find out why
            for att in cdfdat.globalattsget():
                value = cdfdat.globalattsget().get(att)
                try:
                    if isinstance(list(value), list):
                        if len(value) == 1:
                            value = value[0]
                except:
                    pass
                if not att in ['DataAbsFunctionObject','DataBaseValues', 'DataFlagList']:
                    stream.header[att] = value
                else:
                        print ("Found special header content !!!!!!!!!!!!!!!! --  version {}".format(version))
                        #TODO check this - is pickle really necessary?
                        logger.debug("readPYCDF: Found object - loading and unpickling")
                        func = ''
                        try:
                            func = pickle.loads(str.encode(value), encoding="bytes")
                        except:
                            try:
                                print ("old unpickling version")
                                func = pickle.loads(value)
                            except:
                                print ("FAILED to load special content")
                                logger.debug("readPYCDF: Failed to load Object - constructed before v0.2.000?")
                        stream.header[att] = func

        logger.info('readPYCDF: %s Format: %s ' % (filename, cdfformat))

        #if debug:
        #print ("Step2", stream.header)

        variables = cdfdat.cdf_info().get('zVariables')
        timelength = 0

        for key in variables:
            if key.find('time')>=0 or key == 'Epoch':
                # Time column identified
                if not key == 'sectime':
                    ind = KEYLIST.index('time')
                else:
                    ind = KEYLIST.index('sectime')
                try:
                    ttdesc = cdfdat.varinq(key).get('Data_Type_Description')
                    if not ttdesc == 'CDF_TIME_TT2000':
                        print ("WARNING: Time column is not CDF_TIME_TT2000 (found {})".format(ttdesc))
                    col = cdfdat.varget(key)
                    array[ind] = date2num(np.asarray([datetime.utcfromtimestamp(el) for el in cdflib.cdfepoch.unixtime(col)]))
                except:
                    array[ind] = np.asarray([])
            else:
                ind = KEYLIST.index(key)
                addhead = False
                timelength = len(array[0])
                ttdesc = cdfdat.varinq(key).get('Data_Type_Description')
                col = cdfdat.varget(key)
                #print (ttdesc, cdfdat.varinq(key).get('Data_Type'))
                if not len(col) == timelength and len(col) == 1:
                    array[ind] = np.asarray([col[0]]*timelength)
                    addhead = True
                elif not len(col) == timelength and len(col)>1:
                    array[ind] = np.asarray([])
                else:
                    array[ind] = np.asarray(col)
                    addhead = True
                #print (cdfdat.varattsget(key))
                if addhead:
                    vname = cdfdat.varattsget(key).get('name','')
                    if not vname:
                        vname = cdfdat.varattsget(key).get('FIELDNAM','')
                    vunit = cdfdat.varattsget(key).get('units','')
                    if not vunit:
                        vunit = cdfdat.varattsget(key).get('UNITS','')
                    stream.header['col-'+key.lower()] = vname
                    stream.header['unit-col-'+key.lower()] = vunit

        cdfdat.close()
        del cdfdat

    #print (stream.header)
    return DataStream([LineStruct()], stream.header,np.asarray(array))


def writePYCDF(datastream, filename, **kwargs):
    """
    VARIABLES
        new: use compression variable instead of skipcompression
        compression = 0: skip compression
        compression = 1-9: use this compression factor: 
                           9 high compreesion (slow)
                           1 low compression (fast)
               default is 5

    """

    if pyvers and pyvers == 2:
                ch1 = '-'.encode('utf-8') # not working with py3
                ch2 = ''.encode('utf-8')
    else:
                ch1 = '-'
                ch2 = ''

    if not len(datastream.ndarray[0]) > 0 and not len(datastream) > 0:
        return False

    def tt(my_dt_ob):
        ms = my_dt_ob.microsecond/1000.  # fraction
        date_list = [my_dt_ob.year, my_dt_ob.month, my_dt_ob.day, my_dt_ob.hour, my_dt_ob.minute, my_dt_ob.second, ms]
        return date_list


    logger.info("Writing PYCDF Format {}".format(filename))
    mode = kwargs.get('mode')
    skipcompression = kwargs.get('skipcompression')
    compression = kwargs.get('compression')
    version = '1.2'

    if compression == 0: ## temporary solution until all refs to skipcomression are eliminated
        skipcompression = True

    main_cdf_spec = {}
    main_cdf_spec['Compressed'] = False

    leapsecondlastupdate = cdflib.cdfepoch.getLeapSecondLastUpdated()

    if not skipcompression:
        try:
            main_cdf_spec['Compressed'] = True
        except:
            logger.warning("writeIMAGCDF: Compression failed for unknown reason - storing uncompresed data")
            pass

    testname = str(filename+'.cdf')

    if os.path.isfile(testname):
        filename = testname
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream,extend=True)
            os.remove(filename)
            mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
        elif mode == 'replace': # replace existing inputs
            #print filename
            #### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            ####  Please note: Replacing requires a lot memory
            #### If memory issues appear then please overwrite existing data
            #### TODO Optimze sorting
            #### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            try:
                exst = read(path_or_url=filename)
                datastream = joinStreams(datastream,exst,extend=True)
            except:
                logger.error("writePYCDF: Could not interprete existing data set - aborting")
                sys.exit()
            os.remove(filename)
            mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
        elif mode == 'append':
            #print filename
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream,extend=True)
            os.remove(filename)
            mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
        else: # overwrite mode
            #print filename
            os.remove(filename)
            mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
    else:
        mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)

    keylst = datastream._get_key_headers()

    if not 'flag' in keylst:
        keylst.append('flag')
    if not 'comment' in keylst:
        keylst.append('comment')
    if not 'typ' in keylst:
        keylst.append('typ')
    tmpkeylst = ['time']
    tmpkeylst.extend(keylst)
    keylst = tmpkeylst

    headdict = datastream.header
    head, line = [],[]
    globalAttrs = {}

    if not mode == 'append':
        for key in headdict:
            if not key.find('col-') >= 0:
                #print (key, headdict[key])
                if not key in ['DataAbsFunctionObject','DataBaseValues', 'DataFlagList']:
                    globalAttrs[key] = { 0 : str(headdict[key]) }
                else:
                    logger.info("writePYCDF: Found Object in header - pickle and dump ")
                    pfunc = pickle.dumps(headdict[key])
                    globalAttrs[key] = { 0 : pfunc }

    globalAttrs['DataFormat'] = { 0 : 'MagPyCDF{}'.format(version)}

    mycdf.write_globalattrs(globalAttrs)    

    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    for key in keylst:
        var_attrs = {}
        var_spec = {}
        cdfdata = []

        ind = KEYLIST.index(key)
        col = datastream.ndarray[ind]
        if not key in NUMKEYLIST:
            if not key == 'time':
                col = np.asarray(col)

        # Sort out columns only containing nan's
        try:
            test = [elem for elem in col if not isnan(elem)]
            if not len(test) > 0:
                col = np.asarray([])
        except:
            pass
        if not False in checkEqual3(col) and len(col) > 0:
            logger.warning("writePYCDF: Found identical values only for key: %s" % key)
            col = col[:1]

        cdfkey = key.lower()
        if key.find('time') >= 0:
            if key.endswith('time'):
                if key == 'time':
                    cdfkey = 'Epoch'
                try: # Might fail for sectime
                    cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(num2date(elem).replace(tzinfo=None)) for elem in col.astype(np.float64)] )
                except:
                    cdfdata = np.asarray([])
                    pass
                var_spec['Data_Type'] = 33
        elif len(col) > 0:
            if not key in NUMKEYLIST:
                col = list(col)
                col = ['' if el is None else el for el in col]
                col = np.asarray(col) # to get string conversion
                col = list(col) # to get string conversion
                var_spec['Data_Type'] = 51
            else:
                var_spec['Data_Type'] = 45
                col = np.asarray([np.nan if el in [None,ch1] else el for el in col])
                col = col.astype(float)
            cdfdata = col

            var_attrs['name'] = headdict.get('col-'+key,'')       # use 'FIELDNAM' to be conform with NASA / IMAGCDF style ## Version 1.1
            var_attrs['units'] = headdict.get('unit-col-'+key,'')  # use 'UNITS' to be conform with NASA style / IMAGCDF style
            var_attrs['FIELDNAM'] = headdict.get('col-'+key,'')       # use 'FIELDNAM' to be conform with NASA / IMAGCDF style ## Version 1.2
            var_attrs['UNITS'] = headdict.get('unit-col-'+key,'')  # use 'UNITS' to be conform with NASA style / IMAGCDF style ## Version 1.2
            var_attrs['LABLAXIS'] = headdict.get('col-'+key,'') ## Version 1.2

        if len(cdfdata) > 0:
            var_spec['Variable'] = cdfkey
            var_spec['Num_Elements'] = 1
            var_spec['Rec_Vary'] = True # The dimensional sizes, applicable only to rVariables.
            var_spec['Dim_Sizes'] = []

            mycdf.write_var(var_spec, var_attrs=var_attrs, var_data=cdfdata)

    mycdf.close()
    return testname



