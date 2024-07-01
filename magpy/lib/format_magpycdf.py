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
# for export of objects:
import codecs

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
        try:
            variables = temp.cdf_info().get('zVariables')  # cdflib < 1.0.0
        except:
            variables = temp.cdf_info().zVariables  # cdflib >= 1.0.0
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
    debug = kwargs.get('debug')
    cdfvers = 0.9

    if debug:
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
        if debug:
            print(" - read pycdf: version done")

        if headskip:  # TODO find out why
            for att in cdfdat.globalattsget():
                value = cdfdat.globalattsget().get(att)
                if debug:
                    print(" - read pycdf: value", value)
                try:
                    if isinstance(list(value), list):
                        if len(value) == 1:
                            value = value[0]
                except:
                    pass
                if not att in ['DataAbsFunctionObject','DataBaseValues', 'DataFlagList','DataFunctionObject']:
                    stream.header[att] = value
                else:
                        print ("Found special header content !!!!!!!!!!!!!!!! --  version {}".format(version))
                        #TODO check this - is pickle really necessary?
                        logger.debug("readPYCDF: Found object - loading and unpickling")
                        func = ''
                        try:
                            func = pickle.loads(codecs.decode(value.encode(), "base64"))
                        except:
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
                        print (" -> functions loaded")

        logger.info('readPYCDF: %s Format: %s ' % (filename, cdfformat))

        if debug:
            print(" - read pycdf: header done")

        # Testing for CDFLIB version using try except: version attributes are also changed in different cdflib versions, so this is equally effective
        try:
            variables = cdfdat.cdf_info().get('zVariables')  # cdflib < 1.0.0
            cdfvers = 0.9
        except:
            variables = cdfdat.cdf_info().zVariables  # cdflib >= 1.0.0
            cdfvers = 1.0
        timelength = 0

        for key in variables:
            if debug:
                print(" - read pycdf: reading key", key)

            if key.find('time')>=0 or key == 'Epoch':
                # Time column identified
                if not key == 'sectime':
                    ind = KEYLIST.index('time')
                else:
                    ind = KEYLIST.index('sectime')
                try:
                    if cdfvers<1.0:
                        ttdesc = cdfdat.varinq(key).get('Data_Type_Description')
                    else:
                        ttdesc = cdfdat.varinq(key).Data_Type_Description
                    if not ttdesc == 'CDF_TIME_TT2000':
                        print ("WARNING: Time column is not CDF_TIME_TT2000 (found {})".format(ttdesc))
                    col = cdfdat.varget(key)
                    try:
                        array[ind] = cdflib.cdfepoch.to_datetime(cdflib.cdfepoch,col)
                    except TypeError:
                        array[ind] = cdflib.cdfepoch.to_datetime(col)
                except:
                    array[ind] = np.asarray([])
            else:
                ind = KEYLIST.index(key)
                addhead = False
                timelength = len(array[0])
                if debug:
                    print(" - read pycdf: reading type of key", key)
                if cdfvers < 1.0:
                    ttdesc = cdfdat.varinq(key).get('Data_Type_Description')
                else:
                    ttdesc = cdfdat.varinq(key).Data_Type_Description
                col = cdfdat.varget(key)

                if not len(col) == timelength and len(col) == 1:
                    array[ind] = np.asarray([col[0]]*timelength)
                    addhead = True
                elif not len(col) == timelength and len(col)>1:
                    array[ind] = np.asarray([])
                else:
                    array[ind] = np.asarray(col)
                    addhead = True
                if debug:
                    print(" - read pycdf: attsget", key)
                if addhead:
                    if cdfvers > 0:
                        vname = cdfdat.varattsget(key).get('name','')
                        if not vname:
                            vname = cdfdat.varattsget(key).get('FIELDNAM','')
                        vunit = cdfdat.varattsget(key).get('units','')
                        if not vunit:
                            vunit = cdfdat.varattsget(key).get('UNITS','')
                    stream.header['col-'+key.lower()] = vname
                    stream.header['unit-col-'+key.lower()] = vunit

            if debug:
                print(" - read pycdf: done")

        if cdfvers < 1.0:
            cdfdat.close()
        del cdfdat

    if debug:
        print(" - read pycdf: returning")

    return DataStream([LineStruct()], stream.header,np.asarray(array,dtype=object))


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
    cdfvers = 0.9
    debug = False

    if compression == 0: ## temporary solution until all refs to skipcomression are eliminated
        skipcompression = True

    main_cdf_spec = {}
    main_cdf_spec['Compressed'] = False

    try:
        leapsecondlastupdate = cdflib.cdfepoch.getLeapSecondLastUpdated()
    except:
        # removed in new cdflib version
        leapsecondlastupdate = ""

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
            try:
                mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
            except:
                mycdf = cdflib.cdfwrite.CDF(filename,cdf_spec=main_cdf_spec)
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
            try:
                mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
            except:
                mycdf = cdflib.cdfwrite.CDF(filename,cdf_spec=main_cdf_spec)
        elif mode == 'append':
            #print filename
            exst = read(path_or_url=filename)
            datastream = joinStreams(exst,datastream,extend=True)
            os.remove(filename)
            try:
                mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
            except:
                mycdf = cdflib.cdfwrite.CDF(filename,cdf_spec=main_cdf_spec)
        else: # overwrite mode
            #print filename
            os.remove(filename)
            try:
                mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
            except:
                mycdf = cdflib.cdfwrite.CDF(filename,cdf_spec=main_cdf_spec)
    else:
        try:
            mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
        except:
            mycdf = cdflib.cdfwrite.CDF(filename,cdf_spec=main_cdf_spec)

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
                if not key in ['DataAbsFunctionObject','DataBaseValues', 'DataFlagList','DataFunctionObject']:
                    globalAttrs[key] = { 0 : str(headdict[key]) }
                else:
                    logger.info("writePYCDF: Found Object in header - pickle and dump ")
                    #print("writePYCDF: Found Object in header - pickle and dump ")
                    pfunc = codecs.encode(pickle.dumps(headdict[key]), "base64").decode()
                    #pfunc = pickle.dumps(headdict[key])
                    globalAttrs[key] = { 0 : str(pfunc) }

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
            #if not key == 'time':  ### why??
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
            if cdfkey.endswith('time'):
                if cdfkey == 'time':
                    cdfkey = 'Epoch'
                try: # Might fail for sectime
                    cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(num2date(elem).replace(tzinfo=None)) for elem in col.astype(np.float64)] )
                except:
                    cdfdata = np.asarray([])
                var_spec['Data_Type'] = 33
        elif len(col) > 0:
            if not key in NUMKEYLIST:
                col = list(col)
                col = ['' if el is None else el for el in col]
                col = np.asarray(col) # to get string conversion
                col = list(col) # convert back to list for write_var
                var_spec['Data_Type'] = 51 # CHAR
                var_spec['Num_Elements'] = max([len(i) for i in col])
                var_attrs['LABLAXIS'] = str(key) ## Version 1.2
                var_attrs['FIELDNAM'] = str(key) # use 'FIELDNAM' to be conform with NASA / IMAGCDF style ## Version 1.2
            else:
                var_spec['Data_Type'] = 45
                col = np.asarray([np.nan if el in [None,ch1] else el for el in col])
                col = col.astype(float)
                var_attrs['name'] = headdict.get('col-'+key,'')       # use 'FIELDNAM' to be conform with NASA / IMAGCDF style ## Version 1.1
                var_attrs['units'] = headdict.get('unit-col-'+key,'')  # use 'UNITS' to be conform with NASA style / IMAGCDF style
                var_attrs['FIELDNAM'] = headdict.get('col-'+key,'')       # use 'FIELDNAM' to be conform with NASA / IMAGCDF style ## Version 1.2
                var_attrs['UNITS'] = headdict.get('unit-col-'+key,'')  # use 'UNITS' to be conform with NASA style / IMAGCDF style ## Version 1.2
                var_attrs['LABLAXIS'] = headdict.get('col-'+key,'') ## Version 1.2
            cdfdata = col


        #if cdfdata is just a single value, then it will be converted into an array
        try:
            test = len(cdfdata)
        except:
            print ("Failed for {}".format(key))
            cdfdata = np.asarray([cdfdata])

        if len(cdfdata) > 0:
            var_spec['Variable'] = cdfkey
            if not var_spec.get('Num_Elements'):
                var_spec['Num_Elements'] = 1
            var_spec['Rec_Vary'] = True # The dimensional sizes, applicable only to rVariables.
            var_spec['Dim_Sizes'] = []

            mycdf.write_var(var_spec, var_attrs=var_attrs, var_data=cdfdata)

    mycdf.close()
    return testname
