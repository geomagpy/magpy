"""
MagPy
Intermagnet ImagCDF input filter
(based on cdflib)
Written by Roman Leonhardt October 2019
- contains test, read and write functions for
        ImagCDF
- supports python >= 3.5
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

from magpy.stream import *

import cdflib

#import ciso8601 ## fast datetime parser  ciso8601.parse_datetime should be 10 times faster than datetime.strftime

import sys
import logging
logger = logging.getLogger(__name__)

HEADTRANSLATE = {'FormatDescription':'DataFormat', 'IagaCode':'StationID', 'ElementsRecorded':'DataComponents', 'ObservatoryName':'StationName', 'Latitude':'DataAcquisitionLatitude', 'Longitude':'DataAcquisitionLongitude', 'Institution':'StationInstitution', 'VectorSensOrient':'DataSensorOrientation', 'TermsOfUse':'DataTerms','UniqueIdentifier':'DataID','ParentIdentifiers':'SensorID','ReferenceLinks':'StationWebInfo', 'FlagRulesetType':'FlagRulesetType','FlagRulesetVersion':'FlagRulesetVersion'}


def isIMAGCDF(filename):
    """
    Checks whether a file is ImagCDF format
    """

    try:
        temp = cdflib.CDF(filename)
    except:
        return False
    try:
        form = temp.globalattsget().get('FormatDescription')
        if not form.startswith('INTERMAGNET'):
            return False
    except:
        return False

    print ("Running new ImagCDF import filter")

    logger.debug("isIMAGCDF: Found INTERMAGNET CDF data - using cdflib")
    return True

def readIMAGCDF(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet CDF format (1.0,1.1,1.2)
    """

    debug = kwargs.get('debug')

    headers={}
    arraylist = []
    array = [[] for elem in KEYLIST]
    mutipletimerange = False
    newdatalist = []
    tllist = []
    indexarray = np.asarray([])

    cdfdat = cdflib.CDF(filename)

    if debug:
        print ("Reading ImagCDF with cdflib")

    for att in cdfdat.globalattsget():
        value = cdfdat.globalattsget().get(att)
        try:
            if isinstance(list(value), list):
                if len(value) == 1:
                    value = value[0]
        except:
            pass
        if not att in HEADTRANSLATE:
            attname = 'Data'+att
        else:
            attname = HEADTRANSLATE[att] 
        headers[attname] = value

    #Some specials:
    headers['StationIAGAcode'] = headers.get('StationID')
    headers['DataFormat'] = headers.get('DataFormat') + '; ' + cdfdat.globalattsget().get('FormatVersion')
    pubdate = cdflib.cdfepoch.unixtime(headers.get('DataPublicationDate'))
    headers['DataPublicationDate'] = datetime.utcfromtimestamp(pubdate[0]) 

    if debug:
        logger.info("readIMAGCDF: FOUND IMAGCDF file created with version {}".format(headers.get('DataFormat')))

    headers['DataLeapSecondUpdated'] = cdfdat.cdf_info().get('LeapSecondUpdated')

    # Get all available Variables - ImagCDF usually uses only zVariables
    datalist = cdfdat.cdf_info().get('zVariables')


    # New in 0.3.99 - provide a SensorID as well consisting of IAGA code, min/sec 
    # and numerical publevel

    #  IAGA code
    if headers.get('SensorID','') == '':
        try:
            #TODO determine resolution
            headers['SensorID'] = "{}_{}_{}".format(headers.get('StationIAGAcode','xxx').upper()+'sec',headers.get('DataPublicationLevel','0'),'0001')
        except:
            pass


    # #########################################################
    # 1. Now getting individual data and check time columns 
    # #########################################################
    zpos = KEYLIST.index('z') # used for idf records
    for elem in datalist:
        if elem.endswith('Times') and not elem.startswith('Flag'):
            try:
                tl = int(cdfdat.varinq('GeomagneticVectorTimes').get('Last_Rec'))
                tllist.append([tl,elem])
            except:
                pass

    if len(tllist) < 1:
        """
        No time column identified
        -> Check for starttime and sampling rate in header
        """
        if  cdfdat.globalattsget().get('StartTime','') and cdfdat.globalattsget().get('SamplingPeriod',''):
            # TODO Write that function
            st = cdfdat.globalattsget().get('StartTime','')
            sr = cdfdat.globalattsget().get('SamplingPeriod','')
            # get length of f or x 
        else:
            logger.error("readIMAGCDF: No Time information available - aborting")
            return
    elif len(tllist) > 1:
        tl = [el[0] for el in tllist]
        if not max(tl) == min(tl):
            logger.warning("readIMAGCDF: Time columns of different length. Choosing longest as basis")
            newdatalist.append(['time',max(tllist)[1]])
            datnumar1 = date2num(np.asarray([datetime.utcfromtimestamp(el) for el in cdflib.cdfepoch.unixtime(cdfdat.varget(max(tllist)[1]))]))
            datnumar2 = date2num(np.asarray([datetime.utcfromtimestamp(el) for el in cdflib.cdfepoch.unixtime(cdfdat.varget(min(tllist)[1]))]))
            try:
                indexarray = np.nonzero(np.in1d(datnumar1,datnumar2))[0]
            except:
                indexarray = np.asarray([])
            mutipletimerange = True
        else:
            logger.info("readIMAGCDF: Equal length time axes found - assuming identical time")
            if 'GeomagneticVectorTimes' in datalist:
                newdatalist.append(['time','GeomagneticVectorTimes'])
            else:
                newdatalist.append(['time',tllist[0][1]]) # Take the first one
    else:
        #"Single time axis found in file"
        newdatalist.append(['time',tllist[0][1]])

    def Ruleset2Flaglist(flagginglist,rulesettype,rulesetversion):
        if flagruletype in ['Conrad', 'conrad', 'MagPy','magpy']:
            if flagruleversion in ['1.0','1',1]:
                flagcolsconrad = [flagginglist[0],flagginglist[1],flagginglist[3],flagginglist[4],flagginglist[5],flagginglist[6],flagginglist[2]]
                flaglisttmp = []
                for elem in flagcolsconrad:
                    flaglisttmp.append(cdfdat[elem][...])
                flaglist = np.transpose(flaglisttmp)
                flaglist = [list(elem) for elem in flaglist]
                return list(flaglist)
        else:
            logger.warning("readIMAGCDF: Could  not interprete Ruleset")

    if not headers.get('FlagRuleType','') == '':
        logger.info("readIMAGCDF: Found flagging ruleset {} vers.{} - extracting flagging information".format(headers.get('FlagRuleType',''),headers.get('FlagRuleVersion','')))
        flagginglist = [elem for elem in datalist if elem.startswith('Flag')]
        flaglist = Ruleset2Flaglist(flagginglist,headers.get('FlagRuleType',''),headers.get('FlagRuleVersion',''))

    datalist = [elem for elem in datalist if not elem.endswith('Times') and not elem.startswith('Flag')]

    # #########################################################
    # 2. Sort the datalist according to KEYLIST
    # #########################################################
    for key in KEYLIST:
        possvals = [key]
        if key == 'x':
            possvals.extend(['h','i'])
        if key == 'y':
            possvals.extend(['d','e'])
        if key == 'df':
            possvals.append('g')
        if key == 'f':
            possvals.append('s')
        for elem in datalist:
            try:
                label = cdfdat.varattsget(elem).get('LABLAXIS').lower()
                if label in possvals:
                    newdatalist.append([key,elem])
            except:
                pass # for lines which have no Label

    if not len(datalist) == len(newdatalist)-1:
        logger.warning("readIMAGCDF: error encountered in key assignment - please check")

    # #########################################################
    # 3. Create equal length array reducing all data to primary Times and filling nans for non-exist
    # #########################################################
    # (4. eventually completely drop time cols and just store start date and sampling period in header)
    # Deal with scalar data (independent or whatever

    for elem in newdatalist:
        if elem[0] == 'time':
            ttdesc = cdfdat.varinq(elem[1]).get('Data_Type_Description')
            col = cdfdat.varget(elem[1])
            ar = date2num(np.asarray([datetime.utcfromtimestamp(el) for el in cdflib.cdfepoch.unixtime(col)]))
            arlen= len(ar)
            arraylist.append(ar)
            ind = KEYLIST.index('time')
            array[ind] = ar
        else:
            ar = cdfdat.varget(elem[1])
            if elem[0] in NUMKEYLIST:
                with np.errstate(invalid='ignore'):
                    ar[ar > 88880] = float(nan)
                ind = KEYLIST.index(elem[0])
                headers['col-'+elem[0]] = cdfdat.varattsget(elem[1]).get('LABLAXIS').lower()
                headers['unit-col-'+elem[0]] = cdfdat.varattsget(elem[1]).get('UNITS')
                if len(indexarray) > 0 and elem[0] in ['f','df']:  ## this is no good - point to depend_0
                    newar = np.asarray([np.nan]*arlen)
                    newar[indexarray] = ar
                    array[ind] = newar
                    arraylist.append(newar)
                else:
                    array[ind] = ar
                    arraylist.append(ar)
                if elem[0] in ['f','F'] and headers.get('DataComponents','') in ['DIF','dif','idf','IDF'] and not len(array[zpos]) > 0:
                    array[zpos] = ar
                    arraylist.append(ar)
                    headers['col-z'] = cdfdat.varattsget(elem[1]).get('LABLAXIS').lower()
                    headers['unit-col-z'] = cdfdat.varattsget(elem[1]).get('UNITS')

    ndarray = np.array(array)

    stream = DataStream()
    stream = [LineStruct()]

    result = DataStream(stream,headers,ndarray)

    if not headers.get('FlagRuleType','') == '' and len(flaglist) > 0:
        result = result.flag(flaglist)

    return result


def writeIMAGCDF(datastream, filename, **kwargs):
    """
    Writing Intermagnet CDF format (currently: vers1.2) + optional flagging info
    
    """

    print ("Writing CDF data based on cdflib")

    def tt(my_dt_ob):
        ms = my_dt_ob.microsecond/1000.  # fraction
        date_list = [my_dt_ob.year, my_dt_ob.month, my_dt_ob.day, my_dt_ob.hour, my_dt_ob.minute, my_dt_ob.second, ms]
        return date_list


    logger.info("Writing IMAGCDF Format {}".format(filename))
    mode = kwargs.get('mode')
    addflags = kwargs.get('addflags')
    skipcompression = kwargs.get('skipcompression')

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
            datastream = joinStreams(exst,datastream)
            os.remove(filename)
            mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
        elif mode == 'replace' or mode == 'append': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = joinStreams(datastream,exst)
            os.remove(filename)
            mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
        else: # overwrite mode
            #print filename
            os.remove(filename)
            mycdf = cdflib.CDF(filename,cdf_spec=main_cdf_spec)
    else:
        mycdf = cdflib.CDF(filename)

    keylst = datastream._get_key_headers()
    tmpkeylst = ['time']
    tmpkeylst.extend(keylst)
    keylst = tmpkeylst

    headers = datastream.header
    head, line = [],[]
    success = False

    # For test purposes: flagging
    flaglist = []

    # check DataComponents for correctness
    dcomps = headers.get('DataComponents','')
    dkeys = datastream._get_key_headers()
    if 'f' in dkeys and len(dcomps) == 3:
        dcomps = dcomps+'S'
    if 'df' in dkeys and len(dcomps) == 3:
        dcomps = dcomps+'G'
    headers['DataComponents'] = dcomps

    ### #########################################
    ###            Check Header 
    ### #########################################

    INVHEADTRANSLATE = {v: k for k, v in HEADTRANSLATE.items()}
    INVHEADTRANSLATE['StationIAGAcode'] = 'IagaCode'

    globalAttrs = {}
    for key in headers:
        if key in INVHEADTRANSLATE:
            globalAttrs[INVHEADTRANSLATE.get(key)] = { 0 : headers.get(key) }
        elif key.startswith('col-') or key.startswith('unit-'):
            pass
        else:
            globalAttrs[key.replace('Data','',1)] = { 0 : str(headers.get(key)) }


    ## 1. Fixed Part -- current version is 1.2
    ## Transfer MagPy Header to INTERMAGNET CDF attributes
    globalAttrs['FormatDescription'] = { 0 : 'INTERMAGNET CDF format'}
    globalAttrs['FormatVersion'] = { 0 : '1.2'}
    globalAttrs['Title'] = { 0 : 'Geomagnetic time series data'}
    if addflags:
        globalAttrs['FormatVersion'] = { 0 : '1.3'}

    ## 3. Optional flagging information
    ##    identify flags within the data set and if they are present then add an attribute to the header
    if addflags:
        flaglist = datastream.extractflags()
        if len(flaglist) > 0:
            globalAttrs['FlagRulesetVersion'] = '1.0'
            globalAttrs['FlagRulesetType'] = 'Conrad'

    if not headers.get('DataPublicationDate','') == '':
        dat = tt(datastream._testtime(headers.get('DataPublicationDate','')))
        pubdate = cdflib.cdfepoch.compute_tt2000([dat])
    else:
        pubdate = cdflib.cdfepoch.compute_tt2000([tt(datetime.utcnow())])
    globalAttrs['PublicationDate'] = { 0 : pubdate }

    if not headers.get('DataSource','')  == '':
        if headers.get('DataSource','') in ['INTERMAGNET', 'WDC']:
            globalAttrs['Source'] =  { 0 : headers.get('DataSource','')}
        else:
            globalAttrs['Source'] =  { 0 : headers.get('DataSource','')}
    else:
        globalAttrs['Source'] =  { 0 : headers.get('StationInstitution','')}

    if not headers.get('DataStandardLevel','') == '':
        if headers.get('DataStandardLevel','') in ['None','none','Partial','partial','Full','full']:
            globalAttrs['StandardLevel'] = { 0 : headers.get('DataStandardLevel','')}
        else:
            print("writeIMAGCDF: StandardLevel not defined - please specify by yourdata.header['DataStandardLevel'] = ['None','Partial','Full']")
            globalAttrs['StandardLevel'] = { 0 : 'None'}
        if headers.get('DataStandardLevel','') in ['partial','Partial']:
            # one could add a validity check whether provided list is aggreement with standards
            if headers.get('DataPartialStandDesc','') == '':
                print("writeIMAGCDF: PartialStandDesc is missing. Add items like IMOM-11,IMOM-12,IMOM-13 ...")
    else:
        print("writeIMAGCDF: StandardLevel not defined - please specify by yourdata.header['DataStandardLevel'] = ['None','Partial','Full']")
        globalAttrs['StandardLevel'] = { 0 : 'None'}

    if not headers.get('DataStandardName','') == '':
        globalAttrs['StandardName'] = { 0 : headers.get('DataStandardName','')}
    else:
        try:
            #print ("writeIMAGCDF: Asigning StandardName")
            samprate = float(str(headers.get('DataSamplingRate',0)).replace('sec','').strip())
            if int(samprate) == 1:
                stdadd = 'INTERMAGNET_1-Second'
            elif int(samprate) == 60:
                stdadd = 'INTERMAGNET_1-Minute'
            if headers.get('DataPublicationLevel',0) in [3,'3','Q','quasi-definitive','Quasi-definitive']:
                stdadd += '_QD'
                globalAttrs['StandardName'] = { 0 : stdadd }
            elif headers.get('DataPublicationLevel',0) in [4,'4','D','definitive','Definitive']:
                globalAttrs['StandardName'] = { 0 : stdadd }
            else:
                print ("writeIMAGCDF: current Publication level {} does not allow to set StandardName".format(headers.get('DataPublicationLevel',0)))
                globalAttrs['StandardLevel'] = { 0 : 'None'}
        except:
            print ("writeIMAGCDF: Asigning StandardName Failed")


    proj = headers.get('DataLocationReference','')
    longi = headers.get('DataAcquisitionLongitude','')
    lati = headers.get('DataAcquisitionLatitude','')
    try:
        longi = "{:.3f}".format(float(longi))
        lati = "{:.3f}".format(float(lati))
    except:
        print("writeIMAGCDF: could not convert lat long to floats")
    if not longi=='' or lati=='':
        if proj == '':
            patt = mycdf.attrs
            try:
                globalAttrs['Latitude'] = { 0 : float(lati) }
                globalAttrs['Longitude'] = { 0 : float(longi) }
            except:
                globalAttrs['Latitude'] = { 0 : lati }
                globalAttrs['Longitude'] = { 0 : longi }
        else:
            if proj.find('EPSG:') > 0:
                epsg = int(proj.split('EPSG:')[1].strip())
                if not epsg==4326:
                    print ("writeIMAGCDF: converting coordinates to epsg 4326")
                    longi,lati = convertGeoCoordinate(float(longi),float(lati),'epsg:'+str(epsg),'epsg:4326')
                    longi = "{:.3f}".format(float(longi))
                    lati = "{:.3f}".format(float(lati))
            globalAttrs['Latitude'] = { 0 : float(lati) }
            globalAttrs['Longitude'] = { 0 : float(longi) }

    if not 'StationIagaCode' in headers and 'StationID' in headers:
        globalAttrs['IagaCode'] = { 0 : headers.get('StationID','')}


    mycdf.write_globalattrs(globalAttrs)    

    ### #########################################
    ###               Data 
    ### #########################################

    def checkEqualIvo(lst):
        # http://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
        return not lst or lst.count(lst[0]) == len(lst)

    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    ndarray = False
    if len(datastream.ndarray[0]>0):
        ndarray = True

    # Check F/S/G select either S or G, send out warning if presumably F (mean zero, stddeviation < resolution)
    naninds = np.asarray([])
    ## Analyze F and dF columns:
    fcolname = 'S'
    if 'f' in keylst or 'df' in keylst:
        if 'f' in keylst:
            if not 'df' in keylst:
                 #print ("writeIMAGCDF: Found F column") # check whether F or S
                 comps = datastream.header.get('DataComponents')
                 if not comps.endswith('S'):
                     print ("writeIMAGCDF: given components are {}. Checking F column...".format(datastream.header.get('DataComponents')))
                     #calculate delta F and determine average diff
                     datastream = datastream.delta_f()
                     dfmean, dfstd = datastream.mean('df',std=True, percentage=50)
                     if dfmean < 0.0000000001 and dfstd < 0.0000000001:
                         fcolname = 'F'
                         print ("writeIMAGCDF: analyzed F column - values are apparently calculated from vector components - using column name 'F'")
                     else:
                         print ("writeIMAGCDF: analyzed F column - values are apparently independend from vector components - using column name 'S'")
            pos = KEYLIST.index('f')
            col = datastream.ndarray[pos]
        if 'df' in keylst:
            #print ("writeIMAGCDF: Found dF column")
            pos = KEYLIST.index('df')
            col = datastream.ndarray[pos]
        col = col.astype(float)
        
        nonancol = col[~np.isnan(col)]
            
        #print ("IMAG", len(nonancol),datastream.length()[0])
        if len(nonancol) < datastream.length()[0]/2.:
            #shorten col
            print ("writeIMF - reducing f column resolution:", len(nonancol), len(col))
            naninds = np.where(np.isnan(col))[0]
            #print (naninds, len(naninds))
            useScalarTimes=True
            #[inds]=np.take(col_mean,inds[1])
        else:
            #keep column and (later) leave time       
            useScalarTimes=True  # change to False in order to use a single col

    ## get sampling rate of vec, get sampling rate of scalar, if different extract scalar and time use separate, else ..

    for key in keylst:
        # New : assign data to the following variables: var_attrs (meta), var_data (dataarray), var_spec (key??)
        var_attrs = {}
        var_spec = {}

        if key in ['time','sectime','x','y','z','f','dx','dy','dz','df','t1','t2']:
            ind = KEYLIST.index(key)
            if ndarray and len(datastream.ndarray[ind])>0:
                col = datastream.ndarray[ind]
            else:
                col = datastream._get_column(key)
            col = col.astype(float)

            if not False in checkEqual3(col):
                logger.warning("Found identical values only for {}".format(key))
                col = col[:1]

            #{'FIELDNAM': 'Geomagnetic Field Element X', 'VALIDMIN': array([-79999.]), 'VALIDMAX': array([ 79999.]), 'UNITS': 'nT', 'FILLVAL': array([ 99999.]), 'DEPEND_0': 'GeomagneticVectorTimes', 'DISPLAY_TYPE': 'time_series', 'LABLAXIS': 'X'}
            if key == 'time':
                cdfkey = 'GeomagneticVectorTimes'
                cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(num2date(elem).replace(tzinfo=None)) for elem in col] )
                var_spec['Data_Type'] = 33
            elif len(col) > 0:
                #if len(col) > 1000000:
                #    print ("Starting with {}".format(key))
                var_spec['Data_Type'] = 45
                comps = datastream.header.get('DataComponents','')
                keyup = key.upper()
                if key in ['t1','t2']:
                    cdfkey = key.upper().replace('T','Temperature')
                elif not comps == '':
                    try:
                        if key == 'x':
                            compsupper = comps[0].upper()
                        elif key == 'y':
                            compsupper = comps[1].upper()
                        elif key == 'z':
                            compsupper = comps[2].upper()
                        elif key == 'f':
                            compsupper = fcolname ## MagPy requires independend F value
                        elif key == 'df':
                            compsupper = 'G'
                        else:
                            compsupper = key.upper()
                        cdfkey = 'GeomagneticField'+compsupper
                        keyup = compsupper
                    except:
                        cdfkey = 'GeomagneticField'+key.upper()
                        keyup = key.upper()
                else:
                    cdfkey = 'GeomagneticField'+key.upper()

                nonetest = [elem for elem in col if not elem == None]
                if len(nonetest) > 0:
                    cdfdata = col
                    var_attrs['DEPEND_0'] = "GeomagneticVectorTimes"
                    var_attrs['DISPLAY_TYPE'] = "time_series"
                    var_attrs['LABLAXIS'] = keyup
                    var_attrs['FILLVAL'] = np.nan
                    if key in ['x','y','z','h','e','g','t1','t2']:
                        var_attrs['VALIDMIN'] = -88880.0
                        var_attrs['VALIDMAX'] = 88880.0
                    elif key == 'i':
                        var_attrs['VALIDMIN'] = -90.0
                        var_attrs['VALIDMAX'] = 90.0
                    elif key == 'd':
                        var_attrs['VALIDMIN'] = -360.0
                        var_attrs['VALIDMAX'] = 360.0
                    elif key in ['f','s']:
                        if useScalarTimes:
                            if len(naninds) > 0:
                                cdfdata = col[~np.isnan(col)]
                            var_attrs['DEPEND_0'] = "GeomagneticScalarTimes"
                        #else:
                        #    mycdf[cdfkey] = col
                        var_attrs['VALIDMIN'] = 0.0
                        var_attrs['VALIDMAX'] = 88880.0

                for keydic in headers:
                    if keydic == ('col-'+key):
                        if key in ['x','y','z','f','dx','dy','dz','df']:
                            try:
                                var_attrs['FIELDNAM'] = "Geomagnetic Field Element "+key.upper()
                            except:
                                pass
                        if key in ['t1','t2']:
                            try:
                                var_attrs['FIELDNAM'] = "Temperature"+key.replace('t','')
                            except:
                                pass
                    if keydic == ('unit-col-'+key):
                        if key in ['x','y','z','f','dx','dy','dz','df','t1','t2']:
                            try:
                                unit = 'unspecified'
                                if 'unit-col-'+key == 'deg C':
                                    #mycdf[cdfkey].attrs['FIELDNAM'] = "Temperature "+key.upper()
                                    unit = 'Celsius'
                                elif 'unit-col-'+key == 'deg':
                                    unit = 'Degrees of arc'
                                else:
                                    unit = headers.get('unit-col-'+key,'')
                                var_attrs['UNITS'] = unit
                            except:
                                pass
            var_spec['Variable'] = cdfkey
            var_spec['Num_Elements'] = 1
            var_spec['Rec_Vary'] = True # The dimensional sizes, applicable only to rVariables.
            var_spec['Dim_Sizes'] = []

            mycdf.write_var(var_spec, var_attrs=var_attrs, var_data=cdfdata)

    success = filename

    if len(flaglist) > 0 and addflags == True:
        flagstart = 'FlagBeginTimes'
        flagend = 'FlagEndTimes'
        flagcomponents = 'FlagComponents'
        flagcode = 'FlagCode'
        flagcomment = 'FlagDescription'
        flagmodification = 'FlagModificationTimes'
        flagsystemreference = 'FlagSystemReference'
        flagobserver = 'FlagObserver'

        trfl = np.transpose(flaglist)
        #print ("Transposed flaglist", trfl)
        #try:
        ok =True
        if ok:
            mycdf.new(flagstart, type=cdf.const.CDF_TIME_TT2000)
            mycdf[flagstart] = cdf.lib.v_datetime_to_tt2000(trfl[0])
            mycdf.new(flagend, type=cdf.const.CDF_TIME_TT2000)
            mycdf[flagend] = cdf.lib.v_datetime_to_tt2000(trfl[1])
            mycdf.new(flagmodification, type=cdf.const.CDF_TIME_TT2000)
            mycdf[flagmodification] = cdf.lib.v_datetime_to_tt2000(trfl[-1])

            # Here we can select between different content
            if len(flaglist[0]) == 7:
                #[st,et,key,flagnumber,commentarray[idx],sensorid,now]
                # eventually change flagcomponent in the future
                fllist = [flagcomponents,flagcode,flagcomment, flagsystemreference] # , flagobserver]
            elif len(flaglist[0]) == 8:  
                # Future version ??
                fllist = [flagcomponents,flagcode,flagcomment, flagsystemreference, flagobserver]
            for idx, cdfkey in enumerate(fllist):
                if not cdfkey == flagcode:
                    ll = [el.encode('UTF8') for el in trfl[idx+2]]
                else:
                    ll = trfl[idx+2]
                mycdf[cdfkey] = ll
                mycdf[cdfkey].attrs['DEPEND_0'] = "FlagBeginTimes"
                mycdf[cdfkey].attrs['DISPLAY_TYPE'] = "time_series"
                mycdf[cdfkey].attrs['LABLAXIS'] = cdfkey.strip('Flag')
                mycdf[cdfkey].attrs['FILLVAL'] = np.nan
                mycdf[cdfkey].attrs['FIELDNAM'] = cdfkey
                if cdfkey in ['flagcode']:
                    mycdf[cdfkey].attrs['VALIDMIN'] = 0
                    mycdf[cdfkey].attrs['VALIDMAX'] = 9
        #except:
        #    print ("writeIMAGCDF: error when adding flags. skipping this part")
        logger.info("writeIMAGCDF: Flagging information added to file")

    mycdf.close()
    return success


