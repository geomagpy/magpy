"""
MagPy
Intermagnet ImagCDF input filter
(based on cdflib)
Written by Roman Leonhardt October 2019
- contains test, read and write functions for
        ImagCDF
- supports python >= 3.7
- currently requires 1.0.0 >= cdflib <= 1.3.3
"""
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import DataStream, read, subtract_streams, join_streams, magpyversion, example4
from magpy.core.methods import testtime, convert_geo_coordinate, is_number, dictdiff
from magpy.core import flagging
from datetime import datetime, timedelta, timezone
import os
import numpy as np
import cdflib
import logging
logger = logging.getLogger(__name__)

KEYLIST = DataStream().KEYLIST
NUMKEYLIST = DataStream().NUMKEYLIST
HEADTRANSLATE = {'FormatDescription':'DataFormat',
                 'IagaCode':'StationID',
                 'ElementsRecorded':'DataComponents',
                 'ObservatoryName':'StationName',
                 'Latitude':'DataAcquisitionLatitude',
                 'Longitude':'DataAcquisitionLongitude',
                 'Institution':'StationInstitution',
                 'VectorSensOrient':'DataSensorOrientation',
                 'TermsOfUse':'DataTerms',
                 'UniqueIdentifier':'DataID',
                 'ParentIdentifiers':'SensorID',
                 'ReferenceLinks':'StationWebInfo',
                 'FlagRulesetType':'FlagRulesetType',
                 'FlagRulesetVersion':'FlagRulesetVersion'}

FLOATLIST = ['DataAcquisitionLatitude','DataAcquisitionLongitude','DataElevation']

#'ReferenceLinks':'StationWebInfo',


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
        if isinstance(form, list):
            #cdflib 1.0.x
            form = form[0]
        if not form.startswith('INTERMAGNET'):
            return False
    except:
        return False

    logger.debug("isIMAGCDF: Found INTERMAGNET CDF data - using cdflib")
    return True

def readIMAGCDF(filename, headonly=False, **kwargs):
    """
    Reading Intermagnet CDF format (1.0,1.1,1.2)
    """

    debug = kwargs.get('debug')
    select = kwargs.get('select')

    headers={}
    array = [[] for elem in KEYLIST]
    multipletimedict = {}
    newdatalist = []
    tllist = []
    flags = {}
    referencetimecol = []
    indexarray = np.asarray([])
    cdfversion=0.9
    # time conversion datetime64 to datetime
    ue = np.datetime64(0,'s')
    onesec = np.timedelta64(1,'s')

    def Ruleset2Flaglist(flagginglist,rulesettype,rulesetversion,stationid=None, debug=False):
        """
        DESCRIPTION
            convert flags of MagPy1.x and 1.3 versions of ImagCDF to a flaglist again
            DEPRECATED as since MagPy2.0 flags are part of the header information (json dump)
        :param flagginglist:
        :param rulesettype:
        :param rulesetversion:
        :param stationid:
        :param debug:
        :return:
        """
        if debug:
            print ("Rules", rulesettype, rulesetversion)
        if rulesettype in ['Conrad', 'conrad', 'MagPy','magpy'] and len(flagginglist) > 0:
            if rulesetversion in ['1.0','1',1,'2.0',2]:
                flagcolsconrad = [flagginglist[0],flagginglist[1],flagginglist[3],flagginglist[4],flagginglist[5],flagginglist[6],flagginglist[2]]
                flaglisttmp = []
                #'FlagBeginTimes', 'FlagEndTimes', 'FlagComponents', 'FlagCode', 'FlagDescription', 'FlagSystemReference', 'FlagModificationTimes']
                #add(self, sensorid=None, starttime=None, endtime=None, components=None, flagtype=0, labelid='000',
                #    label='',
                #    comment='', groups=None, probabilities=None, stationid='', validity='', operator='', color='',
                #    modificationtime=None, flagversion='2.0', debug=False)
                for elem in flagcolsconrad:
                    flaglisttmp.append(cdfdat[elem][...])
                flaglisttmp[0] = cdflib.cdfepoch.to_datetime(flaglisttmp[0])
                # convert datetime64 to datetime
                ar = flaglisttmp[0]
                ar = (ar - ue) / onesec
                flaglisttmp[0] = np.asarray([datetime.utcfromtimestamp(el) for el in ar])  # datetime.datetime
                flaglisttmp[1] = cdflib.cdfepoch.to_datetime(flaglisttmp[1])
                ar = flaglisttmp[1]
                ar = (ar - ue) / onesec
                flaglisttmp[1] = np.asarray([datetime.utcfromtimestamp(el) for el in ar])  # datetime.datetime
                flaglisttmp[-1] = cdflib.cdfepoch.to_datetime(flaglisttmp[-1])
                ar = flaglisttmp[-1]
                ar = (ar - ue) / onesec
                flaglisttmp[-1] = np.asarray([datetime.utcfromtimestamp(el) for el in ar])  # datetime.datetime
                flaglist = np.transpose(flaglisttmp)
                flaglist = [list(elem) for elem in flaglist]
                unique = []
                for el in flaglist:
                    if [el[0],el[1],el[3],el[4],el[5],el[6]] not in unique:
                        unique.append([el[0],el[1],el[3],el[4],el[5],el[6]])
                flags = flagging.Flags()
                for un in unique:
                    comps = []
                    for el in flaglist:
                        newel = [el[0],el[1],el[3],el[4],el[5],el[6]]
                        if un == newel:
                            comps.append(el[2])
                    if len(comps) == 1 and comps[0].find(",") >= 0:
                        # In case comps already consists of a comma separated string with individual components
                        comps = comps[0].split(",")
                    flags.add(sensorid=un[4], starttime=un[0], endtime=un[1], components=comps, flagtype=int(un[2]), labelid='099', comment=un[3], modificationtime=un[5])
                return flags
            else:
                return {}
        else:
            if debug:
                print ("readIMAGCDF: Could  not interpret flagging ruleset or flagging object is empty")
            logger.warning("readIMAGCDF: Could  not interpret Ruleset")
            return {}


    cdfdat = cdflib.CDF(filename)

    if debug:
        print ("Reading ImagCDF with cdflib")
        print ("  - debug selected: extracting cdfinfo, cdf.globalattributes and ...")
        headers['CDFInfo'] = cdfdat.cdf_info()
        headers['CDFGlobalAttributes'] = cdfdat.globalattsget()

    if select and debug:
        print ("Only data associated with {} time column will be extracted".format(select))

    for att in cdfdat.globalattsget():
        value = cdfdat.globalattsget().get(att)
        if debug:
            print ("readIMAGCDF: ", att, value)
        try:
            if isinstance(list(value), list):
                if len(value) == 1:
                    value = value[0]
        except:
            pass
        if not att in HEADTRANSLATE: # exclude magpy types
            if not att.startswith("Station") and not att.startswith("Sensor") and not att.startswith("Column"):
                attname = 'Data'+att
            else:
                attname = att
        else:
            attname = HEADTRANSLATE[att]
        headers[attname] = value

    # eventually correct webrefs with http
    webrefs = cdfdat.globalattsget().get('ReferenceLinks',[])
    webrefsl = []
    if webrefs and isinstance(webrefs,(list,tuple)):
        for wr in webrefs:
            if wr and not wr.find("://") >= 0:
                wr = "https://"+wr
            webrefsl.append(wr)
    elif webrefs:
        webrefsl = webrefs.split(',')
        webrefsl = ["https://"+el if not el.find('://') >= 0 else el for el in webrefsl]
    headers['StationWebInfo'] = webrefsl #",".join(webrefsl)

    # consider other list like contents
    listlikecontents = {'Institution' : 'StationInstitution'}
    for listlikecont in listlikecontents:
        lconlst = []
        lconts = cdfdat.globalattsget().get(listlikecont,[])
        if lconts and isinstance(lconts,(list,tuple)):
            for lco in lconts:
                lconlst.append(lco)
        elif lconts:
            lconlst = lconts.split(',')
        newname = listlikecontents.get(listlikecont)
        headers[newname] = lconlst

    formatvers = cdfdat.globalattsget().get('FormatVersion')
    if isinstance(formatvers, list):
        formatvers = formatvers[0]
    #Some specials:
    headers['StationIAGAcode'] = headers.get('StationID')
    headers['DataFormat'] = headers.get('DataFormat') + '; ' + formatvers

    try:
        try:
            pubdate = cdflib.cdfepoch.to_datetime(cdflib.cdfepoch,headers.get('DataPublicationDate'))
        except TypeError:
            pubdate = cdflib.cdfepoch.to_datetime(headers.get('DataPublicationDate'))
        try:
            headers['DataPublicationDate'] = testtime(pubdate[0])
        except:
            headers['DataPublicationDate'] = pubdate[0]
        if debug:
            print("Got publication date as", type(headers['DataPublicationDate']))
    except:
        if debug:
            print ("imagcdf warning: Publication date is not provided as tt_2000")
        try:
            pubdate = testtime(headers.get('DataPublicationDate'))
            headers['DataPublicationDate'] = pubdate
        except:
            pass

    if debug:
        logger.info("readIMAGCDF: FOUND IMAGCDF file created with version {}".format(headers.get('DataFormat')))
        print (" - readIMAGCDF: header almost done")

    # Get all available Variables - ImagCDF usually uses only zVariables
    try:
        datalist = cdfdat.cdf_info().get('zVariables')
        cdfversion = 0.9
    except:
        datalist = cdfdat.cdf_info().zVariables
        cdfversion = 1.0
    #if debug:
    #    print (" - cdfversion:", cdflib.__version__)
    if cdfversion < 1.0:
        lsu = cdfdat.cdf_info().get('LeapSecondUpdated')
        if not lsu:
            lsu = cdfdat.cdf_info().get('LeapSecondUpdate')
    else:
        try:
            lsu = cdfdat.cdf_info().LeapSecondUpdate
        except:
            lsu = ""
    headers['DataLeapSecondUpdated'] = lsu
    if debug:
        lsu = cdfdat.cdf_info().LeapSecondUpdate
        print ("LEAP seconds updated:", lsu)

    #  IAGA code
    if headers.get('SensorID','') == '':
        try:
            headers['SensorID'] = "{}_{}_{}".format(headers.get('StationIAGAcode','xxx').upper()+'sec',headers.get('DataPublicationLevel','0'),'0001')
        except:
            pass

    if debug:
        print (" - readIMAGCDF: header done")
        print (headers)
    # #########################################################
    # 1. Now getting individual data and check time columns
    # #########################################################
    zpos = KEYLIST.index('z') # used for idf records
    for elem in datalist:
        if elem.endswith('Times') and not elem.startswith('Flag'):
            try:
                #if elem in ['GeomagneticVectorTimes','GeomagneticTimes','GeomagneticScalarTimes']:
                if cdfversion<1.0:
                    larec = cdfdat.varinq(elem).get('Last_Rec')
                else:
                    larec = cdfdat.varinq(elem).Last_Rec
                tl = int(larec)+1
                tllist.append([tl,elem])
            except:
                pass

    ts = datetime.now()
    if len(tllist) < 1:
        """
        No time column identified
        -> Check for starttime and sampling rate in header
        """
        if  cdfdat.globalattsget().get('StartTime','') and cdfdat.globalattsget().get('SamplingPeriod',''):
            # TODO  - not now but eventually a future improvment - remove time columns and replace by start/increment
            st = cdfdat.globalattsget().get('StartTime','')
            sr = cdfdat.globalattsget().get('SamplingPeriod','')
            # get length of f or x
        else:
            logger.error("readIMAGCDF: No Time information available - aborting")
            return
    elif len(tllist) > 1:
        # Found at least on time column
        # tllist ~ [[86400, 'GeomagneticVectorTimes'], [17280, 'GeomagneticScalarTimes'], [144, 'TemperatureTimes']]
        tllist = sorted(tllist)
        tl = [el[0] for el in tllist]
        namelst = [el[1] for el in tllist]
        if not max(tl) == min(tl):
            if debug:
                print (" Found multiple time columns in file with different lengths")
                print (tllist)
            timecol = None
            pos = -1
            if select:
                for idx,na in enumerate(namelst):
                    nam = na.lower()
                    if nam.find(select.lower()) > -1:
                        timecol = na
                        referencetimecol = [na]
                        pos = idx
            else:
                if debug:
                    print("readIMAGCDF: Time columns of different length. Choosing only longest. Use option select")
                    print("             and check header FileContents for available contents")
                    # get list with max length
                maxlenlist = [el[1] for el in tllist if el[0] == max(tl)]
                if 'GeomagneticVectorTimes' in maxlenlist:
                    timecol = 'GeomagneticVectorTimes'
                elif 'GeomagneticTimes' in maxlenlist:
                    timecol = 'GeomagneticTimes'
                else:
                    timecol = maxlenlist[0]
                referencetimecol = maxlenlist
            if not timecol:
                timecol = tllist[tl.index(max(tl))][1]
                referencetimecol = [timecol]
            newdatalist.append(['time',timecol])
            if debug:
                print (" selected primary time column: {}".format(timecol))
            headers['FileContents'] = tllist
        else:
            logger.info("readIMAGCDF: Equal length time axes found - assuming identical time")
            if 'GeomagneticVectorTimes' in datalist:
                newdatalist.append(['time','GeomagneticVectorTimes'])
            else:
                newdatalist.append(['time',tllist[0][1]]) # Take the first one
    else:
        #"Single time axis found in file"
        newdatalist.append(['time',tllist[0][1]])
    te = datetime.now()

    if headers.get("DataFlags", ""):
        if debug:
            print ("Found flags in header - MagPy 2.0 version - nothing else to do")
        fl = flagging.Flags()
        flagstring = headers.get("DataFlags", flagging.Flags())
        try:
            flags = fl._readJson_string(flagstring, debug=debug)
            headers["DataFlags"] = flags
        except:
            print("Error when interpreting flags - skipping")
    elif not headers.get('FlagRulesetType','') == '':
        if debug:
            print ("readIMAGCDF: Found flagging ruleset {} vers.{} - extracting flagging information".format(headers.get('FlagRulesetType',''),headers.get('FlagRulesetVersion','')))
        logger.info("readIMAGCDF: Found flagging ruleset {} vers.{} - extracting flagging information".format(headers.get('FlagRulesetType',''),headers.get('FlagRulesetVersion','')))
        flagginglist = [elem for elem in datalist if elem.startswith('Flag')]
        flags = Ruleset2Flaglist(flagginglist,headers.get('FlagRulesetType',''),headers.get('FlagRulesetVersion',''), stationid=headers.get("StationID",None), debug=debug)
        flags = flags._set_label_from_comment()
        if debug:
            print ("readIMAGCDF: Flagging information extracted")

    if debug:
        print ("readIMAGCDF: Needed here {}".format((te-ts).total_seconds()))
    datalist = [elem for elem in datalist if not elem.endswith('Times') and not elem.startswith('Flag')]

    # #########################################################
    # 2. Sort the datalist according to KEYLIST
    # #########################################################
    for key in KEYLIST:
        # TODO: V (field strength along inclination is not yet supported)
        possvals = [key]
        if key == 'x':
            possvals.extend(['h','i'])
        if key == 'y':
            possvals.extend(['d','e'])
        if key == 'df':
            possvals.append('g')
        if key == 'f':
            possvals.append('s')
        if key == 't1':
            possvals.extend(['temperature 1','temperature1'])
        if key == 't2':
            possvals.extend(['temperature 2','temperature2'])
        for elem in datalist:
            if debug:
                print ("Variable attributes:", elem, cdfdat.varattsget(elem))
            try:
                label = cdfdat.varattsget(elem).get('LABLAXIS').lower()
                if label in possvals:
                    newdatalist.append([key,elem])
            except:
                pass # for lines which have no Label

    if debug:
        print ("readIMAGCDF: Components in file: {}".format(newdatalist))

    if not len(datalist) == len(newdatalist)-1:
        logger.warning("readIMAGCDF: error encountered in key assignment - please check")

    # #########################################################
    # (4. eventually completely drop time cols and just store start date and sampling period in header)
    # Deal with scalar data (independent or whatever

    delrow = False
    index = 0
    for elem in newdatalist:
        if elem[0] == 'time':
            if cdfversion < 1.0:
                ttdesc = cdfdat.varinq(elem[1]).get('Data_Type_Description')
            else:
                ttdesc = cdfdat.varinq(elem[1]).Data_Type_Description
            col = cdfdat.varget(elem[1])
            try:
                # cdflib version (<0.3.19... Problem: cdflib.cdfepoch.getVersion() does not change, although to_datetime is different and unixtime as well)
                ar = cdflib.cdfepoch.to_datetime(cdflib.cdfepoch,col)
                cdfvers = 18
            except TypeError:
                # cdflib version (>=0.3.19)
                ar = cdflib.cdfepoch.to_datetime(col)
                cdfvers = 19
            except:
                # if second value is 60 (tt_2000 leapsecond timestamp) cdfepoch.unixtime fails
                print ("File contains a leap second - will be ignored")
                seccol = np.asarray([row[5] for row in cdflib.cdfepoch.breakdown(col)])
                # assume that seccol contains a 60 seconds step - identify and remove
                index = seccol.argmax()
                col = np.delete(col,index)
                try:
                    ar = cdflib.cdfepoch.to_datetime(cdflib.cdfepoch,col)
                except TypeError:
                    ar = cdflib.cdfepoch.to_datetime(col)
                delrow = True
            arlen= len(ar)
            ind = KEYLIST.index('time')
            ar = (ar-ue)/onesec
            #array[ind] = np.array(ar, dtype=datetime)  # datetime.datetime # doe snot work any more with cdflib 1.3.3
            array[ind] = np.asarray([datetime.utcfromtimestamp(el) for el in ar])  # datetime.datetime
            #array[ind] = ar  # np.datetime64 might be quicker (NO!) but breaks other methods as total_seconds not defined
        else:
            ar = cdfdat.varget(elem[1])
            if delrow:
                ar = np.delete(ar,index)
            if elem[0] in NUMKEYLIST:
                timedepend = cdfdat.varattsget(elem[1]).get('DEPEND_0')
                if referencetimecol:
                    if timedepend in referencetimecol:
                        fillval = cdfdat.varattsget(elem[1]).get('FILLVAL')
                        headers['CDFfillval'] = fillval
                        if np.isnan(fillval):
                            # if it is nan than the following replace wont work anyway
                            fillval = 99999.0
                        with np.errstate(invalid='ignore'):
                            ar[ar > fillval - 1] = float(np.nan)
                        ind = KEYLIST.index(elem[0])
                        headers['col-' + elem[0]] = cdfdat.varattsget(elem[1]).get('LABLAXIS').lower()
                        headers['unit-col-' + elem[0]] = cdfdat.varattsget(elem[1]).get('UNITS')
                        array[ind] = ar
                else:
                    fillval = cdfdat.varattsget(elem[1]).get('FILLVAL')
                    headers['CDFfillval'] = fillval
                    if np.isnan(fillval):
                        # if it is nan than the following replace wont work anyway
                        fillval = 99999.0
                    with np.errstate(invalid='ignore'):
                        ar[ar > fillval - 1] = float(np.nan)
                    ind = KEYLIST.index(elem[0])
                    headers['col-' + elem[0]] = cdfdat.varattsget(elem[1]).get('LABLAXIS').lower()
                    headers['unit-col-' + elem[0]] = cdfdat.varattsget(elem[1]).get('UNITS')
                    array[ind] = ar
                if elem[0] in ['f','F'] and headers.get('DataComponents','') in ['DIF','dif','idf','IDF'] and not len(array[zpos]) > 0:
                    array[zpos] = ar
                    headers['col-z'] = cdfdat.varattsget(elem[1]).get('LABLAXIS').lower()
                    headers['unit-col-z'] = cdfdat.varattsget(elem[1]).get('UNITS')

    ndarray = np.asarray(array, dtype=object)
    result = DataStream(header=headers,ndarray=ndarray)

    # if flags have been extracted in MagPy1.x manner then add them here
    if not headers.get('FlagRulesetType','') == '' and flags and not result.header.get("DataFlags",""):
        result.header["DataFlags"] = flags

    return result


def writeIMAGCDF(datastream, filename, **kwargs):
    """
    Writing Intermagnet CDF format (currently: vers1.3) + optional flagging info (vers1.3.1)

    """
    debug = kwargs.get('debug')
    fillval = kwargs.get('fillvalue')
    mode = kwargs.get('mode')
    scalar = kwargs.get('scalar')
    temperature1 = kwargs.get('temperature1')
    temperature2 = kwargs.get('temperature2')
    addflags = kwargs.get('addflags')
    skipcompression = kwargs.get('skipcompression')

    flags = {}
    logger.info("Writing IMAGCDF Format {}".format(filename))
    if debug:
        print (" Writing CDF data based on cdflib")

    if not fillval:
        fillval = np.nan
    else:
        if debug:
            print (" Fillvalue of {} selected for filling gaps".format(fillval))

    def tt(my_dt_ob):
        ms = my_dt_ob.microsecond/1000.  # fraction
        date_list = [my_dt_ob.year, my_dt_ob.month, my_dt_ob.day, my_dt_ob.hour, my_dt_ob.minute, my_dt_ob.second, ms]
        return date_list

    datastream = datastream._remove_nancolumns()

    main_cdf_spec = {}
    main_cdf_spec['Compressed'] = False

    # Version dependent extraction of LeapSecond information
    try:
        leapsecondlastupdate = cdflib.cdfepoch.getLeapSecondLastUpdated()
    except:
        try:
            leapsecondlastupdate = cdflib.cdfepoch.LTS[-1]
            leapsecondlastupdate = datetime(leapsecondlastupdate[0],leapsecondlastupdate[1],leapsecondlastupdate[2])
        except:
            leapsecondlastupdate = ""
    if debug:
        print ("LastLeapSecond", leapsecondlastupdate)

    # Does not work
    #main_cdf_spec['LeapSecondUpdate'] = int(datetime.strftime(leapsecondlastupdate, "%Y%m%d"))

    if not skipcompression:
        try:
            main_cdf_spec['Compressed'] = True
        except:
            logger.warning("writeIMAGCDF: Compression failed for unknown reason - storing uncompressed data")
            pass

    testname = str(filename+'.cdf')

    if os.path.isfile(testname):
        filename = testname
    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(filename)
            datastream = join_streams(exst,datastream)
            os.remove(filename)
            try:
                mycdf = cdflib.cdfwrite.CDF(filename,cdf_spec=main_cdf_spec)
            except:
                mycdf = cdflib.CDF(filename, cdf_spec=main_cdf_spec)
        elif mode == 'replace' or mode == 'append': # replace existing inputs
            exst = read(filename)
            datastream = join_streams(datastream, exst)
            os.remove(filename)
            try:
                mycdf = cdflib.cdfwrite.CDF(filename, cdf_spec=main_cdf_spec)
            except:
                mycdf = cdflib.CDF(filename, cdf_spec=main_cdf_spec)
        else: # overwrite mode
            os.remove(filename)
            try:
                mycdf = cdflib.cdfwrite.CDF(filename, cdf_spec=main_cdf_spec)
            except:
                mycdf = cdflib.CDF(filename, cdf_spec=main_cdf_spec)
    else:
        try:
            mycdf = cdflib.cdfwrite.CDF(filename, cdf_spec=main_cdf_spec)
        except:
            mycdf = cdflib.CDF(filename, cdf_spec=main_cdf_spec)


    if debug:
        print ("CDF specifications:", main_cdf_spec)
    keylst = datastream._get_key_headers()
    tmpkeylst = ['time']
    tmpkeylst.extend(keylst)
    keylst = tmpkeylst

    headers = datastream.header

    # check DataComponents for correctness
    dcomps = headers.get('DataComponents','')
    dkeys = datastream.variables()
    if 'f' in dkeys and len(dcomps) == 3:
        dcomps = dcomps+'S'
    elif scalar and len(dcomps) == 3:
        dcomps = dcomps + 'S'
    elif (scalar or 'f' in dkeys) and len(dcomps) == 4:
        dcomps = dcomps[:3] + 'S'
    if 'df' in dkeys and not 'f' in dcomps and len(dcomps) == 3:
        dcomps = dcomps+'G'
    headers['DataComponents'] = dcomps
    if debug:
        print ("Components after checking:", dcomps)

    ### #########################################
    ###            Check Header
    ### #########################################

    INVHEADTRANSLATE = {v: k for k, v in HEADTRANSLATE.items()}
    INVHEADTRANSLATE['StationIAGAcode'] = 'IagaCode'

    globalAttrs = {}
    for key in headers:
        value = headers.get(key)
        if key == "DataFlags" and value and not isinstance(value,str):
            # might be string already in case of multiple file export
            value = value._writeJson_string()
        if is_number(value) and key in FLOATLIST:
            value = float(value)
        else:
            value = str(value)
        if key in INVHEADTRANSLATE:
            globalAttrs[INVHEADTRANSLATE.get(key)] = { 0 : value }
        elif key.startswith('col-') or key.startswith('unit-') or key.endswith('SamplingRate'):
            pass
        elif key == "DataFlags" and not addflags:
            # ignpore flags
            pass
        else:
            globalAttrs[key.replace('Data','',1)] = { 0 : value }


    ## 1. Fixed Part -- current version is 1.3.0
    ## Transfer MagPy Header to INTERMAGNET CDF attributes
    globalAttrs['FormatDescription'] = { 0 : 'INTERMAGNET CDF format'}
    globalAttrs['FormatVersion'] = { 0 : '1.3'}
    globalAttrs['Title'] = { 0 : 'Geomagnetic time series data'}
    globalAttrs['CdflibVersion'] = { 0 : cdflib.__version__}

    ## 3. Optional flagging information
    ##    identify flags within the data set and if they are present then add an attribute to the header
    if addflags and datastream.header.get("DataFlags"):
        globalAttrs['FormatVersion'] = { 0 : '1.3.1'}
    if addflags:
        flags = datastream.header.get("DataFlags", flagging.Flags())
        if flags:
            globalAttrs['FlagRulesetVersion'] = { 0 : '2.0'}
            globalAttrs['FlagRulesetType'] = { 0 : 'Conrad'}
            # if not already converted to strings
            if not isinstance(flags, str):
                datastream.header["DataFlags"] = flags
                flags = flags._writeJson_string()
            #datastream.header["DataFlags"] = flags

    if not headers.get('DataPublicationDate','') == '':
        dat = tt(testtime(headers.get('DataPublicationDate','')))
        pubdate = cdflib.cdfepoch.compute_tt2000([dat])
    else:
        pubdate = cdflib.cdfepoch.compute_tt2000([tt(datetime.now(timezone.utc).replace(tzinfo=None))])
    if isinstance(pubdate,np.ndarray):
        pubdate = pubdate.item()
    globalAttrs['PublicationDate'] = { 0 : pubdate }

    # add leapseconds to global atts
    try:
        leapex = globalAttrs.get("LeapSecondUpdated")
        if leapsecondlastupdate and not leapex:
            lslu = int(datetime.strftime(leapsecondlastupdate,"%Y%m%d"))
            globalAttrs['LeapSecondUpdated'] = {0: lslu}
    except:
        pass

    if not headers.get('DataSource','')  == '':
        if headers.get('DataSource','') in ['INTERMAGNET', 'WDC']:
            globalAttrs['Source'] =  { 0 : headers.get('DataSource','')}
        else:
            globalAttrs['Source'] =  { 0 : headers.get('DataSource','')}
    else:
        globalAttrs['Source'] =  { 0 : headers.get('StationInstitution','')}

    # Webaddress and institution lists
    if not headers.get('StationWebInfo','') == '':
        swl = headers.get('StationWebInfo')
        if isinstance(swl, (list,tuple)):
            globdict = {}
            for insti, instel in enumerate(swl):
                globdict[insti] = instel
            globalAttrs['ReferenceLinks'] = globdict
        else:
            globalAttrs['ReferenceLinks'] = { 0 : headers.get('StationInstitution', '')}
    if not headers.get('StationInstitution','') == '':
        sil = headers.get('StationInstitution','')
        if isinstance(sil, (list,tuple)):
            globdict = {}
            for insti, instel in enumerate(sil):
                globdict[insti] = instel
            globalAttrs['Institution'] = globdict
        else:
            globalAttrs['Institution'] = { 0 : headers.get('StationInstitution', '')}

    if not headers.get('DataStandardLevel','') == '':
        if headers.get('DataStandardLevel','') in ['None','none','Partial','partial','Full','full']:
            globalAttrs['StandardLevel'] = { 0 : headers.get('DataStandardLevel','')}
        else:
            print("writeIMAGCDF: StandardLevel not defined - please specify by yourdata.header['DataStandardLevel'] = ['None','Partial','Full']")
            globalAttrs['StandardLevel'] = { 0 : 'None'}
        if headers.get('DataStandardLevel','') in ['partial','Partial']:
            # one could add a validity check whether provided list is aggreement with standards
            if headers.get('DataPartialStandDesc','') == '' and headers.get('PartialStandDesc','') == '':
                print("writeIMAGCDF: PartialStandDesc is missing. Add items like IMOM-11,IMOM-12,IMOM-13 ...")
    else:
        print("writeIMAGCDF: StandardLevel not defined - please specify by yourdata.header['DataStandardLevel'] = ['None','Partial','Full']")
        globalAttrs['StandardLevel'] = { 0 : 'None'}

    if not headers.get('DataStandardName','') == '':
        globalAttrs['StandardName'] = { 0 : headers.get('DataStandardName','')}
    else:
        try:
            #print ("writeIMAGCDF: Assigning StandardName")
            stdadd = 'INTERMAGNET'
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
            print ("writeIMAGCDF: Assigning StandardName Failed")


    proj = headers.get('DataLocationReference','')
    longi = headers.get('DataAcquisitionLongitude','')
    lati = headers.get('DataAcquisitionLatitude','')
    ele = headers.get('DataElevation','')
    try:
        longi = "{:.3f}".format(float(longi))
        lati = "{:.3f}".format(float(lati))
    except:
        print("writeIMAGCDF: could not convert lat long to floats")
    try:
        ele = "{:.3f}".format(float(ele))
    except:
        print("writeIMAGCDF: could not convert elevation to float")
    if not ele=='':
        globalAttrs['Elevation'] = { 0 : float(ele) }
    if not longi=='' or lati=='':
        if is_number(longi):
            longi = float(longi)
        if is_number(lati):
            lati = float(lati)
        if proj == '':
            globalAttrs['Latitude'] = { 0 : lati }
            globalAttrs['Longitude'] = { 0 : longi }
        else:
            if proj.find('EPSG:') > 0:
                epsg = int(proj.split('EPSG:')[1].strip())
                # currently a problem as old WIC data has already been converted to WGS but reference was not updated
                # result -> will try to convert again whenever called
                # therefore I included some workaround which should not break anything else
                if not epsg==4326:
                    if lati < 48 and lati > 47 and longi < 16 and longi > 15:
                        # workaround for wrong WIC data
                        headers['DataLocationReference'] = 'WGS84, EPSG: 4326'
                        globalAttrs['LocationReference'] =  { 0 : 'WGS84, EPSG: 4326' }
                    else:
                        print ("writeIMAGCDF: converting coordinates from {} to epsg 4326".format(epsg))
                        print (lati,longi, epsg)
                        longi,lati = convert_geo_coordinate(longi,lati,'epsg:'+str(epsg),'epsg:4326')
                        longi = "{:.3f}".format(longi)
                        lati = "{:.3f}".format(lati)
                        print (lati,longi, epsg)
                        # Important update location reference
                        headers['DataLocationReference'] = 'WGS84, EPSG: 4326'
                        globalAttrs['LocationReference'] =  { 0 : 'WGS84, EPSG: 4326' }

            globalAttrs['Latitude'] = { 0 : float(lati) }
            globalAttrs['Longitude'] = { 0 : float(longi) }

    if not 'StationIagaCode' in headers and 'StationID' in headers:
        globalAttrs['IagaCode'] = { 0 : headers.get('StationID','')}

    # writing of global attributes after checking for independency of eventually provided F (S) record - line 595

    ### #########################################
    ###               Data
    ### #########################################
    #def checkEqualIvo(lst):
    #    # http://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
    #    return not lst or lst.count(lst[0]) == len(lst)

    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    ndarray = False
    if len(datastream.ndarray[0])>0:
        ndarray = True

    # Check F/S/G select either S or G, send out warning if presumably F (mean zero, stddeviation < resolution)
    naninds = np.asarray([])
    ## Analyze F and dF columns:
    fcolname = 'S'
    scal = ''
    ftest = DataStream()
    useScalarTimes = False
    ftimecol = np.asarray([])
    t1timecol = np.asarray([])
    t2timecol = np.asarray([])
    fcol, t1col, t2col = [],[],[]
    mainsamprate = datastream.samplingrate()

    if 'f' in keylst or 'df' in keylst:
        if 'f' in keylst:
             scal = 'f'
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
                     print ("writeIMAGCDF: analyzed F column - values are apparently independent from vector components - using column name 'S'")
             if 'df' in keylst:
                 # if df is also part of the data stream, then remove this one
                 datastream = datastream._drop_column('df')
        elif 'df' in keylst:
            scal = 'df'
            if not 'f' in keylst:
                fcolname = 'G'

        # Check sampling rates of main stream and f/df stream
        ftest = datastream.copy()
        ftest = ftest._drop_nans(scal)
        fsamprate = ftest.samplingrate()
        if ftest.length()[0] > 0:
            ftest = ftest.get_gaps()
        ftimecol = ftest.ndarray[0]
        fcol = datastream._get_column(scal)
        if fsamprate-0.1 < mainsamprate and mainsamprate < fsamprate+0.1:
            #Samplingrate of F column and Vector are similar
            useScalarTimes=False
        else:
            useScalarTimes=True
            fcol = ftest._get_column(scal)
    elif scalar:
        fkey = KEYLIST.index('f')
        fcol = scalar.ndarray[fkey]
        ftimecol = scalar.ndarray[0]
        useScalarTimes = True
        if len(fcol) > 0:
            keylst.append('f')
            datastream.header['unit-col-f'] = scalar.header['unit-col-f']
            datastream.header['col-f'] = scalar.header['col-f']

    ## Update DataComponents/Elements records regarding S (independent) or F (vector)
    comps = datastream.header.get('DataComponents')
    if len(comps) == 4 and ('f' in keylst or 'df' in keylst):
        comps = comps[:3] + fcolname
        globalAttrs['ElementsRecorded'] = { 0 : comps}

    if debug:
        print ("writeIMAGCDF: Components before writing global attributes:", comps)

    ## writing Global header data
    mycdf.write_globalattrs(globalAttrs)

    # environment data
    t1test = DataStream()
    t2test = DataStream()
    useTemperature1Times = False
    useTemperature2Times = False
    if 't1' in keylst:
        # Check sampling rates of t1/t2 stream
        t1test = datastream.copy()
        t1test = t1test._drop_nans('t1')
        t1samprate = t1test.samplingrate()
        t1test = t1test.get_gaps()
        t1timecol = t1test.ndarray[0]
        t1col = datastream._get_column('t1')
        if t1samprate - 0.1 < mainsamprate and mainsamprate < t1samprate + 0.1:
            # Samplingrate of t1/t2 column and Vector are similar
            useTemperature1Times = False
        else:
            useTemperature1Times = True
            t1col = t1test._get_column('t1')
    if 't2' in keylst:
        t2test = datastream.copy()
        t2test = t2test._drop_nans('t1')
        t2samprate = t2test.samplingrate()
        t2test = t2test.get_gaps()
        t2timecol = t2test.ndarray[0]
        t2col = datastream._get_column('t2')
        if t2samprate-0.1 < mainsamprate and  mainsamprate < t2samprate+0.1:
            #Samplingrate of t1/t2 column and Vector are similar
            useTemperature2Times=False
        else:
            useTemperature2Times=True
            t2col = t2test._get_column('t2')
    if temperature1:
        t1key = KEYLIST.index('t1')
        t1col = temperature1.ndarray[t1key]
        if len(t1col) > 0:
            keylst.append('t1')
            datastream.header['unit-col-t1'] = temperature1.header['unit-col-t1']
            datastream.header['col-t1'] = temperature1.header['col-t1']
        t1timecol = temperature1.ndarray[0]
        useTemperature1Times = True
    if temperature2:
        t2key = KEYLIST.index('t2')
        t2col = temperature2.ndarray[t2key]
        if len(t2col) > 0:
            keylst.append('t2')
            datastream.header['unit-col-t2'] = temperature2.header['unit-col-t2']
            datastream.header['col-t2'] = temperature2.header['col-t2']
        t2timecol = temperature2.ndarray[0]
        useTemperature2Times = True

    # Redefine time column name in case of different components use the same time column
    maintimes = 'GeomagneticVectorTimes'
    if len(fcol) > 0 and not useScalarTimes:
        maintimes = 'GeomagneticTimes'
    if len(t1col) > 0 and not useTemperature1Times:
        maintimes = 'DataTimes'
    if len(t2col) > 0 and not useTemperature2Times:
        maintimes = 'DataTimes'

    ## get sampling rate of vec, get sampling rate of scalar, if different extract scalar and time use separate, else ..
    fwritten = False
    cdfkey = ''
    cdfdata = None
    for key in keylst:
        # New : assign data to the following variables: var_attrs (meta), var_data (dataarray), var_spec (key??)
        var_attrs = {}
        var_spec = {}
        col = np.asarray([])

        if key in ['time','sectime','x','y','z','f','dx','dy','dz','df','t1','t2','scalartime','temp1time','temp2time']:
            try:
                if not key in ['scalartime','temp1time','temp2time']:
                    ind = KEYLIST.index(key)
                    if len(datastream.ndarray[ind])>0:
                        col = datastream.ndarray[ind]
                    if not 'time' in key:
                        col = col.astype(float)
                    if key in ['f'] and scalar and len(scalar) > 0:
                        col = scalar._get_column(key)
                    if key in ['t1'] and temperature1 and len(temperature1) > 0:
                        col = temperature1._get_column(key)
                    if key in ['t2'] and temperature2 and len(temperature2) > 0:
                        col = temperature2._get_column(key)

                    # eventually use a different fill value (default is nan)
                    if not np.isnan(fillval):
                        col = np.nan_to_num(col, nan=fillval)

                    if not False in checkEqual3(col):
                        logger.warning("Found identical values only for {}".format(key))
                        col = col[:1]

                #{'FIELDNAM': 'Geomagnetic Field Element X', 'VALIDMIN': array([-79999.]), 'VALIDMAX': array([ 79999.]), 'UNITS': 'nT', 'FILLVAL': array([ 99999.]), 'DEPEND_0': 'GeomagneticVectorTimes', 'DISPLAY_TYPE': 'time_series', 'LABLAXIS': 'X'}
                if key == 'time':
                    cdfkey = maintimes
                    cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(elem) for elem in col] )
                    var_spec['Data_Type'] = 33
                elif key == 'scalartime' and useScalarTimes:
                    cdfkey = 'GeomagneticScalarTimes'
                    # use ftimecol Datastream
                    cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(elem) for elem in ftimecol] )
                    var_spec['Data_Type'] = 33
                elif key == 'temp1time' and useTemperature1Times:
                    cdfkey = 'Temperature1Times'
                    cdfdata = cdflib.cdfepoch.compute_tt2000([tt(elem) for elem in t1timecol])
                    var_spec['Data_Type'] = 33
                elif key == 'temp2time' and useTemperature2Times:
                    cdfkey = 'Temperature2Times'
                    cdfdata = cdflib.cdfepoch.compute_tt2000([tt(elem) for elem in t2timecol])
                    var_spec['Data_Type'] = 33
                elif len(col) > 0:
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
                                compsupper = fcolname ## MagPy requires independent F value
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
                    #nonetest = col
                    if len(nonetest) > 0:
                        var_attrs['DEPEND_0'] = maintimes
                        var_attrs['DISPLAY_TYPE'] = "time_series"
                        var_attrs['LABLAXIS'] = keyup
                        var_attrs['FILLVAL'] = fillval
                        if key in ['x','y','z','h','e','g']:
                            cdfdata = col
                            var_attrs['VALIDMIN'] = -88880.
                            var_attrs['VALIDMAX'] = 88880.
                        elif key == 'i':
                            cdfdata = col
                            var_attrs['VALIDMIN'] = -90.
                            var_attrs['VALIDMAX'] = 90.
                        elif key == 'd':
                            cdfdata = col
                            var_attrs['VALIDMIN'] = -360.
                            var_attrs['VALIDMAX'] = 360.
                        elif key in ['t1']:
                            var_attrs['VALIDMIN'] = -273.
                            var_attrs['VALIDMAX'] = 88880.
                            if useTemperature1Times:
                                if not 'temp1time' in keylst:
                                    keylst.append('temp1time')
                                var_attrs['DEPEND_0'] = "Temperature1Times"
                                cdfdata = t1col
                            else:
                                cdfdata = col
                        elif key in ['t2']:
                            var_attrs['VALIDMIN'] = -273.
                            var_attrs['VALIDMAX'] = 88880.
                            if useTemperature2Times:
                                if not 'temp2time' in keylst:
                                    keylst.append('temp2time')
                                var_attrs['DEPEND_0'] = "Temperature2Times"
                                # eventually use a different fill value (default is nan)
                                if not np.isnan(fillval):
                                    t2col = np.nan_to_num(t2col, nan=fillval)
                                cdfdata = t2col
                            else:
                                cdfdata = col
                        elif key in ['f','s','df'] and not fwritten:
                            if useScalarTimes:
                                # write time column
                                keylst.append('scalartime')
                                var_attrs['DEPEND_0'] = "GeomagneticScalarTimes"
                                if not np.isnan(fillval):
                                    fcol = np.nan_to_num(fcol, nan=fillval)
                                cdfdata = fcol
                            else:
                                cdfdata = col
                            fwritten = True
                            var_attrs['VALIDMIN'] = 0.
                            var_attrs['VALIDMAX'] = 88880.

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
                                    if 'unit-col-'+key == 'deg C' or key in ['t1','t2']:
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
            except:
                pass

    success = filename

    # Flags are automatically included in MagPy2.0 as soon as the are contained in the header
    # TODO - remove
    """
    if flags and addflags == True:
        flagstart = 'FlagBeginTimes'
        flagend = 'FlagEndTimes'
        flagcomponents = 'FlagComponents'
        flagcode = 'FlagCode'
        flagcomment = 'FlagDescription'
        flagmodification = 'FlagModificationTimes'
        flagsystemreference = 'FlagSystemReference'
        flagobserver = 'FlagObserver'

        flaglist = np.asarray(flags._list(parameter=['starttime','endtime','components','flagtype','comment','sensorid','operator','modificationtime']), dtype=object)
        trfl = np.transpose(flaglist)[1:]
        try:
            print ("Writing flagging information ...")
            var_attrs = {}
            var_spec = {}
            var_spec['Data_Type'] = 33
            var_spec['Num_Elements'] = 1
            var_spec['Rec_Vary'] = True # The dimensional sizes, applicable only to rVariables.
            var_spec['Dim_Sizes'] = []
            var_spec['Variable'] = flagstart
            cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(el) for el in trfl[0]] )
            mycdf.write_var(var_spec, var_attrs=var_attrs, var_data=cdfdata)
            var_spec['Variable'] = flagend
            cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(el) for el in trfl[1]] )
            mycdf.write_var(var_spec, var_attrs=var_attrs, var_data=cdfdata)
            var_spec['Variable'] = flagmodification
            cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(el) for el in trfl[-1]] )
            mycdf.write_var(var_spec, var_attrs=var_attrs, var_data=cdfdata)

            # Here we can select between different content
            fllist=[]
            ll=[]
            if len(flaglist[0]) == 8:
                #[st,et,key,flagnumber,commentarray[idx],sensorid,now]
                # eventually change flagcomponent in the future
                fllist = [flagcomponents,flagcode,flagcomment, flagsystemreference] # , flagobserver]
            elif len(flaglist[0]) == 9:
                # Future version ??
                fllist = [flagcomponents,flagcode,flagcomment, flagsystemreference, flagobserver]
            for idx, cdfkey in enumerate(fllist):
                var_attrs = {}
                var_spec = {}
                if cdfkey == flagcomponents:
                    ll = [",".join(el) for el in trfl[idx+2]]
                elif not cdfkey in [flagcode,flagcomponents]:
                    ll = [str(el) if el else "-"  for el in trfl[idx+2]]
                elif cdfkey in [flagcode]:
                    ll = [int(el) for el in trfl[idx + 2]]
                else:
                    ll = trfl[idx+2]
                #mycdf[cdfkey] = ll
                cdfdata = ll
                var_attrs['DEPEND_0'] = "FlagBeginTimes"
                var_attrs['DISPLAY_TYPE'] = "time_series"
                var_attrs['LABLAXIS'] = cdfkey.strip('Flag')
                #var_attrs['FILLVAL'] = np.nan
                var_attrs['FIELDNAM'] = cdfkey
                if cdfkey in ['flagcode']:
                    var_attrs['VALIDMIN'] = 0
                    var_attrs['VALIDMAX'] = 9
                if cdfkey in [flagcomponents,flagcomment, flagsystemreference, flagobserver]:
                    var_spec['Data_Type'] = 51
                    var_spec['Num_Elements'] = max([len(i) for i in ll])
                elif cdfkey in [flagcode]:
                    var_spec['Data_Type'] = 45
                    var_spec['Num_Elements'] = 1
                var_spec['Variable'] = cdfkey
                var_spec['Rec_Vary'] = True # The dimensional sizes, applicable only to rVariables.
                var_spec['Dim_Sizes'] = []
                mycdf.write_var(var_spec, var_attrs=var_attrs, var_data=cdfdata)

            logger.info("writeIMAGCDF: Flagging information added to file")
            print ("... success")
        except:
            print ("writeIMAGCDF: error when adding flags. skipping this part")
    """
    mycdf.close()
    return success

if __name__ == '__main__':

    import scipy
    import subprocess
    print()
    print("----------------------------------------------------------")
    print("TESTING: IMAGCDF FORMAT LIBRARY")
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
        testset = 'IMAGCDF'
        try:
            filename = os.path.join('/tmp','{}_{}_{}'.format(testrun, testset, datetime.strftime(t_start_test,'%Y%m%d-%H%M')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            succ1 = writeIMAGCDF(teststream, filename)
            succ2 = isIMAGCDF(filename)
            dat = readIMAGCDF(filename)
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            diff = subtract_streams(teststream, dat, debug=True)
            xm = diff.mean('x')
            ym = diff.mean('y')
            zm = diff.mean('z')
            fm = diff.mean('f')
            if np.abs(xm) > 0.00001 or np.abs(ym) > 0.00001 or np.abs(zm) > 0.00001 or np.abs(fm) > 0.00001:
                 raise Exception("ERROR within data validity test")
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))
        testset = 'IMAGCDFflags'
        try:
            filename = os.path.join('/tmp','{}_{}_{}'.format(testrun, testset, datetime.strftime(t_start_test,'%Y%m%d-%H%M')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            fl = flagging.Flags()
            fl = fl.add(sensorid="Test_0001_0002", starttime="2022-11-01T09:00:00",
                        endtime="2022-11-01T10:00:00", components=['x', 'y', 'z'], labelid='099', debug=False)
            fl = fl.add(sensorid="Test_0001_0002", starttime="2022-11-22T21:56:12.654362",
                        endtime="2022-11-22T21:59:12.654362", components=['x', 'y', 'z'], labelid='099', debug=False)
            teststream.header['DataFlags'] = fl
            succ1 = writeIMAGCDF(teststream, filename, addflags=True)
            succ2 = isIMAGCDF(filename)
            dat = readIMAGCDF(filename)
            flafter = dat.header.get('DataFlags')
            print ("Flags before", fl)
            print ("Flags after", flafter)
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            diff = subtract_streams(teststream, dat, debug=True)
            xm = diff.mean('x')
            ym = diff.mean('y')
            zm = diff.mean('z')
            fm = diff.mean('f')
            if np.abs(xm) > 0.00001 or np.abs(ym) > 0.00001 or np.abs(zm) > 0.00001 or np.abs(fm) > 0.00001:
                 raise Exception("ERROR within data validity test")
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))
        testset = 'IMAGCDF_RW'
        #try:
        ok = True
        if ok:
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            # Testing general IMAGCDF
            print ("A")
            test1 = read(example4, debug=True)
            print (test1)
            test1.write("/tmp", format_type='IMAGCDF', scalar=None, temperature1=None, temperature2=None)  # , debug=True)
            option = None
            print ("B")
            test1rep = read('/tmp/wic_2024*', select=option)  # , debug=True)
            di = dictdiff(test1.header, test1rep.header)
            if not di.get('added') == {} and not di.get('removed') == {}:
                # raise Exception("ERROR within data validity test")
                print("ERROR within data validity test", di)
            # Testing G columns
            test2 = test1.copy()
            test2 = test2.delta_f()
            test2 = test2._drop_column('f')
            test2.write("/tmp", format_type='IMAGCDF', scalar=None, temperature1=None, temperature2=None, mode='overwrite')  # , debug=True)
            print ("C")
            test2rep = read('/tmp/wic_2024*', select=option)  # , debug=True)
            if not test2rep.header.get('DataComponents') in ['XYZG','HEZG']:
                # raise Exception("ERROR within data validity test")
                print("ERROR within data validity test", test2rep.header.get('DataComponents'))
            # Testing multiple time columns and lists in header
            test3 = test1.filter()
            test3 = test3._drop_column('x')
            test3 = test3._drop_column('y')
            test3 = test3._drop_column('z')
            test1 = test1._drop_column('f')
            test1 = test1._drop_column('t1')
            test1 = test1._drop_column('t2')
            test1.header['StationInstitution'] = ['Institute1', 'Institute2']
            test1.write("/tmp", format_type='IMAGCDF', scalar=test3, temperature1=test3, temperature2=test3)  # , debug=True)
            print ("D")
            test3rep = read('/tmp/wic_2024*', select=option)  # , debug=True)
            fc = test3rep.header.get('FileContents')
            if fc:
                for el in fc:
                    if el[1].find('Scalar') >= 0:
                        print("Reading Scalar")
                        scalar = read('/tmp/wic_2024*', select='scalar')
                    if el[1].find('Temperature') >= 0:
                        print("Reading Temperature")
                        temperature = read('/tmp/wic_2024*', select='temperature')
            if not len(test3rep.header.get('FileContents')) == 4:
                #print (test3rep.header.get('FileContents'))
                # raise Exception("ERROR within data validity test")
                print("ERROR within data validity test")
            di = dictdiff(test1.header, test3rep.header)
            #di = dictdiff(test1.header, test3rep.header)
            dv = di.get('value_diffs')
            tli = [el for el in dv]
            if not len(tli) <= 2:
                # only format versions might be changed
                # raise Exception("ERROR within data validity test")
                print("ERROR within data validity test", di)
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        #except Exception as excep:
        #    errors[testset] = str(excep)
        #    print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))

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
