"""
MagPy
Intermagnet ImagCDF input filter
(based on cdflib)
Written by Roman Leonhardt October 2019
- contains test, read and write functions for
        ImagCDF
- supports python >= 3.5
- currently requires cdflib<=0.3.18
"""
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
from magpy.stream import DataStream, read, subtract_streams, join_streams, magpyversion
from magpy.core.methods import testtime, convert_geo_coordinate
from magpy.core import flagging
from datetime import datetime, timedelta, timezone
import os
import numpy as np
import cdflib
import logging
logger = logging.getLogger(__name__)

KEYLIST = DataStream().KEYLIST
NUMKEYLIST = DataStream().NUMKEYLIST
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
    referencetimecol = None
    indexarray = np.asarray([])
    cdfversion=0.9

    cdfdat = cdflib.CDF(filename)

    if debug:
        print ("Reading ImagCDF with cdflib")

    if select:
        print ("Only data associated with {} time column will be extracted".format(select))

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
        #pubdate = cdflib.cdfepoch.unixtime(headers.get('DataPublicationDate'))
        #headers['DataPublicationDate'] = datetime.utcfromtimestamp(pubdate[0])
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
    if debug:
        print (" - cdfversion:",cdfversion)
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
        print ("LEAP seconds updated:", lsu)

    #  IAGA code
    if headers.get('SensorID','') == '':
        try:
            #TODO determine resolution
            headers['SensorID'] = "{}_{}_{}".format(headers.get('StationIAGAcode','xxx').upper()+'sec',headers.get('DataPublicationLevel','0'),'0001')
        except:
            pass

    if debug:
        print (" - readIMAGCDF: header done")
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
        tllist = sorted(tllist)
        tl = [el[0] for el in tllist]
        namelst = [el[1] for el in tllist]
        if not max(tl) == min(tl):
            if debug:
                print (" Found multiple time columns in file with different lengths")
            timecol = None
            pos = -1
            if select:
                for idx,na in enumerate(namelst):
                    nam = na.lower()
                    if nam.find(select.lower()) > -1:
                        timecol = na
                        referencetimecol = na
                        pos = idx
            else:
                logger.warning("readIMAGCDF: Time columns of different length. Choosing longest as basis")
                timecol = tllist[tl.index(max(tl))][1]
            if not timecol:
                timecol = tllist[tl.index(max(tl))][1]
            newdatalist.append(['time',timecol])
            if debug:
                print (" selected primary time column: {}".format(timecol))
            datnumar = []
            for na in namelst:
                datnumar.append(np.asarray([datetime.utcfromtimestamp(el) for el in cdflib.cdfepoch.unixtime(cdfdat.varget(na))]))
            multipletimedict = {}
            for idx,na in enumerate(datnumar):
                refnumar = datnumar[pos]
                if pos == -1:
                    pos = len(datnumar)-1
                if not idx == pos:
                    try:
                        multipletimedict[namelst[idx]]=np.nonzero(np.in1d(refnumar,datnumar[idx]))[0]
                    except:
                        multipletimedict[namelst[idx]]=np.asarray([])
            #if debug:
            #    print (" Lengths of different time columns:", multipletimedict)
        else:
            logger.info("readIMAGCDF: Equal length time axes found - assuming identical time")
            if 'GeomagneticVectorTimes' in datalist:
                newdatalist.append(['time','GeomagneticVectorTimes'])
            else:
                newdatalist.append(['time',tllist[0][1]]) # Take the first one
    else:
        #"Single time axis found in file"
        newdatalist.append(['time',tllist[0][1]])

    def Ruleset2Flaglist(flagginglist,rulesettype,rulesetversion,stationid=None, debug=False):
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
                try:
                    flaglisttmp[0] = cdflib.cdfepoch.to_datetime(cdflib.cdfepoch,flaglisttmp[0])
                except:
                    flaglisttmp[0] = cdflib.cdfepoch.to_datetime(flaglisttmp[0])
                try:
                    flaglisttmp[1] = cdflib.cdfepoch.to_datetime(cdflib.cdfepoch,flaglisttmp[1])
                except:
                    flaglisttmp[1] = cdflib.cdfepoch.to_datetime(flaglisttmp[1])
                try:
                    flaglisttmp[-1] = cdflib.cdfepoch.to_datetime(cdflib.cdfepoch,flaglisttmp[-1])
                except:
                    flaglisttmp[-1] = cdflib.cdfepoch.to_datetime(flaglisttmp[-1])
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
            print ("readIMAGCDF: Could  not interpret flagging ruleset or flagging object is empty")
            logger.warning("readIMAGCDF: Could  not interpret Ruleset")
            return {}


    if not headers.get('FlagRulesetType','') == '':
        if debug:
            print ("readIMAGCDF: Found flagging ruleset {} vers.{} - extracting flagging information".format(headers.get('FlagRulesetType',''),headers.get('FlagRulesetVersion','')))
        logger.info("readIMAGCDF: Found flagging ruleset {} vers.{} - extracting flagging information".format(headers.get('FlagRulesetType',''),headers.get('FlagRulesetVersion','')))
        flagginglist = [elem for elem in datalist if elem.startswith('Flag')]
        flags = Ruleset2Flaglist(flagginglist,headers.get('FlagRulesetType',''),headers.get('FlagRulesetVersion',''), stationid=headers.get("StationID",None), debug=debug)
        if debug:
            print ("readIMAGCDF: Flagging information extracted")

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
        for elem in datalist:
            try:
                label = cdfdat.varattsget(elem).get('LABLAXIS').lower()
                if label in possvals:
                    newdatalist.append([key,elem])
            except:
                pass # for lines which have no Label

    if debug:
        print ("Components in file: {}".format(newdatalist))

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
            #array[ind] = ar.astype(datetime)  # datetime.datetime
            array[ind] = ar  # np.datetime64 might be quicker
        else:
            ar = cdfdat.varget(elem[1])
            if delrow:
                ar = np.delete(ar,index)
            if elem[0] in NUMKEYLIST:
                fillval = cdfdat.varattsget(elem[1]).get('FILLVAL')
                if np.isnan(fillval):
                    # if it is nan than the following replace wont work anyway
                    fillval = 99999.0
                with np.errstate(invalid='ignore'):
                    ar[ar > fillval-1] = float(np.nan)
                ind = KEYLIST.index(elem[0])
                headers['col-'+elem[0]] = cdfdat.varattsget(elem[1]).get('LABLAXIS').lower()
                headers['unit-col-'+elem[0]] = cdfdat.varattsget(elem[1]).get('UNITS')
                timecolumns = list(multipletimedict.keys())
                timedepend = cdfdat.varattsget(elem[1]).get('DEPEND_0')
                if not multipletimedict == {} and timedepend in timecolumns and not referencetimecol:
                    indexarray = multipletimedict.get(timedepend)
                    if debug:
                        print("Timesteps of {}: {}, N(Values): {}".format(timedepend,len(indexarray),len(ar)))
                    newar = np.asarray([np.nan]*arlen)
                    newar[indexarray] = ar
                    array[ind] = newar
                elif not multipletimedict == {} and referencetimecol:
                    if timedepend == referencetimecol:
                        array[ind] = ar
                else:
                    array[ind] = ar
                if elem[0] in ['f','F'] and headers.get('DataComponents','') in ['DIF','dif','idf','IDF'] and not len(array[zpos]) > 0:
                    array[zpos] = ar
                    headers['col-z'] = cdfdat.varattsget(elem[1]).get('LABLAXIS').lower()
                    headers['unit-col-z'] = cdfdat.varattsget(elem[1]).get('UNITS')

    ndarray = np.asarray(array, dtype=object)
    result = DataStream(header=headers,ndarray=ndarray)

    if not headers.get('FlagRulesetType','') == '' and flags:
        result.header["DataFlags"] = flags
        #result = result.flag(flaglist)

    return result


def writeIMAGCDF(datastream, filename, **kwargs):
    """
    Writing Intermagnet CDF format (currently: vers1.2) + optional flagging info

    """
    debug = kwargs.get('debug')
    fillval = kwargs.get('fillvalue')
    mode = kwargs.get('mode')
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
        flags = datastream.header.get("DataFlags", {})
        if flags:
            globalAttrs['FlagRulesetVersion'] = { 0 : '2.0'}
            globalAttrs['FlagRulesetType'] = { 0 : 'Conrad'}

    if not headers.get('DataPublicationDate','') == '':
        dat = tt(testtime(headers.get('DataPublicationDate','')))
        pubdate = cdflib.cdfepoch.compute_tt2000([dat])
    else:
        pubdate = cdflib.cdfepoch.compute_tt2000([tt(datetime.now(timezone.utc).replace(tzinfo=None))])
    if isinstance(pubdate,np.ndarray):
        pubdate = pubdate.item()
    globalAttrs['PublicationDate'] = { 0 : pubdate }

    # check for leapseconds
    try:
        leapex = globalAttrs.get("LeapSecondUpdated").get(0)
        if leapsecondlastupdate and leapex in ["","None"]:
            lslu = datetime.strftime(leapsecondlastupdate,"%Y%m%d")
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
                    longi,lati = convert_geo_coordinate(float(longi),float(lati),'epsg:'+str(epsg),'epsg:4326')
                    longi = "{:.3f}".format(float(longi))
                    lati = "{:.3f}".format(float(lati))
            globalAttrs['Latitude'] = { 0 : float(lati) }
            globalAttrs['Longitude'] = { 0 : float(longi) }

    if not 'StationIagaCode' in headers and 'StationID' in headers:
        globalAttrs['IagaCode'] = { 0 : headers.get('StationID','')}


    # writing of global attributes after checking for independency of eventually provided F (S) record - line 595
    #mycdf.write_globalattrs(globalAttrs)

    ### #########################################
    ###               Data
    ### #########################################

    def checkEqualIvo(lst):
        # http://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
        return not lst or lst.count(lst[0]) == len(lst)

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
    if 'f' in keylst or 'df' in keylst:
        if 'f' in keylst:
            if not 'df' in keylst:
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
                         print ("writeIMAGCDF: analyzed F column - values are apparently independend from vector components - using column name 'S'")
            pos = KEYLIST.index('f')
            col = datastream.ndarray[pos]
        if 'df' in keylst:
            scal = 'df'
            #print ("writeIMAGCDF: Found dF column")
            pos = KEYLIST.index('df')
            col = datastream.ndarray[pos]
        col = col.astype(float)

        # Check sampling rates of main stream and f/df stream
        mainsamprate = datastream.samplingrate()
        ftest = datastream.copy()
        ftest = ftest._drop_nans(scal)
        fsamprate = ftest.samplingrate()
        if ftest.length()[0] > 0:
            ftest = ftest.get_gaps()

        if fsamprate-0.1 < mainsamprate and mainsamprate < fsamprate+0.1:
            #Samplingrate of F column and Vector are similar
            useScalarTimes=False
        else:
            useScalarTimes=True

    ## Update DataComponents/Elements records regarding S (independent) or F (vector)
    comps = datastream.header.get('DataComponents')
    if len(comps) == 4 and 'f' in keylst:
        comps = comps[:3] + fcolname
        globalAttrs['ElementsRecorded'] = { 0 : comps}
    ## writing Global header data
    #print (" Writing ", globalAttrs)
    mycdf.write_globalattrs(globalAttrs)

    ttest = DataStream()
    useTemperatureTimes = False
    if 't1' in keylst:
        # Check sampling rates of t1/t2 stream
        ttest = datastream.copy()
        ttest = ttest._drop_nans('t1')
        tsamprate = ttest.samplingrate()
        ttest = ttest.get_gaps()
        #print ("t lenght", ttest.length())

        if tsamprate-0.1 < mainsamprate and  mainsamprate < tsamprate+0.1:
            #Samplingrate of t1/t2 column and Vector are similar
            useTemperatureTimes=False
        else:
            useTemperatureTimes=True

    ## get sampling rate of vec, get sampling rate of scalar, if different extract scalar and time use separate, else ..

    for key in keylst:
        # New : assign data to the following variables: var_attrs (meta), var_data (dataarray), var_spec (key??)
        var_attrs = {}
        var_spec = {}

        if key in ['time','sectime','x','y','z','f','dx','dy','dz','df','t1','t2','scalartime','temptime']:
          try:
            if not key in ['scalartime','temptime']:
                ind = KEYLIST.index(key)
                if len(datastream.ndarray[ind])>0:
                    col = datastream.ndarray[ind]
                if not 'time' in key:
                    col = col.astype(float)

                # eventually use a different fill value (default is nan)
                if not np.isnan(fillval):
                    col = np.nan_to_num(col, nan=fillval)

                if not False in checkEqual3(col):
                    logger.warning("Found identical values only for {}".format(key))
                    col = col[:1]

            #{'FIELDNAM': 'Geomagnetic Field Element X', 'VALIDMIN': array([-79999.]), 'VALIDMAX': array([ 79999.]), 'UNITS': 'nT', 'FILLVAL': array([ 99999.]), 'DEPEND_0': 'GeomagneticVectorTimes', 'DISPLAY_TYPE': 'time_series', 'LABLAXIS': 'X'}
            if key == 'time':
                cdfkey = 'GeomagneticVectorTimes'
                cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(elem) for elem in col] )
                var_spec['Data_Type'] = 33
            elif key == 'scalartime' and useScalarTimes:
                cdfkey = 'GeomagneticScalarTimes'
                ftimecol = ftest.ndarray[0]
                # use ftest Datastream
                cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(elem) for elem in ftimecol] )
                var_spec['Data_Type'] = 33
            elif key == 'temptime' and useTemperatureTimes:
                cdfkey = 'TemperatureTimes'
                ttimecol = ttest.ndarray[0]
                # use ttest Datastream
                cdfdata = cdflib.cdfepoch.compute_tt2000([tt(elem) for elem in ttimecol])
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
                    var_attrs['DEPEND_0'] = "GeomagneticVectorTimes"
                    var_attrs['DISPLAY_TYPE'] = "time_series"
                    var_attrs['LABLAXIS'] = keyup
                    var_attrs['FILLVAL'] = fillval
                    if key in ['x','y','z','h','e','g']:
                        cdfdata = col
                        var_attrs['VALIDMIN'] = -88880.0
                        var_attrs['VALIDMAX'] = 88880.0
                    elif key == 'i':
                        cdfdata = col
                        var_attrs['VALIDMIN'] = -90.0
                        var_attrs['VALIDMAX'] = 90.0
                    elif key == 'd':
                        cdfdata = col
                        var_attrs['VALIDMIN'] = -360.0
                        var_attrs['VALIDMAX'] = 360.0
                    elif key in ['t1','t2']:
                        var_attrs['VALIDMIN'] = -273.15
                        var_attrs['VALIDMAX'] = 88880.0
                        if useTemperatureTimes:
                            keylst.append('temptime')
                            tcol = ttest._get_column(key)
                            var_attrs['DEPEND_0'] = "TemperatureTimes"
                            cdfdata = tcol
                        else:
                            cdfdata = col
                    elif key in ['f','s','df']:
                        if useScalarTimes:
                            # write time column
                            keylst.append('scalartime')
                            fcol = ftest._get_column(key)
                            #if len(naninds) > 0:
                            #    cdfdata = col[~np.isnan(col)]
                            var_attrs['DEPEND_0'] = "GeomagneticScalarTimes"
                            #mycdf[cdfkey] = fcol
                            cdfdata = fcol
                        else:
                            cdfdata = col
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
          except:
            pass

    success = filename

    if flags and addflags == True:
        flagstart = 'FlagBeginTimes'
        flagend = 'FlagEndTimes'
        flagcomponents = 'FlagComponents'
        flagcode = 'FlagCode'
        flagcomment = 'FlagDescription'
        flagmodification = 'FlagModificationTimes'
        flagsystemreference = 'FlagSystemReference'
        flagobserver = 'FlagObserver'

        flaglist = flags._list(parameter=['starttime','endtime','components','flagtype','comment','sensorid','operator','modificationtime'])
        trfl = np.transpose(flaglist)[1:]
        #print ("Transposed flaglist", trfl)
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
