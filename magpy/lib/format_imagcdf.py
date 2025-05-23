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

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from magpy.stream import *

import cdflib

#import ciso8601 ## fast datetime parser  ciso8601.parse_datetime should be 10 times faster than datetime.strftime

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
    arraylist = []
    array = [[] for elem in KEYLIST]
    multipletimedict = {}
    newdatalist = []
    tllist = []
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
            headers['DataPublicationDate'] = DataStream()._testtime(pubdate[0])
        except:
            headers['DataPublicationDate'] = pubdate[0]
        #pubdate = cdflib.cdfepoch.unixtime(headers.get('DataPublicationDate'))
        #headers['DataPublicationDate'] = datetime.utcfromtimestamp(pubdate[0])
    except:
        if debug:
            print ("imagcdf warning: Publication date is not provided as tt_2000")
        try:
            pubdate = DataStream()._testtime(headers.get('DataPublicationDate'))
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
                datnumar.append(date2num(np.asarray([datetime.utcfromtimestamp(el) for el in cdflib.cdfepoch.unixtime(cdfdat.varget(na))])))
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


    def Ruleset2Flaglist(flagginglist,rulesettype,rulesetversion):
        if rulesettype in ['Conrad', 'conrad', 'MagPy','magpy'] and len(flagginglist) > 0:
            if rulesetversion in ['1.0','1',1]:
                flagcolsconrad = [flagginglist[0],flagginglist[1],flagginglist[3],flagginglist[4],flagginglist[5],flagginglist[6],flagginglist[2]]
                flaglisttmp = []
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
                return list(flaglist)
            else:
                return []
        else:
            print ("readIMAGCDF: Could  not interprete flagging ruleset or flagginglist is empty")
            logger.warning("readIMAGCDF: Could  not interprete Ruleset")
            return []


    if not headers.get('FlagRulesetType','') == '':
        if debug:
            print ("readIMAGCDF: Found flagging ruleset {} vers.{} - extracting flagging information".format(headers.get('FlagRulesetType',''),headers.get('FlagRulesetVersion','')))
        logger.info("readIMAGCDF: Found flagging ruleset {} vers.{} - extracting flagging information".format(headers.get('FlagRulesetType',''),headers.get('FlagRulesetVersion','')))
        flagginglist = [elem for elem in datalist if elem.startswith('Flag')]
        flaglist = Ruleset2Flaglist(flagginglist,headers.get('FlagRulesetType',''),headers.get('FlagRulesetVersion',''))
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
                ar = date2num(cdflib.cdfepoch.to_datetime(cdflib.cdfepoch,col))
                cdfvers = 18
            except TypeError:
                # cdflib version (>=0.3.19)
                ar = date2num(cdflib.cdfepoch.to_datetime(col))
                cdfvers = 19
            except:
                # if second value is 60 (tt_2000 leapsecond timestamp) cdfepoch.unixtime fails
                print ("File contains a leap second - will be ignored")
                seccol = np.asarray([row[5] for row in cdflib.cdfepoch.breakdown(col)])
                # assume that seccol contains a 60 seconds step - identify and remove
                index = seccol.argmax()
                col = np.delete(col,index)
                try:
                    ar = date2num(cdflib.cdfepoch.to_datetime(cdflib.cdfepoch,col))
                except TypeError:
                    ar = date2num(cdflib.cdfepoch.to_datetime(col))
                delrow = True
            arlen= len(ar)
            arraylist.append(ar)
            ind = KEYLIST.index('time')
            array[ind] = ar
        else:
            ar = cdfdat.varget(elem[1])
            if delrow:
                ar = np.delete(ar,index)
            if elem[0] in NUMKEYLIST:
                fillval = cdfdat.varattsget(elem[1]).get('FILLVAL')
                if isnan(fillval):
                    # if it is nan than the following replace wont work anyway
                    fillval = 99999.0
                with np.errstate(invalid='ignore'):
                    ar[ar > fillval-1] = float(nan)
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
                    arraylist.append(newar)
                elif not multipletimedict == {} and referencetimecol:
                    if timedepend == referencetimecol:
                        array[ind] = ar
                        arraylist.append(ar)
                else:
                    array[ind] = ar
                    arraylist.append(ar)
                if elem[0] in ['f','F'] and headers.get('DataComponents','') in ['DIF','dif','idf','IDF'] and not len(array[zpos]) > 0:
                    array[zpos] = ar
                    arraylist.append(ar)
                    headers['col-z'] = cdfdat.varattsget(elem[1]).get('LABLAXIS').lower()
                    headers['unit-col-z'] = cdfdat.varattsget(elem[1]).get('UNITS')

    ndarray = np.array(array, dtype=object)

    stream = DataStream()
    stream = [LineStruct()]

    result = DataStream(stream,headers,ndarray)

    if not headers.get('FlagRulesetType','') == '' and len(flaglist) > 0:
        result = result.flag(flaglist)

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
            datastream = joinStreams(exst,datastream)
            os.remove(filename)
            try:
                mycdf = cdflib.cdfwrite.CDF(filename,cdf_spec=main_cdf_spec)
            except:
                mycdf = cdflib.CDF(filename, cdf_spec=main_cdf_spec)
        elif mode == 'replace' or mode == 'append': # replace existing inputs
            exst = read(filename)
            datastream = joinStreams(datastream, exst)
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
        flaglist = datastream.extractflags()
        if len(flaglist) > 0:
            globalAttrs['FlagRulesetVersion'] = { 0 : '1.0'}
            globalAttrs['FlagRulesetType'] = { 0 : 'Conrad'}

    if not headers.get('DataPublicationDate','') == '':
        dat = tt(datastream._testtime(headers.get('DataPublicationDate','')))
        pubdate = cdflib.cdfepoch.compute_tt2000([dat])
    else:
        pubdate = cdflib.cdfepoch.compute_tt2000([tt(datetime.utcnow())])
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
                    longi,lati = convertGeoCoordinate(float(longi),float(lati),'epsg:'+str(epsg),'epsg:4326')
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
    if len(datastream.ndarray[0]>0):
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
                if ndarray and len(datastream.ndarray[ind])>0:
                    col = datastream.ndarray[ind]
                else:
                    col = datastream._get_column(key)
                col = col.astype(float)

                # eventually use a different fill value (default is nan)
                if not isnan(fillval):
                    col = np.nan_to_num(col, nan=fillval)

                if not False in checkEqual3(col):
                    logger.warning("Found identical values only for {}".format(key))
                    col = col[:1]

            #{'FIELDNAM': 'Geomagnetic Field Element X', 'VALIDMIN': array([-79999.]), 'VALIDMAX': array([ 79999.]), 'UNITS': 'nT', 'FILLVAL': array([ 99999.]), 'DEPEND_0': 'GeomagneticVectorTimes', 'DISPLAY_TYPE': 'time_series', 'LABLAXIS': 'X'}
            if key == 'time':
                cdfkey = 'GeomagneticVectorTimes'
                cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(num2date(elem).replace(tzinfo=None)) for elem in col] )
                var_spec['Data_Type'] = 33
            elif key == 'scalartime' and useScalarTimes:
                cdfkey = 'GeomagneticScalarTimes'
                ftimecol = ftest.ndarray[0]
                # use ftest Datastream
                cdfdata = cdflib.cdfepoch.compute_tt2000( [tt(num2date(elem).replace(tzinfo=None)) for elem in ftimecol] )
                var_spec['Data_Type'] = 33
            elif key == 'temptime' and useTemperatureTimes:
                cdfkey = 'TemperatureTimes'
                ttimecol = ttest.ndarray[0]
                # use ttest Datastream
                cdfdata = cdflib.cdfepoch.compute_tt2000([tt(num2date(elem).replace(tzinfo=None)) for elem in ttimecol])
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
            if len(flaglist[0]) == 7:
                #[st,et,key,flagnumber,commentarray[idx],sensorid,now]
                # eventually change flagcomponent in the future
                fllist = [flagcomponents,flagcode,flagcomment, flagsystemreference] # , flagobserver]
            elif len(flaglist[0]) == 8:
                # Future version ??
                fllist = [flagcomponents,flagcode,flagcomment, flagsystemreference, flagobserver]
            #print (fllist)
            for idx, cdfkey in enumerate(fllist):
                var_attrs = {}
                var_spec = {}
                if not cdfkey == flagcode:
                    ll = [str(el) for el in trfl[idx+2]]
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
