#!/usr/bin/env python

# ----------------------------------------------------------------------------
# Part 1: Import routines for packages
# ----------------------------------------------------------------------------

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

from magpy.stream import *
from magpy.absolutes import *
from magpy.transfer import *

print("Loading python's SQL support")
try:
    # Loading MySQL functionality
    import MySQLdb as mysql
    print("... success")
except ImportError:
    try:
        # Loading alternative MySQL functionality
        import pymysql as mysql
        mysql.install_as_MySQLdb()
        print("... success")
    except:
        print("Failed to import SQL packages 'MySQLdb' or 'pymysql'")
except:
    print("SQL package import failed")
    pass


"""
AVAILABLE METHODS:
---------------------------------
dbgetfloat(db,tablename,sensorid,columnid,revision=None)
dbgetstring(db,tablename,sensorid,columnid,revision=None)
dbupload(db, path,stationid,**kwargs):
dbinit(db):
dbdelete(db,datainfoid,**kwargs):
dbdict2fields(db,header_dict,**kwargs):
dbfields2dict(db,datainfoid):
dbalter(db):
dbupadteDataInfo(db, "MyTable_12345_0001", myheader)
dbupdate(db, table, [key], [value], condition)
dbselect(db, element, table, condition=None, expert=None):
dbcoordinates(db, pier, epsgcode='epsg:4326')
dbsensorinfo(db,sensorid,sensorkeydict=None,sensorrevision = '0001'):
dbdatainfo(db,sensorid,datakeydict=None,tablenum=None,defaultstation='WIC',updatedb=True):
writeDB(db, datastream, tablename=None, StationID=None, mode='replace', revision=None, **kwargs):
dbsetTimesinDataInfo(db, tablename,colstr,unitstr):
stream2db(db, datastream, noheader=None, mode=None, tablename=None, **kwargs):
readDB(db, table, starttime=None, endtime=None, sql=None):
db2stream(db, sensorid=None, begin=None, end=None, tableext=None, sql=None):
diline2db(db, dilinestruct, mode=None, **kwargs):
db2diline(db,**kwargs):
getBaselineProperties(db,datastream,pier=None,distream=None):
flaglist2db(db,flaglist,mode=None,sensorid=None,modificationdate=None):
db2flaglist(db,sensorid, begin=None, end=None):
string2dict(string): 

"""
# ----------------------------------------------------------------------------
# Part 2: Default list definitions - defining fields of database standard tables
# ----------------------------------------------------------------------------

### Modify contents: 
#remove: ('DataDeltaX','DataDeltaY','DataDeltaZ','DataDeltaF','DataDeltaT1', 'DataDeltaT2','DataDeltaVar1','DataDeltaVar2','DataDeltaVar3','DataDeltaVar4', 'DataAbsFunc', 'DataAbsDegree','DataAbsKnots','DataAbsMinTime','DataAbsMaxTime','DataAbsFunctionObject')
#add: ('DataAbsInfo', 'DataBaseValues', 'DataFlagList', 'DataScales')
### DataScales=Text-> Dict; DataBaseValues=Text
###
 


DATAINFOKEYLIST = ['DataID','SensorID','StationID','ColumnContents','ColumnUnits','DataFormat',
                   'DataMinTime','DataMaxTime',
                   'DataSamplingFilter','DataDigitalSampling','DataComponents','DataSamplingRate',
                   'DataType',
                   'DataDeltaReferencePier','DataDeltaReferenceEpoch','DataScaleX',
                   'DataScaleY','DataScaleZ','DataScaleUsed','DataCompensationX',
                   'DataCompensationY','DataCompensationZ','DataSensorOrientation',
                   'DataSensorAzimuth','DataSensorTilt','DataAngularUnit','DataPier',
                 'DataAcquisitionLatitude','DataAcquisitionLongitude','DataLocationReference',
                   'DataElevation','DataElevationRef','DataFlagModification','DataAbsFunc',
                   'DataAbsDegree','DataAbsKnots','DataAbsMinTime','DataAbsMaxTime','DataAbsDate',
                   'DataRating','DataComments','DataSource','DataAbsFunctionObject',
                   'DataDeltaValues', 'DataTerms', 'DataReferences',
                   'DataPublicationLevel', 'DataPublicationDate', 'DataStandardLevel',
                   'DataStandardName', 'DataStandardVersion', 'DataPartialStandDesc','DataRotationAlpha','DataRotationBeta','DataAbsInfo','DataBaseValues','DataArchive']

DATAVALUEKEYLIST = ['CHAR(50)', 'CHAR(50)', 'CHAR(50)', 'TEXT', 'TEXT', 'CHAR(30)',
                    'CHAR(50)','CHAR(50)',
                    'CHAR(100)','CHAR(100)','CHAR(10)','CHAR(100)',
                    'CHAR(100)',
                    'CHAR(20)','CHAR(50)','DECIMAL(20,9)',
                    'DECIMAL(20,9)','DECIMAL(20,9)','CHAR(2)','DECIMAL(20,9)',
                    'DECIMAL(20,9)','DECIMAL(20,9)','CHAR(10)',
                    'DECIMAL(20,9)','DECIMAL(20,9)','CHAR(5)','CHAR(20)',
                    'DECIMAL(20,9)','DECIMAL(20,9)','CHAR(20)',
                    'DECIMAL(20,9)','CHAR(10)','CHAR(50)','CHAR(20)',
                    'INT','DECIMAL(20,9)','CHAR(50)','CHAR(50)','CHAR(50)',
                    'CHAR(10)','TEXT','CHAR(100)','TEXT','CHAR(100)',
                    'TEXT','TEXT','CHAR(50)','CHAR(50)','CHAR(50)','CHAR(100)',
                    'CHAR(50)','TEXT','TEXT','TEXT','TEXT','TEXT','CHAR(50)']


SENSORSKEYLIST = ['SensorID','SensorName','SensorType','SensorSerialNum','SensorGroup','SensorDataLogger',
                  'SensorDataLoggerSerNum','SensorDataLoggerRevision','SensorDataLoggerRevisionComment',
                  'SensorDescription','SensorElements','SensorKeys','SensorModule','SensorDate',
                  'SensorRevision','SensorRevisionComment','SensorRevisionDate','SensorDynamicRange',
                  'SensorTimestepAccuracy', 'SensorGroupDelay', 'SensorPassband', 'SensorAttenuation', 
                  'SensorRMSNoise', 'SensorSpectralNoise', 'SensorAbsoluteError', 'SensorOrthogonality',
                  'SensorVerticality', 'SensorTCoeff', 'SensorElectronicsTCoeff', 'SensorAnalogSampling',
                  'SensorResolution','SensorTime']

STATIONSKEYLIST = ['StationID','StationName','StationIAGAcode','StationInstitution','StationStreet',
                   'StationCity','StationPostalCode','StationCountry','StationWebInfo',
                   'StationEmail','StationDescription','StationK9','StationMeans']

FLAGSKEYLIST = ['FlagID','SensorID','FlagBeginTime','FlagEndTime','FlagComponents','FlagNum','FlagReason','ModificationDate']

BASELINEKEYLIST = ['SensorID','MinTime','MaxTime','TmpMaxTime','BaseFunction','BaseDegree','BaseKnots','BaseComment']

# Optional (if acquisition routine is used)
IPKEYLIST = ['IpName','IP','IpSensors','IpDuty','IpType','IpAccess','IpLocation','IpLocationLat','IpLocationLong','IpSystem','IpMainUser','IpComment']

# Optional (if many piers are available)
PIERLIST = ['PierID','PierName','PierAlternativeName','PierType','PierConstruction','StationID','PierLong','PierLat','PierAltitude','PierCoordinateSystem','PierReference','DeltaDictionary','AzimuthDictionary','DeltaComment']

"""

SENSOR:
        SensorID: a combination of name, serialnumber and revision
        SensorName: a name defined by the observers to refer to the instrument
        SensorType: type of sensor (e.g. fluxgate, overhauzer, temperature ...)
        SensorSerialNum: its serial number
        SensorGroup: Geophysical group (e.g. Magnetism, Gravity, Environment, Meteorology, Seismology, Radiometry, ...)
        SensorDataLogger: type of any electronics connected to the sensor
        SensorDataLoggerSerNum: its serial number
        SensorDataLoggerRevision: 4 digit revision id '0001'
        SensorDataLoggerRevisionComment: description of revision
        SensorDescription: Description of the sensor
        SensorElements: Measured components e.g. x,y,z,t_sensor,t_electonics
        SensorKeys: the keys to be used for the elements in MagPy e.g. 'x,y,z,t1,t2', should have the same number as Elements (check MagPy Manual for this)
        SensorModule: type of sensor connection
        SensorDate: Date of Sensor construction/buy
        SensorRevision: 4 digit number defining a revision ID
        SensorRevisionComment: Comment for current revision - changes to previous number (e.g. calibration)
        SensorRevisionDate: Date of revision - for 0001 this equals the SensorDate
        SensorDynamicRange: 
        SensorTimestepAccuracy:
        SensorGroupDelay:
        SensorPassband:
        SensorAttenuation:
        SensorRMSNoise:
        SensorSpectralNoise:
        SensorAbsoluteError:
        SensorOrthogonality:
        SensorVerticallity:
        SensorTCoeff:
        SensorElectronicsTCoeff:
        SensorAnalogSampling:
        SensorResolution:
STATION:
        StationID: unique ID of the station e.g. IAGA code
        StationIAGAcode:
        StationName: e.g. Cobenzl
        StationStreet: Stations address
        StationCity: Vienna
        StationEmail: 'ramon.egli@zamg.ac.at',
        StationPostalCode: '1190',
        StationCountry: 'Austria',
        StationInstitution: 'Zentralanstalt fuer Meteorologie und Geodynamik',
        StationWebInfo: 'http://www.zamg.ac.at',
        StationDescription: 'Running since 1951.'
        StationK9: k9 limit for location
        StationMeans: Contains a list with mean values e.g. Year:2015,H:20800nT,Z:43000nT
DATAINFO:
        DataID:

FLAGS: (used to store flagging information)

BASELINE: (used to store baseline fit parameters)

IP:
        IpName          name of the machine
        IP              IP address
        IpSensors       comma separated list of sensors eventually attached to the system
        IpDuty          Job of the system behind the ip address: (acquisition, collector, fileserver, backup)
        IpType          Logger type (e.g. eBox 4310 JSK)
        IpAccess        e.g. global, local, only from ip xy
        IpLocation      Location name (GMO-Lab1)
        IpLocationLat   Lat
        IpLocationLong  Long
        IpSystem        operating system (e.g. Ubuntu12.04)
        IpMainUser      add the user
        IpComment       optional comments

PIER:
        PierName                e.g. A2
        PierID                  Reference Number used by BEV
        PierAlernativeName      e.g. Mioara
        PierType                e.g. Aim, Pillar, Groundmark
        PierConstruction        e.g. Glascube, Concretepillar with glasplate, Groundmark
        PierLong                Longitude
        PierLat                 Latitude
        PierAltidude            Altitude surface of Pier or
        PierCoordinateSystem    Location name (GMO-Lab1)
        PierReference           Reference(s) for coordinates and construction
        DeltaDictinoary         Reference List: Looking like A2: 201502_-0.45_0.002_201504_2.15, A16: None_None_None_201505_1.15
                                ( containing pier plus epoch - year or year/month of determination- for dir
                                  as well as delta D, Delta I; and Epoch and delta D - order sensitive -
                                  separated by underlines; non-existing values are marked by 'None')
        AzimuthDictinoary       Reference List: Looking like Z12345_xxx.xx, Z12345_xxx.xx
                                ( containing AzimuthMark plus angle )
        DeltaComment            optional comments on delta values
        PierComment             optional comments on Pier
        StationID               Station at which Pier is located

"""

"""
dbgetfloat

"""

# ----------------------------------------------------------------------------
#  Part 3: Main methods for mysql database communication --
#      dbalter, dbsensorinfo, dbdatainfo, dbdict2fields, dbfields2dict and
# ----------------------------------------------------------------------------

def dbgetPier(db,pierid, rp, value, maxdate=None, l=False, dic='DeltaDictionary'):
    """
    DEFINITION:
        Gets values from DeltaDictionary of the PIERS table
    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - pierid:       (string) The pier you are interested in
        - RP:           (string) ReferencePier
        - value:        (string) one of 'deltaD', 'deltaI' and 'deltaF' - default is 'deltaF'
        - maxdate:      (string) get last value before maxdate
        - l:            (bool) if true return a list of all inputs
        - dic:          (string) dictionary to look at, default is 'DeltaDictionary'
    APPLICATION:
        >>>deltaD =  dbgetPier(db, 'A7','A2','deltaD')

        returns deltaD of A7 relative to A2
    """
    sql = 'SELECT '+ dic +' FROM PIERS WHERE PierID = "' + pierid + '"';
    cursor = db.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()

    if not row:
        print("dbgetPier: No data found for your selection")
        return 0.0

    key = ['pier','epochDir','deltaD','deltaI','epochF','deltaF']
    ind = key.index(value)
    indtdir = key.index('epochDir')
    indtf = key.index('epochF')

    if not row[0] == None:
        try:
            pl1 = row[0].split(',')
            pierlist = [elem.strip().split('_') for elem in pl1 if elem.split('_')[0] == rp]
            if l:
                return pierlist
            else:
                if not value in ['deltaD','deltaI','deltaF']:
                    print("dbgetPier: Select a valid value paramater - check help")
                    return 0.0
                if value in ['deltaD','deltaI']:
                    valuetimes = [t[indtdir] for t in pierlist]
                else:
                    valuetimes = [t[indtf] for t in pierlist]
                if not maxdate:
                    indlv = valuetimes.index(max(valuetimes))
                    return float(pierlist[indlv][ind])
                else:
                    # reformat maxdate to yearmonth
                    valuetimes = [el for el in valuetimes if el <= maxdate]
                    indlv = valuetimes.index(max(valuetimes))
                    return float(pierlist[indlv][ind])
        except:
            print("no deltas found")
            #return row[0]
    else:
        return 0.0

def dbgetlines(db, tablename, lines):
    """
    DEFINITION:
        Get the last x lines from the selected table
    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - tablename:    name of the table
        - lines:        (int) amount of lines to extract
    APPLICATION:
        >>>data = dbgetlines(db, 'DATA_0001_0001', 3600)
        returns a data stream object
    """
    cursor = db.cursor()

    stream = DataStream()
    headsql = 'SHOW COLUMNS FROM %s' % (tablename)
    try:
        cursor.execute(headsql)
    except mysql.IntegrityError as message:
        return message
    except mysql.Error as message:
        return message
    except:
        return 'dbgetlines: unkown error'
    head = cursor.fetchall()
    keys = list(np.transpose(np.asarray(head))[0])

    getsql = 'SELECT * FROM %s ORDER BY time DESC LIMIT %d' % (tablename, lines)
    try:
        cursor.execute(getsql)
    except mysql.IntegrityError as message:
        print(message)
        return stream
    except mysql.Error as message:
        print(message)
        return stream
    except:
        print('dbgetlines: unkown error')
        return stream
    result = cursor.fetchall()
    res = np.transpose(np.asarray(result))

    array =[[] for key in KEYLIST]
    for idx,key in enumerate(KEYLIST):
        if key in keys:
            pos = keys.index(key)
            if key == 'time':
                array[idx] = np.asarray(date2num([stream._testtime(elem) for elem in res[pos]]))
            elif key in NUMKEYLIST:
                array[idx] = res[pos].astype(float)
            else:
                array[idx] = res[pos].astype(object)

    header = dbfields2dict(db,tablename)
    stream = DataStream([LineStruct()],header,np.asarray(array))

    return stream.sorting()


def dbupdate(db,tablename, keys, values, condition=None):
    """
    DEFINITION:
        Perform an update call to add values into specific keys of the selected table
    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - tablename:    name of the table
        - keys:         (list) list of keys to modify
        - values:       (list) list of values for the keys
    Kwargs:
        - condition:     (string) put in an optional where condition
    APPLICATION:
        >>>dbupdate(db, 'DATAINFO', [], [], condition='SensorID="MySensor"')
        returns a string with either 'success' or an error message
    """
    try:
        if not len(keys) == len(values):
            print("dbupdate: amount of keys does not fit provided values")
            return False
    except:
        print("dbupdate: keys and values must be provided as list e.g. [key1,key2,...]")
    if not len(keys) > 0:
        print("dbupdate: provide at least on key/value pair")
        return False

    if not condition:
        condition = ''
    else:
        condition = 'WHERE %s' % condition

    setlist = []
    for idx,el in enumerate(keys):
        st = '%s="%s"' % (el, values[idx])
        setlist.append(st)
    if len(setlist) > 0:
        setstring = ','.join(setlist)
    else:
        setstring = setlist[0]
    updatesql = 'UPDATE %s SET %s %s' % (tablename, setstring, condition)
    cursor = db.cursor()
    print(updatesql)
    try:
        cursor.execute(updatesql)
    except mysql.IntegrityError as message:
        return message
    except mysql.Error as message:
        return message
    except:
        return 'dbupdate: unkown error'
    db.commit()
    cursor.close()
    return 'success'

def dbgetfloat(db,tablename,sensorid,columnid,revision=None):
    """
    DEFINITION:
        Perform a select search and return floats
    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - tablename:    name of the table
        - sensorid:     sensor to match
        - columnid:     column in which search is performed
    Kwargs:
        - revision:     optional sensor revision (not used so far)
    APPLICATION:
        >>>deltaf =  dbgetfloat(db, 'DATAINFO', Sensor, 'DataDeltaF')
        returns deltaF from the DATAINFO table which matches the Sensor
    """
    sql = 'SELECT ' + columnid + ' FROM ' + tablename + ' WHERE SensorID = "' + sensorid + '"';
    try:
        cursor = db.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        if not row[0] == None:
            try:
                fl = float(row[0])
                return fl
            except:
                print("no float found")
                return row[0]
        else:
            return 0.0
    except:
            return 0.0

def dbgetstring(db,tablename,sensorid,columnid,revision=None):
    """
    DEFINITION:
        Perform a select search and return strings
    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - tablename:    name of the table
        - sensorid:     sensor to match
        - columnid:     column in which search is performed
    Kwargs:
        - revision:     optional sensor revision (not used so far)
    APPLICATION:
        >>>stationid =  dbgetstring(db, 'DATAINFO', 'LEMI25_22_0001', 'StationID')
        returns the stationid from the DATAINFO table which matches the Sensor
    """
    sql = 'SELECT ' + columnid + ' FROM ' + tablename + ' WHERE SensorID = "' + sensorid + '"';
    cursor = db.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    try:
        fl = float(row[0])
        return fl
    except:
        return row[0]

def dbupload(db, path,stationid,**kwargs):
    """
    DEFINITION:
        method to upload data to a database and to create an archive
    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - path:         path wher to fine the data (e.g. /home/data/*).
        - stationid:    station code (e.g. WIC).
    Kwargs:
        - starttime = kwargs.get('starttime')
        - endtime = kwargs.get('endtime')
        - headerdict = kwargs.get('headerdict')
        - archivepath = kwargs.get('archivepath')
        - sensorid = kwargs.get('sensorid')
        - tablenum1 = kwargs.get('tablenum1')
        - tablenum2 = kwargs.get('tablenum2')
        --
    RETURNS:
        --
    EXAMPLE:
        >>> dbinit(db)

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        1. Connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
        2. use method
        dbinit(db)
    headerdict =
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    headerdict = kwargs.get('headerdict')
    archivepath = kwargs.get('archivepath')
    sensorid = kwargs.get('sensorid')
    tablenum1 = kwargs.get('tablenum1')
    tablenum2 = kwargs.get('tablenum2')

    if starttime:
        if endtime:
            stream = read(path,starttime=starttime,endtime=endtime)
        else:
            stream = read(path,starttime=starttime)
    elif endtime:
            stream = read(path,endtime=endtime)
    else:
            stream = read(path)

    if headerdict:
        stream.header = headerdict
    stream.header['StationID']=stationid
    #stream.header['DataSamplingRate'] = str(dbsamplingrate(stream)) + ' sec'
    stream.header['DataSamplingRate'] = stream.samplingrate()
    if sensorid:
        stream.header['SensorID']=sensorid

    try:
        if tablenum1:
            stream2db(db,stream,mode='insert',tablename=sensorid+'_'+tablenum1)
        else:
            stream2db(db,stream,mode='insert')
    except:
        if tablenum1:
            stream2db(db,stream,mode='extend',tablename=sensorid+'_'+tablenum1)
        else:
            stream2db(db,stream,mode='extend')

    if archivepath:
        datainfoid = dbdatainfo(db,stream.header['SensorID'],stream.header,updatedb=False)
        stream.header = dbfields2dict(db,datainfoid)
        archivedir = os.path.join(archivepath,stream.header['StationID'],stream.header['SensorID'],datainfoid)
        stream.write(archivedir, filenamebegins=datainfoid+'_', format_type='PYCDF')
        datainfoorg = datainfoid

    if not int(np.round(float(stream.header['DataSamplingRate'].strip(' sec')))) == 60:
        stream = stream.filter(filter_type='gauss',filter_width=timedelta(minutes=1))

        try:
            if tablenum2:
               stream2db(db,stream,mode='insert',tableext=sensorid+'_'+tablenum2)
            else:
               stream2db(db,stream,mode='insert')
        except:
            if tablenum2:
               stream2db(db,stream,mode='extend',tableext=sensorid+'_'+tablenum2)
            else:
               stream2db(db,stream,mode='extend')

        if archivepath:
            datainfoid = dbdatainfo(db,stream.header['SensorID'],stream.header,updatedb=False)
            archivedir = os.path.join(archivepath,stream.header['StationID'],stream.header['SensorID'],datainfoid)
            stream.write(archivedir, filenamebegins=datainfoid+'_', format_type='PYCDF')
        # Reset filter
        stream.header = dbfields2dict(db,datainfoorg)



def dbinit(db):
    """
    DEFINITION:
        set up standard tables of magpy:
        DATAINFO, SENSORS, and STATIONS (and FlAGGING).
        Existing and valid inputs remain unchanged

    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
    Kwargs:
        --
    RETURNS:
        --
    EXAMPLE:
        >>> dbinit(db)

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        1. Connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
        2. use method
        dbinit(db)
    """

    # SENSORS TABLE
    # Create station table input
    headstr = ' CHAR(100), '.join(SENSORSKEYLIST) + ' CHAR(100)'
    headstr = headstr.replace('SensorID CHAR(100)', 'SensorID CHAR(50) NOT NULL PRIMARY KEY')
    headstr = headstr.replace('SensorDescription CHAR(100)', 'SensorDescription TEXT')
    createsensortablesql = "CREATE TABLE IF NOT EXISTS SENSORS (%s)" % headstr

    # STATIONS TABLE
    # Create station table input
    stationstr = ' CHAR(100), '.join(STATIONSKEYLIST) + ' CHAR(100)'
    stationstr = stationstr.replace('StationID CHAR(100)', 'StationID CHAR(50) NOT NULL PRIMARY KEY')
    stationstr = stationstr.replace('StationDescription CHAR(100)', 'StationDescription TEXT')
    stationstr = stationstr.replace('StationIAGAcode CHAR(100)', 'StationIAGAcode CHAR(10)')
    #stationstr = 'StationID CHAR(50) NOT NULL PRIMARY KEY, StationName CHAR(100), StationIAGAcode CHAR(10), StationInstitution CHAR(100), StationStreet CHAR(50), StationCity CHAR(50), StationPostalCode CHAR(20), StationCountry CHAR(50), StationWebInfo CHAR(100), StationEmail CHAR(100), StationDescription TEXT'
    createstationtablesql = "CREATE TABLE IF NOT EXISTS STATIONS (%s)" % stationstr

    # DATAINFO TABLE
    # Create datainfo table
    if not len(DATAINFOKEYLIST) == len(DATAVALUEKEYLIST):
        loggerdatabase.error("CHECK your DATA KEYLISTS")
        return
    FULLDATAKEYLIST = []
    for i, elem in enumerate(DATAINFOKEYLIST):
        newelem = elem + ' ' + DATAVALUEKEYLIST[i]
        FULLDATAKEYLIST.append(newelem)
    datainfostr = ', '.join(FULLDATAKEYLIST)
    createdatainfotablesql = "CREATE TABLE IF NOT EXISTS DATAINFO (%s)" % datainfostr

    # FLAGS TABLE
    # Create flagging table
    flagstr = ' CHAR(100), '.join(FLAGSKEYLIST) + ' CHAR(100)'
    flagstr = flagstr.replace('FlagID CHAR(100)', 'FlagID CHAR(50) NOT NULL PRIMARY KEY')
    flagstr = flagstr.replace('SensorID CHAR(100)', 'SensorID CHAR(50) NOT NULL')
    createflagtablesql = "CREATE TABLE IF NOT EXISTS FLAGS (%s)" % flagstr

    # BASELINE TABLE
    # Create baseline table
    basestr = ' CHAR(100), '.join(BASELINEKEYLIST) + ' CHAR(100)'
    basestr = basestr.replace('SensorID CHAR(100)', 'SensorID CHAR(50) NOT NULL')
    createbaselinetablesql = "CREATE TABLE IF NOT EXISTS BASELINE (%s)" % basestr

    # IP TABLE
    # Create ip addresses table
    ipstr = ' CHAR(100), '.join(IPKEYLIST) + ' CHAR(100)'
    ipstr = ipstr.replace('IP CHAR(100)', 'IP CHAR(50) NOT NULL')
    ipstr = ipstr.replace('IpComment CHAR(100)', 'IpComment TEXT')
    ipstr = ipstr.replace('IpSensors CHAR(100)', 'IpSensors TEXT')
    createiptablesql = "CREATE TABLE IF NOT EXISTS IPS (%s)" % ipstr


    # Pier TABLE
    # Create Pier overview table
    pierstr = ' CHAR(100), '.join(PIERLIST) + ' CHAR(100)'
    pierstr = pierstr.replace('PierID CHAR(100)', 'PierID CHAR(50) NOT NULL')
    pierstr = pierstr.replace('PierComment CHAR(100)', 'PierComment TEXT')
    pierstr = pierstr.replace('DeltaComment CHAR(100)', 'DeltaComment TEXT')
    pierstr = pierstr.replace('DeltaDictionary CHAR(100)', 'DeltaDictionary TEXT')
    pierstr = pierstr.replace('PierReference CHAR(100)', 'PierReference TEXT')
    createpiertablesql = "CREATE TABLE IF NOT EXISTS PIERS (%s)" % pierstr

    cursor = db.cursor()

    cursor.execute(createsensortablesql)
    cursor.execute(createstationtablesql)
    cursor.execute(createdatainfotablesql)
    cursor.execute(createflagtablesql)
    cursor.execute(createbaselinetablesql)
    cursor.execute(createiptablesql)
    cursor.execute(createpiertablesql)

    db.commit()
    cursor.close ()
    dbalter(db)


def dbdelete(db,datainfoid,**kwargs):
    """
    DEFINITION:
       Delete contents of the database
       If datainfoid is provided only this database contents are deleted
       If before is specified all data before the given date are erased
       Else before is determined according to the  samplingrateratio

    PARAMETERS:
    Variables:
        - db:               (mysql database) defined by mysql.connect().
        - datainfoid:       (string) table and dataid
    Kwargs:
        - samplingrateratio:(float) defines the ratio for deleting data older than (samplingperiod(sec)*samplingrateratio) DAYS
                        default = 45
        - timerange:        (int) time range to keep from now in days

    RETURNS:
        --

    EXAMPLE:
        >>> dbdelete(db,'DIDD_3121331_0002_0001')

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        so first connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")

    TODO:
        - If sampling rate not given in DATAINFO get it from the datastream
    """

    samplingrateratio = kwargs.get("samplingrateratio")
    timerange = kwargs.get("timerange")

    if not samplingrateratio:
        samplingrateratio = 12.0

    cursor = db.cursor()
    timeunit = 'DAY'

    # Do steps 1 to 2 if time interval is not given (parameter interval, before)
    if not timerange:
        # 1. Get sampling rate
        # option a - get from db
        try:
            getsr = 'SELECT DataSamplingRate FROM DATAINFO WHERE DataID = "%s"' % datainfoid
            cursor.execute(getsr)
            samplingperiod = float(cursor.fetchone()[0].strip(' sec'))
            loggerdatabase.debug("dbdelete: samplingperiod = %s" % str(samplingperiod))
        except:
            loggerdatabase.error("dbdelete: could not access DataSamplingRate in table %s" % datainfoid)
            samplingperiod = None
        # option b - get directly from stream
        if samplingperiod == None:
            # read stream and get sampling rate there
            #stream = db2stream(db,datainfoid)
            samplingperiod = 5  # TODO
        # 2. Determine time interval to delete
        # factor depends on available space...
        timerange = np.ceil(samplingperiod*samplingrateratio)

    loggerdatabase.debug("dbdelete: selected timerange of %s days" % str(timerange))

    # 3. Delete time interval
    loggerdatabase.info("dbdelete: deleting data of %s older than %s days" % (datainfoid, str(timerange)))
    try:
        deletesql = "DELETE FROM %s WHERE time < ADDDATE(NOW(), INTERVAL -%i %s)" % (datainfoid, timerange, timeunit)
        cursor.execute(deletesql)
    except:
        loggerdatabase.error("dbdelete: error when deleting data")

    # 4. Re-determine length for Datainfo
    try:
        newdatesql = "SELECT min(time),max(time) FROM %s" % datainfoid
        cursor.execute(newdatesql)
        value = cursor.fetchone()
        mintime = value[0]
        maxtime = value[1]
        updatedatainfosql = 'UPDATE DATAINFO SET DataMinTime="%s", DataMaxTime="%s" WHERE DataID="%s"' % (mintime,maxtime,datainfoid)
        cursor.execute(updatedatainfosql)
        loggerdatabase.info("dbdelete: DATAINFO for %s now covering %s to %s" % (datainfoid, mintime, maxtime))
    except:
        loggerdatabase.error("dbdelete: error when re-determining dates for DATAINFO")

    db.commit()
    cursor.close ()


def dbdict2fields(db,header_dict,**kwargs):
    """
    DEFINITION:
        Provide a dictionary with header information according to KEYLISTS STATION, SENSOR and DATAINFO
        Creates database inputs in standard tables

    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - header_dict:  (dict) dictionary with header information
    Kwargs:
        - mode:         (string) can be insert (default) or replace
                          insert tries to insert dict values: if already existing, a warning is issued
                          replace will delete the row with primary key information and then add the dict values (including Nones)
                          update will modify only given dict values
        - onlynone      (bool) to be used with update... will only update data with 'None' entries in db
        - all           (bool) if no datainfoid given, all=True will select all
                          datainfo entries matching a given sensorid and update all
                          tables in datainfo with(only available with mode='update')
    RETURNS:
        --
    EXAMPLE:
        >>> dbdict2fields(db,stream.header,mode='replace')

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        so first connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
    """
    mode = kwargs.get('mode')
    update = kwargs.get('update')
    onlynone = kwargs.get('onlynone')
    alldi = kwargs.get('alldi')

    if update:   # not used any more beginning with version 0.1.259
        mode = 'update'

    if not mode:
        mode = 'insert'

    cursor = db.cursor()

    sensorfieldlst,sensorvaluelst = [],[]
    stationfieldlst,stationvaluelst = [],[]
    datainfofieldlst,datainfovaluelst = [],[]
    usestation, usesensor, usedatainfo = False,False,False
    datainfolst = []

    def executesql(sql):
        """ Internal method for executing sql statements and getting proper error messages """
        message = ''
        try:
            cursor.execute(sql)
        except mysql.IntegrityError as message:
            return message
        except mysql.Error as message:
            return message
        except:
            return 'unkown error'
        return message

    def updatetable(table, primarykey, primaryvalue, key, value):
        if value != header_dict[key]:
            if value is not None and not onlynone:
                print("Will update value %s with %s" % (str(value),header_dict[key]))
                loggerdatabase.warning("dbdict2fields: ID is already existing but field values for field %s are differing: dict (%s); db (%s)" % (key, header_dict[key], str(value)))
                updatesql = 'UPDATE '+table+' SET '+key+' = "'+header_dict[key]+'" WHERE '+primarykey+' = "'+primaryvalue+'"'
                executesql(updatesql)
            else:
                print("value = None: Will update value %s with %s" % (str(value),header_dict[key]))
                updatesql = 'UPDATE '+table+' SET '+key+' = "'+header_dict[key]+'" WHERE '+primarykey+' = "'+primaryvalue+'"'
                executesql(updatesql)

    if "StationID" in header_dict:
        usestation = True
        loggerdatabase.debug("dbdict2fields: Found StationID in dict")
    else:
        loggerdatabase.warning("dbdict2fields: No StationID in dict - skipping any other eventually given station information")
    if "SensorID" in header_dict:
        usesensor = True
        loggerdatabase.debug("dbdict2fields: found SensorID in dict")
    else:
        loggerdatabase.warning("dbdict2fields: No SensorID in dict - skipping any other eventually given sensor information")
    if "DataID" in header_dict:
        usedatainfo = True
        datainfolst = [header_dict['DataID']]
        loggerdatabase.debug("dbdict2fields: found DataID in dict")
    else:
        loggerdatabase.warning("dbdict2fields: No DataID in dict")

    if alldi:
        usedatainfo = True
        if 'SensorID' in header_dict:
            getdatainfosql = 'SELECT DataID FROM DATAINFO WHERE SensorID = "'+header_dict['SensorID']+'"'
            msg = executesql(getdatainfosql)
            if not msg == '':
                loggerdatabase.warning("dbdict2fields: Obtaining DataIDs failed - %s" % msg)
            else:
                datainfolst = [elem[0] for elem in cursor.fetchall()]
                loggerdatabase.info("dbdict2fields: No DataID in dict - option alldi selected so all DATAINFO inputs will be updated")
        else:
            loggerdatabase.warning("dbdict2fields: alldi option requires a SensorID in dict which is not provided - skipping")
            usedatainfo = False

    # 2. Update content for the primary IDs
    for key in header_dict:
        fieldname = key
        fieldvalue = header_dict[key]
        if fieldname in STATIONSKEYLIST:
            if usestation:
                stationfieldlst.append(fieldname)
                stationvaluelst.append(fieldvalue)
        elif fieldname in SENSORSKEYLIST:
            if usesensor:
                sensorfieldlst.append(fieldname)
                sensorvaluelst.append(fieldvalue)
        elif fieldname in DATAINFOKEYLIST:
            if usedatainfo:
                datainfofieldlst.append(fieldname)
                datainfovaluelst.append(fieldvalue)
        else:
            loggerdatabase.warning("dbdict2fields: !!!!!!!! %s not existing !!!!!!!" % fieldname)
            pass

    print(len(stationfieldlst), len(sensorfieldlst), len(datainfofieldlst))
    if mode == 'insert':   #####   Insert ########
        if len(stationfieldlst) > 0:
            insertsql = 'INSERT INTO STATIONS (%s) VALUE (%s)' %  (', '.join(stationfieldlst), '"'+'", "'.join(stationvaluelst)+'"')
            msg = executesql(insertsql)
            if not msg == '':
                loggerdatabase.warning("dbdict2fields: insert for STATIONS failed - %s - try update mode" % msg)
        if len(sensorfieldlst) > 0:
            insertsql = 'INSERT INTO SENSORS (%s) VALUE (%s)' %  (', '.join(sensorfieldlst), '"'+'", "'.join(sensorvaluelst)+'"')
            msg = executesql(insertsql)
            if not msg == '':
                loggerdatabase.warning("dbdict2fields: insert for SENSORS failed - %s - try update mode" % msg)
        if len(datainfofieldlst) > 0:
            for elem in datainfolst:
                if 'DataID' in datainfofieldlst:
                    ind = datainfofieldlst.index('DataID')
                    datainfovaluelst[ind] = elem
                else:
                    datainfofieldlst.append('DataID')
                    datainfovaluelst.append(elem)
                insertsql = 'INSERT INTO DATAINFO (%s) VALUE (%s)' %  (', '.join(datainfofieldlst), '"'+'", "'.join(datainfovaluelst)+'"')
                msg = executesql(insertsql)
                if not msg == '':
                    loggerdatabase.warning("dbdict2fields: insert for DATAINFO of %s failed - %s - try update mode" % (elem,msg))
    elif mode == 'replace':   #####   Replace ########
        if len(stationfieldlst) > 0:
            insertsql = 'REPLACE INTO STATIONS (%s) VALUE (%s)' %  (', '.join(stationfieldlst), '"'+'", "'.join(stationvaluelst)+'"')
            msg = executesql(insertsql)
            if not msg == '':
                loggerdatabase.warning("dbdict2fields: insert for STATIONS failed - %s - try update mode" % msg)
        if len(sensorfieldlst) > 0:
            print (sensorvaluelst)
            insertsql = 'REPLACE INTO SENSORS (%s) VALUE (%s)' %  (', '.join(sensorfieldlst), '"'+'", "'.join(sensorvaluelst)+'"')
            msg = executesql(insertsql)
            if not msg == '':
                loggerdatabase.warning("dbdict2fields: insert for SENSORS failed - %s - try update mode" % msg)
        if len(datainfofieldlst) > 0:
            for elem in datainfolst:
                if 'DataID' in datainfofieldlst:
                    ind = datainfofieldlst.index('DataID')
                    datainfovaluelst[ind] = elem
                else:
                    datainfofieldlst.append('DataID')
                    datainfovaluelst.append(elem)
                insertsql = 'REPLACE INTO DATAINFO (%s) VALUE (%s)' %  (', '.join(datainfofieldlst), '"'+'", "'.join(datainfovaluelst)+'"')
                msg = executesql(insertsql)
                if not msg == '':
                    loggerdatabase.warning("dbdict2fields: insert for DATAINFO of %s failed - %s - try update mode" % (elem,msg))
    elif mode == 'update':   #####   Update ########
        for key in header_dict:
            print(key)
            if key in STATIONSKEYLIST:
                searchsql = "SELECT %s FROM STATIONS WHERE StationID = '%s'" % (key,header_dict['StationID'])
                executesql(searchsql)
                value = cursor.fetchone()[0]
                updatetable('STATIONS','StationID',header_dict['StationID'],key,value)
            if key in SENSORSKEYLIST:
                searchsql = "SELECT %s FROM SENSORS WHERE SensorID = '%s'" % (key,header_dict['SensorID'])
                executesql(searchsql)
                value = cursor.fetchone()[0]
                updatetable('SENSORS','SensorID',header_dict['SensorID'],key,value)
            if key in DATAINFOKEYLIST:
                for elem in datainfolst:
                    searchsql = "SELECT %s FROM DATAINFO WHERE DataID = '%s'" % (key,elem)
                    executesql(searchsql)
                    value = cursor.fetchone()[0]
                    updatetable('DATAINFO','DataID',elem,key,value)
    else:
        loggerdatabase.warning("dbdict2fields: unrecognized mode, needs to be one of insert, replace or update - check help(dbdict2field)")


    db.commit()
    cursor.close ()


def dbfields2dict(db,datainfoid):
    """
    DEFINITION:
        Provide datainfoid to get all informations from tables STATION, SENSORS and DATAINFO
        Use it to get metadata from database for saving cdf archive files
        Returns a dictionary

    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - datainfoid:   (string)
    Kwargs:
        --
    RETURNS:
        --
    EXAMPLE:
        >>> header_dict = dbfields2dict(db,'DIDD_3121331_0001_0001')

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        so first connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
    """
    metadatadict = {}
    cursor = db.cursor()


    #print "DBfields2dict: Running"
    #getids = 'SELECT sensorid FROM DATAINFO WHERE DataID = "'+datainfoid+'"'
    getids = 'SELECT sensorid,stationid FROM DATAINFO WHERE DataID = "'+datainfoid+'"'
    cursor.execute(getids)
    ids = cursor.fetchone()
    if not ids:
        return {}
    loggerdatabase.debug("dbfields2dict: Selected sensorid: %s" % ids[0])

    for key in DATAINFOKEYLIST:
        if not key == 'StationID': # Remove that line when included into datainfo
            getdata = 'SELECT '+ key +' FROM DATAINFO WHERE DataID = "'+datainfoid+'"'
            try:
                cursor.execute(getdata)
                row = cursor.fetchone()
                loggerdatabase.debug("dbfields2dict: got key from DATAINFO - %s" % getdata)
                if isinstance(row[0], basestring):
                    metadatadict[key] = row[0]
                    if key == 'ColumnContents':
                        colsstr = row[0]
                    if key == 'ColumnUnits':
                        colselstr = row[0]
                    if key in ['DataAbsFunctionObject','DataBaseValues']:
                        func = pickle.loads(str(cdf_file.attrs[key]))
                        stream.header[key] = func
                else:
                    if not row[0] == None:
                        metadatadict[key] = float(row[0])
            except mysql.Error as e:
                loggerdatabase.error("dbfields2dict: mysqlerror while adding key %s, %s" % (key,e))
            except:
                loggerdatabase.error("dbfields2dict: unkown error while adding key %s" % key)


    for key in SENSORSKEYLIST:
        getsens = 'SELECT '+ key +' FROM SENSORS WHERE SensorID = "'+ids[0]+'"'
        try:
            cursor.execute(getsens)
            row = cursor.fetchone()
            if isinstance(row[0], basestring):
                metadatadict[key] = row[0]
                if key == 'SensorKeys':
                    senscolsstr = row[0]
                if key == 'SensorElements':
                    senscolselstr = row[0]
            else:
                if row[0] == None:
                    pass
                    #metadatadict[key] = row[0]
                else:
                    metadatadict[key] = float(row[0])
        except:
            # if no sensor information is available e.g. BLV data
            pass

    try:
        if colsstr.find(',') >= 0:
            splitter = ','
        else:
            splitter = '_'
        cols = colsstr.split(splitter)
        colsel = colselstr.split(splitter)
    except:
        loggerdatabase.warning("dbfields2dict: Could not interpret column field in DATAINFO")

    # Use ColumnContent info for creating col information
    #print "DBfields2dict: ", cols
    try:
        for ind,el in enumerate(cols):
            if not el=='':
                col = KEYLIST[ind+1]
                key = 'col-'+col
                unitkey = 'unit-col-'+col
                metadatadict[key] = el
                metadatadict[unitkey] = colsel[ind]
    except:
        loggerdatabase.warning("dbfields2dict: Could not assign column name")

    """
    try:
        senscols = senscolsstr.split(',')
        senscolsel = senscolselstr.split(',')
        print [KEYLIST[ind] for ind,el in enumerate(cols) if not el=='']
        # check whether key info corresponds to column info
        if not len(senscols) == len(cols):
            print "dbfield2dict: DATAINFO column_contents does not match SensorKeys - Using Column_Contents"
            for i, elem in enumerate(cols):
                if not elem == '-':
                    key = 'col-'+elem
                    unitkey = 'unit-col-'+elem
                    metadatadict[key] = cols[i]
                    metadatadict[unitkey] = colsel[i]
        else:
            for i, elem in enumerate(senscols):
                key = 'col-'+elem
                unitkey = 'unit-col-'+elem
                pos = cols.index(senscolsel[i])
                metadatadict[key] = senscolsel[i]
                metadatadict[unitkey] = colsel[pos]
    except:
        loggerdatabase.warning("dbfields2dict: Could not assign column name")
    """
    for key in STATIONSKEYLIST:
        getstat = 'SELECT '+ key +' FROM STATIONS WHERE StationID = "'+ids[1]+'"'
        try:
            cursor.execute(getstat)
            row = cursor.fetchone()
        except:
            print("dbfields2dict: error when executing %s" % getstat)
            row = [None]
        if isinstance(row[0], basestring):
            metadatadict[key] = row[0]
        else:
            if row[0] == None:
                pass
                #metadatadict[key] = row[0]
            else:
                metadatadict[key] = float(row[0])

    return metadatadict


def dbalter(db):
    """
    DEFINITION:
        Use KEYLISTS and changes the columns of standard tables
        DATAINFO, SENSORS, and STATIONS.
        Can be used for changing (adding) contents to tables

    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
    Kwargs:
        --
    RETURNS:
        --
    EXAMPLE:
        >>> dbalter(db)

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        1. Connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
        2. use method
        dbalter(db)
    """
    if not db:
        loggerdatabase.error("dbalter: No database connected - aborting -- please create an empty database first")
        return
    cursor = db.cursor ()

    try:
        checksql = 'SELECT SensorID FROM SENSORS'
        cursor.execute(checksql)
        for key in SENSORSKEYLIST:
            try:
                checksql = 'SELECT ' + key+ ' FROM SENSORS'
                loggerdatabase.debug("dbalter: Checking key: %s" % key)
                cursor.execute(checksql)
                loggerdatabase.debug("dbalter: Found key: %s" % key)
            except:
                loggerdatabase.debug("dbalter: Missing key: %s" % key)
                addstr = 'ALTER TABLE SENSORS ADD ' + key + ' CHAR(100)'
                cursor.execute(addstr)
                loggerdatabase.debug("dbalter: Key added: %s" % key)
    except:
        loggerdatabase.warning("dbalter: Table SENSORS not existing")

    try:
        checksql = 'SELECT StationID FROM STATIONS'
        cursor.execute(checksql)
        for key in STATIONSKEYLIST:
            try:
                checksql = 'SELECT ' + key+ ' FROM STATIONS'
                loggerdatabase.debug("dbalter: Checking key: %s" % key)
                cursor.execute(checksql)
                loggerdatabase.debug("dbalter: Found key: %s" % key)
            except:
                loggerdatabase.debug("dbalter: Missing key: %s" % key)
                addstr = 'ALTER TABLE STATIONS ADD ' + key + ' CHAR(100)'
                cursor.execute(addstr)
                loggerdatabase.debug("dbalter: Key added: %s" % key)
    except:
        loggerdatabase.warning("dbalter: Table STATIONS not existing")

    try:
        checksql = 'SELECT DataID FROM DATAINFO'
        cursor.execute(checksql)
        for ind,key in enumerate(DATAINFOKEYLIST):
            try:
                checksql = 'SELECT ' + key+ ' FROM DATAINFO'
                loggerdatabase.debug("Checking key: %s" % key)
                cursor.execute(checksql)
                loggerdatabase.debug("Found key: %s" % key)
            except:
                loggerdatabase.debug("Missing key: %s" % key)
                if len(DATAVALUEKEYLIST) == len(DATAINFOKEYLIST):
                    addstr = 'ALTER TABLE DATAINFO ADD ' + key + ' ' + DATAVALUEKEYLIST[ind]
                else:
                    print("dbalter: Differences between provided DATAVALUEKEYLIST and DATAINFOKEYLIST")
                    addstr = 'ALTER TABLE DATAINFO ADD ' + key + ' CHAR(100)'
                cursor.execute(addstr)
                loggerdatabase.debug("Key added: %s" % key)
    except:
        loggerdatabase.warning("Table DATAINFO not existing")

    try:
        checksql = 'SELECT PierID FROM PIERS'
        cursor.execute(checksql)
        for ind,key in enumerate(PIERLIST):
            try:
                checksql = 'SELECT ' + key+ ' FROM PIERS'
                loggerdatabase.debug("Checking key: %s" % key)
                cursor.execute(checksql)
                loggerdatabase.debug("Found key: %s" % key)
            except:
                loggerdatabase.debug("Missing key: %s" % key)
                addstr = 'ALTER TABLE PIERS ADD ' + key + ' TEXT'
                cursor.execute(addstr)
                loggerdatabase.debug("Key added: %s" % key)
    except:
        loggerdatabase.warning("Table PIERS not existing")

    db.commit()
    cursor.close ()


def dbselect(db, element, table, condition=None, expert=None, debug=False):
    """
    DESCRIPTION:
        Function to select elements from a table.
    PARAMETERS:
        db (database)
        element         (string)
        table           (string) name of the table
        condition       (string) Where clause
        expert          (String) replaces the complete "Where"
    RETURNS:
        A list containing the matching elements
    EXAMPLE:
        magsenslist = dbselect(db, 'SensorID', 'SENSORS', 'SensorGroup = "Magnetism"')
        tempsenslist = dbselect(db, 'SensorID', 'SENSORS','SensorElements LIKE "%T%"')
        lasttime = dbselect(db,'time','DATATABLE',expert="ORDER BY time DESC LIMIT 1")

    """
    returnlist = []
    if expert:
        sql = "SELECT "+element+" from "+table+" "+expert
    elif not condition:
        sql = "SELECT "+element+" from "+table
    else:
        sql = "SELECT "+element+" from "+table+" WHERE "+condition
    if debug:
        print ("dbselect SQL:", sql)
    try:
        cursor = db.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        if debug:
            print ("dbselect rows:", rows)
        for el in rows:
            if len(el) < 2:
                returnlist.append(el[0])
            else:
                returnlist.append(el)
    except:
        pass

    db.commit()
    cursor.close()
    return returnlist


def dbcoordinates(db, pier, epsgcode='epsg:4326'):

    try:
        from pyproj import Proj, transform
    except ImportError:
        print ("dbcoordinates: You need to install pyproj to use this method")
        return (0.0 , 0.0)

    startlong = dbselect(db,'PierLong','PIERS','PierID = "A2"')
    startlat = dbselect(db,'PierLat','PIERS','PierID = "A2"')
    coordsys = dbselect(db,'PierCoordinateSystem','PIERS','PierID = "A2"')
    startlong = float(startlong[0].replace(',','.'))
    startlat = float(startlat[0].replace(',','.'))
    coordsys = coordsys[0].split(',')[1].lower().replace(' ','')

    # projection 1: GK M34
    p1 = Proj(init=coordsys)
    # projection 2: WGS 84
    p2 = Proj(init='epsg:4326')
    # transform this point to projection 2 coordinates.
    lon1, lat1 = transform(p1,p2,startlong,startlat)

    return (lon1,lat1)


def dbsensorinfo(db,sensorid,sensorkeydict=None,sensorrevision = '0001'):
    """
    DEFINITION:
        checks whether sensorinfo is already available in SENSORS tab
        if not, it creates a new line for the provided sensorid in the selected database db
        Keywords are all database fields provided as dictionary

    PARAMETERS:
    Variables:
        - db:             (mysql database) defined by mysql.connect().
        - sensorid:       (string) code for sensor if.
    Optional variables:
        - sensorkeydict:  (dict) provide a dictionary with sensor information (see SENSORS) .
        - sensorrevision: (string) provide a revision number in format '0001' .
    Kwargs:
        --
    RETURNS:
       sensorid with revision number e.g. DIDD_235178_0001
    USED BY:
       stream2db
    EXAMPLE:
        >>> dbsensorinfo()

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        1. Connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
        2. use method
        dbalter(db)
    """

    sensorhead,sensorvalue,numlst = [],[],[]

    cursor = db.cursor()

    if sensorkeydict:
        for key in sensorkeydict:
            if key in SENSORSKEYLIST:
                sensorhead.append(key)
                sensorvalue.append(sensorkeydict[key])

    loggerdatabase.debug("dbsensorinfo: sensor: ", sensorhead, sensorvalue)

    check = 'SELECT SensorID, SensorRevision, SensorName, SensorSerialNum FROM SENSORS WHERE SensorID LIKE "'+sensorid+'%"'
    try:
        cursor.execute(check)
        rows = cursor.fetchall()
    except:
        loggerdatabase.warning("dbsensorinfo: Could not access table SENSORS in database - creating table SENSORS")
        headstr = ' CHAR(100), '.join(SENSORSKEYLIST) + ' CHAR(100)'
        headstr = headstr.replace('SensorID CHAR(100)', 'SensorID CHAR(50) NOT NULL PRIMARY KEY')
        createsensortablesql = "CREATE TABLE IF NOT EXISTS SENSORS (%s)" % headstr
        cursor.execute(createsensortablesql)

    if len(rows) > 0:
        print ("SensorID is existing in Table SENSORS")
        # SensorID is existing in Table
        loggerdatabase.info("dbsensorinfo: Sensorid already existing in SENSORS")
        loggerdatabase.info("dbsensorinfo: rows: {}".format(rows))
        # Get the maximum revision number
        for i in range(len(rows)):
            rowval = rows[i][1]
            try:
                numlst.append(int(rowval))
            except:
                pass
        try:
            maxnum = max(numlst)
        except:
            maxnum = None

        if isinstance(maxnum, int):
            index = numlst.index(maxnum)
            sensorid = rows[index][0]
        else:
            loggerdatabase.warning("dbsensorinfo: SensorRevision not set - changing to %s" % sensorrevision)
            if rows[0][2] == None:
                sensoridsplit = sensorid.split('_')
                if len(sensoridsplit) == 2:
                    sensorserialnum = sensoridsplit[1]
                else:
                    sensorserialnum = sensorid
            else:
                sensorserialnum = rows[0][2]
            oldsensorid = sensorid
            sensorid = sensorid+'_'+sensorrevision
            updatesensorsql = 'UPDATE SENSORS SET SensorID = "' + sensorid + '", SensorRevision = "' + sensorrevision + '", SensorSerialNum = "' + sensorserialnum + '" WHERE SensorID = "' + oldsensorid + '"'
            cursor.execute(updatesensorsql)
    else:
        print ("SensorID not yet existing in Table SENSORS")
        # SensorID is not existing in Table
        loggerdatabase.info("dbsensorinfo: Sensorid not yet existing in SENSORS.")
        # Check whether given sensorid is incomplete e.g. revision number is missing
        loggerdatabase.info("dbsensorinfo: Creating new sensorid %s " % sensorid)
        if not 'SensorSerialNum' in sensorhead:
            print ("No serial number")
            sensoridsplit = sensorid.split('_')
            if len(sensoridsplit) in [2,3]:
                sensorserialnum = sensoridsplit[1]
            if len(sensoridsplit) == 3:
                if not 'SensorRevision' in sensorhead:
                    sensorhead.append('SensorRevision')
                    sensorvalue.append(sensoridsplit[2])
                index = sensorhead.index('SensorID')
                sensorvalue[index] = sensorid
            else:
                sensorserialnum = sensorid
            sensorhead.append('SensorSerialNum')
            sensorvalue.append(sensorserialnum)
            loggerdatabase.debug("dbsensorinfo: sensor %s, %s" % (sensoridsplit, sensorserialnum))

        if not 'SensorRevision' in sensorhead:
            sensoridsplit = sensorid.split('_')
            if len(sensoridsplit) > 1:
                sensorrevision = sensoridsplit[-1]
            sensorhead.append('SensorRevision')
            sensorvalue.append(sensorrevision)
            if not 'SensorID' in sensorhead:
                sensorhead.append('SensorID')
                sensorvalue.append(sensorid+'_'+sensorrevision)
                sensorid = sensorid+'_'+sensorrevision
            else:
                index = sensorhead.index('SensorID')
                # Why???????? This seems to be wrong (maybe important for OW  -- added an untested corr (leon))
                if 'OW' in sensorvalue:
                    sensorvalue[index] = sensorid+'_'+sensorrevision

        ### create an input for the new sensor
        sensorsql = "INSERT INTO SENSORS(%s) VALUES (%s)" % (', '.join(sensorhead), '"'+'", "'.join(sensorvalue)+'"')
        #print "Adding the following info to SENSORS: ", sensorsql
        cursor.execute(sensorsql)

    db.commit()
    cursor.close ()

    return sensorid


def dbdatainfo(db,sensorid,datakeydict=None,tablenum=None,defaultstation='WIC',updatedb=True):
    """
    DEFINITION:
        provide sensorid and any relevant meta information for datainfo tab
        returns the full datainfoid

    PARAMETERS:
    Variables:
        - db:             (mysql database) defined by mysql.connect().
        - sensorid:       (string) code for sensor if.
    Optional variables:
        - datakeydict:    (dict) provide a dictionary with data table information (see DATAINFO) .
        - tablenum:       (string) provide a table number in format '0001' .
               If tablenum is specified, the corresponding table is selected and DATAINFO is updated with the provided datakeydictdbdatainfo
               If tablenum = 'new', a new number is generated e.g. 0006 if no DATAINFO matching the provided info is found
               If no tablenum is selected all data including all Datainfo is appended to the latest numbe if no DATAINFO matching the provided info is found and no conflict with the existing DATAINFO is found
        - defaultstation: (string) provide a default station code.
               An important keyword is the StationID. Please make sure to provide it. Otherwise the defaultvalue 'WIC' is used

        - updatedb:      (bool) if true (default) then the new infoid is added into the database
    Kwargs:
        --
    RETURNS:
       datainfoid e.g. DIDD_235178_0001_0001
    USED BY:
       stream2db, writeDB
    EXAMPLE:
        >>> tablename = dbdatainfo(db,sensorid,headdict,None,stationid)

    APPLICATION:
        1. read a datastream: stream = read(file)
        2. add additional header info: stream.header['StationID'] = 'WIC'
        3. Open a mysql database
        4. write stream2db
        5. check DATAINFO table: num = dbdatainfo(db,steam.header['SensorID'],stream.header,None,None)
              case 1: new sensor - not yet existing
                      tablenum = 0001
              case 2: sensor existing
                      default: select fitting datainfo (compare only provided fields - not in DATAINFO existing) and get highest number matching the info
                      tablenum=new: add new number, disregarding any previous matching DATAINFO
                      tablenum=0003: returns 0003 if case 1 is satisfied

        Not changeable are:
        DataMinTime CHAR(50)
        DataMaxTime CHAR(50)
    """

    # Define keys here which do not trigger a new revision number in the table
    SKIPKEYS = ['DataID', 'SensorID', 'ColumnContents','ColumnUnits', 'DataFormat','DataTerms','DataDeltaF','DataDeltaT1','DataDeltaT2', 'DataFlagModification', 'DataAbsFunc', 'DataAbsInfo', 'DataBaseValues', 'DataFlagList',
'DataAbsDegree','DataAbsKnots','DataAbsMinTime','DataAbsMaxTime','DataAbsDate', 'DataRating','DataComments','DataSource','DataAbsFunctionObject', 'DataDeltaValues', 'DataTerms', 'DataReferences', 'DataPublicationLevel', 'DataPublicationDate', 'DataStandardLevel','DataStandardName', 'DataStandardVersion', 'DataPartialStandDesc','DataRotationAlpha','DataRotationBeta']

    searchlst = ' '
    datainfohead,datainfovalue=[],[]
    novalues = False
    if datakeydict:
        sm = ''
        sm = datakeydict.get('SensorModule')
        if sm in ['OW','RCS','ow','rcs']:
            print ("dbdatainfo: Skipping SamplingRate for data revision")
            SKIPKEYS.append('DataSamplingRate')

        for key in datakeydict:
            if key in DATAINFOKEYLIST and not key == 'DataMaxTime' and not key == 'DataMinTime':
                # MinTime and Maxtime are changing and therefore are excluded from check
                if not str(datakeydict[key]) == '':
                    datainfohead.append(key)
                    ind = DATAINFOKEYLIST.index(key)
                    ### if key in ['DataFlagList']:
                    ###     add keycontents to FLAGS if not existing
                    if key in ['DataAbsFunctionObject','DataBaseValues']:
                        pfunc = pickle.dumps(datakeydict[key])
                        datainfovalue.append( pfunc )
                    elif DATAVALUEKEYLIST[ind].startswith('DEC') or DATAVALUEKEYLIST[ind].startswith('FLO'):
                        try:
                            datainfovalue.append(float(datakeydict[key]))
                        except:
                            loggerdatabase.warning("dbdatainfo: Trying to read FLOAT value failed")
                            datainfovalue.append(str(datakeydict[key]))
                    elif DATAVALUEKEYLIST[ind].startswith('DEC') or DATAVALUEKEYLIST[ind].startswith('FLO'):
                        try:
                            datainfovalue.append(int(datakeydict[key]))
                        except:
                            loggerdatabase.warning("dbdatainfo: Trying to read INT value failed")
                            datainfovalue.append(str(datakeydict[key]))
                    else:
                        datainfovalue.append(str(datakeydict[key]))
                    #if key == 'DataID' or key.startswith('Column') or key == 'DataFormat' or key == 'DataTerms' or key == 'DataFlagModification' or key.startswith('DataPubli'):
                    if key in SKIPKEYS:
                        pass
                    else:
                        searchlst += 'AND ' + key + ' = "'+ str(datakeydict[key]) +'" '

    datainfonum = '0001'
    numlst,intnumlst = [],[]

    cursor = db.cursor()

    # check for appropriate sensorid
    loggerdatabase.debug("dbdatainfo: Reselecting SensorID")
    #print "dbdatainfo: (1)", sensorid, datakeydict
    sensorid = dbsensorinfo(db,sensorid,datakeydict)
    #print "dbdatainfo: (2)", sensorid
    if 'SensorID' in datainfohead:
        index = datainfohead.index('SensorID')
        datainfovalue[index] = sensorid
    loggerdatabase.debug("dbdatainfo:  -- SensorID is now %s" % sensorid)

    checkinput = 'SELECT StationID FROM DATAINFO WHERE SensorID = "'+sensorid+'"'
    #print checkinput
    try:
        cursor.execute(checkinput)
        rows = cursor.fetchall()
    except:
        loggerdatabase.warning("dbdatainfo: Column StationID not yet existing in table DATAINFO (very old magpy version) - creating it ...")
        stationaddstr = 'ALTER TABLE DATAINFO ADD StationID CHAR(50) AFTER SensorID'
        cursor.execute(stationaddstr)

    # Check STATIONS
    # ##############
    if datakeydict:
        checkstation = 'SELECT StationID FROM STATIONS WHERE StationID = "' + datakeydict["StationID"] +'"'
        try:
            cursor.execute(checkstation)
            rows = cursor.fetchall()
        except:
            loggerdatabase.warning("dbdatainfo: STATIONS not yet existing ...")
            rows=[1]
        if not len(rows) > 0:
            print ("dbdatainfo: Did not find StationID in STATIONS - adding it")
            # Add all Station info to STATIONS
            stationhead, stationvalue = [],[]
            for key in datakeydict:
                if key in STATIONSKEYLIST:
                    stationhead.append(key)
                    stationvalue.append('"'+str(datakeydict[key])+'"')
            sql = 'INSERT INTO STATIONS(%s) VALUES (%s)' % (', '.join(stationhead), ', '.join(stationvalue))
            #loggerdatabase.debug("dbdatainfo: sql: %s" % datainfosql)
            if updatedb:
                cursor.execute(sql)

    checkinput = 'SELECT DataID FROM DATAINFO WHERE SensorID = "'+sensorid+'"'
    loggerdatabase.debug("dbdatainfo: %s " % checkinput)
    try:
        #print checkinput
        cursor.execute(checkinput)
        rows = cursor.fetchall()
        loggerdatabase.debug("dbdatainfo: Number of existing DATAINFO lines: %s" % str(rows))
    except:
        loggerdatabase.warning("dbdatainfo: Could not access table DATAINFO in database")
        loggerdatabase.warning("dbdatainfo: Creating it now")
        if not len(DATAINFOKEYLIST) == len(DATAVALUEKEYLIST):
            loggerdatabase.error("CHECK your DATA KEYLISTS")
            return
        FULLDATAKEYLIST = []
        for i, elem in enumerate(DATAINFOKEYLIST):
            newelem = elem + ' ' + DATAVALUEKEYLIST[i]
            FULLDATAKEYLIST.append(newelem)
        datainfostr = ', '.join(FULLDATAKEYLIST)

        createdatainfotablesql = "CREATE TABLE IF NOT EXISTS DATAINFO (%s)" % datainfostr
        cursor.execute(createdatainfotablesql)

    def joindatainfovalues(head,lst):
        # Submethod for getting sql string from values
        dst = []
        for i, elem in enumerate(head):
            ind = DATAINFOKEYLIST.index(elem)
            if DATAVALUEKEYLIST[ind].startswith('DEC') or DATAVALUEKEYLIST[ind].startswith('FLO') or DATAVALUEKEYLIST[ind].startswith('INT'):
                dst.append(str(lst[i]))
            else:
                dst.append('"'+str(lst[i])+'"')
        return ','.join(dst)

    # check whether input in DATAINFO with sensorid is existing already
    if len(rows) > 0:
        print ("dbdatainfo: Found existing tables")
        # Get maximum number
        for i in range(len(rows)):
            rowval = rows[i][0].replace(sensorid + '_','')
            #print len(rows), rowval, sensorid+'_', rows[i][0]
            try:
                numlst.append(int(rowval))
                #print numlst
            except:
                print ("crap")
                pass
        maxnum = max(numlst)
        loggerdatabase.debug("dbdatainfo: Maxnum: %i" % maxnum)
        # Perform intensive search using any given meta info
        intensivesearch = 'SELECT DataID FROM DATAINFO WHERE SensorID = "'+sensorid+'"' + searchlst
        #print ("dbdatainfo: Searchlist: %s" % intensivesearch)
        loggerdatabase.info("dbdatainfo: Searchlist: %s" % intensivesearch)
        cursor.execute(intensivesearch)
        intensiverows = cursor.fetchall()
        #print intensivesearch, intensiverows
        loggerdatabase.debug("dbdatainfo: Found matching table: %s" % str(intensiverows))
        loggerdatabase.debug("dbdatainfo: using searchlist %s"% intensivesearch)
        loggerdatabase.debug("dbdatainfo: intensiverows: %i" % len(intensiverows))
        if len(intensiverows) > 0:
            print ("dbdatainfo: DataID existing - updating {}".format(intensiverows[0]))
            selectupdate = True
            for i in range(len(intensiverows)):
                # if more than one record is existing select the latest (highest) number
                introwval = intensiverows[i][0].replace(sensorid + '_','')
                try:
                    intnumlst.append(int(introwval))
                except:
                    pass
            intmaxnum = max(intnumlst)
            datainfonum = '{0:04}'.format(intmaxnum)
            #print datainfonum, intmaxnum
        else:
            print ("dbdatainfo: Creating new DataID")
            print ("dbdatainfo: because - {}".format(intensiverows))
            #print (intensivesearch)
            selectupdate = False
            datainfonum = '{0:04}'.format(maxnum+1)
            #print "dbdatainfo", datainfohead, datainfovalue, datainfonum
            # select maximum number + 1
            if 'DataID' in datainfohead:
                selectindex = datainfohead.index('DataID')
                datainfovalue[selectindex] = sensorid + '_' + datainfonum
            else:
                datainfohead.append('DataID')
                datainfovalue.append(sensorid + '_' + datainfonum)
            datainfostring = joindatainfovalues(datainfohead, datainfovalue)
            #print "dbdatainfo", datainfohead, datainfostring
        if selectupdate:
            #print "Here", datainfohead
            sqllst = [key + " ='" + str(datainfovalue[idx]) +"'" for idx, key in enumerate(datainfohead) if key in SKIPKEYS and not key == 'DataAbsFunctionObject' and not key == 'DataBaseValues' and not key == 'DataFlagList' and not key=='DataID']
            if 'DataAbsFunctionObject' in datainfohead: ### Tested Text and Binary so far. No quotes is OK.
                print ("dbdatainfo: adding DataAbsFunctionObjects to DATAINFO is not yet working")
                #pfunc = pickle.dumps(datainfovalue[datainfohead.index('DataAbsFunctionObject')])
                #sqllst.append('DataAbsFunctionObject' + '=' + pfunc)
                # For testing:
                    #datainfosql = 'INSERT INTO DATAINFO(DataAbsFunctionObject) VALUES (%s)' % (pfunc)
                    #cursor.execute(datainfosql)
            if 'DataBaseValues' in datainfohead: ### Tested Text and Binary so far. No quotes is OK.
                print ("dbdatainfo: adding DataBaseValues to DATAINFO is not yet working")
                #pfunc = pickle.dumps(datainfovalue[datainfohead.index('DataBaseValues')])
                #TODO convert pfunc to string
                #sqllst.append('DataBaseValues' + '=' + pfunc)
                # For testing:
                #datainfosql = 'INSERT INTO DATAINFO(DataBaseValues) VALUES (%s)' % (pfunc)
                #cursor.execute(datainfosql)
            if not len(sqllst) > 0:
                novalues = True
            datainfosql = "UPDATE DATAINFO SET " + ", ".join(sqllst) +  " WHERE DataID = '" + sensorid + "_" + datainfonum + "'"
            #print datainfosql
        else:
            #print "No, Here"
            datainfosql = 'INSERT INTO DATAINFO(%s) VALUES (%s)' % (', '.join(datainfohead), datainfostring)
            loggerdatabase.debug("dbdatainfo: sql: %s" % datainfosql)
        if updatedb and not novalues:
            cursor.execute(datainfosql)
        datainfoid = sensorid + '_' + datainfonum
    else:
        print ("dbdatainfo: Creating new table")
        # return 0001
        datainfoid = sensorid + '_' + datainfonum
        if not 'DataID' in datainfohead:
            datainfohead.append('DataID')
            datainfovalue.append(datainfoid)
        else:
            ind = datainfohead.index('DataID')
            datainfovalue[ind] = datainfoid
        # and create datainfo input
        loggerdatabase.debug("dbdatainfo: %s, %s" % (datainfohead, datainfovalue))
        datainfostring = joindatainfovalues(datainfohead, datainfovalue)
        datainfosql = 'INSERT INTO DATAINFO(%s) VALUES (%s)' % (', '.join(datainfohead), datainfostring)
        #print datainfosql
        if updatedb:
            try:
                print ("Updating DATAINFO table")
                cursor.execute(datainfosql)
            except mysql.Error as e:
                print ("Failed: {}".format(e))
            except:
                print ("Failed for unknown reason")


    db.commit()
    cursor.close ()

    return datainfoid

def writeDB(db, datastream, tablename=None, StationID=None, mode='replace', revision=None, debug=False, **kwargs):

    """
    DEFINITION:
        Method to write datastreams to a mysql database

    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - datastream:   (magpy datastream)
    Kwargs:
        - mode:         (string)
                            mode: replace -- replaces existing table contents with new one, also replaces informations from sensors and station table
                            mode: delete -- deletes existing tables and writes new ones -- remove (or make it extremeley difficult to use) this method after initializing of tables
                            mode: insert -- add data not existing in table from stream

         - tablename:   (string) provide the tablename to which data is written
                                 SENSORS and STATIONS remain unchanged, DATAINFO data
                                 is updated if existing
         - StationID:   (string) provide the StationID
    REQUIRES:
        dbdatainfo

    RETURNS:
        --

    EXAMPLE:
        >>> stream.header['StationID'] = 'MyStation'
        >>> stream2db(db,stream)

        # Replace existing contents of table with has meta info of stream and
        # highest revision
        >>> writeDB(db,stream,mode='replace')

        # Writing data without any header info (does not update SENSORS, STATIONS)
        # DATAINFO is updated however, keeping blanks for sensorid and stationid
        >>> writeDB(db,stream,tablename='myid_0001_0001',noheader=True)

    APPLICATION:
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
        stream = read('/path/to/my/files/*', starttime='2013-01-01',endtime='2013-02-01')
        writeDB(db,stream,StationID='MyObsCode')

    TODO:
        - make it possible to create spezial tables by defining an extension (e.g. _sp2013min) where sp indicates special
    """

    if not db:
        loggerdatabase.error("stream2DB: No database connected - aborting -- please create and initiate a database first")
        return

    if not len(datastream.ndarray[0]) > 0:
        print ("writeDB is used for ndarray type - use stream2DB for LineStruct")
        return

    # ----------------------------------------------
    #   Identify tablename
    # ----------------------------------------------

    if tablename:
        if debug:
            print ("writeDB: not bothering with header, sensorid etc")
            print ("         updating DATAINFO only if existing")
        # Just check whether DataID, if not a table, exists and append data
        #return some message on success
    else:
        if not StationID==None:
            datastream.header['StationID'] = StationID

        if not 'SensorID' in datastream.header:
            #loggerdatabase.error("writeDB: No SensorID provided in header - aborting")
            print ("writeDB: No SensorID provided in header - define by datastream.header['SensorID'] = 'YourID' before calling writeDB - aborting")
            return
        if not 'StationID' in datastream.header and not StationID:
            #loggerdatabase.error("writeDB: No StationID provided - use option StationID='MyID'")
            print ("writeDB: No StationID provided - use option StationID='MyStationID'")
            return

        datastream.header['DataSamplingRate'] = datastream.samplingrate()

        # Updating DATAINFO, SENSORS and STATIONS
        #print "Before", datastream.header['SensorID']
        # TODO: Abolute function object
        # Current solution: remove it
        datastream.header['DataAbsFunctionObject'] = ''
        tablename = dbdatainfo(db,datastream.header['SensorID'],datastream.header,None,datastream.header['StationID'])

        #print ("After", tablename, datastream.header.get('SensorID'))

    # ----------------------------------------------
    #   Putting together all data
    # ----------------------------------------------

    keys = datastream._get_key_headers()
    ti = ['time']
    ti.extend(keys)
    keys = ti
    #array = np.asarray([elem for elem in datastream.ndarray if len(elem) > 0], dtype=object)

    # delete all columns which only contain nans ot '-'
    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    def trim_time(t):
        # Rounding time to milliseconds
        s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
        tail = s[-7:]
        f = round(float(tail), 3)
        temp = "%.3f000" % f
        if f == 1.0:
            t = t+timedelta(seconds=1)
            s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
        return "%s%s" % (s[:-7], temp[1:])

    array = [[] for key in KEYLIST]
    for idx,col in enumerate(datastream.ndarray):
        key = KEYLIST[idx]
        if len(col) > 0 and not False in checkEqual3(col):
            print ("Found only identical values of {} in column {}".format(col[0],idx))
            if col[0] in ['nan', float('nan'),NaN,'-',None,'']: #remove place holders
                array[idx] = np.asarray([])
            else: # add as usual
                array[idx] = [el if isinstance(el, basestring) or el in [None] else float(el) for el in datastream.ndarray[idx]] # converts float64 to float-pymsqldb (required for python3 and pymsqldb)
                #array[idx] = datastream.ndarray[idx]
                try:
                    array[idx][np.isnan(array[idx].astype(float))] = None
                except:
                    pass # will fail for strings
        elif key.endswith('time') and len(col) > 0:
            #if col[0]
            try:
                tcol = np.asarray([num2date(elem) for elem in col.astype(float)])
            except:
                try:
                    tstr = DataStream()
                    tcol = np.asarray([tstr._testtime(elem) for elem in col])
                except:
                    tcol = np.asarray([])
            tcol = [trim_time(elem.replace(tzinfo=None)) for elem in tcol]
            array[idx]=np.asarray(tcol)
        elif len(col) > 0: # and KEYLIST[idx] in NUMKEYLIST:
            #array[idx] = datastream.ndarray[idx]
            array[idx] = [el if isinstance(el, basestring) or el in [None] else float(el) for el in datastream.ndarray[idx]] # converts float64 to float-pymsqldb (required for python3 and pymsqldb)
            #print ("idx1", type(array[idx][0]))
            #print ("idx1", array[idx][0], type(array[idx][0]))
            try:
                array[idx] = [None if np.isnan(el) else el for el in array[idx]]
                #array[idx][np.isnan(tarray[idx].astype(float))] = None
            except:
                pass # will fail for strings
        #elif len(col) > 0:
        #    valid_chars='-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        #    el[i] = ''.join([e for e in list(el[i]) if e in list(valid_chars)])
        #    array[idx] = datastream.ndarray[idx].astype(object)
    keys = np.asarray([KEYLIST[idx] for idx,elem in enumerate(array) if len(elem)>0])
    array = np.asarray([elem for elem in array if len(elem)>0], dtype=object)
    dollarstring = ['%s' for elem in keys]

    
    values = array.transpose()

    #print ("Test0", values[0])
    #print ("Type: ", [type(el) for el in values[0]])

    insertmanysql = "INSERT INTO %s(%s) VALUES (%s)" % (tablename, ', '.join(keys), ', '.join(dollarstring))

    values = tuple([tuple(list(val)) for val in values])

    # ----------------------------------------------
    #   if tablename does not yet exist create table/ add column if not yet existing
    # ----------------------------------------------
    cursor = db.cursor ()

    count = 0
    dataheads,collst,unitlst = [],[],[]
    for key in KEYLIST:
        colstr = ''
        unitstr = ''
        if key in keys:
            if key in NUMKEYLIST:
                dataheads.append(key + ' DOUBLE')
            elif key.endswith('time'):
                if key == 'time':
                    dataheads.append(key + ' CHAR(40) NOT NULL PRIMARY KEY')
                else:
                    dataheads.append(key + ' CHAR(40)')
            else:
                dataheads.append(key + ' CHAR(100)')
            ## Getting column and units
            for hkey in datastream.header:
                if key == hkey.replace('col-',''):
                    colstr = datastream.header[hkey]
                elif key == hkey.replace('unit-col-',''):
                    unitstr = datastream.header[hkey]

            #print "Checking key", key
            try:
                sql = "SELECT " + key + " FROM " + tablename + " ORDER BY time DESC LIMIT 1"
                cursor.execute(sql)
                count +=1
            except mysql.Error as e:
                emsg = str(e)
                if emsg.find("Table") >= 0 and emsg.find("doesn't exist") >= 0:
                    # if table not existing
                    pass
                elif emsg.find("Unknown column") >= 0:
                    print ("writeDB: key %s not existing - adding it" % key)
                    # if key not yet existing
                    addsql = "ALTER TABLE " + tablename + " ADD " + dataheads[-1]
                    cursor.execute(addsql)
                else:
                    print ("writeDB: unkown MySQL error when checking for existing tables: %s" %e)
            except:
                print ("writeDB: unkown error when checking for existing tables")

        if not key=='time':
            collst.append(colstr)
            unitlst.append(unitstr)


    if count == 0:
        print ("Table not existing - creating it")
        # Creating table
        createdatatablesql = "CREATE TABLE IF NOT EXISTS %s (%s)" % (tablename,', '.join(dataheads))
        cursor.execute(createdatatablesql)

    #print "Table created/altered"

    # ----------------------------------------------
    #   upload data
    # ----------------------------------------------

    #print insertmanysql
    if mode == 'replace':
        insertmanysql = insertmanysql.replace("INSERT","REPLACE")
    #print (values)
    cursor.executemany(insertmanysql,values)

    # ----------------------------------------------
    #   update DATAINFO - move to a separate method
    # ----------------------------------------------

    dbsetTimesinDataInfo(db, tablename,','.join(collst),','.join(unitlst))

    db.commit()
    cursor.close ()


def dbsetTimesinDataInfo(db, tablename,colstr,unitstr):
    """
    DEFINITION:
        Method to update min time and max time variables in DATAINFO table
        using data from table tablename

    PARAMETERS:
        - db:           (mysql database) defined by mysql.connect().
        - tablename:    (string) name of the table
    APPLICATION:
        >>> dbsetTimesDataInfo(db, "MyTable_12345_0001_0001")
    USED BY:
        - writeDB, stream2DB
    """
    cursor = db.cursor ()

    getminmaxtimesql = "Select MIN(time),MAX(time) FROM " + tablename
    cursor.execute(getminmaxtimesql)
    rows = cursor.fetchall()
    #print (rows)
    loggerdatabase.info("stream2DB: Table now covering a time range from " + str(rows[0][0]) + " to " + str(rows[0][1]))
    # removed columncontents and units from update
    updatedatainfotimesql = 'UPDATE DATAINFO SET DataMinTime = "' + rows[0][0] + '", DataMaxTime = "' + rows[0][1] +'", ColumnContents = "' + colstr +'", ColumnUnits = "' + unitstr +'" WHERE DataID = "'+ tablename + '"'
    #print updatedatainfotimesql
    cursor.execute(updatedatainfotimesql)

    db.commit()
    cursor.close ()


def dbupdateDataInfo(db, tablename, header):
    """
    DEFINITION:
        Method to update DATAINFO table with header information
        using data from table tablename

    PARAMETERS:
        - db:           (mysql database) defined by mysql.connect().
        - tablename:    (string) name of the table
    APPLICATION:
        >>> dbupadteDataInfo(db, "MyTable_12345_0001", myheader)
    """
    cursor = db.cursor ()

    # 1. Select all tables matching table name
    searchtables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%{}%'".format(tablename)
    cursor.execute(searchtables)
    try:
        rows = list(cursor.fetchall()[0])
    except:
        print("dbupdateDataInfo: failed")
        return
    for tab in rows:
        # 2. check whether tab exists
        searchdatainfo = "SELECT DataID FROM DATAINFO WHERE DataID LIKE '%{}%'".format(tab)
        cursor.execute(searchdatainfo)
        try:
            res = list(cursor.fetchall()[0])
            exist = True
        except:
            exist = False

        if exist:
            updatelst = []
            for key in header:
                if key in DATAINFOKEYLIST and not key.startswith('Column'):
                    dbupdate(db, 'DATAINFO', [key], [header[key]], condition='DataID="{}"'.format(tab))
        else:
            print("dbupdateDataInfo: insert for non existing table not yet written - TODO") 



def stream2db(db, datastream, noheader=None, mode=None, tablename=None, **kwargs):
    """
    DEFINITION:
        Method to write datastreams to a mysql database

    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - datastream:   (magpy datastream)
    Kwargs:
        - mode:         (string)
                            mode: replace -- replaces existing table contents with new one, also replaces informations from sensors and station table
                            mode: delete -- deletes existing tables and writes new ones -- remove (or make it extremeley difficult to use) this method after initializing of tables
                            mode: extend -- only add new data points with unique ID's (default) - table must exist already !
                            mode: insert -- create new table with unique ID

                    New Header informations are created with modes 'replace' and 'delete'.
                    If mode = 'extend' then check for the existence of sensorid and datainfo first -> if not available then request mode = 'insert'
                    Mode 'extend' requires an existing input in sensor, station and datainfo tables: tablename needs to be given.
                    Mode 'insert' checks for the existance of existing inputs in sensor, station and datainfo, and eventually adds a new datainfo tab.
                    Mode 'replace' checks for the existance of existing inputs in sensor, station and datainfo, and replaces the stored information: optional tablename can be given.
                         if tablename is given, then data from this table is replaced - otherwise only data from station and sensor are replaced
                    Mode 'delete' completely deletes all tables and creates new ones.
                    Mode 'force' does not check sensors and datainfo tabs. Just creates table tablename. All other conditions follow mode 'replace'.
         - clear:       (bool) If true it will delete the selected table before adding new data
    REQUIRES:
        dbdatainfo, dbsensorinfo

    RETURNS:
        --

    EXAMPLE:
        >>> stream.header['StationID'] = 'MyStation'
        >>> stream2db(db,stream)
        dont't use >>> stream2db(db,stream,mode='extend',tablename=datainfoid)
        >>> stream2db(db,stream,mode='replace')
        >>> stream2db(db,stream,mode='force',tablename='myid_0001_0001')

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        so first connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
        stream = read('/home/leon/Dropbox/Daten/Magnetism/DIDD-WIK/raw/*', starttime='2013-01-01',endtime='2013-02-01')
        datainfoid = dbdatainfo(db,stream.header['SensorID'],stream.header)
        stream2db(db,stream)

    TODO:
        - make it possible to create spezial tables by defining an extension (e.g. _sp2013min) where sp indicates special
    """

    # ----------------------------------------------------------------------------
    # -----  Parameter definition and basic vaildity tests  ----------------------
    # ----------------------------------------------------------------------------
    clear = kwargs.get('clear')
    usekeys = kwargs.get('usekeys')

    if not mode:
        mode = 'insert'

    if not db:
        loggerdatabase.error("stream2DB: No database connected - aborting -- please create an empty database first")
        return

    if len(datastream.ndarray[0]) > 0:
        print("stream2DB: Found ndarray data -- running writeDB")
        writeDB(db,datastream,tablename=tablename,mode='replace')
        return


    cursor = db.cursor ()

    headdict = datastream.header
    head, line = [],[]
    sensorhead, sensorvalue = [],[]
    datainfohead, datainfovalue = [],[]
    stationhead, stationvalue = [],[]
    collst, unitlst = [],[]
    datacolumn = []
    datakeys, dataheads = [],[]
    datavals = []

    tmpstream = DataStream() # currently only used for _is_number access as datastream is converted to a list later on

    if usekeys:
        keylst = usekeys
        if not 'flag' in keylst:
            keylst.append('flag')
        if not 'typ' in keylst:
            keylst.append('typ')
    else:
        keylst = KEYLIST

    if not noheader:
        pass

    if len(datastream) < 1:
        loggerdatabase.error("stream2DB: Empty datastream. Aborting ...")
        return

    # Testing whether SensorID is existing
    try:
        if headdict['SensorID'] == '':
             loggerdatabase.error("stream2DB: Please select a suitable SensorID. Aborting ...")
             return
    except KeyError:
        loggerdatabase.error("stream2DB: SensorID not provided within header. Pleased do that by stream.header['SensorID'] = 'MyID' before calling stream2db.  Aborting ...")
        raise


    #print "Starting DB write with some selections...", datetime.utcnow()

    # ----------------------------------------------------------------------------
    # --------------------- Checking header information --------------------------
    # ----------------------------------------------------------------------------

    loggerdatabase.debug("stream2DB: ### Writing stream to database ###")
    loggerdatabase.debug("stream2DB: --- Starting header extraction ...")

    if not mode == 'force':
        # check SENSORS information
        loggerdatabase.debug("stream2DB: --- Checking SENSORS table ...")
        sensorid = dbsensorinfo(db,headdict['SensorID'],headdict)
        loggerdatabase.debug("stream2DB: Working with sensor: %s" % sensorid)
        # updating dict
        headdict['SensorID'] = sensorid
        #print "Test 1 - checked for existing sensorid info in db (if not existing it is created):", sensorid, headdict
        # Header inf has been updated by dbsensorinfo
        # Now get the new info and add it to the existing headdict
        getsensinfo = 'SELECT * FROM SENSORS WHERE SensorID = "'+sensorid+'"'
        cursor.execute(getsensinfo)
        ids = cursor.fetchone()
        #print ids
        for i, el in enumerate(ids):
            if not el == None:
                #print el
                headdict[SENSORSKEYLIST[i]] = el

        #print "Test 1 - continued", headdict

        # HEADER INFO - TABLE
        # read Header information and put it to the respective tables
        for key in headdict:
            if key.startswith('Sensor'):
                if key == "SensorID":
                    #sensorid = headdict[key]
                    sensorhead.append(key)
                    sensorvalue.append(sensorid)
                else:
                    sensorhead.append(key)
                    sensorvalue.append(headdict[key])
            elif key.startswith('Data'):
                if not key == 'DataInterval':
                    datainfohead.append(key)
                    datainfovalue.append(headdict[key])
            elif key.startswith('Station'):
                if key == "StationID":
                    stationid = headdict[key]
                    stationhead.append(key)
                else:
                    if not key == "Station":
                        stationhead.append(key)
                if not key == "Station":
                    stationvalue.append(str(headdict[key]).replace('http://',''))
            elif key.startswith('col'):
                pass
            elif key.startswith('unit'):
                pass
            else:
                #key not transferred to DB
                loggerdatabase.debug("stream2DB: --- unkown key: %s, %s" % (key, headdict[key]))

        # If no sensorid is available then report error and return:
        try:
            loggerdatabase.info("stream2DB: --- SensorID = %s" % sensorid)
        except:
            loggerdatabase.error("stream2DB:  --- stream2DB: no SensorID specified in stream header. Cannot proceed ...")
            return
        try:
            loggerdatabase.info("stream2DB: --- StationID = %s" % stationid)
        except:
            loggerdatabase.error("stream2DB: --- stream2DB: no StationID specified in stream header. To define use mystream.header['StationID'] = 'MyStationCode'. Cannot proceed ...")
            return

        # If no sensorid is available then report error and return:
        try:
            #print "Trying sampling rate"
            sr = datastream.header['DataSamplingRate']
            loggerdatabase.info("stream2DB: --- DataSamplingRate = %s" % sr)
        except:
            #print "Setting sampling rate"
            #datastream.header['DataSamplingRate'] = str(dbsamplingrate(datastream))+' sec'
            datastream.header['DataSamplingRate'] = datastream.samplingrate()
            sr = datastream.header['DataSamplingRate']
            loggerdatabase.info("stream2DB: --- DataSamplingRate = %s" % sr)

    #print "stream2db2: ", datetime.utcnow()


    # Section 2:
    # ---------    Checking datastream structure -----------------------------
    # ------------------------------------------------------------------------
    #   --- providing datakeys, colstr and unitstr, st and et
    #print "Checking column contents...", datetime.utcnow()

    loggerdatabase.debug("stream2DB: --- Checking column contents ...")
    # HEADER INFO - DATA TABLE
    # select only columss which contain data and get units and column contents

    # Alternative
    keylst = datastream._get_key_headers()
    if not 'flag' in keylst:
        keylst.append('flag')
    if not 'comment' in keylst:
        keylst.append('comment')
    if not 'typ' in keylst:
        keylst.append('typ')

    for key in KEYLIST:
        colstr = ''
        unitstr = ''
        if key in keylst:
            if key in NUMKEYLIST:
                dataheads.append(key + ' FLOAT')
                datakeys.append(key)
            else:
                dataheads.append(key + ' CHAR(100)')
                datakeys.append(key)
            for hkey in headdict:
                if key == hkey.replace('col-',''):
                    colstr = headdict[hkey]
                elif key == hkey.replace('unit-col-',''):
                    unitstr = headdict[hkey]
        if not key == 'time':
            collst.append(colstr)
            unitlst.append(unitstr)

    """
    for key in KEYLIST:
       colstr = '-'
       unitstr = '-'
       if not key.endswith('time') and key in keylst:
           ind = KEYLIST.index(key)
           testlst = [row[ind] for row in datastream]
           try:
               tester = np.array(testlst)
               tester = tester[~isnan(tester)]
               if len(tester)>0:
                   if datastream._is_number(testval):
                       #print "Number"
                       dataheads.append(key + ' FLOAT')
                       datakeys.append(key)
           except:
               #print "String"
               dataheads.append(key + ' CHAR(100)')
               datakeys.append(key)
           #print "stream2db2c: ", key, datetime.utcnow()
       for hkey in headdict:
           if key == hkey.replace('col-',''):
               colstr = headdict[hkey]
           elif key == hkey.replace('unit-col-',''):
               unitstr = headdict[hkey]
       collst.append(colstr)
       unitlst.append(unitstr)
    """

    colstr =  ','.join(collst)
    unitstr = ','.join(unitlst)

    # Update the column data at the end together with time
    #print "stream2db3: ", datetime.utcnow(), datakeys

    st = datetime.strftime(num2date(datastream[0].time).replace(tzinfo=None),'%Y-%m-%d %H:%M:%S.%f')
    et = datetime.strftime(num2date(datastream[-1].time).replace(tzinfo=None),'%Y-%m-%d %H:%M:%S.%f')

    # Test whether DATAINFO table is existing - if not abort as an initialization seems to be required
    getdatainfo = 'SHOW TABLES LIKE "DATAINFO"'
    try:
        cursor.execute(getdatainfo)
        rows = cursor.fetchall()
        if not len(rows) > 0:
            loggerdatabase.error("stream2DB: DATAINFO table not found - use dbinit() first")
            return
    except:
        loggerdatabase.error("stream2DB: DATAINFO table error - use dbinit() first")
        raise

    # ----------------------------------------------------------------------------
    # --------------------- Updating existing tables    --------------------------
    # ----------------------------------------------------------------------------
    #print "Starting update...", datetime.utcnow()

    if not mode == 'force':
        loggerdatabase.debug("stream2DB: --- Checking/Updating existing tables ...")

        if mode=='extend':
            if not tablename:
                loggerdatabase.error("stream2DB: tablename must be specified for mode 'extend'" )
                return
            # Check for the existance of data base contents and sufficient header information
            searchstation = 'SELECT * FROM STATIONS WHERE StationID = "'+stationid+'"'
            searchsensor = 'SELECT * FROM SENSORS WHERE SensorID = "'+sensorid+'"'
            searchdatainfo = 'SHOW TABLES LIKE "'+tablename+'"'

            try:
                cursor.execute(searchstation)
            except:
                loggerdatabase.error("stream2DB: Station table not existing - use mode 'insert' - aborting with error")
                raise

            rows = cursor.fetchall()
            if not len(rows) > 0:
                loggerdatabase.error("stream2DB: Station is not yet existing - use mode 'insert' - aborting with error")
                raise
                #return

            cursor.execute(searchsensor)
            rows = cursor.fetchall()
            if not len(rows) > 0:
                loggerdatabase.error("stream2DB: Sensor is not yet existing - use mode 'insert'")
                raise
            cursor.execute(searchdatainfo)
            rows = cursor.fetchall()
            if not len(rows) > 0:
                loggerdatabase.error("stream2DB: Selected data table is not yet existing - check tablename")
                raise
        else:
            # SENSOR TABLE
            # Create sensor table input
            headstr = ' CHAR(100), '.join(SENSORSKEYLIST) + ' CHAR(100)'
            headstr = headstr.replace('SensorID CHAR(100)', 'SensorID CHAR(50) NOT NULL PRIMARY KEY')
            headstr = headstr.replace('SensorDescription CHAR(100)', 'SensorDescription TEXT')
            createsensortablesql = "CREATE TABLE IF NOT EXISTS SENSORS (%s)" % headstr
            sensorsql = "INSERT INTO SENSORS(%s) VALUES (%s)" % (', '.join(sensorhead), '"'+'", "'.join(sensorvalue)+'"')

            # STATION TABLE
            # Create station table input
            stationstr = ' CHAR(100), '.join(STATIONSKEYLIST) + ' CHAR(100)'
            stationstr = stationstr.replace('StationID CHAR(100)', 'StationID CHAR(50) NOT NULL PRIMARY KEY')
            stationstr = stationstr.replace('StationDescription CHAR(100)', 'StationDescription TEXT')
            stationstr = 'StationID CHAR(50) NOT NULL PRIMARY KEY, StationName CHAR(100), StationIAGAcode CHAR(10), StationInstitution CHAR(100), StationStreet CHAR(50), StationCity CHAR(50), StationPostalCode CHAR(20), StationCountry CHAR(50), StationWebInfo CHAR(100), StationEmail CHAR(100), StationDescription TEXT'
            createstationtablesql = "CREATE TABLE IF NOT EXISTS STATIONS (%s)" % stationstr
            stationsql = "INSERT INTO STATIONS(%s) VALUES (%s)" % (', '.join(stationhead), '"'+'", "'.join(stationvalue)+'"')

            # DATAINFO TABLE
            # Create datainfo table
            if not len(DATAINFOKEYLIST) == len(DATAVALUEKEYLIST):
                loggerdatabase.error("CHECK your DATA KEYLISTS")
                return
            FULLDATAKEYLIST = []
            for i, elem in enumerate(DATAINFOKEYLIST):
                newelem = elem + ' ' + DATAVALUEKEYLIST[i]
                FULLDATAKEYLIST.append(newelem)
            datainfostr = ', '.join(FULLDATAKEYLIST)
            createdatainfotablesql = "CREATE TABLE IF NOT EXISTS DATAINFO (%s)" % datainfostr

            if mode == "delete":
                cursor.execute("DROP TABLE IF EXISTS SENSORS")
                cursor.execute("DROP TABLE IF EXISTS STATIONS")
                cursor.execute("DROP TABLE IF EXISTS DATAINFO")

            cursor.execute(createsensortablesql)
            cursor.execute(createstationtablesql)
            cursor.execute(createdatainfotablesql)

            if mode == "replace":
                try:
                    cursor.execute(sensorsql.replace("INSERT","REPLACE"))
                    cursor.execute(stationsql.replace("INSERT","REPLACE"))
                except:
                    loggerdatabase.warning("stream2DB: Write MySQL: Replace failed")
            else:
                try:
                    loggerdatabase.debug("stream2DB: executing: %s" % sensorsql)
                    cursor.execute(sensorsql)
                except:
                    loggerdatabase.warning("stream2DB: Sensor data already existing: use mode 'replace' to overwrite")
                    loggerdatabase.warning("stream2DB: Perhaps this field does not exist")
                    pass
                try:
                    cursor.execute(stationsql)
                except:
                    loggerdatabase.warning("stream2DB: Station data already existing: use mode 'replace' to overwrite")
                    loggerdatabase.warning("stream2DB: Perhaps this field does not exist")
                    pass

            # DATAINFO TABLE
            # check whether contents exists

            #print "Test 2 - what about sensor id now?? (headdict should contain revision!!):", sensorid, headdict

            tablename = dbdatainfo(db,sensorid,headdict,None,stationid)

            #print "Test 3 - what now??:", sensorid, tablename

    #print "stream2db4: ", datetime.utcnow()

    if not tablename:
        loggerdatabase.error("stream2DB: No Tablename specified")
        return

    if clear:
        cursor.execute("DROP TABLE IF EXISTS " + tablename)

    #print "Creating data table...", datetime.utcnow()

    loggerdatabase.info("stream2DB: Creating/updating data table " + tablename)

    # Checking validity of secondary time column (if given)
    try:
        sectimevalidity = True
        test=datetime.strftime(num2date(datastream[0].sectime).replace(tzinfo=None),'%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        loggerdatabase.warning("stream2DB: Found secondary time column but cannot interpret it! ")
        sectimevalidity = False

    if not isnan(datastream[0].sectime) and datastream._is_number(datastream[0].sectime) and sectimevalidity:
        createdatatablesql = "CREATE TABLE IF NOT EXISTS %s (time CHAR(40) NOT NULL PRIMARY KEY, sectime CHAR(40),  %s)" % (tablename,', '.join(dataheads))
        dollarstring = ['%s' for amount in range(len(datakeys)+2)]
        insertmanysql = "INSERT INTO %s(time, sectime, %s) VALUES (%s)" % (tablename, ', '.join(datakeys), ', '.join(dollarstring))
    else:
        createdatatablesql = "CREATE TABLE IF NOT EXISTS %s (time CHAR(40) NOT NULL PRIMARY KEY, %s)" % (tablename,', '.join(dataheads))
        dollarstring = ['%s' for amount in range(len(datakeys)+1)]
        insertmanysql = "INSERT INTO %s(time, %s) VALUES (%s)" % (tablename, ', '.join(datakeys), ', '.join(dollarstring))

    cursor.execute(createdatatablesql)

    values = []

    # Drop nan lines
    datastream = [x for x in datastream if not isnan(x.time)]
    #print "Datastream: ", datastream

    #print "Uploading data to database now...", datetime.utcnow()

    #print datakeys
    #print datastream[0]
    loggerdatabase.info("stream2db: now adding data to DB")
    for elem in datastream:
        datavals  = []
        #datavals = ['null' if str(getattr(elem,el))=='nan' else str(getattr(elem,el)) for el in datakeys]
        for el in datakeys:
            val = str(getattr(elem,el))
            if val=='nan':
                val = 'null'
            datavals.append(val)

        ## All time steps are rounded to millisecond precision for database
        ## ################################################################
        normaltime = num2date(elem.time)
        ms = int(round(normaltime.microsecond/1000.)*1000.)
        if ms < 1000000:
            ct = datetime.strftime(normaltime.replace(microsecond=ms).replace(tzinfo=None),'%Y-%m-%d %H:%M:%S.%f')
        else:
            ct = datetime.strftime(normaltime.replace(microsecond=0).replace(tzinfo=None)+timedelta(seconds=1),'%Y-%m-%d %H:%M:%S.%f')
        # Take the insertstring creation out of loop
        if not isnan(elem.sectime) and tmpstream._is_number(elem.sectime) and sectimevalidity:
            furthertime = num2date(elem.sectime)
            fms = int(round(furthertime.microsecond/1000.)*1000.)
            if fms < 1000000:
                cst = datetime.strftime(furthertime.replace(microsecond=fms).replace(tzinfo=None),'%Y-%m-%d %H:%M:%S.%f')
            else:
                cst = datetime.strftime(furthertime.replace(microsecond=0).replace(tzinfo=None)+timedelta(seconds=1),'%Y-%m-%d %H:%M:%S.%f')
            lst = [ct, cst]
        else:
            lst = [ct]

        values.append(tuple(lst+datavals))
        #print values
        #datavals  = []


    #print "Finally inserting data ...", datetime.utcnow()

    if mode == "replace" or mode == "force":
        try:
            insertsql = insertmanysql
            insertsql = insertsql.replace("INSERT","REPLACE")
            cursor.executemany(insertsql,values)
        except mysql.Error as e:
            loggerdatabase.error("stream2db: mysqlerror while replacing data: %s" % (e))
        except:
            try:
                cursor.executemany(insertmanysql,values)
            except:
                loggerdatabase.warning("stream2DB: Write MySQL: Replace failed")
    else:
        try:
            #print "Got here"
            cursor.executemany(insertmanysql,values)
        except:
            loggerdatabase.debug("stream2DB: Record at %s already existing: use mode replace to overwrite" % ct)


    #print "stream2db5: ", datetime.utcnow()

    # Select MinTime and MaxTime from datatable and eventually update datainfo
    getminmaxtimesql = "Select MIN(time),MAX(time) FROM " + tablename
    cursor.execute(getminmaxtimesql)
    rows = cursor.fetchall()
    #print rows
    loggerdatabase.info("stream2DB: Table now covering a time range from " + str(rows[0][0]) + " to " + str(rows[0][1]))
    updatedatainfotimesql = 'UPDATE DATAINFO SET DataMinTime = "' + rows[0][0] + '", DataMaxTime = "' + rows[0][1] +'", ColumnContents = "' + colstr +'", ColumnUnits = "' + unitstr +'" WHERE DataID = "'+ tablename + '"'
    #print updatedatainfotimesql
    cursor.execute(updatedatainfotimesql)

    db.commit()
    cursor.close ()

    #print "stream2db6: ", datetime.utcnow()

def readDB(db, table, starttime=None, endtime=None, sql=None):
    """
    sql: provide any additional search criteria
        example: sql = "DataSamplingRate=60 AND DataType='variation'"
    DEFINITION:
        extract data streams from the data base

    PARAMETERS:
    Variables:
        - db:               (mysql database) defined by mysql.connect().
        - table:            (string) tablename or sensorID -> for sensor ID
                                     lowest revision is selected
        - sql:              (string) provide any additional search criteria
                                  example: sql = "x>20000 AND str1='P'"
    Kwargs:

    RETURNS:
        data stream

    EXAMPLE:
        >>> db2stream(db,None,None,None,'DIDD_3121331_0002_0001')

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        so first connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")

    TODO:
        - If sampling rate not given in DATAINFO get it from the datastream
        - begin needs to be string - generalize that
    """
    wherelist = []
    stream = DataStream()

    if not db:
        loggerdatabase.error("DB2stream: No database connected - aborting")
        return stream

    cursor = db.cursor ()

    if not table:
        loggerdatabase.error("DB2stream: Aborting ... either sensorid or table must be specified")
        return
    if starttime:
        #starttime = stream._testtime(begin)
        begin = datetime.strftime(stream._testtime(starttime),"%Y-%m-%d %H:%M:%S")
        wherelist.append('time >= "' + begin + '"')
    if endtime:
        end = datetime.strftime(stream._testtime(endtime),"%Y-%m-%d %H:%M:%S")
        wherelist.append('time <= "' + end + '"')
    if len(wherelist) > 0:
        whereclause = ' AND '.join(wherelist)
    else:
        whereclause = ''
    if sql:
        if len(whereclause) > 0:
            whereclause = whereclause + ' AND ' + sql
        else:
            whereclause = sql

    # 1. Try to locate data table with name 'table'
    # --------------------------------------------
    getcols = 'SHOW COLUMNS FROM ' + table
    try:
        # Table exists - read it
        cursor.execute(getcols)
        rows = cursor.fetchall()
        keys = [el[0] for el in rows]
    except mysql.Error as e:
        # Table does not exist - assume sensor id
        getdatainfo = 'SELECT DataID FROM DATAINFO WHERE SensorID = "' + table + '"'
        cursor.execute(getdatainfo)
        rows = cursor.fetchall()
        if not len(rows) > 0:
            print("No data found - aborting")
            return stream
        rows = sorted(rows)
        #for tab in rows:
        #    revision = tab[0].replace(table,'').strip('_')
        table = rows[0][0]
        print("Did not find specific DataID - opening table:", table)
        try:
            cursor.execute('SHOW COLUMNS FROM ' + table)
            rows = cursor.fetchall()
            keys = [el[0] for el in rows]
        except:
            loggerdatabase.error("readDB: mysqlerror while identifying table: %s" % (e))
            return stream
    except:
        loggerdatabase.error("readDB: mysqlerror while getting table info: %s" % (e))
        return stream


    def checkEqual3(lst):
        return lst[1:] == lst[:-1]

    if len(keys) > 0:
        if len(whereclause) > 0:
            getdatasql = 'SELECT ' + ','.join(keys) + ' FROM ' + table + ' WHERE ' + whereclause
        else:
            getdatasql = 'SELECT ' + ','.join(keys) + ' FROM ' + table
        #print getdatasql
        cursor.execute(getdatasql)
        rows = list(cursor.fetchall())
        print ("readDB: Read rows: {}".format(len(rows)))
        ls = []
        for i in range(len(KEYLIST)):
            ls.append([])

        if len(rows) > 0:
            for ind, line in enumerate(rows):
                for i, elem in enumerate(line):
                    index = KEYLIST.index(keys[i])
                    if keys[i][-4:]=='time':
                        try:
                            ls[index].append(date2num(stream._testtime(elem)))
                        except:
                            if ind == 0:
                                print ("readDB: could not identify time! Column {a} contains {b}, which cannot be interpreted as time by the testtime method".format(a=keys[i],b=elem) )
                            pass
                    else:
                        if keys[i] in NUMKEYLIST:
                            if elem == None or elem == 'null':
                                elem = float(NaN)
                            ls[index].append(float(elem))
                        else:
                            if elem == None or elem == 'null':
                                elem = ''
                            ls[index].append(elem)

            for idx, elem in enumerate(ls):
                ls[idx] = np.asarray(elem)

            for key in keys:
                #print "Reformating key", key
                index = KEYLIST.index(key)
                col = np.asarray(ls[index])
                if not False in checkEqual3(col):
                    print ("readDB: Found identical values only:{}".format(key))
                    #try:
                    if str(col[0]) == '' or str(col[0]) == '-' or str(col[0]).find('0000000000000000') or str(col[0]).find('xyz'):
                        ls[index] = np.asarray([])
                    else:
                        ls[index] = col[:1]
                if key in NUMKEYLIST:
                    ls[index] = np.asarray(col).astype('<f8')

            stream.header = dbfields2dict(db,table)
        else:
            print ("No data found")
            pass

    stream.ndarray = np.asarray(ls, dtype=object)

    cursor.close ()
    return DataStream([LineStruct],stream.header,stream.ndarray)


def db2stream(db, sensorid=None, begin=None, end=None, tableext=None, sql=None):
    """
    sql: provide any additional search criteria
        example: sql = "DataSamplingRate=60 AND DataType='variation'"
    DEFINITION:
        extract data streams from the data base

    PARAMETERS:
    Variables:
        - db:               (mysql database) defined by mysql.connect().
        - sensorid:       (string) table and dataid
        - sql:      (string) provide any additional search criteria
                             example: sql = "DataSamplingRate=60 AND DataType='variation'"
        - datainfoid:       (string) table and dataid
    Kwargs:

    RETURNS:
        data stream

    EXAMPLE:
        >>> db2stream(db,None,None,None,'DIDD_3121331_0002_0001')

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        so first connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")

    TODO:
        - If sampling rate not given in DATAINFO get it from the datastream
        - begin needs to be string - generalize that
    """
    wherelist = []
    stream = DataStream()

    if not db:
        loggerdatabase.error("DB2stream: No database connected - aborting")
        return stream

    cursor = db.cursor ()

    if not tableext and not sensorid:
        loggerdatabase.error("DB2stream: Aborting ... either sensorid or table must be specified")
        return
    if begin:
        #starttime = stream._testtime(begin)
        #begin = datetime.strftime(starttime,"%Y-%m%d %H:%M%:%S")
        wherelist.append('time >= "' + begin + '"')
    if end:
        wherelist.append('time <= "' + end + '"')
    if len(wherelist) > 0:
        whereclause = ' AND '.join(wherelist)
    else:
        whereclause = ''
    if sql:
        if len(whereclause) > 0:
            whereclause = whereclause + ' AND ' + sql
        else:
            whereclause = sql

    #print whereclause

    if not tableext:
        getdatainfo = 'SELECT DataID FROM DATAINFO WHERE SensorID = "' + sensorid + '"'
        cursor.execute(getdatainfo)
        rows = cursor.fetchall()
        for table in rows:
            revision = table[0].replace(sensorid,'').strip('_')
            loggerdatabase.debug("DB2stream: Extracting field values from table %s" % str(table[0]))
            if len(whereclause) > 0:
                getdatasql = 'SELECT * FROM ' + table[0] + ' WHERE ' + whereclause
            else:
                getdatasql = 'SELECT * FROM ' + table[0]
            getcolumnnames = 'SHOW COLUMNS FROM ' + table[0]
            # sqlquery to get column names of table - store that in keylst
            keylst = []
            cursor.execute(getcolumnnames)
            rows = cursor.fetchall()
            for line in rows:
                keylst.append(line[0])
            # sqlquery to extract data
            cursor.execute(getdatasql)
            rows = cursor.fetchall()
            if len(rows) > 0:
                for line in rows:
                    row = LineStruct()
                    for i, elem in enumerate(line):
                        if keylst[i]=='time':
                            setattr(row,keylst[i],date2num(stream._testtime(elem)))
                        else:
                            if elem == None or elem == 'null':
                                elem = float(NaN)
                            if keylst[i] in NUMKEYLIST:
                                setattr(row,keylst[i],float(elem))
                            else:
                                setattr(row,keylst[i],elem)
                    stream.add(row)
                #print "Loaded data from table", table[0]
                stream.header = dbfields2dict(db,table[0])
                break
    else:
        if len(whereclause) > 0:
            getdatasql = 'SELECT * FROM ' + tableext + ' WHERE ' + whereclause
        else:
            getdatasql = 'SELECT * FROM ' + tableext
        getcolumnnames = 'SHOW COLUMNS FROM ' + tableext
        # sqlquery to get column names of table - store that in keylst
        keylst = []
        cursor.execute(getcolumnnames)
        rows = cursor.fetchall()
        for line in rows:
            keylst.append(line[0])
        # sqlquery to extract data
        cursor.execute(getdatasql)
        rows = cursor.fetchall()
        for line in rows:
            row = LineStruct()
            for i, elem in enumerate(line):
                if keylst[i]=='time':
                    setattr(row,keylst[i],date2num(stream._testtime(elem)))
                else:
                    if elem == None or elem == 'null':
                        elem = float(NaN)
                    if keylst[i] in NUMKEYLIST:
                        setattr(row,keylst[i],float(elem))
                    else:
                        setattr(row,keylst[i],elem)
            stream.add(row)

    if tableext:
        stream.header = dbfields2dict(db,tableext)

    cursor.close ()
    return stream

def diline2db(db, dilinestruct, mode=None, **kwargs):
    """
    DEFINITION:
        Method to write dilinestruct to a mysql database

    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
        - dilinestruct: (magpy diline)
    Optional:
        - mode:         (string) - default is insert
                            mode: replace -- replaces existing table contents with new one, also replaces informations from sensors and station table
                            mode: insert -- create new table with unique ID
                            mode: delete -- like insert but drops the existing DIDATA table
    kwargs:
        - tablename:    (string) - specify tablename of the DI table (default is DIDATA)

    EXAMPLE:
        >>> diline2db(db,...)

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        so first connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
    """

    tablename = kwargs.get('tablename')

    loggerdatabase.debug("diline2DB: ### Writing DI values to database ###")

    if len(dilinestruct) < 1:
        loggerdatabase.error("diline2DB: Empty diline. Aborting ...")
        return

    # 1. Create the diline table if not existing
    # - DIDATA
    DIDATAKEYLIST = ['DIID','StartTime','TimeArray','HcArray','VcArray','ResArray','OptArray','LaserArray','FTimeArray',
                                 'FArray','Temperature','ScalevalueFluxgate','ScaleAngle','Azimuth','Pier','Observer','DIInst',
                                 'FInst','FluxInst','InputDate','DIComment']

    # DIDATA TABLE
    if not tablename:
        tablename = 'DIDATA'

    cursor = db.cursor ()
    cursor._defer_warnings = True

    headstr = ' CHAR(100), '.join(DIDATAKEYLIST) + ' CHAR(100)'
    headstr = headstr.replace('DIID CHAR(100)', 'DIID CHAR(100) NOT NULL PRIMARY KEY')
    headstr = headstr.replace('DIComment CHAR(100)', 'DIComment TEXT')
    headstr = headstr.replace('Array CHAR(100)', 'Array TEXT')
    createDItablesql = "CREATE TABLE IF NOT EXISTS %s (%s)" % (tablename,headstr)

    if mode == 'delete':
        try:
            cursor.execute("DROP TABLE IF EXISTS %s" % tablename)
        except:
            loggerdatabase.info("diline2DB: DIDATA table not yet existing")
        loggerdatabase.info("diline2DB: Old DIDATA table has been deleted")

    try:
        cursor.execute(createDItablesql)
        loggerdatabase.info("diline2DB: New DIDATA table created")
    except:
        loggerdatabase.debug("diline2DB: DIDATA table already existing ?? TODO: -- check the validity of this message --")

    # 2. Add DI values to the table
    #   Cycle through all lines of the dilinestruct
    #   - a) convert arrays to underscore separated text like 'nan,nan,765,7656,879.6765,nan"

    for line in dilinestruct:
        insertlst = []
        insertlst.append(str(line.pier)+'_'+datetime.strftime(num2date(line.time[4]),"%Y-%m-%d %H:%M:%S"))
        insertlst.append(datetime.strftime(num2date(line.time[4]),"%Y-%m-%d %H:%M:%S"))
        strlist = [str(elem) for elem in line.time]
        timearray = '%s' % ('_'.join(strlist))
        insertlst.append('%s' % ('_'.join(strlist)))
        strlist = [str(elem) for elem in line.hc]
        hcarray = '%s' % ('_'.join(strlist))
        insertlst.append('%s' % ('_'.join(strlist)))
        strlist = [str(elem) for elem in line.vc]
        vcarray = '%s' % ('_'.join(strlist))
        insertlst.append('%s' % ('_'.join(strlist)))
        strlist = [str(elem) for elem in line.res]
        resarray = '%s' % ('_'.join(strlist))
        insertlst.append('%s' % ('_'.join(strlist)))
        strlist = [str(elem) for elem in line.opt]
        optarray = '%s' % ('_'.join(strlist))
        insertlst.append('%s' % ('_'.join(strlist)))
        strlist = [str(elem) for elem in line.laser]
        laserarray = '%s' % ('_'.join(strlist))
        insertlst.append('%s' % ('_'.join(strlist)))
        strlist = [str(elem) for elem in line.ftime]
        ftimearray = '%s' % ('_'.join(strlist))
        insertlst.append('%s' % ('_'.join(strlist)))
        strlist = [str(elem) for elem in line.f]
        farray = '%s' % ('_'.join(strlist))
        insertlst.append('%s' % ('_'.join(strlist)))
        insertlst.append(str(line.t))
        insertlst.append(str(line.scaleflux))
        insertlst.append(str(line.scaleangle))
        insertlst.append(str(line.azimuth))
        insertlst.append(str(line.pier))
        insertlst.append(str(line.person))
        insertlst.append(str(line.di_inst))
        insertlst.append(str(line.f_inst))
        insertlst.append(str(line.fluxgatesensor))
        insertlst.append(str(line.inputdate))
        insertlst.append("No comment")

        disql = "INSERT INTO %s(%s) VALUES (%s)" % (tablename,', '.join(DIDATAKEYLIST), '"'+'", "'.join(insertlst)+'"')
        if mode == "replace":
            disql = disql.replace("INSERT","REPLACE")

        try:
            cursor.execute(disql)
        except:
            loggerdatabase.debug("diline2DB: data already existing")

    db.commit()
    cursor.close ()

def db2diline(db,**kwargs):
    """
    DEFINITION:
        Method to read DI values from a database and write them to a list of DILineStruct's

    PARAMETERS:
    Variables:
        - db:           (mysql database) defined by mysql.connect().
    kwargs:
        - starttime:    (string/datetime) - time range to select
        - endtime:      (string/datetime) -
        - sql:          (string) - define any additional selection criteria (e.g. "Pier = 'A2'" AND "Observer = 'Mickey Mouse'")
                                important: dont forget the ' '
        - tablename:    (string) - specify tablename of the DI table (default is DIDATA)

    EXAMPLE:
        >>> resultlist = db2diline(db,starttime="2013-01-01",sql="Pier='A2'")

    APPLICATION:
        Requires an existing mysql database (e.g. mydb)
        so first connect to the database
        db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")

    RETURNS:
        list of DILineStruct elements
    """

    # importing absolute classes again....
    try:
        from magpy.absolutes import DILineStruct
    except:
        from magpy.absolutes import DILineStruct

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    sql = kwargs.get('sql')
    tablename = kwargs.get('tablename')

    stream = DataStream() # to access some time functions from the stream package

    if not db:
        loggerdatabase.error("DB2diline: No database connected - aborting")
        return didata

    cursor = db.cursor ()

    resultlist = []

    if not tablename:
        tablename = 'DIDATA_WIC'

    whereclause = ''
    if starttime:
        starttime = stream._testtime(starttime)
        print(starttime)
        st = datetime.strftime(starttime, "%Y-%m-%d %H:%M:%S")
        whereclause += "StartTime >= '%s' " % st
    if endtime:
        if len(whereclause) > 1:
            whereclause += "AND "
        endtime = stream._testtime(endtime)
        et = datetime.strftime(endtime, "%Y-%m-%d %H:%M:%S")
        whereclause += "StartTime <= '%s' " % et
    if sql:
        if len(whereclause) > 1:
            whereclause += "AND "
        whereclause += sql

    if len(whereclause) > 0:
        getdidata = 'SELECT * FROM ' + tablename + ' WHERE ' + whereclause
    else:
        getdidata = 'SELECT * FROM ' + tablename

    #print "Where: ", whereclause
    cursor.execute(getdidata)
    rows = cursor.fetchall()
    for di in rows:
        loggerdatabase.debug("DB2stream: Extracting DI values for %s" % str(di[1]))
        # Zerlege time column
        timelst = [float(elem) for elem in di[2].split('_')]
        distruct = DILineStruct(len(timelst))
        distruct.time = timelst
        distruct.hc = [float(elem) for elem in di[3].split('_')]
        distruct.vc = [float(elem) for elem in di[4].split('_')]
        distruct.res = [float(elem) for elem in di[5].split('_') if len(di[5].split('_')) > 1]
        distruct.opt = [float(elem) for elem in di[6].split('_') if len(di[6].split('_')) > 1]
        distruct.laser = [float(elem) for elem in di[7].split('_') if len(di[7].split('_')) > 1]
        distruct.ftime = [float(elem) for elem in di[8].split('_') if len(di[8].split('_')) > 1]
        distruct.f = [float(elem) for elem in di[9].split('_') if len(di[9].split('_')) > 1]
        distruct.t = di[10]
        distruct.scaleflux =  di[11]
        distruct.scaleangle =  di[12]
        distruct.azimuth =  di[13]
        distruct.person =  di[14]
        distruct.pier =  di[15]
        distruct.di_inst =  di[16]
        distruct.f_inst =  di[17]
        distruct.fluxgatesensor =  di[18]
        distruct.inputdate =  stream._testtime(di[19])
        resultlist.append(distruct)

    return resultlist

def applyDeltas(db, stream):
    """
    DESCRIPTION:
       Extract content of DataDeltaDictionary and apply the corrections to stream.

    PARAMETER:
       db:              name of the mysql data base
       stream:          data stream which is corrected

    RETURNS:
       a data stream with offsets applied
    """
    dataid = stream.header.get('DataID','')
    if dataid == '':
        print ("applyDeltas: No dataid found in streams header - aborting")
        return stream

    deltas = stream.header.get('DataDeltaValues','')
    if deltas == '':
        print ("applyDeltas: No delta values found - returning unmodified stream")
        return stream

    try:
        deltas = deltas.split(',')
        for el in deltas:
            dat = el.split('_')
            print ("Correcting {a} offset: {b}".format(a=dat[0],b=dat[1]))
            if dat[0] == 'time':
                stream = stream.offset({'time': dat[1]})
            if dat[0] in NUMKEYLIST:
                stream = stream.offset({dat[0]: float(dat[1])})
        stream.header['DataDeltaValues'] = ''
    except:
        print("Could not extract delta values for ", dataid)

    return stream


def getBaseline(db,sensorid, date=None):
    """
    DESCRIPTION:
       Method to extract a list of baseline fitting data from db.

    PARAMETER:
       db:              name of the mysql data base
       sensorid:        identification id of sensor in database
       date:            if provided only the line matching the given date will be returned

    RETURNS:
       a list with all selected baseline data
    """

    if not date:
        where = 'SensorID LIKE "%'+sensorid+'%"'
        print(where)
        vals = dbselect(db,'*','BASELINE', where)
        vals = np.asarray(vals).transpose()
    else:
        tmp = DataStream()
        where = 'SensorID LIKE "%{a}%" AND "{b}" >= MinTime'.format(a=sensorid,b=datetime.strftime(tmp._testtime(date),"%Y-%m-%d %H:%M:%S"))
        vals = dbselect(db,'*','BASELINE', where)
        vals = [elem for elem in vals if elem[2]=='' or tmp._testtime(elem[2]) >= tmp._testtime(date)]
        vals = np.asarray(vals).transpose()

    for i,elem in enumerate(vals):
        if i ==2:
            for j, el in enumerate(elem):
                if el == '':
                    vals[i][j] = datetime.strftime(datetime.utcnow(),"%Y-%m-%d %H:%M:%S")

    # Fallback
    if not len(vals) > 0:
        print ("getBaseline: Did not find baseline parameters matching search criteria - returning dummy values for spline fit")
        now = datetime.strftime(datetime.utcnow(),"%Y-%m-%d %H:%M:%S")
        past = datetime.strftime(datetime.utcnow()-timedelta(days=365),"%Y-%m-%d %H:%M:%S")
        vals = [[sensorid],[past],[now],['spline'],[1],[0.1],['Dummy']]
    return vals


def flaglist2db(db,flaglist,mode=None,sensorid=None,modificationdate=None):
    """
    DESCRIPTION:
       Function to converts a python list (actually an array)
       within flagging information to a data base table
       Flag Table looks like:
          data base format: flagID, sensorID, starttime, endtime, 
                components, flagNum, flagReason, ModificationDate

    PARAMETER:
       db: name of the mysql data base
       flaglist: the list containing flagging information of format:
                [[starttime, endtime, singlecomp, flagNum, flagReason, (SensorID, ModificationDate)],...]

    Optional:
       mode: default inserts information if not existing
             use 'delete' to delete any existing input for the given sensorid
       sensorid: a string with the sensor id, if not provided within the list
       modificationdate: a string with the flagging modificationdate, 
                         if not provided within the list

    APPLICATION:
       flaglist2db(db, flaglist, sensorid='MySensor')

    """
    if not sensorid:
        sensorid = 'defaultsensor'
    if not modificationdate:
        modificationdate = datetime.strftime(datetime.utcnow(),"%Y-%m-%d %H:%M:%S.%f")

    if not db:
        print("No database connected - aborting")
        return
    cursor = db.cursor ()

    # Check flaglist:
    if not len(flaglist) > 0:
        if mode == 'delete' and sensorid:
            print("Executing: DELETE FROM FLAGS WHERE SensorID LIKE '{}'".format(sensorid))
            cursor.execute("DELETE FROM FLAGS WHERE SensorID LIKE '{}'".format(sensorid))
            db.commit()
            cursor.close ()
            return
        else:
            print("No data found in flaglist - aborting")
            return

    lentype = len(flaglist[0])

    if lentype <= 5:
        listwithoutcomp = [[elem[0],elem[1],elem[3],elem[4]] for elem in flaglist]
    else:
        listwithoutcomp = [[elem[0],elem[1],elem[3],elem[4],elem[5],elem[6]] for elem in flaglist]
        if sensorid == 'defaultsensor':
            sensorid = listwithoutcomp[-1][4]
    newlst = []
    for elem in listwithoutcomp:
        elem[0] = datetime.strftime(elem[0],"%Y-%m-%d %H:%M:%S.%f")
        elem[1] = datetime.strftime(elem[1],"%Y-%m-%d %H:%M:%S.%f")
        if elem not in newlst:
            newlst.append(elem)

    # Check whether an identical input already exists in the database
    existinglst = db2flaglist(db,sensorid)

    newflaglist = []
    for flag in flaglist:
        itexists = False
        ti0 = datetime.strftime(flag[0],"%Y-%m-%d %H:%M:%S.%f")
        ti1 = datetime.strftime(flag[1],"%Y-%m-%d %H:%M:%S.%f")
        for exist in existinglst:
            #print(ti0,exist[0],ti1,exist[1],flag[2],exist[2])
            if lentype <= 5:
                if ti1 == exist[0] and ti1 == exist[1] and flag[2] == exist[2]:
                    itexists = True
            else:
                if ti0 == exist[0] and ti1 == exist[1] and flag[2] == exist[2] and flag[5] == exist[5]:
                    itexists = True
        if itexists:
            pass
        else:
            newflaglist.append(flag)
    flaglist = newflaglist

    if len(flaglist) == 0:
        return

    for elem in newlst:
        complst = []
        for elem2 in flaglist:
            comp = elem2[2]
            if lentype <= 5:
                el2 = [datetime.strftime(elem2[0],"%Y-%m-%d %H:%M:%S.%f"),datetime.strftime(elem2[1],"%Y-%m-%d %H:%M:%S.%f"),elem2[3],elem2[4]]
            else:
                el2 = [datetime.strftime(elem2[0],"%Y-%m-%d %H:%M:%S.%f"),datetime.strftime(elem2[1],"%Y-%m-%d %H:%M:%S.%f"),elem2[3],elem2[4],elem2[5],elem2[6]]
            if el2 == elem:
                complst.append(comp)
        compstr = '_'.join(complst)
        if lentype <= 5:
            elem.extend([sensorid,modificationdate])
        elem.append(compstr)

    # Flagging TABLE
    # Create flagging table
    if mode == 'delete':
        if sensorid in ['all','All','ALL']:
            print("Executing: TRUNCATE FLAGS")
            cursor.execute("TRUNCATE FLAGS")
        else:
            print("Executing: DELETE FROM FLAGS WHERE SensorID LIKE '{}'".format(sensorid))
            cursor.execute("DELETE FROM FLAGS WHERE SensorID LIKE '{}'".format(sensorid))

    flagstr = 'FlagID INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, SensorID CHAR(50), FlagBeginTime CHAR(50), FlagEndTime CHAR(50), FlagComponents CHAR(50), FlagNum INT, FlagReason TEXT, ModificationDate CHAR(50)'

    # Check whether table exists
    flagids = dbselect(db, 'FlagID', 'FLAGS')
    if not len(flagids) > 0:
        createflagtablesql = "CREATE TABLE IF NOT EXISTS FLAGS (%s)" % flagstr
        cursor.execute(createflagtablesql)
        flagid = 0
    else:
        flagids = [int(el) for el in flagids]
        flagid = max(flagids)

    # add flagging data
    #if lentype <= 4:
    #    flaghead = 'FlagID, FlagBeginTime, FlagEndTime, FlagNum, FlagReason, FlagComponents'
    flaghead = 'FlagID, FlagBeginTime, FlagEndTime, FlagNum, FlagReason, SensorID, ModificationDate, FlagComponents'

    for elem in newlst:
        flagid = flagid+1
        try:
            elem[3] = unicode(elem[3],'utf-8')
            elem[3] = elem[3].encode('ascii',errors='ignore')
        except:
            print ('flag_stream id {}: non-ascii characters in comment. Replacing by unkown reason'.format(flagid))
            elem[3] = 'Unkown reason'
        ne = [str(flagid)]
        ne.extend(elem)
        #try:
        here = True
        if here:
            #print ("Checking:", ne)
            elem = [str(el) for el in ne]
            flagsql = "INSERT INTO FLAGS(%s) VALUES (%s)" % (flaghead, '"'+'", "'.join(elem)+'"')
            #print flagsql
            if mode == "replace":
                try:
                    cursor.execute(flagsql.replace("INSERT","REPLACE"))
                except:
                    print("Write MySQL: Replace failed")
            else:
                try:
                    cursor.execute(flagsql)
                except:
                    print("Record already existing: use mode 'replace' to override")
        #except:
        #    print (" Failed to INSERT {}".format(ne))

    db.commit()
    cursor.close ()


def db2flaglist(db,sensorid, begin=None, end=None, comment=None, flagnumber=-1, key=None, removeduplicates=False):
    """
    DEFINITION:
        Read flagging information for specified sensor from data base and return a flagging list
    PARAMETERS:
        sensorid:	   (string) sensorid for flaglist, default is sensorid of self
        removeduplicates:  (bool) if True, any duplicates with identical start and endtimes are removed
                           -> considerably slower
    RETURNS:
        flaglist:          flaglist contains start, end , key2flag, flagnumber, comment, sensorid,
                           ModificationDate

    EXAMPLE:
       >>>  flaglist = db2flaglist(db,"MySensorID")
    """

    if not db:
        print("No database connected - aborting")
        return []
    cursor = db.cursor ()

    if sensorid in ['all','All','ALL']:
        searchsql = 'SELECT FlagBeginTime, FlagEndTime, FlagComponents, FlagNum, FlagReason, SensorID, ModificationDate FROM FLAGS'
    else:
        searchsql = 'SELECT FlagBeginTime, FlagEndTime, FlagComponents, FlagNum, FlagReason, SensorID, ModificationDate FROM FLAGS WHERE SensorID = "%s"' % sensorid
    if begin:
        #addbeginsql = ' AND FlagBeginTime >= "%s"' % begin
        addbeginsql = ' AND FlagEndTime >= "%s"' % begin
    else:
        addbeginsql = ''
    if end:
        #addendsql = ' AND FlagEndTime <= "%s"' % end
        addendsql = ' AND FlagBeginTime <= "%s"' % end
    else:
        addendsql = ''
    addsql = ''
    if comment:
        addsql += ' AND FlagReason LIKE "%{}%"'.format(comment)
    try:
        if int(flagnumber) > -1:
            addsql += ' AND FlagNum LIKE {}'.format(flagnumber)
    except:
        pass
    if key:
        addsql += ' AND FlagComponents LIKE "%{}%"'.format(key)
    #print ("SQL", searchsql + addbeginsql + addendsql + addsql)
    cursor.execute (searchsql + addbeginsql + addendsql + addsql)
    rows = cursor.fetchall()

    tmp = DataStream()
    res=[]
    for line in rows:
        comps = line[2].split('_')
        for elem in comps:
            res.append([tmp._testtime(line[0]),tmp._testtime(line[1]),elem,int(line[3]),line[4],line[5],tmp._testtime(line[6])])

    cursor.close ()

    def flagclean(flaglist):
            ## Cleanup flaglist -- remove all inputs with duplicate start and endtime 
            ## (use only last input)
            indicies = []
            for line in flaglist:
                inds = [ind for ind,elem in enumerate(flaglist) if elem[0] == line[0] and elem[1] == line[1] and elem[2] == line[2]]
                if len(inds) > 0:
                    index = inds[-1]
                    indicies.append(index)
            uniqueidx = (list(set(indicies)))
            uniqueidx.sort()
            #print(uniqueidx)
            flaglist = [elem for idx, elem in enumerate(flaglist) if idx in uniqueidx]

            return flaglist

    if removeduplicates:
        return flagclean(res)
    else:
        return res

def string2dict(string):
    """
    DESCRIPTION:
       converts string of type 2015_value,2014_value (as used in mysql db to dictionary)
    APLLICATION:

    USED BY:
       absolutes package to extract epoch rotation angles from database
    """
    ### assert basestring...

    mydict = {}
    try:
        if not string == '':
            try:
                elements = string.split(',')
            except:
                return {}
            for el in elements:
                dat = el.split('_')
                mydict[dat[0]] = dat[1]
    except:
        return mydict
    return mydict


