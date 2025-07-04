#!/usr/bin/env python

import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2
from magpy.stream import loggerdatabase, magpyversion, basestring, DataStream
import pymysql as mysql
import numpy as np
import json  # used for storing dictionaries and list in text fields
import hashlib  # used to create unique database ids
from datetime import datetime, timedelta, timezone
from magpy.core.methods import testtime, convert_geo_coordinate, string2dict, round_second, is_number

mysql.install_as_MySQLdb()


class DataBank(object):
    """
    DESCRIPTION:
        Everything regarding flagging, marking and annotating data in MagPy 2.0 and future versions.
        The main object of a flagging class is a dictionary and not a list as used in MagPy 1.x and 0.x.
        The flagging class contains a number of functions and methods to create, modify and analyse data
        flags. An overview about all supported methods is listed below.
        Each flag is mainly characterized by a SensorID, the sensor which was used to determine the flag,
        a time range and components at which the flag was identified. Type and label are also assigned.
        If a flag should be applied to other sensors, you should use the group item, which contains a list
        of sensors or general groups. Groups is a list of dictionaries

    MagPy 2.0:
        db.read is ~15 times faster then readDB in MagPy1.x

    AVAILABLE METHODS:

        diline2db(db, dilinestruct, mode=None, **kwargs):
        db2diline(db,**kwargs):
        getBaselineProperties(db,datastream,pier=None,distream=None):

        flaglist2db(db,flaglist,mode=None,sensorid=None,modificationdate=None):
        db2flaglist(db,sensorid, begin=None, end=None):

        string2dict(string, typ='oldlist'):


        the following methods are contained:
        - _executesql(sqlcommand)  :  execute a sql command
        - alter()  :  Use KEYLISTS and changes the columns of standard tables
        - coordinate(pier, epsgcode)  :  returns tuple with long lat in selected coordinates
        - datainfo()   :  datainfo already contained - return a valid data id
        - dbinit()  :  Initialize a mysql database and set up standard tables of magpy
        - delete(datainfoid)  :  Delete contents of the database
        - dict_to_fields(header_dict,mode)  :  save header information in database
        - fields_to_dict(datainfoid) :    extract header from database
        - get_float(tablename,sensorid,columnid)  :  Perform a select search and return floats
        - get_lines()  : Get the last x lines from the selected table
        - get_pier(pierid, rp, value)  :  Gets values from DeltaDictionary of the PIERS table
        - get_string(tablename,sensorid,columnid)  :   Perform a select search and return strings
        - info(destination,level)  :  database information
        - read(tablename,sql,starttime,endtime)  :  read data from db table and return datastream
        - select(element, table, condition, expert) : select information from table
        - sensorinfo(sensorid, sensorkeydict, sensorrevision) :  sensorid already contained
        - set_time_in_datainfo()  :  update timerange in datainfo
        - tableexists(dataid)  : returns True if such tables are existing
        - update(table, [key], [value], condition)  :  perfomr an update call
        - update_datainfo(tablename, header)  :  update DATAINFO with header information
        - write()  :    read function parameters (NOT the function) to file


        NOT INCLUDED FROM 1.x database
        - dbupload  (create archive based on stream2db) - did not find any usages in MagPy and cobsanalysis

| class         | method | since version | until version | runtime test | result verification | manual | *tested by |
|---------------| ------ | ------------- | ------------- | ------------ | ------------------- | ------ | ---------- |
| **core.database** |  |          |              |              |                     |        | |
| DataBank      | _executesql |  2.0.0 |              | yes*         |                     |        | many |
| DataBank      | alter       | 2.0.0 |               | yes          |                     |  9.2   | db.dbinit |
| DataBank      | coordinate  | 2.0.0 |               | yes          |                     |  9.2  | unused? |
| DataBank      | datainfo    | 2.0.0 |               | yes          | yes*                |       | db.write |
| DataBank      | dbinit      | 2.0.0 |               | yes          |                     |  9.2  | |
| DataBank      | delete      | 2.0.0 |               | yes          |                     |  9.2  | |
| DataBank      | diline_to_db | 2.0.0 |              | yes*         | yes*                |  9.4  | absolutes |
| DataBank      | diline_from_db | 2.0.0 |            | yes*         | yes*                |  9.4  | absolutes |
| DataBank      | dict_to_fields | 2.0.0 |            | yes          |                     |       | |
| DataBank      | fields_to_dict | 2.0.0 |            | yes*         | yes*                |       | db.read, db.get_lines |
| DataBank      | flags_from_db | 2.0.0 |             | yes          | yes                 |  9.3  | |
| DataBank      | flags_to_db | 2.0.0 |               | yes          | yes                 |  9.3  | |
| DataBank      | flags_to_delete | 2.0.0 |           | yes          | yes                 |  9.3  | |
| DataBank      | get_baseline | 2.0.0 |              | yes          | yes                 |  9.4  | |
| DataBank      | get_float   | 2.0.0 |               | yes          | yes                 |  9.2  | |
| DataBank      | get_lines   | 2.0.0 |               | yes          | yes                 |  9.2  | |
| DataBank      | get_pier    |  2.0.0 |              | yes          | yes                 |  9.2  | |
| DataBank      | get_string  | 2.0.0 |               | yes          | yes                 |  9.2  | |
| DataBank      | info        |  2.0.0 |              | yes          |                     |  9.2  | |
| DataBank      | read        |  2.0.0 |              | yes          | yes                 |  9.2  | |
| DataBank      | select      | 2.0.0 |               | yes          | yes                 |  9.2  | |
| DataBank      | sensorinfo  | 2.0.0 |               | yes          | yes*                |       | db.write |
| DataBank      | set_time_in_datainfo | 2.0.0 |      | yes*         | yes*                |       | db.write |
| DataBank      | update      | 2.0.0 |               | yes          | yes                 |  9.2  | |
| DataBank      | update_datainfo | 2.0.0 |           | yes          |                     |       | unused? |
| DataBank      | tableexists | 2.0.0 |               | yes          | yes                 |  9.2  | |
| DataBank      | write       | 2.0.0 |               | yes          | yes                 |  9.2  | |

REMOVED:
    dbaddBLV2DATAINFO(db,blvname, stationid):
    -> will read blvdata from database, extract location data from piers and write everything to DATAINFO
    -> is apparently unused and will be removed

    DATABASE FIELDS:

        SENSOR:
                SensorID: a combination of name, serialnumber and revision
                SensorName: a name defined by the observers to refer to the instrument
                SensorType: type of sensor (e.g. fluxgate, overhauzer, temperature ...)
                SensorSerialNum: its serial number
                SensorGroup: Geophysical group (e.g. Magnetism, Gravity, Environment, Meteorology, Seismology,
                             Radiometry, ...)
                SensorDataLogger: type of any electronics connected to the sensor
                SensorDataLoggerSerNum: its serial number
                SensorDataLoggerRevision: 4 digit revision id '0001'
                SensorDataLoggerRevisionComment: description of revision
                SensorDescription: Description of the sensor
                SensorElements: Measured components e.g. x,y,z,t_sensor,t_electonics
                SensorKeys: the keys to be used for the elements in MagPy e.g. 'x,y,z,t1,t2', should have the same
                            number as Elements (check MagPy Manual for this)
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
                DataTimezone:  contains timezone info (e.g. UTC) if empty, UTC is assumed

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
                TODO: create PierID from pier and stationid i.e. with hashlib
                      Pier is just A2
                      PierName is a better name for it
                      PierAlternativeName
                PierName                e.g. A2
                PierID                  Reference Number used by BEV -- modify
                PierAlernativeName      e.g. Mioara
                PierType                e.g. Aim, Pillar, Groundmark
                PierConstruction        e.g. Glascube, Concretepillar with glasplate, Groundmark
                PierLong                Longitude
                PierLat                 Latitude
                PierAltidude            Altitude surface of Pier or
                PierCoordinateSystem    Location name (GMO-Lab1)
                PierReference           Reference(s) for coordinates and construction
                DeltaDictinoary         Reference List: Looking like A2: 201502_-0.45_0.002_201504_2.15, A16:
                                        None_None_None_201505_1.15
                                        ( containing pier plus epoch - year or year/month of determination- for dir
                                          as well as delta D, Delta I; and Epoch and delta D - order sensitive -
                                          separated by underlines; non-existing values are marked by 'None')
                AzimuthDictinoary       Reference List: Looking like Z12345_xxx.xx, Z12345_xxx.xx
                                        ( containing AzimuthMark plus angle )
                DeltaComment            optional comments on delta values
                PierComment             optional comments on Pier
                StationID               Station at which Pier is located

    """

    def __init__(self, host=None, user='cobs', password='secret', database='cobsdb'):
        """
        Description
            initialize a MagPy database structure and some predefined fields
        """

        self.db = mysql.connect(host=host, user=user, passwd=password, db=database)

        self.DATAINFOKEYLIST = DataStream().DATAINFOKEYLIST

        self.DATAVALUEKEYLIST = ['CHAR(50) NOT NULL PRIMARY KEY', 'CHAR(50)', 'CHAR(50)', 'TEXT', 'TEXT', 'CHAR(30)',
                                 'DATETIME', 'DATETIME', 'CHAR(100)',
                                 'CHAR(100)', 'CHAR(100)', 'CHAR(10)', 'CHAR(100)',
                                 'CHAR(100)',
                                 'CHAR(20)', 'CHAR(50)', 'DECIMAL(20,9)',
                                 'DECIMAL(20,9)', 'DECIMAL(20,9)', 'CHAR(2)', 'DECIMAL(20,9)',
                                 'DECIMAL(20,9)', 'DECIMAL(20,9)', 'CHAR(10)',
                                 'DECIMAL(20,9)', 'DECIMAL(20,9)', 'CHAR(5)', 'TEXT',
                                 'TEXT', 'TEXT', 'CHAR(50)',
                                 'TEXT', 'CHAR(10)', 'CHAR(50)', 'CHAR(20)',
                                 'INT', 'DECIMAL(20,9)', 'CHAR(50)', 'CHAR(50)', 'CHAR(50)',
                                 'CHAR(10)', 'TEXT', 'CHAR(100)', 'TEXT',
                                 'TEXT', 'INT', 'TEXT', 'TEXT',
                                 'CHAR(50)', 'CHAR(50)', 'CHAR(50)',
                                 'CHAR(100)', 'CHAR(50)',
                                 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'CHAR(50)']

        self.SENSORSKEYLIST = DataStream().SENSORSKEYLIST

        self.STATIONSKEYLIST = DataStream().STATIONSKEYLIST

        self.BASELINEKEYLIST = ['SensorID', 'MinTime', 'MaxTime', 'TmpMaxTime', 'BaseFunction', 'BaseDegree',
                                'BaseKnots', 'BaseComment']

        # Optional (if acquisition routine is used)
        self.IPKEYLIST = ['IpName', 'IP', 'IpSensors', 'IpDuty', 'IpType', 'IpAccess', 'IpLocation', 'IpLocationLat',
                          'IpLocationLong', 'IpSystem', 'IpMainUser', 'IpComment']

        # Optional (if many piers are available)
        self.PIERLIST = ['PierID', 'PierName', 'PierAlternativeName', 'PierType', 'PierConstruction', 'StationID',
                         'PierLong', 'PierLat', 'PierAltitude', 'PierCoordinateSystem', 'PierReference',
                         'DeltaDictionary', 'AzimuthDictionary', 'DeltaComment']

        self.FLAGTABLESTRUCT = ['FlagID CHAR(50) NOT NULL PRIMARY KEY',
                           'SensorID CHAR(50)',
                           'StartTime DATETIME',
                           'EndTime DATETIME',
                           'Components CHAR(100)',  # list
                           'FlagType INT',
                           'LabelID CHAR(10)',
                           'Label CHAR(100)',
                           'Comment TEXT',
                           'Groups TEXT',  # dict
                           'Probabilities CHAR(100)',  # list
                           'StationID CHAR(10)',
                           'Validity CHAR(10)',
                           'Operator CHAR(50)',
                           'Color CHAR(50)',
                           'ModificationTime DATETIME',
                           'FlagVersion CHAR(10)'
                           ]
        self.FLAGSKEYLIST = [el.split(" ")[0] for el in self.FLAGTABLESTRUCT]

    def _executesql(self, cursor, sqlcommand):
        """
        DESCRIPTION
            helper method to execute sql and provid some error check.
            if not message
        :param sqlcommand:
        :return:
        """

        message = ''
        try:
            cursor.execute(sqlcommand)
        except mysql.IntegrityError as message:
            return message
        except mysql.Error as message:
            return message
        except:
            return 'unkown error'
        return message


    def alter(self):
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
            dbalter(db)

        APPLICATION:
            Requires an existing mysql database (e.g. mydb)
            1. Connect to the database
            db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
            2. use method
            dbalter(db)
        """
        if not self:
            loggerdatabase.error("dbalter: No database connected - aborting -- please create an empty database first")
            return
        cursor = self.db.cursor()

        try:
            checksql = 'SELECT SensorID FROM SENSORS'
            cursor.execute(checksql)
            for key in self.SENSORSKEYLIST:
                try:
                    checksql = 'SELECT ' + key + ' FROM SENSORS'
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
            for key in self.STATIONSKEYLIST:
                try:
                    checksql = 'SELECT ' + key + ' FROM STATIONS'
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
            for ind, key in enumerate(self.DATAINFOKEYLIST):
                try:
                    checksql = 'SELECT ' + key + ' FROM DATAINFO'
                    loggerdatabase.debug("Checking key: %s" % key)
                    cursor.execute(checksql)
                    loggerdatabase.debug("Found key: %s" % key)
                except:
                    loggerdatabase.debug("Missing key: %s" % key)
                    if len(self.DATAVALUEKEYLIST) == len(self.DATAINFOKEYLIST):
                        addstr = 'ALTER TABLE DATAINFO ADD ' + key + ' ' + self.DATAVALUEKEYLIST[ind]
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
            for ind, key in enumerate(self.PIERLIST):
                try:
                    checksql = 'SELECT ' + key + ' FROM PIERS'
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

        self.db.commit()
        cursor.close()


    def close(self):
        """
        DEFINITION:
            close the database
        """
        try:
            self.db.close()
        except:
            pass


    def coordinates(self, pier="A2", epsgcode='epsg:4326'):
        """
        DEFINITION:
            Extracts coordinate data from piers table
            and converts the coordinates into a desired
            coordinate system based on pyproj
        REQUIREMENTS:
            pyproj
            uses db.select
        PARAMETERS:
            - pier:        (string) existing pier in PIERS
            - epsgcode:    (string) code in which the coordinates are transformed to
                                    default is 4326
        USED BY:
            unkown
        APPLICATION:
            (long, lat) = db.coordinates("A2")
        """
        lon, lat = 0.0, 0.0

        try:
            from pyproj import Proj, transform
        except ImportError:
            print("dbcoordinates: You need to install pyproj to use this method")
            return (lon, lat)

        startlong = self.select('PierLong', 'PIERS', 'PierID LIKE "{}"'.format(pier))
        startlat = self.select('PierLat', 'PIERS', 'PierID LIKE "{}"'.format(pier))
        coordsys = self.select('PierCoordinateSystem', 'PIERS', 'PierID LIKE "{}"'.format(pier))
        if len(startlong) > 0 and len(startlat) > 0 and len(coordsys) > 0:
            startlong = float(startlong[0].replace(',', '.'))
            startlat = float(startlat[0].replace(',', '.'))
            try:
                #coordsys needs to contain somthing like xxx, EPSG : 12345
                coordsystmp = coordsys[0].split(':')[1].replace(' ', '')
            except:
                return (lon, lat)
            coordsys = "epsg:{}".format(coordsystmp)
            lon, lat = convert_geo_coordinate(float(startlong),float(startlat),coordsys,epsgcode)
        else:
            print ("coordinates: no data available for this pier")

        return (lon, lat)


    def datainfo(self, sensorid, datakeydict=None, tablenum=None, defaultstation='WIC', updatedb=True):
        """
        DEFINITION:
            provide sensorid and any relevant meta information for datainfo tab
            returns the full datainfoid

        PARAMETERS:
            - db:             (mysql database) defined by mysql.connect().
            - sensorid:       (string) code for sensor if.
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
            tablename = dbdatainfo(db,sensorid,headdict,None,stationid)

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

        # 1. Define keys here which do not trigger a new revision number in the table
        SKIPKEYS = ['DataID', 'SensorID', 'ColumnContents', 'ColumnUnits', 'DataFormat', 'DataTerms', 'DataDeltaF',
                    'DataDeltaT1', 'DataDeltaT2', 'DataFlagModification', 'DataAbsFunc', 'DataAbsInfo',
                    'DataBaseValues', 'DataFlagList',
                    'DataAbsDegree', 'DataAbsKnots', 'DataAbsMinTime', 'DataAbsMaxTime', 'DataAbsDate', 'DataRating',
                    'DataComments', 'DataSource', 'DataAbsFunctionObject', 'DataDeltaValues', 'DataTerms',
                    'DataReferences', 'DataPublicationLevel', 'DataPublicationDate', 'DataStandardLevel',
                    'DataStandardName', 'DataStandardVersion', 'DataPartialStandDesc', 'DataRotationAlpha',
                    'DataRotationBeta']

        # 2. Scan through header and modify/cleanup formats (e.g. StationID to upper case)
        datakeydict['StationID'] = datakeydict.get('StationID', '').upper()

        # 3. extract keys to be checked and create a searchlist of them
        searchlst = ' '
        datainfohead, datainfovalue = [], []
        rows = []
        novalues = False
        if datakeydict:
            # add some expections for DataSamplingRate
            sm = datakeydict.get('SensorModule', '')
            # if sm in ['OW','RCS','ow','rcs'] or 'Status' in sensorid or 'status' in sensorid:
            if sm in ['OW', 'ow', 'Ow', 'RCS', 'rcs', 'Rcs'] or 'Status' in sensorid or 'status' in sensorid:
                # Avoid sampling rate criteria for data revision for rcs and one wire sensors, as well
                # as any sensorid containing Status information
                # print ("dbdatainfo: Skipping SamplingRate for data revision")
                SKIPKEYS.append('DataSamplingRate')

            for key in datakeydict:
                if key in self.DATAINFOKEYLIST and not key in ['DataMaxTime', 'DataMinTime']:
                    # All keys are tested and added to infohead and infovalue to be updated or
                    # MinTime and Maxtime are changing and therefore are excluded from check
                    if not str(datakeydict[key]) in ['', None, 'Null']:
                        datainfohead.append(key)
                        ind = self.DATAINFOKEYLIST.index(key)
                        ### if key in ['DataFlagList']:
                        ###     add keycontents to FLAGS if not existing
                        if key in ['DataAbsFunctionObject', 'DataBaseValues']:
                            import pickle
                            pfunc = pickle.dumps(datakeydict[key])
                            datainfovalue.append(pfunc)
                        elif self.DATAVALUEKEYLIST[ind].startswith('DEC') or self.DATAVALUEKEYLIST[ind].startswith('FLO'):
                            try:
                                datainfovalue.append(float(datakeydict[key]))
                            except:
                                loggerdatabase.warning("dbdatainfo: Trying to read FLOAT value failed")
                                datainfovalue.append(str(datakeydict[key]))
                        elif self.DATAVALUEKEYLIST[ind].startswith('INT'):
                            # This was wrong until 0.3.98
                            try:
                                datainfovalue.append(int(datakeydict[key]))
                            except:
                                loggerdatabase.warning("dbdatainfo: Trying to read INT value failed")
                                datainfovalue.append(str(datakeydict[key]))
                        else:
                            datainfovalue.append(str(datakeydict[key]))
                        # Extend searchlist to identify revision numbers only with critical info
                        if not key in SKIPKEYS:
                            # For SamplingRate add a range to searchlist allowing for 10percent variation
                            # This is sensible if as minor variation are expected due to rounding in
                            # in arrays of different lengths
                            # "datainfovalue" is not affected
                            if key in ['DataSamplingRate']:
                                # Also regard for not-yet-existing db inputs
                                searchlst += 'AND (({a} > "{b:.2f}" AND {a} < "{c:.2f}") OR {a} IS NULL) '.format(a=key,
                                                                                                                  b=float(
                                                                                                                      datakeydict[
                                                                                                                          key]) * 0.9,
                                                                                                                  c=float(
                                                                                                                      datakeydict[
                                                                                                                          key]) * 1.1)
                            else:
                                # Also regard for not-yet-existing db inputs
                                # searchlst += 'AND ' + key + ' = "'+ str(datakeydict[key]) +'" '
                                searchlst += 'AND ({} = "{}" or {} IS NULL) '.format(key, str(datakeydict[key]), key)

        datainfonum = '0001'
        numlst, intnumlst = [], []

        cursor = self.db.cursor()

        # check for appropriate sensorid
        loggerdatabase.debug("dbdatainfo: Reselecting SensorID")
        sensorid = self.sensorinfo(sensorid, datakeydict)
        if 'SensorID' in datainfohead:
            index = datainfohead.index('SensorID')
            datainfovalue[index] = sensorid
        loggerdatabase.debug("dbdatainfo:  -- SensorID is now %s" % sensorid)

        checkinput = 'SELECT StationID FROM DATAINFO WHERE SensorID = "' + sensorid + '"'
        # print checkinput
        try:
            mesg = self._executesql(cursor, checkinput)
            rows = cursor.fetchall()
        except:
            loggerdatabase.warning(
                "dbdatainfo: Column StationID not yet existing in table DATAINFO (very old magpy version) - creating it ...")
            stationaddstr = 'ALTER TABLE DATAINFO ADD StationID CHAR(50) AFTER SensorID'
            cursor.execute(stationaddstr)

        # Check STATIONS
        # ##############
        if datakeydict:
            checkstation = 'SELECT StationID FROM STATIONS WHERE StationID = "' + datakeydict.get("StationID", '') + '"'
            try:
                cursor.execute(checkstation)
                rows = cursor.fetchall()
            except:
                loggerdatabase.warning("dbdatainfo: STATIONS not yet existing ...")
                rows = [1]
            if not len(rows) > 0:
                print("dbdatainfo: Did not find StationID in STATIONS - adding it")
                # Add all Station info to STATIONS
                stationhead, stationvalue = [], []
                for key in datakeydict:
                    if key in self.STATIONSKEYLIST:
                        stationhead.append(key)
                        stationvalue.append('"' + str(datakeydict[key]) + '"')
                sql = 'INSERT INTO STATIONS(%s) VALUES (%s)' % (', '.join(stationhead), ', '.join(stationvalue))
                # loggerdatabase.debug("dbdatainfo: sql: %s" % datainfosql)
                if updatedb:
                    cursor.execute(sql)

        checkinput = 'SELECT DataID FROM DATAINFO WHERE SensorID = "' + sensorid + '"'
        loggerdatabase.debug("dbdatainfo: %s " % checkinput)
        try:
            # print checkinput
            cursor.execute(checkinput)
            rows = cursor.fetchall()
            loggerdatabase.debug("dbdatainfo: Number of existing DATAINFO lines: %s" % str(rows))
        except:
            loggerdatabase.warning("dbdatainfo: Could not access table DATAINFO in database")
            loggerdatabase.warning("dbdatainfo: Creating it now")
            if not len(self.DATAINFOKEYLIST) == len(self.DATAVALUEKEYLIST):
                loggerdatabase.error("CHECK your DATA KEYLISTS")
                return
            fullDATAKEYLIST = []
            for i, elem in enumerate(self.DATAINFOKEYLIST):
                newelem = elem + ' ' + self.DATAVALUEKEYLIST[i]
                fullDATAKEYLIST.append(newelem)
            datainfostr = ', '.join(fullDATAKEYLIST)

            createdatainfotablesql = "CREATE TABLE IF NOT EXISTS DATAINFO (%s)" % datainfostr

            cursor.execute(createdatainfotablesql)

        def joindatainfovalues(head, lst):
            # Submethod for getting sql string from values
            dst = []
            for i, elem in enumerate(head):
                ind = self.DATAINFOKEYLIST.index(elem)
                if self.DATAVALUEKEYLIST[ind].startswith('DEC') or self.DATAVALUEKEYLIST[ind].startswith('FLO') or \
                        self.DATAVALUEKEYLIST[ind].startswith('INT'):
                    dst.append(str(lst[i]))
                else:
                    dst.append('"' + str(lst[i]) + '"')
            return ','.join(dst)

        # check whether input in DATAINFO with sensorid is existing already
        nullnames = []
        if len(rows) > 0:
            datainfostring = ''
            loggerdatabase.debug("dbdatainfo: Found existing tables")
            # Get maximum number
            for i in range(len(rows)):
                rowval = rows[i][0].replace(sensorid + '_', '')
                # print len(rows), rowval, sensorid+'_', rows[i][0]
                try:
                    numlst.append(int(rowval))
                    # print numlst
                except:
                    print("crap")
                    pass
            maxnum = max(numlst)
            loggerdatabase.debug("dbdatainfo: Maxnum: %i" % maxnum)
            # Perform intensive search using any given meta info
            intensivesearch = 'SELECT DataID FROM DATAINFO WHERE SensorID = "' + sensorid + '"' + searchlst
            loggerdatabase.info("dbdatainfo: Searchlist: %s" % intensivesearch)
            cursor.execute(intensivesearch)
            intensiverows = cursor.fetchall()
            # print intensivesearch, intensiverows
            loggerdatabase.debug("dbdatainfo: Found matching table: %s" % str(intensiverows))
            loggerdatabase.debug("dbdatainfo: using searchlist %s" % intensivesearch)
            loggerdatabase.debug("dbdatainfo: intensiverows: %i" % len(intensiverows))
            if len(intensiverows) > 0:
                loggerdatabase.debug("dbdatainfo: DataID existing - updating {}".format(intensiverows[0]))
                selectupdate = True
                for i in range(len(intensiverows)):
                    # if more than one record is existing select the latest (highest) number
                    introwval = intensiverows[i][0].replace(sensorid + '_', '')
                    try:
                        intnumlst.append(int(introwval))
                    except:
                        pass
                intmaxnum = max(intnumlst)
                datainfonum = '{0:04}'.format(intmaxnum)

                # get a NULL list (identify all keys with input zero)
                # selectedline = [elem for elem in intensiverows if intensiverows[i][0].endswith(datainfonum)][0]
                # Get all fields not in SKIPKEYS with zero values
                #   too be updated as well
                try:
                    getallfields = 'SELECT column_name FROM information_schema.columns WHERE table_name = "DATAINFO" AND column_name NOT IN ("{}")'.format(
                        '","'.join(SKIPKEYS))
                    cursor.execute(getallfields)
                    fieldrows = cursor.fetchall()
                    notskipcolumns = (list(set([el[0] for el in fieldrows])))
                    valselect = 'SELECT {} FROM DATAINFO WHERE DataID = "{}"'.format(','.join(notskipcolumns),
                                                                                     sensorid + '_' + datainfonum)
                    cursor.execute(valselect)
                    fieldrows = cursor.fetchall()
                    valscolumns = [el for el in fieldrows[0]]
                    nullnames = [el for ii, el in enumerate(notskipcolumns) if valscolumns[ii] == None]
                except:
                    nullnames = []
            else:
                loggerdatabase.debug("dbdatainfo: Creating new DataID")
                loggerdatabase.debug("dbdatainfo: because - {}".format(intensiverows))
                # print (intensivesearch)
                selectupdate = False
                datainfonum = '{0:04}'.format(maxnum + 1)

                if 'DataID' in datainfohead:
                    selectindex = datainfohead.index('DataID')
                    datainfovalue[selectindex] = sensorid + '_' + datainfonum
                else:
                    datainfohead.append('DataID')
                    datainfovalue.append(sensorid + '_' + datainfonum)
                datainfostring = joindatainfovalues(datainfohead, datainfovalue)

            if selectupdate:
                sqllst = [key + " ='" + str(datainfovalue[idx]) + "'" for idx, key in enumerate(datainfohead) if (
                            key in SKIPKEYS or key in nullnames) and not key == 'DataAbsFunctionObject' and not key == 'DataBaseValues' and not key == 'DataFlagList' and not key == 'DataID']
                # Add also values if existing input is NULL

                if 'DataAbsFunctionObject' in datainfohead:  ### Tested Text and Binary so far. No quotes is OK.
                    print("dbdatainfo: adding DataAbsFunctionObjects to DATAINFO is not yet working")
                    # pfunc = pickle.dumps(datainfovalue[datainfohead.index('DataAbsFunctionObject')])
                    # sqllst.append('DataAbsFunctionObject' + '=' + pfunc)
                    # For testing:
                    # datainfosql = 'INSERT INTO DATAINFO(DataAbsFunctionObject) VALUES (%s)' % (pfunc)
                    # cursor.execute(datainfosql)
                if 'DataBaseValues' in datainfohead:  ### Tested Text and Binary so far. No quotes is OK.
                    loggerdatabase.debug("dbdatainfo: adding DataBaseValues to DATAINFO is not yet working")
                    # pfunc = pickle.dumps(datainfovalue[datainfohead.index('DataBaseValues')])
                    # TODO convert pfunc to string
                    # sqllst.append('DataBaseValues' + '=' + pfunc)
                    # For testing:
                    # datainfosql = 'INSERT INTO DATAINFO(DataBaseValues) VALUES (%s)' % (pfunc)
                    # cursor.execute(datainfosql)
                if not len(sqllst) > 0:
                    novalues = True
                datainfosql = "UPDATE DATAINFO SET " + ", ".join(
                    sqllst) + " WHERE DataID = '" + sensorid + "_" + datainfonum + "'"
            else:
                # print "No, Here"
                datainfosql = 'INSERT INTO DATAINFO(%s) VALUES (%s)' % (', '.join(datainfohead), datainfostring)
                loggerdatabase.debug("dbdatainfo: sql: %s" % datainfosql)
            if updatedb and not novalues:
                cursor.execute(datainfosql)
            datainfoid = sensorid + '_' + datainfonum
        else:
            loggerdatabase.debug("dbdatainfo: Creating new table")
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

            if updatedb:
                try:
                    # print ("Updating DATAINFO table with {}".format(datainfosql))
                    cursor.execute(datainfosql)
                except mysql.Error as e:
                    print("Failed: {}".format(e))
                except:
                    print("Failed for unknown reason")

        self.db.commit()
        cursor.close()

        return datainfoid


    def dbinit(self):
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
            dbinit(db)

        APPLICATION:
            Requires an existing mysql database (e.g. mydb)
            1. Connect to the database
            db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
            2. use method
            dbinit(db)
        """

        # SENSORS TABLE
        # Create station table input
        headstr = ' CHAR(100), '.join(self.SENSORSKEYLIST) + ' CHAR(100)'
        headstr = headstr.replace('SensorID CHAR(100)', 'SensorID CHAR(50) NOT NULL PRIMARY KEY')
        headstr = headstr.replace('SensorDescription CHAR(100)', 'SensorDescription TEXT')
        createsensortablesql = "CREATE TABLE IF NOT EXISTS SENSORS (%s)" % headstr

        # STATIONS TABLE
        # Create station table input
        stationstr = ' CHAR(100), '.join(self.STATIONSKEYLIST) + ' CHAR(100)'
        stationstr = stationstr.replace('StationID CHAR(100)', 'StationID CHAR(50) NOT NULL PRIMARY KEY')
        stationstr = stationstr.replace('StationDescription CHAR(100)', 'StationDescription TEXT')
        stationstr = stationstr.replace('StationIAGAcode CHAR(100)', 'StationIAGAcode CHAR(10)')
        # stationstr = 'StationID CHAR(50) NOT NULL PRIMARY KEY, StationName CHAR(100), StationIAGAcode CHAR(10), StationInstitution CHAR(100), StationStreet CHAR(50), StationCity CHAR(50), StationPostalCode CHAR(20), StationCountry CHAR(50), StationWebInfo CHAR(100), StationEmail CHAR(100), StationDescription TEXT'
        createstationtablesql = "CREATE TABLE IF NOT EXISTS STATIONS (%s)" % stationstr

        # DATAINFO TABLE
        # Create datainfo table
        if not len(self.DATAINFOKEYLIST) == len(self.DATAVALUEKEYLIST):
            loggerdatabase.error("CHECK your DATA KEYLISTS")
            return
        fullDATAKEYLIST = []
        for i, elem in enumerate(self.DATAINFOKEYLIST):
            newelem = elem + ' ' + self.DATAVALUEKEYLIST[i]
            fullDATAKEYLIST.append(newelem)
        datainfostr = ', '.join(fullDATAKEYLIST)
        createdatainfotablesql = "CREATE TABLE IF NOT EXISTS DATAINFO (%s)" % datainfostr

        # FLAGS TABLE
        # Create flagging table
        createflagtablesql = "CREATE TABLE IF NOT EXISTS FLAGS ({})".format(", ".join(self.FLAGTABLESTRUCT))

        # BASELINE TABLE
        # Create baseline table
        basestr = ' CHAR(100), '.join(self.BASELINEKEYLIST) + ' CHAR(100)'
        basestr = basestr.replace('SensorID CHAR(100)', 'SensorID CHAR(50) NOT NULL')
        createbaselinetablesql = "CREATE TABLE IF NOT EXISTS BASELINE (%s)" % basestr

        # IP TABLE
        # Create ip addresses table
        ipstr = ' CHAR(100), '.join(self.IPKEYLIST) + ' CHAR(100)'
        ipstr = ipstr.replace('IP CHAR(100)', 'IP CHAR(50) NOT NULL PRIMARY KEY')
        ipstr = ipstr.replace('IpComment CHAR(100)', 'IpComment TEXT')
        ipstr = ipstr.replace('IpSensors CHAR(100)', 'IpSensors TEXT')
        createiptablesql = "CREATE TABLE IF NOT EXISTS IPS (%s)" % ipstr

        # Pier TABLE
        # Create Pier overview table
        pierstr = ' CHAR(100), '.join(self.PIERLIST) + ' CHAR(100)'
        pierstr = pierstr.replace('PierID CHAR(100)', 'PierID CHAR(50) NOT NULL PRIMARY KEY')
        pierstr = pierstr.replace('PierComment CHAR(100)', 'PierComment TEXT')
        pierstr = pierstr.replace('DeltaComment CHAR(100)', 'DeltaComment TEXT')
        pierstr = pierstr.replace('DeltaDictionary CHAR(100)', 'DeltaDictionary TEXT')
        pierstr = pierstr.replace('PierReference CHAR(100)', 'PierReference TEXT')
        createpiertablesql = "CREATE TABLE IF NOT EXISTS PIERS (%s)" % pierstr

        cursor = self.db.cursor()

        cursor.execute(createsensortablesql)
        cursor.execute(createstationtablesql)
        cursor.execute(createdatainfotablesql)
        cursor.execute(createflagtablesql)
        cursor.execute(createbaselinetablesql)
        cursor.execute(createiptablesql)
        cursor.execute(createpiertablesql)

        self.db.commit()
        cursor.close()
        self.alter()

    def delete(self, datainfoid, samplingrateratio=None, timerange=None):
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
            delete(db,'DIDD_3121331_0002_0001')

        APPLICATION:
            Requires an existing mysql database (e.g. mydb)
            so first connect to the database
            db = mysql.connect(host="localhost",user="user",passwd="secret",db="mysql")
            # Delete everything older then the last 3 days
            dbdelete(db,'DIDD_3121331_0002_0001',timerange=3)
            # Keep data in dependency of the samplingrate
            #  days2keep = ceil(samplingrate[sec] * samplingrateratio)  (e.g. 12 days for 1 sec data)
            dbdelete(db,'DIDD_3121331_0002_0001',samplingrateratio=12)
        TODO:
            - If sampling rate not given in DATAINFO get it from the datastream
        """

        if not samplingrateratio:
            samplingrateratio = 12.0

        cursor = self.db.cursor()
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
                # stream = db2stream(db,datainfoid)
                samplingperiod = 5  # TODO
            # 2. Determine time interval to delete
            # factor depends on available space...
            timerange = np.ceil(samplingperiod * samplingrateratio)

        loggerdatabase.debug("dbdelete: selected timerange of %s days" % str(timerange))

        # 3. Delete time interval
        loggerdatabase.info("dbdelete: deleting data of %s older than %s days" % (datainfoid, str(timerange)))
        try:
            delcount = 100000
            countstr = "SELECT COUNT(*) FROM {} WHERE time < ADDDATE(NOW(), INTERVAL -{} {})".format(datainfoid,
                                                                                                     timerange,
                                                                                                     timeunit)
            try:
                cursor.execute(countstr)
                msg = cursor.fetchone()
                lines = int(msg[0])
                rangemax = int(np.ceil(lines / delcount))
                deletesql = "DELETE FROM {} WHERE time < ADDDATE(NOW(), INTERVAL -{} {}) LIMIT {}".format(datainfoid,
                                                                                                          timerange,
                                                                                                          timeunit,
                                                                                                          delcount)
                for i in range(0, rangemax):
                    cursor.execute(deletesql)
            except:
                # old way ... faster on short sequences, considerable slower on large
                deletesql = "DELETE FROM %s WHERE time < ADDDATE(NOW(), INTERVAL -%i %s)" % (
                datainfoid, timerange, timeunit)
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
            updatedatainfosql = 'UPDATE DATAINFO SET DataMinTime="%s", DataMaxTime="%s" WHERE DataID="%s"' % (
            mintime, maxtime, datainfoid)
            cursor.execute(updatedatainfosql)
            loggerdatabase.info("dbdelete: DATAINFO for %s now covering %s to %s" % (datainfoid, mintime, maxtime))
        except:
            loggerdatabase.error("dbdelete: error when re-determining dates for DATAINFO")

        self.db.commit()
        cursor.close()

    def diline_to_db(self, dilinestruct, mode=None, tablename=None, stationid='WIC', debug=False):
        """
        DEFINITION:
            Method to write dilinestruct to a mysql database

        PARAMETERS:
        Variables:
            - db:           (mysql database) defined by mysql.connect().
            - dilinestruct: (magpy diline)
            - stationid:    (string) - if you maintain several observatories it is important to use the correct station (default is WIC)
        Optional:
            - mode:         (string) - default is insert
                                mode: replace -- replaces existing table contents with new one, also replaces informations from sensors and station table
                                mode: insert -- create new table with unique ID
                                mode: delete -- like insert but drops the existing DIDATA table
            - tablename:    (string) - specify tablename of the DI table (default is DIDATA)

        EXAMPLE:
            diline_to_db(db,...)

        APPLICATION:
            Requires an existing mysql database (e.g. mydb)
            so first connect to the database
            db = database.DataBank(host="localhost", user="user", passwd="secret", db="mysql")
        """
        from matplotlib.dates import num2date

        success = False
        # DIDATA TABLE
        if not tablename:
            tablename = 'DIDATA'
        if not stationid:
            stationid = 'XXX'

        loggerdatabase.debug("diline_to_db: Writing DI values to database table {}".format(tablename))

        if len(dilinestruct) < 1:
            loggerdatabase.error("diline_to_db: Empty diline. Aborting ...")
            return

        cursor = self.db.cursor()
        cursor._defer_warnings = True

        oldversion = False
        # Determine type of table
        versionsql = "SHOW COLUMNS FROM {} LIKE 'EndTime'".format(tablename)
        msg = self._executesql(cursor, versionsql)
        if msg:
            loggerdatabase.debug("diline_to_db: {}".format(msg))
        else:
            rows = cursor.fetchall()
            ll = len(rows)
            if debug:
                print(versionsql, ll)
            if not ll:
                oldversion = True

        # 1. Create the diline table if not existing
        # - DIDATA
        DIDATAKEYLIST = ['DIID', 'StartTime', 'EndTime', 'TimeArray', 'HcArray', 'VcArray', 'ResArray', 'OptArray',
                         'LaserArray', 'FTimeArray',
                         'FArray', 'Temperature', 'ScalevalueFluxgate', 'ScaleAngle', 'Azimuth', 'Pier', 'Observer',
                         'DIInst',
                         'FInst', 'FluxInst', 'InputDate', 'DIComment', 'StationID']

        headstr = ' CHAR(100), '.join(DIDATAKEYLIST) + ' CHAR(100)'
        headstr = headstr.replace('DIID CHAR(100)', 'DIID CHAR(20) NOT NULL PRIMARY KEY')
        headstr = headstr.replace('StartTime CHAR(100)', 'StartTime DATETIME')
        headstr = headstr.replace('EndTime CHAR(100)', 'EndTime DATETIME')
        headstr = headstr.replace('DIComment CHAR(100)', 'DIComment TEXT')
        headstr = headstr.replace('Array CHAR(100)', 'Array TEXT')
        createDItablesql = "CREATE TABLE IF NOT EXISTS %s (%s)" % (tablename, headstr)

        if mode == 'delete':
            # For some reason this hangs up in verification.py - works flawless from jn however
            msg = self._executesql(cursor, "DROP TABLE IF EXISTS {}".format(tablename))
            if msg:
                print(msg)
                loggerdatabase.info("diline_to_db: DIDATA table not yet existing")
            else:
                loggerdatabase.info("diline_to_db: Old DIDATA table has been deleted")
            print ("Done")
        else:
            if oldversion:
                print("Please note: the {} table contains a MagPy 1.x format".format(tablename))
                print("Update the tablestructure to continue:")
                print("1) Load all old data sets: data = db.diline_from_db()")
                print("2) Save it to the new format: db.diline_to_db(data, mode='delete', stationid='WIC')")
                return False

        msg = self._executesql(cursor, createDItablesql)
        if msg:
            loggerdatabase.debug("diline_to_db: error-- {}".format(msg))
        else:
            loggerdatabase.info("diline_to_db: New DIDATA table created")
        print ("created new")
        # 2. Add DI values to the table
        #   Cycle through all lines of the dilinestruct
        #   - a) convert arrays to underscore separated text like 'nan,nan,765,7656,879.6765,nan"
        def _create_id(pier, mintime, maxtime, stationid, debug=debug):
            idgenerator = "{}{}{}{}".format(pier, mintime, maxtime, stationid)
            if debug:
                print("Creating ID out of ", idgenerator)
            m = hashlib.md5()
            m.update(idgenerator.encode())
            diid = str(int(m.hexdigest(), 16))[0:12]
            if debug:
                print("ID look like ", diid)
            return diid

        for line in dilinestruct:
            insertlst = []
            mintime = np.nanmin(line.time)
            maxtime = np.nanmax(line.time)
            insertlst.append(_create_id((line.pier), mintime, maxtime, stationid))
            insertlst.append(datetime.strftime(num2date(mintime), "%Y-%m-%d %H:%M:%S"))
            insertlst.append(datetime.strftime(num2date(maxtime), "%Y-%m-%d %H:%M:%S"))
            insertlst.append(json.dumps(line.time))
            insertlst.append(json.dumps(line.hc))
            insertlst.append(json.dumps(line.vc))
            insertlst.append(json.dumps(line.res))
            insertlst.append(json.dumps(line.opt))
            insertlst.append(json.dumps(line.laser))
            insertlst.append(json.dumps(line.ftime))
            insertlst.append(json.dumps(line.f))
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
            insertlst.append(stationid)

            disql = "INSERT INTO %s(%s) VALUES (%s)" % (
            tablename, ', '.join(DIDATAKEYLIST), '"' + '", "'.join(insertlst) + '"')
            if mode == "replace":
                disql = disql.replace("INSERT", "REPLACE")
            if debug:
                print(" SQL command: ", disql)
            msg = self._executesql(cursor, disql)
            if msg:
                loggerdatabase.debug("diline_to_db: {}".format(msg))
            else:
                success = True
        self.db.commit()
        cursor.close()
        return success

    def diline_from_db(self, starttime=None, endtime=None, tablename='DIDATA', sql=None, debug=False):
        """
        DEFINITION:
            Method to read DI values from a database and write them to a list of DILineStruct's

        PARAMETERS:
        Variables:
            - db:           (mysql database) defined by mysql.connect().
            - starttime:    (string/datetime) - time range to select
            - endtime:      (string/datetime) - if not given just the day defined by starttime is used
            - sql:          (list) - define any additional selection criteria (e.g. ["Pier = 'A2'", "Observer = 'Mickey Mouse'"] )
                                    important: don't forget the ' '
            - tablename:    (string) - specify tablename of the DI table (default is DIDATA)

        EXAMPLE:
            resultlist = db.diline_from_db(starttime="2013-01-01",sql="Pier='A2'")

        APPLICATION:
            Requires an existing mysql database (e.g. mydb)
            so first connect to the database

        RETURNS:
            list of DILineStruct elements
        """

        from magpy.absolutes import DILineStruct
        from matplotlib.dates import num2date

        resultlist = []
        wherelist = []
        oldversion = False

        if not self:
            loggerdatabase.error("diline_from_db: No database connected - aborting")
            return resultlist

        cursor = self.db.cursor()

        # Determine type of table
        versionsql = "SHOW COLUMNS FROM {} LIKE 'EndTime'".format(tablename)
        msg = self._executesql(cursor, versionsql)
        if msg:
            loggerdatabase.debug("diline_from_db: {}".format(msg))
        else:
            rows = cursor.fetchall()
            ll = len(rows)
            if debug:
                print(versionsql, ll)
            if not ll:
                oldversion = True
                print ("Found old DIDATA structure in databank")

        whereclause = ""
        if starttime:
            starttime = testtime(starttime)
            wherelist.append("StartTime >= '{}'".format(starttime))
        if endtime:
            endtime = testtime(endtime)
            if oldversion:
                wherelist.append("StartTime < '{}'".format(endtime))
            else:
                wherelist.append("EndTime < '{}'".format(endtime))
        if sql and isinstance(sql, basestring):
            elements = sql.split(" AND ")
            wherelist.extend(elements)
        elif sql and isinstance(sql, (list, tuple)):
            wherelist.extend(sql)
        if len(wherelist) > 0:
            whereclause = " WHERE {}".format(" AND ".join(wherelist))

        getdidata = 'SELECT * FROM ' + tablename + whereclause

        if debug:
            print("Call: ", getdidata)
        msg = self._executesql(cursor, getdidata)
        if msg:
            loggerdatabase.debug("diline_from_db: {}".format(msg))
        else:
            rows = cursor.fetchall()
            ll = len(rows)
            if debug:
                print ("diline_from_db: found {} DI values structure in db - importing".format(ll))
            loggerdatabase.debug(
                "diline_from_db: found {} DI values structure in db - importing".format(ll))
            for idx, di in enumerate(rows):
                if oldversion:
                    # Zerlege time column
                    timelst = [float(elem) for elem in di[2].split('_')]
                    # Check for old matplotlib - basis 1.1.0001 - dates and correct them to
                    # matplotlibversion >= 3.3 dates
                    timelst = [el if not el > 719000 else el-719163.0 for el in timelst]
                    distruct = DILineStruct(len(timelst))
                    distruct.time = timelst
                    distruct.hc = [float(elem) for elem in di[3].split('_')]
                    distruct.vc = [float(elem) for elem in di[4].split('_')]
                    distruct.res = [float(elem) for elem in di[5].split('_') if len(di[5].split('_')) > 1]
                    distruct.opt = [float(elem) for elem in di[6].split('_') if len(di[6].split('_')) > 1]
                    distruct.laser = [float(elem) for elem in di[7].split('_') if len(di[7].split('_')) > 1]
                    distruct.ftime = [float(elem) for elem in di[8].split('_') if len(di[8].split('_')) > 1]
                    distruct.f = [float(elem) for elem in di[9].split('_') if len(di[9].split('_')) > 1]
                    try:
                        distruct.t = float(di[10])
                    except:
                        distruct.t = di[10]
                    try:
                        distruct.scaleflux = float(di[11])
                    except:
                        distruct.scaleflux = di[11]
                    try:
                        distruct.scaleangle = float(di[12])
                    except:
                        distruct.scaleangle = di[12]
                    try:
                        distruct.azimuth = float(di[13])
                    except:
                        distruct.azimuth = di[13]
                    distruct.pier = di[14]
                    distruct.person = di[15]
                    distruct.di_inst = di[16]
                    distruct.f_inst = di[17]
                    distruct.fluxgatesensor = di[18]
                    try:
                        distruct.inputdate = testtime(di[19])
                    except:
                        # no input data for AUTODIF
                        distruct.inputdate = num2date(np.nanmean(distruct.time)).replace(tzinfo=None)
                else:
                    if idx == 0:
                        loggerdatabase.debug(
                            "diline_from_db: found {} DI values structure in db - importing".format(ll))
                    timelst = json.loads(di[3])
                    distruct = DILineStruct(len(timelst))
                    distruct.time = timelst
                    distruct.hc = json.loads(di[4])
                    distruct.vc = json.loads(di[5])
                    distruct.res = json.loads(di[6])
                    distruct.opt = json.loads(di[7])
                    distruct.laser = json.loads(di[8])
                    distruct.ftime = json.loads(di[9])
                    distruct.f = json.loads(di[10])
                    try:
                        distruct.t = float(di[11])
                    except:
                        distruct.t = di[11]
                    try:
                        distruct.scaleflux = float(di[12])
                    except:
                        distruct.scaleflux = di[12]
                    try:
                        distruct.scaleangle = float(di[13])
                    except:
                        distruct.scaleangle = di[13]
                    try:
                        distruct.azimuth = float(di[14])
                    except:
                        distruct.azimuth = di[14]
                    distruct.pier = di[15]
                    distruct.person = di[16]
                    distruct.di_inst = di[17]
                    distruct.f_inst = di[18]
                    distruct.fluxgatesensor = di[19]
                    try:
                        distruct.inputdate = testtime(di[20])
                    except:
                        # no input data for AUTODIF
                        distruct.inputdate = num2date(np.nanmean(distruct.time)).replace(tzinfo=None)
                    # ignore comment as stored in di[21]
                    if len(di) > 22:
                        distruct.stationid = di[22]
                resultlist.append(distruct)

        cursor.close()

        return resultlist


    def dict_to_fields(self, header_dict, **kwargs):
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
            db.dict_to_fields(stream.header,mode='replace')

        APPLICATION:
            Requires an existing mysql database (e.g. mydb)
            so first connect to the database
            db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
        """
        mode = kwargs.get('mode')
        update = kwargs.get('update')
        onlynone = kwargs.get('onlynone')
        alldi = kwargs.get('alldi')

        if update:  # not used any more beginning with version 0.1.259
            mode = 'update'

        if not mode:
            mode = 'insert'

        cursor = self.db.cursor()

        sensorfieldlst, sensorvaluelst = [], []
        stationfieldlst, stationvaluelst = [], []
        datainfofieldlst, datainfovaluelst = [], []
        usestation, usesensor, usedatainfo = False, False, False
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
                    print("Will update value %s with %s" % (str(value), header_dict[key]))
                    loggerdatabase.warning(
                        "dict_to_fields: ID is already existing but field values for field %s are differing: dict (%s); db (%s)" % (
                        key, header_dict[key], str(value)))
                    updatesql = 'UPDATE ' + table + ' SET ' + key + ' = "' + header_dict[
                        key] + '" WHERE ' + primarykey + ' = "' + primaryvalue + '"'
                    executesql(updatesql)
                else:
                    print("value = None: Will update value %s with %s" % (str(value), header_dict[key]))
                    updatesql = 'UPDATE ' + table + ' SET ' + key + ' = "' + header_dict[
                        key] + '" WHERE ' + primarykey + ' = "' + primaryvalue + '"'
                    executesql(updatesql)

        if "StationID" in header_dict:
            usestation = True
            loggerdatabase.debug("dict_to_fields: Found StationID in dict")
        else:
            loggerdatabase.warning(
                "dict_to_fields: No StationID in dict - skipping any other eventually given station information")
        if "SensorID" in header_dict:
            usesensor = True
            loggerdatabase.debug("dict_to_fields: found SensorID in dict")
        else:
            loggerdatabase.warning(
                "dict_to_fields: No SensorID in dict - skipping any other eventually given sensor information")
        if "DataID" in header_dict:
            usedatainfo = True
            datainfolst = [header_dict['DataID']]
            loggerdatabase.debug("dict_to_fields: found DataID in dict")
        else:
            loggerdatabase.warning("dict_to_fields: No DataID in dict")

        if alldi:
            usedatainfo = True
            if 'SensorID' in header_dict:
                getdatainfosql = 'SELECT DataID FROM DATAINFO WHERE SensorID = "' + header_dict['SensorID'] + '"'
                msg = executesql(getdatainfosql)
                if not msg == '':
                    loggerdatabase.warning("dict_to_fields: Obtaining DataIDs failed - %s" % msg)
                else:
                    datainfolst = [elem[0] for elem in cursor.fetchall()]
                    loggerdatabase.info(
                        "dict_to_fields: No DataID in dict - option alldi selected so all DATAINFO inputs will be updated")
            else:
                loggerdatabase.warning(
                    "dict_to_fields: alldi option requires a SensorID in dict which is not provided - skipping")
                usedatainfo = False

        # 1. create ColumnContents and ColumnUnits from col-...
        cols = [[key.replace('col-', ''), header_dict.get(key)] for key in header_dict if key.startswith('col-')]
        units = [[key.replace('unit-col-', ''), header_dict.get(key)] for key in header_dict if
                 key.startswith('unit-col-')]
        collst, unitlst = [], []
        for el in DataStream().KEYLIST[1:]:
            adderc, adderu = '', ''
            for col in cols:
                if col[0] == el:
                    adderc = col[1]
            for unit in units:
                if unit[0] == el:
                    adderu = unit[1]
            collst.append(adderc)
            unitlst.append(adderu)
        header_dict['ColumnContents'] = ",".join(collst)
        header_dict['ColumnUnits'] = ",".join(unitlst)

        # 2. Update content for the primary IDs
        for key in header_dict:
            fieldname = key
            fieldvalue = header_dict[key]
            if fieldname in self.STATIONSKEYLIST:
                if usestation:
                    stationfieldlst.append(fieldname)
                    stationvaluelst.append(fieldvalue)
            if fieldname in self.SENSORSKEYLIST:
                if usesensor:
                    sensorfieldlst.append(fieldname)
                    sensorvaluelst.append(fieldvalue)
            if fieldname in self.DATAINFOKEYLIST:
                if usedatainfo:
                    datainfofieldlst.append(fieldname)
                    datainfovaluelst.append(fieldvalue)

        if mode in ['insert','replace']:  #####   Insert ########
            if len(stationfieldlst) > 0:
                fields = ', '.join(stationfieldlst)
                vals = ', '.join(map(repr,stationvaluelst))
                insertsql = '{} INTO STATIONS ({}) VALUE ({})'.format( mode.upper(), fields, vals)
                msg = executesql(insertsql)
                if not msg == '':
                    loggerdatabase.warning("dict_to_fields: insert for STATIONS failed - %s - try update mode" % msg)
            if len(sensorfieldlst) > 0:
                fields = ', '.join(sensorfieldlst)
                vals = ', '.join(map(repr,sensorvaluelst))
                insertsql = '{} INTO SENSORS ({}) VALUE ({})'.format(mode.upper(), fields, vals)
                msg = executesql(insertsql)
                if not msg == '':
                    loggerdatabase.warning("dict_to_fields: insert for SENSORS failed - %s - try update mode" % msg)
            if len(datainfofieldlst) > 0:
                for elem in datainfolst:
                    if 'DataID' in datainfofieldlst:
                        ind = datainfofieldlst.index('DataID')
                        datainfovaluelst[ind] = elem
                    else:
                        datainfofieldlst.append('DataID')
                        datainfovaluelst.append(elem)
                fields = ', '.join(datainfofieldlst)
                vals = ', '.join(map(repr, datainfovaluelst))
                insertsql = '{} INTO DATAINFO ({}) VALUE ({})'.format(mode.upper(), fields, vals)
                msg = executesql(insertsql)
                if not msg == '':
                    loggerdatabase.warning(
                            "dict_to_fields: insert for DATAINFO of %s failed - %s - try update mode" % (elem, msg))
        elif mode == 'update':  #####   Update ########
            for key in header_dict:
                if key in self.STATIONSKEYLIST:
                    searchsql = "SELECT %s FROM STATIONS WHERE StationID = '%s'" % (key, header_dict['StationID'])
                    executesql(searchsql)
                    value = cursor.fetchone()[0]
                    updatetable('STATIONS', 'StationID', header_dict['StationID'], key, value)
                if key in self.SENSORSKEYLIST:
                    searchsql = "SELECT %s FROM SENSORS WHERE SensorID = '%s'" % (key, header_dict['SensorID'])
                    executesql(searchsql)
                    value = cursor.fetchone()[0]
                    updatetable('SENSORS', 'SensorID', header_dict['SensorID'], key, value)
                if key in self.DATAINFOKEYLIST:
                    for elem in datainfolst:
                        searchsql = "SELECT %s FROM DATAINFO WHERE DataID = '%s'" % (key, elem)
                        executesql(searchsql)
                        value = cursor.fetchone()[0]
                        updatetable('DATAINFO', 'DataID', elem, key, value)
        else:
            loggerdatabase.warning(
                "dict_to_fields: unrecognized mode, needs to be one of insert, replace or update - check help(db.dict_to_fields)")

        self.db.commit()
        cursor.close()


    def fields_to_dict(self, datainfoid, debug=False):
        """
        DEFINITION:
            Provide datainfoid to get all information from tables STATION, SENSORS and DATAINFO
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
            header_dict = fields_to_dict(db,'DIDD_3121331_0001_0001')

        APPLICATION:
            Requires an existing mysql database (e.g. mydb)
            so first connect to the database
            db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
        """
        metadatadict = {}
        colsstr = ''
        colselstr = ''
        cols, colsel = [], []
        cursor = self.db.cursor()

        getids = 'SELECT sensorid,stationid FROM DATAINFO WHERE DataID = "' + datainfoid + '"'
        msg = self._executesql(cursor, getids)
        if debug:
            print (msg)
        ids = cursor.fetchone()
        if not ids:
            return {}
        loggerdatabase.debug("fields_to_dict: Selected sensorid: %s" % ids[0])

        for key in self.DATAINFOKEYLIST:
            if not key == 'StationID':  # Remove that line when included into datainfo
                getdata = 'SELECT ' + key + ' FROM DATAINFO WHERE DataID = "' + datainfoid + '"'
                try:
                    cursor.execute(getdata)
                    row = cursor.fetchone()
                    loggerdatabase.debug("fields_to_dict: got key from DATAINFO - %s" % getdata)
                    if isinstance(row[0], basestring):
                        metadatadict[key] = row[0]
                        if key == 'ColumnContents':
                            colsstr = row[0]
                        if key == 'ColumnUnits':
                            colselstr = row[0]
                        if key in ['DataAbsFunctionObject', 'DataBaseValues']:
                            # func = pickle.loads(str(cdf_file.attrs[key]))
                            # stream.header[key] = func
                            pass
                    else:
                        if not row[0] == None:
                            metadatadict[key] = float(row[0])
                except mysql.Error as e:
                    loggerdatabase.error("fields_to_dict: mysqlerror while adding key %s, %s" % (key, e))
                except:
                    loggerdatabase.error("fields_to_dict: unknown error while adding key %s" % key)

        for key in self.SENSORSKEYLIST:
            getsens = 'SELECT ' + key + ' FROM SENSORS WHERE SensorID = "' + ids[0] + '"'
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
                        # metadatadict[key] = row[0]
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
            loggerdatabase.warning("fields_to_dict: Could not interpret column field in DATAINFO")

        # Use ColumnContent info for creating col information
        try:
            for ind, el in enumerate(cols):
                if not el == '':
                    col = DataStream().KEYLIST[ind + 1]
                    key = 'col-' + col
                    unitkey = 'unit-col-' + col
                    metadatadict[key] = el
                    metadatadict[unitkey] = colsel[ind]
        except:
            loggerdatabase.warning("fields_to_dict: Could not assign column name")

        for key in self.STATIONSKEYLIST:
            getstat = 'SELECT ' + key + ' FROM STATIONS WHERE StationID = "' + ids[1] + '"'
            try:
                cursor.execute(getstat)
                row = cursor.fetchone()
            except:
                if debug:
                    print("fields_to_dict: error when executing %s" % getstat)
                row = [None]
            if isinstance(row[0], basestring):
                metadatadict[key] = row[0]
            else:
                if row[0] == None:
                    pass
                    # metadatadict[key] = row[0]
                else:
                    metadatadict[key] = float(row[0])

        return metadatadict


    def flags_to_db(self, flagobject, mode='replace', debug=False):
        """
        DESCRIPTION:
           Function to converts a flagging object to a data base table
           This method is replacing flaglist2db from MagPy 2.0 onwards

        PARAMETER:
           flagobject: a flagging dictionary based on MagPy 2.0 and later

        OPTIONAL:
           mode: 'replace': default - replaces information
                 'insert' : adds if not existing

        APPLICATION:
           # 1. Upload all flagging data and append to existing sensor info
           db.flags_to_db(flagobject)
           # 2. Upload flaglist data for a specific sensor
           selobj = flagobject.select('sensorid',['list of sensors'])
           db.flags_to_db(selobj)
           # 3. Firstly delete all contents for existing sensors in flaglist (or the provided sensorid)
           #    and then upload flaglist data
           flaglist2db(db, flaglist, mode='delete')# (, sensorid='MySensor')
           # 4. Delete all data for a specific sensor
           flaglist2db(db, [], sensorid='MySensor', mode='delete')

           flaglist2db(db, [], mode='delete') is not supported - use TRUNCATE command in database

        """

        # Flag table old contents
        oldflaghead = 'FlagID, FlagBeginTime, FlagEndTime, FlagComponents, FlagNum, FlagReason, SensorID, ModificationDate'
        vallist = []

        # Flag table new structure and contents
        flagtablestruct = self.FLAGTABLESTRUCT
        flaghead = self.FLAGSKEYLIST
        contlist = flagobject.FLAGKEYS[1:]

        if not self:
            loggerdatabase.info("flags_to_db: No database connected - aborting")
            return False
        if not mode:
            mode = 'replace'
        if not flagobject and not len(flagobject) > 1:
            loggerdatabase.info("flags_to_db: Nothing to do - aborting")
            return False

        cursor = self.db.cursor()

        loggerdatabase.info("flaglist2db: Running flaglist2db ...")

        # 0. Check whether type of eventually existing flagging table
        createtab = False
        oldtab = False
        msg = self._executesql(cursor, "SHOW COLUMNS FROM FLAGS")
        if msg:
            print("flags_to_db error with existing table checks:", msg)
            print(" Table not yet existing?")
            createtab = True
        else:
            flagids = self.select('FlagID', 'FLAGS')
            if debug:
                print("Existing inputs", flagids)
            rows = cursor.fetchall()
            colnames = [el[0] for el in rows]
            if debug:
                print(" Table existing - found the following column headers", colnames)
            if 'FlagBeginTime' in colnames and len(flagids) > 0:
                if debug:
                    print ("Found old table structure with contents")
                oldtab = True
                oldflags = self.flags_from_db(mode='old')
                if not oldflags and not isinstance(oldflags.flagdict, dict):
                    return False
                # Now combine all old flags with the new flags
                flagobject.join(oldflags)
            elif 'FlagBeginTime' in colnames:
                if debug:
                    print ("Found old table structure without contents")
                oldtab = True

        if createtab:
            # Table not yet existing - create a new MagPy>2.x flagging table
            createflagtablesql = "CREATE TABLE IF NOT EXISTS FLAGS ({})".format(",".join(flagtablestruct))
            msg = self._executesql(cursor, createflagtablesql)
            if msg:
                print("flags_to_db error when creating new flagging table:", msg)
                return False
        elif oldtab:
            createflagtablesql = "CREATE OR REPLACE TABLE FLAGS ({})".format(",".join(flagtablestruct))
            msg = self._executesql(cursor, createflagtablesql)
            if msg:
                print("flags_to_db error when creating new flagging table:", msg)
                return False

        # 1. Format flagging object to be ready for db upload
        if mode in ['insert', 'append']:
            command = "INSERT IGNORE INTO FLAGS"  # do nothing if already present
        else:
            command = "REPLACE INTO FLAGS"  # replace if already present
        for d in flagobject.flagdict:
            flagid = d
            contdict = flagobject.flagdict[d]
            vallist = [flagid]
            for el in contlist:
                if el in ['components', 'groups', 'probabilities']:
                    # convert lists to string
                    convstr = json.dumps(contdict.get(el))  # use dict2string
                    vallist.append(convstr)
                else:
                    vallist.append(contdict.get(el))
            if debug:
                print(len(flaghead), len(vallist))
                print(" Contents of flags to add", vallist)
                print(" Flagtable heads:", flaghead)

            headstr = ", ".join(flaghead)
            valflist = []
            for el in vallist:
                if isinstance(el, int):
                    vstr = "{}".format(el)
                else:
                    vstr = "'{}'".format(el)
                valflist.append(vstr)
            valstr = ", ".join(valflist)
            flagsql = "{}({}) VALUES ({})".format(command, headstr, valstr)

            if debug:
                print(flagsql)

            msg = self._executesql(cursor, flagsql)
            if msg:
                print("flags_to_db error when inserting/replacing data:", msg)
                pass

        loggerdatabase.info("flags_to_db: Done")
        self.db.commit()
        cursor.close()
        return True

    def flags_from_db(self, sensorid=None, starttime=None, endtime=None, comment=None, flagtype=-1, labelid=None, key=None,
                      debug=False, commentconversion='cobs', **kwargs):
        """
        DEFINITION:
            Read flagging information from data base and return a flagging object
        PARAMETERS:
            sensorid:	       (string) provide the requested sensorid or 'all'
            starttime:	       (string) extract flags which end after this starttime
            endtime:	       (string) extract flags which begine before this endtime
            comment
            flagtpye
            key
        RETURNS:
            flagobject
        EXAMPLE:
           fl = db.flag_from_db("MySensorID")
        """
        removeduplicates = kwargs.get('removeduplicates')
        begin = kwargs.get('begin')
        end = kwargs.get('end')
        flagnumber = kwargs.get('flagnumber')
        tabletype = '2.0'  # might be used in the future
        selecttype = 'SELECT {} FROM FLAGS'.format(", ".join(self.FLAGSKEYLIST))

        if removeduplicates:
            print(" removeduplicates in flags_from_db decrepated since MagPy 2.0")
        if starttime:
            starttime = testtime(starttime)
        elif begin:
            print(" begin replaced by starttime in flags_from_db since MagPy 2.0")
            starttime = testtime(begin)
        if endtime:
            endtime = testtime(endtime)
        elif end and not endtime:
            print(" end replaced by endtime in flags_from_db since MagPy 2.0")
            endtime = testtime(end)
        if flagnumber in [0, 1, 2, 3, 4] and not flagtype == -1:
            flagtype = flagnumber
        if sensorid in ['all', 'All', 'ALL']:
            print(" sensorid 'all' deprecated since MagPy 2.0 - use sensorid=None")
            sensorid = None

        from magpy.core import flagging
        fl = flagging.Flags()

        if not self:
            print(" flags_from_db: No database connected - aborting")
            return flagging.Flags()

        cursor = self.db.cursor()

        # check type of existing table
        msg = self._executesql(cursor, "SHOW COLUMNS FROM FLAGS")
        if msg:
            print(" flags_from_db: No flagging table existing?")
            return flagging.Flags()
        else:
            flagids = self.select('FlagID', 'FLAGS')
            if debug:
                print("Existing amount of inputs:", len(flagids))
            if not len(flagids) > 0:
                print(" flags_from_db: No flagging data in table")
                return flagging.Flags()
            rows = cursor.fetchall()
            colnames = [el[0] for el in rows]
            if debug:
                print(" Flagging Table with inputs existing - found the following column headers", colnames)
            if 'FlagBeginTime' in colnames:
                if debug:
                    print("Found old table structure with contents")
                tabletype = '1.0'
                selecttype = 'SELECT FlagBeginTime, FlagEndTime, FlagComponents, FlagNum, FlagReason, SensorID, ModificationDate FROM FLAGS'

        # now construct WHERE clause
        serachsql = ''
        if starttime or endtime or sensorid or labelid or flagnumber or comment:
            searchsql = 'WHERE'
            searchlist = []
            if sensorid:
                searchlist.append('SensorID = "{}"'.format(sensorid))
            if starttime:
                if tabletype == '1.0':
                    searchlist.append('FlagEndTime >= "{}"'.format(starttime))
                else:
                    searchlist.append('EndTime >= "{}"'.format(starttime))
            if endtime:
                if tabletype == '1.0':
                    searchlist.append('FlagBeginTime <= "{}"'.format(endtime))
                else:
                    searchlist.append('StartTime <= "{}"'.format(endtime))
            if comment:
                if tabletype == '1.0':
                    searchlist.append('FlagReason LIKE "%{}%"'.format(comment))
                else:
                    searchlist.append('Comment LIKE "%{}%"'.format(comment))
            if labelid:
                if not tabletype == '1.0':
                    searchlist.append('LabelID LIKE "{}"'.format(labelid))
            if flagtype in [0, 1, 2, 3, 4]:
                if tabletype == '1.0':
                    searchlist.append('FlagNum LIKE {}'.format(flagtype))
                else:
                    searchlist.append('Flagtype LIKE {}'.format(flagtype))
            if key in DataStream().KEYLIST:
                if tabletype == '1.0':
                    searchlist.append('FlagComponents LIKE "%{}%"'.format(key))
                else:
                    searchlist.append('Components LIKE "%{}%"'.format(flagtype))
            serachsql = "{} {}".format(searchsql, " AND ".join(searchlist))

        sqlcommand = "{} {}".format(selecttype, serachsql)
        msg = self._executesql(cursor, sqlcommand)
        rows = cursor.fetchall()
        if debug:
            print("Obtained {} rows".format(len(rows)))
        # now construct either old structure
        # and the run fl = fl._check_version()
        # {'WIC_1_0001': [['2018-08-02 14:51:33.999992', '2018-08-02 14:51:33.999992', 'x', 3, 'lightning RL', '2023-02-02 10:22:28.888995'], ['2018-08-02 14:51:33.999992', '2018-08-02 14:51:33.999992', 'y', 3, 'lightning RL', '2023-02-02 10:22:28.888995']]}
        # or extract new type
        res = {}
        div = 10000
        for idx, line in enumerate(rows):
            if tabletype == '1.0':
                # convert it directly to new flagging structure here as I do not need to sort for sensors and split
                # up components
                if debug and idx == 0:
                    print (" old type import")
                    print (line)
                labelid = '000'
                operator = 'unknown'
                groups = None
                comps = line[2].split('_')
                # round endtime to the next second
                key = line[5]
                st = testtime(line[0])
                et = round_second(testtime(line[1]))
                ft = line[3]
                if is_number(ft):
                    ft = int(ft)
                else:
                    ft = 0
                if ft == 2:
                    ft = 4
                if not st <= et:
                    st = round_second(testtime(line[0]))
                    et = testtime(line[1])
                if commentconversion == 'cobs':
                    labelid, operator = fl._import_conradosb(line[4])
                    groups = fl._get_cobs_groups(key,line[4])
                if idx/div > 1:
                    div = div + 10000
                    if debug:
                        print (" import reached {}".format(idx))
                fl.add(sensorid=key, starttime=st, endtime=et,
                                      components=comps, flagtype=ft, labelid=labelid,
                                      comment=line[4], modificationtime=line[6],
                                      operator=operator, groups=groups,
                                      flagversion='2.0')
            else:
                cont = {}
                for idx, el in enumerate(fl.FLAGKEYS[1:]):
                    if el in ['components', 'groups', 'probabilities']:
                        if line[idx+1]:
                            co = json.loads(line[idx + 1])
                        else:
                            co = line[idx+1]
                        cont[el] = co
                    else:
                        cont[el] = line[idx+1]
                res[line[0]] = cont
                fl = flagging.Flags(res)

        self.db.commit()
        cursor.close()
        if tabletype == "1.0":
            fl = fl._set_label_from_comment()
        return fl

    def flags_to_delete(self, parameter='sensorid', value=None, debug=False):
        """
        DESCRIPTION:
           Delete specific contents in flagging table as defined by parameter and value
           This method makes use of flagging.select

        PARAMETER:
           parameter: a flagging dictionary based on MagPy 2.0 and later
           value:   a single  value associated with the parameter

           if you chooes parameter="all" then the complete flagging database will be deleted
           (TRUNCATE FLAGS)
        EXAMPLE:
           db.flags_to_delete(parameter="operator", values=["RL"])
        """

        if parameter == 'sensorid':
            fl = self.flags_from_db(sensorid=value)
        elif parameter == 'comment':
            fl = self.flags_from_db(comment=value)
        elif parameter == 'labelid':
            fl = self.flags_from_db(labelid=value)
        elif parameter == 'flagtype':
            fl = self.flags_from_db(flagtype=value)
        else:
            fl = self.flags_from_db()
        cursor = self.db.cursor()

        if parameter == 'all':
            msg = self._executesql(cursor, "TRUNCATE FLAGS")
            if msg:
                print(msg)
        else:
            fl = fl.select(parameter=parameter, values=[value], debug=debug)
            flagids = [d for d in fl.flagdict]
            if debug:
                print(flagids)
            for flagid in flagids:
                delsql = "DELETE FROM FLAGS WHERE FlagID LIKE '{}'".format(flagid)
                if debug:
                    print ("Executing: ", delsql)
                msg = self._executesql(cursor, delsql)
                if msg:
                    print(msg)
        self.db.commit()
        cursor.close()

        loggerdatabase.info("flags_to_delete: Done")
        return True


    def get_float(self, tablename, sensorid, columnid):
        """
        DEFINITION:
            Perform a select search and return floats
        PARAMETERS:
        Variables:
            - tablename:    name of the table
            - sensorid:     sensor to match
            - columnid:     column in which search is performed
        APPLICATION:
            sr =  db.get_float('DATAINFO', teststream1.header.get('SensorID'), 'DataSamplingRate')
        """
        sql = 'SELECT ' + columnid + ' FROM ' + tablename + ' WHERE SensorID = "' + sensorid + '"'
        try:
            cursor = self.db.cursor()
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


    def get_baseline(self, sensorid, date=None, debug=False):
        """
        DESCRIPTION:
            Method to extract baseline fitting data from db. By default the currently valid baseline will be returned.
            Use the option "date" to select previous parameters
        PARAMETER:
            db: name of the mysql data base
            sensorid: identification id of sensor in database
            date:     the line matching the given date will be returned
        USED BY:
            analysis scrips i.e. magnetism_products and f_analysis
        RETURNS:
            a list with all selected baseline data
        """
        # find out the version of the baseline table:
        # if BaseID is existing then MaxTime column == 3
        # else MaxTime ==2
        db = self.db
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if date:
            date = testtime(date)
        else:
            date = now

        def checking_baseline_table(db):
            maxtcol = 2
            vers = '1.0'
            query = "SELECT * FROM BASELINE"
            cur = db.cursor()
            cur.execute(query)
            columns = cur.description
            cur.close()
            result = [{column[0]: index for index, column in enumerate(columns)}]
            result = result[0]
            if 'BaseID' in result:
                idpos = result.get('BaseID')
                if idpos == 0:
                    vers = '1.1'
                else:
                    vers = '2.0'
            maxtcol = result.get('MaxTime')
            return maxtcol, vers


        maxtcol, basetableversion = checking_baseline_table(db)
        if debug:
            print("get baseline: Found BASELINE table version {}".format(basetableversion))

        fallbackvals = [(99, sensorid, now-timedelta(days=365), now, 'spline', 1, 0.3, 'fallback')]
        paralist = ['BaseID','SensorID','MinTime','MaxTime','BaseFunction','BaseDegree','BaseKnots','BaseComment']
        where = 'SensorID LIKE "%' + sensorid + '%"'
        if debug:
            print("get baseline: searchsql: {}".format(where))
        vals = self.select(','.join(paralist), 'BASELINE', where)
        res = {}
        # use fallback if nothing found in db
        if not len(vals) > 0:
            vals = fallbackvals
        for line in vals:
            cont = {}
            for i in list(range(1,8)):
                try:
                    value = line[i]
                    if paralist[i].find('Time') >= 0:
                        if not line[i]:
                            value = now
                        else:
                            value = testtime(line[i])
                    cont[paralist[i]] = value
                except:
                    pass
            res[line[0]] = cont

        if debug:
            print("Now selecting data with corresponding date")
        result = {}
        for el in res:
            lr = res.get(el).get('MinTime')
            hr = res.get(el).get('MaxTime')
            if lr <= date <= hr:
                if debug:
                    print ("Found baseline")
                result[el] = res.get(el)

        if debug:
            print("Finally got:", result)

        return result


    def get_lines(self, tablename, lines):
        """
        DEFINITION:
            Get the last x lines from the selected table
        PARAMETERS:
        Variables:
            - db:           (mysql database) defined by mysql.connect().
            - tablename:    name of the table
            - lines:        (int) amount of lines to extract
        APPLICATION:
            data = dbgetlines(db, 'DATA_0001_0001', 3600)
            returns a data stream object
        """
        cursor = self.db.cursor()

        stream = DataStream()
        headsql = 'SHOW COLUMNS FROM %s' % (tablename)
        message = self._executesql(cursor, headsql)
        if not message:
            head = cursor.fetchall()
        else:
            loggerdatabase.error(message)
            return stream
        keys = list(np.transpose(np.asarray(head))[0])

        getsql = 'SELECT * FROM %s ORDER BY time DESC LIMIT %d' % (tablename, lines)
        message = self._executesql(cursor, getsql)
        if not message:
            result = cursor.fetchall()
        else:
            loggerdatabase.error(message)
            return stream
        res = np.transpose(np.asarray(result))

        array = [[] for key in DataStream().KEYLIST]
        for idx, key in enumerate(DataStream().KEYLIST):
            if key in keys:
                pos = keys.index(key)
                if key == 'time':
                    array[idx] = np.asarray([testtime(elem) for elem in res[pos]])
                elif key in DataStream().NUMKEYLIST:
                    array[idx] = np.asarray(res[pos], dtype=float)
                else:
                    array[idx] = np.asarray(res[pos], dtype=object)

        header = self.fields_to_dict(tablename)
        stream = DataStream(header=header, ndarray=np.asarray(array, dtype=object))

        return stream.sorting()


    def get_pier(self, pierid, rp, value='deltaF', year=None, dic='DeltaDictionary', debug=False):
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
            - dic:          (string) dictionary to look at, default and currently only usable is 'DeltaDictionary'
        APPLICATION:
            deltaD =  db.get_pier('A7','A2','deltaD')

            returns deltaD of A7 relative to A2
        """
        sql = 'SELECT '+ dic +' FROM PIERS WHERE PierID = "' + pierid + '"'
        cursor = self.db.cursor()
        msg = self._executesql(cursor, sql)
        if msg:
            print(" get_pier: sql comment error: {}".format(msg))
            return 0.0

        row = cursor.fetchone()
        if not row:
            if debug:
                print(" get_pier: No data found for your selection")
            return 0.0
        else:
            row = row[0]

        # Identify version (MagPy1.x - string, MagPy2.x json)
        if row.find("_") > 0:
            print (" get_pier: found MagPy1.x structure in database - will be deprecated")
            d = string2dict(row)
        else:
            d = json.loads(row)

        deltadir = d.get(rp,{})
        if deltadir:
            yearlist = [int(year) for year in deltadir]
            datayear = max(yearlist)
            if year:
                datayear = [year if year in yearlist else max(yearlist)][0]
            valdir = deltadir.get(str(datayear))
            res = valdir.get(value, 0.0)
            return float(res)
        else:
            print ("no deltas found")
            return 0.0


    def get_string(self,tablename,sensorid,columnid,revision=None):
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
            stationid =  dbgetstring(db, 'DATAINFO', 'LEMI25_22_0001', 'StationID')
            returns the stationid from the DATAINFO table which matches the Sensor
        """
        sql = 'SELECT ' + columnid + ' FROM ' + tablename + ' WHERE SensorID = "' + sensorid + '"'
        cursor = self.db.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        try:
            fl = float(row[0])
            return fl
        except:
            return row[0]


    def info(self, destination='log', level='full'):
        """
        DEFINITION:
            Provide version info of database and write to log
        PARAMETERS:
            - db:           (mysql database) defined by mysql.connect().
            - destination:  (string) either "log"(default) or "stdout"
            - level:        (string) "full"(default) -> show size as well, else skip size
        """
        report = ""
        size = 'not determined'
        versionsql = "SELECT VERSION()"
        namesql = "SELECT DATABASE()"
        cursor = self.db.cursor()
        cursor.execute(versionsql)
        version = cursor.fetchone()[0]
        cursor.execute(namesql)
        databasename = cursor.fetchone()[0]
        if level == 'full':
            sizesql = 'SELECT sum(round(((data_length + index_length) / 1024 / 1024 / 1024), 2)) as "Size in GB" FROM information_schema.TABLES WHERE table_schema="{}"'.format(
                databasename)
            cursor.execute(sizesql)
            size = cursor.fetchone()[0]
        if destination == 'log':
            loggerdatabase.info(
                "connected to database '{}' (MYSQL Version {}) - size in GB: {}".format(databasename, version, size))
        else:
            report = "connected to database '{}' (MYSQL Version {}) - size in GB: {}".format(databasename, version,
                                                                                             size)
            print(report)
        self.db.commit()
        cursor.close()
        return report


    def read(self, table, starttime=None, endtime=None, sql=None):
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
            - starttime:        (string/datetime)
            - endtime:          (string/datetime)

        Kwargs:

        RETURNS:
            data stream

        EXAMPLE:
            data = db.read('DIDD_3121331_0002_0001')

        APPLICATION:
            Requires an existing mysql database (e.g. mydb)
            so first connect to the database
            db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")

        TODO:
            - If sampling rate not given in DATAINFO get it from the datastream
        """
        wherelist = []
        stream = DataStream()

        if not self.db:
            loggerdatabase.error("readDB: No database connected - aborting")
            return stream

        cursor = self.db.cursor()

        if not table:
            loggerdatabase.error("readDB: Aborting ... either sensorid or table must be specified")
            return
        if starttime:
            # starttime = stream._testtime(begin)
            begin = testtime(starttime)
            wherelist.append('time >= "{}"'.format(begin))
        if endtime:
            end = testtime(endtime)
            wherelist.append('time <= "{}"'.format(end))
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
            # for tab in rows:
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
            loggerdatabase.error("readDB: mysqlerror while getting table info")
            return stream

        def checkEqual3(lst):
            return lst[1:] == lst[:-1]

        if len(keys) > 0:
            if len(whereclause) > 0:
                getdatasql = 'SELECT ' + ','.join(keys) + ' FROM ' + table + ' WHERE ' + whereclause
            else:
                getdatasql = 'SELECT ' + ','.join(keys) + ' FROM ' + table
            cursor.execute(getdatasql)
            rows = np.asarray(cursor.fetchall())
            ls = []
            for i in range(len(DataStream().KEYLIST)):
                ls.append([])

            columns = rows.T
            array = [[] for el in stream.KEYLIST]
            if len(columns) > 0:
                for idx,key in enumerate(keys):
                    pos = stream.KEYLIST.index(key)
                    if not False in checkEqual3(columns[idx]):
                        print("readDB: Found identical values only:{}".format(key))
                        col = columns[idx]
                        if len(col) < 1 or str(col[0]) == '' or str(col[0]) == '-' or str(col[0]).find(
                                '0000000000000000') or str(col[0]).find('xyz'):
                            array[pos] = np.asarray([])
                        else:
                            array[pos] = col[:1]
                    elif key in stream.NUMKEYLIST:
                        array[pos] = np.asarray(columns[idx], dtype=np.float64)
                    elif key.endswith('time'):
                        if isinstance(columns[idx][0], basestring):
                            # old db format
                            temp = np.asarray([testtime(el) for el in columns[idx]]).astype(datetime)
                            array[pos] = np.asarray(temp, dtype=datetime)
                        else:
                            array[pos] = np.asarray(columns[idx], dtype=datetime)
                    else:
                        array[pos] = np.asarray(columns[idx], dtype=object)
                    # consider nan elemenets - done

            stream.ndarray = np.asarray(array, dtype=object)
            stream.header = self.fields_to_dict(table)

        cursor.close()
        return stream


    def select(self, element, table, condition=None, expert=None, debug=False):
        """
        DESCRIPTION:
            Function to select elements from a table.
        PARAMETERS:
            element         (string)
            table           (string) name of the table
            condition       (string) Where clause
            expert          (String) replaces the complete "Where"
        RETURNS:
            A list containing the matching elements
        EXAMPLE:
            magsenslist = db.select('SensorID', 'SENSORS', 'SensorGroup = "Magnetism"')
            tempsenslist = db.select('SensorID', 'SENSORS','SensorElements LIKE "%T%"')
            lasttime = db.select('time','DATATABLE',expert="ORDER BY time DESC LIMIT 1")

        """
        returnlist = []
        if expert:
            sql = "SELECT " + element + " from " + table + " " + expert
        elif not condition:
            sql = "SELECT " + element + " from " + table
        else:
            sql = "SELECT " + element + " from " + table + " WHERE " + condition
        if debug:
            print("dbselect SQL:", sql)
        cursor = self.db.cursor()
        try:
            msg = self._executesql(cursor, sql)
            rows = cursor.fetchall()
            if debug:
                print("select rows:", rows)
                print("select sql call error msg:", msg)
            for el in rows:
                if len(el) < 2:
                    returnlist.append(el[0])
                else:
                    returnlist.append(el)
        except:
            pass

        self.db.commit()
        cursor.close()
        return returnlist


    def sensorinfo(self, sensorid, sensorkeydict=None, sensorrevision='0001'):
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
           write
        EXAMPLE:
            db.sensorinfo(sensorid)

        APPLICATION:
            Requires an existing mysql database (e.g. mydb)
            1. Connect to the database
            db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
            2. use method
            dbalter(db)
        """

        sensorhead, sensorvalue, numlst = [], [], []

        cursor = self.db.cursor()

        if sensorkeydict:
            for key in sensorkeydict:
                if key in self.SENSORSKEYLIST:
                    sensorhead.append(key)
                    sensorvalue.append(sensorkeydict[key])

        loggerdatabase.debug("dbsensorinfo: sensor: ", sensorhead, sensorvalue)
        rows = []

        check = 'SELECT SensorID, SensorRevision, SensorName, SensorSerialNum FROM SENSORS WHERE SensorID LIKE "' + sensorid + '%"'
        try:
            cursor.execute(check)
            rows = cursor.fetchall()
        except:
            loggerdatabase.warning("dbsensorinfo: Could not access table SENSORS in database - creating table SENSORS")
            headstr = ' CHAR(100), '.join(self.SENSORSKEYLIST) + ' CHAR(100)'
            headstr = headstr.replace('SensorID CHAR(100)', 'SensorID CHAR(50) NOT NULL PRIMARY KEY')
            createsensortablesql = "CREATE TABLE IF NOT EXISTS SENSORS (%s)" % headstr
            cursor.execute(createsensortablesql)

        if len(rows) > 0:
            loggerdatabase.debug("SensorID is existing in Table SENSORS")
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
                sensorid = sensorid + '_' + sensorrevision
                updatesensorsql = 'UPDATE SENSORS SET SensorID = "' + sensorid + '", SensorRevision = "' + sensorrevision + '", SensorSerialNum = "' + sensorserialnum + '" WHERE SensorID = "' + oldsensorid + '"'
                cursor.execute(updatesensorsql)
        else:
            sensorserialnum = ''
            print("SensorID not yet existing in Table SENSORS")
            # SensorID is not existing in Table
            loggerdatabase.info("dbsensorinfo: Sensorid not yet existing in SENSORS.")
            # Check whether given sensorid is incomplete e.g. revision number is missing
            loggerdatabase.info("dbsensorinfo: Creating new sensorid %s " % sensorid)
            if not 'SensorSerialNum' in sensorhead:
                print("No serial number")
                sensoridsplit = sensorid.split('_')
                if len(sensoridsplit) in [2, 3]:
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
                    sensorvalue.append(sensorid + '_' + sensorrevision)
                    sensorid = sensorid + '_' + sensorrevision
                else:
                    index = sensorhead.index('SensorID')
                    # Why???????? This seems to be wrong (maybe important for OW  -- added an untested corr (leon))
                    if 'OW' in sensorvalue:
                        sensorvalue[index] = sensorid + '_' + sensorrevision

            ### create an input for the new sensor
            sensorsql = "INSERT INTO SENSORS(%s) VALUES (%s)" % (
            ', '.join(sensorhead), '"' + '", "'.join(sensorvalue) + '"')
            # print "Adding the following info to SENSORS: ", sensorsql
            cursor.execute(sensorsql)

        self.db.commit()
        cursor.close()

        return sensorid

    def set_times_in_datainfo(self, tablename, colstr, unitstr):
        """
        DEFINITION:
            Method to update min time and max time variables in DATAINFO table
            using data from table tablename

        PARAMETERS:
            - db:           (mysql database) defined by mysql.connect().
            - tablename:    (string) name of the table
        APPLICATION:
            dbsetTimesDataInfo(db, "MyTable_12345_0001_0001")
        USED BY:
            - writeDB, stream2DB
        """
        cursor = self.db.cursor()

        getminmaxtimesql = "Select MIN(time),MAX(time) FROM " + tablename
        mesg = self._executesql(cursor, getminmaxtimesql)
        if not mesg:
            rows = cursor.fetchall()
        else:
            print (mesg)
            return
        updatedatainfotimesql = 'UPDATE DATAINFO SET DataMinTime = "{}", DataMaxTime = "{}", ColumnContents = "{}", ColumnUnits = "{}" WHERE DataID = "{}"'.format(testtime(rows[0][0]), testtime(rows[0][1]), colstr, unitstr, tablename)
        mesg = self._executesql(cursor, updatedatainfotimesql)
        if not mesg:
            rows = cursor.fetchall()
        else:
            print (mesg)
            return
        self.db.commit()
        cursor.close()


    def update(self, tablename, keys, values, condition=None):
        """
        DEFINITION:
            Perform an update call to add values into specific keys of the selected table
            If no condition is provided, then an insert call will be created
        PARAMETERS:
        Variables:
            - db:           (mysql database) defined by mysql.connect().
            - tablename:    name of the table
            - keys:         (list) list of keys to modify
            - values:       (list) list of values for the keys
            - condition:     (string) put in an optional where condition
        APPLICATION:
            dbupdate(db, 'DATAINFO', [], [], condition='SensorID="MySensor"')
            returns a string with either 'success' or an error message
        """
        try:
            if not len(keys) == len(values):
                print("update: amount of keys does not fit provided values")
                return False
        except:
            print("update: keys and values must be provided as list e.g. [key1,key2,...]")
        if not len(keys) > 0:
            print("update: provide at least on key/value pair")
            return False

        if not condition:
            condition = ''
        else:
            condition = 'WHERE %s' % condition

        setlist = []
        for idx, el in enumerate(keys):
            st = '%s="%s"' % (el, values[idx])
            setlist.append(st)
        if len(setlist) > 0:
            setstring = ','.join(setlist)
        else:
            setstring = setlist[0]
        if condition:  # might be replaced by INSERT INTO ... ON DUPLICATE UPDATE ...
            updatesql = 'UPDATE %s SET %s %s' % (tablename, setstring, condition)
        else:
            updatesql = "INSERT INTO {} ({}) VALUES ({})".format(tablename, ",".join(keys), ",".join(['"{}"'.format(el) for el in values]))
        cursor = self.db.cursor()
        msg = self._executesql(cursor, updatesql)
        self.db.commit()
        cursor.close()
        if msg:
            print ("update error:", msg)
            return False
        return True


    def update_datainfo(self, tablename, header):
        """
        DEFINITION:
            Method to update DATAINFO table with header information
            using data from table tablename
            makes use of the update method

        PARAMETERS:
            - db:           (mysql database) defined by mysql.connect().
            - tablename:    (string) name of the table
        USED BY:
            unkown
        APPLICATION:
            db.update_datainfo("MyTable_12345_0001", myheader)
        """
        cursor = self.db.cursor()

        # 1. Select all tables matching table name
        searchtables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%{}%'".format(tablename)
        msg = self._executesql(cursor, searchtables)
        try:
            rows = list(cursor.fetchall()[0])
        except:
            print("dbupdateDataInfo: failed", msg)
            return
        for tab in rows:
            # 2. check whether tab exists
            searchdatainfo = "SELECT DataID FROM DATAINFO WHERE DataID LIKE '%{}%'".format(tab)
            msg = self._executesql(cursor, searchdatainfo)
            try:
                res = list(cursor.fetchall()[0])
                exist = True
            except:
                print (msg)
                exist = False

            if exist:
                updatelst = []
                for key in header:
                    if key in self.DATAINFOKEYLIST and not key.startswith('Column'):
                        self.update('DATAINFO', [key], [header[key]], condition='DataID="{}"'.format(tab))
            else:
                print("update_datainfo: insert for non existing table not yet written - TODO")


    def tableexists(self, tablename, debug=False):
        """
        DESCRIPTION
            check whether a table existis or not
        VARIABLES:
            db   :    a link to a mysql database
            tablename  :  the table to be searched (%tablename%)
                          e.g.  MyTable  wil find MyTable, NOTMyTable, OhItsMyTableIndeed
        USED BY:
            cobsanalysis - weather_products
        RETURNS
            True : if one or more table with %tablename% are existing
            False : if tablename is NOT found in database db
        APPLICATION
            db = DataBank(...)
            return db.tableexists('MyTable')

        -- added before 1.0.2 --
        """
        n = []
        sql = "SHOW TABLES LIKE '%{}%'".format(tablename)

        cursor = self.db.cursor()
        msg = self._executesql(cursor, sql)
        n = cursor.fetchall()
        if msg and debug:
            print (msg)
        if n and len(n) > 0:
            return True
        else:
            return False

    def write(self, datastream, tablename=None, StationID=None, mode='replace', revision=None, roundtime=0, keepempty=False, debug=False, **kwargs):

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
             - roundtime:   (int)    round timesteps - default is 0,
                                     can be 0, 10 (round to 10microsec),
                                     100 (round to 100microsec),1000 (round to 1millisec)
                                     Rounding is can be necessary as MagPy uses date2num and num2date methods:
                                     Accuracy of this methods is between 1 micro and 1 milli sec, sometimes an
                                     error of a few microseconds is obtained

        REQUIRES:
            dbdatainfo

        RETURNS:
            --

        EXAMPLE:
            db.write(stream,mode='replace')

            # Writing data without any header info (does not update SENSORS, STATIONS)
            # DATAINFO is updated however, keeping blanks for sensorid and stationid
            db.write(stream,tablename='myid_0001_0001',noheader=True)

        APPLICATION:
            db = mysql.connect (host = "localhost",user = "user",passwd = "secret",db = "mysql")
            stream = read('/path/to/my/files/*', starttime='2013-01-01',endtime='2013-02-01')
            writeDB(db,stream,StationID='MyObsCode')

        """

        if not self.db:
            loggerdatabase.error("write: No database connected - aborting -- please create and initiate a database first")
            return

        if not len(datastream.ndarray[0]) > 0:
            print ("no data found")
            return

        if not roundtime in [False,None,0,10,100,1000]:
            roundtime = False

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
                print ("write: No SensorID provided in header - define by datastream.header['SensorID'] = 'YourID' before calling writeDB - aborting")
                return
            if not 'StationID' in datastream.header and not StationID:
                #loggerdatabase.error("writeDB: No StationID provided - use option StationID='MyID'")
                print ("write: No StationID provided - use option StationID='MyStationID'")
                return

            # Check data sampling rate:
            # #########################
            # 1. Get sampling rate of sequence to be written (will be 0 if only one record is provided)
            rsr = datastream.samplingrate()

            # Updating DATAINFO, SENSORS and STATIONS
            # TODO: Absolute function object
            # Current solution: remove it
            datastream.header['DataAbsFunctionObject'] = ''
            tablename = self.datainfo(datastream.header.get('SensorID'), datastream.header, None, datastream.header.get('StationID'))

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

        array = [[] for key in DataStream().KEYLIST]
        for idx,col in enumerate(datastream.ndarray):
            key = DataStream().KEYLIST[idx]
            nosingleelem = True
            if len(col) > 0:
                nantest = False
                if key in DataStream().NUMKEYLIST:
                    col = col.astype(np.float64)
                    # First test for nans, as this is not easily possible in arrays because nan != nan
                    if np.isnan(np.array(col)).all():
                        # checking whether only nans are present
                        array[idx] = np.asarray([])
                        nosingleelem = False
                        nantest = True
                if not False in checkEqual3(col) and not nantest:
                    # checking for identical elements
                    # TODO Unicode equal comparison in the following - see whether error still present after Jan2019
                    if not keepempty and not col[0] or col[0] in ['nan', '-','']: #remove place holders
                        array[idx] = np.asarray([])
                        nosingleelem = False
            if key.endswith('time') and len(col) > 0 and nosingleelem:
                array[idx]=np.asarray(col.astype(datetime))
            elif len(col) > 0 and nosingleelem: # and KEYLIST[idx] in NUMKEYLIST:
                array[idx] = [el if isinstance(el, basestring) or el in [None] else float(el) for el in datastream.ndarray[idx]] # converts float64 to float-pymsqldb (required for python3 and pymsqldb)
                try:
                    array[idx] = [None if np.isnan(el) else el for el in array[idx]]
                except:
                    pass # will fail for strings

        keys = np.asarray([DataStream().KEYLIST[idx] for idx,elem in enumerate(array) if len(elem)>0])
        array = np.asarray([elem for elem in array if len(elem)>0], dtype=object)
        dollarstring = ['%s' for elem in keys]

        values = array.transpose()

        insertmanysql = "INSERT INTO %s(%s) VALUES (%s)" % (tablename, ', '.join(keys), ', '.join(dollarstring))

        values = tuple([tuple(list(val)) for val in values])

        # ----------------------------------------------
        #   if tablename does not yet exist create table/ add column if not yet existing
        # ----------------------------------------------
        cursor = self.db.cursor ()

        count = 0
        dataheads,collst,unitlst = [],[],[]
        for key in DataStream().KEYLIST:
            colstr = ''
            unitstr = ''
            if key in keys:
                if key in DataStream().NUMKEYLIST:
                    dataheads.append(key + ' DOUBLE')
                elif key.endswith('time'):
                    if key == 'time':
                        dataheads.append(key + ' DATETIME(6) NOT NULL PRIMARY KEY')
                    else:
                        dataheads.append(key + ' DATETIME(6)')
                else:
                    dataheads.append(key + ' CHAR(100)')
                ## Getting column and units
                for hkey in datastream.header:
                    if key == hkey.replace('col-',''):
                        colstr = datastream.header[hkey]
                    elif key == hkey.replace('unit-col-',''):
                        unitstr = datastream.header[hkey]

                sql = "SELECT " + key + " FROM " + tablename + " ORDER BY time DESC LIMIT 1"
                try:
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
                        print ("writeDB: unknown MySQL error when checking for existing tables! SQL: {}, error: {}".format(sql,emsg))
                except:
                    print ("writeDB: unknown error when checking for existing tables")

            if not key=='time':
                collst.append(colstr)
                unitlst.append(unitstr)

        # override collst/unitlst with contents in header (ColumnContents, UnitContents)
        if not datastream.header.get('ColumnContents','') == '':
            collst = datastream.header.get('ColumnContents').split(',')
        if not datastream.header.get('ColumnUnits','') == '':
            unitlst = datastream.header.get('ColumnUnits').split(',')

        if count == 0:
            print ("Table not existing - creating it")
            # Creating table
            createdatatablesql = "CREATE TABLE IF NOT EXISTS %s (%s)" % (tablename,', '.join(dataheads))
            cursor.execute(createdatatablesql)


        # ----------------------------------------------
        #   upload data
        # ----------------------------------------------

        #print insertmanysql
        if mode == 'replace':
            insertmanysql = insertmanysql.replace("INSERT","REPLACE")

        #t1 = datetime.now(timezone.utc).replace(tzinfo=None)

        ## Alternative upload for very large lists (from 0.4.6 on)
        START_INDEX = 0
        LIST_LENGTH=1000
        errorfound = True
        while values[START_INDEX:START_INDEX+LIST_LENGTH]:
            try:
                cursor.executemany(insertmanysql,values[START_INDEX:START_INDEX+LIST_LENGTH])
                errorfound = False
            except mysql.Error as e:
                emsg = str(e)
                print ("write: mysql error when writing - {}".format(emsg))
            except:
                print ("write: unknown error when checking for existing tables")
            START_INDEX += LIST_LENGTH

        # ----------------------------------------------
        #   update DATAINFO - move to a separate method
        # ----------------------------------------------

        if not errorfound:
            self.set_times_in_datainfo(tablename,','.join(collst),','.join(unitlst))

        self.db.commit()
        cursor.close()



if __name__ == '__main__':

    print()
    print("----------------------------------------------------------")
    print("TESTING: Database PACKAGE")
    print("THIS IS A TEST RUN OF THE MAGPY.CORE DATABASE PACKAGE.")
    print("All main methods will be tested. This may take a while.")
    print("If errors are encountered they will be listed at the end.")
    print("Otherwise True will be returned")
    print("----------------------------------------------------------")
    print()
    print("----------------------------------------------------------")
    print("IMPORTANT:")
    print("Tests can only be performed if mysql is installed")
    print("and an empty testing database with the following parameters")
    print("is available on localhost: ")
    print("user: maxmustermann, passwd: geheim, databasename: testdb")
    print("DO NOT YOU THIS DB FOR ANYTHING ELSE EXCEPT TESTING!")
    print("----------------------------------------------------------")
    print()

    # #######################################################
    #                     Runtime testing
    # #######################################################
    from magpy.stream import read,example5

    def create_teststream(startdate=datetime(2022, 11, 21), coverage=86400):
        # Create a random data signal with some nan values in x and z
        c = 1000  # 1000 nan values are filled at random places
        l = coverage
        array = [[] for el in DataStream().KEYLIST]
        import scipy
        win = scipy.signal.windows.hann(60)
        a = np.random.uniform(20950, 21000, size=int((l + 2880) / 2))
        b = np.random.uniform(20950, 21050, size=int((l + 2880) / 2))
        x = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        array[1] = np.asarray(x[1440:-1440])
        a = np.random.uniform(1950, 2000, size=int((l + 2880) / 2))
        b = np.random.uniform(1900, 2050, size=int((l + 2880) / 2))
        y = scipy.signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
        array[2] = np.asarray(y[1440:-1440])
        a = np.random.uniform(44300, 44400, size=(l + 2880))
        z = scipy.signal.convolve(a, win, mode='same') / sum(win)
        array[3] = np.asarray(z[1440:-1440])
        array[4] = np.asarray(np.sqrt((x * x) + (y * y) + (z * z))[1440:-1440])
        var1 = [0] * l
        var1[43200:50400] = [1] * 7200
        varind = DataStream().KEYLIST.index('var1')
        array[varind] = np.asarray(var1)
        array[0] = np.asarray([startdate + timedelta(seconds=i) for i in range(0, l)])
        teststream = DataStream(header={'SensorID': 'Test_0001_0001'}, ndarray=np.asarray(array, dtype=object))
        teststream.header['col-x'] = 'X'
        teststream.header['col-y'] = 'Y'
        teststream.header['col-z'] = 'Z'
        teststream.header['col-f'] = 'F'
        teststream.header['unit-col-x'] = 'nT'
        teststream.header['unit-col-y'] = 'nT'
        teststream.header['unit-col-z'] = 'nT'
        teststream.header['unit-col-f'] = 'nT'
        teststream.header['col-var1'] = 'Switch'
        teststream.header['StationID'] = 'TST'
        return teststream

    teststream1 = create_teststream(startdate=datetime(2022, 11, 22))
    teststream2 = create_teststream(startdate=datetime(2022, 11, 23))

    ok = True
    errors = {}
    successes = {}
    if ok:
        #testrun = './testflagfile.json' # define a test file later on
        t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)
        while True:
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db = DataBank("localhost","maxmustermann","geheim","testdb")
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['__init__'] = ("Version: {}: __init__ {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['__init__'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with __init__.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db.dbinit()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['dbinit'] = ("Version: {}: dbinit {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['dbinit'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with dbinit.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db.alter()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['alter'] = ("Version: {}: alter {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['alter'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with alter.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db.write(teststream1)
                db.write(teststream2)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['write'] = ("Version: {}, write: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['write'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with write.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db.info('stdout')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['info'] = ("Version: {}, info: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['info'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with info.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db.sensorinfo('Test_0001_0001', {'SensorName': 'BestSensorontheGlobe'})
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['sensorinfo'] = ("Version: {}, sensorinfo: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['sensorinfo'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with sensorinfo.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                stationid = db.get_string('DATAINFO', 'Test_0001_0001', 'StationID')
                print(stationid)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['get_string'] = ("Version: {}, get_string: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['get_string'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with get_string.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                tablename = db.datainfo(teststream1.header.get('SensorID'), {'DataComment': 'Add something'}, None,
                                        stationid)
                print(tablename)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['datainfo'] = ("Version: {}, datainfo: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['datainfo'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with datainfo.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                data = db.get_lines(tablename, 1000)
                print(len(data))
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['get_lines'] = ("Version: {}, get_lines: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['get_lines'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with get_lines.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                sr = db.get_float('DATAINFO', teststream1.header.get('SensorID'), 'DataSamplingRate')
                print (sr)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['get_float'] = ("Version: {}, get_float: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['get_float'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with get_float.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                if db.tableexists('Test_0001_0001_0001'):
                    print (" Yes, this table exists")
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['tableexists'] = ("Version: {}, tableexists: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['tableexists'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with tableexists.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db.update('SENSORS', ['SensorGroup'], ['magnetism'], condition='SensorID="Test_0001_0001"')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['update'] = ("Version: {}, updatet: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['update'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with update.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                magsenslist = db.select('SensorID', 'SENSORS', 'SensorGroup = "magnetism"')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['select'] = ("Version: {}, select: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['select'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with select.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                value = db.get_pier('P2','P1','deltaF')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['get_pier'] = ("Version: {}, get_pier: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['get_pier'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with get_pier.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                value = db.get_baseline('LEMI036_2_0001', date="2021-01-01")
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['get_baseline'] = ("Version: {}, get_baseline: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['get_baseline'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with get_baseline.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                (long, lat) = db.coordinates('P1')
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['coordinates'] = ("Version: {}, coordinates: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['coordinates'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with coordinates.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                data = db.read('Test_0001_0001_0001')
                data = db.read('Test_0001_0001_0001', starttime='2022-11-22T08:00:00', endtime='2022-11-22T10:00:00')
                # test with all options sql, starttime, endtime
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['read'] = ("Version: {}, read: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['read'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with read.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db.update_datainfo('Test_0001_0001_0001', data.header)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['read'] = ("Version: {}, read: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['read'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with read.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db.delete('Test_0001_0001_0001', timerange=1)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['read'] = ("Version: {}, read: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['read'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with read.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                from magpy.core import flagging
                fl = flagging.Flags()
                fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T23:56:12.654362",endtime="2022-11-22T23:59:12.654362",components=['x','y','z'],operator='RL',debug=False)
                fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T21:56:12.654362",endtime="2022-11-22T21:59:12.654362",components=['x','y','z'],debug=False)
                fl = fl.add(sensorid="LEMI025_X56878_0002_0001",starttime="2022-11-22T19:56:12.654362",endtime="2022-11-22T19:59:12.654362",components=['x','y','z'],groups={'magnetism':['x','y','z','f'],'LEMI':['x','y','z']}, debug=False)
                db.flags_to_db(fl)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['flags_to_db'] = ("Version: {}, flags_to_db: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['flags_to_db'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with flags_to_db.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                db.flags_to_delete(parameter="operator", value="RL")
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['flags_to_delete'] = ("Version: {}, flags_to_delete: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['flags_to_delete'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with flags_to_delete.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                fl = db.flags_from_db()
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['flags_from_db'] = ("Version: {}, flags_from_db: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['flags_from_db'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with flags_from_db.")
            try:
                ts = datetime.now(timezone.utc).replace(tzinfo=None)
                data = read(example5)
                db.dict_to_fields(data.header)
                te = datetime.now(timezone.utc).replace(tzinfo=None)
                successes['dict_to_fields'] = ("Version: {}, dict_to_fields: {}".format(magpyversion,(te-ts).total_seconds()))
            except Exception as excep:
                errors['dict_to_fields'] = str(excep)
                print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR with dict_to_fields.")

            # If end of routine is reached... break.
            break

        t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
        time_taken = t_end_test - t_start_test
        print(datetime.now(timezone.utc).replace(tzinfo=None), "- Database runtime testing completed in {} s. Results below.".format(time_taken.total_seconds()))

        print()
        print("----------------------------------------------------------")
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
