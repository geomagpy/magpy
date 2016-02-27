# Twisted Client

import sys

from twisted.python import log
from twisted.internet import reactor

from autobahn.websocket import connectWS
from autobahn.wamp import WampClientFactory, WampClientProtocol

# For converting Unicode text
import collections

import collector.envclient as env
import collector.gsmclient as gsm

import MySQLdb

clientname = 'default'

def Dict2SQL(db, dictionary, deleteSensor=None, deleteStation=None):
    """
    Fuction to add dictionary information to a database
    Only build Sensor and Station tables
    """
    sensorhead, sensorvalue = [],[]
    stationhead, stationvalue = [],[]

    cursor = db.cursor()

    if deleteSensor:
        cursor.execute("DROP TABLE IF EXISTS SENSORS")
    if deleteStation:
        cursor.execute("DROP TABLE IF EXISTS STATIONS")

    createtable = """CREATE TABLE IF NOT EXISTS SENSORS (
                      SensorID  CHAR(50) NOT NULL PRIMARY KEY,
                      SensorName CHAR(100),
                      SensorType CHAR(100),
                      SensorGroup CHAR(100),
                      SensorSerialNum CHAR(50),
                      SensorDataLogger CHAR(100),
                      SensorDataLoggerSerNum CHAR(100),
                      SensorDataLoggerRevision CHAR(100),
                      SensorDataLoggerRevisionComment CHAR(100),
                      SensorDescription  CHAR(100),
                      SensorElements  CHAR(100),
                      SensorKeys  CHAR(100),
                      SensorModule  CHAR(10),
                      SensorDate  CHAR(10),
                      SensorRevision  CHAR(10),
                      SensorRevisionComment  CHAR(100),
                      SensorRevisionDate  CHAR(100))"""

    for key in dictionary:
        if key.startswith('Sensor'):
            if key == "SensorID":
                sensorid = dictionary[key]
                sensorhead.append(key)
            else:
                sensorhead.append(key)
            sensorvalue.append(dictionary[key])
        elif key.startswith('Data'):
            pass
        elif key.startswith('Station'):
            if key == "StationID":
                stationid = dictionary[key]
                stationhead.append(key)
            else:
                if not key == "Station":
                    stationhead.append(key)
            if not key == "Station":
                stationvalue.append(dictionary[key].replace('http://',''))

    # SENSOR TABLE
    # Create sensor table input
    headstr = ' CHAR(100), '.join(sensorhead) + ' CHAR(100)'
    headstr = headstr.replace('SensorID CHAR(100)', 'SensorID CHAR(50) NOT NULL PRIMARY KEY')
    createsensortablesql = "CREATE TABLE IF NOT EXISTS SENSORS (%s)" % headstr
    sensorsql = "INSERT IGNORE INTO SENSORS(%s) VALUES (%s)" % (', '.join(sensorhead), '"'+'", "'.join(sensorvalue)+'"')

    # STATION TABLE
    # Create station table input
    stationstr = 'StationID CHAR(50) NOT NULL PRIMARY KEY, StationName CHAR(100), StationIAGAcode CHAR(10), StationInstitution CHAR(100), StationStreet CHAR(50), StationCity CHAR(50), StationPostalCode CHAR(20), StationCountry CHAR(50), StationWebInfo CHAR(100), StationEmail CHAR(100), StationDescription TEXT'
    createstationtablesql = "CREATE TABLE IF NOT EXISTS STATIONS (%s)" % stationstr
    stationsql = "INSERT IGNORE INTO STATIONS(%s) VALUES (%s)" % (', '.join(stationhead), '"'+'", "'.join(stationvalue)+'"')

    try:
        cursor.execute(createsensortablesql)
        #db.commit()
    except:
        log.msg("Sensortable exists already.")
        #db.rollback()

    try:
        cursor.execute(createstationtablesql)
        #db.commit()
    except:
        log.msg("Stationtable exists already.")
        #db.rollback()

    # Upload data and close db
    print sensorid

    cursor.execute(sensorsql)
    cursor.execute(stationsql)
    db.commit()
    cursor.close()



if __name__ == '__main__':

    log.startLogging(sys.stdout)
    #debug = len(sys.argv) > 1 and sys.argv[1] == 'debug'
    '''
    db = MySQLdb.connect (host = "localhost",
                            user = "cobs",
                            passwd = "8ung2rad",
                            db = "wic")

    import collector.all_instruments as inst
    #Dict2SQL(db, inst.didddict,deleteSensor=True,deleteStation=True)
    Dict2SQL(db, inst.fgedict)
    #Dict2SQL(db, inst.lemi36dict)
    #Dict2SQL(db, inst.posdicta)
    #Dict2SQL(db, inst.lemi22dict)
    #Dict2SQL(db, inst.cs1dict)
    #Dict2SQL(db, inst.cs2dict)
    #Dict2SQL(db, inst.gsm90dicta)
    #Dict2SQL(db, inst.gsm90dictb)
    #Dict2SQL(db, inst.lemidict)

    db.close()

    x=1/0
    '''
    db = MySQLdb.connect (host = "localhost",
                            user = "cobs",
                            passwd = "8ung2rad",
                            db = "wic")
    cursor = db.cursor ()
    cursor.execute ("SELECT VERSION()")
    row = cursor.fetchone ()
    print "MySQL server version:", row[0]


    # Ow Wire Server:
    # Initial run:
    #sql = "INSERT INTO SENSORS(SensorID, SensorDescription, SensorModule, SensorElements, SensorKeys, SensorType, SensorGroup) VALUES ('332988040000', 'Mobil', 'ow', 'T', 't1', 'DS18B20', 'Environment')"
    #cursor.execute(sql)
    #db.commit()

    cursor.close ()
    db.close ()

    clientname = 'kallisto'
    factory1 = WampClientFactory("ws://138.22.188.183:9100", debugWamp = False)
    gsm.defineclient(clientname)
    factory1.protocol = gsm.PubSubClient
    factory2 = WampClientFactory("ws://138.22.188.183:9100", debugWamp = False)
    env.defineclient(clientname)
    factory2.protocol = env.PubSubClient

    connectWS(factory1)
    connectWS(factory2)

    reactor.run()

    try:
        cursor.close()
        db.close()
        log.msg("DB closed")
    except:
        pass
