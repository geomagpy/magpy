import sys
from twisted.python import log
from twisted.internet import reactor
from autobahn.websocket import connectWS
from autobahn.wamp import WampClientFactory, WampClientProtocol
# For converting Unicode text
import collections
# Database
import MySQLdb

clientname = 'default'
#clientname

def defineclient(cname):
    global clientname
    clientname = cname
    return

class PubSubClient(WampClientProtocol):
    """
    Class for OneWire communication
    """
    def onSessionOpen(self):
        global clientname
        log.msg("Starting " + clientname + " session")
        # Open database connection
        self.db = MySQLdb.connect("localhost","cobs","passwd","wic" )
        # prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()
        # Initiate subscriptions
        self.line = []
        self.subscribeInst(self.db, self.cursor, clientname)


    def subscribeInst(self, db, cursor, client):
        self.prefix("pos1", "http://example.com/" + client +"/pos1#")
        sql = "SELECT SensorID, SensorDescription, SensorDataLogger, SensorKeys FROM SENSORS WHERE SensorDataLogger = 'POS1'"
        try:
            # Execute the SQL command
            cursor.execute(sql)
            # Fetch all the rows in a list of lists.
            results = self.cursor.fetchall()
            if len(results) < 1:
                 # Initialize ow-table
                 log.msg("No sensors registered so far: Please use something like the following command in your main prog:")
                 log.msg("You need to define a SensorID in the database first so that the program access the proper websocket id:")
                 log.msg("INSERT INTO SENSORS(SensorID, SensorDescription, SensorModule, SensorElements, SensorKeys, SensorType, SensorGroup) VALUES ('OW_332988040000', 'Mobil', 'ow', 'T', 't1', 'DS18B20', 'Environment'")
            for row in results:
                 sensid = str(row[0])
                 sensdesc = row[1]
                 module = row[2].lower()
                 param = row[3]
                 print row, len(param.split(','))
                 #self.checkDB4DataInfo(db,cursor,sensid,sensdesc)
                 #cursor.execute("DROP TABLE %s" % sensid)
                 # Create Sensor Table if it does not yet exist  # TODO: check the length of param for other then temperatur data
                 #createtable = "CREATE TABLE IF NOT EXISTS %s (time  CHAR(40) NOT NULL PRIMARY KEY, f FLOAT, df FLOAT, var2 FLOAT, flag CHAR(100), typ CHAR(100))" % (sensid)
                 #try:
                 #    cursor.execute(createtable)
                 #except:
                 #    log.msg("Table exists already.")
                 subscriptionstring = "%s:%s-value" % (module, sensid)
                 self.subscribe(subscriptionstring, self.onEvent)
                 # Now print fetched result
                 print "sensid=%s,sensdesc=%s,module=%s,param=%s" % \
                            (sensid, sensdesc, module, param )
        except:
            print "Error: unable to fetch data"


    def convertUnicode(self, data):
        # From RichieHindle
        if isinstance(data, unicode):
            return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(self.convertUnicode, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(self.convertUnicode, data))
        else:
            return data

    def onEvent(self, topicUri, event):
        eventdict = self.convertUnicode(event)
        time = ''
        eol = ''
        try:
            sensorid = topicUri.split('/')[-1].split('-')[0].split('#')[1]
            if eventdict['id'] == 99:
                eol = eventdict['value']
            if eol == '':
                if eventdict['id'] in [1,10,14,40]: # replace by some eol parameter
                     self.line.append(eventdict['value'])
            else:
                datainfoid = sensorid +'_0001'
                sql = "INSERT INTO %s(time, f, var1, df, flag, typ) VALUES ('%s', %f, %f, %f, '0000000000000000-', 'xyz')" % (datainfoid, self.line[0], self.line[1], self.line[2], self.line[3])
                self.line = []
                # Prepare SQL query to INSERT a record into the database.
                try:
                    # Execute the SQL command
                    self.cursor.execute(sql)
                    #print "data appended"
                    # Commit your changes in the database
                    self.db.commit()
                except:
                    # Rollback in case there is any error
                    self.db.rollback()
        except:
            pass


    def checkDB4DataInfo(self,db,cursor,sensorid,pier):

        # DATAINFO TABLE
        # Test whether a datainfo table is already existing
        # if not create one with first number
        checkdatainfoexists = 'SHOW TABLES LIKE "DATAINFO"'
        cursor.execute(checkdatainfoexists)
        rows = cursor.fetchall()
        if len(rows) < 1:
            datainfostr = 'DataID CHAR(50) NOT NULL PRIMARY KEY, SensorID CHAR(50), ColumnContents TEXT, ColumnUnits TEXT, DataFormat CHAR(20),DataMinTime CHAR(50), DataMaxTime CHAR(50), DataSamplingFilter CHAR(100), DataDigitalSampling CHAR(100), DataComponents CHAR(10), DataSamplingRate CHAR(100), DataType CHAR(100), DataDeltaX DECIMAL(20,9), DataDeltaY DECIMAL(20,9), DataDeltaZ DECIMAL(20,9),DataDeltaF DECIMAL(20,9),DataDeltaReferencePier CHAR(20),DataDeltaReferenceEpoch CHAR(50),DataScaleX DECIMAL(20,9),DataScaleY DECIMAL(20,9),DataScaleZ DECIMAL(20,9),DataScaleUsed CHAR(2),DataSensorOrientation CHAR(10),DataSensorAzimuth DECIMAL(20,9),DataSensorTilt DECIMAL(20,9), DataAngularUnit CHAR(5),DataPier CHAR(20),DataAcquisitionLatitude DECIMAL(20,9), DataAcquisitionLongitude DECIMAL(20,9), DataLocationReference CHAR(20), DataElevation DECIMAL(20,9), DataElevationRef CHAR(10), DataFlagModification CHAR(50), DataAbsFunc CHAR(20), DataAbsDegree CHAR(10), DataAbsKnots CHAR(10), DataAbsMinTime CHAR(50), DataAbsMaxTime CHAR(50), DataAbsDate CHAR(50), DataRating CHAR(10), DataComments TEXT'
            createdatainfotablesql = "CREATE TABLE IF NOT EXISTS DATAINFO (%s)" % datainfostr
            cursor.execute(createdatainfotablesql)

        getlastnumsql = 'SELECT DataID FROM DATAINFO WHERE SensorID LIKE "%s"' % sensorid
        cursor.execute(getlastnumsql)
        rows = cursor.fetchall()
        if not len(rows) >= 1:
            datainfohead = 'DataID, SensorID, DataPier'
            datainfovalue = '"' + sensorid + '", "' + sensorid + '", "' + pier + '"'
            datainfosql = "INSERT INTO DATAINFO(%s) VALUES (%s)" % (datainfohead, datainfovalue)
            cursor.execute(datainfosql)

