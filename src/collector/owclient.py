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
        self.db = MySQLdb.connect("localhost","cobs","8ung2rad","wic" )
        # prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()
        # Initiate subscriptions
        self.line = []
        self.subscribeInst(self.db, self.cursor, clientname)


    def subscribeInst(self, db, cursor, client):
        self.prefix("ow", "http://example.com/" + client +"/ow#")
        sql = "SELECT SensorID, SensorDescription, SensorModule, SensorKeys FROM SENSORS WHERE SensorModule = 'ow'"
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
                 module = row[2]
                 param = row[3]
                 print row, len(param.split(',')) 
                 #datainfoid = (db,cursor,sensid,sensdesc)
                 #cursor.execute("DROP TABLE %s" % sensid)
                 # Create Sensor Table if it does not yet exist
                 # TODO: check the length of param for other then temperatur data 
                 if len(param.split(',')) == 1:
                     createtable = "CREATE TABLE IF NOT EXISTS %s (time  CHAR(40) NOT NULL PRIMARY KEY, t1 FLOAT, var1 FLOAT, var2 FLOAT, var3 FLOAT, var4 FLOAT, flag CHAR(100), typ CHAR(100))" % (sensid+'_0001')
                 if len(param.split(',')) == 3:
                     createtable = "CREATE TABLE IF NOT EXISTS %s (time  CHAR(40) NOT NULL PRIMARY KEY, t1 FLOAT, var1 FLOAT, var2 FLOAT, var3 FLOAT, var4 FLOAT, flag CHAR(100), typ CHAR(100))" % (sensid+'_0001')
                 if len(param.split(',')) == 5:
                     createtable = "CREATE TABLE IF NOT EXISTS %s (time  CHAR(40) NOT NULL PRIMARY KEY, t1 FLOAT, var1 FLOAT, var2 FLOAT, var3 FLOAT, var4 FLOAT, flag CHAR(100), typ CHAR(100))" % (sensid+'_0001')
                 try:
                     print createtable
                     cursor.execute(createtable)
                 except:
                     log.msg("Table exists already.")
                 subscriptionstring = "%s:%s-value" % (module, sensid.replace('OW_','').strip('_0001'))
                 print subscriptionstring
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
        print "getting events"
        try:
            sensorid = topicUri.split('/')[-1].split('-')[0].split('#')[1]
            print sensorid
            if eventdict['id'] == 8:
                time = eventdict['value']
            elif eventdict['id'] == 5:
                temp = eventdict['value']
            elif eventdict['id'] == 7:
                rh = eventdict['value']
            elif eventdict['id'] == 12:
                vdd = eventdict['value']
            elif eventdict['id'] == 13:
                vad = eventdict['value']
            elif eventdict['id'] == 14:
                vis = eventdict['value']
            elif eventdict['id'] == 99:
                eol = eventdict['value']
            if eol == '':
                if eventdict['id'] in [5,7,8,12,13,14]: # replace by some eol parameter
                     self.line.append(eventdict['value'])
            else:
                print len(self.line)
                # get available sensor parameters from db
                #query = "SELECT * FROM SENSORS WHERE SENSOR_ID = sensorid;"
                # TODO: generalize the information function below (maybe by parameters provided in DB)
                if sensorid in ["3AD754010000","0EB354010000"]:
                    sql = "INSERT INTO %s(time, t1, var2, var3, var4, flag, typ) VALUES ('%s', %f, %f, %f, %f, '0000000000000000-', 'ow')" % (sensorid, self.line[0], self.line[1], self.line[3], self.line[4], self.line[5])
                elif sensorid == "05CE54010000" or sensorid == "CBC454010000":
                    sql = "INSERT INTO %s(time, t1, var1, var2, flag, typ) VALUES ('%s', %f, %f, %f, '0000000000000000-', 'ow')" % (sensorid, self.line[0], self.line[1], self.line[2], self.line[3])
                else:
                    sql = "INSERT INTO %s(time, t1, flag, typ) VALUES ('%s', %f, '0000000000000000-', 'ow')" % (sensorid, self.line[0], self.line[1])
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
            print "No datainfo so far - use dbinit to generate"
        getlastnumsql = 'SELECT DataID FROM DATAINFO WHERE SensorID LIKE "%s%"' % sensorid
        cursor.execute(getlastnumsql)
        rows = cursor.fetchall()
        if not len(rows) >= 1:
            stationid = 'WIC'
            datainfohead = 'DataID, SensorID, StationID'
            datainfovalue = '"' + sensorid + '", "' + sensorid + '", "' + stationid + '"'
            datainfosql = "INSERT INTO DATAINFO(%s) VALUES (%s)" % (datainfohead, datainfovalue)
            cursor.execute(datainfosql)

 

