import sys
from twisted.python import log
from twisted.internet import reactor
try: # version > 0.8.0
    from autobahn.wamp1.protocol import WampClientFactory, WampClientProtocol
except:
    from autobahn.wamp import WampClientFactory, WampClientProtocol
# For converting Unicode text
import collections
# Database
import MySQLdb

clientname = 'default'

def defineclient(cname):
    global clientname
    clientname = cname
    return

IDDICT = {0:'clientname',1:'time',2:'date',3:'time',4:'time',5:'coord',
          10:'f',11:'x',12:'y',13:'z',14:'df',
          30:'t1',31:'t1',32:'t2',33:'var1',34:'t2',40:'var2'}

def timeToArray(timestring):
    # Converts time string of format 2013-12-12T23:12:23.122324
    # to an array similiat to a datetime object
    try:
        splittedfull = timestring.split(' ')
        splittedday = splittedfull[0].split('-')
        splittedsec = splittedfull[1].split('.')
        splittedtime = splittedsec[0].split(':')
        datearray = splittedday + splittedtime
        datearray.append(splittedsec[1])
        datearray = map(int,datearray)
        return datearray
    except:
        log.msg('Error while extracting time array')
        return []

def dataToFile(outputdir, sensorid, filedate, bindata, header):
    # File Operations
    try:
        hostname = socket.gethostname()
        path = os.path.join(outputdir,hostname,sensorid)
        # outputdir defined in main options class
        if not os.path.exists(path):
            os.makedirs(path)
        savefile = os.path.join(path, sensorid+'_'+filedate+".bin")
        if not os.path.isfile(savefile):
            with open(savefile, "wb") as myfile:
                myfile.write(header + "\n")
                myfile.write(bindata + "\n")
        else:
            with open(savefile, "a") as myfile:
                myfile.write(bindata + "\n")
    except:
        log.err("OW - Protocol: Error while saving file")

class PubSubClient(WampClientProtocol):
    """
    Class for communication with moon
    """

    """
    #eventually add an init method - doesnt' work this way
    def __init__(self, stationid, clientname, instrumenttag, idlist, output=None, sensorid=None):
        self.clientname = clientname
        self.stationid = stationid
        self.short = instrumenttag
        self.idlist = idlist
        self.typ = 'f' # or xyzf or etc
        if not output:
            self.output = 'db' # can be either 'db' or 'file', if not db, then file is used
        if not sensorid and output=='file':
            self.sensorid = 'MySensor' # Necessary for file output
    """

    def onSessionOpen(self):
        global clientname
        log.msg("Starting " + clientname + " session")
        # TODO Make all the necessary parameters variable
        # Basic definitions to change
        self.stationid = 'WIC'
        self.output = 'db' # can be either 'db' or 'file', if not db, then file is used
        self.short = 'cs' # can be either 'db' or 'file', if not db, then file is used
        self.idlist = [1,10]
        self.sensorid = 'MySensor' # Necessary for file output
        self.typ = 'f' # or xyzf or etc
        # Open database connection
        self.db = MySQLdb.connect("localhost","cobs","passwd","wic" )
        # prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()
        # Initiate subscriptions
        self.line = []
        self.subscribeInst(self.db, self.cursor, clientname, self.output)

    def subscribeInst(self, db, cursor, client, output):
        self.prefix(self.short, "http://example.com/" + client +"/"+self.short+"#")
        # Unique for cs --- TODO change that
        if self.short == 'cs':
             sensshort = 'G82'
        else:
             sensshort = self.short.upper()

        if output == 'db':
            # #####################################
            # SUBSCRIBEINST: A) Check whether appropriate Sensor information exists in database (output db only)
            # #####################################
            sql = "SELECT SensorID, SensorDescription, SensorDataLogger, SensorKeys FROM SENSORS WHERE SensorID LIKE 'G82%'"

            print "Testing SQL1", sql
            print "------------"

            try:
                # Execute the SQL command
                cursor.execute(sql)
            except:
                log.msg("client: Unable to execute SENSOR sql")
            try:
                # Fetch all the rows in a list of lists.
                results = self.cursor.fetchall()
            except:
                log.msg("client: Unable to fetch SENSOR data from DB")
                results = []

            if len(results) < 1:
                 # Initialize e.g. ow table
                 log.msg("No sensors registered so far: Please use something like the following command in your main prog:")
                 log.msg("You need to define a SensorID in the database first so that the program access the proper websocket id:")
                 log.msg("INSERT INTO SENSORS(SensorID, SensorDescription, SensorModule, SensorElements, SensorKeys, SensorType, SensorGroup) VALUES ('OW_332988040000', 'Mobil', 'ow', 'T', 't1', 'DS18B20', 'Environment'")
                 # TODO Throw an exception here

            try:
                # #####################################
                # SUBSCRIBEINST: B) Obtain sensor information from DB
                # #####################################
                for row in results:
                     sensid = str(row[0])
                     sensdesc = row[1]
                     try:
                         module = row[2].lower()[:3]
                     except:
                         module = 'env'
                     param = row[3]
                     print row, len(param.split(','))

                     # #####################################
                     # SUBSCRIBEINST: C) For each SensorID create a DATAINFO line and an empty data table
                     # TODO this requires some knowledge on provided data
                     # #####################################

                     self.checkDB4DataInfo(db,cursor,sensid,self.stationid,param)

                     # Exception for G823 Caesium mag
                     if module == 'g82':
                         module = 'cs'

                     subscriptionstring = "%s:%s-value" % (module, sensid)
                     self.subscribe(subscriptionstring, self.onEvent)
                     # Now print fetched result
                     print "sensid=%s,sensdesc=%s,module=%s,param=%s, subscript to %s" % \
                                (sensid, sensdesc, module, param, subscriptionstring )
            except:
                log.msg("client: Unable to subscribe to data stream")

        elif output == 'file':
            # #####################################
            # SUBSCRIBEINST: D) subscribe directly
            # #####################################
            subscriptionstring = "%s:%s-value" % (self.short, self.sensorid)
            self.subscribe(subscriptionstring, self.onEvent)

        else:
            log.msg("client: invalid output format selected")

    def onEvent(self, topicUri, event):
        eventdict = self.convertUnicode(event)
        time = ''
        eol = ''
        try:
            sensorid = topicUri.split('/')[-1].split('-')[0].split('#')[1]
            #print sensorid
            if eventdict['id'] == 99:
                eol = eventdict['value']
            if eol == '':
                if eventdict['id'] in self.idlist: # replace by some eol parameter
                     self.line.append(eventdict['value'])
            else:
                paralst = []
                for elem in self.idlist:
                    var = IDDICT[elem]
                    if var == 'time' and time in paralst:
                        var = 'sectime'
                    paralst.append(var)
                if self.output == 'db':
                    datainfoid = sensorid+'_0001'
                    # define insert from provided param
                    parastr = ', '.join(paralst)
                    # separate floats and string
                    nelst = []
                    for elem in self.line:
                        if isinstance(elem, str):
                            elem = "'"+elem+"'"
                        nelst.append(elem)
                    linestr = ', '.join(map(str, nelst))
                    sql = "INSERT INTO %s(%s, flag, typ) VALUES (%s, '0000000000000000-', %s)" % (datainfoid, parastr, linestr, self.typ)
                    self.line = []
                    # Prepare SQL query to INSERT a record into the database.
                    try:
                        # Execute the SQL command
                        self.cursor.execute(sql)
                        # Commit your changes in the database
                        self.db.commit()
                    except:
                        # No regular output here. Otherwise log-file will be smashed
                        #log.msg("client: could not append data to table")
                        # Rollback in case there is any error
                        self.db.rollback()
                elif self.output == 'file':
                    """
                    packcode = '6hL'+len(paralst)*'L'
                    # provide additionall the real names and the units
                    header = "# MagPyBin %s '%s' '%s' %s %s %s %d" % (self.sensor, paralst, paralst, '[nT]', '[1000]', packcode, struct.calcsize(packcode))

                    # If line element f is existing perform the following check
                    try:
                        value = float(data[0].strip('$'))
                        if 10000 < value < 100000:
                            intensity = value
                        else:
                            intensity = 0.0
                    except ValueError:
                        log.err("CS - Protocol: Not a number. Instead found:", data[0])
                        intensity = 88888

                    datearray = timeToArray(timestamp)
                    try:
                        datearray = timeToArray(timestamp)
                        datearray.append(int(intensity*1000))
                        data_bin = struct.pack(packcode,*datearray)
                    except:
                        log.msg('Error while packing binary data')
                        pass

                    # File Operations
                    dataToFile(self.outputdir, self.sensor, filename, data_bin, header)
                    """
                    pass
                else:
                    # Any exception here is caught already before
                    pass
        except:
            # Could not get event
            log.msg("client: event could not be translated")
            pass


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


    def checkDB4DataInfo(self,db,cursor,sensorid,stationid,parameter):

        print "Checking DATAINFO"
        print "#################"
        # DATAINFO TABLE
        # Test whether a datainfo table is already existing
        # if not create one with first number
        checkdatainfoexists = 'SHOW TABLES LIKE "DATAINFO"'
        cursor.execute(checkdatainfoexists)
        rows = cursor.fetchall()
        if len(rows) < 1:
            datainfostr = 'DataID CHAR(50) NOT NULL PRIMARY KEY, SensorID CHAR(50), StationID CHAR(50), ColumnContents TEXT, ColumnUnits TEXT, DataFormat CHAR(20),DataMinTime CHAR(50), DataMaxTime CHAR(50), DataSamplingFilter CHAR(100), DataDigitalSampling CHAR(100), DataComponents CHAR(10), DataSamplingRate CHAR(100), DataType CHAR(100), DataDeltaX DECIMAL(20,9), DataDeltaY DECIMAL(20,9), DataDeltaZ DECIMAL(20,9),DataDeltaF DECIMAL(20,9),DataDeltaReferencePier CHAR(20),DataDeltaReferenceEpoch CHAR(50),DataScaleX DECIMAL(20,9),DataScaleY DECIMAL(20,9),DataScaleZ DECIMAL(20,9),DataScaleUsed CHAR(2),DataSensorOrientation CHAR(10),DataSensorAzimuth DECIMAL(20,9),DataSensorTilt DECIMAL(20,9), DataAngularUnit CHAR(5),DataPier CHAR(20),DataAcquisitionLatitude DECIMAL(20,9), DataAcquisitionLongitude DECIMAL(20,9), DataLocationReference CHAR(20), DataElevation DECIMAL(20,9), DataElevationRef CHAR(10), DataFlagModification CHAR(50), DataAbsFunc CHAR(20), DataAbsDegree INT, DataAbsKnots DECIMAL(20,9), DataAbsMinTime CHAR(50), DataAbsMaxTime CHAR(50), DataAbsDate CHAR(50), DataRating CHAR(10), DataComments TEXT'
            createdatainfotablesql = "CREATE TABLE IF NOT EXISTS DATAINFO (%s)" % datainfostr
            cursor.execute(createdatainfotablesql)

        getlastnumsql = 'SELECT DataID FROM DATAINFO WHERE SensorID LIKE "%s"' % sensorid
        cursor.execute(getlastnumsql)
        rows = cursor.fetchall()
        if not len(rows) >= 1:
            # DATAINFO input for sensors is not yet existing
            # create that and create an empty datatable as well
            datainfoid = sensorid+'_0001'
            datainfohead = 'DataID, SensorID, StationID'
            datainfovalue = '"' + datainfoid + '", "' + sensorid + '", "' + stationid + '"'
            datainfosql = "INSERT INTO DATAINFO(%s) VALUES (%s)" % (datainfohead, datainfovalue)
            cursor.execute(datainfosql)

            # Get the parameters of the dataset
            newparam = []
            for elem in parameter:
                ne = elem + ' FLOAT'
                newparam.append(ne)
            parameterlst = ', '.join(newparam)

            datahead = 'time CHAR(40) NOT NULL PRIMARY KEY, %s, flag CHAR(100), typ CHAR(100)' % parameterlst
            createdatatablesql = "CREATE TABLE IF NOT EXISTS %s (%s)" % (datainfoid,datahead)
            try:
                cursor.execute(createdatatablesql)
                log.msg("Created new data table %s." % datainfoid)
            except:
                log.msg("Data table exists already.")

        self.db.commit()
