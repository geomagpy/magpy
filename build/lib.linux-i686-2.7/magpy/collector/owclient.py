import sys, os, struct
from twisted.python import log
try: # version > 0.8.0
    from autobahn.wamp1.protocol import WampClientProtocol
except:
    from autobahn.wamp import WampClientProtocol
# For converting Unicode text
import collections
# Timing
from datetime import datetime, timedelta
# Database
import MySQLdb

try:
    import magpy.stream as st
    from magpy.database import stream2db
    from magpy.opt import cred as mpcred
    from magpy.transfer import scptransfer
except:
    sys.path.append('/home/leon/Software/magpy/trunk/src')
    import stream as st
    from magpy.database import stream2db
    from magpy.opt import cred as mpcred
    from magpy.transfer import scptransfer

clientname = 'default'
s = []
o = []
marcospath = ''

def sendparameter(mod,cname,cip,marcospath,op,sid,sshc,owlist,dbc=None):
    global clientname
    clientname = cname
    global clientip
    clientip = cip
    global output
    output = op
    global stationid
    stationid = sid
    global module
    module = mod
    global sshcred
    sshcred = sshc
    global o
    o = owlist
    global destpath
    destpath = marcospath
    if output == 'db':
        if not dbc:
            log.msg('collectors owclient: for db output you need to provide the credentials as last option')
        global dbcred
        dbcred = dbc
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
        log.msg('collectors owclient: Error while extracting time array')
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
        log.msg('collectors owclient: Error while saving file')        

class PubSubClient(WampClientProtocol):
    """
    Class for OneWire communication
    """ 
    def onSessionOpen(self):
        global clientname
        global clientip
        global o
        global s
        global destpath
        global output
        global module
        global stationid
        global dbcred
        global sshcred
        log.msg("Starting " + clientname + " session")
        # TODO Make all the necessary parameters variable
        # Basic definitions to change
        self.stationid = stationid
        self.output = output
        self.sensorid = ''
        self.sensortype = ''
        self.sensorgroup = ''
        #self.output = output # can be either 'db' or 'file', if not db, then file is used
        # Open database connection
        self.db = None
        self.cursor = None
        if not output == 'file':
            self.db = MySQLdb.connect(dbcred[0],dbcred[1],dbcred[2],dbcred[3] )
            # prepare a cursor object using cursor() method
            self.cursor = self.db.cursor()
        # Initiate subscriptions
        self.line = []
        self.subscribeInst(self.db, self.cursor, clientname, module, output)

    def subscribeOw(self, owlist):
        """
        Subscribing to all Onewire Instruments
        """
        if output == 'db':        
            # -------------------------------------------------------
            # A. Create database input 
            # -------------------------------------------------------
            # ideal way: upload an already existing file from moon for each sensor
            # check client for existing file:
            for row in owlist:
                print "collectors owclient: Running for sensor", row[0]
                # Try to find sensor in db:
                sql = "SELECT SensorID FROM SENSORS WHERE SensorID LIKE '%s%%'" % row[0]
                try:
                    # Execute the SQL command
                    cursor.execute(sql)
                except:
                    log.msg("collectors owclient: Unable to execute SENSOR sql")
                try:
                    # Fetch all the rows in a list of lists.
                    results = self.cursor.fetchall()
                except:
                    log.msg("collectors owclient: Unable to fetch SENSOR data from DB")
                    results = []
                if len(results) < 1:
                    # Initialize e.g. ow table
                    log.msg("collectors owclient: No sensors registered so far - Getting file from moon and uploading it")
                    # if not present then get a file and upload it
                    #destpath = [path for path, dirs, files in os.walk("/home") if path.endswith('MARCOS')][0]
                    day = datetime.strftime(datetime.utcnow(),'%Y-%m-%d')
                    destfile = os.path.join(destpath,'MoonsFiles', row[0]+'_'+day+'.bin') 
                    datafile = os.path.join('/srv/ws/', clientname, row[0], row[0]+'_'+day+'.bin')
                    try:
                        log.msg("collectors owclient: Downloading data: %s" % datafile)
                        scptransfer(sshcred[0]+'@'+clientip+':'+datafile,destfile,sshcred[1])
                        stream = st.read(destfile)
                        log.msg("collectors owclient: Reading with MagPy... Found: %s datapoints" % str(len(stream)))
                        stream.header['StationID'] = self.stationid
                        stream.header['SensorModule'] = 'OW' 
                        stream.header['SensorType'] = row[1]
                        if not row[2] == 'typus':
                            stream.header['SensorGroup'] = row[2] 
                        if not row[3] == 'location':
                            stream.header['DataLocationReference'] = row[3]
                        if not row[4] == 'info':
                            stream.header['SensorDescription'] = row[4] 
                        stream2db(db,stream)
                    except:
                        log.msg("collectors owclient: Could not upload data to the data base - subscription failed")
                else:
                    log.msg("collectors owclient: Found sensor(s) in DB - subscribing to the highest revision number")
                subscriptionstring = "%s:%s-value" % (module, row[0])
                print "collectors owclient: Subscribing (directing to DB): ", subscriptionstring
                self.subscribe(subscriptionstring, self.onEvent)
        elif output == 'file':
            for row in o:
                print "collectors owclient: Running for sensor", row[0]
                subscriptionstring = "%s:%s-value" % (mod, row[0])
                print "collectors owclient: Subscribing (directing to file): ", subscriptionstring
                self.subscribe(subscriptionstring, self.onEvent)

    def subscribeSensor(self,sensorshort):
        """
        Subscribing to Sensors:
        principally any subscrition is possible if the subscription string is suppported by the moons protocols
        """
        print "collectors owclient: Running for sensor", sensorshort
        # Try to find sensor in db:
        sql = "SELECT SensorID, SensorDescription, SensorDataLogger, SensorKeys FROM SENSORS WHERE SensorID LIKE '%s%%'" % sensorshort
        print sql
        try:
            # Execute the SQL command
            cursor.execute(sql)
        except:
            log.msg("collectors owclient: Unable to execute SENSOR sql")
        try:
            # Fetch all the rows in a list of lists.
            results = self.cursor.fetchall()
            print "SQL-Results:", results
        except:
            log.msg("collectors owclient: Unable to fetch SENSOR data from DB")
            results = []
        if len(results) < 1:
            pass

    def subscribeInst(self, db, cursor, client, mod, output):
        print "Starting Subscription ...."
        self.prefix(mod, "http://example.com/" + client +"/"+mod+"#")
        if mod == 'cs':
             sensshort = 'G82'
        else:
             sensshort = mod.upper()
        if mod == 'ow':
            if not len(o) > 0:
                log.msg('collectors client: No OW sensors available')
            else:
                log.msg('Subscribing OneWire Sensors ...')
                self.subscribeOw(o)
        else:
            self.subscribeSensor(sensshort)

        if output == 'db':
            # -------------------------------------------------------
            # 1. Get available Sensors - read sensors.txt
            # -------------------------------------------------------
        
            # -------------------------------------------------------
            # 2. Create database input 
            # -------------------------------------------------------
            # ideal way: upload an already existing file from moon for each sensor
            # check client for existing file:
            for row in o:
                print "collectors owclient: Running for sensor", row[0]
                # Try to find sensor in db:
                sql = "SELECT SensorID FROM SENSORS WHERE SensorID LIKE '%s%%'" % row[0]
                print sql
                try:
                    # Execute the SQL command
                    cursor.execute(sql)
                except:
                    log.msg("collectors owclient: Unable to execute SENSOR sql")
                try:
                    # Fetch all the rows in a list of lists.
                    results = self.cursor.fetchall()
                    print "SQL-Results:", results
                except:
                    log.msg("collectors owclient: Unable to fetch SENSOR data from DB")
                    results = []
                if len(results) < 1:
                    # Initialize e.g. ow table
                    log.msg("collectors owclient: No sensors registered so far - Getting file from moon and uploading it")
                    # if not present then get a file and upload it
                    #destpath = [path for path, dirs, files in os.walk("/home") if path.endswith('MARCOS')][0]
                    day = datetime.strftime(datetime.utcnow(),'%Y-%m-%d')
                    destfile = os.path.join(destpath,'MoonsFiles', row[0]+'_'+day+'.bin') 
                    datafile = os.path.join('/srv/ws/', clientname, row[0], row[0]+'_'+day+'.bin')
                    try:
                        log.msg("collectors owclient: Downloading data: %s" % datafile)
                        scptransfer(sshcred[0]+'@'+clientip+':'+datafile,destfile,sshcred[1])
                        stream = st.read(destfile)
                        log.msg("collectors owclient: Reading with MagPy... Found: %s datapoints" % str(len(stream)))
                        stream.header['StationID'] = self.stationid
                        stream.header['SensorModule'] = 'OW' 
                        stream.header['SensorType'] = row[1]
                        if not row[2] == 'typus':
                            stream.header['SensorGroup'] = row[2] 
                            self.sensorgroup = row[2]
                        if not row[3] == 'location':
                            stream.header['DataLocationReference'] = row[3]
                        if not row[4] == 'info':
                            stream.header['SensorDescription'] = row[4] 
                        stream2db(db,stream)
                        self.sensorid = stream.header['SensorID']
                        self.sensortype = row[1]
                    except:
                        log.msg("collectors owclient: Could not upload data to the data base - subscription failed")
                elif len(results) == 1:
                    log.msg("collectors owclient: Found sensor in db - subscribing to its table")
                    self.sensorid = results[0][0]
                    self.sensortype = row[1]
                    if not row[2] == 'typus':
                        self.sensorgroup = row[2]
                else:
                    log.msg("collectors owclient: Found several sensors in db - subscribing the highest revision number")
                    self.sensorid = results[-1][0]
                    self.sensortype = row[1]
                    if not row[2] == 'typus':
                        self.sensorgroup = row[2]
                subscriptionstring = "%s:%s-value" % (module, row[0])
                print "collectors owclient: Subscribing: ", subscriptionstring
                self.subscribe(subscriptionstring, self.onEvent)
        elif output == 'file':
            for row in o:
                print "collectors owclient: Running for sensor", row[0]
                subscriptionstring = "%s:%s-value" % (mod, row[0])
                self.subscribe(subscriptionstring, self.onEvent)
   
       
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
            #print sensorid
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
                if self.output == 'file':
                    # Appending data to buffer which contains pcdate, pctime and sensordata
                    # extract time data
                    print sensorid
                    packcode = '6hLl'
                    keylst = ['t1']
                    namelst = ['T']
                    unitlst = ['deg_C']
                    multilst = [1000]
                    #header = "# MagPyBin %s %s %s %s %s %s %d" % (sensor.id, '[t1,var1,var2,var3,var4]', '[T,rh,vdd,vad,vis]', '[deg_C,per,V,V,V]', '[1000,100,100,100,1]', packcode, struct.calcsize(packcode))
                    print self.line
                    datearray = timeToArray(self.line[0])
                    try:
                        datearray.append(int(self.line[1]*1000))
                        try:
                            print self.line[2]
                            if self.line[2] <= 0:
                                humidity = 999999
                            else:
                                humidity = int(self.line[2]*100)
                            print self.line[2]
                            namelst.append('rh')
                            unitlst.append('percent')
                            print "here"
                            multilst.append(100)
                            keylst.append('var1')
                            datearray.append(humidity)
                            print "Well"
                            packcode += 'L'
                        except:
                            print "No line2"
                            pass
                        try:
                            datearray.append(int(self.line[3]*100))
                            packcode += 'l'
                            namelst.append('vdd')
                            unitlst.append('V')
                            multilst.append(100)
                            keylst.append('var2')
                        except:
                            print "No line3"
                            pass
                        try:
                            datearray.append(int(self.line[4]*100))
                            packcode += 'l'
                            namelst.append('vad')
                            unitlst.append('V')
                            multilst.append(100)
                            keylst.append('var3')
                        except:
                            print "No line4"
                            pass
                        try: 
                            datearray.append(self.line[5]*100)
                            packcode += 'l'
                            namelst.append('vis')
                            unitlst.append('V')
                            multilst.append(100)
                            keylst.append('var4')
                        except:
                            print "No line5"
                            pass
                        print packcode, namelst, unitlst, multilst, keylst
                        print str(keylst)
                        #header = "# MagPyBin %s %s %s %s %s %s %d" % (sensorid, str(keylst), str(namelst), str(unitlst), str(multilst), packcode, struct.calcsize(packcode))
                        #data_bin = struct.pack(packcode,*datearray)
                    except:
                        log.msg('collectors owclient: Error while packing binary data')
                        pass
                    self.line = []

                    """
                    # File Operations
                    dataToFile(sensorid, filename, data_bin, header)
                    """
                else:
                    # get suitable datainfoid - requires a database query
                    sql = "SELECT SensorID, SensorGroup, SensorType FROM SENSORS WHERE SensorID LIKE '%s%%'" % sensorid
                    self.cursor.execute(sql)
                    results = self.cursor.fetchall()
                    sid = results[-1][0]
                    sgr = results[-1][1]
                    sty = results[-1][2]
                    dataid = sid+'_0001'
                    # add data to db by execute commands
                    if sty == 'DS18B20':
                        sql = "INSERT INTO %s(time, t1, flag, typ) VALUES ('%s', %f, '0000000000000000-', 'ow')" % (dataid, self.line[0], self.line[1])
                    elif sty.startswith('DS2438'):
                        if sgr == 'humidity':
                            sql = "INSERT INTO %s(time, t1, var1, var2, flag, typ) VALUES ('%s', %f, %f, %f, '0000000000000000-', 'ow')" % (dataid, self.line[0], self.line[1], self.line[2], self.line[3])
                        elif sgr == 'pressure':
                            sql = "INSERT INTO %s(time, t1, var2, var3, var4, flag, typ) VALUES ('%s', %f, %f, %f, %f, '0000000000000000-', 'ow')" % (dataid, self.line[0], self.line[1], self.line[3], self.line[4], self.line[5])
                        else:
                            sql = "INSERT INTO %s(time, t1, var2, var3, var4, flag, typ) VALUES ('%s', %f, %f, %f, %f, '0000000000000000-', 'ow')" % (dataid, self.line[0], self.line[1], self.line[3], self.line[4], self.line[5])
                    else:
                        log.msg("Unkown Sensor type - needs to be added")
                    self.line = []
                    try:
                        # Execute the SQL command
                        self.cursor.execute(sql)
                        # Commit your changes in the database
                        self.db.commit()
                    except:
                        # Rollback in case there is any error
                        self.db.rollback()

        except:
            pass


