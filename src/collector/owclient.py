import sys, os
from twisted.python import log
from twisted.internet import reactor 
from autobahn.websocket import connectWS
from autobahn.wamp import WampClientFactory, WampClientProtocol
# For converting Unicode text
import collections
# Timing
from datetime import datetime, timedelta
# Database
import MySQLdb

sys.path.append('/home/leon/Software/magpy/trunk/src')

import stream as st
import database
from opt import cred as mpcred
from transfer import scptransfer

clientname = 'default'
o = []
marcospath = ''

def defineclient(cname,cip):
    global clientname
    clientname = cname
    global clientip
    clientip = cip
    return    

def putparameter(owlist,marcospath):
    global o
    o = owlist
    global destpath
    destpath = marcospath
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
    Class for OneWire communication
    """ 
    def onSessionOpen(self):
        global clientname
        global clientip
        global o
        global marcospath
        log.msg("Starting " + clientname + " session")
        # TODO Make all the necessary parameters variable
        # Basic definitions to change
        self.stationid = 'MyHome'
        self.output = 'db' # can be either 'db' or 'file', if not db, then file is used
        # Open database connection
        self.db = MySQLdb.connect("localhost","cobs","8ung2rad","wic" )
        # prepare a cursor object using cursor() method
        self.cursor = self.db.cursor()
        # Initiate subscriptions
        self.line = []
        self.subscribeInst(self.db, self.cursor, clientname, self.output)

    def subscribeInst(self, db, cursor, client, output):
        print "Starting Subscription ...."
        self.prefix("ow", "http://example.com/" + client +"/ow#")
        # -------------------------------------------------------
        # 1. Get available Sensors - read sensors.txt
        # -------------------------------------------------------
        if not len(o) > 0:
            log.msg("No OW sensors available")            
        
        if output == 'db':
            # -------------------------------------------------------
            # 2. Create database input 
            # -------------------------------------------------------
            # ideal way: upload an already existing file from moon for each sensor
            # check client for existing file:
            for row in o:
                print "Running for sensor", row[0]
                #destpath = [path for path, dirs, files in os.walk("/home") if path.endswith('MARCOS')][0]
                day = datetime.strftime(datetime.utcnow(),'%Y-%m-%d')
                destfile = os.path.join(destpath,'MoonsFiles', row[0]+'_'+day+'.bin') 
                print "Destfile", destfile
                datafile = os.path.join('/srv/ws/', clientname, row[0], row[0]+'_'+day+'.bin')
                print "Datafile", datafile
                scptransfer(mpcred.lc('leon','user')+'@'+clientip+':'+datafile,destfile,mpcred.lc('leon','passwd'))
                stream = st.read(destfile)
                stream.header['StationID'] = self.stationid
                print "Loaded data of length", len(stream)
                stream2db(db,stream)

                subscriptionstring = "%s:%s-value" % (module, row[0])
                print subscriptionstring
                #self.subscribe(subscriptionstring, self.onEvent)

        elif output == 'file':
            for row in o:
                print "Running for sensor", row[0]
                subscriptionstring = "%s:%s-value" % (module, row[0])
                print subscriptionstring
                #self.subscribe(subscriptionstring, self.onEvent)
   
       
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
                #print len(self.line)
                rev = '_0001'
                did = '_0001'
                dataid = sensorid+rev+did
                # get available sensor parameters from db
                #query = "SELECT * FROM SENSORS WHERE SENSOR_ID = sensorid;"
                # TODO: generalize the information function below (maybe by parameters provided in DB)
                if sensorid in ["3AD754010000","0EB354010000"]:
                    sql = "INSERT INTO %s(time, t1, var2, var3, var4, flag, typ) VALUES ('%s', %f, %f, %f, %f, '0000000000000000-', 'ow')" % (dataid, self.line[0], self.line[1], self.line[3], self.line[4], self.line[5])
                elif sensorid == "05CE54010000" or sensorid == "CBC454010000":
                    sql = "INSERT INTO %s(time, t1, var1, var2, flag, typ) VALUES ('%s', %f, %f, %f, '0000000000000000-', 'ow')" % (dataid, self.line[0], self.line[1], self.line[2], self.line[3])
                else:
                    sql = "INSERT INTO %s(time, t1, flag, typ) VALUES ('%s', %f, '0000000000000000-', 'ow')" % (dataid, self.line[0], self.line[1])
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


