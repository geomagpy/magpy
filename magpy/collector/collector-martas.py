"""
Collector script for obtaining real time data from MARTAS machines

Collector accesses the autobahn websocket from MARTAS and retrieves data. This data is directly
added to a data bank (or file if preferred).
"""
import sys, os, csv
from twisted.python import log
from twisted.internet import reactor
try: # version > 0.8.0
    from autobahn.wamp1.protocol import WampClientFactory, WampClientProtocol
except:
    from autobahn.wamp import WampClientFactory, WampClientProtocol
try: # autovers > 0.7.0:
    from autobahn.twisted.websocket import connectWS
except:
    from autobahn.websocket import connectWS
# For converting Unicode text
import collections

from magpy.collector import subscribe2client as cl
from magpy.opt import cred as mpcred
from magpy.transfer import scptransfer

import pymysql as mysql
from multiprocessing import Process
import pwd

class MartasInfo():
    """
    Class to obtain Sensor information from MARTAS.
    scp process which is used for that purpose cannot
    bin run as root. This would not allow to test for sensors
    at bootup.
    The MartasInfo class is run as an independent process by switching
    to a standard user of the system.

    class is currently not working because the actual version of pexpect.spawn returns an error...
    """
    def GetSensors(self, user, scpuser, scppasswd, source, dest):

        pw = pwd.getpwnam(user)
        uid = pw.pw_uid
        os.setuid(uid)

        print ("setuid successful to", uid)

        try:
            scptransfer(scpuser+'@'+source,dest,scppasswd)
        except:
            print "Could not connect to/get sensor info of client %s" % clientname


# TODO
"""
a) check working state of db version for all sensors OW (OK), LEMI (OK), POS, CS, GSM, ENV (OK), etc
b) check working state of file version  OW (OK), LEMI, POS, CS, GSM, ENV (OK), etc
c) (OK) check autorun of MARTAS on reboot (with .conf (OK) and .sh (OK))
d) check autorun of MARCOS on reboot (with .conf (?) and .sh (?))
e) (OK) check stability (no sensor attached (OK), OW sensor removed while running (OK), OW sensor added while running (Sytem failure on first try, OK on second - HOWEVER all other sensors get lost!!!), Other sensor: adding (requires restart of Martas and Marcos - Marcos is stopped))
f) automatically restart MARCOS once a day (when??, ideally shortly before scheduled upload)
g) add nagios test whether collcetors are running (add to MARCOS/Nagios)
h) (OK) add script to MARCOS for file upload by cron
i) add a websript like (single.html) to MARCOS/WebScripts
j) (OK) add a Version number to MARCOS and MARTAS (eventually add both of them to the MagPy folder...)
k) MARTAS tests: run through WLAN, UMTS
l) test file upload by stream2db before/after collector with changing db info in datainfo and sensor
m) future version: send commands
"""

if __name__ == '__main__':
    # ----------------------------------------------------------
    # 1. Define client (or get it from database?)
    # ----------------------------------------------------------

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #                 do necessary changes below
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # Name of martas
    #clientname = 'raspberrypi'
    clientname = 'ceres'
    # IP of martas
    clientip = '138.22.188.181'
    # Path of MARTAS directory on martas machine
    martaspath = '/home/cobs/MARTAS' 
    # Path of MARCOS directory
    homedir = '/home/cobs'
    defaultuser = 'cobs'
    # Provide Station code
    stationid = 'WIC'
    # Select destination (file or db) - Files are saved in .../MARCOS/MartasFiles/
    dest = 'db'
    # For Testing purposes - Print received data to screen:
    printdata = False
    # Please make sure that the db and scp connection data is stored 
    # within the credential file -otherwise provide this data directly

    dbhost = mpcred.lc('cobsdb','host',path='/home/cobs/.magpycred')
    dbuser = mpcred.lc('cobsdb','user',path='/home/cobs/.magpycred')
    dbpasswd = mpcred.lc('cobsdb','passwd',path='/home/cobs/.magpycred')
    dbname = mpcred.lc('cobsdb','db',path='/home/cobs/.magpycred')
    scpuser = mpcred.lc('ceres','user',path='/home/cobs/.magpycred')
    scppasswd = mpcred.lc('ceres','passwd',path='/home/cobs/.magpycred')
    # You can add to the credential file by using: 
    # mpcred.cc('transfer','myshortcut',user='myuser',passwd='mypasswd',address='no-specific')
    # and than read it by scpuser = mpcred.lc('myshortcut','myuser')
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #                 do necessary changes above
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    print (dbhost, dbuser, dbpasswd, dbname, scpuser)

    logfile = os.path.join(homedir,'MARCOS','Logs','marcos.log')
    log.startLogging(open(logfile,'a'))

    sshcredlst = [scpuser,scppasswd]
    # ----------------------------------------------------------
    # 2. connect to database and check availability and version
    # ----------------------------------------------------------
    try:
        db = mysql.connect (host=dbhost,user=dbuser,passwd=dbpasswd,db=dbname)
        dbcredlst = [dbhost,dbuser,dbpasswd,dbname]
    except:
        print ("Create a credential file first or provide login info for database directly")
        raise
    cursor = db.cursor ()
    cursor.execute ("SELECT VERSION()")
    row = cursor.fetchone ()
    print ("MySQL server version:", row[0])
    cursor.close ()
    db.close ()

    # ----------------------------------------------------------
    # 3. connect to client and get sensor list as well as owlist
    # ----------------------------------------------------------
    print ("Locating MARCOS directory ...")
    destpath = [path for path, dirs, files in os.walk("/home") if path.endswith('MARCOS')][0]
    sensfile = os.path.join(martaspath,'sensors.txt')
    owfile = os.path.join(martaspath,'owlist.csv')
    destsensfile = os.path.join(destpath,'MartasSensors',clientname+'_sensors.txt') 
    destowfile = os.path.join(destpath,'MartasSensors',clientname+'_owlist.csv')
    print ("Getting sensor information from ", clientname)

    ## to be activated if multiprocessing and pexpect are working again
    #MI=MartasInfo()
    #source = clientip+':'+sensfile
    #p = Process(target=MI.GetSensors, args=('cobs',scpuser,scppasswd,source,destsensfile,))
    #p.start()
    #p.join() 
    #source = clientip+':'+owfile
    #p = Process(target=MI.GetSensors, args=('cobs',scpuser,scppasswd,source,destowfile,))
    #p.start()
    #p.join() 
    
    # Please note: scp is not workings from root
    # Therefore the following processes are performed as defaultuser (ideally as a subprocess)
    uid=pwd.getpwnam(defaultuser)[2]
    os.setuid(uid)
 
    try:
        print scpuser+'@'+clientip+':'+sensfile
        scptransfer(scpuser+'@'+clientip+':'+sensfile,destsensfile,scppasswd)
    except:
        print ("Could not connect to/get sensor info of client %s - aborting" % clientname)
        sys.exit()
    print ("Searching for onewire data from ", clientname)
    try:
        scptransfer(scpuser+'@'+clientip+':'+owfile,destowfile,scppasswd)
    except:
        print ("No one wire info available on client %s - proceeding" % clientname)
        pass

    s,o = [],[]
    with open(destsensfile,'rb') as f:
        reader = csv.reader(f)
        s = []
        for line in reader:
            print (line)
            if len(line) < 2:
                try:
                    s.append(line[0].split())
                except:
                    # Empty line for example
                    pass
            else:
                s.append(line)
    print (s)
    if os.path.exists(destowfile):
        with open(destowfile,'rb') as f:
            reader = csv.reader(f)
            o = [line for line in reader]
        print (o)

    factory = WampClientFactory("ws://"+clientip+":9100", debugWamp = False)
    cl.sendparameter(clientname,clientip,destpath,dest,stationid,sshcredlst,s,o,printdata,dbcredlst)
    factory.protocol = cl.PubSubClient
    connectWS(factory)

    reactor.run()

    try:
        cursor.close()
        db.close()
        log.msg("DB closed")
    except:
        pass

