from __future__ import print_function
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

try:
    from magpy.database import *
except:
    print("Failure loading MySQL")
    pass


# TODO
"""
a) check working state of db version for all sensors OW (OK), LEMI, POS, CS, GSM, ENV (OK), etc
b) check working state of file version  OW (OK), LEMI, POS, CS, GSM, ENV (OK), etc
c) check autorun on reboot (with .conf (OK) and .sh (OK))
d) check stability (no sensor attached (OK), OW sensor removed while running (OK), OW sensor added while running (Sytem failure on first try, OK on second - HOWEVER all other sensors get lost!!!), Other sensor: adding (requires restart of Martas and Marcos - Marcos is stopped))
e) automatically restart once a day (when??, ideally shortly before scheduled upload)
f) add nagios test whether collcetors are running (add to MARCOS/Nagios)
g) add script to MARCOS for file upload by cron
h) add a websript like (single.html) ro MARCOS/WebScripts
i) add a Version number to MARCOS and MARTAS (eventually add both of them to the MagPy folder...)
j) MARTAS tests: run through WLAN, UMTS
k) future version: send commands
"""

if __name__ == '__main__':
    # ----------------------------------------------------------
    # 1. Define client (or get it from database?)
    # ----------------------------------------------------------

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #                 do necessary changes below
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # Name of moon
    #clientname = 'raspberrypi'
    clientname = 'titan'
    # IP of moon
    #clientip = '192.168.178.47'
    clientip = '138.22.188.182'
    # Path of MARTAS directory on moon
    martaspath = '/home/cobs/MARTAS'
    # Provide Station code
    stationid = 'MyHome'
    # Select destination (file or db) - Files are saved in .../MARCOS/MoonsFiles/
    dest = 'db'
    # For Testing purposes - Print received data to screen:
    printdata = True
    # Please make sure that the db and scp connection data is stored within the credential file -otherwise provide this data directly
    dbhost = mpcred.lc('mydb','host')
    dbuser = mpcred.lc('mydb','user')
    dbpasswd = mpcred.lc('mydb','passwd')
    dbname = mpcred.lc('mydb','db')
    scpuser = mpcred.lc('cobs','user')
    scppasswd = mpcred.lc('cobs','passwd')
    # You can add to the credential file by using:
    # mpcred.cc('transfer','myshortcut',user='myuser',passwd='mypasswd',address='no-specific')
    # and than read it by scpuser = mpcred.lc('myshortcut','myuser')
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #                 do necessary changes above
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


    log.startLogging(sys.stdout)
    sshcredlst = [scpuser,scppasswd]
    # ----------------------------------------------------------
    # 2. connect to database and check availability and version
    # ----------------------------------------------------------
    try:
        db = mysql.connect (host=dbhost,user=dbuser,passwd=dbpasswd,db=dbname)
        dbcredlst = [dbhost,dbuser,dbpasswd,dbname]
        cursor = db.cursor ()
        cursor.execute ("SELECT VERSION()")
        row = cursor.fetchone ()
        print("MySQL server version:", row[0])
        cursor.close ()
        db.close ()
    except:
        print("Could not connect to database")
        dest = "file"
        #raise

    # ----------------------------------------------------------
    # 3. connect to client and get sensor list as well as owlist
    # ----------------------------------------------------------
    print("Locating MARCOS directory ...")
    destpath = [path for path, dirs, files in os.walk("/home") if path.endswith('MARCOS')][0]
    from os.path import expanduser
    home = expanduser("~")

    destpath = [path for path, dirs, files in os.walk(home) if path.endswith('MARCOS')][0]
    sensfile = os.path.join(martaspath,'sensors.txt')
    owfile = os.path.join(martaspath,'owlist.csv')
    destsensfile = os.path.join(destpath,'MoonsSensors',clientname+'_sensors.txt')
    destowfile = os.path.join(destpath,'MoonsSensors',clientname+'_owlist.csv')
    print("Getting sensor information from ", clientname)
    try:
        scptransfer(scpuser+'@'+clientip+':'+sensfile,destsensfile,scppasswd)
    except:
        print("Could not connect to/get sensor info of client %s - aborting" % clientname)
        sys.exit()
    print("Searching for onewire data from ", clientname)
    try:
        scptransfer(scpuser+'@'+clientip+':'+owfile,destowfile,scppasswd)
    except:
        print("No one wire info available on client %s - proceeding" % clientname)
        pass

    s,o = [],[]
    with open(destsensfile,'rb') as f:
        reader = csv.reader(f)
        s = []
        for line in reader:
            if len(line) < 2:
                s.append(line[0].split())
            else:
                s.append(line)
    print(s)
    with open(destowfile,'rb') as f:
        reader = csv.reader(f)
        o = [line for line in reader]
    print(o)

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
