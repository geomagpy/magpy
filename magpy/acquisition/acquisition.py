"""
Main Acquisition routine of MARTAS: 
Code from TAVENDO GmbH adepted by Roman Leonhardt and Rachel Bailey to be used in the Conrad Observatory.
The main part remained unchanged. I added instrument specific parts for serial communictaion.
The application is mainly based on twisted, autobahn and magpy modules. Please note that autobahn in the past frequently changed its module positions.
Additional requirements:
1) please change the user specific part acclording your system and attached instruments
2) 
Usage:
sudo python acquisition.py

"""
from __future__ import print_function
from __future__ import absolute_import

# -------------------------------------------------------------------
# Import software
# -------------------------------------------------------------------
import sys, time, os, socket
from datetime import datetime, timedelta
import re
import struct, binascii

if sys.platform == 'win32':
    ## on windows, we need to use the following reactor for serial support
    ## http://twistedmatrix.com/trac/ticket/3802
    ##
    from twisted.internet import win32eventreactor
    win32eventreactor.install()
# IMPORT TWISTED
from twisted.internet import reactor
print("Using Twisted reactor", reactor.__class__)
print()
from twisted.python import usage, log
from twisted.protocols.basic import LineReceiver
from twisted.internet.serialport import SerialPort
from twisted.internet import task
from twisted.web.server import Site
from twisted.web.static import File
from autobahn import version as autobahnversion
print("Autobahn Version: ", autobahnversion)
try: # version > 0.8.0
    from autobahn.wamp1.protocol import WampServerFactory, WampServerProtocol, exportRpc
except:
    from autobahn.wamp import WampServerFactory, WampServerProtocol, exportRpc
try: # version > 0.7.0
    from autobahn.twisted.websocket import listenWS
except:
    from autobahn.websocket import listenWS

lastActualtime = datetime.utcnow() # required for cs output
hostname = socket.gethostname()

from serial import PARITY_EVEN
from serial import SEVENBITS

# ------------------------------------------------------------------------
# User specific data
# ------------------------------------------------------------------------

# IMPORT EQUIPMENT-SPECIFIC PROTOCOLS
# ########
#from magpy.acquisition.owprotocol import OwProtocol
#from magpy.acquisition.arduinoprotocol import ArduinoProtocol
#from palmacqprotocol import PalmAcqProtocol
from .gsm19protocol import GSM19Protocol

# Other possible protocals are: lemiprotocol, pos1protocol, envprotocol, csprotocol, gsm90protocol
# SELECT DIRECTORY FOR BUFFER FILES
outputdir = '/srv/ws'
# SELECT DIRECTORY WITH SCRIPTS (usually the home dir of the MARTAS user)
homedir = '/home/cobs'
# WEBPORTS AND COMMUNICATION
webport = 8080                  # Web port to use for embedded Web server
wsurl = "ws://localhost:9100"   # WebSocket port to use for embedded WebSocket $
# INSTRUMENT PORTS
owport = 'u' 			# u for usb
serialport = '/dev/tty' 	# dev/tty for linux like systems
# ONEWIRE SPECIFIC
timeoutow = 30.0		# Defining a measurement frequency in secs (should be >= amount of sensors connected)
timeoutser = 60.0		# Defining a measurement frequency in secs (should be >= amount of sensors connected)
 

# -------------------------------------------------------------------
# Read data of sensors attached to PC:
# 
# "Sensors.txt" should have the following format:
# SENSORNAME	SENSORPORT	SENSORBAUDRATE
# e.g:
# LEMI036_1_0001	USB0	57600
# POS1_N432_0001	S0	9600
# ARDUINO		ACM0	9600     -> ARDUINO comm
# SERIAL		S0	115200   -> for specific calls
# OW			-	-
#
# Notes: OneWire devices do not need this data, all others do.
# -------------------------------------------------------------------

def GetSensors():
    sensors = open(os.path.join(homedir,'MARTAS','sensors.txt'),'r')
    sensordata = sensors.readlines()
    sensorlist = []
    baudratedict, portdict = {}, {}

    for item in sensordata:
        try:
            if not item.startswith('#'):
                bits = item.split()
                sensorname = bits[0]
                sensorlist.append(sensorname)
                portdict[sensorname] = bits[1]
                try:
                    baudratedict[sensorname] = float(bits[2])
                except:
                    # no float, assuming ow
                    baudratedict[sensorname] = 0.0
        except:
            # Possible issue - empty line
            pass

    print("Found", sensorlist, portdict, baudratedict)
    return sensorlist, portdict, baudratedict


# -------------------------------------------------------------------
# WS-MCU protocol:
# -------------------------------------------------------------------

class WsMcuProtocol(WampServerProtocol):

    def onSessionOpen(self):
        ## register topic prefix under which we will publish MCU measurements
        ##
        for sensor in sensorlist:
            if sensor[:3].upper() == 'ENV': # Environmental Sensor 5
        	self.registerForPubSub("http://example.com/"+hostname+"/env#", True)
       		## register methods for RPC
		## does not work in python 2.6.5 (fine in 2.7.3)
       		if sys.version_info >= (2, 7):
           	    self.registerForRpc(self.factory.envProtocol, "http://example.com/"+hostname+"/env-control#")
       		    #else:
       		    #    self.registerMethodForRpc("http://example.com/"+hostname+"/mcu-control#",self.factory.mcuProtocol,McuProtocol.add)
	    elif sensor[:3].upper() == 'KER': # Kern balance
       		self.registerForPubSub("http://example.com/"+hostname+"/kern#", True)
	    elif sensor[:3].upper() == 'ARD': # Arduino board
       		self.registerForPubSub("http://example.com/"+hostname+"/ard#", True)
            elif sensor[:3].upper() == 'PAL': # PalmAcq
                self.registerForPubSub("http://example.com/"+hostname+"/pal#", True)
	    elif sensor[:3].upper() == 'LEM': # Lemi Sensor
       		self.registerForPubSub("http://example.com/"+hostname+"/lemi#", True)
	    elif sensor[:2].upper() == 'OW': # OW Sensor
       		self.registerForPubSub("http://example.com/"+hostname+"/ow#", True) 
	    elif sensor[:3].upper() == 'POS': # POS-1 Overhauzer Sensor
       		self.registerForPubSub("http://example.com/"+hostname+"/pos1#", True)
	    elif sensor[:3].upper() == 'G82': # GSM CS Sensor
       		self.registerForPubSub("http://example.com/"+hostname+"/cs#", True) 
	    elif sensor[:3].upper() == 'SER': # standard serial
       		self.registerForPubSub("http://example.com/"+hostname+"/ser#", True) 
	    elif sensor[:3].upper() == 'GSM': # GEM Overhauzer Sensor (GSM90)
       		self.registerForPubSub("http://example.com/"+hostname+"/gsm#", True)
	    elif sensor[:3].upper() == 'G19': # GEM Overhauzer Sensor (GSM19)
       		self.registerForPubSub("http://example.com/"+hostname+"/gsm#", True)
	    else:
	        log.msg('Sensor type %s is not supported.' % (sensor))

# -------------------------------------------------------------------
# WS-MCU factory
# -------------------------------------------------------------------

class WsMcuFactory(WampServerFactory):

    protocol = WsMcuProtocol
    def __init__(self, url):
        WampServerFactory.__init__(self, url)
        for sensor in sensorlist:
            if sensor[:3].upper() == 'ENV':
                self.envProtocol = EnvProtocol(self,sensor.strip(), outputdir)
	    if sensor[:2].upper() == 'OW':
	        self.owProtocol = OwProtocol(self,owport,outputdir)
            if sensor[:3].upper() == 'POS':
                self.pos1Protocol = Pos1Protocol(self,sensor.strip(), outputdir)
            if sensor[:3].upper() == 'KER':
                print("Test1:", sensor.strip)
                self.kernProtocol = KernProtocol(self,sensor.strip(), outputdir)
            if sensor[:3].upper() == 'ARD':
                self.arduinoProtocol = ArduinoProtocol(self, sensor.strip(), outputdir)
	    if sensor[:3].upper() == 'SER':
                port = serialport+portdict[sensor]
                baudrate = baudratedict[sensor]
                self.callProtocol = CallProtocol(self,sensor.strip(), outputdir,port,baudrate)
            if sensor[:3].upper() == 'PAL':
                self.palmacqProtocol = PalmAcqProtocol(self, sensor.strip(), outputdir)
            if sensor[:3].upper() == 'LEM':
                self.lemiProtocol = LemiProtocol(self,sensor.strip(),sensor[0]+sensor[4:7], outputdir)
	    if sensor[:3].upper() == 'G82':
                self.csProtocol = CsProtocol(self,sensor.strip(), outputdir)
	    if sensor[:3].upper() == 'GSM':
       		self.gsm90Protocol = GSM90Protocol(self,sensor.strip(), outputdir)
	    if sensor[:3].upper() == 'G19':
       		self.gsm19Protocol = GSM19Protocol(self,sensor.strip(), outputdir)

#####################################################################
# MAIN PROGRAM
#####################################################################

if __name__ == '__main__':


    sensorlist, portdict, baudratedict = GetSensors()

    ##  Start Twisted logging system
    ##
    #log.startLogging(sys.stdout)
    logfile = os.path.join(homedir,'MARTAS','Logs','martas.log')
    log.startLogging(open(logfile,'a'))

    ## create Serial2Ws gateway factory
    ##
    wsMcuFactory = WsMcuFactory(wsurl)
    listenWS(wsMcuFactory)
   
    ## create serial port and serial port protocol; modify this according to attached sensors
    ##
    for sensor in sensorlist:
	port = serialport+portdict[sensor]
	baudrate = baudratedict[sensor]

        if sensor[:3].upper() == 'LEM':
	    protocol = wsMcuFactory.lemiProtocol
        if sensor[:3].upper() == 'POS':
	    protocol = wsMcuFactory.pos1Protocol
        if sensor[:3].upper() == 'G82':
	    protocol = wsMcuFactory.csProtocol
        if sensor[:3].upper() == 'GSM':
	    protocol = wsMcuFactory.gsm90Protocol
        if sensor[:3].upper() == 'G19':
	    protocol = wsMcuFactory.gsm19Protocol
        if sensor[:3].upper() == 'ENV':
	    protocol = wsMcuFactory.envProtocol
        if sensor[:3].upper() == 'KER':
	    protocol = wsMcuFactory.kernProtocol
        if sensor[:3].upper() == 'ARD':
	    protocol = wsMcuFactory.arduinoProtocol
        if sensor[:3].upper() == 'PAL':
            protocol = wsMcuFactory.palmacqProtocol

        if sensor[:3].upper() == 'SER':
	    try:
	        log.msg('Serial Call: Initiating sensor and sending commands...')
                # eventually define a command list
                sprot = task.LoopingCall(wsMcuFactory.callProtocol.sendCommands)
                sprot.start(timeoutser)
            except:
                log.msg('Serial Call: Not available.')
        elif sensor[:2].upper() == 'OW':
	    try:
	        log.msg('OneWire: Initiating sensor...')
                oprot = task.LoopingCall(wsMcuFactory.owProtocol.owConnected)
                oprot.start(timeoutow)
            except:
                log.msg('OneWire: Not available.')
        else:
    	    try:
        	log.msg('%s: Attempting to open port %s [%d baud]...' % (sensor, port, baudrate))
                if sensor.startswith('KER'):
                    serialPort = SerialPort(protocol,port,reactor, baudrate=baudrate,bytesize=SEVENBITS,parity=PARITY_EVEN)    
                else:
   	       	    serialPort = SerialPort(protocol, port, reactor, baudrate = baudrate)
              	    log.msg('%s: Port %s [%d baud] connected' % (sensor, port, baudrate))
    	    except:
        	log.msg('%s: Port %s [%d baud] not available' % (sensor, port, baudrate))


    ## create embedded web server for static files
    ##
    webdir = File(".")
    web = Site(webdir)
    reactor.listenTCP(webport, web)

    ## start Twisted reactor:
    ##
    reactor.run()
