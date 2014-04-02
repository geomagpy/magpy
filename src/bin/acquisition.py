#####################################################################
##
## Copyright 2012 Tavendo GmbH
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##
#####################################################################

"""
Code adopted by leon (2012/2013) to be used in the Conrad Observatory.

The main part remained unchanged. I added instrument specific parts for serial communictaion.

ToDo: Use instrument specific classes for each instrument (even for identical systems), put these classes in a library

"""

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
print "Using Twisted reactor", reactor.__class__
print

from twisted.python import usage, log
from twisted.protocols.basic import LineReceiver
from twisted.internet.serialport import SerialPort
from twisted.internet import task
from twisted.web.server import Site
from twisted.web.static import File

# IMPORT WAMP: WepSocket Application Message Protocol
from autobahn.websocket import listenWS
from autobahn.wamp import WampServerFactory, WampServerProtocol, exportRpc

# IMPORT EQUIPMENT-SPECIFIC PROTOCOLS
from lemiprotocol import *
from owprotocol import *
from pos1protocol import *
from envprotocol import *
from csprotocol import *
from gsm90protocol import *

lastActualtime = datetime.utcnow() # required for cs output

hostname = socket.gethostname()
outputdir = '/srv/ws'

# -------------------------------------------------------------------
# Read data of sensors attached to PC:
# 
# "Sensors.txt" should have the following format:
# SENSORNAME	SENSORPORT	SENSORBAUDRATE
# e.g:
# LEMI036_1_0001	USB0	57600
# POS1_N432_0001	S0	9600
# OW			-	-
#
# Notes: OneWire devices do not need this data, all others do.
# -------------------------------------------------------------------

sensors = open('/home/cobs/sensors.txt','r')
sensordata = sensors.readlines()
sensorlist = []
baudratedict, portdict = {}, {}

for item in sensordata:
    bits = item.split()
    sensorname = bits[0]
    sensorlist.append(sensorname)
    portdict[sensorname] = bits[1]
    baudratedict[sensorname] = float(bits[2])

webport = 8080  		# Web port to use for embedded Web server
wsurl = "ws://localhost:9100" 	# WebSocket port to use for embedded WebSocket server
outputdir = '/srv/ws' 		# Directory for storing files

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
	    elif sensor[:3].upper() == 'LEM': # Lemi Sensor
       		self.registerForPubSub("http://example.com/"+hostname+"/lemi#", True)
	    elif sensor[:2].upper() == 'OW': # OW Sensor
       		self.registerForPubSub("http://example.com/"+hostname+"/ow#", True) 
	    elif sensor[:2].upper() == 'POS': # POS-1 Overhauzer Sensor
       		self.registerForPubSub("http://example.com/"+hostname+"/pos1#", True)
	    elif sensor[:3].upper() == 'G82': # GSM CS Sensor
       		self.registerForPubSub("http://example.com/"+hostname+"/cs#", True) 
	    elif sensor[:3].upper() == 'GSM': # GEM Overhauzer Sensor (GSM90)
       		self.registerForPubSub("http://example.com/"+hostname+"/gsm90#", True)
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
	        self.owProtocol = OwProtocol(self,'u',outputdir)
            if sensor[:3].upper() == 'POS':
                self.pos1Protocol = Pos1Protocol(self,sensor.strip(), outputdir)
            if sensor[:3].upper() == 'LEM':
                self.lemiProtocol = LemiProtocol(self,sensor.strip(),sensor[0]+sensor[4:7], outputdir)
	    if sensor[:3].upper() == 'G82':
                self.csProtocol = CsProtocol(self,sensor.strip(), outputdir)
	    if sensor[:3].upper() == 'GSM':
       		self.gsm90Protocol = GSM90Protocol(self,sensor.strip(), outputdir)

#####################################################################
# MAIN PROGRAM
#####################################################################

if __name__ == '__main__':

    # Start Twisted logging system

    #log.startLogging(sys.stdout)
    logfile = '/home/cobs/Logs/twisted_'+hostname+'.log'
    log.startLogging(open(logfile,'a'))

    ## create Serial2Ws gateway factory
    ##
    wsMcuFactory = WsMcuFactory(wsurl)
    listenWS(wsMcuFactory)

    ## create serial port and serial port protocol; modify this according to attached sensors
    ##
    for sensor in sensorlist:
	port = '/dev/tty'+portdict[sensor]
	baudrate = baudratedict[sensor]

        if sensor[:3].upper() == 'LEM':
	    protocol = wsMcuFactory.lemiProtocol
        if sensor[:3].upper() == 'POS':
	    protocol = wsMcuFactory.pos1Protocol
        if sensor[:3].upper() == 'G82':
	    protocol = wsMcuFactory.csProtocol
        if sensor[:3].upper() == 'GSM':
	    protocol = wsMcuFactory.gsm90Protocol
        if sensor[:3].upper() == 'ENV':
	    protocol = wsMcuFactory.envProtocol

        if sensor[:2].upper() == 'OW':
	    try:
	        log.msg('OneWire: Initiating sensor...')
        	timeoutow = 30.0
                oprot = task.LoopingCall(wsMcuFactory.owProtocol.owConnected)
                oprot.start(timeoutow)
            except:
                log.msg('OneWire: Not available.')

        else:
    	    try:
        	log.msg('%s: Attempting to open port %s [%d baud]...' % (sensor, port, baudrate))
        	serialPort = SerialPort(protocol, port, reactor, baudrate = baudrate)
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
