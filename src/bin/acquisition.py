###############################################################################
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
###############################################################################

"""
Code adopted by leon (2012/2013) to be used in the Conrad Observatory

The main part remained unchanged. I added instrument specific parts for serial communictaion.

ToDo: Use instrument specific classes for each instrument (even for identical systems), put these classes in a library

"""

import sys, time, os, socket

if sys.platform == 'win32':
    ## on windows, we need to use the following reactor for serial support
    ## http://twistedmatrix.com/trac/ticket/3802
    ##
    from twisted.internet import win32eventreactor
    win32eventreactor.install()

from twisted.internet import reactor
print "Using Twisted reactor", reactor.__class__
print

from twisted.python import usage, log
from twisted.protocols.basic import LineReceiver
from twisted.internet.serialport import SerialPort
from twisted.web.server import Site
from twisted.web.static import File
from twisted.internet import task

from autobahn import version as autobahnversion
print "Autobahn Version: ", autobahnversion
try: # version > 0.8.0
    from autobahn.wamp1.protocol import WampServerFactory, WampServerProtocol, exportRPC
except:
    from autobahn.wamp import WampServerFactory, WampServerProtocol, exportRPC
try: # version > 0.7.0
    from autobahn.twisted.websocket import listenWS
except:
    from autobahn.websocket import listenWS

from datetime import datetime, timedelta
import re
#from matplotlib.dates import date2num, num2date

try:
    from magpy.acquisition.owprotocols import *
except:
    from acquisition.owprotocols import *

hostname = socket.gethostname()

class Serial2WsOptions(usage.Options):
   optParameters = [
      ['baudrate_usb0', 'b', 57600, 'Serial baudrate of usb-port 0 (com?)'],
      ['port_usb0', 'p','/dev/ttyUSB0', 'Serial port to use, Can be number (0) or description (dev/ttyUSB0)'],
      ['baudrate_usb1', 'b', 9600, 'Serial baudrate of usb-port 1 (com?)'],
      ['port_usb1', 'p','/dev/ttyUSB1', 'Serial port to use, Can be number (1) or description (dev/ttyUSB1)'],
      ['baudrate0', 'b', 9600, 'Serial baudrate of port 0 (com1)'],
      ['port0', 'p','/dev/ttyS0', 'Serial port to use, Can be number (0) or description (dev/ttyS0)'],
      ['baudrate1', 'b', 9600, 'Serial baudrate of port 1 (com2)'],
      ['port1', 'p','/dev/ttyS1', 'Serial port to use, Can be number (0) or description (dev/ttyS1)'],
      ['webport', 'w', 8080, 'Web port to use for embedded Web server'],
      ['wsurl', 's', "ws://localhost:9100", 'WebSocket port to use for embedded WebSocket server'],
      ['outputdir', 's', '/srv/ws', 'Directory for storing files']
   ]


## WS-MCU protocol
##
class WsMcuProtocol(WampServerProtocol):

    def onSessionOpen(self):
       ## register topic prefix under which we will publish MCU measurements
       ##
       #self.registerForPubSub("http://example.com/"+hostname+"/env#", True) # Environmental Server 5
       #self.registerForPubSub("http://example.com/"+hostname+"/cs#", True) # GSM CS Sensor
       self.registerForPubSub("http://example.com/"+hostname+"/ow#", True) # OneWire
       self.registerForPubSub("http://example.com/"+hostname+"/lemi#", True) # Lemi Sensor
       #self.registerForPubSub("http://example.com/"+hostname+"/pos1#", True) # POS-1 Overhauzer Sensor
       #self.registerForPubSub("http://example.com/"+hostname+"/gem90#", True) # GEM Overhauzer Sensor (GSM90)

       ## register methods for RPC
       ##
       ## does not work in python 2.6.5 (fine in 2.7.3)
       #if sys.version_info >= (2, 7):
       #    self.registerForRpc(self.factory.envProtocol, "http://example.com/"+hostname+"/env-control#")
       #else:
       #    self.registerMethodForRpc("http://example.com/"+hostname+"/mcu-control#",self.factory.mcuProtocol,McuProtocol.add)


## WS-MCU factory
##
class WsMcuFactory(WampServerFactory):

    protocol = WsMcuProtocol

    def __init__(self, url):
       WampServerFactory.__init__(self, url)
       self.lemiProtocol = LemiProtocol(self,'LEMI036_1_0001','/srv/ws')
       self.owProtocol = OwProtocol(self,"u")


if __name__ == '__main__':

    ## parse options
    ##
    o = Serial2WsOptions()
    try:
        o.parseOptions()
    except usage.UsageError, errortext:
        print '%s %s' % (sys.argv[0], errortext)
        print 'Try %s --help for usage details' % sys.argv[0]
        sys.exit(1)

    baudrate0 = int(o.opts['baudrate0'])
    port0 = o.opts['port0']
    baudrate1 = int(o.opts['baudrate1'])
    port1 = o.opts['port1']
    baudrate_usb0 = int(o.opts['baudrate_usb0'])
    port_usb0 = o.opts['port_usb0']
    baudrate_usb1 = int(o.opts['baudrate_usb1'])
    port_usb1 = o.opts['port_usb1']
    webport = int(o.opts['webport'])
    wsurl = o.opts['wsurl']
    outputdir = o.opts['outputdir']

    ## start Twisted log system
    ##
    #log.startLogging(sys.stdout)
    logfile = '/home/cobs/Logs/twisted_'+hostname+'.log'
    log.startLogging(open(logfile,'a'))
    #log.startLogging(open(os.path.join(outputdir,socket.gethostname(),'twisted.log'),'w'))

    ## create Serial2Ws gateway factory
    ##
    wsMcuFactory = WsMcuFactory(wsurl)
    listenWS(wsMcuFactory)

    ## create serial port and serial port protocol ; modify that according to attached sensors
    ##
    try:
        log.msg('About to open serial port %s [%d baud] ..' % (port_usb0, baudrate_usb0))
        serialPort = SerialPort(wsMcuFactory.lemiProtocol, port_usb0, reactor, baudrate = baudrate_usb0)
    except:
        log.msg('Serial port %s [%d baud] not available' % (port_usb0, baudrate_usb0))

    #try:
    #    log.msg('About to open serial port %s [%d baud] ..' % (port_usb1, baudrate_usb1))
    #    #serialPort = SerialPort(wsMcuFactory.envProtocol, port_usb1, reactor, baudrate = baudrate_usb1)
    #except:
    #    log.msg('Serial port %s [%d baud] not available' % (port_usb1, baudrate_usb1))

    
    #try:
    #    timeoutfi = 20.0
    #    log.msg('About to open MagPy file connection with timeout [%d sec] ..' % timeoutfi)
    #    if wsMcuFactory.fileProtocol.establishConnection('/srv/ws/xxx/env1/2013/env1_2013-02-20.txt'):
    #        fprot = task.LoopingCall(wsMcuFactory.fileProtocol.fileConnected)
    #        fprot.start(timeoutfi) # call every sixty seconds
    #    else:
    #        log.msg('Selected path_or_url is not accessible')
    #except:
    #    log.msg('File/MagPy module not available')

    try:
        timeoutow = 30.0
        log.msg('About to open one wire connection with timeout [%d sec] ..' % timeoutow)
        oprot = task.LoopingCall(wsMcuFactory.owProtocol.owConnected)
        oprot.start(timeoutow) # call every sixty seconds
    except:
        log.msg('One wire module not available')


    #reactor.callInThread(wsMcuFactory.owProtocol.owConnected)
    #reactor.callInThread(dataFromFile)
    print " All Tasks started"

    ## create embedded web server for static files
    ##
    webdir = File(".")
    web = Site(webdir)
    reactor.listenTCP(webport, web)

    print " Now starting reactor"

    ## start Twisted reactor ..
    ##
    reactor.run()
