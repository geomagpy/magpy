from __future__ import print_function
import sys, time, os, socket

from twisted.protocols.basic import LineReceiver
from autobahn.wamp import exportRpc

from twisted.internet import reactor

from twisted.python import usage, log
from twisted.internet.serialport import SerialPort
from twisted.web.server import Site
from twisted.web.static import File

from autobahn.websocket import listenWS
from autobahn.wamp import WampServerFactory, WampServerProtocol, exportRpc

from datetime import datetime, timedelta
import struct, binascii
import re

hostname = socket.gethostname()
outputdir = '/srv/ws' # is defined in main file - should be overwritten then


lastActualtime = datetime.utcnow() # required for cs output



"""
The ID list for subscriptions:
0: time (PC)    -- str(12:10:32)
1: x            -- float
2: y            -- float
3: z            -- float
4: f            -- float
5: temp         -- float
6: dew(temp2)   -- float
7: humidity     -- float
8: datetime (PC)- str(2013-01-23 12:10:32.712475)
9: date         -- str(2013-01-23)
10: clientname  -- str(europa)
11: sensoralias -- str(Western Tunnel))
12: vdd         -- float
13: vad         -- float
14: vis         -- float
15: df          -- float
16: error code  -- float
17: datetime (sensor) --str(2013-01-23 12:10:32.712475)
99: eol         -- str(eol)
"""

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

def dataToFile(sensorid, filedate, bindata, header):
    # File Operations
    try:
        subdirname = socket.gethostname()
        path = os.path.join(outputdir,subdirname,sensorid)
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


class FileProtocol():
    def __init__(self, wsMcuFactory):
        self.wsMcuFactory = wsMcuFactory
        print("Init")
        self.path_or_url = ''

    def establishConnection(self, path_or_url):
        log.msg('Setting path for data access to: %s ' % (path_or_url))
        # Test whether path_or_url can be reachend
        self.path_or_url = path_or_url
        if os.path.isfile(self.path_or_url):
            return True
        else:
            log.msg('File not existing: %s ' % (path_or_url))
            return False


    def fileConnected(self):
        # include MagPy here: stream = pmread(path_or_url=xxx)
        # return xyzft1 whatsoever contains values
        log.msg('Opening: %s ' % (path_or_url))
        with open(self.path_or_url, 'r') as myfile:
            print (list(myfile)[-1])
        myfile.closed


## GEM -GSM90 protocol
##
class GSM90Protocol(LineReceiver):
    def __init__(self, wsMcuFactory):
        self.wsMcuFactory = wsMcuFactory
        print("Initialize the connection and set automatic mode (use ser.commands?)")

    def initConnection(self, path_or_url):
        log.msg('MagPy Module connected - Accessing file: %s ' % (path_or_url))
        # Test whether path_or_url can be reachend
        self.path_or_url = path_or_url
        log.msg('MagPy Module: File connected')
        return True

    def connectionMade(self):
        log.msg('Serial port connected.')


## Caesium protocol
##
class CsProtocol(LineReceiver):
    """
    Protocol to read GSM CS Sensor data from serial unit
    Each sensor has its own class (that can be improved...)
    The protocol defines the sensor name in its init section, which
    is used to dipatch url links and define local storage folders

    """

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = 'cs'

    def connectionMade(self):
        log.msg('Serial port connected.')

    def processData(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""

        currenttime = datetime.utcnow()
        filename = datetime.strftime(currenttime, "%Y-%m-%d")
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")

        global lastActualtime

        try:
            value = float(data[0])
            if 10000 < value < 100000:
                intensity = value
            else:
                intensity = 0.0
        except ValueError:
            log.err("CS - Protocol: Not a number. Instead found:", data[0])

        # define pathname for local file storage (default dir plus hostname plus sensor plus year) and create if not existing
        try:
            subdirname = socket.gethostname()
            path = os.path.join(outputdir,subdirname,self.sensor,str(currenttime.year))
            if not os.path.exists(path):
                os.makedirs(path)
        except:
            log.err("CS - Protocol: Could not create directory.")

        """
        # put data to buffer
        databuf  = []
        if len(databuf) < 100 and day == currday:
             databuf.append()
        else:
           # cretae new buffer with first value
           #call thread with:
           # pack data to binary
           # if file exists write header containing type id, bytes in line and packing info
           # write the data to file
           pass
        """

        with open(os.path.join(path,self.sensor+'_'+filename+".txt"), "a") as myfile:
            try:
                myfile.write(str(actualtime) + " " + str(intensity) + "\n")
            except:
                log.err("CS - Protocol: Error while saving file")

        #return value every second
        if lastActualtime+timedelta(microseconds=999000) >= currenttime:   # Using ms instead of s accounts for only small errors, not all.
            evt1 = {'id': 4, 'value': 0}
            evt4 = {'id': 0, 'value': 0}
        else:
            evt1 = {'id': 4, 'value': intensity}
            evt4 = {'id': 0, 'value': outtime}
            lastActualtime = currenttime

        return evt1,evt4

    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+hostname+"/cs#"+self.sensor+"-value"
        try:
            data = line.strip('$').split(',')
            evt1, evt4 = self.processData(data)
        except ValueError:
            log.err('CS - Protocol: Unable to parse data %s' % line)
            #return
        except:
            pass


        if evt1['value'] and evt4['value']:
            try:
                ## publish event to all clients subscribed to topic
                ##
                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                self.wsMcuFactory.dispatch(dispatch_url, evt4)
                #log.msg("Analog value: %s" % str(evt4))
            except:
                log.err('CS - Protocol: wsMcuFactory error while dispatching data.')
