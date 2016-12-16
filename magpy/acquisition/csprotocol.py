import sys, time, os, socket
import struct, binascii, re, csv
from datetime import datetime, timedelta

# Twisted
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.python import usage, log
from twisted.internet.serialport import SerialPort
from twisted.web.server import Site
from twisted.web.static import File

try: # version > 0.8.0
    from autobahn.wamp1.protocol import exportRpc
except:
    from autobahn.wamp import exportRpc

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
    def __init__(self, wsMcuFactory, sensor,outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.outputdir = outputdir
        self.hostname = socket.gethostname()

    def connectionMade(self):
        log.msg('%s connected.' % self.sensor)

    def processData(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""

        currenttime = datetime.utcnow()
        # Correction for ms time to work with databank:
        currenttime_ms = currenttime.microsecond/1000000.
        ms_rounded = round(float(currenttime_ms),3)
        if not ms_rounded >= 1.0:
            currenttime = currenttime.replace(microsecond=int(ms_rounded*1000000.))
        else:
            currenttime = currenttime.replace(microsecond=0) + timedelta(seconds=1.0)
        filename = datetime.strftime(currenttime, "%Y-%m-%d")
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        lastActualtime = currenttime
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")

        packcode = '6hLL'
        header = "# MagPyBin %s %s %s %s %s %s %d" % (self.sensor, '[f]', '[f]', '[nT]', '[1000]', packcode, struct.calcsize(packcode))

        try:
            value = float(data[0].strip('$'))
            if 10000 < value < 100000:
                intensity = value
            else:
                intensity = 88888.0
        except ValueError:
            log.err("CS - Protocol: Not a number. Instead found:", data[0])
            intensity = 88888.0

        try:
            datearray = timeToArray(timestamp)
            datearray.append(int(intensity*1000))
            data_bin = struct.pack(packcode,*datearray)
            # File Operations
            dataToFile(self.outputdir, self.sensor, filename, data_bin, header)
        except:
            log.msg('Error while packing binary data')
            pass

        #return value every second
        if lastActualtime+timedelta(microseconds=999000) <= currenttime:   # Using ms instead of s accounts for only small errors, not all.
            evt1 = {'id': 1, 'value': 0}
            evt3 = {'id': 3, 'value': 0}
            evt10 = {'id': 10, 'value': 0}
            evt99 = {'id': 99, 'value': 'eol'}
        else:
            evt1 = {'id': 1, 'value': timestamp}
            evt3 = {'id': 3, 'value': outtime}
            evt10 = {'id': 10, 'value': intensity}
            evt99 = {'id': 99, 'value': 'eol'}
            lastActualtime = currenttime

        return evt1,evt3,evt10,evt99

    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+self.hostname+"/cs#"+self.sensor+"-value"
        try:
            data = line.strip('$').split(',')
            evt1, evt3, evt10, evt99 = self.processData(data)
        except ValueError:
            log.err('CS - Protocol: Unable to parse data %s' % line)
            return
        except:
            pass

        try:
            if evt1['value'] and evt3['value']:
                try:
                    ## publish event to all clients subscribed to topic
                    ##
                    self.wsMcuFactory.dispatch(dispatch_url, evt1)
                    self.wsMcuFactory.dispatch(dispatch_url, evt3)
                    self.wsMcuFactory.dispatch(dispatch_url, evt10)
                    self.wsMcuFactory.dispatch(dispatch_url, evt99)
                    #log.msg("Analog value: %s" % str(evt4))
                except:
                    log.err('CS - Protocol: wsMcuFactory error while dispatching data.')
        except:
            log.err('CS - Protocol: No appropriate data events returned')
            pass
