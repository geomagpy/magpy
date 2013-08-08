import sys, time, os, socket
import struct, binascii, re
from datetime import datetime, timedelta

from twisted.protocols.basic import LineReceiver
from autobahn.wamp import exportRpc

from twisted.internet import reactor

from twisted.python import usage, log
from twisted.internet.serialport import SerialPort
from twisted.web.server import Site
from twisted.web.static import File

from autobahn.websocket import listenWS
from autobahn.wamp import WampServerFactory, WampServerProtocol, exportRpc

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


## GEM -GSM90 protocol
##
class GSM90Protocol(LineReceiver):
    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.outputdir = outputdir
        self.hostname = socket.gethostname()
        print "Initialize the connection and set automatic mode (use ser.commands?)"

    @exportRpc("control-led")
    def controlLed(self, status):
        if status:
            print "turn on LED"
            self.transport.write('1')
        else:
            print "turn off LED"
            self.transport.write('0')

    #def send_command(...?)

    def connectionLost(self):
        log.msg('GSM90 connection lost. Perform steps to restart it!')

    def connectionMade(self):
        log.msg('%s connected.' % self.sensor)

    def processData(self, data):

        currenttime = datetime.utcnow()
        date = datetime.strftime(currenttime, "%Y-%m-%d")
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        packcode = '6hLL'
        header = "# MagPyBin %s %s %s %s %s %s %d" % ('GSM90', '[f]', '[f]', '[nT]', '[1000]', packcode, struct.calcsize(packcode))

        try:
            # Extract data
            data_array = data.strip().split()
            intensity = float(data_array[0])
        except:
            log.err('GSM90 - Protocol: Data formatting error.')
        try:
            # extract time data
            datearray = timeToArray(timestamp)
            try:
                datearray.append(int(intensity*1000.))
                data_bin = struct.pack(packcode,*datearray)
                dataToFile(self.outputdir,self.sensor, date, data_bin, header)
            except:
                log.msg('GSM90 - Protocol: Error while packing binary data')
                pass
        except:
            log.msg('GSM90 - Protocol: Error with binary save routine')
            pass

        evt1 = {'id': 1, 'value': timestamp}
        evt3 = {'id': 3, 'value': outtime}
        evt10 = {'id': 10, 'value': intensity}
        evt99 = {'id': 99, 'value': 'eol'}

        return evt1,evt3,evt10,evt99
         
    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+self.hostname+"/gsm#"+self.sensor+"-value"
        try:
            data = line
            evt1, evt3, evt10, evt99 = self.processData(data)
        except ValueError:
            log.err('GSM90 - Protocol: Unable to parse data %s' % line)
        except:
            pass


        try:
            ## publish event to all clients subscribed to topic
            ##
            self.wsMcuFactory.dispatch(dispatch_url, evt1)
            self.wsMcuFactory.dispatch(dispatch_url, evt3)
            self.wsMcuFactory.dispatch(dispatch_url, evt10)
            self.wsMcuFactory.dispatch(dispatch_url, evt99)
        except:
            log.err('GSM90 - Protocol: wsMcuFactory error while dispatching data.')



