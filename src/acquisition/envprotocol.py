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


## Environment protocol
## --------------------

class EnvProtocol(LineReceiver):
    """
    Protocol to read MessPC EnvironmentalSensor 5 data from usb unit
    Each sensor has its own class (that can be improved...)
    The protocol defines the sensor name in its init section, which 
    is used to dipatch url links and define local storage folders

    """

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.hostname = socket.gethostname()
        self.outputdir = outputdir
        print self.sensor

    @exportRpc("control-led")
    def controlLed(self, status):
        if status:
            print "turn on LED"
            self.transport.write('1')
        else:
            print "turn off LED"
            self.transport.write('0')

    @exportRpc("send-command")
    def sendCommand(self, command):
        if not command == "":
            print command
            #self.transport.write(command)

    def connectionMade(self):
        log.msg('%s connected.' % self.sensor)

    def processEnvData(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""

        currenttime = datetime.utcnow()
        outdate = datetime.strftime(currenttime, "%Y-%m-%d")
        filename = outdate
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        #header = "# MagPyBin, sensor_id, [parameterlist], [unit-conversion-list], packing string, length"
        packcode = '6hLLLL'
        sensorid = self.sensor
        header = "# MagPyBin %s %s %s %s %s %s %d" % (sensorid, '[t1,t2,var1]', '[T,DewPoint,RH]', '[deg_C,deg_C,per rh]', '[1000,1000,1000]', packcode, struct.calcsize(packcode))

        valrh = re.findall(r'\d+',data[0])
        if len(valrh) > 1:
            temp = float(valrh[0] + '.' + valrh[1])
        else:
            temp = float(valrh[0])
        valrh = re.findall(r'\d+',data[1])
        if len(valrh) > 1:
            rh = float(valrh[0] + '.' + valrh[1])
        else:
            rh = float(valrh[0])
        valrh = re.findall(r'\d+',data[2])
        if len(valrh) > 1:
            dew = float(valrh[0] + '.' + valrh[1])
        else:
            dew = float(valrh[0])

        datearray = timeToArray(timestamp)

        try:
            datearray = timeToArray(timestamp)
            datearray.append(int(temp*1000))
            datearray.append(int(dew*1000))
            datearray.append(int(rh*1000))
            data_bin = struct.pack(packcode,*datearray)
        except:
            log.msg('Error while packing binary data')
            pass

        # File Operations
        dataToFile(self.outputdir, sensorid, filename, data_bin, header)

        # create a dictionary out of the input file
 
        evt4 = {'id': 8, 'value': outtime}
        evt2 = {'id': 5, 'value': temp}
        evt1 = {'id': 7, 'value': rh}
        evt3 = {'id': 6, 'value': dew}
        evt5 = {'id': 10, 'value': self.hostname}
        evt6 = {'id': 9, 'value': outdate}
        evt7 = {'id': 99, 'value': 'eol'}
 
        return evt4, evt1, evt2, evt3, evt5, evt6, evt7


    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+self.hostname+"/env#"+self.sensor+"-value"
        try:
            data = line.split()
            if len(data) == 3:
                evt1,evt2,evt3,evt4,evt5,evt6,evt7 = self.processEnvData(data)
            else:
                print 'Data error'


            ## publish event to all clients subscribed to topic
            ##
            self.wsMcuFactory.dispatch(dispatch_url, evt1)
            self.wsMcuFactory.dispatch(dispatch_url, evt2)
            self.wsMcuFactory.dispatch(dispatch_url, evt3)
            self.wsMcuFactory.dispatch(dispatch_url, evt4)
            self.wsMcuFactory.dispatch(dispatch_url, evt5)
            self.wsMcuFactory.dispatch(dispatch_url, evt6)
            self.wsMcuFactory.dispatch(dispatch_url, evt7)
            #log.msg("Analog value: %s" % str(evt4))

        except ValueError:
            log.err('Unable to parse data %s' % line)
            #return

