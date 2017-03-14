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
        log.err("Protocol: Error while saving file")        


## meteolabor BM35 protocol
##
class BM35Protocol(LineReceiver):
    """
    The BM35 protocol for extracting atmospheric pressure data from BM35
    SETUP of BM35 (RS485 version):
        1.) connect a RS485 to RS232 converter
    Supported modes:
        instantaneous values
        in future filtered values
    """

    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.outputdir = outputdir
        self.hostname = socket.gethostname()
        print "Initialize the connection and set automatic mode (use ser.commands?)"
        ### TODO send A00d03000^M to serial - done by serial-init.py !!!

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
        log.msg('BM35 connection lost. Perform steps to restart it!')

    def connectionMade(self):
        log.msg('%s connected.' % self.sensor)

    def processData(self, data):

        currenttime = datetime.utcnow()
        date = datetime.strftime(currenttime, "%Y-%m-%d")
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        ## TODO??? -> Roman!
        #intensity = 88888.8
        pressure1 = 88888.8
        pressure2 = 88888.8
        typ = "none"
        dontsavedata = False

#        packcode = '6hLLL'
#        header = "# MagPyBin %s %s %s %s %s %s %d" % (self.sensor, '[var3,var4]', '[p1,p2]', '[mBar,mBar]', '[1000,1000]', packcode, struct.calcsize(packcode))
        packcode = '6hLL'
        header = "# MagPyBin %s %s %s %s %s %s %d" % (self.sensor, '[var3]', '[p]', '[mBar]', '[1000]', packcode, struct.calcsize(packcode))

        try:
            # Extract data
            data_array = data.strip().split(',')
            #print data_array, len(data_array)
            if len(data_array) == 2:
                typ = "valid"
            # add other types here
        except:
            # TODO??? base x mobile?
            log.err('BM35 - Protocol: Output format not supported - use either base, ... or mobile')
        # Extracting the data from the instrument

        if typ == "valid": 
            pressure1 = float(data_array[0].strip())
            # pressure1 is raw 
            pressure2 = float(data_array[1].strip())
            # pressure2 is calculated from pressure1 by calibration values
        elif typ == "none":
            dontsavedata = True
            pass

        # TODO right now, saving data is not necessary
        try:
            if not typ == "none":
                # extract time data
                datearray = timeToArray(timestamp)
                try:
                    datearray.append(int(pressure2*1000.))
                    data_bin = struct.pack(packcode,*datearray)
                    dataToFile(self.outputdir,self.sensor, date, data_bin, header)
                except:
                    log.msg('BM35 - Protocol: Error while packing binary data')
                    pass
        except:
            log.msg('BM35 - Protocol: Error with binary save routine')
            pass
        evt1 = {'id': 1, 'value': timestamp}
        evt35 = {'id': 35, 'value': pressure2}
        if not ((pressure2 < 1300) and (pressure2 > 800)):
            print('BM35: Druck ausserhalb ',pressure2)
        evt99 = {'id': 99, 'value': 'eol'}

        return evt1,evt35,evt99
         
    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+self.hostname+"/bm3#"+self.sensor+"-value"
        try:
            evt1, evt35, evt99 = self.processData(line)
        except ValueError:
            log.err('BM35 - Protocol: Unable to parse data %s' % line)
        except:
            pass

        try:
            ## publish event to all clients subscribed to topic
            ##
            self.wsMcuFactory.dispatch(dispatch_url, evt1)
            #self.wsMcuFactory.dispatch(dispatch_url, evt3)
            self.wsMcuFactory.dispatch(dispatch_url, evt35)
            self.wsMcuFactory.dispatch(dispatch_url, evt99)
        except:
            log.err('BM35 - Protocol: wsMcuFactory error while dispatching data.')



