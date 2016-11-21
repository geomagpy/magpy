from __future__ import print_function
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


## GEM -GSM90 protocol
##
class GSM90Protocol(LineReceiver):
    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.outputdir = outputdir
        self.hostname = socket.gethostname()
        print("Initialize the connection and set automatic mode (use ser.commands?)")

    @exportRpc("control-led")
    def controlLed(self, status):
        if status:
            print("turn on LED")
            self.transport.write('1')
        else:
            print("turn off LED")
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
        packcode = '6hLLL6hL'
        header = "# MagPyBin %s %s %s %s %s %s %d" % (self.sensor, '[f,var1,sectime]', '[f,errorcode,internaltime]', '[nT,none,none]', '[1000,1,1]', packcode, struct.calcsize(packcode))

        try:
            # Extract data
            # old data looks like 04-22-2015 142244  48464.53 99
            data_array = data.strip().split()
            if len(data_array) == 4:
                intensity = float(data_array[2])
                err_code = int(data_array[3])
                try:
                    try:
                        internal_t = datetime.strptime(data_array[0]+'T'+data_array[1], "%m-%d-%YT%H%M%S.%f")
                    except:
                        internal_t = datetime.strptime(data_array[0]+'T'+data_array[1], "%m-%d-%YT%H%M%S")
                    internal_time = datetime.strftime(internal_t, "%Y-%m-%d %H:%M:%S.%f")
                except:
                    internal_time = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S.%f")
                #print internal_time
            else:
                err_code = 0
                intensity = float(data_array[0])
                internal_time = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
        except:
            log.err('GSM90 - Protocol: Data formatting error. Data looks like: %s' % data)
        try:
            ## GSM90 does not provide any info on whether the GPS reading is OK or not
            gps = True
            if gps:
                baktimestamp = timestamp
                timestamp = internal_time
                internal_time = baktimestamp
            # extract time data
            datearray = timeToArray(timestamp)
            try:
                datearray.append(int(intensity*1000.))
                datearray.append(err_code)
                #print timestamp, internal_time
                internalarray = timeToArray(internal_time)
                datearray.extend(internalarray)
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
        evt4 = {'id': 4, 'value': internal_time}
        evt10 = {'id': 10, 'value': intensity}
        evt40 = {'id': 40, 'value': err_code}
        evt99 = {'id': 99, 'value': 'eol'}

        return evt1,evt3,evt10,evt99

    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+self.hostname+"/gsm#"+self.sensor+"-value"
        try:
            data = line
            #print ("Line", line)
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
