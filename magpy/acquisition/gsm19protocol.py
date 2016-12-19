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
        log.err("Protocol: Error while saving file")        


## GEM -GSM19 protocol
##
class GSM19Protocol(LineReceiver):
    """
    The GSM 19 protocol for extracting RTT data from GSM19
    SETUP of GSM19:
        1.) in the main menu go to C-Info
        2.) in info select B-RS232
        3.) choose BAUD rate of 115200 - press F
        4.) real time RS232 transmission: select yes and press F
        5.) F again, thats it go back to main menu
    Supported modes:
        base
    """

    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.outputdir = outputdir
        self.hostname = socket.gethostname()
        print ("Initialize the connection and set automatic mode (use ser.commands?)")

    @exportRpc("control-led")
    def controlLed(self, status):
        if status:
            print ("turn on LED")
            self.transport.write('1')
        else:
            print ("turn off LED")
            self.transport.write('0')

    #def send_command(...?)

    def connectionLost(self):
        log.msg('GSM19 connection lost. Perform steps to restart it!')

    def connectionMade(self):
        log.msg('%s connected. Getting data...' % self.sensor)

    def processData(self, data):

        currenttime = datetime.utcnow()
        date = datetime.strftime(currenttime, "%Y-%m-%d")
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        intensity = 88888.8
        typ = "none"
        dontsavedata = False

        packcode = '6hLLl'
        header = "# MagPyBin %s %s %s %s %s %s %d" % (self.sensor, '[f,var1]', '[f,err]', '[nT,none]', '[1000,1000]', packcode, struct.calcsize(packcode))

        try:
            # Extract data
            data_array = data.strip().split()
            #print "Data array", len(data_array)
            if len(data_array) == 3:
                typ = "valid"
            # add other types here
        except:
            log.err('GSM19 - Protocol: Output format not supported - use either base, ... or mobile')

        # Extracting the data from the station
        # Extrat time info and use as primary if GPS is on (in this case PC time is secondary)
        #                          PC is primary when a GPS is not connected

        if typ == "valid": # Comprises Mobile and Base Station mode with single sensor and no GPS
            intensity = float(data_array[1])
            #print "Intensity", intensity
            # Extracting time from instrument - put that to the primary time column?
            systemtime = datetime.strptime(date+"-"+data_array[0], "%Y-%m-%d-%H%M%S.%f")
            #print "Times:", systemtime, timestamp
            # Test whether data_array[2] == int
            if len(data_array[2]) < 3:
                typ = "base"
                errorcode = int(data_array[2])
            else:
                typ = "gradient"
                gradient = float(data_array[2])
        elif typ == "none":
            dontsavedata = True
            pass

        try:
            if not typ == "none":
                # extract time data
                datearray = timeToArray(timestamp)
                try:
                    datearray.append(int(intensity*1000.))
                    if typ == 'base':
                        datearray.append(int(errorcode*1000.))
                    else:
                        datearray.append(int(gradient*1000.))
                    data_bin = struct.pack(packcode,*datearray)
                    dataToFile(self.outputdir,self.sensor, date, data_bin, header)
                except:
                    log.msg('GSM19 - Protocol: Error while packing binary data')
                    pass
        except:
            log.msg('GSM19 - Protocol: Error with binary save routine')
            pass

        evt1 = {'id': 1, 'value': timestamp}
        evt3 = {'id': 3, 'value': outtime}
        evt10 = {'id': 10, 'value': intensity}
        evt99 = {'id': 99, 'value': 'eol'}

        return evt1,evt3,evt10,evt99
         
    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+self.hostname+"/gn#"+self.sensor+"-value"
        #print ("got", line)
        try:
            #print line
            evt1, evt3, evt10, evt99 = self.processData(line)
        except ValueError:
            log.err('GSM19 - Protocol: Unable to parse data %s' % line)
        except:
            pass

        #print "Evt:", evt10, evt1

        try:
            ## publish event to all clients subscribed to topic
            ##
            self.wsMcuFactory.dispatch(dispatch_url, evt1)
            self.wsMcuFactory.dispatch(dispatch_url, evt3)
            self.wsMcuFactory.dispatch(dispatch_url, evt10)
            self.wsMcuFactory.dispatch(dispatch_url, evt99)
        except:
            log.err('GSM19 - Protocol: wsMcuFactory error while dispatching data.')
