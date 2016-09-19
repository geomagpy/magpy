from __future__ import print_function


import sys, time, os, socket
import struct, binascii, re
from datetime import datetime, timedelta

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
    # Converts time string of format 2013-12-12 23:12:23.122324
    # to an array similiar to a datetime object
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
        log.err("KERN - Protocol: Error while saving file")        


## Environment protocol
## --------------------

class KernProtocol(LineReceiver):
    """
    Protocol to read KERN balance data from RS232
    The protocol defines the sensor name in its init section, which 
    is used to dipatch url links and define local storage folders

    TODO: rounding error with datearray calculation - leads to microsecond diffs between timestamp and datearry
    -----  corrected that here c---- check out validity in other datasets (e.g. Lemi)
    """

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.hostname = socket.gethostname()
        self.outputdir = outputdir
        print(self.sensor)


    def connectionMade(self):
        log.msg('%s connected.' % self.sensor)

    def processKernData(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""

        currenttime = datetime.utcnow()
        outdate = datetime.strftime(currenttime, "%Y-%m-%d")
        filename = outdate
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        #header = "# MagPyBin, sensor_id, [parameterlist], [unit-conversion-list], packing string, length"
        packcode = '6hLL'
        sensorid = self.sensor
        header = "# MagPyBin %s %s %s %s %s %s %d" % (sensorid, '[var1]', '[Weight]', '[g]', '[10000]', packcode, struct.calcsize(packcode))

        try:
            val1 = data[0].split(',')
            if len(val1) > 0:
                val = float(val1[1])
                typ = val1[0]
            else:
                val = 0.0
                typ = "NN"
        except:
            val = 0.0
            typ = "NN"

        datearray = timeToArray(timestamp)

        try:
            datearray = timeToArray(timestamp)
            datearray.append(int(val*10000))
            data_bin = struct.pack(packcode,*datearray)
            # File Operations
            dataToFile(self.outputdir, sensorid, filename, data_bin, header)
        except:
            log.msg('Error while packing binary data')
            pass

        try:
            # reformat time from datearray to acertain similar rounding of microsecs
            dob = datetime.datetime(*datearray[:7])
            timestamp = datetime.strftime(dob,"%Y-%m-%d %H:%M:%S.%f")
        except:
            log.msg('Error reformating time - remove that !!!')
            pass

  
        evt0 = {'id': 0, 'value': self.hostname}
        evt1 = {'id': 1, 'value': timestamp}
        evt3 = {'id': 3, 'value': outtime}
        evt38 = {'id': 38, 'value': val}
        evt99 = {'id': 99, 'value': 'eol'}
        
        return evt0,evt1,evt3,evt38,evt99


    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+self.hostname+"/kern#"+self.sensor+"-value"
        
        try:
            data = line.split()
            if len(data) == 2:
                evt0,evt1,evt3,evt38,evt99 = self.processKernData(data)
            else:
                print('Data error')

            ## publish event to all clients subscribed to topic
            ##
            try:
                self.wsMcuFactory.dispatch(dispatch_url, evt0)
            except:
                log.err("Missing hostname")
                pass
            try:
                self.wsMcuFactory.dispatch(dispatch_url, evt1)
            except:
                pass
            try:
                self.wsMcuFactory.dispatch(dispatch_url, evt3)
            except:
                pass
            try:
                self.wsMcuFactory.dispatch(dispatch_url, evt38)
                self.wsMcuFactory.dispatch(dispatch_url, evt99)
            except:
                pass 
            #log.msg("Analog value: %s" % str(evt4))

        except ValueError:
            log.err('Unable to parse data %s' % line)
            #return

