from __future__ import print_function
import sys, time, os, socket
import struct, binascii, re, csv
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

iddict = {'f': '10', 'x': '11', 'y': '12', 'z': '13', 'df': '14', 't': '30', 'rh': '33', 'p': '35', 'w': '38'}

"""
0: clientname                   -- str (atlas)
1: timestamp (PC)               -- str (2013-01-23 12:10:32.712475)
2: date (PC)                    -- str (2013-01-23)
3: outtime (PC)                 -- str (12:10:32.712475)
4: timestamp (sensor)           -- str (2013-01-23 12:10:32.712475)
5: GPS coordinates              -- str (??.??N ??.??E)
9: Sensor Description           -- str (to be found in the adict)
10: f                           -- float (48633.04) [nT]
11: x                           -- float (20401.3) [nT]
12: y                           -- float (-30.0) [nT]
13: z                           -- float (43229.7) [nT]
14: df                          -- float (0.06) [nT]
30: T (ambient)                 -- float (7.2) [C]
31: T (sensor)                  -- float (10.0) [C]
32: T (electronics)             -- float (12.5) [C]
33: rh (relative humidity)      -- float (99.0) [%]
34: T (dewpoint)                -- float (6.0) [C]
38: W (weight)                  -- float (24.0042) [g]
40: Error code (POS1)           -- float (80) [-]
60: VDD (support voltage)       -- float (5.02) [V]
61: VAD (measured voltage)      -- float (2.03) [V]
62: VIS (measured voltage)      -- float (0.00043) [V]
"""

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
        log.err("Arduino - Protocol: Error while saving file")


## Arduino protocol
## --------------------

class ArduinoProtocol(LineReceiver):
    """
    Protocol to read Arduino data (usually from ttyACM0)
    Tested so far only for Arduino Uno on a Linux machine
    The protocol works only if the serial output follows the MagPy convention:
    Up to 99 Sensors are supported identified by unique sensor names and ID's.

    ARDUINO OUTPUT:
        - serial output on ttyACM0 needs to follow the MagPy definition:
            Three data sequences are supported:
            1.) The meta information
                The meta information line contains all information for a specific sensor.
                If more than one sensor is connected, then several meta information
                lines should be sent (e.g. M1:..., M2:..., M99:...)
                Meta lines should be resent once in a while (e.g. every 10-100 data points)
                Example:
                     M1: SensorName: MySensor, SensorID: 12345, SensorRevision: 0001
            2.) The header line
                The header line contains information on the provided data for each sensor.
                The typical format includes the MagPy key, the actual Variable and the unit.
                Key and Variable are separeted by an underscore, unit is provided in brackets.
                Like the Meta information the header should be sent out once in a while
                Example:
                     H1: f_F [nT], t1_Temp [deg C], var1_Quality [None], var2_Pressure [mbar]
            3.) The data line:
                The data line containes all data from a specific sensor
                Example:
                     D1: 46543.7898, 6.9, 10, 978.000

         - recording starts after meta and header information have been received

    MARTAS requirements:
         - add the following line to the sensor.txt
            ARDUINO             ACM0    9600
         - on the MARTAS machine an additional information file will be created
           containing the sensor information for connected ARDUINO boards:
           arduinolist.csv:
              "HMC5883_12345_0001","['x', 'y', 'z']"
           This file is used by the MARCOS machine to identify connected sensors and their keys

    """

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.board = sensor
        self.hostname = socket.gethostname()
        self.outputdir = outputdir
        print("Running on board", self.board)
        self.sensor = ''
        self.sensordict = {}
        self.eventstring = ''
        self.eventdict = {}
        self.idlist = []
        self.unitdict = {}
        self.vardict = {}
        self.keydict = {}

    def savearduinolist(self,filename, arduinolist):
        with open(filename, 'wb') as f:
            wr = csv.writer(f, quoting=csv.QUOTE_ALL)
            for row in arduinolist:
                wr.writerow(row)

    def loadarduinolist(self,filename):
        with open(filename, 'rb') as f:
            reader = csv.reader(f)
            arduinolist = [row for row in reader]
        return arduinolist

    def extendarduinolist(self, idnum):
        from os.path import expanduser
        home = expanduser("~")

        martasdir = [path for path, dirs, files in os.walk(home) if path.endswith('MARTAS')][0]
        arduinosensorfile = os.path.join(martasdir,'arduinolist.csv')
        log.msg('Checking Arduinofile: %s' % arduinosensorfile)
        arduinolist = []
        sensorelement = []
        try:
            arduinolist = self.loadarduinolist(arduinosensorfile)
            sensorelement = [elem[0] for elem in arduinolist]
            print("Liste", sensorelement)
        except:
            log.msg('Arduino: No Sensor list so far -or- Error while getting sensor list')
            pass
        if not self.sensordict[idnum] in sensorelement:
            arduinolist.append([self.sensordict[idnum], self.keydict[idnum]])
            self.savearduinolist(arduinosensorfile,arduinolist)

    def connectionMade(self):
        log.msg('%s connected.' % self.board)

    def processArduinoData(self, idnum, data):
        """Convert raw ADC counts into SI units as per datasheets"""
        printdata = False

        currenttime = datetime.utcnow()
        outdate = datetime.strftime(currenttime, "%Y-%m-%d")
        filename = outdate
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")

        datearray = timeToArray(timestamp)
        packcode = '6hL'
        sensorid = self.sensordict[idnum]

        events = self.eventdict[idnum].replace('evt','').split(',')[3:-1]

        if not len(events) == len(data):
            log.msg('Error while assigning events to data')

        values = []
        multiplier = []
        for dat in data:
            try:
                values.append(float(dat))
                datearray.append(int(float(dat)*10000))
                packcode = packcode + 'l'
                multiplier.append(10000)
            except:
                log.msg('Error while appending data to file (non-float?): %s ' % dat )

        try:
            data_bin = struct.pack(packcode,*datearray)
        except:
            log.msg('Error while packing binary data')
            pass

        header = "# MagPyBin %s %s %s %s %s %s %d" % (sensorid, str(self.keydict[idnum]).replace("'","").strip(), str(self.vardict[idnum]).replace("'","").strip(), str(self.unitdict[idnum]).replace("'","").strip(), str(multiplier).replace(" ",""), packcode, struct.calcsize(packcode))

        if printdata:
            #print header
            print(timestamp, values)

        # File Operations
        dataToFile(self.outputdir, sensorid, filename, data_bin, header)

        evt0 = {'id': 0, 'value': self.hostname}
        evt1 = {'id': 1, 'value': timestamp}
        evt3 = {'id': 3, 'value': outtime}
        for idx,event in enumerate(events):
            execstring = "evt"+event+" = {'id': "+event+", 'value': "+ str(values[idx])+"}"
            exec(execstring)
        evt99 = {'id': 99, 'value': 'eol'}

        return eval(self.eventdict[idnum])


    def analyzeHeader(self, line):
        print("Getting Header")
        eventlist = []
        head = line.strip().split(':')
        headernum = int(head[0].strip('H'))
        header = head[1].split(',')
        varlist = []
        keylist = []
        unitlist = []
        for elem in header:
            an = elem.strip(']').split('[')
            try:
                if len(an) < 1:
                    print("Arduino: error when analyzing header")
                    return
            except:
                print("Arduino: error when analyzing header")
                return
            var = an[0].split('_')
            key = var[0].strip().lower()
            variable = var[1].strip().lower()
            unit = an[1].strip()
            if not variable in iddict:
                variable = key
                if not variable in iddict:
                    variable = 'x'
            keylist.append(key)
            varlist.append(variable)
            unitlist.append(unit)
            eventlist.append('evt'+iddict[variable])
        eventstring = ','.join(eventlist)

        if len(eventstring) > 0:
            self.eventdict[headernum] = 'evt0,evt1,evt3,'+eventstring+',evt99'
            print("Found components %s for ID %d" % (eventstring, headernum))
            self.vardict[headernum] = varlist
            self.unitdict[headernum] = unitlist
            self.keydict[headernum] = keylist


    def getMeta(self, line):
        print("Getting Metadata - does not support more than 99 sensors!")
        sensrev = '0001'
        sensid = '12345'
        try:
            metaident = line.strip().split(':')
            metanum = int(metaident[0].strip('M'))
            meta = line[3:].strip().split(',')

            metadict = {}
            for elem in meta:
                el = elem.split(':')
                metadict[el[0].strip()] = el[1].strip()
        except:
            log.msg('Could not interpret meta data - skipping')
            return

        if 'SensorRevsion' in metadict:
            sensrev = metadict['SensorRevision']
        if 'SensorID' in metadict:
            sensid = metadict['SensorID']
        if not 'SensorName' in metadict:
            print("No Sensorname provided - aborting")
            return

        self.sensor = metadict['SensorName']+'_'+sensid+'_'+sensrev
        self.sensordict[metanum] = self.sensor
        print("Found Sensor %s for ID %d" % (self.sensor, metanum))

        # Write a file to the martas directory: arduino.txt containig the sensorids
        # for each connected sensor and its components
        # This can then be used by the collector and the web scripts

        #except:
        #    print "could not interpret meta information"


    def lineReceived(self, line):
        #print "Received line", line
        # Create a list of sensors like for OW
        # dipatch with the appropriate sensor

        lineident = line.split(':')
        try:
            idnum = int(lineident[0][1:])
        except:
            idnum = 999
        if not idnum == 999:
            if not idnum in self.idlist:
                if line.startswith('H'):
                    self.analyzeHeader(line)
                elif line.startswith('M'):
                    self.getMeta(line)
                if idnum in self.eventdict and idnum in self.sensordict:
                    self.idlist.append(idnum)
                    self.extendarduinolist(idnum)
            else:
                if line.startswith('D') and idnum in self.eventdict and idnum in self.sensordict:
                    dataar = line.strip().split(':')
                    dataident = int(dataar[0].strip('D'))
                    data = dataar[1].strip().split(',')
                    eventstring = self.eventdict[dataident]

                    exec(eventstring+" = self.processArduinoData(dataident, data)")

                    ## publish event to all clients subscribed to topic
                    ##
                    eventlist = eventstring.split(',')

                    dispatch_url =  "http://example.com/"+self.hostname+"/ard#"+self.board+"-value"
                    for event in eventlist:
                        self.wsMcuFactory.dispatch(dispatch_url, eval(event))
        else:
            #print "Arduino: could not interpret line", line
            pass
        #except ValueError:
        #    log.err('Unable to parse data %s' % line)
        #    #return
