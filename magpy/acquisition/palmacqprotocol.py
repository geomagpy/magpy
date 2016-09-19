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
        log.err("PalmAcq - Protocol: Error while saving file")


## PalmAcq protocol
## --------------------

class PalmAcqProtocol(LineReceiver):
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
    delimiter = "\r"

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensorid = sensor
        self.hostname = socket.gethostname()
        self.outputdir = outputdir
        self.sensor = ''
        self.sensordict = {}
        self.ConversionConstant = 40/4/float(int("0x800000",16))
        eventstring = "evt0,evt1,evt3,evt11,evt12,evt13,evt32,evt60,evt99"
        self.eventlist = eventstring.split(',')

    def connectionMade(self):
        log.msg('%s connected.' % self.sensorid)

    def extractPalmAcqData(self, line):
        """
         Method to convert hexadecimals to doubles
         Returns a data array
        """
        # INTERPRETING INCOMING DATA AND CONVERTING HEXDECIMALS TO DOUBLE
        if line.startswith('*'):
             try:
                 data = []
                 chunks = []
                 line = line.strip('*')
                 chunks.append(line[:6])
                 chunks.append(line[6:12])
                 chunks.append(line[12:18])
                 trigger = line[18]
                 ar = line.split(':')
                 if len(ar) == 2:
                     extended = ar[1]
                     chunks.append(extended[:4])
                     chunks.append(extended[4:8])
                     chunks.append(extended[8:12])
                     chunks.append(extended[12:16])
                     chunks.append(extended[16:20])
                 for idx, chunk in enumerate(chunks):
                     if len(chunk) == 6:
                         val = hex(int('0x'+chunk,16) ^ int('0x800000',16))
                         val = hex(int(val,16) - int('0x800000',16))
                         # Conversion constanst should be obtained from palmacq-init
                         val = float(int(val,16)) * self.ConversionConstant
                     elif len(chunk) == 4:
                         val = hex(int('0x'+chunk,16) ^ int('0x8000',16))
                         val = hex(int(val,16) - int('0x8000',16))
                         if idx == 3:
                             val = float(int(val,16)) * 0.000575 + 1.0
                         elif idx == 4:
                             val = float(int(val,16)) / 128.0
                         elif idx > 4:
                             val = float(int(val,16)) / 8000.0

                     data.append(val)

                 # SOME TEST OUTPUT
                 #if len(data)> 4:
                 #    print datetime.utcnow(), data
                 #print data, trigger

                 return data, trigger
             except:
                 #print "PALMACQ: an error occurred while interpreting the hexadecimal code"
                 return [], 'N'
        else:
             return [], 'N'

    def processPalmAcqData(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""
        printdata = False

        currenttime = datetime.utcnow()
        outdate = datetime.strftime(currenttime, "%Y-%m-%d")
        filename = outdate
        outtime = datetime.strftime(currenttime, "%H:%M:%S")


        # IMPORTANT : GET TIMESTAMP FROM DATA !!!!!!
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        datearray = timeToArray(timestamp)
        packcode = '6hL'

        # Would probably be good to preserve the hexadecimal format
        # Seems to be extremely effective regarding accuracy and storage
        x = data[0]
        y = data[1]
        z = data[2]
        v = 0.0
        t = 0.0
        p = 0.0
        q = 0.0
        r = 0.0
        if len(data) > 4:
            v = data[3]
            t = data[4]
            p = data[5]
            q = data[6]
            r = data[7]

        datearray.append(x)
        datearray.append(y)
        datearray.append(z)
        datearray.append(int(float(v)*10000))
        datearray.append(int(float(t)*10000))
        datearray.append(p)
        datearray.append(q)
        datearray.append(r)
        packcode = packcode + 'fffllfff'
        multiplier = [1,1,1,10000,10000,1,1,1]

        try:
            data_bin = struct.pack(packcode,*datearray)
        except:
            log.msg('Error while packing binary data')
            pass

        header = "# MagPyBin %s %s %s %s %s %s %d" % (self.sensorid, "[x,y,z,v,t,p,q,r]", "[x,y,z,v,t,p,q,r]", "[V,V,V,V,C,V,V,V]", str(multiplier).replace(" ",""), packcode, struct.calcsize(packcode))

        if printdata:
            #print header
            print(timestamp)

        # File Operations
        try:
            dataToFile(self.outputdir, self.sensorid, filename, data_bin, header)
        except:
            log.msg('Saving failed')
            pass

        evt0 = {'id': 0, 'value': self.hostname}
        evt1 = {'id': 1, 'value': timestamp}
        evt3 = {'id': 3, 'value': outtime}
        evt11 = {'id': 11, 'value': x}
        evt12 = {'id': 12, 'value': y}
        evt13 = {'id': 13, 'value': z}
        evt32 = {'id': 32, 'value': t}
        evt60 = {'id': 60, 'value': v}
        evt99 = {'id': 99, 'value': 'eol'}

        return evt0,evt1,evt3,evt11,evt12,evt13,evt32,evt60,evt99


    def lineReceived(self, line):

        data=[]

        if line:
            data, trigger = self.extractPalmAcqData(line)

        if len(data) > 1:
            evt0,evt1,evt3,evt11,evt12,evt13,evt32,evt60,evt99 = self.processPalmAcqData(data)

            dispatch_url =  "http://example.com/"+self.hostname+"/pal#"+self.sensorid+"-value"

            # eventlist defined in init
            for event in self.eventlist:
                self.wsMcuFactory.dispatch(dispatch_url, eval(event))
