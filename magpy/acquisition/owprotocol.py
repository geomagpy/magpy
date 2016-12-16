from __future__ import print_function
# OneWire part
try:
    import ow
    onewire = True
    owsensorlist = []
except:
    print("Onewire package not available")
    onewire = False

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

if onewire:
    class OwProtocol():
        """
        Protocol to read one wire data from usb DS unit
        All connected sensors are listed and data is distributed in dependency of sensor id
        Dipatch url links are defined by channel 'ow' and id+'value'
        Save path ? folders ?

        """
        def __init__(self, wsMcuFactory, source, outputdir):
            self.wsMcuFactory = wsMcuFactory
            #self.sensor = 'ow'
            self.source = source
            ow.init(source)
            self.root = ow.Sensor('/').sensorList()
            self.hostname = socket.gethostname()
            # TODO: create outputdir if not existing
            self.outputdir = outputdir
            self.reconnectcount = 0
            #self.plist = ["A6B154010000"]
            #self.hlist = ["DACF54010000"]

        def saveowlist(self,filename, owlist):
            with open(filename, 'wb') as f:
                wr = csv.writer(f, quoting=csv.QUOTE_ALL)
                for row in owlist:
                    wr.writerow(row)

        def loadowlist(self,filename):
            with open(filename, 'rb') as f:
                reader = csv.reader(f)
                owlist = [row for row in reader]
            return owlist

        def owConnected(self):
            global owsensorlist
            try:
                self.root = ow.Sensor('/').sensorList()

                if not (self.root == owsensorlist):
                    log.msg('Rereading sensor list')
                    ow.init(self.source)
                    self.root = ow.Sensor('/').sensorList()
                    owsensorlist = self.root
                    self.connectionMade(self.root)
                self.reconnectcount = 0
            except:
                self.reconnectcount = self.reconnectcount + 1
                log.msg('Reconnection event triggered - Number: %d' % self.reconnectcount)
                time.sleep(2)
                if self.reconnectcount < 10:
                    self.owConnected()
                else:
                    print("owConnect: reconnection not possible")

            self.oneWireInstruments(self.root)


        def connectionMade(self,root):
            # A loading eventually existing sensor list
            print("Connection made")
            import os
            martasdir = os.getcwd()
            print ("MARTAS directory", martasdir)

            owsensorfile = os.path.join(martasdir,'owlist.csv')
            owlist = []
            idlist = []
            try:
                owlist = self.loadowlist(owsensorfile)
            except:
                log.msg('One Wire: Error when getting sensor list')
                pass
            self.plist = [elem[0] for elem in owlist if elem[2] == 'pressure']
            self.hlist = [elem[0] for elem in owlist if elem[2] == 'humidity']
            self.clist = [elem[0] for elem in owlist if elem[2] == 'current']
            self.vlist = [elem[0] for elem in owlist if elem[2] == 'voltage']
            if len(owlist) > 0:
                idlist = [el[0] for el in owlist]
            log.msg('One Wire module initialized - found the following sensors:')
            for sensor in root:
                log.msg('Type: %s, ID: %s' % (sensor.type, sensor.id))
                # Use this list to initialize the sensor database including datalogger id and type
                try:
                    # writing this list to the MARTAS directory
                    # do not replace existing inputs, as there are additional columns to provide user information
                    # user info: e.g. for the DS2438 - voltage or pressure
                    # this list tried to be opened on init
                    if not sensor.id in idlist:
                        owrow = [str(sensor.id),str(sensor.type),'typus','location','info']
                        log.msg('One Wire: added new sensor to owlist: %s' % sensor.id)
                        owlist.append(owrow)
                except:
                    log.msg('One Wire: Error when asigning new sensor list')
                    pass
            try:
                self.saveowlist(owsensorfile,owlist)
            except:
                log.msg('One Wire: Error when writing sensor list')
                pass


        def oneWireInstruments(self,root):
            try:
                for sensor in root:
                    if sensor.type == 'DS18B20':
                        #sensor.useCache( False ) # Important for below 15 sec resolution (by default a 15 sec cache is used))
                        self.readTemperature(sensor)
                    #if sensor.type == 'DS2406':
                    #    self.readSHT(sensor)
                    elif sensor.type == 'DS2438':
                        #sensor.useCache( False ) # Important for below 15 sec resolution (by default a 15 sec cache is used))
                        # test for sensorids and provide sensortypus to function (e.g. humidity, pressure, none, etc)
                        if sensor.id in self.plist:
                            sensortypus = "pressure"
                        else:
                            sensortypus = "voltage"
                        self.readBattery(sensor, sensortypus)
            except:
                global owsensorlist
                owsensorlist = []
                self.owConnected()

        def alias(self, sensorid):
            #define a alias dictionary
            sensordict = {"332988040000": "Mobil"}
            try:
                return sensordict[sensorid]
            except:
                return sensorid

        def mpxa4100(self,vad,temp):
            # Calculates pressure for the MPXA4100A6U
            # calibration values (take them from an ini file for the requested sensor.id)
            va = 4.7 # Volts taken from data sheet
            pa = 105 # kPa taken from data sheet
            vb = 0.6 # Volts taken from data sheet
            pb = 20 # kPa taken from data sheet
            # Linear transfer function according to datasheet (MPXA4100A6U)
            mp = (va-vb)/(pa-pb)
            tp = va-((va-vb)/(pa-pb)*pa)
            ph = (vad-tp)/mp*10.0 # Pressure at current altitude in hPa
            #http://de.wikipedia.org/wiki/Barometrische_H%C3%B6henformel
            g = 9.80665
            R = 287.05
            h = 600
            Ch = 0.12
            a = 0.0065
            T = temp + 273.15
            #print "Check the following, maybe numpy not active"
            #if temp < 9.1:
            #    E = 5.6402*(-0.0916 + 1*np.exp(0.06*temp))
            #else:
            #    E = 18.2194*(1.0463 - 1*np.exp(-0.0666*temp))
            #print "E", E
            #x = (g/(R*(T + Ch*E + a*(h/2))))*h
            #pm = ph*np.exp(x)
            #print "Pressure [hPA] sealevel ", pm

            return (vad-tp)/mp*10.0

        def timeToArray(self, timestring):
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
                log.msg('OW - timetoArray: Error while extracting time array')
                return []

        def dataToFile(self, sensorid, filedate, bindata, header):
            # File Operations
            try:
                path = os.path.join(self.outputdir,self.hostname,sensorid)
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
                log.err("OW - datatofile: Error while saving file")

        def readTemperature(self, sensor):

            #t = threading.Timer(1.0, self.readTemperature, [sensor])
            #t.deamon = True
            #t.start()
            dispatch_url =  "http://example.com/"+self.hostname+"/ow#"+sensor.id+"-value"
            currenttime = datetime.utcnow()
            filename = datetime.strftime(currenttime, "%Y-%m-%d")
            actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
            timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
            outtime = datetime.strftime(currenttime, "%H:%M:%S")
            packcode = '6hLl'
            header = "# MagPyBin %s %s %s %s %s %s %d" % (sensor.id, '[t1]', '[T]', '[degC]', '[1000]', packcode, struct.calcsize(packcode))

            try:
                # Extract data
                temp = float(sensor.temperature)
                #print "read Temperature: ", sensor.id, temp

                # extract time data
                datearray = self.timeToArray(timestamp)
                try:
                    datearray.append(int(temp*1000))
                    data_bin = struct.pack(packcode,*datearray)
                except:
                    log.msg('OW - readTemperature: Error while packing binary data')
                    pass

                # File Operations
                self.dataToFile(sensor.id, filename, data_bin, header)

                # Provide data to websocket
                try:
                    # Check STANDARD_ID list for correct numbers
                    evt1 = {'id': 3, 'value': outtime}
                    evt6 = {'id': 1, 'value': timestamp}
                    evt2 = {'id': 30, 'value': temp}
                    evt5 = {'id': 0, 'value': self.hostname}
                    evt8 = {'id': 99, 'value': 'eol'}
                except:
                    print("OW - readTemperature: Problem assigning values to dict")

                try:
                    self.wsMcuFactory.dispatch(dispatch_url, evt1)
                    self.wsMcuFactory.dispatch(dispatch_url, evt6)
                    self.wsMcuFactory.dispatch(dispatch_url, evt2)
                    self.wsMcuFactory.dispatch(dispatch_url, evt5)
                    self.wsMcuFactory.dispatch(dispatch_url, evt8)
                    pass
                except ValueError:
                    log.err('Unable to parse data at %s' % actualtime)
            except:
                log.err('OW - readTemperature: Lost temperature sensor -- reconnecting')
                global owsensorlist
                owsensorlist = []
                self.owConnected()


        def readBattery(self,sensor,sensortypus):
            dispatch_url =  "http://example.com/"+self.hostname+"/ow#"+sensor.id+"-value"
            currenttime = datetime.utcnow()
            filename = datetime.strftime(currenttime, "%Y-%m-%d")
            actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
            timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
            outtime = datetime.strftime(currenttime, "%H:%M:%S")
            packcode = '6hLlLLLf'
            header = "# MagPyBin %s %s %s %s %s %s %d" % (sensor.id, '[t1,var1,var2,var3,var4]', '[T,rh,vdd,vad,vis]', '[deg_C,per,V,V,V]', '[1000,100,100,100,1]', packcode, struct.calcsize(packcode))

            try:
                # Extract data
                #print "Sensor: ", sensor.id, sensortypus
                try:
                    humidity = float(ow.owfs_get('/uncached%s/HIH4000/humidity' % sensor._path))
                except:
                    humidity = float(nan)
                try:
                    #print "Battery sens: T = ", sensor.temperature
                    temp = float(sensor.temperature)
                    #print "Battery sens: VDD = ", sensor.VDD
                    vdd = float(sensor.VDD)
                    #print "Battery sens: VAD = ", sensor.VAD
                    vad = float(sensor.VAD)
                    #print "Battery sens: vis = ", sensor.vis
                    vis = float(sensor.vis)
                    if sensortypus == "pressure":
                        #print "Pressure [hPa]: ", self.mpxa4100(vad,temp)
                        humidity = self.mpxa4100(vad,temp)
                except:
                    log.err("OW - readBattery: Could not asign value")

                # Appending data to buffer which contains pcdate, pctime and sensordata
                # extract time data
                datearray = self.timeToArray(timestamp)

                try:
                    datearray.append(int(temp*1000))
                    if humidity < 0:
                        humidity = 9999
                    datearray.append(int(humidity*100))
                    datearray.append(int(vdd*100))
                    datearray.append(int(vad*100))
                    datearray.append(vis)
                    data_bin = struct.pack(packcode,*datearray)
                except:
                    log.msg('OW - readBattery: Error while packing binary data')
                    pass

                # File Operations
                self.dataToFile(sensor.id, filename, data_bin, header)

                try:
                    evt1 = {'id': 3, 'value': outtime}
                    evt9 = {'id': 1, 'value': timestamp}
                    evt2 = {'id': 30, 'value': temp}
                    if humidity < 100:
                        evt3 = {'id': 33, 'value': humidity}
                    else:
                        evt3 = {'id': 33, 'value': 0}
                    evt5 = {'id': 60, 'value': vdd}
                    evt6 = {'id': 61, 'value': vad}
                    evt7 = {'id': 62, 'value': vis}
                    evt8 = {'id': 99, 'value': 'eol'}
                except:
                    print("OW - readBattery: Problem assigning values to dict")

                try:
                    self.wsMcuFactory.dispatch(dispatch_url, evt1)
                    self.wsMcuFactory.dispatch(dispatch_url, evt9)
                    self.wsMcuFactory.dispatch(dispatch_url, evt2)
                    self.wsMcuFactory.dispatch(dispatch_url, evt3)
                    self.wsMcuFactory.dispatch(dispatch_url, evt5)
                    self.wsMcuFactory.dispatch(dispatch_url, evt6)
                    self.wsMcuFactory.dispatch(dispatch_url, evt7)
                    self.wsMcuFactory.dispatch(dispatch_url, evt8)
                    pass
                except:
                    log.err('OW - readBattery: Unable to parse data at %s' % actualtime)
            except:
                log.err('OW - readBattery: Lost battery sensor -- reconnecting')
                global owsensorlist
                owsensorlist = []
                self.owConnected()
