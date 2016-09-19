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
        log.err("GSMP20 - Protocol: Error while saving file")


## GEM -GSMP-20S3 protocol -- North-South
##
class GSMP20NSProtocol(LineReceiver):
    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.outputdir = outputdir
        self.hostname = socket.gethostname()
        self.dateprev = datetime.strftime(datetime.utcnow(), "%Y-%m-%d")

    def connectionLost(self):
        log.msg('GSMP20 connection lost. Perform steps to restart it!')

    def connectionMade(self):
        log.msg('%s connected.' % self.sensor)

    def processData(self, data):
        """
        Data looks like--- (with GPS lines every minute):
        -- vertical sensor - Old software
        3,3,12.00 111 field1 field2 field3
        3,3,12.00 111 field1 field2 field3
        GPS 16.00 111 field1 field2 field3
        -- horizontal sensor - New software
        time 111 field1 field2 field3                                            (every sec or faster)
        $$$                                                         (every hour, preceeds status line)
        10071506 A 13 250 492 496 329 150 1023 39 39 39 30 29 30 YYYyyyEEENNN 148 149 117 (every hour)
        time 111 field1 field2 field3                                            (every sec or faster)
        time 111 field1 field2 field3                                            (every sec or faster)

        Header:
                        10071506 A 13 250 492 496 329 150 1023 39 39 39 30 29 30 YYYyyyEEENNN 148 149 117

                        <GPS> day/month/year/hour A - locked, V unlocked
                        <13> Console outside air temperature (13C)
                        <250> Battery voltage (25.0V)
                        <492> +5V supply voltage (4.92V)
                        <496> -5V supply voltage (-4.96)
                        <3.3V> +3.3V supply voltage (3.3V)
                        <15.0> silver box power supply (15.0V)
                        <1023> OCXO internal trimpot adjustment level, automatically adjusted via GPS
                        <39> Sensor 1 temperature in C
                        <39>  Sensor 2 temperature in C
                        <39> Sensor 3 temperature in C
                        <30> Light current sensor 1 (3.0uA)
                        <29> Light current sensor 2 (2.9uA)
                        <30> Light current sensor 3 (3.0uA)
                        <YYY>  Sensor 1, sensor 2 sensor 3 lock status Y- locked, N - unlocked
                        <yyy>  Sensor 1 heater status, sensor 2 heater status, sensor 3 heater status y-on, n-off
                        <EEE> Sensor 1 heater, sensor 2 heater, sensor 3 heater E-enabled, D-disabled (used for over heat protection)
                        <NNN> RF sensor 1, RF sensor 2, RF sensor 3, N -on, F-off
                        <148> Sensor 1 RF dc voltage (14.8V)
                        <149> Sensor 2 RF dc voltage (14.9V)
                        <117> Sensor 3 RF dc voltage (11.7V)

        """

        currenttime = datetime.utcnow()
        date = datetime.strftime(currenttime, "%Y-%m-%d")
        cdate = date
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        packcode = '6hLqqqqqq'
        header = "# MagPyBin %s %s %s %s %s %s %d" % (self.sensor, '[x,y,z,dx,dy,dz]', '[NS,N,S,SNS,SN,NNS]', '[pT,pT,pT,pT,pT,pT]', '[1000,1000,1000,1000,1000,1000]', packcode, struct.calcsize(packcode))
        headerlinecoming = False
        datacoming = False

        try:
            # Extract data
            data_array = data.strip().split()
        except:
            log.err('GSMP20 - Protocol: Data formatting error.')

        try:
            if len(data_array) == 5:
                try:
                    gpstime = float(data_array[0])
                    if gpstime > 235900.0: # use date of last day if gpstime > 235900 to prevent next day date for 235959 gps when pctime already is on next day
                        cdate = self.dateprev
                    else:
                        cdate = date
                        self.dateprev = date
                    gpsdate = cdate+'T'+data_array[0]
                    gpstimestamp = datetime.strftime(datetime.strptime(gpsdate, "%Y-%m-%dT%H%M%S.%f"), "%Y-%m-%d %H:%M:%S.%f")
                except:
                    gpstimestamp = timestamp

                #print "Timestamp", gpstime, gpstimestamp, timestamp, cdate, data_array[0]
                timestamp = gpstimestamp
                intensity1 = float(data_array[2])
                intensity2 = float(data_array[3])
                intensity3 = float(data_array[4])
                grad1 = intensity3-intensity1
                grad2 = intensity3-intensity2
                grad3 = intensity2-intensity1
                datacoming =True
            elif len(data_array) == 19:
                headerlinecoming = True

                try:
                    gpstime = str(data_array[0])
                    gpstimestamp = datetime.strftime(datetime.strptime(gpstime, "%d%m%y%H"), "%Y-%m-%d %H:%M:%S.%f")
                    #print "Header", gpstimestamp
                except:
                    gpstimestamp = timestamp
                headtimestamp = gpstimestamp
                gpstatus = data_array[1]                        # str1
                telec = int(data_array[2])                      # t2
                Vbat = float(data_array[3])/10.                 # f
                Vsup1 = float(data_array[4])/100.               # var4
                Vsup2 = float(data_array[5])/100.               # var5
                Vlow = float(data_array[6])/100.                # t1
                PowerSup = float(data_array[7])/10.             # df
                level = data_array[8]                           # str3
                tsens1 = int(data_array[9])                     # x
                tsens2 = int(data_array[10])                    # y
                tsens3 = int(data_array[11])                    # z
                lightcurrent1 = float(data_array[12])/10.       # dx
                lightcurrent2 = float(data_array[13])/10.       # dy
                lightcurrent3 = float(data_array[14])/10.       # dz
                statusstring = data_array[15]                   # str2
                Vsens1 = float(data_array[16])/10.              # var1
                Vsens2 = float(data_array[17])/10.              # var2
                Vsens3 = float(data_array[18])/10.              # var3
            else:
                #print "Found other data:", data, len(data_array)
                pass
        except:
            log.err('GSMP20 - Protocol: Data extraction error.')

        if datacoming:
            try:
                # extract time data
                datearray = timeToArray(timestamp)
                #print datearray
                try:
                    datearray.append(int(intensity1*1000.))
                    datearray.append(int(intensity2*1000.))
                    datearray.append(int(intensity3*1000.))
                    datearray.append(int(grad1*1000.))
                    datearray.append(int(grad2*1000.))
                    datearray.append(int(grad3*1000.))
                    data_bin = struct.pack(packcode,*datearray)
                    dataToFile(self.outputdir,self.sensor, cdate, data_bin, header)
                except:
                    log.msg('GSMP20 - Protocol: Error while packing binary data')
                    print(data)
                    pass
            except:
                log.msg('GSMP20 - Protocol: Error with binary save routine')
                pass

            evt1 = {'id': 1, 'value': timestamp}
            evt3 = {'id': 3, 'value': outtime}
            evt20 = {'id': 20, 'value': intensity1}
            evt21 = {'id': 21, 'value': intensity2}
            evt22 = {'id': 22, 'value': intensity3}
            evt23 = {'id': 23, 'value': grad1}
            evt24 = {'id': 24, 'value': grad2}
            evt25 = {'id': 25, 'value': grad3}
            evt99 = {'id': 99, 'value': 'eol'}

            return evt1,evt3,evt20,evt21,evt22,evt23,evt24,evt25,evt99

        elif headerlinecoming:
            headpackcode = '6hL15lsss'
            try:
                # extract time data
                headarray = timeToArray(headtimestamp)
                try:
                    headarray.append(int(tsens1))               # x
                    headarray.append(int(tsens2))               # y
                    headarray.append(int(tsens3))               # z
                    headarray.append(int(Vbat*10.))             # f
                    headarray.append(int(Vlow*100.))            # t1
                    headarray.append(int(telec))                # t2
                    headarray.append(int(lightcurrent1*10.))    # dx
                    headarray.append(int(lightcurrent2*10.))    # dy
                    headarray.append(int(lightcurrent3*10.))    # dz
                    headarray.append(int(PowerSup*10.))         # df
                    headarray.append(int(Vsens1*10.))           # var1
                    headarray.append(int(Vsens2*10.))           # var2
                    headarray.append(int(Vsens3*10.))           # var3
                    headarray.append(int(Vsup1*100.))           # var4
                    headarray.append(int(Vsup2*100.))           # var5
                    headarray.append(gpstatus)                  # str1
                    headarray.append(statusstring)              # str2
                    headarray.append(level)                     # str3

                    data_head = struct.pack(headpackcode,*headarray)
                    na = self.sensor
                    #statusname
                    statuslst = na.split('_')
                    if len(statuslst) == 3:
                        statusname = '_'.join([statuslst[0]+'Status',statuslst[1],statuslst[2]])
                    headheader = "# MagPyBin %s %s %s %s %s %s %d" % (statusname, '[x,y,z,f,t1,t2,dx,dy,dz,df,var1,var2,var3,var4,var5,str1,str2,str3]', '[Ts1,Ts2,Ts3,Vbat,V3,Tel,L1,L2,L3,Vps,V1,V2,V3,V5p,V5n,GPSstat,Status,OCXO]', '[degC,degC,degC,V,V,degC,A,A,A,V,V,V,V,V,V,None,None,None]', '[1,1,1,10,100,1,10,10,10,10,10,10,10,100,100,1,1,1]', headpackcode, struct.calcsize(headpackcode))
                    dataToFile(self.outputdir,statusname, date, data_head, headheader)
                except:
                    log.msg('GSMP20 - Protocol: Error while packing binary data')
                #evt32 = {'id': 32, 'value': telec}
                #evt30 = {'id': 30, 'value': tsen1}
                #evt31 = {'id': 31, 'value': tsen2}
                #evt36 = {'id': 36, 'value': tsen3}
                #evt60 = {'id': 60, 'value': Vbat}
                # Dispatch this data ?
            except:
                log.msg('GSMP20 - Protocol: Error with binary save routine')
                pass


    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+self.hostname+"/sug#"+self.sensor+"-value"
        try:
            #print line
            evt1,evt3,evt20,evt21,evt22,evt23,evt24,evt25,evt99 = self.processData(line)
            #except ValueError:
            #log.err('GSMP20 - Protocol: Unable to parse data %s' % line)
            ## publish event to all clients subscribed to topic
            ##
            self.wsMcuFactory.dispatch(dispatch_url, evt1)
            self.wsMcuFactory.dispatch(dispatch_url, evt3)
            self.wsMcuFactory.dispatch(dispatch_url, evt20)
            self.wsMcuFactory.dispatch(dispatch_url, evt21)
            self.wsMcuFactory.dispatch(dispatch_url, evt22)
            self.wsMcuFactory.dispatch(dispatch_url, evt23)
            self.wsMcuFactory.dispatch(dispatch_url, evt24)
            self.wsMcuFactory.dispatch(dispatch_url, evt25)
            self.wsMcuFactory.dispatch(dispatch_url, evt99)
        except:
            pass
