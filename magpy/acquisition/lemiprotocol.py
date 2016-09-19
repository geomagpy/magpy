'''
Filename:               lemiprotocol
Part of package:        acquisition
Type:                   Part of data acquisition library

PURPOSE:
        This package will initiate LEMI025 / LEMI036 data acquisition and streaming
        and saving of data.

CONTAINS:
        *LemiProtocol:  (Class - twisted.protocols.basic.LineReceiver)
                        Class for handling data acquisition of LEMI variometers.
                        Includes internal class functions: processLemiData
        _timeToArray:   (Func) ... utility function for LemiProtocol.
        h2d:            (Func) ... utility function for LemiProtocol.
                        Convert hexadecimal to decimal.

IMPORTANT:
        - According to data sheet: 300 millseconds are subtracted from each gps time step
        provided GPStime = GPStime_sent - timedelta(microseconds=300000)
        - upcoming year 3000 bug
DEPENDENCIES:
        twisted, autobahn

CALLED BY:
        magpy.bin.acquisition
'''
from __future__ import print_function

import sys, time, os, socket
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


## Lemi protocol
## -------------

class LemiProtocol(LineReceiver):

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory, sensor, soltag, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.buffer = ''
        self.soltag = soltag    # Start-of-line-tag
        self.hostname = socket.gethostname()
        self.outputdir = outputdir
        self.gpsstate1 = 'A'
        self.gpsstate2 = 'P'
        self.gpsstatelst = []
        flag = 0

    @exportRpc("control-led")
    def controlLed(self, status):
        if status:
            print("turn on LED")
            self.transport.write('1')
        else:
            print("turn off LED")
            self.transport.write('0')


    @exportRpc("send-command")
    def sendCommand(self, command):
        if not command == "":
            print(command)
            #self.transport.write(command)

    def connectionMade(self):
        log.msg('%s connected.' % self.sensor)

    def connectionLost(self):
        log.msg('LEMI connection lost. Perform steps to restart it!')

    def h2d(self,x):
        '''
        Hexadecimal to decimal (for format LEMIBIN2)
        ... Because the binary for dates is in binary-decimal, not just binary.
        '''

        y = int(x/16)*10 + x%16
        return y

    def _timeToArray(self,timestring):
        '''
        Converts time string of format 2013-12-12T23:12:23.122324
        to an array similiar to a datetime object
        '''

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


    def processLemiData(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""
        if len(data) != 153:
            log.err('LEMI - Protocol: Unable to parse data of length %i' % len(data))

        """ TIMESHIFT between serial output (and thus NTP time) and GPS timestamp """
        timedelay = 0.0   ## in sec, most likely in order of 0.1 sec

        currenttime = datetime.utcnow()
        date = datetime.strftime(currenttime, "%Y-%m-%d")
        #actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")

        datearray = self._timeToArray(timestamp)
        date_bin = struct.pack('6hL',datearray[0]-2000,datearray[1],datearray[2],datearray[3],datearray[4],datearray[5],datearray[6])

        # define pathname for local file storage (default dir plus hostname plus sensor plus year) and create if not existing
        path = os.path.join(self.outputdir,self.hostname,self.sensor)
        if not os.path.exists(path):
            os.makedirs(path)

        packcode = "<4cb6B8hb30f3BcBcc5hL"
        header = "LemiBin %s %s %s %s %s %s %d\n" % (self.sensor, '[x,y,z,t1,t2]', '[X,Y,Z,T_sensor,T_elec]', '[nT,nT,nT,deg_C,deg_C]', '[0.001,0.001,0.001,100,100]', packcode, struct.calcsize(packcode))

        # save binary raw data to file
        lemipath = os.path.join(path,self.sensor+'_'+date+".bin")
        if not os.path.exists(lemipath):
            with open(lemipath, "ab") as myfile:
                myfile.write(header)
        try:
            with open(lemipath, "ab") as myfile:
                myfile.write(data+date_bin)
            pass
        except:
            log.err('LEMI - Protocol: Could not write data to file.')

        # unpack data and extract time and first field values
        try:
            data_array = struct.unpack("<4cB6B8hb30f3BcB", data)
        except:
            log.err("LEMI - Protocol: Bit error while reading.")

        try:
            newtime = []
            #for i in range (5,11):
            #    newtime.append(self.correct_bin_time(data_array[i]))
            #time = datetime(2000+newtime[0],newtime[1],newtime[2],newtime[3],newtime[4],int(newtime[5]),int(newtime[6]*1000000))
            biasx = float(data_array[16])/400.
            biasy = float(data_array[17])/400.
            biasz = float(data_array[18])/400.
            x = (data_array[20])*1000.
            xarray = [elem * 1000. for elem in data_array[20:50:3]]
            y = (data_array[21])*1000.
            yarray = [elem * 1000. for elem in data_array[21:50:3]]
            z = (data_array[22])*1000.
            zarray = [elem * 1000. for elem in data_array[22:50:3]]
            temp_sensor = data_array[11]/100.
            temp_el = data_array[12]/100.
            vdd = float(data_array[52])/10.
            gpsstat = data_array[53]
            gps_array = datetime(2000+self.h2d(data_array[5]),self.h2d(data_array[6]),self.h2d(data_array[7]),self.h2d(data_array[8]),self.h2d(data_array[9]),self.h2d(data_array[10]))-timedelta(microseconds=300000)
            gps_time = datetime.strftime(gps_array, "%Y-%m-%d %H:%M:%S")
        except:
            log.err("LEMI - Protocol: Number conversion error.")

        # get the most frequent gpsstate of the last 10 secs
        # this avoids error messages for singular one sec state changes
        self.gpsstatelst.append(gpsstat)
        self.gpsstatelst = self.gpsstatelst[-10:]
        self.gpsstate1 = max(set(self.gpsstatelst),key=self.gpsstatelst.count)
        if not self.gpsstate1 == self.gpsstate2:
            log.msg('LEMI - Protocol: GPSSTATE changed to %s .'  % gpsstat)
        self.gpsstate2 = self.gpsstate1

        #print "GPSSTAT", gpsstat
        # important !!! change outtime to lemi reading when GPS is running
        try:
            if self.gpsstate2 == 'P':
                ## passive mode - no GPS connection -> use ntptime as primary with correction
                evt1 = currenttime-timedelta(seconds=timedelay)
                evt4 = gps_array
            else:
                ## active mode - GPS time is used as primary
                evt4 = currenttime-timedelta(seconds=timedelay)
                evt1 = gps_array
            evt3 = {'id': 3, 'value': outtime}
            #evt1 = {'id': 1, 'value': timestamp}
            #evt4 = {'id': 4, 'value': gps_time}
            evt11 = xarray
            evt12 = yarray
            evt13 = zarray
            #evt11 = {'id': 11, 'value': x}
            #evt12 = {'id': 12, 'value': y}
            #evt13 = {'id': 13, 'value': z}
            evt31 = {'id': 31, 'value': temp_sensor}
            evt32 = {'id': 32, 'value': temp_el}
            evt60 = {'id': 60, 'value': vdd}
            evt99 = {'id': 99, 'value': 'eol'}
        except:
            log.err('LEMI - Protocol: Error assigning "evt" values.')

        return evt1,evt3,evt4,evt11,evt12,evt13,evt31,evt32,evt60,evt99

    def dataReceived(self, data):
        #print "Lemi data here!", self.buffer
        dispatch_url =  "http://example.com/"+self.hostname+"/lemi#"+self.sensor+"-value"
        flag = 0
        WSflag = 0
        debug = False

        """
        # Test range
        self.buffer = self.buffer + data
        if not (self.buffer).startswith(self.soltag):
            lemisearch = (self.buffer).find(self.soltag, 6)
            if not lemisearch == -1:
                print "Lemiserach", lemisearch, self.buffer
                self.buffer = self.buffer[lemisearch:len(self.buffer)]
        if len(self.buffer) == 153:
            # Process data
            print self.buffer
            self.buffer = ''
        """

        try:
            if (self.buffer).startswith(self.soltag) and len(self.buffer) == 153:
                currdata = self.buffer
                self.buffer = ''
                evt1,evt3,evt4,evt11,evt12,evt13,evt31,evt32,evt60,evt99 = self.processLemiData(currdata)
                WSflag = 2

            ### Note: this code for fixing data is more complex than the POS fix code
            ### due to the LEMI device having a start code rather than an EOL code.
            ### It can handle and deal with multiple errors:
            ###  - multiple data parts arriving at once
            ###  - databits being lost. Bad string is then deleted.
            ###  - bad bits infiltrating the data. Bad string is deleted.

            if len(self.buffer) > 153:
                if debug:
                    log.msg('LEMI - Protocol: Warning: Bufferlength (%s) exceeds 153 characters, fixing...' % len(self.buffer))
                lemisearch = (self.buffer).find(self.soltag)
                #print '1', lemisearch
                if (self.buffer).startswith(self.soltag):
                    datatest = len(self.buffer)%153
                    dataparts = int(len(self.buffer)/153)
                    if datatest == 0:
                        if debug:
                            log.msg('LEMI - Protocol: It appears multiple parts came in at once, # of parts:', dataparts)
                        for i in range(dataparts):
                            split_data_string = self.buffer[0:153]
                            if (split_data_string).startswith(self.soltag):
                                if debug:
                                    log.msg('LEMI - Protocol: Processing data part # %s in string...' % (str(i+1)))
                                evt1,evt3,evt4,evt11,evt12,evt13,evt31,evt32,evt60,evt99 = self.processLemiData(split_data_string)
                                WSflag = 2
                                self.buffer = self.buffer[153:len(self.buffer)]
                            else:
                                flag = 1
                                break
                    else:
                        for i in range(dataparts):
                            lemisearch = (self.buffer).find(self.soltag, 6)
                            if lemisearch >= 153:
                                split_data_string = self.buffer[0:153]
                                evt1,evt3,evt4,evt11,evt12,evt13,evt31,evt32,evt60,evt99 = self.processLemiData(split_data_string)
                                WSflag = 2
                                self.buffer = self.buffer[153:len(self.buffer)]
                            elif lemisearch == -1:
                                log.msg('LEMI - Protocol: No header found. Deleting buffer.')
                                self.buffer = ''
                            else:
                                log.msg('LEMI - Protocol: String contains bad data (%s bits). Deleting.' % len(self.buffer[:lemisearch]))
                                self.buffer = self.buffer[lemisearch:len(self.buffer)]
                                flag = 1
                                break

                else:
                    log.msg('LEMI - Protocol: Incorrect header. Attempting to fix buffer... Bufferlength:', len(self.buffer))
                    lemisearch = (self.buffer).find(self.soltag, 6)
                    #lemisearch = repr(self.buffer).find(self.soltag)
                    if lemisearch == -1:
                        log.msg('LEMI - Protocol: No header found. Deleting buffer.')
                        self.buffer = ''
                    else:
                        log.msg('LEMI - Protocol: Bad data (%s bits) deleted. New bufferlength: %s' % (lemisearch,len(self.buffer)))
                        self.buffer = self.buffer[lemisearch:len(self.buffer)]
                        flag = 1

            if flag == 0:
                self.buffer = self.buffer + data

        except:
            log.err('LEMI - Protocol: Error while parsing data.')

        ## publish event to all clients subscribed to topic
        if WSflag == 2:
            for ind, elem in enumerate(evt11):
                t1 = evt1+timedelta(seconds=0.1*ind)
                t2 = evt4+timedelta(seconds=0.1*ind)
                #print t1, t2
                evt1a = {'id': 1, 'value': datetime.strftime(t1,"%Y-%m-%d %H:%M:%S.%f")}
                #evt3a = {'id': 1, 'value': datetime.strftime(t1,"%H:%M:%S.%f")}
                evt4a = {'id': 4, 'value': datetime.strftime(t2,"%Y-%m-%d %H:%M:%S.%f")}
                evt11a = {'id': 11, 'value': evt11[ind]}
                evt12a = {'id': 12, 'value': evt12[ind]}
                evt13a = {'id': 13, 'value': evt13[ind]}
                try:
                    self.wsMcuFactory.dispatch(dispatch_url, evt1a)
                    #self.wsMcuFactory.dispatch(dispatch_url, evt3a)
                    self.wsMcuFactory.dispatch(dispatch_url, evt4a)
                    self.wsMcuFactory.dispatch(dispatch_url, evt11a)
                    self.wsMcuFactory.dispatch(dispatch_url, evt12a)
                    self.wsMcuFactory.dispatch(dispatch_url, evt13a)
                    self.wsMcuFactory.dispatch(dispatch_url, evt31)
                    self.wsMcuFactory.dispatch(dispatch_url, evt32)
                    self.wsMcuFactory.dispatch(dispatch_url, evt60)
                    self.wsMcuFactory.dispatch(dispatch_url, evt99)
                except:
                    log.err('LEMI - Protocol: wsMcuFactory error while dispatching data.')
