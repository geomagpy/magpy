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

def h2d(x):		# Hexadecimal to decimal (for format LEMIBIN2)
    y = int(x/16)*10 + x%16		# Because the binary for dates is in binary-decimal, not just binary.
    return y

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


## Lemi protocol
## -------------

class LemiProtocol(LineReceiver):

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory, sensor, soltag, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.buffer = ''
        self.soltag = soltag 	# Start-of-line-tag
        self.hostname = socket.gethostname()
        self.outputdir = outputdir
        flag = 0

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

    def connectionLost(self):
        log.msg('LEMI connection lost. Perform steps to restart it!')

    def processLemiData(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""
        if len(data) != 153:
            log.err('LEMI - Protocol: Unable to parse data of length %i' % len(data))

        currenttime = datetime.utcnow()
        date = datetime.strftime(currenttime, "%Y-%m-%d")
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")

        datearray = timeToArray(timestamp)
        date_bin = struct.pack('6hL',datearray[0]-2000,datearray[1],datearray[2],datearray[3],datearray[4],datearray[5],datearray[6])

        # define pathname for local file storage (default dir plus hostname plus sensor plus year) and create if not existing
        #hostname = socket.gethostname()
        path = os.path.join(self.outputdir,self.hostname,self.sensor)
        if not os.path.exists(path):
            os.makedirs(path)

        # save binary raw data to file
        try:
            with open(os.path.join(path,self.sensor+'_'+date+".bin"), "ab") as myfile:
                myfile.write(data+date_bin)
            #with open(os.path.join(path,self.sensor+'_'+date+".txt"), "a") as myfile:
                #myfile.write(actualtime+'\n')
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
            y = (data_array[21])*1000.
            z = (data_array[22])*1000.
            temp_sensor = data_array[11]/100.
            temp_el = data_array[12]/100.
            gps_array = datetime(2000+h2d(data_array[5]),h2d(data_array[6]),h2d(data_array[7]),h2d(data_array[8]),h2d(data_array[9]),h2d(data_array[10]))
            gps_time = datetime.strftime(gps_array, "%Y-%m-%d %H:%M:%S")
        except:
            log.err("LEMI - Protocol: Number conversion error.")

        # important !!! change outtime to lemi reading when GPS is running 
        try:
            evt1 = {'id': 1, 'value': timestamp}
            evt3 = {'id': 3, 'value': outtime}
            evt4 = {'id': 4, 'value': gps_time}
            evt11 = {'id': 11, 'value': x}
            evt12 = {'id': 12, 'value': y}
            evt13 = {'id': 13, 'value': z}
            evt31 = {'id': 31, 'value': temp_sensor}
            evt32 = {'id': 32, 'value': temp_el}
            evt99 = {'id': 99, 'value': 'eol'}
        except:
            log.err('LEMI - Protocol: Error assigning "evt" values.')
 
        return evt1,evt3,evt4,evt11,evt12,evt13,evt31,evt32,evt99
         
    def dataReceived(self, data):
        #print "Lemi data here!", self.buffer
        dispatch_url =  "http://example.com/"+self.hostname+"/lemi#"+self.sensor+"-value"
        flag = 0
        WSflag = 0

        try:
            if (self.buffer).startswith(self.soltag) and len(self.buffer) == 153:
                currdata = self.buffer
                self.buffer = ''
                evt1,evt3,evt4,evt11,evt12,evt13,evt31,evt32,evt99 = self.processLemiData(currdata)
                WSflag = 2

            ### Note: this code for fixing data is more complex than the POS fix code
            ### due to the LEMI device having a start code rather than an EOL code.
            ### It can handle and deal with multiple errors:
            ###  - multiple data parts arriving at once
            ###  - databits being lost. Bad string is then deleted.
            ###  - bad bits infiltrating the data. Bad string is deleted.

            if len(self.buffer) > 153:
                log.msg('LEMI - Protocol: Warning: Bufferlength (%s) exceeds 153 characters, fixing...' % len(self.buffer))
                lemisearch = (self.buffer).find(self.soltag)
                print '1', lemisearch
                if (self.buffer).startswith(self.soltag):
                    datatest = len(self.buffer)%153
                    dataparts = int(len(self.buffer)/153)
                    if datatest == 0:
                        log.msg('LEMI - Protocol: It appears multiple parts came in at once, # of parts:', dataparts)
                        for i in range(dataparts):
                            split_data_string = self.buffer[0:153]
                            if (split_data_string).startswith(self.soltag):
                                log.msg('LEMI - Protocol: Processing data part # %s in string...' % (str(i+1)))
                                evt1,evt3,evt4,evt11,evt12,evt13,evt31,evt32,evt99 = self.processLemiData(split_data_string)
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
                                evt1,evt3,evt4,evt11,evt12,evt13,evt31,evt32,evt99 = self.processLemiData(split_data_string)
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
                    lemisearch = repr(self.buffer).find(self.soltag)
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
            try:
                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                self.wsMcuFactory.dispatch(dispatch_url, evt3)
                self.wsMcuFactory.dispatch(dispatch_url, evt4)
                self.wsMcuFactory.dispatch(dispatch_url, evt11)
                self.wsMcuFactory.dispatch(dispatch_url, evt12)
                self.wsMcuFactory.dispatch(dispatch_url, evt13)
                self.wsMcuFactory.dispatch(dispatch_url, evt31)
                self.wsMcuFactory.dispatch(dispatch_url, evt32)
                self.wsMcuFactory.dispatch(dispatch_url, evt99)
            except:
                log.err('LEMI - Protocol: wsMcuFactory error while dispatching data.')

