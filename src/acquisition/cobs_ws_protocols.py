
# OneWire part

try:
    import ow
    onewire = True
    owsensorlist = []
except:
    print "Onewire package not available"
    onewire = False  

#import threading
import sys, time, os, socket

from twisted.protocols.basic import LineReceiver
from autobahn.wamp import exportRpc

from twisted.internet import reactor

from twisted.python import usage, log
from twisted.internet.serialport import SerialPort
from twisted.web.server import Site
from twisted.web.static import File

from autobahn.websocket import listenWS
from autobahn.wamp import WampServerFactory, WampServerProtocol, exportRpc


from datetime import datetime, timedelta
import struct, binascii
import re

hostname = socket.gethostname()
outputdir = '/srv/ws' # is defined in main file - should be overwritten then


lastActualtime = datetime.utcnow() # required for cs output



"""
The ID list for subscriptions:
0: time         -- str(12:10:32)
1: x            -- float
2: y            -- float
3: z            -- float
4: f            -- float
5: temp         -- float
6: dew(temp2)   -- float
7: humidity     -- float
8: datetime     -- str(2013-01-23 12:10:32.712475)
9: date         -- str(2013-01-23)
10: clientname  -- str(europa)
11: sensoralias -- str(Western Tunnel))
12: vdd         -- float
13: vad         -- float 
14: vis         -- float
15:
16:
17: df          -- float
18: qualityindex -- float
19: secdatetime -- str(2013-01-23 12:10:32.712475) 
99: eol         -- str(eol)
"""

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

def dataToFile(sensorid, filedate, bindata, header):
    # File Operations
    try:
        subdirname = socket.gethostname()
        path = os.path.join(outputdir,subdirname,sensorid)
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


class FileProtocol():
    def __init__(self, wsMcuFactory):
        self.wsMcuFactory = wsMcuFactory
        print "Init"
        self.path_or_url = ''

    def establishConnection(self, path_or_url):
        log.msg('Setting path for data access to: %s ' % (path_or_url))
        # Test whether path_or_url can be reachend
        self.path_or_url = path_or_url
        if os.path.isfile(self.path_or_url):
            return True
        else:
            log.msg('File not existing: %s ' % (path_or_url))
            return False
        

    def fileConnected(self):
        # include MagPy here: stream = pmread(path_or_url=xxx)
        # return xyzft1 whatsoever contains values
        log.msg('Opening: %s ' % (path_or_url))
        with open(self.path_or_url, 'r') as myfile:
            print (list(myfile)[-1])
        myfile.closed


## GEM -GSM90 protocol
##
class GSM90Protocol(LineReceiver):
    def __init__(self, wsMcuFactory):
        self.wsMcuFactory = wsMcuFactory
        print "Initialize the connection and set automatic mode (use ser.commands?)"

    def initConnection(self, path_or_url):
        log.msg('MagPy Module connected - Accessing file: %s ' % (path_or_url))
        # Test whether path_or_url can be reachend
        self.path_or_url = path_or_url
        log.msg('MagPy Module: File connected')
        return True
        
    def connectionMade(self):
        log.msg('Serial port connected.')


## Lemi protocol
##
class LemiProtocol(LineReceiver):

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory, sensor):
        self.wsMcuFactory = wsMcuFactory
        #self.sensor = "lemi"
        self.sensor = sensor
        self.buffer = ''
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
        log.msg('LEMI connected.')

    def connectionLost(self):
        log.msg('LEMI connection lost. Perform steps to restart it!')

    #def h2d(x):		# Hexadecimal to decimal (for format LEMIBIN2)
    #    y = int(x/16)*10 + x%16		# Because the binary for dates is in binary-decimal, not just binary.
    #    return y

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
        subdirname = socket.gethostname()
        path = os.path.join(outputdir,subdirname,self.sensor)
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
            x = (data_array[20]-biasx)*1000.
            y = (data_array[21]-biasy)*1000.
            z = (data_array[22]-biasz)*1000.
            temp_sensor = data_array[11]/100.
            temp_el = data_array[12]/100.
        except:
            log.err("LEMI - Protocol: Number conversion error.")

        # important !!! change outtime to lemi reading when GPS is running 
        try:
            evt1 = {'id': 8, 'value': timestamp}
            evt2 = {'id': 1, 'value': x}
            evt3 = {'id': 2, 'value': y}
            evt4 = {'id': 3, 'value': z}
            evt5 = {'id': 5, 'value': temp_sensor}
            evt6 = {'id': 6, 'value': temp_el}
            evt7 = {'id': 99, 'value': 'eol'}
        except:
            log.err('LEMI - Protocol: Error assigning "evt" values.')
 
        return evt1,evt2,evt3,evt4,evt5,evt6,evt7
         
    def dataReceived(self, data):
        #print "Lemi data here!", self.buffer
        dispatch_url =  "http://example.com/"+hostname+"/lemi#"+self.sensor+"-value"
        flag = 0
        WSflag = 0

        try:
            if (self.buffer).startswith("L036") and len(self.buffer) == 153:
                currdata = self.buffer
                self.buffer = ''
                evt1,evt2,evt3,evt4,evt5,evt6,evt7 = self.processLemiData(currdata)
                WSflag = 2

            ### Note: this code for fixing data is more complex than the POS fix code
            ### due to the LEMI device having a start code rather than an EOL code.
            ### It can handle and deal with multiple errors:
            ###  - multiple data parts arriving at once
            ###  - databits being lost. Bad string is then deleted.
            ###  - bad bits infiltrating the data. Bad string is deleted.

            if len(self.buffer) > 153:
                log.msg('LEMI - Protocol: Warning: Bufferlength (%s) exceeds 153 characters, fixing...' % len(self.buffer))
                lemisearch = (self.buffer).find("L036")
                print '1', lemisearch
                if (self.buffer).startswith("L036"):      # check if first part read is start of POS string.
                    datatest = len(self.buffer)%153
                    dataparts = int(len(self.buffer)/153)
                    if datatest == 0:
                        log.msg('LEMI - Protocol: It appears multiple parts came in at once, # of parts:', dataparts)
                        for i in range(dataparts):
                            split_data_string = self.buffer[0:153]
                            if (split_data_string).startswith("L036"):
                                log.msg('LEMI - Protocol: Processing data part # %s in string...' % (str(i+1)))
                                evt1,evt2,evt3,evt4,evt5,evt6,evt7 = self.processLemiData(split_data_string)
                                WSflag = 2
                                self.buffer = self.buffer[153:len(self.buffer)]
                            else:
                                flag = 1
                                break
                    else:
                        for i in range(dataparts):
                            lemisearch = (self.buffer).find("L036", 6)
                            if lemisearch >= 153:
                                split_data_string = self.buffer[0:153]
                                evt1,evt2,evt3,evt4,evt5,evt6,evt7 = self.processLemiData(split_data_string)
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
                    lemisearch = repr(self.buffer).find("L036")
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
            log.error('LEMI - Protocol: Error while parsing data.')

        ## publish event to all clients subscribed to topic
        if WSflag == 2:
            try:
                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                self.wsMcuFactory.dispatch(dispatch_url, evt2)
                self.wsMcuFactory.dispatch(dispatch_url, evt3)
                self.wsMcuFactory.dispatch(dispatch_url, evt4)
                self.wsMcuFactory.dispatch(dispatch_url, evt5)
                self.wsMcuFactory.dispatch(dispatch_url, evt6)
                self.wsMcuFactory.dispatch(dispatch_url, evt7)
            except:
                log.err('LEMI - Protocol: wsMcuFactory error while dispatching data.')


class Pos1Protocol(LineReceiver):

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory, sensor):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        delimiter = '\x00'
        self.buffer = ''

    @exportRpc("control-led")
    def controlLed(self, status):
        if status:
            print "turn on LED"
            self.transport.write('1')
        else:
            print "turn off LED"
            self.transport.write('0')

    @exportRpc("send-command")
    def send_command(self, command):    #----------CHANGED----------
        if not command == "":
            commandstr = []
            for character in command:
                hexch = binascii.hexlify(character)
                commandstr.append(('\\x' + hexch).decode('string_escape'))

            command_hex = ''.join(commandstr) + '\x00'
            self.transport.write(command_hex)  
            # WARNING: something is sent but not recognised as command.

    def connectionLost(self):
        log.msg(' Pos1 connection lost.')

    def connectionMade(self):
        log.msg('POS1 connected.')

        if self.buffer == '':
            currenttime = datetime.utcnow()
            datestr = str(datetime.strftime(currenttime,'%m-%d-%y'))
            timestr = str(datetime.strftime(currenttime,'%H:%M:%S'))
            default_cmds = ['mode text','time ' + timestr,'date ' + datestr,'range 48500','auto 3']
            

           # for item in default_cmds:
           #     self.send_command(item)
           #     print item, self.buffer
           #     self.clearLineBuffer() # flush buffer, commands return strings of varying lengths.

            #log.msg('Starting POS-1 automatic field measurement.')

    def processPos1Data(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""
        if len(data) != 44:
            log.err('POS1 - Protocol: Unable to parse data of length %i' % len(data))

        currenttime = datetime.utcnow()
        date = datetime.strftime(currenttime, "%Y-%m-%d")
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        packcode = '6hLLLh6hL'
        sensorid = 'pos1'
        header = "# MagPyBin %s %s %s %s %s %s %d" % ('POS1', '[f,df,var1,sectime]', '[f,df,var1,GPStime]', '[nT,nT,none,none]', '[1000,1000,1,1]', packcode, struct.calcsize(packcode))

        try:
            # Extract data
            data_array = data.split()
            intensity = float(data_array[0])/1000.
            sigma_int = float(data_array[2])/1000.
            err_code = int(data_array[3].strip('[').strip(']'))
            dataelements = datetime.strptime(data_array[4],"%m-%d-%y")
            newdate = datetime.strftime(dataelements,"%Y-%m-%d")
            gps_time = newdate + ' ' + str(data_array[5])[:11]
        except:
            log.err('POS1 - Protocol: Data formatting error.')
            intensity = 0.0
            sigma_int = 0.0
            err_code = 0.0
        try:
            # extract time data
            datearray = timeToArray(timestamp)
            gpsarray = timeToArray(gps_time)
            try:
                datearray.append(int(intensity*1000))
                datearray.append(int(sigma_int*1000))
                datearray.append(err_code)
                datearray.extend(gpsarray)
                data_bin = struct.pack(packcode,datearray[0],datearray[1],datearray[2],datearray[3],datearray[4],datearray[5],datearray[6],datearray[7],datearray[8],datearray[9],datearray[10],datearray[11],datearray[12],datearray[13],datearray[14],datearray[15],datearray[16])
                # File Operations
                dataToFile(sensorid, date, data_bin, header)
            except:
                log.msg('POS1 - Protocol: Error while packing binary data')
                pass
        except:
            log.msg('POS1 - Protocol: Error with binary save routine')
            pass


        evt1 = {'id': 4, 'value': intensity}
        evt4 = {'id': 8, 'value': timestamp}
        evt5 = {'id': 99, 'value': 'eol'}

        return evt1,evt4,evt5
         

    def dataReceived(self, data):
        dispatch_url =  "http://example.com/"+hostname+"/pos1#"+self.sensor+"-value"
        try:
            #log.msg('Bufferlength:', len(self.buffer))		# debug
            if len(self.buffer) == 44:
                evt1, evt4, evt5 = self.processPos1Data(self.buffer[:44])
                self.buffer = ''

                ## publish event to all clients subscribed to topic
                ##
                if evt1['value'] > 0:
                    self.wsMcuFactory.dispatch(dispatch_url, evt1)
                    self.wsMcuFactory.dispatch(dispatch_url, evt4)
                    self.wsMcuFactory.dispatch(dispatch_url, evt5)
                else:
                    log.err('POS1 - Protocol: Zero value, skipping. (Value still written to file.)')

            self.buffer = self.buffer + data

            if len(self.buffer) > 44:
                log.msg('POS1 - Protocol: Warning: Bufferlength (%s) exceeds 44 characters, fixing...' % len(self.buffer))

                if repr(data).endswith("x00'"):    # check if last part read is end of POS string.
                    datatest = (len(self.buffer))%44
                    # OPTION 1: Data is good, but too much arrived at once. Split and process.
                    if datatest == 0:
                        dataparts = int(len(self.buffer)/44)
                        log.msg('POS1 - Protocol: It appears multiple parts came in at once, # of parts:', dataparts)
                        for i in range(dataparts):
                            split_data_string = self.buffer[i*44:(i*44)+44]
                            log.msg('POS1 - Protocol: Processing data part # %s in string (%s)' % (str(i+1), split_data_string))
                            evt1, evt4, evt5 = self.processPos1Data(split_data_string)
                            if evt1['value'] > 0:
                                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                                self.wsMcuFactory.dispatch(dispatch_url, evt4)
                                self.wsMcuFactory.dispatch(dispatch_url, evt5)
                            else:
                                log.err('POS1 - Protocol: Zero value, skipping. (Value still written to file.)')
                        self.buffer = ''
                    # OPTION 2: Data is bad; bit was lost.
                    else:
                        log.msg('POS1 - Protocol: String contains bad data. Deleting. (String content: %s)' % self.buffer)
                        self.buffer = ''              # If true, bad data. Log and delete.
                else:    # if last part read is not end of POS string, continue reading.
                    log.msg('POS1 - Protocol: Attempting to fix buffer... last part read:', self.buffer[-10:], "Bufferlength:", len(self.buffer))

        except ValueError:
            log.err('POS1 - Protocol: Unable to parse data %s' % data)


## Caesium protocol
##
class CsProtocol(LineReceiver):
    """
    Protocol to read GSM CS Sensor data from serial unit
    Each sensor has its own class (that can be improved...)
    The protocol defines the sensor name in its init section, which 
    is used to dipatch url links and define local storage folders

    """

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory, sensor):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor

    def connectionMade(self):
        log.msg('Cs-Sensor at serial port connected.')

    def processData(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""

        currenttime = datetime.utcnow()
        filename = datetime.strftime(currenttime, "%Y-%m-%d")
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")

        global lastActualtime

        packcode = '6hLL'
        sensorid = self.sensor
        header = "# MagPyBin %s %s %s %s %s %s %d" % (sensorid, '[f]', '[f]', '[nT]', '[1000]', packcode, struct.calcsize(packcode))
        
        try:
            value = float(data[0])
            if 10000 < value < 100000:
                intensity = value
            else:
                intensity = 0.0
        except ValueError:
            log.err("CS - Protocol: Not a number. Instead found:", data[0])
            intensity = float(NaN)

        datearray = timeToArray(timestamp)
        try:
            datearray = timeToArray(timestamp)
            datearray.append(int(intensity*1000))
            data_bin = struct.pack(packcode,*datearray)
        except:
            log.msg('Error while packing binary data')
            pass

        # File Operations
        dataToFile(sensorid, filename, data_bin, header)

        
        #return value every second
        if lastActualtime+timedelta(microseconds=999000) >= currenttime:   # Using ms instead of s accounts for only small errors, not all.
            evt1 = {'id': 4, 'value': 0}
            evt4 = {'id': 0, 'value': 0}
            evt8 = {'id': 8, 'value': 0}
        else:
            evt1 = {'id': 4, 'value': intensity}
            evt4 = {'id': 0, 'value': outtime}
            evt8 = {'id': 8, 'value': timestamp}
            lastActualtime = currenttime

        return evt1,evt4,evt8

    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+hostname+"/cs#"+self.sensor+"-value"
        try:
            data = line.strip('$').split(',')
            evt1, evt4, evt8 = self.processData(data)
        except ValueError:
            log.err('CS - Protocol: Unable to parse data %s' % line)
            #return
        except:
            pass


        if evt1['value'] and evt4['value']:
            try:
                ## publish event to all clients subscribed to topic
                ##
                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                self.wsMcuFactory.dispatch(dispatch_url, evt4)
                self.wsMcuFactory.dispatch(dispatch_url, evt8)
                #log.msg("Analog value: %s" % str(evt4))
            except:
                log.err('CS - Protocol: wsMcuFactory error while dispatching data.')

## Environment protocol
##
class EnvProtocol(LineReceiver):
    """
    Protocol to read MessPC EnvironmentalSensor 5 data from usb unit
    Each sensor has its own class (that can be improved...)
    The protocol defines the sensor name in its init section, which 
    is used to dipatch url links and define local storage folders

    """

    ## need a reference to our WS-MCU gateway factory to dispatch PubSub events
    ##
    def __init__(self, wsMcuFactory):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = "env"

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
        log.msg('Serial port connected.')

    def processEnvData(self, data):
        """Convert raw ADC counts into SI units as per datasheets"""

        currenttime = datetime.utcnow()
        outdate = datetime.strftime(currenttime, "%Y-%m-%d")
        filename = outdate
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        #header = "# MagPyBin, sensor_id, [parameterlist], [unit-conversion-list], packing string, length"
        packcode = '6hLLLL'
        sensorid = 'Env05'
        header = "# MagPyBin %s %s %s %s %s %s %d" % (sensorid, '[t1,t2,var1]', '[T,DewPoint,RH]', '[deg_C,deg_C,per rh]', '[1000,1000,1000]', packcode, struct.calcsize(packcode))

        valrh = re.findall(r'\d+',data[0])
        if len(valrh) > 1:
            temp = float(valrh[0] + '.' + valrh[1])
        else:
            temp = float(valrh[0])
        valrh = re.findall(r'\d+',data[1])
        if len(valrh) > 1:
            rh = float(valrh[0] + '.' + valrh[1])
        else:
            rh = float(valrh[0])
        valrh = re.findall(r'\d+',data[2])
        if len(valrh) > 1:
            dew = float(valrh[0] + '.' + valrh[1])
        else:
            dew = float(valrh[0])

        datearray = timeToArray(timestamp)
        try:
            datearray = timeToArray(timestamp)
            datearray.append(int(temp*1000))
            datearray.append(int(dew*1000))
            datearray.append(int(rh*1000))
            data_bin = struct.pack(packcode,*datearray)
        except:
            log.msg('Error while packing binary data')
            pass

        # File Operations
        dataToFile(sensorid, filename, data_bin, header)

        # create a dictionary out of the input file
 
        evt1 = {'id': 7, 'value': rh}
        evt2 = {'id': 5, 'value': temp}
        evt3 = {'id': 6, 'value': dew}
        evt4 = {'id': 0, 'value': outtime}
        evt5 = {'id': 10, 'value': hostname}
        evt6 = {'id': 9, 'value': outdate}
 
        return evt1, evt2, evt3, evt4, evt5, evt6


    def lineReceived(self, line):
        dispatch_url =  "http://example.com/"+hostname+"/env#"+self.sensor+"-value"
        try:
            data = line.split()
            if len(data) == 3:
                evt1,evt2,evt3,evt4,evt5,evt6 = self.processEnvData(data)
            else:
                print 'Data error'


            ## publish event to all clients subscribed to topic
            ##
            self.wsMcuFactory.dispatch(dispatch_url, evt1)
            self.wsMcuFactory.dispatch(dispatch_url, evt2)
            self.wsMcuFactory.dispatch(dispatch_url, evt3)
            self.wsMcuFactory.dispatch(dispatch_url, evt4)
            self.wsMcuFactory.dispatch(dispatch_url, evt5)
            self.wsMcuFactory.dispatch(dispatch_url, evt6)
            #log.msg("Analog value: %s" % str(evt4))

        except ValueError:
            log.err('Unable to parse data %s' % line)
            #return


if onewire:
    class OwProtocol():
        """
        Protocol to read one wire data from usb DS unit 
        All connected sensors are listed and data is distributed in dependency of sensor id
        Dipatch url links are defined by channel 'ow' and id+'value'
        Save path ? folders ?

        """
        def __init__(self, wsMcuFactory):
            self.wsMcuFactory = wsMcuFactory
            #self.sensor = 'ow'
            ow.init("u")
            self.root = ow.Sensor('/').sensorList()
            self.reconnectcount = 0

        def owConnected(self):
            global owsensorlist
            try:
                self.root = ow.Sensor('/').sensorList()

                if not (self.root == owsensorlist):
                    log.msg('Rereading sensor list')                
                    ow.init("u")
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
                    print "owConnect: reconnection not possible"

            self.oneWireInstruments(self.root)


        def connectionMade(self,root):
            log.msg('One Wire module initialized - found the following sensors:')
            for sensor in root:
                # Use this list to initialize the sensor database including datalogger id and type
                log.msg('Type: %s, ID: %s' % (sensor.type, sensor.id))

        def oneWireInstruments(self,root):
            for sensor in root:
                if sensor.type == 'DS18B20':             
                    #sensor.useCache( False ) # Important for below 15 sec resolution (by default a 15 sec cache is used))
                    self.readTemperature(sensor)
                #if sensor.type == 'DS2406':
                #    self.readSHT(sensor)
                elif sensor.type == 'DS2438':
                    #sensor.useCache( False ) # Important for below 15 sec resolution (by default a 15 sec cache is used))
                    self.readBattery(sensor)

        def alias(self, sensorid):
            #define a alias dictionary
            sensordict = {"332988040000": "Mobil", "504C88040000": "1. Stock: Treppenhaus", 
                          "6C2988040000": "1. Stock: Flur", "FD9087040000": "Nordmauer Erdgeschoss", 
                          "090A88040000": "1. Stock: Wohnzimmer", "BB5388040000": "1. Stock: Kueche",
                          "F58788040000": "1. Stock: Schlafzimmer", "BAAE87040000": "Dach: Nico (T)",
                          "E2FE87040000": "1. Stock: Speis",
                          "BED887040000": "Dach: Flur", "2F3488040000": "1. Stock: Bad (T)", "0EB354010000": "Dach: Nico",
                          "3AD754010000": "Dach: Tina", "CBC454010000": "1. Stock: Kinderzimmer",
                          "05CE54010000": "1. Stock: Bad"}      
            try:
                return sensordict[sensorid]
            except:
                return sensorid

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
                log.msg('Error while extracting time array')
                return []

        def dataToFile(self, sensorid, filedate, bindata, header):
            # File Operations
            try:
                subdirname = socket.gethostname()
                path = os.path.join(outputdir,subdirname,sensorid)
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
            
        def readTemperature(self, sensor):

            #t = threading.Timer(1.0, self.readTemperature, [sensor])
            #t.deamon = True
            #t.start()
            dispatch_url =  "http://example.com/"+hostname+"/ow#"+sensor.id+"-value"
            currenttime = datetime.utcnow()
            filename = datetime.strftime(currenttime, "%Y-%m-%d")
            actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
            timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
            outtime = datetime.strftime(currenttime, "%H:%M:%S")
            #header = "# MagPyBin, sensor_id, [parameterlist], [unit-conversion-list], packing string, length"
            packcode = '6hLL'
            header = "# MagPyBin %s %s %s %s %s %s %d" % (sensor.id, '[t1]', '[T]', '[degC]', '[1000]', packcode, struct.calcsize(packcode))

            try:
                # Extract data
                temp = float(sensor.temperature)

                # extract time data
                datearray = self.timeToArray(timestamp)
                try:
                    datearray.append(int(temp*1000))
                    #data_bin = struct.pack(packcode,datearray[0],datearray[1],datearray[2],datearray[3],datearray[4],datearray[5],datearray[6],datearray[7])
                    data_bin = struct.pack(packcode,*datearray)
                except:
                    log.msg('Error while packing binary data')
                    pass

                # File Operations
                self.dataToFile(sensor.id, filename, data_bin, header)

                # Provide data to websocket
                evt1 = {'id': 0, 'value': outtime}
                evt6 = {'id': 8, 'value': timestamp}
                evt2 = {'id': 5, 'value': temp}
                evt5 = {'id': 10, 'value': hostname}
                evt8 = {'id': 99, 'value': 'eol'}

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
                log.err('Lost temperature sensor -- reconnecting')
                self.owConnected()
                

        def readBattery(self,sensor):
            dispatch_url =  "http://example.com/"+hostname+"/ow#"+sensor.id+"-value"
            currenttime = datetime.utcnow()
            filename = datetime.strftime(currenttime, "%Y-%m-%d")
            actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
            timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
            outtime = datetime.strftime(currenttime, "%H:%M:%S")
            packcode = '6hLLLLLf'
            header = "# MagPyBin %s %s %s %s %s %s %d" % (sensor.id, '[t1,var1,var2,var3,var4]', '[T,rh,vdd,vad,vis]', '[deg_C,per,V,V,V]', '[1000,100,100,100,1]', packcode, struct.calcsize(packcode))

            try:
                # Extract data
                try:
                    humidity = float(ow.owfs_get('/uncached%s/HIH4000/humidity' % sensor._path))
                except:
                    humidity = float(nan)
                temp = float(sensor.temperature)
                vdd = float(sensor.VDD)
                vad = float(sensor.VAD)
                vis = float(sensor.vis)

                # Appending data to buffer which contains pcdate, pctime and sensordata
                # extract time data
                datearray = self.timeToArray(timestamp)

                try:
                    datearray.append(int(temp*1000))
                    datearray.append(int(humidity*100))
                    datearray.append(int(vdd*100))
                    datearray.append(int(vad*100))
                    datearray.append(vis)
                    #data_bin = struct.pack(packcode,datearray[0],datearray[1],datearray[2],datearray[3],datearray[4],datearray[5],datearray[6],datearray[7],datearray[8],datearray[9],datearray[10],datearray[11])
                    data_bin = struct.pack(packcode,*datearray)
                except:
                    log.msg('Error while packing binary data')
                    pass

                # File Operations
                self.dataToFile(sensor.id, filename, data_bin, header)

                evt1 = {'id': 0, 'value': outtime}
                evt9 = {'id': 8, 'value': timestamp}
                evt2 = {'id': 5, 'value': temp}
                if humidity < 100:
                    evt3 = {'id': 7, 'value': humidity}
                else:
                    evt3 = {'id': 7, 'value': 0}
                evt4 = {'id': 11, 'value': self.alias(sensor.id)}
                evt5 = {'id': 12, 'value': vdd}
                evt6 = {'id': 13, 'value': vad}
                evt7 = {'id': 14, 'value': vis}
                evt8 = {'id': 99, 'value': 'eol'}

                try:
                    self.wsMcuFactory.dispatch(dispatch_url, evt1)
                    self.wsMcuFactory.dispatch(dispatch_url, evt9)
                    self.wsMcuFactory.dispatch(dispatch_url, evt2)
                    self.wsMcuFactory.dispatch(dispatch_url, evt3)
                    self.wsMcuFactory.dispatch(dispatch_url, evt4)
                    self.wsMcuFactory.dispatch(dispatch_url, evt5)
                    self.wsMcuFactory.dispatch(dispatch_url, evt6)
                    self.wsMcuFactory.dispatch(dispatch_url, evt7)
                    self.wsMcuFactory.dispatch(dispatch_url, evt8)
                    pass
                except ValueError:
                    log.err('Unable to parse data at %s' % actualtime)
            except:
                log.err('Lost battery sensor -- reconnecting')
                self.owConnected()
