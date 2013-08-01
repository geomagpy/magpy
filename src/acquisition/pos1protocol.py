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


## POS1 protocol
## -------------

class Pos1Protocol(LineReceiver):

    def __init__(self, wsMcuFactory, sensor, outputdir):
        self.wsMcuFactory = wsMcuFactory
        self.sensor = sensor
        self.outputdir = outputdir
        self.hostname = socket.gethostname()
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
    def send_command(self, command):
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
        log.msg('%s connected.' % self.sensor)

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
                dataToFile(self.outputdir,self.sensor, date, data_bin, header)
            except:
                log.msg('POS1 - Protocol: Error while packing binary data')
                pass
        except:
            log.msg('POS1 - Protocol: Error with binary save routine')
            pass

        evt1 = {'id': 8, 'value': timestamp}
        evt2 = {'id': 4, 'value': intensity}
        evt3 = {'id': 15, 'value': sigma_int}
        evt4 = {'id': 16, 'value': err_code}
        #evt5 = {'id': 17, 'value': outdate+' '+outtime}
        evt6 = {'id': 99, 'value': 'eol'}

        return evt1,evt2,evt3,evt4,evt6
         

    def dataReceived(self, data):
        dispatch_url =  "http://example.com/"+self.hostname+"/pos1#"+self.sensor+"-value"
        #print "Got pos1 data"
        try:
            #log.msg('Bufferlength:', len(self.buffer))		# debug
            if len(self.buffer) == 44:
                evt1, evt4, evt7, evt8, evt9 = self.processPos1Data(self.buffer[:44])
                self.buffer = ''

                ## publish event to all clients subscribed to topic
                ##
                if evt1['value'] > 0:
                    self.wsMcuFactory.dispatch(dispatch_url, evt1)
                    self.wsMcuFactory.dispatch(dispatch_url, evt4)
                    self.wsMcuFactory.dispatch(dispatch_url, evt7)
                    self.wsMcuFactory.dispatch(dispatch_url, evt8)
                    self.wsMcuFactory.dispatch(dispatch_url, evt9)
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
                            evt1, evt4, evt7, evt8, evt9 = self.processPos1Data(split_data_string)
                            if evt1['value'] > 0:
                                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                                self.wsMcuFactory.dispatch(dispatch_url, evt4)
                                self.wsMcuFactory.dispatch(dispatch_url, evt7)
                                self.wsMcuFactory.dispatch(dispatch_url, evt8)
                                self.wsMcuFactory.dispatch(dispatch_url, evt9)
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


