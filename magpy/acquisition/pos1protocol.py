'''
Path:                   magpy.acquisition.pos1protocol
Part of package:        acquisition
Type:                   Acquisition protocol, part of data acquisition library

PURPOSE:
        This contains everything needed to get a Russian POS-1 magnetometer
        up and running and saving + sending data.

CONTAINS:
        *Pos1Protocol:  (Class - twisted.protocols.basic.LineReceiver)
                        Class for handling data acquisition of POS-1 magnetometer.
                        Includes internal class functions: processPos1Data
        _timeToArray:   (Func) ... utility function for Pos1Protocol.
        _dataToFile:    (Func) ... utility function for Pos1Protocol.
        startPOS1:      (Func) Starts POS-1 magnetometer and acquisition.
                        Note: REQUIRED for data acquisition.
        _hexifyCommand: (Func) ... utility function for startPOS1.
        _serReadline:   (Func) ... utility function for startPOS1.

DEPENDENCIES:
        twisted, autobahn

CALLED BY:
        magpy.bin.acquisition
'''
from __future__ import print_function

import sys, time, os, socket
import struct, binascii, re
from binascii import hexlify
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


def startPOS1(port,commands):
    '''
    DEFINITION:
    Call this function to initiate POS-1 measurements.
    NOTE: Measurements will not start automatically with power supply.
    THIS FUNCTION IS REQUIRED TO START MEASUREMENTS.

    PARAMETERS:
    Variables:
        - port:         (str) Port of POS-1 magnetometer, e.g. '/dev/ttyS1'
        - commands:     (list) List of commands to send to POS-1.
                        Standard for startup:
                        commands = ['mode text','time ' + timestr,'date ' + datestr,'auto 5']

    RETURNS:
        - None

    EXAMPLE:
        >>>

    APPLICATION:
        >>>

    COMMAND DICTIONARY:
        Note: these commands must usually be entered in the order shown here.
        For responses to be read out, "mode text" must be called as the first command.
        command_dict = {
    'ENQ': 'gives information about the connected equipment',
    'NAK': 'the previous answer is repeated',
    'about': 'brief information about the manufacturer is given',
    'standby on/off': 'sets lowered power consumption on/off',
    'mode text/binary': 'determines form of data output',
    'time': 'gives current internal time in hh:mm:ss',
    'time hh:mm:ss': 'establishes an internal time counter',
    'date': 'gives current internal date in mm-dd-yy',
    'date mm-dd-yy': 'establishes the internal current date',
    'range': 'gives the current measured range in nT',
    'range CENTER': 'defines the centre of the working range',
    'run': 'starts measurement of a magnetic field',
    'auto PRM': 'set the automatic measurement mode, PRM is period (1-86400)',
        }

    ERROR DICTIONARIES:
    An error code is always made up of two digits. Errors1 describes
    the first digit, Errors2 the second digit.
        Errors1 = {
    '1': 'out of range',
    '2': 'No signal',
    '3': 'No signal & out of range',
    '4': 'Low power',
    '5': 'Low power & out of range',
    '6': 'Low power & no signal',
    '7': 'Low power & no signal & out of range',
    '8': 'No errors'
        }

        Errors2 = {
    '0': 'No warnings',
    '1': 'Diapason error',
    '2': 'Short time',
    '3': 'Diapason error & short time',
    '4': 'Low S/N ratio',
    '5': 'Low S/N & diapason error',
    '6': 'Low S/N & short time',
    '7': 'Low S/N & short time & diapason error'
        }
    '''

    # Specify parameters:
    eol = '\x00'
    currenttime = datetime.utcnow()
    datestr = str(datetime.strftime(currenttime,'%m-%d-%y'))
    timestr = str(datetime.strftime(currenttime,'%H:%M:%S'))

    # Initiation commands:
    # NOTE: These only have to be called once to initiate the device.
    # Once called, the device will continue to read unless interrupted.
    try:
        pos_ser = serial.Serial(port, baudrate=9600, parity='N', bytesize=8, stopbits=1)
        print('Connection to POS-1 made.')
    except:
        print('Connection to POS-1 flopped.')
    print('')

    # Send commands and read output from serial device:
    print('Parameters entered:')
    for item in commands:
        command_hex = _hexifyCommand(item,eol)
        print('Command:  ', command)
        ser.write(command_hex)
        response = _serReadline(pos_ser,eol)
        print('Response: ', response)
    print('')


def _hexifyCommand(command,eol):
    '''
    This function translates the command text string into a hex
    string that the serial device can read. 'eol' is the
    end-of-line character, '\x00' for the POS-1 magnetometer.
    '''

    commandstr = []
    for character in command:
        hexch = binascii.hexlify(character)
        commandstr.append(('\\x' + hexch).decode('string_escape'))

    command_hex = ''.join(commandstr) + (eol)

    return command_hex


def _serReadline(ser,eol):
    '''
    Useful little function for reading lines from a serial port
    with non-standard end-of-line characters ('\x00').
    '''

    ser_str = ''
    while True:
        char = ser.read()
        if char == eol:
            break
        ser_str += char
    return ser_str


def _timeToArray(timestring):
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
        return datetime(*datearray)
    except:
        log.msg('Error while extracting time array')
        return []


def _dataToFile(outputdir, sensorid, filedate, bindata, header):
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
            print("turn on LED")
            self.transport.write('1')
        else:
            print("turn off LED")
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
        #print (data)
        if len(data) != 44:
            log.err('POS1 - Protocol: Unable to parse data of length %i' % len(data))

        currenttime = datetime.utcnow()
        date = datetime.strftime(currenttime, "%Y-%m-%d")
        actualtime = datetime.strftime(currenttime, "%Y-%m-%dT%H:%M:%S.%f")
        outtime = datetime.strftime(currenttime, "%H:%M:%S")
        timestamp = datetime.strftime(currenttime, "%Y-%m-%d %H:%M:%S.%f")
        packcode = '6hLLLh6hL'
        header = "# MagPyBin %s %s %s %s %s %s %d" % (self.sensor, '[f,df,var1,sectime]', '[f,df,var1,GPStime]', '[nT,nT,none,none]', '[1000,1000,1,1]', packcode, struct.calcsize(packcode))

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
        #try:
        # extract time data
        datedt = _timeToArray(timestamp)        
        # new line for time shift
        datedt = datedt - timedelta(seconds=6.770)
        gpsdt = _timeToArray(gps_time)
        try:
            try:
                datearray = [datedt.year, datedt.month, datedt.day, datedt.hour, datedt.minute, datedt.second, datedt.microsecond]
                datearray.append(int(intensity*1000))
                datearray.append(int(sigma_int*1000))
                datearray.append(err_code)
                gpsarray = [gpsdt.year, gpsdt.month, gpsdt.day, gpsdt.hour, gpsdt.minute, gpsdt.second, gpsdt.microsecond]
                datearray.extend(gpsarray)
                data_bin = struct.pack(packcode,datearray[0],datearray[1],datearray[2],datearray[3],datearray[4],datearray[5],datearray[6],datearray[7],datearray[8],datearray[9],datearray[10],datearray[11],datearray[12],datearray[13],datearray[14],datearray[15],datearray[16])
                # File Operations
                _dataToFile(self.outputdir,self.sensor, date, data_bin, header)
            except:
                log.msg('POS1 - Protocol: Error while packing binary data')
                pass
        except:
            log.msg('POS1 - Protocol: Error with binary save routine')
            pass

        evt1 = {'id': 1, 'value': timestamp}
        evt3 = {'id': 3, 'value': outtime}
        evt4 = {'id': 4, 'value': gps_time}
        evt10 = {'id': 10, 'value': intensity}
        evt14 = {'id': 14, 'value': sigma_int}
        evt40 = {'id': 40, 'value': err_code}
        evt99 = {'id': 99, 'value': 'eol'}

        return evt1,evt3,evt4,evt10,evt14,evt40,evt99


    def dataReceived(self, data):
        dispatch_url =  "http://example.com/"+self.hostname+"/pos1#"+self.sensor+"-value"
        #print "Got pos1 data"
        try:
            #log.msg('Bufferlength:', len(self.buffer))         # debug
            if len(self.buffer) == 44:
                evt1, evt3,evt4, evt10, evt14, evt40, evt99 = self.processPos1Data(self.buffer[:44])
                self.buffer = ''

                ## publish event to all clients subscribed to topic
                ##
                if evt1['value'] > 0:
                    self.wsMcuFactory.dispatch(dispatch_url, evt1)
                    self.wsMcuFactory.dispatch(dispatch_url, evt3)
                    self.wsMcuFactory.dispatch(dispatch_url, evt4)
                    self.wsMcuFactory.dispatch(dispatch_url, evt10)
                    self.wsMcuFactory.dispatch(dispatch_url, evt14)
                    self.wsMcuFactory.dispatch(dispatch_url, evt40)
                    self.wsMcuFactory.dispatch(dispatch_url, evt99)
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
                            evt1,evt3,evt4,evt10,evt14,evt40,evt99 = self.processPos1Data(split_data_string)
                            if evt1['value'] > 0:
                                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                                self.wsMcuFactory.dispatch(dispatch_url, evt3)
                                self.wsMcuFactory.dispatch(dispatch_url, evt4)
                                self.wsMcuFactory.dispatch(dispatch_url, evt10)
                                self.wsMcuFactory.dispatch(dispatch_url, evt14)
                                self.wsMcuFactory.dispatch(dispatch_url, evt40)
                                self.wsMcuFactory.dispatch(dispatch_url, evt99)
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
