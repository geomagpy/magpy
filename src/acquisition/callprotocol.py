import sys, time, os, socket
import serial
import struct, binascii, re, csv
from datetime import datetime, timedelta
from matplotlib.dates import date2num, num2date
import numpy as np

# Twisted
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.python import usage, log
from twisted.internet.serialport import SerialPort
from twisted.web.server import Site
from twisted.web.static import File

call = True
if call:
    class CallProtocol():
        """
        Protocol to read one wire data from usb DS unit
        All connected sensors are listed and data is distributed in dependency of sensor id
        Dipatch url links are defined by channel 'ow' and id+'value'
        Save path ? folders ?

        """
        def __init__(self, wsMcuFactory, source, outputdir,port,baudrate):
            self.wsMcuFactory = wsMcuFactory
            self.source = source
            self.hostname = socket.gethostname()
            # TODO: create outputdir if not existing
            self.outputdir = outputdir
            self.port = port
            self.baudrate = baudrate
            self.commands = ['11TR00005','12TR00002']
            #eol = '\x00'
            self.eol = '\r'
            #print source

        def lineread(self, ser,eol):
            # FUNCTION 'LINEREAD'
            # Does the same as readline(), but does not require a standard
            # linebreak character ('\r' in hex) to know when a line ends.
            # Variable 'eol' determines the end-of-line char: '\x00'
            # for the POS-1 magnetometer, '\r' for the envir. sensor.
            # (Note: required for POS-1 because readline() cannot detect
            # a linebreak and reads a never-ending line.)

            ser_str = ''
            while True:
                char = ser.read()
                #print char
                if char == eol:
                    break
                ser_str += char
            return ser_str

        def hexify_command(self, command,eol):
            # FUNCTION 'HEXIFY_COMMAND'
            # This function translates the command text string into a hex
            # string that the serial device can read. 'eol' is the
            # end-of-line character. '\r' for the environmental sensor,
            # '\x00' for the POS-1 magnetometer.

            commandstr = []
            for character in command:
                hexch = binascii.hexlify(character)
                commandstr.append(('\\x' + hexch).decode('string_escape'))

            command_hex = (eol) + ''.join(commandstr) + (eol)

            return command_hex


        def send_command(self, ser,command,eol,hex=False):
            if hex:
                command = self.hexify_command(command,eol)
            else:
                command = eol+command+eol
            #print 'Command:  %s \n ' % command.replace(eol,'')
            sendtime = date2num(datetime.utcnow())
            ser.write(command)
            response = self.lineread(ser,eol)
            receivetime = date2num(datetime.utcnow())
            meantime = np.mean([receivetime,sendtime])
            #print "Timediff", (receivetime-sendtime)*3600*24
            return response, num2date(meantime).replace(tzinfo=None)

        def sendCommands(self):
            #print "Connecting ..."
            try:
                ser = serial.Serial(self.port, baudrate=self.baudrate , parity='N', bytesize=8, stopbits=1)
                #print 'Connection made.'
            except:
                print 'SerialCall: Connection flopped.'

            for item in self.commands:
                answer, actime = self.send_command(ser,item,self.eol,hex=False)
                success = self.analyzeResponse(answer, actime)
                time.sleep(2)
                if not success:
                    log.msg('SerialCall: Could not interpret response of system when sending %s' % item)
            ser.close()


        def analyzeResponse(self,answer, actime):
            # A loading eventually existing sensor list
            if len(answer.split(';'))==525:
                self.writeDisdro(answer, actime)
            elif len(answer.split()) == 4 and answer.split()[0].startswith('\x03'):
                self.writeAnemometer(answer,actime)
            else:
                print "SerialCall: Could no analyze data"
                return False
            return True


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
                log.msg('SERIAL - timetoArray: Error while extracting time array')
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
                log.err("SERIAL - datatofile: Error while saving file")

        def dataToCSV(self, sensorid, filedate, asciidata, header):
                # File Operations
                #try:
                path = os.path.join(self.outputdir,self.hostname,sensorid)
                if not os.path.exists(path):
                    os.makedirs(path)
                savefile = os.path.join(path, sensorid+'_'+filedate+".asc")
                asciilist = asciidata.split(';')
                if not os.path.isfile(savefile):
                    with open(savefile, "wb") as csvfile:
                        writer = csv.writer(csvfile,delimiter=';')
                        writer.writerow(header)
                        writer.writerow(asciilist)
                else:
                    with open(savefile, "a") as csvfile:
                        writer = csv.writer(csvfile,delimiter=';')
                        writer.writerow(asciilist)
                #except:
                #log.err("datatoCSV: Error while saving file")

        def writeDisdro(self, line, actime):

            #t = threading.Timer(1.0, self.readTemperature, [sensor])
            #t.deamon = True
            #t.start()
            filename = datetime.strftime(actime, "%Y-%m-%d")
            timestamp = datetime.strftime(actime, "%Y-%m-%d %H:%M:%S.%f")
            outtime = datetime.strftime(actime, "%H:%M:%S")

            try:
                data = line.split(';')

                # Extract data
                sensor = 'LNM'
                serialnum = data[1]
                cumulativerain = float(data[15])        # x
                if cumulativerain > 9000:
                    #send_command(reset)
                    pass
                visibility = int(data[16])              # y
                reflectivity = float(data[17])          # z
                intall = float(data[12] )               # var1
                intfluid = float(data[13])              # var2
                intsolid = float(data[14])              # var3
                quality = int(data[18])
                haildiameter = float(data[19])          # var4
                insidetemp = float(data[36])            # t2
                lasertemp = float(data[37])
                lasercurrent = data[38]
                outsidetemp = float(data[44])           # t1
                Ptotal= int(data[49])                   # f
                Pslow = int(data[51])                   # dx
                Pfast= int(data[53])                    # dy
                Psmall= int(data[55])                   # dz
                revision = '0001' # Software version 2.42
                sensorid = sensor + '_' + serialnum + '_' + revision
            except:
                log.err('SerialCall - writeDisdro: Could not assign data values')

            dispatch_url =  "http://example.com/"+self.hostname+"/ser#"+sensorid+"-value"

            #print sensorid, outsidetemp

            try:
                ##### Write ASCII data file with full outpunt and timestamp
                # extract time data
                # try:
                #print "Writing"
                timestr = timestamp.replace(' ',';')
                asciiline = ''.join([i for i in line if ord(i) < 128])
                asciidata = timestr + ';' + asciiline.strip('\x03').strip('\x02')
                #print asciidata
                header = '# LNM - Telegram5 plus NTP date and time at position 0 and 1'
            except:
                log.msg('SerialCall - writeDisdro: Error while saving ascii data')
            self.dataToCSV(sensorid, filename, asciidata, [header])
            #except:
            #    log.msg('SerialCall - writeDisdro: Error while saving ascii data')

            # Provide data to websocket
            try:
                # Check STANDARD_ID list for correct numbers
                evt3 = {'id': 3, 'value': outtime}
                evt1 = {'id': 1, 'value': timestamp}
                evt30 = {'id': 30, 'value': outsidetemp}
                evt31 = {'id': 31, 'value': insidetemp}
                evt36 = {'id': 36, 'value': cumulativerain}
                evt37 = {'id': 37, 'value': visibility}
                evt39 = {'id': 39, 'value': Ptotal}
                evt0 = {'id': 0, 'value': self.hostname}
                evt99 = {'id': 99, 'value': 'eol'}
            except:
                print "SerialCall - writeDisdro: Problem assigning values to dict"

            try:
                self.wsMcuFactory.dispatch(dispatch_url, evt0)
                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                self.wsMcuFactory.dispatch(dispatch_url, evt3)
                self.wsMcuFactory.dispatch(dispatch_url, evt30)
                self.wsMcuFactory.dispatch(dispatch_url, evt31)
                self.wsMcuFactory.dispatch(dispatch_url, evt36)
                self.wsMcuFactory.dispatch(dispatch_url, evt37)
                self.wsMcuFactory.dispatch(dispatch_url, evt39)
                self.wsMcuFactory.dispatch(dispatch_url, evt99)
            except ValueError:
                log.err('SerialCall - writeDisdro: Unable to parse data at %s' % actualtime)


        def writeAnemometer(self, line, actime):

            # 1. Get serial number:
            sensor = 'ULTRASONICDSP'
            revision = '0001'
            try:
                ser = serial.Serial(self.port, baudrate=self.baudrate , parity='N', bytesize=8, stopbits=1)
                answer, tmptime = self.send_command(ser,'12SH',self.eol,hex=False)
                ser.close()
                serialnum = answer.replace('!12SH','').strip('\x03').strip('\x02')
            except:
                print 'writeAnemometer: Failed to get Serial number.'


            sensorid = sensor + '_' + serialnum + '_' + revision
            dispatch_url =  "http://example.com/"+self.hostname+"/ser#"+sensorid+"-value"

            # 2. Getting data:
            filename = datetime.strftime(actime, "%Y-%m-%d")
            timestamp = datetime.strftime(actime, "%Y-%m-%d %H:%M:%S.%f")
            outtime = datetime.strftime(actime, "%H:%M:%S")

            try:
                data = line.split()
                # Extract data
                windspeed = float(data[0].strip('\x03').strip('\x02'))  # var1
                winddirection = float(data[1])                          # var2
                virtualtemperature = float(data[2])                     # t2
            except:
                windspeed = float('nan')                          # var1
                winddirection = float('nan')                      # var2
                virtualtemperature = float('nan')                 # t2
                print 'writeAnemometer: Failed to interprete data.'

            #print sensorid, windspeed

            packcode = '6hLlll'
            header = "# MagPyBin %s %s %s %s %s %s %d" % (sensorid, '[t2,var1,var2]', '[Tv,V,Dir]', '[deg_C,m_s,deg]', '[10,10,1]', packcode, struct.calcsize(packcode))

            # Appending data to buffer which contains pcdate, pctime and sensordata
            # extract time data
            datearray = self.timeToArray(timestamp)

            try:
                datearray.append(int(virtualtemperature*10))
                datearray.append(int(windspeed*10))
                datearray.append(int(winddirection))
                data_bin = struct.pack(packcode,*datearray)
            except:
                log.msg('writeAnemometer: Error while packing binary data')
                pass

            # File Operations
            self.dataToFile(sensorid, filename, data_bin, header)

            try:
                evt3 = {'id': 3, 'value': outtime}
                evt1 = {'id': 1, 'value': timestamp}
                evt32 = {'id': 32, 'value': virtualtemperature}
                evt50 = {'id': 50, 'value': windspeed}
                evt51 = {'id': 51, 'value': winddirection}
                evt99 = {'id': 99, 'value': 'eol'}
            except:
                print "writeAnemometer: Problem assigning values to dict"

            try:
                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                self.wsMcuFactory.dispatch(dispatch_url, evt3)
                self.wsMcuFactory.dispatch(dispatch_url, evt32)
                self.wsMcuFactory.dispatch(dispatch_url, evt50)
                self.wsMcuFactory.dispatch(dispatch_url, evt51)
                self.wsMcuFactory.dispatch(dispatch_url, evt99)
                pass
            except:
                log.err('writeAnemometer: Unable to parse data at %s' % actualtime)
