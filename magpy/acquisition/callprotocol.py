from __future__ import print_function
import sys, time, os, socket
import serial
import struct, binascii, re, csv
from datetime import datetime, timedelta
from matplotlib.dates import date2num, num2date
import numpy as np
from subprocess import check_call

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
            self.addressanemo = '01'
            self.commands = ['11TR00005',self.addressanemo+'TR00002']
            #eol = '\x00'
            self.eol = '\r'
            #print source
            self.errorcnt = 1
            print ("Initialization of callprotocl finished")

        def lineread(self, ser,eol):
            # FUNCTION 'LINEREAD'
            # Does the same as readline(), but does not require a standard 
            # linebreak character ('\r' in hex) to know when a line ends.
            # Variable 'eol' determines the end-of-line char: '\x00'
            # for the POS-1 magnetometer, '\r' for the envir. sensor.
            # (Note: required for POS-1 because readline() cannot detect    
            # a linebreak and reads a never-ending line.)
            ser_str = ''
            timeout = time.time()+15
            while True:
                char = ser.read()
                if char == eol:
                    break
                if time.time() > timeout:
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
            #print "Sending"
            ser.write(command)
            #print "Received something - interpretation"
            response = self.lineread(ser,eol)
            #print "interprete", response
            receivetime = date2num(datetime.utcnow())
            meantime = np.mean([receivetime,sendtime])
            #print "Timediff", (receivetime-sendtime)*3600*24
            return response, num2date(meantime).replace(tzinfo=None)

        def sendCommands(self):
            #print "Connecting ..."
            try:   
                ser = serial.Serial(self.port, baudrate=self.baudrate , parity='N', bytesize=8, stopbits=1, timeout=10)
                #print 'Connection made.'
            except:
                print('SerialCall: Connection flopped.')

            for item in self.commands:
                #print "sending command", item
                answer, actime = self.send_command(ser,item,self.eol,hex=False)
                success = self.analyzeResponse(answer, actime)
                time.sleep(2)
                #print "success", item
                if not success and self.errorcnt < 5:
                    self.errorcnt = self.errorcnt + 1
                    log.msg('SerialCall: Could not interpret response of system when sending %s' % item) 
                elif not success and self.errorcnt == 5:
                    try:
                        check_call(['/etc/init.d/martas', 'restart'])
                    except subprocess.CalledProcessError:
                        log.msg('SerialCall: check_call didnt work')
                        pass # handle errors in the called executable
                    except:
                        log.msg('SerialCall: check call problem')
                        pass # executable not found
                    #os.system("/etc/init.d/martas restart")
                    log.msg('SerialCall: Restarted martas process')

            ser.close()    


        def analyzeResponse(self,answer, actime):
            # A loading eventually existing sensor list
            if len(answer.split(';'))==525:  
                self.writeDisdro(answer, actime)
            elif len(answer.split()) == 4 and answer.split()[0].startswith('\x03'):
                self.writeAnemometer(answer,actime)
            else:
                if self.errorcnt < 5:
                    print("SerialCall: Could no analyze data", answer)
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
                cumulativerain = float(data[15]) 	# x
                if cumulativerain > 9000:
                    #send_command(reset)
                    pass
                visibility = int(data[16])		# y
                reflectivity = float(data[17])	 	# z
                intall = float(data[12]	)	 	# var1
                intfluid = float(data[13])	 	# var2
                intsolid = float(data[14])	 	# var3
                quality = int(data[18])
                haildiameter = float(data[19])		# var4
                insidetemp = float(data[36])	 	# t2
                lasertemp = float(data[37])
                lasercurrent = data[38]
                outsidetemp = float(data[44])		# t1
                Ptotal= int(data[49])			# f
                Pslow = int(data[51])		 	# dx
                Pfast= int(data[53])		 	# dy
                Psmall= int(data[55])		 	# dz
                synop= data[6]			 	# str1
                revision = '0001' # Software version 2.42
                sensorid = sensor + '_' + serialnum + '_' + revision
            except:
                log.err('SerialCall - writeDisdro: Could not assign data values')

            shortcut = sensorid[:3].lower()
            dispatch_url =  "http://example.com/"+self.hostname+"/"+shortcut+"#"+sensorid+"-value"
 
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
                self.dataToCSV(sensorid, filename, asciidata, [header])
            except:
                log.msg('SerialCall - writeDisdro: Error while saving ascii data')

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
                evt45 = {'id': 45, 'value': synop}
                evt0 = {'id': 0, 'value': self.hostname}
                evt99 = {'id': 99, 'value': 'eol'}
            except:
                print("SerialCall - writeDisdro: Problem assigning values to dict")

            try:
                self.wsMcuFactory.dispatch(dispatch_url, evt0)
                self.wsMcuFactory.dispatch(dispatch_url, evt1)
                self.wsMcuFactory.dispatch(dispatch_url, evt3)
                self.wsMcuFactory.dispatch(dispatch_url, evt30)
                self.wsMcuFactory.dispatch(dispatch_url, evt31)
                self.wsMcuFactory.dispatch(dispatch_url, evt36)
                self.wsMcuFactory.dispatch(dispatch_url, evt37)
                self.wsMcuFactory.dispatch(dispatch_url, evt39)
                self.wsMcuFactory.dispatch(dispatch_url, evt45)
                self.wsMcuFactory.dispatch(dispatch_url, evt99)
            except ValueError:
                log.err('SerialCall - writeDisdro: Unable to parse data at %s' % actualtime)
                

        def writeAnemometer(self, line, actime):

            # 1. Get serial number:
            sensor = 'ULTRASONICDSP'
            revision = '0001'
            address = self.addressanemo
            try:
                ser = serial.Serial(self.port, baudrate=self.baudrate , parity='N', bytesize=8, stopbits=1)
                answer, tmptime = self.send_command(ser,address+'SH',self.eol,hex=False)
                ser.close()
                serialnum = answer.replace('!'+address+'SH','').strip('\x03').strip('\x02')
            except:
                print('writeAnemometer: Failed to get Serial number.')

            
            sensorid = sensor + '_' + serialnum + '_' + revision
            shortcut = sensorid[:3].lower()
            dispatch_url =  "http://example.com/"+self.hostname+"/"+shortcut+"#"+sensorid+"-value"
 
            # 2. Getting data:
            filename = datetime.strftime(actime, "%Y-%m-%d")
            timestamp = datetime.strftime(actime, "%Y-%m-%d %H:%M:%S.%f")
            outtime = datetime.strftime(actime, "%H:%M:%S")

            try:
                data = line.split()
                # Extract data
                windspeed = float(data[0].strip('\x03').strip('\x02')) 	# var1
                winddirection = float(data[1])                          # var2
                virtualtemperature = float(data[2])                     # t2
            except: 
                windspeed = float('nan')                          # var1
                winddirection = float('nan')                      # var2
                virtualtemperature = float('nan')                 # t2
                print('writeAnemometer: Failed to interprete data.')

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
                # File Operations
                self.dataToFile(sensorid, filename, data_bin, header)
            except:
                log.msg('writeAnemometer: Error while packing and writing binary data')
                pass

            try:
                evt3 = {'id': 3, 'value': outtime}
                evt1 = {'id': 1, 'value': timestamp}
                evt32 = {'id': 32, 'value': virtualtemperature}
                evt50 = {'id': 50, 'value': windspeed}
                evt51 = {'id': 51, 'value': winddirection}
                evt99 = {'id': 99, 'value': 'eol'}
            except:
                print("writeAnemometer: Problem assigning values to dict")

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
