#!/usr/bin/env python

from __future__ import print_function
from __future__ import absolute_import

# ###################################################################
# Import packages
# ###################################################################

import serial
import binascii
import csv
import sys, getopt
import time
import os
import re     # for interpretation of lines
import struct # for binary representation
import socket # for hostname identification
import string # for ascii selection
from datetime import datetime, timedelta
from twisted.python import log

SENSORELEMENTS =  ['sensorid','port','baudrate','bytesize','stopbits', 'parity','mode','init','rate','stack','protocol','name','serialnumber','revision','path','pierid','ptime','sensorgroup','sensordesc']


def connect(parameter):
    # Connecting
    print ("Welcome to serial-init")
    print ("------------------------------------------")
    print ("Establishing connection to serial port:")
    try:
        #print ("TEST", baudrate,parity,bytesize,stopbits,timeout)
        ser = serial.Serial(port, baudrate=baudrate, parity=parity, bytesize=bytesize, stopbits=stopbits, timeout=timeout)
        print('.... Connection made.')
    except: 
        print('.... Connection flopped.')
        print('--- Aborting ---')
        sys.exit(2)
    print('')

    return ser


def lineread(ser,eol=None,timelimit=30):
    """
    DESCRIPTION:
       Does the same as readline(), but does not require a standard 
       linebreak character ('\r' in hex) to know when a line ends.
       Variable 'eol' determines the end-of-line char: '\x00'
       for the POS-1 magnetometer, '\r' for the envir. sensor.
       (Note: required for POS-1 because readline() cannot detect
       a linebreak and reads a never-ending line.)
    PARAMETERS:
       eol:   (string) lineend character(s): can be any kind of lineend
                           if not provided, then standard eol's are used
    """
    if not eol:
        eollist = ['\r','\x00','\n']
    else:
        eollist = [eol]

    timeout = time.time()+timelimit
    ser_str = ''
    while True:
        char = ser.read()
        #if char == '\x00':
        if char in eollist:
            break
        if time.time() > timeout:
            break
        ser_str += char
    return ser_str


def hexify_command(command,eol):
    """
    DESCRIPTION:
       This function translates the command text string into a hex
       string that the serial device can read. 'eol' is the 
       end-of-line character. '\r' for the environmental sensor,
       '\x00' for the POS-1 magnetometer.
    """
    commandstr = []
    for character in command:
        hexch = binascii.hexlify(character)
        commandstr.append(('\\x' + hexch).decode('string_escape'))

    command_hex = ''.join(commandstr) + (eol)

    return command_hex

"""
def send_command(ser,command,eol):
    command_hex = hexify_command(command,eol)
    ser.write(command_hex)
    response = lineread(ser,eol)
    print('Response: ', response)
"""

def send_command(ser,command,eol=None,hexify=False,bits=0,report=True):
    """
    DESCRIPTION:
        General method to send commands to a e.g. serial port.
        Returns the response
    PARAMETER:
        bits: (int) provide the amount of bits to be read
        eol:  (string) end-of-line string
    Options:
        POS1:           hexify=True, line=True, eol= '\x00'
        ENV05:          hexify=True, line=True, eol='\r'
        GSM90Sv6:       hexify=False, line=False, eol=''
        GSM90Sv7:       hexify=False, line=False, eol
        GSM90Fv7:       hexify=False, line=False, eol
    """
    if report:
        print('-- Sending command:  ', command)
    if hexify:
        command = hexify_command(command,eol)
        ser.write(command)
    else:
        if not eol:
            ser.write(command)
        else:
            ser.write(command+eol)
    if bits==0:
        response = lineread(ser,eol)
    else:
        response = ser.read(bits)
    if report:
        print('-- Response: ', response)
    return response

def datetime2array(t):
    return [t.year,t.month,t.day,t.hour,t.minute,t.second,t.microsecond]

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
        datearray = list(map(int,datearray))
        return datearray
    except:
        print('Error while extracting time array')
        return []

def dataToFile(outputdir, sensorid, filedate, bindata, header):
    # File Operations
    try:
        #hostname = socket.gethostname()
        path = os.path.join(outputdir,sensorid)
        # outputdir defined in main options class
        if not os.path.exists(path):
            os.makedirs(path)
    except:
        print ("buffer {}: bufferdirectory could not be created - check permissions".format(sensorid))
    try:
        savefile = os.path.join(path, sensorid+'_'+filedate+".bin")
        if not os.path.isfile(savefile):
            with open(savefile, "wb") as myfile:
                myfile.write(header + "\n")
                myfile.write(bindata + "\n")
        else:
            with open(savefile, "a") as myfile:
                myfile.write(bindata + "\n")
    except:
        print("buffer {}: Error while saving file".format(sensorid))


def dataToCSV(outputdir, sensorid, filedate, asciidata, header):
    """
    Writing buffer data to an ASCII file
    """
    # File Operations
    try:
        path = os.path.join(outputdir,sensorid)
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
    except:
        print("buffer {}: Error while saving file".format(sensorid))


def AddSensor(path, dictionary, block=None):
    """
    DESCRIPTION:
        append sensor information to sensors.cfg
    PATH:
        sensors.conf
    """

    owheadline = "# OW block (automatically determined)"
    arduinoheadline = "# Arduino block (automatically determined)"
    mysqlheadline = "# SQL block (automatically determined)"
    owidentifier = '!'
    arduinoidentifier = '?'
    mysqlidentifier = '$'
    delimiter = '\n'

    def makeline(dictionary,delimiter):
        lst = []
        for el in SENSORELEMENTS:
            lst.append(str(dictionary.get(el,'-')))
        return ','.join(lst)+delimiter

    newline = makeline(dictionary,delimiter)

    # 1. if not block in ['OW','Arduino'] abort
    if not block in ['OW','ow','Ow','Arduino','arduino','ARDUINO','SQL','MySQL','mysql','MYSQL','sql']:
        print ("provided block needs to be 'OW', 'Arduino' or 'SQL'")
        return False 

    # 2. check whether sensors.cfg existis
    # abort if not
    # read all lines
    try:
        sensors = open(path,'r')
    except:
        print ("could not read sensors.cfg")
        return False
    sensordata = sensors.readlines()
    if not len(sensordata) > 0:
        print ("no data found in sensors.cfg")
        return False

    if block in ['OW','ow','Ow']:
        num = [line for line in sensordata if line.startswith(owheadline)]
        identifier = owidentifier
        headline = owheadline
    elif block in ['Arduino','arduino','ARDUINO']:
        num = [line for line in sensordata if line.startswith(arduinoheadline)]
        identifier = arduinoidentifier
        headline = arduinoheadline
    elif block in ['SQL','MySQL','mysql','MYSQL','sql']:
        num = [line for line in sensordata if line.startswith(mysqlheadline)]
        identifier = mysqlidentifier
        headline = mysqlheadline

    # 3. Append/Insert line 
    if len(num) > 0:
            cnt = [idx for idx,line in enumerate(sensordata) if line.startswith(identifier) or  line.startswith('#'+identifier)]
            lastline = max(cnt)
            if not (identifier+newline) in sensordata:
                if not ('#'+identifier+newline) in sensordata:
                    sensordata.insert(lastline+1,identifier+newline)
    else:
            sensordata.append(delimiter)
            sensordata.append(headline+delimiter)
            sensordata.append(identifier+newline)

    # 6. write all lines to sensors.cfg
    with open(path, 'w') as f:
        f.write(''.join(sensordata))

    return True

 
def GetSensors(path, identifier=None, secondidentifier=None):
    """
    DESCRIPTION:
        read sensor information from a file
        Now: just define them by hand
    PATH:
        sensors.conf
    CONTENT:
        # sensors.conf contains specific information for each attached sensor
        # ###################################################################
        # Information which need to be provided comprise:
        # sensorid: an unique identifier for the specfic sensor consiting of SensorName, 
        #           its serial number and a revision number (e.g. GSM90_12345_0001)
        # connection: e.g. the port to which the instument is connected (USB0, S1, ACM0, DB-mydb) 
        # serial specifications: baudrate, bytesize, etc
        # sensor mode: passive (sensor is broadcasting data by itself)
        #              active (sensor is sending data upon request)
        # initialization info: 
        #              None (passive sensor broadcasting data without initialization) 
        #              [parameter] (passive sensor with initial init e.g. GSM90,POS1)
        #              [parameter] (active sensor, specific call parameters and wait time)
        # protocol:
        #
        # optional sensor description:
        #
        # each line contains the following information
        # sensorid port baudrate bytesize stopbits parity mode init protocol sensor_description
        #e.g. ENV05_2_0001;USB0;9600;8;1;EVEN;passive;None;Env;wic;A2;'Environment sensor measuring temperature and humidity'

        ENV05_2_0001;USB0;9600;8;1;EVEN;passive;None;Env;'Environment sensor measuring temperature and humidity'
    RETURNS:
        a dictionary containing:
        'sensorid':'ENV05_2_0001', 'port':'USB0', 'baudrate':9600, 'bytesize':8, 'stopbits':1, 'parity':'EVEN', 'mode':'a', 'init':None, 'rate':10, 'protocol':'Env', 'name':'ENV05', 'serialnumber':'2', 'revision':'0001', 'path':'-', 'pierid':'A2', 'ptime':'NTP', 'sensordesc':'Environment sensor measuring temperature and humidity'
    
    """
    sensors = open(path,'r')
    sensordata = sensors.readlines()
    sensorlist = []
    sensordict = {}
    elements = SENSORELEMENTS

    # add identifier here
    # 

    for item in sensordata:
        sensordict = {}
        try:
            parts = item.split(',')
            if item.startswith('#'): 
                continue
            elif item.isspace():
                continue
            elif item.startswith('!') and not identifier: 
                continue
            elif item.startswith('?') and not identifier: 
                continue
            elif item.startswith('$') and not identifier: 
                continue
            elif not identifier and len(item) > 8:
                for idx,part in enumerate(parts):
                    sensordict[elements[idx]] = part
            elif item.startswith(str(identifier)) and len(item) > 8:
                if not secondidentifier:
                    for idx,part in enumerate(parts):
                        if idx == 0:
                            part = part.strip(str(identifier))
                        sensordict[elements[idx]] = part
                elif secondidentifier in parts:
                    for idx,part in enumerate(parts):
                        if idx == 0:
                            part = part.strip(str(identifier))
                        sensordict[elements[idx]] = part
                else:
                    pass
        except:
            # Possible issue - empty line
            pass
        if not sensordict == {}:
            sensorlist.append(sensordict)

    return sensorlist


def GetConf(path):
    """
    DESCRIPTION:
        read default configuration paths etc from a file
        Now: just define them by hand
    PATH:
        defaults are stored in magpymqtt.conf

        File looks like:
        # Configuration data for data transmission using MQTT (MagPy/MARTAS)
        # use # to uncomment
        # ##################################################################
        #
        # Working directory
        # -----------------
        # Please specify the path to the configuration files 
        configpath : /home/leon/CronScripts/MagPyAnalysis/MQTT

        # Definition of the bufferdirectory
        # ---------------------------------
        # Within this path, MagPy's write routine will store binary data files
        bufferdirectory : /srv/ws

        # Serial ports path
        # -----------------
        serialport : /dev/tty
        timeout : 60.0

        # MQTT definitions 
        # ----------------
        broker : localhost
        mqttport : 1883
        mqttdelay : 60

        # One wire configuration
        # ----------------------
        # ports: u for usb ---- NEW: owserver needs to be running - then just get port and address
        owport : usb
        owaddress : localhost

        # Logging
        # ----------------------
        # specify location to which logging information is send
        # e.g. sys.stdout , /home/cobs/logs/logmqtt.log
        logging : sys.stdout

    """
    # Init values:
    confdict = {}
    confdict['sensorsconf'] = '/home/leon/CronScripts/MagPyAnalysis/MQTT/sensors.cfg'
    #confdict['bufferdirectory'] = '/srv/ws'
    confdict['station'] = 'wic'
    confdict['bufferdirectory'] = '/srv/mqtt'
    confdict['serialport'] = '/dev/tty'
    confdict['timeout'] = 60.0
    confdict['broker'] = 'localhost'
    confdict['mqttport'] = 1883
    confdict['mqttdelay'] = 60
    confdict['logging'] = 'sys.stdout'
    confdict['owport'] = 4304
    confdict['owhost'] = 'localhost'

    try:
        config = open(path,'r')
        confs = config.readlines()

        for conf in confs:
            conflst = conf.split(':')
            if conf.startswith('#'): 
                continue
            elif conf.isspace():
                continue
            elif len(conflst) == 2:
                conflst = conf.split(':')
                key = conflst[0].strip()
                value = conflst[1].strip()
                confdict[key] = value
    except:
        print ("Problems when loading conf data from file. Using defaults")

    return confdict



