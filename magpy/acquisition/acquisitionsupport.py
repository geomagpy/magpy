#!/usr/bin/env python

from __future__ import print_function
from __future__ import absolute_import

# ###################################################################
# Import packages
# ###################################################################

import serial
import binascii
import sys, getopt
import time
import os

# class EnvProtocol():
# interpretes  and starts to publish
# Methods:
# 1. Connect (Establish connection)
# 2. Init
# 3. ExtractData

def connect(parameter):
    # Connecting
    print ("Welcome to serial-init")
    print ("------------------------------------------")
    print ("Establishing connection to serial port:")
    try:
        print ("TEST", baudrate,parity,bytesize,stopbits,timeout)
        ser = serial.Serial(port, baudrate=baudrate, parity=parity, bytesize=bytesize, stopbits=stopbits, timeout=timeout)
        print('.... Connection made.')
    except: 
        print('.... Connection flopped.')
        print('--- Aborting ---')
        sys.exit(2)
    print('')

    return ser


def lineread(ser,eol=None):
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

    ser_str = ''
    while True:
        char = ser.read()
        #if char == '\x00':
        if char in eollist:
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


def send_command(ser,command,eol):
    command_hex = hexify_command(command,eol)
    ser.write(command_hex)
    response = lineread(ser,eol)
    print('Response: ', response)


def send_command(ser,command,eol=None,hexify=False,bits=0):
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
    print('-- Response: ', response)
    return response


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




