#!/usr/bin/env python
"""
MQTT collector routine of MARCOS: 
MQTT protocol to be used in the Conrad Observatory.
written by by Roman Leonhardt
 
How should it work:
PURPOSE:
collector_mqtt.py subscribes to published data from MQTT clients.

REQUIREMENTS:
1.) install a MQTT broker (e.g. ubuntu: sudo apt-get install mosquitto mosquitto-clients)
2.) Secure comm and authencation: https://www.digitalocean.com/community/tutorials/how-to-install-and-secure-the-mosquitto-mqtt-messaging-broker-on-ubuntu-16-04

METHODS:
collector_mqtt.py contains the following methods:

GetSensors: read a local definition file (sensors.txt) which contains information
            on SensorID, Port, Bausrate (better Serial communication details), active/passive, 
            init requirements, optional SensorDesc

GetDefaults: read initialization file with local paths, publishing server, ports, etc.

SendInit: send eventually necessary initialization data as defined in sensors.txt

GetActive: Continuously obtain serial data from instrument and convert it to an binary 
           information line to be published (using libraries)

GetPassive: Send scheduled request to serial port to obtain serial data from instrument 
            and convert it to an binary information line to be published (using libraries)

Usage:
python colector_mqtt.py -x -y -z

"""

from __future__ import print_function
from __future__ import absolute_import

# ###################################################################
# Import packages
# ###################################################################

## Import MagPy
## -----------------------------------------------------------
#from magpy.stream import *
from magpy.stream import DataStream, KEYLIST, NUMKEYLIST

## Import Auxiliary
## -----------------------------------------------------------
import threading
import struct
from datetime import datetime 
from matplotlib.dates import date2num, num2date
import numpy as np

## Import MQTT
## -----------------------------------------------------------
try:
    import paho.mqtt.client as mqtt
except:
    print ('Importing mqtt client package failed.')
    print ('Install paho-mqtt to use this functionality.')

global identifier
identifier = {}
streamdict = {}
stream = DataStream()
headdict = {} # store headerlines for file
headstream = {}


"""
def analyse_meta(header,sensorid):
    #
    #Interprete header information
    #
    header = header.decode('utf-8')
    
    # some cleaning actions for false header inputs
    header = header.replace(', ',',')
    header = header.replace('deg C','deg')
    h_elem = header.strip().split()
    packstr = '<'+h_elem[-2]+'B'
    packstr = packstr.encode('ascii','ignore')
    lengthcode = struct.calcsize(packstr)
    si = h_elem[2]
    if not si == sensorid:
        print ("Different sensorids in publish address and header - please check - !!!!!")
        print ("Header: {}, Used SensorID: {}".format(si,sensorid))
    keylist = h_elem[3].strip('[').strip(']').split(',')
    elemlist = h_elem[4].strip('[').strip(']').split(',')
    unitlist = h_elem[5].strip('[').strip(']').split(',')
    multilist = list(map(float,h_elem[6].strip('[').strip(']').split(',')))
    print ("Packing code", packstr)
    print ("keylist", keylist)
    identifier[sensorid+':packingcode'] = packstr
    identifier[sensorid+':keylist'] = keylist
    identifier[sensorid+':elemlist'] = elemlist
    identifier[sensorid+':unitlist'] = unitlist
    identifier[sensorid+':multilist'] = multilist
"""

def analyse_meta(header,sensorid, debug=False):
    """
    source:mqtt:
    Interprete header information
    """
    header = header.decode('utf-8')
    
    # some cleaning actions for false header inputs
    header = header.replace(', ',',')
    header = header.replace('deg C','deg')
    h_elem = header.strip().split()
    if not h_elem[-2].startswith('<'): # e.g. LEMI
        packstr = '<'+h_elem[-2]+'B'
    else:
        packstr = h_elem[-2]
    packstr = packstr.encode('ascii','ignore')
    lengthcode = struct.calcsize(packstr)
    si = h_elem[2]
    if not si == sensorid and debug:
        print ("Different sensorids in publish address and header - please check - !!!!!")
        print ("Header: {}, Used SensorID: {}".format(si,sensorid))
    keylist = h_elem[3].strip('[').strip(']').split(',')
    elemlist = h_elem[4].strip('[').strip(']').split(',')
    unitlist = h_elem[5].strip('[').strip(']').split(',')
    multilist = list(map(float,h_elem[6].strip('[').strip(']').split(',')))
    if debug:
        print ("Packing code: {}".format(packstr))
        print ("keylist: {}".format(keylist))
    identifier[sensorid+':packingcode'] = packstr
    identifier[sensorid+':keylist'] = keylist
    identifier[sensorid+':elemlist'] = elemlist
    identifier[sensorid+':unitlist'] = unitlist
    identifier[sensorid+':multilist'] = multilist

def create_head_dict(header,sensorid):
    """
    source:mqtt:
    Interprete header information
    """
    head_dict={}
    header = header.decode('utf-8')
    # some cleaning actions for false header inputs
    header = header.replace(', ',',')
    header = header.replace('deg C','deg')
    h_elem = header.strip().split()
    if not h_elem[-2].startswith('<'):
        packstr = '<'+h_elem[-2]+'B'
    else: # LEMI
        packstr = h_elem[-2]
    packstr = packstr.encode('ascii','ignore')
    lengthcode = struct.calcsize(packstr)
    si = h_elem[2]
    #if not si == sensorid:
    #    log.msg("Different sensorids in publish address {} and header {} - please check - aborting".format(si,sensorid))
    #    sys.exit()
    keylist = h_elem[3].strip('[').strip(']').split(',')
    elemlist = h_elem[4].strip('[').strip(']').split(',')
    unitlist = h_elem[5].strip('[').strip(']').split(',')
    multilist = list(map(float,h_elem[6].strip('[').strip(']').split(',')))
    #if debug:
    #    log.msg("Packing code: {}".format(packstr))
    #    log.msg("keylist: {}".format(keylist))
    head_dict['SensorID'] = sensorid
    sensl = sensorid.split('_')
    head_dict['SensorName'] = sensl[0]
    head_dict['SensorSerialNumber'] = sensl[1]
    head_dict['SensorRevision'] = sensl[2]
    head_dict['SensorKeys'] = ','.join(keylist)
    head_dict['SensorElements'] = ','.join(elemlist)
    #head_dict['StationID'] = stationid.upper()
    # possible additional data in header (because in sensor.cfg)
    #head_dict['DataPier'] = ...
    #head_dict['SensorModule'] = ...
    #head_dict['SensorGroup'] = ...
    #head_dict['SensorDescription'] = ...
    l1 = []
    l2 = []
    for idx,key in enumerate(KEYLIST):
        if key in keylist:
            l1.append(elemlist[keylist.index(key)])
            l2.append(unitlist[keylist.index(key)])
        else:
            l1.append('')
            l2.append('')
    head_dict['ColumnContents'] = ','.join(l1[1:])
    head_dict['ColumnUnits'] = ','.join(l2[1:])
    return head_dict


def interprete_data(payload, ident, stream, sensorid):
    """
    source:mqtt:
    """
    lines = payload.split(';') # for multiple lines send within one payload
    # allow for strings in payload !!
    array = [[] for elem in KEYLIST]
    keylist = identifier[sensorid+':keylist']
    multilist = identifier[sensorid+':multilist']
    for line in lines:
        data = line.split(',')
        timear = list(map(int,data[:7]))
        #log.msg(timear)
        time = datetime(timear[0],timear[1],timear[2],timear[3],timear[4],timear[5],timear[6])
        array[0].append(date2num(time))
        for idx, elem in enumerate(keylist):
            index = KEYLIST.index(elem)
            if not elem.endswith('time'):
                if elem in NUMKEYLIST:
                    array[index].append(float(data[idx+7])/float(multilist[idx]))
                else:
                    array[index].append(data[idx+7])

    return np.asarray([np.asarray(elem) for elem in array])

def on_connect(client, userdata, flags, rc):
    #print("Connected with result code " + str(rc))
    #print("Setting QOS (Quality of Service): {}".format(qos))
    if str(rc) == '0':
        print ("Connection successful - continue")
        client.connected_flag = True
    elif str(rc) == '5':
        print ("Broker eventually requires authentication - use options -u and -P")
    # important obtain subscription from some config file or provide it directly (e.g. collector -a localhost -p 1883 -t mqtt -s wic)
    #substring = stationid+'/#'
    #client.subscribe(substring,qos=qos)


"""
def on_message(client, userdata, msg):
    #print ("Topic", msg.topic.split('/'))
    sensorid = msg.topic.split('/')[1].strip('meta').strip('data')
    #print ("Receiving message for", sensorid)
    # define a new data stream for each non-existing sensor
    #print (msg.payload)

    metacheck = identifier.get(sensorid+':packingcode','')
    #print ("Too be done: separate sensors ... and Data stream for each sensor")

    if msg.topic.endswith('meta') and metacheck == '':
        #print ("Found header:", str(msg.payload))
        analyse_meta(str(msg.payload),sensorid)
    elif msg.topic.endswith('data'):
        if not metacheck == '':
            stream.ndarray = interprete_data(msg.payload, identifier, stream, sensorid)
            streamdict[sensorid] = stream.ndarray  # to store data from different sensors
            #post1 = KEYLIST.index('t1')
            #posvar1 = KEYLIST.index('var1')
        else:
            print(msg.topic + " " + str(msg.payload))
"""

def on_message(client, userdata, msg):

    #print("message received " ,str(msg.payload.decode("utf-8")))
    #print("message topic=",msg.topic)
    #print("message qos=",msg.qos)
    #print("message retain flag=",msg.retain)

    arrayinterpreted = False
    sensorid = msg.topic.split('/')[1].strip('meta').strip('data').strip('dict')
    #sensorid = msg.topic.strip(stationid).replace('/','').strip('meta').strip('data').strip('dict')
    # define a new data stream for each non-existing sensor
    metacheck = identifier.get(sensorid+':packingcode','')

    if msg.topic.endswith('meta') and metacheck == '':
        analyse_meta(str(msg.payload),sensorid)
        if not sensorid in headdict:
            headdict[sensorid] = msg.payload
            # create stream.header dictionary and it here
            headstream[sensorid] = create_head_dict(str(msg.payload),sensorid)
            #if debug:
            #    log.msg("New headdict: {}".format(headdict))
    elif msg.topic.endswith('dict') and sensorid in headdict:
        #log.msg("Found Dictionary:{}".format(str(msg.payload)))
        head_dict = headstream[sensorid]
        for elem in str(msg.payload).split(','):
            keyvaluespair = elem.split(':')
            try:
                if not keyvaluespair[1] in ['-','-\n','-\r\n']:
                    head_dict[keyvaluespair[0]] = keyvaluespair[1].strip()
            except:
                pass
        #if debug:
        #    log.msg("Dictionary now looks like {}".format(headstream[sensorid]))
    elif msg.topic.endswith('data'):
        if not metacheck == '':
            stream.ndarray = interprete_data(msg.payload, identifier, stream, sensorid)
            streamdict[sensorid] = stream.ndarray  # to store data from different sensors
            #post1 = KEYLIST.index('t1')
            #posvar1 = KEYLIST.index('var1')
        else:
            print(msg.topic + " " + str(msg.payload))

    #print ("on_message finished")
