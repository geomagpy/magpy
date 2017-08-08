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
from magpy.stream import *

## Import MQTT
## -----------------------------------------------------------
import paho.mqtt.client as mqtt

global identifier
identifier = {}
streamdict = {}
stream = DataStream()

def analyse_meta(header,sensorid):
    """
    Interprete header information
    """
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
        print ("Different sensorids in publish address and header - please check - aborting")
        sys.exit()
    keylist = h_elem[3].strip('[').strip(']').split(',')
    elemlist = h_elem[4].strip('[').strip(']').split(',')
    unitlist = h_elem[5].strip('[').strip(']').split(',')
    multilist = list(map(float,h_elem[7].strip('[').strip(']').split(',')))
    print ("Packing code", packstr)
    print ("keylist", keylist)
    identifier[sensorid+':packingcode'] = packstr
    identifier[sensorid+':keylist'] = keylist
    identifier[sensorid+':elemlist'] = elemlist
    identifier[sensorid+':unitlist'] = unitlist
    identifier[sensorid+':multilist'] = multilist

def interprete_data(payload, ident, stream, sensorid):
    lines = payload.split(';') # for multiple lines send within one payload
    # allow for strings in payload !!
    array = [[] for elem in KEYLIST]
    keylist = identifier[sensorid+':keylist']
    multilist = identifier[sensorid+':multilist']
    for line in lines:
        data = line.split(',')
        timear = list(map(int,data[:7]))
        #print (timear)
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
    print("Connected with result code " + str(rc))
    # important obtain subscription from some config file or provide it directly (e.g. collector -a localhost -p 1883 -t mqtt -s wic)
    client.subscribe("wic/#")

def on_message(client, userdata, msg):
    sensorid = msg.topic.strip('wic').replace('/','').strip('meta').strip('data')
    print ("Receiving message for", sensorid)
    # define a new data stream for each non-existing sensor

    metacheck = identifier.get(sensorid+':packingcode','')
    #print ("Too be done: separate sensors ... and Data stream for each sensor")

    if msg.topic.endswith('meta') and metacheck == '':
        #print ("Found header:", str(msg.payload))
        analyse_meta(str(msg.payload),sensorid)
    elif msg.topic.endswith('data'):
        if not metacheck == '':
            stream.ndarray = interprete_data(msg.payload, identifier, stream, sensorid)
            streamdict[sensorid] = stream.ndarray  # to store data from different sensors
            post1 = KEYLIST.index('t1')
            posvar1 = KEYLIST.index('var1')
            #print(" - Meta info existing:", sensorid, num2date(stream.ndarray[0][0]),stream.ndarray[post1],stream.ndarray[posvar1])
            # create a magpy ndarray from payload (with lenght 1 if only one line is send)
            #coverage = 3
            #array = [ar[-coverage:] if len(ar) > coverage else ar for ar in array ]
            #print (array)
            # append data to a buffer array
            # ######################################
            # if writemode == 'file':
            #      if len(array) == solllength:
            #          write()
            # elif writemode == 'db':
            #      ...
            # elif writemoded == 'array':
            #      add to global array 
        else:
            print(msg.topic + " " + str(msg.payload))
