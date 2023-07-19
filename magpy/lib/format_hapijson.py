#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MagPy
Coverage JSON library
- particularly dedicated to webservice support
Written by Roman Leonhardt June 2019
- contains test, read and write function
"""
from __future__ import print_function
import json
import os, sys
from datetime import datetime

from matplotlib.dates import date2num, num2date
import numpy as np

from magpy.stream import KEYLIST, NUMKEYLIST, DataStream, loggerlib, testTimeString


"""
FORMAT looks like
    {
      "type" : "Coverage",
      "domain" : {
        "type": "Domain",
        "domainType": "PointSeries",
        "axes": {
          "x": { "values": [1] },   #north coordinate
          "y": { "values": [20] },  #east coordinate
          "z": { "values": [1] },   #altitude
          "t": { "values": ["2008-01-01T04:00:00Z","2008-01-01T05:00:00Z"] }
        },
        "referencing": [{}]
      },
      "parameters" : {
        "temperature": {...},
        "f": {...}
        "SST" : {
          "type" : "Parameter",
          "observedProperty" : {
            "id" : "http://vocab.nerc.ac.uk/standard_name/sea_surface_temperature/",
            "label" : {
              "en" : "Sea Surface Temperature",
              "de" : "Meeresoberflächentemperatur"
            },
            "description" : {
              "en" : "The temperature of sea water near the surface",
              "de" : "Die Temperatur des Meerwassers nahe der Oberfläche"
            }
          },
          "unit" : {
            "label" : {
              "en" : "Degree Celsius",
              "de" : "Grad Celsius"
            },
            "symbol": {
              "value" : "Cel",
              "type" : "http://www.opengis.net/def/uom/UCUM/"
            }
          }
        }
      },
      "ranges" : {
        "temperature" : {
          "type" : "NdArray",
          "dataType": "float",
          "axisNames": ["t"],
          "shape": [2],
          "values" : [...]
        },
        "f" : {
          "type" : "NdArray",
          "dataType": "float",
          "axisNames": ["t"],
          "shape": [2],
          "values" : [...]
        },
        "SST" : {
          "type" : "NdArray",
          "dataType": "float",
          "axisNames": ["t"],
          "shape": [2],
          "values" : [...]
        }
      }
    }
"""

def isCOVJSON(filename):
    """
    Checks whether a file is JSON format.
    """
    try:
        jsonfile = open(filename, 'r')
        j = json.load(jsonfile)
    except:
        return False
    try:
        if j.get("domain").get("type") == 'Domain':
            # Found Coverage json 
            pass
    except:
        return False

    try:
        if j.get("domain").get("domainType") in ['PointSeries']:
            pass
        else:
            print ("Currently only PointSeries domains are supported")
            return False
    except:
        return False

    return True


def readCOVJSON(filename, headonly=False, **kwargs):
    """
    Reading CoverageJSON format data.

    """
    header = {}
    array = [[] for key in KEYLIST]

    print ("Reading coverage json")

    with open(filename, 'r') as jsonfile:
        dataset = json.load(jsonfile)
        loggerlib.info('Read: {}, Format: {} '.format(filename, "CoverageJSON"))

    # Extract header and data
    axes = dataset.get("domain").get("axes")
    ranges = dataset.get("ranges")
    parameters = dataset.get("parameters")

    times = dataset.get("domain").get("axes").get("t").get("values")
    times = [testTimeString(el) for el in times]
    array[0] = date2num(times)
    stream = DataStream([],header,array)

    try:
        stream.header['DataAcquisitionLatitude'] = dataset.get("domain").get("axes").get("x").get("values")
        stream.header['DataAcquisitionLongitude'] = dataset.get("domain").get("axes").get("y").get("values")
        stream.header['DataElevation'] = dataset.get("domain").get("axes").get("z").get("values")
    except:
        pass

    print (dataset.get('context'))

    def addelement(datastream, key, element, elementdict, parameterdict):
        array = np.asarray(elementdict.get('values'))
        datastream = datastream._put_column(array,key)
        datastream.header['col-{}'.format(key)] = element
        datastream.header['unit-col-{}'.format(key)] = parameterdict.get("unit").get("label")

    numcnt = 0
    strcnt = 1
    AVAILKEYS = NUMKEYLIST
    ELEMENTSTODO = []
    fixedgroups = {'x' : ['x','X','H','I'], 'y' : ['y','Y','D','E'], 'z' : ['z','Z'], 'f' : ['f','F','S'], 'df' : ['g','G']}
    # Firstly assign data from fixed groups, then fill rest
    for element in ranges:
        print ("Dealing with {}".format(element))
        foundgroups = False
        for group in fixedgroups:
            if element in fixedgroups[group]:
                print (" -> adding to {}".format(group))
                addelement(stream, group, element, ranges[element], parameters[element])
                AVAILKEYS = ['USED' if x==group else x for x in AVAILKEYS]
                foundgroups = True
                break
        if not foundgroups:
            ELEMENTSTODO.append(element)

    # Now assign all other elements to appropriate keys
    for element in ELEMENTSTODO:
        print ("Now dealing with {}".format(element))
        # assign element to key
        if ranges.get(element).get('dataType') in ['float','double','int']:
            # get the first key which is not yet used
            index = min([idx for idx,el in enumerate(AVAILKEYS) if not el == 'USED'])
            key = AVAILKEYS[index]
            print (" -> adding to {}".format(key))
            addelement(stream, key, element, ranges[element], parameters[element])
            AVAILKEYS[index] = 'USED'
        else:
            if strcnt <= 4:
                key = "str{}".format(strcnt)
                print (" -> adding to {}".format(key))
                addelement(stream, key, element, ranges[element], parameters[element])
            strcnt += 1
    return stream


def writeCOVJSON(datastream, filename, **kwargs):
    """
    Producing CoverageJSON format data.
    """

    mode = kwargs.get('mode')
    headonly = kwargs.get('headonly')

    returnstring = False
    print ("Headonly", headonly)

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = mergeStreams(exst,datastream,extend=True)
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = mergeStreams(datastream,exst,extend=True)
        elif mode == 'append':
            exst = read(path_or_url=filename)
            datastream = combineStreams(datastream,exst,extend=True)
        else:
            pass
    elif filename.find('StringIO') > -1 and not os.path.exists(filename):
        returnstring = True
        if sys.version_info >= (3,0,0):
            from io import StringIO
        else:
            from StringIO import StringIO
    else:
        pass

    header = datastream.header

    # Convert basic datastream into a dictionary
    # ------------------------------------------ 
    red = {}  # red = resultsdict
    dod = {}  # dod = domaindict
    pad = {}  # pad = parameterdict
    rad = {}  # rad = rangedict
    imd = {}  # imd = intermagnet-meta-dict -> non-standard

    # domain
    # -------
    dod['type'] = "Domain"
    dod['domainType'] = "PointSeries"
    axes = {}
    # Firstly check DataID loacations, then check obscode StationLoc
    axes['x']  = { 'values' : header.get('DataAcquisitionLatitude') }
    axes['y']  = { 'values' : header.get('DataAcquisitionLongitude') }
    axes['z']  = { 'values' : header.get('DataElevation') }
    times = [datetime.strftime(el,"%Y-%m-%dT%H:%M:%SZ") for el in num2date(datastream._get_column('time'))]
    if not headonly:
        axes['t']  = { 'values' : times }
    dod['axes'] = axes
    reference = []
    try:
        ref1 = {}
        ref1['coordinates'] = ["x","y"]
        ref1['system'] = {"type" : header.get("DataLocationReference")}
        reference.append(ref1)
    except:
        pass
    try:
        ref2 = {}
        ref2['coordinates'] = ["z"]
        ref2['system'] = {"type" : header.get("DataElevationRef")}
        reference.append(ref2)
    except:
        pass
    try:
        ref3 = {}
        ref3['coordinates'] = ["t"]
        ref3['system'] = {"type" : "python datetime"}
        reference.append(ref3)
    except:
        pass
    dod['referencing'] = reference

    # parameters and ranges
    # -------
    for element in datastream._get_key_headers():
        # parameters
        # ---
        #print ("Element", element)
        component = datastream.GetKeyName(element)
        unitname = datastream.GetKeyUnit(element)
        # get parametername for each element
        #component = 'H' # based on element
        paradict = {}
        paradict['type'] = "Parameter"
        oP = {}
        oP['id'] = ''
        oP['label'] = ''
        oP['description'] = ''
        paradict['observedProperty'] = oP
        unit = {}
        unit['label'] = unitname
        unit['symbol'] = ''
        paradict['unit'] = unit
        pad[component] = paradict
        # ranges
        # ---
        values = datastream._get_column(element)
        rangedict = {}
        rangedict['type'] = 'NdArray'
        rangedict['dataType'] = 'float'
        rangedict['axisNames'] = [component]
        rangedict['shape'] = [datastream.length()[0]]
        if not headonly:
            rangedict['values'] = list(values)
        rad[component] = rangedict

    
    # INTERMAGNET 
    # -------
    imd['id'] = header.get("StationID") # StationID ( IAGA/Code
    imd['name'] = header.get("StationName") # Station Name
    imd['homepage'] = "Domain"

    #Construct a DataID if not existing
    if header.get("DataID","") == "":
        dataid = header.get("StationID") + "_" + header.get('DataPublicationLevel','variation') + "_0001_0001"
    else:
        dataid = header.get("DataID","")
    # subgroups of intermagnet dict
    observatory = {}
    IMobservatory = {}
    observatoryphotos = {}
    person = {}
    location = {'ObservatoryIAGAcode' : header.get('StationIAGAcode'),
                'LocationName' : header.get('StationName'),
                'Latitude' : header.get('StationLatitude'),
                'Longitude' : header.get('StationLongitude'),
                'Elevation' : header.get('StationElevation'),
                'CoordinateSystem' : header.get('StationLocationReference'),
               }
    institute = {'InstituteName' : header.get('StationInstitution'),
                'Abbreviation' : header.get('StationName'),
                'CountryCode' : header.get('StationCountry'),
                'URL' : header.get('StationWebInfo'),
                'Logo' : header.get('StationLongitude')
               }
    contact = {'City' : header.get('StationCity'),
                'Country' : header.get('StationCountry'),
                'Address' : header.get('StationStreet'),
                'Postcode' : header.get('StationPostalCode'),
                'Email' : header.get('StationEmail'),
                }
    dataset = {'id': dataid, 
                'Type' : header.get('DataType'),
                'PublicationState' : header.get('DataPublicationLevel'),
                'GeomagneticElements' : header.get('DataComponents'),
                'Terms' : header.get('DataTerms'),
                'Sampling' : header.get('DataDigitalSampling'),
                'Filter' : header.get('DataSamplingFilter'),
                'DataStandardLevel' : header.get('DataSamplingLevel'),
                'DataStandardName' : header.get('DataSamplingName'),
                'DataStandardVersion' : header.get('DataSamplingVersion'),
                'PublicationDate' : header.get('DataPublicationDate'),
                'References' : header.get('DataReferences'),
                'Notes' : header.get('DataComments'),
                'BaselineAdoptionDescription' : header.get('DataAbsInfo'),
                'AcquisitionOrientation' : header.get('DataSensorOrientation'),
                'Valid from' : header.get('DataMinTime'),
                'Valid to' : header.get('DataMaxTime')                
                }
    instrument = {}
    """
    for sensor in list of sensorIDs:
        sensdict = {'SerialNumber' : '',
                    'Type' : header.get('DataMinTime'),
                    'InstrumentName' : header.get('DataMaxTime'),
                    'Description' : header.get('DataMinTime'),
                    'DynamicRange' : header.get('DataMaxTime')                
                    }
        instrument[sensor] = sensdict
    """
    imd['dataset'] = dataset
    imd['location'] = location
    imd['contact'] = contact
    imd['institute'] = institute

    red['type'] = 'Coverage'
    red['domain'] = dod
    red['parameters'] = pad
    red['ranges'] = rad
    red['context'] = imd

    # convert 2 json
    if returnstring:
        out = StringIO()
        json.dump(red, out)
        return out
    else:
        with open(filename, 'w') as json_file:
            json.dump(red, json_file)
        return True
    return False

