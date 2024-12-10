#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MagPy
Coverage JSON library
- particularly dedicated to webservice support
Written by Roman Leonhardt June 2019
- contains test, read and write function
"""
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
import json
import os, sys
from datetime import datetime, timedelta, timezone
import numpy as np
from magpy.stream import DataStream, read, magpyversion, join_streams, merge_streams, subtract_streams, loggerlib
from magpy.core.methods import test_timestring, testtime
import dateutil.parser as dparser



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
        with open(filename, 'r') as jsonfile:
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
    debug = kwargs.get('debug')
    header = {}
    array = [[] for key in DataStream().KEYLIST]

    if debug:
        print ("Reading coverage json")

    with open(filename, 'r') as jsonfile:
        dataset = json.load(jsonfile)
        loggerlib.info('Read: {}, Format: {} '.format(filename, "CoverageJSON"))

    # Extract header and data
    axes = dataset.get("domain").get("axes")
    ranges = dataset.get("ranges")
    parameters = dataset.get("parameters")

    times = dataset.get("domain").get("axes").get("t").get("values")
    times = [test_timestring(el) for el in times]
    array[0] = times
    stream = DataStream([],header,array)

    try:
        stream.header['DataAcquisitionLatitude'] = dataset.get("domain").get("axes").get("x").get("values")
        stream.header['DataAcquisitionLongitude'] = dataset.get("domain").get("axes").get("y").get("values")
        stream.header['DataElevation'] = dataset.get("domain").get("axes").get("z").get("values")
    except:
        pass

    if debug:
        print (dataset.get('context'))

    def addelement(datastream, key, element, elementdict, parameterdict):
        array = np.asarray(elementdict.get('values'))
        datastream = datastream._put_column(array,key)
        datastream.header['col-{}'.format(key)] = element
        datastream.header['unit-col-{}'.format(key)] = parameterdict.get("unit").get("label")

    numcnt = 0
    strcnt = 1
    AVAILKEYS = DataStream().NUMKEYLIST
    ELEMENTSTODO = []
    fixedgroups = {'x' : ['x','X','H','I'], 'y' : ['y','Y','D','E'], 'z' : ['z','Z'], 'f' : ['f','F','S'], 'df' : ['g','G']}
    # Firstly assign data from fixed groups, then fill rest
    for element in ranges:
        if debug:
            print ("Dealing with {}".format(element))
        foundgroups = False
        for group in fixedgroups:
            if element in fixedgroups[group]:
                if debug:
                    print (" -> adding to {}".format(group))
                addelement(stream, group, element, ranges[element], parameters[element])
                AVAILKEYS = ['USED' if x==group else x for x in AVAILKEYS]
                foundgroups = True
                break
        if not foundgroups:
            ELEMENTSTODO.append(element)

    # Now assign all other elements to appropriate keys
    for element in ELEMENTSTODO:
        if debug:
            print ("Now dealing with {}".format(element))
        # assign element to key
        if ranges.get(element).get('dataType') in ['float','double','int']:
            # get the first key which is not yet used
            index = min([idx for idx,el in enumerate(AVAILKEYS) if not el == 'USED'])
            key = AVAILKEYS[index]
            if debug:
                print (" -> adding to {}".format(key))
            addelement(stream, key, element, ranges[element], parameters[element])
            AVAILKEYS[index] = 'USED'
        else:
            if strcnt <= 4:
                key = "str{}".format(strcnt)
                if debug:
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

    if os.path.isfile(filename):
        if mode == 'skip': # skip existing inputs
            exst = read(path_or_url=filename)
            datastream = join_streams(exst,datastream,extend=True)
        elif mode == 'replace': # replace existing inputs
            exst = read(path_or_url=filename)
            datastream = merge_streams(datastream,exst,extend=True)
        elif mode == 'append':
            exst = read(path_or_url=filename)
            datastream = join_streams(datastream,exst,extend=True)
        else:
            pass
    elif filename.find('StringIO') > -1 and not os.path.exists(filename):
        returnstring = True
        from io import StringIO
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
    times = [datetime.strftime(el,"%Y-%m-%dT%H:%M:%SZ") for el in datastream._get_column('time')]
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
        if not element.find('time') >= 0:  # no support for secondary times
            component = datastream.get_key_name(element)
            unitname = datastream.get_key_unit(element)
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
        dataid = header.get("StationID","NoCode") + "_" + header.get('DataPublicationLevel','variation') + "_0001_0001"
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


if __name__ == '__main__':

    from scipy import signal
    import subprocess
    print()
    print("----------------------------------------------------------")
    print("TESTING: WDC FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE WDC LIBRARY.")
    print("All main methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()
    # 1. Creating a test data set of minute resolution and 1 month length
    #    This testdata set will then be transformed into appropriate output formats
    #    and written to a temporary folder by the respective methods. Afterwards it is
    #    reloaded and compared to the original data set
    def create_minteststream(startdate=datetime(2022, 11, 1), addnan=True):
        c = 1000  # 4000 nan values are filled at random places to get some significant data gaps
        l = 32 * 1440
        #import scipy
        teststream = DataStream()
        array = [[] for el in DataStream().KEYLIST]
        win = signal.windows.hann(60)
        a = np.random.uniform(20950, 21000, size=int(l / 2))
        b = np.random.uniform(20950, 21050, size=int(l / 2))
        x = signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        if addnan:
            x.ravel()[np.random.choice(x.size, c, replace=False)] = np.nan
        array[1] = x[1440:-1440]
        a = np.random.uniform(1950, 2000, size=int(l / 2))
        b = np.random.uniform(1900, 2050, size=int(l / 2))
        y = signal.convolve(np.concatenate([a, b], axis=0), win, mode='same') / sum(win)
        if addnan:
            y.ravel()[np.random.choice(y.size, c, replace=False)] = np.nan
        array[2] = y[1440:-1440]
        a = np.random.uniform(44300, 44400, size=l)
        z = signal.convolve(a, win, mode='same') / sum(win)
        array[3] = z[1440:-1440]
        array[4] = np.sqrt((x * x) + (y * y) + (z * z))[1440:-1440]
        array[0] = np.asarray([startdate + timedelta(minutes=i) for i in range(0, len(array[1]))])
        teststream = DataStream(header={'SensorID': 'Test_0001_0001'}, ndarray=np.asarray(array, dtype=object))
        minstream = teststream.filter()
        teststream.header['col-x'] = 'X'
        teststream.header['col-y'] = 'Y'
        teststream.header['col-z'] = 'Z'
        teststream.header['col-f'] = 'F'
        teststream.header['unit-col-x'] = 'nT'
        teststream.header['unit-col-y'] = 'nT'
        teststream.header['unit-col-z'] = 'nT'
        teststream.header['unit-col-f'] = 'nT'
        teststream.header['StationID'] = 'XXX'
        teststream.header['StationIAGAcode'] = 'XXX'
        return teststream

    teststream = create_minteststream(addnan=False)
    teststream = teststream.trim('2022-11-22','2022-11-23')

    errors = {}
    successes = {}
    testrun = 'MAGPYTESTFILE'
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)

    while True:
        testset = 'COVJSON'
        try:
            filename = os.path.join('/tmp','{}_{}.json'.format(testrun, datetime.strftime(teststream.start(),'%y%m%d')))
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            succ1 = writeCOVJSON(teststream, filename)
            succ2 = isCOVJSON(filename)
            dat = readCOVJSON(filename)
            if not len(dat) > 0:
                raise Exception("Error - no data could be read")
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            diff = subtract_streams(teststream, dat, debug=True)
            xm = diff.mean('x')
            ym = diff.mean('y')
            zm = diff.mean('z')
            fm = diff.mean('f')
            # agreement should be better than 0.01 nT as resolution is 0.1 nT in file
            if np.abs(xm) > 0.01 or np.abs(ym) > 0.01 or np.abs(zm) > 0.01 or np.abs(fm) > 0.01:
                 raise Exception("ERROR within data validity test")
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))

        break

    t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
    time_taken = t_end_test - t_start_test
    print(datetime.now(timezone.utc).replace(tzinfo=None), "- Stream testing completed in {} s. Results below.".format(time_taken.total_seconds()))

    print()
    print("----------------------------------------------------------")
    del_test_files = 'rm {}*'.format(os.path.join('/tmp',testrun))
    subprocess.call(del_test_files,shell=True)
    for item in successes:
        print ("{} :     {}".format(item, successes.get(item)))
    if errors == {}:
        print("0 errors! Great! :)")
    else:
        print(len(errors), "errors were found in the following functions:")
        print(" {}".format(errors.keys()))
        print()
        for item in errors:
                print(item + " error string:")
                print("    " + errors.get(item))
    print()
    print("Good-bye!")
    print("----------------------------------------------------------")
