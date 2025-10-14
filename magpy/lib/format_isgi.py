#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MagPy
ISGI JSON library
- written to read data from the ISGI webservice
Written by Roman Leonhardt October 2025
- contains test and read function
"""
import sys
sys.path.insert(1, '/home/leon/Software/magpy/')  # should be magpy2
import json
import os, sys
from datetime import datetime, timedelta, timezone
import numpy as np
from magpy.stream import DataStream, read, magpyversion, join_streams, merge_streams, subtract_streams, loggerlib
from magpy.core.methods import test_timestring, testtime
import dateutil.parser as dparser
import subprocess


def isISGIJSON(filename):
    """
    Checks whether a file is of ISGI JSON format.
    """
    try:
        with open(filename, 'r') as jsonfile:
            dataset = json.load(jsonfile)
    except:
        return False
    try:
        if dataset.get("metadata", {}).get('resource', {}).get('name', '').find("ISGI") > -1:
            # Found ISGI json
            pass
        else:
            return False
    except:
        return False
    return True


def readISGIJSON(filename, headonly=False, **kwargs):
    """
    Reading ISGIJSON format data.
    """
    debug = kwargs.get('debug')
    header = {}
    array = [[] for key in DataStream().KEYLIST]

    if debug:
        print("Reading ISGI json")

    with open(filename, 'r') as jsonfile:
        dataset = json.load(jsonfile)
        loggerlib.info('Read: {}, Format: {} '.format(filename, "ISGIJSON"))

    # Extract header and data
    meta = dataset.get("metadata", {})
    timeseries = dataset.get("timeseries", {})

    #  Dealing with the header
    datameta = meta.get('data', {})
    resometa = meta.get('resource', {})

    # resource
    header['StationWebInfo'] = resometa.get('link', '')
    header['StationDescription'] = "".join(resometa.get('description', []))
    header['StationName'] = resometa.get('name', '')
    header['StationID'] = 'ISGI'
    # data meta
    header['SensorID'] = "{}_ISGI_0001".format(datameta.get('name', '').split()[0])
    header['DataID'] = "{}_ISGI_0001_0001".format(datameta.get('name', '').split()[0])
    header['SensorName'] = datameta.get('name', '')
    header['SensorDescription'] = "".join(datameta.get('description', []))

    # extracting the timeseries
    ts = timeseries.get('datapoints')
    variables = timeseries.get('fields')

    # extracting key assignment
    namekeys = {}
    j, k = 0, 0
    STRKEYLIST = [k for k in DataStream().KEYLIST if not k in DataStream().NUMKEYLIST and not k.find('time') > -1]
    for v in variables:
        datatype = v.get('datatype')
        name = v.get('name')
        key = None
        if not datatype == 'char':
            key = DataStream().NUMKEYLIST[j]
            unit = v.get('unit')
            colname = "col-{}".format(key)
            unitname = "unit-col-{}".format(key)
            header[colname] = name
            header[unitname] = unit
            j = j + 1
        elif not name == 'datetime' and len(STRKEYLIST) > k:
            key = STRKEYLIST[k]
            colname = "col-{}".format(key)
            header[colname] = name
            k = k + 1
        if key:
            namekeys[name] = key

    # assigning timeseries data to keys
    tsnames = {}
    for d in ts:
        for el in d:
            value = d.get(el)
            dat = tsnames.get(el, [])
            if el == 'datetime':
                value = (dparser.parse(value)).replace(tzinfo=None)
            dat.append(value)
            tsnames[el] = dat

    array[0] = np.asarray(tsnames.get('datetime'), dtype=object)
    stream = DataStream([], header, array)
    for el in namekeys:
        key = namekeys.get(el)
        array = np.asarray(tsnames.get(el), dtype=object)
        stream = stream._put_column(array, key)

    return stream


if __name__ == '__main__':
    print()
    print("----------------------------------------------------------")
    print("TESTING: ISGI JSON FORMAT LIBRARY")
    print("THIS IS A TEST RUN OF THE ISGI LIBRARY.")
    print("All main methods will be tested. This may take a while.")
    print("A summary will be presented at the end. Any protocols")
    print("or functions with errors will be listed.")
    print("----------------------------------------------------------")
    print()
    # Creating a test data set from the ISGI webservice
    jdata = {'metadata': {'data': {'description': ['am index is an IAGA-endorsed magnetic '
                                                   'activity index (IAGA Bulletin 27, '
                                                   'https://creativecommons.org/licenses/by-nc/4.0/'],
                                   'links': [{'content-role': 'type',
                                              'href': 'https://doi.org/10.25577/et43-6h78'}],
                                   'name': 'am time series'},
                          'resource': {'description': ['ISGI is the service of the '
                                                       'https://doi.org/10.17616/R3WS49'],
                                       'link': 'https://isgi.unistra.fr',
                                       'name': 'ISGI data repository'}},
             'timeseries': {'datapoints': [{'Am': 18,
                                            'An': 19,
                                            'As': 17,
                                            'Kpm': '3o',
                                            'am': 28,
                                            'an': 26,
                                            'as': 30,
                                            'datetime': '2015-08-01T00:00:00.000Z',
                                            'level': 'P'},
                                           {'Am': 18,
                                            'An': 19,
                                            'As': 17,
                                            'Kpm': '2+',
                                            'am': 18,
                                            'an': 16,
                                            'as': 19,
                                            'datetime': '2015-08-01T03:00:00.000Z',
                                            'level': 'P'},
                                           {'Am': 18,
                                            'An': 19,
                                            'As': 17,
                                            'Kpm': '1o',
                                            'am': 10,
                                            'an': 12,
                                            'as': 8,
                                            'datetime': '2015-08-01T06:00:00.000Z',
                                            'level': 'P'},
                                           {'Am': 18,
                                            'An': 19,
                                            'As': 17,
                                            'Kpm': '1+',
                                            'am': 12,
                                            'an': 16,
                                            'as': 8,
                                            'datetime': '2015-08-01T09:00:00.000Z',
                                            'level': 'P'},
                                           {'Am': 18,
                                            'An': 19,
                                            'As': 17,
                                            'Kpm': '1+',
                                            'am': 12,
                                            'an': 15,
                                            'as': 10,
                                            'datetime': '2015-08-01T12:00:00.000Z',
                                            'level': 'P'},
                                           {'Am': 18,
                                            'An': 19,
                                            'As': 17,
                                            'Kpm': '3-',
                                            'am': 27,
                                            'an': 25,
                                            'as': 29,
                                            'datetime': '2015-08-01T15:00:00.000Z',
                                            'level': 'P'}],
                            'fields': [{'datatype': 'char',
                                        'description': ['UTC date and time in ISO 8601'],
                                        'name': 'datetime',
                                        'unit': ''},
                                       {'datatype': 'int',
                                        'description': ['Amplitude of global geomagnetic '
                                                        'null.'],
                                        'name': 'am',
                                        'unit': 'nT'},
                                       {'datatype': 'char',
                                        'description': ['Quasi-logarithmic scale as a '
                                                        'null.'],
                                        'name': 'Kpm',
                                        'unit': 'quasi-logarithmic scale as a third of K '
                                                'units'},
                                       {'datatype': 'int',
                                        'description': ['Amplitude of northern hemisphere '
                                                        'null.'],
                                        'name': 'an',
                                        'unit': 'nT'},
                                       {'datatype': 'int',
                                        'description': ['Amplitude of southern hemisphere '
                                                        'null.'],
                                        'name': 'as',
                                        'unit': 'nT'},
                                       {'datatype': 'int',
                                        'description': ['Daily average of eight am values.',
                                                        'null.'],
                                        'name': 'Am',
                                        'unit': 'nT'},
                                       {'datatype': 'int',
                                        'description': ['Daily average of eight an values.',
                                                        'null.'],
                                        'name': 'An',
                                        'unit': 'nT'},
                                       {'datatype': 'int',
                                        'description': ['Daily average of eight as values.',
                                                        'null.'],
                                        'name': 'As',
                                        'unit': 'nT'},
                                       {'datatype': 'char',
                                        'description': ['Level of data completion:',
                                                        'ISGI Collaborating Institutes.'],
                                        'name': 'level',
                                        'unit': ''}]}}

    filename = '/tmp/MAGPYTESTFILE_isgi.json'
    with open(filename, 'w') as f:
        json.dump(jdata, f)
    errors = {}
    successes = {}
    testrun = 'MAGPYTESTFILE'
    t_start_test = datetime.now(timezone.utc).replace(tzinfo=None)

    while True:
        testset = 'COVJSON'
        try:
            ts = datetime.now(timezone.utc).replace(tzinfo=None)
            succ2 = isISGIJSON(filename)
            dat = readISGIJSON(filename)
            if not len(dat) > 0:
                raise Exception("Error - no data could be read")
            te = datetime.now(timezone.utc).replace(tzinfo=None)
            # validity tests
            tv = dat.mean('x')
            # agreement should be better than 0.01 nT as resolution is 0.1 nT in file
            if not 17 < np.abs(tv) < 18:
                raise Exception("ERROR within data validity test")
            successes[testset] = (
                "Version: {}, {}: {}".format(magpyversion, testset, (te - ts).total_seconds()))
        except Exception as excep:
            errors[testset] = str(excep)
            print(datetime.now(timezone.utc).replace(tzinfo=None), "--- ERROR in library {}.".format(testset))

        break

    t_end_test = datetime.now(timezone.utc).replace(tzinfo=None)
    time_taken = t_end_test - t_start_test
    print(datetime.now(timezone.utc).replace(tzinfo=None),
          "- Stream testing completed in {} s. Results below.".format(time_taken.total_seconds()))

    print()
    print("----------------------------------------------------------")
    del_test_files = 'rm {}*'.format(os.path.join('/tmp', testrun))
    subprocess.call(del_test_files, shell=True)
    for item in successes:
        print("{} :     {}".format(item, successes.get(item)))
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
