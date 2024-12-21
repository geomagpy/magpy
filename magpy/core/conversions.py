# -*- coding: utf-8 -*-

"""
conversions contain all methods to quickly transform magpy stream to other structures

the following methods are contained:
- obspy2magpy  :  converts between traces structure and
- pandas2magpy
- magpy2 pandas

|class | method | since version | until version | runtime test | result verification | manual | *tested by |
|----- | ------ | ------------- | ------------- | ------------ | ------------------- | ------ | ---------- |
|**core.conversions** |  |        |             |              |  |  | |
|    | obspytomagpy      |  2.0.0 |             |              |  |  | |
|    | pandas2magpy      |  2.0.0 |             |              |  |  | |

"""

from magpy.stream import DataStream
import numpy as np
from datetime import timedelta

KEYLIST = DataStream().KEYLIST


def obspy2magpy(opstream, keydict=None):
    """
    Function for converting obspy streams to magpy streams.

    INPUT:
        - opstream          obspy.core.stream.Stream object
                            Obspy stream
        - keydict           dict
                            ID of obspy traces to assign to magpy keys

    OUTPUT:
        - mpstream          Stream in magpy format

    EXAMPLE:
        mpst = obspy2magpy(opst, keydict={'nn.e6046.11.p0': 'x', 'nn.e6046.11.p1': 'y'})
    """
    array = [[] for key in KEYLIST]
    mpstream = DataStream()
    if not keydict:
        keydict = {}

    # Split into channels:
    datadict = {}
    for tr in opstream.traces:
        try:
            datadict[tr.id].append(tr)
        except:
            datadict[tr.id] = [tr]

    twrite = False
    tind = KEYLIST.index("time")
    fillkeys = ['var1', 'var2', 'var3', 'var4', 'var5', 'x', 'y', 'z', 'f']
    key = 'var1'
    for channel in datadict:
        data = datadict[channel]
        # Sort by time:
        data.sort(key=lambda x: x.stats.starttime)

        # Assign magpy keys:
        if channel in keydict:
            ind = KEYLIST.index(keydict[channel])
        else:
            try:
                key = fillkeys.pop(0)
                print("Writing {i} data to key {k}.".format(i=channel, k=key))
            except IndexError:
                print("CAUTION! Out of available keys for data. {} will not be contained in stream.".format(key))
            ind = KEYLIST.index(key)
        mpstream.header['col-'+key] = channel
        mpstream.header['unit-col-'+key] = ''

        # Arrange in preparatory array:
        for d in data:
            if not twrite:  # Only write time array once (for multiple channels):
                _diff = d.stats.endtime.datetime - d.stats.starttime.datetime
                # Work time in milliseconds:
                diff = _diff.days*24.*60.*60.*1000. + _diff.seconds*1000. + _diff.microseconds/1000.
                numval = int(diff/1000. * d.stats.sampling_rate) + 1
                array[tind] += [d.stats.starttime.datetime + timedelta(milliseconds=x/d.stats.sampling_rate*1000.)
                                for x in range(0, numval)]
            else:  # Check anyway
                if d.stats.starttime.datetime not in array[tind]:
                    raise Exception("Time arrays do not match!")  # could be handled
            array[ind] += list(d.data)
        twrite = True

    # Convert to ndarrays:
    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])

    mpstream = DataStream(header=mpstream.header, ndarray=np.asarray(array, dtype=object))

    return mpstream

def pandas2magpy(dataframe,sensorid="RADONWBV_9500_0001", columns=[], units=[], dateformat=None, debug=True):
    """
    DESCRIPTION
        Converts pandas dataframe into a magpy datastream structure
    """
    import pandas as pd
    array = dataframe.to_numpy()
    if not columns:
        columns = list(dataframe.columns)
    st = DataStream()
    st.header = {"SensorID":sensorid}
    ar = [[] for el in KEYLIST]
    for idx,el in enumerate(array.T):
        #print (el)
        if KEYLIST[idx].find('time') > -1:
            if not dateformat:
                #print (el)
                el = pd.to_datetime(el)
            else:
                el = pd.to_datetime(el, format=dateformat, errors='coerce')
        col = 'col-{}'.format(KEYLIST[idx])
        uni = 'unit-col-{}'.format(KEYLIST[idx])
        #print (el)
        if columns and idx < len(columns) and columns[idx]:
            st.header[col] = columns[idx]
        if units and idx < len(units) and units[idx]:
            st.header[uni] = units[idx]
        ar[idx] = el
    st.ndarray = np.asarray(ar,dtype=object)
    #st = drop_nans(st,'time')
    return st
