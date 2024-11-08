"""
GFZ CDF input filter
Rewritten by Roman Leonhardt November 2024
- contains test and read methods
- based on cdflib
- supports python >= 3.5

"""
# Specify what methods are really needed
#from magpy.stream import *
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2

from magpy.stream import DataStream, read, join_streams, subtract_streams,magpyversion
from magpy.core.methods import testtime, extract_date_from_string
from datetime import datetime, timedelta
import numpy as np
import os
import cdflib
import pickle
# for export of objects:
import codecs
import logging
logger = logging.getLogger(__name__)

KEYLIST = DataStream().KEYLIST
NUMKEYLIST = DataStream().NUMKEYLIST

def isGFZCDF(filename):
    """
    Checks whether a file is Nasa CDF format.
    """
    try:
        temp = cdflib.CDF(filename)
    except:
        return False
    try:
        cdfformat = temp.globalattsget().get('DataFormat')
        print ("Found", cdfformat)
    except:
        pass
    try:
        try:
            variables = temp.cdf_info().get('zVariables')  # cdflib < 1.0.0
        except:
            variables = temp.cdf_info().zVariables  # cdflib >= 1.0.0
        if not 'Epoch' in variables:
            if not 'time' in variables:
                return False
    except:
        return False

    logger.debug("format_magpy: Found GFZCDF file %s" % filename)
    return True


def readGFZCDF(filename, headonly=False, **kwargs):
    """
    Reading CDF format data - DTU type.
    """
    #stream = DataStream()
    #stream = DataStream([],{})
    stream = DataStream([],{},np.asarray([[] for key in KEYLIST]))

    array = [[] for key in KEYLIST]
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    oldtype = kwargs.get('oldtype')
    getfile = True

    #oldtype=True
    # Some identification parameters used by Juergs
    jind = ["H0", "D0", "Z0", "F0", "HNscv", "HEscv", "Zscv", "Basetrig", "time", \
"HNvar", "HEvar", "Zvar", "T1", "T2", "timeppm", "timegps", \
"timefge", "Fsc", "HNflag", "HEflag", "Zflag", "Fscflag", "FscQP", \
"T1flag", "T2flag", "Timeerr", "Timeerrtrig"]

    # Check whether header information is already present
    headskip = False
    if stream.header == None:
        stream.header.clear()
    else:
        headskip = True

    theday = extract_date_from_string(filename)
    try:
        if starttime:
            if not theday[-1] >= datetime.date(testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(testtime(endtime)):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True
    logbaddata = False

    # Get format type:
    # Juergens DTU type is using different date format (MATLAB specific)
    # MagPy type is using datetime objects
    if getfile:
        try:
            cdf_file = cdflib.CDF(filename)
        except:
            return stream
        try:
            cdfformat = cdf_file.attrs['DataFormat']
        except:
            logger.info("readPYCDF: No format specification in CDF - passing")
            cdfformat = 'Unknown'
            pass
        OMNIACE = False
        try:
            title = str(cdf_file.attrs['TITLE'])
            if 'ACE' in title:
                OMNIACE = True
        except:
            pass

        if headskip:
            for key in cdf_file.attrs:
                if not key in ['DataAbsFunctionObject','DataBaseValues', 'DataFlagList']:
                    stream.header[key] = str(cdf_file.attrs[key])
                else:
                    logger.debug("readPYCDF: Found object - loading and unpickling")
                    try:
                        func = pickle.loads(str(cdf_file.attrs[key]))
                        stream.header[key] = func
                    except:
                        logger.debug("readPYCDF: Failed to load Object - constructed before v0.2.000?")

        #if headonly:
        #    cdf_file.close()
        #    return DataStream(stream, stream.header)

        logger.info('readPYCDF: %s Format: %s ' % (filename, cdfformat))

        for key in cdf_file:
            #print key
            #try:
            #    print key, cdf_file[key].attrs['LABLAXIS'], cdf_file[key].attrs['UNITS']
            #except:
            #    print key
            # first get time or epoch column
            #lst = cdf_file[key]
            #print key
            if key.find('time')>=0 or key == 'Epoch':
                #ti = cdf_file[key][...]
                #row = LineStruct()
                if str(cdfformat) == 'MagPyCDF':
                    if not oldtype:
                        if not key == 'sectime':
                            ind = KEYLIST.index('time')
                        else:
                            ind = KEYLIST.index('sectime')
                        try:
                            array[ind] = np.asarray(date2num(cdf_file[key][...]))
                        except:
                            array[ind] = np.asarray([])
                            pass ### catches exceptions if sectime is nan
                    else:
                        #ti = [date2num(elem) for elem in ti]
                        #stream._put_column(ti,'time')
                        for elem in cdf_file[key][...]:
                            row = LineStruct()
                            row.time = date2num(elem)
                            stream.add(row)
                            del row
                else:
                    if not oldtype:
                        if key  == 'time':
                            ind = KEYLIST.index(key)
                            array[ind] = np.asarray(cdf_file[key][...]) + 730120.
                        elif key == 'Epoch':
                            ind = KEYLIST.index('time')
                            array[ind] = np.asarray(date2num(cdf_file[key][...]))
                    else:
                        for elem in cdf_file[key][...]:
                            row = LineStruct()
                            # correcting matlab day (relative to 1.1.2000) to python day (1.1.1)
                            if type(elem) in [float,np.float64]:
                                row.time = 730120. + elem
                            else:
                                row.time = date2num(elem)
                            stream.add(row)
                            del row
                #del ti
            elif key == 'HNvar' or key == 'x':
                x = cdf_file[key][...]
                if len(x) == 1:
                    # This is the case if identical data is found
                    try:
                        length = len(cdf_file['Epoch'][...])
                    except:
                        length = len(cdf_file['time'][...])
                    x = [x[0]] * length
                if len(x) > 0:
                    if not oldtype:
                        ind = KEYLIST.index('x')
                        array[ind] = np.asarray(x)
                    else:
                        stream._put_column(x,'x')
                    del x
                    #if not headskip:
                    stream.header['col-x'] = 'x'
                    try:
                        stream.header['col-x'] = cdf_file['x'].attrs['name']
                    except:
                        pass
                    try:
                        stream.header['unit-col-x'] = cdf_file['x'].attrs['units']
                    except:
                        # Apply default unit:
                        stream.header['unit-col-x'] = 'nT'
                        pass
            elif key == 'HEvar' or key == 'y':
                y = cdf_file[key][...]
                if len(y) == 1:
                    # This is the case if identical data is found
                    try:
                        length = len(cdf_file['Epoch'][...])
                    except:
                        length = len(cdf_file['time'][...])
                    y = [y[0]] * length
                if len(y) > 0:
                    if not oldtype:
                        ind = KEYLIST.index('y')
                        array[ind] = np.asarray(y)
                    else:
                        stream._put_column(y,'y')
                    del y
                    try:
                        stream.header['col-y'] = cdf_file['y'].attrs['name']
                    except:
                        stream.header['col-y'] = 'y'
                    try:
                        stream.header['unit-col-y'] = cdf_file['y'].attrs['units']
                    except:
                        # Apply default unit:
                        stream.header['unit-col-y'] = 'nT'
                        pass
            elif key == 'Zvar' or key == 'z':
                z = cdf_file[key][...]
                if len(z) == 1:
                    # This is the case if identical data is found
                    try:
                        length = len(cdf_file['Epoch'][...])
                    except:
                        length = len(cdf_file['time'][...])
                    z = [z[0]] * length
                if len(z) > 0:
                    if not oldtype:
                        ind = KEYLIST.index('z')
                        array[ind] = np.asarray(z)
                    else:
                        stream._put_column(z,'z')
                    del z
                    try:
                        stream.header['col-z'] = cdf_file['z'].attrs['name']
                    except:
                        stream.header['col-z'] = 'z'
                    try:
                        stream.header['unit-col-z'] = cdf_file['z'].attrs['units']
                    except:
                        # Apply default unit:
                        stream.header['unit-col-z'] = 'nT'
                        pass
            elif key == 'Fsc' or key == 'f':
                f = cdf_file[key][...]
                if len(f) == 1:
                    # This is the case if identical data is found
                    try:
                        length = len(cdf_file['Epoch'][...])
                    except:
                        length = len(cdf_file['time'][...])
                    f = [f[0]] * length
                if len(f) > 0:
                    if not oldtype:
                        ind = KEYLIST.index('f')
                        array[ind] = np.asarray(f)
                    else:
                        stream._put_column(f,'f')
                    del f
                    try:
                        stream.header['col-f'] = cdf_file['f'].attrs['name']
                    except:
                        stream.header['col-f'] = 'f'
                    try:
                        stream.header['unit-col-f'] = cdf_file['f'].attrs['units']
                    except:
                        # Apply default unit:
                        stream.header['unit-col-f'] = 'nT'
                        pass
            elif key.endswith('scv'): # solely found in juergs files - now define magpy header info
                try:
                    # Please note: using only the last value to identify scalevalue
                    # - a change of scale values should leed to a different cdf archive !!
                    stream.header['DataScaleX'] = cdf_file['HNscv'][...][-1]
                    stream.header['DataScaleY'] = cdf_file['HEscv'][...][-1]
                    stream.header['DataScaleZ'] = cdf_file['Zscv'][...][-1]
                    stream.header['DataSensorOrientation'] = 'hdz'
                except:
                    # print "error while interpreting header"
                    pass
            elif key in h0_mfi_KEYDICT and OMNIACE: # MAG DATA (H0)
                data = cdf_file[key][...]
                flag = cdf_file['Q_FLAG'][...]
                #for i in range(0,len(data)):
                #    f = flag[i]
                #    if f != 0:
                #        data[i] = float('nan')
                if key == 'BGSM':
                    skey_x = h0_mfi_KEYDICT[key][0]
                    skey_y = h0_mfi_KEYDICT[key][1]
                    skey_z = h0_mfi_KEYDICT[key][2]
                    splitdata = np.hsplit(data, 3)
                    stream.header['col-'+skey_x] = 'Bx'
                    stream.header['unit-col-'+skey_x] = 'nT'
                    stream.header['col-'+skey_y] = 'By'
                    stream.header['unit-col-'+skey_y] = 'nT'
                    stream.header['col-'+skey_z] = 'Bz'
                    stream.header['unit-col-'+skey_z] = 'nT'
                    for ikey, skey in enumerate([skey_x, skey_y, skey_z]):
                        if not oldtype:
                            ind = KEYLIST.index(skey)
                            array[ind] = np.asarray(splitdata[ikey])
                        else:
                            stream._put_column(splitdata[ikey],skey)
                elif key == 'Magnitude':
                    skey = h0_mfi_KEYDICT[key]
                    stream.header['col-'+skey] = 'Bt'
                    stream.header['unit-col-'+skey] = cdf_file[key].attrs['UNITS']
                    if not oldtype:
                        ind = KEYLIST.index(skey)
                        array[ind] = np.asarray(data)
                    else:
                        stream._put_column(data,skey)
            elif key in h1_epm_KEYDICT and OMNIACE: # EPAM DATA (H1)
                data = cdf_file[key][...]
                badval = cdf_file[key].attrs['FILLVAL']
                for i in range(0,len(data)):
                    d = data[i]
                    if d == badval:
                        data[i] = float('nan')
                skey = h1_epm_KEYDICT[key]
                stream.header['col-'+skey] = cdf_file[key].attrs['LABLAXIS']
                stream.header['unit-col-'+skey] = cdf_file[key].attrs['UNITS']
                if not oldtype:
                    ind = KEYLIST.index(skey)
                    array[ind] = np.asarray(data)
                else:
                    stream._put_column(data,skey)
            elif key in k0_epm_KEYDICT and OMNIACE: # EPAM DATA (K0)
                data = cdf_file[key][...]
                badval = cdf_file[key].attrs['FILLVAL']
                for i in range(0,len(data)):
                    d = data[i]
                    if d == badval:
                        data[i] = float('nan')
                skey = k0_epm_KEYDICT[key]
                stream.header['col-'+skey] = cdf_file[key].attrs['LABLAXIS']
                stream.header['unit-col-'+skey] = cdf_file[key].attrs['UNITS']
                if not oldtype:
                    ind = KEYLIST.index(skey)
                    array[ind] = np.asarray(data)
                else:
                    stream._put_column(data,skey)
            elif key in h0_swe_KEYDICT and OMNIACE: # SWEPAM DATA
                data = cdf_file[key][...]
                badval = cdf_file[key].attrs['FILLVAL']
                for i in range(0,len(data)):
                    d = data[i]
                    if d == badval:
                        data[i] = float('nan')
                skey = h0_swe_KEYDICT[key]
                stream.header['col-'+skey] = cdf_file[key].attrs['LABLAXIS']
                stream.header['unit-col-'+skey] = cdf_file[key].attrs['UNITS']
                if not oldtype:
                    ind = KEYLIST.index(skey)
                    array[ind] = np.asarray(data)
                else:
                    stream._put_column(data,skey)
            else:
                if key.lower() in KEYLIST:
                    arkey = cdf_file[key][...]
                    if len(arkey) == 1:
                        # This is the case if identical data is found
                        try:
                            length = len(cdf_file['Epoch'][...])
                        except:
                            length = len(cdf_file['time'][...])
                        arkey = [arkey[0]] * length
                    if len(arkey) > 0:
                        if not oldtype:
                            ind = KEYLIST.index(key.lower())
                            array[ind] = np.asarray(arkey).astype(object)
                        else:
                            stream._put_column(arkey,key.lower())
                        stream.header['col-'+key.lower()] = key.lower()
                        try:
                            stream.header['unit-col-'+key.lower()] = cdf_file[key.lower()].attrs['units']
                        except:
                            # eventually apply default deg C for temperatures if not provided in header
                            if key.lower() in ['t1','t2']:
                                stream.header['unit-col-'+key.lower()] = "*C"
                            pass
                        try:
                            stream.header['col-'+key.lower()] = cdf_file[key.lower()].attrs['name']
                        except:
                            pass

        cdf_file.close()
        del cdf_file

    if not oldtype:
        return DataStream([LineStruct()], stream.header,np.asarray(array))
    else:
        return DataStream(stream, stream.header,stream.ndarray)
