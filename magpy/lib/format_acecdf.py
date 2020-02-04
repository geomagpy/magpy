"""
MagPy
MagPy input/output filters
Written by Roman Leonhardt June 2012
- contains test and read function, toDo: write function
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from io import open

# Specify what methods are really needed
from magpy.stream import *

import logging
logger = logging.getLogger(__name__)

import gc
import cdflib

# K0 (Browsing - not for Serious Science) ACE-EPAM data from the OMNI database:
k0_epm_KEYDICT = {#'H_lo',                      # H (0.48-0.97 MeV)     (UNUSED)
                'Ion_very_lo': 'var1',          # Ion (47-65 keV) 1/(cm2 s ster MeV)
                'Ion_lo': 'var2',               # Ion (310-580 keV) 1/(cm2 s ster MeV)
                'Ion_mid': 'var3',              # Ion (310-580 keV) 1/(cm2 s ster MeV)
                'Ion_hi': 'var5',               # Ion (1060-1910 keV) 1/(cm2 s ster MeV)
                'Electron_lo': 'z',             # Electron (38-53 keV) 1/(cm2 s ster MeV)
                'Electron_hi': 'f'              # Electron (175-315 keV) 1/(cm2 s ster MeV)
                   }
# H1 (Level 2 final 5min data) ACE-EPAM data from the OMNI database:
h1_epm_KEYDICT = {
                'P1': 'var1',                   # Ion (47-65 keV) 1/(cm2 s ster MeV)
                'P3': 'var2',                   # Ion (115-195 keV) 1/(cm2 s ster MeV)
                'P5': 'var3',                   # Ion (310-580 keV) 1/(cm2 s ster MeV)
                'P7': 'var5',                   # Ion (1060-1910 keV) 1/(cm2 s ster MeV)
                'DE1': 'z',                     # Electron (38-53 keV) 1/(cm2 s ster MeV)
                'DE4': 'f'                      # Electron (175-315 keV) 1/(cm2 s ster MeV)
                # (... Many, MANY other unused keys.)
                   }
# H0 (Level 2 final 64s data) ACE-SWEPAM data from the OMNI database:
h0_swe_KEYDICT = {
                'Np': 'var1',                   # H_Density #/cc
                'Vp': 'var2',                   # SW_H_Speed km/s
                'Tpr': 'var3',                  # H_Temp_radial Kelvin
                # (... Many other keys unused.)
                   }
# H0 (Level 2 final 16s data) ACE-MAG data from the OMNI database:
h0_mfi_KEYDICT = {
                'Magnitude': 'f',               # B-field total magnitude (Bt)
                'BGSM': ['x','y','z'],          # B-field in GSM coordinates (Bx, By, Bz)
                #'BGSEc': ['x','y','z'],        # B-field in GSE coordinates
                # (... Many other keys unused.)
                   }

def isACECDF(filename):
    """
    Checks whether a file is Nasa CDF format.
    """
    try:
        temp = cdflib.CDF(filename)
    except:
        return False
    try:
        title = temp.globalattsget().get('TITLE')
        if not 'ACE' in title:
            return False
    except:
        pass
    try:
        variables = temp.cdf_info().get('zVariables')
        if not 'Epoch' in variables:
            if not 'time' in variables:
                return False
    except:
        return False
    print ("format_magpy: Found ACE CDF file {}".format(filename))

    logger.debug("format_magpy: Found ACE CDF file {}".format(filename))
    return True


def readACECDF(filename, headonly=False, **kwargs):
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
    OMNIACE = True

    # Check whether header information is already present
    headskip = False
    if stream.header == None:
        stream.header.clear()
    else:
        headskip = True

    theday = extractDateFromString(filename)
    try:
        if starttime:
            if not theday[-1] >= datetime.date(stream._testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(stream._testtime(endtime)):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True
    logbaddata = False

    if getfile:
        try:
            cdf_file = cdf.CDF(filename)
        except:
            return stream

        logger.info('readACECDF: %s Format: %s ' % (filename, cdfformat))

        for key in cdf_file:
            if key.find('time')>=0 or key == 'Epoch':
                        if key  == 'time':
                            ind = KEYLIST.index(key)
                            array[ind] = np.asarray(cdf_file[key][...]) + 730120.
                        elif key == 'Epoch':
                            ind = KEYLIST.index('time')
                            array[ind] = np.asarray(date2num(cdf_file[key][...]))
            elif key in h0_mfi_KEYDICT and OMNIACE: # MAG DATA (H0)
                data = cdf_file[key][...]
                flag = cdf_file['Q_FLAG'][...]
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

        cdf_file.close()
        del cdf_file

    return DataStream([LineStruct()], stream.header,np.asarray(array))

