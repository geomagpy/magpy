"""
MagPy
MagPy input filter for NetCDF4 format files (*.nc).
REQUIRES PACKAGE netCDF4
Written by RLB April 2017
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division
from datetime import datetime
from matplotlib.dates import date2num
import numpy as np
try:
    from netCDF4 import Dataset
except ImportError:     # No netCDF4 support
    pass

from magpy.stream import DataStream
import logging
logger = logging.getLogger(__name__)


DSCOVR_KEYDICT = {# Magnetometer data:
                  'bx_gse':     'x',    # Interplanetary Magnetic Field strength Bx component in GSE coordinates
                  'by_gse':     'y',    # Interplanetary Magnetic Field strength By component in GSE coordinates
                  'bz_gse':     'z',    # Interplanetary Magnetic Field strength Bz component in GSE coordinates
                  'bt':         'f',    # Interplanetary Magnetic Field strength Bt
                  # Plasma data:
                  'proton_density':     'var1', # solar wind proton density
                  'proton_speed':       'var2', # solar wind proton speed
                  'proton_temperature': 'var3', # solar wind proton temperature
                  }


def isNETCDF(filename):
    """
    Checks whether a file is a netCDF4 format.
    """
    try:
        temp = Dataset(filename, 'r')
    except:
        return False

    logger.info("isNETCDF: Found netCDF4 format data")
    return True


def readNETCDF(filename, headonly=False, **kwargs):
    """
    Reading NetCDF format data.
    To see all attributes of file: print(ncdata.ncattrs())
    """
    
    stream = DataStream([],{})
    headers = {}
    array = [[] for key in stream.KEYLIST]
    timef = "%Y-%m-%dT%H:%M:%S.%fZ"
    
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')

    ncdata = Dataset(filename, 'r')
    filestart = datetime.strptime(ncdata.time_coverage_start, timef)
    fileend = datetime.strptime(ncdata.time_coverage_end, timef)
    
    # Check if file is within defined time ranges:
    getfile = True
    try:
        if starttime:
            if not filestart >= datetime.date(stream._testtime(starttime)):
                getfile = False
        if endtime:
            if not fileend <= datetime.date(stream._testtime(endtime)):
                getfile = False
    except:
        getfile = True
        
    # Read data into assigned columns:
    if getfile:
        logger.info("readNETCDF: Reading {}".format(filename))
        
        if ncdata.program == 'DSCOVR':
            logger.info("readNETCDF: File contains DSCOVR data. Using appropriate keys")
            KEYDICT = DSCOVR_KEYDICT
        
        if ncdata.variables['time'].units == 'milliseconds since 1970-01-01T00:00:00Z':
            array[0] = np.array([date2num(datetime.utcfromtimestamp(x/1000.))
                                 for x in ncdata.variables['time'][...]])
        else:
            logger.warning("readNETCDF: Could not identify time format. Time array probably incorrect")
            array[0] = ncdata.variables['time'][...]
        
        for var in ncdata.variables:
            if var in KEYDICT:
                column = ncdata.variables[var]
                coldata = column[...]
                key = KEYDICT[var]
                idx = stream.KEYLIST.index(key)
                # Convert from MaskedArray
                coldata = np.array(coldata, dtype=np.float)
                # Replace masked values with NaNs:
                coldata[np.where(coldata==float(column.missing_value))] = np.NaN

                array[idx] = coldata
                headers['col-'+key] = var
                headers['unit-col-'+key] = ncdata.variables[var].units
                
        # Fill in additional header data:
        for attr in ncdata.ncattrs():
            headers[attr] = getattr(ncdata, attr)

    ncdata.close()
    return DataStream([], headers, np.asarray(array))

