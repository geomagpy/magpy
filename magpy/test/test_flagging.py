#!/usr/bin/env python
"""
MagPy - Basic Runtime tests including durations  
"""
from __future__ import print_function

# Define packges to be used (local refers to test environment) 
# ------------------------------------------------------------
local = True
if local:
    import sys
    sys.path.insert(1,'/home/leon/Software/magpy/')

from magpy.stream import *   
from magpy.database import *   
import magpy.transfer as tr
import magpy.absolutes as di
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as cred

## READ Source dictionary fron test cfg file or provide by options

    # ++++++++++++++++++++++++++++++++++++++++++++++++
    # Read different files and get header and key info
    # ++++++++++++++++++++++++++++++++++++++++++++++++
    #   please note that a full test requires about xxx min

SOURCEDICT = {
                ## GENERAL File Formats
		#'IMAGCDF': {'source':example1, 'keys': ['x','y','z','f'], 'length': 86400, 'id':'', 'wformat': ['IMAGCDF']},
 		'GP20S3': {'source':'/home/leon/Cloud/Daten/GP20S3NSS2_012201_0001_0001_2019-07-12.cdf'}
             }

ok=True
if ok:
    # Tested MagPy Version
    print ("------------------------------------------------")
    print ("MagPyVersion: {}".format(magpyversion))
    resultdict = {}
    print ("------------------------------------------------")
    print ("Test conditions:")
    print ("Reading two different data sets with second and microsecond resolution.")
    print ("A certain time range with a known amount of data points will be flagged")
    print ("and flags will be saved to file. This flagfile will be read and amount")
    print ("of flags will be verified. The outlier method will be checked against an")
    print ("an expected flag number. Flags will be saved as well and the flagging")
    print ("information (status) will be checked and modified.")
    for key in SOURCEDICT:
        SensorID = False
        sensorid = ''
        print ("------------------------------------------------")
        print ("Running analysis for {}".format(key))
        print ("------------------------------------------------")
        checkdict = SOURCEDICT.get(key)
        stream = read(checkdict['source'],debug=True)
        foundlen = stream.length()[0]
        print (foundlen)
        t1 = datetime.utcnow()
        print ("Step 1: flagging outliers ...")
        print (stream._get_key_headers())
        diffdata = stream.flag_outlier(threshold=4,timerange=timedelta(seconds=60),stdout=True)
        print (diffdata._get_key_headers())
        diffdata = diffdata.remove_flagged()
        t2 = datetime.utcnow()
        flagtime = (t2-t1)
        foundflaglen = diffdata.length()[0]
        print (foundflaglen)

        ind = KEYLIST.index('flag')
        ts,te = diffdata._find_t_limits()
        print ("Step 1: flagging time range...")
        diffdata = diffdata.flag_stream('x', 2, "too be used in any case", ts, ts+timedelta(hours=1))
        t2 = datetime.utcnow()

