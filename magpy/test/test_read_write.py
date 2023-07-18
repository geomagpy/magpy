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
    sys.path.insert(1,'/Users/leon/Software/magpy/')

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
"""
SOURCEDICT = {
                ## GENERAL File Formats
		'IMAGCDF':'example1',
		'GDAS':os.path.join(tspath,'WIC_v1_20150218.sec'),
		'PYBIN':os.path.join(tspath,'GSM90_14245_0002_2015-05-28.bin'), 
		'IWT':os.path.join(tspath,'E_iWT20140920.raw'), 
		'PYCDF':os.path.join(tspath,'LEMI025_22_0003_0001_2015-05-28.cdf'), 
		'DTU2':os.path.join(tspath,'TDC1_20150220.cdf'), 
		'DIDD':os.path.join(tspath,'APR0214.WIK'), 
		'RCSMETEO':os.path.join(tspath,'METEO_2012-01-30.txt'),
		'LNM':os.path.join(tspath,'LNM_0351_0001_2015-08-13.asc'),
		'RADON':os.path.join(tspath,'COBSEXP_2_tmp_2012-09-27.txt'),
		'IAGA':os.path.join(tspath,'WIK20120111d.min'),
		'WDC':os.path.join(tspath,'wik2012.wdc'),
		'IAF':os.path.join(tspath,'aaa','aaa10apr.bin'),
		'IMF':os.path.join(tspath,'SEP1214.WIC'),
		'IYFV':os.path.join(tspath,'aaa','yearmean.aaa'),
		'DKA':os.path.join(tspath,'aaa','*.dka'),
		'WDC':os.path.join(tspath,'asp2014m','asp1401m.wdc'),
		'BLV':os.path.join(tspath,'aaa','*.blv'),
		'GRAVSG':os.path.join(tspath,'G1101122.025'),
		'LEMIBIN':os.path.join(tspath,'L025_066.B01'),
		'RMRCS':os.path.join(tspath,'RCS-2F3Aanw-2016-02-01_00-00-00.txt'),
		'GSM19':os.path.join(tspath,'23.txt'),
		'PMAG2':os.path.join(tspath,'CO091231.CAP'),
		'PMAG1':os.path.join(tspath,'ZAGCPMAG-LOG_2010_07_09.txt'),
		'LEMIHF':os.path.join(tspath,'WIC20121103v.txt'),
		'USBLOG':os.path.join(tspath,'Tunnel_13.09.2010.txt'),
		'IONO':os.path.join(tspath,'IM_0923113614.CSV'),
		'PYASCII':os.path.join(tspath,'MyASCII_2015-11-22.txt'),
		'PYSTR':os.path.join(tspath,'BLV_FGE_S0252_0001_GSM90_14245_0002_A2.txt'),
		'PYASCII':os.path.join(tspath,'MyASCII_2015-11-22.txt'),
		'NASACDF':os.path.join(tspath,'ace_1m_2016-12-08.cdf'),
		'LIPPGRAV':os.path.join(tspath,'20161021.TLT')
                }
"""

#TODO add a cdf file with leapseconds

exepath = os.getcwd()
datadir = 'testdata'
if not exepath.endswith('test'):
    exepath = os.path.join(exepath,'magpy','test') # travis...

SOURCEDICT = {
                ## GENERAL File Formats
		'IAGA_ZIP': {'source':example1, 'length': 86400, 'id':'', 'wformat': ['PYCDF','IAGA']},
		'IMAGCDF': {'source':example4, 'keys': ['x','y','z','f','t1','t2'], 'length': 604798, 'id':'', 'wformat': ['IMAGCDF']},
 		'WEBSERVICE': {'source':'https://cobs.zamg.ac.at/data/webservice/query.php?id=WIC'},
		'IAGA': {'source':example5, 'keys': ['x','y','z','f'], 'length': 86400, 'id':'', 'wformat': ['PYCDF','IAGA','IMAGCDF']},
		'DKA': {'source':os.path.join(exepath,datadir,'kvalue.dka'), 'keys': ['var1'], 'length': 2920, 'id':''},
		#'IAF': {'source':'/media/leon/6439-3834/products/data/magnetism/definitive/wic2018/IAF/WIC18FEB.BIN', 'wformat': ['IAF','IMF']},
		#'WDC': {'source':'/media/leon/6439-3834/products/data/magnetism/definitive/wic2018/WDC/WIC2018.WDC', 'wformat': ['WDC']},
		#'PYBIN': {'source':'/home/leon/Cloud/Daten/LEMI036_2_0001_2019-10-10.bin'},
		#'MagPyCDF1.1': {'source':'/media/leon/6439-3834/products/data/magnetism/definitive/wic2018/magpy/Definitive_min_mag_2018.cdf','description':'PYCDF with pickled basevalue list in header - created in Py2'},
		#'MagPyCDF1.1': {'source':'/home/leon/Cloud/Daten/BLVtest.cdf','description':'PYCDF with string columns and identical-value columns - created in Py2'},
		#'AceCDF': {'source':'/home/leon/Cloud/Daten/ace_5m_2019-07-03.cdf','description':'ACECDF file from NASA'},
		#'MagPyTxt': {'source':'/home/leon/Cloud/Daten/BLVdata.txt','description':'PYTXT with meta and various different column types including identical-value columns - created in Py2', 'wformat': ['PYCDF']},
                # Not working in Travis CI tests
 		#'FTP': {'source':'ftp://ftp.nmh.ac.uk/wdc/obsdata/hourval/single_year/2011/fur2011.wdc', 'length': 8760, 'keys': ['x', 'y', 'z', 'f'], 'wformat': ['WDC']}
             }


def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    added = d1_keys - d2_keys
    removed = d2_keys - d1_keys
    modified = {o : (d1[o], d2[o]) for o in intersect_keys if d1[o] != d2[o]}
    same = set(o for o in intersect_keys if d1[o] == d2[o])
    return added, removed, modified, same

ok=True
if ok:
    # Tested MagPy Version
    print ("MagPyVersion: {}".format(magpyversion))
    resultdict = {}
    resultlist = []
    for key in SOURCEDICT:
        SensorID = False
        sensorid = ''
        print("------------------------------------------------")
        print("Running analysis for {}".format(key))
        print("------------------------------------------------")
        ### Checking if source isfile
        checkdict = SOURCEDICT.get(key)
        if not '//' in checkdict['source']:
            print ("Input file exists? {}".format(os.path.isfile(checkdict['source'])))
        t1 = datetime.utcnow()
        stream = read(checkdict['source'],debug=True)
        t2 = datetime.utcnow()
        foundkeys = stream._get_key_headers()
        foundid = stream.header.get('SensorID')
        readtime = (t2-t1)
        foundlen = stream.length()[0]
        print (foundlen, readtime, foundkeys, foundid)
        #print (stream.header)
        if not len(stream.ndarray[0]) > 1:
            foundtype = "Old linestruct ??"
            print ("WARNING: length = 0")
        # Testing important header information
        # SensorElements, DataSamplingRate, ColumnUnits, ColumnContents, 
        print ("Underlying data format:", stream.header.get('DataFormat'))
        #print (stream.header.get('DataSamplingRate'))
        #print (stream.header.get('col-x'),stream.header.get('unit-col-x'))
        # Content verification
        resultdict[key] = 'failure'
        if foundlen > 0 and not foundid == '':
            resultdict[key] = 'success'
            if checkdict.get('length'):
                if not foundlen == checkdict.get('length'):
                    resultdict[key] = 'failure - length not fitting'
            if checkdict.get('keys'):
                if not foundkeys == checkdict.get('keys',[]):
                    resultdict[key] = 'failure - different keys'
        print ("READ result for {}: {}".format(key,resultdict[key]))
        # Testing write function (if available)
        wlist = checkdict.get('wformat',[])
        if len(wlist) > 0:
            wpath = '/tmp'
            wname = '{}_all'.format(key)
            wresultdict = {}
            for wformat in wlist:
                print ("  --------")
                print ("  WRITING test: {} to {}".format(key,wformat))
                print ("  -> writing to: {}/{}".format(wpath,wname))
                wresultdict[wformat] = 'success'
                t1 = datetime.utcnow()
                fn = stream.write(wpath,filenamebegins=wname,coverage='all',format_type=wformat)
                print ("  -> write returned: {}".format(fn))
                if fn:
                    print ("  Writing successful")
                    t2 = datetime.utcnow()
                    print ("  Duration {}".format(t2-t1))
                    # Read again and analyze differences
                    print ("  ---------------------------------------")
                    print ("     Reading {} for cross check".format(fn))
                    data = read(fn)
                    print ("     Checking Metainformation after writing")
                    print ("     ---------------------------------------")
                    print ("     Check", data.length()[0])
                    added, removed, modified, same = dict_compare(stream.header, data.header)
                    print ("     Only in orignal:", added)
                    print ("     Only in reload:", removed)
                    print ("     Modified (org/new):", modified)
                    print ("     Same", same)
                    print ("     Checking Metainformation after writing")
                    print ("     ---------------------------------------")
                    diff = subtractStreams(stream,data)
                    if not foundkeys == data._get_key_headers() and checkdict.get('keys'):
                        wresultdict[wformat] = "Crosscheck failure - keys: original {} unequal {} ".format(foundkeys,data._get_key_headers())
                    else:
                        for comp in foundkeys:
                            if diff.mean(comp) > 0.00000001:
                                wresultdict[wformat] = "Crosscheck failure - datacontent"
                    if not foundid.replace("sec","").replace("min","") == data.header.get('SensorID').replace("sec","").replace("min",""):
                        wresultdict[wformat] = "Crosscheck failure - sensorid"
                    if not foundlen == data.length()[0]:
                        wresultdict[wformat] = "Crosscheck failure - length"
                else:
                    wresultdict[wformat] = "no success -- write function not existing -- check magpy.log for details"
                print ("  => {}: Write and cross check result: {}".format(wformat,wresultdict[wformat]))
        if wresultdict[wformat] == 'success':
            resultlist.append(True)
        else:
            resultlist.append(False)

print ("--------------------------------------------")
print ("SUMMARY:")
for res in resultdict:
    print ("  {}: {}".format(res, resultdict.get(res)))

if not all(resultlist):
    sys.exit(1)

