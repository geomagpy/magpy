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

SOURCEDICT = {
 		'FTP': {'source':'ftp://www.zamg.ac.at', 
                        'cred':'zamg', 
                        'logfile':'/tmp/transfer_test.log',
                        'local':'',
                        'remote':'/data/environment'},
 		#'SCP': {'source':''},
 		#'GIN': {'source':''}
             }

def getcred(credentials):
    import magpy.opt.cred as mpcred
    creddict = {}
    try:
        creddict['address']=mpcred.lc(credentials,'address','')
        creddict['user']=mpcred.lc(credentials,'user','')
        if creddict.get('user') == '':
            print ("no credentials found")
            return False
        creddict['passwd']=mpcred.lc(credentials,'passwd','')
        creddict['port']=mpcred.lc(credentials,'port','')
        return creddict
    except:
        return False


ok=True
if ok:
    # Tested MagPy Version
    print ("------------------------------------------------")
    print ("MagPyVersion: {}".format(magpyversion))
    resultdict = {}
    print ("------------------------------------------------")
    print ("Test conditions:")
    print ("Data will be transferred via FTP to a remote server.")

    for key in SOURCEDICT:
        SensorID = False
        sensorid = ''
        print("------------------------------------------------")
        print("Running analysis for {}".format(key))
        print("------------------------------------------------")
        checkdict = SOURCEDICT.get(key)
        creds = checkdict.get('cred')
        credvals = getcred(creds)
        print (credvals)
        if credvals:
             resultdict['credentials'] = 'success'
        else:
             resultdict['credentials'] = 'failure'
             break
        t1 = datetime.utcnow()
        if key == 'FTP':
            # use example1 file for upload
            # Test 1:
            import magpy
            localfile = (os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(magpy.stream.__file__)),'examples','example1.cdf')))
            success = ftpdatatransfer(  localfile=localfile, 
                                      	ftppath=checkdict.get('remote'), 
					myproxy=credvals.get('address'), 
	                                port=credvals.get('port'),
					login=credvals.get('user'),
					passwd=credvals.get('passwd'),                      
                	                logfile=checkdict.get('logfile')  )
            if success:
                 resultdict['upload'] = 'success'
            else:
                 resultdict['upload'] = 'failure'

            localfile = (os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(magpy.stream.__file__)),'examples','example2.bin')))
            # the following should fail and create a logfile
            success = ftpdatatransfer(  localfile=localfile, 
                                      	ftppath=checkdict.get('remote'), 
					myproxy=credvals.get('address'), 
	                                port=credvals.get('port'),
					login=credvals.get('user'),
					passwd='xxx',                      
                	                logfile=checkdict.get('logfile')  )
            if os.path.isfile(checkdict.get('logfile')):
                resultdict['missingcreated'] = 'success'
            else:
                resultdict['missingcreated'] = 'failure'
            # this should work and upload both example2 and example3
            localfile = (os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(magpy.stream.__file__)),'examples','example3.txt')))
            success = ftpdatatransfer(  localfile=localfile, 
                                      	ftppath=checkdict.get('remote'), 
					myproxy=credvals.get('address'), 
	                                port=credvals.get('port'),
					login=credvals.get('user'),
					passwd=credvals.get('passwd'),                      
                	                logfile=checkdict.get('logfile')  )
            if success:
                 resultdict['missingupload'] = 'success'
            else:
                 resultdict['missingupload'] = 'failure'

            #ftpdatatransfer(localfile=localfile, ftppath=remotepath,myproxy=address,             
            #                port=port,login=user,passwd=passwd, logfile=pathtolog,raiseerror=True)
            resultdict['list'] = 'success'
            resultdict['download'] = 'success'
        elif key == 'SCP':
            pass

        print ("  -> Transfer check result for {}: {}".format(key,resultdict))

