#!/usr/bin/env python
"""
Test program to verify the correctness of DI analyses

version 1.0.0:

DESCRIPTION:
 Method loads two di files, one from the northen hemisphere (F values provided in DI file)
 and one from the southern hemisphere (F values in IAGA2002 file). 
 Three variants from NXY (nothern hemisphere) are analyzed:
   a) file with residual technqiue
   b) file with zero technique
   c) file based on zero techninque with obtained S0 for D and I used as residual
      (as a result, S0 for D and I should be zero)
 Two variants from SXY (southern hemisphere)
   a) file with zero technique
   b) file based on zero techninque with obtained S0 for D and I used as residual
      (as a result, S0 for D and I should be zero)
 TODO: add a json and a AutoDIF file
 All analyses results are compared against a dictionary with expected results.

RETURN:
 BOOL
 If all expected tests are satisfied then the test_di method will return "True".
 Details of the tests are printed to standard out. 
"""

local = True
if local:
    import sys
    sys.path.insert(1,'/home/leon/Software/magpy/')
from magpy.stream import *
import magpy.absolutes as di


EXPECTED = {"NXY" : {"S0D" : 16.815, "HD" : -19.762, "HE" : -93.382, "S0Z" : 15.972, "ZE" : -93.267, "SV" : 1.000, "D" : 311.27127, "I" : 85.80933, "F": 56327.0}, 
            "SXX" : {"S0D" : 6.8, "HD" : 0.7, "HE" : 3.8, "S0Z" : 7.2, "ZE" : 4.3, "SV" : 0.981, "D" : '338:13:41.4', "I" : -65},
            "SXY" : {"S0D" : 5.123, "HD" : 3.826, "HE" : 0.778, "S0Z" : 6.046, "ZE" : 1.916, "SV" : 0.981, "D" : 15.68170, "I" : -57.19036, "F": 37820.98}}



def CompareValuesDict(dic1, dic2, accepteddiff=0.01, debug =False):
     """
     check whether diff of two dicts is below a threshold value
     """
     identical = True
     for key in dic1:
         val1 = dic1[key]
         val2 = dic2.get(key,None)
         if val2:
             diff = np.abs(val1-val2)
             if debug:
                 print (key, diff)
             if diff > accepteddiff:
                 identical = False
     if debug:
         print ("Comparing ", identical)
     return identical

def ExtractValues(stream, line):
        RESULT = {}
        RESULT['S0D'] = stream.ndarray[7][0]
        RESULT['HD'] = stream.ndarray[8][0]
        RESULT['HE'] = stream.ndarray[9][0]
        RESULT['S0Z'] = stream.ndarray[10][0]
        RESULT['ZE'] = stream.ndarray[11][0]
        RESULT['D'] = stream.ndarray[2][0]
        RESULT['I'] = stream.ndarray[1][0]
        RESULT['F'] = stream.ndarray[4][0]
        return RESULT

def test_di(): 

    exepath = os.getcwd()
    datadir = 'testdata'
    successlist = []

    dipath = os.path.join(exepath,'{}/'.format(datadir))
    varionxy = os.path.join(exepath,'{}/NXY*'.format(datadir))
    variosxy = os.path.join(exepath,'{}/SXY*'.format(datadir))
    varionxxv = os.path.join(exepath,'{}/NXX_vario*'.format(datadir))
    varionxxs = os.path.join(exepath,'{}/NXX_scalar*'.format(datadir))
    #variosxy = os.path.join(exepath,'{}/SXY*'.format(datadir))
    print (varionxy)
    print (variosxy)

    success1 = True
    try:
        #1. Northen hemisphere NXY
        nabsresult = di.absoluteAnalysis(dipath,varionxy,varionxy,diid='NXY.txt',stationid='NXY',pier='2', alpha=0.0, deltaF=0.0)
        RESULT = ExtractValues(nabsresult, 0)
        success1 = CompareValuesDict(EXPECTED.get('NXY'),RESULT)
        zero1 = nabsresult.ndarray[7][-1]
        zero2 = nabsresult.ndarray[10][-1]
        if np.abs(zero1) > 0.1 or np.abs(zero2) > 0.1:
            success1 = False
    except:
        success1 = False
    print ("1. Northen Hemisphere NXY: ", success1)
    successlist.append(success1)

    success2 = True
    try:
        #2. Southern hemisphere SXY
        sabsresult = di.absoluteAnalysis(dipath,variosxy,variosxy,diid='SXY.txt',stationid='SXY',pier='A', alpha=0.0, deltaF=0.0)
        RESULT = ExtractValues(sabsresult, 0)
        success2 = CompareValuesDict(EXPECTED.get('SXY'),RESULT)
        zero1 = sabsresult.ndarray[7][-1]
        zero2 = sabsresult.ndarray[10][-1]
        if np.abs(zero1) > 0.1 or np.abs(zero2) > 0.1:
            success2 = False
    except:
        success2 = False
    print ("2. Southern Hemisphere SXY: ", success2)
    successlist.append(success2)

    success3 = True
    try:
        nxxabsresult = di.absoluteAnalysis(dipath,varionxxv,varionxxs,diid='NXX.txt',stationid='NXX',pier='A2', alpha=0.0, deltaF=-0.5)
    except:
        success3 = False
    print ("3. Northen Hemisphere NXX: ", success3)
    successlist.append(success3)

    success4 = True
    try:
        nxxabsresult = di.absoluteAnalysis(dipath,varionxxv,varionxxs,diid='NXX.txt',stationid='NXX',pier='A16', abstype='autodif', azimuth=267.5, alpha=0.0, deltaF=-0.5)
    except:
        success4 = False
    print ("4. Northen Hemisphere AUTODIF NXX: ", success4)
    successlist.append(success4)

    """
    filename = '/home/leon/Downloads/sit/DI_jan.json'
    #filename = '/home/leon/Cloud/Software/MagPyAnalysis/USGS json/observation_webservice_example.json'

    data = di.readJSONABS(filename)
    print (data)

    absresult = di.absoluteAnalysis(filename,'/home/leon/Downloads/sit/*.min','',stationid='SIT',diid='.json',deltaF=0.0) 

    #'/home/leon/Downloads/sit/'

    success5 = True
    try:
        #dipath = ...
        nxzabsresult = di.absoluteAnalysis(dipath,varionxxv,varionxxs,diid='NXX.txt',stationid='NXX',pier='A16', abstype='autodif', azimuth=267.5, alpha=0.0, deltaF=-0.5)
    except:
        success5 = False
    print ("5. Northen Hemisphere JSON/WEBSERVICE NXZ: ", success4)
    successlist.append(success4)
    """

    if False in successlist:
        return False
    else:
        return True


test = test_di()

print ("RESULT", test)


