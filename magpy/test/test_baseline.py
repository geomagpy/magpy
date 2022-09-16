"""
Test program to verify the correctness of Baseline determinations

version 1.0.0:

DESCRIPTION:
 Method loads di files and analyses them with vario and scalar data.
 Then it calculates baseline corrected variation data.
 Finally it compares baseline corrected data with original DI measurements.

IMPORTANT:
 Baseline test is not yet implemented in CI

RETURN:
 BOOL
 If all expected tests are satisfied then the test_baseline method will return "True".
 Details of the tests are printed to standard out.
"""

local = True
if local:
    import sys
    sys.path.insert(1,'/Users/leon/Software/magpy/')
from magpy.stream import *
import magpy.absolutes as di


def test_baseline(dipath=None, variopath=None, scalarpath=None,debug=False):
    if not dipath:
        return False
    if not variopath:
        return False
    if not scalarpath:
        return False
    absresult = di.absoluteAnalysis(dipath,os.path.join(variopath,"*"),os.path.join(scalarpath,"*"),compensation=True,diid='A2_WIC.txt',stationid='WIC',pier='A2', alpha=0.0, deltaF=0.0, debug=debug)
    if debug:
        print ("Analyzed DI data", absresult.ndarray)
    vario = read(os.path.join(variopath,"*2022-08-22.cdf"))
    if debug:
        print ("Reading variometer data: {}, N={}".format(vario.header.get("sensorid"),vario.length()[0]))
    #apply compensation    varion
    vario = vario.compensation()
    if debug:
        print ("Applied compensation data: x={}, y={}, z={}".format(vario.ndarray[1][0],vario.ndarray[2][0],vario.ndarray[3][0]))
    #apply baseline file
    func = vario.baseline(absresult, fitfunc="mean")
    if debug:
        print ("Baseline correction done")
    #apply baseline correction
    vario = vario.bc()
    if debug:
        print ("Applied baseline correction: x={}, y={}, z={}".format(vario.ndarray[1][0],vario.ndarray[2][0],vario.ndarray[3][0]))
    #print (vario.ndarray)
    #subtract from orgdata
    varioxyz = vario.hdz2xyz()
    absxyz = absresult.idf2xyz()
    teststream = subtractStreams(varioxyz,absxyz)
    if debug:
        print (teststream.ndarray)
        #print ("Applied baseline correction: x={}, y={}, z={}".format(vario.ndarray[1][0],vario.ndarray[2][0],vario.ndarray[3][0]))


exepath = "/Users/leon/Cloud/Daten/FGE"
dipath = os.path.join(exepath,"di-data")
variopath = os.path.join(exepath,"vario1")
scalarpath = os.path.join(exepath,"scalar")
# Using compensation values requires db access
debug = True

test = test_baseline(dipath=dipath, variopath=variopath, scalarpath=scalarpath,debug=debug)

print ("RESULT:", test)

if test:
    sys.exit(0)
else:
    sys.exit(1)
