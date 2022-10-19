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
import copy
try:
    from deepdiff import DeepDiff
except:
    print ("Deepdiff not installed -  skipping indepth header analysis")

def test_baseline(dipath=None, variopath=None, scalarpath=None,debug=False):
    if not dipath:
        return False
    if not variopath:
        return False
    if not scalarpath:
        return False
    absresult = di.absoluteAnalysis(dipath,os.path.join(variopath,"*"),os.path.join(scalarpath,"*"),compensation=True,diid='A2_WIC.txt',stationid='WIC',pier='A2', alpha=0.0, deltaF=0.0, debug=debug)
    if debug:
        print ("Analyzed DI data", absresult.length()[0])
    vario = read(os.path.join(variopath,"*2022-08-22.cdf"))
    if debug:
        print ("Reading variometer data: {}, N={}".format(vario.header.get("sensorid"),vario.length()[0]))
    #apply compensation    varion
    vario = vario.compensation()
    if debug:
        print ("Applied compensation data: x={}, y={}, z={}".format(vario.ndarray[1][0],vario.ndarray[2][0],vario.ndarray[3][0]))
    #apply baseline file
    testdict1 = copy.deepcopy(vario.header)
    meanabs = (date2num(absresult._find_t_limits()[1])-date2num(absresult._find_t_limits()[0]))/2 + date2num(absresult._find_t_limits()[0])
    func1 = vario.baseline(absresult, fitfunc="poly", extradays=0, startabs=absresult._find_t_limits()[0], endabs=meanabs,debug=debug)
    func2 = vario.baseline(absresult, fitfunc="mean", extradays=0, startabs=meanabs, endabs=absresult._find_t_limits()[1],debug=debug)
    func = [func1,func2]

    #vario.func_to_file(func, "/tmp/tmpfit.json" debug=debug)
    test = ""
    if test == "simple":
        print (" Testing simple baseline methods")
        print ("     -- dropping all line with NAN in basevalues")
        absresult = absresult._drop_nans('dx')
        absresult = absresult._drop_nans('dy')
        absresult = absresult._drop_nans('dz')
        print ("     -- calculation medians for basevalues")
        bh, bhstd = absresult.mean('dx',meanfunction='median',std=True)
        bd, bdstd = absresult.mean('dy',meanfunction='median',std=True)
        bz, bzstd = absresult.mean('dz',meanfunction='median',std=True)
        print ("       -> Basevalues:")
        print ("          Delta H = {a} +/- {b}".format(a=bh, b=bhstd))
        print ("          Delta D = {a} +/- {b}".format(a=bd, b=bdstd))
        print ("          Delta Z = {a} +/- {b}".format(a=bz, b=bzstd))
        print ("     -- Performing constant basevalue correction")
        vario = vario.simplebasevalue2stream([bh,bd,bz])
    else:
        #apply baseline correction
        vario = vario.bc(debug=debug)#function=[func])

    if debug:
        print ("Baseline correction done")
        print ("After applied baseline correction: x={}, y={}, z={}".format(vario.ndarray[1][0],vario.ndarray[2][0],vario.ndarray[3][0]))
    testdict2 = copy.deepcopy(vario.header)
    try:
        diff = DeepDiff(testdict1, testdict2)
        print ("header differences after running baseline and bc", diff)
    except:
        pass

    #print (vario.ndarray)
    #subtract from orgdata
    varioxyz = vario.hdz2xyz()
    absxyz = absresult.idf2xyz()
    teststream = subtractStreams(varioxyz,absxyz)
    if debug:
        print (teststream.ndarray)
        #print ("Applied baseline correction: x={}, y={}, z={}".format(vario.ndarray[1][0],vario.ndarray[2][0],vario.ndarray[3][0]))
    meanx = teststream.mean('x')
    meany = teststream.mean('y')
    meanz = teststream.mean('z')
    #if debug:
    print ("Mean differences between DI and BL corrected varion: ", meanx, meany, meanz)
    val = True
    for el in [meanx, meany, meanz]:
        if abs(meanx) > 1:
            val = False
    return val

exepath = os.getcwd()
if not exepath.endswith('test'):
    exepath = os.path.join(exepath,'magpy','test') # travis...
datadir = 'testdata'
dipath = os.path.join(exepath,datadir,"di-data")
variopath = os.path.join(exepath,datadir,"vario1")
scalarpath = os.path.join(exepath,datadir,"scalar")

# Using compensation values requires db access
debug = False

try:
    test = test_baseline(dipath=dipath, variopath=variopath, scalarpath=scalarpath,debug=debug)
except:
    test = False

if test:
    print ("baseline test successfully finished")

if test:
    sys.exit(0)
else:
    sys.exit(1)
