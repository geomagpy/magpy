#!/usr/bin/env python
"""
Test program to read all supported formats. Only working on dedicated machines

DESCRIPTION:
    Tries to read all files in a specific directory
RETURN:
    BOOL
    If all expected tests are satisfied then the test_di method will return "True".
    Details of the tests are printed to standard out.
"""

local = True
if local:
    import sys
    sys.path.insert(1,'/Users/leon/Software/magpy/')
    sys.path.insert(1,'/home/leon/Software/magpy/')

from magpy.stream import *
import copy
from magpy.core import activity as act
from magpy.core import plot as mp
from magpy.core import flagging
from magpy.core.methods import dictdiff
import fnmatch

source = '/home/leon/Cloud/Daten/MagPyTestFiles/datastreams/'
matches = {}
success = True
types = '*.*'
for root, dirnames, filenames in os.walk(source):
    for filename in fnmatch.filter(filenames, types):
        key = os.path.basename(os.path.normpath(root))
        values = matches.get(key,[])
        values.append(os.path.join(root, filename))
        matches[key] = values

print (sys.version)
for key in matches:
    print ("Testing projected format library {}".format(key))
    testfiles = matches.get(key,[])
    for file in testfiles:
        testdata = read(file)
        if not len(testdata) > 0:
            print ("   FAILED: ", file, len(testdata))
            success = False

print ("RESULT:", success)

if success:
    sys.exit(0)
else:
    sys.exit(1)
