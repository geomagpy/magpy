#!/usr/bin/python

"""
Create a simple function which has the following options:
decode -h                                       # help
decode -r filename/path                         # extract file
decode -r path -s startdate -e enddate          # with dates like "2013-11-22"
decode -r path -f IAGA                          # define outputformat
decode -r path -w path2write                    # define path to write to
"""
from magpy.stream import *
import sys, getopt
#sys.path.append('/home/leon/Software/magpy/trunk/src')
#from stream import *

def main(argv):
    inputpath = ''
    outputpath = ''
    starttime = ''
    endtime = ''
    format_type = ''
    try:
        opts, args = getopt.getopt(argv,"hr:w:s:e:f:",["read=","write=","sdate=","edate=","format="])
    except getopt.GetoptError:
        print 'convert.py -r <path2read> -w <path2write> -s <startdate> -e <enddate> -f <outputformat>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'Usage:'
            print 'convert.py -r <path2read> -w <path2write> -s <startdate> -e <enddate> -f <outputformat>'
            print 'Examples:'
            print 'Single file:         convert.py -r /my/path/myfile.bin'
            print 'Mulitple files:      convert.py -r /my/path/*.bin'
            print 'Time range:          convert.py -r /my/path/* -s 2014-01-01 -e 2014-01-15'
            print 'Save as IAGA:        convert.py -r /my/path/myfile.bin - f IAGA'
            print 'Define outputpath:   convert.py -r /my/path/myfile.bin - w /my/other/path/'
            sys.exit()
        elif opt in ("-r", "--read"):
            inputpath = arg
        elif opt in ("-w", "--write"):
            outputpath = arg
        elif opt in ("-s", "--sdate"):
            starttime = arg
        elif opt in ("-e", "--edate"):
            endtime = arg
        elif opt in ("-f", "--format"):
            format_type = arg

    print format_type

    if outputpath == '':
        outputpath = inputpath
    outputpath = os.path.abspath(outputpath)
    if os.path.isfile(outputpath):
        outputpath = os.path.split(outputpath)[0]
    else:
        # Test whether wpath is already a dirctory
        if not os.path.isdir(outputpath):
            print "Error: cannot interpret path to write to"
            sys.exit()
    if inputpath == '':
         print 'Specify a file/path by the -r option:'
         print 'convert.py -r <path2read>'
         print '-- check convert.py -h for more options'
         sys.exit()
    if format_type == '':
        format_type = 'PYASCII'
    else:
        if not format_type in PYMAG_SUPPORTED_FORMATS:
            print "Error: Unkown format! Take one of (currently some are not support writing - check MagPy's development site for info)", PYMAG_SUPPORTED_FORMATS
            sys.exit()

    print "Reading file(s): ", inputpath
    if starttime != '' and endtime != '':
        data = read(inputpath,starttime=starttime,endtime=endtime)
    elif starttime != '':
        data = read(inputpath,starttime=starttime)
    elif endtime != '':
        data = read(inputpath,endtime=endtime)
    else:
        data = read(inputpath)
    num = len(data)
    if num == 0:
        print "Error: Data could not be read"
    print "Found %s datapoints" % str(len(data))
    print "Writing %s data ..." % format_type
    # Check whether wpath is really a path
    data.write(outputpath,format_type=format_type)
    print "finished!"

if __name__ == "__main__":
   main(sys.argv[1:])
