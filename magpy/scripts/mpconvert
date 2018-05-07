#!/usr/bin/python

"""
Simple method to convert data files in different formats
"""
from __future__ import print_function
from __future__ import unicode_literals


from magpy.stream import read, PYMAG_SUPPORTED_FORMATS
import sys, getopt, os

def main(argv):
    inputpath = ''
    outputpath = ''
    starttime = ''
    endtime = ''
    format_type = ''
    coverage= None
    metainfo= None
    try:
        opts, args = getopt.getopt(argv,"hr:w:s:e:f:m:c:",["read=","write=","sdate=","edate=","format=","metainfo=","coverage="])
    except getopt.GetoptError:
        print ('convert.py -r <path2read> -w <path2write> -s <startdate> -e <enddate> -f <outputformat>  -m <metainfo> -c <coverage>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('Usage:')
            print ('------------')
            print ('convert.py -r <path2read> -w <path2write> -s <startdate> -e <enddate> -f <outputformat> -m <metainfo> -c <coverage>')
            print ('')
            print ('Description:')
            print ('------------')
            print ('Python program to convert bewteen data formats based on MagPy.')
            print ('Typical applications are the conversion of binary data formats')
            print ('to readable ASCII data sets or the conversion.')
            print ('')
            print ('Options:')
            print ('------------')
            print ('-r:			specify file to read data from e.g. -r "/my/path/myfile.bin"')
            print ('-w:			path to write data to: default is inputpath plus subfolder out')
            print ('-s:			startdate: year-month-day e.g. 2018-01-23')
            print ('-e:			enddate: e.g. 2018-01-26')
            print ('-c:			coverage: one of "day" (DEFAULT),"month","year","all" ')
            print ('-f:			supported output format:')
            print ('   			 -- PYASCII: MagPy basic ASCII    --- (DEFAULT)')
            print ('   			 -- IAGA: IAGA 2002 text format')
            print ('   			 -- IMAGCDF: Intermagnet CDF Format')
            print ('   			 -- IAF: Intermagnet archive Format')
            print ('   			 -- WDC: World Data Centre format')
            print ('   			 -- IMF: Intermagnet Format')
            print ('   			 -- PYSTR: MagPy full ascii')
            print ('   			 -- PYCDF: MagPy CDF variant')
            print ('   			 -- DKA: K value format Intermagnet')
            print ('   			 -- DIDD: Output format from MinGeo DIDD')
            print ('   			 -- BLV: Baseline format Intermagnet')
            print ('   			 -- IYFV: Yearly mean format Intermagnet')
            print ('   			 -- JSON: JavaScript Object Notation')
            print ('   			 -- LATEX: LateX data')
            print ('-m:			additional meta information:')
            print ('			a string like "DataStandardLevel:Partial,StationIAGACode:WIC"')
            print ('')
            print ('Examples:')
            print ('------------')
            print ('Single file:	mpconvert.py -r /my/path/myfile.bin')
            print ('Multiple files:	mpconvert.py -r "/my/path/*.bin"')
            print ('Time range:		mpconvert.py -r "/my/path/*" -s 2014-01-01 -e 2014-01-15')
            print ('Save as IAGA:	mpconvert.py -r /my/path/myfile.bin -f IAGA')
            print ('IAGA to IMAGCDF:	mpconvert.py -r /iagaseconds/201801* -f IMAGCDF -c month')
            print ('Define outputpath:	mpconvert.py -r /my/input/path/myfile.bin -w /my/output/path/')
            print ('')
            print ('!!!!  please note: !!!!')
            print ('!!!! using wildcards in filenames (asterix) requires quotes !!!!')

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
        elif opt in ("-m", "--metainfo"):
            metainfo = arg
        elif opt in ("-c", "--coverage"):
            coverage = arg

    if not inputpath:
        print ("mpconvert.py: no input data specified - aborting")
        sys.exit()
    if outputpath == '':
        outputpath = inputpath
        wildcard = False
        try:
            if '*' in os.path.split(outputpath)[1]:
                wildcard = True
        except:
            pass
        if os.path.isfile(outputpath) or outputpath.endswith('*') or wildcard:
            outputpath = os.path.split(outputpath)[0]
        outputpath = os.path.join(outputpath,'out')        
    outputpath = os.path.abspath(outputpath)
    if os.path.isfile(outputpath):
        outputpath = os.path.split(outputpath)[0]
    else:
        # Test whether wpath is already a dirctory
        if not os.path.isdir(outputpath):
            if not os.path.exists(outputpath):
                print ("mpconvert.py: Directory {} not yet existing. Trying to create it...".format(outputpath))
                try:
                    os.makedirs(outputpath)
                    print ("          ... OK")
                except:
                    print ("          ... failed! - check permissions")
                    sys.exit()

            else:
                print ('mpconvert.py: Error: cannot interpret path to write to')
                sys.exit()
    if inputpath == '':
         print ('mpconvert.py: Specify a file/path by the -r option:')
         print ('mpconvert.py: convert.py -r <path2read>')
         print ('mpconvert.py: -- check convert.py -h for more options')
         sys.exit()
    if format_type == '':
        format_type = 'PYASCII'
    else:
        if not format_type in PYMAG_SUPPORTED_FORMATS:
            print ("mpconvert.py: Error: Unkown format! Choose one of the following output formats:")
            for key in PYMAG_SUPPORTED_FORMATS:
                val = PYMAG_SUPPORTED_FORMATS[key]
                if 'w' in val[0] and len(val) > 1:
                    print (" -- {}: {}".format(key, val[1]))
            sys.exit()

    if not coverage in ['month','year','all']:
        coverage = None

    print ("mpconvert.py: Starting conversion to {}:".format(format_type)) 
    print ("             loading data from {}".format(inputpath))
    print ("             and writing it to {}".format(outputpath))
    print ("Reading data ...")
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
        print ("mpconvert.py: Error: No readable data found")
    print ("mpconvert.py: Found {} timesteps ...".format(data.length()[0]))

    if metainfo:
        print ("mpconvert.py: Adding/Replacing provided meta information")
        metalist = metainfo.split(',')
        for pair in metalist:
            metadat = pair.split(':')
            if len(metadat) == 2:
                key = metadat[0]
                value = metadat[1]
                print ("              -> {} = {}".format(key,value))
                data.header[key] = value

    print ("mpconvert.py: Writing data to {} now...".format(outputpath))
    # Check whether wpath is really a path
    data.write(outputpath,format_type=format_type, coverage=coverage)
    print ("conversion finished!")

if __name__ == "__main__":
   main(sys.argv[1:])


