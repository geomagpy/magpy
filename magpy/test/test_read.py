#!/usr/bin/env python
"""
Trying to read all files in a give directory one after the other and report format and success
"""
from __future__ import print_function
local = True
if local:
    import sys
    sys.path.insert(1,'/home/leon/Software/magpy/')
    sys.path.insert(1,'/Users/leon/Software/magpy/')

from magpy.stream import *
import getopt
import fnmatch


def walk_dir(directory_path, filename, date, dateformat,excludelist=[]):
    """
    Method to extract filename with wildcards or date patterns by walking through a local directory structure
    """
    # Walk through files in directory_path, including subdirectories
    pathlist = []
    if filename == '':
        filename = '*'
    if dateformat in ['','ctime','mtime']:
        filepat = filename
    else:
        filepat = filename % date
    #print ("Checking directory {} for files with {}".format(directory_path, filepat))
    for root, _, filenames in os.walk(directory_path):
        for filename in filenames:
            if fnmatch.fnmatch(filename, filepat):
                file_path = os.path.join(root,filename)
                if dateformat in ['ctime','mtime']:
                    if dateformat == 'ctime':
                        tcheck = datetime.fromtimestamp(os.path.getctime(file_path))
                    if dateformat == 'mtime':
                        tcheck = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if tcheck.date() == date.date():
                        pathlist.append(file_path)
                else:
                    pathlist.append(file_path)
    dropexlist = []
    for line in pathlist:
        add = True
        for el in excludelist:
            if line.find(el.strip()) > -1:
                add = False
        if add:
            dropexlist.append(line)

    return dropexlist


def read_test(path,excludelist=[],debug=False):
    faillist = []
    succlist = []
    l = walk_dir(path,"","","",excludelist=excludelist)
    print ("----------------------------------------------")
    for p in l:
        print ("Testing {}".format(p))
        t1= datetime.utcnow()
        data = read(p)
        t2= datetime.utcnow()
        if data.length()[0] > 0:
            print (" -> success for {} in {} seconds".format(data.header.get("DataFormat"), (t2-t1).total_seconds()))
            succlist.append(data.header.get("DataFormat"))
        else:
            print (" -> !! failed")
            faillist.append(p)
    print ("----------------------------------------------")
    print (list(set(succlist)))
    print ("----------------------------------------------")
    print (faillist)

def main(argv):
    version = "1.0.0"
    path = ''
    exclude = ["WIC.txt","/.",".raw",".py","readme","CALY","testflags","example6",".BIN",".bin"]
    debug = False
    filelist = []

    try:
        opts, args = getopt.getopt(argv,"hp:D",["path=","debug=",])
    except getopt.GetoptError:
        print('test_read.py -p <path>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-------------------------------------')
            print('Description:')
            print('test_read.py tries to read data from all supported formats ')
            print('-------------------------------------')
            print('Usage:')
            print('test_read.py -p <directory>')
            print('-------------------------------------')
            print('Options:')
            print('-p            : directory path')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('---------')
            sys.exit()
        elif opt in ("-p", "--path"):
            path = arg
        elif opt in ("-x", "--path"):
            exclude = arg.split(",")
        elif opt in ("-D", "--debug"):
            debug = True

    # Read configuration file
    # -----------------------
    print ("Running test_read.py - version {}".format(version))
    print ("-------------------------------")

    summary = read_test(path,excludelist=exclude,debug=debug)

    print ("----------------------------------------------------------------")
    print ("test_read finished")
    print ("----------------------------------------------------------------")
    print (summary)

if __name__ == "__main__":
   main(sys.argv[1:])

