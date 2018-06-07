#!/usr/bin/python

"""
Dealing with credentials. Simple routine to avoid human readable passwords
in scripts.
"""
from __future__ import print_function
from __future__ import unicode_literals

import sys, getopt
from magpy.opt import cred as mpcred

def main(argv):
    view = ''
    typ = ''
    shortcut = ''
    database = ''
    user = ''
    password = ''
    smtp = ''
    address = ''
    host = ''
    port = ''
    try:
        opts, args = getopt.getopt(argv,"hvt:c:d:u:p:s:a:o:l:",["view=","typ=","shortcut=","database=","user=","password=","smtp=","address=","host=","port="])
    except getopt.GetoptError:
        print ('addcred.py -v <listexisting> -t <type> -c <credentialshortcut> -d <database> -u <user> -p <password> -s <smtp> -a <address> -o <host> -l <port>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('Sending data to a remote host by scp or ftp.')
            print ('Requires existing credential information (see cred.py).')
            print ('-------------------------------------')
            print ('Usage:')
            print ('addcred.py -v <listexisting> -t <type> -c <credentialshortcut>')
            print (' -d <database> -u <user> -p <password> -s <smtp> -a <address> -o <host>')
            print (' -l <port>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-v       : view all existing credentials')
            print ('-t       : define type of data: db, transfer or mail')
            print ('-c       : shortcut to access stored information')
            print ('-d       : name of a database for db type')
            print ('-u       : user name')
            print ('-p       : password (will be encrypted)')
            print ('-s       : smtp address for mail types')
            print ('-a       : address for transfer type')
            print ('-o       : host of database')
            print ('-l       : port of transfer protocol')
            print ('-------------------------------------')
            print ('Examples:')
            print ('python addcred.py -t transfer -c zamg -u max -p geheim ')
            print ('                  -a "ftp://ftp.remote.ac.at" -l 21')
            print ('!!!!  please note: put path in quotes !!!!!!')
            sys.exit()
        elif opt in ("-v", "--view"):
            mpcred.sc()
            sys.exit()
        elif opt in ("-c", "--shortcut"):
            shortcut = arg
        elif opt in ("-d", "--database"):
            database = arg
        elif opt in ("-t", "--type"):
            typ = arg
        elif opt in ("-u", "--user"):
            user = arg
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-s", "--smtp"):
            smtp = arg
        elif opt in ("-a", "--address"):
            address = arg
        elif opt in ("-o", "--host"):
            host = arg
        elif opt in ("-l", "--port"):
            port = arg

    if user == '':
        print ('Specify a user using the -u option:')
        print ('-- check addcred.py -h for more options and requirements')
        sys.exit()
    if password == '':
        print ('No password provided!')
    if shortcut == '':
        print ('Specify (and remember) a shortcut using the -c option:')
        print ('-- check addcred.py -h for more options and requirements')
        sys.exit()
    if typ == '':
        print ('Specify a type among db, mail or transfer uing the -t option:')
        print ('-- check addcred.py -h for more options and requirements')
        sys.exit()
    elif typ == 'db':
        if host == '':
            print ('Host required for db, add using -o option')
            sys.exit()
        if database == '':
            print ('Database name required for db, add using -d option')
            sys.exit()
        mpcred.cc('db', shortcut, db=database,user=user,passwd=password,host=host)
    elif typ == 'transfer':
        mpcred.cc('transfer', shortcut, user=user, passwd=password, address=address, port=port)
    elif typ == 'mail':
        mpcred.cc('mail', shortcut, user=user, passwd=password, smtp=smtp)
    else:
        print ('Type  needs to be either db, mail or transfer.')
        print ('-- check addcred.py -h for more options and requirements')
        sys.exit()


if __name__ == "__main__":
   main(sys.argv[1:])


