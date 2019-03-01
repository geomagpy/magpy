#!/usr/bin/env python
"""
DESCRIPTION:
    MagPy credentials functions:
    Methods for storing and accessing user/password information for data bases and ftp accounts. Sensitive data is encrypted.
    For even better security please change the permission by chmod 600 ~/.magpycred

    Credential file looks like:
    # MagPy credentials
    # Database
    db1 = {name = dbname1,host=dbhost1,user=dbuser1,passwd=dbpasswd1}
    db2 = {name = dbname2,host=dbhost2,user=dbuser2,passwd=dbpasswd2}
    ...
    # FileTransfer
    ftname1,ftpasswd1
    ftname2,ftpasswd2
    ...
    # Mailing Adresses
    maname1,masmtp1,maasswd1
    maname2,masmtp2,maasswd2
    ...

    Please note: '#' isnot allowed in names, ',' are not allowed in passwds

METHODS:
    method createcred() creates an encrypted credential file at the user homedirectory (.magpycred)
    method loadcred() reads this credetial file

PARAMETERS:
    None
REQUIREMENTS:
    independent of any other magpy package

EXAMPLE:
    >>> from magpy.opt import cred as mpcred
    >>> from magpy import transfer
    >>> # Creating credentials
    >>> mpcred.cc('db', 'maindatabase', db='mydb',name='max',passwd='secret',host='myhost')
    >>> mpcred.cc('mail', 'firstmail', name='moritz',smtp='schiller.ed',passwd='evenmoresecret')
    >>> mpcred.cc('transfer', 'ftp1', name='friedrich',adress='ftp.find.the.fish',passwd='somehowsecret')
    >>> # Using credentials
    >>> ftpget(ftpname=mpcred.lc(ftp1[name]), etc)
    >>> sendmail(mailname=mpcred.lc(firstmail[name]), mailadress=mpcred.lc(firstmail[adress]), mailpasswd=mpcred.lc(firstmail[passwd]), text='Hello World', attach='mycompleteharddisk')

"""
from __future__ import print_function

import base64
import pickle
import os
from os.path import expanduser
import platform

pyversion = platform.python_version()


def saveobj(obj, filename):
    with open(filename, 'wb') as f:
        pickle.dump(obj,f,pickle.HIGHEST_PROTOCOL)

def loadobj(filename):
    if pyversion.startswith('2'):
        with open(filename, 'r') as f:
            return pickle.load(f) # encoding='latin1' if python3 to read python2 cred
    else:
        with open(filename, 'rb') as f:
            return pickle.load(f,encoding='latin1')

def getuser():
    sysuser = os.getenv("USER")
    # if sysuser could not be identified try Logname
    if sysuser == None:
        print("Getuser: Running from crontab -- trying Logname to identify user")
        try:
            sysuser = os.getenv("LOGNAME").replace("LOGNAME=", "")
        except:
            sysuser = None
        if not sysuser == None:
            print("Getuser: ... succes - using", sysuser)
    # if sysuser still could not be identified assume that uid 1000 is the defaultuser (on linux)
    if sysuser == None or sysuser == 'None':
        print("Getuser: Cannot identify user by standard procedures - switching to default uid 1000")
        sysuser = pwd.getpwuid(1000)[0]
        print("Getuser: now using", sysuser)
    return sysuser
    # path to home directory
    #if os.isatty(sys.stdin.fileno()):
    #    sysuser = os.getenv("USER")
    #    pass
    #else:
    #    sysuser = os.getenv("LOGNAME").replace("LOGNAME=", "")
    #    pass


def cc(typus, name, user=None,passwd=None,smtp=None,db=None,address=None,remotedir=None,port=None,host=None):
    """
    Method for creating credentials
    """
    if not user:
        user = ''
    if not passwd:
        passwd = ''
    if not db:
        db = ''
    if not address:
        address = ''
    if not host:
        host = ''
    if not smtp:
        smtp = ''
    if not port:
        port = ''

    sysuser = getuser()
    home = expanduser('~'+sysuser)
    # previously used expanduser('~') which does not work for root
    credentials = os.path.join(home,'.magpycred')

    try:
        dictslist = loadobj(credentials)
    except:
        print("Credentials: Could not load credentials file - creating it ...")
        dictslist = []
        pass

    foundinput = False
    for d in dictslist:
        if d[0] == name:
            print("Credentials: %s - Input for %s is already existing - going to overwrite it" % (typus, name))
            foundinput = True

    if foundinput:
        dictslist = [elem for elem in dictslist if not elem[0] == name]

    if not user:
        print('Credentials: user missing')
        return
    if not passwd:
        print('Credentials: no password provided')
        #return
    if typus == 'db':
        if not db:
            print('Credentials: database missing')
            return
        if not host:
            print('Credentials: host missing')
            return
        pwd = base64.b64encode(passwd)
        dictionary = {'user': user, 'passwd': pwd, 'db':db, 'host':host}
    if typus == 'mail':
        if not smtp:
            print('Credentials: smtp adress missing')
            return
        pwd = base64.b64encode(passwd)
        dictionary = {'user': user, 'passwd': pwd, 'smtp':smtp}
    if typus == 'transfer':
        if not address:
            print('Credentials: address missing')
            return
        pwd = base64.b64encode(passwd)
        dictionary = {'user': user, 'passwd': pwd, 'address':address, 'port':port }

    dictslist.append([name,dictionary])

    saveobj(dictslist, credentials)
    print("Credentials: Added entry for ", name)
    entries = [n[0] for n in dictslist]
    print("Credentials: Now containing entries for", entries)


def lc(dictionary,value,path=None):
    """
    Load credentials
    """

    if not path:
        sysuser = getuser()
        home = expanduser('~'+sysuser)
        # previously used expanduser('~') which does not work for root
        credentials = os.path.join(home,'.magpycred')
    else:
        credentials = path
    print("Accessing credential file:", credentials)

    try:
        dictslist = loadobj(credentials)
    except:
        print("Credentials: Could not load file")
        return

    for d in dictslist:
        if d[0] == dictionary:
            if 'passwd' in value:
                return base64.b64decode(d[1][value])
            return d[1][value]

    print("Credentials: value/dict not found")
    return

def sc():
    """
    Show credentials
    """

    sysuser = getuser()
    home = expanduser('~'+sysuser)
    # previously used expanduser('~') which does not work for root
    credentials = os.path.join(home,'.magpycred')
    print("Credentials: Overview of existing credentials:")
    try:
        dictslist = loadobj(credentials)
    except:
        print("Credentials: Could not load file")
        return

    for d in dictslist:
        print(d)
    return dictslist

def dc(name):
    """
    Drop credentials for 'name'
    """
    sysuser = getuser()
    home = expanduser('~'+sysuser)
    # previously used expanduser('~') which does not work for root
    credentials = os.path.join(home,'.magpycred')
    try:
        dictslist = loadobj(credentials)
    except:
        print("Credentials: Could not load credentials file - aborting ...")
        return

    foundinput = False
    for d in dictslist:
        if d[0] == name:
            foundinput = True
    if foundinput:
        newdlist = [d for d in dictslist if not d[0] == name]
        saveobj(newdlist, credentials)
        print("Credentials: Removed entry for ", name)
        entries = [n[0] for n in newdlist]
        print("Credentials: Now containing entries for", entries)
    else:
        print("Credentials: Input for %s not found - aborting" % (name))
        return
