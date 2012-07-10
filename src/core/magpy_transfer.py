import ftplib
import sys
import thread, time, string, os
import math
import pylab
import numpy as np
import scipy as sp

from scipy import interpolate
from pylab import *
from datetime import datetime
from matplotlib.dates import date2num, num2date

from core.magpy_stream import PyMagLog

# Define defaults:

# ####################
# Check log files
# ####################

def _checklogfile(logfile):
    data=[] 
    try:
       lfile = file(logfile,"r")
       line=lfile.readline()
       while line!="":
          print line
          data.append(line)
          line=lfile.readline()
       lfile.close()
       # Now remove the file
       if len(data) > 0:
          os.remove(logfile)
    except:
       pass
    # delete logfile
    return data

# ####################
# Ftp transfer
# ####################
def ftpdatatransfer (**kwargs):
    """
    Tranfering data to a ftp server
    """
    plog = PyMagLog()
    localpath = kwargs.get('localpath')
    ftppath = kwargs.get('ftppath')
    filestr = kwargs.get('filestr')
    myproxy = kwargs.get('myproxy')
    port = kwargs.get('port')
    login = kwargs.get('login')
    passwd = kwargs.get('passwd')
    logfile = kwargs.get('logfile')

    filelocal = os.path.join(localpath,filestr)
    logpath = os.path.split(logfile)[0]
    logname = os.path.split(logfile)[1]
                            
    try:
        site = ftplib.FTP()
        site.connect(myproxy, port)
        site.set_debuglevel(1)
        msg = site.login(login,passwd)        
        site.cwd(ftppath)
        try:
            site.delete(filestr)
        except:
            #print 'File not present so far'
            pass
        filetosend = open(filelocal,'rb')
        site.storbinary('STOR ' + filestr,filetosend)
        filetosend.close()
        site.quit()
        # Now send missing files from log
        _missingvals(myproxy, port, login, passwd, logpath, logname)
        # Clean up - Remove transferred file
        #os.remove(filestr)
    except:
        plog.addlog(' -- FTP Upload failed - appending %s to missing value logfile' % filestr)
        newline = "\n"
        #os.chdir(logpath)
        lfile = open(os.path.join(logfile),"a")
        lfile.write(path + '  ' + filestr + '  ' + ftppath )
        lfile.write(newline)
        lfile.close()


# ####################
# Transfer missing files
# ####################
def _missingvals(myproxy, port, login, passwd, logfile):
    plog = PyMagLog()
    dat=_checklogfile(logfile)
    nr_lines=len(dat)
    for i in range(nr_lines):
        loginfo = dat[i].split()
        npath = loginfo[0]
        filetosend = loginfo[1]
        nftppath = loginfo[2]
        plog.addlog(' -- Uploading prviously missing vals: %s' % loginfo[1])
        ftpdatatransfer (localpath=npath, ftppath=nftppath, filestr=filetosend, myproxy=myproxy, port=port, login=login, passwd=passwd, logfile=logfile) 

# ####################
# 1. ftp check
# ####################
def ftpdirlist (**kwargs):
    """
    Getting directory listing of ftp server
    """
    plog = PyMagLog()
    
    ftppath = kwargs.get('ftppath')
    myproxy = kwargs.get('myproxy')
    port = kwargs.get('port')
    login = kwargs.get('login')
    passwd = kwargs.get('passwd')
    try:
        site = ftplib.FTP()
        site.connect(myproxy, port)
        site.set_debuglevel(1)
        msg = site.login(login,passwd)        
        site.cwd(ftppath)
        try:
            files=site.nlst()
        except ftplib.error_perm, resp:
            if str(resp) == "550 No files found":
                plog.addwarn("no files in this directory")
            else:
                raise
            pass
        site.quit()
    except:
        plog.addwarn("FTP check failed")
        return

    return files

# ####################
# 2. ftp: remove files 
# ####################
def ftpremove (**kwargs):
    """
    Removing files from ftp server
    """
    plog = PyMagLog()
    ftppath = kwargs.get('ftppath')
    filestr = kwargs.get('filestr')
    myproxy = kwargs.get('myproxy')
    port = kwargs.get('port')
    login = kwargs.get('login')
    passwd = kwargs.get('passwd')
    try:
        site = ftplib.FTP()
        site.connect(myproxy, port)
        site.set_debuglevel(1)
        msg = site.login(login,passwd)        
        site.cwd(ftppath)
        try:
            site.delete(filestr)
        except:
            plog.addwarn('File not present so far')
            pass
        site.quit()
    except:
        plog.addwarn('FTP file removal failed')
        pass
    return

# ####################
# 3. ftp: download files 
# ####################
def ftpget (**kwargs):
    """
    Downloading files from ftp server
    """
    plog = PyMagLog()

    
    localpath = kwargs.get('localpath')
    ftppath = kwargs.get('ftppath')
    filestr = kwargs.get('filestr')
    myproxy = kwargs.get('myproxy')
    port = kwargs.get('port')
    login = kwargs.get('login')
    passwd = kwargs.get('passwd')
    logfile = kwargs.get('logfile')

    downloadpath = os.path.normpath(os.path.join(localpath,filestr))
    if not os.path.exists(localpath):
        raise ValueError, 'Local directory is not existing' 

    try:
        site = ftplib.FTP()
        site.connect(myproxy, port)
        site.set_debuglevel(1)
        msg = site.login(login,passwd)        
        site.cwd(ftppath)
        try:
            # get the remote file to the local directory
            site.retrbinary('RETR %s' % filestr, open(downloadpath,'wb').write)
            #site.delete(filestr)
        except:
            plog.addwarn('Could not download file %s' % filestr)
            pass
        site.quit()
    except:
        plog.addwarn('FTP file download failed')
        pass
    return

