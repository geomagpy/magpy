#!/usr/bin/env python
"""
MagPy-transfer: Procedures for data transfer

Written by Roman Leonhardt 2011/2012
Version 1.0 (from the 23.02.2012)
"""


from core.magpy_stream import *
import ftplib

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
    :type cleanup: bool
    :param cleanup: if true, transfered files are removed from the local directory
    Example:

    ftpdatatransfer(localfile='/home/leon/file.txt', ftppath='/stories/currentdata/radon',myproxy='',login='login',passwd='passwd',logfile='radon.log')
    """
    plog = PyMagLog()
    localfile = kwargs.get('localfile')
    localpath = kwargs.get('localpath')
    ftppath = kwargs.get('ftppath')
    filestr = kwargs.get('filestr')
    myproxy = kwargs.get('myproxy')
    port = kwargs.get('port')
    login = kwargs.get('login')
    passwd = kwargs.get('passwd')
    logfile = kwargs.get('logfile')
    cleanup = kwargs.get('cleanup')

    if not localpath:
        localpath = ''

    if not localfile:
        filelocal = os.path.join(localpath,filestr)
    else:
        localpath = os.path.split(localfile)[0]
        filestr = os.path.split(localfile)[1]
        filelocal = localfile

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
        if cleanup:
            os.remove(filelocal)
        # Now send missing files from log
        _missingvals(myproxy, port, login, passwd, logfile)
    except:
        loggertransfer.warning(' -- FTP Upload failed - appending %s to missing value logfile' % filestr)
        if os.path.isfile(filelocal):
            #plog.addlog(' -- FTP Upload failed - appending %s to missing value logfile' % filestr)
            newline = "\n"
            #os.chdir(logpath)
            lfile = open(os.path.join(logfile),"a")
            lfile.write(localpath + '  ' + filestr + '  ' + ftppath )
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
        loggertransfer.info(' -- Uploading previously missing vals: %s' % loginfo[1])
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
    if not port:
        port = 21
    try:
        print "here"
        site = ftplib.FTP()
        print myproxy, port
        site.connect(myproxy, port)
        print "here1"
        site.set_debuglevel(1)
        msg = site.login(login,passwd)
        print "here2"
        site.cwd(ftppath)
        print "now here"
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
