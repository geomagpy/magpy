#!/usr/bin/env python
"""
MagPy-transfer: Procedures for data transfer

Written by Roman Leonhardt and Rachel Bailey 2011/2012/2013/2014
Version 1.0 (from the 23.02.2012)
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import division

from magpy.stream import *
import ftplib
import subprocess
import logging
logger = logging.getLogger(__name__)

try:
    import pexpect
    ssh = True
except:
    logger.warning("SSH support not active - please install the pexpect package to use this functionality")
    ssh = False

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
          print(line)
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
    DEFINITION:
        Tranfering data to an ftp server

    PARAMETERS:
    Variables:
        - ftppath:      (str) Path within ftp to send file to.
        - myproxy:      (str) URL/IP to connect to.
        - localfile:    (str) Direct path to file to send.
    Kwargs:
        - cleanup:      (bool) If True, transfered files are removed from the local directory.
        - filestr:      (str) Name of file if in current directory (replaces localfile, combine with localpath).
        - localpath:    (str) Path to folder containing file (replaces localfile, combine with filestr).
        - login:        (str) Login username for ftp server.
        - logfile:      (str) Path to logfile. Failed transfers are listed here to send later.
        - passwd:       (str) Login password for ftp server.
        - port:         (int) Port to connect to, if required.
        - raiseerror:   (bool) If True, raises error when ftp transfer fails.

    RETURNS:
        - Nada.

    EXAMPLE:
        >>> ftpdatatransfer(
                localfile='/home/me/file.txt',
                ftppath='/data/magnetism/this',
                myproxy='www.example.com',
                login='mylogin',
                passwd='mypassword',
                logfile='/home/me/Logs/magpy-transfer.log'
                            )

    APPLICATION:
        >>>
    """
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
    raiseerror = kwargs.get('raiseerror')

    if not localpath:
        localpath = ''

    if not localfile:
        filelocal = os.path.join(localpath,filestr)
    else:
        localpath = os.path.split(localfile)[0]
        filestr = os.path.split(localfile)[1]
        filelocal = localfile

    retrycount = 0
    timeout = None
    connectsuccess = False
    debug = False
    if debug:
        print ("ftpdatatransfer: running ftp transfer")

    try:
        site = ftplib.FTP()
        while retrycount < 5:
            try:
                if debug:
                    print ("Trying: {}, {}".format(retrycount, timeout))
                site.connect(myproxy, port,timeout)
                retrycount = 10
                connectsuccess = True
            except:
                timeout = 10
                retrycount += 1

        if not connectsuccess:
            loggertransfer.error(' -- connection failed 5 times - aborting')
            raise
        if debug:
            print ("ftpdatatransfer: connection establish -- continue")
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
        existing = False
        if os.path.isfile(filelocal):
            #plog.addlog(' -- FTP Upload failed - appending %s to missing value logfile' % filestr)
            newline = "\n"
            #os.chdir(logpath)
            try:
                lfile = open(os.path.join(logfile),"r")
                for line in lfile:
                    if filestr in line:
                        existing = True
                lfile.close()
            except:
                existing = False
            if not existing:
                lfile = open(os.path.join(logfile),"a")
                lfile.write(localpath + '  ' + filestr + '  ' + ftppath )
                lfile.write(newline)
                lfile.close()
        if raiseerror:
            raise


# ####################
# Transfer missing files
# ####################
def _missingvals(myproxy, port, login, passwd, logfile):
    dat=_checklogfile(logfile)
    nr_lines=len(dat)
    for i in range(nr_lines):
        loginfo = dat[i].split()
        npath = loginfo[0]
        filetosend = loginfo[1]
        nftppath = loginfo[2]
        loggertransfer.info(' -- Uploading previously missing vals: %s' % loginfo[1])
        ftpdatatransfer (localpath=npath, ftppath=nftppath, filestr=filetosend, myproxy=myproxy, port=port, login=login, passwd=passwd, logfile=logfile)


if ssh:
    def scptransfer(src,dest,passwd,**kwargs):
        """
        DEFINITION:
            copy file by scp

        PARAMETERS:
        Variables:
            - src:        (string) e.g. /path/to/local/file or user@remotehost:/path/to/remote/file
            - dest:       (string) e.g. /path/to/local/file or user@remotehost:/path/to/remote/file
            - passwd:     (string) users password
        Kwargs:
            - timeout:    (int)  define timeout - default is 30 

        REQUIRES:
            Requires package pexpect

        USED BY:
           cleanup
        """
        timeout = kwargs.get('timeout')

        COMMAND="scp -oPubKeyAuthentication=no %s %s" % (src, dest)

        child = pexpect.spawn(COMMAND)
        if timeout:
            child.timeout=timeout
        child.expect('assword:')   # please not "assword" is correct as it supports both "password" and "Password"
        child.sendline(passwd)
        child.expect(pexpect.EOF)
        print(child.before)

    def ssh_remotefilelist(remotepath, filepat, user, host, passwd):
        """
        DEFINITION:
            login via ssh into remote directory and return list of all files (including path)
            which contain a given file pattern

        PARAMETERS:
        Variables:
            - remotepath:         (string) basepath, all files and directories above are searched.
            - filepat:            (string) filepattern
            - user:       (string) user for ssh login
            - host:       (string) host (IP or name)
            - passwd:             (string) passwd for user

        RETURNS:
           list with full file paths matching filepattern

        USED BY:
           cleanup

        EXAMPLE:
            >>> filelist = ssh_remotefilelist('/path/to/mydata', '.bin', user,host,passwd)
        """

        searchstr = 'find %s -type f | grep "%s"' % (remotepath,filepat)
        COMMAND= "ssh %s@%s '%s';" % (user,host,searchstr)

        child = pexpect.spawn(COMMAND)
        #child.expect('password:')
        child.expect('assword:')   # please not "assword" is correct as it supports both "password" and "Password"
        child.sendline(passwd)
        child.expect(pexpect.EOF)
        result = child.before
        resultlst = result.split('\r\n')
        return resultlst


# ####################
# Upload data to Edinburgh GIN
# ####################
def ginupload(filename=None, user=None, password=None, url=None,**kwargs):
    """
    DEFINITION:
        Method to upload data to the Intermagnet GINs using curl
        tries to upload data to the gin - if not succesful ...
    PARAMETERS:
      required:
        filename        (string) filename including path
        user            (string) GIN user
        password        (string) GIN passwd
        url             (string) url address (e.g. http://app.geomag.bgs.ac.uk/GINFileUpload/Cache)
      kwargs:
        faillog         (bool) if True the current string will be saved once to a TODO file if the upload fails.
                                Additionally the TODO file will be checked and any data within will be tried to be uploaded.
        stdout          (bool) if True the return will be printed
        logpath         (string) provide the full pathname (e.g. C:\MyLogs\myginuploadlog.txt) for the log
    REQUIRES:
        - a working installation of curl
        - the package subprocess
    """

    faillog = kwargs.get('faillog')
    logpath = kwargs.get('logpath')
    stdout = kwargs.get('stdout')
    success = False

    if not logpath:
        logpath = "/tmp/ginupload.log"

    commandlist = []

    # Read any eventually existing file with failed uploads
    # -----------------------------------------------------
    if faillog:
        try:
            lines = [line.rstrip('\n') for line in open(logpath)]
            commandlist = lines
            if stdout:
                print(commandlist)
            os.remove(logpath)
        except:
            if stdout:
                print("No faillog existing")
            pass

    curlstring = 'curl -F "File=@'+filename+';type=text/plain" -F "Format=plain" -F "Request=Upload" -u '+user+':'+password+' --digest '+url
    if not curlstring in commandlist:
        commandlist.append(curlstring)

    # Process each upload
    # -----------------------------------------------------
    for command in commandlist:
        p = subprocess.Popen(command,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, error = p.communicate()
        if stdout:
            print(out, error)
        if faillog:
            if not "Success" in out:
                with open(logpath, 'a') as f:
                    f.write(command+'\n')
        if "Success" in out:
            success = True

    return success

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
        except ftplib.error_perm as resp:
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
        print("here")
        site = ftplib.FTP()
        print(myproxy, port)
        site.connect(myproxy, port)
        print("here1")
        site.set_debuglevel(1)
        msg = site.login(login,passwd)
        print("here2")
        site.cwd(ftppath)
        print("now here")
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

def ftpget(ftpaddress,ftpname,ftppasswd,remotepath,localpath,identifier,port=None,**kwargs):
    """
    Load all files from remotepath to localpath which ends with identifier
    """
    delete = kwargs.get('delete')

    if not port:
        port = 21
    if not delete:
        delete = False
    else:
        delete = True

    print("Starting ftpget ...")
    try:
        ftp = ftplib.FTP()
        print("FTP connecting to ", ftpaddress, port)
        ftp.connect(ftpaddress, int(port))
        print("... successful")
        print("FTP login ", ftpname)
        ftp.login(ftpname,ftppasswd)
        print("... successful")
        print("FTP switching directory ...")
        ftp.cwd(remotepath)
        print("... successful")
        filenames = ftp.nlst()
        #print "Found the following files", filenames

        for filename in filenames:
            if filename.endswith(identifier):
                print(filename)
                loggertransfer.info(' ftpget: Getting file %s' % filename)
                localfilename = os.path.join(localpath,filename)
                with open(localfilename, "wb") as myfile:
                    ftp.retrbinary("RETR " + filename, myfile.write)
                    myfile.close()
                    if delete:
                        ftp.delete(filename)
        ftp.close()
    except:
        print("FTP Connection error - skipping ftpget")
        pass

"""
def ftpget (**kwargs):
    #Downloading files from ftp server
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
"""
