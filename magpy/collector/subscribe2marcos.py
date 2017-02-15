#!/usr/bin/env python
"""
MARCOS subscription routines and class.
Written by Roman Leonhardt 2017
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import magpy.stream as st
from magpy.database import *

import threading

# provide datainfoid list
# provide update interval
# optional: provide keylist

# def getInitialDataInfo(remotedb, datainfoid):
# -> returns data actuality
# -> sampling rate
# -> available keys
# -> get stream with meta

# def checkLocalDB(localdb, datainfoid, stream): ##### not necessary - done by writeDB
# -> checks existance
# -> eventually adds info in SENSORS, STATIONS and DATAINFO

# def subscribeMARCOS(remotedb, localdb, datainfoidlist, datakeylist = None e.g.[['x','y'],None,['f']], frequency=10)
# -> connect to remote DB
# -> for each datainfoid:
#    - get and check Initial Info
#    - run subscription task to file or db (same datainfoid)


# collector_marcos defines remote and localdb, datainfolist, keylist and interval

class MARCOSsubscription(object):
    """
    Application:
        subscript1 = MARCOSsubscription(parameterlist)
        subscript1.run()
    """
    def __init__(self, remotedb={}, localdb={}, filepath=None, datainfoid=None, keylist=None, interval=10):
        self.remotedb = remotedb
        self.localdb = localdb
        self.rhost = remotedb.get('host','')
        self.ruser = remotedb.get('user','')
        self.rpwd = remotedb.get('passwd','')
        self.rname = remotedb.get('name','')
        self.lhost = localdb.get('host','')
        self.luser = localdb.get('user','')
        self.lpwd = localdb.get('passwd','')
        self.lname = localdb.get('name','')
        self.filepath = filepath
        self.datainfoid = datainfoid
        self.keylist = keylist
        self.interval = interval # period in seconds, how often we read data
        self.samplingrate = 1.0
        self.dataarray = [] # used fro storing binary data - only after 100 points have received
        self.writestream = DataStream()
        self.counter = 0
        self.deltalength, self.len2, self.len1 = 0, 0, 0

        # Check validity of each parameter
        if remotedb:
            try:
                rdb = mysql.connect(host=self.rhost,user=self.ruser,passwd=self.rpwd,db=self.rname)
                rcursor = rdb.cursor()
                rcursor.execute ("SELECT VERSION()")
                row = rcursor.fetchone()
                rcursor.execute ("select @@hostname")
                host = rcursor.fetchone()
                print("Remote MySQL server ({}), Version: {}".format(host[0],row[0]))
                rcursor.close()
                rdb.close()
            except:
                print("Remote MySQL server connection failed")
                remotedb = None

        if localdb:
            try:
                #dbalter(localdb)
                ldb = mysql.connect(host=self.lhost,user=self.luser,passwd=self.lpwd,db=self.lname)
                lcursor = ldb.cursor()
                lcursor.execute ("SELECT VERSION()")
                row = lcursor.fetchone()
                lcursor.execute ("select @@hostname")
                host = lcursor.fetchone()
                print("Local MySQL server ({}), Version: {}".format(host[0],row[0]))
                lcursor.close()
                ldb.close()
            except:
                print("Local MySQL server connection failed")
                localdb = None

    def getInitialDataInfo(self, remotedb, datainfoid):
        """
        obtain any initial information not yet available
        """
        rdb = mysql.connect(host=self.rhost,user=self.ruser,passwd=self.rpwd,db=self.rname)
        try:
            li = dbselect(rdb, 'time', str(datainfoid), expert='ORDER BY time DESC LIMIT 1')
        except:
            print ("subcribeMARCOS: check whether provided datainfoid is really existing")
            li = [] 
        lasttime = datetime.strptime(li[-1],"%Y-%m-%d %H:%M:%S.%f")
        # check validity of time
        if ((datetime.utcnow()-lasttime).days) > 1:
            print ("subscribeMARCOS: no current data available for this dataid")
        # if OK proceed...
        teststream = readDB(rdb, datainfoid, starttime=datetime.strftime(lasttime,"%Y-%m-%d"))  
        print ("subscribeMARCOS: Initiating stream content for {} with length {}".format(self.datainfoid, teststream.length()[0]))

        rdb.close()        
        # add teststream to localdb or file
        return teststream


    def getNewData(self, remotedb, datainfoid, lines):
        """
        obtain any initial information not yet available
        """
        rdb = mysql.connect(host=self.rhost,user=self.ruser,passwd=self.rpwd,db=self.rname)
        if lines <= 1:
            lines = 2 ## get at least two lines
        print ("Getting new data ...")
        teststream = dbgetlines(rdb, datainfoid, lines)
        print ("... found {} datapoints".format(teststream.length()[0]))
        rdb.close()
         
        return teststream

    def appendData(self,stream,keylist,init=False):
        if keylist:
            stream = stream.selectkeys(keylist)
        #print ("Appending", localdb)
        if self.localdb:
            ldb = mysql.connect(host=self.lhost,user=self.luser,passwd=self.lpwd,db=self.lname)
            if init:
                writeDB(ldb,stream)
            else:
                writeDB(ldb,stream,tablename=self.datainfoid)
            ldb.close()
        if self.filepath:
            # TODO finish this method
            self.counter += 1
            if self.counter < 10:
                self.writestream = appendStreams([self.writestream, stream])
                if self.counter == 1:
                    self.len1 = self.writestream.length()[0]
                if self.counter == 9:
                    self.len2 = self.writestream.length()[0]
                    self.deltalength = self.len2-self.len1
                #self.writestream.extend(stream.container,stream.header,stream.ndarray)
                # remove duplicates
                #writeDB(localdb,stream)
                print ("Step", self.counter,self.writestream.length()[0], self.len2, self.len1)
                # ################################################
                # if length not changing then don't write data !!!
                # ################################################
            else:
                # if self.deltalength > 0:
                # get ndarray and convert it to data array for bin data
                dataarray = [[] for key in KEYLIST] 
                dataar = [[] for i in range(len(KEYLIST)+7)] 
                dataarray[0] = np.asarray([datetime.strftime(num2date(date),"%Y-%m-%d %H:%M:%S.%f") for date in self.writestream.ndarray[0]])
                day = datetime.strftime(num2date(self.writestream.ndarray[0][-1]),"%Y-%m-%d")
                #print (self.writestream.ndarray[0])
                #print (dataarray[0])
                packcode = '6hL'
                multiplier = 100000
                multilst = []
                tmpar = np.asarray([self.timeToArray(elem) for elem in dataarray[0]])
                #print (np.asarray(tmpar))
                dataar = np.transpose(np.asarray(tmpar))
                #print (dataar)
                keylst,namelst, unitlst = [],[],[] # list for binary data header
                for elem in NUMKEYLIST:
                    ar = self.writestream.ndarray[KEYLIST.index(elem)]*multiplier
                    if len(ar) > 0:
                        dataar = np.append(dataar,np.asarray([ar]),axis=0)
                        packcode = packcode + 'l'
                        multilst.append(multiplier)
                        keylst.append(elem)
                        namelst.append(stream.header.get('col-'+elem, ''))
                        unitlst.append(stream.header.get('unit-col-'+elem, ''))
                dataar = np.transpose(dataar)
                sensorid = stream.header.get('SensorID','')
                print (self.deltalength, day, str(keylst), unitlst, namelst, stream.header.get('SensorID',''), packcode, multilst, dataar)
                
                #header = "# MagPyBin %s %s %s %s %s %s %d" % (sensorid, str(keylst), str(namelst), str(unitlst), str(multilst), packcode, struct.calcsize(packcode))
                #data_bin = struct.pack(packcode,*dataar)
                #dataToFile(filepath, sensorid, day, data_bin, header)

                print ("Data", self.writestream.length()[0],self.writestream.ndarray[0][0],self.writestream.ndarray[0][-1])  
                self.writestream = stream.copy()
                self.counter, self.deltalength = 0, 0

    def run(self):
        # establish task
        # perform query in defined interval 

        if self.remotedb:
            try:
                startstream = self.getInitialDataInfo(self.remotedb, self.datainfoid)
                self.samplingrate = float(startstream.header.get('DataSamplingRate',''))
            except:
                print ("subscribeMARCOS: error during initialization - aborting")
                sys.exit()
        else:
            print ("No MARCOS database connected - aborting")
            sys.exit()

        if self.interval < self.samplingrate:
            self.interval = self.samplingrate
        
        if self.localdb and startstream.length()[0]>0:
            # save startstream and its metainfo
            self.appendData(startstream,self.keylist,init=True)
            pass
        
        if self.filepath:
            # save startstream as bin
            pass

        self.subscribe2Data()

    def subscribe2Data(self):
        #t1 = datetime.utcnow()
        newstream = self.getNewData(self.remotedb, self.datainfoid, int(self.interval/self.samplingrate))
        if newstream.length()[0]>0:
            self.appendData(newstream,self.keylist)
        threading.Timer(self.interval, self.subscribe2Data).start()

    def timeToArray(self, timestring):
        # Converts time string of format 2013-12-12 23:12:23.122324
        # to an array similiar to a datetime object
        try:
            splittedfull = timestring.split(' ')
            splittedday = splittedfull[0].split('-')
            splittedsec = splittedfull[1].split('.')
            splittedtime = splittedsec[0].split(':')
            datearray = splittedday + splittedtime
            datearray.append(splittedsec[1])
            datearray = map(int,datearray)
            return datearray
        except:
            print('... Error while extracting time array')
            return []

    def dataToFile(self, outputdir, sensorid, filedate, bindata, header):
        # File Operations
        try:
            path = os.path.join(outputdir,sensorid)
            # outputdir defined in main options class
            if not os.path.exists(path):
                os.makedirs(path)
            savefile = os.path.join(path, sensorid+'_'+filedate+".bin")
            if not os.path.isfile(savefile):
                with open(savefile, "wb") as myfile:
                    myfile.write(header + "\n")
                    myfile.write(bindata + "\n")
            else:
                with open(savefile, "a") as myfile:
                    myfile.write(bindata + "\n")
        except:
            print('Error while saving file')


