#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

#sys.stdout.flush()
debug = False # Print every single output
#print( 'available system path is:')
#for el in sys.path:
#    print( 'el is:\t{}'.format( el))

#import obspy as op
try:
    print( 'Trying to import obspy methods for reading...')
    from obspy import read as opread
    from obspy import Stream, UTCDateTime
    print( '\n\n\nsuccessfull...')
except Exception as ex:
    if( debug):
        print( 'Exception is:\n{}'.format( ex))
    print( 'Obspy not available, continuing...')
    pass
from os import listdir, walk
from datetime import datetime, timedelta, timezone
from os.path import isfile, isdir, join, abspath, dirname
from fnmatch import fnmatch
mrpath = dirname( abspath( __file__))
magpyind = mrpath.rfind( 'magpy')
mrpath = mrpath[0:magpyind+5]
#print( 'mrpath is: {}'.format( mrpath))
#sys.path.insert( 0, abspath( '../../'))
sys.path.insert( 0, abspath( mrpath))
from stream import read, datetime, DataStream, loadflags, array2stream
from database import readDB, mysql
from time import sleep
#from database import mysql
del sys.path[0]
if( debug):
    print( 'magpy.stream successfully imported')
from matplotlib.dates import date2num


from scipy.interpolate import interp1d
from itertools import chain

if( debug):
    import matplotlib.pyplot as plt
    print( 'DOC TEXT FROM matplotlib.pyplot - Successfull import of pyplot:\n"{}"'.format( plt.__doc__))
    #sys.exit()
import numpy as np
from numpy import shape, hstack, array, vstack, matrix, pi, nanmax#, where, nanstd

try:
    #sys.path.insert( 0, abspath('../../'))
    sys.path.insert( 0, abspath( mrpath))
    #import mpplot as mp
    import mpplot as mp
    del sys.path[0]
    #print( 'sys.path[0] is: {}'.format( sys.path[0]))
    #if( debug):
    print( 'magpy.mpplot from defined path "{}" successfully imported'.format( abspath( mrpath)))
    #print( mp.__doc__)
    #sys.exit()
except Exception as ex:
    print( 'magpy.mpplot from defined path "{}" import did not work. Interrupted with: {}'.format( abspath( mrpath), ex))
    sys.exit()
    #sys.exit()
######
from matplotlib.dates import num2date

from num2date20time import NUM2DATE20TIME
from stamptodate import STAMPTODATE
#import itertools

import gc
gc.enable()

commonpath = abspath( './')



#sys.exit()
#############################
# CONSTANTS DEF
#############################
mue0 = 4.0*pi*pow(10,-7)
trafofacE = np.power(10.0,-6.0) ##### Volt / digits
eledist = 60.0 # Elextrodenabstand sofern beide gleich sind
pico = np.power(10.0, -12.0)
nano = np.power(10.0, -9.0)
micro = np.power(10.0, -6.0)
#############################
# TEST
#############################
#startdate = datetime.strptime("2017-03-09", "%Y-%m-%d")
#enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
#enddate = datetime.strptime("2017-03-10", "%Y-%m-%d")
#############################




class MR:
    """
    MR (former MULTIREAD), V1.3
    
    
    Reading is done for:
        MiniSeed - through obspy
        Super-Gradiometer files - through geomagpy
        Super-Gradiometer database-token - through geomagpy
        LEMI025 variometer data - through geomagpy -- under construction ---
        LEMI036 variometer data - through geomagpy
        Geomagpy DataStream() objects - through geomagpy
    Including a timestamp referred to 
    YYYY-MM-DD-HH-mm-ss 0000-01-01-00-00-00 == 0.0
    called a zerotime and datetime.datetime objects as timestamps
    
    "zerotime" calculations are based on:
    
    Wiki-explanations on "Gregorian Calendar - 
    https://de.wikipedia.org/wiki/Gregorianischer_Kalender --> 
    === Schaltregeln ===
    Die julianische Schaltregel wird im gregorianischen Kalender mit Hilfe 
    zweier weiterer Regeln relativiert: Die durch vier ganzzahlig teilbaren 
    Jahre der [[Christliche Zeitrechnung|christlichen Zeitrechnung]] sind wie 
    bisher im julianischen Kalender Schaltjahre. Da jedes vierte Kalenderjahr 
    einen Tag länger als die 365 Tage langen, dazwischen liegenden 
    [[Gemeinjahr]]e ist, beträgt die mittlere Länge eines Kalenderjahres 
    365,25 Tage, was aber gegenüber dem [[Tropisches Jahr|tropischen Jahr]] mit 
    365,24219 Tagen zu lang ist (einen Tag Abweichung nach 128 Jahren).
    In den durch 100 ganzzahlig teilbaren (und damit auch durch vier ganzzahlig 
    teilbaren) Jahren (1600, 1700, 1800, 1900, 2000, 2100 usw.) entfällt 
    entgegen der ersten (julianischen) Regel der Schalttag, so dass das 
    mittlere Kalenderjahr mit 365,24 Tagen nur noch etwa 0,0022 Tage vom 
    tropischen Jahr abweicht, etwas zu kurz ist (einen Tag Abweichung nach 
    457 Jahren). Die ganzzahlig durch 400 teilbaren (und damit auch durch 100 
    ganzzahlig teilbaren) Jahre (1600, 2000 usw.) sind entgegen der zweiten und 
    in Übereinstimmung mit der ersten Regel Schaltjahre. Die mittlere Länge des 
    Kalenderjahres ergibt sich dadurch zu 365,2425 Tagen. Die verbleibende 
    Differenz 0,00031 Tage zum mittleren tropischen Jahr wurde von den 
    Reformern als vernachlässigbar klein hingenommen. Die Abweichung wird erst 
    nach etwa 3225 Jahren einen Tag betragen."
    
    
    
    V1.1:
        - Added common __GetTimeInd__(self, colmp) function returns:
            self.TimeInd
        - Added common __GetDataInd__(self, colmp, **kwargs) function returns:
            self.DataInd, self.DataColNames
        - Added common __EquiInterpol__(self, **kwargs) function returns:
            intptime, intpdata
    
    
    
    V1.2:
        - Added ColList option to get only specific columns in stream
        - Added ApplyFlag option and flagpath option to implement magpy-flags
        information
        - Added Magpy filter according to InterMagnet recommendation
        -- under construction --
    
    
    
    V1.3:
        - Added a functionality to get the header line entries with
        __GetColHeaderInfo__. Is still not implemented in every reading routine.
        !!! UNDER CONSTRUCTION !!!
        So far only implemented for SingleAbsRead routine
        results are returned in self.HeaderInfo variable
    
    
    V1.4:
        - Added a functionality to read GFZ Kp files
        - Added a functionality to get the header line entries with
        __GetColHeaderInfo__. Is still not implemented in every reading routine.
        !!! UNDER CONSTRUCTION !!!
        So far only implemented for SingleAbsRead routine
        results are returned in self.HeaderInfo variable
        
        - Added a functionality to read WIC definitive second and minute data
          WICDefDataRead ... Header info is mayby somehow unappropriate for
          data read, cause there is NO sensorname, sensorrevision or sensor-
          serialnummer available in definitive data
            
        
    ----------
    
    Attributes
    ----------
        
        starttime:
            datetime.object 
            Depicting the starting time of dataset or defines the start time
            for cutting timerange of Magpy DataStream() object.
        
        endtime: 
            datetime.object 
            Depicting the ending time of dataset or defines the end time
            for cutting timerange of Magpy DataStream() object.
        
        sensortype: 
            str
            Naming the type of sensorfile or sensor-database token to be read.
            Valid is:
                GradRead
                dBGrad
                VarioRead
                SingleAbsRead
                dBVario
                mpstream
                MiniSeed --- under construction ---
                picologtxt --- under construction ---
                WICDefDataRead
        data:
            Geomagpy DataStream() object
            Geomagpy DataStream() object which is will be converted to
            "data" and "zerotime" ouput. All columns of DataStream().ndarray
            will be taken as long as the length of the column is longer than
            one sample long.
        
        path:
            os.path object
            Common path of files to be investigated
        
        SrcStr:
            str
            Search-string to look for in path and it's subdirectories
            Is set default to "LEMI036" if sensortype == "VarioRead" and
            set to "GP20S3" if sensortype == "GradRead"
            Cannot be applied to "picologtxt" sensortype.
        
        channels:
            List of strings
            Strings in list are used to search for more specific filetype
            endings. For Seed files use empty string e.g.: channels = ''
            Default is:
                'bin'
        
        TrF ( optional):
            Boolean
            Apply Volt/Digit - transformation factor to MiniSeed data if 
                True
            Don't apply if
                False
            Default is:
                False
        
        TrFVal:
            float
            Specifically define transformation factor for MiniSeed data.
            Default value is:
                self.trafoval = 10.0**(-6.0) Volt/ Digit
        
        ColList:
            List of Strings with column names to look for
            --- under construction ---
        
        ApplyFlags (optional):
            Boolean
            True: Applying defined flags from flagfile
            False: do nothing ... default
        
            flagpath (required):
                os.path object
                Path of files containing the flags in json file-format.
            Will replace any flaginformaton col-name with those listed in ColList
        
        InterMagFilter (optional):
            Boolean
            True: Applying filter according to InterMagnet recommendations
            False: do nothing ... default
    
    Methods
    -------
        
        Positions: !!! applies to Supergrad-data until now !!!
            Applies to defined Gauss-Krueger coordinates in referrence
            frame to sensor data for further use in calculations of
            offsets, etc.
        
        GetData: 
            Performs read of data for given "sensortype" from "starttime" to 
            "endtime" and derives zerotime for given datetime.objects within 
            that timerange
        
        stationinfo:
            Returns information about the sensor beeing read.
            Using either 
            [ obspy.meta.network, obspy.meta.station, obspy.meta.channel]
            or
            magpy.header['SensorID']
            depending on which kind of sensor data is beeing read
        
        HeaderInfo:
            Returns information about the sensor header info beeing read.
            Using either DataCont.HeaderInfo to read available header entries.
            Then compare with read columns and select appropriately.
            !!!! UNDER CONSTRUCTION !!!!
        
    Returns
    -------
        
        zerotime: 
            numpy.array
            Array of equidistantly spaced zerotime values for given 
            datetime.objects
        
        data:
            numpy.array
            Array of datavalues equidistantly spaced for every column in 
            "sensortype"-data
        
        sensPos:
            numpy.array
            Array of sensor-short name and Gauss-Krueger coordinates in 
            referrence frame for further use in calculations of
            offsets, etc.
        
        mydate:
            datetime.datetime
            Array of datetime.datetime objects corresponding to the timestamps
            in zerotime
        
    Example
    -------
        
        from mr import MR
        import datetime as datet
        
        
        pathstring = './'
        
        starttime = datet.datetime( year = 2019, month = 8, day = 24)
        endtime = starttime + datet.timedelta( hours = 1)
        
        
        sensortype = 'GradRead'
        DataCont = MR(starttime = starttime, endtime = endtime, sensortype = 
                      sensortype, path = os.path.abspath( pathstring))
        zerotime, data = DataCont.GetData()
        
        
        
        sensortype = 'VarioRead'
        DataCont = MR(starttime = starttime, endtime = endtime, sensortype = 
                      sensortype, path = os.path.abspath( pathstring), 
                      ColList = ['x', 'y', 'z'])
        zerotime, data = DataCont.GetData()
        stationinfo = DataCont.stationinfo
        
        
        
        sensortype = 'dBVario'
        DataCont = MR(starttime = starttime, endtime = endtime, sensortype = 
                      sensortype, hostip = '112.22.128.135', usr = 'myuser',
                      pw = 'mypw', db = 'mydb')
        zerotime, data = DataCont.GetData()
        
        
        
        sensortype = 'dBRead'
        DataCont = MR(starttime = starttime, endtime = endtime, sensortype = 
                      sensortype, hostip = '112.22.128.135', usr = 'myuser',
                      pw = 'mypw', db = 'mydb')
        zerotime, data = DataCont.GetData()
        
        
        
        sensortype = 'mpstream'
        DataCont = MR(starttime = starttime, endtime = endtime, sensortype = 
                      sensortype, data = magpy.stream.DataStream())
        zerotime, data = DataCont.GetData()
        
        
        
        sensortype = 'SingleAbsRead'
        commonColList = ['x', 'y', 'z']
        commonchannels = 'cdf'
        commonsearchstring = 'GP20S3NS_012201_0001_'
        DataCont = MR(starttime = starttime, endtime = endtime, 
                      sensortype = sensortype, SrcStr = searchstring, 
                      path = os.path.abspath( pathstring), 
                      channels = commonchannels, 
                      ColList = commonColList, ApplyFlags = True,
                      flagpath = flagpath)
        zerotime, data = DataCont.GetData()
        ######
        # STILL UNDER CONSTRUCTION
        ######
        HeaderInfo = DataCont.HeaderInfo # not available in every routine now!!
        label = []
        for ColListname, sensname in HeaderInfo:
            if( ColListname in commonColList):
                label.append( sensname)
        ######
        
        
        sensortype = 'MiniSeed'
        DataCont = MR(starttime = starttime, endtime = endtime,
                      sensortype = sensortype, 
                      path = os.path.abspath( pathstring), 
                      channels = ['pri0', 'pri1'], 
                      SrcStr = str( 'e604616'), 
                      TrF = True, TrFVal = 0.00000000001)
        zerotime, data, sensPos, mydate = DataCont.GetData()
        DataCont.stationinfo # gets MiniSeed Stationinfo consisting of
        DataCont.stationinfo = 
            [DataCont.meta.network, DataCont.meta.station, DataCont.channel]
        
    """
    
    
    
    def MyException( self, ex):
        """
        raise self.MyException("My Exception test is...")
        """
        for el in ex:
            print( 'Exception "{}" has occured.'.format( el))
            #sys.exit()
        sys.exit()
        #pass
    
    
    
    def __init__( self, **kwargs):
        print( '__init__ just started...')
        try:
            self.filetype = 'bin'
            self.trafoval = trafofacE
            self.trafofac = False
            self.ColList = []
            self.ApplyFlags = False
            self.InterMagFilter = False
            self.DataSourceHeader = []
            for item, value in kwargs.items():
                if( item == 'starttime'):
                    """
                    
                    get starttime to be investigated
                    """
                    self.starttime = value
                    if( debug):
                        print( 'self.starttime is: {}'.format( self.starttime))
                if( item == 'endtime'):
                    """
                    
                    get endtime to be investigated
                    """
                    self.endtime = value
                    if( debug):
                        print( 'self.endtime is: {}'.format( self.endtime))
                if( item == 'sensortype'):
                    """
                    
                    get sensortype to be investigated
                    """
                    self.sensortype = value
                    if( self.sensortype == 'mpstream'):
                        data_set = False
                        for it, val in kwargs.items():
                            if( it == 'data'):
                                self.data = val
                                data_set = True
                        if( data_set):
                            print( '\n\n\nMissing "data" keyword and ndarray...stopping')
                            sys.exit()
                    elif( self.sensortype.startswith( 'dB')):
                        """
                        Database read through :
                        
                        db = mysql.connect( host = self.dbIP, user=self.dbUsr, passwd = self.dbPswd, db = self.dbName)
                        """
                        for it, val in kwargs.items():
                            if( it == 'hostip'):
                                """
                                do .....
                                
                                set Host-IP-Adress of mysql database to be investigated
                                """
                                self.dbIP = val
                                if( debug):
                                    print( 'self.dbIP is: {}'.format( self.dbIP))
                            if( it == 'usr'):
                                """
                                do .....
                                
                                set username of mysql database to be investigated
                                """
                                self.dbUsr = val
                                if( debug):
                                    print( 'self.dbUsr is: {}'.format( self.dbUsr))
                            if( it == 'pw'):
                                """
                                do .....
                                
                                set password of mysql database to be investigated
                                """
                                self.dbPswd = val
                                if( debug):
                                    print( 'self.dbPswd is: {}'.format( self.dbPswd))
                            if( it == 'db'):
                                """
                                do .....
                                
                                set database name of mysql database to be investigated
                                """
                                self.dbName = val
                                if( debug):
                                    print( 'self.dbName is: {}'.format( self.dbName))
                    else:
                        if( not self.sensortype.startswith( 'dB') and not self.sensortype.startswith( 'mp')):
                            try:
                                """
                                
                                get Searchstring and search path
                                """
                                if( self.sensortype == 'GradRead'):
                                    self.SearchString = 'GP20S3'
                                elif( self.sensortype == 'VarioRead'):
                                    self.SearchString = 'LEMI036'
                                elif( self.sensortype == 'G823A'):
                                    self.SearchString = 'G823A'
                                elif( self.sensortype == 'GSM90'):
                                    self.SearchString = 'GSM90_1013973'
                                elif( self.sensortype == 'GSM19'):
                                    self.SearchString = 'GSM19_6067473'
                                elif( self.sensortype == 'picologtxt'):
                                    self.SearchString = 'picologtxt'
                                elif( self.sensortype == 'GFZKpRead'):
                                    self.SearchString = 'GFZKpRead'
                                elif( self.sensortype == 'IAGAtxt'):
                                    self.SearchString = ''
                                for it, val in kwargs.items():
                                    if( it == 'path'):
                                        """
                                        do .....
                                        
                                        get common filepath to be investigated
                                        """
                                        self.path = val
                                        if( debug):
                                            print( 'self.path is: {}'.format( self.path))
                                    if( it == 'SrcStr'):
                                        """
                                        
                                        
                                        define specific search string besides self.filetype
                                        """
                                        if( self.sensortype != 'picologtxt'):
                                            self.SearchString = val
                                            if( debug):
                                                print( 'self.SearchString', self.SearchString)
                                        else: # SEARCH FOR picologtxt files beginning with date as name
                                            date = self.starttime
                                            val = str( date.year) + '_' + str( '{:02d}'.format( date.month)) + '_' + str('{:02d}'.format( date.day)) + '_' + str('{:02d}'.format( date.hour)) + str('{:02d}'.format( date.minute)) + str('{:02d}'.format( date.second))
                                            self.SearchString = val
                            except Exception as ex:
                                #print( 'Sensortype is: {}. Filepath missing...stopping!'.format( self.sensortype))
                                self.MyException(ex)
                        elif( self.sensortype.startswith( 'mp')):
                            if( self.sensortype == 'mpascii'):
                                try:
                                    self.SearchString = 'MPascii'
                                    #print( 'init self.SearchString with : {}'.format( self.SearchString))
                                    #sys.exit()
                                    for it, val in kwargs.items():
                                        if( it == 'path'):
                                            """
                                            do .....
                                            
                                            get common filepath to be investigated
                                            """
                                            self.path = val
                                            if( debug):
                                                print( 'self.path is: {}'.format( self.path))
                                        if( it == 'SrcStr'):
                                            """
                                            
                                            
                                            define specific search string besides self.filetype
                                            """
                                            if( self.sensortype != 'picologtxt'):
                                                self.SearchString = val
                                                if( debug):
                                                    print( 'self.SearchString', self.SearchString)
                                            else: # SEARCH FOR picologtxt files beginning with date as name
                                                date = self.starttime
                                                val = str( date.year) + '_' + str( '{:02d}'.format( date.month)) + '_' + str('{:02d}'.format( date.day)) + '_' + str('{:02d}'.format( date.hour)) + str('{:02d}'.format( date.minute)) + str('{:02d}'.format( date.second))
                                                self.SearchString = val
                                except Exception as ex:
                                    #print( 'Sensortype is: {}. Filepath missing...stopping!'.format( self.sensortype))
                                    self.MyException(ex)
                    if( debug):
                        print( 'self.sensortype is: {}'.format( self.sensortype))
                if( item == 'channels'):
                    """
                    
                    get endtime to be investigated
                    """
                    self.filetype = value
                    if( debug):
                        print( 'self.filetype is: {}'.format( self.filetype))
                if( debug):
                    print( 'item is: {}\t: {}'.format( item, value))
                
                if( item == 'TrF'):
                    """
                    
                    set to apply the transformation factor for Miniseed data
                    or not
                    """
                    self.trafofac = value
                    if( debug):
                        print( 'self.trafofac is: {}'.format( self.trafofac))
                    for it, val in kwargs.items():
                        if( it == 'TrFVal'):
                            """
                            
                            get transformation factor for Miniseed data or leave it
                            as default
                            """
                            self.trafoval = val
                            if( debug):
                                print( 'self.trafoval is: {}'.format( self.trafoval))
                if( item == 'ColList'):
                    """
                    define Columns to read
                    """
                    self.ColList = value
                    if( debug):
                        print( 'self.ColList is: {}'.format( self.ColList))
                
                if( item == 'ApplyFlags'):
                    """
                    
                    get endtime to be investigated
                    """
                    self.ApplyFlags = value
                    if( debug):
                        print( 'self.ApplyFlags is: {}'.format( self.ApplyFlags))
                if( item == 'flagpath'):
                    """
                    
                    get endtime to be investigated
                    """
                    self.flagpath = value
                    if( debug):
                        print( 'self.flagpath is: {}'.format( self.flagpath))
                if( item == 'InterMagFilter'):
                    """
                    
                    get endtime to be investigated
                    """
                    self.InterMagFilter = value
                    if( debug):
                        print( 'self.InterMagFilter is: {}'.format( self.InterMagFilter))
                
                
                
                
            """
            if( item == 'filetype'):
                try:
                    self.filetype = value
                except Exception as ex:
                    print( 'filetype is: {}. Filepath missing...stopping!'.format( self.filetype))
            """
            if( debug):
                for el in dir( self):
                    print( 'element in self is:\t{}'.format( el))
                #sys.exit()
            #print( '{}\{}\{}\{}'.format( self.sensortype, self.starttime, self.endtime, self.filetype, ))
            
        except Exception as ex:
            #print( 'An expection occured.\tExepction is:\n\t{}'.format( ex))
            self.MyException( ex)
            #sys.exit()
    
    
    
    def __dBInit__( self):
        
        
        
        ##################
        # CONNECT TO DATABASE
        ##################
        #mrpath = dirname( abspath( __file__))
        #magpyind = mrpath.rfind( 'magpy')
        #mrpath = mrpath[0:magpyind+5]
        #print( 'mrpath is: {}'.format( mrpath))
        #sys.path.insert( 0, abspath( '../../'))
        #sys.path.insert( 0, abspath( mrpath))
        
        #del sys.path[0]
        from inspect import stack, getmodule
        #from magpy.stream import num2date
        #import os
        import subprocess as sp
        
        
        
        if( debug):
            print( '\n\n\nConnecting to database {} at: {}, from module: {}'.format( self.dbIP, datetime.utcnow(), getmodule( stack()[1][0])))
            response = sp.call(["ping", "-c", "1", "127.0.0.1"], stdout=sp.PIPE, stderr=sp.PIPE)
            print( 'response is: {}'.format( response))
            #sys.exit()
        
        try:
            print( 'Using primary database...')
            #response = os.system("ping -c 1 " + self.dbIP)
            response = sp.call(["ping", "-c", "1", self.dbIP], stdout=sp.PIPE, stderr=sp.PIPE)
            
            #and then check the response...
            if response == 0:
                print( 'Primary host at {} is reachable'.format( self.dbIP))
                try:
                    db = mysql.connect( host = self.dbIP, user=self.dbUsr, passwd = self.dbPswd, db = self.dbName)
                    self.dB = db
                    print( '\nConnected to database at: {}'.format( datetime.utcnow()))
                except:
                    print( 'Cannot connect to database on {}!'.format( self.dbIP))
            else:
                print( 'Host: {} is NOT reachable...switching'.format( self.dbIP))
                sys.exit()
        except:
            try:
                print( '\tUsing backup database...')
                self.dbIP = "138.22.188.191"
                #response = os.system("ping -c 1 " + self.dbIP)
                response = sp.call(["ping", "-c", "1", self.dbIP], stdout=sp.PIPE, stderr=sp.PIPE)
                #and then check the response...
                if response == 0:
                    print( '\tBackup host at {} is reachable'.format( self.dbIP))
                    try:
                        db = mysql.connect( host = self.dbIP, user=self.dbUsr, passwd = self.dbPswd, db = self.dbName)
                        self.dB = db
                        print( '\n\tConnected to database at: {}'.format( datetime.utcnow()))
                    except:
                        print( 'Cannot connect to database on {}!'.format( self.dbIP))
                else:
                    print( '\tHost: {} is NOT reachable...switching'.format( self.dbIP))
                    sys.exit()
            except:
                try:
                    print( '\t\tUsing local database...')
                    self.dbIP = "127.0.0.1"
                    #response = os.system("ping -c 1 " + self.dbIP)
                    response = sp.call(["ping", "-c", "1", self.dbIP], stdout=sp.PIPE, stderr=sp.PIPE)
                    #and then check the response...
                    if response == 0:
                        print( '\t\tLocal host at {} is reachable'.format( self.dbIP))
                        try:
                            db = mysql.connect( host = self.dbIP, user=self.dbUsr, passwd = self.dbPswd, db = self.dbName)
                            self.dB = db
                            print( '\n\t\tConnected to database at: {}'.format( datetime.utcnow()))
                        except Exception as ex:
                            print( '\t\tCannot connect to database on {}!'.format( self.dbIP))
                            print( '\t\tExeption is:\t{}!'.format( ex))
                            sys.exit()
                    else:
                        print( '\t\tHost: {} is NOT reachable!'.format( self.dbIP))
                        sys.exit()
                except Exception as ex:
                    print( '\t\t\tNo Connection to localhost database...stopping with exception:\n{}'.format( ex))
                    sys.exit()
        return
    
    
    
    def __getdays__( self):
        startdate = self.starttime
        enddate = self.endtime
        mytimeitercheck = timedelta( seconds = int( ( enddate - startdate).total_seconds()))
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        #sys.exit()
        print( 'total seconds to process {}'.format( mytimeitercheck.total_seconds()))
        if( float( mytimeitercheck.total_seconds()) < 0.0):
            print( 'endtime before starttime! STOPPING!')
            sys.exit()
        days = []
        while startdate <= enddate:
            
            if( len( days) == 0):
                days.append( startdate)
                #startdate += mytimeitercheck
                print( days)
            else:
                if( startdate.day != days[-1].day):
                    print( startdate)
                    days.append( startdate)
            startdate += mytimeitercheck
        self.days = days
        return
    
    
    def __LookForFilesMatching__( self):
        
        mydirs = []
        myfiles = []
        walkout = walk( self.path)
        for root, b, el in walkout:
            #print( 'a {}\tb {}\tel {}'.format( a, b, el))
            for name in el:
                #print( 'a {}\tb {}\tel {}'.format( root, b, el))
                checkvar = join( root, name)
                if( debug):
                    print( 'checkvar is: {}'.format( checkvar))
                    #sys.exit()
                if( isdir( checkvar)):
                    mydirs.append( checkvar)
                elif( isfile( checkvar)):
                    myfiles.append( checkvar)
        
        if( debug):
            print( 'List of directories is:')
            for k, el in enumerate( mydirs):
                print( 'directory[{}] is: {}'.format( k, el))
        if( debug):
            print( 'List of files is:')
            for k, el in enumerate( myfiles):
                print( 'File[{}] is: {}'.format( k, el))
        #sys.exit()
        if( debug):
            print( '\n\n\nLength of myfiles is: {}\n\n\n'.format( len( myfiles)))
        gdfiles = []
        if( len( myfiles) > 1):
            if( debug):
                print( '\n\n\nFollowing files found:\n\n\n')
            for name in myfiles:
                if( debug):
                    print( 'Checking filename:\n{}: {}\n'.format( name, '*' + self.SearchString + '*'))
                if( fnmatch( name, '*' + self.SearchString + '*')):
                    directory,filename = os.path.split( name)
                    if( filename not in [ os.path.split( f)[1] for f in gdfiles]):
                        gdfiles.append( name)
                        if( debug):
                            print( 'Filename:\t{} ...matching\n'.format( name))
                            #sys.exit()
        else:
            #print( 'No files for {} found...stopping'.format( self.SearchString))
            self.MyException( 'No files for {} found...stopping'.format( self.SearchString))
        #sys.exit()
        if( debug):
            print( 'used files:\n\n\n')
            for el in gdfiles:
                print( el)
        
        #sys.exit()
        self.files = gdfiles
        return
    
    
    
    def __GetSensName__(self, colmp):
        self.sensname = []
        #print( 'colmp.header...modules\n')
        #for el in colmp.header.itervalues():
        #    print( el)
        #sys.exit()
        for k, f in zip( colmp.header.keys(), colmp.header.values()):
            if( debug):
                print( k, f)
            if( k.startswith('unit-col') and not k.endswith('time')):
                colnameind = k.rindex( '-') + 1
                self.sensname.append( tuple( [k[colnameind:], f]))
        if( debug):
            print( 'self.sensname is:\t{}'.format( self.sensname))
        return self.sensname
    
    
    
    def __GetColHeaderInfo__(self, colmp): # NEED MANUAL PART IN THE BEGINNING!!!
        self.HeaderInfo = []
        #print( 'colmp.header...modules\n')
        #for el in colmp.header.itervalues():
        #    print( el)
        #sys.exit()
        #print( colmp.header.keys())
        #print( colmp.header.values())
        #sys.exit()
        for k, f in zip( colmp.header.keys(), colmp.header.values()):
            if( debug):
                print( k, f)
            if( k.startswith('col') and not k.endswith('time')):
                colnameind = k.rindex( '-') + 1
                self.HeaderInfo.append( tuple( [k[colnameind:], f]))
        if( debug):
            print( 'self.HeaderInfo is:\t{}'.format( self.HeaderInfo))
        #sys.exit()
        return self.HeaderInfo
    
    
    
    def __GetTimeInd__(self, colmp):
        self.Timeind = 0
        self.missing = False
        try:
            tI = colmp.KEYLIST.index('time')
            try:
                testlen = len( colmp.ndarray[tI])
                if( debug):
                    print( 'testlen is:\t{}'.format( testlen))
                if( testlen < 2):
                    raise Exception( 'length of timestamps < 2!...')
                else:
                    print( 'Primary timestamps are valid')
            except:
                try:
                    tI = colmp.KEYLIST.index('sectime')
                    try:
                        testlen = len( colmp.ndarray[tI])
                        if( testlen < 2):
                            raise Exception( 'length of timestamps < 2!...stopping')
                        else:
                            print( 'Secondary timestamps are valid')
                    except Exception as ex:
                        self.MyException( ex)
                        self.missing = True
                        sys.exit()
                    
                    
                except Exception as ex:
                    print( 'No valid timestamps. Exception:\n{}'.format( ex))
                    self.missing = True
                    sys.exit()
        except:
            try:
                tI = colmp.KEYLIST.index('sectime')
                print( 'Secondary timestamps are valid')
            except:
                try:
                    #print( )
                    tI = np.argwhere( [ e.startswith( 'Time') for e in colmp[0].split('\t')])[0][0]
                except Exception as ex:
                    print( 'No valid timestamps. Exception:\n{}'.format( ex))
                    self.missing = True
                    sys.exit()
        self.TimeInd = tI
        return
    
    
    
    def __GetDataInd__(self, colmp, **kwargs):
        
        if( len( kwargs.items()) > 0):
            for item, val in kwargs.items():
                if( item == 'ColList'):
                    Columns = val
            index_vec = []
            if( type( colmp) == DataStream):
                for el in colmp.KEYLIST:
                    if( el in Columns):
                        index_vec.append( colmp.KEYLIST.index( el))
                        if( debug):
                            print( 'el in KEYLIST is:\t{}'.format( el))
                if( debug):
                    print( 'length of index_vec is:\t{}'.format( len( index_vec)))
                #index_vec.remove( colmp.KEYLIST.index( 'time'))
                if( debug):
                    print( 'length of index_vec is:\t{}'.format( len( index_vec)))
                time = colmp.ndarray[self.TimeInd]
                #print( np.shape( index_vec))
                #sys.exit()
            else:
                #print( colmp)
                #sys.exit()
                ncolmp = []
                for row in colmp:
                    #row = row[::]
                    row = row.split('\t')
                    ncolmp.append( row)
                colmp = ncolmp
                del ncolmp
                #colmp = colmp[1::,:]
                for el in colmp:
                    #print( el)
                    #print( Columns)
                    #sys.exit()
                    #print( shape(colmp))
                    #print( shape(el))
                    #print( el)
                    #print( Columns)
                    #print( type( el))
                    #print( type( Columns))
                    #print( all( item in el for item in Columns))
                    #sys.exit()
                    checkvar = [item in el for item in Columns]
                    #print( checkvar)
                    #sys.exit()
                    if( all( checkvar)):
                        print( el)
                        #sys.exit()
                        index_vec.append( list( np.argwhere( [item in Columns for item in el]).flatten()))
                        #print( index_vec[-1])
                        #sys.exit()
                        if( debug):
                            print( 'el in KEYLIST is:\t{}'.format( el))
                if( debug):
                    print( 'length of index_vec is:\t{}'.format( len( index_vec)))
                #index_vec.remove( colmp.KEYLIST.index( 'time'))
                if( debug):
                    print( 'length of index_vec is:\t{}'.format( len( index_vec)))
                print( self.TimeInd)
                print( colmp[0])
                time = colmp[self.TimeInd]
            timlen = np.shape( time)[0]
            #index_vec = index_vec[0]
            print( 'index_vec:', index_vec)
            #sys.exit()
            ###
            # check for goodind: indices which ndarray output is equally long than the "time" ndarray
            goodind_vec = []
            bakcolmp = colmp
            if( type( colmp) == DataStream):
                colmp = colmp.ndarray[index_vec]
                for el, ind in zip( colmp, index_vec):
                    if( debug):
                        print( 'shape of colmp-el of index {} is: {}'.format( np.shape( el), ind))
                    if( np.shape( el)[0] == timlen):
                        try:
                            dump = np.nanmean( el.astype(float)) # check if the values can be transformed to floats
                            goodind_vec.append( ind)
                        except:
                            pass
                self.DataInd = goodind_vec
                ColNames = []
                for k, el in enumerate( bakcolmp.KEYLIST):
                    if( k in index_vec):
                        ColNames.append( el)
                self.DataColNames = ColNames
            else:
                for el, ind in zip( colmp, index_vec):
                    if( debug):
                        print( 'shape of colmp-el of index {} is: {}'.format( np.shape( el), ind))
                    if( np.shape( el)[0] == timlen):
                        try:
                            dump = np.nanmean( el.astype(float)) # check if the values can be transformed to floats
                            goodind_vec.append( ind)
                        except:
                            pass
                self.DataInd = goodind_vec
                ColNames = []
                for k, el in enumerate( bakcolmp.KEYLIST):
                    if( k in index_vec):
                        ColNames.append( el)
                self.DataColNames = ColNames
        else:
            index_vec = []
            for el in colmp.KEYLIST:
                index_vec.append( colmp.KEYLIST.index( el))
                if( debug):
                    print( 'el in KEYLIST is:\t{}'.format( el))
            if( debug):
                print( 'length of index_vec is:\t{}'.format( len( index_vec)))
            index_vec.remove( colmp.KEYLIST.index( 'time'))
            index_vec.remove( colmp.KEYLIST.index( 'sectime'))
            if( debug):
                print( 'length of index_vec is:\t{}'.format( len( index_vec)))
            
            time = colmp.ndarray[self.TimeInd]
            timlen = np.shape( time)[0]
            ###
            # check for goodind: indices which ndarray output is equally long than the "time" ndarray
            goodind_vec = []
            bakcolmp = colmp
            colmp = colmp.ndarray[index_vec]
            for el, ind in zip( colmp, index_vec):
                if( debug):
                    print( 'shape of colmp-el of index {} is: {}'.format( np.shape( el), ind))
                if( np.shape( el)[0] == timlen):
                    try:
                        dump = np.nanmean( el.astype(float)) # check if the values can be transformed to floats
                        goodind_vec.append( ind)
                    except:
                        pass
            self.DataInd = goodind_vec
            ColNames = []
            for k, el in enumerate( bakcolmp.KEYLIST):
                if( k in self.DataInd):
                    ColNames.append( el)
            dump = {}
            for nentry, valentry in zip( ColNames, self.DataInd):
                dump[nentry] = valentry
            self.DataColNames = dump
            del dump
            #self.DataColNames = ColNames
            
        print( 'Data Column indices are {}'.format( self.DataInd))
        print( 'Data Column names are : {}'.format( self.DataColNames))
        return
    
    
    
    def __EquiInterpol__(self, **kwargs):
        kind = 'linear'
        for item, val in kwargs.items():
            if( item == 'data'):
                data = val
            if( item == 'time'):
                time = val
            if( item == 'kind'):
                kind = val
        if( np.nanmax( np.shape( data)) < 2):
            print( 'not enough data - N < 2 samples per column...stopping')
            sys.exit()
        leng = np.nanmax( np.shape( time))
        startt = np.nanmin( time)
        endt = np.nanmax( time)
        dt = ( float( endt) - float( startt))/ ( float( leng) - 1.0) 
        if( debug):
            print( 'leng is:\t{}'.format( leng))
            print( 'startt is:\t{}'.format( startt))
            print( 'endt is:\t{}'.format( endt))
            print( 'dt is:\t{}'.format( dt))
        if( np.shape( data)[0] != leng):
            wdata = data.T
        else:
            wdata = data
        if( debug):
            print( 'time is:\n{}'.format( time))
            print( 'data is:\n{}'.format( data))
            print( 'shape time is:\t{}'.format( np.shape( time)))
            print( 'shape data before __EquiInterpol__ is:\t{}'.format( np.shape( data)))
        intptime = np.arange( startt, endt + dt, dt)
        intptime = intptime[0:np.nanmax( np.shape( wdata))]
        if( debug):
            print( 'intptime is:\n{}'.format( intptime))
            print( 'shape of intptime is:\n{}'.format( np.shape( intptime)))
        dimone, dimtwo = np.shape( wdata)
        if( dimone > dimtwo):
            intpdata = np.zeros( np.shape( wdata.T))
            if( debug):
                print( 'shape intpdata with wdata.T is:\t{}'.format( np.shape( intpdata)))
            for k, el in enumerate( wdata.T):
                if( debug):
                    print( 'shape time is:\t{}'.format( np.shape( time)))
                    print( 'shape el with wdata.T is:\t{}'.format( np.shape( el)))
                intpdata[k, :] = interp1d(time, el, bounds_error=False, kind=kind, fill_value = 'extrapolate')(intptime)
        else:
            intpdata = np.zeros( np.shape( wdata))
            if( debug):
                print( 'shape intpdata with wdata is:\t{}'.format( np.shape( intpdata)))
            for k, el in enumerate( wdata):
                if( debug):
                    print( 'shape time is:\t{}'.format( np.shape( time)))
                    print( 'shape el with wdata is:\t{}'.format( np.shape( el)))
                intpdata[:, k] = interp1d(time, el, bounds_error=False, kind=kind, fill_value = 'extrapolate')(intptime)
            #print( 'Exception:\n"{}"\noccurred'.format( ex))
            #sys.exit()
        #if( np.shape( data)[1] != leng):
        #    intpdata = intpdata.T
        if( debug):
            print( 'intptime is:\n{}'.format( intptime))
            print( 'intpdata is:\n{}'.format( intpdata))
            print( 'shape intptime is:\t{}'.format( np.shape( intptime)))
            print( 'shape intpdata after __EquiInterpol__ is:\t{}'.format( np.shape( intpdata)))
        return intptime, intpdata
    
    
    
    def __RmNanInf__(self, dump):
        mygoodind = []
        for d, in dump.T:
            #changeind.append( np.where( np.logical_or( np.logical_or( np.abs( d - av) > lv, np.isnan( d)), np.isinf( d))))
            if( debug):
                print( 'd\t=\t{}'.format( d))
            b = d == np.nan
            c = d == np.inf
            if( debug):
                print( 'shape of b\t=\t{}'.format( np.shape( b)))
                print( 'shape of c\t=\t{}'.format( np.shape( c)))
            #hind = np.argwhere( b | c).flatten()
            boolyesvec = np.invert( b | c)
            if( debug and ( np.any( boolyesvec))):
                print( 'boolyesvec\t=\t{}'.format( boolyesvec))
                #print( 'len of boolyesvec:\t:{}'.format( len( [ f for f in boolyesvec if f == False])))
            #f = np.where( boolyesvec)[1]#.flatten()
            f = np.nonzero( boolyesvec)[1]#.flatten()
            #print( 'e is:\n\t{}'.format( e))
            #if( debug and ( np.any( b) | np.any( c))):
            if( debug):
                print( 'f\t=\t{}'.format( f))
            mygoodind.append( f)
        if( len( list( chain( *mygoodind))) == 0):
            self.MyException('No good indices...stopping')
        self.goodind = mygoodind
        
        return self.goodind
    
    
    
    def __ApplyFlags__(self, colmp):
        print( '\nLoading Flags...')
        self.flaglist = loadflags( self.flagpath, begin = self.starttime, end = self.endtime)
        ####
        # REPLACING col-name by appropriate col-name from self.ColList
        # TEMPORARY SOLUTION UNTIL ROMAN GIVES ME A FLAGFILE FOR GP20S3
        ####
        dump = []
        if( len( self.ColList) > 1):
            print( 'Adapting flag-information according to ColList')
            for newcolname in self.ColList:
                for a in self.flaglist:
                    strt, endt, colname, dum1, dum2, dum3, dum4 = a[0], a[1], a[2], a[3], a[4], a[5], a[6]
                    dump.append( [strt, endt, newcolname, dum1, dum2, dum3, dum4])
            self.flaglist = dump
            print( 'Adapting flag-information according to ColList..done')
        else:
            print( 'Flag-information left as it is...trying to apply existing flags')
            pass
        
        ####
        
        print( '\n...done')
        if( debug):
            print( 'Flags being applied now:')
            for el in self.flaglist:
                #print( el)
                print( 'column name', el[2], 'starttime', el[0], 'endtime', el[1])
        
        print( '\nApplying {} Flags...'.format( len( self.flaglist)))
        #print( 'num2date( colmp.ndarray[0])', num2date( colmp.ndarray[0]))
        #from datetime import timezone
        #utc = timezone.utc
        #minflag = self.flaglist[-1][0].replace( tzinfo = utc)
        #maxflag = self.flaglist[-1][1].replace( tzinfo = utc)
        #print( 'minflag', minflag)
        #print( 'maxflag', maxflag)
        #print( "colmp.header", colmp.header)
        #print( "colmp.header['SensorID']", colmp.header['SensorID'])
        #sys.exit()
        #for el in num2date( colmp.ndarray[0]):
        #    #print( el)
        #    if( ( el >= minflag) and ( el <= maxflag)):
        #    #if( ( el <= maxflag)):
        #        print( 'el within bounds', el)
        flaggeddata = colmp.flag( self.flaglist)
        #print( 'np.shape( flaggeddata.remove_flagged().ndarray[1])', np.shape( flaggeddata.remove_flagged().ndarray[1]))
        #print( 'np.shape( colmp.ndarray[1])', np.shape( colmp.ndarray[1]))
        
        #sys.exit()
        print( '\n...done')
        print( '\nRemoving flagged data...')
        colmp = flaggeddata.remove_flagged()
        print( '\n...done')
        return colmp
    
    
    
    def __InterMagnet_filter__(self, colmp):
        print( '\nFiltering according to InterMagnet recommendations...')
        colmp = colmp.filter()
        print( '\n...done')
        return colmp
    
    
    
    def Positions( self):
        ######################################
        # DEFINE ACURRATE SENSOR POSITIONS
        ######################################
        sys.path.insert( 0, dirname( abspath( __file__)))
        sensPos = np.load( os.path.join( '/home/niko/Software/sources/pythonsources/magfrqfunc/v1.1/magpy/magpy/opt/frqfilter','./SuperGradPos.npy'))
        del sys.path[0]
        return sensPos
    
    
    
    def SeedRead( self):
        from fnmatch import filter as flt
        if( debug):
            print( '\n\n\nStarting SeedRead...')
        startdate = self.starttime
        enddate = self.endtime
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        sstartdate = startdate
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        self.__getdays__()
        days = self.days
        startdate = self.starttime
        #print(startdate)
        # TIMERANGE DEFINITION
        ###################################
        #cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        #cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        ###################################
        colst = Stream()
        #testcolst = Stream()
        ###################################
        # DIVISOR DEFINITION FOR AVERAGE FREQUENCY INTERVALS
        ###################################
        #divisor = 2.0 # standard value
        if( debug):
            print( 'colst is :\t{}'.format( colst))
        #sys.exit()
        
        
        
        for day in days:
            doy = str(day.timetuple().tm_yday).zfill(3)
            print( '\n\n\nLooking in\t{}\twith daynumber\t{}:...'.format( day, doy))
            self.__LookForFilesMatching__()
            if( debug):
                for k, f in enumerate( self.files):
                    print( 'self.file[{}] is :\t{}'.format( k, f))
            st = Stream()
            if( debug):
                print( 'self.filetype is {}'.format( self.filetype))
            if( debug):
                print( 'isinstance( self.filetype, list)\n{}'.format( isinstance( self.filetype, list)))
                #print( 'type( self.filetype)\n{}'.format( type( self.filetype)))
            
            if( isinstance( self.filetype, str) & len( self.filetype) != 0):
                print( 'type( self.filetype)\n{}'.format( type( self.filetype)))
                for c in self.filetype: #str( 'e604618*')
                    #c = self.filetype
                    files = []
                    for el in self.SearchString:
                        files.append( sorted([f for f in flt( self.files, '*' + el + '*') if c in f ]))
                    if( debug):
                        print( 'trying to read the following files\n{}'.format( files))
                    #print( 'shape of files: {}'.format( shape( files)))
                    files = list( array( files).flatten())
                    files = np.unique( files)
                    #print( 'shape of files: {}'.format( shape( files)))
                    #print( 'files: {}'.format( files))
                    #files = files.
                    #sys.exit()
                    if( debug):
                        for k, f in enumerate( files):
                            print( 'file[{}] is :\t{}'.format( k, f))
                    
                    #tfiles = [f for f in fnmatch.filter(allfiles,str('e604616*')) if c in f ]
                    #print len(fnmatch.filter(f, str('e604617*' ) ) ) > 0
                    #print(c,f)
                    if( debug):
                        print( 'Day {} contains filetype: {}'.format( doy, c)) # , files
                    for f in files:
                        if( debug):
                            print( 'trying to read\t{}'.format( f))
                        for day in days:
                            doy = str(day.timetuple().tm_yday).zfill(3)
                            if( fnmatch( f, '*/' + str( doy) + '/*')):
                                try:
                                    st = opread( f)
                                    if( debug):
                                        for k, f in enumerate( st):
                                            print( 'st[{}] is :\t{}'.format( k, f))
                                    #sys.exit()
                                    colst += st
                                    del st
                                except Exception as ex:
                                    print( 'There was exception:\n{}\twhile reading file {}'.format( ex, f))
                                    pass
                                #print(st[0].stats)
                                #for f in files:
                                #testst += read(join(doy, f))
                                #print(st[0].stats)
            elif( isinstance( self.filetype, str) & len( self.filetype) == 0):
                files = []
                for el in self.SearchString:
                    files.append( sorted([f for f in flt( self.files, '*' + el + '*')]))
                if( debug):
                    print( 'trying to read the following files\n{}'.format( files))
                #sys.exit()
                #print( 'shape of files: {}'.format( shape( files)))
                files = list( array( files).flatten())
                files = np.unique( files)
                #print( 'shape of files: {}'.format( shape( files)))
                #print( 'files: {}'.format( files))
                #files = files.
                #sys.exit()
                if( debug):
                    for k, f in enumerate( files):
                        print( 'file[{}] is :\t{}'.format( k, f))
                
                #tfiles = [f for f in fnmatch.filter(allfiles,str('e604616*')) if c in f ]
                #print len(fnmatch.filter(f, str('e604617*' ) ) ) > 0
                #print(c,f)
                if( debug):
                    print( 'Day {} contains filetype: {}'.format( doy, '')) # , files
                for f in files:
                    if( debug):
                        print( 'trying to read\t{}'.format( f))
                    for day in days:
                        doy = str(day.timetuple().tm_yday).zfill(3)
                        if( debug):
                            print(f)
                            print("'*/' + str( doy) + '/*' is: ", '*/' + str( doy) + '/*')
                            print(self.SearchString)
                        #if( fnmatch( f, '*/' + str( doy) + '/*')): # old
                        mysearchstring = str('*') + str( self.SearchString) + str('*')
                        if( debug):
                            print( mysearchstring)
                            print( type( mysearchstring))
                            print( type( f))
                            print(  f)
                            print( fnmatch( f, mysearchstring))
                        if( fnmatch( f, mysearchstring)):
                            try:
                                st = opread( f)
                                if( debug):
                                    for k, r in enumerate( st):
                                        print( 'st[{}] is :\t{}'.format( k, r))
                                #sys.exit()
                                colst += st
                                del st
                            except Exception as ex:
                                print( 'There was exception:\n{}\twhile reading file {}'.format( ex, f))
                                pass
                            #print(st[0].stats)
                            #for f in files:
                            #testst += read(join(doy, f))
                            #print(st[0].stats)
            if( debug):
                for k, f in enumerate( colst):
                    print( 'colst[{}] is :\t{}'.format( k, f))
            #sys.exit()
        if( debug):
            print( 'colst is :\t{}'.format( colst))
        if( debug):
            for k, f in enumerate( colst):
                print( 'colst[{}] before sorting is :\t{}'.format( k, f))
                for n, e in enumerate( f):
                    print( 'element[{}] is: {}'.format( n, e))
        colst = colst.sort(keys=['starttime','endtime'])
        if( debug):
            for k, f in enumerate( colst):
                print( 'colst[{}] is :\t{}'.format( k, f))
                for n, e in enumerate( f):
                    print( 'element[{}] is: {}'.format( n, e))
        #colst = colst.slice(starttime =  UTCDateTime(colst[0].stats.starttime), endtime =  UTCDateTime(colst[0].stats.starttime + timedelta(seconds=cutoffset/colst[0].stats.sampling_rate)))
        #colst = colst.slice(starttime =  UTCDateTime(colst[0].stats.starttime), endtime =  UTCDateTime(colst[0].stats.starttime + timedelta(seconds=cutoffset/colst[0].stats.sampling_rate)))
        print( 'day start: {}'.format( datetime.strptime( self.starttime.isoformat(), "%Y-%m-%dT%H:%M:%S")))
        print( 'day end: {}'.format( datetime.strptime( self.endtime.isoformat(), "%Y-%m-%dT%H:%M:%S")))
        starto = UTCDateTime( self.starttime)
        endo = UTCDateTime( self.endtime)
        SeedArray = []
        if( debug):
            print( '\n\n\nTriming the Stream to times between:\n{}\t-\t{}'.format( starto, endo))
            print( '{} traces are in colst'.format( len( colst.select(channel = '*'))))
        MinTimeLeng = 9999999999
        if( debug):
            print( 'available channels: {}'.format( colst.select(channel = '*').traces))
        for k, trace in enumerate( colst.select(channel = '*').traces):
            self.stationinfo = [trace.meta.network, trace.meta.station, trace.meta.channel]
            if( trace.trim( starttime = starto, endtime = endo).stats.npts > 1):
                dump = trace.trim( starttime = starto, endtime = endo)
                dt = dump.stats.delta # sampling rate
                N = dump.stats.npts # number of points/ samples
                if( N < MinTimeLeng):
                    MinTimeLeng = N
                    ietime = []
                    for n in range(MinTimeLeng):
                        ietime.append( self.starttime + timedelta( seconds = n* dt))
                    if( debug):
                        print( 'dt[{}] is: {}'.format( k, dt))
                        print( 'N[{}] is: {}'.format( k, MinTimeLeng))
                        print( 'ietime[{}] is: {}'.format( k, ietime))
                    ztime = NUM2DATE20TIME( ietime) # get zerotime
                    if( debug):
                        print( 'ztime[{}] - ztime[0] is:\n{}'.format( k, ztime -ztime[0]))
                if( debug):
                    dump1 = dump.data
                    print( dump1[0:MinTimeLeng])
                    print( MinTimeLeng)
                if( np.nanstd( dump.data) != 0.0):
                    SeedArray.append( dump[0:MinTimeLeng])
        CutArray = []
        for el in SeedArray:
            if( len( CutArray) == 0):
                CutArray.append( el[0:MinTimeLeng])
            else:
                if( not any( f in el for f in CutArray)):
                    CutArray.append( el[0:MinTimeLeng])
        del SeedArray
        if( debug):
            print( 'shape of CutArray is: {}'.format( shape( CutArray)))
        Ntraces = min( shape( CutArray))
        if( debug):
            print( 'Ntraces is: {}'.format( Ntraces))
        if( Ntraces > 0 ):
            mseries = array( CutArray).reshape( Ntraces, -1)
        else:
            mseries = np.array([])
        #for n in range(1,min(len(colst.select(channel = 'p0').traces), len(colst.select(channel = 'p1').traces))):
        #    iterate = colst.select(channel = 'p0').traces[n]
        if( Ntraces > 0 ):
            self.zerotime = ztime
        else:
            self.zerotime = np.array([])
        if( self.trafofac == True):
            print( '\n\n\nTransformation factor is: {}, applying {} Volts/ Digit'.format( self.trafofac, self.trafoval))
            self.data = mseries* self.trafoval
        else:
            self.data = mseries
        gc.collect()
        del gc.garbage[:]
        return self.zerotime, self.data
    
    
    
    def GradRead(self):
        if( debug):
            print( '\n\n\nStarting GradRead...')
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        #sstartdate = startdate
        startdate = self.starttime
        enddate = self.endtime
        sstartdate = startdate
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        #sys.exit()
        self.__getdays__()
        days = self.days
        startdate = self.starttime
        #channelse = ['pri0','pri1']
        #print(startdate)
        print ( '\n\n\n\tstarttime\t\t=\tzstarttime\n\n\n')
        print ( '\t{}\t=\t{}'.format( startdate, zstartdate))
        print ( '\n\n\n\tendtime\t\t\t=\tzendtime\n\n\n')
        print ( '\t{}\t=\t{}'.format( enddate, zenddate))
        #sys.exit()
        ###################################
        # TIMERANGE DEFINITION
        ###################################
        cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        
        cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        ###################################
        # READ IN MAGNETIC RECORDS
        ###################################
        #dirstring = [str('./gradfiles/EW'), str('./gradfiles/NS'), str('./gradfiles/VS')]
        #######################
        #ewpathrelpath = str('sources/gradfiles/EW')
        #nspathrelpath = str('sources/gradfiles/NS')
        #vspathrelpath = str('sources/gradfiles/VS')
        #dirstring = [os.path.join( commonpath, ewpathrelpath), os.path.join( commonpath, nspathrelpath), os.path.join( commonpath, vspathrelpath)]
        #######################
        #import fnmatch
        #from magpy.stream import read, datetime, DataStream
        #from scipy.interpolate import interp1d
        #from itertools import chain
        colmp = DataStream()
        
        fileending = '.' + self.filetype
        ########################
        # READ EW, NS, V
        #########################
        
        
        
        
        
        
        #########################
        # READ EW
        #########################
        
        
        #if( len( self.SearchString) < len( 'GP20S3EW_111201_0001_')):
        sensname = 'GP20S3EW_111201_0001_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelew = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        if( debug):
            print( 'Used self.SearchString: "{}"'.format( sensname))
            #sys.exit()
        channelew = sum(channelew,[])
        if( debug):
            print( 'Looking for files like this:')
            for el in channelew:
                print( el)
            #sys.exit()
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #print ( 'all selected data: {}'.format( a))
        
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelew:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        self.__LookForFilesMatching__()
        if( debug):
            print( 'Found files like this:')
            for el in self.files:
                print( el)
            #sys.exit()
        allfiles = []
        n = 0
        for ewel in channelew:
            for el in self.files:
                if( fnmatch( el, '*' + ewel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'EW #{} file is: {}'.format( n, el))
        if( debug):
            print( 'allfiles is:')
            print( allfiles)
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
            #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'EW #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        fndheaderinfo = read( allfiles[0]).header # backup of temp.header for later use
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...EW #{} file {} read.'.format( k, f))
            #colmp = mp.appendStreams([colmp, temp]) # CONCATENATE MAGPY STREAMS
            try:
                self.stationinfo = [temp.header['SensorName'], temp.header['SensorSerialNum'], temp.header['SensorRevision']]
            except:
                self.stationinfo = [self.SearchString[0:self.SearchString.index( '_')], self.SearchString[self.SearchString.index( '_'):self.SearchString.rfind( '_')],'']
            identstr = str( self.stationinfo[0]) + str( self.stationinfo[1]) + str( self.stationinfo[2])
            print( '...{} #{} file {} read.'.format( identstr, k, f))
            #print(k, temp.ndarray[2])
            #colmp = mp.appendStreams([colmp, temp]) # CONCATENATE MAGPY STREAMS
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            #colmp.extend(temp , temp.header, temp.ndarray)
            print( '...{} #{} file {} appended.'.format( identstr, k, f))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            #colmp.extend(temp , temp.header, temp.ndarray)
            print( '...EW #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'EW matching read...removing duplicates')
            #sys.exit()
        colmp.header = fndheaderinfo
        if( self.InterMagFilter):
            colmp = self.__InterMagnet_filter__(colmp)
        if( self.ApplyFlags):
            colmp = self.__ApplyFlags__(colmp)
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        #index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z'), colmp.KEYLIST.index('dx'), colmp.KEYLIST.index('dy'), colmp.KEYLIST.index('dz')]
        self.__GetDataInd__( colmp, ColList = self.ColList)
        index_vec = self.DataInd
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        #sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'EW dataarray reshaped...converting timestamps')
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().items())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el in helpt:
                print( el)
            #sys.exit()
        ztime = NUM2DATE20TIME( num2date( helpt))
        if( debug):
            print( 'ztime is:')
            for el in ztime:
                print( el)
            #sys.exit()
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().items() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - EW data array')
            #sys.exit()
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        ewmseries = mseries
        ewtime = ietime
        
        print( 'Reading EW-Data...done\n')
        
        
        
        #########################
        # READ NS
        #########################
        colmp = DataStream()
        #channelsm = [['GP20S3NS_012201_0001_0001_' + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        sensname = 'GP20S3NS_012201_0001_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelns = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        #channelns = [[self.SearchString + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        channelns = sum(channelns,[])
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelns:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        allfiles = []
        n = 0
        for nsel in channelns:
            for el in self.files:
                if( fnmatch( el, '*' + nsel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'NS #{} file is: {}'.format( n, el))
        
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
            #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'NS #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        fndheaderinfo = read( allfiles[0]).header # backup of temp.header for later use
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...NS #{} file {} read.'.format( k, f))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            print( '...NS #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'NS matching read...removing duplicates')
            #sys.exit()
        colmp.header = fndheaderinfo
        if( self.ApplyFlags):
            colmp = self.__ApplyFlags__(colmp)
        if( self.InterMagFilter):
            self.__InterMagnet_filter__(colmp)
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        #index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z'), colmp.KEYLIST.index('dx'), colmp.KEYLIST.index('dy'), colmp.KEYLIST.index('dz')]
        self.__GetDataInd__( colmp, ColList = self.ColList)
        index_vec = self.DataInd
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        #sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'NS dataarray reshaped...converting timestamps')
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().items())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el in helpt:
                print( el)
            #sys.exit()
        ztime = NUM2DATE20TIME( num2date( helpt))
        if( debug):
            print( 'ztime is:')
            for el in ztime:
                print( el)
            #sys.exit()
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().items() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - NS data array')
            #sys.exit()
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        nsmseries = mseries
        nstime = ietime
        
        print( 'Reading NS-Data...done\n')
        
        
        
        #########################
        # READ V
        #########################
        colmp = DataStream()
        #channelsm = [['GP20S3V_911005_0001_0001_' + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        sensname = 'GP20S3V_911005_0001_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelv = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        channelv = sum(channelv,[])
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelv:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        allfiles = []
        n = 0
        for vsel in channelv:
            for el in self.files:
                if( fnmatch( el, '*' + vsel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'V #{} file is: {}'.format( n, el))
        
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
            #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'VS #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        fndheaderinfo = read( allfiles[0]).header # backup of temp.header for later use
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...V #{} file {} read.'.format( k, f))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            print( '...V #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'V matching read...removing duplicates')
            #sys.exit()
        colmp.header = fndheaderinfo
        if( self.ApplyFlags):
            colmp = self.__ApplyFlags__(colmp)
        if( self.InterMagFilter):
            self.__InterMagnet_filter__(colmp)
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        #index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z'), colmp.KEYLIST.index('dx'), colmp.KEYLIST.index('dy'), colmp.KEYLIST.index('dz')]
        self.__GetDataInd__( colmp, ColList = self.ColList)
        index_vec = self.DataInd
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        #sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'V dataarray reshaped...converting timestamps')
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().items())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el in helpt:
                print( el)
            #sys.exit()
        ztime = NUM2DATE20TIME( num2date( helpt))
        if( debug):
            print( 'ztime is:')
            for el in ztime:
                print( el)
            #sys.exit()
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().items() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - V data array')
            #sys.exit()
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        vmseries = mseries
        vtime = ietime
        
        
        
        print( 'Reading V-Data...done\nImplement sensor offsets and derive equidistantly sampled series by \ninterpolation to common samples.')
        
        
        
        
        
        """
        ######################################
        # DEFINE ACURRATE SENSOR POSITIONS
        ######################################
        sensorpos_N = array( [ -34855.5869, 310273.3534, 1087.85])
        #sensorpos_S = array( [ -34856.7915, 310073.3882, 1086.229]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_S = array( [ -34856.7915, 310073.3882, 1087.229])
        sensorpos_NS = array( [ -34856.4903, 310123.3873, 1087.16])
        #sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.64]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.14])
        #sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.54]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.14])
        sensorpos_W = array( [ -34947.11138, 310137.2636, 1087.24])
        sensorpos_TB = array( [ -34856.47464, 310125.9872, 1087.17])
        sensorpos_TA = array( [ -34856.43362, 310132.7971, 1087.2])
        sensorpos_B = array( [ -34856.43362, 310125.7971, 887.2])
        
        sensPos = vstack( ( sensorpos_N, sensorpos_S, sensorpos_NS, sensorpos_E, sensorpos_EW, sensorpos_W, sensorpos_TB, sensorpos_TA, sensorpos_B))
        
        sensName = array( ['N', 'S', 'NS', 'E', 'EW', 'W', 'TB', 'TA', 'B'])
        sensPos = hstack( ( np.atleast_2d( sensName).T, sensPos))
        """
        sensPos = self.Positions()
        
        ######################################
        # CUT OUT SAMPLES WHICH ARE AVAILABLE FROM ALL SENSORS
        ######################################
        common_starttime = np.max( [ nstime[0], ewtime[0], vtime[0]])
        common_endtime = np.min( [ nstime[-1], ewtime[-1], vtime[-1]])
        
        
        ns_t_minind = np.argmin( np.abs( nstime - common_starttime))
        ns_t_maxind = np.argmin( np.abs( nstime - common_endtime))
        NS_leng = ns_t_maxind - ns_t_minind
        
        ew_t_minind = np.argmin( np.abs( ewtime - common_starttime))
        ew_t_maxind = np.argmin( np.abs( ewtime - common_endtime))
        EW_leng = ew_t_maxind - ew_t_minind
        
        v_t_minind = np.argmin( np.abs( vtime - common_starttime))
        v_t_maxind = np.argmin( np.abs( vtime - common_endtime))
        V_leng = v_t_maxind - v_t_minind
        
        
        common_leng = np.min( [NS_leng, EW_leng, V_leng])
        if( debug):
            print( 'common_leng is: {}'.format( common_leng))
        
        if( common_leng == NS_leng):
            si = float( nstime[ns_t_maxind] - nstime[ns_t_minind])/ float(NS_leng - 1)
        elif( common_leng == EW_leng):
            si = float( ewtime[ew_t_maxind] - ewtime[ew_t_minind])/ float(EW_leng - 1)
        elif( common_leng == V_leng):
            si = float( vtime[v_t_maxind] - vtime[v_t_minind])/ float(V_leng - 1)
        else:
            print( 'ERROR cutting only sensors data from all available sensors')
            return
        newdt = (common_endtime - common_starttime)/(float(common_leng) - 1.0)
        print( '\n\n\nResampling with new sampling intervall: {} seconds'.format( newdt))
        #ietime = np.linspace(common_starttime, common_endtime, common_leng) # changed due to errors in linspace routine
        ietime = np.arange(common_starttime, common_endtime + newdt, newdt)
        
        # NS
        temp = interp1d(nstime,nsmseries,fill_value='extrapolate',kind='linear')(ietime)
        nsmseries = temp
        
        # EW
        temp = interp1d(ewtime,ewmseries,fill_value='extrapolate',kind='linear')(ietime)
        ewmseries = temp
        
        # V
        temp = interp1d(vtime,vmseries,fill_value='extrapolate',kind='linear')(ietime)
        vmseries = temp
        print( '\n\n\n...done, stacking arrays together.')
        ######################################
        # CONCATENATE ALL THREE DIRECTIONS
        ######################################
        mseries = np.vstack( ( nsmseries, ewmseries, vmseries))* pico
        print( '\n\n\n...done.')
        ######################################
        print( 'Reading gradient files between {} and {} done'.format( startdate, enddate))
        gc.collect()
        del gc.garbage[:]
        self.data = mseries
        self.zerotime = ietime
        self.sensPos = sensPos
        return self.zerotime, self.data, self.sensPos#, np.vstack(( aseries, bseries))
    
    
    
    def AbsRead( self):
        if( debug):
            print( '\n\n\nStarting AbsRead...')
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        #sstartdate = startdate
        startdate = self.starttime
        enddate = self.endtime
        sstartdate = startdate
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        #sys.exit()
        self.__getdays__()
        days = self.days
        startdate = self.starttime
        #channelse = ['pri0','pri1']
        #print(startdate)
        print ( '\n\n\n\tstarttime\t\t=\tzstarttime\n\n\n')
        print ( '\t{}\t=\t{}'.format( startdate, zstartdate))
        print ( '\n\n\n\tendtime\t\t\t=\tzendtime\n\n\n')
        print ( '\t{}\t=\t{}'.format( enddate, zenddate))
        #sys.exit()
        ###################################
        # TIMERANGE DEFINITION
        ###################################
        cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        
        cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        ###################################
        # READ IN MAGNETIC RECORDS
        ###################################
        #dirstring = [str('./gradfiles/EW'), str('./gradfiles/NS'), str('./gradfiles/VS')]
        #######################
        #ewpathrelpath = str('sources/gradfiles/EW')
        #nspathrelpath = str('sources/gradfiles/NS')
        #vspathrelpath = str('sources/gradfiles/VS')
        #dirstring = [os.path.join( commonpath, ewpathrelpath), os.path.join( commonpath, nspathrelpath), os.path.join( commonpath, vspathrelpath)]
        #######################
        #import fnmatch
        #from magpy.stream import read, datetime, DataStream
        #from scipy.interpolate import interp1d
        #from itertools import chain
        colmp = DataStream()
        
        fileending = '.' + self.filetype
        ########################
        # READ EW, NS, V
        #########################
        
        
        
        
        
        
        #########################
        # READ EW
        #########################
        
        
        #if( len( self.SearchString) < len( 'GP20S3EW_111201_0001_')):
        sensname = 'GP20S3EW_111201_0001_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelew = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        
        channelew = sum(channelew,[])
        if( debug):
            print( 'Looking for files like this:')
            for el in channelew:
                print( el)
            #sys.exit()
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #print ( 'all selected data: {}'.format( a))
        
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelew:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        self.__LookForFilesMatching__()
        if( debug):
            print( 'Found files like this:')
            for el in self.files:
                print( el)
            #sys.exit()
        allfiles = []
        n = 0
        for ewel in channelew:
            for el in self.files:
                if( fnmatch( el, '*' + ewel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'EW #{} file is: {}'.format( n, el))
        
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
            #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'EW #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        fndheaderinfo = read( allfiles[0]).header # backup of temp.header for later use
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...EW #{} file {} read.'.format( k, f))
            #colmp = mp.appendStreams([colmp, temp]) # CONCATENATE MAGPY STREAMS
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            #colmp.extend(temp , temp.header, temp.ndarray)
            print( '...EW #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'EW matching read...removing duplicates')
            #sys.exit()
        colmp.header = fndheaderinfo
        if( self.ApplyFlags):
            colmp = self.__ApplyFlags__(colmp)
        if( self.InterMagFilter):
            self.__InterMagnet_filter__(colmp)
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        #index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        self.__GetDataInd__( colmp, ColList = [ 'x', 'y', 'z'])
        index_vec = self.DataInd
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        #sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'EW dataarray reshaped...converting timestamps')
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().items())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el in helpt:
                print( el)
            #sys.exit()
        ztime = NUM2DATE20TIME( num2date( helpt))
        if( debug):
            print( 'ztime is:')
            for el in ztime:
                print( el)
            #sys.exit()
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().items() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - EW data array')
            #sys.exit()
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        ewmseries = mseries
        ewtime = ietime
        
        print( 'Reading EW-Data...done\n')
        
        
        
        #########################
        # READ NS
        #########################
        colmp = DataStream()
        #channelsm = [['GP20S3NS_012201_0001_0001_' + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        sensname = 'GP20S3NS_012201_0001_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelns = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        #channelns = [[self.SearchString + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        channelns = sum(channelns,[])
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelns:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        allfiles = []
        n = 0
        for nsel in channelns:
            for el in self.files:
                if( fnmatch( el, '*' + nsel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'NS #{} file is: {}'.format( n, el))
        
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
            #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'NS #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        fndheaderinfo = read( allfiles[0]).header # backup of temp.header for later use
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...NS #{} file {} read.'.format( k, f))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            print( '...NS #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'NS matching read...removing duplicates')
            #sys.exit()
        colmp.header = fndheaderinfo
        if( self.ApplyFlags):
            colmp = self.__ApplyFlags__(colmp)
        if( self.InterMagFilter):
            self.__InterMagnet_filter__(colmp)
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        #index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        self.__GetDataInd__( colmp, ColList = [ 'x', 'y', 'z'])
        index_vec = self.DataInd
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        #sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'NS dataarray reshaped...converting timestamps')
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().items())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el in helpt:
                print( el)
            #sys.exit()
        ztime = NUM2DATE20TIME( num2date( helpt))
        if( debug):
            print( 'ztime is:')
            for el in ztime:
                print( el)
            #sys.exit()
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().items() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - NS data array')
            #sys.exit()
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        nsmseries = mseries
        nstime = ietime
        
        print( 'Reading NS-Data...done\n')
        
        
        
        #########################
        # READ V
        #########################
        colmp = DataStream()
        #channelsm = [['GP20S3V_911005_0001_0001_' + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        sensname = 'GP20S3V_911005_0001_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelv = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        channelv = sum(channelv,[])
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelv:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        allfiles = []
        n = 0
        for vsel in channelv:
            for el in self.files:
                if( fnmatch( el, '*' + vsel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'V #{} file is: {}'.format( n, el))
        
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
            #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'VS #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        fndheaderinfo = read( allfiles[0]).header # backup of temp.header for later use
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...V #{} file {} read.'.format( k, f))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            print( '...V #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'V matching read...removing duplicates')
            #sys.exit()
        colmp.header = fndheaderinfo
        if( self.ApplyFlags):
            colmp = self.__ApplyFlags__(colmp)
        if( self.InterMagFilter):
            self.__InterMagnet_filter__(colmp)
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        #index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        self.__GetDataInd__( colmp, ColList = [ 'x', 'y', 'z'])
        index_vec = self.DataInd
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        #sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'V dataarray reshaped...converting timestamps')
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().items())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el in helpt:
                print( el)
            #sys.exit()
        ztime = NUM2DATE20TIME( num2date( helpt))
        if( debug):
            print( 'ztime is:')
            for el in ztime:
                print( el)
            #sys.exit()
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().items() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - V data array')
            #sys.exit()
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        vmseries = mseries
        vtime = ietime
        
        
        
        print( 'Reading V-Data...done\nImplement sensor offsets and derive equidistantly sampled series by \ninterpolation to common samples.')
        
        
        
        
        
        """
        ######################################
        # DEFINE ACURRATE SENSOR POSITIONS
        ######################################
        sensorpos_N = array( [ -34855.5869, 310273.3534, 1087.85])
        #sensorpos_S = array( [ -34856.7915, 310073.3882, 1086.229]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_S = array( [ -34856.7915, 310073.3882, 1087.229])
        sensorpos_NS = array( [ -34856.4903, 310123.3873, 1087.16])
        #sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.64]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.14])
        #sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.54]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.14])
        sensorpos_W = array( [ -34947.11138, 310137.2636, 1087.24])
        sensorpos_TB = array( [ -34856.47464, 310125.9872, 1087.17])
        sensorpos_TA = array( [ -34856.43362, 310132.7971, 1087.2])
        sensorpos_B = array( [ -34856.43362, 310125.7971, 887.2])
        
        sensPos = vstack( ( sensorpos_N, sensorpos_S, sensorpos_NS, sensorpos_E, sensorpos_EW, sensorpos_W, sensorpos_TB, sensorpos_TA, sensorpos_B))
        
        sensName = array( ['N', 'S', 'NS', 'E', 'EW', 'W', 'TB', 'TA', 'B'])
        sensPos = hstack( ( np.atleast_2d( sensName).T, sensPos))
        """
        sensPos = self.Positions()
        
        ######################################
        # CUT OUT SAMPLES WHICH ARE AVAILABLE FROM ALL SENSORS
        ######################################
        common_starttime = np.max( [ nstime[0], ewtime[0], vtime[0]])
        common_endtime = np.min( [ nstime[-1], ewtime[-1], vtime[-1]])
        
        
        ns_t_minind = np.argmin( np.abs( nstime - common_starttime))
        ns_t_maxind = np.argmin( np.abs( nstime - common_endtime))
        NS_leng = ns_t_maxind - ns_t_minind
        
        ew_t_minind = np.argmin( np.abs( ewtime - common_starttime))
        ew_t_maxind = np.argmin( np.abs( ewtime - common_endtime))
        EW_leng = ew_t_maxind - ew_t_minind
        
        v_t_minind = np.argmin( np.abs( vtime - common_starttime))
        v_t_maxind = np.argmin( np.abs( vtime - common_endtime))
        V_leng = v_t_maxind - v_t_minind
        
        
        common_leng = np.min( [NS_leng, EW_leng, V_leng])
        if( debug):
            print( 'common_leng is: {}'.format( common_leng))
        
        if( common_leng == NS_leng):
            si = float( nstime[ns_t_maxind] - nstime[ns_t_minind])/ float(NS_leng - 1)
        elif( common_leng == EW_leng):
            si = float( ewtime[ew_t_maxind] - ewtime[ew_t_minind])/ float(EW_leng - 1)
        elif( common_leng == V_leng):
            si = float( vtime[v_t_maxind] - vtime[v_t_minind])/ float(V_leng - 1)
        else:
            print( 'ERROR cutting only sensors data from all available sensors')
            return
        newdt = (common_endtime - common_starttime)/(float(common_leng) - 1.0)
        print( '\n\n\nResampling with new sampling intervall: {} seconds'.format( newdt))
        #ietime = np.linspace(common_starttime, common_endtime, common_leng) # changed due to errors in linspace routine
        ietime = np.arange(common_starttime, common_endtime + newdt, newdt)
        
        # NS
        temp = interp1d(nstime,nsmseries,fill_value='extrapolate',kind='linear')(ietime)
        nsmseries = temp
        
        # EW
        temp = interp1d(ewtime,ewmseries,fill_value='extrapolate',kind='linear')(ietime)
        ewmseries = temp
        
        # V
        temp = interp1d(vtime,vmseries,fill_value='extrapolate',kind='linear')(ietime)
        vmseries = temp
        print( '\n\n\n...done, stacking arrays together.')
        ######################################
        # CONCATENATE ALL THREE DIRECTIONS
        ######################################
        mseries = np.vstack( ( nsmseries, ewmseries, vmseries))* pico
        print( '\n\n\n...done.')
        ######################################
        print( 'Reading absolutes files between {} and {} done'.format( startdate, enddate))
        self.zerotime = ietime
        self.data = mseries
        self.sensPos = sensPos
        gc.collect()
        del gc.garbage[:]
        #self.sensPos
        return self.zerotime, self.data, self.sensPos#, np.vstack(( aseries, bseries))
    
    
    
    def SingleAbsRead( self):
        if( debug):
            print( '\n\n\nStarting SingleAbsRead...')
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        #sstartdate = startdate
        startdate = self.starttime
        enddate = self.endtime
        sstartdate = startdate
        
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        self.__getdays__()
        days = self.days
        startdate = self.starttime
        #sys.exit()
        #channelse = ['pri0','pri1']
        #print(startdate)
        print ( '\n\n\n\tstarttime\t\t=\tzstarttime\n\n\n')
        print ( '\t{}\t=\t{}'.format( startdate, zstartdate))
        print ( '\n\n\n\tendtime\t\t\t=\tzendtime\n\n\n')
        print ( '\t{}\t=\t{}'.format( enddate, zenddate))
        #sys.exit()
        ###################################
        # TIMERANGE DEFINITION
        ###################################
        cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        
        cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        ###################################
        # READ IN MAGNETIC RECORDS
        ###################################
        #dirstring = [str('./gradfiles/EW'), str('./gradfiles/NS'), str('./gradfiles/VS')]
        #######################
        #ewpathrelpath = str('sources/gradfiles/EW')
        #nspathrelpath = str('sources/gradfiles/NS')
        #vspathrelpath = str('sources/gradfiles/VS')
        #dirstring = [os.path.join( commonpath, ewpathrelpath), os.path.join( commonpath, nspathrelpath), os.path.join( commonpath, vspathrelpath)]
        #######################
        #import fnmatch
        #from magpy.stream import read, datetime, DataStream
        #from scipy.interpolate import interp1d
        #from itertools import chain
        colmp = DataStream()
        
        fileending = '.' + self.filetype
        ########################
        # READ EW, NS, V
        #########################
        
        
        
        
        
        
        #########################
        # READ SINGLE SENSOR AXIS
        #########################
        
        
        #if( len( self.SearchString) < len( 'GP20S3EW_111201_0001_')):
        sensname = 'GP20S3'
        #sensname = sensname + self.SearchString
        #sensname = 'GP20S3EW_111201_0001_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelsg = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        #channelsg = np.unique( channelsg)
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        
        channelsg = sum(channelsg,[])
        if( debug):
            print( 'Looking for files like this:')
            for el in channelsg:
                print( el)
            #sys.exit()
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #print ( 'all selected data: {}'.format( a))
        
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelsg:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        self.__LookForFilesMatching__()
        if( debug):
            print( 'Found files like this:')
            for el in self.files:
                print( el)
            #sys.exit()
        allfiles = []
        n = 0
        print( 'looking for sgel: {}'.format( channelsg))
        for sgel in channelsg:
            if( debug):
                print( 'looking for sgel: {}'.format( sgel))
            for el in self.files:
                if( fnmatch( el, '*' + sgel)):
                    n = n + 1
                    allfiles.append( el)
                    print( '{} #{} file is: {}'.format( self.SearchString, n, el))
                    #sys.exit()
        #sys.exit()
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
            #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'EW #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        #print(allfiles)
        #sys.exit()
        
        
        
        self.__GetColHeaderInfo__( read( allfiles[0]))
        if( debug):
            print( 'self.HeaderInfo is:\n')
            for el in self.HeaderInfo:
                print( 'el is:\t{}'.format( el))
            #sys.exit()
        fndheaderinfo = read( allfiles[0]).header # backup of temp.header for later use
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            #print( temp.header.keys())
            #print( temp.header.values())
            #sys.exit()
            #print(temp.header)
            #print( self.SearchString[0:self.SearchString.rfind( '_')])
            try:
                self.stationinfo = [temp.header['SensorName'], temp.header['SensorSerialNum'], temp.header['SensorRevision']]
            except:
                self.stationinfo = [self.SearchString[0:self.SearchString.index( '_')], self.SearchString[self.SearchString.index( '_'):self.SearchString.rfind( '_')],'']
            identstr = str( self.stationinfo[0]) + str( self.stationinfo[1]) + str( self.stationinfo[2])
            print( '...{} #{} file {} read.'.format( identstr, k, f))
            #print(k, temp.ndarray[2])
            #colmp = mp.appendStreams([colmp, temp]) # CONCATENATE MAGPY STREAMS
            #print( 'actual colmp length is: {}'.format( colmp.length))
            #print( 'temp length to be added is: {}'.format( temp.length))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            #colmp.extend(temp , temp.header, temp.ndarray)
            print( '...{} #{} file {} appended.'.format( identstr, k, f))
        colmp.header = fndheaderinfo
        if( debug):
            print( '{} matching read...removing duplicates'.format( identstr))
            #sys.exit()
        if( self.ApplyFlags):
            colmp = self.__ApplyFlags__(colmp)
        if( self.InterMagFilter):
            self.__InterMagnet_filter__(colmp)
        
        #print( sstartdate, enddate)
        #sys.exit()
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        #print( colmp.ndarray[2])
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        #index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        self.__GetDataInd__( colmp, ColList = self.ColList)
        index_vec = self.DataInd
        ColNames = self.DataColNames
        
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        #sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( '{} dataarray reshaped...converting timestamps'.format( self.SearchString))
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        print( 'found average internal POSIX sampling interval is {} days'.format( dt))
        #sys.exit()
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        
        #print( 'cut_start, cut_end')
        #print( cut_start, cut_end)
        #print( 'iedt')
        #print( iedt)
        
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().items())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        #print( 'shape of helpt is {}'.format( np.shape( helpt)))
        #sys.exit()
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el, dl in zip( helpt, num2date( helpt)):
                print( el, dl)
            #sys.exit()
        print( 'Deriving ztime...')
        if( debug):
            helpt = np.sort( np.nanmean( helpt) + (np.random.sample( len( helpt)) - 0.5)* 1.0* np.nanstd( helpt))
        ztime = NUM2DATE20TIME( num2date( helpt)) # GETTING zerotime timestamps
        print( 'ztime derived.')
        ztimedt = ( ztime[-1] - ztime[0])/ float( len( ztime) - 1)
        print( 'found ztime dt: {} seconds'.format( ztimedt))
        if( debug):
            for el in (ztime[1::] - ztime[:-1:]):
                print( 'ppm difference to nominal sampling interval {}'.format( np.abs( el - ztimedt)/ ztimedt* 1000000.0))
            #sys.exit()
        #sys.exit()
        ##########################
        #BEGIN DEBUG OF TIMESTAMPS !!!! KEEP FOR DEBUGGING !!!!
        ##########################
        if( False):
            print( 'Deriving checkstamps...')
            checkdates = STAMPTODATE( ztime)
            print( 'checkstamps derived.')
            for el in checkdates:
                year = el.year
                print( el)
                #if( year > 2020):
                #    sys.exit()
            #´sys.exit()
            print( 'Deriving recheckztime...')
            recheckztime = NUM2DATE20TIME( checkdates)
            print( 'recheckztime derived.')
            #sys.exit()
        if( False):
            if( True):
                print( 'helpt is:')
                #from datetime import timezone
                for k, ( el, dl, cl, bl, al) in enumerate( zip( helpt, num2date( helpt), ztime, checkdates, recheckztime)):
                    print( k, el, dl, cl, bl, al, ( dl - bl).total_seconds())
                    #sys.exit()
                    #if( np.abs( al - cl) > 1.0):
                    if( bl.year == 2021):
                        print( 'PROBLEM WITH ztime or with STAMPTODATE...STOPPING')
                        #!!! WO IS DAS PROBLE MIT STAMPTODATE !!! ztime passt vermeintlich !!!
                        sys.exit()
                    #sleep( 1)
                sys.exit()
            else:
                print( 'helpt is:')
                #from datetime import timezone
                for k, ( el, dl, cl) in enumerate( zip( helpt, num2date( helpt), ztime)):
                    print( k, el, dl, cl)#, al - cl)
            sys.exit()
        
        
        
        
        
        
        #END DEBUG
        ##########################
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().items() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - {} data array'.format( self.SearchString))
            #sys.exit()
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        sgmseries = mseries
        sgtime = ietime
        
        print( 'Reading {}-Data...done\n'.format(self.SearchString))
        
        
        
        """
        ######################################
        # DEFINE ACURRATE SENSOR POSITIONS
        ######################################
        sensorpos_N = array( [ -34855.5869, 310273.3534, 1087.85])
        #sensorpos_S = array( [ -34856.7915, 310073.3882, 1086.229]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_S = array( [ -34856.7915, 310073.3882, 1087.229])
        sensorpos_NS = array( [ -34856.4903, 310123.3873, 1087.16])
        #sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.64]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.14])
        #sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.54]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.14])
        sensorpos_W = array( [ -34947.11138, 310137.2636, 1087.24])
        sensorpos_TB = array( [ -34856.47464, 310125.9872, 1087.17])
        sensorpos_TA = array( [ -34856.43362, 310132.7971, 1087.2])
        sensorpos_B = array( [ -34856.43362, 310125.7971, 887.2])
        
        sensPos = vstack( ( sensorpos_N, sensorpos_S, sensorpos_NS, sensorpos_E, sensorpos_EW, sensorpos_W, sensorpos_TB, sensorpos_TA, sensorpos_B))
        
        sensName = array( ['N', 'S', 'NS', 'E', 'EW', 'W', 'TB', 'TA', 'B'])
        sensPos = hstack( ( np.atleast_2d( sensName).T, sensPos))
        """
        sensPos = self.Positions()
        
        ######################################
        # CUT OUT SAMPLES WHICH ARE AVAILABLE FROM ALL SENSORS
        ######################################
        common_starttime = np.max([ sgtime[0]])
        common_endtime = np.min( [sgtime[-1]])
        
        
        sg_t_minind = np.argmin( np.abs( sgtime - common_starttime))
        sg_t_maxind = np.argmin( np.abs( sgtime - common_endtime))
        SG_leng = sg_t_maxind - sg_t_minind
        
        
        common_leng = np.min( [SG_leng])
        if( debug):
            print( 'common_leng is: {}'.format( common_leng))
        
        if( common_leng == SG_leng):
            si = float( sgtime[sg_t_maxind] - sgtime[sg_t_minind])/ float(SG_leng - 1)
        else:
            print( 'ERROR cutting only sensors data from all available sensors')
            return
        newdt = (common_endtime - common_starttime)/(float(common_leng) - 1.0)
        print( '\n\n\nResampling with new sampling intervall: {} seconds'.format( newdt))
        #ietime = np.linspace(common_starttime, common_endtime, common_leng) # changed due to errors in linspace routine
        ietime = np.arange(common_starttime, common_endtime + newdt, newdt)
        
        
        # SG
        temp = interp1d( sgtime, sgmseries,fill_value='extrapolate',kind='linear')(ietime)
        sgmseries = temp
        
        print( '\n\n\n...done, stacking arrays together.')
        ######################################
        # CONCATENATE ALL THREE DIRECTIONS
        ######################################
        mseries = sgmseries#* pico
        print( '\n\n\n...done.')
        ######################################
        print( 'Reading absolutes files between {} and {} done'.format( startdate, enddate))
        self.zerotime = ietime
        self.data = mseries
        self.sensPos = sensPos
        gc.collect()
        del gc.garbage[:]
        #self.sensPos
        return self.zerotime, self.data, self.sensPos#, np.vstack(( aseries, bseries))
    
    
    
    def VarioRead( self):
        if( debug):
            print( '\n\n\nStarting VarioRead...')
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        #sstartdate = startdate
        startdate = self.starttime
        enddate = self.endtime
        sstartdate = startdate
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        #sys.exit()
        self.__getdays__()
        days = self.days
        #startdate = self.starttime
        #channelse = ['pri0','pri1']
        print('startdate', startdate)
        print ( '\n\n\n\tstarttime\t\t=\tzstarttime\n\n\n')
        print ( '\t{}\t=\t{}'.format( startdate, zstartdate))
        print ( '\n\n\n\tendtime\t\t\t=\tzendtime\n\n\n')
        print ( '\t{}\t=\t{}'.format( enddate, zenddate))
        #sys.exit()
        ###################################
        # TIMERANGE DEFINITION
        ###################################
        cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        
        cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        ###################################
        # READ IN MAGNETIC RECORDS
        ###################################
        #dirstring = [str('./gradfiles/EW'), str('./gradfiles/NS'), str('./gradfiles/VS')]
        #######################
        #ewpathrelpath = str('sources/gradfiles/EW')
        #nspathrelpath = str('sources/gradfiles/NS')
        #vspathrelpath = str('sources/gradfiles/VS')
        #dirstring = [os.path.join( commonpath, ewpathrelpath), os.path.join( commonpath, nspathrelpath), os.path.join( commonpath, vspathrelpath)]
        #######################
        #import fnmatch
        #from magpy.stream import read, datetime, DataStream
        #from scipy.interpolate import interp1d
        #from itertools import chain
        colmp = DataStream()
        
        fileending = '.' + self.filetype
        ########################
        # READ Vario
        #########################
        
        
        
        
        
        
        #########################
        # READ Vario
        #########################
        
        
        sensname = 'LEMI036_1_0002_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            #pass
            if( debug):
                print( 'sensname still {}'.format( sensname))
                print( 'self.SearchString still {}'.format( self.SearchString))
        else:
            sensname = self.SearchString
            if( debug):
                print( 'sensname changed to {}'.format( sensname))
                print( 'self.SearchString changed to {}'.format( self.SearchString))
            #if( not self.SearchString.startswith( 'GSM90_1013973') and not self.SearchString.startswith( 'GSM19_6067473')):
            if( not self.SearchString.startswith( 'GSM90') and not self.SearchString.startswith( 'GSM19') and not self.SearchString.startswith( 'LEMI')):
                sensname = sensname[0:16] # reducing again to sensorname without date info
            if( debug):
                print( 'sensname is: {}'.format( sensname))
            #sys.exit()
        channelvario = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        if( debug):
            print( 'channelvario is: {}'.format( channelvario))
        #sys.exit()
        
        channelvario = sum(channelvario,[])
        if( debug):
            print( 'channelvario is: {}'.format( channelvario))
        #sys.exit()
        if( debug):
            print( 'Looking for files like this:')
            for el in channelvario:
                print( el)
            #sys.exit()
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #print ( 'all selected data: {}'.format( a))
        
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelew:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        self.__LookForFilesMatching__()
        if( debug):
            print( 'Found files like this:')
            for el in self.files:
                print( el)
            #sys.exit()
        allfiles = []
        n = 0
        for vel in channelvario:
            for el in self.files:
                if( debug):
                    print( 'el\t= {},\tvel = {}'.format( el, vel))
                #if( fnmatch( el, '*' + vel)):
                #print( 'el endswith vel : {}'.format( el.endswith( vel)))
                if( el.endswith( vel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'Vario #{} file is: {}'.format( n, el))
        if( debug):
            print( 'allfiles is: {}'.format( allfiles))
        #sys.exit()
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'EW #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...Vario #{} file {} read.'.format( k, f))
            #print( temp.header['DataCompensationX'])
            if( self.ColList == ['x', 'y', 'z']):
                try:
                    datacompx = float( temp.header['DataCompensationX'])#*micro
                    datacompy = float( temp.header['DataCompensationY'])#*micro
                    datacompz = float( temp.header['DataCompensationZ'])#*micro
                    datacomp = np.array( [ datacompx, datacompy, datacompz])*1000.0
                    print( 'datacompensations are: X: {}, Y: {}, Z: {}'.format( datacomp[0], datacomp[1], datacomp[2]))
                    #sys.exit()
                    #print( 'temp',temp.ndarray[[1,2,3]])
                    #dummy = temp.ndarray[[1,2,3]]
                    temp.ndarray[[1,2,3]] = temp.ndarray[[1,2,3]] + datacomp
                    #print( 'temp',temp.ndarray[[1,2,3]])
                except:
                    pass
            #sys.exit()
            #colmp = mp.appendStreams([colmp, temp]) # CONCATENATE MAGPY STREAMS
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            #colmp.extend(temp , temp.header, temp.ndarray)
            for ol in temp.header.items():
                colmp.header[ol[0]] = ol[1]  # adding header info from temp to colmp for later self.sensname variable read
            print( '...Vario #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'Vario matching read...removing duplicates')
            #sys.exit()
        #print( 'colmp', colmp.ndarray[[1,2,3]])
        #dirvar = colmp.header.items()
        #print( dir( dirvar))
        #for el in dirvar:
        #    print( el)
        #print('\n\n\n\n')
        #for el in dir( colmp.header):
        #    print( el)
        #sys.exit()
        if( self.ApplyFlags):
            #print( '...applying flags')
            colmp = self.__ApplyFlags__(colmp)
        if( self.InterMagFilter):
            #print( '...applying intermagnet specified filter')
            self.__InterMagnet_filter__(colmp)
        
        if( np.any( colmp.ndarray[16] == b'P')): # CHECKING IF GPS-status is bad
            print( 'BAD GPS-STATUS!...SWITCHING TO SECONDARY TIME BEFORE TRIMMING!')
            colmp = colmp.use_sectime( swap = True) # SWITCH TIMECOLUMNS SO THAT NTP TIMECOLUMN IS IN COLUMN 0 AND TRIM CAN BE APPLIED PROPERLY
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        #print( sstartdate)
        #print( enddate)
        #print( colmp.ndarray[0])
        #sys.exit()
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        self.stationinfo = str( colmp.header['SensorID'])
        
        
        
        self.__GetSensName__( colmp)
        if( debug):
            print( 'self.sensname is:\n')
            for el in self.sensname:
                print( 'el is:\t{}'.format( el))
            #sys.exit()
        
        
        
        
        self.__GetColHeaderInfo__( colmp)
        if( debug):
            print( 'self.HeaderInfo is:\n')
            for el in self.HeaderInfo:
                print( 'el is:\t{}'.format( el))
            #sys.exit()
        
        
        
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        """
        if( debug):
            index_vec = []
            print( 'Available columns in variodata:\n\n\n')
            for k, el in enumerate( colmp.KEYLIST):
                print( '{}/\t{}'.format( k, el))
                testvar = list( chain( colmp.ndarray[ colmp.KEYLIST.index( el)]))
                numchk = [isinstance( f, float) for f in testvar]
                print( testvar)
                if( len( testvar) > 1 and all(numchk)):
                    index_vec.append( colmp.KEYLIST.index( el))
            index_vec.remove( colmp.KEYLIST.index( 'time'))
            print( 'Filled columns in variodata:\n\n\n')
            for k, el in enumerate( index_vec):
                print( '{}/\t{}'.format( k, el))
            #sys.exit()
        else:
            index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        """
        self.__GetDataInd__( colmp, ColList = self.ColList)
        index_vec = self.DataInd
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        if( debug):
            print( 'Got timestamps, datacolumn-names, column indices and arraydim')
        #sum_leng = len(iterateTime)
        """
        temp = matrix( list( chain( colmp.ndarray[index_vec]))).reshape( arraydim, -1)
        if( debug):
            for k, el in enumerate( temp):
                print( '\nrow[{}] of temp is:\t{}'.format( k, el))
        mseries = (temp).astype(float)#/ mue0
        
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'Vario dataarray reshaped...converting timestamps')
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().iteritems())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el in helpt:
                print( el)
            #sys.exit()
        ztime = NUM2DATE20TIME( num2date( helpt))
        if( debug):
            print( 'ztime is:')
            for el in ztime:
                print( el)
            #sys.exit()
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().iteritems() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - Vario data array')
            #sys.exit()
        
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        vseries = mseries
        """
        try:
            #xI = vario.KEYLIST.index('dx')
            #yI = vario.KEYLIST.index('dy')
            #zI = vario.KEYLIST.index('dz')
            #index_vec = [vario.KEYLIST.index('x'), vario.KEYLIST.index('y'), vario.KEYLIST.index('z')]#, vario.KEYLIST.index('f')]#, vario.KEYLIST.index('dx'), vario.KEYLIST.index('dy'), vario.KEYLIST.index('dz')]
            
            ##################
            # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
            ##################
            
            #from magpy.stream import num2date
            if( debug):
                print( 'iterateTime is:\n{}'.format( iterateTime))
            time_vario = iterateTime
            time_vario_zero = NUM2DATE20TIME( num2date( time_vario))
            if( debug):
                print( 'time vario zero is:\n{}'.format( time_vario_zero))
        except Exception as ex:
            print( 'Something went wrong with the timestamps')
            self.MyException(ex)
        
        try:
            #time_vario_zero = dump[0]
            #print time_vario_zero
            
            #vario = vario.ndarray[[xI, yI, zI]]
            vario = colmp.ndarray[index_vec]
            #varioarray = np.matrix( list( itertools.chain( *vario))).reshape( ( 3, -1)).T
            arraydim = nanmax( shape( index_vec))
            if( debug):
                print( 'numbers of rows in array:\t{}'.format( arraydim))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
                print( 'index_vec:\t{}'.format( index_vec))
            varioarray = matrix( list( chain( *vario)))#.reshape( ( arraydim, -1)).T
            if( debug):
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            varioarray = varioarray.reshape( arraydim, -1).T
            #print( ColNames)
            #sys.exit()
            if( debug):
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            #sys.exit()
            ietime, vario = self.__EquiInterpol__( time = time_vario_zero, data = varioarray)
            """
            #from magpy.stream import num2date
            stamp_starttime = NUM2DATE20TIME( num2date(time_vario[0])) # CONVERT TO ZEROTIME VALUES
            stamp_endtime = NUM2DATE20TIME( num2date(time_vario[-1])) # CONVERT TO ZEROTIME VALUES
            dt_vario = (stamp_endtime - stamp_starttime)/float(shape(time_vario)[0] - 1)
            #int_tvario = np.linspace( stamp_starttime, stamp_endtime, len( time_vario))
            int_tvario = np.arange( stamp_starttime, stamp_endtime + dt_vario, dt_vario)
            ietime = int_tvario
            """
            if( debug):
                print( 'shape of ietime is:\t{}'.format( np.shape( ietime)))
                print( 'shape of time_vario_zero is:\t{}'.format( np.shape( time_vario_zero)))
                print( 'shape of varioarray.T is:\t{}'.format( np.shape( varioarray.T)))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
            
        except Exception as ex:
            self.MyException(ex)
        vseries = vario
        vtime = ietime
        
        print( 'Reading Vario-Data...done\n')
        #print( 'vseries is\n{}'.format( vseries))
        #print( 'vtime is\n{}'.format( vtime))
        
        gc.collect()
        del gc.garbage[:]
        self.data = vseries
        self.zerotime = vtime
        return self.zerotime, self.data
        #return vtime, vseries#, np.vstack(( aseries, bseries))
    
    
    
    
    """
    READING picologtxt files
    --- under construction ---
    """
    def picologtxt( self):
        if( debug):
            print( '\n\n\nStarting picologtxt...')
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        #sstartdate = startdate
        startdate = self.starttime
        enddate = self.endtime
        sstartdate = startdate
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        #sys.exit()
        self.__getdays__()
        days = self.days
        startdate = self.starttime
        #channelse = ['pri0','pri1']
        #print(startdate)
        print ( '\n\n\n\tstarttime\t\t=\tzstarttime\n\n\n')
        print ( '\t{}\t=\t{}'.format( startdate, zstartdate))
        print ( '\n\n\n\tendtime\t\t\t=\tzendtime\n\n\n')
        print ( '\t{}\t=\t{}'.format( enddate, zenddate))
        #sys.exit()
        ###################################
        # TIMERANGE DEFINITION
        ###################################
        cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        
        colmp = []
        cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        
        fileending = '.' + self.filetype
        
        #########################
        # READ picologtxt
        #########################
        
        
        sensname = '2016'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
            #if( not self.SearchString.startswith( 'GSM90_1013973') and not self.SearchString.startswith( 'GSM19_6067473')):
            if( not self.SearchString.startswith( 'GSM90') and not self.SearchString.startswith( 'GSM19') and not self.SearchString.startswith( 'LEMI')):
                sensname = sensname[0:17] # reducing again to sensorname without date info
            if( debug):
                print( 'sensname is: {}'.format( sensname))
            #sys.exit()
        
        channelvario = [[sensname + fileending for f in days]]
        
        #sys.exit()
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        if( debug):
            print( 'channelvario is: {}'.format( channelvario))
        #sys.exit()
        
        channelvario = np.unique( sum(channelvario,[]))
        if( debug):
            print( 'channelvario is: {}'.format( channelvario))
        #sys.exit()
        if( debug):
            print( 'Looking for files like this:')
            for el in channelvario:
                print( el)
            #sys.exit()
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #print ( 'all selected data: {}'.format( a))
        
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelew:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        for el in channelvario:
            print(el)
        
        
        
        self.__LookForFilesMatching__()
        
        
        if( debug):
            print( 'Found files like this:')
            for el in self.files:
                print( el)
            #sys.exit()
        allfiles = []
        n = 0
        for vel in channelvario:
            for el in self.files:
                if( debug):
                    print( 'el\t= {},\tvel = {}'.format( el, vel))
                #if( fnmatch( el, '*' + vel)):
                #print( 'el endswith vel : {}'.format( el.endswith( vel)))
                if( el.endswith( vel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'Vario #{} file is: {}'.format( n, el))
        
        if( debug):
            print( 'allfiles is: {}'.format( allfiles))
        #sys.exit()
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'EW #{} file is: {}'.format( k+1, f))
        """
        #temp = DataStream()
        #import codecs# to reencode read files
        for k, f in enumerate( allfiles):
            #with codecs.open(f,'r','string-escape') as file:
            #temp=f.read()
            temp = open(f)#, encoding="ascii", errors="surrogateescape")  # READING picologtxt ascii files
            print( '...picologtxt #{} file {} read.'.format( k, f))
            #print( temp.header['DataCompensationX'])
            data = temp.read()
            data = data.split( '\n')
            
            #print( len( data))
            ztime = []
            dataarray = []
            print( data[0])
            print( self.ColList)
            #sys.exit()
            #sys.exit()
            #print( tI)
            #sys.exit()
            if( False):
                while( str( ''.join(e for e in data[0] if e.isalnum())).startswith('Time') or str( ''.join(e for e in data[0] if e.isalnum())).startswith('ms')):
                    print( data[0])
                    del data[0] # DROPPING HEADER INFO
            
            for row in data:
                #print( row)
                if( str( ''.join(e for e in row if e.isalnum())).startswith('Time') or str( ''.join(e for e in row if e.isalnum())).startswith('ms')):
                    #print( row)
                    row = row.split('\t')
                    #print( row)
                    #sys.exit()
                else:
                    #print( row)
                    row = row[1::] # CAUSE THERE IS A UNICODE SPECIAL CHARACTER '\x' AT POSITION 0
                    #print( row)
                    
                    row = row.split('\t')
                    #print( row)
                    #sys.exit()
                    row = [ el.replace( ',', '.') for el in row]
                    #print( row)
                    #sys.exit()
                    #
                    #print( row)
                    #print( len( ztime))
                    #print( )
                    #print( [float( row[1])/1000.0, float( row[2])/1000.0, float( row[3])/1000.0])
                    #sys.exit()
                    try:
                        ztime.append( float( int( row[0]))/1000.0)
                        #print( float( row[1]), float( row[2]), float( row[3]))
                        dataarray.append( [float( row[1])/1000.0, float( row[2])/1000.0, float( row[3])/1000.0])
                        #print( dataarray[-1])
                        #sys.exit()
                        
                    except:
                        pass
                
                #print( ztime[-1])
            #print( len( ztime))
            #print( np.array( dataarray))
            #print( np.shape( dataarray))
            #!!!! WORKING UNTIL HERE BUT NOT CHECKED FURTHER TO THE END OF READING PROCESS !!!!
            #sys.exit()
            ztime = np.array( ztime)
            #print( shape( ztime))
            #print( shape( dataarray))
            #sys.exit()
            dataarray = np.array( dataarray)
            sortind = np.argsort( ztime)
            dataarray = dataarray[sortind, :]
            ztime = ztime[sortind]
            #colmp = mp.appendStreams([colmp, temp]) # CONCATENATE MAGPY STREAMS
            #colmp = dataarray
            #del dataarray
            colmp = np.hstack( ( np.atleast_2d( ztime).T, dataarray)) # CONCATENATE MAGPY STREAMS
            
            #colmp.extend(temp , temp.header, temp.ndarray)
            #for ol in temp.header.items():
            #    colmp.header[ol[0]] = ol[1]  # adding header info from temp to colmp for later self.sensname variable read
            #print( '...Vario #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'picologtxt matching read...removing duplicates')
            #sys.exit()
        print( shape( colmp))
        #print( type( colmp))
        #sys.exit()
        
        
        res = date2num( STAMPTODATE( NUM2DATE20TIME( self.starttime) + colmp[:,0]))
        colmp[:,0] = res
        bakcolmp = colmp
        print( bakcolmp[:,0])
        #mydict = dict( zip( ['time', 'dx', 'dy', 'dz'], bakcolmp))
        dictlist = ['time', 'dx', 'dy', 'dz']
        colmp = DataStream()
        array = np.asarray( [ np.zeros( ( np.max( np.shape( bakcolmp)))) for el in colmp.KEYLIST], dtype = object)
        print( np.shape( array))
        #sys.exit()
        #array[]
        colmp.ndarray = array
        for dictel, d in zip( dictlist, bakcolmp.T):
            colmp.ndarray[colmp.KEYLIST.index(dictel)] = np.asarray( d, dtype = object)
        colmp.header['SensorID'] = 'PICOLOG'
        #colmp = colmp._put_column(colmp[:,0], 'time', columnname='Rain',columnunit='mm in 1h')
        #print( 'colmp', colmp.ndarray[[1,2,3]])
        #dirvar = colmp.header.items()
        #print( dir( dirvar))
        #for el in dirvar:
        #    print( el)
        #print('\n\n\n\n')
        #for el in dir( colmp.header):
        #    print( el)
        #sys.exit()
        if( True):
            if( self.ApplyFlags):
                #print( '...applying flags')
                colmp = self.__ApplyFlags__(colmp)
            if( self.InterMagFilter):
                #print( '...applying intermagnet specified filter')
                self.__InterMagnet_filter__(colmp)
            
            if( np.any( colmp.ndarray[16] == b'P')): # CHECKING IF GPS-status is bad
                print( 'BAD GPS-STATUS!...SWITCHING TO SECONDARY TIME BEFORE TRIMMING!')
                colmp = colmp.use_sectime( swap = True) # SWITCH TIMECOLUMNS SO THAT NTP TIMECOLUMN IS IN COLUMN 0 AND TRIM CAN BE APPLIED PROPERLY
            temp = colmp.trim( starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
            colmp = temp
            del temp
            #print( sstartdate)
            #print( enddate)
            #print( colmp.ndarray[0])
            #sys.exit()
            colmp = colmp.removeduplicates()
            colmp = colmp.sorting()
            self.stationinfo = str( colmp.header['SensorID'])
            
            
            
            self.__GetSensName__( colmp)
            print( 'self.sensname is:\n')
            for el in self.sensname:
                print( 'el is:\t{}'.format( el))
            if( debug):
                print( 'self.sensname is:\n')
                for el in self.sensname:
                    print( 'el is:\t{}'.format( el))
                #sys.exit()
            
            
            self.__GetTimeInd__(colmp)
            tI = self.TimeInd
        self.TimeInd = 0
        tI = self.TimeInd
        self.stationinfo = str( 'WIK-PICOLOG')
        self.sensname = str( 'PICOLOG')
        self.DataColNames = ['dx', 'dy', 'dz']
        #print( self.TimeInd, tI, self.stationinfo, self.sensname, self.DataColNames)
        #sys.exit()
        """
        if( debug):
            index_vec = []
            print( 'Available columns in variodata:\n\n\n')
            for k, el in enumerate( colmp.KEYLIST):
                print( '{}/\t{}'.format( k, el))
                testvar = list( chain( colmp.ndarray[ colmp.KEYLIST.index( el)]))
                numchk = [isinstance( f, float) for f in testvar]
                print( testvar)
                if( len( testvar) > 1 and all(numchk)):
                    index_vec.append( colmp.KEYLIST.index( el))
            index_vec.remove( colmp.KEYLIST.index( 'time'))
            print( 'Filled columns in variodata:\n\n\n')
            for k, el in enumerate( index_vec):
                print( '{}/\t{}'.format( k, el))
            #sys.exit()
        else:
            index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        """
        self.__GetDataInd__( colmp, ColList = self.ColList)
        
        index_vec = self.DataInd
        #index_vec = [0,1,2]
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[0].astype( float)
        del ztime
        if( debug):
            print( 'Got timestamps, datacolumn-names, column indices and arraydim')
        #sum_leng = len(iterateTime)
        """
        temp = matrix( list( chain( colmp.ndarray[index_vec]))).reshape( arraydim, -1)
        if( debug):
            for k, el in enumerate( temp):
                print( '\nrow[{}] of temp is:\t{}'.format( k, el))
        mseries = (temp).astype(float)#/ mue0
        
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'Vario dataarray reshaped...converting timestamps')
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().iteritems())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el in helpt:
                print( el)
            #sys.exit()
        ztime = NUM2DATE20TIME( num2date( helpt))
        if( debug):
            print( 'ztime is:')
            for el in ztime:
                print( el)
            #sys.exit()
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().iteritems() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - Vario data array')
            #sys.exit()
        
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        vseries = mseries
        """
        try:
            #xI = vario.KEYLIST.index('dx')
            #yI = vario.KEYLIST.index('dy')
            #zI = vario.KEYLIST.index('dz')
            #index_vec = [vario.KEYLIST.index('x'), vario.KEYLIST.index('y'), vario.KEYLIST.index('z')]#, vario.KEYLIST.index('f')]#, vario.KEYLIST.index('dx'), vario.KEYLIST.index('dy'), vario.KEYLIST.index('dz')]
            
            ##################
            # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
            ##################
            
            #from magpy.stream import num2date
            if( debug):
                print( 'iterateTime is:\n{}'.format( iterateTime))
            time_vario = iterateTime
            time_vario_zero = NUM2DATE20TIME( num2date( time_vario))
            if( debug):
                print( 'time vario zero is:\n{}'.format( time_vario_zero))
        except Exception as ex:
            print( 'Something went wrong with the timestamps')
            self.MyException(ex)
        
        try:
            #time_vario_zero = dump[0]
            #print time_vario_zero
            
            #vario = vario.ndarray[[xI, yI, zI]]
            vario = colmp.ndarray[index_vec]
            #varioarray = np.matrix( list( itertools.chain( *vario))).reshape( ( 3, -1)).T
            arraydim = nanmax( shape( index_vec))
            if( debug):
                print( 'numbers of rows in array:\t{}'.format( arraydim))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
                print( 'index_vec:\t{}'.format( index_vec))
            varioarray = matrix( list( chain( *vario)))#.reshape( ( arraydim, -1)).T
            if( debug):
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            varioarray = varioarray.reshape( arraydim, -1).T
            #print( ColNames)
            #sys.exit()
            if( debug):
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            #sys.exit()
            ietime, vario = self.__EquiInterpol__( time = time_vario_zero, data = varioarray)
            """
            #from magpy.stream import num2date
            stamp_starttime = NUM2DATE20TIME( num2date(time_vario[0])) # CONVERT TO ZEROTIME VALUES
            stamp_endtime = NUM2DATE20TIME( num2date(time_vario[-1])) # CONVERT TO ZEROTIME VALUES
            dt_vario = (stamp_endtime - stamp_starttime)/float(shape(time_vario)[0] - 1)
            #int_tvario = np.linspace( stamp_starttime, stamp_endtime, len( time_vario))
            int_tvario = np.arange( stamp_starttime, stamp_endtime + dt_vario, dt_vario)
            ietime = int_tvario
            """
            if( debug):
                print( 'shape of ietime is:\t{}'.format( np.shape( ietime)))
                print( 'shape of time_vario_zero is:\t{}'.format( np.shape( time_vario_zero)))
                print( 'shape of varioarray.T is:\t{}'.format( np.shape( varioarray.T)))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
            
        except Exception as ex:
            self.MyException(ex)
        vseries = vario
        vtime = ietime
        
        print( 'Reading Vario-Data...done\n')
        #print( 'vseries is\n{}'.format( vseries))
        #print( 'vtime is\n{}'.format( vtime))
        
        gc.collect()
        del gc.garbage[:]
        self.data = vseries
        self.zerotime = vtime
        return self.zerotime, self.data
        #return vtime, vseries#, np.vstack(( aseries, bseries))
    
    
    
    
    """
    READING IAGA-2002 ascii files
    --- under construction ---
    """
    def IAGAtxtRead( self):
        if( debug):
            print( '\n\n\nStarting IAGAtxtRead...')
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        #sstartdate = startdate
        startdate = self.starttime
        enddate = self.endtime
        sstartdate = startdate
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        #sys.exit()
        self.__getdays__()
        days = self.days
        print( 'days', days)
        #sys.exit()
        startdate = self.starttime
        #channelse = ['pri0','pri1']
        #print(startdate)
        print ( '\n\n\n\tstarttime\t\t=\tzstarttime\n\n\n')
        print ( '\t{}\t=\t{}'.format( startdate, zstartdate))
        print ( '\n\n\n\tendtime\t\t\t=\tzendtime\n\n\n')
        print ( '\t{}\t=\t{}'.format( enddate, zenddate))
        #sys.exit()
        ###################################
        # TIMERANGE DEFINITION
        ###################################
        cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        
        colmp = []
        cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        
        #print( 'self.filetype', self.filetype)
        #sys.exit()
        fileending = self.filetype
        print( 'fileending', fileending)
        #########################
        # READ picologtxt
        #########################
        
        if( debug):
            print( 'self.SearchString', self.SearchString)
        sensname = '2016'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            sensname = self.SearchString
            if( debug):
                print( 'sensname is: {}'.format( sensname))
                
        elif( ( len( self.SearchString) >= len( sensname)) & ( fileending.endswith( '.sec'))):
            """
            CASE OF IAGA-2002 ascii files
            """
            sensname = self.SearchString
        else:
            sensname = self.SearchString
            if( debug):
                print( 'sensname is: {}'.format( sensname))
                #sys.exit()
            #if( not self.SearchString.startswith( 'GSM90_1013973') and not self.SearchString.startswith( 'GSM19_6067473')):
            if( not self.SearchString.startswith( 'GSM90') and not self.SearchString.startswith( 'GSM19') and not self.SearchString.startswith( 'LEMI') and not self.SearchString.endswith( '.sec')):
                sensname = sensname[0:17] # reducing again to sensorname without date info
            if( debug):
                print( 'sensname is: {}'.format( sensname))
            #sys.exit()
        if( debug):
            print( 'self.SearchString', self.SearchString)
        #sys.exit()
        #for d in days:
        #    print( 'd', d)
        #    daystring = datetime.strftime( d, '%Y%m%d')
        #    print( 'daystring', daystring)
        #sys.exit()
        channelvario = [[sensname + datetime.strftime( f, '%Y%m%d') + fileending for f in days]] # appending days datainfo to searchlist
        
        #sys.exit()
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        if( debug):
            print( 'channelvario is: {}'.format( channelvario))
        #print( 'channelvario is: {}'.format( channelvario))
        #sys.exit()
        
        channelvario = np.unique( sum(channelvario,[]))
        if( debug):
            print( 'channelvario is: {}'.format( channelvario))
            #sys.exit()
        if( True):
            print( 'Looking for files like this:')
            for el in channelvario:
                print( el)
            #sys.exit()
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #print ( 'all selected data: {}'.format( a))
        
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelew:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        #for el in channelvario:
        #    print(el)
        #sys.exit()
        
        
        self.__LookForFilesMatching__()
        
        
        if( debug):
            print( 'Found files like this:')
            for el in self.files:
                print( el)
                #sys.exit()
            #sys.exit()
        allfiles = []
        n = 0
        for vel in channelvario:
            for el in self.files:
                if( debug):
                    print( 'el\t= {},\tvel = {}'.format( el, vel))
                #if( fnmatch( el, '*' + vel)):
                #print( 'el endswith vel : {}'.format( el.endswith( vel)))
                if( el.endswith( vel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'Vario #{} file is: {}'.format( n, el))
                    #sys.exit()
        #sys.exit()
        if( debug):
            print( 'allfiles is: {}'.format( allfiles))
        #sys.exit()
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        #print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
        #sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
        #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'EW #{} file is: {}'.format( k+1, f))
        """
        #temp = DataStream()
        #import codecs# to reencode read files
        """
        Checking how many data columns are needed
        """
        
        for k, f in enumerate( allfiles):
            columncount = 0
            #with codecs.open(f,'r','string-escape') as file:
            #temp=f.read()
            print( 'reading \n{}...'.format( f))
            temp = open(f)#, encoding="ascii", errors="surrogateescape")  # READING picologtxt ascii files
            print( '...IAGA-2002 txt #{} file \n{} read.'.format( k, f))
            
            #print( temp.header['DataCompensationX'])
            data = temp.read()
            bakdata = data
            data = data.split( '\n')
            if( debug):
                print( 'data')
                print( data)
                print( 'shape of data', np.shape(data))
                #sys.exit()
            #self.DataSourceHeader = []
            if( data[0].endswith('IAGA-2002                                    |')):
                linecount = 1
                for line in data[1::]:
                    if( line.startswith( ' Source of Data')):
                        #print( 'index found {}'.format( line.rfind( 'Source of Data') + len( ' Source of Data')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Source of Data') + len( ' Source of Data'):-1])
                        print( ' Source of Data {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Station Name')):
                        #print( 'index found {}'.format( line.rfind( 'Station Name') + len( ' Station Name')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Station Name') + len( ' Station Name'):-1])
                        print( ' Station Name {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' IAGA Code')):
                        #print( 'index found {}'.format( line.rfind( 'IAGA Code') + len( ' IAGA Code')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'IAGA Code') + len( ' IAGA Code'):-1])
                        print( ' IAGA Code {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Geodetic Latitude')):
                        #print( 'index found {}'.format( line.rfind( 'Geodetic Latitude') + len( ' Geodetic Latitude')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Geodetic Latitude') + len( ' Geodetic Latitude'):-1])
                        print( ' Geodetic Latitude {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Geodetic Longitude')):
                        #print( 'index found {}'.format( line.rfind( 'Geodetic Longitude') + len( ' Geodetic Longitude')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Geodetic Longitude') + len( ' Geodetic Longitude'):-1])
                        print( ' Geodetic Longitude {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Elevation')):
                        #print( 'index found {}'.format( line.rfind( 'Elevation') + len( ' Elevation')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Elevation') + len( ' Elevation'):-1])
                        print( ' Elevation {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Reported')):
                        #print( 'index found {}'.format( line.rfind( 'Reported') + len( ' Reported')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Reported') + len( ' Reported'):-1])
                        print( ' Reported {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Sensor Orientation')):
                        #print( 'index found {}'.format( line.rfind( 'Sensor Orientation') + len( ' Sensor Orientation')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Sensor Orientation') + len( ' Sensor Orientation'):-1])
                        print( ' Sensor Orientation {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Digital Sampling')):
                        #print( 'index found {}'.format( line.rfind( 'Digital Sampling') + len( ' Digital Sampling')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Digital Sampling') + len( ' Digital Sampling'):-1])
                        print( ' Digital Sampling {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Data Interval Type')):
                        #print( 'index found {}'.format( line.rfind( 'Data Interval Type') + len( ' Data Interval Type')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Data Interval Type') + len( ' Data Interval Type'):-1])
                        print( ' Data Interval Type {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Data Type')):
                        #print( 'index found {}'.format( line.rfind( 'Data Type') + len( ' Data Type')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Data Type') + len( ' Data Type'):-1])
                        print( ' Data Type {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' #')):
                        #print( 'index found {}'.format( line.rfind( '#') + len( ' #')))
                        #sys.exit()
                        #self.DataSource = line[ line.rfind( 'Data Type') + len( ' Data Type'):-1]
                        #print( 'string found {}'.format( self.DataSource))
                        linecount = linecount  + 1
                    elif( line.startswith( 'DATE')):
                        for el in line[:-1].split( ' '):
                            if( len( el) != 0):
                                columncount = columncount + 1
                        #print( 'assuming {} columns will be found'.format( columncount))
            elif( ( data[0].startswith('20')) | ( data[0].endswith('1 65')) | ( data[0].endswith('1 60')) | ( data[0].endswith('1 80'))):
                linecount = 1
                line = data[0]
                if( debug):
                    print( 'line in data[0::] is: {}'.format( line))
                    #sys.exit()
                if( line.startswith( '20')):
                    for el in line[:-1].split( ' '):
                        if( len( el) != 0):
                            columncount = columncount + 1
                    print( 'assuming {} columns will be found'.format( columncount))
                #sys.exit()
        
        #print( 'columncount', columncount)
        #sys.exit()
        """
        Reading data
        """
        allmydate = []
        #allx = [[] for i in range( columncount - 3)]
        allx = []
        for k, f in enumerate( allfiles):
            #with codecs.open(f,'r','string-escape') as file:
            #temp=f.read()
            print( 'reading \n{}...'.format( f))
            temp = open(f)#, encoding="ascii", errors="surrogateescape")  # READING picologtxt ascii files
            print( '...IAGA-2002 txt #{} file \n{} read.'.format( k, f))
            #print( temp.header['DataCompensationX'])
            data = temp.read()
            data = data.split( '\n')
            #self.DataSourceHeader = []
            if( data[0].endswith('IAGA-2002                                    |')):
                linecount = 1
                for line in data[1::]:
                    if( line.startswith( ' Source of Data')):
                        #print( 'index found {}'.format( line.rfind( 'Source of Data') + len( ' Source of Data')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Source of Data') + len( ' Source of Data'):-1])
                        print( ' Source of Data {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Station Name')):
                        #print( 'index found {}'.format( line.rfind( 'Station Name') + len( ' Station Name')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Station Name') + len( ' Station Name'):-1])
                        print( ' Station Name {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' IAGA Code')):
                        #print( 'index found {}'.format( line.rfind( 'IAGA Code') + len( ' IAGA Code')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'IAGA Code') + len( ' IAGA Code'):-1])
                        print( ' IAGA Code {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Geodetic Latitude')):
                        #print( 'index found {}'.format( line.rfind( 'Geodetic Latitude') + len( ' Geodetic Latitude')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Geodetic Latitude') + len( ' Geodetic Latitude'):-1])
                        print( ' Geodetic Latitude {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Geodetic Longitude')):
                        #print( 'index found {}'.format( line.rfind( 'Geodetic Longitude') + len( ' Geodetic Longitude')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Geodetic Longitude') + len( ' Geodetic Longitude'):-1])
                        print( ' Geodetic Longitude {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Elevation')):
                        #print( 'index found {}'.format( line.rfind( 'Elevation') + len( ' Elevation')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Elevation') + len( ' Elevation'):-1])
                        print( ' Elevation {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Reported')):
                        #print( 'index found {}'.format( line.rfind( 'Reported') + len( ' Reported')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Reported') + len( ' Reported'):-1])
                        print( ' Reported {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Sensor Orientation')):
                        #print( 'index found {}'.format( line.rfind( 'Sensor Orientation') + len( ' Sensor Orientation')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Sensor Orientation') + len( ' Sensor Orientation'):-1])
                        print( ' Sensor Orientation {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Digital Sampling')):
                        #print( 'index found {}'.format( line.rfind( 'Digital Sampling') + len( ' Digital Sampling')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Digital Sampling') + len( ' Digital Sampling'):-1])
                        print( ' Digital Sampling {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Data Interval Type')):
                        #print( 'index found {}'.format( line.rfind( 'Data Interval Type') + len( ' Data Interval Type')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Data Interval Type') + len( ' Data Interval Type'):-1])
                        print( ' Data Interval Type {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' Data Type')):
                        #print( 'index found {}'.format( line.rfind( 'Data Type') + len( ' Data Type')))
                        #sys.exit()
                        self.DataSourceHeader.append( line[ line.rfind( 'Data Type') + len( ' Data Type'):-1])
                        print( ' Data Type {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                    elif( line.startswith( ' #')):
                        #print( 'index found {}'.format( line.rfind( '#') + len( ' #')))
                        #sys.exit()
                        #self.DataSource = line[ line.rfind( 'Data Type') + len( ' Data Type'):-1]
                        #print( 'string found {}'.format( self.DataSource))
                        linecount = linecount  + 1
                    elif( line.startswith( 'DATE')):
                        #columncount = 0
                        #for el in line[:-1].split( ' '):
                        #    if( len( el) != 0):
                        #        columncount = columncount + 1
                        print( 'assuming {} columns will be found'.format( columncount))
                        linecount = linecount  + 1
                mydate = []
                x = [[] for i in range( columncount - 3)]
                #print( 'shape of x is: {}'.format( np.shape( x)))
                #sys.exit()
                
                for line in data[linecount::]:
                    elements = line.split( ' ')
                    usableind = np.argwhere( [len( f) != 0 for f in elements] ).flatten()
                    #print( 'usableind', usableind)
                    if( len( usableind) > 0):
                        date = elements[usableind[0]]
                        time = elements[usableind[1]]
                        time = time.split( '.')[0]
                        microsec = elements[usableind[1]].split( '.')[1]
                        doy = elements[2]
                        for i in range( columncount - 3):
                            #print( i)
                            #print( 'elements[i + 3]', elements[usableind[i + 3]])
                            x[i].append( float( elements[usableind[i + 3]]))
                            #y.append( float( elements[4]))
                            #z.append( float( elements[5]))
                            #f.append( float( elements[6]))
                        #print( 'x', x)
                        #sys.exit()
                        datestring = date + ' ' + time + ' ' + str( int( float(microsec)* 1000.0)) + ' ' + doy
                        #print( 'date + ' ' + time + ' ' + str( int( float(microsec)* 1000.0)): {}'.format( datestring))
                        mydate.append( datetime.strptime(datestring, "%Y-%m-%d %H:%M:%S %f %j"))
                        #print( '[-1] is: {}'.format( mydate[-1]))
                    #print( 'NUM2DATE20TIME( mydate[-1]) is: {}'.format( NUM2DATE20TIME( mydate[-1])))
                    #sys.exit()
                    #for el in line.split( ''):
                    #    print( 'el in line is: {}'.format( el))
                #print( 'self.DataSourceHeader: {}'.format( self.DataSourceHeader))
                #print( 'ztime' , ztime)
                #print( 'shape of ztime' , np.shape( ztime))
                #print( 'x' , x)
                #print( 'shape of dataarray' , np.shape( dataarray))
                #sys.exit()
            elif( ( data[0].startswith('20')) | ( data[0].endswith('1 65')) | ( data[0].endswith('1 60')) | ( data[0].endswith('80'))): # WIK - txt files
                linecount = 1
                self.DataSourceHeader.append( 'WIK-LEMI025')
                for line in data[0::]:
                    if( line.startswith( '20')):
                        #print( 'index found {}'.format( line.rfind( 'Source of Data') + len( ' Source of Data')))
                        #sys.exit()
                        #print( ' Source of Data {}'.format( self.DataSourceHeader[-1]))
                        linecount = linecount  + 1
                mydate = []
                #x = [[] for i in range( columncount - 10)]
                #print( 'shape of x is: {}'.format( np.shape( x)))
                #sys.exit()
                x = []
                baklinecount = linecount
                for k, line in enumerate( data[0::]):
                    #print( 'line in data is: {}'.format( line))
                    elements = line.split( ' ')
                    #print( 'elements in line: {}'.format( elements))
                    usableind = np.argwhere( [len( f) != 0 for f in elements] ).flatten()
                    #print( 'usableind', usableind)
                    if( len( usableind) > 0):
                        date = elements[usableind[0]] + '-' + elements[usableind[1]] + '-' + elements[usableind[2]]
                        time = elements[usableind[3]] + ':' + elements[usableind[4]] + ':' + elements[usableind[5]]
                        #time = time.split( '.')[0]
                        #microsec = elements[usableind[1]].split( '.')[1]
                        #doy = elements[2]
                        #for i in range( columncount - 10):
                            #print( i)
                            #print( 'elements[i + 3]', elements[usableind[i + 3]])
                            #x[i].append( [ float( elements[6]), float( elements[7]), float( elements[8])])
                            #print( 'x[i]', x[i])
                            #y.append( float( elements[4]))
                            #z.append( float( elements[5]))
                            #f.append( float( elements[6]))
                        x.append( [ float( elements[6]), float( elements[7]), float( elements[8])])
                        #print( 'x[-1]', x[-1])
                        #print( 'x', x)
                        #sys.exit()
                        datestring = date + ' ' + time
                        if( debug):
                            print( 'date + ' ' + time + ' ' + str( int( float(microsec)* 1000.0)): {}'.format( datestring))
                        #sys.exit()
                        mydate.append( datetime.strptime(datestring, "%Y-%m-%d %H:%M:%S.%f"))
                        #print( 'mydate[-1] is: {}'.format( mydate[-1]))
                    #print( 'NUM2DATE20TIME( mydate[-1]) is: {}'.format( NUM2DATE20TIME( mydate[-1])))
                    #sys.exit()
                    #for el in line.split( ''):
                    #    print( 'el in line is: {}'.format( el))
                #print( 'self.DataSourceHeader: {}'.format( self.DataSourceHeader))
                #print( 'ztime' , ztime)
                #print( 'shape of ztime' , np.shape( ztime))
                #print( 'x' , x)
                #print( 'shape of dataarray' , np.shape( dataarray))
                #sys.exit()
            [ allmydate.append( f) for f in mydate]
            #[ allx.append( f) for f in x]
            for l, triple in enumerate( np.array( x)):
                #print( 'triple[{}]={}'.format( l, triple))
                #print( 'np.shape( triple)', np.shape( np.atleast_2d( triple)))
                #sys.exit()
                allx.append( triple)
            
            #[ allx.append( f) for f in g for g in x]
            if( debug):
                print( 'np.shape( allx)', np.shape( allx))
                #sys.exit()
        #if( False):
        #    print( 'allmydate')
        #    if( type( allmydate[0]) == datetime):
        #        print( 'datetime.datetime found')
        #        sys.exit()
        #    for el in allmydate:
        #        print( 'el in allmydate {}'.format( el))
        #        print( 'type( el)', type( el))
        #        if( type( el) == datetime):
        #            print( 'datetime.datetime found')
        #            sys.exit()
        #sys.exit()
        ztime = np.array( NUM2DATE20TIME( allmydate))
        #print( 'ztime')
        #for el in ztime:
        #    print( 'el in ztime', el)
        #sys.exit()
        sortind = np.argsort( ztime)
        ztime = ztime[sortind]
        dataarray = np.array( allx)[sortind, :]
        #print( 'ztime')
        #for el in ztime:
        #    print( 'el in ztime {}'.format( el))
        #sys.exit()
        #
        #print( data)
        #print( np.shape( ztime))
        #print( np.shape( dataarray))
        #sys.exit()
        #plt.plot( np.diff( ztime), alpha = 0.2)
        #plt.show()
        #sys.exit()
        
        
        if( debug):
            print( 'IAGA-2002 matching read...removing duplicates')
            #sys.exit()
        #print( shape( colmp))
        #print( type( colmp))
        #sys.exit()
        
        colmp = np.hstack( ( np.atleast_2d( ztime).T, dataarray)) # CONCATENATE MAGPY STREAMS
        #print(' shape( colmp)', shape( colmp))
        res = date2num( STAMPTODATE( ztime))#.astype( float)
        #print( 'res')
        #for el in res:
        #    print( 'el in res', el)
        #sys.exit()
        colmp[:,0] = res
        bakcolmp = colmp
        #plt.plot( bakcolmp[:-1,0], np.diff( bakcolmp[:,1:], axis = 0), alpha = 0.2)
        #plt.show()
        #sys.exit()
        print( bakcolmp[:,0])
        print( 'shape of bakcolmp', np.shape( bakcolmp))
        #sys.exit()
        #mydict = dict( zip( ['time', 'dx', 'dy', 'dz'], bakcolmp))
        dictlist = ['time']#, 'x', 'y', 'z', 'f']
        for el in self.ColList:
            dictlist.append( el)
        #print( 'dictlist is: {}'.format( dictlist))
        #sys.exit()
        colmp = DataStream()
        array = np.asarray( [ np.zeros( ( np.max( np.shape( bakcolmp)))) for el in colmp.KEYLIST], dtype = object)
        #print( np.shape( array))
        #sys.exit()
        #array[]
        #plt.plot( ztime, dataarray, alpha = 0.2)
        #plt.show()
        #sys.exit()
        colmp.ndarray = array
        for dictel, d in zip( dictlist, bakcolmp.T):
            colmp.ndarray[colmp.KEYLIST.index(dictel)] = np.asarray( d, dtype = object)
        if( data[0].endswith('IAGA-2002                                    |')):
            colmp.header['SensorID'] = self.DataSourceHeader[2]#'GFZ'
        else:
            colmp.header['SensorID'] = str( 'UNKNOWN')
        #colmp = colmp._put_column(colmp[:,0], 'time', columnname='Rain',columnunit='mm in 1h')
        #print( 'colmp', colmp.ndarray[[1,2,3]])
        #dirvar = colmp.header.items()
        #print( dir( dirvar))
        #for el in dirvar:
        #    print( el)
        #print('\n\n\n\n')
        #for el in dir( colmp.header):
        #    print( el)
        #sys.exit()
        if( False):
            for dictel in dictlist:
                if( dictel != 'time'):
                    plt.plot( colmp.ndarray[colmp.KEYLIST.index('time')], colmp.ndarray[colmp.KEYLIST.index(dictel)], alpha = 0.2)
            plt.show()
            sys.exit()
        if( True):
            if( self.ApplyFlags):
                #print( '...applying flags')
                colmp = self.__ApplyFlags__(colmp)
            if( self.InterMagFilter):
                #print( '...applying intermagnet specified filter')
                self.__InterMagnet_filter__(colmp)
            
            if( np.any( colmp.ndarray[16] == b'P')): # CHECKING IF GPS-status is bad
                print( 'BAD GPS-STATUS!...SWITCHING TO SECONDARY TIME BEFORE TRIMMING!')
                colmp = colmp.use_sectime( swap = True) # SWITCH TIMECOLUMNS SO THAT NTP TIMECOLUMN IS IN COLUMN 0 AND TRIM CAN BE APPLIED PROPERLY
            #print( 'sstartdate', sstartdate)
            #print( 'enddate', enddate)
            #print( 'STAMPTODATE( ztime[0])', STAMPTODATE( ztime[0]))
            #print( 'STAMPTODATE( ztime[-1])', STAMPTODATE( ztime[-1]))
            if( debug):
                print( 'colmp.ndarray[0] before triming', colmp.ndarray[0])
            temp = colmp.trim( starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
            colmp = temp
            if( debug):
                print( 'colmp.ndarray[0] after triming', colmp.ndarray[0])
            del temp
            #print( sstartdate)
            #print( enddate)
            #print( colmp.ndarray[0])
            #sys.exit()
            colmp = colmp.removeduplicates()
            if( debug):
                print( 'colmp.ndarray[0] after removedduplicates', colmp.ndarray[0])
            colmp = colmp.sorting()
            if( debug):
                print( 'colmp.ndarray[0] after sorting', colmp.ndarray[0])
            try:
                self.stationinfo = str( colmp.header['SensorID'])
            except:
                self.stationinfo = str( 'UNKNOWN')
            
            
            
            self.__GetSensName__( colmp)
            print( 'self.sensname is:\n')
            for el in self.sensname:
                print( 'el is:\t{}'.format( el))
            if( debug):
                print( 'self.sensname is:\n')
                for el in self.sensname:
                    print( 'el is:\t{}'.format( el))
                #sys.exit()
            
            
            self.__GetTimeInd__(colmp)
            tI = self.TimeInd
        self.TimeInd = 0
        tI = self.TimeInd
        #self.stationinfo = str( 'WIK-PICOLOG')
        #self.sensname = str( 'PICOLOG')
        if( False):
            self.DataColNames = ['x', 'y', 'z', 'f']
        else:
            self.DataColNames = self.ColList
        #print( self.TimeInd, tI, self.stationinfo, self.sensname, self.DataColNames)
        #sys.exit()
        self.__GetDataInd__( colmp, ColList = self.ColList)
        
        index_vec = self.DataInd
        #index_vec = [0,1,2]
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[0].astype( float)
        #plt.plot( np.diff( iterateTime), alpha = 0.2)
        #plt.show()
        #sys.exit()
        del ztime
        if( debug):
            print( 'Got timestamps, datacolumn-names, column indices and arraydim')
        #sys.exit()
        #sum_leng = len(iterateTime)
        #print( 'iterateTime', iterateTime)
        #sys.exit()
        try:
            #xI = vario.KEYLIST.index('dx')
            #yI = vario.KEYLIST.index('dy')
            #zI = vario.KEYLIST.index('dz')
            #index_vec = [vario.KEYLIST.index('x'), vario.KEYLIST.index('y'), vario.KEYLIST.index('z')]#, vario.KEYLIST.index('f')]#, vario.KEYLIST.index('dx'), vario.KEYLIST.index('dy'), vario.KEYLIST.index('dz')]
            
            ##################
            # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
            ##################
            
            #from magpy.stream import num2date
            if( debug):
                print( 'iterateTime is:\n{}'.format( iterateTime))
            time_vario = iterateTime
            #print( 'time_vario', time_vario)
            #sys.exit()  
            time_vario_zero = NUM2DATE20TIME( num2date( time_vario))
            if( debug):
                print( 'time vario zero is:\n{}'.format( time_vario_zero))
            #sys.exit()
        except Exception as ex:
            print( 'Something went wrong with the timestamps')
            self.MyException(ex)
        #import matplotlib.pyplot as plt
        #plt.plot( time_vario_zero, ( colmp.ndarray[index_vec]).T, alpha = 0.2)
        #plt.show()
        #sys.exit()
        try:
            #time_vario_zero = dump[0]
            #print time_vario_zero
            
            #vario = vario.ndarray[[xI, yI, zI]]
            vario = colmp.ndarray[index_vec]
            #varioarray = np.matrix( list( itertools.chain( *vario))).reshape( ( 3, -1)).T
            arraydim = nanmax( shape( index_vec))
            if( debug):
                print( 'numbers of rows in array:\t{}'.format( arraydim))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
                print( 'index_vec:\t{}'.format( index_vec))
            varioarray = matrix( list( chain( *vario)))#.reshape( ( arraydim, -1)).T
            if( debug):
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            varioarray = varioarray.reshape( arraydim, -1).T
            #print( ColNames)
            #sys.exit()
            if( debug):
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            #sys.exit()
            ietime, vario = self.__EquiInterpol__( time = time_vario_zero, data = varioarray)
            """
            #from magpy.stream import num2date
            stamp_starttime = NUM2DATE20TIME( num2date(time_vario[0])) # CONVERT TO ZEROTIME VALUES
            stamp_endtime = NUM2DATE20TIME( num2date(time_vario[-1])) # CONVERT TO ZEROTIME VALUES
            dt_vario = (stamp_endtime - stamp_starttime)/float(shape(time_vario)[0] - 1)
            #int_tvario = np.linspace( stamp_starttime, stamp_endtime, len( time_vario))
            int_tvario = np.arange( stamp_starttime, stamp_endtime + dt_vario, dt_vario)
            ietime = int_tvario
            """
            if( debug):
                print( 'shape of ietime is:\t{}'.format( np.shape( ietime)))
                print( 'shape of time_vario_zero is:\t{}'.format( np.shape( time_vario_zero)))
                print( 'shape of varioarray.T is:\t{}'.format( np.shape( varioarray.T)))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
            
        except Exception as ex:
            self.MyException(ex)
        vseries = vario
        vtime = ietime
        
        print( 'Reading {}-Data...done\n'.format( colmp.header['SensorID']))
        #print( 'vseries is\n{}'.format( vseries))
        #print( 'vtime is\n{}'.format( vtime))
        
        gc.collect()
        del gc.garbage[:]
        self.data = vseries#* nano
        self.zerotime = vtime
        return self.zerotime, self.data
        #return vtime, vseries#, np.vstack(( aseries, bseries))
    
    
    
    def GFZKpRead( self):
        if( debug):
            print( '\n\n\nStarting GFZKpRead...')
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        #sstartdate = startdate
        startdate = self.starttime
        enddate = self.endtime
        sstartdate = startdate
        
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        self.__getdays__()
        days = self.days
        startdate = self.starttime
        #sys.exit()
        #channelse = ['pri0','pri1']
        #print(startdate)
        print ( '\n\n\n\tstarttime\t\t=\tzstarttime\n\n\n')
        print ( '\t{}\t=\t{}'.format( startdate, zstartdate))
        print ( '\n\n\n\tendtime\t\t\t=\tzendtime\n\n\n')
        print ( '\t{}\t=\t{}'.format( enddate, zenddate))
        #sys.exit()
        ###################################
        # TIMERANGE DEFINITION
        ###################################
        cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        
        cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        ###################################
        # READ IN MAGNETIC RECORDS
        ###################################
        #dirstring = [str('./gradfiles/EW'), str('./gradfiles/NS'), str('./gradfiles/VS')]
        #######################
        #ewpathrelpath = str('sources/gradfiles/EW')
        #nspathrelpath = str('sources/gradfiles/NS')
        #vspathrelpath = str('sources/gradfiles/VS')
        #dirstring = [os.path.join( commonpath, ewpathrelpath), os.path.join( commonpath, nspathrelpath), os.path.join( commonpath, vspathrelpath)]
        #######################
        #import fnmatch
        #from magpy.stream import read, datetime, DataStream
        #from scipy.interpolate import interp1d
        #from itertools import chain
        colmp = DataStream()
        
        fileending = '.' + self.filetype
        ########################
        # READ EW, NS, V
        #########################
        
        
        
        
        
        
        #########################
        # READ SINGLE SENSOR AXIS
        #########################
        
        
        #if( len( self.SearchString) < len( 'GP20S3EW_111201_0001_')):
        sensname = 'gfzkp'
        #sensname = sensname + self.SearchString
        #sensname = 'GP20S3EW_111201_0001_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelsg = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y%m')  + fileending for f in days]]
        #channelsg = np.unique( channelsg)
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        
        channelsg = sum(channelsg,[])
        if( debug):
            print( 'Looking for files like this:')
            for el in channelsg:
                print( el)
            #sys.exit()
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #print ( 'all selected data: {}'.format( a))
        
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelsg:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        self.__LookForFilesMatching__()
        if( debug):
            print( 'Found files like this:')
            for el in self.files:
                print( el)
            #sys.exit()
        allfiles = []
        n = 0
        print( 'looking for sgel: {}'.format( channelsg))
        for sgel in channelsg:
            if( debug):
                print( 'looking for sgel: {}'.format( sgel))
            for el in self.files:
                if( fnmatch( el, '*' + sgel)):
                    n = n + 1
                    allfiles.append( el)
                    print( '{} #{} file is: {}'.format( self.SearchString, n, el))
                    #sys.exit()
        #sys.exit()
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
            #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'EW #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        #print(allfiles)
        #sys.exit()
        
        
        
        self.__GetColHeaderInfo__( read( allfiles[0]))
        if( debug):
            print( 'self.HeaderInfo is:\n')
            for el in self.HeaderInfo:
                print( 'el is:\t{}'.format( el))
            #sys.exit()
        fndheaderinfo = read( allfiles[0]).header # backup of temp.header for later use
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            #print( temp.header.keys())
            #print( temp.header.values())
            #sys.exit()
            #print(temp.header)
            #print( self.SearchString[0:self.SearchString.rfind( '_')])
            try:
                self.stationinfo = [temp.header['SensorName'], temp.header['SensorSerialNum'], temp.header['SensorRevision']]
            except:
                self.stationinfo = [self.SearchString[0:self.SearchString.index( 'kp') + 2], self.SearchString[self.SearchString.index( 'kp') + 1:self.SearchString.rfind( '.cdf')],'']
            identstr = str( self.stationinfo[0]) + str( self.stationinfo[1]) + str( self.stationinfo[2])
            print( '...{} #{} file {} read.'.format( identstr, k, f))
            #sys.exit()
            #print(k, temp.ndarray[2])
            #colmp = mp.appendStreams([colmp, temp]) # CONCATENATE MAGPY STREAMS
            print( 'actual colmp length is: {}'.format( colmp.length))
            print( 'temp length to be added is: {}'.format( temp.length))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            #colmp.extend(temp , temp.header, temp.ndarray)
            print( '...{} #{} file {} appended.'.format( identstr, k, f))
        colmp.header = fndheaderinfo
        if( debug):
            print( '{} matching read...removing duplicates'.format( identstr))
            #sys.exit()
        if( self.ApplyFlags):
            colmp = self.__ApplyFlags__(colmp)
        if( self.InterMagFilter):
            self.__InterMagnet_filter__(colmp)
        
        #print( sstartdate, enddate)
        #sys.exit()
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        #print( colmp.ndarray[2])
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        #index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        self.__GetDataInd__( colmp, ColList = self.ColList)
        index_vec = self.DataInd
        ColNames = self.DataColNames
        
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        #sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        if( debug):
            print( 'iterateTime: {}'.format( iterateTime))     
        #sys.exit()
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung

        if( debug):
            print( '{} dataarray reshaped...converting timestamps'.format( self.SearchString))
            #sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        print( 'found average internal POSIX sampling interval is {} days'.format( dt))
        #sys.exit()
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        
        #print( 'cut_start, cut_end')
        #print( cut_start, cut_end)
        #print( 'iedt')
        #print( iedt)
        
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            print( 'ietime is: {}'.format( ietime))
        
        if( debug):
            varlist = list( locals().items())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        if( debug):
            print( 'helpt is {}'.format( helpt))
            print( 'shape of helpt is {}'.format( np.shape( helpt)))
        #sys.exit()
        #sys.exit()
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            print( 'nanmax( shape( helpt)) > nanmax( shape( mseries))')
            helpt = helpt[0:nanmax( shape( mseries)):]
        #sys.exit()
        if( debug):
            print( 'helpt is:')
            for el, dl in zip( helpt, num2date( helpt)):
                print( 'helpt', el, 'num2date( helpt)', dl)
            #sys.exit()
        print( 'Deriving ztime...')
        if( False):
            helpt = np.sort( np.nanmean( helpt) + (np.random.sample( len( helpt)) - 0.5)* 1.0* np.nanstd( helpt))
        #print( 'help found is: {}'.format( helpt))
        #sys.exit()
        ztime = NUM2DATE20TIME( num2date( helpt)) # GETTING zerotime timestamps
        #sys.exit()
        if( debug):
            print( 'ztime found is: {}'.format( ztime))
        print( 'ztime derived.')
        #sys.exit()
        ztimedt = ( ztime[-1] - ztime[0])/ float( len( ztime) - 1)
        print( 'found ztime dt: {} seconds'.format( ztimedt))
        if( debug):
            for el in (ztime[1::] - ztime[:-1:]):
                print( 'ppm difference to nominal sampling interval {}'.format( np.abs( el - ztimedt)/ ztimedt* 1000000.0))
            #sys.exit()
        #sys.exit()
        ##########################
        #BEGIN DEBUG OF TIMESTAMPS !!!! KEEP FOR DEBUGGING !!!!
        ##########################
        if( False):
            print( 'Deriving checkstamps...')
            checkdates = STAMPTODATE( ztime)
            print( 'checkstamps derived.')
            for el in checkdates:
                year = el.year
                print( el)
                #if( year > 2020):
                #    sys.exit()
            #´sys.exit()
            print( 'Deriving recheckztime...')
            recheckztime = NUM2DATE20TIME( checkdates)
            print( 'recheckztime derived.')
            #sys.exit()
        if( False):
            if( True):
                print( 'helpt is:')
                #from datetime import timezone
                for k, ( el, dl, cl, bl, al) in enumerate( zip( helpt, num2date( helpt), ztime, checkdates, recheckztime)):
                    print( k, el, dl, cl, bl, al, ( dl - bl).total_seconds())
                    #sys.exit()
                    #if( np.abs( al - cl) > 1.0):
                    if( bl.year == 2021):
                        print( 'PROBLEM WITH ztime or with STAMPTODATE...STOPPING')
                        #!!! WO IS DAS PROBLE MIT STAMPTODATE !!! ztime passt vermeintlich !!!
                        sys.exit()
                    #sleep( 1)
                sys.exit()
            else:
                print( 'helpt is:')
                #from datetime import timezone
                for k, ( el, dl, cl) in enumerate( zip( helpt, num2date( helpt), ztime)):
                    print( k, el, dl, cl)#, al - cl)
            sys.exit()
        
        
        
        
        
        
        #END DEBUG
        ##########################
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().items() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - {} data array'.format( self.SearchString))
            #sys.exit()
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        sgmseries = mseries
        sgtime = ietime
        
        print( 'Reading {}-Data...done\n'.format(self.SearchString))
        
        
        
        """
        ######################################
        # DEFINE ACURRATE SENSOR POSITIONS
        ######################################
        sensorpos_N = array( [ -34855.5869, 310273.3534, 1087.85])
        #sensorpos_S = array( [ -34856.7915, 310073.3882, 1086.229]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_S = array( [ -34856.7915, 310073.3882, 1087.229])
        sensorpos_NS = array( [ -34856.4903, 310123.3873, 1087.16])
        #sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.64]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.14])
        #sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.54]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.14])
        sensorpos_W = array( [ -34947.11138, 310137.2636, 1087.24])
        sensorpos_TB = array( [ -34856.47464, 310125.9872, 1087.17])
        sensorpos_TA = array( [ -34856.43362, 310132.7971, 1087.2])
        sensorpos_B = array( [ -34856.43362, 310125.7971, 887.2])
        
        sensPos = vstack( ( sensorpos_N, sensorpos_S, sensorpos_NS, sensorpos_E, sensorpos_EW, sensorpos_W, sensorpos_TB, sensorpos_TA, sensorpos_B))
        
        sensName = array( ['N', 'S', 'NS', 'E', 'EW', 'W', 'TB', 'TA', 'B'])
        sensPos = hstack( ( np.atleast_2d( sensName).T, sensPos))
        """
        sensPos = self.Positions()
        
        ######################################
        # CUT OUT SAMPLES WHICH ARE AVAILABLE FROM ALL SENSORS
        ######################################
        common_starttime = np.max([ sgtime[0]])
        common_endtime = np.min( [sgtime[-1]])
        
        
        sg_t_minind = np.argmin( np.abs( sgtime - common_starttime))
        sg_t_maxind = np.argmin( np.abs( sgtime - common_endtime))
        SG_leng = sg_t_maxind - sg_t_minind
        
        
        common_leng = np.min( [SG_leng])
        if( debug):
            print( 'common_leng is: {}'.format( common_leng))
        #sys.exit()
        if( common_leng == SG_leng):
            si = float( sgtime[sg_t_maxind] - sgtime[sg_t_minind])/ float(SG_leng - 1)
        else:
            print( 'ERROR cutting only sensors data from all available sensors')
            return
        newdt = (common_endtime - common_starttime)/(float(common_leng) - 1.0)
        print( '\n\n\nResampling with new sampling intervall: {} seconds'.format( newdt))
        #ietime = np.linspace(common_starttime, common_endtime, common_leng) # changed due to errors in linspace routine
        ietime = np.arange(common_starttime, common_endtime + newdt, newdt)
        
        
        # SG
        temp = interp1d( sgtime, sgmseries,fill_value='extrapolate',kind='linear')(ietime)
        sgmseries = temp
        
        print( '\n\n\n...done, stacking arrays together.')
        ######################################
        # CONCATENATE ALL THREE DIRECTIONS
        ######################################
        mseries = sgmseries#* pico
        print( '\n\n\n...done.')
        ######################################
        print( 'Reading absolutes files between {} and {} done'.format( startdate, enddate))
        self.zerotime = ietime
        self.data = mseries
        self.sensPos = sensPos
        gc.collect()
        del gc.garbage[:]
        #self.sensPos
        return self.zerotime, self.data#, self.sensPos#, np.vstack(( aseries, bseries))
    
    
    
    def dBRead( self):
        if( debug):
            print( '\n\n\nStarting dBRead...')
        #from scipy.interpolate import interp1d
        startdate = self.starttime
        enddate = self.endtime
        ##################
        # CONNECT TO DATABASE
        ##################
        
        #from magpy.database import mysql, readDB
        #from inspect import stack, getmodule
        #from magpy.stream import num2date
        
        nsmissing = False
        ewmissing = False
        vmissing = False
        print ( '\n\n\n\tstarttime\n\n\n')
        print ( '\t{}'.format( startdate))
        print ( '\n\n\n\tendtime\n\n\n')
        print ( '\t{}'.format( enddate))
        
        #print 'datetime of processing is: {}'.format( datetime.utcnow()), getmodule( stack()[1][0])
        try:
            self.__dBInit__() # initialize database connection
            
            
            ##################
            # NS
            #
            # READ IN SUPERGRAD NS-SENSORS GRADIENTS
            ##################
            try:
                ns = readDB( self.dB, 'GP20S3NS_012201_0001_0001', starttime = startdate, endtime = enddate)
                print( 'NS-data read...')
            except Exception as ex:
                #self.MyException(ex)
                print( 'Reading ns-data failed with execption\n{}...'.format( ex))
                return
                #ns = readDB( db, 'GP20S3NS_012201_0001_0001', starttime = startdate, endtime = enddate)
                #ns.sorting()
            try:
                ns = ns.removeduplicates()
                print( 'length of ns is:\t{}'.format( ns.length()))
            except Exception as ex:
                print( 'removing duplicates of ns-data failed with execption\n{}...'.format( ex))
                #self.MyException(ex)
                return
                
            
            ##################
            # IDENTIFY CORRECT COLUMNS
            ##################
            # headers are in 
            # ns.header
            #for el in ns.selectkeys():
            #    print('KEYLIST entries of NS are:\t{}'.format( el))
            self.__GetTimeInd__(ns)
            tI = self.TimeInd
            self.__GetDataInd__(ns)
            index_vec = self.DataInd
            ColNames = self.DataColNames
            try:
                #xI = ns.KEYLIST.index('dx')
                #yI = ns.KEYLIST.index('dy')
                #zI = ns.KEYLIST.index('dz')
                #index_vec = [ns.KEYLIST.index('x'), ns.KEYLIST.index('y'), ns.KEYLIST.index('z'), ns.KEYLIST.index('dx'), ns.KEYLIST.index('dy'), ns.KEYLIST.index('dz')]
                arraydim = nanmax( shape( index_vec))
                ##################
                # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
                ##################
                
                
                time_ns = ns.ndarray[tI]
                #goodind = where( time_ns != None)[0]#.flatten
                #print( 'shape of goodind: {}'.format( shape( goodind)))
                #print( 'shape of time_ns: {}'.format( shape( time_ns)))
                #time_ns = time_ns[goodind]
                #print( 'time_ns is: {}'.format( time_ns))
                time_ns_zero = NUM2DATE20TIME( num2date( time_ns))
                #time_ns_zero = dump[0]
                #ns = ns.ndarray[[xI, yI, zI]]
                ns = ns.ndarray[index_vec]
                #nsarray = np.matrix( list( itertools.chain( *ns))).reshape( ( 3, -1)).T
                nsarray = matrix( list( chain( *ns))).reshape( arraydim, -1).T
                if( debug):
                    print( 'Shape of NS-matrix before interpolation:\t{}'.format( np.shape( nsarray)))
                
                stamp_starttime = NUM2DATE20TIME( num2date(time_ns[0])) # CONVERT TO ZEROTIME VALUES
                stamp_endtime = NUM2DATE20TIME( num2date(time_ns[-1])) # CONVERT TO ZEROTIME VALUES
                dt_ns = (stamp_endtime - stamp_starttime)/float(shape(time_ns)[0] -1)
                #int_tns = np.linspace( stamp_starttime, stamp_endtime, len( time_ns))
                int_tns = np.arange( stamp_starttime, stamp_endtime + dt_ns, dt_ns)
                #print( 'type of time_ns is:\t{}'.format( type( time_ns)))
                #print( 'type of time_ns_zero is:\t{}'.format( type( time_ns_zero)))
                #print( 'type of nsarray.T is:\t{}'.format( type( nsarray.T)))
                #print( 'type of int_tns is:\t{}'.format( type( int_tns)))
                if( debug):
                    print( 'shape of nsarray before checking=\t{}'.format( np.shape( nsarray)))
                self.__RmNanInf__( nsarray)
                if( debug):
                    print( 'Goodind in NS:\n\n\n')
                    #print( self.goodind)
                    print( 'shape of self.goodind=\t{}'.format( np.shape( self.goodind)))
                nnsarray = np.zeros( ( 6, np.shape( int_tns)[0]))
                for k, ind in enumerate( self.goodind):
                    if( debug):
                        print( k, ind)
                    nnsarray[k, :] = interp1d( time_ns_zero[ind], np.array(nsarray[ind, k]).flatten(), bounds_error=False, fill_value = 'extrapolate', kind = 'linear')( int_tns)
                nsarray = nnsarray
                if( debug):
                    print( 'nsarray=\t{}'.format( nsarray))
                    print( 'shape of nsarray=\t{}'.format( np.shape( nsarray)))
                #sys.stdin.readline()
                if( debug):
                    for k, el in enumerate( nsarray):
                        print( 'el[{}]\t=\t{}'.format( k, el))
                        print( 'length of el[{}]\t=\t{}'.format( k, len( el)))
                #sys.stdin.readline()
                #print ( ' len(int_tns): ', len(int_tns), ' len(time_ns): ', len(time_ns), 'si is: ', dt_ns)
            except Exception as ex:
                print( 'reading ns-data failed with execption {}...'.format( ex))
                print( '...replacing data with zeros')
                int_tns =  np.zeros( ( 1, 1))
                self.nsmissing = True
                #db.close() #closing connection to database
                #return
            
            
            
            ##################
            # EW
            #
            # READ IN SUPERGRAD EW-SENSORS GRADIENTS
            ##################
            try:
                ew = readDB( self.dB, 'GP20S3EW_111201_0001_0001', starttime = startdate, endtime = enddate)
            except Exception as ex:
                #self.MyException(ex)
                print( 'Reading ew-data failed with execption\n{}...'.format( ex))
                ewmissing = True
                #return
                #ew = readDB( db, 'GP20S3EW_111201_0001_0001', starttime = startdate, endtime = enddate)
                #ew.sorting()
            if( not ewmissing):
                try:
                    ew = ew.removeduplicates()
                    print( 'length of ew is:\t{}'.format( ew.length()))
                except Exception as ex:
                    print( 'removing duplicates of ew-data failed with execption\n{}...'.format( ex))
                    #self.MyException(ex)
                    ewmissing = True
                    #return
            
            
            ##################
            # IDENTIFY CORRECT COLUMNS
            ##################
            if( not ewmissing):
                self.__GetTimeInd__(ew)
                tI = self.TimeInd
                self.__GetDataInd__(ew)
                index_vec = self.DataInd
                ColNames = self.DataColNames
                ewmissing = self.missing
            if( not ewmissing):
                try:
                    #xI = ew.KEYLIST.index('dx')
                    #yI = ew.KEYLIST.index('dy')
                    #zI = ew.KEYLIST.index('dz')
                    #index_vec = [ew.KEYLIST.index('x'),ew.KEYLIST.index('y'),ew.KEYLIST.index('z'),ew.KEYLIST.index('dx'),ew.KEYLIST.index('dy'),ew.KEYLIST.index('dz')]
                    arraydim = nanmax( shape( index_vec))
                    ##################
                    # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
                    ##################
                    
                    time_ew = ew.ndarray[tI]
                    time_ew_zero = NUM2DATE20TIME( num2date( time_ew))
                    #time_ew_zero = dump[0]
                    #ew = ew.ndarray[[xI, yI, zI]]
                    ew = ew.ndarray[index_vec]
                    #ewarray = np.matrix( list( itertools.chain( *ew))).reshape( ( 3, -1)).T
                    ewarray = matrix( list( chain( *ew))).reshape( arraydim, -1).T
                    
                    
                    stamp_starttime = NUM2DATE20TIME( num2date(time_ew[0])) # CONVERT TO ZEROTIME VALUES
                    stamp_endtime = NUM2DATE20TIME( num2date(time_ew[-1])) # CONVERT TO ZEROTIME VALUES
                    dt_ew = (stamp_endtime - stamp_starttime)/float(shape(time_ew)[0] -1)
                    #int_tew = np.linspace( stamp_starttime, stamp_endtime, len( time_ew))
                    int_tew = np.arange( stamp_starttime, stamp_endtime + dt_ew, dt_ew)
                    #print( 'type of time_ew is:\t{}'.format( type( time_ew)))
                    #print( 'type of time_ew_zero is:\t{}'.format( type( time_ew_zero)))
                    #print( 'type of ewarray.T is:\t{}'.format( type( ewarray.T)))
                    #print( 'type of int_tew is:\t{}'.format( type( int_tew)))
                    #ewarray = interp1d( time_ew_zero, ewarray.T, fill_value = 'extrapolate', kind = 'linear')( int_tew)
                    self.__RmNanInf__(ewarray)
                    eewarray = np.zeros( ( 6, np.shape( int_tew)[0]))
                    for k, ind in enumerate( self.goodind):
                        eewarray[k, :] = interp1d( time_ew_zero[ind], np.array(ewarray[ind, k]).flatten(), bounds_error=False, fill_value = 'extrapolate', kind = 'linear')( int_tew)
                    ewarray = eewarray
                    if( debug):
                        for k, el in enumerate( ewarray):
                            print( 'el[{}]\t=\t{}'.format( k, el))
                            print( 'lenght of el[{}]\t=\t{}'.format( k, len( el)))
                    #sys.stdin.readline()
                    #print ( ' len(int_tew): ', len(int_tew), ' len(time_ew): ', len(time_ew), 'si is: ', dt_ew)
                except Exception as ex:
                    print( 'reading ew-data failed with execption {}...'.format( ex))
                    print( '...replacing data with zeros')
                    int_tew =  np.zeros( ( 1, 1))
                    ewmissing = True
                    #db.close() #closing connection to database
                    #return
            else:
                int_tew = np.zeros( ( 1, 1))
            
            
            
            ##################
            # V
            #
            # READ IN SUPERGRAD V-SENSORS GRADIENTS
            ##################
            try:
                v = readDB( self.dB, 'GP20S3V_911005_0001_0001', starttime = startdate, endtime = enddate)
            except Exception as ex:
                #self.MyException(ex)
                print( 'Reading v-data failed with execption\n{}...'.format( ex))
                vmissing = True
                #return
                #v = readDB( db, 'GP20S3V_911005_0001_0001', starttime = startdate, endtime = enddate)
                #v.sorting()
            try:
                v = v.removeduplicates()
                print( 'length of v is:\t{}'.format( v.length()))
            except Exception as ex:
                print( 'removing duplicates of ns-data failed with execption\n{}...'.format( ex))
                #self.MyException(ex)
                vmissing = True
                #return
            
            
            ##################
            # IDENTIFY CORRECT COLUMNS
            ##################
            if( not vmissing):
                self.__GetTimeInd__(v)
                tI = self.TimeInd
                self.__GetDataInd__(v)
                index_vec = self.DataInd
                ColNames = self.DataColNames
                vmissing = self.missing
            if( not vmissing):
                try:
                    #xI = v.KEYLIST.index('dx')
                    #yI = v.KEYLIST.index('dy')
                    #zI = v.KEYLIST.index('dz')
                    #index_vec = [v.KEYLIST.index('x'),v.KEYLIST.index('y'),v.KEYLIST.index('z'),v.KEYLIST.index('dx'),v.KEYLIST.index('dy'),v.KEYLIST.index('dz')]
                    arraydim = nanmax( shape( index_vec))
                    ##################
                    # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
                    ##################
                    
                    time_v = v.ndarray[tI]
                    time_v_zero = NUM2DATE20TIME( num2date( time_v))
                    #time_v_zero = dump[0]
                    #v = v.ndarray[[xI, yI, zI]]
                    v = v.ndarray[index_vec]
                    #varray = np.matrix( list( itertools.chain( *v))).reshape( ( 3, -1)).T
                    varray = matrix( list( chain( *v))).reshape( arraydim, -1).T
                    
                    
                    stamp_starttime = NUM2DATE20TIME( num2date(time_v[0])) # CONVERT TO ZEROTIME VALUES
                    stamp_endtime = NUM2DATE20TIME( num2date(time_v[-1])) # CONVERT TO ZEROTIME VALUES
                    dt_v = (stamp_endtime - stamp_starttime)/float(shape(time_v)[0] -1)
                    #int_tv = np.linspace( stamp_starttime, stamp_endtime, len( time_v))
                    int_tv = np.arange( stamp_starttime, stamp_endtime + dt_v, dt_v)
                    #print( 'type of time_v is:\t{}'.format( type( time_v)))
                    #print( 'type of time_v_zero is:\t{}'.format( type( time_v_zero)))
                    #print( 'type of varray.T is:\t{}'.format( type( varray.T)))
                    #print( 'type of int_tv is:\t{}'.format( type( int_tv)))
                    #varray = interp1d( time_v_zero, varray.T, fill_value = 'extrapolate', kind = 'linear')( int_tv)
                    self.__RmNanInf__(varray)
                    vvarray = np.zeros( ( 6, np.shape( int_tv)[0]))
                    for k, ind in enumerate( self.goodind):
                        vvarray[k, :] = interp1d( time_v_zero[ind], np.array(varray[ind, k]).flatten(), bounds_error=False, fill_value = 'extrapolate', kind = 'linear')( int_tv)
                    varray = vvarray
                    if( debug):
                        for k, el in enumerate( varray):
                            print( 'el[{}]\t=\t{}'.format( k, el))
                            print( 'lenght of el[{}]\t=\t{}'.format( k, len( el)))
                    #print ( ' len(int_tv): ', len(int_tv), ' len(time_v): ', len(time_v), 'si is: ', dt_v)
                except Exception as ex:
                    print( 'reading v-data failed with execption {}...'.format( ex))
                    print( '...replacing data with zeros')
                    int_tv = np.zeros( ( 1, 1))
                    vmissing = True
                    #db.close() #closing connection to database
                    #return
            else:
                int_tv = np.zeros( ( 1, 1))
        except Exception as ex:
            self.dB.close() #fallback closing connection to database
            self.MyException( ex)
            return
        #db.close() #closing connection to database
        self.dB.close() #closing connection to database
        
        
        
        ####################
        # REPLACE MISSING DATA WITH ZEROS
        ####################
        testlist = [np.nanmax( np.shape( int_tns)), np.nanmax( np.shape( int_tew)), np.nanmax( np.shape( int_tv))]
        if( debug):
            print( 'testlist is:\t{}'.format( testlist))
        reflen = int( np.nanmax( testlist))
        refmaxind = np.argmax( testlist)
        if( refmaxind == 0):
            reft = int_tns
        elif( refmaxind == 1):
            reft = int_tew
        elif( refmaxind == 2):
            reft = int_tv
        
        if( nsmissing):
            print( 'ns-entries missing...')
            int_tns = reft
            nsarray = np.zeros( ( 6, reflen))
            print( 'Replacing missing NS columns with zeros...')
        if( ewmissing):
            print( 'ew-entries missing...')
            int_tew = reft
            ewarray = np.zeros( ( 6, reflen))
            print( 'Replacing missing EW columns with zeros...')
        if( vmissing):
            print( 'v-entries missing...')
            int_tv = reft
            varray = np.zeros( ( 6, reflen))
            print( 'Replacing missing V columns with zeros...')
        if( nsmissing and ewmissing and vmissing):
            self.MyException('No data available...stopping!')
            return
        
        
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        
        sstartdate = startdate
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        
        #print startdate
        #print enddate
        
        ######################################
        # DEFINE ACURRATE SENSOR POSITIONS
        ######################################
        """
        sensorpos_N = array( [ -34855.5869, 310273.3534, 1087.85])
        sensorpos_S = array( [ -34856.7915, 310073.3882, 1086.229])
        sensorpos_NS = array( [ -34856.4903, 310123.3873, 1087.16])
        sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.64])
        sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.54])
        sensorpos_W = array( [ -34947.11138, 310137.2636, 1087.24])
        sensorpos_TB = array( [ -34856.47464, 310125.9872, 1087.14])
        sensorpos_TA = array( [ -34856.43362, 310132.7971, 1087.2])
        sensorpos_B = array( [ -34856.43362, 310125.7971, 887.2])
        
        sensPos = vstack( ( sensorpos_N, sensorpos_S, sensorpos_NS, sensorpos_E, sensorpos_EW, sensorpos_W, sensorpos_TB, sensorpos_TA, sensorpos_B))
        
        sensName = array( ['N', 'S', 'NS', 'E', 'EW', 'W', 'TB', 'TA', 'B'])
        sensPos = hstack( ( np.atleast_2d( sensName).T, sensPos))
        """
        sensPos = self.Positions()
        ######################################
        # CUT OUT SAMPLES WHICH ARE AVAILABLE FROM ALL SENSORS
        ######################################
        common_starttime = np.max( [ int_tns[0], int_tew[0], int_tv[0]])
        common_endtime = np.min( [ int_tns[-1], int_tew[-1], int_tv[-1]])
        
        
        ns_t_minind = np.argmin( np.abs( int_tns - common_starttime))
        ns_t_maxind = np.argmin( np.abs( int_tns - common_endtime))
        NS_leng = ns_t_maxind - ns_t_minind
        
        ew_t_minind = np.argmin( np.abs( int_tew - common_starttime))
        ew_t_maxind = np.argmin( np.abs( int_tew - common_endtime))
        EW_leng = ew_t_maxind - ew_t_minind
            
        v_t_minind = np.argmin( np.abs( int_tv - common_starttime))
        v_t_maxind = np.argmin( np.abs( int_tv - common_endtime))
        V_leng = v_t_maxind - v_t_minind
        
        
        common_leng = np.min( [NS_leng, EW_leng, V_leng])
        
        if( common_leng == NS_leng):
            si = float( int_tns[ns_t_maxind] - int_tns[ns_t_minind])/ float(NS_leng - 1)
        elif( common_leng == EW_leng):
            si = float( int_tew[ew_t_maxind] - int_tew[ew_t_minind])/ float(EW_leng - 1)
        elif( common_leng == V_leng):
            si = float( int_tv[v_t_maxind] - int_tv[v_t_minind])/ float(V_leng - 1)
        else:
            print( 'ERROR cutting only sensors data from all available sensors...stopping')
            sys.exit()
        #ietime = np.linspace( common_starttime, common_endtime, common_leng)
        ietime = np.arange( common_starttime, common_endtime + si, si)
        
        # NS
        if( debug):
            print( 'shape of nsarray: {}'.format(np.shape( nsarray)))
            print( 'shape of int_tns: {}'.format(np.shape( int_tns)))
        #temp = interp1d( int_tns, nsarray.T, fill_value = 'extrapolate', kind = 'linear')( ietime)
        temp = interp1d( int_tns, nsarray, fill_value = 'extrapolate', kind = 'linear')( ietime)
        nsmseries = temp
        
        # EW
        if( debug):
            print( 'shape of ewarray: {}'.format(np.shape( ewarray)))
            print( 'shape of int_tew: {}'.format(np.shape( int_tew)))
        #temp = interp1d( int_tew, ewarray.T, fill_value = 'extrapolate', kind = 'linear')( ietime)
        temp = interp1d( int_tew, ewarray, fill_value = 'extrapolate', kind = 'linear')( ietime)
        ewmseries = temp
        
        # V
        if( debug):
            print( 'shape of varray: {}'.format(np.shape( varray)))
            print( 'shape of int_tv: {}'.format(np.shape( int_tv)))
        #temp = interp1d( int_tv, varray.T, fill_value = 'extrapolate', kind = 'linear')( ietime)
        temp = interp1d( int_tv, varray, fill_value = 'extrapolate', kind = 'linear')( ietime)
        vmseries = temp
        
        
        ######################################
        # CONCATENATE ALL THREE DIRECTIONS
        ######################################
        mseries = np.vstack( ( nsmseries, ewmseries, vmseries))* pico
        ######################################
        self.zerotime = ietime
        self.data = mseries
        self.sensPos = sensPos
        gc.collect()
        del gc.garbage[:]
        return self.zerotime, self.data, self.sensPos#, np.vstack(( aseries, bseries))
    
    
    
    def dBVarioRead( self):
        if( debug):
            print( '\n\n\nStarting dBVarioRead...')
        startdate = self.starttime
        enddate = self.endtime
        ##################
        # CONNECT TO DATABASE
        ##################
        #from magpy.database import readDB
        #from inspect import stack, getmodule
        
        #print 'datetime of processing is: {}'.format( datetime.utcnow()), getmodule( stack()[1][0])
        
        #db = mysql.connect( host = "138.22.188.195", user="cobs", passwd = "8ung2rad", db = "cobsdb")
        print ( '\n\n\n\tstarttime\n\n\n')
        print ( '\t{}'.format( startdate))
        print ( '\n\n\n\tendtime\n\n\n')
        print ( '\t{}'.format( enddate))
        self.__dBInit__()
        
        ##################
        # NS
        #
        # READ IN SUPERGRAD NS-SENSORS GRADIENTS
        ##################
        VarioSensorString = self.SearchString#'LEMI036_1_0002_0001'
        try:
            vario = readDB( self.dB, VarioSensorString, starttime = startdate, endtime = enddate)
            #ns.sorting()
            vario = vario.removeduplicates()
            print( 'length of vario is:\t{}'.format( vario.length()))
        except Exception as ex:
            self.dB.close()
            print( 'Variometer database entry problem with exception:\n{} ...stopping'.format( ex))
            sys.exit()
        self.dB.close()
        
        ##################
        # IDENTIFY CORRECT COLUMNS
        ##################
        self.__GetTimeInd__(vario)
        tI = self.TimeInd
        self.__GetDataInd__( vario)
        index_vec = self.DataInd
        ColNames = self.DataColNames
        
        """
        time_vario = vario.ndarray[tI]
        time_vario_zero = NUM2DATE20TIME( num2date( time_vario))
        #time_vario_zero = dump[0]
        #print time_vario_zero
        
        #vario = vario.ndarray[[xI, yI, zI]]
        vario = vario.ndarray[index_vec]
        #varioarray = np.matrix( list( itertools.chain( *vario))).reshape( ( 3, -1)).T
        arraydim = nanmax( shape( index_vec))
        if( debug):
            print( 'numbers of rows in array:\t{}'.format( arraydim))
            print( 'shape of vario is:\t{}'.format( np.shape( vario)))
            print( 'index_vec:\t{}'.format( index_vec))
        varioarray = matrix( list( chain( *vario)))#.reshape( ( arraydim, -1)).T
        if( debug):
            print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
        varioarray = varioarray.reshape( arraydim, -1).T
        if( debug):
            print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
        
        ietime, vario = self.__EquiInterpol__( time = time_vario_zero, data = varioarray)
        """
        #sys.exit()
        try:
            #xI = vario.KEYLIST.index('dx')
            #yI = vario.KEYLIST.index('dy')
            #zI = vario.KEYLIST.index('dz')
            #index_vec = [vario.KEYLIST.index('x'), vario.KEYLIST.index('y'), vario.KEYLIST.index('z')]#, vario.KEYLIST.index('f')]#, vario.KEYLIST.index('dx'), vario.KEYLIST.index('dy'), vario.KEYLIST.index('dz')]
            
            ##################
            # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
            ##################
            
            #from magpy.stream import num2date
            time_vario = vario.ndarray[tI]
            time_vario_zero = NUM2DATE20TIME( num2date( time_vario))
            #time_vario_zero = dump[0]
            #print time_vario_zero
            
            #vario = vario.ndarray[[xI, yI, zI]]
            vario = vario.ndarray[index_vec]
            #varioarray = np.matrix( list( itertools.chain( *vario))).reshape( ( 3, -1)).T
            arraydim = nanmax( shape( index_vec))
            if( debug):
                print( 'numbers of rows in array:\t{}'.format( arraydim))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
                print( 'index_vec:\t{}'.format( index_vec))
            varioarray = matrix( list( chain( vario)))#.reshape( ( arraydim, -1)).T
            if( debug):
                print( 'varioarray is:\t{}'.format( varioarray))
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            varioarray = ( varioarray.reshape( arraydim, -1).T).astype(float)
            if( debug):
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            #sys.exit()
            ietime, vario = self.__EquiInterpol__( time = time_vario_zero, data = varioarray)
            """
            #from magpy.stream import num2date
            stamp_starttime = NUM2DATE20TIME( num2date(time_vario[0])) # CONVERT TO ZEROTIME VALUES
            stamp_endtime = NUM2DATE20TIME( num2date(time_vario[-1])) # CONVERT TO ZEROTIME VALUES
            dt_vario = (stamp_endtime - stamp_starttime)/float(shape(time_vario)[0] - 1)
            #int_tvario = np.linspace( stamp_starttime, stamp_endtime, len( time_vario))
            int_tvario = np.arange( stamp_starttime, stamp_endtime + dt_vario, dt_vario)
            ietime = int_tvario
            """
            if( debug):
                print( 'shape of ietime is:\t{}'.format( np.shape( ietime)))
                print( 'shape of time_vario_zero is:\t{}'.format( np.shape( time_vario_zero)))
                print( 'shape of varioarray.T is:\t{}'.format( np.shape( varioarray.T)))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
            
        except Exception as ex:
            self.MyException(ex)
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        
        #sstartdate = startdate
        #zstartdate = NUM2DATE20TIME(startdate)
        #zenddate = NUM2DATE20TIME(enddate)
        
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        
        #print startdate
        #print enddate
        
        ######################################
        # APPLY SI-UNIT CONVERSION
        ######################################
        mseries = vario* nano
        ######################################
        self.zerotime = ietime
        self.data = mseries
        return self.zerotime, self.data#, np.vstack(( aseries, bseries))
    
    
    
    def MPStreamRead( self):
        if( debug):
            print( '\n\n\nStarting MPStreamRead...')
        #startdate = self.starttime
        #enddate = self.endtime
        ##################
        # CONNECT TO DATABASE
        ##################
        vario = self.data
        
        vario = vario.removeduplicates()
        vario = vario.trim( starttime = self.starttime, endtime = self.endtime)
        
        ##################
        # IDENTIFY CORRECT COLUMNS
        ##################
        self.__GetTimeInd__(vario)
        tI = self.TimeInd
        #xI = vario.KEYLIST.index('dx')
        #yI = vario.KEYLIST.index('dy')
        #zI = vario.KEYLIST.index('dz')
        #index_vec = [vario.KEYLIST.index('x'), vario.KEYLIST.index('y'), vario.KEYLIST.index('z')]#, vario.KEYLIST.index('f')]#, vario.KEYLIST.index('dx'), vario.KEYLIST.index('dy'), vario.KEYLIST.index('dz')]
        self.__GetDataInd__( vario)
        index_vec = self.DataInd
        ColNames = self.DataColNames
        """
        index_vec = []
        for el in vario.KEYLIST:
            index_vec.append( vario.KEYLIST.index( el))
            if( debug):
                print( 'el in KEYLIST is:\t{}'.format( el))
        if( debug):
            print( 'length of index_vec is:\t{}'.format( len( index_vec)))
        index_vec.remove( vario.KEYLIST.index( 'time'))
        if( debug):
            print( 'length of index_vec is:\t{}'.format( len( index_vec)))
        ##################
        # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
        ##################
        
        #from magpy.stream import num2date
        time_vario = vario.ndarray[tI]
        dump = NUM2DATE20TIME( num2date( time_vario))
        #time_vario_zero = dump[0]
        time_vario_zero = dump
        #print time_vario_zero
        
        #vario = vario.ndarray[[xI, yI, zI]]
        goodind_vec = []
        bakvario = vario
        vario = vario.ndarray[index_vec]
        for el, ind in zip( vario, index_vec):
            if( debug):
                print( 'shape of vario-el of index {} is: {}'.format( np.shape( el), ind))
            if( np.shape( el)[0] > 0):
                goodind_vec.append( ind)
        index_vec = goodind_vec
        vario = bakvario.ndarray[index_vec]
        #varioarray = np.matrix( list( itertools.chain( *vario))).reshape( ( 3, -1)).T
        """
        arraydim = np.nanmax( np.shape( index_vec))
        if( debug):
            print( 'numbers of rows in array:\t{}'.format( arraydim))
            print( 'shape of vario is:\t{}'.format( np.shape( vario)))
            print( 'index_vec:\t{}'.format( index_vec))
        varioarray = matrix( list( chain( *vario)))#.reshape( ( arraydim, -1)).T
        if( debug):
            print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
        varioarray = varioarray.reshape( arraydim, -1).T
        if( debug):
            print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
        #from magpy.stream import num2date
        stamp_starttime = NUM2DATE20TIME( num2date(time_vario[0])) # CONVERT TO ZEROTIME VALUES
        stamp_endtime = NUM2DATE20TIME( num2date(time_vario[-1])) # CONVERT TO ZEROTIME VALUES
        dt_vario = (stamp_endtime - stamp_starttime)/float(shape(time_vario)[0] - 1)
        int_tvario = np.linspace( stamp_starttime, stamp_endtime, len( time_vario))
        ietime = int_tvario
        if( debug):
            print( 'shape of ietime is:\t{}'.format( np.shape( ietime)))
            print( 'shape of time_vario_zero is:\t{}'.format( np.shape( time_vario_zero)))
            print( 'shape of varioarray.T is:\t{}'.format( np.shape( varioarray.T)))
        vario = interp1d( time_vario_zero, varioarray.T, fill_value = 'extrapolate', kind = 'linear')( ietime)
        if( debug):
            print ( ' len(int_tvario): ', len(int_tvario), ' len(time_vario): ', len(time_vario), 'si is: ', dt_vario)
            print( 'shape of vario is:\t{}'.format( np.shape( vario)))
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        
        #sstartdate = startdate
        #zstartdate = NUM2DATE20TIME(startdate)
        #zenddate = NUM2DATE20TIME(enddate)
        
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        
        #print startdate
        #print enddate
        
        ######################################
        # APPLY SI-UNIT CONVERSION
        ######################################
        mseries = vario#* nano
        ######################################
        self.zerotime = ietime
        self.data = mseries
        return self.zerotime, self.data#, np.vstack(( aseries, bseries))



    def WICDefDataRead( self):
        if( debug):
            print( '\n\n\nStarting WICDefDataRead...')
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        #sstartdate = startdate
        startdate = self.starttime
        enddate = self.endtime
        sstartdate = startdate
        
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        self.__getdays__()
        days = self.days
        startdate = self.starttime
        #sys.exit()
        #channelse = ['pri0','pri1']
        #print(startdate)
        print ( '\n\n\n\tstarttime\t\t=\tzstarttime\n\n\n')
        print ( '\t{}\t=\t{}'.format( startdate, zstartdate))
        print ( '\n\n\n\tendtime\t\t\t=\tzendtime\n\n\n')
        print ( '\t{}\t=\t{}'.format( enddate, zenddate))
        #sys.exit()
        ###################################
        # TIMERANGE DEFINITION
        ###################################
        cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        
        cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        ###################################
        # READ IN MAGNETIC RECORDS
        ###################################
        #dirstring = [str('./gradfiles/EW'), str('./gradfiles/NS'), str('./gradfiles/VS')]
        #######################
        #ewpathrelpath = str('sources/gradfiles/EW')
        #nspathrelpath = str('sources/gradfiles/NS')
        #vspathrelpath = str('sources/gradfiles/VS')
        #dirstring = [os.path.join( commonpath, ewpathrelpath), os.path.join( commonpath, nspathrelpath), os.path.join( commonpath, vspathrelpath)]
        #######################
        #import fnmatch
        #from magpy.stream import read, datetime, DataStream
        #from scipy.interpolate import interp1d
        #from itertools import chain
        colmp = DataStream()
        
        fileending = '.' + self.filetype
        ########################
        # READ EW, NS, V
        #########################
        
        
        
        
        
        
        #########################
        # READ SINGLE SENSOR AXIS
        #########################
        
        
        #if( len( self.SearchString) < len( 'GP20S3EW_111201_0001_')):
        sensname = 'wic' # starting piece of searchstring for filename to look for
        #sensname = sensname + self.SearchString
        #sensname = 'GP20S3EW_111201_0001_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelsg = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y%m%d')  + 'd' + fileending[1:] + fileending for f in days]]
        #channelsg = np.unique( channelsg)
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        
        channelsg = sum(channelsg,[])
        if( debug):
            print( 'Looking for files like this:')
            for el in channelsg:
                print( el)
            sys.exit()
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #print ( 'all selected data: {}'.format( a))
        
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelsg:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        self.__LookForFilesMatching__()
        if( debug):
            print( 'Found files like this:')
            for el in self.files:
                print( el)
            sys.exit()
        allfiles = []
        n = 0
        print( 'looking for sgel: {}'.format( channelsg))
        for sgel in channelsg:
            if( debug):
                print( 'looking for sgel: {}'.format( sgel))
            for el in self.files:
                if( fnmatch( el, '*' + sgel)):
                    n = n + 1
                    allfiles.append( el)
                    print( '{} #{} file is: {}'.format( self.SearchString, n, el))
                    #sys.exit()
        #sys.exit()
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
            sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'EW #{} file is: {}'.format( k+1, f))
        """
        temp = DataStream()
        #print(allfiles)
        #sys.exit()
        
        
        
        self.__GetColHeaderInfo__( read( allfiles[0]))
        if( debug):
            print( 'self.HeaderInfo is:\n')
            for el in self.HeaderInfo:
                print( 'el is:\t{}'.format( el))
            sys.exit()
        fndheaderinfo = read( allfiles[0]).header # backup of temp.header for later use
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            if( debug):
                print( '\ntemp.header.keys')
                for el in temp.header.keys():
                    print( '\tel in temp.header.keys', el)
                print( '\ntemp.header.values')
                for el in temp.header.values():
                    print( '\tel in temp.header.values', el)
                print('\ntemp.header')
                for el in temp.header:
                    print( '\tel in temp.header', el)
                sys.exit()
            #print( self.SearchString[0:self.SearchString.rfind( '_')])
            try:
                self.stationinfo = [temp.header['SensorID'], temp.header['DataComments'], temp.header['StationIAGAcode']]
            except:
                self.stationinfo = [self.SearchString[0:self.SearchString.index( '_')], self.SearchString[self.SearchString.index( '_'):self.SearchString.rfind( '_')],'']
            identstr = str( self.stationinfo[0]) + str( self.stationinfo[1]) + str( self.stationinfo[2])
            print( '...{} #{} file {} read.'.format( identstr, k, f))
            #print(k, temp.ndarray[2])
            #colmp = mp.appendStreams([colmp, temp]) # CONCATENATE MAGPY STREAMS
            #print( 'actual colmp length is: {}'.format( colmp.length))
            #print( 'temp length to be added is: {}'.format( temp.length))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            #colmp.extend(temp , temp.header, temp.ndarray)
            print( '...{} #{} file {} appended.'.format( identstr, k, f))
        colmp.header = fndheaderinfo
        if( debug):
            print( '{} matching read...removing duplicates'.format( identstr))
            #sys.exit()
        if( self.ApplyFlags):
            colmp = self.__ApplyFlags__(colmp)
        if( self.InterMagFilter):
            self.__InterMagnet_filter__(colmp)
        
        #print( sstartdate, enddate)
        #sys.exit()
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        #print( colmp.ndarray[2])
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        self.__GetTimeInd__(colmp)
        tI = self.TimeInd
        #index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        self.__GetDataInd__( colmp, ColList = self.ColList)
        index_vec = self.DataInd
        ColNames = self.DataColNames
        
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        #sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( '{} dataarray reshaped...converting timestamps'.format( self.SearchString))
            sys.exit()
        
        #stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        #stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
        #dt = (stamp_endtime - stamp_starttime)/float( nanmax( shape(t)) -1)
        dt = (t[-1] - t[0])/float( nanmax( shape(t)) - 1)
        print( 'found average internal POSIX sampling interval is {} days'.format( dt))
        #sys.exit()
        iedt = (cut_end - cut_start)/float( nanmax( shape(t)) - 1)
        
        #print( 'cut_start, cut_end')
        #print( cut_start, cut_end)
        #print( 'iedt')
        #print( iedt)
        
        ietime = np.arange( cut_start, cut_end + iedt, iedt)
        if( debug):
            varlist = list( locals().items())
            
            for el in [ietime, iedt, t, dt]:
                varname = [v for v, k in varlist if k is el][0]
                if( isinstance( el, np.ndarray)):
                    print( 'len of {} is {}'.format( varname , len( el)))
                elif( type(el) == float):
                    print( 'value of {} is {}'.format( varname, el))
                else:
                    print( 'value of {} is {}, type is: {}'.format( varname, el, type( el)))
        #helpt = np.linspace(t[0], t[-1],len(t))
        
        helpt = np.arange(t[0], t[-1] + dt, dt)
        #print( 'shape of helpt is {}'.format( np.shape( helpt)))
        #sys.exit()
        if( nanmax( shape( helpt)) > nanmax( shape( mseries))):
            helpt = helpt[0:-1:]
        if( debug):
            print( 'helpt is:')
            for el, dl in zip( helpt, num2date( helpt)):
                print( el, dl)
            sys.exit()
        print( 'Deriving ztime...')
        if( debug):
            helpt = np.sort( np.nanmean( helpt) + (np.random.sample( len( helpt)) - 0.5)* 1.0* np.nanstd( helpt))
        ztime = NUM2DATE20TIME( num2date( helpt)) # GETTING zerotime timestamps
        print( 'ztime derived.')
        ztimedt = ( ztime[-1] - ztime[0])/ float( len( ztime) - 1)
        print( 'found ztime dt: {} seconds'.format( ztimedt))
        if( debug):
            for el in (ztime[1::] - ztime[:-1:]):
                print( 'ppm difference to nominal sampling interval {}'.format( np.abs( el - ztimedt)/ ztimedt* 1000000.0))
            sys.exit()
        #sys.exit()
        ##########################
        #BEGIN DEBUG OF TIMESTAMPS !!!! KEEP FOR DEBUGGING !!!!
        ##########################
        if( False):
            print( 'Deriving checkstamps...')
            checkdates = STAMPTODATE( ztime)
            print( 'checkstamps derived.')
            for el in checkdates:
                year = el.year
                print( el)
                #if( year > 2020):
                #    sys.exit()
            #´sys.exit()
            print( 'Deriving recheckztime...')
            recheckztime = NUM2DATE20TIME( checkdates)
            print( 'recheckztime derived.')
            #sys.exit()
        if( False):
            if( True):
                print( 'helpt is:')
                #from datetime import timezone
                for k, ( el, dl, cl, bl, al) in enumerate( zip( helpt, num2date( helpt), ztime, checkdates, recheckztime)):
                    print( k, el, dl, cl, bl, al, ( dl - bl).total_seconds())
                    #sys.exit()
                    #if( np.abs( al - cl) > 1.0):
                    if( bl.year == 2021):
                        print( 'PROBLEM WITH ztime or with STAMPTODATE...STOPPING')
                        #!!! WO IS DAS PROBLE MIT STAMPTODATE !!! ztime passt vermeintlich !!!
                        sys.exit()
                    #sleep( 1)
                sys.exit()
            else:
                print( 'helpt is:')
                #from datetime import timezone
                for k, ( el, dl, cl) in enumerate( zip( helpt, num2date( helpt), ztime)):
                    print( k, el, dl, cl)#, al - cl)
            sys.exit()
        
        
        
        
        
        
        #END DEBUG
        ##########################
        if( debug):
            for f in [ ztime, mseries, ietime]:
                varname = [k for k, v in locals().items() if v is f][1]
                if( debug):
                    print( 'shape of {} is : {}'.format( varname, np.shape(f)))
        
        ######################################
        if( debug):
            print( 'Interpolate equidistantly - {} data array'.format( self.SearchString))
            #sys.exit()
        temp = interp1d( ztime, mseries, fill_value='extrapolate', kind='linear')(ietime)
        mseries = temp
        sgmseries = mseries
        sgtime = ietime
        
        print( 'Reading {}-Data...done\n'.format(self.SearchString))
        
        
        
        """
        ######################################
        # DEFINE ACURRATE SENSOR POSITIONS
        ######################################
        sensorpos_N = array( [ -34855.5869, 310273.3534, 1087.85])
        #sensorpos_S = array( [ -34856.7915, 310073.3882, 1086.229]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_S = array( [ -34856.7915, 310073.3882, 1087.229])
        sensorpos_NS = array( [ -34856.4903, 310123.3873, 1087.16])
        #sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.64]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_E = array( [ -34747.14438, 310136.0651, 1087.14])
        #sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.54]) # vermutlich falsch, da sensor mittlerweile auch auf einem gleichhohen glassockel steht
        sensorpos_EW = array( [ -34807.1433, 310136.4247, 1087.14])
        sensorpos_W = array( [ -34947.11138, 310137.2636, 1087.24])
        sensorpos_TB = array( [ -34856.47464, 310125.9872, 1087.17])
        sensorpos_TA = array( [ -34856.43362, 310132.7971, 1087.2])
        sensorpos_B = array( [ -34856.43362, 310125.7971, 887.2])
        
        sensPos = vstack( ( sensorpos_N, sensorpos_S, sensorpos_NS, sensorpos_E, sensorpos_EW, sensorpos_W, sensorpos_TB, sensorpos_TA, sensorpos_B))
        
        sensName = array( ['N', 'S', 'NS', 'E', 'EW', 'W', 'TB', 'TA', 'B'])
        sensPos = hstack( ( np.atleast_2d( sensName).T, sensPos))
        """
        sensPos = self.Positions()
        
        ######################################
        # CUT OUT SAMPLES WHICH ARE AVAILABLE FROM ALL SENSORS
        ######################################
        common_starttime = np.max([ sgtime[0]])
        common_endtime = np.min( [sgtime[-1]])
        
        
        sg_t_minind = np.argmin( np.abs( sgtime - common_starttime))
        sg_t_maxind = np.argmin( np.abs( sgtime - common_endtime))
        SG_leng = sg_t_maxind - sg_t_minind
        
        
        common_leng = np.min( [SG_leng])
        if( debug):
            print( 'common_leng is: {}'.format( common_leng))
        
        if( common_leng == SG_leng):
            si = float( sgtime[sg_t_maxind] - sgtime[sg_t_minind])/ float(SG_leng - 1)
        else:
            print( 'ERROR cutting only sensors data from all available sensors')
            return
        newdt = (common_endtime - common_starttime)/(float(common_leng) - 1.0)
        print( '\n\n\nResampling with new sampling intervall: {} seconds'.format( newdt))
        #ietime = np.linspace(common_starttime, common_endtime, common_leng) # changed due to errors in linspace routine
        ietime = np.arange(common_starttime, common_endtime + newdt, newdt)
        
        
        # SG
        temp = interp1d( sgtime, sgmseries,fill_value='extrapolate',kind='linear')(ietime)
        sgmseries = temp
        
        print( '\n\n\n...done, stacking arrays together.')
        ######################################
        # CONCATENATE ALL THREE DIRECTIONS
        ######################################
        mseries = sgmseries#* pico
        print( '\n\n\n...done.')
        ######################################
        print( 'Reading absolutes files between {} and {} done'.format( startdate, enddate))
        self.zerotime = ietime
        self.data = mseries
        self.sensPos = sensPos
        gc.collect()
        del gc.garbage[:]
        #self.sensPos
        return self.zerotime, self.data#, self.sensPos#, np.vstack(( aseries, bseries))
    
    
    
    """
    READING IAGA-2002 ascii files
    --- under construction ---
    """
    def MPascii( self):
        if( debug):
            print( '\n\n\nStarting MPascii...')
        #startdate = datetime.strptime("2018-02-07", "%Y-%m-%d")
        #sstartdate = startdate
        startdate = self.starttime
        enddate = self.endtime
        sstartdate = startdate
        zstartdate = NUM2DATE20TIME( startdate)
        zenddate = NUM2DATE20TIME( enddate)
        ##enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
        #enddate = datetime.strptime("2018-02-08", "%Y-%m-%d")
        
        #sys.exit()
        self.__getdays__()
        days = self.days
        print( 'days', days)
        #sys.exit()
        startdate = self.starttime
        #channelse = ['pri0','pri1']
        #print(startdate)
        print ( '\n\n\n\tstarttime\t\t=\tzstarttime\n\n\n')
        print ( '\t{}\t=\t{}'.format( startdate, zstartdate))
        print ( '\n\n\n\tendtime\t\t\t=\tzendtime\n\n\n')
        print ( '\t{}\t=\t{}'.format( enddate, zenddate))
        #sys.exit()
        ###################################
        # TIMERANGE DEFINITION
        ###################################
        cut_start = zstartdate # ZEROTIME FOR TIMERANGE TO ANALYSE
        #cutoffset = 3600.0*24.0 # DURATION OF TIMERANGE TO ANALYSE
        
        #cut_end = zstartdate + cutoffset # ENDTIME FOR TIMERANGE TO ANALYSE
        
        colmp = []
        cut_end = zenddate # ENDTIME FOR TIMERANGE TO ANALYSE
        
        #print( 'self.filetype', self.filetype)
        #sys.exit()
        fileending = self.filetype
        print( 'fileending', fileending)
        #########################
        # READ picologtxt
        #########################
        
        if( debug):
            print( 'self.SearchString', self.SearchString)
        sensname = '2016'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            sensname = self.SearchString
            if( debug):
                print( 'sensname is: {}'.format( sensname))
                
        elif( ( len( self.SearchString) >= len( sensname)) & ( fileending.endswith( '.sec'))):
            """
            CASE OF magpy ascii files
            """
            sensname = self.SearchString
        else:
            sensname = self.SearchString
            if( debug):
                print( 'sensname is: {}'.format( sensname))
                #sys.exit()
            #if( not self.SearchString.startswith( 'GSM90_1013973') and not self.SearchString.startswith( 'GSM19_6067473')):
            if( not self.SearchString.startswith( 'GSM90') and not self.SearchString.startswith( 'GSM19') and not self.SearchString.startswith( 'LEMI') and not self.SearchString.endswith( '.sec')):
                sensname = sensname[0:17] # reducing again to sensorname without date info
            if( debug):
                print( 'sensname is: {}'.format( sensname))
            #sys.exit()
        if( debug):
            print( 'self.SearchString', self.SearchString)
        #sys.exit()
        #for d in days:
        #    print( 'd', d)
        #    daystring = datetime.strftime( d, '%Y%m%d')
        #    print( 'daystring', daystring)
        #sys.exit()
        channelvario = [[sensname + datetime.strftime( f, '%Y-%m-%d') + fileending for f in days]] # appending days datainfo to searchlist
        
        #sys.exit()
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        if( debug):
            print( 'channelvario is: {}'.format( channelvario))
        #print( 'channelvario is: {}'.format( channelvario))
        #sys.exit()
        
        channelvario = np.unique( sum(channelvario,[]))
        if( debug):
            print( 'channelvario is: {}'.format( channelvario))
            #sys.exit()
        if( debug):
            print( 'Looking for files like this:')
            for el in channelvario:
                print( el)
            #sys.exit()
        """
        a = [sorted(listdir(f+'/')) for f in dirstring]
        
        #print ( 'all selected data: {}'.format( a))
        
        
        #tage = original_sum(a,[])
        
        tage = a
        alltage = []
        for k in range( 0, len(tage)):
            for channel in channelew:
                alltage += ( sorted(fnmatch.filter(tage[k], channel)))
                #print ( 'verzeichnis: {}, file: {}'.format(channel, tage[k]))
        #print ('allfiles magnetic list: {}'.format( alltage))
        
        tij = dirstring 
        """
        #for el in channelvario:
        #    print(el)
        #sys.exit()
        
        
        self.__LookForFilesMatching__()
        
        
        if( debug):
            print( 'Found files like this:')
            for el in self.files:
                print( el)
                #sys.exit()
            #sys.exit()
        allfiles = []
        n = 0
        for vel in channelvario:
            for el in self.files:
                if( debug):
                    print( 'el\t= {},\tvel = {}'.format( el, vel))
                #if( fnmatch( el, '*' + vel)):
                #print( 'el endswith vel : {}'.format( el.endswith( vel)))
                if( el.endswith( vel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'Vario #{} file is: {}'.format( n, el))
                    #sys.exit()
        #sys.exit()
        if( debug):
            print( 'allfiles is: {}'.format( allfiles))
        #sys.exit()
        if( len( allfiles) < 1):
            print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
            sys.exit()
        #print( 'No files for {} found for time: {}-{}...stopping'.format( sensname, self.starttime, self.endtime))
        #sys.exit()
        
        if( debug):
            print( 'Found matching files:')
            for el in allfiles:
                print( el)
        #sys.exit()
        """
        allfiles = []
        for day in alltage:
            for folder in tij:
                #print ( folder + '/' + day)
                if( os.path.isfile( folder + '/' + day)):
                    allfiles.append( folder + '/' + day)
        allfiles = sorted( allfiles)
        for k, f in enumerate( allfiles):
            print( 'EW #{} file is: {}'.format( k+1, f))
        """
        #temp = DataStream()
        #import codecs# to reencode read files
        """
        Checking how many data columns are needed
        """
        
        for k, f in enumerate( allfiles):
            columncount = 0
            #with codecs.open(f,'r','string-escape') as file:
            #temp=f.read()
            print( 'reading \n{}...'.format( f))
            temp = open(f)#, encoding="ascii", errors="surrogateescape")  # READING picologtxt ascii files
            print( '...Magpy ASCII txt #{} file \n{} read.'.format( k, f))
            
            #print( temp.header['DataCompensationX'])
            data = temp.read()
            bakdata = data
            data = data.split("\n")
            if( debug):
                print( 'data')
                print( data)
                print( 'data[0]')
                print( data[0])
                print( 'shape of data', np.shape(data))
                #sys.exit()
            #self.DataSourceHeader = []
            if( data[0].endswith(' # MagPy - ASCII')):
                linecount = 1
                
                AvailableColumns = []
                print( ' # MagPy - ASCII line found...')
                #import time as tim
                for line in data[1::]:
                    #print( ' line: "{}"'.format( line))
                    #tim.sleep(1)
                    if( ( line.startswith( '" #') or line.startswith( ' #') or line.startswith( '# head') or line.startswith( '-[') or line.startswith( ' # Station')) and not line.startswith( '# data')):
                        if( line.startswith( ' # DataCompensationZ')):
                            self.DataSourceHeader.append( 'DataCompensationZ' + ' = ' + str( line[ line.rfind( ' # DataCompensationZ:') + len( ' # DataCompensationZ') + 1:]))
                            print( 'self.DataSourceHeader[-1]',self.DataSourceHeader[-1])
                        elif( line.startswith( ' # DataCompensationY')):
                            self.DataSourceHeader.append( 'DataCompensationY' + ' = ' + str( line[ line.rfind( ' # DataCompensationY:') + len( ' # DataCompensationY') + 1:]))
                            print( 'self.DataSourceHeader[-1]',self.DataSourceHeader[-1])
                        elif( line.startswith( ' # DataCompensationX')):
                            self.DataSourceHeader.append( 'DataCompensationX' + ' = ' + str( line[ line.rfind( ' # DataCompensationX:') + len( ' # DataCompensationX') + 1:]))
                            print( 'self.DataSourceHeader[-1]',self.DataSourceHeader[-1])
                        elif( line.startswith( '# head')):
                            linecount = linecount + 1
                            #print( 'linecount', linecount)
                        elif( line.startswith( '-[],x[nT],y[nT],z[nT]')):
                            items = line.split( ',')
                            for o, item in enumerate( items):
                                if( item != '-[]'):
                                    columncount = columncount + 1
                                    self.DataSourceHeader.append( 'col{}'.format( o) + ' = ' + item)
                                    AvailableColumns.append( [ o, item])
                                    print( 'self.DataSourceHeader[-1]',self.DataSourceHeader[-1])
                            #linecount = linecount + 1
                        #elif( line.startswith( '" #') or line.startswith( ' #')):
                        linecount = linecount + 1
                        print( ' linecount: {}, line: "{}"'.format( linecount, line))
                    elif( line.startswith( '# data')):
                        linecount = linecount + 1
                    else:
                        pass
                print( 'linecount', linecount)
                #sys.exit()
        AvailableColumns = np.array( AvailableColumns)
        if( debug):
            print( 'AvailableColumns\n', AvailableColumns)
            #sys.exit()
        #print( 'columncount', columncount)
        #sys.exit()
        """
        Testing if all columns can be read. If not, non-readable columns are dropped
        """
        for k, f in enumerate( allfiles):
            #with codecs.open(f,'r','string-escape') as file:
            #temp=f.read()
            #print( 'reading \n{}...'.format( f))
            temp = open(f)#, encoding="ascii", errors="surrogateescape")  # READING picologtxt ascii files
            #print( '...Magpy ASCII txt #{} file \n{} read.'.format( k, f))
            #print( temp.header['DataCompensationX'])
            data = temp.read()
            data = data.split( '\n')
            #self.DataSourceHeader = []
            if( data[0].endswith(' # MagPy - ASCII')):
                #linecount = 1
                #for line in data[linecount::]:
                mydate = []
                x = []
                #print( 'shape of x is: {}'.format( np.shape( x)))
                #sys.exit()
                usableind = np.array( [ int( f) for f in AvailableColumns[:,0]])#np.argwhere( [ isinstance( float( f), np.floating) for f in elements[AvailableColumns]] ).flatten()
                if( debug):
                    print( 'usableind', usableind)
                    #sys.exit()
                for line in data[linecount:linecount+1]:
                    elements = line.split( ',')
                    if( debug):
                        print( 'elements')
                        for o, el in enumerate( elements):
                            print( 'element [{}] is: {}'.format( o, el))
                        #sys.exit()
                    if( len( elements) > 1):
                        for o, ind in enumerate( usableind):
                            if( debug):
                                print( 'elements[ind] is: {}'.format( elements[ind]))
                            try:
                                float( elements[ind])
                                is_float = True
                            except:
                                is_float = False
                            if( debug):
                                print( 'elements[ind] is floating: {}'.format( is_float))
                            if( not is_float):
                                print( '\nremoving index: {}'.format( ind))
                                print( 'AvailableColumns before removal', AvailableColumns)
                                AvailableColumns = np.vstack( ( AvailableColumns[:o,:], AvailableColumns[o+1:,:]))
                                print( 'AvailableColumns after removal', AvailableColumns)
        #sys.exit()                                
                                
        """
        Reading data
        """
        allmydate = []
        #allx = [[] for i in range( columncount - 3)]
        allx = []
        
        for k, f in enumerate( allfiles):
            #with codecs.open(f,'r','string-escape') as file:
            #temp=f.read()
            print( 'reading \n{}...'.format( f))
            temp = open(f)#, encoding="ascii", errors="surrogateescape")  # READING picologtxt ascii files
            print( '...Magpy ASCII txt #{} file \n{} read.'.format( k, f))
            #print( temp.header['DataCompensationX'])
            data = temp.read()
            data = data.split( '\n')
            #self.DataSourceHeader = []
            if( data[0].endswith(' # MagPy - ASCII')):
                #linecount = 1
                #for line in data[linecount::]:
                mydate = []
                x = []
                #print( 'shape of x is: {}'.format( np.shape( x)))
                #sys.exit()
                usableind = np.array( [ int( f) for f in AvailableColumns[:,0]])#np.argwhere( [ isinstance( float( f), np.floating) for f in elements[AvailableColumns]] ).flatten()
                if( debug):
                    print( 'usableind', usableind)
                    #sys.exit()
                for line in data[linecount::]:
                    elements = line.split( ',')
                    if( debug):
                        print( 'elements')
                        for o, el in enumerate( elements):
                            print( 'element [{}] is: {}'.format( o, el))
                        #sys.exit()
                    if( len( elements) > 1):
                        stamp = elements[0].split( 'T')
                        if( debug):
                            print( 'stamp', stamp)
                        dates = stamp[0]
                        date = dates.split( '-')
                        if( debug):
                            print( 'dates')
                            for o, el in enumerate( date):
                                print( 'date [{}] is: {}'.format( o, el))
                            #sys.exit()
                        times = stamp[1]
                        time = times.split( ':')
                        if( debug):
                            print( 'times')
                            for o, el in enumerate( time):
                                print( 'time [{}] is: {}'.format( o, el))
                            #sys.exit()
                        #time = time.split( '.')[0]
                        #microsec = time[-1].split( '.')[1]
                        #doy = elements[2]
                        #for i in range( columncount - 3):
                        #    #print( i)
                        #    #print( 'elements[i + 3]', elements[usableind[i + 3]])
                        #    x[i].append( float( elements[usableind[i + 3]]))
                        #    #y.append( float( elements[4]))
                        #    #z.append( float( elements[5]))
                        #    #f.append( float( elements[6]))
                        x.append( [ float( elements[f]) for f in usableind])
                        #print( 'x', x)
                        #sys.exit()
                        datestring = dates + ' ' + times# + ' ' + str( int( float(microsec)* 1000.0))# + ' ' + doy
                        #print( 'date + ' ' + time + ' ' + str( int( float(microsec)* 1000.0)): {}'.format( datestring))
                        mydate.append( datetime.strptime(datestring, "%Y-%m-%d %H:%M:%S.%f").replace( tzinfo = timezone.utc))
                        #print( '[-1] is: {}'.format( mydate[-1]))
                        #sys.exit()
                    #print( 'NUM2DATE20TIME( mydate[-1]) is: {}'.format( NUM2DATE20TIME( mydate[-1])))
                    #sys.exit()
                    #for el in line.split( ''):
                    #    print( 'el in line is: {}'.format( el))
                #print( 'self.DataSourceHeader: {}'.format( self.DataSourceHeader))
                #print( 'ztime' , ztime)
                #print( 'shape of ztime' , np.shape( ztime))
                #print( 'x' , x)
                #print( 'shape of dataarray' , np.shape( dataarray))
                #sys.exit()
            #[ allmydate.append( f) for f in mydate]
            allmydate.extend( mydate)
            if( debug):
                print( 'allmydate', allmydate)
                sys.exit()
            #print( 'allmydate', allmydate)
            #sys.exit()
            #[ allx.append( f) for f in x]
            #for l, triple in enumerate( np.array( x)):
            #    #print( 'triple[{}]={}'.format( l, triple))
            #    #print( 'np.shape( triple)', np.shape( np.atleast_2d( triple)))
            #    #sys.exit()
            #    allx.append( triple)
            allx.extend( x)
            if( debug):
                print( 'allx', allx)
                sys.exit()
            #[ allx.append( f) for f in g for g in x]
            if( debug):
                print( 'np.shape( allx)', np.shape( allx))
                print( 'np.shape( allx)', np.shape( allmydate))
                sys.exit()
        
        SelectedIndex = []
        #print( 'SelectIndex')
        for ind, name in zip( AvailableColumns[:,0], AvailableColumns[:,1]):
            if( debug):
                print( 'Ind: {} is: {}'.format( ind, name))
            if( name[0:name.rfind( '[')] in self.ColList):
                if( debug):
                    print( 'found {} index {}'.format( name, ind))
                    #sys.exit()
                SelectedIndex.append( int( ind) - 1)
        SelectedIndex = np.array( SelectedIndex)
        print( 'SelectedIndex', SelectedIndex)
        #sys.exit()
        #if( False):
        #    print( 'allmydate')
        #    if( type( allmydate[0]) == datetime):
        #        print( 'datetime.datetime found')
        #        sys.exit()
        #    for el in allmydate:
        #        print( 'el in allmydate {}'.format( el))
        #        print( 'type( el)', type( el))
        #        if( type( el) == datetime):
        #            print( 'datetime.datetime found')
        #            sys.exit()
        #sys.exit()
        ztime = np.array( NUM2DATE20TIME( allmydate))
        #print( 'ztime')
        #for el in ztime:
        #    print( 'el in ztime', el)
        #sys.exit()
        sortind = np.argsort( ztime)
        ztime = ztime[sortind]
        if( debug):
            print( 'allx')
            for el in np.array( allx)[0:10:,:]:
                print( 'el in allx: {}'.format( el))
                #sys.exit()
        dataarray = np.array( allx)[:, SelectedIndex][sortind, :]
        
        #print( 'ztime')
        #for el in ztime:
        #    print( 'el in ztime {}'.format( el))
        #sys.exit()
        #
        #print( data)
        #print( np.shape( ztime))
        #print( np.shape( dataarray))
        #sys.exit()
        #plt.plot( np.diff( ztime), alpha = 0.2)
        #plt.show()
        #sys.exit()
        
        
        if( debug):
            print( 'Magpy ASCII matching read...removing duplicates')
            #sys.exit()
        #print( shape( colmp))
        #print( type( colmp))
        #sys.exit()
        
        colmp = np.hstack( ( np.atleast_2d( ztime).T, dataarray)) # CONCATENATE MAGPY STREAMS
        #print(' shape( colmp)', shape( colmp))
        res = date2num( STAMPTODATE( ztime))#.astype( float)
        #print( 'res')
        #for el in res:
        #    print( 'el in res', el)
        #sys.exit()
        colmp[:,0] = res
        bakcolmp = colmp
        #plt.plot( bakcolmp[:-1,0], np.diff( bakcolmp[:,1:], axis = 0), alpha = 0.2)
        #plt.show()
        #sys.exit()
        print( bakcolmp[:,0])
        #print( 'shape of bakcolmp', np.shape( bakcolmp))
        #sys.exit()
        #mydict = dict( zip( ['time', 'dx', 'dy', 'dz'], bakcolmp))
        dictlist = ['time']#, 'x', 'y', 'z', 'f']
        for el in self.ColList:
            dictlist.append( el)
        #print( 'dictlist is: {}'.format( dictlist))
        #sys.exit()
        colmp = DataStream()
        array = np.asarray( [ np.zeros( ( np.max( np.shape( bakcolmp)))) for el in colmp.KEYLIST], dtype = object)
        #print( np.shape( array))
        #sys.exit()
        #array[]
        #plt.plot( ztime, dataarray, alpha = 0.2)
        #plt.show()
        #sys.exit()
        colmp.ndarray = array
        for dictel, d in zip( dictlist, bakcolmp.T):
            colmp.ndarray[colmp.KEYLIST.index(dictel)] = np.asarray( d, dtype = object)
        if( data[0].endswith(' # MagPy - ASCII')):
            colmp.header['SensorID'] = str( 'UNKNOWN-MagPy-ASCII')#'GFZ'
        else:
            colmp.header['SensorID'] = str( 'UNKNOWN')
        #colmp = colmp._put_column(colmp[:,0], 'time', columnname='Rain',columnunit='mm in 1h')
        #print( 'colmp', colmp.ndarray[[1,2,3]])
        #dirvar = colmp.header.items()
        #print( dir( dirvar))
        #for el in dirvar:
        #    print( el)
        #print('\n\n\n\n')
        #for el in dir( colmp.header):
        #    print( el)
        #sys.exit()
        if( False):
            for dictel in dictlist:
                if( dictel != 'time'):
                    plt.plot( colmp.ndarray[colmp.KEYLIST.index('time')], colmp.ndarray[colmp.KEYLIST.index(dictel)], alpha = 0.2)
            plt.show()
            sys.exit()
        if( True):
            if( self.ApplyFlags):
                #print( '...applying flags')
                colmp = self.__ApplyFlags__(colmp)
            if( self.InterMagFilter):
                #print( '...applying intermagnet specified filter')
                self.__InterMagnet_filter__(colmp)
            
            if( np.any( colmp.ndarray[16] == b'P')): # CHECKING IF GPS-status is bad
                print( 'BAD GPS-STATUS!...SWITCHING TO SECONDARY TIME BEFORE TRIMMING!')
                colmp = colmp.use_sectime( swap = True) # SWITCH TIMECOLUMNS SO THAT NTP TIMECOLUMN IS IN COLUMN 0 AND TRIM CAN BE APPLIED PROPERLY
            #print( 'sstartdate', sstartdate)
            #print( 'enddate', enddate)
            #print( 'STAMPTODATE( ztime[0])', STAMPTODATE( ztime[0]))
            #print( 'STAMPTODATE( ztime[-1])', STAMPTODATE( ztime[-1]))
            if( debug):
                print( 'colmp.ndarray[0] before triming', colmp.ndarray[0])
            temp = colmp.trim( starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
            colmp = temp
            if( debug):
                print( 'colmp.ndarray[0] after triming', colmp.ndarray[0])
            del temp
            #print( sstartdate)
            #print( enddate)
            #print( colmp.ndarray[0])
            #sys.exit()
            colmp = colmp.removeduplicates()
            if( debug):
                print( 'colmp.ndarray[0] after removedduplicates', colmp.ndarray[0])
            colmp = colmp.sorting()
            if( debug):
                print( 'colmp.ndarray[0] after sorting', colmp.ndarray[0])
            try:
                self.stationinfo = str( colmp.header['SensorID'])
            except:
                self.stationinfo = str( 'UNKNOWN')
            
            
            
            self.__GetSensName__( colmp)
            print( 'self.sensname is:\n')
            for el in self.sensname:
                print( 'el is:\t{}'.format( el))
            if( debug):
                print( 'self.sensname is:\n')
                for el in self.sensname:
                    print( 'el is:\t{}'.format( el))
                #sys.exit()
            
            
            self.__GetTimeInd__(colmp)
            tI = self.TimeInd
        self.TimeInd = 0
        tI = self.TimeInd
        #self.stationinfo = str( 'WIK-PICOLOG')
        #self.sensname = str( 'PICOLOG')
        if( False):
            self.DataColNames = ['x', 'y', 'z', 'f']
        else:
            self.DataColNames = self.ColList
        #print( self.TimeInd, tI, self.stationinfo, self.sensname, self.DataColNames)
        #sys.exit()
        self.__GetDataInd__( colmp, ColList = self.ColList)
        
        index_vec = self.DataInd
        #index_vec = [0,1,2]
        ColNames = self.DataColNames
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[0].astype( float)
        #plt.plot( np.diff( iterateTime), alpha = 0.2)
        #plt.show()
        #sys.exit()
        del ztime
        if( debug):
            print( 'Got timestamps, datacolumn-names, column indices and arraydim')
        #sys.exit()
        #sum_leng = len(iterateTime)
        #print( 'iterateTime', iterateTime)
        #sys.exit()
        try:
            #xI = vario.KEYLIST.index('dx')
            #yI = vario.KEYLIST.index('dy')
            #zI = vario.KEYLIST.index('dz')
            #index_vec = [vario.KEYLIST.index('x'), vario.KEYLIST.index('y'), vario.KEYLIST.index('z')]#, vario.KEYLIST.index('f')]#, vario.KEYLIST.index('dx'), vario.KEYLIST.index('dy'), vario.KEYLIST.index('dz')]
            
            ##################
            # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
            ##################
            
            #from magpy.stream import num2date
            if( debug):
                print( 'iterateTime is:\n{}'.format( iterateTime))
            time_vario = iterateTime
            #print( 'time_vario', time_vario)
            #sys.exit()  
            time_vario_zero = NUM2DATE20TIME( num2date( time_vario))
            if( debug):
                print( 'time vario zero is:\n{}'.format( time_vario_zero))
            #sys.exit()
        except Exception as ex:
            print( 'Something went wrong with the timestamps')
            self.MyException(ex)
        #import matplotlib.pyplot as plt
        #plt.plot( time_vario_zero, ( colmp.ndarray[index_vec]).T, alpha = 0.2)
        #plt.show()
        #sys.exit()
        try:
            #time_vario_zero = dump[0]
            #print time_vario_zero
            
            #vario = vario.ndarray[[xI, yI, zI]]
            vario = colmp.ndarray[index_vec]
            #varioarray = np.matrix( list( itertools.chain( *vario))).reshape( ( 3, -1)).T
            arraydim = nanmax( shape( index_vec))
            if( debug):
                print( 'numbers of rows in array:\t{}'.format( arraydim))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
                print( 'index_vec:\t{}'.format( index_vec))
            varioarray = matrix( list( chain( *vario)))#.reshape( ( arraydim, -1)).T
            if( debug):
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            varioarray = varioarray.reshape( arraydim, -1).T
            #print( ColNames)
            #sys.exit()
            if( debug):
                print( 'shape of varioarray is:\t{}'.format( np.shape( varioarray)))
            #sys.exit()
            ietime, vario = self.__EquiInterpol__( time = time_vario_zero, data = varioarray)
            """
            #from magpy.stream import num2date
            stamp_starttime = NUM2DATE20TIME( num2date(time_vario[0])) # CONVERT TO ZEROTIME VALUES
            stamp_endtime = NUM2DATE20TIME( num2date(time_vario[-1])) # CONVERT TO ZEROTIME VALUES
            dt_vario = (stamp_endtime - stamp_starttime)/float(shape(time_vario)[0] - 1)
            #int_tvario = np.linspace( stamp_starttime, stamp_endtime, len( time_vario))
            int_tvario = np.arange( stamp_starttime, stamp_endtime + dt_vario, dt_vario)
            ietime = int_tvario
            """
            if( debug):
                print( 'shape of ietime is:\t{}'.format( np.shape( ietime)))
                print( 'shape of time_vario_zero is:\t{}'.format( np.shape( time_vario_zero)))
                print( 'shape of varioarray.T is:\t{}'.format( np.shape( varioarray.T)))
                print( 'shape of vario is:\t{}'.format( np.shape( vario)))
            
        except Exception as ex:
            self.MyException(ex)
        vseries = vario
        vtime = ietime
        
        print( 'Reading {}-Data...done\n'.format( colmp.header['SensorID']))
        #print( 'vseries is\n{}'.format( vseries))
        #print( 'vtime is\n{}'.format( vtime))
        
        gc.collect()
        del gc.garbage[:]
        self.data = vseries#* nano
        self.zerotime = vtime
        return self.zerotime, self.data
        #return vtime, vseries#, np.vstack(( aseries, bseries))
    
    
    def GetData(self):
        if( self.sensortype == 'MiniSeed'):
            self.zerotime, self.data = self.SeedRead()
        elif( self.sensortype == 'GradRead'):
            self.zerotime, self.data, self.sensPos = self.GradRead()
        elif( self.sensortype == 'SingleAbsRead'):
            self.zerotime, self.data, self.sensPos = self.SingleAbsRead()
        elif( self.sensortype == 'dBGrad'):
            self.zerotime, self.data, self.sensPos = self.dBRead()
        elif( self.sensortype == 'VarioRead'):
            self.zerotime, self.data = self.VarioRead()
        elif( self.sensortype == 'G823A' or self.sensortype == 'GSM90' or self.sensortype == 'GSM19'):
            self.zerotime, self.data = self.VarioRead()
        elif( self.sensortype == 'dBVario'):
            self.zerotime, self.data = self.dBVarioRead()
        elif( self.sensortype == 'mpstream'):
            self.zerotime, self.data = self.MPStreamRead()
        elif( self.sensortype == 'picologtxt'):
            self.zerotime, self.data = self.picologtxt()
        elif( self.sensortype == 'IAGAtxt'):
            self.zerotime, self.data = self.IAGAtxtRead()
        elif( self.sensortype == 'GFZKpRead'):
            self.zerotime, self.data = self.GFZKpRead()
        elif( self.sensortype == 'WICDefDataRead'):
            self.zerotime, self.data = self.WICDefDataRead()
        elif( self.sensortype == 'mpascii'):
            self.zerotime, self.data = self.MPascii()
        else:
            print( '\n\n\nsensortype is not valid...stopping')
            sys.exit()
        self.datetime = STAMPTODATE( self.zerotime)
        self.sensPos = self.Positions()
        return self.zerotime, self.data, self.sensPos, self.datetime