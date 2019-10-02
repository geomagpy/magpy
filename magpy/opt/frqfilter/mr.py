#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import sys
import os

debug = False # Print every single output


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
from datetime import datetime, timedelta
from os.path import isfile, isdir, join, abspath
from fnmatch import fnmatch
sys.path.insert( 0, abspath('../../'))
from stream import read, datetime, DataStream
del sys.path[0]
if( debug):
    print( 'magpy.stream successfully imported')
from matplotlib.dates import date2num


from scipy.interpolate import interp1d
from itertools import chain

if( debug):
    import matplotlib.pyplot as plt
import numpy as np
from numpy import shape, hstack, array, vstack, matrix, pi, nanmax#, where, nanstd

try:
    sys.path.insert( 0, abspath('../../'))
    #import mpplot as mp
    import mpplot as mp
    del sys.path[0]
    #print( 'sys.path[0] is: {}'.format( sys.path[0]))
    if( debug):
        print( 'mpplot imported')
except Exception as ex:
    print( 'mpplot import did not work. Interrupted with: {}'.format( ex))
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




#############################
# CONSTANTS DEF
#############################
mue0 = 4.0*pi*pow(10,-7)
trafofacE = np.power(10.0,-6.0) ##### Volt / digits
eledist = 60.0 # Elextrodenabstand sofern beide gleich sind
pico = np.power(10.0, -12.0)
nano = np.power(10.0, -9.0)
#############################
# TEST
#############################
#startdate = datetime.strptime("2017-03-09", "%Y-%m-%d")
#enddate = datetime.strptime("2017-06-26", "%Y-%m-%d")
#enddate = datetime.strptime("2017-03-10", "%Y-%m-%d")
#############################




class MR:
    """
    MR (former MULTIREAD), V1.0alpha
    
    
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
    
    
    
    ...
    
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
                dBVario
                mpstream
                MiniSeed --- under construction ---
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
        
        channels:
            List of strings
            Strings in list are used to search for more specific filetype
            endings
            Default is:
                'bin'
        
        TrF:
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
    
    Methods
    -------
        
        Positions: applies to Supergrad-data until now
            Applies to defined Gauss-Krueger coordinates in referrence
            frame to sensor data for further use in calculations of
            offsets, etc.
        
        GetData: 
            Performs read of data for given "sensortype" from "starttime" to 
            "endtime" and derives zerotime for given datetime.objects within 
            that timerange
        
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
                      sensortype, path = os.path.abspath( pathstring))
        zerotime, data = DataCont.GetData()
        
        
        
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
        
        
        
        sensortype = 'MiniSeed'
        DataCont = MR(starttime = starttime, endtime = endtime,
                      sensortype = sensortype, 
                      path = os.path.abspath( pathstring), 
                      channels = ['pri0', 'pri1'], 
                      SrcStr = str( 'e604616'), 
                      TrF = True, TrFVal = 0.00000000001)
        zerotime, data, sensPos, mydate = DataCont.GetData()
        
    """
    
    
    
    class MyException(Exception):
        """
        raise self.MyException("My Exception test is...")
        """
        pass
    
    
    
    def __init__( self, **kwargs):
        print( '__init__ just started...')
        try:
            self.filetype = 'bin'
            self.trafoval = trafofacE
            self.trafofac = False
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
                                        self.SearchString = val
                            except Exception as ex:
                                print( 'Sensortype is: {}. Filepath missing...stopping!'.format( self.sensortype))
                                
                    if( debug):
                        print( 'self.sensortype is: {}'.format( self.sensortype))
                if( item == 'channels'):
                    """
                    
                    get endtime to be investigated
                    """
                    self.filetype = value
                    if( debug):
                        print( 'self.filetype is: {}'.format( self.filetype))
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
            print( 'An expection occured.\tExepction is:\n\t{}'.format( ex))
            #sys.exit()
    
    
    
    def __dBInit__( self):
        
        
        
        ##################
        # CONNECT TO DATABASE
        ##################
        from magpy.database import mysql
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
    
    
    
    def __LookForFilesMatching__( self):
        
        mydirs = []
        myfiles = []
        walkout = walk( self.path)
        for root, b, el in walkout:
            #print( 'a {}\tb {}\tel {}'.format( a, b, el))
            for name in el:
                #print( 'a {}\tb {}\tel {}'.format( root, b, el))
                checkvar = join( root, name)
                #if( debug):
                #    print( 'checkvar is: {}'.format( checkvar))
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
        else:
            #print( 'No files for {} found...stopping'.format( self.SearchString))
            raise self.MyException( 'No files for {} found...stopping'.format( self.SearchString))
        
        if( debug):
            print( 'used files:\n\n\n')
            for el in gdfiles:
                print( el)
        
        #sys.exit()
        self.files = gdfiles
        return
    
    
    
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
            raise self.MyException('No good indices...stopping')
        self.goodind = mygoodind
        
        return self.goodind
    
    
    
    def Positions( self):
        ######################################
        # DEFINE ACURRATE SENSOR POSITIONS
        ######################################
        """
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
        
        #sensPos = vstack( ( sensorpos_N, sensorpos_S, sensorpos_NS, sensorpos_E, sensorpos_EW, sensorpos_W, sensorpos_TB, sensorpos_TA, sensorpos_B))
        sensPos = vstack( ( sensorpos_NS, sensorpos_N, sensorpos_S, sensorpos_EW, sensorpos_E, sensorpos_W, sensorpos_TA, sensorpos_TB, sensorpos_B))
        
        #sensName = array( ['N', 'S', 'NS', 'E', 'EW', 'W', 'TB', 'TA', 'B'])
        sensName = array( ['NS', 'N', 'S', 'EW', 'E', 'W', 'TA', 'TB', 'B'])
        sensPos = hstack( ( np.atleast_2d( sensName).T, sensPos))
        np.save( './SuperGradPos.npy', sensPos)
        """
        sensPos = np.load( './SuperGradPos.npy')
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
        days = []
        while startdate <= enddate:
            days.append(startdate)
            startdate += timedelta(days=1)
            #channels = ['pri0', 'pri1', 'pri2', 'pri3', 'pri4', 'pri5']
            #channels = ['pri3', 'pri4', 'pri5']
            #channels = ['pri0', 'pri1', 'pri2']
        #channelse = ['pri0','pri1']
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
            for c in self.filetype: #str( 'e604618*')
                files = []
                for el in self.SearchString:
                    files.append( sorted([f for f in flt( self.files, '*' + el + '*') if c in f ]))
                #print( 'shape of files: {}'.format( shape( files)))
                files = list( array( files).flatten())
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
            if( debug):
                for k, f in enumerate( colst):
                    print( 'colst[{}] is :\t{}'.format( k, f))
            #sys.exit()
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
        for k, trace in enumerate( colst.select(channel = '*').traces):
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
        Ntraces = min( shape( CutArray))
        mseries = array( CutArray).reshape( Ntraces, -1)
        #for n in range(1,min(len(colst.select(channel = 'p0').traces), len(colst.select(channel = 'p1').traces))):
        #    iterate = colst.select(channel = 'p0').traces[n]
        self.zerotime = ztime
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
        days = []
        while startdate <= enddate:
            days.append(startdate)
            startdate += timedelta(days=1)
            #channels = ['pri0', 'pri1', 'pri2', 'pri3', 'pri4', 'pri5']
            #channels = ['pri3', 'pri4', 'pri5']
            #channels = ['pri0', 'pri1', 'pri2']
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
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        try:
            tI = colmp.KEYLIST.index('time')
        except:
            try:
                tI = colmp.KEYLIST.index('sectime')
            except Exception as ex:
                print( 'No valid timestamps. Exception:\n{}'.format( ex))
                sys.exit()
        
        index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z'), colmp.KEYLIST.index('dx'), colmp.KEYLIST.index('dy'), colmp.KEYLIST.index('dz')]
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'EW dataarray reshaped...converting timestamps')
            #sys.exit()
        
        stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
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
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...NS #{} file {} read.'.format( k, f))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            print( '...NS #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'NS matching read...removing duplicates')
            #sys.exit()
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        try:
            tI = colmp.KEYLIST.index('time')
        except:
            try:
                tI = colmp.KEYLIST.index('sectime')
            except Exception as ex:
                print( 'No valid timestamps. Exception:\n{}'.format( ex))
                sys.exit()
        
        index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z'), colmp.KEYLIST.index('dx'), colmp.KEYLIST.index('dy'), colmp.KEYLIST.index('dz')]
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'NS dataarray reshaped...converting timestamps')
            #sys.exit()
        
        stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
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
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...V #{} file {} read.'.format( k, f))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            print( '...V #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'V matching read...removing duplicates')
            #sys.exit()
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        try:
            tI = colmp.KEYLIST.index('time')
        except:
            try:
                tI = colmp.KEYLIST.index('sectime')
            except Exception as ex:
                print( 'No valid timestamps. Exception:\n{}'.format( ex))
                sys.exit()
        
        index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z'), colmp.KEYLIST.index('dx'), colmp.KEYLIST.index('dy'), colmp.KEYLIST.index('dz')]
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'V dataarray reshaped...converting timestamps')
            #sys.exit()
        
        stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
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
        days = []
        while startdate <= enddate:
            days.append(startdate)
            startdate += timedelta(days=1)
            #channels = ['pri0', 'pri1', 'pri2', 'pri3', 'pri4', 'pri5']
            #channels = ['pri3', 'pri4', 'pri5']
            #channels = ['pri0', 'pri1', 'pri2']
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
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        try:
            tI = colmp.KEYLIST.index('time')
        except:
            try:
                tI = colmp.KEYLIST.index('sectime')
            except Exception as ex:
                print( 'No valid timestamps. Exception:\n{}'.format( ex))
                sys.exit()
        
        index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'EW dataarray reshaped...converting timestamps')
            #sys.exit()
        
        stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
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
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...NS #{} file {} read.'.format( k, f))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            print( '...NS #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'NS matching read...removing duplicates')
            #sys.exit()
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        try:
            tI = colmp.KEYLIST.index('time')
        except:
            try:
                tI = colmp.KEYLIST.index('sectime')
            except Exception as ex:
                print( 'No valid timestamps. Exception:\n{}'.format( ex))
                sys.exit()
        
        index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'NS dataarray reshaped...converting timestamps')
            #sys.exit()
        
        stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
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
        for k, f in enumerate( allfiles):
            temp = read( f)  # READING MAGPY STREAMS
            print( '...V #{} file {} read.'.format( k, f))
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            print( '...V #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'V matching read...removing duplicates')
            #sys.exit()
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        try:
            tI = colmp.KEYLIST.index('time')
        except:
            try:
                tI = colmp.KEYLIST.index('sectime')
            except Exception as ex:
                print( 'No valid timestamps. Exception:\n{}'.format( ex))
                sys.exit()
        
        index_vec = [colmp.KEYLIST.index('x'), colmp.KEYLIST.index('y'), colmp.KEYLIST.index('z')]
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        sum_leng = len(iterateTime)
        
        temp = (matrix( list( chain( colmp.ndarray[index_vec])))).reshape( arraydim,-1)
        
        mseries = (temp).astype(float)#/ mue0
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'V dataarray reshaped...converting timestamps')
            #sys.exit()
        
        stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
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
        days = []
        while startdate <= enddate:
            days.append(startdate)
            startdate += timedelta(days=1)
            #channels = ['pri0', 'pri1', 'pri2', 'pri3', 'pri4', 'pri5']
            #channels = ['pri3', 'pri4', 'pri5']
            #channels = ['pri0', 'pri1', 'pri2']
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
        # READ Vario
        #########################
        
        
        
        
        
        
        #########################
        # READ Vario
        #########################
        
        
        sensname = 'LEMI036_1_0002_'
        if( len( self.SearchString) < len( sensname) and fnmatch( sensname, '*' + self.SearchString + '*')):
            pass
        else:
            sensname = self.SearchString
        channelvario = [[sensname + (datetime(f.year, f.month, f.day) ).strftime('%Y-%m-%d')  + fileending for f in days]]
        
        #channels = ['LEMI036_1_0002_' + liststart + '.bin': 'LEMI036_1_0002_' + listend + '.bin']
        #tage = sorted(listdir(dirstring+'/'))
        
        
        channelvario = sum(channelvario,[])
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
                if( fnmatch( el, '*' + vel)):
                    n = n + 1
                    allfiles.append( el)
                    print( 'Vario #{} file is: {}'.format( n, el))
        
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
            #colmp = mp.appendStreams([colmp, temp]) # CONCATENATE MAGPY STREAMS
            colmp = mp.appendStreams((colmp, temp)) # CONCATENATE MAGPY STREAMS
            #colmp.extend(temp , temp.header, temp.ndarray)
            print( '...Vario #{} file {} appended.'.format( k, f))
        if( debug):
            print( 'Vario matching read...removing duplicates')
            #sys.exit()
        
        temp = colmp.trim(starttime = sstartdate, endtime = enddate) # CUT OUT ONLY TIME OF INTEREST
        colmp = temp
        del temp
        colmp = colmp.removeduplicates()
        colmp = colmp.sorting()
        
        
        try:
            tI = colmp.KEYLIST.index('time')
        except:
            try:
                tI = colmp.KEYLIST.index('sectime')
            except Exception as ex:
                print( 'No valid timestamps. Exception:\n{}'.format( ex))
                sys.exit()
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
        arraydim = nanmax( shape( index_vec))
        iterateTime = colmp.ndarray[tI]
        sum_leng = len(iterateTime)
        
        temp = matrix( list( chain( colmp.ndarray[index_vec]))).reshape( arraydim, -1)
        if( debug):
            for k, el in enumerate( temp):
                print( '\nrow[{}] of temp is:\t{}'.format( k, el))
        mseries = (temp).astype(float)#/ mue0
        
        t = iterateTime #  - 0.19999 % Zeitkorrektur wie bei letzer Impedanzauswertung
        if( debug):
            print( 'Vario dataarray reshaped...converting timestamps')
            #sys.exit()
        
        stamp_starttime = NUM2DATE20TIME( num2date(t[0])) # CONVERT TO ZEROTIME VALUES
        stamp_endtime = NUM2DATE20TIME( num2date(t[-1])) # CONVERT TO ZEROTIME VALUES
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
        vtime = ietime
        
        print( 'Reading Vario-Data...done\n')
        
        gc.collect()
        del gc.garbage[:]
        self.data = vseries
        self.zerotime = vtime
        
        return vtime, vseries#, np.vstack(( aseries, bseries))
    
    
    
    def dBRead( self):
        if( debug):
            print( '\n\n\nStarting dBRead...')
        #from scipy.interpolate import interp1d
        startdate = self.starttime
        enddate = self.endtime
        ##################
        # CONNECT TO DATABASE
        ##################
        from magpy.database import readDB
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
        self.__dBInit__() # initialize database connection
        
        
        ##################
        # NS
        #
        # READ IN SUPERGRAD NS-SENSORS GRADIENTS
        ##################
        try:
            ns = readDB( self.dB, 'GP20S3NS_012201_0001_0001', starttime = startdate, endtime = enddate)
            #ns = readDB( db, 'GP20S3NS_012201_0001_0001', starttime = startdate, endtime = enddate)
            #ns.sorting()
            ns = ns.removeduplicates()
            
            
            
            ##################
            # IDENTIFY CORRECT COLUMNS
            ##################
            # headers are in 
            # ns.header
            try:
                tI = ns.KEYLIST.index('time')
            except:
                try:
                    tI = ns.KEYLIST.index('sectime')
                except Exception as ex:
                    print( 'No valid timestamps. Exception:\n{}'.format( ex))
                    #sys.exit()
            #xI = ns.KEYLIST.index('dx')
            #yI = ns.KEYLIST.index('dy')
            #zI = ns.KEYLIST.index('dz')
            index_vec = [ns.KEYLIST.index('x'), ns.KEYLIST.index('y'), ns.KEYLIST.index('z'), ns.KEYLIST.index('dx'), ns.KEYLIST.index('dy'), ns.KEYLIST.index('dz')]
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
            dump = NUM2DATE20TIME( num2date( time_ns))
            #time_ns_zero = dump[0]
            time_ns_zero = dump
            #ns = ns.ndarray[[xI, yI, zI]]
            ns = ns.ndarray[index_vec]
            #nsarray = np.matrix( list( itertools.chain( *ns))).reshape( ( 3, -1)).T
            nsarray = matrix( list( chain( *ns))).reshape( arraydim, -1).T
            
            
            stamp_starttime = NUM2DATE20TIME( num2date(time_ns[0])) # CONVERT TO ZEROTIME VALUES
            stamp_endtime = NUM2DATE20TIME( num2date(time_ns[-1])) # CONVERT TO ZEROTIME VALUES
            dt_ns = (stamp_endtime - stamp_starttime)/float(shape(time_ns)[0] -1)
            #int_tns = np.linspace( stamp_starttime, stamp_endtime, len( time_ns))
            int_tns = np.arange( stamp_starttime, stamp_endtime, dt_ns)
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
                    print( 'lenght of el[{}]\t=\t{}'.format( k, len( el)))
            #sys.stdin.readline()
            #print ( ' len(int_tns): ', len(int_tns), ' len(time_ns): ', len(time_ns), 'si is: ', dt_ns)
        except Exception as ex:
            print( 'reading ns-data failed with execption {}...'.format( ex))
            print( '...replacing data with zeros')
            int_tns =  np.zeros( ( 1, 1))
            nsmissing = True
            #db.close() #closing connection to database
            #return
        
        
        
        ##################
        # EW
        #
        # READ IN SUPERGRAD EW-SENSORS GRADIENTS
        ##################
        try:
            ew = readDB( self.dB, 'GP20S3EW_111201_0001_0001', starttime = startdate, endtime = enddate)
            #ew = readDB( db, 'GP20S3EW_111201_0001_0001', starttime = startdate, endtime = enddate)
            #ew.sorting()
            ew = ew.removeduplicates()
            
            
            
            ##################
            # IDENTIFY CORRECT COLUMNS
            ##################
            
            try:
                tI = ew.KEYLIST.index('time')
            except:
                try:
                    tI = ew.KEYLIST.index('sectime')
                except Exception as ex:
                    print( 'No valid timestamps. Exception:\n{}'.format( ex))
                    #sys.exit()
            #xI = ew.KEYLIST.index('dx')
            #yI = ew.KEYLIST.index('dy')
            #zI = ew.KEYLIST.index('dz')
            index_vec = [ew.KEYLIST.index('x'),ew.KEYLIST.index('y'),ew.KEYLIST.index('z'),ew.KEYLIST.index('dx'),ew.KEYLIST.index('dy'),ew.KEYLIST.index('dz')]
            arraydim = nanmax( shape( index_vec))
            ##################
            # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
            ##################
            
            time_ew = ew.ndarray[tI]
            dump = NUM2DATE20TIME( num2date( time_ew))
            #time_ew_zero = dump[0]
            time_ew_zero = dump
            
            #ew = ew.ndarray[[xI, yI, zI]]
            ew = ew.ndarray[index_vec]
            #ewarray = np.matrix( list( itertools.chain( *ew))).reshape( ( 3, -1)).T
            ewarray = matrix( list( chain( *ew))).reshape( arraydim, -1).T
            
            
            stamp_starttime = NUM2DATE20TIME( num2date(time_ew[0])) # CONVERT TO ZEROTIME VALUES
            stamp_endtime = NUM2DATE20TIME( num2date(time_ew[-1])) # CONVERT TO ZEROTIME VALUES
            dt_ew = (stamp_endtime - stamp_starttime)/float(shape(time_ew)[0] -1)
            #int_tew = np.linspace( stamp_starttime, stamp_endtime, len( time_ew))
            int_tew = np.arange( stamp_starttime, stamp_endtime, dt_ew)
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
        
        
        
        ##################
        # V
        #
        # READ IN SUPERGRAD V-SENSORS GRADIENTS
        ##################
        try:
            v = readDB( self.dB, 'GP20S3V_911005_0001_0001', starttime = startdate, endtime = enddate)
            #v = readDB( db, 'GP20S3V_911005_0001_0001', starttime = startdate, endtime = enddate)
            #v.sorting()
            v = v.removeduplicates()
            
            
            
            ##################
            # IDENTIFY CORRECT COLUMNS
            ##################
            
            try:
                tI = v.KEYLIST.index('time')
            except:
                try:
                    tI = v.KEYLIST.index('sectime')
                except Exception as ex:
                    print( 'No valid timestamps. Exception:\n{}'.format( ex))
                    #sys.exit()
            #xI = v.KEYLIST.index('dx')
            #yI = v.KEYLIST.index('dy')
            #zI = v.KEYLIST.index('dz')
            index_vec = [v.KEYLIST.index('x'),v.KEYLIST.index('y'),v.KEYLIST.index('z'),v.KEYLIST.index('dx'),v.KEYLIST.index('dy'),v.KEYLIST.index('dz')]
            arraydim = nanmax( shape( index_vec))
            ##################
            # REARANGE COLUMNS TO MATRIX WITH N x 3 DIMENSION
            ##################
            
            time_v = v.ndarray[tI]
            dump = NUM2DATE20TIME( num2date( time_v))
            #time_v_zero = dump[0]
            time_v_zero = dump
            #v = v.ndarray[[xI, yI, zI]]
            v = v.ndarray[index_vec]
            #varray = np.matrix( list( itertools.chain( *v))).reshape( ( 3, -1)).T
            varray = matrix( list( chain( *v))).reshape( arraydim, -1).T
            
            
            stamp_starttime = NUM2DATE20TIME( num2date(time_v[0])) # CONVERT TO ZEROTIME VALUES
            stamp_endtime = NUM2DATE20TIME( num2date(time_v[-1])) # CONVERT TO ZEROTIME VALUES
            dt_v = (stamp_endtime - stamp_starttime)/float(shape(time_v)[0] -1)
            #int_tv = np.linspace( stamp_starttime, stamp_endtime, len( time_v))
            int_tv = np.arange( stamp_starttime, stamp_endtime, dt_v)
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
        if( ewmissing):
            print( 'ew-entries missing...')
            int_tew = reft
            ewarray = np.zeros( ( 6, reflen))
        if( vmissing):
            print( 'v-entries missing...')
            int_tv = reft
            varray = np.zeros( ( 6, reflen))
        
        
        
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
            print 'shape of nsarray: {}'.format(np.shape( nsarray))
            print 'shape of int_tns: {}'.format(np.shape( int_tns))
        #temp = interp1d( int_tns, nsarray.T, fill_value = 'extrapolate', kind = 'linear')( ietime)
        temp = interp1d( int_tns, nsarray, fill_value = 'extrapolate', kind = 'linear')( ietime)
        nsmseries = temp
        
        # EW
        if( debug):
            print 'shape of ewarray: {}'.format(np.shape( ewarray))
            print 'shape of int_tew: {}'.format(np.shape( int_tew))
        #temp = interp1d( int_tew, ewarray.T, fill_value = 'extrapolate', kind = 'linear')( ietime)
        temp = interp1d( int_tew, ewarray, fill_value = 'extrapolate', kind = 'linear')( ietime)
        ewmseries = temp
        
        # V
        if( debug):
            print 'shape of varray: {}'.format(np.shape( varray))
            print 'shape of int_tv: {}'.format(np.shape( int_tv))
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
        from magpy.database import readDB
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
        VarioSensorString = 'LEMI036_1_0002_0001'
        try:
            vario = readDB( self.dB, VarioSensorString, starttime = startdate, endtime = enddate)
            #ns.sorting()
            vario = vario.removeduplicates()
        except Exception as ex:
            self.dB.close()
            print( 'Variometer database entry problem with exception:\n{} ...stopping'.format( ex))
            sys.exit()
        self.dB.close()
        
        ##################
        # IDENTIFY CORRECT COLUMNS
        ##################
        
        try:
            tI = vario.KEYLIST.index('time')
        except:
            try:
                tI = vario.KEYLIST.index('sectime')
            except Exception as ex:
                print( 'No valid timestamps. Exception:\n{} ...stopping'.format( ex))
                sys.exit()
        #xI = vario.KEYLIST.index('dx')
        #yI = vario.KEYLIST.index('dy')
        #zI = vario.KEYLIST.index('dz')
        index_vec = [vario.KEYLIST.index('x'), vario.KEYLIST.index('y'), vario.KEYLIST.index('z')]#, vario.KEYLIST.index('f')]#, vario.KEYLIST.index('dx'), vario.KEYLIST.index('dy'), vario.KEYLIST.index('dz')]
        
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
        
        try:
            tI = vario.KEYLIST.index('time')
        except:
            try:
                tI = vario.KEYLIST.index('sectime')
            except Exception as ex:
                print( 'No valid timestamps. Exception:\n{} ...stopping'.format( ex))
                sys.exit()
        #xI = vario.KEYLIST.index('dx')
        #yI = vario.KEYLIST.index('dy')
        #zI = vario.KEYLIST.index('dz')
        #index_vec = [vario.KEYLIST.index('x'), vario.KEYLIST.index('y'), vario.KEYLIST.index('z')]#, vario.KEYLIST.index('f')]#, vario.KEYLIST.index('dx'), vario.KEYLIST.index('dy'), vario.KEYLIST.index('dz')]
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
    
    
    
    def GetData(self):
        if( self.sensortype == 'MiniSeed'):
            self.zerotime, self.data = self.SeedRead()
        elif( self.sensortype == 'GradRead'):
            self.zerotime, self.data, self.sensPos = self.GradRead()
        elif( self.sensortype == 'dBGrad'):
            self.zerotime, self.data, self.sensPos = self.dBRead()
        elif( self.sensortype == 'VarioRead'):
            self.zerotime, self.data = self.VarioRead()
        elif( self.sensortype == 'dBVario'):
            self.zerotime, self.data = self.dBVarioRead()
        elif( self.sensortype == 'mpstream'):
            self.zerotime, self.data = self.MPStreamRead()
        else:
            print( '\n\n\nsensortype is not valid...stopping')
            sys.exit()
        self.datetime = STAMPTODATE( self.zerotime)
        self.sensPos = self.Positions()
        return self.zerotime, self.data, self.sensPos, self.datetime