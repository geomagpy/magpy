"""
# RCS to magpy input filter
 Written by Richard Mandl

### RCS : Direct data from the Conrad Observatory RCS System
    DESCRIPTION:
        rcs directory and filename format:
        RCS/Logs/FP/2011/01/FP-GENLOG_2011_01_01.txt
        RCS/Logs/FP/config/FP-config_2011_01_01_000000.INI

    EXAMPLE:
        read(file, FP='FP', signals='1-2,4,6-8')


### RMRCS : Read extracted tab-separated files from RCS with header and signal descriptions
    DESCRIPTION:
        filename format:
        RCS-FP-2011-01-01_00-00-00.txt

    PARAMETERS:
        starttime
        endtime

    EXAMPLE:
        read('RCS-FP-2011-01-01_00-00-00.txt')

#This code only reads data from rcs log-files without knowing details about the signals
#The configuration has to be added by confincRCS, that reads rcs ini-files
"""
#from __future__ import print_function
#from __future__ import unicode_literals
#from __future__ import absolute_import
#from __future__ import division
from datetime import *
from matplotlib.dates import date2num
import numpy as np

from magpy.stream import DataStream
from magpy.stream import *
# TODO irgendwann integrieren
from magpy.lib.format_rcs_lib import *
# TODO was ist das?
# RCS header information is located in seperate files so this import: 
#from magpy.lib.getRCSheader import *
import os
from io import open
import logging
logger = logging.getLogger(__name__)

KEYLIST = DataStream().KEYLIST

# since there are a lot of signals in one rcs file compared to magpy, a maximum is defined
MAXSIGNALS = 15

def isRCS(filename):
    """
    Checks whether a file is a RCS file:

    if a unique pattern (e.g. in first line or filename or content) is recognized:
        return True
    else:
        return False
    tries to find a LabView Timestamp in the third column (seconds since 1904)
    difference to UNIX timestamp (seconds since 1970): 1293840000 
    3376684800 means 2011-01-01. Before there were no RCS loggings

    Alternatively the directory '_ALERTLOG' is accepted as filename:

    '_ALERTLOG' is part of a whole RCS folder
    The code assumes to find data files in the file structure
    """
    # TODO maybe a better solution, when filename shall be found by readRCS itself
    if 'RCS/Logs/_ALERTLOG' in filename:
        # read(rcs_path+'RCS/Logs/*', FP=FP, start='2022-02-02')
        return True
    try:
        temp = open(filename,'rt').readline()
        tempcol = temp.split('\t')
        if not (int(tempcol[0])>20110000 and int(tempcol[1])<240001 and float(tempcol[2].replace(',','.'))>3376684800):
            return False
    except:
        return False
    return True

def isRMRCS(filename):
    """
    Checks whether a file is ASCII RCS format.
    """
    try:
        with open(filename, 'r', encoding='utf-8', newline='', errors='ignore') as fh:
            temp = fh.readline()
    except:
        return False
    try:
        if not temp.startswith('# RCS'):
            return False
    except:
        return False
    return True


def readRCS(filename, headonly=False, **kwargs):
    """
    DESCRIPTION:
        Reading RCS data
    PARAMETERS:
        FP .. if omitted try to get FP from filename
        signals - e.g. signals='1-10,13,15-20' is used to select the data columns in a RCS data file
           if omitted take first MAXSIGNALS signals
           careful: error if less than MAXSIGNALS are available!
        rcs_dir .. filename will be ignored. Options FP and starttime can be others than in filename
           e.g. rcs_dir = '/home/cobs/n/RCS'
        config_dir .. explicitly set config_dir. Useful when not all data is available
        config_file .. useful when only one config file is available
        starttime .. if omitted, the first timestamp in filename will be taken
        endtime .. if omitted continue as long as there is no critical change in the config files

        special parameters:
        checktimestamps .. compare LabView and UTC time stamps to find errors
        ini_ignore .. ignore changes in config files like shortname or longname to detect changes of configuration
           default is ['messagetype','defaultrange'] for these are for real time warnings and not of interest afterwards
        
    --------------------------------------- example config file ------------------------------------------------------------------------------------------------------------------
        [op]
        user=Monitor
        [general]
        Client_Name=zagtfps1
        Client_IP=138.22.188.71
        Server_IP=138.22.188.209

        [frontpanel]
        1="Spannungsüberwachung 1-11"
        2=reserve
        3=reserve
        4=reserve
        5=reserve
        6=reserve
        7=reserve
        8=Status

        [states]
        1="ZAGTFPS1     M1      I,cFP-AI-110    CH00    1P1     13,8V 1: GPS/LWL Konv: GPS8, GPS9       OK      Out Of Range, S AV266H40        mA      y=6000x+0       AI"
        2="ZAGTFPS1     M1      I,cFP-AI-110    CH01    1P2     13,8V Modul Ausgang 2 NC        OK      Out Of Range, S AR0-30H40       mA      y=6000x+0       AI"
        3="ZAGTFPS1     M1      I,cFP-AI-110    CH02    1P3     13,8V Modul Ausgang 3 NC        OK      Out Of Range, S AR0-30H40       mA      y=6000x+0       AI"
        4="ZAGTFPS1     M1      I,cFP-AI-110    CH03    2P1     27,6V 1: Hirschm.LWL/LAN; FP S1; GPS-Ausstr.8,9 OK      Out Of Range, S AV495H40        mA      y=6000x+0       AI"
        5="ZAGTFPS1     M1      I,cFP-AI-110    CH04    2P2     27,6V Modul Ausgang 2 NC        OK      Out Of Range, S AR0-30H40       mA      y=6000x+0       AI"
        6="ZAGTFPS1     M1      I,cFP-AI-110    CH05    2P3     27,6V Modul Ausgang 3 NC        OK      Out Of Range, S AR0-30H40       mA      y=6000x+0       AI"
        7="ZAGTFPS1     M1      I,cFP-AI-110    CH06    3P1     Sonderspannungsmodul Ausgang 1 NC       OK      Out Of Range, S AR0-30H40       mA      y=6000x+0       AI"
        8="ZAGTFPS1     M1      I,cFP-AI-110    CH07    3P2     Sonderspannungsmodul Ausgang 2 NC       OK      Out Of Range, S AR0-30H40       mA      y=6000x+0       AI"
        9="ZAGTFPS1     M2      I,cFP-AI-110    CH00    3P3     Sonderspannungsmodul Ausgang 3 NC       OK      Out Of Range, S AR0-30H40       mA      y=6000x+0       AI"
        10="ZAGTFPS1    M2      I,cFP-AI-110    CH01    3P4+    Sonderspannungsmodul Ausgang 4 NC       OK      Out Of Range, S AR0-30H40       mA      y=6000x+0       AI"
        11="ZAGTFPS1    M2      I,cFP-AI-110    CH02    3P4-    Sonderspannungsmodul Ausgang 4 NC       OK      Out Of Range, S AR0-30H40       mA      y=6000x+0       AI"
    --------------------------------------- end     config file ------------------------------------------------------------------------------------------------------------------

    --------------------------------------- example data file (first line) -------------------------------------------------------------------------------------------------------
    20250205	000000	3821558399,99	250.895E+0	13.806E+0	18.567E+0	507.979E+0	13.806E+0	-18.567E+0	32.850E+0	56.654E+0	-42.371E+0	37.610E+0	38.563E+0
    --------------------------------------- end     data file --------------------------------------------------------------------------------------------------------------------


    """
    # Initialize contents of DataStream object
    stream = DataStream([],{})
    headers = {} 
    array = [[] for key in stream.KEYLIST]

    FPs = ['2F2','2F3A','2F3B','2F4','G0','S1','S2','S3','S4','S5','M6','T7','L8','VGA','VGB']

    debug = kwargs.get('debug')

    # FP given by kwargs is valid, if not try to get from filename
    FP = kwargs.get('FP')
    if not FP:
        try:
            FP = os.path.basename(filename).split('-')[0]
        except:
            if debug:
                print('skipping '+filename+'...')
    if not FP in FPs:
        if debug:
            print(FP+' is not a fieldpoint FP')
        # TODO was wenn FP nicht existiert?
        quit()
    headers['FP'] = FP
    if debug:
        print('got valid FP name: '+FP)

    # selection of signals
    signalsstr = kwargs.get('signals')
    if signalsstr is None:
        # no signals given - taking as much as possible
        signalsstr = "1-"+str(MAXSIGNALS)
    try:
        # produce an array of signal numbers
        signalsarray = []
        sigranges = signalsstr.split(',')
        for sig in sigranges:
            sigchan = sig.split('-')
            if len(sigchan) == 1:
                # a single number
                signalsarray.append(int(sigchan[0]))
            elif len(sigchan) == 2:
                # a range of signals
                for s in range(int(sigchan[0]),int(sigchan[1])+1):
                    signalsarray.append(s)
            else:
                # something went wrong
                print('could not create an array of signal numbers')
        # cut if necessary
        if len(signalsarray) > MAXSIGNALS:
            signalsarray = signalsarray[0:MAXSIGNALS]
    except:
        print('exception: could not create an array of signal numbers')
    headers['signals'] = signalsarray

    # assign signals to keys (at least one signal)
    headers['SensorKeys'] = stream.KEYLIST[1]
    for i in range(1,len(signalsarray)):
        headers['SensorKeys'] += ','
        headers['SensorKeys'] += stream.KEYLIST[i+1]


    # special switches
    # check timestamps (RCS calculates time in format YYYYMMDD  hhmmss  from LabView timestamp)
    checktimestamps = kwargs.get('checktimestamps')
    if checktimestamps is None:
        checktimestamps = False
    headers['checktimestamps'] = checktimestamps
    # config files .ini - ignore changes in config files
    ini_ignore = kwargs.get('ini_ignore')
    if not ini_ignore:
        ini_ignore = ['messagetype','defaultrange']
    headers['ini_ignore'] = ini_ignore


    # get info from filename
    # TODO filename must be from starttime's year, e.g. read('FP-GENLOG_2022_02_02.txt', start='2022-05-05')
    # TODO as a compromise isRCS() accepts '/path/to/RCS/Logs/*'
    #      e.g. read('/path/to/RCS/Logs/*', start='2022-02-02T12:00:00') _ALERTLOG needs to be in Logs/
    #      use start instead of starttime !!

    # rcs_dir
    abs_rcs_path = os.path.abspath(filename)
    if 'RCS/Logs/' in abs_rcs_path:
        # if rcs_dir is detected, rcs_dir will be overwritten if given explicitly later
        try:
            rcs_dir = abs_rcs_path.split('/Logs')[0]
            #rcs_dir = os.path.abspath(os.path.join(filename,'../..'))
            if not os.path.isdir(os.path.join(rcs_dir,'Logs',FP)):
                rcs_dir = None
        except:
            pass
    if debug: 
        print('using RCS directory: '+rcs_dir)
    day_from_filename = None
    if not rcs_dir:
        # day
        try:
            day_from_filename = datetime.strptime(filename.split('-GENLOG_')[1],'%Y_%m_%d.txt')
        except Exception as e:
            print(e)
            print('warning: problem getting date from filename')

    # time range
    starttime = kwargs.get('starttime')
    if not starttime:
        #print('try start=starttime instead of starttime=starttime')
        # TODO Kompromiss start statt starttime
        try:
            starttime = testtime(kwargs.get('start'))
        except:
            pass
    if not starttime:
        if day_from_filename:
            starttime = day_from_filename
    headers['starttime'] = starttime
    if 'endtime' in kwargs:
        endtime = kwargs.get('endtime')
    elif 'end' in kwargs:
        endtime = kwargs.get('end')
    else:
        endtime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    try:
        # string to datetime
        endtime = testtime(endtime)
    except Exception as e:
        print(e)
        quit()
        pass

    if not starttime:
        # TODO ueberlegen...
        print('no starttime given, giving up')
        quit()

    # config_files given by kwargs
    config_dir = kwargs.get('config_dir')
    rcs_dir_kw = kwargs.get('rcs_dir')
    if rcs_dir_kw:
        # explicitly given rcs_dir overrules that one from filename 'RCS/Logs/*'
        rcs_dir = rcs_dir_kw
    if config_dir and rcs_dir:
        config_diraux =  os.path.join(rcs_dir,'Logs',FP,datetime(starttime,'%Y/%m'))
        if not config_dir == config_diraux:
            print('contradiction: config_dir: '+config_dir+' vs. '+config_diraux)
            quit()
    if config_dir:
        if os.path.isdir(config_dir):
            print('config_dir '+config_dir+' does not exist')
            quit()
    elif rcs_dir:
        try:
            config_diraux =  os.path.join(rcs_dir,'Logs',FP,starttime.strftime('%Y/%m'))
        except Exception as e:
            print(e)
        if not os.path.isdir(config_diraux):
            print('rcs config directory '+config_diraux+' does not exist')
            quit()
    config_file = kwargs.get('config_file')
    if config_file:
        if not os.path.isfile(config_file):
            print('config_file '+config_file+' does not exist')
            quit()

    if debug:
        print('got filename: '+filename)

    # data files
    file_dir = os.path.dirname(filename)
    if not rcs_dir and not config_dir and not config_file:
        # no info about directories given
        try:
            rel_config_dir = os.path.abspath(os.path.join(file_dir,'../../config'))
            if os.path.isdir(rel_config_dir):
                config_dir = rel_config_dir
        except:
            pass
    if not rcs_dir and not FP == os.path.basename(filename).split('-')[0]:
        # different FP, search in other parts of rcs directory
        try:
            rel_data_dir = os.path.abspath(os.path.join(file_dir,'../../../',FP))
            if os.path.isdir(rel_data_dir):
                rcs_dir = os.path.abspath(os.path.join(rel_data_dir,'../../'))
        except:
            pass

    headers['SensorID'] = 'RCS{}_20110203_0001'.format(FP) # 20110203 corresponds to the date at which RCS was activated in SGO
    headers["SensorName"] = 'RCS{}'.format(FP)
    headers["SensorSerialNum"] = "20110203"
    headers["SensorRevision"] = "0001"
    headers["SensorModule"] = "RCS"
    headers["SensorGroup"] = "environment"
    headers["SensorDataLogger"] = "{}".format(FP)

    # configlist
    if rcs_dir:
        configlist = os.listdir(os.path.join(rcs_dir,'Logs',FP,'config'))
        # TODO gehoert das hier her?
        headers['inidir'] = os.path.join(rcs_dir,'Logs',FP,'config')
    elif config_dir:
        configlist = os.listdir(config_dir)
        # TODO gehoert das hier her?
        headers['inidir'] = config_dir


    # main loop
    time = starttime
    #try:
    if True:
        valid_from, valid_until, signaldict = get_max_range(time,configlist,headers)
    else:
    #except Exception as e:
        print('problem while get_max_range')
        print(e)
        quit()
    try:
        # TODO Ergebnis besser checken, Fehler abfangen
        if endtime:
            if valid_until < endtime:
                print('time range is smaller than given endtime. setting to '+str(valid_until))
                endtime = valid_until
        else:
            # TODO Unterschied??
            endtime = valid_until
    except Exception as e:
        print(e)
    getfile = True

    # TODO brauch ich das, oder ist headers schon, wie es sein soll?
    for i,sig in enumerate(signaldict):
        # TODO can be different, maybe a combination of short and longname
        headers['col-'+KEYLIST[i+1]] = signaldict[sig]['shortname']
        headers['unit-col-'+KEYLIST[i+1]] = signaldict[sig]['unit']
        if headers['unit-col-'+KEYLIST[i+1]] == '--':
            headers['unit-col-'+KEYLIST[i+1]] = ''

    while getfile:
        # TODO soll filename Vorrang haben?
        if rcs_dir:
            # RCS/Logs/2F4/2024/11/2F4-GENLOG_2014_11_01.txt
            filename = os.path.join(rcs_dir,'Logs',FP,datetime.strftime(time,'%Y/%m/'+FP+'-GENLOG_%Y_%m_%d.txt'))
        if os.path.isfile(filename):
            # TODO irgendwann weg
            print(filename)
            fh = open(filename, 'rt')
        else:
            print(filename+' not found')
            quit()
        for line in fh:
            elem = line.strip().split('\t')
            if elem == []:
                print("blank line !?")
            else:
                try:
                    calculatedTimestamp = elem[0][0:4]+'-'+elem[0][4:6]+'-'+elem[0][6:8]+'T'+elem[1][0:2]+':'+elem[1][2:4]+':'+elem[1][4:6]
                    calculatedTimestampDatetime = datetime.strptime(calculatedTimestamp,"%Y-%m-%dT%H:%M:%S")
                    # TODO following line does not work !?
                    #calculatedTimestampDatetime = stream._testtime(calculatedTimestamp)
                except:
                    print('no valid timestamp')
                if checktimestamps:
                    try:
                        LabViewTimestamp = datetime(1904,1,1,0,0) + timedelta(seconds=float(elem[2].replace(',','.')))
                    except:
                        pass
                    # TODO das geht wohl nicht, wenn ein Fehler im try...
                    if LabViewTimestamp - calculatedTimestampDatetime > timedelta(milliseconds=500):
                        print('wrong calculated timestamp at ',LabViewTimestamp,' vs ',calculatedTimestamp)
                # timestamp
                # skip samples outside the time interval
                if calculatedTimestampDatetime < starttime:
                    continue
                if calculatedTimestampDatetime > endtime:
                    continue
                valerror=False
                try:
                    # magpy1
                    #array[0].append(date2num(calculatedTimestampDatetime))
                    # magpy2
                    array[0].append(calculatedTimestampDatetime)
                except:
                    print("could not append timestamp "+calculatedTimestamp)
                    valerror = True
                # data selected by signalsarray
                if not valerror:
                    try:
                        for i in range(len(signalsarray)):
                            array[i+1].append(float(elem[signalsarray[i]+2]))
                    except:
                        print("could not append value(s) at "+calculatedTimestamp)
        fh.close()
        file_found = False
        no_files_left = False
        while not file_found and not no_files_left:
            # TODO Dateieinlesemodus wie magpy
            time = time + timedelta(days=1)
            if rcs_dir:
            # RCS/Logs/2F4/2024/11/2F4-GENLOG_2014_11_01.txt
                filename = os.path.join(rcs_dir,'Logs',FP,str(time.year),str(time.month),datetime.strftime(time,FP+'-GENLOG_%Y_%m_%d.txt'))
                if os.path.isfile(filename):
                    file_found = True
            if time > endtime:
                no_files_left = True
        if time - timedelta(days=1) > endtime:
            getfile=False

    fh.close()
    stream = DataStream(header=headers,ndarray=np.asarray(array,dtype=object))
    return stream
#    print ("Hi Richard, this is the skeleton to insert your code")


def readRMRCS(filename, headonly=False, **kwargs):
    """
    Reading RMRCS format data. (Richard Mandl's RCS extraction)
    # RCS Fieldpoint T7
    # Conrad Observatorium, www.zamg.ac.at
    # 2012-02-01 00:00:00
    #
    # 12="ZAGTFPT7      M6      I,cFP-AI-110    CH00    AP23    Niederschlagsmesser     --      Unwetter, S     AR0-20H0.1      mm      y=500x+0        AI"
    # 13="ZAGTFPT7      M6      I,cFP-AI-110    CH01    JC      Schneepegelsensor       OK      Mastverwehung, S        AR0-200H0       cm      y=31250x+0      AI"
    # 14="ZAGTFPT7      M6      I,cFP-AI-110    CH02    430A_T  Wetterhuette - Lufttemperatur   -       -, B    AR-35-45H0      C       y=4000x-35      AI"
    # 15="ZAGTFPT7      M6      I,cFP-AI-110    CH03    430A_F  Wetterhuette - Luftfeuchte      -       -, B    AR0-100H0       %       y=5000x+0       AI"
    #
    1328054403.99       20120201 000004 49.276E-6       49.826E+0       -11.665E+0      78.356E+0
    1328054407.99       20120201 000008 79.480E-6       49.823E+0       -11.677E+0      78.364E+0
    1328054411.99       20120201 000012 68.555E-6       49.828E+0       -11.688E+0      78.389E+0
    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    getfile = True

    debug = kwargs.get('debug')
    if debug:
        print ("RCS: found data from Richards Perl script")

    # read file and split text into channels
    # --------------------------------------
    stream = DataStream()
    headers = {}
    array = [[] for key in KEYLIST]
    data = []
    measurement = []
    unit = []
    i = 0
    key = None

    # try to get day from filename (platform independent)
    # --------------------------------------
    try:
        # RCS-2F4-2025-07-24_00-00-00.txt
        FP = filename.split('-')[1]
        theday = datetime.strptime(filename.split('_')[0],'RCS-'+FP+'-%Y-%m-%d')
    except:
        logger.warning('problem getting date from filename')

    try:
        if starttime:
            if not theday[-1] >= datetime.date(testtime(starttime)):
                getfile = False
        if endtime:
            if not theday[0] <= datetime.date(testtime(endtime)):
                getfile = False
    except:
        # Date format not recognized. Need to read all files
        getfile = True

    if getfile:
        with open(filename, 'r', encoding='utf-8', newline='', errors='ignore') as fh:
            for line in fh:
                if line.isspace():
                    # blank line
                    pass
                elif line.startswith('# RCS Fieldpoint'):
                    # data header
                    fieldpoint = line.replace('# RCS Fieldpoint','').strip()
                elif line.startswith('#'):
                    # data header
                    colsstr = line.split(',')
                    if (len(colsstr) == 3):
                        # select the lines with three komma separeted parts -> they describe the data
                        meastype = colsstr[1].split()
                        unittype = colsstr[2].split()
                        measurement.append(meastype[2])
                        unit.append(unittype[2])
                        headers['col-'+KEYLIST[i+1]] = measurement[i]
                        headers['unit-col-'+KEYLIST[i+1]] = unit[i]
                        if headers['unit-col-'+KEYLIST[i+1]] == '--':
                            headers['unit-col-'+KEYLIST[i+1]] = ''
                        i=i+1
                elif headonly:
                    # skip data for option headonly
                    continue
                else:
                    # data entry - may be written in multiple columns
                    # row beinhaltet die Werte eine Zeile
                    elem = line[:-1].split()
                    gottime = False

                    try:
                        array[0].append(datetime.strptime(elem[1],"%Y-%m-%dT%H:%M:%S"))
                        add = 2
                        gottime = True
                    except:
                        try:
                            array[0].append(datetime.strptime(elem[1]+'T'+elem[2],"%Y%m%dT%H%M%S"))
                            add = 3
                            gottime = True
                        except:
                            raise ValueError("Can't read date format in RCS file")
                    if gottime:
                        for i in range(len(unit)):
                            try:
                                array[i+1].append(float(elem[i+add]))
                            except:
                                array[i+1].append(np.nan)
                                pass

            array = [np.asarray(el) for el in array]
            headers['SensorID'] = 'RCS{}_20160114_0001'.format(fieldpoint) # 20160114 corresponds to the date at which RCS was activated TODO
            headers["SensorName"] = 'RCS{}'.format(fieldpoint)
            headers["SensorSerialNum"] = "20160114"
            headers["SensorRevision"] = "0001"
            headers["SensorModule"] = "RCS"
            headers["DataFormat"] = "RCS-Perl"
            headers["SensorGroup"] = "environment"
            headers["SensorDataLogger"] = "{}".format(fieldpoint)
    else:
        headers = stream.header
        stream =[]

    return DataStream(header=headers, ndarray=np.asarray(array,dtype=object))
