"""
MagPy
NOAA ACE  input filter
Updated by Roman Leonhardt October 2024
- contains test and read function
"""

from magpy.stream import DataStream, merge_streams
import os
import re
import numpy as np
from datetime import datetime, timezone
import dateutil.parser as dparser
from magpy.core.methods import testtime, test_timestring
import json
import logging
logger = logging.getLogger(__name__)

KEYLIST=DataStream().KEYLIST

def isNOAAACE(filename):
    """
    Checks whether a file is NOAA ACE format.
    """
    try:
        with open(filename, "rt") as fi:
            temp1= fi.readline()
            temp2= fi.readline()
            temp3= fi.readline()
    except:
        return False
    try:
        if not temp1.startswith(':'):
            return False
        if not 'NOAA' in temp3:
            return False
    except:
        return False
    logger.info("format_noaa: Found ACE file %s" % filename)
    return True


def readNOAAACE(filename, headonly=False, **kwargs):
    """
    Reading NOAA Ace format data.
    ACE data streams use following key formats:
        1min Data:
        ----------
        *MAG:
        Bx [nT]                                 x
        By [nT]                                 y
        Bz [nT]                                 z
        Bt [nT]                                 f
        Lat [degrees +/- 90.0]                  t1
        Lon [degrees 0.0 - 360.0]               t2
        *SWEPAM:
        Proton density [p/cc]                   var1
        Bulk solar wind speed [km/s]            var2
        Ion temperature [degrees K]             var3

        5min data:
        ----------
        *SIS:
        Integral proton flux > 10 MeV [p/cs2-sec-ster]                          x
        Integral proton flux > 30 MeV [p/cs2-sec-ster]                          y
        *EPAM:
        Differential Electron 38-53 Flux [particles/cm2-s-ster-MeV]             z
        Differential Electron 175-315 Flux [particles/cm2-s-ster-MeV]           f
        Differential Proton 47-68 keV Flux [particles/cm2-s-ster-MeV]           var1
        Differential Proton 115-195 keV Flux [particles/cm2-s-ster-MeV]         var2
        Differential Proton 310-580 keV Flux [particles/cm2-s-ster-MeV]         var3
        Differential Proton 795-1193 keV Flux [particles/cm2-s-ster-MeV]        var4
        Differential Proton 1060-1900 keV Flux [particles/cm2-s-ster-MeV]       var5

    """
    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    # Use only clean data (status var S = 0)
    cleandata = kwargs.get('cleandata')
    getfile = True
    tmpdaystring = ''
    daystring = ''
    KEYLIST = DataStream().KEYLIST

    if cleandata == None:
        cleandata = True

    fh = open(filename, 'rt')
    # read file and split text into channels
    stream = DataStream()
    #stream = DataStream([],{})
    # Check whether header infromation is already present
    if stream.header is None:
        headers = {}
    else:
        headers = stream.header

    # get day from filename (platform independent)
    splitpath = os.path.split(filename)
    try:
        splitname = splitpath[1].split('_')
        if len(splitname) == 3: # file is current file
            tmpdaystring = datetime.strftime(datetime.now(timezone.utc).replace(tzinfo=None),'%Y%m%d')
            datatype = splitpath[1].split('_')[1]
        elif len(splitname) == 4:
            tmpdaystring = splitpath[1].split('_')[0]
            datatype = splitpath[1].split('_')[2]
        elif len(splitname) == 5:
            tmpdaystring = splitpath[1].split('_')[1]
            datatype = splitpath[1].split('_')[2]
        elif len(splitname) == 6:
            tmpdaystring = splitpath[1].split('_')[2]
            datatype = splitpath[1].split('_')[2]
        daystring = re.findall(r'\d+',tmpdaystring)[0]
        day = datetime.strftime(datetime.strptime(daystring, "%Y%m%d"),"%Y-%m-%d")
        # Select only files within eventually defined time range
        if starttime:
            if not datetime.strptime(day,'%Y-%m-%d') >= datetime.strptime(datetime.strftime(testtime(starttime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
        if endtime:
            if not datetime.strptime(day,'%Y-%m-%d') <= datetime.strptime(datetime.strftime(testtime(endtime),'%Y-%m-%d'),'%Y-%m-%d'):
                getfile = False
    except:
        if splitpath[0] == '/tmp':
            # internet file accessed
            pass
        else:
            logger.warning("Could not identify typical NOAA date in %s. Reading all ..." % daystring)
        getfile = True

    if getfile:
        array = [[] for key in KEYLIST]
        indtime = KEYLIST.index('time')
        indx = KEYLIST.index('x')
        indy = KEYLIST.index('y')
        indz = KEYLIST.index('z')
        indf = KEYLIST.index('f')
        indt1 = KEYLIST.index('t1')
        indt2 = KEYLIST.index('t2')
        indvar1 = KEYLIST.index('var1')
        indvar2 = KEYLIST.index('var2')
        indvar3 = KEYLIST.index('var3')
        indvar4 = KEYLIST.index('var4')
        indvar5 = KEYLIST.index('var5')
        indstr1 = KEYLIST.index('str1')

        logger.info('readNOAAACE: Reading %s' % (filename))

        for line in fh:
            if line.isspace():
                # blank line
                continue
            elif line.startswith(':'):
                # data info
                pass
            elif line.startswith('#'):
                if line.startswith('# Units:'):
                    unitline = line.replace('# Units:','')
                    unitelem = unitline.split()
                    unit = unitelem[-1]
                    colname = ''
                    keytypes = ['time', 'str1']
                    # NOTE: Some keys repeat themselves. They are ordered so that they do not
                    # double in 1min and 5min combined datasets.
                    for i in range(len(unitelem)-1):
                        colname += unitelem[i]
                    if colname == 'Protondensity':
                        headers['col-var1'] = colname
                        headers['unit-col-var1'] = unit
                    if colname == 'Bulkspeed':
                        headers['col-var2'] = 'Solar wind speed'
                        headers['unit-col-var2'] = unit
                    if colname == 'Iontempeturedegrees':
                        headers['col-var3'] = 'Ion temperature'
                        headers['unit-col-var3'] = unit
                    if colname == 'protonflux':
                        headers['col-x'] = 'Integral Proton flux > 10 MeV'
                        headers['col-y'] = 'Integral Proton flux > 30 MeV'
                        headers['unit-col-x'] = unit
                        headers['unit-col-y'] = unit
                    if colname == 'DifferentialFlux':
                        headers['col-z'] = 'Diff Electron flux 38-53'
                        headers['col-f'] = 'Diff Electron flux 175-315'
                        headers['unit-col-z'] = unit
                        headers['unit-col-f'] = unit
                        headers['col-var1'] = 'Diff Proton flux 47-68 keV'
                        headers['col-var2'] = 'Diff Proton flux 115-195 keV'
                        headers['col-var3'] = 'Diff Proton flux 310-580 keV'
                        headers['col-var4'] = 'Diff Proton flux 795-1193 keV'
                        headers['col-var5'] = 'Diff Proton flux 1060-1900 keV'
                        headers['unit-col-var1'] = unit
                        headers['unit-col-var2'] = unit
                        headers['unit-col-var3'] = unit
                        headers['unit-col-var4'] = unit
                        headers['unit-col-var5'] = unit
                    if colname == 'Bx,By,Bz,Btin':
                        headers['col-x'] = 'Bx'
                        headers['col-y'] = 'By'
                        headers['col-z'] = 'Bz'
                        headers['col-f'] = 'Bt'
                        headers['unit-col-x'] = 'nT'
                        headers['unit-col-y'] = 'nT'
                        headers['unit-col-z'] = 'nT'
                        headers['unit-col-f'] = 'nT'
                        headers['col-t1'] = 'Lat'
                        headers['col-t2'] = 'Lon'
                        headers['unit-col-t1'] = 'degrees'
                        headers['unit-col-t2'] = 'degrees'
            elif headonly:
                # skip data for option headonly
                continue
            else:
                nanval = np.nan
                #row = LineStruct()
                dataelem = line.split()
                date = datetime(int(dataelem[0]),int(dataelem[1]),int(dataelem[2]),int(dataelem[3][:2]),int(dataelem[3][2:]))
                status = int(dataelem[6])
                #row.time = date2num(date)
                array[indstr1].append(status)
                array[indtime].append(date)
                if cleandata == True:
                    if status == 0: # indicates good data
                        usedata = True
                    else:
                        usedata = False
                elif cleandata == False:
                    usedata = True

                if usedata:
                    if datatype == 'swepam':
                        if (float(dataelem[7]) > -9999):
                            array[indvar1].append(float(dataelem[7]))
                        else:
                            array[indvar1].append(nanval)
                        if (float(dataelem[8]) > -9999):
                            array[indvar2].append(float(dataelem[8]))
                        else:
                            array[indvar2].append(nanval)
                        if (float(dataelem[9]) > -9999):
                            array[indvar3].append(float(dataelem[9]))
                        else:
                            array[indvar3].append(nanval)
                    elif datatype == 'sis':
                        if (float(dataelem[7]) > -9999):
                            array[indx].append(float(dataelem[7]))
                        else:
                            array[indx].append(nanval)
                        if (float(dataelem[8]) == 0): # status of high energy
                            keytypes.append('y')
                            if (float(dataelem[9]) > -9999):
                                array[indy].append(float(dataelem[9]))
                            else:
                                array[indy].append(nanval)
                        else:
                            array[indy].append(nanval)
                    elif datatype == 'mag':
                        if (float(dataelem[7]) > -9999):
                            array[indx].append(float(dataelem[7]))
                        else:
                            array[indx].append(nanval)
                        if (float(dataelem[8]) > -9999):
                            array[indy].append(float(dataelem[8]))
                        else:
                            array[indy].append(nanval)
                        if (float(dataelem[9]) > -9999):
                            array[indz].append(float(dataelem[9]))
                        else:
                            array[indz].append(nanval)
                        if (float(dataelem[10]) > -9999):
                            array[indf].append(float(dataelem[10]))
                        else:
                            array[indf].append(nanval)
                        if (float(dataelem[11]) > -9999):
                            array[indt1].append(float(dataelem[11]))
                        else:
                            array[indt1].append(nanval)
                        if (float(dataelem[12]) > -9999):
                            array[indt2].append(float(dataelem[12]))
                        else:
                            array[indt2].append(nanval)
                    elif datatype == 'epam':
                        if (float(dataelem[7]) > -9999):
                            array[indz].append(float(dataelem[7]))
                        else:
                            array[indz].append(nanval)
                        if (float(dataelem[8]) > -9999):
                            array[indf].append(float(dataelem[8]))
                        else:
                            array[indf].append(nanval)
                        if (float(dataelem[9]) == 0):
                            if (float(dataelem[10]) > -9999):
                                array[indvar1].append(float(dataelem[10]))
                            else:
                                array[indvar1].append(nanval)
                            if (float(dataelem[11]) > -9999):
                                array[indvar2].append(float(dataelem[11]))
                            else:
                                array[indvar2].append(nanval)
                            if (float(dataelem[12]) > -9999):
                                array[indvar3].append(float(dataelem[12]))
                            else:
                                array[indvar3].append(nanval)
                            if (float(dataelem[13]) > -9999):
                                array[indvar4].append(float(dataelem[13]))
                            else:
                                array[indvar4].append(nanval)
                            if (float(dataelem[14]) > -9999):
                                array[indvar5].append(float(dataelem[14]))
                            else:
                                array[indvar5].append(nanval)
                        else:
                            array[indvar1].append(nanval)
                            array[indvar2].append(nanval)
                            array[indvar3].append(nanval)
                            array[indvar4].append(nanval)
                            array[indvar5].append(nanval)
                else:
                    if datatype == 'swepam':
                        array[indvar1].append(nanval)
                        array[indvar2].append(nanval)
                        array[indvar3].append(nanval)
                    elif datatype == 'sis':
                        array[indx].append(nanval)
                        array[indy].append(nanval)
                    elif datatype == 'mag':
                        array[indx].append(nanval)
                        array[indy].append(nanval)
                        array[indz].append(nanval)
                        array[indf].append(nanval)
                        array[indt1].append(nanval)
                        array[indt2].append(nanval)
                    elif datatype == 'epam':
                        array[indz].append(nanval)
                        array[indf].append(nanval)
                        array[indvar1].append(nanval)
                        array[indvar2].append(nanval)
                        array[indvar3].append(nanval)
                        array[indvar4].append(nanval)
                        array[indvar5].append(nanval)
    fh.close()

    if datatype == 'swepam':
        keytypes = ['time', 'str1', 'var1', 'var2', 'var3']
    elif datatype == 'sis':
        keytypes = ['time', 'str1', 'x', 'y']
    elif datatype == 'mag':
        keytypes = ['time', 'str1', 'x', 'y', 'z', 'f', 't1', 't2']
    elif datatype == 'epam':
        keytypes = ['time', 'str1', 'z', 'f', 'var1', 'var2', 'var3', 'var4', 'var5']

    for key in keytypes:
        array[KEYLIST.index(key)] = np.asarray(array[KEYLIST.index(key)],dtype=object)

    headers["DataFormat"] = "NOAATXT"

    return DataStream(header=headers, ndarray=np.asarray(array,dtype=object))


def isDSCOVR(filename):
    """
    Checks whether a file is DSCOVR JSON format.
    """
    try:
        jsonfile = open(filename, 'r')
        j = json.load(jsonfile)
    except:
        return False
    try:
        if j.get("domain").get("type") == 'Domain':
            # Found Coverage json - use separate filter
            return False
    except:
        pass
    # j is a list of dictionaries in case of GOES
    try:
        if j[0].get("flux",'') and j[0].get("electron_correction",''):
            # FOUND GOES data
            return False
    except:
        pass
    return True


def readDSCOVR(filename, headonly=False, **kwargs):
    """
    Reading JSON format data.
    """
    stream = DataStream()
    header = {}
    array = [[] for key in KEYLIST]

    with open(filename, 'r') as jsonfile:
        dataset = json.load(jsonfile)
        logger.info('Read: %s, Format: %s ' % (filename, "JSON"))

        fillkeys = ['var1', 'var2', 'var3', 'var4', 'var5', 'x', 'y', 'z', 'f']
        datakeys = dataset[0]
        keydict = {}

        for i, key in enumerate(datakeys):
            if 'time' in key:
                keydict[i] = 'time'
            elif key == 'density':
                keydict[i] = 'var1'
                fillkeys.pop(fillkeys.index('var1'))
            elif key == 'speed':
                keydict[i] = 'var2'
                fillkeys.pop(fillkeys.index('var2'))
            elif key == 'temperature':
                keydict[i] = 'var3'
                fillkeys.pop(fillkeys.index('var3'))
            elif 'bx' in key.lower():
                keydict[i] = 'x'
                fillkeys.pop(fillkeys.index('x'))
            elif 'by' in key.lower():
                keydict[i] = 'y'
                fillkeys.pop(fillkeys.index('y'))
            elif 'bz' in key.lower():
                keydict[i] = 'z'
                fillkeys.pop(fillkeys.index('z'))
            elif 'bt' in key.lower():
                keydict[i] = 'f'
                fillkeys.pop(fillkeys.index('f'))
            else:
                try:
                    keydict[i] = fillkeys.pop(0)
                except IndexError:
                    logger.warning(
                        "CAUTION! Out of available keys for data. {} will not be contained in stream.".format(key))
                    print("CAUTION! Out of available keys for data. {} will not be contained in stream.".format(key))

            if 'time' in key:
                data = [test_timestring(str(x[i])) for x in dataset[1:]]
            else:

                data = [np.nan if x[i] is None else float(x[i]) for x in dataset[1:]]
            array[KEYLIST.index(keydict[i])] = data
            header['col-' + keydict[i]] = key
            if keydict[i] == 'var1' and key == 'density':
                header['unit-col-var1'] = "cm^-3"
            elif keydict[i] == 'var2' and key == 'speed':
                header['unit-col-var2'] = "km/s"
            elif keydict[i] == 'var3':
                header['unit-col-var3'] = "K"
            elif keydict[i] in ['x','y','z','f']:
                 header['unit-col-'+keydict[i]] = "nT"
            elif keydict[i] in ['var1','var2']:
                 header['unit-col-'+keydict[i]] = "deg"
            else:
                 header['unit-col-' + keydict[i]] = ''

    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx], dtype=object)

    stream = DataStream([], header, np.asarray(array, dtype=object))

    return stream


def isXRAY(filename):
    """
    Checks whether a file is DSCOVR JSON format.
    """
    try:
        jsonfile = open(filename, 'r')
        j = json.load(jsonfile)
    except:
        return False
    try:
        if j.get("domain").get("type") == 'Domain':
            # Found Coverage json - use separate filter
            return False
    except:
        pass
    # j is a list of dictionaries in case of GOES
    try:
        if j[0].get("flux",'') and j[0].get("electron_correction",''):
            # Found GOES data
            pass
        else:
            return False
    except:
        pass
    return True

def readXRAY(filename, headonly=False, **kwargs):
    """
    #xray = 'https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json'
    #path = '/home/cobs/SPACE/incoming/GOES/16'
    :param source:
    :param debug:
    :return:
    """
    debug = kwargs.get('debug')
    stream = DataStream()
    array1 = [[] for key in KEYLIST]
    array2 = [[] for key in KEYLIST]
    header = {}
    sat = 0

    header['SensorName'] = 'XRS'
    header['SensorGroup'] =  'GOES SolarSatellite'
    header['SensorType'] =  'XRS'

    with open(filename, 'r') as jsonfile:
        data = json.load(jsonfile)
        logger.info('Read: %s, Format: %s ' % (filename, "JSON"))

        #with urllib.request.urlopen(source) as url:
        #data = json.loads(url.read().decode())
        if debug:
            print("DEBUG", data)
        pos1 = KEYLIST.index('x')
        pos2 = KEYLIST.index('var1')
        for element in data:
            if element.get('energy') == '0.1-0.8nm':
                date = dparser.parse(element.get('time_tag'))
                array1[0].append(date.replace(tzinfo=None))
                array1[pos1].append(float(element.get('flux')))
                array1[pos1+1].append(float(element.get('observed_flux')))
                array1[pos1+2].append(float(element.get('electron_correction')))
                array1[pos1+3].append(int(element.get('electron_contaminaton')))
                sat = element.get('satellite')
                #print (date)
            else:
                date = dparser.parse(element.get('time_tag'))
                var1 = float(element.get('flux'))
                var2 = float(element.get('observed_flux'))
                var3 = float(element.get('electron_correction'))
                var4 = int(element.get('electron_contaminaton'))
                array2[0].append(date.replace(tzinfo=None))
                array2[pos2].append(float(element.get('flux')))
                array2[pos2+1].append(float(element.get('observed_flux')))
                array2[pos2+2].append(float(element.get('electron_correction')))
                array2[pos2+3].append(int(element.get('electron_contaminaton')))
                sat = element.get('satellite')
        header['SensorDataLogger'] =  'GOES{}'.format(sat)
        header['SensorID'] = "{}_{}_0001".format(header.get('SensorName'),header.get('SensorDataLogger'))
        header['col-x'] = '0.1-0.8nm'
        header['unit-col-x'] = 'Watts/mw'
        header['col-y'] = 'observed_flux'
        header['col-z'] = 'electron_correction'
        header['col-f'] = 'electron_contaminaton'
        header['col-var1'] = '0.05-0.4nm'
        header['unit-col-var1'] = 'Watts/mw'
        header['col-var2'] = 'observed_flux'
        header['col-var3'] = 'electron_correction'
        header['col-var4'] = 'electron_contamination'
        stream1 = DataStream(header=header, ndarray=np.asarray([np.asarray(el) for el in array1],dtype=object))
        stream2 = DataStream(header=header, ndarray=np.asarray([np.asarray(el) for el in array2],dtype=object))

        return merge_streams(stream1,stream2)
