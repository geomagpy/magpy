"""
MagPy
Auxiliary input filter - TSF format - iGRAV, SG
Written by Roman Leonhardt June 2012/updated March 2024
- contains test and read function, toDo: write function
"""

#from io import open

from magpy.stream import DataStream
from datetime import datetime
import numpy as np
from magpy.core.methods import testtime, extract_date_from_string

KEYLIST = DataStream().KEYLIST

def OpenFile(filename, mode='w'):
    f = open(filename, mode, newline='')
    return f

def isTSF(filename):
    """
    Checks whether a file is ASCII SG file format.
    """

    try:
        with open(filename, "rt") as fi:
            temp = fi.readline()
    except:
        return False
    try:
        if not temp.startswith('[TSF-file]'):
            return False
    except:
        return False
    return True

def readTSF(filename, headonly=False, **kwargs):
    """
    Reading SG-Gravity data files.
    """

    starttime = kwargs.get('starttime')
    endtime = kwargs.get('endtime')
    channels = kwargs.get('channels')
    debug = kwargs.get('debug')
    getfile = True
    stream = DataStream()
    array = [[] for key in KEYLIST]
    # Check whether header information is already present
    headers = {}

    def get_channels(c, debug=False):
        channellist = list(range(1,16))
        if c:
            clist = []
            c1 = c.split(',')
            for e in c1:
                c2 = None
                if e.find('-') > 0:
                    ce = e.split('-')
                    try:
                        c2 = list(range(int(ce[0]),int(ce[1])))
                    except:
                        pass
                else:
                    try:
                        c2 = [int(e)]
                    except:
                        pass
                if c2:
                    clist.extend(c2)
            if len(clist) > 0:
                channellist = sorted(clist)
        if debug:
            print ("read_tsf: selected the following channels for read (limit <= 15 channels): {}".format(channellist[:15]))
        return channellist[:15]

    channellist = get_channels(channels, debug=debug)

    theday = extract_date_from_string(filename)

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

    fh = open(filename, 'rt')

    ncol = 0
    ncoli = 0
    ucol = 0
    ucoli = 0
    dind = 0
    getchannel = False
    getunit = False
    if getfile:
        datablogstarts = False
        for line in fh:
            if line.isspace():
                # blank line
                continue
            #elif line.startswith(' '):
            #    continue
            elif line.startswith('[TSF-file]'):
                contline = line.split()
                stream.header['DataFormat'] = "TSF"
            elif line.startswith('[TIMEFORMAT]'):
                contline = line.split()
                val = contline[1]
            elif line.startswith('[INCREMENT]'):
                contline = line.split()
                stream.header['DataSamplingRate'] = contline[1]
            elif line.startswith('[CHANNELS]'):
                getchannel = True
                #line = fh.readline()
                #while not line.startswith('['):
                #    #except:
                #    #    pass
                #    # eventually do ot like that
                #
                #CO:SG025:Grav-1
                #CO:SG025:Grav-2
                #CO:SG025:Baro-1
                #CO:SG025:Baro-2
                pass
            elif line.startswith('   ') and getchannel:
                ncol += 1
                if ncol in channellist:
                    ncoli += 1
                    colnames = line.split(':')[2]
                    key = KEYLIST[ncoli]
                    stream.header['col-'+key] = colnames.strip()
                #else:
                #    ncol = 15
            elif line.startswith('[UNITS]'):
                getchannel = False
                getunit = True
            elif line.startswith('   ') and getunit:
                ucol += 1
                if ucol in channellist:
                    ucoli += 1
                    unitnames = line.strip()
                    key = KEYLIST[ucoli]
                    stream.header['unit-col-'+key] = unitnames
                #else:
                #    ucol = 15
                #VOLT
                #VOLT
                #mbar
                #mbar
            elif line.startswith('[UNDETVAL]'):
                getunit = False
                pass
            elif line.startswith('[PHASE_LAG_1_DEG_CPD]'):
                #0.0390
                pass
            elif line.startswith('[PHASE_LAG_1_DEG_CPD_ERROR]'):
                #0.0001
                pass
            elif line.startswith('[N_LATITUDE_DEG]'):
                #47.9288
                contline = line.split()
                stream.header['DataAcquisitionLatitude'] = contline[1]
            elif line.startswith('[N_LATITUDE_DEG_ERROR]'):
                #0.0005
                pass
            elif line.startswith('[E_LONGITUDE_DEG]') :
                #015.8609
                contline = line.split()
                stream.header['DataAcquisitionLongitude'] = contline[1]
            elif line.startswith('[E_LONGITUDE_DEG_ERROR]'):
                #0.0005
                pass
            elif line.startswith('[HEIGHT_M_1]'):
                #1045.00
                contline = line.split()
                stream.header['DataElevation'] = contline[1]
            elif line.startswith('[HEIGHT_M_1_ERROR]'):
                #0.10
                pass
            elif line.startswith('[GRAVITY_CAL_1_UGAL_V]'):
                #-77.8279
                contline = line.split()
                stream.header['DataScaleX'] = contline[1]
            elif line.startswith('[GRAVITY_CAL_1_UGAL_V_ERROR]'):
                #0.5000
                pass
            elif line.startswith('[PRESSURE_CAL_MBAR_V]'):
                #1.0000
                contline = line.split()
                stream.header['DataScaleY'] = contline[1]
            elif line.startswith('[PRESSURE_CAL_MBAR_V_ERROR]'):
                #0.0001
                pass
            elif line.startswith('[AUTHOR]'):
                #(bruno.meurers@univie.ac.at)
                contline = line.split()
                stream.header['SensorDecription'] = contline[1]
            elif line.startswith('[PHASE_LAG_2_DEG_CPD]'):
                #0.0000
                pass
            elif line.startswith('[PHASE_LAG_2_DEG_CPD_ERROR]'):
                #0.0000
                pass
            elif line.startswith('[HEIGHT_M_2]'):
                #00.00
                pass
            elif line.startswith('[HEIGHT_M_2_ERROR]'):
                #0.00
                pass
            elif line.startswith('[GRAVITY_CAL_2_UGAL_V]'):
                #-77.8279
                contline = line.split()
                stream.header['DataScaleZ'] = contline[1]
            elif line.startswith('[GRAVITY_CAL_2_UGAL_V_ERROR]'):
                #0.5000
                pass
            elif line.startswith('[PRESSURE_ADMIT_HPA_NMS2]'):
                #03.5300
                pass
            elif line.startswith('[PRESSURE_MEAN_HPA]'):
                #1000.0
                pass
            elif line.startswith('[COMMENT]'):
                pass
                #SG CT-025 Moved from Vienna to Conrad Observatory 2007/11/07
                #Institute of Meteorology and Geophysics Vienna, Austria
                #Instrument owner Central Institute for Meteorology and Geodynamics
                #Geology Limestone
                #Calibration method LSQ fit to absolute gravity measurements
                #Installation by Eric Brinton (GWR) November 7, 2007
                #Installation Team N.Blaumoser, S.Haden, P.Melichar, B.Meurers, R.Steiner
                #Maintenance by N.Blaumoser, M.Goeschke, S.Haden, B.Meurers
                #date           time       Grav_1     Grav_2    Baro_1    Baro_2
            elif line.startswith('[DATA]'):
                datablogstarts = True
                if headonly:
                    # skip data for option headonly
                    return stream
            else:
                if datablogstarts:
                    dind = 0
                    # Read data - select according to channels
                    colsstr = line.split()
                    datatime = colsstr[0]+'-'+colsstr[1]+'-'+colsstr[2]+'T'+colsstr[3]+':'+colsstr[4]+':'+colsstr[5]
                    array[0].append(datetime.strptime(datatime,"%Y-%m-%dT%H:%M:%S"))
                    for n in channellist:
                        dind += 1
                        if n < len(colsstr)-5:
                            array[dind].append(float(colsstr[n+5]))
                else:
                    # some header lines not noted above found
                    pass

    for idx, elem in enumerate(array):
        array[idx] = np.asarray(array[idx])

    stream = DataStream(header=stream.header,ndarray=np.asarray(array,dtype=object))

    fh.close()
    return stream
